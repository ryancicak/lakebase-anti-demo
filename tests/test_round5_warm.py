from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server.round5_warm import (
    MAX_PROVENANCE_PROBE_FAILURES,
    SELF_VERIFIABLE_BLOCK_RETRY_SECONDS,
    WARM_ATTEMPT_TOKEN_REUSE_SECONDS,
    BlockedWarmError,
    InMemoryRound5WarmStore,
    LakebaseRound5WarmStore,
    RetryableWarmError,
    Round5LaunchCapsule,
    Round5RunnerReceipt,
    Round5SharedReceipt,
    Round5Variant,
    Round5VariantReceipt,
    Round5WarmCoordinator,
    Round5WarmPreparation,
    Round5WarmState,
    WarmClaimUnavailableError,
    WarmFenceLostError,
    WarmStoreConflictError,
    _slot_from_json,
    _to_json,
)

DIGEST = "a" * 64


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 9, 15, tzinfo=UTC)
        self.monotonic_ns = 1_000

    def __call__(self) -> datetime:
        return self.now

    def monotonic(self) -> int:
        value = self.monotonic_ns
        self.monotonic_ns += 1
        return value

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)
        self.monotonic_ns += int(seconds * 1_000_000_000)


def capsule(
    clock: Clock,
    *,
    generation: int,
    fence: int,
    credential_generation: int = 1,
    renew_in: float = 2_000,
    warm_attempt_token: str | None = None,
    broker_epoch: str | None = None,
) -> Round5LaunchCapsule:
    return Round5LaunchCapsule(
        generation=generation,
        coordinator_fence=fence,
        credential_generation=credential_generation,
        broker_epoch=broker_epoch or f"broker-{credential_generation}",
        runner_contexts={"lakebase": object(), "competitor": object()},
        aws_control_contexts={
            Round5Variant.AURORA: object(),
            Round5Variant.RDS: object(),
        },
        lakebase_context=object(),
        variant_contexts={
            Round5Variant.AURORA: object(),
            Round5Variant.RDS: object(),
        },
        control_expires_at=clock.now + timedelta(seconds=4_000),
        dispatch_expires_at={
            "lakebase": clock.now + timedelta(seconds=4_000),
            "competitor": clock.now + timedelta(seconds=4_000),
        },
        expires_at=clock.now + timedelta(seconds=3_600),
        renew_by=clock.now + timedelta(seconds=renew_in),
        warm_attempt_token=(
            warm_attempt_token
            or f"attempt-{generation}-{credential_generation}"
        ),
    )


def preparation(
    clock: Clock,
    *,
    generation: int,
    fence: int,
    credential_generation: int = 1,
    renew_in: float = 2_000,
    warm_attempt_token: str | None = None,
    broker_epoch: str | None = None,
) -> Round5WarmPreparation:
    expires_at = clock.now + timedelta(seconds=4_500)
    runners = {
        lane: Round5RunnerReceipt(
            lane_id=lane,
            instance_id=instance_id,
            boot_id=f"boot-{lane}",
            process_boot_id=f"process-{lane}",
            process_pid=101 if lane == "lakebase" else 102,
            instance_type="c7i.2xlarge",
            image_sha256=DIGEST,
            loaded_harness_sha256=DIGEST,
            capacity_model_sha256=DIGEST,
            expires_at=expires_at,
        )
        for lane, instance_id in (
            ("lakebase", "i-0123456789abcdef0"),
            ("competitor", "i-0fedcba9876543210"),
        )
    }
    shared = Round5SharedReceipt(
        source_sha256=DIGEST,
        config_sha256=DIGEST,
        runner_image_sha256=DIGEST,
        fanin_contract_sha256=DIGEST,
        capacity_model_sha256=DIGEST,
        lakebase_binding_sha256=DIGEST,
        static_network_fixture_sha256=DIGEST,
        lakebase_runner=runners["lakebase"],
        competitor_runner=runners["competitor"],
    )
    variants = {
        variant: Round5VariantReceipt(
            variant=variant,
            target_sha256=DIGEST,
            source_sha256=DIGEST,
            secret_ref_sha256=DIGEST,
            role_sha256=DIGEST,
            auth_sha256=DIGEST,
            tls_sha256=DIGEST,
            security_group_sha256=DIGEST,
            subnet_sha256=DIGEST,
            vpc_sha256=DIGEST,
            proxy_absent=True,
            proxy_absence_observed_at=clock.now,
            request_template_sha256=DIGEST,
            expires_at=expires_at,
        )
        for variant in Round5Variant
    }
    return Round5WarmPreparation(
        shared_receipt=shared,
        variants=variants,
        capsule=capsule(
            clock,
            generation=generation,
            fence=fence,
            credential_generation=credential_generation,
            renew_in=renew_in,
            warm_attempt_token=warm_attempt_token,
            broker_epoch=broker_epoch,
        ),
    )


class Provider:
    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.prepare_started = asyncio.Event()
        self.release_prepare = asyncio.Event()
        self.prepare_calls = 0
        self.refresh_calls = 0
        self.reconcile_calls = 0
        self.reconcile_result = False
        self.provenance_current = True
        self.validate_calls = 0
        self.prepare_error: Exception | None = None
        self.refresh_error: Exception | None = None
        self.validate_error: Exception | None = None
        self.refresh_preparation_override: (
            Callable[[object, Round5LaunchCapsule], Round5WarmPreparation] | None
        ) = None
        self.attempt_tokens: list[str] = []
        self.requires_cleaned_bout_calls: list[bool] = []
        # Optional override so a test can mint a preparation with a mutated
        # (e.g. unattested) receipt to exercise the fresh-PRELOAD invariant guard.
        self.preparation_factory: Callable[..., Round5WarmPreparation] = preparation

    async def reconcile(self, slot) -> bool:
        del slot
        self.reconcile_calls += 1
        return self.reconcile_result

    async def validate_ready(self, slot, launch_capsule) -> bool:
        del slot, launch_capsule
        self.validate_calls += 1
        if self.validate_error is not None:
            raise self.validate_error
        return self.provenance_current

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
        requires_cleaned_bout: bool,
    ) -> Round5WarmPreparation:
        del process_epoch
        self.attempt_tokens.append(warm_attempt_token)
        self.requires_cleaned_bout_calls.append(requires_cleaned_bout)
        self.prepare_calls += 1
        self.prepare_started.set()
        await self.release_prepare.wait()
        if self.prepare_error is not None:
            raise self.prepare_error
        return self.preparation_factory(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            warm_attempt_token=warm_attempt_token,
            broker_epoch=broker_epoch,
        )

    async def refresh_capsule(
        self,
        slot,
        previous: Round5LaunchCapsule,
    ) -> Round5LaunchCapsule:
        self.refresh_calls += 1
        if self.refresh_error is not None:
            raise self.refresh_error
        return capsule(
            self.clock,
            generation=slot.generation,
            fence=slot.coordinator_fence,
            credential_generation=previous.credential_generation + 1,
            warm_attempt_token=previous.warm_attempt_token,
            broker_epoch=previous.broker_epoch,
        )

    async def refresh_preparation(
        self,
        slot,
        previous: Round5LaunchCapsule,
    ) -> Round5WarmPreparation:
        # The keep-alive republishes a FRESH preparation off a live probe:
        # the runner identity (boot ids / digests) is deterministic here, so it
        # is byte-identical to the warm's receipts (immutable identity), while
        # expires_at advances to clock.now + horizon (renewable provenance).
        self.refresh_calls += 1
        if self.refresh_error is not None:
            raise self.refresh_error
        if self.refresh_preparation_override is not None:
            return self.refresh_preparation_override(slot, previous)
        return preparation(
            self.clock,
            generation=slot.generation,
            fence=slot.coordinator_fence,
            credential_generation=previous.credential_generation + 1,
            warm_attempt_token=previous.warm_attempt_token,
            broker_epoch=previous.broker_epoch,
        )


def coordinator(
    clock: Clock,
    provider: Provider,
    store: InMemoryRound5WarmStore | None = None,
    *,
    process_epoch: str = "process-one",
) -> Round5WarmCoordinator:
    return Round5WarmCoordinator(
        installation_id="install-one",
        warm_contract_sha256=DIGEST,
        store=store or InMemoryRound5WarmStore(),
        provider=provider,
        process_epoch=process_epoch,
        broker_epoch="broker-one",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )


async def warm_ready(
    manager: Round5WarmCoordinator,
    provider: Provider,
) -> None:
    await manager.store.initialize()
    first = asyncio.create_task(manager.run_one_cycle())
    await asyncio.wait_for(provider.prepare_started.wait(), timeout=1)
    provider.release_prepare.set()
    await asyncio.wait_for(first, timeout=1)


async def test_start_is_live_during_a_simulated_3600_second_automatic_warm() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)

    task = await manager.start()
    await asyncio.wait_for(provider.prepare_started.wait(), timeout=1)
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert not task.done()
    status = await manager.public_status()
    assert status["round5_ring_ready"] is False

    clock.advance(3_600)
    provider.release_prepare.set()
    for _ in range(100):
        slot = await manager.store.read("install-one")
        if slot is not None and slot.state == Round5WarmState.READY:
            break
        await asyncio.sleep(0)
    assert slot is not None and slot.state == Round5WarmState.READY
    assert set(slot.variants) == {Round5Variant.AURORA, Round5Variant.RDS}
    assert all(receipt.proxy_absent for receipt in slot.variants.values())
    assert await manager.public_status() == {
        **await manager.public_status(),
        "round5_ring_ready": True,
    }
    await manager.close()


async def test_claim_is_o1_and_performs_no_provider_work() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    counts = (
        provider.prepare_calls,
        provider.refresh_calls,
        provider.reconcile_calls,
        provider.validate_calls,
    )

    claimed, claimed_capsule = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )

    assert claimed.state == Round5WarmState.CLAIMED
    assert claimed_capsule is manager.capsule
    assert provider.prepare_calls == counts[0]
    assert provider.refresh_calls == counts[1]
    assert provider.reconcile_calls == counts[2]
    assert provider.validate_calls == counts[3]


async def test_return_unstarted_claim_restores_same_ready_generation() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    ready = await manager.store.read("install-one")
    assert ready is not None
    generation = ready.generation

    claimed, _ = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    assert claimed.claim is not None

    returned = await manager.return_unstarted_claim(claimed.claim.claim_id)
    assert returned.state == Round5WarmState.READY
    assert returned.generation == generation
    assert returned.claim is None

    claimed_again, _ = await manager.claim(
        session_id="session-two",
        bout_id="bout-two",
        selected_variant=Round5Variant.AURORA,
        bout_fence=8,
    )
    assert claimed_again.state == Round5WarmState.CLAIMED
    assert claimed_again.generation == generation


async def test_atomic_claim_allows_concurrent_same_generation_renewal() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    ready = await manager.store.read("install-one")
    assert ready is not None
    locked_slot = replace(
        ready,
        revision=ready.revision + 2,
        provenance_expires_at=ready.provenance_expires_at + timedelta(seconds=30),
    )
    locked_slot_holder = [locked_slot]
    supplied_now = clock.now - timedelta(minutes=5)
    locked_now = clock.now + timedelta(seconds=3)
    statements: list[str] = []

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return False

    class Connection:
        def transaction(self):
            return Transaction()

    class Cursor:
        connection = Connection()
        rows: list[tuple[object, ...]] = []

        async def execute(self, statement, parameters=None):
            text = str(statement)
            statements.append(text)
            if "SELECT ring_key, fencing_token" in text:
                self.rows = [
                    ("main-ring", 4, None, supplied_now),
                    ("cleanup-ring", 8, None, supplied_now),
                ]
            elif "SELECT payload" in text and "round5_warm_slot" in text:
                self.rows = [(json.dumps(_to_json(locked_slot_holder[0])),)]
            elif "SELECT clock_timestamp()" in text:
                self.rows = [(locked_now,)]
            elif "UPDATE anti_demo_coordination.ring_lease" in text:
                self.rows = [(locked_now, locked_now, locked_now + timedelta(seconds=90))]
            else:
                self.rows = []

        async def fetchall(self):
            return self.rows

        async def fetchone(self):
            return self.rows[0] if self.rows else None

    cursor = Cursor()

    async def run(callback):
        return await callback(cursor)

    store = LakebaseRound5WarmStore(run)

    async def claim():
        return await store.claim_ready_with_leases(
            ready,
            session_id="session-one",
            bout_id="bout-one",
            selected_variant=Round5Variant.AURORA,
            capsule_generation=ready.generation,
            capsule_broker_epoch=ready.broker_epoch,
            capsule_warm_attempt_token=ready.warm_attempt_token,
            main_ring_key="main-ring",
            cleanup_ring_key="cleanup-ring",
            operator=SimpleNamespace(
                subject="operator-one",
                display_name="Operator",
                email="operator@example.com",
            ),
            round_id="survive_connection_spike",
            round_title="Connection Spike",
            competitor_id="aurora_serverless_v2",
            competitor_name="Aurora",
            now=supplied_now,
            ttl=timedelta(seconds=90),
            claim_ttl=timedelta(seconds=60),
        )

    claimed, main, cleanup = await claim()
    assert claimed.claim is not None
    assert claimed.revision == locked_slot.revision + 1
    assert claimed.generation == ready.generation
    assert claimed.coordinator_fence == ready.coordinator_fence
    assert claimed.state == Round5WarmState.CLAIMED
    assert claimed.claim.capsule_generation == ready.generation
    assert claimed.claim.warm_attempt_token == ready.warm_attempt_token
    assert claimed.claim.claimed_at == locked_now
    assert claimed.claim.claim_expires_at == locked_now + timedelta(seconds=60)
    assert main.started_at == cleanup.started_at == locked_now
    materialize_index = next(
        index
        for index, value in enumerate(statements)
        if "INSERT INTO anti_demo_coordination.ring_lease" in value
    )
    lock_indexes = [
        index for index, value in enumerate(statements) if "FOR UPDATE" in value
    ]
    clock_index = next(
        index
        for index, value in enumerate(statements)
        if "SELECT clock_timestamp()" in value
    )
    assert lock_indexes and materialize_index < min(lock_indexes) < clock_index
    assert claimed.claim is not None
    assert claimed.claim.lakebase_job_id != claimed.claim.competitor_job_id

    assert ready.shared_receipt is not None
    mutated_shared_identity = replace(
        ready.shared_receipt,
        config_sha256="1" * 64,
    )
    mutated_variant_identity = dict(ready.variants)
    mutated_variant_identity[Round5Variant.AURORA] = replace(
        ready.variants[Round5Variant.AURORA],
        auth_sha256="2" * 64,
    )
    expired_shared_receipt = replace(
        ready.shared_receipt,
        lakebase_runner=replace(
            ready.shared_receipt.lakebase_runner,
            expires_at=locked_now - timedelta(seconds=1),
        ),
    )
    unsafe_locked_slots = (
        replace(
            ready,
            revision=ready.revision + 3,
            broker_epoch="broker-refreshed",
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            warm_attempt_token="attempt-rewarmed",
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            coordinator_fence=ready.coordinator_fence + 1,
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            generation=ready.generation + 1,
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            provenance_expires_at=locked_now - timedelta(seconds=1),
        ),
        replace(claimed, revision=ready.revision + 3),
        replace(
            ready,
            revision=ready.revision + 3,
            ready_expires_at=locked_now - timedelta(seconds=1),
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            renew_by=locked_now - timedelta(seconds=1),
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            shared_receipt=expired_shared_receipt,
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            shared_receipt=mutated_shared_identity,
        ),
        replace(
            ready,
            revision=ready.revision + 3,
            variants=mutated_variant_identity,
        ),
    )
    for unsafe_locked_slot in unsafe_locked_slots:
        locked_slot_holder[0] = unsafe_locked_slot
        with pytest.raises(
            WarmClaimUnavailableError,
            match="READY generation changed while claiming",
        ):
            await claim()


async def test_runner_reboot_invalidates_ready_before_claim_or_bell() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    assert manager.ring_ready

    # An ATTESTED runner identity change (reboot) surfaces as validate_ready -> False
    # (Patch 1 tri-state: IDENTITY_CHANGED, distinct from a merely STALE/ABSENT beat),
    # and must fail closed AT ONCE -- this is the case that legitimately demotes.
    provider.provenance_current = False
    assert await manager.run_one_cycle() == 0.0
    with pytest.raises(WarmClaimUnavailableError):
        await manager.claim(
            session_id="session-one",
            bout_id="bout-one",
            selected_variant=Round5Variant.AURORA,
            bout_fence=7,
        )
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.last_error_code == "runner_provenance_changed"
    assert not manager.ring_ready


async def test_credential_rotation_swaps_capsule_without_losing_readiness() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    before = manager.capsule
    assert before is not None
    before_slot = await manager.store.read("install-one")
    assert before_slot is not None and before_slot.ready_expires_at is not None
    before_expiry = before_slot.ready_expires_at
    clock.advance(2_001)

    await manager.run_one_cycle()

    assert manager.capsule is not None
    assert manager.capsule is not before
    assert manager.capsule.credential_generation == 2
    after_slot = await manager.store.read("install-one")
    assert after_slot is not None and after_slot.ready_expires_at is not None
    assert after_slot.ready_expires_at > before_expiry
    assert after_slot.ready_expires_at == min(
        after_slot.shared_receipt.lakebase_runner.expires_at,
        after_slot.shared_receipt.competitor_runner.expires_at,
        *(receipt.expires_at for receipt in after_slot.variants.values()),
        manager.capsule.expires_at,
    )
    assert (await manager.public_status())["round5_ring_ready"] is True


@pytest.mark.parametrize("identity_part", ("shared", "runner", "variant"))
async def test_receipt_renewal_rejects_every_nonrenewable_identity_class(
    identity_part: str,
) -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    slot = await manager.store.read("install-one")
    previous = manager.capsule
    assert slot is not None and slot.shared_receipt is not None
    assert previous is not None

    clock.advance(60)
    refreshed = preparation(
        clock,
        generation=slot.generation,
        fence=slot.coordinator_fence,
        credential_generation=previous.credential_generation + 1,
        warm_attempt_token=previous.warm_attempt_token,
        broker_epoch=previous.broker_epoch,
    )
    shared_receipt = refreshed.shared_receipt
    variants = dict(refreshed.variants)
    if identity_part == "shared":
        shared_receipt = replace(shared_receipt, config_sha256="1" * 64)
    elif identity_part == "runner":
        shared_receipt = replace(
            shared_receipt,
            lakebase_runner=replace(
                shared_receipt.lakebase_runner,
                process_pid=shared_receipt.lakebase_runner.process_pid + 1,
            ),
        )
    else:
        variants[Round5Variant.RDS] = replace(
            variants[Round5Variant.RDS],
            request_template_sha256="2" * 64,
        )

    with pytest.raises(WarmFenceLostError, match="renewed receipt .* identity changed"):
        await manager.store.update_capsule_receipt(
            slot,
            capsule=refreshed.capsule,
            shared_receipt=shared_receipt,
            variants=variants,
            now=clock.now,
        )


async def test_retryable_refresh_stays_ready_then_escalates_when_margin_lost() -> None:
    # A transient (throttled/timed-out) credential+receipt refresh is an in-place
    # retry, NOT a teardown: the still-held capsule is valid, so READY persists and
    # the beat retries shortly. Only once the held credential can no longer meet
    # the launch margin does it escalate to a full rewarm (freshness_lost).
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    provider.refresh_error = RetryableWarmError("credential_refresh_timeout")

    clock.advance(2_001)  # past renew_by, but the launch-margin cliff still holds
    delay = await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    # Stays READY and keeps the capsule (in-place retry), NOT torn down to WARMING.
    # Patch 3: it also stays CLAIMABLE across this window -- renew_by is only the
    # refresh trigger, not the claimability cliff, so a transient refresh failure past
    # renew_by (while the launch margin still holds) must NOT flash the idle fight card
    # Temporarily Unavailable. Mutation: reintroducing `renew_by > now` to _claimable
    # makes ring_ready False here and fails this assertion.
    assert slot.state == Round5WarmState.READY
    assert manager.capsule is not None
    assert manager.ring_ready
    assert delay > 0

    clock.advance(200)  # now the held credential can no longer meet the margin
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.last_error_code == "credential_refresh_retryable"
    assert manager.capsule is None
    assert (await manager.public_status())["round5_ring_ready"] is False


async def test_idle_keepalive_soak_stays_claimable_across_credential_cliffs() -> None:
    # Time-accelerated idle soak (Fix C / Patch 3): drive the keep-alive across MANY
    # credential launch-margin cliffs, renew_by walls, and provenance windows, and
    # assert the idle ring is claimable on EVERY cycle -- i.e., zero "Temporarily
    # Unavailable" flickers. The refresh fires with lead before the true cliff, so the
    # capsule is always republished before it can lapse.
    # Mutation: gating _claimable on `renew_by > now` (or refreshing off renew_by after
    # the cliff, or instant-demoting a stale probe) drops ring_ready on some step.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    assert manager.ring_ready
    refreshes_before = provider.refresh_calls

    # 10s steps: below provenance freshness (15s) so the per-cycle provenance renewal
    # keeps it fresh, and below the refresh lead (60s) so the cliff refresh always
    # fires in time. ~4000s of simulated continuous idle.
    for step in range(400):
        clock.advance(10)
        await manager.run_one_cycle()
        slot = await manager.store.read("install-one")
        assert slot is not None and slot.state == Round5WarmState.READY, (
            f"left READY at step {step} (t+{(step + 1) * 10}s): {slot.state if slot else None}"
        )
        assert manager.ring_ready, f"idle flicker at step {step} (t+{(step + 1) * 10}s)"

    # The credential was actually refreshed across the soak (not a frozen capsule).
    assert provider.refresh_calls > refreshes_before


async def test_stale_attestation_is_strike_not_identity_change() -> None:
    # Patch 1 / F: a STALE or ABSENT resident heartbeat surfaces as RetryableWarmError
    # (validate_ready), which the coordinator absorbs with the provenance strike budget
    # -- idle READY and claimability persist. Only after the budget is exhausted does a
    # persistent miss demote. Mutation: mapping stale attestation to an instant
    # freshness_lost demotes on the first cycle and fails the in-loop READY assertions.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    provider.validate_error = RetryableWarmError("runner_attestation_stale")

    for _ in range(MAX_PROVENANCE_PROBE_FAILURES - 1):
        delay = await manager.run_one_cycle()
        assert delay > 0  # in-place retry, not a teardown
        slot = await manager.store.read("install-one")
        assert slot is not None and slot.state == Round5WarmState.READY
        assert manager.ring_ready  # stale beat never withdraws idle claimability

    # Budget spent -> a persistent miss finally fails closed.
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None and slot.state == Round5WarmState.WARMING


async def test_persistent_delivery_failure_while_idle_soft_degrades_without_teardown() -> None:
    # Patch 2: a sustained outbox failure while Round 5 is IDLE (no active claim) must
    # SOFT-degrade -- refuse new claims but keep the durable READY slot and the
    # in-memory capsule intact (no freshness_lost, no WARMING rewarm). Mutation:
    # hard-demoting on idle (freshness_lost / _capsule=None) tears down a ring that had
    # no bout to protect and reintroduces the idle Temporarily Unavailable churn.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    assert manager.ring_ready

    await manager.invalidate_readiness("resident_control_delivery_failed")

    slot = await manager.store.read("install-one")
    assert slot is not None
    # Durable slot stays READY; the capsule is retained for instant recovery.
    assert slot.state == Round5WarmState.READY
    assert manager.capsule is not None
    # But new claims are refused (soft-degraded) until the control plane recovers.
    assert not manager.ring_ready
    with pytest.raises(WarmClaimUnavailableError):
        await manager.claim(
            session_id="session-degraded",
            bout_id="bout-degraded",
            selected_variant=Round5Variant.AURORA,
            bout_fence=7,
        )

    # A CURRENT keep-alive probe proves the control plane healthy again -> re-admit.
    provider.provenance_current = True
    await manager.run_one_cycle()
    assert manager.ring_ready


async def test_delivery_failure_during_active_bout_does_not_touch_warm_slot() -> None:
    # Patch 2: while a bout is ACTIVE the warm slot is CLAIMED (never READY), so
    # invalidate_readiness is a no-op on the warm slot -- it must not soft-degrade or
    # rewarm a ring that a live bout owns; the active bout fails through its own
    # control-delivery path. Mutation: an unconditional freshness_lost here would
    # corrupt/steal the live bout's claimed ring.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-active",
        bout_id="bout-active",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    assert claimed.state == Round5WarmState.CLAIMED

    await manager.invalidate_readiness("resident_control_delivery_failed")

    slot = await manager.store.read("install-one")
    assert slot is not None
    # The claimed ring is untouched -- still CLAIMED for the live bout.
    assert slot.state == Round5WarmState.CLAIMED
    assert slot.claim is not None
    assert not manager._readiness_degraded


async def test_retryable_warm_retry_reuses_the_same_in_flight_token() -> None:
    # Bounded token mint rate (post-bout rewarm storm fix). A retryable WARMING
    # failure must RE-PRELOAD the SAME in-flight token rather than mint a distinct
    # token every beat: the single-bout, systemd-respawned resident can only ever
    # beat ONE token, so a per-beat mint produces a superseded PRELOAD backlog it
    # can never drain (validate_ready reads ABSENT/STALE forever -- the observed
    # 149/149 post-towel idle flicker). The token reused across the retry is the
    # exact one that finally publishes READY.
    clock = Clock()
    provider = Provider(clock)
    provider.prepare_error = RetryableWarmError("first_attempt_failed")
    manager = coordinator(clock, provider)
    provider.release_prepare.set()
    await manager.run_one_cycle()
    first = (await manager.store.read("install-one")).warm_attempt_token
    provider.prepare_error = None
    clock.advance(2)
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert first is not None
    assert slot.state == Round5WarmState.READY
    # REUSED, not rotated: exactly one live PRELOAD token, and it is the one now READY.
    assert slot.warm_attempt_token == first
    assert provider.attempt_tokens == [first, first]


async def test_warm_attempt_token_rotates_once_after_reuse_window_lapses() -> None:
    # If the resident never attests the in-flight token, the coordinator still does
    # not churn a fresh token per beat: it reuses the SAME token until the bounded
    # reuse window lapses, then deliberately rotates EXACTLY once (a fresh delivered
    # PRELOAD), so the outbox never accrues an unbounded superseded backlog.
    clock = Clock()
    provider = Provider(clock)
    provider.prepare_error = RetryableWarmError("resident_restarting")
    manager = coordinator(clock, provider)
    provider.release_prepare.set()
    await manager.run_one_cycle()  # attempt 1: mint `first`, backoff <= 1s
    first = (await manager.store.read("install-one")).warm_attempt_token
    clock.advance(2)
    await manager.run_one_cycle()  # attempt 2: REUSE `first` (well within window)
    assert set(provider.attempt_tokens) == {first}
    # Cross the reuse window (and any accrued backoff); the next mint rotates ONCE.
    clock.advance(WARM_ATTEMPT_TOKEN_REUSE_SECONDS + 5)
    await manager.run_one_cycle()
    rotated = set(provider.attempt_tokens)
    assert len(rotated) == 2  # one deliberate rotation, not per-beat churn
    assert first in rotated


async def test_ready_clears_reuse_so_freshness_lost_mints_fresh_token() -> None:
    # Anti-replay: a token may be reused only WHILE in-flight and never-READY. Once
    # it publishes READY (becomes publicly claimable), a later freshness_lost must
    # mint a genuinely FRESH token instead of re-warming the already-claimable one.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    provider.release_prepare.set()
    await manager.run_one_cycle()  # -> READY(token_ready)
    ready = await manager.store.read("install-one")
    assert ready is not None and ready.state == Round5WarmState.READY
    token_ready = ready.warm_attempt_token
    # A genuine attested identity change demotes READY -> WARMING.
    provider.provenance_current = False
    await manager.run_one_cycle()
    provider.provenance_current = True
    clock.advance(2)
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.warm_attempt_token != token_ready
    # The already-READY token is never re-preloaded after it left READY.
    assert provider.attempt_tokens[1:].count(token_ready) == 0


async def test_publish_ready_blocked_when_preparation_lacks_fresh_preload() -> None:
    # Fresh-PRELOAD invariant: a preparation whose runner receipts carry an
    # UNATTESTED resident process identity (exactly what a skipped or superseded
    # re-PRELOAD would yield) must fail closed with a distinct, named block and
    # never reach READY on stale evidence.
    clock = Clock()
    provider = Provider(clock)

    def _unattested(*args, **kwargs):
        prep = preparation(*args, **kwargs)
        stale_runners = {
            "lakebase": replace(prep.shared_receipt.lakebase_runner, process_boot_id="unattested"),
            "competitor": replace(
                prep.shared_receipt.competitor_runner, process_boot_id="unattested"
            ),
        }
        shared = replace(
            prep.shared_receipt,
            lakebase_runner=stale_runners["lakebase"],
            competitor_runner=stale_runners["competitor"],
        )
        return replace(prep, shared_receipt=shared)

    provider.preparation_factory = _unattested
    manager = coordinator(clock, provider)
    provider.release_prepare.set()
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.BLOCKED
    assert slot.last_error_code == "warm_ready_without_fresh_preload"


def _refreshed_preparation(
    clock: Clock,
    slot,
    previous: Round5LaunchCapsule,
    **capsule_overrides: object,
) -> Round5WarmPreparation:
    prep = preparation(
        clock,
        generation=slot.generation,
        fence=slot.coordinator_fence,
        credential_generation=previous.credential_generation + 1,
        warm_attempt_token=previous.warm_attempt_token,
    )
    if capsule_overrides:
        return replace(prep, capsule=replace(prep.capsule, **capsule_overrides))
    return prep


async def test_expired_renew_by_refresh_fails_closed_without_refresh_loop() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    clock.advance(2_001)

    provider.refresh_preparation_override = lambda slot, previous: _refreshed_preparation(
        clock, slot, previous, renew_by=clock.now - timedelta(seconds=1)
    )
    assert await manager.run_one_cycle() == 0.0
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.last_error_code == "credential_refresh_expired"
    assert provider.refresh_calls == 1


async def test_refresh_returning_under_margined_capsule_fails_closed() -> None:
    # A refresh that returns a capsule whose control credentials are too short to
    # cover the launch margin must fail closed (WARMING, capsule dropped), never
    # publish a READY that cannot actually launch a bout in time.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    clock.advance(2_001)

    provider.refresh_preparation_override = lambda slot, previous: _refreshed_preparation(
        clock,
        slot,
        previous,
        control_expires_at=clock.now + timedelta(seconds=10),
        dispatch_expires_at={
            "lakebase": clock.now + timedelta(seconds=10),
            "competitor": clock.now + timedelta(seconds=10),
        },
        expires_at=clock.now + timedelta(seconds=10),
        renew_by=clock.now + timedelta(seconds=5),
    )
    assert await manager.run_one_cycle() == 0.0
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.last_error_code == "credential_margin_insufficient"
    assert manager.capsule is None


async def test_retryable_prepare_backs_off_then_self_heals_to_ready() -> None:
    # A retryable warm failure records a future next_retry_at and does NOT block;
    # a cycle before that instant does zero extra prepare work; once the backoff
    # elapses and the transient clears, the slot reaches READY with no sticky error.
    clock = Clock()
    provider = Provider(clock)
    provider.prepare_error = RetryableWarmError("baseline_probe_throttled")
    manager = coordinator(clock, provider)
    provider.release_prepare.set()

    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.next_retry_at is not None and slot.next_retry_at > clock.now
    assert slot.last_error_code == "baseline_probe_throttled"
    first_attempts = slot.attempt_count
    prepare_after_first = provider.prepare_calls

    # A cycle before next_retry_at must not re-run prepare.
    await manager.run_one_cycle()
    assert provider.prepare_calls == prepare_after_first

    # A second failure escalates the backoff window (attempt_count grows).
    clock.advance(120)
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.attempt_count > first_attempts

    # Clear the transient; the next attempt self-heals to READY.
    clock.advance(120)
    provider.prepare_error = None
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.READY
    assert slot.last_error_code is None


async def test_ready_keep_alive_refreshes_once_per_renew_by_window() -> None:
    # Keep-alive is per-renew_by-window, not per-cycle: crossing renew_by triggers
    # exactly one refresh; a cycle within the same window does none; and across
    # three windows (which also crosses the runner-receipt horizon, since each
    # refresh republishes fresh receipts) there is never a full prepare() rewarm.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    assert provider.prepare_calls == 1 and provider.refresh_calls == 0

    clock.advance(2_001)
    await manager.run_one_cycle()
    assert provider.refresh_calls == 1
    slot = await manager.store.read("install-one")
    assert slot is not None and slot.renew_by is not None and slot.renew_by > clock.now

    # Same window, no clock movement -> no additional refresh.
    await manager.run_one_cycle()
    assert provider.refresh_calls == 1

    clock.advance(2_001)
    await manager.run_one_cycle()
    assert provider.refresh_calls == 2

    clock.advance(2_001)
    await manager.run_one_cycle()
    assert provider.refresh_calls == 3

    assert provider.prepare_calls == 1  # never a full rewarm across the horizon
    slot = await manager.store.read("install-one")
    assert slot is not None and slot.state == Round5WarmState.READY
    assert (await manager.public_status())["round5_ring_ready"] is True


async def test_retryable_validate_ready_probe_stays_ready_in_place() -> None:
    # A throttled/timed-out provenance PROBE (validate_ready RetryableWarmError) is
    # a transient read failure, not a runner identity change: keep the capsule,
    # stay READY, retry in place. It must NOT fold into freshness_lost -> full
    # prepare() (the overnight churn), and it must NOT be swallowed into a fake
    # "still current" success.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)

    provider.validate_error = RetryableWarmError("runner_provenance_probe_retryable")
    for _ in range(MAX_PROVENANCE_PROBE_FAILURES - 1):
        clock.advance(1)
        delay = await manager.run_one_cycle()
        slot = await manager.store.read("install-one")
        assert slot is not None
        assert slot.state == Round5WarmState.READY  # stays READY in place
        assert manager.capsule is not None
        assert provider.prepare_calls == 1  # no full rewarm
        assert delay > 0

    # Once the probe keeps failing past the bounded budget, escalate to a rewarm.
    clock.advance(1)
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.last_error_code == "runner_provenance_probe_retryable"
    assert manager.capsule is None

    # A recovered probe then warms cleanly back to READY (self-heal).
    provider.validate_error = None
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.READY


async def test_self_verifiable_block_retries_but_permanent_block_latches() -> None:
    # A SELF-VERIFIABLE block (insufficient credential margin) must re-attempt on a
    # bounded interval and recover in-process -- never a latch a human must clear --
    # and must not be reported as terminal. A TRUE permanent block (config/identity)
    # latches, is reported terminal, and is never silently re-attempted.
    clock = Clock()
    provider = Provider(clock)
    provider.release_prepare.set()
    provider.prepare_error = BlockedWarmError("credential_margin_insufficient")
    manager = coordinator(clock, provider)

    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None and slot.state == Round5WarmState.BLOCKED
    assert slot.last_error_code == "credential_margin_insufficient"
    assert manager.public_status_cached()["round5_warm_blocked_terminal"] is False

    blocked_prepare_calls = provider.prepare_calls
    await manager.run_one_cycle()  # before the interval -> no re-attempt
    assert provider.prepare_calls == blocked_prepare_calls

    clock.advance(SELF_VERIFIABLE_BLOCK_RETRY_SECONDS + 1)
    provider.prepare_error = None
    await manager.run_one_cycle()  # recheck -> re-warm; may need one more cycle
    slot = await manager.store.read("install-one")
    assert slot is not None
    if slot.state != Round5WarmState.READY:
        await manager.run_one_cycle()
        slot = await manager.store.read("install-one")
    assert slot.state == Round5WarmState.READY

    clock2 = Clock()
    provider2 = Provider(clock2)
    provider2.release_prepare.set()
    provider2.prepare_error = BlockedWarmError("warm_baseline_invalid")
    manager2 = coordinator(clock2, provider2)
    await manager2.run_one_cycle()
    slot2 = await manager2.store.read("install-one")
    assert slot2 is not None and slot2.state == Round5WarmState.BLOCKED
    assert manager2.public_status_cached()["round5_warm_blocked_terminal"] is True

    permanent_calls = provider2.prepare_calls
    clock2.advance(SELF_VERIFIABLE_BLOCK_RETRY_SECONDS + 100)
    await manager2.run_one_cycle()
    assert provider2.prepare_calls == permanent_calls  # permanent block never churns
    slot2 = await manager2.store.read("install-one")
    assert slot2 is not None and slot2.state == Round5WarmState.BLOCKED


async def test_ready_capsule_past_launch_margin_refreshes_instead_of_full_rewarm() -> None:
    # Regression for the overnight rewarm storm. Once a READY capsule reaches its
    # renew_by it no longer meets the launch margin (dispatch/control creds are
    # minted at margin+epsilon), and the keep-alive used to gate on launch margin
    # BEFORE the renew_by refresh -- so it declared freshness_lost and full
    # prepare()-rewarmed every ~3 minutes (151 cycles overnight). The keep-alive
    # now gates on capsule IDENTITY only, so the capsule is refreshed in place and
    # the slot stays READY.
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    assert provider.prepare_calls == 1
    before = manager.capsule
    assert before is not None and before.meets_launch_margin(clock.now)
    # Past renew_by (2000s) AND past the launch margin (control creds now within
    # PROXY_SETUP_DEADLINE+margin = 1860s of the +4000s expiry) but not past any
    # receipt/capsule expiry -- exactly the boundary that triggered the storm.
    clock.advance(2_200)
    assert not before.meets_launch_margin(clock.now)

    delay = await manager.run_one_cycle()

    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.READY  # NOT torn down to WARMING
    assert provider.prepare_calls == 1  # NO full prepare() rewarm storm
    assert provider.refresh_calls == 1  # refreshed in place instead
    assert manager.capsule is not None
    assert manager.capsule.credential_generation == 2
    assert manager.capsule.meets_launch_margin(clock.now)
    refreshed_broker_epoch = manager.capsule.broker_epoch
    assert slot.broker_epoch == refreshed_broker_epoch
    assert delay > 0
    assert (await manager.public_status())["round5_ring_ready"] is True

    await manager.run_one_cycle()
    after_heartbeat = await manager.store.read("install-one")
    assert after_heartbeat is not None
    assert after_heartbeat.broker_epoch == refreshed_broker_epoch
    assert manager.capsule.broker_epoch == refreshed_broker_epoch
    assert manager.ring_ready


async def test_warm_baseline_classification_splits_at_the_raise_site() -> None:
    # The overnight class of failure must be split at the raise site, not lumped
    # under a bare ``except Exception``:
    #   * a typed config/identity/orphan-Proxy/fixture defect
    #     (ConnectionSpikeLiveConfigurationError) is PERMANENT -> BlockedWarmError.
    #   * transient runner availability, throttle, or timeout is RETRYABLE ->
    #     RetryableWarmError, never a terminal block (a BLOCKED slot is never
    #     re-attempted by the same process).
    from server import connection_spike_live as live

    class _Engine:
        def __init__(self, exc: Exception) -> None:
            self._exc = exc

        async def warm(self, generation, warm_attempt_token):
            del generation, warm_attempt_token
            raise self._exc

        async def warm_with_physical_runners_from(self, other, generation):
            del other, generation
            return object()

    prov_config = object.__new__(live.LiveRound5WarmProvider)
    prov_config._cleaned_bout_id = None
    prov_config._engine_factory = lambda competitor_id: _Engine(
        live.ConnectionSpikeLiveConfigurationError(
            "Round 5 warm source or physical runner identity changed"
        )
    )
    with pytest.raises(BlockedWarmError) as blocked:
        await prov_config.prepare(
            generation=1,
            coordinator_fence=1,
            process_epoch="p",
            broker_epoch="b",
            warm_attempt_token="t",
            requires_cleaned_bout=False,
        )
    assert blocked.value.code == "warm_baseline_invalid"

    prov_throttle = object.__new__(live.LiveRound5WarmProvider)
    prov_throttle._cleaned_bout_id = None
    prov_throttle._engine_factory = lambda competitor_id: _Engine(
        TimeoutError("SSM control-plane throttled")
    )
    with pytest.raises(RetryableWarmError) as retry:
        await prov_throttle.prepare(
            generation=1,
            coordinator_fence=1,
            process_epoch="p",
            broker_epoch="b",
            warm_attempt_token="t",
            requires_cleaned_bout=False,
        )
    assert retry.value.code == "warm_provider_retryable"
    assert not isinstance(retry.value, BlockedWarmError)

    prov_ssm_ping = object.__new__(live.LiveRound5WarmProvider)
    prov_ssm_ping._cleaned_bout_id = None
    prov_ssm_ping._engine_factory = lambda competitor_id: _Engine(
        live.ConnectionSpikeLiveTransientError(
            "Round 5 lakebase runner is temporarily unavailable in SSM"
        )
    )
    with pytest.raises(RetryableWarmError) as retry:
        await prov_ssm_ping.prepare(
            generation=1,
            coordinator_fence=1,
            process_epoch="p",
            broker_epoch="b",
            warm_attempt_token="t",
            requires_cleaned_bout=False,
        )
    assert retry.value.code == "warm_provider_retryable"
    assert not isinstance(retry.value, BlockedWarmError)


async def test_duplicate_bell_returns_one_server_context() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.RDS,
        bout_fence=9,
    )
    assert claimed.claim is not None

    first = await manager.accept_bell(claimed.claim.claim_id)
    second = await manager.accept_bell(claimed.claim.claim_id)

    assert second is first
    assert first.t0_monotonic_ns == 1_000
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.RUNNING
    assert slot.bell_id == first.bell_id


async def test_exact_cleanup_atomically_increments_generation_and_rewarms() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    await manager.accept_bell(claimed.claim.claim_id)
    cleaning = await manager.begin_cleanup(claimed.claim.claim_id)
    assert cleaning.state == Round5WarmState.CLEANING
    cleaning_status = await manager.public_status()
    assert cleaning_status["round5_start_stage"] == "cleaning"
    assert cleaning_status["round5_ring_ready"] is False

    next_slot = await manager.finish_cleanup_and_rewarm(claimed.claim.claim_id)

    assert next_slot.generation == 2
    assert next_slot.state == Round5WarmState.WARMING
    assert next_slot.claim is None
    assert next_slot.cleaned_bout_id == claimed.claim.bout_id
    assert next_slot.requires_cleaned_bout is True
    assert next_slot.shared_receipt is None
    assert manager.capsule is None
    rewarming_status = await manager.public_status()
    assert rewarming_status["round5_start_stage"] == "rewarming"
    assert rewarming_status["round5_ring_ready"] is False
    assert [event.event_type for event in await manager.store.events("install-one")][-2:] == [
        "cleanup_started",
        "rewarm_enqueued",
    ]
    # The short TU is truthful and bounded: no fake READY is exposed between
    # CLEANING and generation N+1, and the next successful warm cycle returns
    # the card to READY without oscillating.
    await manager.run_one_cycle()
    ready = await manager.store.read("install-one")
    assert ready is not None
    assert ready.generation == 2
    assert ready.state == Round5WarmState.READY
    ready_status = await manager.public_status()
    assert ready_status["round5_start_stage"] == "ready"
    assert ready_status["round5_ring_ready"] is True
    assert ready.last_error_code is None


async def test_lakebase_slot_json_roundtrip_preserves_cleaned_bout_id() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-json-roundtrip",
        bout_id="bout-json-roundtrip",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    await manager.accept_bell(claimed.claim.claim_id)
    await manager.begin_cleanup(claimed.claim.claim_id)
    warming = await manager.finish_cleanup_and_rewarm(claimed.claim.claim_id)

    payload = _to_json(warming)
    assert isinstance(payload, dict)
    assert payload["cleaned_bout_id"] == claimed.claim.bout_id
    assert payload["requires_cleaned_bout"] is True
    assert _slot_from_json(payload) == warming


async def test_post_bell_towel_rewarm_records_fence_contention_and_reaches_ready() -> None:
    """The live post-bell-towel wedge: cleanup CONVERGES (gen N+1 WARMING) but the
    rewarm keeps LOSING the fence at publish_ready (or the held-lease beat), so READY
    never lands. Before the fix that loss escaped ``run_one_cycle`` into ``run()``'s
    SILENT 1s retry -- ``last_error_code`` stayed ``None``, no log fired, and WARMING
    churned a full paid ``prepare()`` every cycle for 11+ minutes at attempt 20+.

    This drives the REAL coordinator over the REAL transactional store through the
    exact post-bell lifecycle (claim -> bell -> begin_cleanup -> converge -> rewarm),
    injects a TRANSIENT rewarm fence loss, and asserts the rewarm:
      * records a durable, named ``last_error_code`` (``warm_fence_contention``)
        during the contention window -- so the public status shows WHY, and
      * still CONVERGES to READY generation N+1 (not stuck WARMING).

    Mutation: without the WARMING-branch ``WarmFenceLostError`` handler the loss is
    swallowed silently, ``warm_fence_contention`` is NEVER recorded, and this test's
    ``observed_codes`` assertion fails on the live bug path.
    """

    clock = Clock()
    provider = Provider(clock)
    provider.reconcile_result = True
    manager = coordinator(clock, provider)
    store = manager.store
    await warm_ready(manager, provider)
    provider.prepare_started.clear()
    provider.release_prepare.clear()

    claimed, _ = await manager.claim(
        session_id="session-post-bell-towel",
        bout_id="bout-post-bell-towel",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    await manager.accept_bell(claim_id)  # RUNNING (bell present -> post-bell path)
    await manager.begin_cleanup(claim_id)  # towel -> CLEANING
    converged = await manager.converge_cleanup(claim_id)  # cleanup settles -> N+1 WARMING
    assert converged.state == Round5WarmState.WARMING
    assert converged.generation == claimed.generation + 1
    assert converged.requires_cleaned_bout is True
    provider.prepare_started.clear()
    provider.release_prepare.clear()

    # A TRANSIENT rewarm fence loss (the live wedge trigger): publish_ready loses the
    # fence twice, then succeeds. A single-owner process must self-heal, not wedge.
    real_publish = store.publish_ready
    losses = {"n": 0}

    async def flaky_publish(slot, **kwargs):
        if losses["n"] < 2:
            losses["n"] += 1
            raise WarmFenceLostError("simulated post-bell rewarm fence contention")
        return await real_publish(slot, **kwargs)

    store.publish_ready = flaky_publish  # type: ignore[method-assign]

    # Drive run_one_cycle directly (deterministic: no real-time backoff sleeps).
    # Each cycle releases any in-flight prepare, then advances the fake clock past the
    # recorded next_retry_at so the following cycle re-attempts the rewarm.
    observed_codes: set[str] = set()
    result_state = None
    for _ in range(200):
        provider.release_prepare.set()
        delay = await manager.run_one_cycle()
        provider.prepare_started.clear()
        provider.release_prepare.clear()
        slot = await store.read("install-one")
        if slot is not None and slot.last_error_code:
            observed_codes.add(slot.last_error_code)
        if (
            slot is not None
            and slot.state == Round5WarmState.READY
            and slot.generation == claimed.generation + 1
        ):
            result_state = slot
            break
        clock.advance(max(delay, 0.0) + 1.0)

    # The contention was named durably (mutation point: None without the handler).
    assert "warm_fence_contention" in observed_codes
    assert losses["n"] >= 1
    # ...and the same-process rewarm still converged to READY generation N+1.
    assert result_state is not None
    assert result_state.state == Round5WarmState.READY
    assert result_state.generation == claimed.generation + 1
    assert result_state.claim is None
    assert result_state.last_error_code is None
    assert set(result_state.variants) == {Round5Variant.AURORA, Round5Variant.RDS}


async def test_pre_bell_abandon_fences_into_cleaning_retaining_claim() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    status = await manager.public_status()
    assert status["round5_start_stage"] == "claim-drain"

    # The direct return-to-READY fast path was REMOVED (it rested on a forgeable
    # in-process settled signal). abandon_claim now fences into CLEANING with the
    # claim retained; convergence to N+1 READY happens only after exact durable
    # SETTLED + provider absence.
    fenced = await manager.abandon_claim(claimed.claim.claim_id)

    assert fenced.state == Round5WarmState.CLEANING
    assert fenced.claim is not None and fenced.claim.claim_id == claimed.claim.claim_id
    assert manager.ring_ready is False
    assert [event.event_type for event in await manager.store.events("install-one")][-1] == (
        "cleanup_started"
    )


async def cleaning_coordinator(
    clock: Clock,
    store: InMemoryRound5WarmStore,
) -> tuple[Round5WarmCoordinator, str]:
    provider = Provider(clock)
    manager = coordinator(clock, provider, store)
    await warm_ready(manager, provider)
    claimed, _capsule = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    await manager.accept_bell(claim_id)
    await manager.begin_cleanup(claim_id)
    return manager, claim_id


async def test_cleanup_failure_stays_cleaning_with_durable_named_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    manager, claim_id = await cleaning_coordinator(clock, store)

    async def blocked(_claim_id: str):
        raise BlockedWarmError("cleanup_reconcile_blocked")

    monkeypatch.setattr(manager, "converge_cleanup", blocked)
    cleaning = await store.read("install-one")
    assert cleaning is not None

    delay = await manager._drive_cleanup_convergence(cleaning, claim_id)
    persisted = await store.read("install-one")

    assert delay >= 1.0
    assert persisted is not None
    assert persisted.state == Round5WarmState.CLEANING
    assert persisted.claim is not None and persisted.claim.claim_id == claim_id
    assert persisted.last_error_code == "cleanup_reconcile_blocked"
    assert persisted.last_error_at is not None
    assert persisted.next_retry_at is not None
    assert manager.ring_ready is False


async def test_publish_ready_refuses_while_cleanup_claim_is_retained() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    manager, claim_id = await cleaning_coordinator(clock, store)
    cleaning = await store.read("install-one")
    assert cleaning is not None
    assert cleaning.claim is not None

    with pytest.raises(WarmFenceLostError, match="cleanup is owed"):
        await store.publish_ready(
            cleaning,
            preparation=preparation(
                clock,
                generation=cleaning.generation,
                fence=cleaning.coordinator_fence,
                warm_attempt_token=cleaning.warm_attempt_token or "attempt-1-1",
            ),
            now=clock.now,
        )

    persisted = await store.read("install-one")
    assert persisted is not None
    assert persisted.state == Round5WarmState.CLEANING
    assert persisted.claim is not None
    assert persisted.claim.claim_id == claim_id


async def test_restart_does_not_demote_cleaning_claim_to_ready() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    first, claim_id = await cleaning_coordinator(clock, store)
    del first
    clock.advance(91)
    replacement = coordinator(
        clock,
        Provider(clock),
        store,
        process_epoch="process-two",
    )
    await replacement.run_one_cycle()
    persisted = await store.read("install-one")
    assert persisted is not None
    assert persisted.state == Round5WarmState.CLEANING
    assert persisted.claim is not None
    assert persisted.claim.claim_id == claim_id
    assert persisted.generation == 1
    status = await replacement.public_status()
    assert status["round5_cleanup_owed"] is True
    assert status["round5_ring_ready"] is False


async def test_cleanup_fence_loss_stays_cleaning_and_names_takeover(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    manager, claim_id = await cleaning_coordinator(clock, store)

    async def lost(_claim_id: str):
        raise WarmFenceLostError("lease moved")

    monkeypatch.setattr(manager, "converge_cleanup", lost)
    cleaning = await store.read("install-one")
    assert cleaning is not None

    with caplog.at_level("WARNING"):
        delay = await manager._drive_cleanup_convergence(cleaning, claim_id)

    persisted = await store.read("install-one")
    assert delay == 1.0
    assert persisted is not None
    assert persisted.state == Round5WarmState.CLEANING
    assert persisted.claim is not None and persisted.claim.claim_id == claim_id
    assert "round5_cleanup_fence_lost" in caplog.text
    assert "action=await_takeover" in caplog.text


async def test_cleanup_conflict_reaches_next_generation_exactly_once() -> None:
    class ChurningStore(InMemoryRound5WarmStore):
        conflicts = 3

        async def finish_cleanup_and_rewarm(self, slot, **kwargs):
            if self.conflicts:
                self.conflicts -= 1
                await self.heartbeat_coordinator(
                    slot,
                    now=kwargs["now"],
                    ttl=timedelta(seconds=90),
                )
                raise WarmStoreConflictError("forced revision churn")
            return await super().finish_cleanup_and_rewarm(slot, **kwargs)

    clock = Clock()
    store = ChurningStore()
    manager, claim_id = await cleaning_coordinator(clock, store)

    warmed = await manager.finish_cleanup_and_rewarm(claim_id)

    assert warmed.generation == 2
    assert warmed.state == Round5WarmState.WARMING
    events = await store.events("install-one")
    assert sum(event.event_type == "rewarm_enqueued" for event in events) == 1


async def test_cleanup_lost_success_response_is_idempotent() -> None:
    class LostResponseStore(InMemoryRound5WarmStore):
        lose_once = True

        async def finish_cleanup_and_rewarm(self, slot, **kwargs):
            warmed = await super().finish_cleanup_and_rewarm(slot, **kwargs)
            if self.lose_once:
                self.lose_once = False
                raise WarmStoreConflictError("response lost after commit")
            return warmed

    clock = Clock()
    store = LostResponseStore()
    manager, claim_id = await cleaning_coordinator(clock, store)

    first = await manager.finish_cleanup_and_rewarm(claim_id)
    second = await manager.finish_cleanup_and_rewarm(claim_id)

    assert first.generation == second.generation == 2
    assert sum(
        event.event_type == "rewarm_enqueued"
        for event in await store.events("install-one")
    ) == 1


@pytest.mark.parametrize("drift", ["fence", "owner", "claim"])
async def test_cleanup_hostile_identity_drift_fails_closed(drift: str) -> None:
    class HostileStore(InMemoryRound5WarmStore):
        changed = False

        async def finish_cleanup_and_rewarm(self, slot, **kwargs):
            if not self.changed:
                self.changed = True
                changes: dict[str, object] = {}
                if drift == "fence":
                    changes["coordinator_fence"] = slot.coordinator_fence + 1
                elif drift == "owner":
                    changes["coordinator_owner"] = "hostile-process"
                else:
                    assert slot.claim is not None
                    changes["claim"] = replace(
                        slot.claim,
                        claim_id="claim-hostile",
                    )
                await self._replace(
                    slot,
                    "warm_step",
                    now=kwargs["now"],
                    **changes,
                )
                raise WarmStoreConflictError("hostile drift")
            return await super().finish_cleanup_and_rewarm(slot, **kwargs)

    clock = Clock()
    store = HostileStore()
    manager, claim_id = await cleaning_coordinator(clock, store)

    with pytest.raises(WarmFenceLostError):
        await manager.finish_cleanup_and_rewarm(claim_id)

    slot = await store.read("install-one")
    assert slot is not None and slot.generation == 1
    assert slot.state == Round5WarmState.CLEANING


async def test_same_process_verified_clean_slot_recovers_from_cleaning() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    manager, claim_id = await cleaning_coordinator(clock, store)
    manager._verified_clean_claim_ids.add(claim_id)

    await manager.run_one_cycle()

    slot = await store.read("install-one")
    assert slot is not None
    assert slot.generation == 2
    assert slot.state == Round5WarmState.WARMING


async def test_foreign_live_cleaning_owner_is_refused_without_mutation() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    owner, claim_id = await cleaning_coordinator(clock, store)
    foreign = coordinator(
        clock,
        Provider(clock),
        store,
        process_epoch="process-two",
    )
    before = await store.read("install-one")
    events_before = await store.events("install-one")

    with pytest.raises(
        WarmFenceLostError,
        match="ownership is not current",
    ):
        await foreign.finish_cleanup_and_rewarm(claim_id)

    assert await store.read("install-one") == before
    assert await store.events("install-one") == events_before
    assert claim_id not in foreign._verified_clean_claim_ids
    assert owner.process_epoch == "process-one"


async def test_expired_cleaning_owner_is_refused_without_mutation() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    manager, claim_id = await cleaning_coordinator(clock, store)
    before = await store.read("install-one")
    events_before = await store.events("install-one")
    clock.advance(91)

    with pytest.raises(
        WarmFenceLostError,
        match="ownership is not current",
    ):
        await manager.finish_cleanup_and_rewarm(claim_id)

    assert await store.read("install-one") == before
    assert await store.events("install-one") == events_before


@pytest.mark.parametrize(
    ("error", "state", "code", "start_stage"),
    [
        (
            RetryableWarmError("provider_throttled"),
            Round5WarmState.WARMING,
            "provider_throttled",
            "rewarming",
        ),
        (
            BlockedWarmError("runner_capacity"),
            Round5WarmState.BLOCKED,
            "runner_capacity",
            "terminal-blocked",
        ),
    ],
)
async def test_warm_failures_are_classified_without_raw_provider_detail(
    error: Exception,
    state: Round5WarmState,
    code: str,
    start_stage: str,
) -> None:
    clock = Clock()
    provider = Provider(clock)
    provider.prepare_error = error
    manager = coordinator(clock, provider)
    provider.release_prepare.set()

    await manager.run_one_cycle()

    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == state
    assert slot.last_error_code == code
    assert slot.attempt_count == 1
    status = await manager.public_status()
    assert status["round5_start_stage"] == start_stage
    assert status["round5_warm_last_error_code"] == code


async def test_new_process_never_adopts_a_dead_process_capsule() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider = Provider(clock)
    first = coordinator(clock, provider, store, process_epoch="process-one")
    await warm_ready(first, provider)
    assert (await first.public_status())["round5_ring_ready"] is True

    replacement_provider = Provider(clock)
    replacement_provider.release_prepare.set()
    replacement = coordinator(
        clock,
        replacement_provider,
        store,
        process_epoch="process-two",
    )
    clock.advance(91)
    await replacement.run_one_cycle()

    events = await store.events("install-one")
    assert replacement_provider.prepare_calls == 1
    assert replacement.capsule is not first.capsule
    takeover = next(
        event for event in reversed(events) if event.event_type == "warm_started"
    )
    assert takeover.detail["leader_replaced"] is True


async def test_claim_expiry_fences_into_cleaning_with_claim_retained() -> None:
    # An expired CLAIMED generation must fail SAFE into CLEANING with its claim
    # retained -- never back to READY/WARMING -- because a pre-bell claim may have
    # staged ARM residents whose exact job IDs live only in the durable claim, and
    # because dropping the claim here races the manager's begin_cleanup (the live
    # no-bell WarmFenceLostError loop). (Was: returned WARMING and dropped claim.)
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.RDS,
        bout_fence=1,
    )
    manager._capsule = None
    clock.advance(181)

    returned = await manager.store.release_expired_claim(
        claimed,
        now=clock.now,
        still_fresh=False,
    )

    assert returned.state == Round5WarmState.CLEANING
    assert returned.claim is not None
    assert returned.claim.claim_id == claimed.claim.claim_id


def test_two_physical_runners_are_mandatory() -> None:
    clock = Clock()
    valid = preparation(clock, generation=1, fence=1).shared_receipt
    with pytest.raises(ValueError, match="two distinct physical runners"):
        replace(
            valid,
            competitor_runner=replace(
                valid.competitor_runner,
                instance_id=valid.lakebase_runner.instance_id,
            ),
        )


async def test_claim_refuses_when_round_five_is_not_ready() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await manager.store.ensure_warming(
        installation_id="install-one",
        warm_contract_sha256=DIGEST,
        process_epoch="process-one",
        broker_epoch="broker-one",
        now=clock.now,
    )

    with pytest.raises(WarmClaimUnavailableError, match="preparing backstage"):
        await manager.claim(
            session_id="session-one",
            bout_id="bout-one",
            selected_variant=Round5Variant.AURORA,
            bout_fence=1,
        )


@pytest.mark.parametrize(
    ("identity_field", "stale_value"),
    [
        ("broker_epoch", "broker-stale"),
        ("warm_attempt_token", "attempt-stale"),
    ],
)
async def test_stale_capsule_identity_is_not_advertised_or_claimable(
    identity_field: str,
    stale_value: str,
) -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert manager.capsule is not None
    assert manager.ring_ready

    manager._capsule = replace(manager.capsule, **{identity_field: stale_value})

    assert manager.ring_ready is False
    status = await manager.public_status()
    assert status["round5_ring_ready"] is False
    assert status["round5_start_stage"] == "identity-refresh"
    with pytest.raises(WarmClaimUnavailableError, match="preparing backstage"):
        await manager.claim(
            session_id="session-stale",
            bout_id="bout-stale",
            selected_variant=Round5Variant.AURORA,
            bout_fence=1,
        )


async def test_restart_reconciles_running_cleanup_before_rewarming() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider = Provider(clock)
    first = coordinator(clock, provider, store, process_epoch="process-one")
    await warm_ready(first, provider)
    claimed, _ = await first.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    await first.accept_bell(claimed.claim.claim_id)

    clock.advance(91)
    replacement_provider = Provider(clock)
    replacement_provider.reconcile_result = True
    replacement = coordinator(
        clock,
        replacement_provider,
        store,
        process_epoch="process-two",
    )

    await replacement.run_one_cycle()

    slot = await store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.generation == 2
    assert slot.claim is None
    assert slot.coordinator_owner == "process-two"
    assert sum(
        event.event_type == "rewarm_enqueued"
        for event in await store.events("install-one")
    ) == 1


async def test_current_process_running_claim_is_never_reconciled_as_crash_debt() -> None:
    clock = Clock()
    provider = Provider(clock)
    provider.reconcile_result = True
    manager = coordinator(clock, provider, process_epoch="process-one")
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    await manager.accept_bell(claimed.claim.claim_id)
    calls_before = provider.reconcile_calls

    await manager.run_one_cycle()

    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.RUNNING
    assert provider.reconcile_calls == calls_before


async def test_new_coordinator_reconciles_a_previously_blocked_generation() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    failed_provider = Provider(clock)
    failed_provider.prepare_error = BlockedWarmError("runner_capacity")
    failed_provider.release_prepare.set()
    failed = coordinator(
        clock,
        failed_provider,
        store,
        process_epoch="process-one",
    )
    await failed.run_one_cycle()
    blocked = await store.read("install-one")
    assert blocked is not None and blocked.state == Round5WarmState.BLOCKED

    clock.advance(91)
    recovered_provider = Provider(clock)
    recovered_provider.release_prepare.set()
    recovered = coordinator(
        clock,
        recovered_provider,
        store,
        process_epoch="process-two",
    )
    await recovered.run_one_cycle()

    ready = await store.read("install-one")
    assert ready is not None and ready.state == Round5WarmState.READY
    assert ready.last_error_code is None


async def test_contract_change_prepares_never_bouted_generation_two_to_ready() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    first_provider = Provider(clock)
    first = coordinator(
        clock,
        first_provider,
        store,
        process_epoch="process-one",
    )
    await warm_ready(first, first_provider)
    original = await store.read("install-one")
    assert original is not None and original.generation == 1

    clock.advance(91)
    replacement_provider = Provider(clock)
    replacement_provider.release_prepare.set()
    replacement = Round5WarmCoordinator(
        installation_id="install-one",
        warm_contract_sha256="f" * 64,
        store=store,
        provider=replacement_provider,
        process_epoch="process-two",
        broker_epoch="broker-two",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await replacement.run_one_cycle()

    adopted = await store.read("install-one")
    assert adopted is not None
    assert adopted.generation == 2
    assert adopted.state == Round5WarmState.READY
    assert adopted.warm_contract_sha256 == "f" * 64
    assert adopted.cleaned_bout_id is None
    assert adopted.requires_cleaned_bout is False
    assert replacement_provider.requires_cleaned_bout_calls == [False]
