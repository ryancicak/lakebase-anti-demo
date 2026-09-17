from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server.round5_warm import (
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
) -> Round5LaunchCapsule:
    return Round5LaunchCapsule(
        generation=generation,
        coordinator_fence=fence,
        credential_generation=credential_generation,
        broker_epoch=f"broker-{credential_generation}",
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
        self.attempt_tokens: list[str] = []

    async def reconcile(self, slot) -> bool:
        del slot
        self.reconcile_calls += 1
        return self.reconcile_result

    async def validate_ready(self, slot, launch_capsule) -> bool:
        del slot, launch_capsule
        self.validate_calls += 1
        return self.provenance_current

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
    ) -> Round5WarmPreparation:
        del process_epoch, broker_epoch
        self.attempt_tokens.append(warm_attempt_token)
        self.prepare_calls += 1
        self.prepare_started.set()
        await self.release_prepare.wait()
        if self.prepare_error is not None:
            raise self.prepare_error
        return preparation(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            warm_attempt_token=warm_attempt_token,
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


async def test_atomic_claim_uses_one_post_lock_database_timestamp() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    ready = await manager.store.read("install-one")
    assert ready is not None
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
                self.rows = [(json.dumps(_to_json(ready)),)]
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
    claimed, main, cleanup = await store.claim_ready_with_leases(
        ready,
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        capsule_generation=ready.generation,
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
    assert claimed.claim is not None
    assert claimed.claim.claimed_at == locked_now
    assert claimed.claim.claim_expires_at == locked_now + timedelta(seconds=60)
    assert main.started_at == cleanup.started_at == locked_now
    lock_indexes = [
        index for index, value in enumerate(statements) if "FOR UPDATE" in value
    ]
    clock_index = next(
        index
        for index, value in enumerate(statements)
        if "SELECT clock_timestamp()" in value
    )
    assert lock_indexes and clock_index > max(lock_indexes)
    assert claimed.claim is not None
    assert claimed.claim.lakebase_job_id != claimed.claim.competitor_job_id


async def test_runner_reboot_invalidates_ready_before_claim_or_bell() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    assert manager.ring_ready

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


async def test_refresh_failure_removes_readiness_before_margin_is_lost() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    provider.refresh_error = RetryableWarmError("credential_refresh_timeout")
    clock.advance(2_001)

    await manager.run_one_cycle()

    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert manager.capsule is None
    assert (await manager.public_status())["round5_ring_ready"] is False


async def test_persistent_resident_delivery_failure_durably_removes_readiness() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    assert manager.ring_ready

    await manager.invalidate_readiness("resident_control_delivery_failed")

    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.last_error_code == "resident_control_delivery_failed"
    assert manager.capsule is None
    status = manager.public_status_cached()
    assert status["round5_ring_ready"] is False
    assert (
        status["round5_warm_last_error_code"]
        == "resident_control_delivery_failed"
    )


async def test_each_warm_retry_uses_a_new_durable_attempt_token() -> None:
    clock = Clock()
    provider = Provider(clock)
    provider.prepare_error = RetryableWarmError("first_attempt_failed")
    manager = coordinator(clock, provider)
    provider.release_prepare.set()
    await manager.run_one_cycle()
    first = (await manager.store.read("install-one")).warm_attempt_token
    provider.prepare_error = None
    clock.advance(1)
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert first is not None
    assert slot.warm_attempt_token is not None
    assert slot.warm_attempt_token != first
    assert provider.attempt_tokens == [first, slot.warm_attempt_token]


async def test_expired_renew_by_refresh_fails_closed_without_refresh_loop() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    clock.advance(2_001)

    async def expired(slot, previous):
        provider.refresh_calls += 1
        return replace(
            capsule(
                clock,
                generation=slot.generation,
                fence=slot.coordinator_fence,
                credential_generation=2,
                warm_attempt_token=previous.warm_attempt_token,
            ),
            renew_by=clock.now - timedelta(seconds=1),
        )

    provider.refresh_capsule = expired
    assert await manager.run_one_cycle() == 0.0
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.last_error_code == "credential_refresh_expired"
    assert provider.refresh_calls == 1


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

    next_slot = await manager.finish_cleanup_and_rewarm(claimed.claim.claim_id)

    assert next_slot.generation == 2
    assert next_slot.state == Round5WarmState.WARMING
    assert next_slot.claim is None
    assert next_slot.shared_receipt is None
    assert manager.capsule is None
    assert [event.event_type for event in await manager.store.events("install-one")][-2:] == [
        "cleanup_started",
        "rewarm_enqueued",
    ]


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
    ("error", "state", "code"),
    [
        (RetryableWarmError("provider_throttled"), Round5WarmState.WARMING, "provider_throttled"),
        (BlockedWarmError("runner_capacity"), Round5WarmState.BLOCKED, "runner_capacity"),
    ],
)
async def test_warm_failures_are_classified_without_raw_provider_detail(
    error: Exception,
    state: Round5WarmState,
    code: str,
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


async def test_claim_expiry_returns_to_warming_when_capsule_is_not_fresh() -> None:
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

    assert returned.state == Round5WarmState.WARMING
    assert returned.claim is None


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


async def test_contract_change_enqueues_a_new_warm_generation() -> None:
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
