"""Server-seam integration for the attested resident IDENTITY-change re-establishment.

Distinct from the post-bout rewarm storm (``test_round5_rewarm_seam.py``): that
storm was a STALE/ABSENT beat backlog. This is the class observed during a
concurrent round's ``make_schema_change_safely`` DB load -- the shared single-bout
resident kept RESPAWNING (a genuinely NEW process boot id / pid) while Round 5 sat
idle READY, so ``validate_ready`` read an ATTESTED ``IDENTITY_CHANGED`` (not a
transient miss). The old ``freshness_lost``-only path re-warmed over the SAME stale
provider engines and cleared ``last_error`` at the transient ``publish_ready``, so
the coordinator looped identity-refresh <-> rewarming for ~20 minutes with
``err=None`` and an un-claimable ring, recovering only on a full process restart.

These tests drive the REAL ``Round5WarmCoordinator`` over the REAL
``Round5ResidentTransport`` + ``InMemoryRound5ControlStore`` + ``resident_liveness``
classifier, against a runner double that can RESPAWN (change its attested identity)
mid-flight. They assert the fix end to end:

* a one-lane attested identity change forces ONE clean, fenced rewarm to a fresh
  token whose receipts capture the NEW identity, then validates CURRENT and becomes
  claimable again (READY, current-generation semantics, anti-replay);
* the recovery is OBSERVABLE -- a durable named ``last_error`` and an un-claimable
  ring across the whole recovery, INCLUDING the transient READY the rewarm publishes
  (the exact window the old code blanked to ``err=None``);
* the old resident binding/job is RETIRED (a durable CANCEL) before the fresh
  PRELOAD, and the stale provider engines are discarded;
* a resident whose identity NEVER stabilizes does NOT loop forever -- it mints a
  BOUNDED number of distinct tokens and escalates to the named, self-verifiable
  ``runner_identity_unstable`` block; and
* once the identity stabilizes (load subsides), the SAME process self-recovers to a
  claimable READY without a restart, minting distinct tokens each episode.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from server.round5_control import (
    InMemoryRound5ControlStore,
    ResidentLiveness,
    Round5ControlBinding,
    Round5ControlDispatcher,
    Round5ControlKind,
    Round5ResidentTransport,
    Round5RunnerEvent,
    Round5RunnerEventKind,
    canonical_request_sha256,
)
from server.round5_warm import (
    MAX_IDENTITY_REESTABLISH_ATTEMPTS,
    InMemoryRound5WarmStore,
    Round5LaunchCapsule,
    Round5RunnerReceipt,
    Round5SharedReceipt,
    Round5Variant,
    Round5VariantReceipt,
    Round5WarmCoordinator,
    Round5WarmPreparation,
    Round5WarmState,
)

pytestmark = pytest.mark.asyncio

DIGEST = "a" * 64
HARNESS = "b" * 64
INSTALLATION = "install-identity-seam"
LANES = ("lakebase", "competitor")
INSTANCE_ID = {
    "lakebase": "i-0123456789abcdef0",
    "competitor": "i-0fedcba9876543210",
}
# The identity each lane's resident STARTS at; a respawn rotates it (see
# RunnerDouble.change_identity), so the receipts a clean rewarm seals capture
# whatever identity the resident attested at THAT PRELOAD.
BASE_IDENTITY = {
    "lakebase": ("boot-lakebase-0", "process-lakebase-0", 101),
    "competitor": ("boot-competitor-0", "process-competitor-0", 102),
}


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 9, 25, tzinfo=UTC)
        self._mono = 1_000

    def __call__(self) -> datetime:
        return self.now

    def monotonic(self) -> int:
        self._mono += 1
        return self._mono

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)
        self._mono += int(seconds * 1_000_000_000)


def _canonical_request(**values: object) -> dict[str, object]:
    request = {"protocol": "round5-fanin-v2", **values}
    request["prepared_request_digest"] = hashlib.sha256(
        json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return request


def _resident_job_id(generation: int, token: str, lane: str) -> str:
    return hashlib.sha256(
        f"round5-resident\0{generation}\0{token}\0{lane}".encode()
    ).hexdigest()


def _preload_binding(
    generation: int, token: str, lane: str, request: dict[str, object]
) -> Round5ControlBinding:
    boot, _process, _pid = BASE_IDENTITY[lane]
    return Round5ControlBinding(
        installation_id=INSTALLATION,
        lane_id=lane,
        generation=generation,
        warm_attempt_token=token,
        claim_id=None,
        bout_id=None,
        bell_id=None,
        fence=0,
        job_id=_resident_job_id(generation, token, lane),
        # The binding's boot id is the pre-attestation runner boot; the attested
        # process boot id / pid come from the agent_ready the runner emits, which is
        # where a respawn shows up.
        runner_boot_id=boot,
        runner_process_boot_id="unattested",
        runner_harness_sha256=HARNESS,
        request_sha256=canonical_request_sha256(request),
    )


def _capsule(
    clock: Clock, *, generation: int, fence: int, token: str, broker_epoch: str
) -> Round5LaunchCapsule:
    return Round5LaunchCapsule(
        generation=generation,
        coordinator_fence=fence,
        credential_generation=1,
        broker_epoch=broker_epoch,
        runner_contexts={"lakebase": object(), "competitor": object()},
        aws_control_contexts={Round5Variant.AURORA: object(), Round5Variant.RDS: object()},
        lakebase_context=object(),
        variant_contexts={Round5Variant.AURORA: object(), Round5Variant.RDS: object()},
        control_expires_at=clock.now + timedelta(seconds=4_000),
        dispatch_expires_at={
            "lakebase": clock.now + timedelta(seconds=4_000),
            "competitor": clock.now + timedelta(seconds=4_000),
        },
        expires_at=clock.now + timedelta(seconds=3_600),
        renew_by=clock.now + timedelta(seconds=2_000),
        warm_attempt_token=token,
    )


def _preparation(
    clock: Clock,
    *,
    generation: int,
    fence: int,
    token: str,
    broker_epoch: str,
    identity: dict[str, tuple[str, str, int]],
) -> Round5WarmPreparation:
    """Seal receipts that capture the identity attested at THIS PRELOAD.

    ``identity`` is the runner's CURRENT per-lane (boot, process_boot, pid) at the
    moment the fresh PRELOAD's agent_ready landed -- exactly what a real clean rewarm
    would seal, so a subsequent CURRENT probe matches when the resident is stable and
    an IDENTITY_CHANGED when it respawned afterward.
    """

    expires_at = clock.now + timedelta(seconds=4_500)
    runners = {}
    for lane in LANES:
        boot, process, pid = identity[lane]
        runners[lane] = Round5RunnerReceipt(
            lane_id=lane,
            instance_id=INSTANCE_ID[lane],
            boot_id=boot,
            process_boot_id=process,
            process_pid=pid,
            instance_type="c7i.2xlarge",
            image_sha256=DIGEST,
            loaded_harness_sha256=HARNESS,
            capacity_model_sha256=DIGEST,
            expires_at=expires_at,
        )
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
        capsule=_capsule(
            clock, generation=generation, fence=fence, token=token, broker_epoch=broker_epoch
        ),
    )


class RunnerDouble:
    """A single-bout resident that can RESPAWN (change its attested identity).

    It beats exactly one token per lane (a newer PRELOAD supersedes an older one),
    and emits AGENT_READY/HEARTBEAT events under its CURRENT identity. A respawn
    (``change_identity`` / ``churn``) rotates the boot id + pid so a subsequent beat
    under the same token attests a NEW identity -- the exact IDENTITY_CHANGED the
    real ``resident_liveness`` demotes on.
    """

    def __init__(self, store: InMemoryRound5ControlStore, clock: Clock) -> None:
        self.store = store
        self.clock = clock
        self.up = True
        # When True, every beat is preceded by a respawn (identity rotation): the
        # resident never stabilizes (a sustained respawn storm under load).
        self.churn = False
        self._epoch = 0
        self.identity = dict(BASE_IDENTITY)
        self._beating: dict[str, dict] = {}
        self._ready_tokens: set[tuple[str, str]] = set()
        self.preloads: dict[str, list[str]] = {lane: [] for lane in LANES}
        # A strictly-increasing per-event nudge so every emitted attestation has a
        # unique, later occurred_at than the last -- otherwise ``latest_resident_
        # attestation`` (max by occurred_at) could return a stale agent_ready that
        # merely tied a fresh respawn heartbeat on the same wall-clock tick.
        self._event_seq = 0

    def change_identity(self, lanes: tuple[str, ...] = LANES) -> None:
        """Model a systemd respawn: a NEW process boot id / pid for the lanes."""
        self._epoch += 1
        for lane in lanes:
            boot0, _process0, pid0 = BASE_IDENTITY[lane]
            self.identity[lane] = (
                boot0,
                f"process-{lane}-{self._epoch}",
                pid0 + 1_000 * self._epoch,
            )

    def current_identity(self) -> dict[str, tuple[str, str, int]]:
        return dict(self.identity)

    def agent_ready_for(self, lane: str, token: str) -> bool:
        return (lane, token) in self._ready_tokens

    async def _emit(
        self,
        binding: Round5ControlBinding,
        kind: Round5RunnerEventKind,
        sequence: int,
    ) -> None:
        boot, process, pid = self.identity[binding.lane_id]
        attested = Round5ControlBinding(
            **{**binding.wire_value(), "runner_process_boot_id": process}
        )
        payload = {
            "worker_ready_indexes": [0, 1, 2, 3],
            "runner_boot_id": boot,
            "runner_process_boot_id": process,
            "process_pid": pid,
            "runner_harness_sha256": HARNESS,
        }
        if kind is Round5RunnerEventKind.AGENT_READY:
            payload["worker_count"] = 4
            payload["warm_attempt_token"] = binding.warm_attempt_token
        self._event_seq += 1
        await self.store.append_runner_event(
            Round5RunnerEvent(
                event_id=hashlib.sha256(
                    f"{binding.job_id}\0{kind.value}\0{sequence}\0{process}".encode()
                ).hexdigest(),
                binding=attested,
                sequence=sequence,
                kind=kind,
                occurred_at=self.clock.now + timedelta(microseconds=self._event_seq),
                payload=payload,
            )
        )

    async def consume(self) -> None:
        """Consume every PRELOAD in the outbox; a newer one supersedes the older."""
        newest: dict[str, Round5ControlBinding] = {}
        for event, _published in list(self.store.outbox.values()):
            if event.kind is not Round5ControlKind.PRELOAD:
                continue
            lane = event.lane_id
            token = event.binding.warm_attempt_token
            if token not in self.preloads[lane]:
                self.preloads[lane].append(token)
            prior = newest.get(lane)
            if prior is None or self.preloads[lane].index(token) >= self.preloads[
                lane
            ].index(prior.warm_attempt_token):
                newest[lane] = event.binding
        if not self.up:
            return
        for lane, binding in newest.items():
            beating = self._beating.get(lane)
            if beating is not None and beating["token"] == binding.warm_attempt_token:
                continue
            self._beating[lane] = {
                "token": binding.warm_attempt_token,
                "binding": binding,
                "seq": 1,
            }
            if (lane, binding.warm_attempt_token) not in self._ready_tokens:
                await self._emit(binding, Round5RunnerEventKind.AGENT_READY, 1)
                self._ready_tokens.add((lane, binding.warm_attempt_token))

    async def beat(self) -> None:
        if not self.up:
            return
        if self.churn:
            self.change_identity()
        for _lane, state in self._beating.items():
            state["seq"] += 1
            await self._emit(state["binding"], Round5RunnerEventKind.HEARTBEAT, state["seq"])


class SeamProvider:
    """A Round5WarmProvider over the REAL transport + an identity-mutable runner."""

    def __init__(self, clock: Clock, store: InMemoryRound5ControlStore, runner: RunnerDouble):
        self.clock = clock
        self.store = store
        self.runner = runner
        self.authority_guard = None
        dispatcher = Round5ControlDispatcher(store, self._noop_send)
        self.transport = Round5ResidentTransport(store, dispatcher, sleep=self._sleep)
        self.attempt_tokens: list[str] = []
        self.reconcile_calls = 0
        self.reestablish_calls = 0
        self.engines_present = False

    @staticmethod
    async def _noop_send(event) -> None:
        return

    @staticmethod
    async def _sleep(_delay: float) -> None:
        return

    async def reconcile(self, slot) -> bool:
        self.reconcile_calls += 1
        return slot.state in {Round5WarmState.RUNNING, Round5WarmState.CLEANING}

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
        del process_epoch, requires_cleaned_bout
        self.attempt_tokens.append(warm_attempt_token)
        for lane in LANES:
            request = _canonical_request(lane=lane, generation=generation, token=warm_attempt_token)
            binding = _preload_binding(generation, warm_attempt_token, lane, request)
            await self.transport.preload(binding=binding, request=request)
        await self.runner.consume()
        from server.round5_warm import RetryableWarmError

        for lane in LANES:
            if not self.runner.agent_ready_for(lane, warm_attempt_token):
                raise RetryableWarmError("resident_restarting")
            request = _canonical_request(lane=lane, generation=generation, token=warm_attempt_token)
            binding = _preload_binding(generation, warm_attempt_token, lane, request)
            await self.transport.wait_agent_ready(binding, not_before=None)
        # Fresh engines are now installed for this attempt.
        self.engines_present = True
        # The receipts capture the identity the resident attested THIS PRELOAD.
        return _preparation(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            token=warm_attempt_token,
            broker_epoch=broker_epoch,
            identity=self.runner.current_identity(),
        )

    async def validate_ready(self, slot, capsule) -> bool:
        del capsule
        token = slot.warm_attempt_token or ""
        if not self.engines_present or slot.shared_receipt is None:
            return False
        expected = {
            "lakebase": slot.shared_receipt.lakebase_runner,
            "competitor": slot.shared_receipt.competitor_runner,
        }
        states = []
        for lane, receipt in expected.items():
            states.append(
                await self.transport.resident_liveness(
                    installation_id=INSTALLATION,
                    lane_id=lane,
                    warm_attempt_token=token,
                    runner_boot_id=receipt.boot_id,
                    process_boot_id=receipt.process_boot_id,
                    process_pid=receipt.process_pid,
                    harness_sha256=receipt.loaded_harness_sha256,
                    now=self.clock.now,
                )
            )
        if any(state is ResidentLiveness.IDENTITY_CHANGED for state in states):
            return False
        from server.round5_warm import RetryableWarmError

        if any(state in (ResidentLiveness.STALE, ResidentLiveness.ABSENT) for state in states):
            raise RetryableWarmError("runner_attestation_stale")
        return True

    async def reestablish(self, slot) -> None:
        """Discard the stale engines so the next prepare is a genuinely fresh PRELOAD.

        Mirrors ``LiveRound5WarmProvider.reestablish``: the resident this generation
        attested is gone, so drop the installed engines/receipts. The old claim-less
        resident-generation PRELOAD is retired by the SUPERSEDING fresh PRELOAD the
        clean rewarm issues (not an explicit CANCEL), which the runner double models
        via "a newer PRELOAD supersedes the older" -- asserted by the callers.
        """
        del slot
        self.reestablish_calls += 1
        self.engines_present = False

    async def refresh_preparation(self, slot, capsule) -> Round5WarmPreparation:
        del capsule
        return _preparation(
            self.clock,
            generation=slot.generation,
            fence=slot.coordinator_fence,
            token=slot.warm_attempt_token,
            broker_epoch=slot.broker_epoch,
            identity=self.runner.current_identity(),
        )

    async def refresh_capsule(self, slot, previous) -> Round5LaunchCapsule:
        del previous
        return _capsule(
            self.clock,
            generation=slot.generation,
            fence=slot.coordinator_fence,
            token=slot.warm_attempt_token,
            broker_epoch=slot.broker_epoch,
        )


def _coordinator(clock: Clock, provider: SeamProvider) -> Round5WarmCoordinator:
    return Round5WarmCoordinator(
        installation_id=INSTALLATION,
        warm_contract_sha256=DIGEST,
        store=InMemoryRound5WarmStore(),
        provider=provider,
        process_epoch="process-identity-seam",
        broker_epoch="broker-identity-seam",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )


async def _drive_to_claimable(
    manager: Round5WarmCoordinator, provider: SeamProvider, limit: int = 40
):
    for _ in range(limit):
        await provider.runner.beat()
        await manager.run_one_cycle()
        if manager.ring_ready:
            return await manager.store.read(INSTALLATION)
        provider.clock.advance(1)
    return await manager.store.read(INSTALLATION)


async def _new_seam():
    clock = Clock()
    store = InMemoryRound5ControlStore()
    runner = RunnerDouble(store, clock)
    provider = SeamProvider(clock, store, runner)
    manager = _coordinator(clock, provider)
    await manager.store.initialize()
    return clock, store, runner, provider, manager


async def test_one_lane_identity_change_forces_clean_rewarm_and_converges() -> None:
    clock, _store, runner, provider, manager = await _new_seam()

    ready_a = await _drive_to_claimable(manager, provider)
    assert ready_a is not None and ready_a.state == Round5WarmState.READY
    assert manager.ring_ready
    token_a = ready_a.warm_attempt_token
    assert token_a is not None

    # A single lane's resident respawns (attested IDENTITY_CHANGED) while idle READY.
    runner.change_identity(("lakebase",))
    await runner.beat()
    delay = await manager.run_one_cycle()
    # Fail-closed AT ONCE: WARMING, un-claimable, and the clean re-establishment ran.
    assert delay == 0.0
    demoted = await manager.store.read(INSTALLATION)
    assert demoted is not None and demoted.state == Round5WarmState.WARMING
    assert not manager.ring_ready
    # The clean re-establishment ran: stale engines discarded (forcing a fresh
    # PRELOAD) before the rewarm.
    assert provider.reestablish_calls == 1

    # The clean rewarm converges to a fresh token whose receipts capture the NEW
    # identity, then validates CURRENT and becomes claimable again.
    ready_b = await _drive_to_claimable(manager, provider)
    assert ready_b is not None and ready_b.state == Round5WarmState.READY
    assert manager.ring_ready
    token_b = ready_b.warm_attempt_token
    assert token_b is not None and token_b != token_a  # anti-replay: fresh token
    # The old resident generation was RETIRED by the superseding fresh PRELOAD:
    # exactly one PRELOAD per lane for token_b, and the runner now beats only B.
    for lane in LANES:
        assert runner.preloads[lane].count(token_b) == 1
        assert runner.preloads[lane] == [token_a, token_b]
    # Current-generation semantics: the new READY serves the fresh identity.
    assert ready_b.shared_receipt is not None
    assert (
        ready_b.shared_receipt.lakebase_runner.process_boot_id
        == runner.identity["lakebase"][1]
    )
    assert await provider.validate_ready(ready_b, None) is True

    # READY holds across further idle probes (no flicker back into recovery).
    for _ in range(6):
        await runner.beat()
        await manager.run_one_cycle()
        clock.advance(3)
    steady = await manager.store.read(INSTALLATION)
    assert steady is not None and steady.state == Round5WarmState.READY
    assert manager.ring_ready
    assert steady.warm_attempt_token == token_b


async def test_identity_change_is_observable_across_the_transient_ready() -> None:
    # The exact regression: the old code cleared last_error at the rewarm's transient
    # publish_ready, so an idle sampler saw err=None while the ring churned. The fix
    # keeps a durable NAMED error and an un-claimable ring across the WHOLE recovery,
    # including the transient READY, until a CURRENT probe proves a stable identity.
    clock, _store, runner, provider, manager = await _new_seam()
    ready_a = await _drive_to_claimable(manager, provider)
    assert manager.ring_ready

    # Respawn, then respawn AGAIN right after the rewarm publishes READY so the ring
    # never confirms CURRENT: sample the public status through the churn.
    runner.change_identity()
    await runner.beat()
    await manager.run_one_cycle()  # READY -> reestablish -> WARMING
    await runner.beat()
    await manager.run_one_cycle()  # WARMING -> prepare -> transient READY(B)
    transient = await manager.store.read(INSTALLATION)
    assert transient is not None and transient.state == Round5WarmState.READY
    status = manager.public_status_cached()
    # OBSERVABLE at the transient READY: a named error, an un-claimable ring, and the
    # start stage the operator sees is the recovery, never a blank "ready".
    assert status["round5_ring_ready"] is False
    assert status["round5_warm_last_error_code"] == "runner_identity_reestablishing"
    assert status["round5_start_stage"] == "identity-refresh"
    assert not manager.ring_ready
    assert ready_a is not None


async def test_persistent_identity_churn_is_bounded_and_named_block() -> None:
    # A resident whose identity NEVER stabilizes (a sustained respawn storm under a
    # concurrent round's DB load) must NOT loop forever. It mints a BOUNDED number of
    # distinct tokens and escalates to the named, SELF-VERIFIABLE block
    # runner_identity_unstable -- observable, not an invisible err=None churn.
    clock, _store, runner, provider, manager = await _new_seam()
    await _drive_to_claimable(manager, provider)
    assert manager.ring_ready

    runner.churn = True  # every beat is a respawn: identity never stabilizes
    blocked = None
    for _ in range(80):
        slot = await manager.store.read(INSTALLATION)
        if slot is not None and slot.state == Round5WarmState.BLOCKED:
            blocked = slot
            break
        await runner.beat()
        await manager.run_one_cycle()
        clock.advance(1)

    assert blocked is not None
    assert blocked.last_error_code == "runner_identity_unstable"
    # Bounded, observable, and NOT terminal-latched: it self-verifies (re-checks on a
    # bounded interval) so it recovers once the resident settles, while never being
    # claimable during the churn.
    from server.round5_warm import _blocked_is_terminal

    assert _blocked_is_terminal(blocked) is False
    assert not manager.ring_ready
    status = manager.public_status_cached()
    assert status["round5_warm_last_error_code"] == "runner_identity_unstable"
    # Bounded token mint: on the order of the re-establishment cap, never one-per-beat.
    distinct = set(provider.attempt_tokens)
    assert len(distinct) <= MAX_IDENTITY_REESTABLISH_ATTEMPTS + 1
    # Each distinct token was preloaded at most once per lane (no superseded flood).
    for lane in LANES:
        assert len(runner.preloads[lane]) == len(set(runner.preloads[lane]))


async def test_identity_churn_then_stabilizes_self_recovers_without_restart() -> None:
    # "Restart clears" was a symptom, not a requirement: once the identity stabilizes
    # (the concurrent load subsides), the SAME process must self-recover to a
    # claimable READY -- no restart. Distinct tokens each episode (anti-replay).
    clock, _store, runner, provider, manager = await _new_seam()
    ready_a = await _drive_to_claimable(manager, provider)
    token_a = ready_a.warm_attempt_token

    runner.churn = True
    # Churn for a few beats (below the block cap), then the load subsides.
    for _ in range(5):
        await runner.beat()
        await manager.run_one_cycle()
        clock.advance(1)
        slot = await manager.store.read(INSTALLATION)
        # never claimable, and never a silent err=None, while churning
        assert not manager.ring_ready
        if slot is not None and slot.state == Round5WarmState.BLOCKED:
            break
    runner.churn = False  # identity settles on one stable value

    recovered = await _drive_to_claimable(manager, provider, limit=80)
    assert recovered is not None and recovered.state == Round5WarmState.READY
    assert manager.ring_ready
    assert recovered.warm_attempt_token != token_a  # anti-replay across episodes
    # The recovered receipts pin the settled identity, and it validates CURRENT.
    assert await provider.validate_ready(recovered, None) is True
    # The recovery budget was reset by the CURRENT probe.
    assert manager._identity_reestablish_failures == 0


async def test_repeated_distinct_identity_changes_mint_distinct_tokens() -> None:
    # Three successive, DISTINCT single-episode identity changes each converge to a
    # fresh claimable READY with a distinct token (anti-replay + single owner).
    clock, _store, runner, provider, manager = await _new_seam()
    ready = await _drive_to_claimable(manager, provider)
    seen = [ready.warm_attempt_token]

    for _ in range(3):
        runner.change_identity()
        await runner.beat()
        await manager.run_one_cycle()  # demote
        ready = await _drive_to_claimable(manager, provider)
        assert ready is not None and ready.state == Round5WarmState.READY
        assert manager.ring_ready
        assert await provider.validate_ready(ready, None) is True
        seen.append(ready.warm_attempt_token)

    assert len(set(seen)) == len(seen)  # every episode minted a distinct token
    # Single owner throughout: exactly one durable warm slot, one coordinator.
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.warm_attempt_token == seen[-1]


class _RetireRecordingEngine:
    """Minimal engine double: records retire calls and cleanup-lineage plumbing."""

    def __init__(self) -> None:
        self.retired = 0
        self.authority_guard = None
        self._round5_cleanup_janitor_owned = False

    def require_cleaned_bout(self) -> None:  # pragma: no cover - not exercised here
        pass

    def retain_cleaned_bout(self, _bout_id: str) -> None:  # pragma: no cover
        pass

    async def retire_resident_generation(self) -> None:
        self.retired += 1


async def test_live_provider_reestablish_discards_engines_and_retires() -> None:
    # LiveRound5WarmProvider.reestablish must RETIRE every installed engine and then
    # DISCARD the engines/receipts, so the next prepare() builds fresh engines and
    # issues a genuinely fresh PRELOAD rather than re-warming the changed identity.
    from server.connection_spike_live import LiveRound5WarmProvider

    engines = {
        "aurora_serverless_v2": _RetireRecordingEngine(),
        "rds_postgres": _RetireRecordingEngine(),
    }
    provider = LiveRound5WarmProvider(object(), lambda _competitor: _RetireRecordingEngine())
    provider._engines = dict(engines)
    provider._receipts = {"aurora_serverless_v2": object(), "rds_postgres": object()}

    await provider.reestablish(object())

    for engine in engines.values():
        assert engine.retired == 1
    assert provider._engines == {}
    assert provider._receipts == {}
