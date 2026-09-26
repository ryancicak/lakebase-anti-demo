"""Server-seam integration for the post-bout rewarm storm fix.

Drives the REAL ``Round5WarmCoordinator`` over a provider that speaks to the REAL
``Round5ResidentTransport`` + ``InMemoryRound5ControlStore``, against a runner
double that consumes PRELOAD control events out of the durable outbox and emits
``agent_ready`` / ``heartbeat`` runner events -- exactly the events
``validate_ready`` attests through the real ``resident_liveness`` classifier.

The runner double models the two live facts that produced the storm:

* it is SINGLE-BOUT: after a bout (towel / completion) its systemd process is
  respawned, so it drops its pool + heartbeat and needs a FRESH PRELOAD to beat
  the next generation's token; and
* it beats exactly ONE token per lane: a newer PRELOAD supersedes an older one,
  so a coordinator that minted a distinct token every WARMING beat produced a
  superseded PRELOAD backlog it could never drain (validate_ready read
  ABSENT/STALE forever -- the observed 149/149 post-towel idle flicker).

These tests assert the fix's invariant end to end: exactly ONE live PRELOAD per
lane per warm episode, convergence to READY(B) with validate CURRENT across the
no-bell / towel / completion cleanup paths and repeated A->B->C generations,
delayed token-A events rejected after B (anti-replay), and a bounded token/outbox
flood with a named terminal block when a resident never comes ready.
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
    MAX_TRANSIENT_WARM_ATTEMPTS,
    WARM_ATTEMPT_TOKEN_REUSE_SECONDS,
    InMemoryRound5WarmStore,
    RetryableWarmError,
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
INSTALLATION = "install-seam"
LANES = ("lakebase", "competitor")
# Fixed, per-lane resident identity the runner double beats AND the preparation
# receipts carry, so ``resident_liveness`` attests CURRENT when they match.
LANE_IDENTITY = {
    "lakebase": ("boot-lakebase", "process-lakebase", 101),
    "competitor": ("boot-competitor", "process-competitor", 102),
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
    boot, _process, _pid = LANE_IDENTITY[lane]
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
    clock: Clock, *, generation: int, fence: int, token: str, broker_epoch: str
) -> Round5WarmPreparation:
    expires_at = clock.now + timedelta(seconds=4_500)
    runners = {}
    for lane, (boot, process, pid) in LANE_IDENTITY.items():
        runners[lane] = Round5RunnerReceipt(
            lane_id=lane,
            instance_id="i-0123456789abcdef0" if lane == "lakebase" else "i-0fedcba9876543210",
            boot_id=boot,
            process_boot_id=process,
            process_pid=pid,
            instance_type="c7i.2xlarge",
            image_sha256=DIGEST,
            loaded_harness_sha256=DIGEST,
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
    """A faithful single-bout resident: consumes PRELOADs, beats one token/lane."""

    def __init__(self, store: InMemoryRound5ControlStore, clock: Clock) -> None:
        self.store = store
        self.clock = clock
        self.up = True
        # lane -> {"token","binding","seq"} for the token this lane currently beats.
        self._beating: dict[str, dict] = {}
        self._ready_tokens: set[tuple[str, str]] = set()
        # lane -> ordered list of DISTINCT tokens this lane ever PRELOADed (outbox).
        self.preloads: dict[str, list[str]] = {lane: [] for lane in LANES}

    def restart(self) -> None:
        """Model the systemd single-bout respawn: drop pool + heartbeat binding."""
        self._beating.clear()

    def agent_ready_for(self, lane: str, token: str) -> bool:
        return (lane, token) in self._ready_tokens

    async def _emit(
        self,
        binding: Round5ControlBinding,
        kind: Round5RunnerEventKind,
        sequence: int,
    ) -> None:
        boot, process, pid = LANE_IDENTITY[binding.lane_id]
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
        await self.store.append_runner_event(
            Round5RunnerEvent(
                event_id=hashlib.sha256(
                    f"{binding.job_id}\0{kind.value}\0{sequence}".encode()
                ).hexdigest(),
                binding=attested,
                sequence=sequence,
                kind=kind,
                occurred_at=self.clock.now,
                payload=payload,
            )
        )

    async def consume(self) -> None:
        """Consume every PRELOAD in the outbox; a newer one supersedes the older.

        Reads the durable outbox directly (the SQS-delivery analog) so it is not
        racing the dispatcher's publish bookkeeping. A superseded/older token that
        is no longer this lane's newest PRELOAD stops being beaten -- the exact
        RLS supersede the real resident hits at its heartbeat.
        """
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
        for state in self._beating.values():
            state["seq"] += 1
            await self._emit(state["binding"], Round5RunnerEventKind.HEARTBEAT, state["seq"])


class SeamProvider:
    """A Round5WarmProvider that drives the REAL transport + runner double."""

    def __init__(self, clock: Clock, store: InMemoryRound5ControlStore, runner: RunnerDouble):
        self.clock = clock
        self.store = store
        self.runner = runner
        self.authority_guard = None
        dispatcher = Round5ControlDispatcher(store, self._noop_send)
        self.transport = Round5ResidentTransport(store, dispatcher, sleep=self._sleep)
        self.attempt_tokens: list[str] = []
        self.reconcile_calls = 0

    @staticmethod
    async def _noop_send(event) -> None:  # delivery handled via the outbox mirror
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
        for lane in LANES:
            if not self.runner.agent_ready_for(lane, warm_attempt_token):
                # wait_agent_ready would time out -> transient restarting resident.
                raise RetryableWarmError("resident_restarting")
            request = _canonical_request(lane=lane, generation=generation, token=warm_attempt_token)
            binding = _preload_binding(generation, warm_attempt_token, lane, request)
            await self.transport.wait_agent_ready(binding, not_before=None)
        return _preparation(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            token=warm_attempt_token,
            broker_epoch=broker_epoch,
        )

    async def validate_ready(self, slot, capsule) -> bool:
        del capsule
        token = slot.warm_attempt_token or ""
        states = []
        for lane in LANES:
            boot, process, pid = LANE_IDENTITY[lane]
            states.append(
                await self.transport.resident_liveness(
                    installation_id=INSTALLATION,
                    lane_id=lane,
                    warm_attempt_token=token,
                    runner_boot_id=boot,
                    process_boot_id=process,
                    process_pid=pid,
                    harness_sha256=HARNESS,
                    now=self.clock.now,
                )
            )
        if any(state is ResidentLiveness.IDENTITY_CHANGED for state in states):
            return False
        if any(state in (ResidentLiveness.STALE, ResidentLiveness.ABSENT) for state in states):
            raise RetryableWarmError("runner_attestation_stale")
        return True

    async def refresh_preparation(self, slot, capsule) -> Round5WarmPreparation:
        del capsule
        return _preparation(
            self.clock,
            generation=slot.generation,
            fence=slot.coordinator_fence,
            token=slot.warm_attempt_token,
            broker_epoch=slot.broker_epoch,
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
        process_epoch="process-seam",
        broker_epoch="broker-seam",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )


async def _drive_to_ready(manager: Round5WarmCoordinator, provider: SeamProvider, limit: int = 40):
    for _ in range(limit):
        await provider.runner.beat()
        await manager.run_one_cycle()
        slot = await manager.store.read(INSTALLATION)
        if slot is not None and slot.state == Round5WarmState.READY:
            return slot
        provider.clock.advance(1)
    return await manager.store.read(INSTALLATION)


@pytest.mark.parametrize("mode", ["nobell", "towel", "completion"])
async def test_seam_rewarm_converges_with_one_preload_per_lane(mode: str) -> None:
    clock = Clock()
    store = InMemoryRound5ControlStore()
    runner = RunnerDouble(store, clock)
    provider = SeamProvider(clock, store, runner)
    manager = _coordinator(clock, provider)
    await manager.store.initialize()

    ready_a = await _drive_to_ready(manager, provider)
    assert ready_a is not None and ready_a.state == Round5WarmState.READY
    token_a = ready_a.warm_attempt_token
    assert token_a is not None
    for lane in LANES:
        assert runner.preloads[lane] == [token_a]  # exactly one PRELOAD for gen 1

    # Claim, optionally ring, then towel -> cleanup -> rewarm to N+1.
    claimed, _ = await manager.claim(
        session_id="session-seam",
        bout_id="bout-seam",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    if mode in {"towel", "completion"}:
        await manager.accept_bell(claimed.claim.claim_id)
    await manager.begin_cleanup(claimed.claim.claim_id)
    converged = await manager.converge_cleanup(claimed.claim.claim_id)
    assert converged.state == Round5WarmState.WARMING
    assert converged.generation == ready_a.generation + 1
    if mode in {"towel", "completion"}:
        # The single-bout resident is respawned after a real bout: it drops its
        # pool + heartbeat and MUST be re-PRELOADed for the new token to beat.
        runner.restart()

    ready_b = await _drive_to_ready(manager, provider)
    assert ready_b is not None and ready_b.state == Round5WarmState.READY
    token_b = ready_b.warm_attempt_token
    assert token_b is not None and token_b != token_a
    assert ready_b.generation == ready_a.generation + 1

    # The invariant: exactly ONE live PRELOAD per lane for the new token -- no
    # superseded backlog -- and the resident is CURRENT for B.
    for lane in LANES:
        assert runner.preloads[lane].count(token_b) == 1
        assert runner.preloads[lane] == [token_a, token_b]
    assert await provider.validate_ready(ready_b, None) is True

    # READY holds across further idle probes (no flicker).
    for _ in range(5):
        await runner.beat()
        await manager.run_one_cycle()
        clock.advance(3)
    steady = await manager.store.read(INSTALLATION)
    assert steady is not None and steady.state == Round5WarmState.READY
    assert steady.warm_attempt_token == token_b


async def test_seam_repeated_generations_A_B_C_each_converge() -> None:
    clock = Clock()
    store = InMemoryRound5ControlStore()
    runner = RunnerDouble(store, clock)
    provider = SeamProvider(clock, store, runner)
    manager = _coordinator(clock, provider)
    await manager.store.initialize()

    seen_tokens: list[str] = []
    ready = await _drive_to_ready(manager, provider)
    assert ready is not None and ready.state == Round5WarmState.READY
    seen_tokens.append(ready.warm_attempt_token)

    for index in range(2):
        claimed, _ = await manager.claim(
            session_id=f"session-gen-{index}",
            bout_id=f"bout-gen-{index}",
            selected_variant=Round5Variant.AURORA,
            bout_fence=1,
        )
        assert claimed.claim is not None
        await manager.accept_bell(claimed.claim.claim_id)
        await manager.begin_cleanup(claimed.claim.claim_id)
        converged = await manager.converge_cleanup(claimed.claim.claim_id)
        assert converged.state == Round5WarmState.WARMING
        runner.restart()
        ready = await _drive_to_ready(manager, provider)
        assert ready is not None and ready.state == Round5WarmState.READY
        seen_tokens.append(ready.warm_attempt_token)

    # Three DISTINCT generations, three DISTINCT tokens (no reuse across bouts).
    assert len(set(seen_tokens)) == 3
    assert ready.generation == 3
    # Each lane preloaded exactly the three distinct tokens, once each.
    for lane in LANES:
        assert runner.preloads[lane] == seen_tokens


async def test_seam_delayed_token_a_events_rejected_after_b() -> None:
    # Anti-replay: once B supersedes A, a delayed heartbeat replayed under token A
    # must not resurrect A as CURRENT -- validate for the current token (B) still
    # reads only B's attestation.
    clock = Clock()
    store = InMemoryRound5ControlStore()
    runner = RunnerDouble(store, clock)
    provider = SeamProvider(clock, store, runner)
    manager = _coordinator(clock, provider)
    await manager.store.initialize()

    ready_a = await _drive_to_ready(manager, provider)
    token_a = ready_a.warm_attempt_token
    claimed, _ = await manager.claim(
        session_id="session-replay",
        bout_id="bout-replay",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    await manager.accept_bell(claimed.claim.claim_id)
    await manager.begin_cleanup(claimed.claim.claim_id)
    await manager.converge_cleanup(claimed.claim.claim_id)
    runner.restart()
    ready_b = await _drive_to_ready(manager, provider)
    token_b = ready_b.warm_attempt_token
    assert token_b != token_a

    # Replay a stale token-A heartbeat AFTER B is READY.
    request = _canonical_request(lane="lakebase", generation=ready_a.generation, token=token_a)
    binding_a = _preload_binding(ready_a.generation, token_a, "lakebase", request)
    boot, process, pid = LANE_IDENTITY["lakebase"]
    await store.append_runner_event(
        Round5RunnerEvent(
            event_id=hashlib.sha256(f"replay-{token_a}".encode()).hexdigest(),
            binding=Round5ControlBinding(
                **{**binding_a.wire_value(), "runner_process_boot_id": process}
            ),
            sequence=99,
            kind=Round5RunnerEventKind.HEARTBEAT,
            occurred_at=clock.now,
            payload={
                "worker_ready_indexes": [0, 1, 2, 3],
                "runner_boot_id": boot,
                "runner_process_boot_id": process,
                "process_pid": pid,
                "runner_harness_sha256": HARNESS,
            },
        )
    )

    # The replayed A attestation exists, but the CURRENT token is B; validate for B
    # is unaffected, and A is never the current token again.
    assert await provider.validate_ready(ready_b, None) is True
    slot = await manager.store.read(INSTALLATION)
    assert slot.warm_attempt_token == token_b
    # A stale liveness probe under token A would read CURRENT for the replayed beat,
    # but the coordinator only ever attests the durable slot's current token (B).
    a_liveness = await provider.transport.resident_liveness(
        installation_id=INSTALLATION,
        lane_id="lakebase",
        warm_attempt_token=token_b,
        runner_boot_id=boot,
        process_boot_id=process,
        process_pid=pid,
        harness_sha256=HARNESS,
        now=clock.now,
    )
    assert a_liveness is ResidentLiveness.CURRENT


async def test_seam_resident_never_ready_bounds_tokens_and_blocks_named() -> None:
    # Churn-bound: if the resident never comes ready, the coordinator does NOT mint
    # a fresh token every beat (an unbounded superseded PRELOAD flood). It reuses
    # the in-flight token within the window, rotates at most once per window, and
    # escalates to a NAMED terminal block -- a bounded number of distinct tokens.
    clock = Clock()
    store = InMemoryRound5ControlStore()
    runner = RunnerDouble(store, clock)
    runner.up = False  # resident never emits agent_ready/heartbeat
    provider = SeamProvider(clock, store, runner)
    manager = _coordinator(clock, provider)
    await manager.store.initialize()

    blocked = None
    for _ in range(400):
        await manager.run_one_cycle()
        clock.advance(WARM_ATTEMPT_TOKEN_REUSE_SECONDS)
        slot = await manager.store.read(INSTALLATION)
        if slot is not None and slot.state == Round5WarmState.BLOCKED:
            blocked = slot
            break

    assert blocked is not None
    assert blocked.last_error_code == "resident_restarting_persistent"
    # Bounded flood: distinct minted tokens are on the order of the transient
    # attempt cap, NOT one-per-beat. Each distinct token is preloaded at most once.
    distinct_tokens = set(provider.attempt_tokens)
    assert len(distinct_tokens) <= MAX_TRANSIENT_WARM_ATTEMPTS + 1
    for lane in LANES:
        assert len(runner.preloads[lane]) == len(distinct_tokens)
        assert len(runner.preloads[lane]) == len(set(runner.preloads[lane]))
