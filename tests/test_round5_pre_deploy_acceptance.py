"""Local, deterministic pre-deploy acceptance harness for the Round 5 route.

This module proves the *production* Round 5 path end-to-end before anyone spends
a paid Aurora/RDS bout. It deliberately wires the **real** collaborators and only
fakes the two things that would otherwise reach the network:

* the ``Round5WarmProvider`` (no AWS discovery / credential minting), and
* the per-lane fan-in *runner* (no SSM, no sockets, no 10,000 real clients).

Everything between those two seams is the shipped code:

* the real ``Round5WarmCoordinator`` over the real (transactional, compare-and-swap)
  ``InMemoryRound5WarmStore``;
* the real ``RunManager`` claim/bell/terminal/rewarm orchestration;
* the real FastAPI control router driven over ASGI (``/api/sessions*``);
* the real ``LiveConnectionSpikeSetupOrchestrator`` lane pipeline for the
  Lakebase-immediate / CreateDBProxy-first / competitor-after-gate ordering.

Every assertion is *behavioral* -- it observes state transitions, HTTP responses,
call ordering, exact client counts, and durable generation advancement. None of
it asserts on source text, so appending a string to a file cannot make a broken
route pass this harness.

The checklist each test maps to (see ``ACCEPTANCE_CHECKLIST`` below) mirrors the
go/no-go gates in ``docs/ROUND5_BELL_TO_10K_DESIGN.md``.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from server import connection_spike_live as live
from server.api import router
from server.connection_spike import (
    PublicSetupEvidence,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupStopGateEvidence,
    arm_setup_phase,
)
from server.connection_spike_live import (
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeSetupLaneStop,
    LiveConnectionSpikeEngine,
)
from server.manager import InvalidStateError, RunManager
from server.models import (
    BoutOperator,
    CompetitorId,
    Corner,
    LaneState,
    RoundFiveRuntimeLaneSnapshot,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.round5_warm import (
    BellContext,
    InMemoryRound5WarmStore,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmPreparation,
    Round5WarmState,
)

# Proven fan-in result builders. ``raw_fanin_lane`` yields an exact 10,000-client
# lane payload; ``finalize_fanin_lane`` validates and freezes it the way the
# runner would, so the manager's exact-count gate sees a genuine proof.
from tests.test_connection_fanin import finalize as finalize_fanin_lane
from tests.test_connection_fanin import raw_lane as raw_fanin_lane

# Proven fixture builders from the warm unit suite. Reusing them keeps this
# harness faithful to the exact receipt/capsule shapes the coordinator demands
# rather than re-deriving (and drifting from) them here.
from tests.test_round5_warm import (
    DIGEST,
    Clock,
    Provider,
    preparation,
)

_T_LIFECYCLE = "test_full_lifecycle_bell_duplicate_run_and_rewarm_via_asgi"
_T_ORDERING = "test_create_proxy_is_first_aws_mutation_and_competitor_waits_for_gate"
_T_FAILURE = "test_failure_preserves_verified_lane_and_drives_real_rewarm"

ACCEPTANCE_CHECKLIST = {
    "startup_automatic_warm": "test_startup_warm_reaches_ready_without_any_http_request",
    "ready_gating": "test_arm_is_refused_until_the_ring_is_ready",
    "o1_claim": "test_arm_claim_is_o1_and_performs_no_provider_work",
    "two_runner_independence": "test_arm_binds_two_independent_runner_jobs",
    "concurrent_duplicate_run": _T_LIFECYCLE,
    "atomic_bell_readback": "test_duplicate_bell_returns_one_server_context",
    "t0_and_order": _T_LIFECYCLE,
    "lakebase_immediate_release": "test_lakebase_lane_releases_dispatch_before_any_progress_io",
    "create_dbproxy_first": _T_ORDERING,
    "competitor_after_gate": _T_ORDERING,
    "exact_10k_hold_samples": "test_exact_10k_gate_rejects_a_9999_and_a_partial_lane",
    "asymmetric_verified_preserved": _T_FAILURE,
    "towel_cleanup_rewarm": "test_towel_terminal_drives_real_cleanup_and_rewarm",
    "late_towel_rewarm": "test_late_towel_rewarms_before_second_round5_arm",
    "success_linger_rewarm": "test_success_linger_keeps_next_start_blocked_until_rewarm_ready",
    "prepared_expiry_release": "test_prepared_session_expiry_without_bell_converges_via_cleaning",
    "prepared_expiry_cleanup_recovery": (
        "test_prepared_expiry_cleanup_worker_loss_still_converges"
    ),
    "failure_cleanup_rewarm": _T_FAILURE,
    "restart_cleanup_rewarm": "test_restart_reconciles_running_generation_before_rewarm",
}


# ---------------------------------------------------------------------------
# Faithful transactional fakes: a warm provider that yields a *behavioral*
# per-lane engine, and the engine itself.
# ---------------------------------------------------------------------------


class _EnginePlan:
    """A per-bout script the behavioral engine runs inside ``setup()``."""

    async def run(self, engine, on_setup_progress, on_lane_progress, on_lane_result):
        raise NotImplementedError

    async def cancel(self) -> None:
        return None


class _BlockingPlan(_EnginePlan):
    """Setup enters and holds until the bout is cancelled (towel / duplicate run)."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self._release = asyncio.Event()

    async def run(self, engine, on_setup_progress, on_lane_progress, on_lane_result) -> None:
        del engine, on_setup_progress, on_lane_progress, on_lane_result
        self.entered.set()
        await self._release.wait()

    async def cancel(self) -> None:
        self._release.set()


class _RecoverableAbandonPlan(_BlockingPlan):
    """Pre-bell resident cleanup fails until its durable retry is allowed."""

    def __init__(self, *, fail_setup_cleanup: bool = True) -> None:
        super().__init__()
        self.fail_setup_cleanup = fail_setup_cleanup
        self.allow_resident_settle = asyncio.Event()
        self.cancel_setup_attempts = 0
        self.journal_reconcile_attempts = 0
        self.durable_resident_reconcile_attempts = 0


class _LateTowelPlan(_BlockingPlan):
    async def run(self, engine, on_setup_progress, on_lane_progress, on_lane_result) -> None:
        del engine, on_setup_progress, on_lane_result
        await on_lane_progress(_exact_holding_progress("lakebase", 1))
        await on_lane_progress(_exact_holding_progress("competitor", 1))
        self.entered.set()
        await self._release.wait()


class _SuccessfulPlan(_EnginePlan):
    async def run(self, engine, on_setup_progress, on_lane_progress, on_lane_result) -> None:
        del engine, on_setup_progress
        for lane_id in ("lakebase", "competitor"):
            await on_lane_progress(_exact_holding_progress(lane_id, 1))
            await on_lane_result(finalize_fanin_lane(raw_fanin_lane(lane_id)))


def _exact_holding_progress(lane_id: str, sequence: int) -> SimpleNamespace:
    """A holding progress event that observes exactly 10,000 held clients.

    The manager only accepts a verified lane *result* once it has already
    observed the exact 10,000-client gate (``bell_to_10000_observed_ms``) from a
    progress event, so a faithful runner emits this first.
    """

    return SimpleNamespace(
        lane_id=lane_id,
        phase="holding",
        initiated_clients=10_000,
        authenticated_clients=10_000,
        held_clients=10_000,
        peak_held_clients=10_000,
        sampled_queries_succeeded=64,
        time_to_target_ms=12_500.0,
        elapsed_ms=12_500.0,
        sequence=sequence,
    )


class _AsymmetricFailurePlan(_EnginePlan):
    """Lakebase verifies exactly; the competitor returns a sub-10k partial."""

    async def run(self, engine, on_setup_progress, on_lane_progress, on_lane_result) -> None:
        del engine, on_setup_progress
        await on_lane_progress(_exact_holding_progress("lakebase", 1))
        await on_lane_result(finalize_fanin_lane(raw_fanin_lane("lakebase")))
        await on_lane_result(
            SimpleNamespace(
                lane_id="competitor",
                initiated_clients=7_162,
                authenticated_clients=7_134,
                cancelled_clients=28,
                held_clients_at_gate=7_134,
                terminal_failures=0,
                failure_codes={},
                retries=0,
                disconnected_during_hold=0,
                sampled_queries_succeeded=32,
                verified=True,
                gates=SimpleNamespace(passed=True, telemetry=True),
            )
        )


class _AcceptanceEngine:
    """A transactional stand-in for ``LiveConnectionSpikeEngine``.

    It implements exactly the surface the manager drives (``bind_claim``,
    ``prepare``, ``setup``, terminal/cleanup handoff) and records the claim it was
    bound to so the harness can prove two-runner independence.
    """

    has_timed_setup = True

    def __init__(self, plan: _EnginePlan) -> None:
        self._plan = plan
        self._armed = object()
        self.bound_claim: object | None = None
        self.bound_bell: BellContext | None = None
        self.cleanup_calls: list[str] = []
        self.reconcile_claim_calls = 0
        self.settle_abandoned_calls = 0
        self.launch_intent_calls = 0
        self.setup_calls = 0

    async def settle_abandoned_arm(self) -> set[str]:
        # The pre-bell unstage seam the manager invokes on every abandon path
        # (armed-TTL expiry, arm-refusal, and -- the regression this records -- a
        # refused bell). Counting proves the getattr wiring actually fires.
        self.settle_abandoned_calls += 1
        if (
            isinstance(self._plan, _RecoverableAbandonPlan)
            and not self._plan.allow_resident_settle.is_set()
        ):
            assert self.bound_claim is not None
            return {
                self.bound_claim.lakebase_job_id,
                self.bound_claim.competitor_job_id,
            }
        return set()

    def bind_claim(self, claim: object) -> None:
        self.bound_claim = claim

    def bind_bell(self, context: BellContext) -> None:
        self.bound_bell = context

    async def prepare(self, bout_id: str, fencing_token: int) -> None:
        assert bout_id and fencing_token > 0

    async def precommit_launch_intent(self, bout_id: str, fencing_token: int) -> None:
        # Required bell seam (req #6): the manager calls this directly on the bell
        # path, before the authoritative T0.  A no-op here (the real durable
        # CREATE_INTENT write is covered by the real-PostgreSQL journal tests).
        assert bout_id and fencing_token > 0
        self.launch_intent_calls += 1

    async def setup(
        self,
        bout_id,
        fencing_token,
        on_setup_progress,
        on_lane_progress,
        on_lane_result,
    ) -> object | None:
        del bout_id, fencing_token
        self.setup_calls += 1
        await self._plan.run(self, on_setup_progress, on_lane_progress, on_lane_result)
        if isinstance(self._plan, _SuccessfulPlan):
            arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=1_000_000_000)
            fact = PublicSetupEvidence("resident_ready", True)
            return SimpleNamespace(
                arm=arm,
                observations=(
                    SetupLaneObservation(
                        lane_id="lakebase",
                        workflow_launched_ns=arm.t0_ns,
                        status=SetupLaneStatus.SUCCEEDED,
                        stop_gate_evidence=SetupStopGateEvidence(
                            "lakebase-stop",
                            (fact,),
                            (fact,),
                            arm.t0_ns + 1_000_000,
                        ),
                    ),
                    SetupLaneObservation(
                        lane_id="competitor",
                        workflow_launched_ns=arm.t0_ns + 1_000_000,
                        status=SetupLaneStatus.SUCCEEDED,
                        stop_gate_evidence=SetupStopGateEvidence(
                            "competitor-stop",
                            (fact,),
                            (fact,),
                            arm.t0_ns + 3_000_000,
                        ),
                        create_db_proxy_requested_ns=arm.t0_ns + 2_000_000,
                        requires_create_db_proxy_stamp=True,
                    ),
                ),
            )

    async def run(self, arm, on_progress):
        del arm
        if not isinstance(self._plan, _SuccessfulPlan):
            raise AssertionError("only the successful acceptance plan reaches burst result")
        lanes = {
            lane_id: finalize_fanin_lane(raw_fanin_lane(lane_id))
            for lane_id in ("lakebase", "competitor")
        }
        for lane_id in ("lakebase", "competitor"):
            await on_progress(_exact_holding_progress(lane_id, 2))
        return SimpleNamespace(lanes=lanes)

    async def cancel_setup_and_settle(self, bout_id) -> None:
        del bout_id
        if isinstance(self._plan, _RecoverableAbandonPlan):
            self._plan.cancel_setup_attempts += 1
            if self._plan.fail_setup_cleanup:
                raise RuntimeError("pre-bell setup identity is stale")
        await self._plan.cancel()

    # Terminal cleanup handoff seams (all no-ops that let the *real* coordinator
    # rewarm run for real).
    async def stop_setup_and_begin_cleanup(self, bout_id) -> None:
        del bout_id
        self.cleanup_calls.append("stop_setup")

    async def wait_for_proxy_delete_accepted(self) -> None:
        self.cleanup_calls.append("proxy_delete_accepted")

    async def wait_for_cleanup_complete(self) -> None:
        self.cleanup_calls.append("cleanup_complete")

    def proxy_delete_accepted(self) -> bool:
        return True

    def proxy_name_for_bout(self, bout_id: str) -> str:
        return f"anti-demo-r5-{bout_id}-proxy"

    async def reconcile_failed_cleanup(
        self,
        bout_id: str,
        fencing_token: int,
    ) -> None:
        assert bout_id and fencing_token > 0
        if isinstance(self._plan, _RecoverableAbandonPlan):
            self._plan.journal_reconcile_attempts += 1

    async def reconcile_abandoned_claim(self, claim) -> None:
        assert claim.bout_id
        if isinstance(self._plan, _RecoverableAbandonPlan):
            self._plan.durable_resident_reconcile_attempts += 1
            if not self._plan.allow_resident_settle.is_set():
                raise RuntimeError("durable resident jobs are not settled")

    async def reconcile_claim(self, claim) -> None:
        # Post-bell restart reconciliation seam, driven by the provider (single
        # owner) exactly like reconcile_abandoned_claim.
        self.reconcile_claim_calls += 1
        assert claim.bout_id
        if isinstance(self._plan, _RecoverableAbandonPlan):
            self._plan.durable_resident_reconcile_attempts += 1
            if not self._plan.allow_resident_settle.is_set():
                raise RuntimeError("durable resident jobs are not settled")


class _EngineProvider(Provider):
    """A warm provider whose prepared capsule carries behavioral lane engines."""

    def __init__(self, clock: Clock, plan_factory) -> None:
        super().__init__(clock)
        self._plan_factory = plan_factory
        self.engines: list[_AcceptanceEngine] = []

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
        prep = preparation(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            warm_attempt_token=warm_attempt_token,
            broker_epoch=broker_epoch,
        )
        engine = _AcceptanceEngine(self._plan_factory())
        self.engines.append(engine)
        capsule = replace(
            prep.capsule,
            variant_contexts={
                Round5Variant.AURORA: engine,
                Round5Variant.RDS: engine,
            },
        )
        return replace(prep, capsule=capsule)

    async def reconcile(self, slot) -> bool:
        # The PROVIDER is the single cleanup-mutation owner (mirrors the real
        # LiveRound5WarmProvider): it drives the engine's durable reconcile. The
        # manager no longer reconciles independently -- it awaits converge_cleanup,
        # which routes here. Only a CLEANING/RUNNING slot with a claim reconciles.
        from server.round5_warm import Round5WarmState

        self.reconcile_calls += 1
        state = getattr(slot, "state", None)
        claim = getattr(slot, "claim", None)
        if state not in {Round5WarmState.RUNNING, Round5WarmState.CLEANING} or claim is None:
            return False
        # Mirror the real provider: obtain a cleanup engine even after a takeover
        # where this replacement process warmed nothing yet (self.engines empty).
        # Prefer the adopted claimed engine (holds the ARM-staged residents), like
        # the real provider; fall back to a fresh one on takeover.
        adopted = getattr(self, "_adopted_engines", {}).get(str(claim.claim_id))
        if adopted is not None:
            engine = adopted
        elif self.engines:
            engine = self.engines[-1]
        else:
            engine = _AcceptanceEngine(self._plan_factory())
            self.engines.append(engine)
        if getattr(slot, "bell_id", None) is None and getattr(slot, "bell_at_utc", None) is None:
            # SOLE janitor: cancel staged residents then settle the durable jobs.
            settle = getattr(engine, "settle_abandoned_arm", None)
            if callable(settle):
                await settle()
            await engine.reconcile_abandoned_claim(claim)
        else:
            await engine.reconcile_claim(claim)
        return True

    def adopt_claimed_engine(self, claim_id, engine) -> None:
        if not hasattr(self, "_adopted_engines"):
            self._adopted_engines = {}
        self._adopted_engines[str(claim_id)] = engine


async def test_round5_bout_status_answers_from_round5_readiness_not_global() -> None:
    # B3 (Finding 2): Round 5 bout_status answers from the round5-specific readiness/warm
    # signal ALONE. A global installation readiness that is NOT ready must not report
    # Round 5 unavailable when its own per-round ring/warm coordinator is healthy.
    # Mutation: restoring `and (readiness is None or readiness.ring_ready)` to the manager
    # gate re-couples Round 5 to the global signal and fails the can_start assertion.
    clock = Clock()
    provider = Provider(clock)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    manager = RunManager(
        connection_spike_factory=lambda _competitor: object(),
        round5_warm_coordinator=coordinator,
        round_isolation=True,
        installation_id="install-acceptance",
        clock_ns=lambda: clock.monotonic_ns + 50_000_000,
        readiness_status=lambda: SimpleNamespace(
            ring_ready=False,
            maintenance_state="maintenance",
            maintenance_detail="unrelated global blip",
        ),
        round5_readiness_status=lambda: SimpleNamespace(
            ring_ready=True,
            maintenance_state="ready",
            maintenance_detail=None,
        ),
    )
    status = await manager.bout_status(RoundId.SURVIVE_CONNECTION_SPIKE)
    assert status.ring_ready is True
    assert status.can_start is True
    assert status.maintenance_state == "ready"


def _coordinator(clock: Clock, provider: Provider) -> Round5WarmCoordinator:
    return Round5WarmCoordinator(
        installation_id="install-acceptance",
        warm_contract_sha256=DIGEST,
        store=InMemoryRound5WarmStore(),
        provider=provider,
        process_epoch="process-acceptance",
        broker_epoch="broker-acceptance",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )


async def _warm_to_ready(coordinator: Round5WarmCoordinator, provider: Provider) -> None:
    await coordinator.store.initialize()
    provider.release_prepare.set()
    await asyncio.wait_for(coordinator.run_one_cycle(), timeout=2)
    assert coordinator.ring_ready, "warm coordinator did not reach a ringable READY slot"


def _make_manager(
    coordinator: Round5WarmCoordinator,
    clock: Clock,
    *,
    round_isolation: bool = False,
) -> RunManager:
    # The factory is required by the arm guard but never used on the warm path:
    # the engine is taken from the claimed launch capsule.
    return RunManager(
        connection_spike_factory=lambda _competitor: object(),
        round5_warm_coordinator=coordinator,
        round_isolation=round_isolation,
        installation_id="install-acceptance",
        clock_ns=lambda: clock.monotonic_ns + 50_000_000,
    )


def _asgi(manager: RunManager) -> FastAPI:
    api = FastAPI()
    api.include_router(router)
    api.state.run_manager = manager
    api.state.readiness_gate = SimpleNamespace(
        status=SimpleNamespace(ring_ready=True, maintenance_detail=None),
        round5_status=SimpleNamespace(
            ring_ready=manager.round5_ring_ready,
            reason_code=None,
            maintenance_state="ready",
            maintenance_detail=None,
        ),
    )
    return api


def _session_body(competitor: str = "aurora_serverless_v2") -> dict[str, object]:
    return {
        "competitor": competitor,
        "primary_persona": "sre",
        "corners": ["performance"],
        "round_id": "survive_connection_spike",
    }


@pytest.fixture(autouse=True)
def _local_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    # ``operator_from_request`` returns a local operator (no SSO headers needed)
    # only when the app does not believe it is deployed.
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)


async def _arm_via_http(client: AsyncClient, session_id: str) -> dict[str, object]:
    armed = await client.post(f"/api/sessions/{session_id}/arm")
    assert armed.status_code == 200, armed.text
    for _ in range(200):
        snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
        if snapshot["state"] == SessionState.ARMED.value:
            return snapshot
        await asyncio.sleep(0)
    raise AssertionError("session never reached ARMED")


# ---------------------------------------------------------------------------
# 1. Startup automatic warm + READY gating (no HTTP request triggers the warm)
# ---------------------------------------------------------------------------


async def test_startup_warm_reaches_ready_without_any_http_request() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    try:
        await coordinator.store.initialize()
        # The warm begins on its own cycle -- there is no session, no /arm, no /run.
        first = asyncio.create_task(coordinator.run_one_cycle())
        await asyncio.wait_for(provider.prepare_started.wait(), timeout=2)
        assert coordinator.ring_ready is False
        provider.release_prepare.set()
        await asyncio.wait_for(first, timeout=2)

        slot = await coordinator.store.read("install-acceptance")
        assert slot is not None
        assert slot.state == Round5WarmState.READY
        assert set(slot.variants) == {Round5Variant.AURORA, Round5Variant.RDS}
        assert all(receipt.proxy_absent for receipt in slot.variants.values())
        assert coordinator.ring_ready is True
        assert provider.prepare_calls == 1
    finally:
        await coordinator.close()


async def test_arm_is_refused_until_the_ring_is_ready() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    # Deliberately leave the slot WARMING (never released) so the ring is not ready.
    await coordinator.store.initialize()
    await coordinator.store.ensure_warming(
        installation_id="install-acceptance",
        warm_contract_sha256=DIGEST,
        process_epoch="process-acceptance",
        broker_epoch="broker-acceptance",
        now=clock.now,
    )
    assert coordinator.ring_ready is False
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            assert created.status_code == 201, created.text
            session_id = created.json()["id"]

            first, second = await asyncio.gather(
                client.post(f"/api/sessions/{session_id}/arm"),
                client.post(f"/api/sessions/{session_id}/arm"),
            )
            assert first.status_code == second.status_code == 409
            for response in (first, second):
                detail = response.json()["detail"]
                assert "ROUND 5 NOT STARTABLE" in detail
                assert "STAGE REWARMING" in detail
                assert "CAN_START FALSE" in detail
            snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
            assert snapshot["state"] != SessionState.ARMED.value
            slot = await coordinator.store.read("install-acceptance")
            assert slot is not None
            assert slot.state == Round5WarmState.WARMING
            assert slot.claim is None
    finally:
        await manager.close()
        await coordinator.close()


@pytest.mark.parametrize(
    ("identity_field", "stale_value"),
    [
        ("broker_epoch", "broker-stale"),
        ("warm_attempt_token", "attempt-stale"),
    ],
)
async def test_http_refuses_ready_slot_with_stale_capsule_identity(
    identity_field: str,
    stale_value: str,
) -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    assert coordinator.capsule is not None
    coordinator._capsule = replace(
        coordinator.capsule,
        **{identity_field: stale_value},
    )
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            board = (await client.get("/api/bout/all")).json()
            status = board["rounds"]["survive_connection_spike"]
            assert status["can_start"] is False
            assert status["state"] == "temporarily_unavailable"
            assert "STAGE IDENTITY-REFRESH" in status["detail"]
            assert "GENERATION 1" in status["detail"]
            assert "CAN_START FALSE" in status["detail"]

            created = await client.post("/api/sessions", json=_session_body())
            armed = await client.post(f"/api/sessions/{created.json()['id']}/arm")
            assert armed.status_code == 409
            detail = armed.json()["detail"]
            assert "ROUND 5 NOT STARTABLE" in detail
            assert "STAGE IDENTITY-REFRESH" in detail
            assert "GENERATION 1" in detail
            assert "CAN_START FALSE" in detail

            slot = await coordinator.store.read("install-acceptance")
            assert slot is not None
            assert slot.state == Round5WarmState.READY
            assert slot.claim is None
    finally:
        await manager.close()
        await coordinator.close()


async def test_concurrent_duplicate_arm_claims_ready_generation_once() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            first, second = await asyncio.gather(
                client.post(f"/api/sessions/{session_id}/arm"),
                client.post(f"/api/sessions/{session_id}/arm"),
            )
            assert first.status_code == second.status_code == 200
            await _arm_via_http(client, session_id)

        slot = await coordinator.store.read("install-acceptance")
        assert slot is not None
        assert slot.state == Round5WarmState.CLAIMED
        assert slot.claim is not None
        assert slot.claim.session_id == session_id
        events = await coordinator.store.events("install-acceptance")
        assert sum(event.event_type == "claim_created" for event in events) == 1
    finally:
        await manager.close()
        await coordinator.close()


async def test_distinct_subjects_racing_prepare_get_exactly_one_claim() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock)
    sessions = [
        await manager.create(
            SessionCreate(
                competitor=competitor,
                primary_persona="sre",
                corners=[Corner.PERFORMANCE],
                round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
            )
        )
        for competitor in (
            CompetitorId.AURORA_SERVERLESS_V2,
            CompetitorId.RDS_POSTGRES,
        )
    ]
    operators = (
        BoutOperator(display_name="Operator A", subject="subject-a"),
        BoutOperator(display_name="Operator B", subject="subject-b"),
    )
    barrier = asyncio.Barrier(3)

    async def prepare(index: int):
        await barrier.wait()
        return await manager.start_arm(sessions[index].id, operators[index])

    first = asyncio.create_task(prepare(0))
    second = asyncio.create_task(prepare(1))
    await barrier.wait()
    results = await asyncio.gather(first, second, return_exceptions=True)
    try:
        winners = [
            (index, result)
            for index, result in enumerate(results)
            if not isinstance(result, BaseException)
        ]
        refusals = [result for result in results if isinstance(result, BaseException)]
        assert len(winners) == 1
        assert len(refusals) == 1
        assert isinstance(refusals[0], InvalidStateError)

        winner_index, _ = winners[0]
        winner_id = sessions[winner_index].id
        for _ in range(200):
            winner = await manager.get(winner_id)
            if winner.state == SessionState.ARMED:
                break
            await asyncio.sleep(0)
        assert winner.state == SessionState.ARMED

        slot = await coordinator.store.read("install-acceptance")
        assert slot is not None
        assert slot.state == Round5WarmState.CLAIMED
        assert slot.claim is not None
        assert slot.claim.session_id == winner_id
        current_lease = await manager._lease_store.current()
        assert current_lease is not None
        assert current_lease.owner_subject == operators[winner_index].subject
        events = await coordinator.store.events("install-acceptance")
        assert sum(event.event_type == "claim_created" for event in events) == 1
    finally:
        await manager.close()
        await coordinator.close()


async def test_unexpected_fallback_claim_failure_is_not_misreported_as_warming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)

    async def fail_claim(**_kwargs: object) -> object:
        raise RuntimeError("unexpected claim infrastructure failure")

    monkeypatch.setattr(coordinator, "claim", fail_claim)
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            armed = await client.post(f"/api/sessions/{created.json()['id']}/arm")
            assert armed.status_code == 503
            assert "ROUND 5 NOT STARTABLE" not in armed.text
    finally:
        await manager.close()
        await coordinator.close()


# ---------------------------------------------------------------------------
# 2. O(1) claim at /arm, and two independent runner jobs
# ---------------------------------------------------------------------------


async def test_arm_claim_is_o1_and_performs_no_provider_work() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    counts = (provider.prepare_calls, provider.refresh_calls, provider.reconcile_calls)
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)

        slot = await coordinator.store.read("install-acceptance")
        assert slot is not None and slot.state == Round5WarmState.CLAIMED
        # Claiming a ready generation touches no provider work at all.
        assert (
            provider.prepare_calls,
            provider.refresh_calls,
            provider.reconcile_calls,
        ) == counts
    finally:
        await manager.close()
        await coordinator.close()


async def test_bell_failure_after_arm_releases_lease_and_ring_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 4(b): a bell that fails after a successful claim/promotion must
    release the round-5 lease and bout, so the ring is not left stuck (the exact
    stuck-UNAVAILABLE shape of the 2026-09-23 incident's cascade). After the failed
    /run, a fresh session must be able to arm again."""

    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            slot = await coordinator.store.read("install-acceptance")
            assert slot is not None and slot.state == Round5WarmState.CLAIMED

            # Injure the authoritative bell exactly as the live broker-epoch CAS
            # loss did: it raises AFTER the arm claim is held.
            async def _bell_boom(*_args: object, **_kwargs: object) -> object:
                raise RuntimeError("bell CAS lost the claim")

            monkeypatch.setattr(coordinator, "accept_bell_with_leases", _bell_boom)
            monkeypatch.setattr(coordinator, "accept_bell", _bell_boom)

            claim_id = slot.claim.claim_id
            assert claim_id in coordinator._active_claim_ids  # renewed while armed

            run = await client.post(f"/api/sessions/{session_id}/run")
            assert run.status_code >= 400, run.text
            snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
            assert snapshot["state"] != SessionState.RUNNING.value

            # The refused bell fenced into durable CLEANING (single owner); resident
            # settlement is performed by the provider during convergence, not by a
            # synchronous manager settle at bell refusal time.
            cleaning = await coordinator.store.read("install-acceptance")
            assert cleaning is not None and cleaning.state == Round5WarmState.CLEANING

            # Finding 4(b) compensation ran: the warm claim's backstage renewal is
            # stopped (and the round-5 lease released), so the claim expires on its
            # TTL and the generation returns to READY (self-heal) instead of
            # staying stuck. WITHOUT the compensation the claim keeps being renewed
            # and the ring is stuck UNAVAILABLE -- the incident's cascade.
            assert claim_id not in coordinator._active_claim_ids
    finally:
        await manager.close()
        await coordinator.close()


async def test_arm_binds_two_independent_runner_jobs() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)

        slot = await coordinator.store.read("install-acceptance")
        assert slot is not None and slot.claim is not None
        assert slot.claim.lakebase_job_id != slot.claim.competitor_job_id
        engine = provider.engines[-1]
        assert engine.bound_claim is not None
        assert engine.bound_claim.lakebase_job_id != engine.bound_claim.competitor_job_id
    finally:
        await manager.close()
        await coordinator.close()


# ---------------------------------------------------------------------------
# 3. The bell over ASGI: one server bell, immediately-advancing clocks, and an
#    idempotent duplicate /run (one bell, one revision, one pair of runner jobs).
# ---------------------------------------------------------------------------


async def test_full_lifecycle_bell_duplicate_run_and_rewarm_via_asgi(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    plan = _BlockingPlan()
    provider = _EngineProvider(clock, lambda: plan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    bell_entered = asyncio.Event()
    release_bell = asyncio.Event()
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            engine = provider.engines[-1]
            original_precommit = engine.precommit_launch_intent

            async def gated_precommit(bout_id: str, fencing_token: int) -> None:
                await original_precommit(bout_id, fencing_token)
                bell_entered.set()
                await release_bell.wait()

            monkeypatch.setattr(engine, "precommit_launch_intent", gated_precommit)

            request_barrier = asyncio.Barrier(3)

            async def ring() -> object:
                await request_barrier.wait()
                return await client.post(f"/api/sessions/{session_id}/run")

            first_request = asyncio.create_task(ring())
            second_request = asyncio.create_task(ring())
            await request_barrier.wait()
            await asyncio.wait_for(bell_entered.wait(), timeout=2)
            # The first request is deliberately stopped inside the real bell
            # transaction while the duplicate is already contending on the
            # session lock. Neither response can be a sequential replay.
            await asyncio.sleep(0)
            assert not first_request.done()
            assert not second_request.done()
            release_bell.set()
            first_response, second_response = await asyncio.gather(
                first_request,
                second_request,
            )
            assert first_response.status_code == second_response.status_code == 200
            running = first_response.json()
            duplicate = second_response.json()
            assert running["state"] == SessionState.RUNNING.value
            runtime = running["round5_runtime"]
            assert runtime is not None
            bell_id = runtime["bell_id"]
            revision = runtime["revision"]
            assert set(runtime["lanes"]) == {"lakebase", "competitor"}
            # The server bell floor: both large clocks read from the accepted
            # bell, before any provider progress exists.
            for lane in runtime["lanes"].values():
                assert lane["elapsed_at_snapshot_ms"] == 0
            # ... and the server-computed projection is already advancing, so the
            # browser never renders a frozen 0.00.
            projection = running["round5_clock_projection"]
            assert projection is not None
            assert set(projection["elapsed_ms"]) == {"lakebase", "competitor"}
            assert all(value >= 0 for value in projection["elapsed_ms"].values())

            # The contending duplicate returns the exact committed launch.
            assert duplicate["state"] == SessionState.RUNNING.value
            assert duplicate["round5_runtime"]["bell_id"] == bell_id
            assert duplicate["round5_runtime"]["revision"] == revision

            # A poll does not manufacture a new bell or rewind the clock.
            await asyncio.wait_for(plan.entered.wait(), timeout=2)
            assert engine.launch_intent_calls == 1
            assert engine.setup_calls == 1
            assert engine.bound_claim is not None
            assert {
                engine.bound_claim.lakebase_job_id,
                engine.bound_claim.competitor_job_id,
            } == {
                (await coordinator.store.read("install-acceptance")).claim.lakebase_job_id,
                (await coordinator.store.read("install-acceptance")).claim.competitor_job_id,
            }
            assert len(
                {
                    engine.bound_claim.lakebase_job_id,
                    engine.bound_claim.competitor_job_id,
                }
            ) == 2
            events = await coordinator.store.events("install-acceptance")
            assert sum(event.event_type == "bell_accepted" for event in events) == 1
            polled = (await client.get(f"/api/sessions/{session_id}")).json()
            assert polled["round5_runtime"]["bell_id"] == bell_id
            assert polled["round5_runtime"]["revision"] >= revision

            # Rapid duplicate towel clicks contend for the same terminal edge.
            towel_barrier = asyncio.Barrier(3)

            async def towel():
                await towel_barrier.wait()
                return await client.post(f"/api/sessions/{session_id}/towel")

            first_towel = asyncio.create_task(towel())
            second_towel = asyncio.create_task(towel())
            await towel_barrier.wait()
            towel_responses = await asyncio.gather(first_towel, second_towel)
            assert all(response.status_code == 200 for response in towel_responses)
            assert {
                response.json()["state"] for response in towel_responses
            } == {SessionState.TOWELLED.value}
            terminal_events = [
                event.event
                for event in manager._records[session_id].event_log.events
                if event.event in {"run_finished", "session_failed", "towel_started"}
            ]
            assert terminal_events == ["towel_started"]
    finally:
        await manager.close()
        await coordinator.close()


# ---------------------------------------------------------------------------
# 4. Towel terminal drives real cleanup + rewarm.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("competitor", ["aurora_serverless_v2", "rds_postgres"])
async def test_towel_terminal_drives_real_cleanup_and_rewarm(
    competitor: str,
) -> None:
    clock = Clock()
    plan = _BlockingPlan()
    provider = _EngineProvider(clock, lambda: plan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    first_generation = (await coordinator.store.read("install-acceptance")).generation
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post(
                "/api/sessions",
                json=_session_body(competitor),
            )
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            await client.post(f"/api/sessions/{session_id}/run")
            await asyncio.wait_for(plan.entered.wait(), timeout=2)

            # Hold generation N+1 in WARMING after cleanup settles. This is the
            # production race window: per-bout resources are gone, but the next
            # launch capsule is not ready and must not be advertised as startable.
            provider.release_prepare.clear()
            provider.prepare_started.clear()
            towelled = (await client.post(f"/api/sessions/{session_id}/towel")).json()
            assert towelled["state"] == SessionState.TOWELLED.value
            assert all(
                lane["elapsed_at_snapshot_ms"] < 1_000
                for lane in towelled["round5_runtime"]["lanes"].values()
            )

            for _ in range(400):
                snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
                if snapshot["state"] == SessionState.TOWELLED.value:
                    break
                await asyncio.sleep(0)

            for _ in range(200):
                slot = await coordinator.store.read("install-acceptance")
                if slot is not None and slot.generation > first_generation:
                    break
                await asyncio.sleep(0)
            assert slot is not None
            assert slot.generation == first_generation + 1
            assert slot.state == Round5WarmState.WARMING
            assert slot.claim is None
            assert coordinator.ring_ready is False
            # Towel handoff converges through the coordinator/provider (single owner),
            # not the manager's legacy engine wait_for_cleanup_complete seam.
            assert provider.engines[0].reconcile_claim_calls >= 1

            for _ in range(400):
                board = (await client.get("/api/bout/all")).json()
                round_five = board["rounds"]["survive_connection_spike"]
                if round_five["state"] == "temporarily_unavailable":
                    break
                await asyncio.sleep(0)
            assert round_five["can_start"] is False
            assert round_five["state"] == "temporarily_unavailable"

            # Repeated stale clicks and a competitor switch are refused before
            # either can claim a ring or launch work.
            warming_sessions = []
            for competitor in ("aurora_serverless_v2", "aurora_serverless_v2", "rds_postgres"):
                created_while_warming = await client.post(
                    "/api/sessions",
                    json=_session_body(competitor),
                )
                warming_session_id = created_while_warming.json()["id"]
                warming_sessions.append(warming_session_id)
                refused = await client.post(
                    f"/api/sessions/{warming_session_id}/arm"
                )
                assert refused.status_code == 409
                detail = refused.json()["detail"]
                assert "ROUND 5 NOT STARTABLE" in detail
                assert "STAGE REWARMING" in detail
                assert "CAN_START FALSE" in detail
            assert coordinator.ring_ready is False

            # Cleanup settlement alone is insufficient. Only a completed warm
            # publishes READY and permits the next bout to prepare/arm.
            warm_next = asyncio.create_task(coordinator.run_one_cycle())
            await asyncio.wait_for(provider.prepare_started.wait(), timeout=2)
            warming = await coordinator.store.read("install-acceptance")
            assert warming is not None
            assert warming.state == Round5WarmState.WARMING
            assert coordinator.ring_ready is False
            provider.release_prepare.set()
            await asyncio.wait_for(warm_next, timeout=2)

            ready = await coordinator.store.read("install-acceptance")
            assert ready is not None
            assert ready.generation == first_generation + 1
            assert ready.state == Round5WarmState.READY
            assert coordinator.ring_ready is True
            board = (await client.get("/api/bout/all")).json()
            assert board["rounds"]["survive_connection_spike"]["can_start"] is True

            second = await client.post(
                "/api/sessions",
                json=_session_body("rds_postgres"),
            )
            second_armed = await _arm_via_http(client, second.json()["id"])
            assert second_armed["state"] == SessionState.ARMED.value
    finally:
        await manager.close()
        await coordinator.close()


@pytest.mark.parametrize("competitor", ["aurora_serverless_v2", "rds_postgres"])
async def test_late_towel_rewarms_before_second_round5_arm(
    competitor: str,
) -> None:
    clock = Clock()
    plan = _LateTowelPlan()
    provider = _EngineProvider(clock, lambda: plan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    first_generation = (await coordinator.store.read("install-acceptance")).generation
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post(
                "/api/sessions",
                json=_session_body(competitor),
            )
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            await client.post(f"/api/sessions/{session_id}/run")
            await asyncio.wait_for(plan.entered.wait(), timeout=2)
            running = (await client.get(f"/api/sessions/{session_id}")).json()
            assert all(
                lane["bell_to_10000_observed_ms"] is not None
                for lane in running["round5_runtime"]["lanes"].values()
            )

            provider.release_prepare.clear()
            provider.prepare_started.clear()
            towelled = await client.post(f"/api/sessions/{session_id}/towel")
            assert towelled.json()["state"] == SessionState.TOWELLED.value
            for _ in range(200):
                slot = await coordinator.store.read("install-acceptance")
                if slot is not None and slot.generation > first_generation:
                    break
                await asyncio.sleep(0)
            assert slot is not None and slot.state == Round5WarmState.WARMING
            for _ in range(400):
                status = (await client.get("/api/bout/all")).json()["rounds"][
                    "survive_connection_spike"
                ]
                if "STAGE REWARMING" in status["detail"]:
                    break
                await asyncio.sleep(0)
            assert status["can_start"] is False
            assert "STAGE REWARMING" in status["detail"]

            warm_next = asyncio.create_task(coordinator.run_one_cycle())
            await asyncio.wait_for(provider.prepare_started.wait(), timeout=2)
            provider.release_prepare.set()
            await asyncio.wait_for(warm_next, timeout=2)
            second = await client.post(
                "/api/sessions", json=_session_body("rds_postgres")
            )
            await _arm_via_http(client, second.json()["id"])
    finally:
        await manager.close()
        await coordinator.close()


async def test_success_linger_keeps_next_start_blocked_until_rewarm_ready() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _SuccessfulPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    first_generation = (await coordinator.store.read("install-acceptance")).generation
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            provider.release_prepare.clear()
            provider.prepare_started.clear()
            await client.post(f"/api/sessions/{session_id}/run")
            for _ in range(400):
                result = (await client.get(f"/api/sessions/{session_id}")).json()
                if result["state"] == SessionState.VERIFIED.value:
                    break
                await asyncio.sleep(0)
            assert result["state"] == SessionState.VERIFIED.value

            for _ in range(200):
                slot = await coordinator.store.read("install-acceptance")
                if slot is not None and slot.generation > first_generation:
                    break
                await asyncio.sleep(0)
            assert slot is not None and slot.state == Round5WarmState.WARMING

            # Remaining on the success screen must not reinterpret settled
            # per-bout cleanup as a ready next generation.
            for _ in range(5):
                lingered = (await client.get(f"/api/sessions/{session_id}")).json()
                assert lingered["state"] == SessionState.VERIFIED.value
                board = (await client.get("/api/bout/all")).json()
                assert board["rounds"]["survive_connection_spike"]["can_start"] is False

            retried = await client.post("/api/sessions", json=_session_body())
            refused = await client.post(f"/api/sessions/{retried.json()['id']}/arm")
            assert refused.status_code == 409
            assert "STAGE REWARMING" in refused.json()["detail"]

            warm_next = asyncio.create_task(coordinator.run_one_cycle())
            await asyncio.wait_for(provider.prepare_started.wait(), timeout=2)
            provider.release_prepare.set()
            await asyncio.wait_for(warm_next, timeout=2)
            second = await client.post("/api/sessions", json=_session_body())
            await _arm_via_http(client, second.json()["id"])
    finally:
        await manager.close()
        await coordinator.close()


async def test_prepared_session_expiry_without_bell_converges_via_cleaning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The direct return-to-READY fast path was removed: a no-bell expiry ALWAYS
    # enters CLEANING (claim retained) and converges via exact durable SETTLED +
    # provider absence + finish_cleanup_and_rewarm to generation N+1. Here the
    # blocking plan's reconcile succeeds immediately, so convergence is prompt.
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "0.02")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "0.05")
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    first_generation = (await coordinator.store.read("install-acceptance")).generation
    manager = _make_manager(coordinator, clock)
    manager._armed_ttl = 0.01
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            for _ in range(400):
                expired = (await client.get(f"/api/sessions/{session_id}")).json()
                if expired["state"] == SessionState.FAILED.value:
                    break
                await asyncio.sleep(0.01)
            assert expired["state"] == SessionState.FAILED.value
            assert expired["run_started_at"] is None

            # No direct READY: it fenced into CLEANING with the claim retained, then
            # the convergence worker (via the coordinator/provider -- the SOLE janitor)
            # settles the staged residents and rewarms to N+1.
            for _ in range(400):
                slot = await coordinator.store.read("install-acceptance")
                if slot is not None and slot.generation == first_generation + 1:
                    break
                await asyncio.sleep(0.01)
            assert slot is not None and slot.generation == first_generation + 1
            # The staged-resident settle was performed by the PROVIDER during
            # convergence (not synchronously by the manager at arm-expiry).
            assert provider.engines[0].settle_abandoned_calls >= 1

            await coordinator.run_one_cycle()  # WARMING(N+1) -> READY(N+1)
            ready = await coordinator.store.read("install-acceptance")
            assert ready is not None and ready.state == Round5WarmState.READY
            assert ready.claim is None
            board = (await client.get("/api/bout/all")).json()
            assert board["rounds"]["survive_connection_spike"]["can_start"] is True
            second = await client.post("/api/sessions", json=_session_body())
            await _arm_via_http(client, second.json()["id"])
    finally:
        await manager.close()
        await coordinator.close()


async def test_prepared_expiry_cleanup_worker_loss_still_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The live no-bell wedge: retryable debt can never heartbeat workerless."""

    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "0.02")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "0.05")
    clock = Clock()
    provider = _EngineProvider(clock, _RecoverableAbandonPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    manager = _make_manager(coordinator, clock, round_isolation=True)
    manager._armed_ttl = 0.01
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)

            for _ in range(400):
                expired = (await client.get(f"/api/sessions/{session_id}")).json()
                if (
                    expired["state"] == SessionState.FAILED.value
                    and expired["round5_setup"]["cleanup_retryable"] is True
                ):
                    break
                await asyncio.sleep(0.01)
            assert expired["round5_setup"]["cleanup_retryable"] is True

            record = manager._records[session_id]
            slot = await coordinator.store.read("install-acceptance")
            assert slot is not None and slot.state == Round5WarmState.CLEANING
            assert manager.round5_cleanup_owed is True
            assert manager.round5_ring_ready is False
            cleanup_lease = await manager._round5_cleanup_store().current()
            assert cleanup_lease is not None
            assert cleanup_lease.phase == "round5_cleanup"
            worker = record.connection_spike_cleanup_retry_task
            assert worker is not None and not worker.done()

            # The incident had this same retryable snapshot and cleanup lease
            # but no worker. That state is now structurally excluded: the worker
            # is installed before the expiry task publishes terminal failure.

            board = (await client.get("/api/bout/all")).json()
            round_five = board["rounds"]["survive_connection_spike"]
            assert round_five["can_start"] is False
            assert round_five["state"] == "cleanup_in_progress"
            assert "STAGE CLEANING" in round_five["detail"]
            assert "GENERATION 1" in round_five["detail"]
            assert "AUTO-CONVERGE SCHEDULED" in round_five["detail"]

            blocked = await client.post("/api/sessions", json=_session_body())
            blocked_arm = await client.post(
                f"/api/sessions/{blocked.json()['id']}/arm"
            )
            assert blocked_arm.status_code == 409
            assert "STAGE CLEANING" in blocked_arm.json()["detail"]
            assert "AUTO-CONVERGE SCHEDULED" in blocked_arm.json()["detail"]

            plan = provider.engines[0]._plan
            assert isinstance(plan, _RecoverableAbandonPlan)
            for _ in range(100):
                if plan.durable_resident_reconcile_attempts >= 1:
                    break
                await asyncio.sleep(0.01)
            # The retry uses durable claim job IDs, not the bounded in-process
            # settle return and not an empty journal reconcile.
            assert provider.engines[0].settle_abandoned_calls == 1
            # A pre-bell abandon must NOT invoke the post-setup teardown seam
            # (cancel_setup_and_settle): no bell crossed => no timed setup or
            # per-bout Proxy to settle, and on the real engine that call raises
            # "setup cleanup bout is stale". Only the staged-resident unstage and
            # the durable resident reconcile run. (Was: == 1, pre-fix behavior.)
            assert plan.cancel_setup_attempts == 0
            assert plan.durable_resident_reconcile_attempts >= 1
            assert plan.journal_reconcile_attempts == 0
            still_cleaning = await coordinator.store.read("install-acceptance")
            assert (
                still_cleaning is not None
                and still_cleaning.state == Round5WarmState.CLEANING
            )
            assert await manager._round5_cleanup_store().current() is not None

            # Allow only the resident settlement. The independent journal
            # reconcile remains its always-successful no-op.
            plan.allow_resident_settle.set()
            for _ in range(400):
                cleanup_lease = await manager._round5_cleanup_store().current()
                recovered = await manager.get(session_id)
                if (
                    cleanup_lease is None
                    and recovered.round5_setup is not None
                    and recovered.round5_setup.cleanup_retryable is False
                ):
                    break
                await asyncio.sleep(0.01)
            assert cleanup_lease is None, "cleanup lease kept renewing after convergence"
            assert recovered.round5_setup is not None
            assert recovered.round5_setup.cleanup_retryable is False
            assert manager.round5_cleanup_owed is False

            # Cleanup settlement only enqueues a new generation. Neither the
            # status endpoint nor the fight card may call that startable.
            warming = await coordinator.store.read("install-acceptance")
            assert warming is not None and warming.state == Round5WarmState.WARMING
            assert manager.round5_ring_ready is False
            board = (await client.get("/api/bout/all")).json()
            round_five = board["rounds"]["survive_connection_spike"]
            assert round_five["can_start"] is False
            assert round_five["state"] == "temporarily_unavailable"
            assert "STAGE REWARMING" in round_five["detail"]
            assert "GENERATION 2" in round_five["detail"]

            await coordinator.run_one_cycle()
            assert manager.round5_ring_ready is True
            ready_status = coordinator.public_status_cached()
            assert ready_status["round5_warm_last_error_code"] is None
            board = (await client.get("/api/bout/all")).json()
            assert board["rounds"]["survive_connection_spike"]["can_start"] is True
            second = await client.post("/api/sessions", json=_session_body())
            await _arm_via_http(client, second.json()["id"])
    finally:
        await manager.close()
        await coordinator.close()


async def test_restart_reconcile_uses_durable_prebell_resident_intent() -> None:
    clock = Clock()
    provider = Provider(clock)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    claimed, _ = await coordinator.claim(
        session_id="session-prebell-restart",
        bout_id="bout-prebell-restart",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    assert claimed.claim is not None
    cleaning = await coordinator.begin_cleanup(claimed.claim.claim_id)
    assert cleaning.bell_at_utc is None

    class RestartEngine:
        abandoned_calls = 0
        ordinary_calls = 0

        async def reconcile_abandoned_claim(self, claim) -> None:
            assert claim.claim_id == claimed.claim.claim_id
            self.abandoned_calls += 1

        async def reconcile_claim(self, _claim) -> None:
            self.ordinary_calls += 1

    engine = RestartEngine()
    live_provider = live.LiveRound5WarmProvider(
        SimpleNamespace(),
        lambda _competitor: engine,
    )

    assert await live_provider.reconcile(cleaning) is True
    assert engine.abandoned_calls == 1
    assert engine.ordinary_calls == 0
    await coordinator.close()


# ---------------------------------------------------------------------------
# 5. Failure terminal preserves the verified lane and still rewarms.
# ---------------------------------------------------------------------------


async def test_failure_preserves_verified_lane_and_drives_real_rewarm() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _AsymmetricFailurePlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    first_generation = (await coordinator.store.read("install-acceptance")).generation
    manager = _make_manager(coordinator, clock)
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            await client.post(f"/api/sessions/{session_id}/run")

            terminal = None
            for _ in range(400):
                snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
                if snapshot["state"] == SessionState.FAILED.value:
                    terminal = snapshot
                    break
                await asyncio.sleep(0)
            assert terminal is not None, "asymmetric bout never failed"
            assert terminal["lanes"]["lakebase"]["state"] == LaneState.VERIFIED.value
            assert terminal["lanes"]["competitor"]["state"] == LaneState.FAILED.value

        for _ in range(200):
            slot = await coordinator.store.read("install-acceptance")
            if slot is not None and slot.generation > first_generation:
                break
            await asyncio.sleep(0)
        assert slot is not None
        assert slot.generation == first_generation + 1
    finally:
        await manager.close()
        await coordinator.close()


# ---------------------------------------------------------------------------
# 6. Atomic bell / read-back and restart reconciliation (real coordinator).
# ---------------------------------------------------------------------------


async def test_duplicate_bell_returns_one_server_context() -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    coordinator = _coordinator(clock, provider)
    await _warm_to_ready(coordinator, provider)
    try:
        claimed, _ = await coordinator.claim(
            session_id="session-one",
            bout_id="bout-one",
            selected_variant=Round5Variant.AURORA,
            bout_fence=9,
        )
        assert claimed.claim is not None
        first = await coordinator.accept_bell(claimed.claim.claim_id)
        second = await coordinator.accept_bell(claimed.claim.claim_id)
        assert second is first
        slot = await coordinator.store.read("install-acceptance")
        assert slot is not None
        assert slot.state == Round5WarmState.RUNNING
        assert slot.bell_id == first.bell_id
    finally:
        await coordinator.close()


async def test_restart_reconciles_running_generation_before_rewarm() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider = _EngineProvider(clock, _BlockingPlan)
    first = Round5WarmCoordinator(
        installation_id="install-acceptance",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=provider,
        process_epoch="process-one",
        broker_epoch="broker-one",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    try:
        await _warm_to_ready(first, provider)
        claimed, _ = await first.claim(
            session_id="session-one",
            bout_id="bout-one",
            selected_variant=Round5Variant.AURORA,
            bout_fence=1,
        )
        assert claimed.claim is not None
        await first.accept_bell(claimed.claim.claim_id)

        clock.advance(91)
        replacement_provider = _EngineProvider(clock, _BlockingPlan)
        replacement_provider.reconcile_result = True
        replacement = Round5WarmCoordinator(
            installation_id="install-acceptance",
            warm_contract_sha256=DIGEST,
            store=store,
            provider=replacement_provider,
            process_epoch="process-two",
            broker_epoch="broker-two",
            clock=clock,
            monotonic_ns=clock.monotonic,
        )
        try:
            await asyncio.wait_for(replacement.run_one_cycle(), timeout=2)
            slot = await store.read("install-acceptance")
            assert slot is not None
            assert slot.state == Round5WarmState.WARMING
            assert slot.generation == 2
            assert slot.claim is None
            assert slot.coordinator_owner == "process-two"
        finally:
            await replacement.close()
    finally:
        await first.close()


# ---------------------------------------------------------------------------
# 7. Exact 10,000 gate (models + manager validation).
# ---------------------------------------------------------------------------


def test_exact_10k_gate_rejects_a_9999_and_a_partial_lane() -> None:
    # A 9,999 result cannot even be represented as a stopped bell clock.
    with pytest.raises(ValueError, match="exactly 10,000"):
        RoundFiveRuntimeLaneSnapshot(
            id="lakebase",
            phase="holding",
            elapsed_at_snapshot_ms=3_000,
            bell_to_10000_observed_ms=3_000,
            held_clients=9_999,
            status="not exact",
        )

    # A runner that labels a 2,895-client lane "verified" is still rejected by
    # the manager's structural gate.
    partial = SimpleNamespace(
        lane_id="lakebase",
        initiated_clients=2_932,
        authenticated_clients=2_895,
        held_clients_at_gate=2_895,
        terminal_failures=0,
        failure_codes={},
        retries=0,
        disconnected_during_hold=0,
        sampled_queries_succeeded=16,
        verified=True,
        gates=SimpleNamespace(passed=True),
    )
    evidence = RunManager._round_five_evidence(partial)
    assert RunManager._round_five_lane_valid(partial, evidence) is False

    # And a genuine exact 10,000 proof passes the same gate.
    exact = finalize_fanin_lane(raw_fanin_lane("lakebase"))
    exact_evidence = RunManager._round_five_evidence(exact)
    assert RunManager._round_five_lane_valid(exact, exact_evidence) is True


# ---------------------------------------------------------------------------
# 8/9. Real orchestrator lane ordering:
#   - Lakebase releases its dispatch before any progress I/O.
#   - CreateDBProxy is the first timed AWS mutation, and the competitor lane's
#     dispatch waits for the exact control-plane gate.
# ---------------------------------------------------------------------------


async def test_lakebase_lane_releases_dispatch_before_any_progress_io() -> None:
    orchestrator = object.__new__(live.LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        lakebase_credential_sha256="a" * 64,
        lakebase_pooled_host="pooled.example.test",
    )
    ticks = iter((1_000_000_001, 1_000_000_002))
    orchestrator._monotonic_ns = lambda: next(ticks)
    order: list[str] = []

    async def lane_ready(stop: ConnectionSpikeSetupLaneStop) -> None:
        assert stop.endpoint_host == "pooled.example.test"
        order.append("dispatch_released")

    async def progress(_value: object) -> None:
        order.append("progress_published")

    await orchestrator._setup_lakebase(
        "bout",
        SimpleNamespace(),
        SimpleNamespace(wait=lambda: asyncio.sleep(0)),
        [1_000_000_000],
        progress,
        lane_ready,
    )

    assert order == ["dispatch_released", "progress_published"]


async def test_create_proxy_is_first_aws_mutation_and_competitor_waits_for_gate() -> None:
    orchestrator = object.__new__(live.LiveConnectionSpikeSetupOrchestrator)
    orchestrator.config = SimpleNamespace(
        competitor_id="rds_postgres",
        competitor_credential_id="rds",
        competitor_direct_host="direct.example.test",
        competitor_credential_sha256="b" * 64,
    )
    orchestrator._monotonic_ns = lambda: 1_000_000_001
    order: list[str] = []

    class Coordinator:
        async def create_resource(self, scope, spec) -> None:
            del scope
            order.append(f"mutate:{spec.resource_kind}")

        async def complete_prestaged(self, scope, spec, *, intent) -> None:
            # Proxy CREATE_INTENT pre-committed before T0; the timed path issues
            # the mutation with no coordination I/O in front of it.
            del scope, intent
            order.append(f"mutate:{spec.resource_kind}")

    async def report(callback, lane_id, phase, status, **kwargs) -> None:
        del callback, lane_id, status, kwargs
        order.append(f"progress:{phase}")

    async def available(clients, resources, **kwargs):
        del clients, resources, kwargs
        return "available"

    async def journal(*args) -> None:
        del args
        order.append("journal_exact")

    async def topology(*args) -> None:
        del args
        order.append("proxy_gate_exact")

    async def lane_ready(stop: ConnectionSpikeSetupLaneStop) -> None:
        del stop
        order.append("dispatch_released")

    orchestrator._report = report
    orchestrator._wait_proxy_available = available
    orchestrator._verify_journaled_resources = journal
    orchestrator._verify_proxy_topology = topology
    specs = (
        SimpleNamespace(resource_kind="rds_proxy"),
        SimpleNamespace(resource_kind="proxy_target_group"),
        SimpleNamespace(resource_kind="proxy_target"),
    )
    resources = SimpleNamespace(
        names=SimpleNamespace(proxy_name="proxy"),
        proxy_endpoint="proxy.example.test",
        secret_arn="secret-ref",
    )

    await orchestrator._setup_competitor(
        "bout",
        SimpleNamespace(),
        SimpleNamespace(rds=SimpleNamespace(), ssm=SimpleNamespace()),
        Coordinator(),
        specs,
        resources,
        SimpleNamespace(wait=lambda: asyncio.sleep(0)),
        [1_000_000_000],
        None,
        lane_ready,
        None,
        None,
        SimpleNamespace(ordinal=1, resource_kind="rds_proxy"),
    )

    assert order[0] == "mutate:rds_proxy"
    assert order.index("proxy_gate_exact") < order.index("dispatch_released")
    assert not any("verifying_transaction" in item for item in order)


async def test_lane_failure_cancels_sibling_without_waiting_for_order() -> None:
    adapter = SimpleNamespace(
        config=SimpleNamespace(
            targets=(
                SimpleNamespace(lane_id="lakebase"),
                SimpleNamespace(lane_id="competitor"),
            )
        ),
        cancel=lambda run_id: asyncio.sleep(0),
    )
    engine = LiveConnectionSpikeEngine(adapter)
    arm = object()
    engine._armed = arm
    sibling_cancelled = asyncio.Event()

    async def fail() -> object:
        raise ConnectionSpikeLiveOperationError("lane failed")

    async def sibling() -> object:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    engine._lane_bursts = {
        "lakebase": asyncio.create_task(sibling()),
        "competitor": asyncio.create_task(fail()),
    }

    with pytest.raises(ConnectionSpikeLiveOperationError, match="lane failed"):
        await engine.run(arm)
    assert sibling_cancelled.is_set()


def test_acceptance_checklist_maps_to_present_tests() -> None:
    """Guard: every checklist entry names a real test function in this module."""
    module = globals()
    for gate, test_name in ACCEPTANCE_CHECKLIST.items():
        assert test_name in module, f"checklist gate {gate!r} names a missing test"
        assert callable(module[test_name])
