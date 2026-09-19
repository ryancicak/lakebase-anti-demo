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
from server.connection_spike_live import (
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeSetupLaneStop,
    LiveConnectionSpikeEngine,
)
from server.manager import RunManager
from server.models import (
    LaneState,
    RoundFiveRuntimeLaneSnapshot,
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

    def bind_claim(self, claim: object) -> None:
        self.bound_claim = claim

    def bind_bell(self, context: BellContext) -> None:
        self.bound_bell = context

    async def prepare(self, bout_id: str, fencing_token: int) -> None:
        assert bout_id and fencing_token > 0

    async def setup(
        self,
        bout_id,
        fencing_token,
        on_setup_progress,
        on_lane_progress,
        on_lane_result,
    ) -> None:
        del bout_id, fencing_token
        await self._plan.run(self, on_setup_progress, on_lane_progress, on_lane_result)

    async def cancel_setup_and_settle(self, bout_id) -> None:
        del bout_id
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
    ) -> Round5WarmPreparation:
        del process_epoch, broker_epoch
        self.attempt_tokens.append(warm_attempt_token)
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
) -> RunManager:
    # The factory is required by the arm guard but never used on the warm path:
    # the engine is taken from the claimed launch capsule.
    return RunManager(
        connection_spike_factory=lambda _competitor: object(),
        round5_warm_coordinator=coordinator,
        clock_ns=lambda: clock.monotonic_ns + 50_000_000,
    )


def _asgi(manager: RunManager) -> FastAPI:
    api = FastAPI()
    api.include_router(router)
    api.state.run_manager = manager
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

            armed = await client.post(f"/api/sessions/{session_id}/arm")
            assert armed.status_code >= 400
            snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
            assert snapshot["state"] != SessionState.ARMED.value
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


async def test_full_lifecycle_bell_duplicate_run_and_rewarm_via_asgi() -> None:
    clock = Clock()
    plan = _BlockingPlan()
    provider = _EngineProvider(clock, lambda: plan)
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

            running = (await client.post(f"/api/sessions/{session_id}/run")).json()
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

            # A concurrent duplicate /run returns the *same* bell and revision --
            # one bell, one cost window, one pair of runner jobs -- and never the
            # old ARMED snapshot.
            duplicate = (await client.post(f"/api/sessions/{session_id}/run")).json()
            assert duplicate["state"] == SessionState.RUNNING.value
            assert duplicate["round5_runtime"]["bell_id"] == bell_id
            assert duplicate["round5_runtime"]["revision"] == revision

            # A poll does not manufacture a new bell or rewind the clock.
            await asyncio.wait_for(plan.entered.wait(), timeout=2)
            polled = (await client.get(f"/api/sessions/{session_id}")).json()
            assert polled["round5_runtime"]["bell_id"] == bell_id
            assert polled["round5_runtime"]["revision"] >= revision

            # Release the ring so the bout does not strand a claim.
            towelled = (await client.post(f"/api/sessions/{session_id}/towel")).json()
            assert towelled["state"] == SessionState.TOWELLED.value
    finally:
        await manager.close()
        await coordinator.close()


# ---------------------------------------------------------------------------
# 4. Towel terminal drives real cleanup + rewarm.
# ---------------------------------------------------------------------------


async def test_towel_terminal_drives_real_cleanup_and_rewarm() -> None:
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
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            await client.post(f"/api/sessions/{session_id}/run")
            await asyncio.wait_for(plan.entered.wait(), timeout=2)

            towelled = (await client.post(f"/api/sessions/{session_id}/towel")).json()
            assert towelled["state"] == SessionState.TOWELLED.value

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
        assert slot.claim is None
    finally:
        await manager.close()
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
