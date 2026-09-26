"""TEST-ONLY production-fidelity coverage for the final Round 5 test-gate gaps.

This module adds regression coverage the existing suites do not exercise, using
REAL production classes (``LiveConnectionSpikeEngine``, ``LiveRound5WarmProvider``,
``Round5WarmCoordinator``) wired through controlled boundary fakes -- never mocks
of the classes under test. No production source file is touched by this commit.

Gaps closed (see the accompanying report for full detail):

  1. The engine-level janitor-ownership guard on ``stop_and_begin_cleanup`` /
     ``stop_setup_and_begin_cleanup`` was only ever exercised indirectly (via the
     provider's adoption bookkeeping tests); nothing proved the guarded METHODS
     themselves suppress every mutation when already janitor-owned.
  2. ``_ensure_post_bell_provider_cleanup_started``'s two branches (completed vs.
     partial setup) were never driven through a REAL engine's real bind/setup,
     REAL provider adoption, and REAL coordinator cleanup convergence.
  3. The transfer-before-wake ordering inside ``Round5WarmCoordinator.begin_cleanup``
     was only proven against a hand-rolled provider double that overrides the
     transfer method itself; nothing proved it with the REAL provider's REAL
     ``transfer_adopted_engine_at_cleaning``.
  4. ``Round5WarmCoordinator.close()`` interacting with the real provider's
     adopted-engine registry had no coverage at all.
  5. The restart -> recovered-claim -> second-bout regression (test_i in
     ``test_round5_nobell_authority.py``) never drove the second bout past
     RUNNING to an actual verified game-session outcome.

Design note on "real coordinator + real provider": several tests below build a
``Round5WarmCoordinator`` with the lightweight, already-proven ``Provider`` test
double (from ``test_round5_warm``) ONLY to reach a durable CLAIMED/RUNNING slot
cheaply and deterministically. Immediately before the cleanup-convergence seam
under test, ``coord.provider`` is swapped to a REAL ``LiveRound5WarmProvider``
instance with the REAL engine already adopted into it. `Round5WarmCoordinator`
stores its provider as a plain mutable attribute and every method the tests
exercise after the swap (`begin_cleanup`, `converge_cleanup`, `run_one_cycle`,
`close`) reads `self.provider` fresh, so the swap is exactly as real as
constructing the coordinator with that provider from the start -- it just avoids
re-deriving the (separately, exhaustively tested) warm-to-READY provider
machinery inside this file.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from test_round5_warm import (  # shared, already-proven deterministic harness
    DIGEST,
    Clock,
    Provider,
    coordinator,
    warm_ready,
)

import server.connection_spike_live as live
from server.connection_spike_live import (
    ConnectionSpikeSetupLaneStop,
    ConnectionSpikeTarget,
    LiveConnectionSpikeEngine,
    LiveRound5WarmProvider,
)
from server.round5_warm import (
    InMemoryRound5WarmStore,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmState,
)

ACCOUNT = "123456789012"


# ---------------------------------------------------------------------------
# Shared fakes: controlled boundaries, never mocks of the classes under test.
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _accept_recording_adapter_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the two seams `run`'s dispatch path uses past the fake adapter's `{}`.

    Mirrors the same seam-level stub used by
    ``test_round5_lane_starts_on_its_own_stop.py`` -- it never touches the code
    under test (the engine's cleanup/reconcile/coordinator paths), only the
    unrelated lane-payload finalization a fake adapter's raw ``{}`` cannot satisfy.
    """

    monkeypatch.setattr(
        live,
        "_finalize_lane_payload",
        lambda arm, raw, *, lane_id, **kwargs: SimpleNamespace(lane_id=lane_id),
    )


def _target(lane_id: str, *, competitor_id: str = "") -> ConnectionSpikeTarget:
    return ConnectionSpikeTarget(
        lane_id=lane_id,
        secret_arn=(
            "" if lane_id == "lakebase" else f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:x"
        ),
        endpoint_host=f"{lane_id}-pooled.example.test",
        credential_host=f"{lane_id}-direct.example.test",
        competitor_id=competitor_id,
        competitor_target_id="sealed-instance" if competitor_id else "",
        competitor_resource_id="db-SEALED" if competitor_id else "",
        credential_sha256="c" * 64,
        observer_credential_sha256="d" * 64,
    )


class _StageRecorder:
    """Controlled boundary fake standing in for the resident transport."""

    def __init__(self) -> None:
        self.staged: list[object] = []

    async def stage(self, *, binding, request):
        del request
        self.staged.append(binding)


class _GateAdapter:
    """Controlled boundary fake standing in for ``LiveConnectionSpikeAdapter``.

    Implements exactly the surface ``LiveConnectionSpikeEngine`` calls on a lane
    adapter during bind/ARM/setup/dispatch/reconcile -- nothing more -- so the
    engine code under test runs for real against it.
    """

    def __init__(self) -> None:
        self.config = SimpleNamespace(
            targets=(_target("lakebase"), _target("competitor", competitor_id="rds_postgres")),
            runner_instance_type=live.FANIN_RUNNER_INSTANCE_TYPE,
            trust_bundle_sha256="a" * 64,
            resident_installation_id="install-gate",
            runner_harness_sha256="e" * 64,
        )
        self.prepared_boot_id = "gate-runner-boot"
        self._resident_process_boot_id = "gate-process-boot"
        self._resident_process_pid = 4242
        self._resident_transport = _StageRecorder()
        self.dispatched: list[str] = []
        self.cancel_job_calls: list[str] = []
        self.cancel_resident_calls: list[object] = []
        self.stage_prepared_release_calls: list[object] = []
        # Held open so a dispatched lane's job stays "active" (in
        # ``_active_run_ids``) until the test releases it -- the exact window
        # cleanup convergence must cancel through.
        self.release_execute = asyncio.Event()

    async def preflight_capacity(self, run_id, **digests):
        del run_id, digests
        return SimpleNamespace(
            sufficient=True,
            failures=(),
            boot_id=self.prepared_boot_id,
            runner_harness_sha256="e" * 64,
            model_sha256=live.fanin_capacity_model_sha256(),
            receipt={"boot_id": self.prepared_boot_id},
        )

    async def ensure_dispatch_capsule(self, context_id, **kwargs):
        del context_id, kwargs

    async def execute(self, run_id, request, *, targets=None, on_progress=None, **kwargs):
        del on_progress, kwargs
        self.dispatched.extend(str(target["lane_id"]) for target in request["targets"])
        await self.release_execute.wait()
        return {}

    execute_prepared = execute

    def settlement_pending(self, run_id):
        del run_id
        return False

    async def cancel_job(self, job_id):
        self.cancel_job_calls.append(job_id)

    async def cancel_resident(self, *, binding=None, **kwargs):
        self.cancel_resident_calls.append(binding if binding is not None else kwargs)

    async def stage_prepared_release(self, *, binding, request):
        del request
        self.stage_prepared_release_calls.append(binding)


class _CompletingOrchestrator:
    """Controlled boundary fake: both lanes reach their setup stop (completed setup)."""

    def __init__(self) -> None:
        self.begin_cleanup_calls: list[str] = []
        self.prove_bout_absent_calls: list[str] = []
        self.unresolved_ids: tuple[str, ...] = ()

    async def prepare(self, bout_id, fencing_token):
        del bout_id, fencing_token

    async def setup(
        self,
        bout_id,
        fencing_token,
        on_progress=None,
        on_lane_ready=None,
        on_lane_stage=None,
    ):
        del fencing_token, on_progress
        competitor_stop = ConnectionSpikeSetupLaneStop(
            lane_id="competitor",
            launched_ns=1_000,
            stopped_ns=2_000_000,
            credential_sha256="c" * 64,
            endpoint_host="competitor-pooled.example.test",
            secret_arn=f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:x",
        )
        assert on_lane_stage is not None
        await on_lane_stage(competitor_stop)
        lakebase_stop = ConnectionSpikeSetupLaneStop(
            lane_id="lakebase",
            launched_ns=1_000,
            stopped_ns=3_400_000_000,
            credential_sha256="c" * 64,
            endpoint_host="lakebase-pooled.example.test",
        )
        assert on_lane_ready is not None
        await on_lane_ready(lakebase_stop)
        await on_lane_ready(competitor_stop)
        return SimpleNamespace(bout_id=bout_id)

    async def begin_cleanup(self, bout_id):
        self.begin_cleanup_calls.append(bout_id)

    async def unresolved_bout_ids(self):
        return self.unresolved_ids

    async def prove_bout_absent(self, bout_id):
        self.prove_bout_absent_calls.append(bout_id)


class _StallingOrchestrator:
    """Controlled boundary fake: competitor is staged, then setup never returns.

    Models a real bout shape: the Lakebase resident is staged at ARM (via
    ``engine.prepare``) and the competitor's late-bound Proxy request is staged
    behind its ready gate (``on_lane_stage``), but neither lane's setup STOP ever
    arrives -- e.g. a refused bell that must tear the in-flight setup down.
    """

    def __init__(self) -> None:
        self.begin_cleanup_calls: list[str] = []
        self.prove_bout_absent_calls: list[str] = []
        self.unresolved_ids: tuple[str, ...] = ()
        self.competitor_staged = asyncio.Event()

    async def prepare(self, bout_id, fencing_token):
        del bout_id, fencing_token

    async def setup(
        self,
        bout_id,
        fencing_token,
        on_progress=None,
        on_lane_ready=None,
        on_lane_stage=None,
    ):
        del bout_id, fencing_token, on_progress, on_lane_ready
        competitor_stop = ConnectionSpikeSetupLaneStop(
            lane_id="competitor",
            launched_ns=1_000,
            stopped_ns=2_000_000,
            credential_sha256="c" * 64,
            endpoint_host="competitor-pooled.example.test",
            secret_arn=f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:x",
        )
        assert on_lane_stage is not None
        await on_lane_stage(competitor_stop)
        self.competitor_staged.set()
        # Blocks until the setup task is cancelled by cleanup convergence.
        await asyncio.Event().wait()

    async def begin_cleanup(self, bout_id):
        self.begin_cleanup_calls.append(bout_id)

    async def unresolved_bout_ids(self):
        return self.unresolved_ids

    async def prove_bout_absent(self, bout_id):
        self.prove_bout_absent_calls.append(bout_id)


def _unused_engine_factory(_competitor_id):
    raise AssertionError(
        "the adopted engine must be reused; the provider's engine_factory "
        "fallback must never run for a claim that was adopted"
    )


async def _claimed_running(clock: Clock, *, bout_id: str, bout_fence: int, session_id: str):
    """Reach a durable RUNNING slot (real accept-bell) via the proven test harness."""

    warm_provider = Provider(clock)
    coord = coordinator(clock, warm_provider)
    await warm_ready(coord, warm_provider)
    claimed, _capsule = await coord.claim(
        session_id=session_id,
        bout_id=bout_id,
        selected_variant=Round5Variant.AURORA,
        bout_fence=bout_fence,
    )
    assert claimed.claim is not None
    claim = claimed.claim
    await coord.accept_bell(claim.claim_id)
    running = await coord.store.read(coord.installation_id)
    assert running is not None and running.state == Round5WarmState.RUNNING
    return coord, claim


def _swap_in_real_provider(
    coord: Round5WarmCoordinator, engine: object, claim_id: str
) -> LiveRound5WarmProvider:
    """Install a REAL ``LiveRound5WarmProvider`` with ``engine`` adopted.

    Mirrors ``Round5WarmCoordinator.__init__``'s own authority-guard wiring so the
    swapped-in provider is exactly as real as one supplied at construction.
    """

    provider = LiveRound5WarmProvider(SimpleNamespace(), _unused_engine_factory)
    provider.authority_guard = coord._authority_guard
    provider.adopt_claimed_engine(claim_id, engine)
    coord.provider = provider
    return provider


# ===========================================================================
# 1. Engine-level janitor-ownership guard: real engine, both guarded methods.
# ===========================================================================


class _Item1Adapter:
    def __init__(self) -> None:
        self.cancel_resident_calls: list[object] = []

    async def cancel_resident(self, **kwargs):
        self.cancel_resident_calls.append(kwargs)

    def settlement_pending(self, run_id):
        del run_id
        return False


class _Item1Orchestrator:
    def __init__(self) -> None:
        self.begin_cleanup_calls: list[str] = []

    async def begin_cleanup(self, bout_id):
        self.begin_cleanup_calls.append(bout_id)


async def test_stop_and_begin_cleanup_is_fully_suppressed_when_janitor_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_round5_cleanup_janitor_owned=True`` must suppress every effect of
    ``stop_and_begin_cleanup``: no cleanup task, no lane cancellation, no
    orchestrator mutation, and no other engine-state mutation.

    Mutation check (performed separately, not committed): deleting the
    ``if getattr(self, "_round5_cleanup_janitor_owned", False): return`` guard in
    ``stop_and_begin_cleanup`` makes this test fail, because the suppressed
    effects below would then all occur.
    """

    adapter = _Item1Adapter()
    orchestrator = _Item1Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    arm = object()
    engine._armed = arm
    engine._active_run_ids = {"lakebase": "runner-one", "competitor": "runner-two"}
    engine._setup_result = SimpleNamespace(bout_id="bout-owned")
    engine._round5_cleanup_janitor_owned = True

    created_task_names: list[str | None] = []
    original_create_task = asyncio.create_task

    def _spy_create_task(coro, **kwargs):
        created_task_names.append(kwargs.get("name"))
        return original_create_task(coro, **kwargs)

    monkeypatch.setattr(live.asyncio, "create_task", _spy_create_task)

    before_active = dict(engine._active_run_ids)
    before_setup_result = engine._setup_result

    await engine.stop_and_begin_cleanup(arm)

    assert created_task_names == [], "no cleanup task must be created while janitor-owned"
    assert adapter.cancel_resident_calls == [], "no lane cancellation while janitor-owned"
    assert orchestrator.begin_cleanup_calls == [], "no orchestrator mutation while janitor-owned"
    assert engine._active_run_ids == before_active, "no external mutation while janitor-owned"
    assert engine._setup_result is before_setup_result
    assert engine._cleanup_bout_id is None


async def test_stop_and_begin_cleanup_contrast_runs_for_real_when_not_janitor_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contrast case proving the harness above is wired correctly: with the flag
    unset (production default), the exact same engine state DOES drive a real
    cleanup task, lane cancellation, and orchestrator mutation."""

    adapter = _Item1Adapter()
    orchestrator = _Item1Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    arm = object()
    engine._armed = arm
    engine._active_run_ids = {"lakebase": "runner-one", "competitor": "runner-two"}
    engine._setup_result = SimpleNamespace(bout_id="bout-owned")
    # _round5_cleanup_janitor_owned intentionally left unset (falsy default).

    created_task_names: list[str | None] = []
    original_create_task = asyncio.create_task

    def _spy_create_task(coro, **kwargs):
        created_task_names.append(kwargs.get("name"))
        return original_create_task(coro, **kwargs)

    monkeypatch.setattr(live.asyncio, "create_task", _spy_create_task)

    await engine.stop_and_begin_cleanup(arm)

    assert created_task_names == ["round5-engine-cleanup-start"]
    assert {tuple(sorted(c.items())) for c in adapter.cancel_resident_calls} == {
        (("generation", 0), ("job_id", "runner-one"), ("lane_id", "lakebase")),
        (("generation", 0), ("job_id", "runner-two"), ("lane_id", "competitor")),
    }
    assert orchestrator.begin_cleanup_calls == ["bout-owned"]
    assert engine._cleanup_bout_id == "bout-owned"
    assert engine._setup_result is None


async def test_stop_setup_and_begin_cleanup_is_fully_suppressed_when_janitor_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same suppression contract as above, for the setup-teardown guarded method,
    including staged residents (which only this method ever settles).

    Mutation check (performed separately, not committed): deleting the guard in
    ``stop_setup_and_begin_cleanup`` makes this test fail.
    """

    adapter = _Item1Adapter()
    orchestrator = _Item1Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    engine._setup_bout_id = "bout-owned-setup"
    staged_binding = SimpleNamespace(lane_id="lakebase", job_id="staged-job-one")
    engine._resident_bindings = {"lakebase": staged_binding}
    engine._round5_cleanup_janitor_owned = True

    created_task_names: list[str | None] = []
    original_create_task = asyncio.create_task

    def _spy_create_task(coro, **kwargs):
        created_task_names.append(kwargs.get("name"))
        return original_create_task(coro, **kwargs)

    monkeypatch.setattr(live.asyncio, "create_task", _spy_create_task)

    before_bindings = dict(engine._resident_bindings)

    await engine.stop_setup_and_begin_cleanup("bout-owned-setup")

    assert created_task_names == [], "no cleanup task must be created while janitor-owned"
    assert adapter.cancel_resident_calls == [], "no staged-resident settlement while janitor-owned"
    assert orchestrator.begin_cleanup_calls == [], "no orchestrator mutation while janitor-owned"
    assert engine._resident_bindings == before_bindings
    assert engine._cleanup_bout_id is None


async def test_stop_setup_and_begin_cleanup_contrast_runs_for_real_when_not_janitor_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _Item1Adapter()
    orchestrator = _Item1Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    engine._setup_bout_id = "bout-owned-setup"
    staged_binding = SimpleNamespace(lane_id="lakebase", job_id="staged-job-one")
    engine._resident_bindings = {"lakebase": staged_binding}
    # _round5_cleanup_janitor_owned intentionally left unset.

    created_task_names: list[str | None] = []
    original_create_task = asyncio.create_task

    def _spy_create_task(coro, **kwargs):
        created_task_names.append(kwargs.get("name"))
        return original_create_task(coro, **kwargs)

    monkeypatch.setattr(live.asyncio, "create_task", _spy_create_task)

    await engine.stop_setup_and_begin_cleanup("bout-owned-setup")

    assert created_task_names == ["round5-engine-setup-cleanup-start-bout-owned-setup"]
    assert adapter.cancel_resident_calls == [{"binding": staged_binding}]
    assert engine._resident_bindings == {}
    assert orchestrator.begin_cleanup_calls == ["bout-owned-setup"]
    assert engine._cleanup_bout_id == "bout-owned-setup"


# ===========================================================================
# 2. _ensure_post_bell_provider_cleanup_started: completed vs. partial setup,
#    driven through a REAL engine bind/setup, REAL provider adoption, and REAL
#    coordinator -> provider.reconcile -> engine.reconcile_claim.
# ===========================================================================


async def test_completed_setup_branch_teardown_is_exact_via_real_stack() -> None:
    clock = Clock()
    coord, claim = await _claimed_running(
        clock, bout_id="bout-gate-complete", bout_fence=5, session_id="session-gate-complete"
    )

    adapter = _GateAdapter()
    orchestrator = _CompletingOrchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)

    engine.bind_claim(claim)
    engine._armed = await engine.check()
    await engine.prepare(claim.bout_id, claim.bout_fence)
    assert "lakebase" in engine._resident_bindings
    setup_result = await engine.setup(claim.bout_id, claim.bout_fence)
    assert engine._setup_result is setup_result
    assert engine._setup_bout_id == claim.bout_id
    assert set(engine._active_run_ids) == {"lakebase", "competitor"}

    provider = _swap_in_real_provider(coord, engine, claim.claim_id)

    await coord.begin_cleanup(claim.claim_id)
    assert engine._round5_cleanup_janitor_owned is True
    warmed = await coord.converge_cleanup(claim.claim_id)

    # Exact setup teardown (completed-setup branch: _stop_and_begin_cleanup_once).
    assert orchestrator.begin_cleanup_calls == [claim.bout_id]
    assert orchestrator.prove_bout_absent_calls == [claim.bout_id]
    assert engine._setup_result is None
    assert engine._cleanup_bout_id == claim.bout_id
    assert engine._active_run_ids == {}
    cancelled_lanes = {
        binding.lane_id
        for binding in adapter.cancel_resident_calls
        if hasattr(binding, "lane_id")
    }
    assert cancelled_lanes == {"lakebase", "competitor"}
    assert set(adapter.cancel_job_calls) == {claim.lakebase_job_id, claim.competitor_job_id}

    # Provider released the claim on successful convergence; coordinator rewarmed.
    assert claim.claim_id not in provider._adopted_engines
    assert warmed.state == Round5WarmState.WARMING
    assert warmed.generation == claim.capsule_generation + 1

    adapter.release_execute.set()
    await asyncio.gather(*engine._lane_bursts.values(), return_exceptions=True)


async def test_partial_setup_branch_settles_staged_residents_via_real_stack() -> None:
    clock = Clock()
    coord, claim = await _claimed_running(
        clock, bout_id="bout-gate-partial", bout_fence=6, session_id="session-gate-partial"
    )

    adapter = _GateAdapter()
    orchestrator = _StallingOrchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)

    engine.bind_claim(claim)
    engine._armed = await engine.check()
    await engine.prepare(claim.bout_id, claim.bout_fence)
    lakebase_binding = engine._resident_bindings["lakebase"]

    setup_task = asyncio.create_task(engine.setup(claim.bout_id, claim.bout_fence))
    await asyncio.wait_for(orchestrator.competitor_staged.wait(), timeout=2)
    # Actual setup_result/setup_bout_id: legitimately mid-flight, not hand-assigned.
    assert engine._setup_bout_id == claim.bout_id
    assert engine._setup_result is None
    assert set(engine._resident_bindings) == {"lakebase", "competitor"}
    competitor_binding = engine._resident_bindings["competitor"]
    # Staging the competitor's late-bound Proxy request pre-registers its job id
    # for cancellation purposes even though the lane was never dispatched; the
    # Lakebase resident staged at ARM (via `prepare`) carries no such entry yet.
    assert engine._active_run_ids == {"competitor": competitor_binding.job_id}

    provider = _swap_in_real_provider(coord, engine, claim.claim_id)

    await coord.begin_cleanup(claim.claim_id)
    assert engine._round5_cleanup_janitor_owned is True
    warmed = await coord.converge_cleanup(claim.claim_id)

    # The in-flight setup task was cancelled by cleanup convergence.
    assert setup_task.cancelled()

    # Staged resident settlement (partial-setup branch only): both staged
    # bindings -- lakebase from ARM, competitor from the late-bound Proxy stage --
    # were cancelled through the adapter, using the exact bindings.
    assert adapter.cancel_resident_calls == [lakebase_binding, competitor_binding]
    assert engine._resident_bindings == {}

    # Exact setup teardown (partial-setup branch: _stop_setup_and_begin_cleanup_once).
    assert orchestrator.begin_cleanup_calls == [claim.bout_id]
    assert orchestrator.prove_bout_absent_calls == [claim.bout_id]
    assert engine._setup_result is None
    assert engine._cleanup_bout_id == claim.bout_id
    assert engine._setup_task is None

    assert claim.claim_id not in provider._adopted_engines
    assert warmed.state == Round5WarmState.WARMING
    assert warmed.generation == claim.capsule_generation + 1


async def test_removing_ensure_post_bell_cleanup_call_fails_this_suite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutation probe for item 2: if ``reconcile_claim`` stopped calling
    ``_ensure_post_bell_provider_cleanup_started``, setup teardown would never
    happen. Simulated here (rather than editing the source file) by patching the
    call out on the engine instance, proving the completed-setup test above would
    catch that regression."""

    clock = Clock()
    coord, claim = await _claimed_running(
        clock, bout_id="bout-gate-mutation", bout_fence=7, session_id="session-gate-mutation"
    )
    adapter = _GateAdapter()
    orchestrator = _CompletingOrchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    engine.bind_claim(claim)
    engine._armed = await engine.check()
    await engine.prepare(claim.bout_id, claim.bout_fence)
    await engine.setup(claim.bout_id, claim.bout_fence)
    assert engine._setup_result is not None

    async def _removed_guard(_claim):
        return None

    monkeypatch.setattr(engine, "_ensure_post_bell_provider_cleanup_started", _removed_guard)

    provider = _swap_in_real_provider(coord, engine, claim.claim_id)
    await coord.begin_cleanup(claim.claim_id)
    await coord.converge_cleanup(claim.claim_id)

    # With the call removed, teardown never happens: this is exactly the failure
    # the completed-setup test's assertions surface (the orchestrator is never
    # torn down). Reconstructive provider cleanup may still clear the local
    # result cache; that is not evidence the active setup was stopped.
    assert orchestrator.begin_cleanup_calls == []
    del provider

    adapter.release_execute.set()
    await asyncio.gather(*engine._lane_bursts.values(), return_exceptions=True)


# ===========================================================================
# 3. Transfer-before-wake ordering: real coordinator + real provider + real
#    engine. Wake is instrumented; the transfer method itself is never touched.
# ===========================================================================


class _Item3Orchestrator:
    def __init__(self) -> None:
        self.unresolved_ids: tuple[str, ...] = ()

    async def unresolved_bout_ids(self):
        return self.unresolved_ids

    async def prove_bout_absent(self, bout_id):
        del bout_id


class _Item3Adapter:
    def __init__(self) -> None:
        self.cancel_job_calls: list[str] = []

    async def cancel_job(self, job_id):
        self.cancel_job_calls.append(job_id)


async def test_transfer_precedes_wake_with_real_provider_and_real_engine() -> None:
    """``begin_cleanup`` must establish exclusive janitor ownership of the
    adopted engine BEFORE waking the supervised loop, using the REAL provider's
    REAL ``transfer_adopted_engine_at_cleaning`` (never overridden here) and a
    REAL engine. Wake is observed by wrapping the coordinator's own
    ``asyncio.Event.set``, not by touching the transfer method.

    Swapping the two statements in ``begin_cleanup`` (waking before transferring)
    makes the assertion below deterministically fail, because the flag would
    still read ``False`` at the instant ``.set()`` fires.
    """

    clock = Clock()
    coord, claim = await _claimed_running(
        clock, bout_id="bout-race-real", bout_fence=8, session_id="session-race-real"
    )

    engine = LiveConnectionSpikeEngine(_Item3Adapter(), setup_orchestrator=_Item3Orchestrator())
    engine.bind_claim(claim)

    provider = _swap_in_real_provider(coord, engine, claim.claim_id)

    observed_owned_at_wake: list[bool] = []
    original_set = coord._wake.set

    def _instrumented_set():
        observed_owned_at_wake.append(bool(getattr(engine, "_round5_cleanup_janitor_owned", False)))
        return original_set()

    coord._wake.set = _instrumented_set

    await coord.begin_cleanup(claim.claim_id)

    assert observed_owned_at_wake == [True], (
        "the engine must already be transferred at the exact instant the "
        "supervised loop is woken"
    )

    # Belt-and-suspenders: race the manager-driven convergence against the
    # supervised loop for the SAME CLEANING slot; exactly one reconcile must run,
    # on the adopted (already-transferred) real engine.
    reconcile_calls: list[bool] = []
    original_reconcile_claim = engine.reconcile_claim

    async def _spy_reconcile_claim(claim_arg):
        reconcile_calls.append(bool(engine._round5_cleanup_janitor_owned))
        return await original_reconcile_claim(claim_arg)

    engine.reconcile_claim = _spy_reconcile_claim

    warmed_a, warmed_b = await asyncio.gather(
        coord.converge_cleanup(claim.claim_id),
        coord.run_one_cycle(),
    )

    assert reconcile_calls == [True]
    assert claim.claim_id not in provider._adopted_engines
    final = warmed_a if warmed_a.state == Round5WarmState.WARMING else warmed_b
    assert final.state == Round5WarmState.WARMING
    assert final.generation == claim.capsule_generation + 1


# ===========================================================================
# 4. Coordinator.close with a real provider: target shutdown-registry contract.
# ===========================================================================


class _Item4Adapter:
    def __init__(self) -> None:
        self.release_cancel_job = asyncio.Event()
        self.cancel_job_started = asyncio.Event()
        self.cancel_job_calls: list[str] = []

    async def cancel_job(self, job_id):
        self.cancel_job_calls.append(job_id)
        self.cancel_job_started.set()
        await self.release_cancel_job.wait()


class _Item4Orchestrator:
    def __init__(self) -> None:
        self.unresolved_ids: tuple[str, ...] = ()

    async def unresolved_bout_ids(self):
        return self.unresolved_ids

    async def prove_bout_absent(self, bout_id):
        del bout_id


async def test_close_clears_idle_adopted_engines_and_never_mutates_registry_after_store_close() -> (
    None
):
    """Already-true contract, now covered: entries with no in-flight work at
    close time are released, and that release happens strictly before the
    durable store is closed (never after)."""

    clock = Clock()
    warm_provider = Provider(clock)
    coord = coordinator(clock, warm_provider)
    await warm_ready(coord, warm_provider)

    provider = LiveRound5WarmProvider(SimpleNamespace(), _unused_engine_factory)
    provider.adopt_claimed_engine("idle-claim-a", SimpleNamespace())
    provider.adopt_claimed_engine("idle-claim-b", SimpleNamespace())
    coord.provider = provider

    call_order: list[str] = []
    original_release_all = provider.release_all_adopted_engines

    def _spy_release_all():
        call_order.append("release_all_adopted_engines")
        return original_release_all()

    provider.release_all_adopted_engines = _spy_release_all

    original_store_close = coord.store.close

    async def _spy_store_close():
        call_order.append("store.close")
        await original_store_close()

    coord.store.close = _spy_store_close

    await coord.close()

    assert provider._adopted_engines == {}
    assert call_order == ["release_all_adopted_engines", "store.close"], (
        "no registry mutation may happen after the durable store is closed"
    )


async def test_close_does_not_prematurely_clear_a_claim_with_in_flight_cleanup() -> None:
    """Landed shutdown contract (Finding B): an adopted engine whose claim is
    CURRENTLY CLEANING with an in-flight provider.reconcile must NOT be released out
    from under that reconcile, and the store must NOT be closed while it runs.
    close() keeps the fence alive and WAITS for the reconcile to actually finish,
    then releases adoption + closes the store. (Previously an xfail documenting the
    then-unbuilt fix; the fix has landed so it is now a hard assertion.)"""

    clock = Clock()
    coord, claim = await _claimed_running(
        clock, bout_id="bout-close-inflight", bout_fence=9, session_id="session-close-inflight"
    )

    adapter = _Item4Adapter()
    orchestrator = _Item4Orchestrator()
    engine = LiveConnectionSpikeEngine(adapter, setup_orchestrator=orchestrator)
    engine.bind_claim(claim)

    provider = _swap_in_real_provider(coord, engine, claim.claim_id)
    await coord.begin_cleanup(claim.claim_id)
    assert engine._round5_cleanup_janitor_owned is True

    converge_task = asyncio.create_task(coord.converge_cleanup(claim.claim_id))
    await asyncio.wait_for(adapter.cancel_job_started.wait(), timeout=2)

    # The claim is CURRENTLY CLEANING with a live reconcile in flight.
    slot = await coord.store.read(coord.installation_id)
    assert slot is not None and slot.state == Round5WarmState.CLEANING

    # close() must BLOCK while the shielded-AWS-modelling reconcile runs: it holds the
    # fence and does not clear adoption or revoke authority prematurely.
    close_task = asyncio.create_task(coord.close())
    for _ in range(5):
        await asyncio.sleep(0)
        if coord._closed:
            break
    assert coord._closed is True
    assert not close_task.done()
    assert claim.claim_id in provider._adopted_engines
    assert coord._authority_revoked is False

    # The in-flight reconcile finishes; only then does close complete.
    adapter.release_cancel_job.set()
    await asyncio.wait_for(close_task, timeout=2)
    warmed = await asyncio.wait_for(converge_task, timeout=2)
    assert warmed.state == Round5WarmState.WARMING
    assert claim.claim_id not in provider._adopted_engines
    assert coord._authority_revoked is True


# ===========================================================================
# 5. Mandatory second bout, extended through a verified game-session outcome.
# ===========================================================================


async def test_restart_recovered_claim_then_second_bout_reaches_verified() -> None:
    """Extends ``test_i_restart_while_claimed_recovers_exact_jobs_then_second_bout``
    (test_round5_nobell_authority.py) past durable RUNNING: the SECOND bout, armed
    and rung on the RECOVERED process after a real restart, is driven through the
    real ``RunManager``/ASGI control surface all the way to
    ``SessionState.VERIFIED``.
    """

    from httpx import ASGITransport, AsyncClient
    from test_round5_pre_deploy_acceptance import (
        _arm_via_http,
        _asgi,
        _EngineProvider,
        _make_manager,
        _session_body,
        _SuccessfulPlan,
    )

    from server.models import SessionState

    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider_a = _EngineProvider(clock, _SuccessfulPlan)
    process_a = Round5WarmCoordinator(
        installation_id="install-acceptance",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=provider_a,
        process_epoch="process-a",
        broker_epoch="broker-acceptance",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    provider_a.release_prepare.set()
    await asyncio.wait_for(process_a.run_one_cycle(), timeout=2)
    assert process_a.ring_ready

    manager_a = _make_manager(process_a, clock)
    app_a = _asgi(manager_a)
    async with AsyncClient(
        transport=ASGITransport(app=app_a), base_url="http://anti-demo.test"
    ) as client_a:
        created = await client_a.post("/api/sessions", json=_session_body())
        session_id = created.json()["id"]
        await _arm_via_http(client_a, session_id)
    await manager_a.close()

    first_slot = await store.read("install-acceptance")
    assert first_slot is not None and first_slot.claim is not None
    first_claim = first_slot.claim
    generation = first_slot.generation

    # Restart: a fresh process on the same durable store; the bell was never rung.
    clock.advance(process_a._coordinator_ttl.total_seconds() + 1)
    provider_b = _EngineProvider(clock, _SuccessfulPlan)
    process_b = Round5WarmCoordinator(
        installation_id="install-acceptance",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=provider_b,
        process_epoch="process-b",
        broker_epoch="broker-acceptance",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await process_b.run_one_cycle()

    recovered = await store.read("install-acceptance")
    assert recovered is not None
    assert recovered.state == Round5WarmState.WARMING
    assert recovered.generation == generation + 1

    provider_b.prepare_started.clear()
    provider_b.release_prepare.set()
    await asyncio.wait_for(process_b.run_one_cycle(), timeout=2)
    assert process_b.ring_ready

    manager_b = _make_manager(process_b, clock)
    app_b = _asgi(manager_b)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app_b), base_url="http://anti-demo.test"
        ) as client_b:
            second = await client_b.post("/api/sessions", json=_session_body())
            second_session_id = second.json()["id"]
            await _arm_via_http(client_b, second_session_id)

            # The mandatory second bout: a genuinely different claim than the
            # abandoned first one, arming for real on the recovered process.
            second_slot = await store.read("install-acceptance")
            assert second_slot is not None and second_slot.claim is not None
            assert second_slot.claim.claim_id != first_claim.claim_id

            await client_b.post(f"/api/sessions/{second_session_id}/run")
            result: dict[str, object] = {}
            for _ in range(400):
                result = (await client_b.get(f"/api/sessions/{second_session_id}")).json()
                if result["state"] == SessionState.VERIFIED.value:
                    break
                await asyncio.sleep(0)
            assert result.get("state") == SessionState.VERIFIED.value
    finally:
        await manager_b.close()
        await process_b.close()
