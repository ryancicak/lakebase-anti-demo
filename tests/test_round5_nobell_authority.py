"""Round 5 no-bell authority regression tests (A-J).

These reproduce and lock down the live no-bell wedge (Case A, gen17):

  * An actively-arming claim was dropped ~14s before its arm deadline because the
    capsule's dispatch-credential *launch margin* had lapsed while CLAIMED, even
    though the runner identity was intact and the operator was mid-arm.
  * ``release_expired_claim`` then normalized CLAIMED -> WARMING, dropping the
    durable claim, so a rewarm ran over the staged ARM resident
    (``resident readiness binding changed`` -> terminal ``warm_baseline_unexpected``)
    and the manager's own ``begin_cleanup`` lost the claim
    (``WarmFenceLostError`` loop).

The fix moves claim-renewal to capsule IDENTITY (``_capsule_belongs``), fences an
expired claim into CLEANING with the claim retained (never READY/WARMING), cancels
a held provider op when the coordinator fence is lost, and classifies a resident
binding change during a cleanup transition as retryable rather than terminal.

The shared fake harness (deterministic Clock, fake Provider, coordinator factory)
is reused from ``test_round5_warm`` so these tests use the same CAS-accurate
in-memory store and injected clock -- no real sleeps, no AWS, no restart.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

# Real-RunManager harness (blocker 8): drive the production expiry/cleanup path,
# not tautological direct coordinator calls.
from test_round5_pre_deploy_acceptance import (
    _arm_via_http,
    _asgi,
    _EngineProvider,
    _make_manager,
    _RecoverableAbandonPlan,
    _session_body,
    _warm_to_ready,
)
from test_round5_pre_deploy_acceptance import (
    _coordinator as _accept_coordinator,
)
from test_round5_warm import (  # shared deterministic harness
    DIGEST,
    Clock,
    Provider,
    coordinator,
    preparation,
    warm_ready,
)

from server.connection_spike_live import (
    LiveConnectionSpikeEngine,
    LiveRound5WarmProvider,
)
from server.manager import InvalidStateError, RunManager
from server.models import SessionState
from server.round5_control import Round5ResidentBindingChangedError
from server.round5_warm import (
    DEFAULT_CLAIM_TTL_SECONDS,
    BlockedWarmError,
    InMemoryRound5WarmStore,
    RetryableWarmError,
    Round5BoutClaim,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmSlot,
    Round5WarmState,
    WarmClaimUnavailableError,
    WarmCoordinatorHeldError,
    WarmFenceLostError,
    WarmStoreConflictError,
    _blocked_is_terminal,
    _slot_from_json,
    _to_json,
)

# asyncio_mode = "auto" (pyproject) auto-marks the async tests; no global mark, so
# the two synchronous tests here are not spuriously flagged.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tight_margin_preparation(
    clock: Clock,
    *,
    generation: int,
    fence: int,
    warm_attempt_token: str | None = None,
    broker_epoch: str | None = None,
    dispatch_in: float,
    control_in: float,
    expires_in: float,
    renew_in: float,
):
    """A preparation whose capsule launch margin lapses early (the incident shape).

    The capsule is claimable at ``clock.now`` (all margins hold) but its dispatch
    credential margin lapses ``dispatch_in - (RUNNER_DEADLINE+DISPATCH_MARGIN)``
    seconds later, while the runner IDENTITY stays byte-identical.
    """

    base = preparation(
        clock,
        generation=generation,
        fence=fence,
        warm_attempt_token=warm_attempt_token,
        broker_epoch=broker_epoch,
    )
    tight = replace(
        base.capsule,
        control_expires_at=clock.now + timedelta(seconds=control_in),
        dispatch_expires_at={
            "lakebase": clock.now + timedelta(seconds=dispatch_in),
            "competitor": clock.now + timedelta(seconds=dispatch_in),
        },
        expires_at=clock.now + timedelta(seconds=expires_in),
        renew_by=clock.now + timedelta(seconds=renew_in),
    )
    return replace(base, capsule=tight)


class _TightMarginProvider(Provider):
    """Fake provider whose warm publishes a short-launch-margin capsule.

    Dispatch margin needs RUNNER_DEADLINE(660)+DISPATCH_MARGIN(60)=720s of slack,
    so with ``dispatch_in`` seconds of dispatch-credential horizon the launch
    margin lapses ``dispatch_in - 720`` seconds after the capsule is minted while
    the runner IDENTITY stays byte-identical.
    """

    def __init__(
        self,
        clock: Clock,
        *,
        dispatch_in: float = 725.0,
        control_in: float = 1_865.0,
        expires_in: float = 724.0,
        renew_in: float = 700.0,
    ) -> None:
        super().__init__(clock)
        self.dispatch_in = dispatch_in
        self.control_in = control_in
        self.expires_in = expires_in
        self.renew_in = renew_in

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
        requires_cleaned_bout: bool,
    ):
        del process_epoch
        self.attempt_tokens.append(warm_attempt_token)
        self.requires_cleaned_bout_calls.append(requires_cleaned_bout)
        self.prepare_calls += 1
        self.prepare_started.set()
        await self.release_prepare.wait()
        if self.prepare_error is not None:
            raise self.prepare_error
        return _tight_margin_preparation(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            warm_attempt_token=warm_attempt_token,
            broker_epoch=broker_epoch,
            dispatch_in=self.dispatch_in,
            control_in=self.control_in,
            expires_in=self.expires_in,
            renew_in=self.renew_in,
        )


class _RecordingProvider(Provider):
    """Records the claim reconciled during cleanup and settles on demand."""

    def __init__(self, clock: Clock) -> None:
        super().__init__(clock)
        self.reconciled_abandoned_claim = None
        self.reconcile_result = True

    async def reconcile(self, slot) -> bool:
        self.reconcile_calls += 1
        claim = getattr(slot, "claim", None)
        if claim is not None and getattr(slot, "bell_id", None) is None:
            self.reconciled_abandoned_claim = claim
        return self.reconcile_result


class _SlowProvider(Provider):
    """Prepare blocks until released or cancelled; records whether it completed."""

    def __init__(self, clock: Clock) -> None:
        super().__init__(clock)
        self.prepare_completed = False

    async def prepare(
        self,
        *,
        generation: int,
        coordinator_fence: int,
        process_epoch: str,
        broker_epoch: str,
        warm_attempt_token: str,
        requires_cleaned_bout: bool,
    ):
        del process_epoch, requires_cleaned_bout
        self.prepare_calls += 1
        self.prepare_started.set()
        await self.release_prepare.wait()  # cancelled when the coordinator fence is lost
        self.prepare_completed = True
        return preparation(
            self.clock,
            generation=generation,
            fence=coordinator_fence,
            warm_attempt_token=warm_attempt_token,
            broker_epoch=broker_epoch,
        )


async def _drive_warm(manager: Round5WarmCoordinator, provider: Provider) -> None:
    """Run one WARMING->READY cycle deterministically (store already initialized)."""

    provider.prepare_started.clear()
    provider.release_prepare.set()
    await asyncio.wait_for(manager.run_one_cycle(), timeout=1)


def _count(events, event_type: str) -> int:
    return sum(1 for event in events if event.event_type == event_type)


# ---------------------------------------------------------------------------
# A. Deterministic 14s skew: claim never releases before the arm deadline.
# ---------------------------------------------------------------------------


async def test_a_14s_skew_active_claim_never_releases_before_arm_deadline() -> None:
    clock = Clock()
    provider = _TightMarginProvider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)

    claimed, _ = await manager.claim(
        session_id="session-a",
        bout_id="bout-a",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    t0 = clock.now
    # ARM lands 14s after the claim; both TTLs are 180s, so the *arm* deadline
    # (t0+194) is 14s LATER than the raw claim deadline (t0+180).
    arm_deadline = t0 + timedelta(seconds=14 + DEFAULT_CLAIM_TTL_SECONDS)

    offset = 0.0
    saw_lapsed_margin = False
    for target in (10, 30, 60, 120, 171, 180, 185, 193):
        clock.advance(target - offset)
        offset = target
        await manager.run_one_cycle()
        slot = await manager.store.read("install-one")
        assert slot is not None
        # Never released, never dropped, never fenced early: the arm is in flight.
        assert slot.state == Round5WarmState.CLAIMED, f"state at t0+{target}s"
        assert slot.claim is not None
        assert slot.claim.claim_id == claimed.claim.claim_id
        if not manager._capsule_current(slot, clock.now):
            # The exact incident condition: launch margin lapsed while identity held.
            saw_lapsed_margin = True
            assert manager._capsule_belongs(slot) is True

    assert saw_lapsed_margin, "test must exercise a lapsed launch margin"
    final = await manager.store.read("install-one")
    assert final is not None and final.claim is not None
    # Renewed well past the arm deadline: it can never precede armed_expires_at.
    assert final.claim.claim_expires_at > arm_deadline


# ---------------------------------------------------------------------------
# B. Active claim + stale launch margin renews by IDENTITY, not launch margin.
# ---------------------------------------------------------------------------


async def test_b_active_claim_renews_by_identity_when_launch_margin_stale() -> None:
    clock = Clock()
    # Launch margin lapses ~5s after the capsule is minted; identity stays intact.
    provider = _TightMarginProvider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)

    claimed, _ = await manager.claim(
        session_id="session-b",
        bout_id="bout-b",
        selected_variant=Round5Variant.RDS,
        bout_fence=2,
    )
    assert claimed.claim is not None
    original_expiry = claimed.claim.claim_expires_at

    # Advance past the lapsed launch margin but WELL within the durable renewal
    # horizon, so the only thing that could stop renewal is the (reverted) margin
    # gate -- isolating the identity-vs-launch-margin distinction this test exists
    # for. (Blocker 10's separate horizon test covers the leak bound.)
    clock.advance(200)
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert manager._capsule_current(slot, clock.now) is False  # launch margin lapsed
    assert manager._capsule_belongs(slot) is True  # runner identity intact

    await manager.run_one_cycle()

    renewed = await manager.store.read("install-one")
    assert renewed is not None
    assert renewed.state == Round5WarmState.CLAIMED
    assert renewed.claim is not None
    assert renewed.claim.claim_id == claimed.claim.claim_id
    # Renewed by identity despite the stale launch margin (old code -> WARMING).
    assert renewed.claim.claim_expires_at > original_expiry
    assert renewed.claim.claim_expires_at == clock.now + manager._claim_ttl


# ---------------------------------------------------------------------------
# C. Interleave the generic warm release and the manager cleanup, both orders:
#    exactly one CLAIMED->CLEANING, claim retained, no WARMING/preload until settle.
# ---------------------------------------------------------------------------


async def _setup_inactive_claim(clock: Clock, provider: Provider):
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-c",
        bout_id="bout-c",
        selected_variant=Round5Variant.AURORA,
        bout_fence=3,
    )
    assert claimed.claim is not None
    # Operator went away: stop backstage renewal.
    manager.release_claim_active(claimed.claim.claim_id)
    return manager, claimed.claim.claim_id


async def test_c_release_then_manager_cleanup_is_one_cleaning_transition() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager, claim_id = await _setup_inactive_claim(clock, provider)
    prepare_calls_before = provider.prepare_calls

    # Coordinator's generic path fires first (expired, inactive) -> CLEANING.
    clock.advance(DEFAULT_CLAIM_TTL_SECONDS + 1)
    await manager.run_one_cycle()  # re-acquires lease, fences expired claim -> CLEANING
    slot = await manager.store.read("install-one")
    assert slot is not None and slot.state == Round5WarmState.CLEANING
    assert slot.claim is not None and slot.claim.claim_id == claim_id
    assert _count(await manager.store.events("install-one"), "cleanup_started") == 1

    # Manager begin_cleanup arrives second: idempotent, no second transition.
    again = await manager.begin_cleanup(claim_id)
    assert again.state == Round5WarmState.CLEANING
    assert again.claim is not None and again.claim.claim_id == claim_id
    assert _count(await manager.store.events("install-one"), "cleanup_started") == 1

    # No rewarm/preload while cleaning and unsettled.
    assert provider.prepare_calls == prepare_calls_before
    assert slot.state != Round5WarmState.WARMING

    # Only after settlement does the rewarm generation appear.
    warmed = await manager.finish_cleanup_and_rewarm(claim_id)
    assert warmed.state == Round5WarmState.WARMING
    assert warmed.generation == slot.generation + 1
    await _drive_warm(manager, provider)
    assert provider.prepare_calls == prepare_calls_before + 1
    ready = await manager.store.read("install-one")
    assert ready is not None and ready.state == Round5WarmState.READY


async def test_c_manager_cleanup_then_release_is_one_cleaning_transition() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager, claim_id = await _setup_inactive_claim(clock, provider)

    # Manager begin_cleanup fires first on a still-live claim -> CLEANING.
    cleaning = await manager.begin_cleanup(claim_id)
    assert cleaning.state == Round5WarmState.CLEANING
    assert cleaning.claim is not None and cleaning.claim.claim_id == claim_id
    assert _count(await manager.store.events("install-one"), "cleanup_started") == 1

    # Generic release arrives second: state is no longer CLAIMED, so it is a no-op.
    clock.advance(DEFAULT_CLAIM_TTL_SECONDS + 1)
    unchanged = await manager.store.release_expired_claim(
        cleaning, now=clock.now, still_fresh=False
    )
    assert unchanged is cleaning  # early return, no new transition
    await manager.run_one_cycle()  # CLEANING branch just waits for the manager
    assert _count(await manager.store.events("install-one"), "cleanup_started") == 1
    slot = await manager.store.read("install-one")
    assert slot is not None and slot.state == Round5WarmState.CLEANING
    assert slot.claim is not None and slot.claim.claim_id == claim_id


# ---------------------------------------------------------------------------
# D. Stateful convergence with a deterministic fake clock and no restart:
#    READY -> claim -> no bell -> CLEANING -> settle -> WARMING -> READY (N+1)
#    -> second claim + bell, exact job IDs preserved throughout.
# ---------------------------------------------------------------------------


async def test_d_no_bell_converges_via_real_manager_without_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Drive the PRODUCTION RunManager path (arm -> armed-TTL expiry ->
    # _retry_connection_spike_cleanup) over a real coordinator+store, with a
    # deterministic fake clock and no restart. Reconcile failure must NOT finish
    # the rewarm; only a proven resident settlement converges to N+1 READY.
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "0.02")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "0.05")
    clock = Clock()
    provider = _EngineProvider(clock, _RecoverableAbandonPlan)
    coord = _accept_coordinator(clock, provider)
    await _warm_to_ready(coord, provider)
    manager = _make_manager(coord, clock, round_isolation=True)
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
                snap = (await client.get(f"/api/sessions/{session_id}")).json()
                if (
                    snap["state"] == SessionState.FAILED.value
                    and snap["round5_setup"]["cleanup_retryable"] is True
                ):
                    break
                await asyncio.sleep(0.01)

            record = manager._records[session_id]
            claim = record.round5_warm_slot.claim
            assert claim is not None
            lakebase_job, competitor_job = claim.lakebase_job_id, claim.competitor_job_id
            generation = record.round5_warm_slot.generation

            cleaning = await coord.store.read("install-acceptance")
            assert cleaning is not None and cleaning.state == Round5WarmState.CLEANING
            assert cleaning.claim is not None
            # Exact job IDs preserved through the ownership transition into CLEANING.
            assert cleaning.claim.lakebase_job_id == lakebase_job
            assert cleaning.claim.competitor_job_id == competitor_job

            plan = provider.engines[0]._plan
            assert isinstance(plan, _RecoverableAbandonPlan)
            # Reconcile keeps failing (resident not settled) -> stays CLEANING, the
            # rewarm generation is NEVER produced while reconcile fails.
            for _ in range(50):
                if plan.durable_resident_reconcile_attempts >= 1:
                    break
                await asyncio.sleep(0.01)
            assert plan.durable_resident_reconcile_attempts >= 1
            still = await coord.store.read("install-acceptance")
            assert still is not None and still.state == Round5WarmState.CLEANING
            assert still.generation == generation  # finish_cleanup_and_rewarm not called

            # Allow the resident to settle -> reconcile proves absence -> converge.
            plan.allow_resident_settle.set()
            for _ in range(400):
                warming = await coord.store.read("install-acceptance")
                if warming is not None and warming.generation == generation + 1:
                    break
                await asyncio.sleep(0.01)
            assert warming is not None and warming.generation == generation + 1

            await coord.run_one_cycle()  # WARMING -> READY (N+1), same process
            ready = await coord.store.read("install-acceptance")
            assert ready is not None and ready.state == Round5WarmState.READY
            assert ready.generation == generation + 1
            # Episodic cleanup lineage cleared on the N+1 READY (blocker 4).
            assert ready.requires_cleaned_bout is False
            assert coord.public_status_cached()["round5_warm_blocked_terminal"] is False

            # A second bout arms cleanly with no restart.
            second = await client.post("/api/sessions", json=_session_body())
            await _arm_via_http(client, second.json()["id"])
    finally:
        await manager.close()
        await coord.close()


# ---------------------------------------------------------------------------
# E. Pre-bell expiry must NOT invoke the post-setup cancel_setup_and_settle seam.
# ---------------------------------------------------------------------------


class _CleanupEngineSpy:
    def __init__(self, deferred: set[str] | None = None) -> None:
        self.settle_abandoned_called = False
        self.cancel_setup_called = False
        self.cancel_and_cleanup_called = False
        # Handoff-path external mutators (must stay False whenever a coordinator
        # owns cleanup): the manager must NOT drive the engine's own stop/settle.
        self.stop_and_begin_cleanup_called = False
        self.stop_setup_and_begin_cleanup_called = False
        # Truly-local cancellation (allowed): cancels in-process tasks only.
        self.cancel_local_called = False
        self._deferred = deferred or set()

    async def settle_abandoned_arm(self) -> set[str]:
        self.settle_abandoned_called = True
        return set(self._deferred)

    async def cancel_and_cleanup(self, arm) -> None:  # pragma: no cover - guarded off pre-bell
        del arm
        self.cancel_and_cleanup_called = True

    async def cancel_setup_and_settle(self, bout_id) -> None:
        del bout_id
        self.cancel_setup_called = True

    async def stop_and_begin_cleanup(self, arm) -> None:
        del arm
        self.stop_and_begin_cleanup_called = True

    async def stop_setup_and_begin_cleanup(self, bout_id) -> None:
        del bout_id
        self.stop_setup_and_begin_cleanup_called = True

    async def cancel_local_round5_run_tasks(self) -> None:
        self.cancel_local_called = True


class _FakeCleanupCoordinator:
    """A coordinator double that records durable cleanup transitions but performs
    NO external resource mutation -- so a test can assert the manager delegates to
    it instead of driving the engine's own stop/settle."""

    def __init__(self) -> None:
        self.begin_cleanup_calls: list[str] = []
        self.converge_cleanup_calls: list[str] = []
        self.released_active: list[str] = []

    def release_claim_active(self, claim_id: str) -> None:
        self.released_active.append(claim_id)

    async def begin_cleanup(self, claim_id: str):
        self.begin_cleanup_calls.append(claim_id)
        return SimpleNamespace(
            state=Round5WarmState.CLEANING,
            claim=SimpleNamespace(claim_id=claim_id),
        )

    async def converge_cleanup(self, claim_id: str):
        self.converge_cleanup_calls.append(claim_id)
        return SimpleNamespace(
            state=Round5WarmState.WARMING,
            claim=None,
        )


def _cleanup_record(engine, *, bell_id, bell_at_utc, arm=None):
    slot = SimpleNamespace(
        claim=SimpleNamespace(claim_id="claim-e"),
        bell_id=bell_id,
        bell_at_utc=bell_at_utc,
    )
    return SimpleNamespace(
        connection_spike_engine=engine,
        connection_spike_arm=arm,
        round5_warm_slot=slot,
        snapshot=SimpleNamespace(id="session-e"),
        connection_spike_setup_result=object(),
    )


def _cleanup_self():
    """A minimal RunManager-like self for calling _cleanup_connection_spike directly.

    Carries a None coordinator (so the durable-truth refresh is a no-op) and the
    real static classifier + bound durable-refresh method.
    """

    import types as _types

    fake = SimpleNamespace(
        _round5_warm_coordinator=None,
        _round5_prebell_cleanup_required=RunManager._round5_prebell_cleanup_required,
    )
    fake._refresh_round5_slot_from_durable = _types.MethodType(
        RunManager._refresh_round5_slot_from_durable, fake
    )
    return fake


async def test_e_prebell_cleanup_does_not_invoke_setup_cleanup() -> None:
    engine = _CleanupEngineSpy()
    record = _cleanup_record(engine, bell_id=None, bell_at_utc=None)  # pre-bell
    fake_self = _cleanup_self()
    ok = await RunManager._cleanup_connection_spike(fake_self, record)
    assert ok is True
    assert engine.settle_abandoned_called is True  # staged residents unstaged
    assert engine.cancel_setup_called is False  # invariant 4: no post-setup teardown
    assert engine.cancel_and_cleanup_called is False  # no arm pre-bell


async def test_e_postbell_cleanup_still_invokes_setup_cleanup() -> None:
    engine = _CleanupEngineSpy()
    record = _cleanup_record(
        engine, bell_id="bell-1", bell_at_utc=object(), arm=None
    )  # bell crossed
    fake_self = _cleanup_self()
    ok = await RunManager._cleanup_connection_spike(fake_self, record)
    assert ok is True
    assert engine.cancel_setup_called is True  # post-setup teardown still runs post-bell


# ---------------------------------------------------------------------------
# F. Fence loss cancels the held prepare; it cannot finish and record BLOCKED.
# ---------------------------------------------------------------------------


async def test_f_fence_loss_cancels_held_prepare_and_never_blocks() -> None:
    clock = Clock()
    provider = _SlowProvider(clock)
    store = InMemoryRound5WarmStore()
    manager = Round5WarmCoordinator(
        installation_id="install-one",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=provider,
        process_epoch="process-one",
        broker_epoch="broker-one",
        clock=clock,
        monotonic_ns=clock.monotonic,
        coordinator_ttl_seconds=0.06,  # beat interval ~0.02s
    )
    await store.initialize()

    cycle = asyncio.create_task(manager.run_one_cycle())
    await asyncio.wait_for(provider.prepare_started.wait(), timeout=1)

    # A second live process seizes coordination once the first lease can expire.
    clock.advance(0.1)
    await store.acquire_coordinator(
        installation_id="install-one",
        process_epoch="process-two",
        broker_epoch="broker-two",
        now=clock.now,
        ttl=timedelta(seconds=90),
    )

    # The lease beat detects the lost fence and cancels the stale prepare. The cycle
    # now HANDLES the loss gracefully instead of raising into run()'s silent 1s retry:
    # it never records a spurious terminal BLOCK, and -- because authority genuinely
    # moved to process-two -- it must NOT write a contention retry under the foreign
    # fence (the WARMING-branch handler records only while THIS process still owns the
    # coordinator). It returns a positive backoff delay; the next cycle's
    # acquire_coordinator then defers to the new owner.
    delay = await asyncio.wait_for(cycle, timeout=1)
    assert delay > 0

    assert provider.prepare_completed is False  # the held prepare was aborted
    slot = await store.read("install-one")
    assert slot is not None
    assert slot.coordinator_owner == "process-two"
    assert slot.state != Round5WarmState.BLOCKED
    assert slot.last_error_code is None  # no write under the foreign fence


# ---------------------------------------------------------------------------
# G. Binding-changed classification: retryable during a cleanup transition,
#    still fail-closed as a true baseline drift with no claim/debt.
# ---------------------------------------------------------------------------


class _BindingChangedEngine:
    def require_cleaned_bout(self) -> None:  # noqa: D401 - fake seam
        return None

    def retain_cleaned_bout(self, bout_id: str) -> None:  # noqa: D401 - fake seam
        del bout_id

    async def warm(self, generation: int, warm_attempt_token: str):
        del generation, warm_attempt_token
        raise Round5ResidentBindingChangedError("resident readiness binding changed")

    async def warm_with_physical_runners_from(self, source, generation):  # pragma: no cover
        del source, generation
        raise AssertionError("aurora.warm should raise before the RDS runner reuse")


def _binding_changed_provider() -> LiveRound5WarmProvider:
    return LiveRound5WarmProvider(SimpleNamespace(), lambda _competitor: _BindingChangedEngine())


async def test_g_binding_changed_is_retryable_when_cleanup_debt_exists() -> None:
    provider = _binding_changed_provider()
    with pytest.raises(RetryableWarmError) as excinfo:
        await provider.prepare(
            generation=7,
            coordinator_fence=1,
            process_epoch="process-one",
            broker_epoch="broker-one",
            warm_attempt_token="attempt-g",
            requires_cleaned_bout=True,  # a claim was just cleaned: transition debt
        )
    assert excinfo.value.code == "warm_resident_binding_transition"


async def test_g_binding_changed_fails_closed_without_claim_or_debt() -> None:
    provider = _binding_changed_provider()
    with pytest.raises(BlockedWarmError) as excinfo:
        await provider.prepare(
            generation=7,
            coordinator_fence=1,
            process_epoch="process-one",
            broker_epoch="broker-one",
            warm_attempt_token="attempt-g",
            requires_cleaned_bout=False,  # no prior claim/debt: genuine drift
        )
    assert excinfo.value.code == "warm_baseline_unexpected"


# ---------------------------------------------------------------------------
# H. Two-process takeover over one durable store: the replacement converges an
#    inherited no-bell claim and the stale owner is rejected.
# ---------------------------------------------------------------------------


async def test_h_two_process_takeover_converges_and_rejects_stale_owner() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()  # same CAS/fence semantics as the durable store
    provider_a = Provider(clock)
    provider_b = _RecordingProvider(clock)
    process_a = coordinator(clock, provider_a, store, process_epoch="process-a")
    process_b = coordinator(clock, provider_b, store, process_epoch="process-b")

    await warm_ready(process_a, provider_a)
    claimed, _ = await process_a.claim(
        session_id="session-h",
        bout_id="bout-h",
        selected_variant=Round5Variant.AURORA,
        bout_fence=6,
    )
    assert claimed.claim is not None
    generation = claimed.generation

    # process-a dies; process-b takes over once the coordinator lease can expire.
    clock.advance(process_a._coordinator_ttl.total_seconds() + 1)
    provider_b.reconcile_result = True
    await process_b.run_one_cycle()

    slot = await store.read("install-one")
    assert slot is not None
    assert slot.coordinator_owner == "process-b"  # takeover
    assert slot.state == Round5WarmState.WARMING  # inherited claim converged
    assert slot.generation == generation + 1
    assert provider_b.reconciled_abandoned_claim is not None
    assert provider_b.reconciled_abandoned_claim.claim_id == claimed.claim.claim_id

    # The stale owner's fenced writes are rejected.
    with pytest.raises(WarmFenceLostError):
        await store.heartbeat_coordinator(
            claimed, now=clock.now, ttl=process_a._coordinator_ttl
        )


# ---------------------------------------------------------------------------
# I. Restart while CLAIMED (no bell) recovers with the exact job IDs, then a
#    second bout arms and rings -- without restarting the recovered process.
# ---------------------------------------------------------------------------


async def test_i_restart_while_claimed_recovers_exact_jobs_then_second_bout() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider_a = Provider(clock)
    process_a = coordinator(clock, provider_a, store, process_epoch="process-a")
    await warm_ready(process_a, provider_a)
    claimed, _ = await process_a.claim(
        session_id="session-i",
        bout_id="bout-i",
        selected_variant=Round5Variant.RDS,
        bout_fence=9,
    )
    assert claimed.claim is not None
    lakebase_job = claimed.claim.lakebase_job_id
    competitor_job = claimed.claim.competitor_job_id
    generation = claimed.generation

    # Restart: a fresh process on the same durable store, no bell was ever rung.
    clock.advance(process_a._coordinator_ttl.total_seconds() + 1)
    provider_b = _RecordingProvider(clock)
    provider_b.reconcile_result = True
    process_b = coordinator(clock, provider_b, store, process_epoch="process-b")
    await process_b.run_one_cycle()

    assert provider_b.reconciled_abandoned_claim is not None
    assert provider_b.reconciled_abandoned_claim.lakebase_job_id == lakebase_job
    assert provider_b.reconciled_abandoned_claim.competitor_job_id == competitor_job
    recovered = await store.read("install-one")
    assert recovered is not None
    assert recovered.state == Round5WarmState.WARMING
    assert recovered.generation == generation + 1

    # Second bout arms + rings on the recovered process, no further restart.
    await _drive_warm(process_b, provider_b)
    ready = await store.read("install-one")
    assert ready is not None and ready.state == Round5WarmState.READY
    claimed2, _ = await process_b.claim(
        session_id="session-i2",
        bout_id="bout-i2",
        selected_variant=Round5Variant.AURORA,
        bout_fence=10,
    )
    assert claimed2.claim is not None
    context = await process_b.accept_bell(claimed2.claim.claim_id)
    assert context.bout_id == "bout-i2"
    running = await store.read("install-one")
    assert running is not None and running.state == Round5WarmState.RUNNING


# ---------------------------------------------------------------------------
# J. Preserve e54 fail-closed scoping: a genuine baseline drift with no claim or
#    cleanup debt still latches the terminal block (the other half of G, and the
#    guard the e54 chaos/redaction suites depend on -- those suites are run in
#    full and must remain green apart from the four preexisting failures).
# ---------------------------------------------------------------------------


async def test_j_true_baseline_drift_still_fails_closed() -> None:
    # A config/identity defect (ConnectionSpikeLiveConfigurationError-style) with
    # no cleanup lineage must remain a terminal baseline block, unchanged from e54.
    from server.connection_spike_live import ConnectionSpikeLiveConfigurationError

    class _DriftEngine:
        def require_cleaned_bout(self) -> None:
            return None

        def retain_cleaned_bout(self, bout_id: str) -> None:
            del bout_id

        async def warm(self, generation: int, warm_attempt_token: str):
            del generation, warm_attempt_token
            raise ConnectionSpikeLiveConfigurationError(
                "Round 5 clean baseline contains prior-bout add-ons"
            )

        async def warm_with_physical_runners_from(self, source, generation):  # pragma: no cover
            del source, generation
            raise AssertionError("unreachable")

    provider = LiveRound5WarmProvider(SimpleNamespace(), lambda _c: _DriftEngine())
    with pytest.raises(BlockedWarmError) as excinfo:
        await provider.prepare(
            generation=3,
            coordinator_fence=1,
            process_epoch="process-one",
            broker_epoch="broker-one",
            warm_attempt_token="attempt-j",
            requires_cleaned_bout=True,  # even mid-lineage, a config defect is terminal
        )
    assert excinfo.value.code == "warm_baseline_invalid"


# ---------------------------------------------------------------------------
# Blocker 1. Post-bell cleanup classification derives from durable bell truth,
# not a stale cached CLAIMED slot: a post-bell abort MUST run setup teardown.
# ---------------------------------------------------------------------------


async def test_blocker1_stale_claimed_slot_with_bell_context_is_post_bell() -> None:
    engine = _CleanupEngineSpy()
    # The cached slot still reads CLAIMED / null-bell (never refreshed), but the
    # durable bell was accepted (bell context present) -- authoritative post-bell.
    record = _cleanup_record(engine, bell_id=None, bell_at_utc=None)
    record.round5_bell_context = SimpleNamespace(bell_id="bell-1")
    assert RunManager._round5_prebell_cleanup_required(record) is False
    fake_self = _cleanup_self()
    ok = await RunManager._cleanup_connection_spike(fake_self, record)
    assert ok is True
    # Post-setup teardown (Proxy/journal/absence) is mandatory on a post-bell abort.
    assert engine.cancel_setup_called is True


# ---------------------------------------------------------------------------
# Blocker 2. Durable authority guard: a stale coordinator is refused at the
# mutation boundary even if the provider would swallow cancellation.
# ---------------------------------------------------------------------------


class _SwallowingEngine:
    """An engine whose warm/reconcile would SWALLOW a CancelledError and 'succeed'."""

    def __init__(self) -> None:
        self.warm_called = False
        self.reconcile_abandoned_called = False
        self.authority_guard = None

    def require_cleaned_bout(self) -> None:
        return None

    def retain_cleaned_bout(self, bout_id: str) -> None:
        del bout_id

    async def warm(self, generation: int, warm_attempt_token: str):
        self.warm_called = True
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass  # swallow -- advisory cancellation must not be the only defense
        return SimpleNamespace()

    async def warm_with_physical_runners_from(self, source, generation):
        return SimpleNamespace()

    async def reconcile_abandoned_claim(self, claim) -> None:
        self.reconcile_abandoned_called = True

    async def reconcile_claim(self, claim) -> None:
        self.reconcile_abandoned_called = True


async def _stale_guard() -> None:
    raise WarmFenceLostError("coordinator authority moved before provider mutation")


async def test_blocker2_prepare_refused_by_guard_before_any_mutation() -> None:
    engine = _SwallowingEngine()
    provider = LiveRound5WarmProvider(SimpleNamespace(), lambda _c: engine)
    provider.authority_guard = _stale_guard
    with pytest.raises(WarmFenceLostError):
        await provider.prepare(
            generation=5,
            coordinator_fence=1,
            process_epoch="process-one",
            broker_epoch="broker-one",
            warm_attempt_token="attempt-2",
            requires_cleaned_bout=False,
        )
    # The mutation never ran -- the guard refused first, regardless of the engine
    # being willing to swallow a cancellation.
    assert engine.warm_called is False


async def test_blocker2_reconcile_refused_by_guard_before_any_mutation() -> None:
    engine = _SwallowingEngine()
    provider = LiveRound5WarmProvider(SimpleNamespace(), lambda _c: engine)
    provider.authority_guard = _stale_guard
    slot = SimpleNamespace(
        state=Round5WarmState.CLEANING,
        claim=SimpleNamespace(selected_variant=Round5Variant.AURORA, bout_id="bout-2"),
        bell_id=None,
        bell_at_utc=None,
        cleaned_bout_id=None,
        requires_cleaned_bout=True,
    )
    with pytest.raises(WarmFenceLostError):
        await provider.reconcile(slot)
    assert engine.reconcile_abandoned_called is False


async def test_blocker2_coordinator_guard_refuses_after_takeover() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider_a = Provider(clock)
    process_a = coordinator(clock, provider_a, store, process_epoch="process-a")
    await warm_ready(process_a, provider_a)
    # While process-a owns the coordinator lease the guard passes.
    await process_a._authority_guard()
    # A replacement process seizes coordination once the lease can expire.
    clock.advance(process_a._coordinator_ttl.total_seconds() + 1)
    await store.acquire_coordinator(
        installation_id="install-one",
        process_epoch="process-b",
        broker_epoch="broker-b",
        now=clock.now,
        ttl=process_a._coordinator_ttl,
    )
    with pytest.raises(WarmFenceLostError):
        await process_a._authority_guard()


# ---------------------------------------------------------------------------
# Blocker 3. Reject a bell whose claim has expired at the authoritative now.
# ---------------------------------------------------------------------------


async def test_blocker3_bell_rejected_on_expired_claim_boundaries() -> None:
    from datetime import timedelta as _td

    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-3",
        bout_id="bout-3",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    slot = await manager.store.read("install-one")
    assert slot is not None
    expiry = claimed.claim.claim_expires_at

    # Equality counts as expired.
    with pytest.raises(WarmClaimUnavailableError):
        await manager.store.accept_bell(
            slot, claim_id=claimed.claim.claim_id, bell_id="bell-eq", bell_at_utc=expiry
        )
    # +1us past the deadline is expired.
    with pytest.raises(WarmClaimUnavailableError):
        await manager.store.accept_bell(
            slot,
            claim_id=claimed.claim.claim_id,
            bell_id="bell-past",
            bell_at_utc=expiry + _td(microseconds=1),
        )
    # -1us before the deadline is accepted (slot unchanged by the refusals above).
    running = await manager.store.accept_bell(
        slot,
        claim_id=claimed.claim.claim_id,
        bell_id="bell-ok",
        bell_at_utc=expiry - _td(microseconds=1),
    )
    assert running.state == Round5WarmState.RUNNING


async def test_blocker3_concurrent_bell_and_cleanup_from_same_revision() -> None:
    from datetime import timedelta as _td

    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claimed, _ = await manager.claim(
        session_id="session-3c",
        bout_id="bout-3c",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    slot = await manager.store.read("install-one")
    assert slot is not None
    # One transition wins from a given revision; the other, from the SAME stale
    # revision, is rejected by the CAS -- a bell and a cleanup cannot both land.
    running = await manager.store.accept_bell(
        slot,
        claim_id=claimed.claim.claim_id,
        bell_id="bell-win",
        bell_at_utc=claimed.claim.claim_expires_at - _td(seconds=1),
    )
    assert running.state == Round5WarmState.RUNNING
    with pytest.raises(WarmStoreConflictError):
        await manager.store.begin_cleanup(
            slot, claim_id=claimed.claim.claim_id, now=clock.now
        )


# ---------------------------------------------------------------------------
# Blocker 4. The cleanup-lineage signal is episodic (cleared on N+1 READY), and
# the parser honors an explicit false while inferring only when absent.
# ---------------------------------------------------------------------------


async def test_blocker4_requires_cleaned_bout_is_episodic_across_rewarm() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-4",
        bout_id="bout-4",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    coord.release_claim_active(claimed.claim.claim_id)
    clock.advance(DEFAULT_CLAIM_TTL_SECONDS + 1)
    await coord.run_one_cycle()  # CLAIMED -> CLEANING
    await coord.run_one_cycle()  # CLEANING self-drive -> reconcile -> WARMING gen2

    warming = await store.read("install-one")
    assert warming is not None
    assert warming.state == Round5WarmState.WARMING and warming.generation == 2
    assert warming.requires_cleaned_bout is True  # immediate lineage active
    assert warming.cleaned_bout_id == "bout-4"

    await _drive_warm(coord, provider)  # WARMING -> READY (N+1)
    ready = await store.read("install-one")
    assert ready is not None and ready.state == Round5WarmState.READY
    assert ready.requires_cleaned_bout is False  # episodic: lineage cleared
    assert ready.cleaned_bout_id == "bout-4"  # audit retained


def test_blocker4_parser_honors_explicit_false_and_infers_when_absent() -> None:
    clock = Clock()
    slot = Round5WarmSlot(
        installation_id="install-one",
        generation=3,
        state=Round5WarmState.WARMING,
        revision=1,
        coordinator_fence=1,
        process_epoch="process-one",
        broker_epoch="broker-one",
        warm_contract_sha256=DIGEST,
        warming_started_at=clock.now,
        cleaned_bout_id="bout-x",
        requires_cleaned_bout=False,
    )
    payload = _to_json(slot)
    assert isinstance(payload, dict)

    # Explicit false round-trips as false even though cleaned_bout_id is retained.
    assert payload["requires_cleaned_bout"] is False
    restored = _slot_from_json(payload)
    assert restored.requires_cleaned_bout is False
    assert restored.cleaned_bout_id == "bout-x"

    # Mixed-version: a row written before the flag was episodic omits it entirely;
    # infer True from the retained cleaned_bout_id for backward compatibility.
    legacy = dict(payload)
    legacy.pop("requires_cleaned_bout")
    assert _slot_from_json(legacy).requires_cleaned_bout is True

    # Explicit true is honored.
    explicit_true = dict(payload)
    explicit_true["requires_cleaned_bout"] = True
    assert _slot_from_json(explicit_true).requires_cleaned_bout is True


# ---------------------------------------------------------------------------
# Blocker 5. A same-process self-fenced CLEANING self-drives to N+1 READY with
# NO manager cleanup worker and NO restart (arm setup failure / cleared active).
# ---------------------------------------------------------------------------


async def test_blocker5_self_fenced_cleaning_self_drives_without_manager() -> None:
    clock = Clock()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-5",
        bout_id="bout-5",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    # Synchronous arm setup failure: active-id cleared, no manager cleanup worker.
    coord.release_claim_active(claimed.claim.claim_id)
    clock.advance(DEFAULT_CLAIM_TTL_SECONDS + 1)

    slot = None
    for _ in range(6):  # ONLY the coordinator loop runs; it must self-converge.
        await coord.run_one_cycle()
        slot = await coord.store.read("install-one")
        if slot is not None and slot.state == Round5WarmState.WARMING:
            break
    assert slot is not None and slot.state == Round5WarmState.WARMING
    assert slot.generation == claimed.generation + 1
    # The coordinator itself drove the abandoned-claim reconcile (not a manager).
    assert provider.reconciled_abandoned_claim is not None
    assert provider.reconciled_abandoned_claim.claim_id == claimed.claim.claim_id

    await _drive_warm(coord, provider)
    ready = await coord.store.read("install-one")
    assert ready is not None and ready.state == Round5WarmState.READY


# ---------------------------------------------------------------------------
# Blocker 6. The forgeable settlement-proof API and the direct CLAIMED->READY
# fast path are GONE: no-bell abandon always fences into CLEANING (claim retained).
# ---------------------------------------------------------------------------


async def test_blocker6_no_direct_release_token_and_abandon_fences_cleaning() -> None:
    clock = Clock()
    provider = Provider(clock)
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-6",
        bout_id="bout-6",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None

    # The forgeable-proof direct-release API is removed entirely (store + coord).
    assert not hasattr(coord.store, "release_claim")
    import inspect as _inspect

    assert "resident_settled" not in _inspect.signature(coord.abandon_claim).parameters

    # abandon_claim now fences into CLEANING retaining the claim -- never READY.
    fenced = await coord.abandon_claim(claimed.claim.claim_id)
    assert fenced.state == Round5WarmState.CLEANING
    assert fenced.claim is not None and fenced.claim.claim_id == claimed.claim.claim_id
    assert coord.ring_ready is False

    persisted = await coord.store.read("install-one")
    assert persisted is not None and persisted.state == Round5WarmState.CLEANING
    assert persisted.claim is not None


# ---------------------------------------------------------------------------
# Blocker 7. claim() wakes the supervised loop so a fresh claim is renewed
# promptly instead of after a long READY sleep.
# ---------------------------------------------------------------------------


async def test_blocker7_claim_wakes_supervised_loop_to_renew() -> None:
    clock = Clock()
    provider = Provider(clock)
    provider.release_prepare.set()  # warm completes immediately
    manager = coordinator(clock, provider)
    await manager.start()
    try:
        for _ in range(200):
            if manager.ring_ready:
                break
            await asyncio.sleep(0.005)
        assert manager.ring_ready

        claimed, _ = await manager.claim(
            session_id="session-7",
            bout_id="bout-7",
            selected_variant=Round5Variant.AURORA,
            bout_fence=1,
        )
        assert claimed.claim is not None

        # With wake(), the loop re-cycles immediately and renews the claim. Without
        # it the loop would sleep on the (much longer) READY interval and no
        # claim_renewed event would appear within this window.
        renewed = False
        for _ in range(120):
            events = await manager.store.events("install-one")
            if any(e.event_type == "claim_renewed" for e in events):
                renewed = True
                break
            await asyncio.sleep(0.005)
        assert renewed, "supervised loop did not wake to renew the fresh claim"
    finally:
        await manager.close()


# ---------------------------------------------------------------------------
# Blocker 10. A leaked/stuck active-id cannot renew a claim forever: renewal is
# bounded by a durable horizon and the claim then fences into CLEANING.
# ---------------------------------------------------------------------------


async def test_blocker10_leaked_active_claim_is_bounded_and_fences() -> None:
    clock = Clock()
    provider = Provider(clock)  # long credential horizon: identity stays valid
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-10",
        bout_id="bout-10",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    assert claimed.claim.claim_id in coord._active_claim_ids

    # A leaked ARM task never cleared the active-id; time passes far beyond the
    # durable renewal horizon while the runner identity is still perfectly valid.
    clock.advance(coord._max_active_arm_renewal.total_seconds() + 1)
    assert coord._capsule_belongs(await coord.store.read("install-one")) is True
    await coord.run_one_cycle()

    slot = await coord.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.CLEANING  # bounded: not renewed forever
    assert claimed.claim.claim_id not in coord._active_claim_ids  # leak cleared


# ---------------------------------------------------------------------------
# Blocker 12. BLOCKED is never terminal while claim/cleanup/resident debt exists.
# ---------------------------------------------------------------------------


def test_blocker12_blocked_not_terminal_while_debt_outstanding() -> None:
    clock = Clock()
    with_debt = Round5WarmSlot(
        installation_id="install-one",
        generation=2,
        state=Round5WarmState.BLOCKED,
        revision=1,
        coordinator_fence=1,
        process_epoch="process-one",
        broker_epoch="broker-one",
        warm_contract_sha256=DIGEST,
        warming_started_at=clock.now,
        last_error_code="warm_baseline_unexpected",
        last_error_at=clock.now,
        cleaned_bout_id="bout-x",
        requires_cleaned_bout=True,
    )
    # Outstanding resident/cleanup debt -> self-verifiable, never terminal.
    assert _blocked_is_terminal(with_debt) is False

    # The same non-self-verifiable code with NO debt IS terminal (unchanged e54).
    no_debt = replace(
        with_debt, requires_cleaned_bout=False, cleaned_bout_id=None
    )
    assert _blocked_is_terminal(no_debt) is True


# ---------------------------------------------------------------------------
# 764-follow-up Blocker 2. Concurrent cleanup convergence is COALESCED: exactly
# one provider reconcile+finish sequence per claim under concurrent triggers.
# ---------------------------------------------------------------------------


async def test_b2_concurrent_converge_cleanup_is_coalesced() -> None:
    clock = Clock()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-b2",
        bout_id="bout-b2",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    coord.release_claim_active(claimed.claim.claim_id)
    cleaning = await coord.begin_cleanup(claimed.claim.claim_id)
    assert cleaning.state == Round5WarmState.CLEANING
    provider.reconcile_calls = 0

    # Fire the manager-style request and the supervised-loop-style request at once.
    results = await asyncio.gather(
        coord.converge_cleanup(claimed.claim.claim_id),
        coord.converge_cleanup(claimed.claim.claim_id),
    )
    # Exactly ONE external reconcile sequence ran for the claim.
    assert provider.reconcile_calls == 1
    assert all(r.state == Round5WarmState.WARMING for r in results)
    assert results[0].generation == claimed.generation + 1


# ---------------------------------------------------------------------------
# Blocker 3. Per-mutation authority guard: after a takeover the NEXT external
# mutation is refused even mid-method (not just at the orchestration boundary).
# ---------------------------------------------------------------------------


async def test_b3_per_mutation_guard_aborts_settle_after_takeover() -> None:
    from server.connection_spike_live import LiveConnectionSpikeEngine

    eng = object.__new__(LiveConnectionSpikeEngine)
    b1 = SimpleNamespace(job_id="j1")
    b2 = SimpleNamespace(job_id="j2")
    eng._resident_bindings = {"lakebase": b1, "competitor": b2}
    eng._active_run_ids = {}
    cancels: list[str] = []

    class _Adapter:
        async def cancel_resident(self, *, binding) -> None:
            cancels.append(binding.job_id)

    eng._lane_adapters = {"lakebase": _Adapter(), "competitor": _Adapter()}

    class _Guard:
        def __init__(self, fail_at: int) -> None:
            self.n = 0
            self.fail_at = fail_at

        async def __call__(self) -> None:
            self.n += 1
            if self.n >= self.fail_at:
                raise WarmFenceLostError("coordinator authority moved")

    # Guard passes for entry(1) + lane1(2), then fails before lane2(3).
    eng.authority_guard = _Guard(fail_at=3)
    with pytest.raises(WarmFenceLostError):
        await eng._settle_staged_residents()
    # The first resident cancel ran; the SECOND was refused mid-method by the guard.
    assert cancels == ["j1"]


async def test_b3_engine_guard_refuses_reconcile_before_any_mutation() -> None:
    from server.connection_spike_live import LiveConnectionSpikeEngine

    eng = object.__new__(LiveConnectionSpikeEngine)
    cancels: list[str] = []

    class _Adapter:
        async def cancel_job(self, job_id: str) -> None:
            cancels.append(job_id)

    eng._lane_adapters = {"lakebase": _Adapter(), "competitor": _Adapter()}
    eng._job_ids = {"lakebase": "l" * 64, "competitor": "c" * 64}

    async def _stale() -> None:
        raise WarmFenceLostError("coordinator authority moved")

    eng.authority_guard = _stale
    claim = SimpleNamespace(bout_id="bout-x", bout_fence=1)
    with pytest.raises(WarmFenceLostError):
        await eng.reconcile_abandoned_claim(claim)
    # Refused before ANY external job cancel.
    assert cancels == []


# ---------------------------------------------------------------------------
# Blocker 5. The exact armed-lease deadline is honored: a long configured TTL
# (1800s) renews through the deadline, NOT the coarse leak-fallback horizon.
# ---------------------------------------------------------------------------


async def test_b5_long_armed_deadline_renews_past_fallback_horizon() -> None:
    clock = Clock()
    provider = Provider(clock)  # long credential horizon
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-b5",
        bout_id="bout-b5",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    # Register a 1800s armed deadline -- LONGER than the coarse leak fallback.
    armed_deadline = clock.now + timedelta(seconds=1800)
    coord.set_active_claim_deadline(claimed.claim.claim_id, armed_deadline)
    assert coord._max_active_arm_renewal.total_seconds() < 1800  # fallback is shorter

    # Advance well past the coarse fallback horizon but before the armed deadline.
    clock.advance(coord._max_active_arm_renewal.total_seconds() + 300)
    assert clock.now < armed_deadline
    await coord.run_one_cycle()
    slot = await coord.store.read("install-one")
    assert slot is not None
    # Renewed (not fenced): the exact armed deadline governs, not the fallback.
    assert slot.state == Round5WarmState.CLAIMED
    assert slot.claim is not None

    # Past the armed deadline, renewal stops and the claim fences into CLEANING.
    clock.advance(1800)
    assert clock.now >= armed_deadline
    await coord.run_one_cycle()
    fenced = await coord.store.read("install-one")
    assert fenced is not None and fenced.state == Round5WarmState.CLEANING


# ---------------------------------------------------------------------------
# Blocker 4. A claim-bearing BLOCKED never warms/preps over the debt: it routes
# into CLEANING and converges, preserving the claim identities.
# ---------------------------------------------------------------------------


async def test_b4_claim_bearing_blocked_routes_into_cleaning() -> None:
    clock = Clock()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider)
    await coord.store.initialize()

    claim = Round5BoutClaim(
        claim_id="claim-b4",
        bell_id="bell-b4",
        session_id="session-b4",
        bout_id="bout-b4",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
        claimed_at=clock.now,
        claim_expires_at=clock.now + timedelta(seconds=180),
        capsule_generation=1,
        lakebase_job_id="a" * 64,
        competitor_job_id="b" * 64,
        warm_attempt_token="attempt-b4",
    )
    blocked = Round5WarmSlot(
        installation_id="install-one",
        generation=1,
        state=Round5WarmState.BLOCKED,
        revision=1,
        coordinator_fence=1,
        process_epoch="process-one",
        broker_epoch="broker-one",
        warm_contract_sha256=DIGEST,
        warming_started_at=clock.now,
        claim=claim,
        last_error_code="warm_baseline_unexpected",
        last_error_at=clock.now,
        coordinator_owner="process-one",
        coordinator_lease_expires_at=clock.now + timedelta(seconds=90),
    )
    coord.store._slots["install-one"] = blocked
    coord._last_slot = blocked

    prepare_before = provider.prepare_calls
    await coord.run_one_cycle()
    slot = await coord.store.read("install-one")
    assert slot is not None
    # Routed into cleanup and converged to N+1 WARMING -- NOT normalized/prepped.
    assert slot.state == Round5WarmState.WARMING
    assert slot.generation == 2
    assert provider.reconciled_abandoned_claim is not None
    assert provider.reconciled_abandoned_claim.claim_id == "claim-b4"
    assert provider.prepare_calls == prepare_before  # never prepared over the debt

    # A BLOCKED slot with NO claim/debt stays terminal (unchanged e54 behavior).
    assert _blocked_is_terminal(
        replace(blocked, claim=None, requires_cleaned_bout=False)
    ) is True


# ---------------------------------------------------------------------------
# Blocker 1 / Finding 3. Durable post-bell truth: even if the local cache never
# learned the bell, cleanup adopts the committed RUNNING slot -- and, with a
# coordinator configured, performs NO external mutation itself (single owner).
# ---------------------------------------------------------------------------


async def test_b1_durable_running_slot_adopted_and_manager_does_not_mutate() -> None:
    clock = Clock()
    provider = Provider(clock)
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-b1",
        bout_id="bout-b1",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    # Accept the bell durably (committed RUNNING slot with a bell id).
    context = await coord.accept_bell(claimed.claim.claim_id)
    assert context.bell_id
    durable = await coord.store.read("install-one")
    assert durable is not None and durable.state == Round5WarmState.RUNNING

    engine = _CleanupEngineSpy()
    # The LOCAL record never learned the bell (stale CLAIMED cache, bell_context None),
    # simulating a crash between the committed accept_bell and the local update.
    stale_slot = SimpleNamespace(
        claim=SimpleNamespace(claim_id=claimed.claim.claim_id),
        bell_id=None,
        bell_at_utc=None,
    )
    record = SimpleNamespace(
        connection_spike_engine=engine,
        connection_spike_arm=None,
        round5_warm_slot=stale_slot,
        round5_bell_context=None,
        snapshot=SimpleNamespace(id="session-b1"),
        connection_spike_setup_result=object(),
    )
    fake_self = SimpleNamespace(
        _round5_warm_coordinator=coord,
        _round5_prebell_cleanup_required=RunManager._round5_prebell_cleanup_required,
    )
    import types as _types

    fake_self._refresh_round5_slot_from_durable = _types.MethodType(
        RunManager._refresh_round5_slot_from_durable, fake_self
    )

    ok = await RunManager._cleanup_connection_spike(fake_self, record)
    assert ok is True
    # Durable truth was adopted: the stale CLAIMED/null-bell cache is replaced by
    # the committed RUNNING slot (post-bell), so a later classification is correct.
    assert getattr(record.round5_warm_slot, "bell_id", None) is not None
    # Single owner: with a coordinator configured the manager performs NO external
    # cleanup mutation here -- the provider/converge owns the post-setup teardown.
    assert engine.cancel_setup_called is False
    assert engine.settle_abandoned_called is False


# ---------------------------------------------------------------------------
# 5ecc-follow-up: single cleanup owner. The manager request and the supervised
# loop converge the SAME claim to exactly ONE reconcile sequence and one N+1.
# ---------------------------------------------------------------------------


async def test_owner_manager_and_loop_converge_once_to_n_plus_1() -> None:
    clock = Clock()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-owner",
        bout_id="bout-owner",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    generation = claimed.generation
    coord.release_claim_active(claimed.claim.claim_id)
    await coord.begin_cleanup(claimed.claim.claim_id)
    provider.reconcile_calls = 0

    # A manager-style converge request and the supervised loop (which routes its
    # CLEANING branch through the SAME converge_cleanup) fire simultaneously.
    _, cycle_delay = await asyncio.gather(
        coord.converge_cleanup(claimed.claim.claim_id),
        coord.run_one_cycle(),
    )
    # Exactly ONE external reconcile sequence, exactly one N+1 transition.
    assert provider.reconcile_calls == 1
    slot = await coord.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.generation == generation + 1


async def test_owner_takeover_old_process_issues_no_further_mutation() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider_a = _RecordingProvider(clock)
    provider_b = _RecordingProvider(clock)
    provider_b.reconcile_result = True
    process_a = coordinator(clock, provider_a, store, process_epoch="process-a")
    process_b = coordinator(clock, provider_b, store, process_epoch="process-b")

    await warm_ready(process_a, provider_a)
    claimed, _ = await process_a.claim(
        session_id="session-takeover",
        bout_id="bout-takeover",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    process_a.release_claim_active(claimed.claim.claim_id)
    await process_a.begin_cleanup(claimed.claim.claim_id)  # CLEANING, owned by A

    # process-b seizes coordination once A's lease can expire.
    clock.advance(process_a._coordinator_ttl.total_seconds() + 1)
    await store.acquire_coordinator(
        installation_id="install-one",
        process_epoch="process-b",
        broker_epoch="broker-b",
        now=clock.now,
        ttl=process_a._coordinator_ttl,
    )
    provider_a.reconcile_calls = 0

    # The OLD process can no longer converge -- it is refused before any external
    # mutation (authority moved), so it issues zero reconciles.
    with pytest.raises(WarmFenceLostError):
        await process_a.converge_cleanup(claimed.claim.claim_id)
    assert provider_a.reconcile_calls == 0

    # The NEW owner converges the inherited claim to N+1.
    await process_b.run_one_cycle()
    slot = await store.read("install-one")
    assert slot is not None
    assert slot.coordinator_owner == "process-b"
    assert slot.state == Round5WarmState.WARMING
    assert slot.generation == claimed.generation + 1


async def test_owner_converge_failure_never_strands_cleaning_silently() -> None:
    clock = Clock()

    class _RaisingReconcileProvider(_RecordingProvider):
        raise_once = True

        async def reconcile(self, slot) -> bool:
            self.reconcile_calls += 1
            # Only fail the CLEANUP reconcile (a CLEANING slot); the warm loop also
            # calls reconcile on a WARMING slot and must not be disrupted.
            if getattr(slot, "state", None) != Round5WarmState.CLEANING:
                return True
            if self.raise_once:
                self.raise_once = False
                raise BlockedWarmError("cleanup_reconcile_blocked")
            claim = getattr(slot, "claim", None)
            if claim is not None and getattr(slot, "bell_id", None) is None:
                self.reconciled_abandoned_claim = claim
            return True

    provider = _RaisingReconcileProvider(clock)
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-strand",
        bout_id="bout-strand",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    generation = claimed.generation
    coord.release_claim_active(claimed.claim.claim_id)
    await coord.begin_cleanup(claimed.claim.claim_id)

    # A convergence whose reconcile RAISES must not strand the slot: it stays
    # CLEANING (owed + retryable), never silently WARMING/BLOCKED.
    with pytest.raises(BlockedWarmError):
        await coord.converge_cleanup(claimed.claim.claim_id)
    stranded = await coord.store.read("install-one")
    assert stranded is not None
    assert stranded.state == Round5WarmState.CLEANING
    assert stranded.claim is not None and stranded.claim.claim_id == claimed.claim.claim_id

    # A subsequent convergence settles it and advances to N+1 -- same process.
    settled = await coord.converge_cleanup(claimed.claim.claim_id)
    assert settled.state == Round5WarmState.WARMING
    assert settled.generation == generation + 1
    assert provider.reconciled_abandoned_claim is not None


# ---------------------------------------------------------------------------
# Finding 2: coalescing lifetime is owned by task completion, not the caller.
# ---------------------------------------------------------------------------


class _BlockingReconcileProvider(_RecordingProvider):
    """Cleanup reconcile blocks on an event so creator cancellation can be raced."""

    def __init__(self, clock: Clock) -> None:
        super().__init__(clock)
        self.reconcile_started = asyncio.Event()
        self.release_reconcile = asyncio.Event()
        self.reconcile_result = True
        self.raise_on_reconcile: Exception | None = None

    async def reconcile(self, slot) -> bool:
        if getattr(slot, "state", None) != Round5WarmState.CLEANING:
            return True  # warm-path reconcile is not the subject here
        self.reconcile_calls += 1
        self.reconcile_started.set()
        await self.release_reconcile.wait()
        if self.raise_on_reconcile is not None:
            raise self.raise_on_reconcile
        claim = getattr(slot, "claim", None)
        if claim is not None and getattr(slot, "bell_id", None) is None:
            self.reconciled_abandoned_claim = claim
        return True


async def _cleaning_coordinator(clock, provider):
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-f2",
        bout_id="bout-f2",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    coord.release_claim_active(claimed.claim.claim_id)
    await coord.begin_cleanup(claimed.claim.claim_id)
    return coord, claimed.claim.claim_id


async def test_f2_creator_cancel_then_third_call_stays_one_reconcile() -> None:
    clock = Clock()
    provider = _BlockingReconcileProvider(clock)
    coord, cid = await _cleaning_coordinator(clock, provider)
    provider.reconcile_calls = 0

    creator = asyncio.create_task(coord.converge_cleanup(cid))
    joiner = asyncio.create_task(coord.converge_cleanup(cid))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)

    # Cancel the CREATOR while the joiner waits and the shared reconcile runs.
    creator.cancel()
    with pytest.raises(asyncio.CancelledError):
        await creator
    # A third call arriving immediately must coalesce onto the SAME running task.
    third = asyncio.create_task(coord.converge_cleanup(cid))
    await asyncio.sleep(0)

    provider.release_reconcile.set()
    r_join, r_third = await asyncio.gather(joiner, third)
    # Exactly ONE reconcile despite creator cancellation + an immediate third call.
    assert provider.reconcile_calls == 1
    assert r_join.state == Round5WarmState.WARMING
    assert r_third.state == Round5WarmState.WARMING


async def test_f2_exception_removes_task_so_retry_can_start() -> None:
    clock = Clock()
    provider = _BlockingReconcileProvider(clock)
    provider.release_reconcile.set()
    provider.raise_on_reconcile = BlockedWarmError("cleanup_reconcile_blocked")
    coord, cid = await _cleaning_coordinator(clock, provider)
    provider.reconcile_calls = 0

    with pytest.raises(BlockedWarmError):
        await coord.converge_cleanup(cid)
    await asyncio.sleep(0)  # let the done-callback clear the registry entry
    assert cid not in coord._cleanup_tasks

    # A retry after the failure starts a FRESH reconcile (not coalesced onto a dead
    # task) and converges.
    provider.raise_on_reconcile = None
    result = await coord.converge_cleanup(cid)
    assert result.state == Round5WarmState.WARMING
    assert provider.reconcile_calls == 2


async def test_f2_inherited_restart_concurrent_manager_and_loop_stays_one() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider_a = _RecordingProvider(clock)
    provider_b = _RecordingProvider(clock)
    provider_b.reconcile_result = True
    process_a = coordinator(clock, provider_a, store, process_epoch="process-a")
    process_b = coordinator(clock, provider_b, store, process_epoch="process-b")
    await warm_ready(process_a, provider_a)
    claimed, _ = await process_a.claim(
        session_id="session-f2r",
        bout_id="bout-f2r",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    cid = claimed.claim.claim_id
    # Restart: B inherits the CLAIMED slot. Its run_one_cycle (inherited path, now
    # routed through converge) and a concurrent manager-style converge collapse to one.
    clock.advance(process_a._coordinator_ttl.total_seconds() + 1)
    provider_b.reconcile_calls = 0
    _, delay = await asyncio.gather(
        process_b.converge_cleanup(cid),
        process_b.run_one_cycle(),
    )
    assert provider_b.reconcile_calls == 1
    slot = await store.read("install-one")
    assert slot is not None and slot.state == Round5WarmState.WARMING
    assert slot.generation == claimed.generation + 1


# ---------------------------------------------------------------------------
# Finding 1: a stale cleanup lease is retired once another owner advances to N+1.
# ---------------------------------------------------------------------------


async def test_f1_stale_cleanup_lease_retired_after_owner_advances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "10")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "10")
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider_a = _EngineProvider(clock, _RecoverableAbandonPlan)
    provider_b = _EngineProvider(clock, _RecoverableAbandonPlan)
    process_a = Round5WarmCoordinator(
        installation_id="install-acceptance",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=provider_a,
        process_epoch="process-a",
        broker_epoch="broker-a",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    process_b = Round5WarmCoordinator(
        installation_id="install-acceptance",
        warm_contract_sha256=DIGEST,
        store=store,
        provider=provider_b,
        process_epoch="process-b",
        broker_epoch="broker-b",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await _warm_to_ready(process_a, provider_a)
    manager = _make_manager(process_a, clock, round_isolation=True)
    manager._armed_ttl = 0.01
    app = _asgi(manager)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)
            first_generation = (await store.read("install-acceptance")).generation
            for _ in range(400):
                snap = (await client.get(f"/api/sessions/{session_id}")).json()
                if snap["state"] == SessionState.FAILED.value:
                    break
                await asyncio.sleep(0.01)
            record = manager._records[session_id]
            # A stale cleanup lease is held and the slot is CLEANING.
            assert await manager._round5_cleanup_store().current() is not None
            cleaning = await store.read("install-acceptance")
            assert cleaning is not None and cleaning.state == Round5WarmState.CLEANING
            claim_id = cleaning.claim.claim_id

            # ANOTHER owner (process-b) seizes coordination and finishes the cleanup,
            # advancing the durable head to a claimless N+1 (WARMING).
            clock.advance(process_a._coordinator_ttl.total_seconds() + 1)
            await process_b.store.acquire_coordinator(
                installation_id="install-acceptance",
                process_epoch="process-b",
                broker_epoch="broker-b",
                now=clock.now,
                ttl=process_b._coordinator_ttl,
            )
            b_cleaning = await store.read("install-acceptance")
            advanced = await store.finish_cleanup_and_rewarm(
                b_cleaning, claim_id=claim_id, now=clock.now
            )
            assert (
                advanced.state == Round5WarmState.WARMING
                and advanced.generation == first_generation + 1
                and advanced.claim is None
            )

            # The stale process-a manager now detects completion+advance and RETIRES
            # its local claim/lease -- releasing the cleanup lease so the overlay is
            # startable again -- instead of WarmFenceLost-looping forever.
            retired = await manager._retry_connection_spike_cleanup(
                record, record.connection_spike_engine
            )
            assert retired is True
            assert await manager._round5_cleanup_store().current() is None
            assert manager.round5_cleanup_owed is False
    finally:
        await manager.close()
        await process_a.close()
        await process_b.close()


# ---------------------------------------------------------------------------
# Finding 3: with a coordinator configured the manager performs ZERO external
# cleanup mutation (no settle/cancel/janitor) -- the provider is the sole janitor.
# ---------------------------------------------------------------------------


async def test_f3_manager_runs_no_external_janitor_when_coordinator_present() -> None:
    clock = Clock()
    provider = Provider(clock)
    coord = coordinator(clock, provider)
    await coord.store.initialize()

    engine = _CleanupEngineSpy()
    # A post-bell-shaped record WITH an arm present: pre-fix this reached the full
    # engine.cancel_and_cleanup janitor while the coordinator reconciled a fresh
    # engine (dual mutation). With a coordinator it must now be a pure no-op.
    record = _cleanup_record(engine, bell_id="bell-x", bell_at_utc=object(), arm=object())
    fake_self = SimpleNamespace(
        _round5_warm_coordinator=coord,
        _round5_prebell_cleanup_required=RunManager._round5_prebell_cleanup_required,
    )
    import types as _types

    fake_self._refresh_round5_slot_from_durable = _types.MethodType(
        RunManager._refresh_round5_slot_from_durable, fake_self
    )

    ok = await RunManager._cleanup_connection_spike(fake_self, record)
    assert ok is True
    assert engine.settle_abandoned_called is False
    assert engine.cancel_and_cleanup_called is False
    assert engine.cancel_setup_called is False


# ---------------------------------------------------------------------------
# Zero-external-mutation invariant: with a coordinator, EVERY manager cleanup
# trigger (post-bell, refused-bell, pre-arm handoff, no-bell) must delegate to
# the coordinator's single owner and issue NO external resource mutation of its
# own. Each test below fails if its guard is reverted (mutation-verified).
# ---------------------------------------------------------------------------


def _handoff_self(coordinator) -> SimpleNamespace:
    import types as _types

    async def _noop(*_args, **_kwargs) -> None:
        return None

    fake = SimpleNamespace(
        _round5_warm_coordinator=coordinator,
        _round5_local_cancel_timeout=5.0,
        _mark_connection_spike_cleanup_in_progress=_noop,
        _retain_connection_spike_cleanup_lease=_noop,
        _mark_connection_spike_cleanup_pending=_noop,
        _complete_connection_spike_cleanup_handoff=_noop,
    )
    del _types
    return fake


async def test_zero_external_mutation_post_bell() -> None:
    """post-bell (_cleanup_connection_spike): a coordinator makes it a pure no-op
    -- no settle/cancel of external residents by the manager."""

    engine = _CleanupEngineSpy()
    coordinator = _FakeCleanupCoordinator()
    record = _cleanup_record(engine, bell_id="bell-x", bell_at_utc=object(), arm=object())
    import types as _types

    fake_self = SimpleNamespace(
        _round5_warm_coordinator=coordinator,
        _round5_prebell_cleanup_required=RunManager._round5_prebell_cleanup_required,
    )
    fake_self._refresh_round5_slot_from_durable = _types.MethodType(
        RunManager._refresh_round5_slot_from_durable, fake_self
    )
    ok = await RunManager._cleanup_connection_spike(fake_self, record)
    assert ok is True
    assert engine.settle_abandoned_called is False
    assert engine.cancel_and_cleanup_called is False
    assert engine.cancel_setup_called is False
    assert engine.stop_and_begin_cleanup_called is False
    assert engine.stop_setup_and_begin_cleanup_called is False


async def test_zero_external_mutation_prearm_handoff() -> None:
    """pre-arm handoff (_begin_connection_spike_cleanup_handoff): with a
    coordinator + claim the manager fences durable CLEANING and cancels ONLY its
    local in-process tasks; it must never call the engine's external
    stop/settle."""

    engine = _CleanupEngineSpy()
    coordinator = _FakeCleanupCoordinator()
    fake_self = _handoff_self(coordinator)
    # A live pre-existing cleanup task short-circuits the trailing task schedule,
    # keeping the unit test free of background work.
    pending = asyncio.create_task(asyncio.Event().wait())
    try:
        record = SimpleNamespace(
            connection_spike_engine=engine,
            operator=object(),
            task=None,
            round5_warm_slot=SimpleNamespace(
                claim=SimpleNamespace(claim_id="claim-h")
            ),
            connection_spike_arm=object(),
            snapshot=SimpleNamespace(
                id="session-h",
                state=SessionState.VERIFIED,
                round5_setup=SimpleNamespace(setup_validated=True),
            ),
            connection_spike_cleanup_task=pending,
        )
        await RunManager._begin_connection_spike_cleanup_handoff(fake_self, record)
    finally:
        pending.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pending

    assert coordinator.begin_cleanup_calls == ["claim-h"]
    assert engine.cancel_local_called is True  # local-only cancellation is allowed
    assert engine.stop_and_begin_cleanup_called is False
    assert engine.stop_setup_and_begin_cleanup_called is False
    assert engine.settle_abandoned_called is False


async def test_zero_external_mutation_no_bell_and_refused_bell() -> None:
    """no-bell + refused-bell both route through _begin_round5_warm_cleanup, which
    fences durable CLEANING through the coordinator and touches the engine NOT AT
    ALL (the provider is the sole janitor)."""

    engine = _CleanupEngineSpy()
    coordinator = _FakeCleanupCoordinator()
    fake_self = SimpleNamespace(_round5_warm_coordinator=coordinator)
    record = SimpleNamespace(
        connection_spike_engine=engine,
        round5_warm_slot=SimpleNamespace(claim=SimpleNamespace(claim_id="claim-n")),
        snapshot=SimpleNamespace(id="session-n"),
    )
    ok = await RunManager._begin_round5_warm_cleanup(fake_self, record)
    assert ok is True
    assert coordinator.released_active == ["claim-n"]
    assert coordinator.begin_cleanup_calls == ["claim-n"]
    # The engine was never touched by the manager on either trigger.
    assert engine.settle_abandoned_called is False
    assert engine.cancel_and_cleanup_called is False
    assert engine.cancel_setup_called is False
    assert engine.stop_and_begin_cleanup_called is False
    assert engine.stop_setup_and_begin_cleanup_called is False


# ---------------------------------------------------------------------------
# Single-owner follow-ups: post-bell handoff, adoption cap, shielded-task hygiene.
# ---------------------------------------------------------------------------


async def test_post_bell_handoff_retry_and_supervised_loop_one_reconcile() -> None:
    clock = Clock()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-handoff",
        bout_id="bout-handoff",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    generation = claimed.generation
    await coord.accept_bell(claimed.claim.claim_id)
    coord.release_claim_active(claimed.claim.claim_id)
    await coord.begin_cleanup(claimed.claim.claim_id)
    provider.reconcile_calls = 0

    _, _ = await asyncio.gather(
        coord.converge_cleanup(claimed.claim.claim_id),
        coord.run_one_cycle(),
    )
    assert provider.reconcile_calls == 1
    slot = await coord.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.generation == generation + 1


async def test_refused_bell_prearm_and_loop_share_one_converge_owner() -> None:
    clock = Clock()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-refused",
        bout_id="bout-refused",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    coord.release_claim_active(claimed.claim.claim_id)
    await coord.begin_cleanup(claimed.claim.claim_id)
    provider.reconcile_calls = 0
    _, _ = await asyncio.gather(
        coord.converge_cleanup(claimed.claim.claim_id),
        coord.run_one_cycle(),
    )
    assert provider.reconcile_calls == 1


def _bare_provider(cap: int = 8) -> LiveRound5WarmProvider:
    provider = object.__new__(LiveRound5WarmProvider)
    provider._adopted_engines = {}
    provider._adopted_engine_order = []
    provider._adopted_engine_cap_exceeded = 0
    provider._adopted_engine_high_water = 0
    provider.ADOPTED_ENGINE_SAFETY_CAP = cap
    return provider


def test_adopted_engine_registry_release_lifecycle_prunes_dead_entries() -> None:
    """The deterministic release lifecycle -- not a hard cap -- bounds the
    registry: an adopt/release pair leaves nothing behind, and ``release_all``
    empties it."""

    provider = _bare_provider(cap=8)
    cap = provider.ADOPTED_ENGINE_SAFETY_CAP
    for index in range(cap + 8):
        provider.adopt_claimed_engine(f"claim-{index}", SimpleNamespace())
        provider.release_adopted_engine(f"claim-{index}")
    assert len(provider._adopted_engines) == 0
    assert provider._adopted_engine_order == []

    for index in range(cap):
        provider.adopt_claimed_engine(f"hold-{index}", SimpleNamespace())
    provider.release_all_adopted_engines()
    assert len(provider._adopted_engines) == 0
    assert provider._adopted_engine_order == []


def test_adopted_engine_registry_never_evicts_live_engines_over_cap() -> None:
    """The registry cap is observability-ONLY: exceeding it must NEVER evict a
    live engine (which would lose that claim's ARM-staged resident bindings and
    re-introduce the warm_baseline_unexpected incident). With well over the cap
    of concurrently adopted/live engines, EVERY one is retained; the breach is
    surfaced via a counter/high-water mark, not by discarding cleanup state."""

    cap = 8
    provider = _bare_provider(cap=cap)
    live = cap + 57  # comfortably over a real >64 equivalent for cap=8
    engines: dict[str, SimpleNamespace] = {}
    for index in range(live):
        engine = SimpleNamespace()
        engines[f"live-{index}"] = engine
        provider.adopt_claimed_engine(f"live-{index}", engine)

    # NOTHING was evicted: every live claim's engine is still present and is the
    # exact object that was adopted (bindings intact), in adoption order.
    assert len(provider._adopted_engines) == live
    assert provider._adopted_engine_order == [f"live-{i}" for i in range(live)]
    for claim_id, engine in engines.items():
        assert provider._adopted_engines[claim_id] is engine

    # The breach is observable but non-destructive.
    assert provider._adopted_engine_cap_exceeded > 0
    assert provider._adopted_engine_high_water == live

    # Marking some engines as CLEANING (janitor-owned) and adopting still more
    # never evicts them either -- a live CLEANING engine is the LAST thing that
    # may be dropped.
    for index in range(0, live, 2):
        provider.transfer_adopted_engine_at_cleaning(f"live-{index}")
    for index in range(live, live + 5):
        provider.adopt_claimed_engine(f"extra-{index}", SimpleNamespace())
    assert len(provider._adopted_engines) == live + 5
    for index in range(0, live, 2):
        assert provider._adopted_engines[f"live-{index}"]._round5_cleanup_janitor_owned is True

    # Only the deterministic lifecycle removes entries.
    provider.release_all_adopted_engines()
    assert len(provider._adopted_engines) == 0


class _AdoptingProvider(_RecordingProvider):
    """A coordinator provider double that models the live provider's adoption/
    transfer contract and records the exact engine object each cleanup reconcile
    ran on, plus whether that engine was already transferred (janitor-owned) at
    reconcile time."""

    def __init__(self, clock: Clock) -> None:
        super().__init__(clock)
        self._adopted: dict[str, SimpleNamespace] = {}
        self.reconciled_engines: list[SimpleNamespace] = []
        self.reconciled_with_untransferred = False
        self.reconciled_with_fresh_engine = False

    def adopt_claimed_engine(self, claim_id: str, engine: SimpleNamespace) -> None:
        engine._round5_cleanup_janitor_owned = False
        self._adopted[str(claim_id)] = engine

    def transfer_adopted_engine_at_cleaning(self, claim_id: str) -> None:
        engine = self._adopted.get(str(claim_id))
        if engine is not None:
            engine._round5_cleanup_janitor_owned = True

    def release_adopted_engine(self, claim_id: str) -> None:
        self._adopted.pop(str(claim_id), None)

    def release_all_adopted_engines(self) -> None:
        self._adopted.clear()

    async def reconcile(self, slot) -> bool:
        if getattr(slot, "state", None) == Round5WarmState.CLEANING:
            claim = getattr(slot, "claim", None)
            engine = self._adopted.get(str(claim.claim_id)) if claim else None
            if engine is None:
                # A fresh (non-adopted) engine reconciling a live CLEANING claim
                # means the ARM-staged bindings were lost -- the race we forbid.
                self.reconciled_with_fresh_engine = True
            else:
                self.reconciled_engines.append(engine)
                if not getattr(engine, "_round5_cleanup_janitor_owned", False):
                    self.reconciled_with_untransferred = True
        return await super().reconcile(slot)


async def test_transfer_vs_loop_race_reconciles_only_the_adopted_transferred_engine() -> None:
    """Transfer/adoption must establish EXCLUSIVE ownership before CLEANING is
    converged by the loop -- no race window. With ``begin_cleanup`` (manager
    side) and ``run_one_cycle`` (supervised loop) racing to converge the same
    CLEANING slot, the SINGLE reconcile must run on the ADOPTED engine, that
    engine must already be transferred (janitor-owned) when reconciled, and no
    fresh engine may ever reconcile a live claim."""

    clock = Clock()
    provider = _AdoptingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-race",
        bout_id="bout-race",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    generation = claimed.generation
    await coord.accept_bell(claim_id)
    # The manager adopts its claimed engine at ARM, well before CLEANING.
    engine = SimpleNamespace()
    provider.adopt_claimed_engine(claim_id, engine)
    coord.release_claim_active(claim_id)

    # begin_cleanup (manager) transfers the adopted engine + fences CLEANING +
    # wakes; the loop races to converge the same CLEANING slot.
    await coord.begin_cleanup(claim_id)
    assert engine._round5_cleanup_janitor_owned is True  # transferred before wake
    provider.reconcile_calls = 0

    await asyncio.gather(
        coord.converge_cleanup(claim_id),
        coord.run_one_cycle(),
    )

    assert provider.reconcile_calls == 1
    assert provider.reconciled_engines == [engine]
    assert provider.reconciled_with_untransferred is False
    assert provider.reconciled_with_fresh_engine is False
    slot = await coord.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING
    assert slot.generation == generation + 1
    # The adopted engine was released deterministically on convergence.
    assert claim_id not in provider._adopted


async def test_f2_creator_cancel_does_not_leave_task_exception_unobserved(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = Clock()
    provider = _BlockingReconcileProvider(clock)
    provider.raise_on_reconcile = RuntimeError("forced reconcile failure")
    coord, cid = await _cleaning_coordinator(clock, provider)

    with caplog.at_level("ERROR"):
        creator = asyncio.create_task(coord.converge_cleanup(cid))
        await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)
        creator.cancel()
        with pytest.raises(asyncio.CancelledError):
            await creator
        provider.release_reconcile.set()
        await asyncio.sleep(0.05)

    assert any(
        "round5_cleanup_convergence_failed" in record.message
        for record in caplog.records
    )
    provider.raise_on_reconcile = None
    provider.reconcile_calls = 0
    warmed = await coord.converge_cleanup(cid)
    assert warmed.state == Round5WarmState.WARMING
    assert provider.reconcile_calls == 1


# ---------------------------------------------------------------------------
# Final race audit: three deploy-blocking lifecycle holes.
#   H1  arm failure after prepare/stage but before _mark_bout_armed
#   H2  shutdown during an in-flight converge
#   H3  transfer/wake ordering + staged residents post-bell + no-binding cancel
# Each test is mutation-sensitive: reverting its fix makes it fail.
# ---------------------------------------------------------------------------


async def test_h1_arm_failure_after_stage_enters_cleaning_and_converges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H1: an arm failure AFTER prepare staged the Lakebase resident but before
    ``_mark_bout_armed`` completes must fence durable CLEANING (retaining the claim
    + exact job ids) and converge through ONE provider janitor to N+1 -- never treat
    the manager's coordinator no-op as external cleanup proof and release the leases
    over an orphaned staged resident. No rewarm/startability is produced before the
    resident settlement is proven.

    Mutation-sensitive: with the old arm-failure path (``_cleanup_connection_spike``
    no-op True -> ``_fail``) the durable slot never reaches CLEANING and this fails.
    """

    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "0.02")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "0.05")
    clock = Clock()
    provider = _EngineProvider(clock, _RecoverableAbandonPlan)
    coord = _accept_coordinator(clock, provider)
    await _warm_to_ready(coord, provider)
    manager = _make_manager(coord, clock, round_isolation=True)
    app = _asgi(manager)

    # prepare() has already staged the resident by the time _mark_bout_armed runs;
    # force _mark_bout_armed to raise to model the exact arm-failure window.
    async def _boom(self, record, expires_at):  # noqa: ANN001
        raise InvalidStateError("arm failed after stage")

    monkeypatch.setattr(RunManager, "_mark_bout_armed", _boom)

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            armed = await client.post(f"/api/sessions/{session_id}/arm")
            assert armed.status_code == 200, armed.text

            for _ in range(400):
                snap = (await client.get(f"/api/sessions/{session_id}")).json()
                if snap["state"] == SessionState.FAILED.value:
                    break
                await asyncio.sleep(0.01)
            assert snap["state"] == SessionState.FAILED.value

            record = manager._records[session_id]
            claim = record.round5_warm_slot.claim
            assert claim is not None
            lakebase_job, competitor_job = claim.lakebase_job_id, claim.competitor_job_id

            # Fenced into durable CLEANING, retaining the claim + exact job ids.
            cleaning = await coord.store.read("install-acceptance")
            assert cleaning is not None and cleaning.state == Round5WarmState.CLEANING
            assert cleaning.claim is not None
            assert cleaning.claim.lakebase_job_id == lakebase_job
            assert cleaning.claim.competitor_job_id == competitor_job
            generation = cleaning.generation

            # While the resident is unsettled, the janitor keeps failing -> stays
            # CLEANING, no N+1, and the overlay is NOT startable before proof.
            plan = provider.engines[0]._plan
            for _ in range(50):
                if plan.durable_resident_reconcile_attempts >= 1:
                    break
                await asyncio.sleep(0.01)
            assert plan.durable_resident_reconcile_attempts >= 1
            still = await coord.store.read("install-acceptance")
            assert still is not None and still.state == Round5WarmState.CLEANING
            assert still.generation == generation
            board = (await client.get("/api/bout/all")).json()
            assert board["rounds"]["survive_connection_spike"]["can_start"] is False

            # Prove the resident settled -> single converge owner reaches N+1.
            plan.allow_resident_settle.set()
            for _ in range(400):
                warming = await coord.store.read("install-acceptance")
                if warming is not None and warming.generation == generation + 1:
                    break
                await asyncio.sleep(0.01)
            assert warming is not None and warming.generation == generation + 1

            await coord.run_one_cycle()  # WARMING -> READY (N+1), same process
            ready = await coord.store.read("install-acceptance")
            assert ready is not None and ready.state == Round5WarmState.READY
            assert ready.generation == generation + 1
    finally:
        await manager.close()
        await coord.close()


class _CloseDrainProvider(_RecordingProvider):
    """Blocks the cleanup reconcile and records the shutdown ordering so a test can
    prove close() drains the in-flight converge BEFORE releasing adopted engines
    and closing the store."""

    def __init__(self, clock: Clock, events: list[str]) -> None:
        super().__init__(clock)
        self.reconcile_started = asyncio.Event()
        self.release_reconcile = asyncio.Event()
        self.reconcile_cancelled = False
        self.events = events
        self._adopted: dict[str, object] = {}

    def adopt_claimed_engine(self, claim_id: str, engine: object) -> None:
        self._adopted[str(claim_id)] = engine

    def transfer_adopted_engine_at_cleaning(self, claim_id: str) -> None:
        return None

    def release_adopted_engine(self, claim_id: str) -> None:
        self._adopted.pop(str(claim_id), None)

    def release_all_adopted_engines(self) -> None:
        self.events.append("release_all")
        self._adopted.clear()

    async def reconcile(self, slot) -> bool:
        if getattr(slot, "state", None) != Round5WarmState.CLEANING:
            return True
        self.reconcile_calls += 1
        self.reconcile_started.set()
        try:
            await self.release_reconcile.wait()
        except asyncio.CancelledError:
            self.reconcile_cancelled = True
            self.events.append("reconcile_cancelled")
            raise
        self.events.append("reconcile_done")
        return True


class _CloseRecordingStore(InMemoryRound5WarmStore):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events_log = events

    async def close(self) -> None:
        self._events_log.append("store_close")
        return await super().close()


async def test_h2_close_waits_for_inflight_converge_then_releases_and_closes() -> None:
    """H2 / Finding B: coordinator.close() must NOT cancel an in-flight converge (a
    cancel cannot stop a shielded AWS call and would drop the fence while it runs).
    It must keep the fence alive (the task's own heartbeat) and WAIT for the converge
    to actually finish, and only THEN revoke authority, release adopted engines, and
    close the store -- in that exact order.

    Mutation-sensitive: if close() released adoption / closed the store before the
    in-flight converge finished, the recorded order would not be
    ``[reconcile_done, release_all, store_close]`` and adoption would not still be
    held while the reconcile is in flight.
    """

    clock = Clock()
    events: list[str] = []
    store = _CloseRecordingStore(events)
    provider = _CloseDrainProvider(clock, events)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-close",
        bout_id="bout-close",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    engine = SimpleNamespace()
    coord.adopt_claimed_engine(claim_id, engine)
    coord.release_claim_active(claim_id)
    await coord.begin_cleanup(claim_id)

    converge = asyncio.create_task(coord.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)

    close_task = asyncio.create_task(coord.close())
    for _ in range(5):
        await asyncio.sleep(0)
        if coord._closed:
            break
    # close is BLOCKING on the in-flight converge: it has NOT revoked authority, NOT
    # released adoption, and NOT closed the store while the reconcile is in flight.
    assert coord._closed is True
    assert not close_task.done()
    assert coord._authority_revoked is False
    assert claim_id in provider._adopted
    assert events == []

    # Let the (shielded-AWS-modelling) reconcile finish; only now may close proceed.
    provider.release_reconcile.set()
    await asyncio.wait_for(close_task, timeout=2)

    assert provider.reconcile_cancelled is False
    assert events == ["reconcile_done", "release_all", "store_close"]
    assert coord._authority_revoked is True
    assert provider._adopted == {}

    warmed = await asyncio.wait_for(converge, timeout=2)
    assert warmed.state == Round5WarmState.WARMING


async def test_h2b_close_empty_snapshot_still_revokes_and_refuses_new_converge() -> None:
    """Ops High: close() must revoke authority and refuse new cleanup work even when
    the point-in-time ``_cleanup_tasks`` snapshot is EMPTY. Otherwise a converge that
    arrives after the snapshot registers a task, passes the authority guard, and
    mutates the durable slot AFTER ``store.close()`` / ``release_all_adopted_engines``.

    Mutation-sensitive: if authority is revoked only when the snapshot had pending
    tasks (the reported bug), ``_authority_revoked`` stays False here; if
    ``converge_cleanup`` does not refuse once closing, it registers a task, reconciles
    and advances the slot past CLEANING after store close -- both assertions below
    fail.
    """

    clock = Clock()
    events: list[str] = []
    store = _CloseRecordingStore(events)
    provider = _CloseDrainProvider(clock, events)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-close-empty",
        bout_id="bout-close-empty",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord.adopt_claimed_engine(claim_id, SimpleNamespace())
    coord.release_claim_active(claim_id)
    await coord.begin_cleanup(claim_id)

    # No in-flight converge -> the snapshot at close time is EMPTY.
    assert not [t for t in coord._cleanup_tasks.values() if not t.done()]
    await coord.close()

    # Authority revoked + closed + store closed even though the snapshot was empty.
    assert coord._authority_revoked is True
    assert coord._closed is True
    assert "store_close" in events

    # A converge (or begin_cleanup) arriving AFTER close is refused: no new task, no
    # reconcile, no mutation. Release the reconcile gate first so that IF the refusal
    # regressed the reconcile would run to completion (and be caught) rather than hang.
    # wait_for bounds it so a missing refuse-guard fails FAST rather than blocking on
    # the _CloseDrainProvider reconcile.
    provider.release_reconcile.set()
    reconcile_before = provider.reconcile_calls
    with pytest.raises(WarmFenceLostError):
        await asyncio.wait_for(coord.converge_cleanup(claim_id), timeout=2)
    assert provider.reconcile_calls == reconcile_before
    assert claim_id not in coord._cleanup_tasks
    with pytest.raises(WarmFenceLostError):
        await asyncio.wait_for(coord.begin_cleanup(claim_id), timeout=2)

    # Durable CLEANING (with claim) left for takeover -- no mutation after store close.
    slot = await store.read(coord.installation_id)
    assert slot is not None
    assert slot.state == Round5WarmState.CLEANING
    assert slot.claim is not None


async def test_h2c_converge_arriving_during_close_is_refused_no_task_no_mutation() -> None:
    """A converge that races an in-progress close() (after close's snapshot, while it
    is still WAITING for the in-flight blocked reconcile) is refused -- creating no
    new task and no mutation. Authority is revoked only AFTER the in-flight op
    finishes (Finding B), never mid-flight.
    """

    clock = Clock()
    events: list[str] = []
    store = _CloseRecordingStore(events)
    provider = _CloseDrainProvider(clock, events)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-close-race",
        bout_id="bout-close-race",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord.adopt_claimed_engine(claim_id, SimpleNamespace())
    coord.release_claim_active(claim_id)
    await coord.begin_cleanup(claim_id)

    # One in-flight converge, blocked inside reconcile.
    converge1 = asyncio.create_task(coord.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)

    close_task = asyncio.create_task(coord.close())
    # Let close run its critical section (set flag + snapshot) and begin WAITING for
    # the blocked converge to finish. Authority is NOT revoked while it waits.
    for _ in range(5):
        await asyncio.sleep(0)
        if coord._closed:
            break
    assert coord._closed is True
    assert not close_task.done()
    assert coord._authority_revoked is False

    # A converge arriving now (after the snapshot) is refused -> no new task. wait_for
    # bounds it so a missing refuse-guard fails FAST instead of blocking on the
    # _CloseDrainProvider reconcile.
    with pytest.raises(WarmFenceLostError):
        await asyncio.wait_for(coord.converge_cleanup(claim_id), timeout=2)
    assert not close_task.done()

    # Release the in-flight op; close then completes, revoking authority last.
    provider.release_reconcile.set()
    await asyncio.wait_for(close_task, timeout=2)
    warmed = await asyncio.wait_for(converge1, timeout=2)
    assert warmed.state == Round5WarmState.WARMING

    # Exactly one reconcile ever started (the in-flight one); the refused converge
    # created none. Store closed AFTER adoption release + reconcile completion.
    assert provider.reconcile_cancelled is False
    assert provider.reconcile_calls == 1
    assert events == ["reconcile_done", "release_all", "store_close"]
    assert coord._authority_revoked is True


async def test_h3_handoff_cancels_local_before_begin_cleanup_wakes_loop() -> None:
    """H3 ordering: the handoff must cancel local in-process lane bursts/run tasks
    BEFORE begin_cleanup makes the slot CLEANING visible and wakes the loop.

    Mutation-sensitive: if begin_cleanup runs before the local cancellation, the
    recorded order flips and this fails.
    """

    order: list[str] = []
    engine = _CleanupEngineSpy()

    async def _record_cancel() -> None:
        order.append("cancel_local")

    engine.cancel_local_round5_run_tasks = _record_cancel  # type: ignore[assignment]

    class _OrderedCoordinator(_FakeCleanupCoordinator):
        async def begin_cleanup(self, claim_id: str):
            order.append("begin_cleanup")
            return await super().begin_cleanup(claim_id)

    coordinator_double = _OrderedCoordinator()
    fake_self = _handoff_self(coordinator_double)
    pending = asyncio.create_task(asyncio.Event().wait())
    try:
        record = SimpleNamespace(
            connection_spike_engine=engine,
            operator=object(),
            task=None,
            round5_warm_slot=SimpleNamespace(
                claim=SimpleNamespace(claim_id="claim-order")
            ),
            connection_spike_arm=object(),
            snapshot=SimpleNamespace(
                id="session-order",
                state=SessionState.VERIFIED,
                round5_setup=SimpleNamespace(setup_validated=True),
            ),
            connection_spike_cleanup_task=pending,
        )
        await RunManager._begin_connection_spike_cleanup_handoff(fake_self, record)
    finally:
        pending.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pending

    assert order == ["cancel_local", "begin_cleanup"]


async def test_h3_post_bell_cleanup_settles_staged_residents_and_bursts_with_setup_result() -> None:
    """H3 provider: post-bell cleanup MUST settle ARM-staged residents AND cancel
    still-running lane bursts even when ``_setup_result`` already exists, running the
    complete ``_stop_setup_and_begin_cleanup_once`` teardown exactly once.

    Mutation-sensitive: the old narrow ``_stop_and_begin_cleanup_once()`` skipped
    staged residents and bursts, so ``settled``/``burst_cancelled`` would be empty.
    """

    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._cleanup_bout_id = None
    engine._setup_task = None
    engine._active_run_ids = {}
    engine._run_burst_tasks = set()
    engine._cleanup_start_lock = asyncio.Lock()
    settled: list[str] = []

    class _Adapter:
        async def cancel_resident(self, *, binding) -> None:
            settled.append(binding.job_id)

        async def cancel_job(self, job_id) -> None:  # pragma: no cover - not hit here
            settled.append(f"job:{job_id}")

    engine._lane_adapters = {"lakebase": _Adapter(), "competitor": _Adapter()}
    engine._resident_bindings = {
        "lakebase": SimpleNamespace(job_id="lb-1"),
        "competitor": SimpleNamespace(job_id="cp-1"),
    }

    burst_cancelled = asyncio.Event()

    async def _burst() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            burst_cancelled.set()
            raise

    engine._lane_bursts = {"lakebase": asyncio.create_task(_burst())}
    await asyncio.sleep(0)  # let the burst reach its await before cleanup cancels it

    cleanup_bouts: list[str] = []

    class _Orch:
        async def begin_cleanup(self, bout_id) -> None:
            cleanup_bouts.append(bout_id)

    engine._setup_orchestrator = _Orch()
    engine._setup_result = SimpleNamespace(bout_id="bout-1")

    claim = SimpleNamespace(bout_id="bout-1")
    await engine._ensure_post_bell_provider_cleanup_started(claim)
    # Idempotent: a second call performs no second cleanup.
    await engine._ensure_post_bell_provider_cleanup_started(claim)

    assert set(settled) == {"lb-1", "cp-1"}  # staged residents SETTLED
    assert burst_cancelled.is_set()  # active burst cancelled
    assert cleanup_bouts == ["bout-1"]  # exactly one complete setup teardown
    assert engine._cleanup_bout_id == "bout-1"
    assert engine._resident_bindings == {}


async def test_h3_cancel_resident_lane_no_binding_uses_cancel_job_not_typeerror() -> None:
    """H3 TypeError fix: a lane with no staged binding must settle the durable job
    via ``cancel_job`` (the binding-only real adapter would raise TypeError on the
    legacy ``generation/lane_id/job_id`` kwargs)."""

    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._resident_bindings = {}
    engine._warm_generation = 1
    calls: list[tuple[str, object]] = []

    class _Adapter:
        async def cancel_resident(self, *, binding) -> None:  # binding-only, like prod
            calls.append(("resident", binding))

        async def cancel_job(self, job_id) -> None:
            calls.append(("job", job_id))

    engine._lane_adapters = {"lakebase": _Adapter()}

    await engine._cancel_resident_lane("lakebase", "run-xyz")
    assert calls == [("job", "run-xyz")]


# ---------------------------------------------------------------------------
# e33bb3c race-audit expansion: Findings A (towel/run bursts), B (shielded boto
# vs close), C (LeaseLostError on _mark_bout_armed), plus the bounded
# cancel_local timeout/except wrapper coverage. Each is mutation-sensitive.
# ---------------------------------------------------------------------------


async def test_finding_a_cancel_local_cancels_launched_bursts() -> None:
    """Finding A: run() pops _lane_bursts into a local ``launched`` map (mirrored in
    _run_burst_tasks), so cancelling only _lane_bursts left the ACTUALLY-running bursts
    alive and able to dispatch after CLEANING. cancel_local_round5_run_tasks must
    cancel the mirrored launched bursts too.

    Mutation-sensitive: if cancel_local ignores _run_burst_tasks, the launched burst is
    never cancelled and this fails.
    """

    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._setup_task = None
    engine._lane_bursts = {}
    dispatched: list[str] = []
    burst_cancelled = asyncio.Event()

    async def _burst() -> None:
        try:
            await asyncio.Event().wait()
            dispatched.append("dispatched-after-cleaning")  # only if never cancelled
        except asyncio.CancelledError:
            burst_cancelled.set()
            raise

    task = asyncio.create_task(_burst())
    await asyncio.sleep(0)  # let the burst reach its await (i.e. it is running)
    engine._run_burst_tasks = {task}

    await engine.cancel_local_round5_run_tasks()

    assert burst_cancelled.is_set()
    assert dispatched == []
    assert task.cancelled()


def test_finding_a_run_mirrors_bursts_into_run_burst_tasks() -> None:
    """run() must mirror the launched bursts into _run_burst_tasks so a concurrent
    towel/abandon can reach and cancel them (structural backstop for Finding A)."""

    import inspect

    source = inspect.getsource(LiveConnectionSpikeEngine.run)
    assert "self._run_burst_tasks = {" in source
    # cancel_local delegates to the shared _cancel_local_bursts chokepoint, which is
    # what actually cancels both _lane_bursts and the run()-popped _run_burst_tasks.
    cancel_source = inspect.getsource(
        LiveConnectionSpikeEngine.cancel_local_round5_run_tasks
    )
    assert "_cancel_local_bursts" in cancel_source
    # Assert the ACTUAL cancel-set construction (not a mere docstring mention): both
    # registries are read and unpacked into the set that is cancelled+awaited.
    chokepoint = inspect.getsource(LiveConnectionSpikeEngine._cancel_local_bursts)
    assert 'getattr(self, "_run_burst_tasks"' in chokepoint
    assert "*run_bursts" in chokepoint
    assert "*lane_bursts.values()" in chokepoint


async def test_finding_b_close_holds_fence_until_inflight_reconcile_finishes() -> None:
    """Finding B: a reconcile may hold a shielded, un-cancellable AWS call. close()
    must keep the coordinator fence held (heartbeat alive) and WAIT for that call to
    finish before revoking authority / releasing adoption / closing the store --
    otherwise a replica could acquire_coordinator and double-reconcile. While the
    in-flight reconcile runs, a second replica's acquire MUST be refused.

    Mutation-sensitive: a close() that cancels/revokes + closes the store on a timeout
    would drop the fence, ``store_close`` would appear before the reconcile finished,
    and the held-acquire assertion would not hold.
    """

    clock = Clock()
    events: list[str] = []
    store = _CloseRecordingStore(events)
    provider = _CloseDrainProvider(clock, events)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store, process_epoch="process-one")
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-b",
        bout_id="bout-b",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord.adopt_claimed_engine(claim_id, SimpleNamespace())
    coord.release_claim_active(claim_id)
    await coord.begin_cleanup(claim_id)

    converge = asyncio.create_task(coord.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)

    close_task = asyncio.create_task(coord.close())
    for _ in range(5):
        await asyncio.sleep(0)
        if coord._closed:
            break
    assert not close_task.done()

    # The coordinator fence is HELD while the reconcile runs: a second replica cannot
    # acquire coordination, so it cannot double-reconcile the same resource.
    with pytest.raises(WarmCoordinatorHeldError):
        await store.acquire_coordinator(
            installation_id=coord.installation_id,
            process_epoch="process-two",
            broker_epoch="broker-two",
            now=clock(),
            ttl=timedelta(seconds=90),
        )
    # The store is NOT closed and authority NOT revoked while the AWS call is in flight.
    assert "store_close" not in events
    assert coord._authority_revoked is False

    provider.release_reconcile.set()
    await asyncio.wait_for(close_task, timeout=2)
    warmed = await asyncio.wait_for(converge, timeout=2)
    assert warmed.state == Round5WarmState.WARMING
    assert events == ["reconcile_done", "release_all", "store_close"]
    assert coord._authority_revoked is True


async def test_finding_c_mark_bout_armed_lease_loss_still_fences_cleaning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding C: production _mark_bout_armed releases the round5/ring leases and then
    raises on an armed-lease CAS failure (LeaseLostError). The arm-failure handler must
    STILL fence durable CLEANING (begin_cleanup is independent of those leases) so the
    slot never stays CLAIMED with an orphaned staged resident and a startable overlay,
    even though the cleanup-lease retain fails.

    Mutation-sensitive: if a retain failure aborted begin_cleanup, the slot would stay
    CLAIMED / the overlay startable and this fails.
    """

    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "0.02")
    monkeypatch.setenv("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "0.05")
    clock = Clock()
    provider = _EngineProvider(clock, _RecoverableAbandonPlan)
    coord = _accept_coordinator(clock, provider)
    await _warm_to_ready(coord, provider)
    manager = _make_manager(coord, clock, round_isolation=True)
    app = _asgi(manager)

    async def _lease_loss_then_raise(self, record, expires_at):  # noqa: ANN001
        # Mimic the production LeaseLostError path: release the round5/ring leases
        # (so the cleanup-lease retain will fail), then raise.
        await self._release_round5_lease(record)
        await self._release_bout(record)
        raise InvalidStateError("ROUND 5 CLEANUP AUTHORITY EXPIRED")

    monkeypatch.setattr(RunManager, "_mark_bout_armed", _lease_loss_then_raise)

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            armed = await client.post(f"/api/sessions/{session_id}/arm")
            assert armed.status_code == 200, armed.text

            for _ in range(400):
                snap = (await client.get(f"/api/sessions/{session_id}")).json()
                if snap["state"] == SessionState.FAILED.value:
                    break
                await asyncio.sleep(0.01)
            assert snap["state"] == SessionState.FAILED.value

            # Durable CLEANING was fenced despite the leases having been released.
            cleaning = await coord.store.read("install-acceptance")
            assert cleaning is not None and cleaning.state == Round5WarmState.CLEANING
            assert cleaning.claim is not None
            generation = cleaning.generation

            # The overlay is NOT startable while cleanup is unproven.
            board = (await client.get("/api/bout/all")).json()
            assert board["rounds"]["survive_connection_spike"]["can_start"] is False

            # Convergence still reaches N+1 once the resident settles.
            plan = provider.engines[0]._plan
            plan.allow_resident_settle.set()
            for _ in range(400):
                warming = await coord.store.read("install-acceptance")
                if warming is not None and warming.generation == generation + 1:
                    break
                await asyncio.sleep(0.01)
            assert warming is not None and warming.generation == generation + 1
    finally:
        await manager.close()
        await coord.close()


async def _run_handoff_with_cancel_local(
    cancel_local, *, claim_id: str, session_id: str, caplog
):
    """Drive _begin_connection_spike_cleanup_handoff with a spy engine whose
    cancel_local_round5_run_tasks is ``cancel_local``; return the fake coordinator."""

    engine = _CleanupEngineSpy()
    engine.cancel_local_round5_run_tasks = cancel_local  # type: ignore[assignment]
    coordinator_double = _FakeCleanupCoordinator()
    fake_self = _handoff_self(coordinator_double)
    fake_self._round5_local_cancel_timeout = 0.05
    pending = asyncio.create_task(asyncio.Event().wait())
    try:
        record = SimpleNamespace(
            connection_spike_engine=engine,
            operator=object(),
            task=None,
            round5_warm_slot=SimpleNamespace(claim=SimpleNamespace(claim_id=claim_id)),
            connection_spike_arm=object(),
            snapshot=SimpleNamespace(
                id=session_id,
                state=SessionState.VERIFIED,
                round5_setup=SimpleNamespace(setup_validated=True),
            ),
            connection_spike_cleanup_task=pending,
        )
        with caplog.at_level("ERROR"):
            # wait_for so a mutation that strips the timeout/except (bare await on a
            # never-resolving cancel_local) HANGS and fails deterministically here.
            await asyncio.wait_for(
                RunManager._begin_connection_spike_cleanup_handoff(fake_self, record),
                timeout=3,
            )
    finally:
        pending.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pending
    return coordinator_double


async def test_handoff_cancel_local_timeout_still_enters_cleaning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The bounded asyncio.timeout around cancel_local_round5_run_tasks must let
    begin_cleanup proceed even when local cancellation NEVER resolves.

    Mutation-sensitive: stripping the timeout/except to a bare ``await cancel_local()``
    hangs forever here, so the wait_for(timeout=3) trips and the test fails.
    """

    never = asyncio.Event()

    async def _never_resolves() -> None:
        await never.wait()

    try:
        coordinator_double = await _run_handoff_with_cancel_local(
            _never_resolves, claim_id="claim-to", session_id="session-to", caplog=caplog
        )
    finally:
        never.set()

    assert coordinator_double.begin_cleanup_calls == ["claim-to"]
    assert any(
        "did not complete before CLEANING" in record.message
        for record in caplog.records
    )


async def test_handoff_cancel_local_raises_still_enters_cleaning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The except around cancel_local_round5_run_tasks must swallow a raised local
    cancellation and still enter durable CLEANING.

    Mutation-sensitive: stripping the except lets the RuntimeError propagate to the
    handoff's outer except, which marks pending and returns WITHOUT begin_cleanup, so
    begin_cleanup_calls would be empty and this fails.
    """

    async def _raises() -> None:
        raise RuntimeError("forced local cancellation failure")

    coordinator_double = await _run_handoff_with_cancel_local(
        _raises, claim_id="claim-raise", session_id="session-raise", caplog=caplog
    )

    assert coordinator_double.begin_cleanup_calls == ["claim-raise"]
    assert any(
        "local run-task cancellation failed" in record.message
        for record in caplog.records
    )


# ---------------------------------------------------------------------------
# Bounded shutdown handoff: close() must not wait unbounded for a shielded AWS
# call past the platform graceful-shutdown grace (Databricks Apps/Uvicorn 60s).
# ---------------------------------------------------------------------------


async def test_shutdown_fast_reconcile_still_closes_orderly() -> None:
    """(1) An in-flight cleanup that finishes BEFORE the shutdown deadline still gets
    the full orderly close: reconcile completes, THEN authority revoked, adopted
    engines released, store closed -- in that order."""

    clock = Clock()
    events: list[str] = []
    store = _CloseRecordingStore(events)
    provider = _CloseDrainProvider(clock, events)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-fast",
        bout_id="bout-fast",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord.adopt_claimed_engine(claim_id, SimpleNamespace())
    coord.release_claim_active(claim_id)
    await coord.begin_cleanup(claim_id)

    converge = asyncio.create_task(coord.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)
    # Ample deadline; release the op so it finishes well within it.
    provider.release_reconcile.set()
    await asyncio.wait_for(coord.close(shutdown_deadline_seconds=30), timeout=3)

    assert events == ["reconcile_done", "release_all", "store_close"]
    assert coord._authority_revoked is True
    warmed = await asyncio.wait_for(converge, timeout=2)
    assert warmed.state == Round5WarmState.WARMING


async def test_shutdown_deadline_leaves_cleaning_and_refuses_takeover_while_held(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """(2) A stalled shielded op hits the shutdown deadline: close() RETURNS (does not
    hang), logs round5_shutdown_blocked, does NOT revoke authority / release adoption /
    close the store, leaves the slot CLEANING, and -- while this process's heartbeat +
    authority are still held -- a second replica's acquire is refused (no double
    reconcile).

    Mutation-sensitive: an unbounded wait would hang here (wait_for trips); revoking
    authority on the deadline would flip _authority_revoked and fail the assertion.
    """

    clock = Clock()
    events: list[str] = []
    store = _CloseRecordingStore(events)
    provider = _CloseDrainProvider(clock, events)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store, process_epoch="process-one")
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-deadline",
        bout_id="bout-deadline",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord.adopt_claimed_engine(claim_id, SimpleNamespace())
    coord.release_claim_active(claim_id)
    await coord.begin_cleanup(claim_id)

    converge = asyncio.create_task(coord.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)

    with caplog.at_level("ERROR"):
        # Bounded: close returns on the (short) deadline even though the op stalls.
        await asyncio.wait_for(
            coord.close(shutdown_deadline_seconds=0.1), timeout=5
        )

    # Structured shutdown-blocked signal carrying the blocked claim id.
    blocked = [r for r in caplog.records if "round5_shutdown_blocked" in r.message]
    assert blocked, "round5_shutdown_blocked was not emitted"
    assert claim_id in blocked[0].message

    # NOTHING that could enable a concurrent replica mutation happened.
    assert coord._authority_revoked is False
    assert "store_close" not in events
    assert "release_all" not in events
    assert claim_id in provider._adopted

    # Durable CLEANING retained for restart/takeover.
    slot = await store.read(coord.installation_id)
    assert slot is not None and slot.state == Round5WarmState.CLEANING and slot.claim is not None

    # While THIS process still holds the fence (heartbeat alive), a replica acquire is
    # refused -> no double reconcile.
    with pytest.raises(WarmCoordinatorHeldError):
        await store.acquire_coordinator(
            installation_id=coord.installation_id,
            process_epoch="process-two",
            broker_epoch="broker-two",
            now=clock(),
            ttl=timedelta(seconds=90),
        )

    # New work is still refused once closing.
    with pytest.raises(WarmFenceLostError):
        await coord.converge_cleanup(claim_id)

    # Cleanup: let the stalled op finish so the test's task does not leak.
    provider.release_reconcile.set()
    await asyncio.wait_for(converge, timeout=2)


async def test_shutdown_after_process_death_second_coordinator_converges() -> None:
    """(3) After the blocked process 'dies' (its coordinator lease TTL expires), a
    second coordinator on the same durable store acquires coordination and converges
    the retained CLEANING slot to N+1 -- the takeover mechanism the deadline handoff
    relies on. The taker still passes _require_current_cleanup_owner."""

    clock = Clock()
    store = InMemoryRound5WarmStore()

    # Process one reaches CLEANING then 'dies' without converging (no close()).
    provider_one = _RecordingProvider(clock)
    provider_one.reconcile_result = True
    coord_one = coordinator(clock, provider_one, store, process_epoch="process-one")
    await warm_ready(coord_one, provider_one)
    claimed, _ = await coord_one.claim(
        session_id="session-takeover",
        bout_id="bout-takeover",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord_one.release_claim_active(claim_id)
    await coord_one.begin_cleanup(claim_id)
    generation = claimed.generation

    cleaning = await store.read("install-one")
    assert cleaning is not None and cleaning.state == Round5WarmState.CLEANING

    # Simulate process death: the coordinator lease TTL expires.
    clock.advance(600)

    provider_two = _RecordingProvider(clock)
    provider_two.reconcile_result = True
    coord_two = coordinator(clock, provider_two, store, process_epoch="process-two")
    taken = await store.acquire_coordinator(
        installation_id="install-one",
        process_epoch="process-two",
        broker_epoch="broker-two",
        now=clock(),
        ttl=timedelta(seconds=90),
    )
    assert taken.coordinator_owner == "process-two"
    assert taken.state == Round5WarmState.CLEANING  # CLEANING retained across takeover

    # The new owner converges the retained CLEANING slot to N+1.
    warmed = await coord_two.converge_cleanup(claim_id)
    assert warmed.state == Round5WarmState.WARMING
    assert warmed.generation == generation + 1
    assert provider_two.reconcile_calls >= 1


# ---------------------------------------------------------------------------
# c62affa race-audit follow-ups: A2 (post-bell janitor cancels run-popped bursts),
# A3 (run() cancel cancels launched bursts), Finding B nuance (close() cancellation
# does not cancel in-flight cleanup / drop the heartbeat), and the begin_cleanup
# lifecycle-lock Low (closed-check atomic with the flag).
# ---------------------------------------------------------------------------


async def test_a2_post_bell_janitor_cancels_run_burst_tasks_before_orchestrator() -> None:
    """Finding A2: the post-bell janitor (_stop_setup_and_begin_cleanup_once, reached
    via _ensure_post_bell_provider_cleanup_started) must cancel the run()-popped
    _run_burst_tasks -- not only _lane_bursts (which run() has already emptied) --
    BEFORE the orchestrator begin_cleanup / Proxy delete.

    Mutation-sensitive: snapshotting only _lane_bursts leaves the run burst alive, so
    burst_cancelled never fires and the recorded order lacks the cancel-before-begin.
    """

    engine = object.__new__(LiveConnectionSpikeEngine)
    engine._cleanup_bout_id = None
    engine._setup_task = None
    engine._active_run_ids = {}
    engine._lane_bursts = {}  # empty: run() already popped its bursts
    engine._resident_bindings = {}
    engine._lane_adapters = {}
    engine._cleanup_start_lock = asyncio.Lock()

    order: list[str] = []
    burst_cancelled = asyncio.Event()

    async def _burst() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            order.append("burst_cancelled")
            burst_cancelled.set()
            raise

    task = asyncio.create_task(_burst())
    await asyncio.sleep(0)
    engine._run_burst_tasks = {task}  # the run()-popped launched burst

    class _Orch:
        async def begin_cleanup(self, bout_id) -> None:
            order.append("orchestrator_begin_cleanup")

    engine._setup_orchestrator = _Orch()
    engine._setup_result = SimpleNamespace(bout_id="bout-a2")

    await engine._ensure_post_bell_provider_cleanup_started(
        SimpleNamespace(bout_id="bout-a2")
    )

    assert burst_cancelled.is_set()
    assert order == ["burst_cancelled", "orchestrator_begin_cleanup"]
    assert engine._cleanup_bout_id == "bout-a2"


async def test_a3_run_cancel_cancels_launched_bursts_no_dispatch_after() -> None:
    """Finding A3: cancelling run() (e.g. SIGTERM) must cancel+await the launched
    bursts in its finally rather than silently wiping the set, so no orphan burst can
    dispatch after teardown. After awaiting run(), cancel_local finds them gone.

    Mutation-sensitive: a finally that only does ``_run_burst_tasks = set()`` leaves
    the burst running -> burst.cancelled() is False and it dispatches.
    """

    engine = object.__new__(LiveConnectionSpikeEngine)
    arm = object()
    engine._armed = arm
    engine._setup_orchestrator = None  # skip the setup_result precondition
    engine._setup_task = None
    engine._run_burst_tasks = set()
    engine._active_run_ids = {}
    engine._lane_adapters = {}
    target = SimpleNamespace(lane_id="lakebase")
    engine._runtime_targets = lambda: (target,)  # instance attr shadows the method

    dispatched: list[str] = []

    async def _burst() -> None:
        await asyncio.Event().wait()
        dispatched.append("dispatched-after-cancel")  # only if never cancelled

    burst = asyncio.create_task(_burst())
    engine._lane_bursts = {"lakebase": burst}

    run_task = asyncio.create_task(engine.run(arm, None))
    # Let run() pop the burst into _run_burst_tasks and block supervising it.
    for _ in range(5):
        await asyncio.sleep(0)
        if burst in engine._run_burst_tasks or engine._run_burst_tasks:
            break

    run_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task

    assert burst.cancelled()
    assert dispatched == []
    assert engine._run_burst_tasks == set()  # cleared after cancel+await

    # cancel_local now finds nothing to do (already cancelled) -- no error.
    await engine.cancel_local_round5_run_tasks()


async def test_b_nuance_close_cancellation_leaves_cleanup_running_and_fence_held() -> None:
    """Finding B nuance: if close() ITSELF is cancelled (SIGTERM cancels the close
    task), it must NOT cancel the in-flight cleanup or drop its heartbeat -- the
    cleanup keeps running with the fence held until it finishes or the process dies,
    so a replica still cannot acquire coordination.

    Mutation-sensitive: a close() that awaited pending via asyncio.gather would have
    the gather cancel the cleanup task on close cancellation -> converge.cancelled().
    """

    clock = Clock()
    events: list[str] = []
    store = _CloseRecordingStore(events)
    provider = _CloseDrainProvider(clock, events)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store, process_epoch="process-one")
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-bnuance",
        bout_id="bout-bnuance",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord.adopt_claimed_engine(claim_id, SimpleNamespace())
    coord.release_claim_active(claim_id)
    await coord.begin_cleanup(claim_id)

    converge = asyncio.create_task(coord.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)

    close_task = asyncio.create_task(coord.close())
    for _ in range(5):
        await asyncio.sleep(0)
        if coord._closed:
            break
    assert coord._closed is True and not close_task.done()

    # SIGTERM-style: cancel close() itself while it waits for the in-flight cleanup.
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    # The in-flight converge is untouched; the fence is still held.
    assert not converge.done()
    assert coord._authority_revoked is False
    assert "store_close" not in events
    with pytest.raises(WarmCoordinatorHeldError):
        await store.acquire_coordinator(
            installation_id=coord.installation_id,
            process_epoch="process-two",
            broker_epoch="broker-two",
            now=clock(),
            ttl=timedelta(seconds=90),
        )

    # Cleanup: let the converge finish so the task does not leak.
    provider.release_reconcile.set()
    await asyncio.wait_for(converge, timeout=2)


async def test_low_begin_cleanup_serialized_with_close_across_store_read_yield() -> None:
    """Architecture Low: begin_cleanup's closed-check AND its durable transition run
    under the SAME lifecycle lock as close(), so a begin_cleanup that yields at
    store.read cannot resume and mutate after close() has closed the store. With the
    read gated, close() must block on the lock (cannot set the flag / close the store)
    until begin_cleanup's CAS completes.

    Mutation-sensitive: checking _closed without the lock lets close() run to store
    close while begin_cleanup is parked in read, so its CAS lands AFTER store_close
    (and _closed flips True while begin_cleanup is parked).
    """

    clock = Clock()
    order: list[str] = []

    class _GatedStore(InMemoryRound5WarmStore):
        def __init__(self) -> None:
            super().__init__()
            self.gate = asyncio.Event()
            self.read_started = asyncio.Event()
            self.arm_gate = False

        async def read(self, installation_id):
            if self.arm_gate:
                self.arm_gate = False
                self.read_started.set()
                await self.gate.wait()
            return await super().read(installation_id)

        async def begin_cleanup(self, *args, **kwargs):
            order.append("begin_cleanup_cas")
            return await super().begin_cleanup(*args, **kwargs)

        async def close(self) -> None:
            order.append("store_close")
            return await super().close()

    store = _GatedStore()
    provider = _RecordingProvider(clock)
    provider.reconcile_result = True
    coord = coordinator(clock, provider, store)
    await warm_ready(coord, provider)
    claimed, _ = await coord.claim(
        session_id="session-low",
        bout_id="bout-low",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    coord.adopt_claimed_engine(claim_id, SimpleNamespace())
    coord.release_claim_active(claim_id)

    # begin_cleanup will now yield inside store.read WHILE holding the lifecycle lock.
    store.arm_gate = True
    begin_task = asyncio.create_task(coord.begin_cleanup(claim_id))
    await asyncio.wait_for(store.read_started.wait(), timeout=1)

    # close() cannot proceed: it blocks acquiring the SAME lifecycle lock.
    close_task = asyncio.create_task(coord.close())
    for _ in range(5):
        await asyncio.sleep(0)
    assert not close_task.done()
    assert coord._closed is False  # close has NOT set the flag (blocked on the lock)

    # Release begin_cleanup's read -> its CAS completes, the lock is released.
    store.gate.set()
    cleaning = await asyncio.wait_for(begin_task, timeout=2)
    assert cleaning.state == Round5WarmState.CLEANING
    await asyncio.wait_for(close_task, timeout=2)

    # The CAS strictly precedes store close: no store mutation after store.close.
    assert "begin_cleanup_cas" in order and "store_close" in order
    assert order.index("begin_cleanup_cas") < order.index("store_close")
