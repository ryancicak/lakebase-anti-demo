"""The ring-lease heartbeats renew off ``lease_lock`` and give a lease up in time.

A renew is one coordinator round trip, and a black-holed socket answers neither yes
nor no. So both heartbeats, the main ring's and Round 5's cleanup ring's, renew without
holding ``record.lease_lock``: a towel, a read of the card and a terminal settle never
queue behind them. Each renew is bounded by how long the lease can still be proven
live on this process's clock -- when its last confirmed write was sent, plus the TTL
the coordinator applied -- and the lease is declared lost one heartbeat before that,
so the bout's run is stopped while its row still fences the ring, however far the
coordinator's clock runs ahead. In one process the ring is not claimed again, by
another card or by the same one, until the work a lost lease cancelled has finished;
a claim that wins the row meanwhile is given back. Work that outlives its bout never
releases a fence claimed after its own.

The stores below answer a renew only when the test says so; left unanswered, that is
the black-holed socket.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import psycopg
import pytest

from server.coordination import InMemoryBoutLeaseStore, LeaseLostError
from server.cost_ledger import LakebaseCostLedgerStore
from server.manager import InvalidStateError, RunManager, SessionRecord
from server.models import (
    BoutOperator,
    CompetitorId,
    CooldownLaneState,
    CooldownState,
    Corner,
    LaneState,
    ResetMode,
    RoundId,
    SessionCreate,
    SessionState,
    TowelSnapshot,
    TowelState,
)
from server.round5_cleanup_owed import clear_round5_cleanup_owed
from server.round5_warm import Round5Variant, Round5WarmState
from server.round6_race import COMPETITOR_LANE, LAKEBASE_LANE, Round6RaceEngine
from server.targets import TargetNotArmedError
from server.verifier import LaneResult, VerificationResult
from tests.test_manager import (
    BackgroundCleanupConnectionSpikeEngine,
    BlockingTowelConnectionSpikeEngine,
    FakeLiveTarget,
    FakeModelScoreEngine,
    FakeResolver,
    FakeSafeChangeEngine,
    HangingArmResolver,
    HangingArmTarget,
    LoseTerminalLeaseStore,
    TowelRecoveryEngine,
    cost_window_manager,
    cost_window_session,
    drain_record_operations,
    make_verifier,
    round_four_request,
    running_round_three,
    sealed_receipt,
)
from tests.test_round6_race import BASELINE, FakeLane

OWNER = BoutOperator(display_name="Owner", subject="owner-subject")
OTHER = BoutOperator(display_name="Other Presenter", subject="other-subject")
LOST = "Ring lease lost; active work was stopped before another bout can begin."


class GatedCoordinator(InMemoryBoutLeaseStore):
    """Once gated, a renew or release of ``phase``, a transition into one phase, or one
    session's claim, awaits an answer.

    With ``renew_lands_first`` a gated renew is applied at once and only its answer waits.
    """

    def __init__(self, *, phase: str = "run_committed", **store) -> None:
        super().__init__(**store)
        self.phase = phase
        self.gate_renew = False
        self.renew_lands_first = False
        self.gate_release = False
        self.gate_transition_to: str | None = None
        self.gate_claim_for: str | None = None
        self.renewing = asyncio.Event()
        self.renew_answer = asyncio.Event()
        self.renew_abandoned = asyncio.Event()
        self.releasing = asyncio.Event()
        self.release_answer = asyncio.Event()
        self.transitioning = asyncio.Event()
        self.transition_answer = asyncio.Event()
        self.claiming = asyncio.Event()
        self.claim_answer = asyncio.Event()

    async def claim(self, **claim):
        if claim["session_id"] == self.gate_claim_for:
            self.claiming.set()
            await self.claim_answer.wait()
        return await super().claim(**claim)

    async def transition(self, lease, **transition):
        if transition["phase"] == self.gate_transition_to:
            # Like psycopg on a dead socket, a cancellation does not end the wait.
            self.transitioning.set()
            while not self.transition_answer.is_set():
                try:
                    await self.transition_answer.wait()
                except asyncio.CancelledError:
                    continue
        return await super().transition(lease, **transition)

    async def renew(self, lease, *, ttl):
        if not (self.gate_renew and lease.phase == self.phase):
            return await super().renew(lease, ttl=ttl)
        landed = await super().renew(lease, ttl=ttl) if self.renew_lands_first else None
        self.renewing.set()
        try:
            await self.renew_answer.wait()
        except asyncio.CancelledError:
            self.renew_abandoned.set()
            raise
        return landed if landed is not None else await super().renew(lease, ttl=ttl)

    async def release(self, lease):
        if self.gate_release and lease.phase == self.phase:
            self.releasing.set()
            await self.release_answer.wait()
        return await super().release(lease)


class DeafRenew(InMemoryBoutLeaseStore):
    """Once ``deaf``, a renew outlasts every cancellation until its socket closes.

    psycopg meets a cancellation by trying a server-side cancel and then waiting on
    the same socket again, which a black-holed socket never answers either.
    """

    def __init__(self) -> None:
        super().__init__()
        self.deaf = False
        self.renew_calls = 0
        self.renewing = asyncio.Event()
        self.socket_closed = asyncio.Event()

    async def renew(self, lease, *, ttl):
        self.renew_calls += 1
        if not self.deaf:
            return await super().renew(lease, ttl=ttl)
        self.renewing.set()
        while not self.socket_closed.is_set():
            try:
                await self.socket_closed.wait()
            except asyncio.CancelledError:
                continue
        raise ConnectionError("server closed the connection unexpectedly")


class SlowRenew(InMemoryBoutLeaseStore):
    """Every renew succeeds, ``delay`` seconds after it is asked, as a slow coordinator does."""

    def __init__(self, delay: float) -> None:
        super().__init__()
        self.delay = delay
        self.renewals = 0

    async def renew(self, lease, *, ttl):
        await asyncio.sleep(self.delay)
        renewed = await super().renew(lease, ttl=ttl)
        self.renewals += 1
        return renewed


class SlowCommitAnswer(InMemoryBoutLeaseStore):
    """The bell's ARMED -> RUN_COMMITTED commit lands at once; its answer comes ``delay`` late."""

    def __init__(self, delay: float) -> None:
        super().__init__()
        self.delay = delay

    async def transition(self, lease, **transition):
        moved = await super().transition(lease, **transition)
        if transition["phase"] == "run_committed":
            await asyncio.sleep(self.delay)
        return moved


class FailingRenew(InMemoryBoutLeaseStore):
    """Once ``failing``, every renew fails at once, as a refused connection does."""

    def __init__(self) -> None:
        super().__init__()
        self.failing = False
        self.failures = 0

    async def renew(self, lease, *, ttl):
        if self.failing:
            self.failures += 1
            raise ConnectionError("coordination endpoint unreachable")
        return await super().renew(lease, ttl=ttl)


@dataclass
class StubbornLane(FakeLane):
    """Once ``stubborn``, a history read parks and outlasts every cancellation until released."""

    stubborn: bool = False
    parked: asyncio.Event = field(default_factory=asyncio.Event)
    released: asyncio.Event = field(default_factory=asyncio.Event)

    async def read_history(self, order):
        if not self.stubborn:
            return await super().read_history(order)
        self.parked.set()
        while not self.released.is_set():
            try:
                await self.released.wait()
            except asyncio.CancelledError:
                continue
        raise asyncio.CancelledError


class StubbornResetEngine(FakeSafeChangeEngine):
    """Round 2 whose first environment reset outlasts every cancellation until released.

    With ``hold_run`` the bout's run waits, cancellably, until it is stopped.
    """

    def __init__(self, *, hold_run: bool = False) -> None:
        super().__init__()
        self.hold_run = hold_run
        self.stubborn = True
        self.resetting = asyncio.Event()
        self.released = asyncio.Event()

    async def run(self, arm, on_progress):
        if self.hold_run:
            await asyncio.Event().wait()
        return await super().run(arm, on_progress)

    async def reset(self, competitor, on_progress):
        if self.stubborn:
            self.resetting.set()
            while not self.released.is_set():
                try:
                    await self.released.wait()
                except asyncio.CancelledError:
                    continue
            self.stubborn = False
        return await super().reset(competitor, on_progress)


class VerifiedRound5Context:
    """A warm Round 5 context whose start state always verifies."""

    def bind_claim(self, claim) -> None:
        self.claim = claim

    async def verify_start_state(self, bout_id, fencing_token) -> None:
        return None


class GatedWarmCoordinator:
    """Enough of the Round 5 warm coordinator for its atomic claim, which awaits an answer
    before it claims the main ring and then the cleanup ring."""

    store = SimpleNamespace(claim_ready_with_leases=lambda *_a, **_k: None)
    ring_ready = True
    installation_id = "install-test"

    def __init__(self) -> None:
        context = VerifiedRound5Context()
        self._capsule = SimpleNamespace(
            variant_contexts={Round5Variant.RDS: context, Round5Variant.AURORA: context}
        )
        self.slot = SimpleNamespace(state=Round5WarmState.READY, claim=None, generation=49)
        self.claiming = asyncio.Event()
        self.claim_answer = asyncio.Event()
        self.renewal_stopped: list[str] = []
        self.returned: list[str] = []

    def public_status_cached(self):
        return {
            "round5_ring_ready": True,
            "round5_warm_state": "ready",
            "round5_warm_generation": 49,
        }

    async def claim_with_leases(self, **claim):
        self.claiming.set()
        await self.claim_answer.wait()
        rings = [
            await store.claim(
                session_id=claim["session_id"],
                operator=claim["operator"],
                phase="checking",
                session_state=SessionState.CHECKING,
                round_id=claim["round_id"],
                round_title=claim["round_title"],
                competitor_id=claim["competitor_id"],
                competitor_name=claim["competitor_name"],
                ttl=timedelta(seconds=30),
            )
            for store in (claim["main_store"], claim["cleanup_store"])
        ]
        self.slot = SimpleNamespace(
            state=Round5WarmState.CLAIMED,
            generation=49,
            claim=SimpleNamespace(claim_id="claim-1"),
        )
        return self.slot, self._capsule, rings[0], rings[1]

    def release_claim_active(self, claim_id: str) -> None:
        self.renewal_stopped.append(claim_id)

    async def return_unstarted_claim(self, claim_id: str, **_rings):
        self.release_claim_active(claim_id)
        self.returned.append(claim_id)
        self.slot = SimpleNamespace(state=Round5WarmState.READY, claim=None, generation=49)
        return self.slot


class AtomicReturnWarmCoordinator(GatedWarmCoordinator):
    """On the production warm store, returning an unstarted claim releases both its rings
    in the same transaction."""

    store = SimpleNamespace(
        claim_ready_with_leases=lambda *_a, **_k: None,
        return_unstarted_with_leases=lambda *_a, **_k: None,
    )

    async def return_unstarted_claim(self, claim_id: str, **rings):
        for ring in ("main", "cleanup"):
            assert await rings[f"{ring}_store"].release(rings[f"{ring}_lease"])
        return await super().return_unstarted_claim(claim_id)


class RefusedStartRound5Context(VerifiedRound5Context):
    async def verify_start_state(self, bout_id, fencing_token) -> None:
        raise InvalidStateError("Round 5 ring fence is no longer current")


class AtomicBellWarmCoordinator:
    """Enough of the Round 5 warm coordinator for its atomic bell, which commits both
    leases at once and answers ``delay`` seconds later."""

    store = SimpleNamespace(
        claim_ready_with_leases=lambda *_a, **_k: None,
        accept_bell_with_leases=lambda *_a, **_k: None,
    )
    ring_ready = True
    installation_id = "install-test"

    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.cleanups: list[str] = []
        self.renewal_stopped: list[str] = []

    def public_status_cached(self):
        return {
            "round5_ring_ready": True,
            "round5_warm_state": "claimed",
            "round5_warm_generation": 49,
        }

    def require_dispatchable(self, claim) -> None:
        """The claimed capsule here is always well inside its dispatch margin."""

    async def accept_bell_with_leases(
        self, claim_id, *, main_store, main_lease, cleanup_store, cleanup_lease, ttl, **_seams
    ):
        committed = [
            await store.transition(
                lease,
                operator=lease.operator,
                expected_phase="armed",
                phase="run_committed",
                session_state=SessionState.RUNNING,
                ttl=ttl,
            )
            for store, lease in ((main_store, main_lease), (cleanup_store, cleanup_lease))
        ]
        await asyncio.sleep(self.delay)
        return SimpleNamespace(claim_id=claim_id), committed[0], committed[1]

    def release_claim_active(self, claim_id: str) -> None:
        self.renewal_stopped.append(claim_id)

    async def begin_cleanup(self, claim_id: str):
        self.cleanups.append(claim_id)
        return SimpleNamespace(
            state=Round5WarmState.CLEANING,
            generation=49,
            claim=SimpleNamespace(claim_id=claim_id),
        )


class AtomicBellEngine:
    def __init__(self) -> None:
        self.precommitted: list[int] = []

    async def precommit_launch_intent(self, session_id: str, fencing_token: int) -> None:
        self.precommitted.append(fencing_token)

    def bind_bell(self, context) -> None:
        raise AssertionError("the bell released its lanes")


class HungCostLedger:
    """The cost ledger's close goes unanswered until ``answer`` is set."""

    def __init__(self) -> None:
        self.closing = asyncio.Event()
        self.answer = asyncio.Event()
        self.closes: list[tuple[str, datetime, str]] = []

    async def record_estimates(self, estimates) -> None:
        return None

    async def close_bout(self, *, installation_id, bout_id, window_end, terminal_outcome):
        self.closes.append((bout_id, window_end, terminal_outcome))
        self.closing.set()
        await self.answer.wait()
        return ()


class _LedgerCursor:
    def __init__(self, *, refused: bool) -> None:
        self.refused = refused

    async def __aenter__(self) -> _LedgerCursor:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def execute(self, sql: str, params: tuple = ()) -> None:
        if self.refused:
            raise psycopg.OperationalError("server closed the connection unexpectedly")


class _LedgerConnection:
    def __init__(self, cursor: _LedgerCursor) -> None:
        self._cursor = cursor

    async def __aenter__(self) -> _LedgerConnection:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def cursor(self) -> _LedgerCursor:
        return self._cursor


class LedgerClosingThroughLakebase:
    """A cost ledger that closes through the Lakebase ledger's own connection loop, whose
    first connection is refused."""

    def __init__(self) -> None:
        self.connections = 0
        self.closes = 0

        async def connect(**_kwargs: object) -> _LedgerConnection:
            self.connections += 1
            return _LedgerConnection(_LedgerCursor(refused=self.connections == 1))

        self.lakebase = LakebaseCostLedgerStore(
            endpoint_name="coordination-endpoint",
            database="anti_demo",
            host="coordination.example",
            user="app-service-principal",
            connector=connect,
            workspace_client=SimpleNamespace(
                postgres=SimpleNamespace(
                    generate_database_credential=lambda _name: SimpleNamespace(token="tok")
                )
            ),
        )

    async def record_estimates(self, estimates) -> None:
        return None

    async def close_bout(self, *, installation_id, bout_id, window_end, terminal_outcome):
        async def close(cursor) -> tuple:
            await cursor.execute("UPDATE cost_ledger SET window_end = %s", (window_end,))
            return ()

        closed = await self.lakebase._run(close)
        self.closes += 1
        return closed


class RecordingCostLedger:
    def __init__(self) -> None:
        self.closes: list[tuple[str, datetime, str]] = []

    async def record_estimates(self, estimates) -> None:
        return None

    async def close_bout(self, *, installation_id, bout_id, window_end, terminal_outcome):
        self.closes.append((bout_id, window_end, terminal_outcome))
        return ()


class HeldCompetitorResolver:
    """FakeResolver's lanes, with Aurora's answer held until ``release`` is set."""

    def __init__(self) -> None:
        self.release = asyncio.Event()

    def resolve(self, competitor: CompetitorId):
        return (
            FakeLiveTarget("lakebase", "Lakebase", 0.005),
            FakeLiveTarget("competitor", "Aurora Serverless v2", 0.005, after=self.release),
        )


class CountingRenew(InMemoryBoutLeaseStore):
    renewals = 0

    async def renew(self, lease, *, ttl):
        self.renewals += 1
        return await super().renew(lease, ttl=ttl)


class FirstCostCloseHangsThenFails(HungCostLedger):
    """Only the first close hangs, and fails once released; every later close lands."""

    async def close_bout(self, *, installation_id, bout_id, window_end, terminal_outcome):
        self.closes.append((bout_id, window_end, terminal_outcome))
        if len(self.closes) > 1:
            return ()
        self.closing.set()
        await self.answer.wait()
        raise RuntimeError("consuming input failed: server closed the connection unexpectedly")


class ArmAnswerLostThenDark(InMemoryBoutLeaseStore):
    """The arm's CHECKING -> ARMED commit lands but its answer is lost; releases then fail."""

    def __init__(self) -> None:
        super().__init__()
        self.dark = False

    async def transition(self, lease, **transition):
        moved = await super().transition(lease, **transition)
        if transition["expected_phase"] == "checking" and transition["phase"] == "armed":
            self.dark = True
            raise ConnectionError("armed commit response was lost")
        return moved

    async def release(self, lease):
        if self.dark:
            raise ConnectionError("coordination endpoint unreachable")
        return await super().release(lease)


class FirstReleaseRefused(InMemoryBoutLeaseStore):
    """The first release of ``phase`` is refused while its row stays held."""

    def __init__(self, *, phase: str | None) -> None:
        super().__init__()
        self.phase = phase
        self.refused = 0

    async def release(self, lease):
        if lease.phase == self.phase and not self.refused:
            self.refused += 1
            return False
        return await super().release(lease)


class CountingResetEngine(FakeSafeChangeEngine):
    def __init__(self) -> None:
        super().__init__()
        self.resets = 0

    async def reset(self, competitor, on_progress):
        self.resets += 1
        return await super().reset(competitor, on_progress)


class StubbornRunEngine(FakeSafeChangeEngine):
    """Round 2 whose run, once started, outlasts every cancellation until released."""

    def __init__(self) -> None:
        super().__init__()
        self.running = asyncio.Event()
        self.released = asyncio.Event()

    async def run(self, arm, on_progress):
        self.running.set()
        while not self.released.is_set():
            try:
                await self.released.wait()
            except asyncio.CancelledError:
                continue
        raise asyncio.CancelledError


def _short_leases(manager: RunManager, *, heartbeat: float = 0.05, ttl: float = 0.5) -> None:
    """By default a 50 ms heartbeat on a 0.5 s lease: the loss is due 50 ms before expiry."""

    manager._lease_heartbeat = heartbeat
    manager._active_lease_ttl = ttl
    manager._running_lease_ttl = ttl


def _round_six() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="data_analyst",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.ANALYZE_LIVE_ORDERS,
    )


def _round_one() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="sre",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.WAKE_IDLE_APP,
    )


def _round_two() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
    )


def _round_five() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="sre",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
    )


def _lanes_that_never_deliver() -> tuple[FakeLane, ...]:
    return (
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=None),
        FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=None),
    )


def _engine(lanes: tuple[FakeLane, ...]) -> Round6RaceEngine:
    return Round6RaceEngine(
        lanes,
        baseline=BASELINE,
        poll_seconds=0.005,
        watch_seconds=0.005,
        lane_timeout_seconds=30.0,
    )


async def _until(predicate):
    for _ in range(1000):
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return value
        await asyncio.sleep(0.005)
    raise AssertionError("condition never held")


async def _reach(manager: RunManager, session_id: str, state: SessionState):
    async def reached():
        snapshot = await manager.get(session_id)
        return snapshot if snapshot.state == state else None

    return await _until(reached)


async def _sleep_past(moment: datetime) -> None:
    await asyncio.sleep(max(0.0, (moment - datetime.now(UTC)).total_seconds()) + 0.1)


def _events(record: SessionRecord) -> list[str]:
    return [event.event for event in record.event_log.events]


async def _ring_free(store: InMemoryBoutLeaseStore) -> None:
    async def free() -> bool:
        return await store.current() is None

    await _until(free)


async def _claim_directly(store: InMemoryBoutLeaseStore, record: SessionRecord, phase: str):
    return await store.claim(
        session_id=record.snapshot.id,
        operator=OWNER,
        phase=phase,
        session_state=SessionState.CHECKING,
        round_id=record.snapshot.round.id.value,
        round_title=record.snapshot.round.title,
        competitor_id=record.snapshot.competitor.id.value,
        competitor_name=record.snapshot.competitor.short_name,
        ttl=timedelta(seconds=30),
    )


async def _cleaning(store: InMemoryBoutLeaseStore, engine: StubbornResetEngine):
    """A Round 2 bout that verified and is deleting its environments under its cooldown fence."""

    manager = RunManager(safe_change_factory=lambda: engine, lease_store=store)
    _short_leases(manager)
    created = await manager.create(_round_two())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await _reach(manager, created.id, SessionState.VERIFIED)
    await asyncio.wait_for(engine.resetting.wait(), timeout=5)
    record = manager._records[created.id]
    assert record.lease is not None and record.lease.phase == "cooldown"
    assert record.cooldown_task is not None and not record.cooldown_task.done()
    return manager, record


async def _holding(manager: RunManager, store: InMemoryBoutLeaseStore, ttl: timedelta):
    """A card holding a heartbeating ``run_committed`` fence, with no engine behind it."""

    created = await manager.create(_round_six())
    record = manager._records[created.id]
    record.operator = OWNER
    committed = await store.claim(
        session_id=created.id,
        operator=OWNER,
        phase="run_committed",
        session_state=SessionState.RUNNING,
        round_id=record.snapshot.round.id.value,
        round_title=record.snapshot.round.title,
        competitor_id=record.snapshot.competitor.id.value,
        competitor_name=record.snapshot.competitor.short_name,
        ttl=ttl,
    )
    record.lease = committed
    manager._start_lease_heartbeat(record, ttl)
    return record, committed


async def _running(
    store: InMemoryBoutLeaseStore,
    lanes: tuple[FakeLane, ...],
    *,
    heartbeat: float = 0.05,
    ttl: float = 0.5,
    **manager_options,
):
    """A Round 6 bout past its bell whose lanes never deliver, so it runs until stopped."""

    engines = [_engine(lanes)]
    manager = RunManager(
        live_orders_factory=lambda _competitor: (
            engines.pop(0) if engines else _engine(_lanes_that_never_deliver())
        ),
        lease_store=store,
        **manager_options,
    )
    _short_leases(manager, heartbeat=heartbeat, ttl=ttl)
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await _reach(manager, created.id, SessionState.RUNNING)
    record = manager._records[created.id]
    assert record.task is not None and not record.task.done()
    return manager, record


async def test_the_heartbeat_renews_outside_the_lease_lock_and_installs_its_answer() -> None:
    store = GatedCoordinator()
    manager = RunManager(lease_store=store)
    manager._lease_heartbeat = 0.01
    record, committed = await _holding(manager, store, timedelta(seconds=30))
    store.gate_renew = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)

    await asyncio.wait_for(record.lease_lock.acquire(), timeout=0.2)
    record.lease_lock.release()

    store.renew_answer.set()

    async def renewed() -> bool:
        async with record.lease_lock:
            return record.lease.expires_at > committed.expires_at

    await _until(renewed)
    assert manager._same_exact_lease(record.lease, committed)
    assert record.snapshot.state == SessionState.DRAFT
    await manager.close()


async def test_a_renew_answered_after_the_fence_moved_is_not_a_loss_of_the_new_fence() -> None:
    """The stale renew is refused on phase; that refusal is about the old fence only."""

    store = GatedCoordinator()
    manager = RunManager(lease_store=store)
    manager._lease_heartbeat = 0.01
    ttl = timedelta(seconds=30)
    record, committed = await _holding(manager, store, ttl)
    store.gate_renew = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)

    # A towel moves the fence while the heartbeat waits on its renew, and leaves this
    # heartbeat running, as only a handoff that forgot to restart it would.
    await asyncio.wait_for(record.lease_lock.acquire(), timeout=0.2)
    try:
        towel = await store.transition(
            committed,
            operator=OWNER,
            expected_phase="run_committed",
            phase="towel_cleanup",
            session_state=SessionState.RUNNING,
            ttl=ttl,
        )
        record.lease = towel
    finally:
        record.lease_lock.release()
    store.renew_answer.set()

    async def towel_renewed() -> bool:
        async with record.lease_lock:
            return record.lease.expires_at > towel.expires_at

    await _until(towel_renewed)
    assert manager._same_exact_lease(record.lease, towel)
    assert "session_failed" not in _events(record)
    assert record.snapshot.state == SessionState.DRAFT
    held = await store.current()
    assert held is not None and held.phase == "towel_cleanup"
    await manager.close()


async def test_a_renew_that_outlasts_its_cancellation_is_abandoned_in_time(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = DeafRenew()
    manager = RunManager(lease_store=store)
    _short_leases(manager, heartbeat=0.2, ttl=1.0)
    record, _ = await _holding(manager, store, timedelta(seconds=1.0))
    store.deaf = True
    try:
        with caplog.at_level(logging.WARNING, logger="server.manager"):
            await asyncio.wait_for(store.renewing.wait(), timeout=5)
            held = await store.current()
            assert held is not None

            await _until(lambda: record.lease is None)
        assert datetime.now(UTC) < held.expires_at
        assert "session_failed" in _events(record)
    finally:
        store.socket_closed.set()
    lines = [r.getMessage() for r in caplog.records]
    (abandoned,) = [line for line in lines if "renew abandoned" in line]
    (lost,) = [line for line in lines if "declared lost" in line]
    for line in (abandoned, lost):
        assert "reason=timeout" in line
        assert f"session={record.snapshot.id}" in line
        assert "phase=run_committed" in line
    await manager.close()


async def test_a_failing_renew_is_retried_until_the_lease_can_no_longer_be_proven_live(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = FailingRenew()
    manager = RunManager(lease_store=store)
    _short_leases(manager, heartbeat=0.2, ttl=1.0)
    record, _ = await _holding(manager, store, timedelta(seconds=1.0))
    lost_at: list[datetime] = []
    handle_lost_lease = manager._handle_lost_lease

    async def recording(lost_record, lease):
        lost_at.append(datetime.now(UTC))
        await handle_lost_lease(lost_record, lease)

    monkeypatch.setattr(manager, "_handle_lost_lease", recording)
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        store.failing = True
        await _until(lambda: store.failures >= 1)
        held = await store.current()
        assert held is not None
        expires_at = held.expires_at

        # A transient failure is not a loss while the lease is still provably live...
        await _until(lambda: store.failures >= 3)
        assert lost_at == []
        # ...and becomes one before the lease expires, not on the first retry after.
        await _until(lambda: lost_at)
    assert lost_at[0] < expires_at
    assert record.lease is None
    assert "session_failed" in _events(record)
    (line,) = [r.getMessage() for r in caplog.records if "declared lost" in r.getMessage()]
    assert "reason=deadline" in line
    await manager.close()


async def test_a_terminal_settle_of_the_fence_outranks_a_renew_that_never_answers() -> None:
    store = GatedCoordinator()
    manager = RunManager(lease_store=store)
    _short_leases(manager)
    record, committed = await _holding(manager, store, timedelta(seconds=0.5))
    heartbeat = record.lease_heartbeat_task
    store.gate_renew = True
    store.gate_release = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)
    settling = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.releasing.wait(), timeout=5)

    # Past its deadline the heartbeat stops without declaring a loss: the settle
    # owns this fence, and its release may already have cleared the row.
    assert heartbeat is not None
    await asyncio.wait_for(asyncio.wait({heartbeat}), timeout=5)
    assert "session_failed" not in _events(record)
    assert record.lease is not None and manager._same_exact_lease(record.lease, committed)

    store.release_answer.set()
    assert await asyncio.wait_for(settling, timeout=5) == "released"
    assert record.lease is None
    assert "session_failed" not in _events(record)
    await manager.close()


async def test_a_stalled_renew_queues_neither_the_towel_nor_a_read_of_the_card() -> None:
    store = GatedCoordinator()
    manager, record = await _running(store, _lanes_that_never_deliver())
    store.gate_renew = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)
    assert not record.lease_lock.locked()

    started = time.monotonic()
    towel = asyncio.create_task(manager.start_towel(record.snapshot.id, OWNER))
    read = asyncio.create_task(manager.get(record.snapshot.id))
    towelled, card = await asyncio.wait_for(asyncio.gather(towel, read), timeout=2)
    assert time.monotonic() - started < 0.5
    assert towelled.towel is not None
    assert card.id == record.snapshot.id
    held = await store.current()
    assert held is not None and held.phase == "towel_cleanup"
    # The towel's own heartbeat replaced the stalled one, which abandoned its renew.
    await asyncio.wait_for(store.renew_abandoned.wait(), timeout=5)

    await drain_record_operations(manager, record)
    finished = await manager.get(record.snapshot.id)
    assert finished.state == SessionState.TOWELLED
    assert "session_failed" not in _events(record)
    receipt = await sealed_receipt(record.snapshot.id)
    assert receipt is not None and receipt.sealing_event == "towel_finished"
    assert await store.current() is None
    await manager.close()


async def test_a_stalled_renew_stops_the_bout_before_its_lease_expires() -> None:
    store = GatedCoordinator()
    lanes = _lanes_that_never_deliver()
    manager, record = await _running(store, lanes, heartbeat=0.2, ttl=1.0)
    run = record.task
    assert run is not None
    stopped_at: list[datetime] = []
    run.add_done_callback(lambda _done: stopped_at.append(datetime.now(UTC)))
    store.gate_renew = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)
    held = await store.current()
    assert held is not None and held.session_id == record.snapshot.id
    expires_at = held.expires_at

    # While the row is still the bout's, a second presenter's Prepare is refused.
    second = await manager.create(_round_six())
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager.start_arm(second.id, OTHER)

    # The renew never answers. The run is stopped before the row expires...
    await asyncio.wait_for(asyncio.wait({run}), timeout=5)
    assert stopped_at[0] < expires_at
    reads = [lane.history_reads for lane in lanes]
    failed = await manager.get(record.snapshot.id)
    assert failed.state == SessionState.FAILED
    assert failed.failure == LOST
    assert "session_failed" in _events(record)
    assert record.lease is None
    assert store.renew_abandoned.is_set()
    # ...and nothing it started reads or writes the sources again, through the expiry.
    await _sleep_past(expires_at)
    assert [lane.history_reads for lane in lanes] == reads

    # The ring is free and its last owner has settled: the next Prepare is admitted.
    settlement = record.settlement_task
    if settlement is not None and not settlement.done():
        await asyncio.wait_for(asyncio.shield(settlement), timeout=5)
    armed = await manager.start_arm(second.id, OTHER)
    assert armed.state == SessionState.CHECKING
    holder = await store.current()
    assert holder is not None and holder.session_id == second.id
    assert run.done()
    await manager.close()


async def test_a_second_prepare_is_refused_until_the_stopped_bout_has_finished() -> None:
    """The durable row can expire before a stopped run has finished; one process waits."""

    store = GatedCoordinator()
    stubborn = StubbornLane(COMPETITOR_LANE, "AWS", arrives_on_read=None)
    lanes = (FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=None), stubborn)
    manager, record = await _running(store, lanes)
    run = record.task
    assert run is not None
    store.gate_renew = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)
    stubborn.stubborn = True
    try:
        await asyncio.wait_for(stubborn.parked.wait(), timeout=5)

        failed = await _reach(manager, record.snapshot.id, SessionState.FAILED)
        assert failed.failure == LOST
        await _ring_free(store)
        assert not run.done()

        second = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
            await manager.start_arm(second.id, OTHER)
        assert await store.current() is None
    finally:
        # Released however the test ends, or the loop's teardown waits on it forever.
        stubborn.released.set()
    await asyncio.wait_for(asyncio.wait({run}), timeout=5)
    settlement = record.settlement_task
    if settlement is not None and not settlement.done():
        await asyncio.wait_for(asyncio.shield(settlement), timeout=5)
    armed = await manager.start_arm(second.id, OTHER)
    assert armed.state == SessionState.CHECKING
    holder = await store.current()
    assert holder is not None and holder.session_id == second.id
    await manager.close()


async def test_a_coordinator_clock_running_ahead_cannot_outlast_the_stopped_bout() -> None:
    """The row's expiry is stamped on the coordinator's clock, which here runs ahead.

    The heartbeat's deadline must come from this clock: the moment each confirmed write
    was sent, plus the TTL the coordinator applied to it.
    """

    # Ahead by more than the 200 ms margin, so its own expiry read here would be too late.
    skew = timedelta(seconds=0.35)
    store = GatedCoordinator(clock=lambda: datetime.now(UTC) + skew)
    manager, record = await _running(store, _lanes_that_never_deliver(), heartbeat=0.2, ttl=1.0)
    run = record.task
    assert run is not None
    stopped_at: list[datetime] = []
    run.add_done_callback(lambda _done: stopped_at.append(datetime.now(UTC)))
    store.gate_renew = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)
    held = await store.current()
    assert held is not None
    expires_here = held.expires_at - skew

    await _ring_free(store)
    assert run.done()
    assert stopped_at[0] < expires_here
    second = await manager.create(_round_six())
    armed = await manager.start_arm(second.id, OTHER)
    assert armed.state == SessionState.CHECKING
    assert (await manager.get(record.snapshot.id)).failure == LOST
    await manager.close()


async def test_a_claim_that_lands_after_the_last_owner_lost_the_ring_is_given_back() -> None:
    """The claim passed every check, then waited; the ring's owner lost it meanwhile."""

    store = GatedCoordinator()
    stubborn = StubbornLane(COMPETITOR_LANE, "AWS", arrives_on_read=None)
    lanes = (FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=None), stubborn)
    manager, record = await _running(store, lanes)
    run = record.task
    assert run is not None
    second = await manager.create(_round_six())
    store.gate_claim_for = second.id
    arming = asyncio.create_task(manager.start_arm(second.id, OTHER))
    stubborn.stubborn = True
    try:
        await asyncio.wait_for(store.claiming.wait(), timeout=5)
        store.gate_renew = True
        failed = await _reach(manager, record.snapshot.id, SessionState.FAILED)
        assert failed.failure == LOST
        await _ring_free(store)
        assert not run.done()

        store.claim_answer.set()
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await asyncio.wait_for(arming, timeout=5)
        assert await store.current() is None
        assert manager._records[second.id].lease is None
        assert (await manager.get(second.id)).state == SessionState.DRAFT
    finally:
        store.claim_answer.set()
        stubborn.released.set()
    await asyncio.wait_for(asyncio.wait({run}), timeout=5)
    settlement = record.settlement_task
    if settlement is not None and not settlement.done():
        await asyncio.wait_for(asyncio.shield(settlement), timeout=5)
    armed = await manager.start_arm(second.id, OTHER)
    assert armed.state == SessionState.CHECKING
    await manager.close()


async def test_a_card_does_not_rearm_while_its_own_lost_cleanup_is_stopping() -> None:
    store = GatedCoordinator(phase="cooldown")
    engine = StubbornResetEngine()
    manager, record = await _cleaning(store, engine)
    cleanup = record.cooldown_task
    assert cleanup is not None
    store.gate_renew = True
    try:
        failed = await _reach(manager, record.snapshot.id, SessionState.FAILED)
        assert failed.failure == LOST
        await _ring_free(store)
        assert not cleanup.done()

        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(record.snapshot.id, OWNER)
        assert await store.current() is None
    finally:
        engine.released.set()
    await asyncio.wait_for(asyncio.wait({cleanup}), timeout=5)
    armed = await manager.start_arm(record.snapshot.id, OWNER)
    assert armed.state == SessionState.CHECKING
    holder = await store.current()
    assert holder is not None and holder.session_id == record.snapshot.id
    await manager.close()


async def test_a_stale_cleanup_never_releases_a_fence_claimed_after_its_own() -> None:
    """However the ring is claimed again, the cleanup still unwinding must leave it."""

    store = GatedCoordinator(phase="cooldown")
    engine = StubbornResetEngine()
    manager, record = await _cleaning(store, engine)
    cleanup = record.cooldown_task
    assert cleanup is not None
    store.gate_renew = True
    try:
        await _reach(manager, record.snapshot.id, SessionState.FAILED)
        await _ring_free(store)
        fresh = await _claim_directly(store, record, "checking")
        async with record.lease_lock:
            record.lease = fresh
    finally:
        engine.released.set()
    await asyncio.wait_for(asyncio.wait({cleanup}), timeout=5)
    assert record.lease == fresh
    assert await store.current() == fresh
    await manager.close()


async def test_a_stale_redo_reset_never_releases_a_fence_claimed_after_its_own() -> None:
    """The operator's cleanup after a lost bout, unwinding late, leaves a newer fence alone."""

    store = GatedCoordinator()
    engine = StubbornResetEngine(hold_run=True)
    manager = RunManager(safe_change_factory=lambda: engine, lease_store=store)
    _short_leases(manager)
    created = await manager.create(_round_two())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await _reach(manager, created.id, SessionState.RUNNING)
    record = manager._records[created.id]
    store.gate_renew = True
    await _reach(manager, created.id, SessionState.FAILED)
    await _ring_free(store)
    store.gate_renew = False
    try:
        await manager.start_cooldown(created.id, OWNER)
        await asyncio.wait_for(engine.resetting.wait(), timeout=5)
        reset = record.cooldown_task
        assert reset is not None
        held = record.lease
        assert held is not None and held.phase == "cooldown"
        manager._cancel_lease_heartbeat(record)
        assert await store.release(held)
        fresh = await _claim_directly(store, record, "checking")
        async with record.lease_lock:
            record.lease = fresh
    finally:
        engine.released.set()
    await asyncio.wait_for(asyncio.wait({reset}), timeout=5)
    assert record.lease == fresh
    assert await store.current() == fresh
    await manager.close()


async def test_a_hung_round_five_cleanup_renew_queues_neither_the_towel_nor_a_read() -> None:
    engine = BlockingTowelConnectionSpikeEngine()
    main = InMemoryBoutLeaseStore()
    round5 = GatedCoordinator(ring_key="round5")
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        lease_store=main,
        round5_lease_store=round5,
    )
    manager._lease_heartbeat = 0.05
    created = await manager.create(_round_five())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=5)
    record = manager._records[created.id]
    round5.gate_renew = True
    try:
        await asyncio.wait_for(round5.renewing.wait(), timeout=5)
        assert not record.lease_lock.locked()

        started = time.monotonic()
        towel = asyncio.create_task(manager.start_towel(created.id, OWNER))
        read = asyncio.create_task(manager.get(created.id))
        towelled, card = await asyncio.wait_for(asyncio.gather(towel, read), timeout=2)
        assert time.monotonic() - started < 0.5
        assert towelled.towel is not None
        assert card.id == created.id
    finally:
        round5.renew_answer.set()
    await asyncio.wait_for(engine.cleanup_started.wait(), timeout=5)
    engine.delete_accepted.set()
    engine.cleanup_complete.set()

    async def towel_ready():
        snapshot = await manager.get(created.id)
        return snapshot if snapshot.towel and snapshot.towel.state == "ready" else None

    settled = await _until(towel_ready)
    assert settled.state == SessionState.TOWELLED
    assert await main.current() is None
    assert await round5.current() is None
    await manager.close()


def _deaf(store: DeafRenew, *, settle: float = 15.0) -> RunManager:
    """A lease far from its deadline, so a renew is abandoned only when its heartbeat stops."""

    manager = RunManager(lease_store=store)
    manager._lease_heartbeat = 0.01
    manager._shutdown_settle_timeout = settle
    return manager


def _fence(lease) -> tuple[str, int]:
    return (lease.lease_id, lease.fencing_token)


async def test_a_renew_on_a_dead_socket_is_not_renewed_again_until_it_ends() -> None:
    store = DeafRenew()
    manager = _deaf(store)
    ttl = timedelta(seconds=30)
    record, committed = await _holding(manager, store, ttl)
    store.deaf = True
    try:
        await asyncio.wait_for(store.renewing.wait(), timeout=5)
        calls = store.renew_calls
        # A towel or a terminal settle restarts the heartbeat; the old one abandons its renew.
        manager._start_lease_heartbeat(record, ttl)
        abandoned = await _until(lambda: record.abandoned_renewals.get(_fence(committed)))
        assert abandoned in manager._abandoned_calls
        await asyncio.sleep(0.3)
        assert store.renew_calls == calls
        assert record.lease is not None
        store.deaf = False
    finally:
        store.socket_closed.set()
    await asyncio.wait_for(asyncio.wait({abandoned}), timeout=5)

    async def renewed_again() -> bool:
        async with record.lease_lock:
            return record.lease.expires_at > committed.expires_at

    await _until(renewed_again)
    assert store.renew_calls > calls
    assert record.abandoned_renewals == {}
    assert manager._abandoned_calls == set()
    await manager.close()


async def test_close_waits_for_an_abandoned_renew_but_not_past_the_settle_window() -> None:
    store = DeafRenew()
    manager = _deaf(store, settle=0.3)
    record, committed = await _holding(manager, store, timedelta(seconds=30))
    store.deaf = True
    try:
        await asyncio.wait_for(store.renewing.wait(), timeout=5)
        started = time.monotonic()
        await asyncio.wait_for(manager.close(), timeout=5)
        assert 0.25 <= time.monotonic() - started < 2.0
        abandoned = record.abandoned_renewals.get(_fence(committed))
        assert abandoned is not None and not abandoned.done()
    finally:
        store.socket_closed.set()
    await asyncio.wait_for(asyncio.wait({abandoned}), timeout=5)


async def test_close_returns_as_soon_as_an_abandoned_renew_ends() -> None:
    store = DeafRenew()
    manager = _deaf(store)
    record, committed = await _holding(manager, store, timedelta(seconds=30))
    store.deaf = True
    try:
        await asyncio.wait_for(store.renewing.wait(), timeout=5)
        closing = asyncio.create_task(manager.close())
        abandoned = await _until(lambda: record.abandoned_renewals.get(_fence(committed)))
        await asyncio.sleep(0.05)
        assert not closing.done()
        started = time.monotonic()
        store.socket_closed.set()
        await asyncio.wait_for(closing, timeout=5)
        assert time.monotonic() - started < 1.0
        assert abandoned.done()
    finally:
        store.socket_closed.set()


class RefusingRenew(InMemoryBoutLeaseStore):
    """Once ``refusing``, the coordinator answers every renew: not your lease."""

    refusing = False

    async def renew(self, lease, *, ttl):
        if self.refusing:
            raise LeaseLostError("RING LEASE LOST DURING ACTIVE WORK")
        return await super().renew(lease, ttl=ttl)


async def test_a_refused_renew_is_declared_lost_and_says_why(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = RefusingRenew()
    manager = RunManager(lease_store=store)
    _short_leases(manager)
    record, _ = await _holding(manager, store, timedelta(seconds=0.5))
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        store.refusing = True
        await _until(lambda: record.lease is None)
    (line,) = [r.getMessage() for r in caplog.records if "declared lost" in r.getMessage()]
    assert "reason=lease_lost" in line
    assert f"session={record.snapshot.id}" in line
    assert "phase=run_committed" in line
    assert "remaining_seconds=" in line
    await manager.close()


async def test_a_towel_stalled_on_the_coordinator_never_holds_the_run_past_its_lease() -> None:
    """The towel holds both locks a loss needs; its transition must give up by the deadline.

    Neither the towel nor the lost lease may then leave anything behind: no towel over a
    failed bout, however late the abandoned transition answers, and no second owner.
    """

    store = GatedCoordinator()
    store.gate_transition_to = "towel_cleanup"
    lanes = _lanes_that_never_deliver()
    # A 200 ms margin: the towel unwinding shares the window the run has to stop in.
    manager, record = await _running(store, lanes, heartbeat=0.2, ttl=1.0)
    run = record.task
    assert run is not None
    stopped_at: list[datetime] = []
    run.add_done_callback(lambda _done: stopped_at.append(datetime.now(UTC)))
    store.gate_renew = True
    try:
        await asyncio.wait_for(store.renewing.wait(), timeout=5)
        held = await store.current()
        assert held is not None
        expires_at = held.expires_at
        towel = asyncio.create_task(manager.start_towel(record.snapshot.id, OWNER))
        await asyncio.wait_for(store.transitioning.wait(), timeout=5)
        assert record.lock.locked() and record.lease_lock.locked()

        done, _ = await asyncio.wait({towel}, timeout=5)
        assert towel in done
        with pytest.raises(InvalidStateError, match="TOWEL CLEANUP REFUSED"):
            towel.result()
        # The abandoned transition may still land at the coordinator, late.
        store.transition_answer.set()
        await asyncio.wait_for(asyncio.wait({run}), timeout=5)
        assert stopped_at[0] < expires_at
        failed = await manager.get(record.snapshot.id)
        assert failed.state == SessionState.FAILED
        assert failed.failure == LOST
        assert failed.towel is None
        reads = [lane.history_reads for lane in lanes]
        await _sleep_past(expires_at)
        assert [lane.history_reads for lane in lanes] == reads
        assert (await manager.get(record.snapshot.id)).towel is None

        # A towel row the late answer left behind lapses on its own TTL; then, with
        # the bout stopped, the ring is anyone's.
        await _ring_free(store)
        second = await manager.create(_round_six())
        assert (await manager.start_arm(second.id, OTHER)).state == SessionState.CHECKING
    finally:
        store.transition_answer.set()
        store.renew_answer.set()
    await manager.close()


async def test_a_slow_coordinator_that_answers_in_time_is_not_a_lost_lease(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Each renew takes three call timeouts but lands well inside the lease."""

    store = SlowRenew(delay=0.3)
    manager = RunManager(lease_store=store)
    manager._lease_heartbeat = 0.05
    manager._terminal_release_call_timeout = 0.1
    record, committed = await _holding(manager, store, timedelta(seconds=1.0))
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        await asyncio.sleep(2.5)
    assert record.lease is not None
    assert record.lease.expires_at > committed.expires_at
    assert store.renewals >= 3
    assert "session_failed" not in _events(record)
    assert not [
        r.getMessage()
        for r in caplog.records
        if "renew abandoned" in r.getMessage() or "declared lost" in r.getMessage()
    ]
    await manager.close()


async def test_an_owner_cleanup_does_not_claim_the_ring_while_its_lost_run_is_stopping() -> None:
    store = GatedCoordinator()
    engine = StubbornRunEngine()
    manager = RunManager(safe_change_factory=lambda: engine, lease_store=store)
    _short_leases(manager)
    created = await manager.create(_round_two())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(engine.running.wait(), timeout=5)
    run = manager._records[created.id].task
    assert run is not None
    store.gate_renew = True
    try:
        failed = await _reach(manager, created.id, SessionState.FAILED)
        assert failed.failure == LOST
        await _ring_free(store)
        assert not run.done()

        with pytest.raises(InvalidStateError) as refused:
            await manager.start_cooldown(created.id, OWNER)
        assert "STILL STOPPING" in str(refused.value)
        # The refusal says what the operator can do about it.
        assert "TRY AGAIN" in str(refused.value)
        assert "RESTART THE APP" in str(refused.value)
        assert await store.current() is None
    finally:
        engine.released.set()
    await asyncio.wait_for(asyncio.wait({run}), timeout=5)
    resetting = await manager.start_cooldown(created.id, OWNER)
    assert resetting.cooldown is not None
    holder = await store.current()
    assert holder is not None and holder.phase == "cooldown"

    async def cleaned():
        snapshot = await manager.get(created.id)
        return snapshot.cooldown is not None and snapshot.cooldown.state == CooldownState.READY

    await _until(cleaned)
    await manager.close()


async def test_a_stale_renew_that_succeeds_after_the_fence_moved_is_not_installed() -> None:
    """The old fence's renew landed at the coordinator; its answer arrives after a towel."""

    store = GatedCoordinator()
    store.renew_lands_first = True
    manager = RunManager(lease_store=store)
    manager._lease_heartbeat = 0.01
    ttl = timedelta(seconds=30)
    record, committed = await _holding(manager, store, ttl)
    store.gate_renew = True
    await asyncio.wait_for(store.renewing.wait(), timeout=5)

    async with record.lease_lock:
        towel = await store.transition(
            committed,
            operator=OWNER,
            expected_phase="run_committed",
            phase="towel_cleanup",
            session_state=SessionState.RUNNING,
            ttl=ttl,
        )
        record.lease = towel
    store.renew_answer.set()

    async def towel_renewed() -> bool:
        async with record.lease_lock:
            return record.lease.expires_at > towel.expires_at

    await _until(towel_renewed)
    assert record.lease.phase == "towel_cleanup"
    assert manager._same_exact_lease(record.lease, towel)
    assert "session_failed" not in _events(record)
    assert record.snapshot.state == SessionState.DRAFT
    await manager.close()


@pytest.mark.parametrize(
    "warm",
    [GatedWarmCoordinator, AtomicReturnWarmCoordinator],
    ids=["warm slot returned alone", "rings returned with it"],
)
async def test_a_round_five_atomic_claim_that_lands_after_a_loss_gives_back_both_rings(
    warm: type[GatedWarmCoordinator], caplog: pytest.LogCaptureFixture
) -> None:
    """Round 5 claims the main ring, its cleanup ring and a warm slot in one call; the
    ring's last owner lost it while that call waited. Giving the claim back loses nothing,
    whether its rings go back with the warm slot or after it."""

    caplog.set_level(logging.WARNING, logger="server.manager")
    main = GatedCoordinator()
    round5 = InMemoryBoutLeaseStore(ring_key="round5")
    warm = warm()
    stubborn = StubbornLane(COMPETITOR_LANE, "AWS", arrives_on_read=None)
    lanes = (FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=None), stubborn)
    manager, record = await _running(
        main,
        lanes,
        connection_spike_factory=lambda _competitor: object(),
        round5_lease_store=round5,
        round5_warm_coordinator=warm,
    )
    run = record.task
    assert run is not None
    second = await manager.create(_round_five())
    arming = asyncio.create_task(manager.start_arm(second.id, OTHER))
    stubborn.stubborn = True
    try:
        await asyncio.wait_for(warm.claiming.wait(), timeout=5)
        main.gate_renew = True
        failed = await _reach(manager, record.snapshot.id, SessionState.FAILED)
        assert failed.failure == LOST
        await _ring_free(main)
        assert not run.done()

        warm.claim_answer.set()
        done, _ = await asyncio.wait({arming}, timeout=5)
        assert arming in done
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            arming.result()
        claimed = manager._records[second.id]
        assert await main.current() is None
        assert await round5.current() is None
        assert claimed.lease is None and claimed.round5_lease is None
        assert claimed.lease_heartbeat_task is None
        assert claimed.round5_lease_heartbeat_task is None
        assert warm.returned == ["claim-1"]
        assert "claim-1" in warm.renewal_stopped
        assert claimed.ring_lease_lost is False
        declared, _alarms = _lost_lines(caplog)
        assert not [line for line in declared if f"session={second.id}" in line.getMessage()]
    finally:
        warm.claim_answer.set()
        stubborn.released.set()
    await asyncio.wait_for(asyncio.wait({run}), timeout=5)
    await manager.close()


def _refusing_round_five_on_the_warm_store():
    """A Round 5 manager whose start-state check refuses every Prepare, on a warm store
    that gives a refused claim back with both its rings."""

    main = InMemoryBoutLeaseStore()
    round5 = InMemoryBoutLeaseStore(ring_key="round5")
    warm = AtomicReturnWarmCoordinator()
    warm.claim_answer.set()
    refused = RefusedStartRound5Context()
    warm._capsule.variant_contexts = {Round5Variant.RDS: refused, Round5Variant.AURORA: refused}
    manager = RunManager(
        connection_spike_factory=lambda _competitor: object(),
        lease_store=main,
        round5_lease_store=round5,
        round5_warm_coordinator=warm,
    )
    return manager, main, round5, warm


async def test_a_round_five_prepare_the_warm_store_gives_back_whole_loses_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The production warm store returns a refused Prepare's claim and both its rings in
    one transaction. Nothing ran under them, so finding the rows gone after it is no loss:
    no warning, and the ring is not held as lost."""

    manager, main, round5, warm = _refusing_round_five_on_the_warm_store()
    created = await manager.create(_round_five())
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        with pytest.raises(InvalidStateError, match="no longer current"):
            await manager.start_arm(created.id, OWNER)
    record = manager._records[created.id]
    assert warm.returned == ["claim-1"]
    assert record.lease is None and record.round5_lease is None
    assert record.lease_heartbeat_task is None
    assert record.round5_lease_heartbeat_task is None
    assert await main.current() is None
    assert await round5.current() is None
    assert record.ring_lease_lost is False
    assert _lost_lines(caplog) == ([], [])
    await manager.close()


async def test_a_round_five_prepare_given_back_beside_running_ring_work_still_loses_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The control: that transaction never checks expiry, so it proves no unbroken hold.
    Beside ring work still running, the gone row stays a loss, said out loud."""

    manager, main, _round5, warm = _refusing_round_five_on_the_warm_store()
    created = await manager.create(_round_five())
    record = manager._records[created.id]
    settles = asyncio.Event()
    settlement = asyncio.create_task(settles.wait(), name=f"round-settlement-{created.id}")
    record.settlement_task = settlement
    try:
        with caplog.at_level(logging.WARNING, logger="server.manager"):
            with pytest.raises(InvalidStateError, match="no longer current"):
                await manager.start_arm(created.id, OWNER)
            await _until(lambda: _lost_lines(caplog)[1])
        assert warm.returned == ["claim-1"]
        assert record.lease is None
        assert record.ring_lease_lost is True
        assert await main.current() is None
        (declared,), (alarm,) = _lost_lines(caplog)
        assert f"session={created.id}" in declared.getMessage()
        assert settlement.get_name() in alarm.getMessage()
    finally:
        settles.set()
    await settlement
    await manager.close()


def _armed_proof(record: SessionRecord) -> float:
    adopted = record.lease
    assert adopted is not None
    return record.arm_commit_sent_at + (adopted.expires_at - adopted.updated_at).total_seconds()


async def test_an_armed_row_adopted_by_a_terminal_settle_is_proven_from_its_arm_commit() -> None:
    store = ArmAnswerLostThenDark()
    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier(), lease_store=store)
    manager._terminal_settle_deadline = 0.05
    manager._terminal_release_backoff_cap = 0.02
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.FAILED)
    record = manager._records[created.id]

    adopted = record.lease
    assert adopted is not None and adopted.phase == "armed"
    assert record.lease_proof is not None and record.lease_proof[0] == adopted
    assert record.lease_proof[1] == pytest.approx(_armed_proof(record))
    store.dark = False
    await manager.close()


async def test_an_armed_row_adopted_by_release_bout_is_proven_from_its_arm_commit() -> None:
    store = ArmAnswerLostThenDark()
    manager = RunManager(lease_store=store)
    created = await manager.create(_round_one())
    record = manager._records[created.id]
    record.operator = OWNER
    await manager._claim_bout(record, OWNER)
    record.snapshot.state = SessionState.CHECKING
    with pytest.raises(ConnectionError):
        await manager._mark_bout_armed(record, datetime.now(UTC) + timedelta(seconds=60))
    assert record.lease is not None and record.lease.phase == "checking"

    assert await manager._release_bout(record) is False
    adopted = record.lease
    assert adopted is not None and adopted.phase == "armed"
    assert record.lease_proof is not None and record.lease_proof[0] == adopted
    assert record.lease_proof[1] == pytest.approx(_armed_proof(record))
    store.dark = False
    await manager.close()


@pytest.mark.parametrize("failing", ["reset", "release"])
async def test_a_cleanup_retry_stops_once_a_newer_fence_owns_the_ring(failing: str) -> None:
    """The first cleanup attempt fails; the ring is claimed again before its retry."""

    engine = CountingResetEngine()
    engine.reset_failures = 1 if failing == "reset" else 0
    store = FirstReleaseRefused(phase="cooldown" if failing == "release" else None)
    manager = RunManager(safe_change_factory=lambda: engine, lease_store=store)
    manager._cleanup_retry_initial = 0.3
    created = await manager.create(_round_two())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await _reach(manager, created.id, SessionState.VERIFIED)
    record = manager._records[created.id]
    cleanup = record.cooldown_task
    assert cleanup is not None

    retrying = (
        "Automatic cleanup retry scheduled"
        if failing == "reset"
        else "Cleanup verified · Releasing the ring"
    )

    def waiting_to_retry() -> bool:
        cooldown = record.snapshot.cooldown
        return cooldown is not None and all(
            lane.status == retrying for lane in cooldown.lanes.values()
        )

    await _until(waiting_to_retry)
    assert engine.resets == 1
    manager._cancel_lease_heartbeat(record)
    assert await store.release(record.lease)
    fresh = await _claim_directly(store, record, "checking")
    async with record.lease_lock:
        record.lease = fresh
    ready_before = _events(record).count("cooldown_ready")

    await asyncio.wait_for(asyncio.wait({cleanup}), timeout=5)
    assert engine.resets == 1
    assert record.lease == fresh
    assert await store.current() == fresh
    assert _events(record).count("cooldown_ready") == ready_before
    await manager.close()


async def test_round_five_cleanup_outliving_a_lost_main_lease_keeps_the_ring_closed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Round 5's backstage cleanup runs on past a lost main lease, by design. Here the main
    ring is shared with Round 6, and stays closed in this process until cleanup ends."""

    main = GatedCoordinator(phase="cooldown")
    round5 = InMemoryBoutLeaseStore(ring_key="round5")
    engine = BackgroundCleanupConnectionSpikeEngine()
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=main,
        round5_lease_store=round5,
    )
    _short_leases(manager)
    created = await manager.create(_round_five())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await _reach(manager, created.id, SessionState.VERIFIED)
    await asyncio.wait_for(engine.cleanup_started.wait(), timeout=5)
    record = manager._records[created.id]
    cleanup = record.connection_spike_cleanup_task
    assert cleanup is not None and not cleanup.done()
    assert record.lease is not None and record.lease.phase == "cooldown"
    main.gate_renew = True
    try:
        with caplog.at_level(logging.ERROR, logger="server.manager"):
            await _until(lambda: record.lease is None)
            await _ring_free(main)
            assert not cleanup.done()

            later = await manager.create(_round_six())
            with pytest.raises(InvalidStateError, match="STILL STOPPING"):
                await manager.start_arm(later.id, OTHER)
            assert await main.current() is None

            # Other replicas are not fenced: operators are told work still runs.
            await _until(lambda: any("still runs" in r.getMessage() for r in caplog.records))
        (alarm,) = [r.getMessage() for r in caplog.records if "still runs" in r.getMessage()]
        assert f"session={record.snapshot.id}" in alarm
        assert cleanup.get_name() in alarm
    finally:
        engine.delete_accepted.set()
        engine.cleanup_complete.set()
    await asyncio.wait_for(asyncio.wait({cleanup}), timeout=5)
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def test_close_stops_a_heartbeat_that_cleared_its_own_slot_to_wait_on_the_run() -> None:
    """A Round 5 cleanup heartbeat lost its lease and now waits for a run that will not stop."""

    main = InMemoryBoutLeaseStore()
    round5 = RefusingRenew(ring_key="round5")
    manager = RunManager(lease_store=main, round5_lease_store=round5)
    manager._lease_heartbeat = 0.01
    manager._shutdown_settle_timeout = 0.2
    created = await manager.create(_round_five())
    record = manager._records[created.id]
    record.operator = OWNER
    record.snapshot.state = SessionState.RUNNING
    sent = time.monotonic()
    lease = await round5.claim(
        session_id=created.id,
        operator=OWNER,
        phase="run_committed",
        session_state=SessionState.RUNNING,
        round_id=record.snapshot.round.id.value,
        round_title=record.snapshot.round.title,
        competitor_id=record.snapshot.competitor.id.value,
        competitor_name=record.snapshot.competitor.short_name,
        ttl=timedelta(seconds=1),
    )
    manager._set_lease(record, lease, sent, round5=True)
    released = asyncio.Event()

    async def stubborn_run() -> None:
        while not released.is_set():
            try:
                await released.wait()
            except asyncio.CancelledError:
                continue

    record.task = asyncio.create_task(stubborn_run(), name="stubborn-round5-run")
    round5.refusing = True
    manager._start_round5_lease_heartbeat(record, timedelta(seconds=1))
    heartbeat = record.round5_lease_heartbeat_task
    assert heartbeat is not None
    try:
        await _until(lambda: record.round5_lease_heartbeat_task is None)
        assert not heartbeat.done()
        await asyncio.wait_for(manager.close(), timeout=5)
        assert heartbeat.done()
    finally:
        released.set()
        heartbeat.cancel()
        await asyncio.gather(heartbeat, record.task, return_exceptions=True)


async def test_a_bell_commit_answered_after_its_lease_lapsed_never_starts_the_run() -> None:
    """The commit landed, but its answer came so late that the lease it returned can no
    longer be proven live: the bell is refused instead of starting a run under it."""

    store = SlowCommitAnswer(delay=0.6)
    lanes = _lanes_that_never_deliver()
    manager = RunManager(live_orders_factory=lambda _competitor: _engine(lanes), lease_store=store)
    _short_leases(manager, heartbeat=0.1, ttl=0.5)
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    with pytest.raises(InvalidStateError, match="FIGHT CARD EXPIRED"):
        await manager.start_run(created.id, OWNER)
    record = manager._records[created.id]
    assert not any(lane.started for lane in lanes)
    assert not [event for lane in lanes for event in lane.events if event.startswith("commit:")]
    assert "run_started" not in _events(record)
    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    assert failed.failure is not None and "FIGHT CARD EXPIRED" in failed.failure
    assert await store.current() is None
    await manager.close()


async def test_a_round_five_atomic_bell_answered_after_its_leases_lapsed_releases_nothing() -> None:
    """The atomic bell committed both leases, but answered too late to prove them live: the
    lanes are never released, and the bell takes the refused-bell cleanup path."""

    main = InMemoryBoutLeaseStore()
    round5 = InMemoryBoutLeaseStore(ring_key="round5")
    warm = AtomicBellWarmCoordinator(delay=0.6)
    engine = AtomicBellEngine()
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        lease_store=main,
        round5_lease_store=round5,
        round5_warm_coordinator=warm,
    )
    _short_leases(manager, heartbeat=0.1, ttl=0.5)
    manager._cleanup_retry_initial = 30
    created = await manager.create(_round_five())
    record = manager._records[created.id]
    record.operator = OWNER
    record.connection_spike_engine = engine
    record.connection_spike_arm = SimpleNamespace()
    record.round5_warm_slot = SimpleNamespace(
        state=Round5WarmState.CLAIMED,
        generation=49,
        claim=SimpleNamespace(claim_id="claim-1"),
    )
    for store, cleanup_ring in ((main, False), (round5, True)):
        sent = time.monotonic()
        armed = await store.claim(
            session_id=created.id,
            operator=OWNER,
            phase="armed",
            session_state=SessionState.ARMED,
            round_id=record.snapshot.round.id.value,
            round_title=record.snapshot.round.title,
            competitor_id=record.snapshot.competitor.id.value,
            competitor_name=record.snapshot.competitor.short_name,
            ttl=timedelta(seconds=180),
        )
        manager._set_lease(record, armed, sent, round5=cleanup_ring)
    record.snapshot.state = SessionState.ARMED
    try:
        with pytest.raises(InvalidStateError, match="FIGHT CARD EXPIRED"):
            await manager.start_run(created.id, OWNER)
        assert engine.precommitted
        assert warm.cleanups == ["claim-1"]
        assert (await manager.get(created.id)).state == SessionState.FAILED
        assert record.lease_heartbeat_task is None
        assert record.task is None
    finally:
        clear_round5_cleanup_owed(created.id)
        await manager.close()


async def test_a_hung_cost_close_holds_neither_the_towel_nor_a_read() -> None:
    """The towel waits on the cost ledger only briefly, under the record lock that reads
    and the lease's loss handling also take. Unanswered, the close goes on and lands
    later with the towel's own end, so no retry can disagree with it."""

    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine()
    ledger = HungCostLedger()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    manager._cost_close_wait = 0.2
    created = await running_round_three(manager, engine)
    record = manager._records[created.id]
    manager._cost_ledger_store = ledger
    record.cost_bout_id = "bout-under-test"
    record.cost_bout_started_at = datetime.now(UTC) - timedelta(seconds=1)
    record.cost_bout_kind = "run"
    try:
        towel = asyncio.ensure_future(manager.start_towel(created.id))
        await asyncio.wait_for(ledger.closing.wait(), timeout=1)
        asked = time.monotonic()
        await asyncio.wait_for(manager.get(created.id), timeout=1)
        accepted = await asyncio.wait_for(towel, timeout=1)
        assert time.monotonic() - asked < 0.6
        assert accepted.state == SessionState.TOWELLED
        assert accepted.towel is not None and accepted.towel.cost_close_failure is None
        assert record.cooldown_task is not None

        # Cleanup tries the close again before it releases the ring; that wait is bounded too.
        await _until(lambda: len(ledger.closes) == 2)
        asked = time.monotonic()
        await asyncio.wait_for(manager.get(created.id), timeout=1)
        assert time.monotonic() - asked < 0.6
        await _until(lambda: record.cooldown_task is not None and record.cooldown_task.done())
        assert await lease_store.current() is None
    finally:
        ledger.answer.set()
    await _until(lambda: not manager._abandoned_calls)
    assert set(ledger.closes) == {
        ("bout-under-test", accepted.towel.requested_at, SessionState.TOWELLED.value)
    }
    assert record.cost_bout_id is None
    settled = await manager.get(created.id)
    assert settled.towel is not None and settled.towel.cost_close_failure is None
    assert manager._releasable(record) is True
    await manager.close()


async def test_a_late_cost_close_failure_never_reports_a_window_cleanup_closed() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine()
    ledger = FirstCostCloseHangsThenFails()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    manager._cost_close_wait = 0.2
    created = await running_round_three(manager, engine)
    record = manager._records[created.id]
    manager._cost_ledger_store = ledger
    record.cost_bout_id = "bout-under-test"
    record.cost_bout_started_at = datetime.now(UTC) - timedelta(seconds=1)
    record.cost_bout_kind = "run"

    await manager.start_towel(created.id)
    await _until(lambda: record.cooldown_task is not None and record.cooldown_task.done())
    assert len(ledger.closes) == 2
    assert record.cost_bout_id is None

    ledger.answer.set()
    await _until(lambda: not manager._abandoned_calls)
    settled = await manager.get(created.id)
    assert settled.towel is not None and settled.towel.cost_close_failure is None
    await manager.close()


async def test_a_cost_close_landing_late_never_clears_a_newer_bout() -> None:
    ledger = HungCostLedger()
    manager = RunManager()
    manager._cost_ledger_store = ledger
    created = await manager.create(_round_six())
    record = manager._records[created.id]
    record.cost_bout_id = "first"
    record.cost_bout_started_at = datetime.now(UTC) - timedelta(seconds=1)
    record.cost_bout_kind = "run"
    closing = asyncio.ensure_future(
        manager._close_cost_bout(
            record,
            bout_id="first",
            ended_at=datetime.now(UTC),
            outcome=SessionState.TOWELLED.value,
        )
    )
    await asyncio.wait_for(ledger.closing.wait(), timeout=1)
    record.cost_bout_id = "second"

    ledger.answer.set()
    await asyncio.wait_for(closing, timeout=1)
    assert record.cost_bout_id == "second"
    assert record.cost_bout_kind == "run"
    await manager.close()


async def test_losing_round_five_cleanup_lease_keeps_the_main_ring_until_cleanup_ends() -> None:
    """Round 5 holds both rings until its backstage cleanup ends. Losing the cleanup ring
    first must not hand the main ring to Round 6 while that cleanup is still running."""

    main = InMemoryBoutLeaseStore()
    round5 = RefusingRenew(ring_key="round5")
    engine = BackgroundCleanupConnectionSpikeEngine()
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=main,
        round5_lease_store=round5,
    )
    _short_leases(manager)
    created = await manager.create(_round_five())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await _reach(manager, created.id, SessionState.VERIFIED)
    await asyncio.wait_for(engine.cleanup_started.wait(), timeout=5)
    record = manager._records[created.id]
    cleanup = record.connection_spike_cleanup_task
    assert cleanup is not None and not cleanup.done()
    round5.refusing = True
    try:
        await _until(lambda: record.round5_lease is None)
        round5.refusing = False
        assert not cleanup.done()

        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError):
            await manager.start_arm(later.id, OTHER)
        held = await main.current()
        assert held is not None and held.session_id == created.id
    finally:
        engine.delete_accepted.set()
        engine.cleanup_complete.set()
    await asyncio.wait_for(asyncio.wait({cleanup}), timeout=5)
    await _ring_free(main)
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


class FailingBackgroundCleanup(BackgroundCleanupConnectionSpikeEngine):
    """Round 5 backstage cleanup that fails in flight on ``fail``; its reconcile then waits
    for ``reconciled`` before it converges."""

    def __init__(self) -> None:
        super().__init__()
        self.fail = asyncio.Event()
        self.reconciling = asyncio.Event()
        self.reconciled = asyncio.Event()

    async def wait_for_proxy_delete_accepted(self) -> None:
        await self.fail.wait()
        raise RuntimeError("the proxy delete was never accepted")

    def proxy_delete_accepted(self) -> bool:
        return False

    async def reconcile_failed_cleanup(self, bout_id, current_fencing_token) -> None:
        self.reconciling.set()
        await self.reconciled.wait()
        await super().reconcile_failed_cleanup(bout_id, current_fencing_token)


async def test_a_cleanup_failing_after_its_lease_was_lost_converges_under_a_new_one() -> None:
    """The cleanup ring is lost, then the cleanup in flight fails. Convergence claims a new
    cleanup lease and reconciles under it with the main ring still held, and only then
    releases both rings."""

    main = InMemoryBoutLeaseStore()
    round5 = RefusingRenew(ring_key="round5")
    engine = FailingBackgroundCleanup()
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=main,
        round5_lease_store=round5,
    )
    _short_leases(manager)
    manager._cleanup_retry_initial = 0.01
    manager._cleanup_retry_max = 0.05
    created = await manager.create(_round_five())
    try:
        await manager.start_arm(created.id, OWNER)
        await _reach(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id, OWNER)
        await _reach(manager, created.id, SessionState.VERIFIED)
        await asyncio.wait_for(engine.cleanup_started.wait(), timeout=5)
        record = manager._records[created.id]
        lost = record.round5_lease
        assert lost is not None and lost.phase == "round5_cleanup"
        round5.refusing = True
        await _until(lambda: record.round5_lease is None)
        round5.refusing = False
        engine.fail.set()

        await asyncio.wait_for(engine.reconciling.wait(), timeout=5)
        reclaimed = record.round5_lease
        assert reclaimed is not None and reclaimed.phase == "round5_cleanup"
        assert reclaimed.fencing_token > lost.fencing_token
        assert _fence(await round5.current()) == _fence(reclaimed)
        held = await main.current()
        assert held is not None and held.session_id == created.id
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError):
            await manager.start_arm(later.id, OTHER)

        engine.reconciled.set()
        await _ring_free(main)
        await _ring_free(round5)
        assert engine.reconcile_calls == [(created.id, reclaimed.fencing_token)]
        settled = await manager.get(created.id)
        assert settled.round5_setup is not None
        assert settled.round5_setup.cleanup_retryable is False
        assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    finally:
        engine.fail.set()
        engine.reconciled.set()
        clear_round5_cleanup_owed(created.id)
        await manager.close()


async def test_a_terminal_release_given_up_at_its_ttl_keeps_the_ring_closed_while_work_runs() -> (
    None
):
    """The background release ran out of lease while the run was still finishing, say on a
    slow cost-window close: the row lapses, and this process waits for that run."""

    store = GatedCoordinator(phase="run_committed")
    store.gate_release = True
    manager = RunManager(
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=store,
    )
    _short_leases(manager)
    manager._terminal_release_call_timeout = 0.05
    finished = await manager.create(_round_six())
    record = manager._records[finished.id]
    record.operator = OWNER
    sent = time.monotonic()
    lease = await store.claim(
        session_id=finished.id,
        operator=OWNER,
        phase="run_committed",
        session_state=SessionState.VERIFIED,
        round_id=record.snapshot.round.id.value,
        round_title=record.snapshot.round.title,
        competitor_id=record.snapshot.competitor.id.value,
        competitor_name=record.snapshot.competitor.short_name,
        ttl=timedelta(seconds=0.5),
    )
    manager._set_lease(record, lease, sent)
    closed = asyncio.Event()
    run = asyncio.create_task(closed.wait(), name=f"run-{finished.id}")
    record.task = run
    manager._retry_terminal_release_in_background(record, lease)
    try:
        await _until(lambda: record.lease is None)
        await _ring_free(store)
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
    finally:
        closed.set()
        store.release_answer.set()
    await run
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def test_settlement_started_before_a_terminal_release_leaves_the_ring_open() -> None:
    """Round 4 starts settlement before it releases; the next arm of its round waits for
    that settlement itself, so the release closes nothing here."""

    store = InMemoryBoutLeaseStore()
    manager = RunManager(
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=store,
    )
    _short_leases(manager)
    finished = await manager.create(_round_six())
    record = manager._records[finished.id]
    record.operator = OWNER
    sent = time.monotonic()
    lease = await _claim_directly(store, record, "run_committed")
    manager._set_lease(record, lease, sent)
    settled = asyncio.Event()
    settlement = asyncio.create_task(settled.wait(), name=f"round-settlement-{finished.id}")
    record.settlement_task = settlement
    try:
        assert await manager._release_bout(record)
        assert record.ring_lease_lost is False
        later = await manager.create(_round_six())
        assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    finally:
        settled.set()
    await settlement
    await manager.close()


async def test_a_cancelled_runs_cost_close_is_its_own_cleanup_and_still_retried() -> None:
    """A lost lease cancels the run mid-bout while the coordinator blips. The run's
    ``finally`` closes its cost window from the already-cancelled task: that close is the
    task's own cleanup, not a call anyone gave up on, so it gets its one retry and the
    card can ring again."""

    ledger = LedgerClosingThroughLakebase()
    resolver = HeldCompetitorResolver()
    manager = cost_window_manager(ledger, resolver=resolver, verifier=make_verifier())
    manager._lease_heartbeat = 0.01
    created = await cost_window_session(manager)
    record = manager._records[created.id]
    await manager.start_arm(created.id)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    run = record.task
    committed = record.lease
    assert run is not None and committed is not None and record.cost_bout_id is not None
    ring = manager._lease_store_for_record(record)

    async def refused(lease, *, ttl):
        raise LeaseLostError("RING LEASE LOST DURING ACTIVE WORK")

    ring.renew = refused
    await asyncio.wait_for(asyncio.wait({run}), timeout=5)
    del ring.renew
    assert await ring.release(committed)
    resolver.release.set()

    assert run.cancelled()
    assert (await manager.get(created.id)).failure == LOST
    assert ledger.connections == 2
    assert ledger.closes == 1
    assert record.cost_bout_id is None
    await manager.start_arm(created.id)
    await _reach(manager, created.id, SessionState.ARMED)
    assert (await manager.start_run(created.id)).state == SessionState.RUNNING
    await manager.close()


async def test_a_towelled_runs_own_cost_close_ends_the_window_at_the_towel() -> None:
    """The towel's own close can still be landing when the run's ``finally`` closes the
    same window, after towel cleanup has moved the card's clock on. Both end it at the
    towel, so neither disagrees with the other."""

    ledger = RecordingCostLedger()
    manager = RunManager()
    manager._cost_ledger_store = ledger
    created = await manager.create(_round_six())
    record = manager._records[created.id]
    towel_at = datetime.now(UTC)
    record.cost_bout_id = "bout-under-test"
    record.cost_bout_started_at = towel_at - timedelta(seconds=30)
    record.cost_bout_kind = "run"
    record.snapshot.state = SessionState.TOWELLED
    record.snapshot.towel = TowelSnapshot(state=TowelState.CLEANING, requested_at=towel_at)
    record.snapshot.updated_at = towel_at + timedelta(seconds=3)

    async def stopped() -> None:
        return None

    await manager._run_cost_bout(record, "bout-under-test", stopped())
    assert ledger.closes == [("bout-under-test", towel_at, SessionState.TOWELLED.value)]
    assert record.cost_bout_id is None
    await manager.close()


async def test_an_old_fences_abandoned_renew_never_holds_up_a_new_fence() -> None:
    """The ring was lost with a renew stuck on a dead socket, then claimed again. The new
    fence renews at once: only a renew of its own fence may hold it back."""

    store = DeafRenew()
    manager = _deaf(store)
    record, old = await _holding(manager, store, timedelta(seconds=30))
    store.deaf = True
    try:
        await asyncio.wait_for(store.renewing.wait(), timeout=5)
        manager._cancel_lease_heartbeat(record)
        abandoned = await _until(lambda: next(iter(manager._abandoned_calls), None))
        async with record.lease_lock:
            record.lease = None
        assert await store.release(old)
        store.deaf = False

        sent = time.monotonic()
        new = await store.claim(
            session_id=record.snapshot.id,
            operator=OWNER,
            phase="run_committed",
            session_state=SessionState.RUNNING,
            round_id=record.snapshot.round.id.value,
            round_title=record.snapshot.round.title,
            competitor_id=record.snapshot.competitor.id.value,
            competitor_name=record.snapshot.competitor.short_name,
            ttl=timedelta(seconds=0.5),
        )
        manager._set_lease(record, new, sent)
        manager._start_lease_heartbeat(record, timedelta(seconds=0.5))
        await asyncio.sleep(0.8)

        assert record.lease is not None and record.lease.lease_id == new.lease_id
        assert record.lease.expires_at > new.expires_at
        assert not abandoned.done()
    finally:
        store.socket_closed.set()
    await asyncio.wait_for(asyncio.wait({abandoned}), timeout=5)
    await manager.close()


async def test_stopping_a_heartbeat_stops_the_renew_it_just_sent() -> None:
    """A terminal settle stops the heartbeat so the fence can lapse. A renew the heartbeat
    created on its last tick, not yet sent, must not reach the coordinator afterwards."""

    store = CountingRenew()
    manager = RunManager(lease_store=store)
    created = await manager.create(_round_one())
    record = manager._records[created.id]
    record.operator = OWNER
    sent = time.monotonic()
    lease = await _claim_directly(store, record, "run_committed")
    manager._set_lease(record, lease, sent)
    proven_until = manager._lease_proven_until(lease, record.lease_proof)
    heartbeat = asyncio.create_task(
        manager._renew_within(record, store, lease, timedelta(seconds=30), proven_until, 0.01)
    )
    record.lease_heartbeat_task = heartbeat
    record.lease_heartbeat_lease = lease
    await asyncio.sleep(0)

    manager._cancel_lease_heartbeat(record, lease)
    await asyncio.gather(heartbeat, return_exceptions=True)
    await asyncio.sleep(0.05)
    assert store.renewals == 0
    await manager.close()


@pytest.mark.parametrize(
    "slot",
    [
        "bell_handoff_task",
        "bell_convergence_task",
        "model_score_terminal_task",
        "settlement_task",
        "connection_spike_cleanup_task",
        "connection_spike_cleanup_retry_task",
    ],
)
async def test_ring_work_a_loss_leaves_running_keeps_the_ring_closed(slot: str) -> None:
    """A lost lease stops the run and the cooldown. This work it leaves to finish, and the
    ring stays closed in this process until it has."""

    store = RefusingRenew()
    manager = RunManager(
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=store,
    )
    _short_leases(manager)
    lost = await manager.create(_round_six())
    record = manager._records[lost.id]
    record.operator = OWNER
    record.snapshot.state = SessionState.RUNNING
    sent = time.monotonic()
    lease = await _claim_directly(store, record, "run_committed")
    manager._set_lease(record, lease, sent)
    finishes = asyncio.Event()
    work = asyncio.create_task(finishes.wait(), name=f"lost-ring-{slot}-{lost.id}")
    setattr(record, slot, work)
    store.refusing = True
    manager._start_lease_heartbeat(record, timedelta(seconds=30))
    try:
        await _until(lambda: record.lease is None)
        store.refusing = False
        assert await store.release(lease)

        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
        assert await store.current() is None
    finally:
        finishes.set()
    await work
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


class JumpingClock(InMemoryBoutLeaseStore):
    """A coordinator whose clock jumps ahead on ``jump``: every row lapses there at once,
    while each lease's ``expires_at`` is still in this process's future."""

    def __init__(self) -> None:
        self.ahead = timedelta(0)
        super().__init__(clock=lambda: datetime.now(UTC) + self.ahead)

    def jump(self) -> None:
        self.ahead = timedelta(minutes=5)


async def _finishing(store: InMemoryBoutLeaseStore):
    """A Round 6 card holding its ``run_committed`` fence while its run still finishes."""

    manager = RunManager(
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=store,
    )
    _short_leases(manager)
    created = await manager.create(_round_six())
    record = manager._records[created.id]
    record.operator = OWNER
    record.snapshot.state = SessionState.RUNNING
    sent = time.monotonic()
    lease = await _claim_directly(store, record, "run_committed")
    manager._set_lease(record, lease, sent)
    finishes = asyncio.Event()
    run = asyncio.create_task(finishes.wait(), name=f"run-{created.id}")
    record.task = run
    return manager, record, lease, finishes, run


async def test_a_background_release_that_finds_the_row_lapsed_keeps_the_ring_closed() -> None:
    """The coordinator's clock jumped, so the row lapsed while this process could still
    prove it. The background release finds the ring empty without having released it, and
    the run still finishing keeps the ring closed here."""

    store = JumpingClock()
    manager, record, lease, finishes, run = await _finishing(store)
    store.jump()
    assert lease.expires_at > datetime.now(UTC)
    manager._retry_terminal_release_in_background(record, lease)
    try:
        await _until(lambda: record.lease is None)
        assert record.ring_lease_lost is True
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
        assert await store.current() is None
    finally:
        finishes.set()
    await run
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def test_a_terminal_settle_that_finds_the_row_lapsed_keeps_the_ring_closed() -> None:
    """The same lapse, met by the bout's own terminal release: the bout still finishes with
    its own outcome, and its run still finishing keeps the ring closed here."""

    store = JumpingClock()
    manager, record, lease, finishes, run = await _finishing(store)
    store.jump()
    assert lease.expires_at > datetime.now(UTC)
    message = "The AWS lane's verifier lost its connection"
    try:
        await manager._finish_live_orders_failure(record, message)
        failed = await manager.get(record.snapshot.id)
        assert failed.state == SessionState.FAILED
        assert failed.failure == message
        assert record.lease is None
        assert record.ring_lease_lost is True
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
        assert await store.current() is None
    finally:
        finishes.set()
    await run
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def test_a_terminal_release_the_coordinator_confirms_leaves_the_ring_open() -> None:
    """The control: a confirmed release is the bout giving its ring up, so the run still
    finishing after it closes nothing here."""

    store = InMemoryBoutLeaseStore()
    manager, record, _lease, finishes, run = await _finishing(store)
    try:
        await manager._finish_live_orders_failure(record, "The AWS lane's verifier failed")
        assert record.lease is None
        assert record.ring_lease_lost is False
        later = await manager.create(_round_six())
        assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
        assert not run.done()
    finally:
        finishes.set()
    await run
    await manager.close()


async def test_a_bout_release_that_finds_the_row_lapsed_keeps_the_ring_closed() -> None:
    """``_release_bout`` meets the same lapse: it reports the ring released, as its callers
    expect, but the run still finishing keeps the ring closed here."""

    store = JumpingClock()
    manager, record, lease, finishes, run = await _finishing(store)
    store.jump()
    assert lease.expires_at > datetime.now(UTC)
    try:
        assert await manager._release_bout(record) is True
        assert record.lease is None
        assert record.ring_lease_lost is True
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
        assert await store.current() is None
    finally:
        finishes.set()
    await run
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def test_a_bout_release_the_coordinator_confirms_leaves_the_ring_open() -> None:
    store = InMemoryBoutLeaseStore()
    manager, record, _lease, finishes, run = await _finishing(store)
    try:
        assert await manager._release_bout(record) is True
        assert record.lease is None
        assert record.ring_lease_lost is False
        later = await manager.create(_round_six())
        assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
        assert not run.done()
    finally:
        finishes.set()
    await run
    await manager.close()


async def test_the_board_keeps_a_ring_closed_while_the_work_that_lost_it_still_runs() -> None:
    """The lost row is gone, so the coordinator alone would call the round free.

    Prepare on that ring is refused here until the work that lost it has stopped, so
    the board says so too, rather than offering a start that would be refused. Other
    rounds' rings are not touched.
    """

    manager = RunManager(
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=InMemoryBoutLeaseStore(),
        round_isolation=True,
        installation_id="install-board",
    )
    created = await manager.create(_round_six())
    record = manager._records[created.id]
    record.operator = OWNER
    record.snapshot.state = SessionState.RUNNING
    store = manager._lease_store_for_record(record)
    sent = time.monotonic()
    lease = await _claim_directly(store, record, "run_committed")
    manager._set_lease(record, lease, sent)
    finishes = asyncio.Event()
    run = asyncio.create_task(finishes.wait(), name=f"run-{created.id}")
    record.task = run
    try:
        assert await InMemoryBoutLeaseStore.release(store, lease)
        assert await manager._release_bout(record) is True
        assert record.ring_lease_lost is True
        assert await store.current() is None

        stopping = await manager.bout_status(round_id=RoundId.ANALYZE_LIVE_ORDERS)
        assert stopping.active is False
        assert stopping.can_start is False
        assert stopping.maintenance_detail is not None
        assert "ACTIVE WORK IS STILL STOPPING" in stopping.maintenance_detail
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
        board = await manager.all_bout_statuses()
        assert board[RoundId.ANALYZE_LIVE_ORDERS].can_start is False
        assert board[RoundId.WAKE_IDLE_APP].can_start is True
    finally:
        finishes.set()
    await run
    reopened = await manager.bout_status(round_id=RoundId.ANALYZE_LIVE_ORDERS)
    assert reopened.can_start is True
    assert reopened.maintenance_detail is None
    await manager.close()


async def _release_finishing(manager: RunManager, record: SessionRecord, path: str) -> None:
    if path == "terminal settle":
        await manager._finish_live_orders_failure(record, "The AWS lane's verifier failed")
    else:
        assert await manager._release_bout(record) is True


def _lost_lines(caplog: pytest.LogCaptureFixture) -> tuple[list, list]:
    declared = [r for r in caplog.records if "declared lost" in r.getMessage()]
    alarms = [r for r in caplog.records if "work that lost it still runs" in r.getMessage()]
    return declared, alarms


@pytest.mark.parametrize("path", ["terminal settle", "release_bout"])
async def test_a_release_that_finds_the_row_gone_warns_and_raises_the_alarm(
    path: str, caplog: pytest.LogCaptureFixture
) -> None:
    """The row is gone, so another replica can claim the ring now, and the run still
    finishing here is lost-ring work that has already outlived it."""

    store = JumpingClock()
    manager, record, lease, finishes, run = await _finishing(store)
    store.jump()
    try:
        with caplog.at_level(logging.WARNING, logger="server.manager"):
            await _release_finishing(manager, record, path)
            await _until(lambda: _lost_lines(caplog)[1])
        assert record.ring_lease_lost is True
    finally:
        finishes.set()
    await run
    await manager.close()
    (declared,), (alarm,) = _lost_lines(caplog)
    assert declared.levelno == logging.WARNING
    for detail in (
        "lease=main",
        "reason=row_absent",
        f"session={record.snapshot.id}",
        "phase=run_committed",
        f"lease_id={lease.lease_id}",
        f"fence={lease.fencing_token}",
        "remaining_seconds=",
    ):
        assert detail in declared.getMessage()
    assert alarm.levelno == logging.ERROR
    assert run.get_name() in alarm.getMessage()


async def test_a_release_from_the_bouts_own_run_does_not_raise_the_alarm_on_that_run(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The run giving the ring up is not work outliving it: only the warning is logged."""

    store = JumpingClock()
    manager, record, _lease, finishes, idle = await _finishing(store)
    finishes.set()
    await idle
    released = asyncio.Event()
    finished = asyncio.Event()

    async def run_that_releases() -> None:
        assert await manager._release_bout(record) is True
        released.set()
        await finished.wait()

    store.jump()
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        run = asyncio.create_task(run_that_releases(), name=f"run-{record.snapshot.id}")
        record.task = run
        try:
            await asyncio.wait_for(released.wait(), timeout=5)
            await asyncio.sleep(0.05)
            assert not run.done()
        finally:
            finished.set()
        await run
    declared, alarms = _lost_lines(caplog)
    assert len(declared) == 1
    assert alarms == []
    await manager.close()


@pytest.mark.parametrize("path", ["terminal settle", "release_bout"])
async def test_a_confirmed_release_neither_warns_nor_raises_the_alarm(
    path: str, caplog: pytest.LogCaptureFixture
) -> None:
    store = InMemoryBoutLeaseStore()
    manager, record, _lease, finishes, run = await _finishing(store)
    try:
        with caplog.at_level(logging.WARNING, logger="server.manager"):
            await _release_finishing(manager, record, path)
            await asyncio.sleep(0.05)
        assert record.ring_lease_lost is False
        assert not run.done()
    finally:
        finishes.set()
    await run
    await manager.close()
    assert _lost_lines(caplog) == ([], [])


async def test_round_four_finding_its_row_gone_at_its_terminal_settle_says_so(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Round 4's terminal publication finds its row gone without a confirmed release. The
    bout still fails as a lost lease, now with the warning, and the run that only awaits
    that publication is not taken for work outliving the ring."""

    store = LoseTerminalLeaseStore(phase="run_committed", current_state="none")
    engine = FakeModelScoreEngine()
    engine.settle_and_restore_baseline = None
    manager = RunManager(model_score_factory=lambda _competitor: engine, lease_store=store)
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        await manager.start_run(created.id, OWNER)
        failed = await _reach(manager, created.id, SessionState.FAILED)
        await drain_record_operations(manager, record)
    assert failed.failure == "Round 4's ring lease was lost before the result could be published."
    assert record.ring_lease_lost is True
    (declared,), alarms = _lost_lines(caplog)
    for detail in (
        "lease=main",
        "reason=row_absent",
        f"session={created.id}",
        "phase=run_committed",
    ):
        assert detail in declared.getMessage()
    assert alarms == []
    await manager.close()


async def _failing_round_one(store: InMemoryBoutLeaseStore):
    """A Round 1 bout failing after its bell; neither its idle watch nor its card's next arm
    ever sees zero."""

    manager = RunManager(
        live_orders_factory=lambda _competitor: _engine(_lanes_that_never_deliver()),
        lease_store=store,
        resolver=HangingArmResolver(),
    )
    _short_leases(manager)
    created = await manager.create(_round_one())
    record = manager._records[created.id]
    record.operator = OWNER
    record.snapshot.state = SessionState.RUNNING
    record.run_started_monotonic_ns = time.monotonic_ns()
    record.live_targets = (
        HangingArmTarget("lakebase", "Lakebase"),
        HangingArmTarget("competitor", "Aurora Serverless v2"),
    )
    sent = time.monotonic()
    lease = await _claim_directly(store, record, "run_committed")
    manager._set_lease(record, lease, sent)
    return manager, record


@pytest.mark.parametrize("lapsed", [True, False], ids=["lost release", "confirmed release"])
async def test_round_one_idle_watch_after_a_failure_leaves_the_ring_open(lapsed: bool) -> None:
    """``_fail`` settles Round 1's release, then watches both lanes return to idle for up
    to fifteen minutes. The watch only reads the two control planes, so the ring stays open
    beside it, whether the release was confirmed or found the row lapsed."""

    store = JumpingClock()
    manager, record = await _failing_round_one(store)
    if lapsed:
        store.jump()
    await manager._fail(record, "The Lakebase lane could not be verified")
    watch = record.cooldown_task
    assert watch is not None and not watch.done()
    assert record.lease is None
    assert record.ring_lease_lost is lapsed
    later = await manager.create(_round_six())
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    assert not watch.done()
    await manager.close()


class HeldCooldownTarget:
    """A control plane whose answer to an idle watch is held until the test lets it go."""

    def __init__(self, target_id: str, answer: dict[str, object]) -> None:
        self.id = self.name = target_id
        self.answer = answer
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.entered.set()
        await self.release.wait()
        return self.answer


class CountingCooldownTarget:
    """A control plane answering at once, counting how often it is asked."""

    def __init__(self, target_id: str, answer: dict[str, object] | Exception) -> None:
        self.id = self.name = target_id
        self.answer = answer
        self.calls = 0

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.calls += 1
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


class DeadSocketTarget(HangingArmTarget):
    """A control-plane call that, like psycopg on a black-holed socket, outlives its first
    cancellation until the socket gives up."""

    def __init__(self, id: str, name: str) -> None:
        super().__init__(id, name)
        self.gives_up = asyncio.Event()

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        try:
            return await super().assert_armed(not_before=not_before)
        except asyncio.CancelledError:
            await self.gives_up.wait()
            raise


def _at_zero() -> tuple[CountingCooldownTarget, CountingCooldownTarget]:
    """Both control planes reporting zero at once: Lakebase has no automatic scale-to-zero
    to wait for, and the AWS lane is paused."""

    return (
        CountingCooldownTarget("lakebase", {"eligible": False}),
        CountingCooldownTarget("competitor", {"state": "PAUSED"}),
    )


def _still_active() -> tuple[CountingCooldownTarget, CountingCooldownTarget]:
    return (
        CountingCooldownTarget("lakebase", TargetNotArmedError("Lakebase is still ACTIVE")),
        CountingCooldownTarget("competitor", TargetNotArmedError("Aurora is still ACTIVE")),
    )


def _in_state(state: CooldownState) -> Callable[[dict], bool]:
    return lambda cooldown: cooldown["state"] == state


def _both_lanes_settled(cooldown: dict) -> bool:
    return all(
        lane["state"] in {CooldownLaneState.CONFIRMED_ZERO, CooldownLaneState.NOT_SUPPORTED}
        for lane in cooldown["lanes"].values()
    )


def _hold_publish(
    record: SessionRecord, monkeypatch: pytest.MonkeyPatch, holds: Callable[[dict], bool]
) -> tuple[asyncio.Event, asyncio.Event]:
    """Hold the publisher of the record's first cooldown that ``holds`` is true of, once it is
    published, until ``let_go`` is set. Returns ``(held, let_go)``."""

    held, let_go = asyncio.Event(), asyncio.Event()
    publish = record.event_log.publish

    async def publish_then_hold(event, payload, **kwargs):
        published = await publish(event, payload, **kwargs)
        cooldown = payload.get("cooldown")
        if cooldown is not None and not held.is_set() and holds(cooldown):
            held.set()
            await let_go.wait()
        return published

    monkeypatch.setattr(record.event_log, "publish", publish_then_hold)
    return held, let_go


def _hold_wait(
    record: SessionRecord, monkeypatch: pytest.MonkeyPatch
) -> tuple[asyncio.Event, asyncio.Event]:
    """Hold the record's idle watch, once it has started watching, as its first wait ends: the
    wait for its next poll or, when that comes first, for its deadline. Held until ``let_go``
    is set. Returns ``(held, let_go)``."""

    held, let_go = asyncio.Event(), asyncio.Event()
    sleep = asyncio.sleep

    async def sleep_then_hold(delay, result=None):
        slept = await sleep(delay, result)
        if (
            not held.is_set()
            and asyncio.current_task() is record.idle_watch_task
            and "cooldown_started" in _events(record)
        ):
            held.set()
            await let_go.wait()
        return slept

    monkeypatch.setattr("server.manager.asyncio.sleep", sleep_then_hold)
    return held, let_go


async def _rearmed_at(
    manager: RunManager,
    record: SessionRecord,
    monkeypatch: pytest.MonkeyPatch,
    state: CooldownState,
):
    """Fail the Round 1 bout, then arm its card again once its idle watch has published the
    cooldown as ``state`` but before the watch goes on to settle it. Returns the new bout's
    lease, and whether the ring stood lost at the re-arm, once the old watch has ended,
    however it ended."""

    held, let_go = _hold_publish(record, monkeypatch, _in_state(state))
    await manager._fail(record, "The Lakebase lane could not be verified")
    watch = record.cooldown_task
    assert watch is not None
    try:
        await asyncio.wait_for(held.wait(), timeout=5)
        lost = record.ring_lease_lost
        assert (await manager.start_arm(record.snapshot.id, OWNER)).state == SessionState.CHECKING
        lease = record.lease
        assert lease is not None and lease.phase == "checking"
    finally:
        let_go.set()
    await asyncio.wait_for(asyncio.wait({watch}), timeout=5)
    return lease, lost


async def _rearmed_as_the_idle_watch_times_out(
    manager: RunManager, record: SessionRecord, monkeypatch: pytest.MonkeyPatch
):
    """The re-arm lands once the watch has timed out, before it reaches its failure fence."""

    manager._cooldown_timeout = 0.3
    manager._cooldown_poll_timeout = 0.05
    manager._arm_poll = 0.01
    return await _rearmed_at(manager, record, monkeypatch, CooldownState.FAILED)


async def _rearmed_as_the_idle_watch_turns_ready(
    manager: RunManager, record: SessionRecord, monkeypatch: pytest.MonkeyPatch
):
    """The re-arm lands once the watch has seen both lanes at zero, before it settles its
    release."""

    record.live_targets = _at_zero()
    return await _rearmed_at(manager, record, monkeypatch, CooldownState.READY)


@pytest.mark.parametrize("lapsed", [True, False], ids=["lost release", "confirmed release"])
async def test_an_idle_watch_timing_out_after_its_card_rearmed_leaves_the_new_lease_alone(
    lapsed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The owner arms the failed card again as its idle watch times out, before the watch
    reaches its failure fence. That fence is for the lease the watch started under, which the
    release already gave up: the new bout's lease keeps its heartbeat and its row."""

    store = JumpingClock()
    manager, record = await _failing_round_one(store)
    if lapsed:
        store.jump()
    rearmed, lost = await _rearmed_as_the_idle_watch_times_out(manager, record, monkeypatch)
    assert lost is lapsed
    await asyncio.sleep(2 * manager._active_lease_ttl)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    assert record.lease_heartbeat_lease is not None
    assert manager._same_exact_lease(record.lease_heartbeat_lease, rearmed)
    held = await store.current()
    assert held is not None and manager._same_exact_lease(held, rearmed)
    assert record.lease is not None and manager._same_exact_lease(record.lease, rearmed)
    assert record.ring_lease_lost is False
    await manager.close()


@pytest.mark.parametrize("lapsed", [True, False], ids=["lost release", "confirmed release"])
async def test_an_idle_watch_turning_ready_after_its_card_rearmed_releases_nothing(
    lapsed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failed card's idle watch sees both lanes at zero, and the owner arms the card again
    before the watch settles its release. That release is of the lease the watch started
    under, which the failure already gave up: the new bout's checking lease keeps its
    heartbeat and its row through cooldown_ready."""

    store = JumpingClock()
    manager, record = await _failing_round_one(store)
    if lapsed:
        store.jump()
    rearmed, lost = await _rearmed_as_the_idle_watch_turns_ready(manager, record, monkeypatch)
    assert lost is lapsed
    assert "cooldown_ready" in _events(record)
    await asyncio.sleep(2 * manager._active_lease_ttl)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    assert record.lease_heartbeat_lease is not None
    assert manager._same_exact_lease(record.lease_heartbeat_lease, rearmed)
    held = await store.current()
    assert held is not None and manager._same_exact_lease(held, rearmed)
    assert record.lease is not None and manager._same_exact_lease(record.lease, rearmed)
    assert record.ring_lease_lost is False
    await manager.close()


async def _watching_round_one(store: InMemoryBoutLeaseStore, *, at_zero: bool = False):
    """A Round 1 verdict holding its fence in the cooldown phase while the idle watch begun on
    that fence runs. Its control planes answer zero at once, or never."""

    manager, record = await _failing_round_one(store)
    manager._cooldown_timeout = 0.3
    manager._cooldown_poll_timeout = 0.05
    manager._arm_poll = 0.01
    manager._active_lease_ttl = 5.0
    if at_zero:
        record.live_targets = _at_zero()
    async with record.lock:
        await manager._transition_bout_to_cleanup(record, SessionState.VERIFIED)
        record.snapshot.cooldown = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
    cooling = record.lease
    assert cooling is not None and cooling.phase == "cooldown"
    manager._schedule_round_one_cooldown(record)
    watch = record.cooldown_task
    assert watch is not None
    return manager, record, cooling, watch


async def test_an_idle_watch_timing_out_pins_the_cooldown_fence_it_started_under() -> None:
    """The control: a verdict keeps Round 1's fence in its cooldown phase while the watch
    runs, and the watch timing out under it pins that fence as cooldown_failed and stops
    renewing it, so the round stays closed for one ordinary TTL."""

    store = InMemoryBoutLeaseStore()
    manager, record, cooling, watch = await _watching_round_one(store)
    await asyncio.wait_for(watch, timeout=5)
    pinned = record.lease
    assert pinned is not None and pinned.phase == "cooldown_failed"
    assert manager._same_lease_authority(pinned, cooling)
    assert record.lease_heartbeat_task is None
    held = await store.current()
    assert held is not None and manager._same_exact_lease(held, pinned)
    await manager.close()


async def test_the_next_bouts_idle_watch_replaces_one_its_card_left_running() -> None:
    """An earlier bout's idle watch can still be waiting on a control plane when the card's
    next bout ends: the re-arm cancelled it, but a call on a dead socket outlives that. It
    acts on nothing the card now shows or holds, so the next bout's watch replaces it rather
    than waiting behind it, and pins the new cooldown fence."""

    store = InMemoryBoutLeaseStore()
    manager, record = await _failing_round_one(store)
    manager._cooldown_timeout = 30
    manager._cooldown_poll_timeout = 30
    manager._arm_poll = 0.01
    dead = (
        DeadSocketTarget("lakebase", "Lakebase"),
        DeadSocketTarget("competitor", "Aurora Serverless v2"),
    )
    record.live_targets = dead
    await manager._fail(record, "The Lakebase lane could not be verified")
    stale = record.cooldown_task
    assert stale is not None
    await asyncio.wait_for(asyncio.gather(*(target.entered.wait() for target in dead)), timeout=5)
    assert (await manager.start_arm(record.snapshot.id, OWNER)).state == SessionState.CHECKING
    await asyncio.wait_for(asyncio.gather(*(target.cancelled.wait() for target in dead)), timeout=5)
    assert not stale.done()
    manager._cooldown_timeout = 0.3
    manager._cooldown_poll_timeout = 0.05
    manager._active_lease_ttl = 5.0
    async with record.lock:
        await manager._transition_bout_to_cleanup(
            record, SessionState.VERIFIED, expected_phase="checking"
        )
        record.snapshot.cooldown = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
    cooling = record.lease
    assert cooling is not None and cooling.phase == "cooldown"
    manager._schedule_round_one_cooldown(record)
    watch = record.cooldown_task
    assert watch is not None and watch is not stale
    for target in dead:
        target.gives_up.set()
    await asyncio.wait_for(asyncio.wait({stale}), timeout=5)
    await asyncio.wait_for(watch, timeout=5)
    pinned = record.lease
    assert pinned is not None and pinned.phase == "cooldown_failed"
    assert manager._same_lease_authority(pinned, cooling)
    assert record.lease_heartbeat_task is None
    await manager.close()


async def test_the_idle_watch_never_replaces_a_towel_or_cleanup_in_its_slot() -> None:
    """The control: only the record's own idle watch is replaced. A towel or cleanup in the
    same slot acts on the round's sources and is left to finish."""

    store = InMemoryBoutLeaseStore()
    manager, record = await _failing_round_one(store)
    record.snapshot.cooldown = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
    finishes = asyncio.Event()
    towel = asyncio.create_task(finishes.wait(), name=f"towel-cleanup-{record.snapshot.id}")
    record.cooldown_task = towel
    manager._schedule_round_one_cooldown(record)
    await asyncio.sleep(0)
    assert record.cooldown_task is towel
    assert record.idle_watch_task is None
    assert not towel.done()
    finishes.set()
    await towel
    await manager.close()


@pytest.mark.parametrize("name", ["auto-artifact-cleanup", "auto-idle-watch"])
async def test_cleanup_in_the_idle_watch_slot_still_keeps_a_lost_ring_closed(name: str) -> None:
    """The control: the same slot carries towel and artifact cleanups, which act on the
    round's sources, and those still hold the ring they lost until they end. Only the idle
    watch the record started is let off, never a task for carrying its name."""

    store = JumpingClock()
    manager, record, _lease, finishes, run = await _finishing(store)
    finishes.set()
    await run
    deleted = asyncio.Event()
    cleanup = asyncio.create_task(deleted.wait(), name=f"{name}-{record.snapshot.id}")
    record.cooldown_task = cleanup
    store.jump()
    try:
        assert await manager._release_bout(record) is True
        assert record.ring_lease_lost is True
        later = await manager.create(_round_six())
        with pytest.raises(InvalidStateError, match="STILL STOPPING"):
            await manager.start_arm(later.id, OTHER)
    finally:
        deleted.set()
    await cleanup
    assert (await manager.start_arm(later.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def _a_stale_answer_meets_the_next_bouts_cooldown(store: InMemoryBoutLeaseStore) -> None:
    """An earlier bout's idle watch is still waiting on both control planes when the card's
    next bout installs its own cooldown and lease. What those control planes then answer is
    about the earlier bout: it confirms nothing in the new cooldown, so the next bout's watch
    still polls the AWS lane, which is active, and the new cooldown lease is kept."""

    manager = RunManager(lease_store=store)
    manager._arm_poll = 0.01
    manager._cooldown_poll_timeout = 1
    manager._cooldown_timeout = 2
    created = await manager.create(_round_one())
    record = manager._records[created.id]
    stale_lakebase = HeldCooldownTarget("lakebase", {"eligible": False})
    stale_competitor = HeldCooldownTarget(
        "competitor", {"state": "PAUSED", "observed_at": "2020-01-01T00:00:00+00:00"}
    )
    record.live_targets = (stale_lakebase, stale_competitor)
    record.snapshot.state = SessionState.FAILED
    record.snapshot.cooldown = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
    manager._schedule_round_one_cooldown(record)
    stale = record.cooldown_task
    assert stale is not None
    try:
        await asyncio.wait_for(stale_lakebase.entered.wait(), timeout=5)
        await asyncio.wait_for(stale_competitor.entered.wait(), timeout=5)
        fresh_lease = await _claim_directly(store, record, "cooldown")
        manager._set_lease(record, fresh_lease, time.monotonic())
        manager._start_lease_heartbeat(record, fresh_lease.expires_at - fresh_lease.updated_at)
        fresh_competitor = CountingCooldownTarget(
            "competitor", TargetNotArmedError("the next bout is still ACTIVE")
        )
        async with record.lock:
            record.live_targets = (
                CountingCooldownTarget("lakebase", {"eligible": False}),
                fresh_competitor,
            )
            record.snapshot.cooldown = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
        published = len(record.event_log.events)
        stale_lakebase.release.set()
        stale_competitor.release.set()
        await asyncio.wait_for(asyncio.wait({stale}), timeout=5)
        published_by_the_stale_watch = _events(record)[published:]

        manager._schedule_round_one_cooldown(record)
        fresh = record.cooldown_task
        assert fresh is not None
        await _until(lambda: fresh.done() or fresh_competitor.calls)
        cooldown = record.snapshot.cooldown
        assert cooldown is not None
        assert cooldown.lanes["competitor"].state != CooldownLaneState.CONFIRMED_ZERO
        assert fresh_competitor.calls > 0
        held = await store.current()
        assert held is not None and manager._same_exact_lease(held, fresh_lease)
        assert record.lease is not None and manager._same_exact_lease(record.lease, fresh_lease)
        assert published_by_the_stale_watch == []
    finally:
        await manager.close()


async def test_a_stale_idle_watch_cannot_confirm_or_release_the_next_bouts_cooldown() -> None:
    await _a_stale_answer_meets_the_next_bouts_cooldown(InMemoryBoutLeaseStore())


@pytest.mark.parametrize("moment", ["on its last answer", "between polls", "at its deadline"])
async def test_a_stale_idle_watch_does_nothing_once_its_cooldown_is_replaced(
    moment: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wherever an earlier watch is when the card's next bout installs its cooldown, it does
    nothing further: it marks no cooldown ready or failed, publishes nothing, and never polls
    the next bout's control planes."""

    store = InMemoryBoutLeaseStore()
    manager = RunManager(lease_store=store)
    manager._cooldown_poll_timeout = 0.05
    manager._arm_poll = 0.5 if moment == "at its deadline" else 0.01
    manager._cooldown_timeout = 0.3 if moment == "at its deadline" else 30
    created = await manager.create(_round_one())
    record = manager._records[created.id]
    record.live_targets = _at_zero() if moment == "on its last answer" else _still_active()
    record.snapshot.state = SessionState.FAILED
    record.snapshot.cooldown = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
    if moment == "on its last answer":
        held, let_go = _hold_publish(record, monkeypatch, _both_lanes_settled)
    else:
        held, let_go = _hold_wait(record, monkeypatch)
    manager._schedule_round_one_cooldown(record)
    stale = record.cooldown_task
    assert stale is not None
    fresh = _still_active()
    try:
        await asyncio.wait_for(held.wait(), timeout=5)
        async with record.lock:
            record.live_targets = fresh
            replaced = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
            record.snapshot.cooldown = replaced
        published = len(record.event_log.events)
    finally:
        let_go.set()
    await asyncio.wait_for(asyncio.wait({stale}), timeout=5)
    assert _events(record)[published:] == []
    assert [target.calls for target in fresh] == [0, 0]
    assert record.snapshot.cooldown is replaced
    assert replaced.state == CooldownState.WATCHING
    assert {lane.state for lane in replaced.lanes.values()} == {CooldownLaneState.WATCHING}
    await manager.close()


async def _towel_round_trip(
    manager: RunManager, store: InMemoryBoutLeaseStore, record: SessionRecord
):
    """Take the record's cooldown fence through towel_cleanup and back under the towel's own
    cooldown, as a towel's settlement does: the same lease id and fence, in the cooldown phase
    again."""

    async with record.lock:
        manager._cancel_lease_heartbeat(record)
        async with record.lease_lock:
            cooling = record.lease
        assert cooling is not None
        towelled = await store.transition(
            cooling,
            operator=OWNER,
            expected_phase="cooldown",
            phase="towel_cleanup",
            session_state=SessionState.TOWELLED,
            ttl=timedelta(seconds=30),
        )
        manager._set_lease(record, towelled, time.monotonic())
        record.snapshot.cooldown = manager._new_towel_cooldown(record)
        await manager._transition_bout_to_cleanup(
            record, SessionState.TOWELLED, expected_phase="towel_cleanup"
        )
    return record.lease


@pytest.mark.parametrize("moment", ["times out", "turns ready"])
async def test_a_watch_whose_cooldown_a_towel_replaced_leaves_the_same_fence_alone(
    moment: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A towel can take a cooldown fence through towel_cleanup and back to cooldown: the same
    lease id and fence, in the same phase again, under the towel's own cooldown. A watch begun
    on that fence before the towel neither pins nor releases it: the towel's watch will."""

    store = InMemoryBoutLeaseStore()
    ready = moment == "turns ready"
    manager, record, cooling, watch = await _watching_round_one(store, at_zero=ready)
    held, let_go = _hold_publish(
        record, monkeypatch, _in_state(CooldownState.READY if ready else CooldownState.FAILED)
    )
    try:
        await asyncio.wait_for(held.wait(), timeout=5)
        towelled = await _towel_round_trip(manager, store, record)
        assert towelled is not None and manager._same_exact_lease(towelled, cooling)
    finally:
        let_go.set()
    await asyncio.wait_for(asyncio.wait({watch}), timeout=5)
    assert record.lease is not None and manager._same_exact_lease(record.lease, towelled)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    row = await store.current()
    assert row is not None and manager._same_exact_lease(row, towelled)
    await manager.close()


@pytest.mark.parametrize("phase", ["checking", "cooldown"])
@pytest.mark.parametrize("moment", ["times out", "turns ready"])
async def test_a_watch_whose_card_holds_a_newer_lease_leaves_that_lease_alone(
    moment: str, phase: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cooldown a watch observes is still the one shown, but the card holds a newer lease.
    The watch neither pins nor releases that lease, nor stops its heartbeat, at its timeout or
    at its READY, whatever phase the lease is in."""

    store = InMemoryBoutLeaseStore()
    ready = moment == "turns ready"
    manager, record, _cooling, watch = await _watching_round_one(store, at_zero=ready)
    held, let_go = _hold_publish(
        record, monkeypatch, _in_state(CooldownState.READY if ready else CooldownState.FAILED)
    )
    try:
        await asyncio.wait_for(held.wait(), timeout=5)
        manager._cancel_lease_heartbeat(record)
        assert record.lease is not None and await store.release(record.lease) is True
        newer = await _claim_directly(store, record, phase)
        manager._set_lease(record, newer, time.monotonic())
        manager._start_lease_heartbeat(record, newer.expires_at - newer.updated_at)
    finally:
        let_go.set()
    await asyncio.wait_for(asyncio.wait({watch}), timeout=5)
    assert record.lease is not None and manager._same_exact_lease(record.lease, newer)
    assert record.lease_heartbeat_lease is not None
    assert manager._same_exact_lease(record.lease_heartbeat_lease, newer)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    row = await store.current()
    assert row is not None and manager._same_exact_lease(row, newer)
    await manager.close()


def _round_one_verdict() -> VerificationResult:
    """Round 1's verdict with the AWS lane unverified: the card fails and can be armed again."""

    now = time.monotonic_ns()
    return VerificationResult(
        started_ns=now,
        completed_ns=now,
        launch_skew_ms=None,
        lanes={
            "lakebase": LaneResult(
                id="lakebase",
                name="Lakebase",
                state=LaneState.VERIFIED,
                elapsed_ms=1.0,
                attempts=1,
                first_attempt_ns=now,
                completed_ns=now,
                connection_closed_at=datetime.now(UTC),
                error=None,
            ),
            "competitor": LaneResult(
                id="competitor",
                name="Aurora Serverless v2",
                state=LaneState.FAILED,
                elapsed_ms=None,
                attempts=1,
                first_attempt_ns=now,
                completed_ns=None,
                connection_closed_at=None,
                error="The transaction could not be verified",
            ),
        },
    )


@pytest.mark.parametrize("phase", ["checking", "cooldown"])
async def test_an_idle_watch_at_its_timeout_is_stopped_by_its_card_rearming_on_a_lapsed_fence(
    phase: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The verdict's cooldown row lapses on the coordinator clock before its heartbeat notices,
    and the owner arms the card again the instant its idle watch times out, before the watch
    reaches its failure fence. The re-arm stops that watch, and the newer bout's lease, still
    checking or already in its own cooldown, keeps its phase, its row and its heartbeat."""

    store = JumpingClock()
    manager, record = await _failing_round_one(store)
    manager._cooldown_timeout = 0.3
    manager._cooldown_poll_timeout = 0.05
    manager._arm_poll = 0.01
    record.bell_committed_at = record.snapshot.run_started_at = datetime.now(UTC)
    await manager._finish(record, _round_one_verdict())
    watch = record.cooldown_task
    assert watch is not None
    assert record.lease is not None and record.lease.phase == "cooldown"
    held, let_go = _hold_publish(record, monkeypatch, _in_state(CooldownState.FAILED))
    try:
        await asyncio.wait_for(held.wait(), timeout=5)
        store.jump()
        assert (await manager.start_arm(record.snapshot.id, OWNER)).state == SessionState.CHECKING
        if phase == "cooldown":
            async with record.lock:
                await manager._transition_bout_to_cleanup(
                    record, SessionState.VERIFIED, expected_phase="checking"
                )
                record.snapshot.cooldown = manager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
        newer = record.lease
        assert newer is not None and newer.phase == phase
    finally:
        let_go.set()
    await asyncio.wait_for(asyncio.wait({watch}), timeout=5)
    assert watch.cancelled()
    await asyncio.sleep(2 * manager._active_lease_ttl)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    assert record.lease_heartbeat_lease is not None
    assert manager._same_exact_lease(record.lease_heartbeat_lease, newer)
    row = await store.current()
    assert row is not None and manager._same_exact_lease(row, newer)
    assert record.lease is not None and manager._same_exact_lease(record.lease, newer)
    assert record.ring_lease_lost is False
    await manager.close()


async def test_a_refused_rearm_leaves_the_idle_watch_holding_its_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The owner arms the failed card again while its verdict's cooldown fence still holds
    the ring, and the claim is refused. Only the idle watch begun on that fence ever ends it,
    so the refusal leaves the watch running: it times out, pins the fence as cooldown_failed
    and stops renewing it, where a stopped watch would leave the fence renewed for good."""

    store = InMemoryBoutLeaseStore()
    manager, record = await _failing_round_one(store)
    manager._cooldown_timeout = 0.5
    manager._cooldown_poll_timeout = 0.05
    manager._arm_poll = 0.01
    manager._active_lease_ttl = 5.0
    held, let_go = _hold_wait(record, monkeypatch)
    record.bell_committed_at = record.snapshot.run_started_at = datetime.now(UTC)
    await manager._finish(record, _round_one_verdict())
    watch = record.cooldown_task
    assert watch is not None and watch is record.idle_watch_task
    cooling = record.lease
    assert cooling is not None and cooling.phase == "cooldown"
    try:
        await asyncio.wait_for(held.wait(), timeout=5)
        with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
            await manager.start_arm(record.snapshot.id, OWNER)
    finally:
        let_go.set()
    await asyncio.wait_for(asyncio.wait({watch}), timeout=5)
    pinned = record.lease
    assert pinned is not None and pinned.phase == "cooldown_failed"
    assert manager._same_lease_authority(pinned, cooling)
    assert record.lease_heartbeat_task is None
    row = await store.current()
    assert row is not None and manager._same_exact_lease(row, pinned)
    assert not watch.cancelled()
    await manager.close()


async def _a_watch_left_running(store: InMemoryBoutLeaseStore):
    """A failed Round 1 bout's idle watch still waiting on both control planes while the card's
    next bout runs, as it would be had nothing stopped it at the re-arm."""

    manager, record = await _failing_round_one(store)
    await manager._fail(record, "The Lakebase lane could not be verified")
    stale = record.cooldown_task
    stale_targets = record.live_targets
    assert stale is not None and stale_targets is not None
    await asyncio.wait_for(
        asyncio.gather(*(target.entered.wait() for target in stale_targets)), timeout=5
    )
    record.snapshot.state = SessionState.RUNNING
    record.bell_committed_at = record.snapshot.run_started_at = datetime.now(UTC)
    record.run_started_monotonic_ns = time.monotonic_ns()
    record.live_targets = (
        HangingArmTarget("lakebase", "Lakebase"),
        HangingArmTarget("competitor", "Aurora Serverless v2"),
    )
    sent = time.monotonic()
    manager._set_lease(record, await _claim_directly(store, record, "run_committed"), sent)
    return manager, record, stale, stale_targets


@pytest.mark.parametrize(
    ("ending", "first_event"),
    [("finish", "run_finished"), ("fail", "session_failed"), ("towel", "towel_update")],
)
async def test_the_next_bouts_cooldown_stops_an_idle_watch_left_running(
    ending: str, first_event: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """However the next bout ends, it stops a watch the last one left running before it
    publishes its own cooldown, not only once its own watch replaces that one."""

    store = InMemoryBoutLeaseStore()
    manager, record, stale, stale_targets = await _a_watch_left_running(store)
    stopped: list[bool] = []
    publish = record.event_log.publish

    async def publish_noting_the_stale_watch(event, payload, **kwargs):
        if event == first_event and not stopped:
            await asyncio.wait({stale}, timeout=1)
            stopped.append(stale.done())
        return await publish(event, payload, **kwargs)

    monkeypatch.setattr(record.event_log, "publish", publish_noting_the_stale_watch)
    if ending == "finish":
        await manager._finish(record, _round_one_verdict())
    elif ending == "fail":
        await manager._fail(record, "The AWS lane could not be verified")
    else:
        await manager.start_towel(record.snapshot.id, OWNER)
        await _until(lambda: stopped)
    assert stopped == [True]
    assert all(target.cancelled.is_set() for target in stale_targets)
    await manager.close()


async def test_a_round_one_redo_timing_out_pins_the_cooldown_fence_it_claimed() -> None:
    """The owner's re-do watches under the cooldown fence it claimed, and timing out pins that
    fence as cooldown_failed and stops renewing it, as the automatic watch does."""

    store = InMemoryBoutLeaseStore()
    manager = RunManager(lease_store=store)
    _short_leases(manager)
    manager._cooldown_timeout = 0.3
    manager._cooldown_poll_timeout = 0.05
    manager._arm_poll = 0.01
    manager._active_lease_ttl = 5.0
    created = await manager.create(_round_one())
    record = manager._records[created.id]
    record.operator = OWNER
    record.snapshot.state = SessionState.VERIFIED
    record.live_targets = (
        HangingArmTarget("lakebase", "Lakebase"),
        HangingArmTarget("competitor", "Aurora Serverless v2"),
    )
    await manager.start_cooldown(created.id, OWNER)
    redo = record.cooldown_task
    assert redo is not None and record.idle_watch_task is None
    cooling = record.lease
    assert cooling is not None and cooling.phase == "cooldown"
    await asyncio.wait_for(redo, timeout=5)
    pinned = record.lease
    assert pinned is not None and pinned.phase == "cooldown_failed"
    assert manager._same_lease_authority(pinned, cooling)
    assert record.lease_heartbeat_task is None
    row = await store.current()
    assert row is not None and manager._same_exact_lease(row, pinned)
    await manager.close()


class GatedPinStore(InMemoryBoutLeaseStore):
    """A coordinator holding a cooldown_failed transition until the test lets it through."""

    def __init__(self) -> None:
        super().__init__()
        self.pinning = asyncio.Event()
        self.let_through = asyncio.Event()

    async def transition(self, lease, *, phase, **kwargs):
        if phase == "cooldown_failed":
            self.pinning.set()
            await self.let_through.wait()
        return await super().transition(lease, phase=phase, **kwargs)


@pytest.mark.parametrize("dropped", [False, True], ids=["still held", "dropped meanwhile"])
async def test_an_idle_watch_pins_its_fence_off_the_lease_lock_once_its_heartbeat_stopped(
    dropped: bool,
) -> None:
    """The failure fence is one coordinator round trip, which a black-holed socket never
    answers. Like a renew it is sent off lease_lock, and only once the fence's heartbeat has
    stopped, so nothing renews the row past the fence while the pin waits. Its answer is
    installed only if the record still holds that exact lease: a dropped lease stays dropped."""

    store = GatedPinStore()
    manager, record, cooling, watch = await _watching_round_one(store)
    await asyncio.wait_for(store.pinning.wait(), timeout=5)
    assert not record.lease_lock.locked()
    assert record.lease_heartbeat_task is None
    if dropped:
        async with record.lease_lock:
            record.lease = None
    store.let_through.set()
    await asyncio.wait_for(watch, timeout=5)
    if dropped:
        assert record.lease is None
    else:
        pinned = record.lease
        assert pinned is not None and pinned.phase == "cooldown_failed"
        assert manager._same_lease_authority(pinned, cooling)
    await manager.close()
