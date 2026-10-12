"""A finished bout whose ring release cannot be confirmed at once still finishes.

Round 6's two finishes, ``_fail``, Round 1's automatic return to idle and Round 5's
failure after verified cleanup have nobody who retries their release afterwards. Each
retries the exact fence it finished under, still heartbeating, for a bounded time, and
then finishes either way: the result is published, a receipt sealed, the round's
residue settled, and no heartbeat outlives the bout. Past the bound the durable TTL is
what frees the ring.

The coordinator here goes dark at the first release of one lease phase and stays dark
until the test heals it, so the terminal release and its fallback read both fail.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from server.api import router
from server.coordination import InMemoryBoutLeaseStore
from server.manager import (
    _ARMED_CARD_CANCELLED,
    _BELL_ACCEPTANCE_UNCONFIRMED,
    InvalidStateError,
    RunManager,
    SessionRecord,
)
from server.models import (
    BoutOperator,
    CompetitorId,
    CooldownState,
    Corner,
    RoundId,
    SessionCreate,
    SessionSnapshot,
    SessionState,
)
from server.receipts import SEALING_EVENTS, load_receipts
from server.round6_race import (
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    Round6RaceEngine,
    Round6RaceError,
)
from server.targets import TargetConfigurationError
from tests.test_heartbeat_renew import JumpingClock
from tests.test_manager import (
    BackgroundCleanupConnectionSpikeEngine,
    FakeLiveTarget,
    FakeModelScoreEngine,
    FakeResolver,
    FakeSafeChangeEngine,
    HangingArmResolver,
    RetryTerminalReleaseStore,
    SequencedCooldownResolver,
    SuccessfulTwoPhaseConnectionSpikeEngine,
    TransitionRaceRecoveryEngine,
    drain_record_operations,
    make_verifier,
    round_four_request,
    sealed_receipt,
)
from tests.test_round6_race import BASELINE, FakeLane

OWNER = BoutOperator(display_name="Owner", subject="owner-subject")
OTHER = BoutOperator(display_name="Other Presenter", subject="other-subject")
ARM_REFUSED = "A backstage check failed; no run started. The operator log has the diagnosis."


class OutageAtTerminalRelease(InMemoryBoutLeaseStore):
    """A coordinator that goes dark at the first release of ``trip_phase``, until healed.

    With ``answer_lost`` that first release does clear the row; only its answer is lost.
    """

    def __init__(self, *, trip_phase: str, answer_lost: bool = False) -> None:
        super().__init__()
        self.trip_phase = trip_phase
        self.answer_lost = answer_lost
        self.outage = False
        self.healed = False
        self.tripped = asyncio.Event()
        # A second release in the dark: the funnel retried instead of giving up.
        self.retried = asyncio.Event()
        self.release_attempts = 0
        self.renewals_after_heal = 0
        self.released_phases: list[str] = []

    def heal(self) -> None:
        self.outage = False
        self.healed = True

    async def release(self, lease):
        if not self.tripped.is_set() and lease.phase == self.trip_phase:
            if self.answer_lost:
                await super().release(lease)
            self.outage = True
            self.tripped.set()
        if self.outage:
            self.release_attempts += 1
            if self.release_attempts > 1:
                self.retried.set()
            raise ConnectionError("coordination endpoint unreachable")
        released = await super().release(lease)
        if released:
            self.released_phases.append(lease.phase)
        return released

    async def current(self):
        if self.outage:
            raise ConnectionError("coordination endpoint unreachable")
        return await super().current()

    async def renew(self, lease, *, ttl):
        if self.outage:
            raise ConnectionError("coordination endpoint unreachable")
        renewed = await super().renew(lease, ttl=ttl)
        if self.healed:
            self.renewals_after_heal += 1
        return renewed


def _bounded(manager: RunManager, *, deadline: float) -> None:
    """Short leases and the terminal settle deadline under test.

    Every outage below ends, healed or past the deadline, well inside the TTL, so the
    lease is still the bout's own for as long as its release is retried.
    """

    manager._lease_heartbeat = 0.01
    manager._active_lease_ttl = 1.0
    manager._running_lease_ttl = 1.0
    manager._terminal_settle_deadline = deadline
    manager._terminal_release_backoff_cap = 0.02


HEALS = pytest.mark.parametrize("heals", [True, False], ids=["released", "unprovable"])


def _deadline(heals: bool) -> float:
    return 10.0 if heals else 0.2


async def _until(predicate):
    for _ in range(1000):
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.005)
    raise AssertionError("condition never held")


async def _reach(manager: RunManager, session_id: str, state: SessionState):
    async def reached():
        snapshot = await manager.get(session_id)
        return snapshot if snapshot.state == state else None

    return await _until(reached)


async def _never_renewed_after(store: OutageAtTerminalRelease) -> None:
    """Heal the coordinator and show the finished bout's lease is never renewed again.

    The bound passed before any release was confirmed, so only the non-renewing
    background retry, or else the lease's own TTL, frees the ring.
    """

    store.heal()
    await _ring_freed(store)
    assert store.renewals_after_heal == 0


async def _ring_freed(store: OutageAtTerminalRelease) -> None:
    async def free() -> bool:
        return await store.current() is None

    await _until(free)


def _events(record: SessionRecord) -> list[str]:
    return [event.event for event in record.event_log.events]


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


def _round_three() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="data_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.RECOVER_DELETED_ORDER,
    )


def _round_five() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="sre",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
    )


def _round_six() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="data_analyst",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.ANALYZE_LIVE_ORDERS,
    )


def _round_six_lanes() -> tuple[FakeLane, FakeLane]:
    return (
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=3),
    )


def _round_six_engine(
    lanes: tuple[FakeLane, ...],
    engine_type: type[Round6RaceEngine] = Round6RaceEngine,
) -> Round6RaceEngine:
    return engine_type(
        lanes,
        baseline=BASELINE,
        poll_seconds=0.001,
        watch_seconds=0.001,
        lane_timeout_seconds=5.0,
    )


class FailsAfterItsOrdersCommit(Round6RaceEngine):
    """Rings and measures, then fails, so the failure funnel owes real rows."""

    async def run(self, arm, order, guardrail_order, on_progress):
        await super().run(arm, order, guardrail_order, on_progress)
        raise Round6RaceError("The AWS lane's verifier lost its connection")


class RefusingPrepareEngine(Round6RaceEngine):
    async def prepare(self, notify):
        raise RuntimeError("AWS DMS DescribeReplicationTasks was throttled")


class RefusingArmSafeChangeEngine(FakeSafeChangeEngine):
    async def arm(self, competitor, on_progress):
        raise RuntimeError("the Aurora clone API was throttled")


class MisconfiguredTarget(FakeLiveTarget):
    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.arm_calls += 1
        raise TargetConfigurationError("sealed endpoint does not match the manifest")


class MisconfiguredResolver:
    def resolve(self, competitor: CompetitorId):
        return (
            MisconfiguredTarget("lakebase", "Lakebase", 0.001),
            FakeLiveTarget("competitor", "Aurora Serverless v2", 0.001),
        )


def _assert_settled(engine: Round6RaceEngine, lanes: tuple[FakeLane, ...]) -> None:
    assert engine._owned == {}
    for lane in lanes:
        assert {"delete:checkout", "delete:checkout-guardrail"} <= set(lane.events)
        assert set(lane.source) == {BASELINE.order_id}
        assert lane.events[-1] == "park"


async def test_round_six_withholds_its_result_until_a_retried_release_confirms_it() -> None:
    store = OutageAtTerminalRelease(trip_phase="run_committed")
    lanes = _round_six_lanes()
    engine = _round_six_engine(lanes)
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(True))
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(store.retried.wait(), timeout=5)
    record = manager._records[created.id]

    # Pending: nothing terminal yet, the ring held and heartbeating, other Prepares wait.
    pending = await manager.get(created.id)
    assert pending.state == SessionState.RUNNING
    assert not set(_events(record)) & SEALING_EVENTS
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()
    challenger = await manager.create(_round_six())
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager.start_arm(challenger.id, OTHER)

    store.heal()
    finished = await _reach(manager, created.id, SessionState.VERIFIED)
    assert finished.failure is None
    assert finished.comparison is not None
    assert finished.comparison.winner_lane_id == LAKEBASE_LANE
    assert finished.remembered_result is not None
    assert finished.remembered_result.startswith("LAKEBASE WINS · MARGIN ")
    assert record.live_orders_result is not None
    assert _events(record).count("run_finished") == 1
    assert "session_failed" not in _events(record)
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.outcome == "declared"
    assert receipt.remembered_result == finished.remembered_result

    await drain_record_operations(manager, record)
    _assert_settled(engine, lanes)
    assert record.lease is None
    assert record.lease_heartbeat_task is None
    assert await store.current() is None
    assert (await manager.start_arm(challenger.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


async def test_round_six_publishes_and_settles_a_measured_result_whose_release_stays_unprovable(
) -> None:
    store = OutageAtTerminalRelease(trip_phase="run_committed")
    lanes = _round_six_lanes()
    engine = _round_six_engine(lanes)
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(False))
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    finished = await _reach(manager, created.id, SessionState.VERIFIED)
    record = manager._records[created.id]

    assert store.outage
    assert finished.comparison is not None
    assert finished.comparison.winner_lane_id == LAKEBASE_LANE
    assert finished.remembered_result is not None
    assert record.live_orders_result is not None
    assert _events(record).count("run_finished") == 1
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.outcome == "declared"
    await drain_record_operations(manager, record)
    _assert_settled(engine, lanes)
    assert record.lease_heartbeat_task is None

    await _never_renewed_after(store)
    assert (await manager.get(created.id)).state == SessionState.VERIFIED
    challenger = await manager.create(_round_six())
    assert (await manager.start_arm(challenger.id, OTHER)).state == SessionState.CHECKING
    await manager.close()


@pytest.mark.parametrize("first_outcome", ["false", "exception"])
async def test_round_six_retries_its_fence_while_the_coordinator_still_names_the_bout(
    first_outcome: str,
) -> None:
    store = RetryTerminalReleaseStore(phase="run_committed", first_outcome=first_outcome)
    lanes = _round_six_lanes()
    engine = _round_six_engine(lanes)
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(True))
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(store.retry_started.wait(), timeout=5)
    record = manager._records[created.id]
    assert record.snapshot.state == SessionState.RUNNING
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()
    assert "run_finished" not in _events(record)

    store.allow_success.set()
    finished = await _reach(manager, created.id, SessionState.VERIFIED)
    assert finished.remembered_result is not None
    assert finished.remembered_result.startswith("LAKEBASE WINS · MARGIN ")
    assert store.release_calls == 2
    assert _events(record).count("run_finished") == 1
    await drain_record_operations(manager, record)
    _assert_settled(engine, lanes)
    assert record.lease_heartbeat_task is None
    assert await store.current() is None
    await manager.close()


async def test_round_six_keeps_its_result_when_its_release_cleared_the_row_but_lost_the_answer(
) -> None:
    store = OutageAtTerminalRelease(trip_phase="run_committed", answer_lost=True)
    lanes = _round_six_lanes()
    engine = _round_six_engine(lanes)
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(True))
    # Long gaps between retries, so the heartbeat meets the cleared row first.
    manager._terminal_release_backoff_cap = 0.5
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)

    async def retrying() -> bool:
        return store.release_attempts >= 4

    await _until(retrying)
    store.heal()
    finished = await _reach(manager, created.id, SessionState.VERIFIED)
    record = manager._records[created.id]
    assert finished.comparison is not None
    assert finished.comparison.winner_lane_id == LAKEBASE_LANE
    assert _events(record).count("run_finished") == 1
    assert "session_failed" not in _events(record)
    await drain_record_operations(manager, record)
    _assert_settled(engine, lanes)
    assert record.lease is None
    assert record.lease_heartbeat_task is None
    assert await store.current() is None
    await manager.close()


@HEALS
async def test_round_six_failure_is_published_and_settled_after_an_unconfirmed_release(
    heals: bool,
) -> None:
    store = OutageAtTerminalRelease(trip_phase="run_committed")
    lanes = _round_six_lanes()
    engine = _round_six_engine(lanes, FailsAfterItsOrdersCommit)
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(heals))
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    record = manager._records[created.id]
    if heals:
        await asyncio.wait_for(store.retried.wait(), timeout=5)
        assert (await manager.get(created.id)).state == SessionState.RUNNING
        assert "session_failed" not in _events(record)
        store.heal()

    failed = await _reach(manager, created.id, SessionState.FAILED)
    assert failed.failure == "The AWS lane's verifier lost its connection"
    assert _events(record).count("session_failed") == 1
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.outcome == "stopped_short"
    await drain_record_operations(manager, record)
    _assert_settled(engine, lanes)
    assert record.lease_heartbeat_task is None
    if heals:
        assert await store.current() is None
    else:
        await _never_renewed_after(store)
    await manager.close()


@dataclasses.dataclass
class HeldParkLane(FakeLane):
    """A Round 6 lane whose park, once held, waits until the test lets it finish."""

    hold_park: bool = False
    parking: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    unhold: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)

    async def park(self, notify):
        if self.hold_park:
            self.parking.set()
            await self.unhold.wait()
        await super().park(notify)


async def test_rearming_while_an_ordinary_failure_settles_does_not_report_a_lost_ring() -> None:
    """Round 6 failed after its orders committed, and its settlement is still parking.

    The owner's re-arm waits for that settlement, but nothing lost the ring, so the
    refusal says the last bout is settling rather than telling the room to restart.
    """

    lanes = (
        HeldParkLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        HeldParkLane(COMPETITOR_LANE, "AWS", arrives_on_read=3),
    )
    engines = [_round_six_engine(lanes, FailsAfterItsOrdersCommit), _round_six_engine(lanes)]
    manager = RunManager(
        live_orders_factory=lambda _competitor: engines.pop(0),
        lease_store=InMemoryBoutLeaseStore(),
    )
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    for lane in lanes:
        lane.hold_park = True
    await manager.start_run(created.id, OWNER)
    failed = await _reach(manager, created.id, SessionState.FAILED)
    assert failed.failure == "The AWS lane's verifier lost its connection"
    await asyncio.wait_for(lanes[0].parking.wait(), timeout=5)
    record = manager._records[created.id]
    settlement = record.settlement_task
    assert settlement is not None and not settlement.done()
    assert record.ring_lease_lost is False

    with pytest.raises(InvalidStateError) as refused:
        await manager.start_arm(created.id, OWNER)
    assert str(refused.value) == "THIS CARD'S LAST BOUT IS STILL SETTLING · TRY AGAIN IN A MOMENT"

    for lane in lanes:
        lane.hold_park = False
        lane.unhold.set()
    await asyncio.wait_for(settlement, timeout=5)
    assert (await manager.start_arm(created.id, OWNER)).state == SessionState.CHECKING
    await manager.close()


@pytest.mark.parametrize(
    ("request_", "factory", "summary"),
    [
        (
            _round_two(),
            {"safe_change_factory": RefusingArmSafeChangeEngine},
            "The isolated-change start state could not be verified.",
        ),
        (
            _round_six(),
            {
                "live_orders_factory": lambda _competitor: _round_six_engine(
                    (FakeLane(LAKEBASE_LANE, "Lakebase"), FakeLane(COMPETITOR_LANE, "AWS")),
                    RefusingPrepareEngine,
                )
            },
            "Round 6 could not confirm its pipelines at rest and its sources at baseline.",
        ),
    ],
    ids=["round-2", "round-6"],
)
@HEALS
async def test_a_checking_card_whose_failure_cannot_confirm_its_release_still_fails_and_frees(
    request_: SessionCreate,
    factory: dict[str, object],
    summary: str,
    heals: bool,
) -> None:
    store = OutageAtTerminalRelease(trip_phase="checking")
    manager = RunManager(lease_store=store, **factory)
    _bounded(manager, deadline=_deadline(heals))
    created = await manager.create(request_)
    await manager.start_arm(created.id, OWNER)
    record = manager._records[created.id]
    if heals:
        await asyncio.wait_for(store.retried.wait(), timeout=5)
        assert (await manager.get(created.id)).state == SessionState.CHECKING
        assert record.lease_heartbeat_task is not None
        assert not record.lease_heartbeat_task.done()
        # Designed: a card outside Round 1 that is still checking offers no control.
        with pytest.raises(InvalidStateError, match="Only a Round 1 start-state check"):
            await manager.cancel_arm(created.id, OWNER)
        with pytest.raises(InvalidStateError, match="must be running before throwing"):
            await manager.start_towel(created.id, OWNER)
        store.heal()

    failed = await _reach(manager, created.id, SessionState.FAILED)
    assert failed.failure == f"{summary} {ARM_REFUSED}"
    assert _events(record).count("session_failed") == 1
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "session_failed"
    assert receipt.outcome == "stopped_short"
    assert receipt.failure == failed.failure
    assert record.lease_heartbeat_task is None
    if heals:
        assert await store.current() is None
    else:
        await _never_renewed_after(store)
    # Freed without close(): the owner prepares the card again.
    assert (await manager.start_arm(created.id, OWNER)).state == SessionState.CHECKING
    await manager.close()


@HEALS
async def test_round_one_owner_cancel_of_a_check_settles_its_release_and_finishes(
    heals: bool,
) -> None:
    """The owner's cancel lands while the check's own failure is retrying its release.

    The cancel stops that retry, settles the release itself, bounded, and finishes as
    a cancellation either way: never as a failure too, never with a heartbeat left.
    """

    store = OutageAtTerminalRelease(trip_phase="checking")
    manager = RunManager(
        resolver=MisconfiguredResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    # The check's own settle outlives the test, so the cancel always lands on it;
    # only the cancel's settle gets the deadline under test.
    _bounded(manager, deadline=10.0)
    manager._arm_poll = 0.001
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await asyncio.wait_for(store.retried.wait(), timeout=5)
    record = manager._records[created.id]
    assert (await manager.get(created.id)).state == SessionState.CHECKING
    manager._terminal_settle_deadline = _deadline(heals)

    check = record.task
    assert check is not None
    cancelling = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    if heals:

        async def check_stopped() -> bool:
            return check.done()

        await _until(check_stopped)
        before = store.release_attempts

        async def cancel_retrying() -> bool:
            return store.release_attempts >= before + 2

        await _until(cancel_retrying)
        assert not cancelling.done()
        assert (await manager.get(created.id)).state == SessionState.CHECKING
        assert not set(_events(record)) & SEALING_EVENTS
        # The cancel's settle renews the fence it retries, and only while it retries.
        heartbeat = record.lease_heartbeat_task
        assert heartbeat is not None and not heartbeat.done()
        store.heal()

    cancelled = await asyncio.wait_for(cancelling, timeout=5)
    assert cancelled.state == SessionState.FAILED
    assert cancelled.failure is not None
    assert cancelled.failure.startswith("Fight-card check cancelled")
    assert _events(record).count("session_cancelled") == 1
    assert "session_failed" not in _events(record)
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "session_cancelled"
    assert receipt.outcome == "stopped_short"
    assert receipt.failure == cancelled.failure
    assert record.lease_heartbeat_task is None
    if heals:
        assert await store.current() is None
    else:
        await _never_renewed_after(store)
    await drain_record_operations(manager, record)
    assert record.terminal_release_retry_task is None
    assert record.lease is None
    # A second press returns the cancelled card.
    again = await manager.cancel_arm(created.id, OWNER)
    assert again.failure == cancelled.failure
    assert _events(record).count("session_cancelled") == 1
    await manager.close()


async def test_a_cancelled_check_cancel_request_cannot_orphan_finalization() -> None:
    store = OutageAtTerminalRelease(trip_phase="checking")
    resolver = HangingArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier(), lease_store=store)
    _bounded(manager, deadline=10.0)
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await asyncio.wait_for(resolver.lakebase.entered.wait(), timeout=5)
    record = manager._records[created.id]

    request = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.wait_for(store.retried.wait(), timeout=5)
    assert len(record.checking_cancel_tasks) == 1
    finalizer = next(iter(record.checking_cancel_tasks.values()))
    assert finalizer is not None and finalizer is not request and not finalizer.done()

    repeated = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.sleep(0)
    assert not repeated.done()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert (await _read_promptly(manager, created.id)).state == SessionState.CHECKING
    assert not set(_events(record)) & SEALING_EVENTS

    store.heal()
    await asyncio.wait_for(finalizer, timeout=5)
    repeated_result = await asyncio.wait_for(repeated, timeout=5)
    cancelled = await manager.get(created.id)
    assert repeated_result.state == cancelled.state
    assert repeated_result.failure == cancelled.failure
    assert cancelled.state == SessionState.FAILED
    assert cancelled.failure is not None
    assert cancelled.failure.startswith("Fight-card check cancelled")
    assert _events(record).count("session_cancelled") == 1
    assert "session_failed" not in _events(record)
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "session_cancelled"
    receipts = [item for item in load_receipts() if item.session_id == created.id]
    assert [item.sealing_event for item in receipts] == ["session_cancelled"]
    assert record.checking_cancel_tasks == {}
    assert record.task is None
    assert record.lease_heartbeat_task is None
    assert record.terminal_release_retry_task is None
    assert record.lease is None

    assert (await manager.start_arm(created.id, OWNER)).state == SessionState.CHECKING
    await manager.close()


async def test_close_tracks_and_drains_a_checking_cancel_finalizer() -> None:
    store = OutageAtTerminalRelease(trip_phase="checking")
    resolver = HangingArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier(), lease_store=store)
    _bounded(manager, deadline=10.0)
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await asyncio.wait_for(resolver.lakebase.entered.wait(), timeout=5)
    record = manager._records[created.id]

    request = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.wait_for(store.retried.wait(), timeout=5)
    assert len(record.checking_cancel_tasks) == 1
    finalizer = next(iter(record.checking_cancel_tasks.values()))
    assert finalizer is not None and not finalizer.done()

    store.heal()
    await asyncio.wait_for(manager.close(), timeout=5)
    await asyncio.gather(request, return_exceptions=True)
    assert finalizer.done()
    assert record.checking_cancel_tasks == {}
    assert record.lease_heartbeat_task is None
    assert record.terminal_release_retry_task is None


async def test_close_gives_checking_cancel_time_to_seal_its_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = HangingArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    manager._shutdown_cleanup_timeout = 5
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await asyncio.wait_for(resolver.lakebase.entered.wait(), timeout=5)
    record = manager._records[created.id]
    original_publish = record.event_log.publish
    publishing_cancel = asyncio.Event()
    allow_publish = asyncio.Event()

    async def blocked_publish(event, payload, **kwargs):
        if event == "session_cancelled":
            publishing_cancel.set()
            await allow_publish.wait()
        return await original_publish(event, payload, **kwargs)

    monkeypatch.setattr(record.event_log, "publish", blocked_publish)
    cancelling = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.wait_for(publishing_cancel.wait(), timeout=5)

    closing = asyncio.create_task(manager.close())
    await asyncio.sleep(0)
    assert not closing.done()
    allow_publish.set()
    await asyncio.wait_for(closing, timeout=5)
    cancelled = await asyncio.wait_for(cancelling, timeout=5)
    assert cancelled.state == SessionState.FAILED
    assert _events(record).count("session_cancelled") == 1
    assert record.checking_cancel_tasks == {}


async def test_close_drains_an_armed_cancel_finalizer_before_coordinator_shutdown() -> None:
    store = OutageAtTerminalRelease(trip_phase="armed")
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    _bounded(manager, deadline=10.0)
    manager._shutdown_cleanup_timeout = 5
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]

    request = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.wait_for(store.retried.wait(), timeout=5)
    finalizer = record.armed_expiry_task
    assert record.armed_release_pending
    assert finalizer is not None and not finalizer.done()

    closing = asyncio.create_task(manager.close())
    await asyncio.sleep(0)
    assert not closing.done()
    assert not finalizer.cancelled()

    store.heal()
    await asyncio.wait_for(closing, timeout=5)
    await asyncio.gather(request, return_exceptions=True)
    assert finalizer.done()
    assert record.armed_release_pending is False
    assert record.armed_expiry_task is None
    assert record.lease is None
    assert _events(record).count("session_cancelled") == 1


@HEALS
async def test_round_one_return_to_idle_never_leaves_an_immortal_cooldown_lease(
    heals: bool,
) -> None:
    store = OutageAtTerminalRelease(trip_phase="cooldown")
    manager = RunManager(
        resolver=SequencedCooldownResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    _bounded(manager, deadline=_deadline(heals))
    manager._arm_poll = 0.001
    manager._cooldown_timeout = 0.5
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await _reach(manager, created.id, SessionState.VERIFIED)
    record = manager._records[created.id]

    if heals:
        await asyncio.wait_for(store.retried.wait(), timeout=5)
        assert record.lease_heartbeat_task is not None
        assert not record.lease_heartbeat_task.done()
        store.heal()
        await _ring_freed(store)
        await drain_record_operations(manager, record)
        idle = await manager.get(created.id)
        assert idle.cooldown is not None
        assert idle.cooldown.state == CooldownState.READY
        assert record.lease is None
        assert record.lease_heartbeat_task is None
        retried = await manager.start_cooldown(created.id, OWNER)
        assert retried.cooldown is not None
        assert retried.cooldown.state == CooldownState.READY
        await manager.close()
        return

    async def cooldown_failed():
        snapshot = await manager.get(created.id)
        cooldown = snapshot.cooldown
        return cooldown if cooldown is not None and cooldown.state == CooldownState.FAILED else None

    failed = await _until(cooldown_failed)
    assert failed.failure == "Return to idle was verified, but ring release could not be confirmed."
    await drain_record_operations(manager, record)
    assert record.lease_heartbeat_task is None
    await _never_renewed_after(store)
    # The owner's retry is no longer refused by the bout's own lease.
    again = await manager.start_cooldown(created.id, OWNER)
    assert again.cooldown is not None
    assert again.cooldown.state != CooldownState.FAILED
    await drain_record_operations(manager, record)
    redone = await manager.get(created.id)
    assert redone.cooldown is not None
    assert redone.cooldown.state == CooldownState.READY
    assert await store.current() is None
    await manager.close()


class RefusingCheckConnectionSpikeEngine:
    """A Round 5 engine whose start-state check fails, and whose cleanup verifies."""

    def __init__(self) -> None:
        self.settled: list[str] = []

    async def check(self):
        raise RuntimeError("RDS Proxy DescribeDBProxies was throttled")

    async def cancel_and_cleanup(self, _arm) -> None:
        return None

    async def cancel_setup_and_settle(self, bout_id) -> None:
        self.settled.append(bout_id)


class NoBellRoundFiveEngine(SuccessfulTwoPhaseConnectionSpikeEngine):
    def __init__(self) -> None:
        super().__init__(burst_valid=True)
        self.run_started = asyncio.Event()

    async def run(self, arm, on_progress):
        self.run_started.set()
        return await super().run(arm, on_progress)


@HEALS
async def test_round_five_failure_after_verified_cleanup_frees_its_main_ring(heals: bool) -> None:
    """A refused Round 5 arm whose cleanup verified fails through ``_fail``."""

    store = OutageAtTerminalRelease(trip_phase="checking")
    engine = RefusingCheckConnectionSpikeEngine()
    manager = RunManager(connection_spike_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(heals))
    created = await manager.create(_round_five())
    await manager.start_arm(created.id, OWNER)
    record = manager._records[created.id]
    if heals:
        await asyncio.wait_for(store.retried.wait(), timeout=5)
        assert engine.settled == [created.id]
        # The failure is withheld while its main ring is retried, still heartbeating.
        assert (await manager.get(created.id)).state == SessionState.CHECKING
        assert not set(_events(record)) & SEALING_EVENTS
        assert record.lease_heartbeat_task is not None
        assert not record.lease_heartbeat_task.done()
        store.heal()

    failed = await _reach(manager, created.id, SessionState.FAILED)
    if not heals:
        # Failed without a confirmed release, so the release is still owed.
        assert record.terminal_release_retry_task is not None
    assert failed.failure is not None
    assert failed.failure.startswith("The Round 5 start state could not be verified.")
    assert _events(record).count("session_failed") == 1
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "session_failed"
    assert receipt.outcome == "stopped_short"
    assert receipt.failure == failed.failure
    await drain_record_operations(manager, record)
    assert record.lease_heartbeat_task is None
    assert record.round5_lease is None
    if heals:
        assert await store.current() is None
    else:
        await _never_renewed_after(store)
    await drain_record_operations(manager, record)
    assert record.terminal_release_retry_task is None
    await manager.close()


async def _holding(manager: RunManager, store: InMemoryBoutLeaseStore):
    """A Round 1 bout holding a heartbeating ``run_committed`` fence."""

    created = await manager.create(_round_one())
    record = manager._records[created.id]
    record.operator = OWNER
    ttl = timedelta(seconds=manager._running_lease_ttl)
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
    return record, committed, ttl


class StalledCoordinator(InMemoryBoutLeaseStore):
    """Calls that do not answer until ``answer()``; a refused release answers False."""

    def __init__(
        self,
        *,
        stall_release: bool = True,
        stall_current: bool = False,
        refuse_release: bool = False,
    ) -> None:
        super().__init__()
        self.stall_release = stall_release
        self.stall_current = stall_current
        self.refuse_release = refuse_release
        self.answered = asyncio.Event()
        self.release_started = asyncio.Event()
        self.release_calls = 0
        self.current_calls = 0
        self.counting_renewals = False
        self.renewals = 0

    def answer(self) -> None:
        self.stall_release = False
        self.stall_current = False
        self.refuse_release = False
        self.answered.set()

    async def release(self, lease):
        self.release_calls += 1
        if self.stall_release:
            self.release_started.set()
            await self.answered.wait()
        if self.refuse_release:
            return False
        return await super().release(lease)

    async def current(self):
        self.current_calls += 1
        if self.stall_current:
            await self.answered.wait()
        return await super().current()

    async def renew(self, lease, *, ttl):
        renewed = await super().renew(lease, ttl=ttl)
        if self.counting_renewals:
            self.renewals += 1
        return renewed


def _stalled(store: StalledCoordinator) -> RunManager:
    manager = RunManager(lease_store=store)
    _bounded(manager, deadline=10.0)
    manager._terminal_release_call_timeout = 5.0
    return manager


@pytest.mark.parametrize(
    ("stall_release", "refuse_release"),
    [(True, False), (False, True)],
    ids=["release-stalls", "release-refused-current-stalls"],
)
async def test_the_terminal_settle_deadline_bounds_calls_that_never_answer(
    stall_release: bool, refuse_release: bool
) -> None:
    """The deadline is absolute: no call outlives it, and none starts after it."""

    store = StalledCoordinator(
        stall_release=stall_release, stall_current=True, refuse_release=refuse_release
    )
    manager = _stalled(store)
    manager._terminal_settle_deadline = 0.05
    record, committed, _ = await _holding(manager, store)

    started = time.monotonic()
    outcome = await asyncio.wait_for(
        manager._settle_terminal_release(record, committed), timeout=2
    )
    elapsed = time.monotonic() - started

    assert outcome == "unknown"
    assert elapsed < 0.2
    if stall_release:
        # The release spent the whole deadline, so the fallback read never began.
        assert store.release_calls == 1
        assert store.current_calls == 0
    assert record.lease_heartbeat_task is None
    store.answer()
    await manager.close()


class DeafRelease(InMemoryBoutLeaseStore):
    """A release that outlasts every cancellation until its socket closes.

    psycopg meets a cancellation by trying a server-side cancel and then waiting on
    the same socket again, which a black-holed socket never answers either.
    """

    def __init__(self) -> None:
        super().__init__()
        self.socket_closed = asyncio.Event()

    async def release(self, lease):
        while not self.socket_closed.is_set():
            try:
                await self.socket_closed.wait()
            except asyncio.CancelledError:
                continue
        raise ConnectionError("server closed the connection unexpectedly")


async def test_the_terminal_settle_deadline_holds_against_a_release_that_ignores_cancellation(
) -> None:
    store = DeafRelease()
    manager = RunManager(lease_store=store)
    _bounded(manager, deadline=0.05)
    manager._terminal_release_call_timeout = 5.0
    record, committed, _ = await _holding(manager, store)

    started = time.monotonic()
    settle = asyncio.create_task(manager._settle_terminal_release(record, committed))
    try:
        done, _ = await asyncio.wait({settle}, timeout=1.0)
        assert settle in done, "the settle is still waiting on a call that will not end"
        assert time.monotonic() - started < 0.5
        assert settle.result() == "unknown"
        assert record.lease_heartbeat_task is None
    finally:
        store.socket_closed.set()
        await asyncio.gather(settle, return_exceptions=True)
    await manager.close()


async def test_a_cancelled_terminal_settle_stops_its_heartbeat_and_lets_the_ttl_expire() -> None:
    store = StalledCoordinator()
    manager = _stalled(store)
    record, committed, _ = await _holding(manager, store)
    heartbeat = record.lease_heartbeat_task

    settling = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    settling.cancel()
    with pytest.raises(asyncio.CancelledError):
        await settling

    assert record.lease_heartbeat_task is None
    assert heartbeat is not None
    await asyncio.sleep(0)
    assert heartbeat.done()
    assert not manager._terminal_release_owns(record, committed)

    store.answer()
    store.counting_renewals = True
    held = await store.current()
    assert held is not None and held.lease_id == committed.lease_id
    await asyncio.sleep(0.05)
    assert store.renewals == 0
    await _ring_freed(store)
    await manager.close()


async def test_concurrent_terminal_settles_keep_the_guard_until_the_last_one_exits() -> None:
    store = StalledCoordinator()
    manager = _stalled(store)
    record, committed, _ = await _holding(manager, store)

    first = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    # An equal copy, not the same object: ownership is by the exact fence.
    second = asyncio.create_task(
        manager._settle_terminal_release(record, dataclasses.replace(committed))
    )
    for _ in range(3):
        await asyncio.sleep(0)
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second

    assert manager._terminal_release_owns(record, committed)
    assert not manager._terminal_release_owns(
        record, dataclasses.replace(committed, phase="towel_cleanup")
    )
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()

    store.answer()
    assert await asyncio.wait_for(first, timeout=5) == "released"
    assert not manager._terminal_release_owns(record, committed)
    assert record.lease_heartbeat_task is None
    assert await store.current() is None
    await manager.close()


async def test_a_settle_after_a_cancelled_one_renews_the_fence_it_still_retries() -> None:
    store = StalledCoordinator(stall_release=False, refuse_release=True)
    manager = _stalled(store)
    record, committed, _ = await _holding(manager, store)

    first = asyncio.create_task(manager._settle_terminal_release(record, committed))

    async def retried() -> bool:
        return store.release_calls >= 2

    await _until(retried)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert record.lease_heartbeat_task is None

    successor = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.sleep(0)
    heartbeat = record.lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    assert record.lease_heartbeat_lease is not None
    assert manager._same_exact_lease(record.lease_heartbeat_lease, committed)
    store.counting_renewals = True

    async def renewed() -> bool:
        return store.renewals >= 2

    await _until(renewed)
    store.answer()
    assert await asyncio.wait_for(successor, timeout=5) == "released"
    assert record.lease_heartbeat_task is None
    assert heartbeat.done()
    await manager.close()


async def test_a_settle_never_replaces_a_live_heartbeat() -> None:
    store = StalledCoordinator()
    manager = _stalled(store)
    record, committed, _ = await _holding(manager, store)
    heartbeat = record.lease_heartbeat_task

    settling = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    assert record.lease_heartbeat_task is heartbeat
    store.answer()
    assert await asyncio.wait_for(settling, timeout=5) == "released"
    await manager.close()


async def test_a_settle_exiting_removes_only_its_own_registration() -> None:
    """Two settles of one fence register equal entries; each exit removes its own."""

    store = StalledCoordinator()
    manager = _stalled(store)
    record, committed, _ = await _holding(manager, store)

    first = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    second = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.sleep(0)
    assert len(record.terminal_release_settles) == 2
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert len(record.terminal_release_settles) == 1
    assert manager._terminal_release_owns(record, committed)
    assert record.lease_heartbeat_task is not None

    store.answer()
    assert await asyncio.wait_for(second, timeout=5) == "released"
    assert record.terminal_release_settles == []
    assert record.lease_heartbeat_task is None
    await manager.close()


async def test_two_settles_that_finish_on_their_own_leave_nothing_behind() -> None:
    """One settle confirms the release while its sibling of the same fence is in flight.

    The first returns "released" while the sibling is still registered, and the
    sibling then finds the fence gone and returns "lost". No cancellation is
    involved: whichever exits last owns the heartbeat's end.
    """

    store = StalledCoordinator()
    manager = _stalled(store)
    record, committed, _ = await _holding(manager, store)

    first = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    second = asyncio.create_task(manager._settle_terminal_release(record, committed))

    async def both_releasing() -> bool:
        return store.release_calls >= 2

    await _until(both_releasing)
    store.answer()
    outcomes = await asyncio.wait_for(asyncio.gather(first, second), timeout=5)

    assert outcomes == ["released", "lost"]
    assert await store.current() is None
    assert record.lease is None
    assert record.lease_heartbeat_task is None
    assert record.lease_heartbeat_lease is None
    assert record.terminal_release_settles == []
    assert record.terminal_release_retry_task is None
    assert manager._unfinished_operations(record) == ()
    assert manager._releasable(record)
    await manager.close()


@pytest.mark.parametrize("ending", ["heals", "closes"])
async def test_an_unknown_settle_leaves_a_retry_even_if_its_sibling_is_then_cancelled(
    ending: str,
) -> None:
    store = StalledCoordinator(stall_release=False, refuse_release=True)
    manager = _stalled(store)
    record, committed, _ = await _holding(manager, store)

    manager._terminal_settle_deadline = 0.1
    first = asyncio.create_task(manager._settle_terminal_release(record, committed))

    async def refused() -> bool:
        return store.release_calls >= 1

    await _until(refused)
    manager._terminal_settle_deadline = 10.0
    second = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.sleep(0)
    assert len(record.terminal_release_settles) == 2

    assert await asyncio.wait_for(first, timeout=5) == "unknown"
    retry = record.terminal_release_retry_task
    assert retry is not None and not retry.done()
    # The sibling still settles under the heartbeat.
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()

    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    assert record.lease_heartbeat_task is None
    assert record.terminal_release_settles == []
    assert record.terminal_release_retry_task is retry
    assert not retry.done()
    store.counting_renewals = True

    if ending == "heals":
        store.answer()
        await asyncio.wait_for(retry, timeout=5)
        assert await store.current() is None
        assert record.lease is None
        assert manager._releasable(record)
    else:
        await asyncio.wait_for(manager.close(), timeout=5)
        assert retry.done()
    assert store.renewals == 0
    assert record.terminal_release_retry_task is None
    assert record.lease_heartbeat_task is None
    await manager.close()


class HangingReleaseOf(InMemoryBoutLeaseStore):
    """Releases of ``phase`` (any, if None) do not answer until ``answer()``."""

    def __init__(self, *, phase: str | None, ring_key: str = "main") -> None:
        super().__init__(ring_key=ring_key)
        self.phase = phase
        self.answered = asyncio.Event()
        self.release_started = asyncio.Event()
        self.release_calls = 0
        self.released_phases: list[str] = []

    def answer(self) -> None:
        self.answered.set()

    async def release(self, lease):
        self.release_calls += 1
        if self.phase is None or lease.phase == self.phase:
            self.release_started.set()
            await self.answered.wait()
        released = await super().release(lease)
        if released:
            self.released_phases.append(lease.phase)
        return released


async def test_the_public_towel_is_accepted_while_a_measured_bouts_release_hangs() -> None:
    """Round 6 has measured and is settling its ring when the operator throws the towel."""

    store = HangingReleaseOf(phase="run_committed")
    lanes = _round_six_lanes()
    engine = _round_six_engine(lanes)
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    manager._lease_heartbeat = 0.01
    manager._terminal_settle_deadline = 10.0
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    record = manager._records[created.id]
    assert (await manager.get(created.id)).state == SessionState.RUNNING
    committed = await store.current()
    assert committed is not None and committed.phase == "run_committed"

    started = time.monotonic()
    towelled = await asyncio.wait_for(manager.start_towel(created.id, OWNER), timeout=2)
    assert time.monotonic() - started < 0.5
    assert towelled.towel is not None
    held = await store.current()
    assert held is not None and held.phase == "towel_cleanup"
    assert (held.lease_id, held.fencing_token) == (committed.lease_id, committed.fencing_token)
    assert record.lease is not None and record.lease.phase == "towel_cleanup"

    # The stale terminal release now answers and is refused: the towel keeps its row.
    store.answer()
    await drain_record_operations(manager, record)
    finished = await manager.get(created.id)
    assert finished.state == SessionState.TOWELLED
    assert "run_finished" not in _events(record)
    assert "run_committed" not in store.released_phases
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "towel_finished"
    assert record.lease_heartbeat_task is None
    assert await store.current() is None
    await manager.close()


async def test_the_public_towel_is_accepted_while_a_round_four_release_hangs() -> None:
    """Round 4 has measured and is settling its ring when the operator throws the towel."""

    store = HangingReleaseOf(phase="run_committed")
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda _competitor: engine,
        lease_store=store,
    )
    manager._lease_heartbeat = 0.01
    manager._terminal_settle_deadline = 10.0
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    record = manager._records[created.id]
    assert (await manager.get(created.id)).state == SessionState.RUNNING
    committed = await store.current()
    assert committed is not None and committed.phase == "run_committed"

    started = time.monotonic()
    towelled = await asyncio.wait_for(manager.start_towel(created.id, OWNER), timeout=2)
    assert time.monotonic() - started < 0.5
    assert towelled.towel is not None
    held = await store.current()
    assert held is not None and held.phase == "towel_cleanup"
    assert (held.lease_id, held.fencing_token) == (committed.lease_id, committed.fencing_token)
    assert record.lease is not None and record.lease.phase == "towel_cleanup"

    store.answer()
    await drain_record_operations(manager, record)
    finished = await manager.get(created.id)
    assert finished.state == SessionState.TOWELLED
    assert "run_finished" not in _events(record)
    assert "run_committed" not in store.released_phases
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "towel_finished"
    assert record.lease_heartbeat_task is None
    assert await store.current() is None
    await manager.close()


async def test_an_emergency_towel_is_not_queued_behind_a_hanging_terminal_release() -> None:
    """The towel moves the fence while the release is in flight; the stale release fails."""

    store = StalledCoordinator()
    manager = _stalled(store)
    record, committed, ttl = await _holding(manager, store)

    settling = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    await asyncio.wait_for(record.lease_lock.acquire(), timeout=0.5)
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
    manager._start_lease_heartbeat(record, ttl)
    towel_heartbeat = record.lease_heartbeat_task

    # The in-flight release of the pre-towel copy now answers, and is refused.
    store.answer()
    assert await asyncio.wait_for(settling, timeout=5) == "lost"
    held = await store.current()
    assert held == towel
    assert record.lease == towel
    assert record.lease_heartbeat_task is towel_heartbeat
    assert towel_heartbeat is not None and not towel_heartbeat.done()
    await manager.close()


async def test_a_cancelled_terminal_settle_leaves_a_replacement_fence_alone() -> None:
    store = StalledCoordinator(stall_release=False, refuse_release=True)
    manager = _stalled(store)
    record, committed, ttl = await _holding(manager, store)

    settling = asyncio.create_task(manager._settle_terminal_release(record, committed))

    async def retried() -> bool:
        return store.release_calls >= 2

    await _until(retried)
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
    manager._start_lease_heartbeat(record, ttl)
    towel_heartbeat = record.lease_heartbeat_task

    settling.cancel()
    with pytest.raises(asyncio.CancelledError):
        await settling

    assert record.lease_heartbeat_task is towel_heartbeat
    assert towel_heartbeat is not None and not towel_heartbeat.done()
    assert not manager._terminal_release_owns(record, towel)
    held = await store.current()
    assert held is not None and held.phase == "towel_cleanup"
    store.answer()
    await manager.close()


async def test_a_retried_terminal_release_never_releases_a_fence_that_moved_on() -> None:
    """The ring belongs to whoever moved the fence, typically a towel.

    The stores match a release on the lease id and fence token alone, so a retry
    that released its stale copy would delete the towel's row from under it.
    """

    store = OutageAtTerminalRelease(trip_phase="run_committed")
    manager = RunManager(lease_store=store)
    _bounded(manager, deadline=_deadline(True))
    record, committed, ttl = await _holding(manager, store)
    settling = asyncio.create_task(manager._settle_terminal_release(record, committed))
    await asyncio.wait_for(store.retried.wait(), timeout=5)

    async with record.lease_lock:
        record.lease = await store.transition(
            committed,
            operator=OWNER,
            expected_phase="run_committed",
            phase="towel_cleanup",
            session_state=SessionState.RUNNING,
            ttl=ttl,
        )
    manager._start_lease_heartbeat(record, ttl)
    store.heal()

    assert await asyncio.wait_for(settling, timeout=5) == "lost"
    held = await store.current()
    assert held is not None and held.phase == "towel_cleanup"
    assert store.released_phases == []
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()
    await manager.close()


class FailsAfterTheBellConnectionSpikeEngine(BackgroundCleanupConnectionSpikeEngine):
    """Accepts the bell, then fails; its backstage cleanup verifies at once."""

    def __init__(self) -> None:
        super().__init__()
        self.delete_accepted.set()
        self.cleanup_complete.set()
        self.ran = asyncio.Event()

    async def run(self, _arm, on_progress):
        await on_progress(SimpleNamespace(phase="burst"))
        self.ran.set()
        raise RuntimeError("Aurora dispatch failed after the bell")


async def test_round_five_post_bell_failure_keeps_its_cleanup_owed_until_the_ring_releases(
) -> None:
    """A live Round 5 failure whose main ring release goes unconfirmed after cleanup.

    The failure is published and sealed once. The cleanup that verified backstage
    still owes the ring, so it is left retryable, its Round 5 cleanup authority is
    kept, and the main heartbeat keeps the ring only while that retry is owed.
    Healed, automatic convergence releases both and nothing renews afterwards.
    """

    main = OutageAtTerminalRelease(trip_phase="cooldown")
    round5 = InMemoryBoutLeaseStore(ring_key="round5")
    engine = FailsAfterTheBellConnectionSpikeEngine()
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        lease_store=main,
        round5_lease_store=round5,
    )
    _bounded(manager, deadline=0.2)
    manager._cleanup_retry_initial = 0.05
    manager._cleanup_retry_max = 0.1
    created = await manager.create(_round_five())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(engine.ran.wait(), timeout=5)
    record = manager._records[created.id]

    failed = await _reach(manager, created.id, SessionState.FAILED)
    assert failed.failure is not None
    assert failed.comparison is None
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "session_failed"
    assert receipt.outcome == "stopped_short"
    await asyncio.wait_for(main.retried.wait(), timeout=5)

    async def owed():
        snapshot = await manager.get(created.id)
        setup = snapshot.round5_setup
        return snapshot if setup is not None and setup.cleanup_retryable else None

    pending = await _until(owed)
    assert pending.state == SessionState.FAILED
    assert _events(record).count("session_failed") == 1
    assert record.lease is not None and record.lease.phase == "cooldown"
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()
    cleanup_lease = await round5.current()
    assert cleanup_lease is not None and cleanup_lease.phase == "round5_cleanup"
    assert record.round5_lease is not None

    main.heal()

    async def converged():
        snapshot = await manager.get(created.id)
        setup = snapshot.round5_setup
        return snapshot if setup is not None and not setup.cleanup_retryable else None

    settled = await _until(converged)
    assert settled.state == SessionState.FAILED
    await _ring_freed(main)
    assert main.released_phases == ["cooldown"]
    assert await round5.current() is None
    await drain_record_operations(manager, record)
    assert record.lease is None
    assert record.round5_lease is None
    assert record.lease_heartbeat_task is None
    renewed = main.renewals_after_heal
    await asyncio.sleep(0.05)
    assert main.renewals_after_heal == renewed
    assert _events(record).count("session_failed") == 1
    await manager.close()


def _long_lived(manager: RunManager) -> None:
    """A foreground settle far shorter than the lease's TTL."""

    _bounded(manager, deadline=0.1)
    manager._running_lease_ttl = 30.0


async def test_a_coordinator_that_heals_after_the_deadline_frees_the_ring_long_before_its_ttl(
) -> None:
    store = OutageAtTerminalRelease(trip_phase="run_committed")
    manager = RunManager(lease_store=store)
    _long_lived(manager)
    record, committed, _ = await _holding(manager, store)

    assert await manager._settle_terminal_release(record, committed) == "unknown"
    assert record.lease_heartbeat_task is None
    retry = record.terminal_release_retry_task
    assert retry is not None and not retry.done()
    assert not manager._releasable(record)

    started = time.monotonic()
    store.heal()
    await _ring_freed(store)
    assert time.monotonic() - started < 5.0
    await asyncio.wait_for(retry, timeout=5)
    assert store.released_phases == ["run_committed"]
    assert store.renewals_after_heal == 0
    assert record.lease is None
    assert record.terminal_release_retry_task is None
    assert manager._unfinished_operations(record) == ()
    await manager.close()


async def test_the_background_release_never_touches_a_fence_that_moved_on() -> None:
    store = OutageAtTerminalRelease(trip_phase="run_committed")
    manager = RunManager(lease_store=store)
    _long_lived(manager)
    record, committed, ttl = await _holding(manager, store)
    assert await manager._settle_terminal_release(record, committed) == "unknown"
    retry = record.terminal_release_retry_task
    assert retry is not None

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
    store.heal()
    await asyncio.wait_for(retry, timeout=5)

    held = await store.current()
    assert held is not None and held.phase == "towel_cleanup"
    assert store.released_phases == []
    assert record.lease == towel
    assert record.terminal_release_retry_task is None
    await manager.close()


async def test_close_stops_the_background_release() -> None:
    store = OutageAtTerminalRelease(trip_phase="run_committed")
    manager = RunManager(lease_store=store)
    _long_lived(manager)
    manager._shutdown_settle_timeout = 1.0
    record, committed, _ = await _holding(manager, store)
    assert await manager._settle_terminal_release(record, committed) == "unknown"
    retry = record.terminal_release_retry_task
    assert retry is not None and not retry.done()

    await asyncio.wait_for(manager.close(), timeout=5)
    assert retry.done()
    assert record.terminal_release_retry_task is None
    assert record.lease_heartbeat_task is None
    assert store.renewals_after_heal == 0


async def _armed_round_six(manager: RunManager) -> tuple[str, SessionRecord]:
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    return created.id, manager._records[created.id]


async def _armed_round_five(manager: RunManager) -> tuple[str, SessionRecord]:
    created = await manager.create(_round_five())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    return created.id, manager._records[created.id]


async def _read_promptly(manager: RunManager, session_id: str):
    return await asyncio.wait_for(manager.get(session_id), timeout=0.5)


class BeforeRunningTarget(FakeLiveTarget):
    """A Round 1 lane parked after the bell commit but before RUNNING publication."""

    def __init__(
        self,
        lane_id: str,
        name: str,
        delay: float,
        *,
        entered: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        super().__init__(lane_id, name, delay)
        self._entered = entered
        self._release = release

    async def prepare(self):
        self._entered.set()
        await self._release.wait()
        return await super().prepare()


class BeforeRunningResolver:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.targets = (
            BeforeRunningTarget(
                "lakebase",
                "Lakebase",
                0.005,
                entered=self.entered,
                release=self.release,
            ),
            BeforeRunningTarget(
                "competitor",
                "Aurora Serverless v2",
                0.025,
                entered=self.entered,
                release=self.release,
            ),
        )

    def resolve(self, _competitor: CompetitorId):
        return self.targets


class BeforeRunningSafeChangeEngine(FakeSafeChangeEngine):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, arm, on_progress):
        self.entered.set()
        await self.release.wait()
        return await super().run(arm, on_progress)


class BeforeRunningRecoveryEngine(TransitionRaceRecoveryEngine):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, arm, on_progress, on_started, stop_control=None):
        self.entered.set()
        await self.release.wait()
        return await super().run(
            arm,
            on_progress,
            on_started,
            stop_control=stop_control,
        )


async def _assert_committed_cancel_refused(
    manager: RunManager,
    session_id: str,
) -> SessionRecord:
    record = manager._records[session_id]
    committed = await manager._lease_store.current()
    assert committed is not None
    assert committed.phase == "run_committed"
    assert record.lease == committed
    assert record.bell_committed_at is not None

    # Every round's card that shows the bell rang gets the same answer: there is
    # nothing left before the bell to cancel.
    assert record.snapshot.state == SessionState.RUNNING
    with pytest.raises(InvalidStateError, match="has not rung"):
        await manager.cancel_arm(session_id, OWNER)

    # A projection lagging behind the accepted bell can still read ARMED. Cancelling
    # is decided by the immutable accepted bell and the exact lease phase, not by it.
    async with record.lock:
        record.snapshot.state = SessionState.ARMED
        record.snapshot.run_started_at = None
    with pytest.raises(InvalidStateError, match="has not rung"):
        await manager.cancel_arm(session_id, OWNER)

    assert await manager._lease_store.current() == committed
    assert record.lease == committed
    assert "session_cancelled" not in _events(record)
    return record


async def _assert_one_run_receipt(
    manager: RunManager,
    session_id: str,
) -> None:
    terminal = await _reach(manager, session_id, SessionState.VERIFIED)
    assert terminal.run_started_at is not None
    record = manager._records[session_id]
    assert _events(record).count("run_finished") == 1
    assert "session_cancelled" not in _events(record)
    receipt = await sealed_receipt(session_id)
    assert receipt is not None
    assert receipt.sealing_event == "run_finished"
    assert [
        item.sealing_event
        for item in load_receipts()
        if item.session_id == session_id
    ] == ["run_finished"]


async def _cancel_start_waiter_after_bell_commit(
    manager: RunManager,
    monkeypatch: pytest.MonkeyPatch,
    session_id: str,
) -> SessionSnapshot:
    original_commit = manager._commit_bout
    committed = asyncio.Event()
    release_handoff = asyncio.Event()

    async def blocked_commit(record, operator):
        await original_commit(record, operator)
        committed.set()
        await release_handoff.wait()

    monkeypatch.setattr(manager, "_commit_bout", blocked_commit)
    waiter = asyncio.create_task(manager.start_run(session_id, OWNER))
    await asyncio.wait_for(committed.wait(), timeout=5)

    record = manager._records[session_id]
    assert record.snapshot.state == SessionState.ARMED
    assert record.lease is not None
    assert record.lease.phase == "run_committed"
    assert record.bell_handoff_task is not None
    assert not record.bell_handoff_task.done()
    assert record.bell_handoff_bout_id == record.snapshot.receipt_bout_id
    assert record.bell_handoff_task in manager._unfinished_operations(record)
    assert manager._releasable(record) is False

    retry = asyncio.create_task(manager.start_run(session_id, OWNER))
    await asyncio.sleep(0)
    assert not retry.done()

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert record.bell_handoff_task is not None
    assert not record.bell_handoff_task.done()
    assert record.lease is not None
    assert record.lease.phase == "run_committed"

    release_handoff.set()
    accepted = await asyncio.wait_for(retry, timeout=5)
    assert accepted.state == SessionState.RUNNING
    assert record.bell_committed_at is not None
    assert record.task is not None
    return accepted


async def test_reg_ss1_hung_post_commit_handoff_is_bounded_and_converges(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    resolver = BeforeRunningResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    manager._bell_handoff_timeout = 0.01
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    original_commit = manager._commit_bout
    committed = asyncio.Event()
    never = asyncio.Event()

    async def hung_after_commit(record, operator):
        await original_commit(record, operator)
        committed.set()
        await never.wait()

    monkeypatch.setattr(manager, "_commit_bout", hung_after_commit)
    caplog.set_level("ERROR", logger="server.manager")
    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(committed.wait(), timeout=5)

    with pytest.raises(InvalidStateError, match="runner handoff"):
        await asyncio.wait_for(ringing, timeout=5)
    failed = await asyncio.wait_for(
        _reach(manager, created.id, SessionState.FAILED),
        timeout=5,
    )
    assert failed.failure == "The accepted bell could not complete its runner handoff."
    assert await manager._lease_store.current() is None
    assert manager._records[created.id].lease_heartbeat_task is None
    assert "Bell handoff timed out" in caplog.text
    assert "outcome=runner_handoff_incomplete" in caplog.text
    await manager.close()


async def test_reg_ss1_commit_answer_loss_adopts_durable_fence_before_converging() -> None:
    class CommitThenHangStore(InMemoryBoutLeaseStore):
        def __init__(self) -> None:
            super().__init__()
            self.committed = asyncio.Event()
            self.never = asyncio.Event()

        async def transition(self, lease, **kwargs):
            transitioned = await super().transition(lease, **kwargs)
            if kwargs["expected_phase"] == "armed" and kwargs["phase"] == "run_committed":
                self.committed.set()
                await self.never.wait()
            return transitioned

    store = CommitThenHangStore()
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._bell_handoff_timeout = 0.01
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(store.committed.wait(), timeout=5)

    record = manager._records[created.id]
    assert record.lease is not None and record.lease.phase == "armed"
    durable = await store.current()
    assert durable is not None and durable.phase == "run_committed"
    armed_at = record.armed_at_monotonic
    assert armed_at is not None
    # The armed-window timer can wake after the durable commit but before its
    # lost response times out. It must defer to the handoff convergence instead
    # of releasing the stale ARMED copy and discarding adoption authority.
    assert await manager._abandon_armed_bout(record, armed_at) is False
    with pytest.raises(InvalidStateError, match="runner handoff"):
        await asyncio.wait_for(ringing, timeout=5)

    failed = await asyncio.wait_for(
        _reach(manager, created.id, SessionState.FAILED),
        timeout=5,
    )
    assert failed.failure == "The accepted bell could not complete its runner handoff."
    assert await store.current() is None
    assert record.lease is None
    assert record.lease_heartbeat_task is None
    await manager.close()


async def test_owner_cancel_hands_reserved_arm_to_uncommitted_bell_convergence() -> None:
    class HangBeforeBellCommitStore(InMemoryBoutLeaseStore):
        def __init__(self) -> None:
            super().__init__()
            self.entered = asyncio.Event()
            self.never = asyncio.Event()

        async def transition(self, lease, **kwargs):
            if kwargs["expected_phase"] == "armed" and kwargs["phase"] == "run_committed":
                self.entered.set()
                await self.never.wait()
            return await super().transition(lease, **kwargs)

    store = HangBeforeBellCommitStore()
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._bell_handoff_timeout = 0.02
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(store.entered.wait(), timeout=5)
    cancelling = asyncio.create_task(manager.cancel_arm(created.id, OWNER))

    await asyncio.gather(cancelling, return_exceptions=True)
    with pytest.raises(InvalidStateError, match="runner handoff"):
        await asyncio.wait_for(ringing, timeout=5)
    failed = await manager.get(created.id)
    record = manager._records[created.id]
    assert failed.state == SessionState.FAILED, (
        failed.state,
        record.armed_at_monotonic,
        record.armed_release_pending,
        record.armed_expiry_task,
        _events(record),
        await store.current(),
    )
    assert failed.run_started_at is None
    assert await store.current() is None
    assert record.armed_release_pending is False
    assert record.armed_expiry_task is None
    assert _events(record).count("session_failed") == 1
    await manager.close()


@pytest.mark.parametrize("first", ["cancel", "bell"])
async def test_owner_cancel_and_bell_honor_card_lock_order(
    monkeypatch: pytest.MonkeyPatch,
    first: str,
) -> None:
    manager = RunManager(resolver=BeforeRunningResolver(), verifier=make_verifier())
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]
    original_commit = manager._commit_bout
    commit_entered = asyncio.Event()
    release_commit = asyncio.Event()

    async def gated_commit(owned, operator):
        commit_entered.set()
        await release_commit.wait()
        return await original_commit(owned, operator)

    monkeypatch.setattr(manager, "_commit_bout", gated_commit)
    try:
        if first == "cancel":
            original_abandon = manager._abandon_after_arm_stops
            reserved = asyncio.Event()
            release_cancel = asyncio.Event()

            async def gated_abandon(*args, **kwargs):
                reserved.set()
                await release_cancel.wait()
                return await original_abandon(*args, **kwargs)

            monkeypatch.setattr(manager, "_abandon_after_arm_stops", gated_abandon)
            cancelling = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
            await asyncio.wait_for(reserved.wait(), timeout=5)
            assert record.armed_release_pending is True
            ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
            await asyncio.sleep(0)
            try:
                release_cancel.set()
                results = await asyncio.wait_for(
                    asyncio.gather(cancelling, ringing, return_exceptions=True),
                    timeout=5,
                )
            finally:
                release_cancel.set()
            snapshot = await manager.get(created.id)
            assert snapshot.state == SessionState.FAILED
            assert snapshot.run_started_at is None
            assert _events(record).count("session_cancelled") == 1
            assert isinstance(results[0], SessionSnapshot)
            assert isinstance(results[1], InvalidStateError)
            assert await manager._lease_store.current() is None
        else:
            ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
            await asyncio.wait_for(commit_entered.wait(), timeout=5)
            cancelling = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
            await asyncio.sleep(0)
            release_commit.set()
            results = await asyncio.gather(ringing, cancelling, return_exceptions=True)
            snapshot = await manager.get(created.id)
            assert snapshot.state in {SessionState.RUNNING, SessionState.VERIFIED}
            assert snapshot.run_started_at is not None
            assert "session_cancelled" not in _events(record)
            assert isinstance(results[1], InvalidStateError)
        assert record.armed_release_pending is False
        assert record.armed_expiry_task is None or record.armed_expiry_task.done()
    finally:
        release_commit.set()
        await manager.close()


async def test_same_turn_bell_and_cancel_never_leave_an_armed_card() -> None:
    manager = RunManager(resolver=BeforeRunningResolver(), verifier=make_verifier())
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]

    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    cancelling = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    results = await asyncio.gather(ringing, cancelling, return_exceptions=True)
    snapshot = await manager.get(created.id)
    assert snapshot.state != SessionState.ARMED
    assert record.armed_release_pending is False
    assert sum(isinstance(item, InvalidStateError) for item in results) == 1
    if snapshot.run_started_at is None:
        assert snapshot.state == SessionState.FAILED
        assert _events(record).count("session_cancelled") == 1
        assert "session_failed" not in _events(record)
    else:
        assert snapshot.state in {SessionState.RUNNING, SessionState.VERIFIED, SessionState.FAILED}
        assert "session_cancelled" not in _events(record)
    await manager.close()


async def test_lost_armed_commit_answer_adopts_and_releases_its_own_phase() -> None:
    class ArmedThenLoseAnswerStore(InMemoryBoutLeaseStore):
        async def transition(self, lease, **kwargs):
            transitioned = await super().transition(lease, **kwargs)
            if kwargs["expected_phase"] == "checking" and kwargs["phase"] == "armed":
                raise ConnectionError("armed commit response was lost")
            return transitioned

    store = ArmedThenLoseAnswerStore()
    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    failed = await _reach(manager, created.id, SessionState.FAILED)

    assert failed.run_started_at is None
    assert await store.current() is None
    assert manager._records[created.id].lease is None
    assert (await manager.start_arm(created.id, OWNER)).state == SessionState.CHECKING
    await manager.close()


async def test_lost_armed_commit_retries_adopted_phase_after_transient_release() -> None:
    class ArmedThenLoseAnswerThenFailOneRelease(InMemoryBoutLeaseStore):
        def __init__(self) -> None:
            super().__init__()
            self._armed_release_failures = 1

        async def transition(self, lease, **kwargs):
            transitioned = await super().transition(lease, **kwargs)
            if kwargs["expected_phase"] == "checking" and kwargs["phase"] == "armed":
                raise ConnectionError("armed commit response was lost")
            return transitioned

        async def release(self, lease):
            if lease.phase == "armed" and self._armed_release_failures:
                self._armed_release_failures -= 1
                raise ConnectionError("adopted armed release was transiently unreachable")
            return await super().release(lease)

    store = ArmedThenLoseAnswerThenFailOneRelease()
    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    failed = await _reach(manager, created.id, SessionState.FAILED)

    assert failed.run_started_at is None
    assert failed.failure is not None
    assert await store.current() is None
    assert manager._records[created.id].lease is None
    assert manager._records[created.id].terminal_release_retry_task is None
    assert _events(manager._records[created.id]).count("session_failed") == 1
    assert (await manager.start_arm(created.id, OWNER)).state == SessionState.CHECKING
    await manager.close()


async def test_unknown_settle_retries_adopted_armed_after_deadline() -> None:
    class ArmedThenLoseAnswerThenDarkUntilHeal(InMemoryBoutLeaseStore):
        def __init__(self) -> None:
            super().__init__()
            self.outage = False

        async def transition(self, lease, **kwargs):
            transitioned = await super().transition(lease, **kwargs)
            if kwargs["expected_phase"] == "checking" and kwargs["phase"] == "armed":
                self.outage = True
                raise ConnectionError("armed commit response was lost")
            return transitioned

        def heal(self) -> None:
            self.outage = False

        async def release(self, lease):
            if self.outage:
                raise ConnectionError("adopted armed release unreachable")
            return await super().release(lease)

    store = ArmedThenLoseAnswerThenDarkUntilHeal()
    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._terminal_settle_deadline = 0.05
    manager._terminal_release_backoff_cap = 0.02
    manager._active_lease_ttl = 10.0
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    failed = await _reach(manager, created.id, SessionState.FAILED)
    record = manager._records[created.id]
    retry = record.terminal_release_retry_task
    holder = await store.current()

    assert failed.run_started_at is None
    assert failed.failure is not None
    assert holder is not None and holder.phase == "armed"
    assert record.lease is not None and record.lease.phase == "armed"
    assert retry is not None and not retry.done()

    store.heal()
    await asyncio.wait_for(retry, timeout=5)
    assert await store.current() is None
    assert record.lease is None
    assert record.terminal_release_retry_task is None
    assert (await manager.start_arm(created.id, OWNER)).state == SessionState.CHECKING
    await manager.close()


async def test_adopted_armed_takeover_fails_the_card() -> None:
    class ArmedThenLoseAnswerThenTakeover(InMemoryBoutLeaseStore):
        def __init__(self) -> None:
            super().__init__()
            self._armed_release_attempted = False

        async def transition(self, lease, **kwargs):
            transitioned = await super().transition(lease, **kwargs)
            if kwargs["expected_phase"] == "checking" and kwargs["phase"] == "armed":
                raise ConnectionError("armed commit response was lost")
            return transitioned

        async def release(self, lease):
            if lease.phase == "armed":
                self._armed_release_attempted = True
                return False
            return await super().release(lease)

        async def current(self):
            active = await super().current()
            if (
                active is not None
                and active.phase == "armed"
                and self._armed_release_attempted
            ):
                return dataclasses.replace(
                    active,
                    lease_id="other-lease",
                    fencing_token=active.fencing_token + 1,
                    session_id="other-session",
                    owner_subject="other-subject",
                )
            return active

    store = ArmedThenLoseAnswerThenTakeover()
    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    failed = await _reach(manager, created.id, SessionState.FAILED)
    record = manager._records[created.id]
    holder = await store.current()

    assert failed.run_started_at is None
    assert failed.failure is not None
    assert failed.state == SessionState.FAILED
    assert "session_failed" in _events(record)
    assert record.lease is None
    assert holder is not None and holder.session_id == "other-session"
    await manager.close()


async def test_expired_adopted_armed_claim_fails_the_card() -> None:
    class ArmedThenExpireThenOtherClaims(InMemoryBoutLeaseStore):
        async def transition(self, lease, **kwargs):
            transitioned = await super().transition(lease, **kwargs)
            if kwargs["expected_phase"] == "checking" and kwargs["phase"] == "armed":
                self._lease = dataclasses.replace(
                    transitioned,
                    expires_at=datetime.now(UTC) + timedelta(milliseconds=20),
                )
                raise ConnectionError("armed commit response was lost")
            return transitioned

        async def release(self, lease):
            if lease.phase == "armed":
                await asyncio.sleep(0.04)
                released = await super().release(lease)
                if not released:
                    try:
                        await super().claim(
                            session_id="other-session",
                            operator=OTHER,
                            phase="checking",
                            session_state=SessionState.CHECKING,
                            round_id="wake_idle_app",
                            round_title="Wake this idle app",
                            competitor_id="aurora_serverless_v2",
                            competitor_name="Aurora Serverless v2",
                            ttl=timedelta(seconds=10),
                        )
                    except Exception:
                        pass
                return released
            return await super().release(lease)

    store = ArmedThenExpireThenOtherClaims()
    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._active_lease_ttl = 0.02
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    failed = await _reach(manager, created.id, SessionState.FAILED)
    record = manager._records[created.id]
    holder = await store.current()

    assert failed.run_started_at is None
    assert failed.failure is not None
    assert failed.state == SessionState.FAILED
    assert "session_failed" in _events(record)
    assert record.lease is None
    assert holder is not None and holder.session_id == "other-session"
    await manager.close()


async def test_round_six_cost_refusal_after_commit_is_an_accepted_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = RunManager(
        live_orders_factory=lambda _competitor: _round_six_engine(_round_six_lanes()),
    )
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    async def refuse_cost_window(*_args, **_kwargs):
        raise InvalidStateError("The bout cost ledger refused the accepted bell.")

    monkeypatch.setattr(manager, "_open_cost_bout", refuse_cost_window)
    with pytest.raises(InvalidStateError, match="cost window") as refused:
        await manager.start_run(created.id, OWNER)

    failed = await manager.get(created.id)
    record = manager._records[created.id]
    assert failed.state == SessionState.FAILED
    assert failed.run_started_at is not None
    assert failed.failure == str(refused.value)
    assert "session_cancelled" not in _events(record)
    assert _events(record).count("session_failed") == 1
    assert await manager._lease_store.current() is None
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "session_failed"
    await manager.close()


async def test_reg_ss1_unknown_commit_handoff_releases_lock_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = RunManager(resolver=BeforeRunningResolver(), verifier=make_verifier())
    manager._bell_handoff_timeout = 0.01
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    entered = asyncio.Event()
    never = asyncio.Event()

    async def hung_commit(_record, _operator):
        entered.set()
        await never.wait()

    monkeypatch.setattr(manager, "_commit_bout", hung_commit)
    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(entered.wait(), timeout=5)

    with pytest.raises(InvalidStateError, match="runner handoff"):
        await asyncio.wait_for(ringing, timeout=5)
    snapshot = await asyncio.wait_for(manager.get(created.id), timeout=0.1)
    assert snapshot.state == SessionState.FAILED
    assert snapshot.run_started_at is None
    assert snapshot.failure == (
        "The bell handoff timed out before durable acceptance; the ring was released."
    )
    assert await manager._lease_store.current() is None
    record = manager._records[created.id]
    assert record.bell_handoff_task is None
    assert record.armed_release_pending is False
    assert record.armed_expiry_task is None
    await manager.close()


async def test_reg_ss1_precommit_exception_releases_the_known_armed_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = RunManager(resolver=BeforeRunningResolver(), verifier=make_verifier())
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    async def refused_commit(_record, _operator):
        raise ConnectionError("coordinator refused before transition")

    monkeypatch.setattr(manager, "_commit_bout", refused_commit)
    with pytest.raises(ConnectionError, match="refused before transition"):
        await manager.start_run(created.id, OWNER)

    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    assert failed.run_started_at is None
    assert await manager._lease_store.current() is None
    await manager.close()


async def test_reg_ss1_unreadable_commit_answer_is_not_falsely_reported_as_a_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryBoutLeaseStore()
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._bell_handoff_timeout = 0.01
    manager._terminal_release_call_timeout = 0.02
    manager._armed_ttl = 0.5
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    # Once nothing renews the armed row, its presence is asked about once per this.
    manager._active_lease_ttl = 0.05
    entered = asyncio.Event()
    never = asyncio.Event()

    async def hung_commit(_record, _operator):
        entered.set()
        await never.wait()

    original_current = store.current

    async def unreadable_current():
        raise ConnectionError("coordinator unreadable")

    monkeypatch.setattr(manager, "_commit_bout", hung_commit)
    monkeypatch.setattr(store, "current", unreadable_current)
    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(entered.wait(), timeout=5)
    with pytest.raises(InvalidStateError) as refused:
        await asyncio.wait_for(ringing, timeout=5)

    failed = await manager.get(created.id)
    record = manager._records[created.id]
    assert failed.state == SessionState.FAILED
    assert failed.run_started_at is None
    assert failed.failure == (
        "Bell acceptance could not be confirmed during a coordinator outage; "
        "the ring is left to its durable expiry."
    )
    # The presenter is told what the card says, not a different story.
    assert str(refused.value) == failed.failure
    assert record.lease is not None and record.lease.phase == "armed"
    assert record.lease_heartbeat_task is None

    monkeypatch.setattr(store, "current", original_current)
    durable = await store.current()
    assert durable is not None and durable.phase == "armed"

    # Nothing renews or releases that row. Once it has expired, the card lets go of
    # it without calling it lost, and retention can forget the card.
    async def let_go() -> bool:
        return record.lease is None

    await _until(let_go)
    assert await store.current() is None
    assert record.ring_lease_lost is False
    await drain_record_operations(manager, record)
    assert manager._releasable(record)
    manager._session_retention_seconds = 0.001
    manager._session_retention_floor = 0
    await asyncio.sleep(0.01)
    await manager.create(_round_one())
    assert created.id not in manager._records
    await manager.close()


async def test_a_bell_commit_that_raises_in_an_outage_is_refused_with_the_card_message() -> None:
    """The bell's commit fails outright and the coordinator cannot say whether it landed.

    The card fails as unconfirmed, and the bell is answered with that same sentence as a
    conflict the browser shows, not as a server error.
    """

    class DarkFromTheBell(InMemoryBoutLeaseStore):
        def __init__(self) -> None:
            super().__init__()
            self.outage = False

        async def transition(self, lease, **kwargs):
            if kwargs["phase"] == "run_committed":
                self.outage = True
            if self.outage:
                raise ConnectionError("server closed the connection unexpectedly")
            return await super().transition(lease, **kwargs)

        async def current(self):
            if self.outage:
                raise ConnectionError("coordination endpoint unreachable")
            return await super().current()

        async def release(self, lease):
            if self.outage:
                raise ConnectionError("coordination endpoint unreachable")
            return await super().release(lease)

    store = DarkFromTheBell()
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._terminal_release_call_timeout = 0.02
    api_app = FastAPI()
    api_app.include_router(router)
    api_app.state.run_manager = manager

    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://anti-demo.test",
    ) as client:
        created = await client.post("/api/sessions", json=_round_one().model_dump(mode="json"))
        assert created.status_code == 201, created.text
        session_id = created.json()["id"]
        armed = await client.post(f"/api/sessions/{session_id}/arm")
        assert armed.status_code == 200, armed.text
        await _reach(manager, session_id, SessionState.ARMED)
        rung = await client.post(f"/api/sessions/{session_id}/run")
        card = (await client.get(f"/api/sessions/{session_id}")).json()

    assert store.outage is True
    assert rung.status_code == 409, rung.text
    assert card["state"] == SessionState.FAILED.value
    assert card["run_started_at"] is None
    assert card["failure"] == _BELL_ACCEPTANCE_UNCONFIRMED
    assert rung.json()["detail"] == card["failure"]
    store.outage = False
    await manager.close()


async def test_a_read_that_ignores_cancellation_cannot_hold_the_bell_past_its_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bell's convergence asks the coordinator what it holds, bounded by a deadline.

    A read on a black-holed socket outlasts its own cancellation. The deadline still
    ends the wait: the card fails as unconfirmed and the bell answers.
    """

    store = InMemoryBoutLeaseStore()
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._bell_handoff_timeout = 0.01
    manager._terminal_release_call_timeout = 0.05
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    entered = asyncio.Event()
    never = asyncio.Event()
    socket_closed = asyncio.Event()

    async def hung_commit(_record, _operator):
        entered.set()
        await never.wait()

    async def deaf_current():
        while not socket_closed.is_set():
            try:
                await socket_closed.wait()
            except asyncio.CancelledError:
                continue
        raise ConnectionError("server closed the connection unexpectedly")

    monkeypatch.setattr(manager, "_commit_bout", hung_commit)
    monkeypatch.setattr(store, "current", deaf_current)
    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        done, _ = await asyncio.wait({ringing}, timeout=1.0)
        assert ringing in done, "the bell is still waiting on a read that will not end"
        with pytest.raises(InvalidStateError, match="could not be confirmed"):
            ringing.result()
        failed = await manager.get(created.id)
        assert failed.state == SessionState.FAILED
        assert failed.run_started_at is None
    finally:
        socket_closed.set()
        await asyncio.gather(ringing, return_exceptions=True)
    await manager.close()


async def test_an_unconfirmed_bell_keeps_its_pointer_while_its_committed_row_lives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bell's commit landed and only its answer was lost, during the outage.

    The row the coordinator holds is then the bell's ``run_committed``, the armed
    fence's own authority, which outlives the armed copy's expiry. The card keeps its
    pointer while that row lives and lets go of it once that row has expired too.
    """

    class CommitThenHangStore(InMemoryBoutLeaseStore):
        def __init__(self) -> None:
            super().__init__()
            self.committed = asyncio.Event()

        async def transition(self, lease, **kwargs):
            transitioned = await super().transition(lease, **kwargs)
            if kwargs["phase"] == "run_committed":
                self.committed.set()
                await asyncio.Event().wait()
            return transitioned

    store = CommitThenHangStore()
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    manager._bell_handoff_timeout = 0.01
    manager._terminal_release_call_timeout = 0.02
    manager._armed_ttl = 0.2
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]
    armed = record.lease
    assert armed is not None and armed.phase == "armed"
    manager._active_lease_ttl = 0.05
    manager._running_lease_ttl = 1.0
    original_current = store.current

    async def unreadable_current():
        raise ConnectionError("coordinator unreadable")

    monkeypatch.setattr(store, "current", unreadable_current)
    ringing = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(store.committed.wait(), timeout=5)
    with pytest.raises(InvalidStateError, match="could not be confirmed"):
        await asyncio.wait_for(ringing, timeout=5)
    monkeypatch.setattr(store, "current", original_current)

    async def armed_copy_expired() -> bool:
        return datetime.now(UTC) > armed.expires_at + timedelta(milliseconds=150)

    await _until(armed_copy_expired)
    committed = await store.current()
    assert committed is not None and committed.phase == "run_committed"
    assert record.lease == armed

    async def let_go() -> bool:
        return record.lease is None

    await _until(let_go)
    assert await store.current() is None
    assert record.ring_lease_lost is False
    await drain_record_operations(manager, record)
    assert manager._releasable(record)
    await manager.close()


async def test_reg_ss1_no_bell_card_can_cancel_after_its_ring_was_already_released() -> None:
    """A pre-run refusal must not strand an ARMED card behind a missing lease."""

    store = InMemoryBoutLeaseStore()
    manager = RunManager(
        resolver=BeforeRunningResolver(),
        verifier=make_verifier(),
        lease_store=store,
    )
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]
    assert record.lease is not None
    assert await store.release(record.lease)
    async with record.lease_lock:
        record.lease = None

    cancelled = await asyncio.wait_for(
        manager.cancel_arm(created.id, OWNER),
        timeout=5,
    )
    assert cancelled.state == SessionState.FAILED
    assert cancelled.failure == _ARMED_CARD_CANCELLED
    assert cancelled.run_started_at is None
    await manager.close()


async def test_reg_ss1_stale_no_bell_finalizer_clears_only_its_reservation() -> None:
    manager = RunManager(resolver=BeforeRunningResolver(), verifier=make_verifier())
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]
    armed_at = record.armed_at_monotonic
    assert armed_at is not None

    record.armed_release_pending = True
    record.armed_expiry_task = asyncio.current_task()
    record.snapshot.state = SessionState.RUNNING
    record.bell_committed_at = record.snapshot.updated_at
    assert not await manager._abandon_armed_bout(
        record,
        armed_at,
        release_reserved=True,
    )
    assert record.armed_release_pending is False
    assert record.armed_expiry_task is None
    await manager.close()


async def test_reg_ss1_r1_cancel_after_accepted_bell_keeps_run_and_idle_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = BeforeRunningResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    idle_watch_scheduled = asyncio.Event()
    original_schedule = manager._schedule_round_one_cooldown

    def observe_schedule(record: SessionRecord) -> None:
        idle_watch_scheduled.set()
        original_schedule(record)

    monkeypatch.setattr(manager, "_schedule_round_one_cooldown", observe_schedule)
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    accepted = await _cancel_start_waiter_after_bell_commit(
        manager,
        monkeypatch,
        created.id,
    )
    assert accepted.state == SessionState.RUNNING
    await asyncio.wait_for(resolver.entered.wait(), timeout=5)
    await _assert_committed_cancel_refused(manager, created.id)

    resolver.release.set()
    await _assert_one_run_receipt(manager, created.id)
    await asyncio.wait_for(idle_watch_scheduled.wait(), timeout=5)
    assert manager._records[created.id].snapshot.cooldown is not None
    await manager.close()


async def test_reg_ss1_r2_cancel_after_accepted_bell_keeps_run_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = BeforeRunningSafeChangeEngine()
    manager = RunManager(safe_change_factory=lambda: engine)
    cleanup_scheduled = asyncio.Event()
    original_schedule = manager._schedule_owned_artifact_cleanup

    def observe_schedule(record: SessionRecord, *, recovery: bool) -> None:
        assert recovery is False
        cleanup_scheduled.set()
        original_schedule(record, recovery=recovery)

    monkeypatch.setattr(manager, "_schedule_owned_artifact_cleanup", observe_schedule)
    created = await manager.create(_round_two())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    accepted = await _cancel_start_waiter_after_bell_commit(
        manager,
        monkeypatch,
        created.id,
    )
    assert accepted.state == SessionState.RUNNING
    await asyncio.wait_for(engine.entered.wait(), timeout=5)
    await _assert_committed_cancel_refused(manager, created.id)

    engine.release.set()
    await _assert_one_run_receipt(manager, created.id)
    await asyncio.wait_for(cleanup_scheduled.wait(), timeout=5)
    assert manager._records[created.id].snapshot.cooldown is not None
    await manager.close()


async def test_reg_ss1_r3_cancel_after_accepted_bell_keeps_run_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = BeforeRunningRecoveryEngine()
    manager = RunManager(recovery_factory=lambda: engine)
    cleanup_scheduled = asyncio.Event()
    original_schedule = manager._schedule_owned_artifact_cleanup

    def observe_schedule(record: SessionRecord, *, recovery: bool) -> None:
        assert recovery is True
        cleanup_scheduled.set()
        original_schedule(record, recovery=recovery)

    monkeypatch.setattr(manager, "_schedule_owned_artifact_cleanup", observe_schedule)
    created = await manager.create(_round_three())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    accepted = await _cancel_start_waiter_after_bell_commit(
        manager,
        monkeypatch,
        created.id,
    )
    assert accepted.state == SessionState.RUNNING
    await asyncio.wait_for(engine.entered.wait(), timeout=5)
    await _assert_committed_cancel_refused(manager, created.id)

    engine.complete_competitor.set()
    engine.release.set()
    await _assert_one_run_receipt(manager, created.id)
    await asyncio.wait_for(cleanup_scheduled.wait(), timeout=5)
    assert manager._records[created.id].snapshot.cooldown is not None
    await manager.close()


@pytest.mark.parametrize("round_number", [1, 2, 3], ids=["round-1", "round-2", "round-3"])
async def test_a_towel_before_the_first_timed_step_stops_the_bout(round_number: int) -> None:
    """Rounds 1-3 read RUNNING from the accepted bell, before their runner's first timed step.

    A towel in that window is a committed run stopped at zero, not a refusal: the bout
    is towelled, its cleanup runs and the ring comes back.
    """

    store = InMemoryBoutLeaseStore()
    if round_number == 1:
        resolver = BeforeRunningResolver()
        manager = RunManager(resolver=resolver, verifier=make_verifier(), lease_store=store)
        request, entered, release = _round_one(), resolver.entered, resolver.release
    elif round_number == 2:
        safe_change = BeforeRunningSafeChangeEngine()
        manager = RunManager(safe_change_factory=lambda: safe_change, lease_store=store)
        request, entered, release = _round_two(), safe_change.entered, safe_change.release
    else:
        recovery = BeforeRunningRecoveryEngine()
        recovery.complete_competitor.set()
        manager = RunManager(recovery_factory=lambda: recovery, lease_store=store)
        request, entered, release = _round_three(), recovery.entered, recovery.release
    created = await manager.create(request)
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    rung = await manager.start_run(created.id, OWNER)
    assert rung.state == SessionState.RUNNING
    await asyncio.wait_for(entered.wait(), timeout=5)
    record = manager._records[created.id]
    assert record.bell_committed_at is not None
    assert record.run_started_monotonic_ns is None

    towelled = await asyncio.wait_for(manager.start_towel(created.id, OWNER), timeout=5)
    assert towelled.towel is not None
    release.set()
    stopped = await _reach(manager, created.id, SessionState.TOWELLED)
    assert stopped.towel is not None
    assert "run_finished" not in _events(record)
    await drain_record_operations(manager, record)
    assert record.lease is None
    assert await store.current() is None
    await manager.close()


@pytest.mark.parametrize("round_number", [1, 2, 3], ids=["round-1", "round-2", "round-3"])
async def test_a_terminal_card_is_never_revived_by_its_committed_run(round_number: int) -> None:
    """The card turns FAILED while its committed run is still parked in its engine.

    A terminal state is authoritative. When the run gets going again, its callbacks
    neither start the clock nor publish a result over the failure.
    """

    if round_number == 1:
        resolver = BeforeRunningResolver()
        manager = RunManager(resolver=resolver, verifier=make_verifier())
        request, entered, release = _round_one(), resolver.entered, resolver.release
    elif round_number == 2:
        safe_change = BeforeRunningSafeChangeEngine()
        manager = RunManager(safe_change_factory=lambda: safe_change)
        request, entered, release = _round_two(), safe_change.entered, safe_change.release
    else:
        recovery = BeforeRunningRecoveryEngine()
        recovery.complete_competitor.set()
        manager = RunManager(recovery_factory=lambda: recovery)
        request, entered, release = _round_three(), recovery.entered, recovery.release
    created = await manager.create(request)
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, OWNER)
    await asyncio.wait_for(entered.wait(), timeout=5)
    record = manager._records[created.id]
    run = record.task
    assert run is not None and not run.done()
    failure = "Ring lease lost; active work was stopped before another bout can begin."
    async with record.lock:
        assert record.bell_committed_at is not None
        record.snapshot.state = SessionState.FAILED
        record.snapshot.failure = failure

    release.set()
    await asyncio.wait_for(asyncio.gather(run, return_exceptions=True), timeout=5)
    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    assert failed.failure == failure
    assert "run_started" not in _events(record)
    assert "run_finished" not in _events(record)
    receipts = [item for item in load_receipts() if item.session_id == created.id]
    assert all(item.outcome != "declared" for item in receipts)
    await manager.close()


async def test_close_drains_committed_bell_handoff_before_cancelling_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = BeforeRunningResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    original_commit = manager._commit_bout
    committed = asyncio.Event()
    release_handoff = asyncio.Event()

    async def blocked_commit(record, operator):
        await original_commit(record, operator)
        committed.set()
        await release_handoff.wait()

    monkeypatch.setattr(manager, "_commit_bout", blocked_commit)
    waiter = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(committed.wait(), timeout=5)
    closing = asyncio.create_task(manager.close())
    await asyncio.sleep(0)
    assert not closing.done()

    release_handoff.set()
    accepted = await asyncio.wait_for(waiter, timeout=5)
    assert accepted.state == SessionState.RUNNING
    await asyncio.wait_for(closing, timeout=5)

    record = manager._records[created.id]
    assert record.bell_handoff_task is None
    assert manager._unfinished_operations(record) == ()


async def test_close_discovers_and_drains_cancelled_bell_convergence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = RunManager(resolver=BeforeRunningResolver(), verifier=make_verifier())
    manager._shutdown_settle_timeout = 0.01
    manager._shutdown_cleanup_timeout = 5
    created = await manager.create(_round_one())
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)

    original_commit = manager._commit_bout
    original_converge = manager._converge_cancelled_bell_handoff
    committed = asyncio.Event()
    convergence_entered = asyncio.Event()
    allow_convergence = asyncio.Event()
    never = asyncio.Event()

    async def blocked_commit(record, operator):
        await original_commit(record, operator)
        committed.set()
        await never.wait()

    async def blocked_converge(*args, **kwargs):
        convergence_entered.set()
        await allow_convergence.wait()
        return await original_converge(*args, **kwargs)

    monkeypatch.setattr(manager, "_commit_bout", blocked_commit)
    monkeypatch.setattr(manager, "_converge_cancelled_bell_handoff", blocked_converge)
    waiter = asyncio.create_task(manager.start_run(created.id, OWNER))
    await asyncio.wait_for(committed.wait(), timeout=5)

    closing = asyncio.create_task(manager.close())
    try:
        await asyncio.wait_for(convergence_entered.wait(), timeout=5)
        convergence = manager._records[created.id].bell_convergence_task
        assert convergence is not None and not convergence.done()
        assert not closing.done()

        allow_convergence.set()
        await asyncio.wait_for(closing, timeout=5)
        await asyncio.gather(waiter, return_exceptions=True)
        assert convergence.done()
        assert manager._records[created.id].bell_convergence_task is None
        assert await manager._lease_store.current() is None
        assert _events(manager._records[created.id]).count("session_failed") == 1
    finally:
        allow_convergence.set()
        if not closing.done():
            await asyncio.gather(closing, return_exceptions=True)


@pytest.mark.parametrize("round_number", [1, 6], ids=["round-1", "round-6"])
async def test_owner_cancel_wins_while_armed_publication_is_in_flight(
    monkeypatch: pytest.MonkeyPatch,
    round_number: int,
) -> None:
    store = HangingReleaseOf(phase="checking")
    if round_number == 1:
        manager = RunManager(
            resolver=FakeResolver(),
            verifier=make_verifier(),
            lease_store=store,
        )
        request = _round_one()
    else:
        engine = _round_six_engine(_round_six_lanes())
        manager = RunManager(
            live_orders_factory=lambda _competitor: engine,
            lease_store=store,
        )
        request = _round_six()
    entered = asyncio.Event()
    resume = asyncio.Event()
    original = manager._mark_bout_armed

    async def blocked_mark(record, armed_expires_at):
        entered.set()
        await resume.wait()
        await original(record, armed_expires_at)

    monkeypatch.setattr(manager, "_mark_bout_armed", blocked_mark)
    created = await manager.create(request)
    await manager.start_arm(created.id, OWNER)
    await asyncio.wait_for(entered.wait(), timeout=5)
    record = manager._records[created.id]
    arm_task = record.task
    assert arm_task is not None and not arm_task.done()
    assert (await manager.get(created.id)).state == SessionState.ARMED

    cancelling = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    assert record.armed_release_pending
    resume.set()
    store.answer()
    cancelled = await asyncio.wait_for(cancelling, timeout=5)

    assert cancelled.state == SessionState.FAILED
    assert cancelled.failure == _ARMED_CARD_CANCELLED
    assert arm_task.done()
    assert _events(record).count("session_cancelled") == 1
    assert "session_failed" not in _events(record)
    assert "armed" not in _events(record)
    receipts = [item for item in load_receipts() if item.session_id == created.id]
    assert [item.sealing_event for item in receipts] == ["session_cancelled"]
    await drain_record_operations(manager, record)
    assert record.armed_expiry_task is None
    assert record.lease_heartbeat_task is None
    assert manager._unfinished_operations(record) == ()
    await manager.close()


@HEALS
async def test_an_owner_cancel_of_an_armed_card_settles_its_release_and_finishes(
    heals: bool,
) -> None:
    store = OutageAtTerminalRelease(trip_phase="armed")
    engine = _round_six_engine(_round_six_lanes())
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(heals))
    session_id, record = await _armed_round_six(manager)

    cancelling = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
    await asyncio.wait_for(store.tripped.wait(), timeout=5)
    if heals:
        await asyncio.wait_for(store.retried.wait(), timeout=5)
        # The card is read, and its bell refused, while the release is retried.
        pending = await _read_promptly(manager, session_id)
        assert pending.state == SessionState.ARMED
        assert not set(_events(record)) & SEALING_EVENTS
        with pytest.raises(InvalidStateError):
            await manager.start_run(session_id, OWNER)
        store.heal()

    cancelled = await asyncio.wait_for(cancelling, timeout=5)
    assert cancelled.state == SessionState.FAILED
    assert cancelled.failure == _ARMED_CARD_CANCELLED
    assert _events(record).count("session_cancelled") == 1
    assert "session_failed" not in _events(record)
    receipt = await sealed_receipt(session_id)
    assert receipt is not None
    assert receipt.sealing_event == "session_cancelled"
    assert receipt.outcome == "stopped_short"
    assert record.lease_heartbeat_task is None
    assert record.armed_expiry_task is None
    if heals:
        assert await store.current() is None
    else:
        await _never_renewed_after(store)
    await drain_record_operations(manager, record)
    assert record.terminal_release_retry_task is None
    assert record.lease is None
    # A second press returns the released card.
    again = await manager.cancel_arm(session_id, OWNER)
    assert again.failure == _ARMED_CARD_CANCELLED
    assert _events(record).count("session_cancelled") == 1
    await manager.close()


async def test_a_cancelled_owner_request_cannot_orphan_an_armed_release() -> None:
    store = HangingReleaseOf(phase="armed")
    engine = _round_six_engine(_round_six_lanes())
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=10.0)
    session_id, record = await _armed_round_six(manager)

    request = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    release = record.armed_expiry_task
    assert release is not None and release is not request and not release.done()

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert not release.done()
    assert (await _read_promptly(manager, session_id)).state == SessionState.ARMED
    with pytest.raises(InvalidStateError, match="being released"):
        await manager.start_run(session_id, OWNER)

    store.answer()
    assert await asyncio.wait_for(release, timeout=5)
    cancelled = await manager.get(session_id)
    assert cancelled.state == SessionState.FAILED
    assert cancelled.failure == _ARMED_CARD_CANCELLED
    assert _events(record).count("session_cancelled") == 1
    assert "session_failed" not in _events(record)
    receipt = await sealed_receipt(session_id)
    assert receipt is not None
    assert receipt.sealing_event == "session_cancelled"
    assert record.armed_expiry_task is None
    assert record.lease_heartbeat_task is None
    assert await store.current() is None
    await manager.close()


@pytest.mark.parametrize("first_request", ["dropped", "waiting"])
@pytest.mark.parametrize("round_number", [1, 3], ids=["round-1", "round-3"])
async def test_a_repeated_owner_cancel_joins_the_armed_release_in_flight(
    round_number: int,
    first_request: str,
) -> None:
    """The owner presses cancel again while the first press's release is still settling.

    A browser retries a cancel whose answer it lost, and a presenter presses twice. Either
    way the repeat waits for the release already running and answers with the released
    card: one release, one cancellation, one receipt.
    """

    store = HangingReleaseOf(phase="armed")
    if round_number == 1:
        manager = RunManager(resolver=FakeResolver(), verifier=make_verifier(), lease_store=store)
        request = _round_one()
    else:
        engine = TransitionRaceRecoveryEngine()
        manager = RunManager(recovery_factory=lambda: engine, lease_store=store)
        request = _round_three()
    _bounded(manager, deadline=10.0)
    created = await manager.create(request)
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]

    first = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    release = record.armed_expiry_task
    assert release is not None and not release.done()
    if first_request == "dropped":
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

    repeated = asyncio.create_task(manager.cancel_arm(created.id, OWNER))
    await asyncio.sleep(0.05)
    assert not repeated.done()
    assert record.armed_expiry_task is release
    assert (await _read_promptly(manager, created.id)).state == SessionState.ARMED

    store.answer()
    cancelled = await asyncio.wait_for(repeated, timeout=5)
    assert cancelled.state == SessionState.FAILED
    assert cancelled.failure == _ARMED_CARD_CANCELLED
    assert cancelled.run_started_at is None
    if first_request == "waiting":
        assert (await asyncio.wait_for(first, timeout=5)).failure == _ARMED_CARD_CANCELLED
    assert store.release_calls == 1
    assert store.released_phases == ["armed"]
    assert _events(record).count("session_cancelled") == 1
    assert "session_failed" not in _events(record)
    receipts = [item for item in load_receipts() if item.session_id == created.id]
    assert [item.sealing_event for item in receipts] == ["session_cancelled"]
    await drain_record_operations(manager, record)
    assert record.armed_release_pending is False
    assert record.armed_expiry_task is None
    assert record.lease is None
    assert await store.current() is None
    await manager.close()


async def test_a_repeated_round_five_owner_cancel_joins_its_no_bell_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, engine, session_id, record = await _round5_no_bell_harness()
    entered = asyncio.Event()
    allow = asyncio.Event()
    original = manager._abandon_armed_bout

    async def gated_abandon(*args, **kwargs):
        entered.set()
        await allow.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(manager, "_abandon_armed_bout", gated_abandon)
    first = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
    repeated: asyncio.Task | None = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        release = record.armed_expiry_task
        assert release is not None and not release.done()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        repeated = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
        await asyncio.sleep(0.05)
        assert not repeated.done()
        assert record.armed_expiry_task is release

        allow.set()
        cancelled = await asyncio.wait_for(repeated, timeout=5)
        assert cancelled.state == SessionState.FAILED
        assert cancelled.run_started_at is None
        assert _events(record).count("session_failed") == 1
        assert "run_started" not in _events(record)
        assert not engine.run_started.is_set()
        assert not record.armed_release_pending
    finally:
        allow.set()
        for task in (first, repeated):
            if task is not None and not task.done():
                await asyncio.gather(task, return_exceptions=True)
        await manager.close()


@pytest.mark.parametrize("first", ["expiry", "owner-cancel"])
async def test_armed_expiry_and_owner_cancel_have_one_terminal_winner(first: str) -> None:
    store = HangingReleaseOf(phase="armed")
    engine = _round_six_engine(_round_six_lanes())
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=10.0)
    session_id, record = await _armed_round_six(manager)
    armed_at = record.armed_at_monotonic
    assert armed_at is not None
    manager._cancel_armed_expiry(record, force=True)

    if first == "expiry":
        winner = asyncio.create_task(manager._abandon_armed_bout(record, armed_at))
        record.armed_expiry_task = winner
        expected_event = "session_failed"
        expected_failure = "Fight card expired before the bell."
    else:
        winner = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
        expected_event = "session_cancelled"
        expected_failure = _ARMED_CARD_CANCELLED

    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    assert record.armed_release_pending
    assert store.release_calls == 1
    assert not set(_events(record)) & SEALING_EVENTS

    if first == "expiry":
        with pytest.raises(InvalidStateError, match="changed before"):
            await manager.cancel_arm(session_id, OWNER)
    else:
        # The timer lost the reservation race and reports that it changed nothing.
        assert not await manager._abandon_armed_bout(record, armed_at)

    assert store.release_calls == 1
    store.answer()
    won = await asyncio.wait_for(winner, timeout=5)
    if first == "expiry":
        assert won is True
    else:
        assert won.state == SessionState.FAILED

    finished = await manager.get(session_id)
    assert finished.state == SessionState.FAILED
    assert finished.failure is not None and finished.failure.startswith(expected_failure)
    sealing = [event for event in _events(record) if event in SEALING_EVENTS]
    assert sealing == [expected_event]
    receipt = await sealed_receipt(session_id)
    assert receipt is not None
    assert receipt.sealing_event == expected_event
    receipts = [item for item in load_receipts() if item.session_id == session_id]
    assert [item.sealing_event for item in receipts] == [expected_event]
    assert store.release_calls == 1

    await drain_record_operations(manager, record)
    assert record.armed_expiry_task is None
    assert record.lease_heartbeat_task is None
    assert record.terminal_release_retry_task is None
    assert record.terminal_release_settles == []
    assert record.lease is None
    assert await store.current() is None
    await manager.close()


@HEALS
async def test_an_armed_window_that_expires_while_its_release_fails_still_finishes(
    heals: bool,
) -> None:
    store = OutageAtTerminalRelease(trip_phase="armed")
    engine = _round_six_engine(_round_six_lanes())
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=_deadline(heals))
    manager._armed_ttl = 0.05
    session_id, record = await _armed_round_six(manager)

    await asyncio.wait_for(store.tripped.wait(), timeout=5)
    if heals:
        await asyncio.wait_for(store.retried.wait(), timeout=5)
        pending = await _read_promptly(manager, session_id)
        assert pending.state == SessionState.ARMED
        assert not set(_events(record)) & SEALING_EVENTS
        store.heal()

    expired = await _reach(manager, session_id, SessionState.FAILED)
    assert expired.failure is not None
    assert expired.failure.startswith("Fight card expired before the bell.")
    assert _events(record).count("session_failed") == 1
    receipt = await sealed_receipt(session_id)
    assert receipt is not None
    assert receipt.sealing_event == "session_failed"
    assert receipt.outcome == "stopped_short"
    assert record.lease_heartbeat_task is None
    if heals:
        assert await store.current() is None
    else:
        await _never_renewed_after(store)
    await drain_record_operations(manager, record)
    assert record.terminal_release_retry_task is None
    assert record.armed_expiry_task is None
    # Freed without close(): the owner prepares the card again.
    assert (await manager.start_arm(session_id, OWNER)).state == SessionState.CHECKING
    await manager.close()


def _declared_lost(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if "declared lost" in r.getMessage()]


@pytest.mark.parametrize("round_number", [1, 6], ids=["round-1", "round-6"])
async def test_an_armed_window_expiring_untouched_is_not_a_lost_lease(
    round_number: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The armed row ends at its own TTL, and the window's timer wakes just after it.

    Finding that row gone is the window expiring as designed: the card fails as expired,
    nothing is declared lost, and the ring is this process's to claim again at once.
    """

    store = InMemoryBoutLeaseStore()
    if round_number == 1:
        manager = RunManager(resolver=FakeResolver(), verifier=make_verifier(), lease_store=store)
        request = _round_one()
    else:
        manager = RunManager(
            live_orders_factory=lambda _competitor: _round_six_engine(_round_six_lanes()),
            lease_store=store,
        )
        request = _round_six()
    manager._armed_ttl = 0.2
    created = await manager.create(request)
    caplog.set_level(logging.WARNING, logger="server.manager")
    await manager.start_arm(created.id, OWNER)
    await _reach(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]

    expired = await _reach(manager, created.id, SessionState.FAILED)
    assert expired.failure is not None
    assert expired.failure.startswith("Fight card expired before the bell.")
    assert _events(record).count("session_failed") == 1
    await drain_record_operations(manager, record)
    assert record.lease is None
    assert record.ring_lease_lost is False
    assert _declared_lost(caplog) == []
    assert await store.current() is None
    assert (await manager.start_arm(created.id, OWNER)).state == SessionState.CHECKING
    await manager.close()


async def _holding_unrenewed(
    manager: RunManager,
    store: InMemoryBoutLeaseStore,
    phase: str,
    ttl: float,
):
    """A Round 1 card holding a ``phase`` fence that nothing renews."""

    created = await manager.create(_round_one())
    record = manager._records[created.id]
    record.operator = OWNER
    sent = time.monotonic()
    lease = await store.claim(
        session_id=created.id,
        operator=OWNER,
        phase=phase,
        session_state=SessionState.ARMED,
        round_id=record.snapshot.round.id.value,
        round_title=record.snapshot.round.title,
        competitor_id=record.snapshot.competitor.id.value,
        competitor_name=record.snapshot.competitor.short_name,
        ttl=timedelta(seconds=ttl),
    )
    manager._set_lease(record, lease, sent)
    return record, lease


async def _release_unrenewed(manager: RunManager, record: SessionRecord, lease, path: str):
    if path == "terminal settle":
        assert await manager._settle_terminal_release(record, lease) == "released"
    else:
        assert await manager._release_bout(record) is True


@pytest.mark.parametrize("path", ["terminal settle", "release_bout"])
@pytest.mark.parametrize("phase", ["armed", "cooldown_failed"])
async def test_an_unrenewed_fence_found_gone_after_its_deadline_simply_expired(
    phase: str,
    path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = InMemoryBoutLeaseStore()
    manager = RunManager(lease_store=store)
    record, lease = await _holding_unrenewed(manager, store, phase, ttl=0.05)
    await asyncio.sleep(0.1)
    assert await store.current() is None

    with caplog.at_level(logging.WARNING, logger="server.manager"):
        await _release_unrenewed(manager, record, lease, path)
    assert record.lease is None
    assert record.ring_lease_lost is False
    assert _declared_lost(caplog) == []
    await manager.close()


@pytest.mark.parametrize("path", ["terminal settle", "release_bout"])
@pytest.mark.parametrize("phase", ["armed", "cooldown_failed"])
async def test_an_unrenewed_fence_found_gone_before_its_deadline_is_still_lost(
    phase: str,
    path: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = JumpingClock()
    manager = RunManager(lease_store=store)
    record, lease = await _holding_unrenewed(manager, store, phase, ttl=30)
    store.jump()
    assert await store.current() is None

    with caplog.at_level(logging.WARNING, logger="server.manager"):
        await _release_unrenewed(manager, record, lease, path)
    assert record.lease is None
    assert record.ring_lease_lost is True
    (declared,) = _declared_lost(caplog)
    assert "reason=row_absent" in declared
    assert f"phase={phase}" in declared
    await manager.close()


async def test_a_hanging_armed_release_does_not_hold_the_card_and_spares_a_new_holder(
) -> None:
    """The armed fence lapses mid-release and another bout takes the ring.

    The hung release of the old copy then answers: it frees nothing, the
    coordinator names the new holder, and the card reports its lost lease
    instead of a cancellation.
    """

    store = HangingReleaseOf(phase="armed")
    engine = _round_six_engine(_round_six_lanes())
    manager = RunManager(live_orders_factory=lambda _competitor: engine, lease_store=store)
    _bounded(manager, deadline=10.0)
    session_id, record = await _armed_round_six(manager)
    armed = record.lease
    assert armed is not None

    cancelling = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
    await asyncio.wait_for(store.release_started.wait(), timeout=5)
    assert (await _read_promptly(manager, session_id)).state == SessionState.ARMED

    # The armed row lapses and another session claims the ring.
    await InMemoryBoutLeaseStore.release(store, armed)
    other = await manager.create(_round_six())
    newcomer = await store.claim(
        session_id=other.id,
        operator=OTHER,
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id=armed.round_id,
        round_title=armed.round_title,
        competitor_id=armed.competitor_id,
        competitor_name=armed.competitor_name,
        ttl=timedelta(seconds=30),
    )
    store.answer()

    with pytest.raises(InvalidStateError):
        await asyncio.wait_for(cancelling, timeout=5)
    assert await store.current() == newcomer
    assert "session_cancelled" not in _events(record)
    lost = await manager.get(session_id)
    assert lost.state == SessionState.FAILED
    assert lost.failure is not None and "lease lost" in lost.failure.lower()
    assert record.lease is None
    assert record.lease_heartbeat_task is None
    await manager.close()


@HEALS
async def test_round_five_failure_releases_its_cleanup_lease_without_holding_the_card(
    heals: bool,
) -> None:
    main = InMemoryBoutLeaseStore()
    round5 = HangingReleaseOf(phase=None, ring_key="round5")
    engine = RefusingCheckConnectionSpikeEngine()
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        lease_store=main,
        round5_lease_store=round5,
    )
    _bounded(manager, deadline=_deadline(heals))
    manager._terminal_release_call_timeout = 5.0
    created = await manager.create(_round_five())
    await manager.start_arm(created.id, OWNER)
    record = manager._records[created.id]
    await asyncio.wait_for(round5.release_started.wait(), timeout=5)
    cleanup_lease = record.round5_lease
    assert cleanup_lease is not None

    # The card is read while its Round 5 release is parked.
    parked = await _read_promptly(manager, created.id)
    if heals:
        assert parked.state == SessionState.CHECKING
        assert not set(_events(record)) & SEALING_EVENTS
        assert record.round5_lease is not None
        assert manager._same_exact_lease(record.round5_lease, cleanup_lease)
        round5.answer()

    failed = await _reach(manager, created.id, SessionState.FAILED)
    assert failed.state == SessionState.FAILED
    assert failed.failure is not None
    assert failed.failure.startswith("The Round 5 start state could not be verified.")
    assert _events(record).count("session_failed") == 1
    receipt = await sealed_receipt(created.id)
    assert receipt is not None
    assert receipt.sealing_event == "session_failed"
    assert await main.current() is None
    assert record.round5_lease_heartbeat_task is None
    if not heals:
        retry = record.round5_release_retry_task
        assert retry is not None and not retry.done()
        assert record.round5_lease is not None
        assert manager._same_exact_lease(record.round5_lease, cleanup_lease)
        round5.answer()
        await asyncio.wait_for(retry, timeout=5)
    async def round5_free() -> bool:
        return await round5.current() is None

    await _until(round5_free)
    await drain_record_operations(manager, record)
    assert record.round5_lease is None
    assert record.round5_release_retry_task is None
    assert round5.released_phases == [cleanup_lease.phase]
    await manager.close()


async def _round5_no_bell_harness() -> tuple[RunManager, NoBellRoundFiveEngine, str, SessionRecord]:
    engine = NoBellRoundFiveEngine()
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        lease_store=InMemoryBoutLeaseStore(),
        round5_lease_store=InMemoryBoutLeaseStore(ring_key="round5"),
    )
    _bounded(manager, deadline=10.0)
    manager._cleanup_retry_initial = 10.0
    session_id, record = await _armed_round_five(manager)
    return manager, engine, session_id, record


async def test_round_five_owner_cancel_reserves_before_starting_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, engine, session_id, record = await _round5_no_bell_harness()
    entered = asyncio.Event()
    allow = asyncio.Event()
    original = manager._abandon_armed_bout

    async def gated_abandon(*args, **kwargs):
        entered.set()
        await allow.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(manager, "_abandon_armed_bout", gated_abandon)
    cancelling: asyncio.Task | None = None
    try:
        cancelling = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert record.armed_release_pending
        assert (await _read_promptly(manager, session_id)).state == SessionState.ARMED
        with pytest.raises(InvalidStateError, match="being released"):
            await manager.start_run(session_id, OWNER)
        assert not engine.run_started.is_set()

        allow.set()
        cancelled = await asyncio.wait_for(cancelling, timeout=5)
        assert cancelled.state == SessionState.FAILED
        assert cancelled.run_started_at is None
        assert _events(record).count("session_failed") == 1
        assert "run_started" not in _events(record)
    finally:
        allow.set()
        if cancelling is not None and not cancelling.done():
            await asyncio.gather(cancelling, return_exceptions=True)
        await manager.close()


@pytest.mark.parametrize("trigger", ["owner-cancel", "expiry"])
@pytest.mark.parametrize("blocked_helper", ["retain", "begin"])
async def test_round_five_no_bell_cleanup_io_does_not_hold_the_card(
    monkeypatch: pytest.MonkeyPatch,
    trigger: str,
    blocked_helper: str,
) -> None:
    manager, engine, session_id, record = await _round5_no_bell_harness()
    entered = asyncio.Event()
    allow = asyncio.Event()
    attribute = (
        "_retain_connection_spike_cleanup_lease"
        if blocked_helper == "retain"
        else "_begin_round5_warm_cleanup"
    )
    original = getattr(manager, attribute)

    async def blocked(*args, **kwargs):
        entered.set()
        await allow.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(manager, attribute, blocked)
    operation: asyncio.Task | None = None
    try:
        if trigger == "owner-cancel":
            operation = asyncio.create_task(manager.cancel_arm(session_id, OWNER))
        else:
            armed_at = record.armed_at_monotonic
            assert armed_at is not None
            manager._cancel_armed_expiry(record, force=True)
            operation = asyncio.create_task(manager._abandon_armed_bout(record, armed_at))
            record.armed_expiry_task = operation

        await asyncio.wait_for(entered.wait(), timeout=5)
        assert record.armed_release_pending
        pending = await _read_promptly(manager, session_id)
        assert pending.state == SessionState.ARMED
        assert not set(_events(record)) & SEALING_EVENTS
        with pytest.raises(InvalidStateError, match="being released"):
            await manager.start_run(session_id, OWNER)
        assert not engine.run_started.is_set()

        allow.set()
        result = await asyncio.wait_for(operation, timeout=5)
        if trigger == "expiry":
            assert result is True
        else:
            assert result.state == SessionState.FAILED
        failed = await manager.get(session_id)
        assert failed.state == SessionState.FAILED
        assert failed.run_started_at is None
        assert _events(record).count("session_failed") == 1
        assert "run_started" not in _events(record)
        assert not record.armed_release_pending
        receipt = await sealed_receipt(session_id)
        assert receipt is not None
        assert receipt.sealing_event == "session_failed"
    finally:
        allow.set()
        if operation is not None and not operation.done():
            await asyncio.gather(operation, return_exceptions=True)
        await manager.close()


async def test_cancelled_round_five_expiry_keeps_cleanup_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _engine, session_id, record = await _round5_no_bell_harness()
    entered = asyncio.Event()
    never = asyncio.Event()
    original = manager._retain_connection_spike_cleanup_lease

    async def blocked_retain(*args, **kwargs):
        entered.set()
        await never.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(manager, "_retain_connection_spike_cleanup_lease", blocked_retain)
    armed_at = record.armed_at_monotonic
    assert armed_at is not None
    manager._cancel_armed_expiry(record, force=True)
    expiry = asyncio.create_task(manager._abandon_armed_bout(record, armed_at))
    record.armed_expiry_task = expiry
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert record.armed_release_pending
        expiry.cancel()
        with pytest.raises(asyncio.CancelledError):
            await expiry

        failed = await manager.get(session_id)
        assert failed.state == SessionState.FAILED
        assert failed.run_started_at is None
        assert failed.round5_setup is not None
        assert failed.round5_setup.cleanup_retryable
        assert _events(record).count("session_failed") == 1
        receipt = await sealed_receipt(session_id)
        assert receipt is not None
        assert receipt.sealing_event == "session_failed"
        assert not record.armed_release_pending
        assert record.armed_expiry_task is None
        retry = record.connection_spike_cleanup_retry_task
        assert retry is not None and not retry.done()
    finally:
        await manager.close()


async def test_close_discovers_and_drains_round_five_no_bell_replacement_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _engine, session_id, record = await _round5_no_bell_harness()
    child_entered = asyncio.Event()
    allow_child = asyncio.Event()
    never = asyncio.Event()
    armed_at = record.armed_at_monotonic
    assert armed_at is not None
    original_mark_pending = manager._mark_connection_spike_cleanup_pending

    async def blocked_mark_pending(*args, **kwargs):
        child_entered.set()
        await allow_child.wait()
        return await original_mark_pending(*args, **kwargs)

    async def parent_that_hands_off_on_cancel() -> None:
        try:
            await never.wait()
        except asyncio.CancelledError:
            child = asyncio.create_task(
                manager._finalize_round5_no_bell_abandon(record, armed_at),
                name=f"round5-cancelled-no-bell-{session_id}",
            )
            record.armed_expiry_task = child
            raise

    monkeypatch.setattr(
        manager,
        "_mark_connection_spike_cleanup_pending",
        blocked_mark_pending,
    )
    manager._cancel_armed_expiry(record, force=True)
    record.armed_release_pending = True
    parent = asyncio.create_task(parent_that_hands_off_on_cancel())
    record.armed_expiry_task = parent
    manager._shutdown_cleanup_timeout = 0.01
    manager._shutdown_settle_timeout = 5

    closing = asyncio.create_task(manager.close())
    try:
        await asyncio.wait_for(child_entered.wait(), timeout=5)
        child = record.armed_expiry_task
        assert child is not None and child is not parent and not child.done()
        assert not closing.done()

        allow_child.set()
        await asyncio.wait_for(closing, timeout=5)
        assert parent.cancelled()
        assert child.done()
        assert record.armed_release_pending is False
        assert record.armed_expiry_task is None
        assert _events(record).count("session_failed") == 1
        receipt = await sealed_receipt(session_id)
        assert receipt is not None
        assert receipt.sealing_event == "session_failed"
    finally:
        allow_child.set()
        if not closing.done():
            await asyncio.gather(closing, return_exceptions=True)
