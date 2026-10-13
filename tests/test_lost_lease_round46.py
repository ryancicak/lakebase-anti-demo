"""Lease loss must schedule Round 4/6 settlement.

Cancelling the run leaves ``_track``ed starts and MERGE/commits running.
Settlement waits those out, restores/deletes residue, and parks the lanes.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from server.coordination import InMemoryBoutLeaseStore, LeaseLostError
from server.manager import InvalidStateError, RunManager
from server.models import (
    BoutOperator,
    CompetitorId,
    Corner,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.round6_race import COMPETITOR_LANE, LAKEBASE_LANE, Round6RaceEngine
from tests.test_manager import (
    BlockingTowelLiveOrdersEngine,
    BlockingTowelModelScoreEngine,
    round_four_request,
    wait_for_state,
)
from tests.test_recovery import build_engine
from tests.test_round4_race import BASELINE as ROUND4_BASELINE
from tests.test_round4_race import FakeLane as Round4FakeLane
from tests.test_round4_race import FakeSource as Round4FakeSource
from tests.test_round4_race import engine as round4_engine
from tests.test_round6_race import BASELINE as ROUND6_BASELINE
from tests.test_round6_race import FakeLane as Round6FakeLane


def round_six_request() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="software_engineer",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.ANALYZE_LIVE_ORDERS,
    )


@pytest.mark.asyncio
async def test_round_four_lost_lease_schedules_settlement():
    engine = BlockingTowelModelScoreEngine()
    manager = RunManager(model_score_factory=lambda _c: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-4")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)
    record = manager._records[created.id]
    assert record.lease is not None
    await manager._handle_lost_lease(record, record.lease)
    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    await asyncio.wait_for(engine.baseline_restored.wait(), timeout=1)
    assert engine.settle_calls == 1
    assert record.settlement_task is None or record.settlement_task.done()


@pytest.mark.asyncio
async def test_round_six_lost_lease_parks_lanes_and_removes_orders():
    engine = BlockingTowelLiveOrdersEngine()
    manager = RunManager(
        live_orders_factory=lambda _c: engine,
        lease_store=InMemoryBoutLeaseStore(),
    )
    operator = BoutOperator(display_name="Round Six Owner", subject="owner-6")
    created = await manager.create(round_six_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=2)
    record = manager._records[created.id]
    assert record.lease is not None
    await manager._handle_lost_lease(record, record.lease)
    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    await asyncio.wait_for(engine.cleaned.wait(), timeout=5)
    assert engine._owned == {}
    for lane in engine.lanes:
        assert lane.events[-1] == "park"
    await manager.close()


@pytest.mark.asyncio
async def test_round_three_lost_lease_cancels_lanes_before_restore():
    """Lease loss must not let Aurora create a recovery after FAILED."""

    engine, _lakebase, aurora, _rds, _timeline = build_engine()
    started = asyncio.Event()
    keep = asyncio.Event()

    async def blocked_wait(plan, recovery_at, report):
        del plan, recovery_at, report
        started.set()
        await keep.wait()

    aurora.wait_recovery_point = blocked_wait  # type: ignore[method-assign]
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=InMemoryBoutLeaseStore(),
    )
    operator = BoutOperator(display_name="Round Three Owner", subject="owner-3")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.RECOVER_DELETED_ORDER,
        )
    )
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(started.wait(), timeout=5)
    assert aurora.create_calls == 0
    record = manager._records[created.id]
    assert record.lease is not None
    run = record.task
    assert run is not None
    await manager._handle_lost_lease(record, record.lease)
    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    assert aurora.create_calls == 0
    keep.set()
    await asyncio.wait_for(asyncio.gather(run, return_exceptions=True), timeout=5)
    assert aurora.create_calls == 0
    assert aurora.abandon_calls == 1
    await manager.close()


@pytest.mark.asyncio
async def test_lost_lease_settlement_blocks_rearm_until_it_finishes():
    engine = BlockingTowelModelScoreEngine()
    hold = asyncio.Event()
    original = engine.settle_and_restore_baseline

    async def blocked_settle() -> None:
        await hold.wait()
        await original()

    engine.settle_and_restore_baseline = blocked_settle  # type: ignore[method-assign]
    manager = RunManager(model_score_factory=lambda _c: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-4")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)
    record = manager._records[created.id]
    assert record.lease is not None
    await manager._handle_lost_lease(record, record.lease)
    settlement = record.settlement_task
    assert settlement is not None and not settlement.done()
    if record.task is not None:
        await asyncio.gather(record.task, return_exceptions=True)
    with pytest.raises(InvalidStateError, match="ACTIVE WORK IS STILL STOPPING"):
        await manager.start_arm(created.id, operator)
    hold.set()
    await asyncio.wait_for(settlement, timeout=5)
    await manager.close()


@pytest.mark.asyncio
async def test_rearm_during_lost_lease_publish_already_sees_settlement():
    """The failure event yields; a re-arm inside that yield must meet the settlement."""

    engine = BlockingTowelModelScoreEngine()
    hold = asyncio.Event()
    original = engine.settle_and_restore_baseline

    async def blocked_settle() -> None:
        await hold.wait()
        await original()

    engine.settle_and_restore_baseline = blocked_settle  # type: ignore[method-assign]
    store = InMemoryBoutLeaseStore()
    manager = RunManager(model_score_factory=lambda _c: engine, lease_store=store)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-4")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)
    record = manager._records[created.id]
    assert record.lease is not None

    publish = record.event_log.publish
    rearm: list[BaseException | object] = []

    async def racing_publish(event, *args, **kwargs):
        if event == "session_failed" and not rearm:
            if record.task is not None:
                await asyncio.gather(record.task, return_exceptions=True)
            # A lost lease is one whose row has gone; the ring is claimable again.
            for scoped in store._scoped_stores.values():
                scoped._lease = None
            try:
                rearm.append(await manager.start_arm(created.id, operator))
            except InvalidStateError as refused:
                rearm.append(refused)
        return await publish(event, *args, **kwargs)

    record.event_log.publish = racing_publish  # type: ignore[method-assign]
    await manager._handle_lost_lease(record, record.lease)
    assert len(rearm) == 1
    assert isinstance(rearm[0], InvalidStateError)
    assert "ACTIVE WORK IS STILL STOPPING" in str(rearm[0])
    hold.set()
    assert record.settlement_task is not None
    await asyncio.wait_for(record.settlement_task, timeout=5)
    await manager.close()


# -- A lease lost on renew while the bell's work is still on the wire ----------------------


class LosesOnRenew(InMemoryBoutLeaseStore):
    """A coordinator that, once ``lose`` is set, answers the bout's renew: the lease is gone."""

    def __init__(self) -> None:
        super().__init__()
        self.lose = False

    async def renew(self, lease, *, ttl):
        if self.lose and lease.phase == "run_committed":
            raise LeaseLostError("RING LEASE EXPIRED")
        return await super().renew(lease, ttl=ttl)


async def _until(predicate) -> None:
    for _ in range(1000):
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition never held")


@dataclass
class HeldCheckoutLane(Round6FakeLane):
    """Round 6's lane whose bout checkout stays on the wire until released, then lands."""

    hold: asyncio.Event = field(default_factory=asyncio.Event)
    holding: asyncio.Event = field(default_factory=asyncio.Event)

    async def commit(self, order):
        if order.status == "checkout":
            self.holding.set()
            await self.hold.wait()
        await super().commit(order)


@pytest.mark.asyncio
async def test_round_six_lease_lost_on_renew_settles_a_checkout_still_on_the_wire():
    lakebase = Round6FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=None)
    aws = HeldCheckoutLane(COMPETITOR_LANE, "AWS", arrives_on_read=None)
    engine = Round6RaceEngine(
        (lakebase, aws),
        baseline=ROUND6_BASELINE,
        poll_seconds=0.005,
        watch_seconds=0.005,
        lane_timeout_seconds=30.0,
    )
    store = LosesOnRenew()
    manager = RunManager(live_orders_factory=lambda _c: engine, lease_store=store)
    manager._lease_heartbeat = 0.02
    operator = BoutOperator(display_name="Round Six Owner", subject="owner-6")
    created = await manager.create(round_six_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(aws.holding.wait(), timeout=5)
    record = manager._records[created.id]
    run = record.task
    assert run is not None

    store.lose = True
    await _until(lambda: record.snapshot.state == SessionState.FAILED)
    settlement = record.settlement_task
    await asyncio.gather(run, return_exceptions=True)
    await asyncio.sleep(0.05)
    # The checkout is still on the wire, so settling has deleted and parked nothing yet.
    assert settlement is not None and not settlement.done()
    for lane in (lakebase, aws):
        assert not [event for event in lane.events if event.startswith("delete:")]
        assert "park" not in lane.events

    aws.hold.set()
    await asyncio.wait_for(asyncio.shield(settlement), timeout=5)
    assert "commit:checkout" in aws.events
    for lane in (lakebase, aws):
        assert set(lane.source) == {ROUND6_BASELINE.order_id}, lane.events
        assert lane.events[-1] == "park", lane.events
    assert engine._owned == {}
    assert record.snapshot.failure == (
        "Ring lease lost; active work was stopped before another bout can begin."
    )
    assert [event.event for event in record.event_log.events].count("session_failed") == 1
    await manager.close()


class HeldMergeSource(Round4FakeSource):
    """Round 4's Delta source, holding the bout's MERGE on the wire until released."""

    def __init__(self) -> None:
        super().__init__()
        self.hold = asyncio.Event()
        self.holding = asyncio.Event()

    async def commit(self, update, *, after_version, on_acknowledged=None):
        if update.proof_nonce != ROUND4_BASELINE.proof_nonce:
            self.holding.set()
            await self.hold.wait()
        return await super().commit(
            update, after_version=after_version, on_acknowledged=on_acknowledged
        )


@pytest.mark.asyncio
async def test_round_four_lease_lost_on_renew_settles_a_merge_still_on_the_wire():
    source = HeldMergeSource()
    lakebase = Round4FakeLane(LAKEBASE_LANE, source, delay=0.01)
    aws = Round4FakeLane(COMPETITOR_LANE, source, delay=0.01)
    engine = round4_engine(source, lakebase, aws, timeout=30.0)
    store = LosesOnRenew()
    manager = RunManager(model_score_factory=lambda _c: engine, lease_store=store)
    manager._lease_heartbeat = 0.02
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-4")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(source.holding.wait(), timeout=5)
    record = manager._records[created.id]
    run = record.task
    assert run is not None
    bout_nonce = record.model_score_pending_update.proof_nonce

    store.lose = True
    await _until(lambda: record.snapshot.state == SessionState.FAILED)
    settlement = record.settlement_task
    await asyncio.gather(run, return_exceptions=True)
    await asyncio.sleep(0.05)
    # The MERGE is still on the wire, so settling has restored and parked nothing yet.
    assert settlement is not None and not settlement.done()
    assert source.commits == []
    assert lakebase.parks == aws.parks == 0

    source.hold.set()
    await asyncio.wait_for(asyncio.shield(settlement), timeout=5)
    assert [commit.proof_nonce for commit in source.commits] == [
        bout_nonce,
        ROUND4_BASELINE.proof_nonce,
    ]
    assert source.row == ROUND4_BASELINE
    assert lakebase.destination == aws.destination == ROUND4_BASELINE
    assert lakebase.parked and aws.parked
    assert record.snapshot.failure == (
        "Round 4's ring lease was lost before the result could be published."
    )
    assert [event.event for event in record.event_log.events].count("session_failed") == 1
    await manager.close()
