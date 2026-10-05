"""Round 6 through the manager: what the card, the verdict and the receipt say about a bout.

The engine is Round 6's real two-lane engine (``server/round6_race.py``) on scripted lanes, so
these pin the manager's half: each lane's own clock on the card, the verdict taken from the
engine's resolution and never recomputed, the one-line result in Round 4's shape, Lakebase alone
where the AWS lane is not installed, and a Prepare that waits for the previous bout to settle.
"""

from __future__ import annotations

import asyncio

import pytest

from server.manager import InvalidStateError, RunManager
from server.models import (
    BoutOperator,
    ComparisonKind,
    CompetitorId,
    Corner,
    LaneState,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.receipts import derive_receipt
from server.round6_race import (
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    PROTOCOL,
    Round6RaceEngine,
)
from tests.test_round6_race import BASELINE, FakeLane


def _engine(*lanes: FakeLane, lane_timeout_seconds: float = 5.0) -> Round6RaceEngine:
    return Round6RaceEngine(
        lanes,
        baseline=BASELINE,
        poll_seconds=0.001,
        watch_seconds=0.001,
        lane_timeout_seconds=lane_timeout_seconds,
    )


def _round_six(competitor: CompetitorId = CompetitorId.AURORA_SERVERLESS_V2) -> SessionCreate:
    return SessionCreate(
        competitor=competitor,
        primary_persona="data_analyst",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.ANALYZE_LIVE_ORDERS,
    )


async def _reach(manager: RunManager, session_id: str, *states: SessionState):
    for _ in range(1000):
        snapshot = await manager.get(session_id)
        if snapshot.state in states:
            return snapshot
        await asyncio.sleep(0.002)
    raise AssertionError(f"session never reached {states}")


async def _bout(manager: RunManager, competitor=CompetitorId.AURORA_SERVERLESS_V2):
    created = await manager.create(_round_six(competitor))
    await manager.start_arm(created.id)
    armed = await _reach(manager, created.id, SessionState.ARMED, SessionState.FAILED)
    assert armed.state == SessionState.ARMED, armed.failure
    await manager.start_run(created.id)
    return await _reach(manager, created.id, SessionState.VERIFIED, SessionState.FAILED)


def _metric(snapshot, spec_id: str, lane_id: str | None = None):
    return next(
        metric
        for metric in snapshot.metrics
        if metric.spec_id == spec_id and metric.lane_id == lane_id
    )


async def test_both_lanes_race_and_the_card_shows_each_clock_and_the_verdict() -> None:
    competitors: list[CompetitorId] = []
    engine = _engine(
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=20),
    )

    def factory(competitor):
        competitors.append(competitor)
        return engine

    manager = RunManager(live_orders_factory=factory)
    finished = await _bout(manager, CompetitorId.RDS_POSTGRES)

    assert competitors == [CompetitorId.RDS_POSTGRES]
    assert finished.state == SessionState.VERIFIED
    lakebase, aws = finished.lanes[LAKEBASE_LANE], finished.lanes[COMPETITOR_LANE]
    assert lakebase.state == aws.state == LaneState.VERIFIED
    assert lakebase.elapsed_ms is not None and aws.elapsed_ms is not None
    assert lakebase.elapsed_ms < aws.elapsed_ms
    # Each lane's clock on the card is its own metric, not a shared figure.
    assert _metric(finished, "bell_to_exact_history_ms", LAKEBASE_LANE).value == lakebase.elapsed_ms
    assert _metric(finished, "bell_to_exact_history_ms", COMPETITOR_LANE).value == aws.elapsed_ms
    for lane_id in (LAKEBASE_LANE, COMPETITOR_LANE):
        assert _metric(finished, "exact_order_verified", lane_id).value is True
        assert _metric(finished, "checkout_verified", lane_id).value is True
    assert _metric(finished, "commit_skew_ms").value >= 0

    assert finished.comparison is not None
    assert finished.comparison.kind == ComparisonKind.MEASURED
    assert finished.comparison.winner_lane_id == LAKEBASE_LANE
    assert "AWS DMS and Glue start cold at the bell" in finished.comparison.detail
    assert finished.remembered_result is not None
    assert finished.remembered_result.startswith("LAKEBASE WINS · MARGIN ")
    assert derive_receipt(finished, "run_finished").remembered_result == (
        finished.remembered_result
    )

    evidence = aws.evidence
    assert evidence["total_display"] == "$84.50"
    assert evidence["protocol"] == PROTOCOL
    assert evidence["proof_nonce"] != BASELINE.proof_nonce
    assert evidence["checkout_guardrail_order_id"] != evidence["order_id"]
    assert evidence["history_reads"] == 20
    await manager.close()


async def test_aws_wins_when_its_order_arrives_first() -> None:
    """The verdict mapping has no side: the same evidence rule crowns AWS."""

    engine = _engine(
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=20),
        FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=1),
    )
    manager = RunManager(live_orders_factory=lambda competitor: engine)
    finished = await _bout(manager)

    assert finished.state == SessionState.VERIFIED
    assert finished.comparison is not None
    assert finished.comparison.winner_lane_id == COMPETITOR_LANE
    assert finished.remembered_result is not None
    assert finished.remembered_result.startswith("AWS WINS · MARGIN ")
    await manager.close()


async def test_lakebase_races_alone_where_the_aws_lane_is_not_installed() -> None:
    engine = _engine(FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=2))
    manager = RunManager(live_orders_factory=lambda competitor: engine)
    created = await manager.create(_round_six())
    await manager.start_arm(created.id)
    armed = await _reach(manager, created.id, SessionState.ARMED)
    competitor = armed.lanes[COMPETITOR_LANE]
    assert competitor.state == LaneState.NOT_SUPPORTED
    assert "not installed" in competitor.evidence["unsupported_reason"]
    await manager.start_run(created.id)
    finished = await _reach(manager, created.id, SessionState.VERIFIED, SessionState.FAILED)

    assert finished.state == SessionState.VERIFIED
    assert finished.lanes[COMPETITOR_LANE].state == LaneState.NOT_SUPPORTED
    assert finished.comparison is not None
    assert finished.comparison.kind == ComparisonKind.CAPABILITY_GAP
    assert finished.remembered_result is not None
    assert finished.remembered_result.startswith("LAKEBASE ")
    assert finished.remembered_result.endswith(" · AWS LANE NOT INSTALLED")
    # One lane has nothing to be skewed against.
    assert not [metric for metric in finished.metrics if metric.spec_id == "commit_skew_ms"]
    await manager.close()


async def test_prepare_decides_the_aws_lane_and_both_clocks_start_at_the_bell() -> None:
    """Nothing before Prepare's answer says the AWS lane is not installed.

    The browser adopts the snapshot the arm call returns. v1.0's Round 6 marked its AWS lane not
    supported there, and the browser held that through Prepare and past the bell: the first try
    of v1.1 showed "NOT INSTALLED" for 30 to 50 seconds while the AWS lane raced.
    """

    engine = _engine(
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=5),
    )
    manager = RunManager(live_orders_factory=lambda competitor: engine)
    created = await manager.create(_round_six())
    arming = await manager.start_arm(created.id)
    assert arming.lanes[COMPETITOR_LANE].state == LaneState.SEALED
    armed = await _reach(manager, created.id, SessionState.ARMED)
    assert armed.lanes[COMPETITOR_LANE].state == LaneState.SEALED
    await manager.start_run(created.id)
    finished = await _reach(manager, created.id, SessionState.VERIFIED, SessionState.FAILED)
    assert finished.state == SessionState.VERIFIED

    events = []
    async with asyncio.timeout(5):
        async for event in manager.events(created.id):
            events.append(event)
            if event.event == "run_finished":
                break
    for event in events:
        session = event.payload.get("session")
        if isinstance(session, dict):
            assert session["lanes"][COMPETITOR_LANE]["state"] != "not_supported", event.event
    lane_updates = [event.payload for event in events if event.event == "lane_update"]
    assert all(update["state"] != LaneState.NOT_SUPPORTED for update in lane_updates)
    # Each lane's first update is its own start, at the bell, on the same clock.
    first: dict[str, dict] = {}
    for update in lane_updates:
        first.setdefault(update["lane_id"], update)
    assert set(first) == {LAKEBASE_LANE, COMPETITOR_LANE}
    for update in first.values():
        assert update["state"] == LaneState.CONNECTING
        assert update["elapsed_ms"] == 0.0
    await manager.close()


async def test_prepare_prices_the_aws_side_it_actually_races() -> None:
    """The receipt built at create cannot know which lanes race; Prepare's arm does."""

    def components(snapshot) -> set[str]:
        assert snapshot.cost_receipt is not None
        return {line.component for line in snapshot.cost_receipt.lines}

    racing = RunManager(
        live_orders_factory=lambda competitor: _engine(
            FakeLane(LAKEBASE_LANE, "Lakebase"), FakeLane(COMPETITOR_LANE, "AWS")
        )
    )
    created = await racing.create(_round_six())
    await racing.start_arm(created.id)
    armed = components(await _reach(racing, created.id, SessionState.ARMED))
    assert any(component.startswith("AWS DMS replication instance") for component in armed)
    assert not any("CDC-to-Delta stack" in component for component in armed)
    await racing.close()

    alone = RunManager(
        live_orders_factory=lambda competitor: _engine(FakeLane(LAKEBASE_LANE, "Lakebase"))
    )
    created = await alone.create(_round_six())
    await alone.start_arm(created.id)
    armed = components(await _reach(alone, created.id, SessionState.ARMED))
    assert not any("DMS" in component for component in armed)
    assert any("CDC-to-Delta stack" in component for component in armed)
    await alone.close()


async def test_a_lane_that_never_delivers_bounds_the_margin_rather_than_measuring_it() -> None:
    engine = _engine(
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=None),
        lane_timeout_seconds=0.05,
    )
    manager = RunManager(live_orders_factory=lambda competitor: engine)
    finished = await _bout(manager)

    assert finished.state == SessionState.VERIFIED
    aws = finished.lanes[COMPETITOR_LANE]
    assert aws.state == LaneState.FAILED
    assert aws.status == "Did not deliver the order within its bound"
    assert aws.elapsed_ms is None
    # Its clock is the floor it ran to, not an error (Ryan, 2026-10-03: without it "it looks
    # like the app crapped out").
    assert aws.error is None
    assert aws.evidence["censored"] is True
    assert aws.evidence["lower_bound_ms"] == 50.0
    assert aws.evidence["display_value"] == ">0.05s"
    assert finished.comparison is not None
    assert finished.comparison.kind == ComparisonKind.ADJUDICATED_STOPPAGE
    assert finished.comparison.winner_lane_id == LAKEBASE_LANE
    assert finished.remembered_result == "LAKEBASE WINS · MARGIN IS A LOWER BOUND"
    receipt = derive_receipt(finished, "run_finished")
    assert receipt.opponent_lane.lower_bound is True
    assert receipt.opponent_lane.ms == 50.0
    assert receipt.margin_ms is None
    await manager.close()


async def test_a_lane_that_fails_is_no_result_never_a_loss() -> None:
    engine = _engine(
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        FakeLane(
            COMPETITOR_LANE,
            "AWS",
            arrives_on_read=None,
            failure_after_start="The Glue run ended FAILED: no error message",
        ),
    )
    manager = RunManager(live_orders_factory=lambda competitor: engine)
    finished = await _bout(manager)

    assert finished.state == SessionState.FAILED
    assert finished.remembered_result is None
    assert finished.comparison is not None
    assert finished.comparison.kind == ComparisonKind.NOT_COMPARABLE
    assert finished.comparison.winner_lane_id is None
    assert "The Glue run ended FAILED" in (finished.failure or "")
    assert finished.lanes[COMPETITOR_LANE].error == ("The Glue run ended FAILED: no error message")
    await manager.close()


async def test_a_towel_before_the_bell_stops_the_bout_with_neither_lane_timed() -> None:
    """Between the press and the engine's bell each source's baseline is read, off every
    clock: a towel there stops the bout, and both lanes are untimed rather than zero.

    It used to be refused with 409 "The live bout clock has not started", because only
    Round 4 was allowed a towel before its bell.
    """

    gate = asyncio.Event()
    gate.set()
    reading = asyncio.Event()

    class HeldAtTheBaseline(FakeLane):
        async def read_source(self, order_id):
            reading.set()
            await gate.wait()
            return await super().read_source(order_id)

    engine = _engine(
        HeldAtTheBaseline(LAKEBASE_LANE, "Lakebase"),
        HeldAtTheBaseline(COMPETITOR_LANE, "AWS"),
    )
    manager = RunManager(live_orders_factory=lambda competitor: engine)
    created = await manager.create(_round_six())
    await manager.start_arm(created.id)
    await _reach(manager, created.id, SessionState.ARMED)
    gate.clear()
    reading.clear()
    await manager.start_run(created.id)
    await asyncio.wait_for(reading.wait(), timeout=2)

    towelled = await manager.start_towel(created.id)

    assert towelled.towel is not None
    assert towelled.towel.censored_lower_bounds_ms == {}
    for lane in towelled.lanes.values():
        assert lane.state == LaneState.TOWELLED
        assert lane.elapsed_ms is None
        assert "not timed" in lane.status.lower()
    gate.set()
    await manager.close()


async def test_the_owner_releases_an_armed_card_to_change_the_matchup() -> None:
    """"Change the matchup" before the bell is every round's now, not only Round 5's.

    The armed window's own release, as the owner's choice: the ring is free at once, and it
    is published as a cancellation, as Round 1's pre-arm cancel is, so the browser returns
    to the card without an error (a `session_failed` would have shown "expired").
    """

    engine = _engine(FakeLane(LAKEBASE_LANE, "Lakebase"), FakeLane(COMPETITOR_LANE, "AWS"))
    manager = RunManager(live_orders_factory=lambda competitor: engine)
    owner = BoutOperator(display_name="Owner", subject="owner")
    intruder = BoutOperator(display_name="Intruder", subject="intruder")
    created = await manager.create(_round_six())
    await manager.start_arm(created.id, owner)
    await _reach(manager, created.id, SessionState.ARMED)

    with pytest.raises(InvalidStateError, match="ONLY THE RING OWNER"):
        await manager.cancel_arm(created.id, intruder)

    cancelled = await manager.cancel_arm(created.id, owner)
    assert cancelled.state == SessionState.FAILED
    assert cancelled.run_started_at is None
    assert cancelled.remembered_result is None
    assert cancelled.failure == (
        "Fight card released by the ring owner before the bell. "
        "No run started and no result was recorded."
    )
    assert (await manager.bout_status(RoundId.ANALYZE_LIVE_ORDERS)).active is False
    events = [event.event for event in manager._records[created.id].event_log.events]
    assert "session_cancelled" in events
    assert "session_failed" not in events
    assert await manager.cancel_arm(created.id, owner) == cancelled

    # The round arms again at once, with no window to wait out.
    again = await manager.create(_round_six())
    await manager.start_arm(again.id, owner)
    armed = await _reach(manager, again.id, SessionState.ARMED, SessionState.FAILED)
    assert armed.state == SessionState.ARMED, armed.failure
    await manager.close()


async def test_the_next_prepare_waits_for_the_previous_bout_to_settle() -> None:
    """A Prepare pressed while the last bout still parks its pipeline waits, visibly."""

    release = asyncio.Event()

    class SlowToSettle(Round6RaceEngine):
        async def settle_and_cleanup_owned(self) -> None:
            await release.wait()
            await super().settle_and_cleanup_owned()

    def build(competitor):
        return SlowToSettle(
            (FakeLane(LAKEBASE_LANE, "Lakebase"), FakeLane(COMPETITOR_LANE, "AWS")),
            baseline=BASELINE,
            poll_seconds=0.001,
            watch_seconds=0.001,
            lane_timeout_seconds=5.0,
        )

    manager = RunManager(live_orders_factory=build)
    first = await _bout(manager)
    assert first.state == SessionState.VERIFIED

    second = await manager.create(_round_six())
    await manager.start_arm(second.id)
    waiting = None
    for _ in range(500):
        waiting = await manager.get(second.id)
        if waiting.lanes[LAKEBASE_LANE].status.startswith("Removing the previous bout"):
            break
        await asyncio.sleep(0.002)
    assert waiting is not None
    assert waiting.state == SessionState.CHECKING
    assert waiting.lanes[LAKEBASE_LANE].status == (
        "Removing the previous bout's orders and parking its pipeline first"
    )

    release.set()
    armed = await _reach(manager, second.id, SessionState.ARMED, SessionState.FAILED)
    assert armed.state == SessionState.ARMED, armed.failure
    await manager.close()
