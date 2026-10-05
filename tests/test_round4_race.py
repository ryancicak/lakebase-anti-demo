"""Round 4's two-lane cold race: its resolution, its bell and its way back to the baseline.

The lanes here are fakes on real (short) time. Each carries the source's row to its destination
a fixed delay after it is started, which is exactly the shape a cold integration has.
"""

from __future__ import annotations

import asyncio

import pytest

from server.model_score import DeltaCommit, ModelScoreContract, ModelScoreRow, ModelScoreUpdate
from server.round4_race import (
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    LaneOutcome,
    Round4NotArmedError,
    Round4Phase,
    Round4RaceEngine,
    Round4RaceError,
    Round4SettleError,
    Verdict,
    resolve,
)

CONTRACT = ModelScoreContract(
    pipeline_id="pipeline-1",
    source_table="main.anti_demo.model_scores_source",
    synced_table="anti_demo.anti_demo_online.model_scores",
)
BASELINE = CONTRACT.baseline


def outcome(lane_id, *, verified=True, elapsed=None, negative=None, timed_out=False, failure=None):
    return LaneOutcome(
        lane_id=lane_id,
        verified=verified,
        elapsed_ms=elapsed,
        last_negative_ms=negative,
        reads=1,
        max_read_gap_ms=250.0,
        failure=failure,
        timed_out=timed_out,
    )


def test_a_lane_whose_row_arrived_before_the_other_began_its_last_miss_wins_by_the_gap():
    resolution = resolve(
        {
            LAKEBASE_LANE: outcome(LAKEBASE_LANE, elapsed=31_500.0, negative=31_250.0),
            COMPETITOR_LANE: outcome(COMPETITOR_LANE, elapsed=77_800.0, negative=77_550.0),
        }
    )

    assert resolution.verdict is Verdict.WIN
    assert resolution.winner == LAKEBASE_LANE
    assert resolution.margin_ms == pytest.approx(46_300.0)


def test_the_competitor_wins_exactly_as_lakebase_would():
    resolution = resolve(
        {
            LAKEBASE_LANE: outcome(LAKEBASE_LANE, elapsed=9_000.0, negative=8_750.0),
            COMPETITOR_LANE: outcome(COMPETITOR_LANE, elapsed=4_000.0, negative=3_750.0),
        }
    )

    assert resolution.verdict is Verdict.WIN
    assert resolution.winner == COMPETITOR_LANE


def test_arrivals_that_overlap_are_within_resolution_not_a_win():
    resolution = resolve(
        {
            LAKEBASE_LANE: outcome(LAKEBASE_LANE, elapsed=10_200.0, negative=9_950.0),
            COMPETITOR_LANE: outcome(COMPETITOR_LANE, elapsed=10_300.0, negative=10_050.0),
        }
    )

    assert resolution.verdict is Verdict.WITHIN_RESOLUTION
    assert resolution.winner is None
    assert resolution.margin_ms is None


def test_a_first_read_that_is_already_exact_can_still_win():
    # The branch prototype could never award this: no negative read, so no pair to compare.
    resolution = resolve(
        {
            LAKEBASE_LANE: outcome(LAKEBASE_LANE, elapsed=300.0, negative=120.0),
            COMPETITOR_LANE: outcome(COMPETITOR_LANE, elapsed=60_000.0, negative=59_750.0),
        }
    )

    assert resolution.verdict is Verdict.WIN
    assert resolution.winner == LAKEBASE_LANE


def test_a_lane_that_ran_out_its_bound_gives_the_other_a_lower_bound_never_a_margin():
    resolution = resolve(
        {
            LAKEBASE_LANE: outcome(LAKEBASE_LANE, elapsed=31_000.0, negative=30_750.0),
            COMPETITOR_LANE: outcome(
                COMPETITOR_LANE, verified=False, negative=419_750.0, timed_out=True
            ),
        }
    )

    assert resolution.verdict is Verdict.LOWER_BOUND
    assert resolution.winner == LAKEBASE_LANE
    assert resolution.margin_ms is None


def test_a_lane_that_errored_is_not_a_loss():
    # An IAM refusal or a failed Glue run measures nothing, so nobody wins over it.
    resolution = resolve(
        {
            LAKEBASE_LANE: outcome(LAKEBASE_LANE, elapsed=31_000.0, negative=30_750.0),
            COMPETITOR_LANE: outcome(
                COMPETITOR_LANE, verified=False, failure="The Glue run ended FAILED: AccessDenied"
            ),
        }
    )

    assert resolution.verdict is Verdict.NO_RESULT
    assert resolution.winner is None
    assert "AccessDenied" in resolution.detail


def test_nothing_verified_is_no_result():
    resolution = resolve(
        {
            LAKEBASE_LANE: outcome(LAKEBASE_LANE, verified=False),
            COMPETITOR_LANE: outcome(COMPETITOR_LANE, verified=False),
        }
    )

    assert resolution.verdict is Verdict.NO_RESULT


def test_a_lakebase_only_bout_declares_no_winner():
    resolution = resolve({LAKEBASE_LANE: outcome(LAKEBASE_LANE, elapsed=31_000.0, negative=1.0)})

    assert resolution.verdict is Verdict.NO_RESULT


class FakeSource:
    def __init__(self, row=BASELINE, *, commit_delay=0.01, commit_error=None):
        self.row = row
        self.version = 10
        self.commit_delay = commit_delay
        self.commit_error = commit_error
        self.commits: list[ModelScoreUpdate] = []

    async def check_storage(self, entity_id):
        return self.row

    async def read(self, entity_id):
        return self.row

    async def head_version(self):
        return self.version

    async def table_id(self):
        return "table-1"

    async def commit(self, update, *, after_version, on_acknowledged=None):
        await asyncio.sleep(self.commit_delay)
        if self.commit_error is not None:
            raise self.commit_error
        self.version += 1
        self.row = update.row
        self.commits.append(update)
        if on_acknowledged is not None:
            on_acknowledged()
        return DeltaCommit(version=self.version, committed_at=None)  # type: ignore[arg-type]


class FakeReader:
    def __init__(self, lane):
        self.lane = lane
        self.closed = False

    async def read(self, entity_id):
        return self.lane.destination

    async def aclose(self):
        self.closed = True
        self.lane.closed_readers += 1


class FakeLane:
    def __init__(self, lane_id, source, *, delay, start_error=None, failure=None, parked=True):
        self.lane_id = lane_id
        self.label = lane_id.title()
        self.source = source
        self.delay = delay
        self.start_error = start_error
        self.failure_text = failure
        self.parked = parked
        self.destination = BASELINE
        self.carry: asyncio.Task | None = None
        self.started = 0
        self.parks = 0
        self.ensures = 0
        self.closed_readers = 0
        self.opened_readers = 0
        self.bells = []

    async def confirm_parked(self, notify):
        if not self.parked:
            await self.park(notify)

    async def open_reader(self):
        self.opened_readers += 1
        return FakeReader(self)

    async def start(self, bell):
        self.bells.append(bell)
        if self.start_error is not None:
            raise self.start_error
        self.started += 1
        self.parked = False
        self.carry = asyncio.create_task(self._carry())

    async def ensure_running(self, bell):
        self.ensures += 1
        if not self.parked:
            return False
        await self.start(bell)
        return True

    async def _carry(self):
        await asyncio.sleep(self.delay)
        while True:
            self.destination = self.source.row
            await asyncio.sleep(0.005)

    async def failure(self):
        return self.failure_text

    async def park(self, notify):
        self.parks += 1
        if self.carry is not None:
            self.carry.cancel()
            self.carry = None
        self.parked = True

    async def evidence(self, commit, bell):
        return {"lane": self.lane_id}


class StallingLane(FakeLane):
    """Carries the source's row once per start and then stops following it, as Lakebase's
    continuous sync did once on the test installation (gate bout rds-8)."""

    async def _carry(self):
        await asyncio.sleep(self.delay)
        self.destination = self.source.row
        await asyncio.Event().wait()


def engine(source, *lanes, timeout=2.0, kick=60.0):
    return Round4RaceEngine(
        source,
        lanes,
        contract=CONTRACT,
        poll_seconds=0.01,
        lane_timeout_seconds=timeout,
        watch_seconds=0.02,
        settle_timeout_seconds=2.0,
        settle_poll_seconds=0.01,
        carry_kick_seconds=kick,
    )


async def quiet(*_args):
    return None


def update(nonce="round4-v1-" + "a" * 32):
    return ModelScoreUpdate("customer-0001", 0.81, "risk-v1", nonce)


@pytest.mark.asyncio
async def test_a_clean_bout_times_both_lanes_from_one_bell_and_the_faster_wins():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.05)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.25)
    race = engine(source, lakebase, aws)

    arm = await race.prepare(quiet)
    result = await race.run(arm, update(), quiet)

    assert lakebase.started == aws.started == 1
    assert source.commits == [update()]
    fast, slow = result.outcomes[LAKEBASE_LANE], result.outcomes[COMPETITOR_LANE]
    assert fast.verified and slow.verified
    assert fast.elapsed_ms < slow.elapsed_ms
    assert fast.evidence == {"lane": LAKEBASE_LANE}
    assert result.resolution.verdict is Verdict.WIN
    assert result.resolution.winner == LAKEBASE_LANE
    # Every connection the bout opened is closed again.
    assert lakebase.closed_readers == lakebase.opened_readers
    assert aws.closed_readers == aws.opened_readers


@pytest.mark.asyncio
async def test_prepare_starts_nothing_and_parks_a_lane_left_running():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01, parked=False)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.01)
    race = engine(source, lakebase, aws)

    arm = await race.prepare(quiet)

    assert lakebase.started == aws.started == 0
    assert lakebase.parks == 1 and lakebase.parked
    assert arm.source_version == 10
    assert arm.source_table_id == "table-1"


@pytest.mark.asyncio
async def test_prepare_settles_a_destination_left_off_its_baseline_first():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.02)
    aws.destination = ModelScoreRow("customer-0001", 0.81, "risk-v1", "round4-v1-" + "b" * 32)
    race = engine(source, lakebase, aws)

    await race.prepare(quiet)

    assert aws.ensures == 1
    assert aws.destination == BASELINE
    assert aws.parked and lakebase.parked
    # Settling knows nothing about what a destination holds, so the lane starts from the snapshot.
    assert [bell.source_version for bell in aws.bells] == [None]


@pytest.mark.asyncio
async def test_the_bell_tells_every_lane_the_version_prepare_verified():
    # Each lane resumes just after it, as a checkpointed integration would.
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.02)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.04)
    race = engine(source, lakebase, aws)
    arm = await race.prepare(quiet)

    await race.run(arm, update(), quiet)

    assert [bell.source_version for bell in lakebase.bells] == [arm.source_version]
    assert [bell.source_version for bell in aws.bells] == [arm.source_version]


@pytest.mark.asyncio
async def test_prepare_refuses_a_source_row_this_demo_did_not_write():
    source = FakeSource(row=ModelScoreRow("customer-0001", 0.5, "someone-else", "theirs"))
    race = engine(source, FakeLane(LAKEBASE_LANE, source, delay=0.01))

    with pytest.raises(Round4NotArmedError):
        await race.prepare(quiet)


@pytest.mark.asyncio
async def test_a_lane_that_cannot_start_fails_and_the_bout_declares_nothing():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.05)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.05, start_error=RuntimeError("refused"))
    race = engine(source, lakebase, aws)

    result = await race.run(await race.prepare(quiet), update(), quiet)

    assert result.outcomes[LAKEBASE_LANE].verified
    failed = result.outcomes[COMPETITOR_LANE]
    assert not failed.verified and not failed.timed_out
    assert "could not be started: refused" in (failed.failure or "")
    assert result.resolution.verdict is Verdict.NO_RESULT


@pytest.mark.asyncio
async def test_a_lane_whose_integration_fails_stops_waiting_at_once():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.05)
    aws = FakeLane(COMPETITOR_LANE, source, delay=10.0, failure="run FAILED: AccessDenied")
    race = engine(source, lakebase, aws, timeout=5.0)

    result = await asyncio.wait_for(race.run(await race.prepare(quiet), update(), quiet), 2.0)

    assert result.outcomes[COMPETITOR_LANE].failure == "run FAILED: AccessDenied"


@pytest.mark.asyncio
async def test_a_lane_that_never_delivers_fails_by_the_frozen_bound():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.02)
    aws = FakeLane(COMPETITOR_LANE, source, delay=10.0)
    race = engine(source, lakebase, aws, timeout=0.3)

    result = await race.run(await race.prepare(quiet), update(), quiet)

    assert "did not deliver the row within" in (result.outcomes[COMPETITOR_LANE].failure or "")
    assert result.outcomes[COMPETITOR_LANE].timed_out
    # The bound it ran out is its clock's floor; a lane that delivered has none.
    assert result.outcomes[COMPETITOR_LANE].bound_ms == 300.0
    assert result.outcomes[LAKEBASE_LANE].bound_ms is None
    assert result.resolution.verdict is Verdict.LOWER_BOUND


@pytest.mark.asyncio
async def test_a_failed_commit_fails_the_bout_and_closes_every_connection():
    source = FakeSource(commit_error=RuntimeError("DELTA_CONCURRENT_APPEND"))
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.02)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.02)
    race = engine(source, lakebase, aws)

    with pytest.raises(RuntimeError, match="DELTA_CONCURRENT_APPEND"):
        await race.run(await race.prepare(quiet), update(), quiet)

    assert lakebase.closed_readers == lakebase.opened_readers
    assert aws.closed_readers == aws.opened_readers


@pytest.mark.asyncio
async def test_a_prepare_is_rung_once():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    race = engine(source, lakebase)
    arm = await race.prepare(quiet)
    await race.run(arm, update(), quiet)
    await race.settle()

    with pytest.raises(Round4RaceError, match="already been rung"):
        await race.run(arm, update("round4-v1-" + "c" * 32), quiet)


@pytest.mark.asyncio
async def test_settle_restores_the_row_while_the_lanes_run_and_then_parks_them():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.02)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.04)
    race = engine(source, lakebase, aws)
    await race.run(await race.prepare(quiet), update(), quiet)

    await race.settle()

    assert source.row == BASELINE
    assert lakebase.destination == BASELINE and aws.destination == BASELINE
    assert lakebase.parked and aws.parked
    assert source.commits[-1].proof_nonce == BASELINE.proof_nonce


@pytest.mark.asyncio
async def test_a_towel_mid_bout_waits_for_the_commit_on_the_wire_before_restoring():
    source = FakeSource(commit_delay=0.2)
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=5.0)
    aws = FakeLane(COMPETITOR_LANE, source, delay=5.0)
    race = engine(source, lakebase, aws, timeout=10.0)
    arm = await race.prepare(quiet)

    bout = asyncio.create_task(race.run(arm, update(), quiet))
    await asyncio.sleep(0.05)
    bout.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bout
    lakebase.delay = aws.delay = 0.01
    await race.settle()

    # The bout's MERGE landed after the towel; settling restored over it, not under it.
    assert [commit.proof_nonce for commit in source.commits] == [
        update().proof_nonce,
        BASELINE.proof_nonce,
    ]
    assert source.row == BASELINE
    assert lakebase.parked and aws.parked


@pytest.mark.asyncio
async def test_a_running_lane_that_stalls_on_the_restored_row_is_started_again_once():
    source = FakeSource()
    lakebase = StallingLane(LAKEBASE_LANE, source, delay=0.05)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.02)
    race = engine(source, lakebase, aws, kick=0.1)
    await race.run(await race.prepare(quiet), update(), quiet)
    notes: list[str] = []

    async def note(message):
        notes.append(message)

    await race.settle(note)

    assert lakebase.destination == BASELINE and aws.destination == BASELINE
    # Parked once by the kick and once by the settle; started at the bell and by the kick.
    assert lakebase.started == 2 and lakebase.parks == 2 and lakebase.parked
    assert aws.started == 1
    assert any("had not carried the restored row after 0s" in item for item in notes)


@pytest.mark.asyncio
async def test_a_lane_the_carry_starts_from_parked_keeps_its_whole_bound():
    # Its cold start is the wait; restarting it partway would only make that longer.
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.3)
    aws.destination = ModelScoreRow("customer-0001", 0.81, "risk-v1", "round4-v1-" + "b" * 32)
    race = engine(source, lakebase, aws, kick=0.1)

    await race.prepare(quiet)

    assert aws.destination == BASELINE
    assert aws.started == 1 and aws.parks == 1


@pytest.mark.asyncio
async def test_settle_refuses_to_overwrite_someone_else_s_row():
    source = FakeSource(row=ModelScoreRow("customer-0001", 0.5, "someone-else", "theirs"))
    race = engine(source, FakeLane(LAKEBASE_LANE, source, delay=0.01))

    with pytest.raises(Round4SettleError):
        await race.settle()


@pytest.mark.asyncio
async def test_a_lane_that_cannot_park_fails_the_settle_by_name():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.01)

    async def refuse(_notify):
        raise RuntimeError("still billing")

    aws.park = refuse
    race = engine(source, lakebase, aws)

    with pytest.raises(Round4SettleError, match="Competitor: still billing"):
        await race.settle()
    assert lakebase.parked


@pytest.mark.asyncio
async def test_an_observer_that_fails_never_fails_the_bout():
    # A disconnected event stream raises from every progress callback; the bout goes on.
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.02)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.04)
    race = engine(source, lakebase, aws)

    async def gone(*_arguments):
        raise RuntimeError("SSE observer disconnected")

    result = await race.run(await race.prepare(gone), update(), gone)
    await race.settle(gone)

    assert result.resolution.verdict is Verdict.WIN
    assert lakebase.parked and aws.parked


@pytest.mark.asyncio
async def test_a_slow_status_call_never_stretches_that_lane_s_reads():
    # Live, Lakebase's pipeline status took 1.3 s. Asked inline, it opened 1.6 s holes in
    # Lakebase's reads while the AWS lane was read every 250 ms.
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.6)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.6)

    async def slow_status():
        await asyncio.sleep(0.4)
        return None

    lakebase.failure = slow_status
    race = engine(source, lakebase, aws)

    result = await race.run(await race.prepare(quiet), update(), quiet)

    assert result.outcomes[LAKEBASE_LANE].verified
    assert result.outcomes[LAKEBASE_LANE].max_read_gap_ms < 150


@pytest.mark.asyncio
async def test_a_slow_observer_never_delays_a_read_and_still_hears_every_step_in_order():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.05)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.3)
    race = engine(source, lakebase, aws)
    heard: list[tuple[str, Round4Phase]] = []

    async def slow(lane_id, phase, _status, _elapsed_ms):
        await asyncio.sleep(0.2)
        heard.append((lane_id, phase))

    result = await race.run(await race.prepare(quiet), update(), slow)

    # Every step reached the observer before the result did, in order for each lane.
    for lane_id in (LAKEBASE_LANE, COMPETITOR_LANE):
        assert [phase for lane, phase in heard if lane == lane_id] == [
            Round4Phase.STARTING,
            Round4Phase.WAITING,
            Round4Phase.VERIFIED,
        ]
    # Not behind two 200 ms STARTING deliveries, and no read waited on one.
    assert (result.outcomes[LAKEBASE_LANE].elapsed_ms or 0) < 200
    assert max(outcome.max_read_gap_ms for outcome in result.outcomes.values()) < 150


@pytest.mark.asyncio
async def test_a_row_that_landed_before_the_integration_failed_is_still_verified():
    # The integration writes the row and then fails while a read that missed it is in flight.
    # The next read decides, and it finds the row: the lane delivered.
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.02)
    aws = FakeLane(COMPETITOR_LANE, source, delay=10.0)
    watched = asyncio.Event()
    read_in_flight = asyncio.Event()

    async def fails_after_writing():
        watched.set()
        await read_in_flight.wait()
        aws.destination = source.row
        return "run FAILED after its batch"

    class InFlightReader(FakeReader):
        async def read(self, entity_id):
            row = self.lane.destination
            if watched.is_set() and not read_in_flight.is_set():
                read_in_flight.set()
                await asyncio.sleep(0.02)
            return row

    async def open_reader():
        aws.opened_readers += 1
        return InFlightReader(aws)

    aws.failure = fails_after_writing
    aws.open_reader = open_reader
    race = engine(source, lakebase, aws)

    # Bounded: a verifier that waited on the status call inline would never read again.
    result = await asyncio.wait_for(race.run(await race.prepare(quiet), update(), quiet), 2.0)

    delivered = result.outcomes[COMPETITOR_LANE]
    assert delivered.verified and delivered.failure is None


class OrderedLane(FakeLane):
    """Logs when its connection opens, when it is confirmed parked, and when it is read."""

    def __init__(self, *args, log, **kwargs):
        super().__init__(*args, **kwargs)
        self.log = log

    async def confirm_parked(self, notify):
        await asyncio.sleep(0.05)
        self.log.append(("parked", self.lane_id))

    async def open_reader(self):
        self.log.append(("opened", self.lane_id))
        reader = await super().open_reader()
        lane = self

        class LoggingReader:
            async def read(self, entity_id):
                lane.log.append(("read", lane.lane_id))
                return await reader.read(entity_id)

            async def aclose(self):
                await reader.aclose()

        return LoggingReader()


@pytest.mark.asyncio
async def test_prepare_wakes_each_destination_while_the_lanes_are_confirmed_parked():
    """Opening a connection is what wakes a paused Aurora, 12 s of a 23 s Prepare on 2026-09-29.

    So it opens at once, but nothing is read from a destination until its lane is parked.
    """

    log: list[tuple[str, str]] = []
    source = FakeSource()
    lanes = [
        OrderedLane(lane_id, source, delay=0.01, log=log)
        for lane_id in (LAKEBASE_LANE, COMPETITOR_LANE)
    ]
    race = engine(source, *lanes)

    arm = await race.prepare(quiet)

    assert arm.source_version == 10 and arm.source_table_id == "table-1"
    for lane_id in (LAKEBASE_LANE, COMPETITOR_LANE):
        opened = log.index(("opened", lane_id))
        parked = log.index(("parked", lane_id))
        assert opened < parked < log.index(("read", lane_id))
    for lane in lanes:
        assert lane.opened_readers == lane.closed_readers == 1


@pytest.mark.asyncio
async def test_prepare_opens_a_destination_again_when_its_early_connection_fails():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.01)
    attempts = 0
    opened = aws.open_reader

    async def refused_once():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("the database is still waking")
        return await opened()

    aws.open_reader = refused_once
    race = engine(source, lakebase, aws)

    await race.prepare(quiet)

    assert attempts == 2
    assert aws.opened_readers == aws.closed_readers == 1
    assert lakebase.opened_readers == lakebase.closed_readers == 1


@pytest.mark.asyncio
async def test_prepare_closes_its_early_connections_when_it_refuses():
    source = FakeSource(row=ModelScoreRow("customer-0001", 0.5, "someone-else", "theirs"))
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.01)
    race = engine(source, lakebase, aws)

    with pytest.raises(Round4NotArmedError):
        await race.prepare(quiet)

    assert lakebase.opened_readers == lakebase.closed_readers
    assert aws.opened_readers == aws.closed_readers


@pytest.mark.asyncio
async def test_a_wake_opens_and_closes_each_destination_and_survives_one_that_will_not():
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    aws = FakeLane(COMPETITOR_LANE, source, delay=0.01)

    async def refused():
        raise ConnectionError("paused and not answering yet")

    aws.open_reader = refused
    race = engine(source, lakebase, aws)

    await race.wake()

    assert lakebase.opened_readers == lakebase.closed_readers == 1
    # A wake starts nothing: both integrations stay parked, so the race stays cold.
    assert lakebase.started == aws.started == 0
