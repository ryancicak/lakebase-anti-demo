"""One refused AWS start must neither abandon its sibling nor hold its refusal back from the bout.

The bell starts DMS and Glue together. A default ``gather`` raises on the first
refusal and leaves the other start running untracked, so settlement's drain can
finish and park while that start is still on its way to being accepted. Awaiting
both instead must not keep the refusal from the bout: a lane that could not be
started is no result, never a lane that ran out its bound.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from server.round6_race import COMPETITOR_LANE, LAKEBASE_LANE, Verdict, new_bout_orders
from tests.test_round6_race import BASELINE, FakeLane, engine, quiet
from tests.test_round6_race_live import FakeJob, FakeTask, aws_lane


class BlockingDmsStart(FakeTask):
    def __init__(self) -> None:
        super().__init__("stopped")
        self.entered = threading.Event()
        self.release = threading.Event()
        self.accepted = threading.Event()

    def start(self):
        self.calls.append("start")
        self.entered.set()
        self.release.wait(timeout=5)
        self.calls.append("accepted")
        self.accepted.set()
        return ("resume-processing", 0.0)


class RefusingGlueStart(FakeJob):
    def start(self, arguments):
        self.calls.append("start")
        raise RuntimeError("ConcurrentRunsExceededException")


class BlockingGlueStart(FakeJob):
    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()
        self.accepted = threading.Event()

    def start(self, arguments):
        self.calls.append("start")
        self.release.wait(timeout=5)
        self.accepted.set()
        return ("jr_1", 0.0)


class RefusingDmsStart(FakeTask):
    def start(self):
        self.calls.append("start")
        raise RuntimeError("InvalidResourceStateFault")


def racing_aws_lane(task: FakeTask, job: FakeJob) -> FakeLane:
    """AWS's lane in a real bout: scripted, except that it starts and parks as the live lane does,
    through ``task`` and ``job``. Its order never arrives."""

    lane = FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=None)
    live = aws_lane(task, job)
    lane.start = live.start  # type: ignore[method-assign]
    lane.park = live.park  # type: ignore[method-assign]
    return lane


async def test_a_refused_glue_start_waits_for_the_dms_start_before_failing() -> None:
    task, job = BlockingDmsStart(), RefusingGlueStart()
    lane = aws_lane(task, job)
    start = asyncio.ensure_future(lane.start())
    await asyncio.to_thread(task.entered.wait, 5)
    await asyncio.sleep(0.05)
    # Still pending while DMS's start is: settling waits on this start before it parks.
    assert not start.done()
    task.release.set()
    with pytest.raises(RuntimeError, match="ConcurrentRunsExceeded"):
        await asyncio.wait_for(start, timeout=5)
    # So nothing is accepted after the start has answered.
    assert task.accepted.is_set()


async def test_a_refusal_is_reported_at_once_while_the_other_start_is_still_awaited() -> None:
    task, job = BlockingDmsStart(), RefusingGlueStart()
    lane = aws_lane(task, job)
    reported: list[BaseException] = []
    start = asyncio.ensure_future(lane.start(on_refused=reported.append))
    await asyncio.to_thread(task.entered.wait, 5)
    for _ in range(500):
        if reported:
            break
        await asyncio.sleep(0.01)
    assert [str(refusal) for refusal in reported] == ["ConcurrentRunsExceededException"]
    assert not start.done()
    task.release.set()
    with pytest.raises(RuntimeError, match="ConcurrentRunsExceeded"):
        await asyncio.wait_for(start, timeout=5)
    assert len(reported) == 1


async def test_a_refused_dms_start_waits_for_the_glue_start_and_keeps_its_run_id() -> None:
    task, job = RefusingDmsStart(), BlockingGlueStart()
    lane = aws_lane(task, job)
    start = asyncio.ensure_future(lane.start())
    await asyncio.sleep(0.05)
    assert not start.done()
    job.release.set()
    with pytest.raises(RuntimeError, match="InvalidResourceStateFault"):
        await asyncio.wait_for(start, timeout=5)
    assert job.accepted.is_set()
    # The run Glue did accept is kept: the lane's evidence names it.
    assert lane._run_id == "jr_1"


async def test_when_both_starts_are_refused_the_refusal_not_raised_is_logged(caplog) -> None:
    lane = aws_lane(RefusingDmsStart(), RefusingGlueStart())
    with (
        caplog.at_level("WARNING", logger="server.round6_race_live"),
        pytest.raises(RuntimeError) as raised,
    ):
        await lane.start()
    (dropped,) = {"InvalidResourceStateFault", "ConcurrentRunsExceededException"} - {
        str(raised.value)
    }
    logged = [record.getMessage() for record in caplog.records]
    assert any(f"RuntimeError: {dropped}" in line for line in logged), logged


async def test_both_starts_accepted_is_unchanged() -> None:
    task, job = FakeTask(), FakeJob()
    lane = aws_lane(task, job)
    await lane.start()
    assert task.calls == ["start"] and job.calls == ["start"]
    assert lane._run_id == "jr_1"


async def test_a_glue_refusal_is_no_result_though_dms_s_start_outlasts_the_bound() -> None:
    task, job = BlockingDmsStart(), RefusingGlueStart()
    race = engine(
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        racing_aws_lane(task, job),
        lane_timeout_seconds=0.5,
    )
    arm = await race.prepare(quiet)
    result = await race.run(arm, *new_bout_orders(BASELINE), quiet)
    # Scored before DMS answered, so the bound never came into it.
    assert not task.accepted.is_set()
    aws = result.outcomes[COMPETITOR_LANE]
    assert not aws.timed_out and aws.bound_ms is None
    assert aws.failure == "AWS could not be started: ConcurrentRunsExceededException"
    assert result.outcomes[LAKEBASE_LANE].verified
    assert result.resolution.verdict is Verdict.NO_RESULT
    assert result.resolution.winner is None
    task.release.set()
    await asyncio.wait_for(race.settle(), timeout=5)


async def test_settling_parks_only_once_a_start_still_on_the_wire_is_accepted() -> None:
    task, job = BlockingDmsStart(), RefusingGlueStart()
    race = engine(
        FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1),
        racing_aws_lane(task, job),
        lane_timeout_seconds=0.3,
    )
    arm = await race.prepare(quiet)
    await race.run(arm, *new_bout_orders(BASELINE), quiet)
    assert await asyncio.to_thread(task.entered.wait, 5)
    assert not task.accepted.is_set()
    settling = asyncio.create_task(race.settle())
    # Past the bout's bound, and DMS's start is still on its way.
    await asyncio.sleep(0.4)
    assert not settling.done()
    assert task.calls == ["start"] and job.calls == ["start"]
    task.release.set()
    await asyncio.wait_for(settling, timeout=5)
    assert task.calls == ["start", "accepted", "park"]
    assert job.calls == ["start", "park"]
