"""Round 4's Glue writer control: start at the bell, park after, and nothing else.

The rules come from the cold race on 2026-09-29, where Glue refused a start seconds after the
previous run read STOPPED: parked means no active run *and* the release settle has passed, and a
start retries that one refusal within a bound.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta

import pytest
from botocore.exceptions import ClientError

from server import round4_glue
from server.round4_glue import GlueLaneError, GlueWriterJob

NOW = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)


def client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Operation")


class FakeGlue:
    def __init__(self, runs=None, *, refusals=0, start_error=None):
        self.runs = list(runs or [])
        self.refusals = refusals
        self.start_error = start_error
        self.started: list[dict[str, str]] = []
        self.stopped: list[list[str]] = []
        self.stop_errors: list[dict] = []
        #: How many reads a stopped run stays STOPPING before it reads STOPPED.
        self.stopping_reads = 1

    def get_job_runs(self, JobName, MaxResults):  # noqa: N803 - boto3's keyword
        for run in self.runs:
            if run["JobRunState"] == "STOPPING":
                run["_reads"] = run.get("_reads", 0) + 1
                if run["_reads"] > self.stopping_reads:
                    run["JobRunState"] = "STOPPED"
                    run["CompletedOn"] = NOW
        return {"JobRuns": [dict(run) for run in self.runs[:MaxResults]]}

    def get_job_run(self, JobName, RunId):  # noqa: N803
        return {"JobRun": next(dict(run) for run in self.runs if run["Id"] == RunId)}

    def start_job_run(self, JobName, Arguments):  # noqa: N803
        if self.start_error is not None:
            raise self.start_error
        if self.refusals:
            self.refusals -= 1
            raise client_error("ConcurrentRunsExceededException")
        self.started.append(dict(Arguments))
        run_id = f"jr_{len(self.started)}"
        self.runs.insert(0, {"Id": run_id, "JobRunState": "STARTING", "StartedOn": NOW})
        return {"JobRunId": run_id}

    def batch_stop_job_run(self, JobName, JobRunIds):  # noqa: N803
        self.stopped.append(list(JobRunIds))
        for run in self.runs:
            if run["Id"] in JobRunIds:
                run["JobRunState"] = "STOPPING"
        return {"SuccessfulSubmissions": [], "Errors": self.stop_errors}


class FakeS3:
    def __init__(self, objects=None):
        self.objects = objects or {}

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise client_error("NoSuchKey")
        return {"Body": io.BytesIO(json.dumps(self.objects[Key]).encode())}


class FakeSession:
    def __init__(self, glue, s3=None):
        self.glue = glue
        self.s3 = s3 or FakeS3()

    def client(self, name, config=None):
        return {"glue": self.glue, "s3": self.s3}[name]


class Clock:
    def __init__(self):
        self.value = 0.0
        self.slept: list[float] = []

    def __call__(self):
        return self.value

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.value += seconds


def job(glue, *, clock=None, now=NOW, s3=None):
    clock = clock or Clock()
    return GlueWriterJob(
        FakeSession(glue, s3),
        "lakebase-ant-x-r4-writer-rds",
        bucket="lakebase-ant-x-r4-glue",
        clock=clock,
        sleep=clock.sleep,
        now=lambda: now,
    ), clock


def ended(seconds_ago: float, state="STOPPED", run_id="jr_old"):
    return {"Id": run_id, "JobRunState": state, "CompletedOn": NOW - timedelta(seconds=seconds_ago)}


def test_a_job_whose_last_run_ended_long_ago_is_parked():
    writer, _ = job(FakeGlue([ended(600)]))

    state = writer.park_state()

    assert state.parked
    assert state.settle_remaining_seconds == 0


def test_a_job_whose_run_just_stopped_is_not_parked_yet():
    writer, _ = job(FakeGlue([ended(10)]))

    state = writer.park_state()

    assert not state.parked
    assert state.settle_remaining_seconds == pytest.approx(20.0)


def test_a_host_clock_behind_glue_never_waits_more_than_one_settle():
    writer, _ = job(FakeGlue([ended(-15)]))

    assert writer.park_state().settle_remaining_seconds == round4_glue.RELEASE_SETTLE_SECONDS


@pytest.mark.parametrize("state", ["STARTING", "RUNNING", "STOPPING", "WAITING"])
def test_an_active_run_means_not_parked(state):
    writer, _ = job(FakeGlue([{"Id": "jr_1", "JobRunState": state}]))

    assert not writer.park_state().parked


def test_parking_a_parked_job_stops_nothing():
    glue = FakeGlue([ended(600)])
    writer, clock = job(glue)

    writer.park()

    assert glue.stopped == []
    assert clock.slept == []


def test_parking_stops_the_run_waits_for_it_and_then_waits_out_the_settle():
    glue = FakeGlue([{"Id": "jr_1", "JobRunState": "RUNNING", "StartedOn": NOW}])
    writer, clock = job(glue)
    said: list[str] = []

    state = writer.park(said.append)

    assert glue.stopped == [["jr_1"]]
    assert state.active == ()
    # A poll after the stop, one more while it still read STOPPING, then the whole settle,
    # because it ended "now".
    assert clock.slept == [
        round4_glue.STOP_POLL_SECONDS,
        round4_glue.STOP_POLL_SECONDS,
        round4_glue.RELEASE_SETTLE_SECONDS,
    ]
    assert any("release" in line for line in said)


def test_a_run_already_stopping_is_waited_for_not_stopped_again():
    glue = FakeGlue([{"Id": "jr_1", "JobRunState": "STOPPING"}])
    writer, _ = job(glue)

    writer.park()

    assert glue.stopped == []


def test_a_run_that_will_not_stop_fails_the_park_by_name():
    glue = FakeGlue([{"Id": "jr_1", "JobRunState": "RUNNING"}])
    glue.stopping_reads = 10_000
    writer, _ = job(glue)

    with pytest.raises(GlueLaneError, match="did not stop"):
        writer.park()


def test_a_stop_glue_refuses_for_another_reason_is_reported():
    glue = FakeGlue([{"Id": "jr_1", "JobRunState": "RUNNING"}])
    glue.stop_errors = [{"JobRunId": "jr_1", "ErrorDetail": {"ErrorCode": "AccessDenied"}}]
    writer, _ = job(glue)

    with pytest.raises(GlueLaneError, match="AccessDenied"):
        writer.park()


def test_a_start_passes_its_arguments_as_strings():
    glue = FakeGlue()
    writer, _ = job(glue)

    run_id, delayed = writer.start({"--run_tag": "bout-1", "--trigger_seconds": 1})

    assert run_id == "jr_1"
    assert delayed == 0
    assert glue.started == [{"--run_tag": "bout-1", "--trigger_seconds": "1"}]


def test_a_start_retries_only_an_unreleased_slot():
    glue = FakeGlue(refusals=2)
    writer, clock = job(glue)

    run_id, delayed = writer.start({"--run_tag": "bout-1"})

    assert run_id == "jr_1"
    assert clock.slept == [round4_glue.START_RETRY_POLL_SECONDS] * 2
    assert delayed == pytest.approx(2 * round4_glue.START_RETRY_POLL_SECONDS)


def test_a_start_gives_up_on_a_slot_that_is_never_released():
    glue = FakeGlue(refusals=10_000)
    writer, _ = job(glue)

    with pytest.raises(GlueLaneError, match="still holds"):
        writer.start({"--run_tag": "bout-1"})


def test_any_other_start_refusal_is_the_answer():
    glue = FakeGlue(start_error=client_error("AccessDeniedException"))
    writer, clock = job(glue)

    with pytest.raises(ClientError):
        writer.start({"--run_tag": "bout-1"})
    assert clock.slept == []


def test_a_run_marker_is_read_when_there_is_one():
    s3 = FakeS3({"markers/rds/bout-1.json": {"stream_started_at": "2026-09-29T12:00:50+00:00"}})
    writer, _ = job(FakeGlue(), s3=s3)

    assert writer.marker("rds", "bout-1") == {"stream_started_at": "2026-09-29T12:00:50+00:00"}
    assert writer.marker("rds", "bout-2") is None
