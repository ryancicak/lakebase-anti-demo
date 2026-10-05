"""Round 6's DMS change-capture task: first start, resume, and park, against a scripted DMS."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from botocore.exceptions import ClientError

from server.round6_dms import (
    PARKED_CONFIRM_SECONDS,
    START_GRACE_SECONDS,
    TRANSITION_BOUND_SECONDS,
    DmsCaptureTask,
    DmsLaneError,
    DmsTaskState,
)

TASK = "arn:aws:dms:us-west-2:123456789012:task:ABCDEFGHIJKLMNOP"


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class ScriptedDms:
    """Answers describe calls from a script of statuses, and records every verb."""

    def __init__(self, statuses: Iterator[str] | list[str], **fields: str) -> None:
        self._statuses = iter(statuses)
        self._last = "unknown"
        self.fields = fields
        self.calls: list[tuple[str, dict]] = []
        self.stop_error: str | None = None

    def describe_replication_tasks(self, **arguments):
        self.calls.append(("describe", arguments))
        self._last = next(self._statuses, self._last)
        return {
            "ReplicationTasks": [{"ReplicationTaskArn": TASK, "Status": self._last, **self.fields}]
        }

    def start_replication_task(self, **arguments):
        self.calls.append(("start", arguments))
        return {}

    def stop_replication_task(self, **arguments):
        self.calls.append(("stop", arguments))
        if self.stop_error:
            raise ClientError({"Error": {"Code": self.stop_error}}, "StopReplicationTask")
        return {}


class Session:
    def __init__(self, dms: ScriptedDms) -> None:
        self.dms = dms

    def client(self, service, **_options):
        assert service == "dms"
        return self.dms


def task_for(dms: ScriptedDms, clock: Clock | None = None) -> DmsCaptureTask:
    clock = clock or Clock()
    return DmsCaptureTask(Session(dms), TASK, clock=clock, sleep=clock.sleep)


def verbs(dms: ScriptedDms) -> list[str]:
    return [name for name, _ in dms.calls if name != "describe"]


def test_a_task_that_never_ran_starts_replication_which_makes_its_slot():
    dms = ScriptedDms(["ready"])
    kind, _ = task_for(dms).start()
    assert kind == "start-replication"
    start = next(arguments for name, arguments in dms.calls if name == "start")
    assert start == {"ReplicationTaskArn": TASK, "StartReplicationTaskType": "start-replication"}


def test_a_stopped_task_resumes_from_its_slot():
    dms = ScriptedDms(["stopped"])
    kind, _ = task_for(dms).start()
    assert kind == "resume-processing"


def test_a_failed_task_resumes_rather_than_starting_over():
    # Starting over would open a second slot and lose the changes since the last bout.
    dms = ScriptedDms(["failed"])
    assert task_for(dms).start()[0] == "resume-processing"


def test_a_start_waits_out_a_stop_still_in_progress():
    dms = ScriptedDms(["stopping", "stopping", "stopped"])
    assert task_for(dms).start()[0] == "resume-processing"
    assert verbs(dms) == ["start"]


def refuse_start(dms: ScriptedDms, code: str = "InvalidResourceStateFault") -> None:
    def refuse(**arguments):
        dms.calls.append(("start", arguments))
        raise ClientError({"Error": {"Code": code}}, "StartReplicationTask")

    dms.start_replication_task = refuse


def test_a_running_task_is_not_started_twice():
    # Only DMS refusing the start, with the task then read as running, proves it was running.
    dms = ScriptedDms(["running"])
    refuse_start(dms)
    assert task_for(dms).start()[0] == "already-running"
    assert verbs(dms) == ["start"]


def test_a_stale_running_read_at_the_bell_still_starts_the_task():
    # rc14, 2026-10-03: DMS read `running` 12 s after the task stopped, the bell skipped its
    # start, and the lane failed 30 s later on a task that never ran. DMS accepts the start,
    # because the task is in fact stopped.
    dms = ScriptedDms(["running", "stopped"])
    assert task_for(dms).start()[0] == "resume-processing"
    assert verbs(dms) == ["start"]


def test_a_start_refused_mid_transition_is_asked_again_at_rest():
    # The read said stopped, but DMS was still stopping the task, and refused.
    dms = ScriptedDms(["stopped", "stopping", "stopped", "stopped"])
    refused = {"left": 1}
    accept = dms.start_replication_task

    def refuse_once(**arguments):
        if refused["left"]:
            refused["left"] -= 1
            dms.calls.append(("start", arguments))
            raise ClientError(
                {"Error": {"Code": "InvalidResourceStateFault"}}, "StartReplicationTask"
            )
        return accept(**arguments)

    dms.start_replication_task = refuse_once
    assert task_for(dms).start()[0] == "resume-processing"
    assert verbs(dms) == ["start", "start"]


def test_a_refused_start_names_the_task_and_the_kind():
    clock = Clock()
    dms = ScriptedDms(["stopped"])
    refuse_start(dms)
    with pytest.raises(DmsLaneError, match="resume-processing.*InvalidResourceStateFault"):
        task_for(dms, clock).start()
    # Asked again at rest until the bound, which turns a task DMS never starts into a failure.
    assert clock.now >= TRANSITION_BOUND_SECONDS
    assert len(verbs(dms)) > 3
    assert set(verbs(dms)) == {"start"}


def test_a_start_refused_for_any_other_reason_is_not_asked_again():
    dms = ScriptedDms(["stopped"])
    refuse_start(dms, "AccessDeniedFault")
    with pytest.raises(DmsLaneError, match="AccessDeniedFault"):
        task_for(dms).start()
    assert verbs(dms) == ["start"]


def test_wait_running_reports_dms_own_reason_when_the_task_stops_instead():
    dms = ScriptedDms(["starting", "failed"], LastFailureMessage="slot invalidated")
    with pytest.raises(DmsLaneError, match="slot invalidated"):
        task_for(dms).wait_running()


def test_a_stale_stopped_just_after_a_start_is_not_read_as_a_failure():
    # DMS can report the status the task had before the start for a moment.
    dms = ScriptedDms(["stopped", "stopped", "starting", "running"])
    assert task_for(dms).wait_running().running


def test_a_task_that_never_moves_is_a_failure_once_the_grace_passes():
    clock = Clock()
    dms = ScriptedDms(iter(lambda: "stopped", None))
    with pytest.raises(DmsLaneError, match="stopped instead of running"):
        task_for(dms, clock).wait_running()
    assert clock.now >= START_GRACE_SECONDS


def test_park_stops_a_running_task_once_and_waits_for_stopped():
    dms = ScriptedDms(["running", "stopping", "stopping", "stopped"])
    state = task_for(dms).park()
    assert state.status == "stopped"
    assert verbs(dms) == ["stop"]


def test_park_waits_a_starting_task_into_running_before_stopping_it():
    # DMS will not stop a task that is still starting.
    dms = ScriptedDms(["starting", "running", "stopping", "stopped"])
    task_for(dms).park()
    assert verbs(dms) == ["stop"]


def test_park_is_one_read_for_a_parked_task():
    for status in ("stopped", "ready", "failed"):
        dms = ScriptedDms([status])
        task_for(dms).park()
        assert verbs(dms) == []
        assert len(dms.calls) == 1


def test_a_task_that_stopped_between_the_read_and_the_stop_is_parked():
    dms = ScriptedDms(["running", "stopped"])
    dms.stop_error = "InvalidResourceStateFault"
    assert task_for(dms).park().status == "stopped"


def test_a_task_that_will_not_stop_is_a_named_failure_not_a_hang():
    clock = Clock()
    dms = ScriptedDms(iter(lambda: "stopping", None))
    with pytest.raises(DmsLaneError, match="did not stop"):
        task_for(dms, clock).park()
    assert clock.now >= TRANSITION_BOUND_SECONDS


def test_a_missing_task_says_so():
    class Missing(ScriptedDms):
        def describe_replication_tasks(self, **_arguments):
            raise ClientError({"Error": {"Code": "ResourceNotFoundFault"}}, "Describe")

    with pytest.raises(DmsLaneError, match="does not exist"):
        task_for(Missing([])).state()


def test_the_describe_asks_for_this_task_only_and_without_settings():
    dms = ScriptedDms(["stopped"])
    task_for(dms).state()
    _, arguments = dms.calls[0]
    assert arguments == {
        "Filters": [{"Name": "replication-task-arn", "Values": [TASK]}],
        "WithoutSettings": True,
    }


def test_a_task_needs_its_sealed_arn():
    with pytest.raises(DmsLaneError):
        DmsCaptureTask(Session(ScriptedDms([])), "")


# --- rc19, 2026-10-05: the status lags; the task's own dates decide ------------------------------

# rc19's task, as DMS dated it: the last start accepted at 08:13:34, stopped at 08:14:08.
ACCEPTED = datetime(2026, 10, 5, 8, 13, 34, 665000, tzinfo=UTC)
STOPPED = datetime(2026, 10, 5, 8, 14, 8, 50000, tzinfo=UTC)
LATER = datetime(2026, 10, 5, 8, 14, 50, tzinfo=UTC)


def dated(status: str, *, accepted: datetime = ACCEPTED, stopped: datetime | None = STOPPED):
    payload = {"Status": status, "ReplicationTaskStartDate": accepted}
    if stopped is not None:
        payload["ReplicationTaskStats"] = {"StopDate": stopped}
    return payload


class DatedDms(ScriptedDms):
    """Answers each describe with the next scripted task, dates and all."""

    def __init__(self, tasks: list[dict]) -> None:
        super().__init__([])
        self._tasks = iter(tasks)
        self._task: dict = {}

    def describe_replication_tasks(self, **arguments):
        self.calls.append(("describe", arguments))
        self._task = next(self._tasks, self._task)
        return {"ReplicationTasks": [{"ReplicationTaskArn": TASK, **self._task}]}


def test_rc19_a_refused_start_of_a_task_that_last_stopped_is_asked_again_until_dms_accepts():
    # At 08:14:29 DMS refused the bell's start of a task it had stopped at 08:14:08, then read
    # `running`. The old start took that for "already running", and the bout went on with a
    # task that never ran again. By its dates the task last stopped, so it is asked again.
    dms = DatedDms([dated("running"), dated("running"), dated("stopped"), dated("stopped")])
    refusals = {"left": 2}

    def refuse_twice(**arguments):
        dms.calls.append(("start", arguments))
        if refusals["left"]:
            refusals["left"] -= 1
            raise ClientError(
                {"Error": {"Code": "InvalidResourceStateFault"}}, "StartReplicationTask"
            )
        return {}

    dms.start_replication_task = refuse_twice
    assert task_for(dms).start()[0] == "resume-processing"
    assert verbs(dms) == ["start", "start", "start"]


def test_a_task_its_dates_say_is_running_is_already_running():
    dms = DatedDms([dated("running", accepted=LATER, stopped=STOPPED)])
    refuse_start(dms)
    assert task_for(dms).start()[0] == "already-running"
    assert verbs(dms) == ["start"]


def test_park_does_not_take_a_stale_stopped_just_after_a_start_for_parked():
    # The start was accepted after the last stop; the status still says what it was before.
    dms = DatedDms(
        [
            dated("stopped", accepted=LATER, stopped=STOPPED),
            dated("starting", accepted=LATER, stopped=STOPPED),
            dated("running", accepted=LATER, stopped=STOPPED),
            dated("stopping", accepted=LATER, stopped=STOPPED),
            dated("stopped", accepted=LATER, stopped=datetime(2026, 10, 5, 8, 15, 30, tzinfo=UTC)),
        ]
    )
    state = task_for(dms).park()
    assert state.status == "stopped"
    assert verbs(dms) == ["stop"]


def test_park_believes_a_status_at_rest_its_dates_never_catch_up_with():
    # A date DMS never wrote, not a lagging read: the same answer for a full minute.
    clock = Clock()
    dms = DatedDms([dated("stopped", accepted=LATER, stopped=STOPPED)])
    state = task_for(dms, clock).park()
    assert state.status == "stopped"
    assert verbs(dms) == []
    assert PARKED_CONFIRM_SECONDS <= clock.now < TRANSITION_BOUND_SECONDS


@pytest.mark.parametrize(
    ("status", "accepted", "stopped", "parked", "running"),
    [
        # rc19's task after its stop: stopped by its status and its dates.
        ("stopped", ACCEPTED, STOPPED, True, False),
        # And the stale `running` it read 20 s later: not running, since it last stopped.
        ("running", ACCEPTED, STOPPED, False, False),
        # Started after its last stop: a `stopped` read lags the start.
        ("stopped", LATER, STOPPED, False, False),
        ("running", LATER, STOPPED, False, True),
        # Never stopped since its first start.
        ("running", ACCEPTED, None, False, True),
    ],
)
def test_the_dates_decide_parked_and_running(status, accepted, stopped, parked, running):
    state = DmsTaskState.from_api(dated(status, accepted=accepted, stopped=stopped))
    assert state.parked is parked
    assert state.running_since_last_stop is running


def test_without_dates_the_status_decides_as_before():
    for status in ("stopped", "ready", "failed"):
        assert DmsTaskState(status).parked
    assert DmsTaskState("running").running_since_last_stop
    assert not DmsTaskState("starting").parked
