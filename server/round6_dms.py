"""Round 6's AWS lane: start a DMS change-capture task at the bell, and park it after the bout.

The installer builds the tasks (``infra/aws/round6_aws.tf``); this is everything that is done to
them afterwards, by the installer's proof and by the app. Two verbs and one question, on a standing
task, with DMS's own task status as the only record: nothing here keeps state between calls, so a
restarted process asks DMS exactly what it would have remembered.

**The first start is different from every later one.** A task that has never run is ``ready``, and
starting it creates its logical replication slot at the source's current position. From then on
the task is ``stopped`` between bouts, keeps its slot, and resumes from it, so the bell's start
reads every change since the last bout, the bout's own among them
(docs/design/v1.1-rounds-4-6-aws.md, section 4).

**Parked is stopped.** DMS will not stop a task that is still starting, so a park waits a starting
task into ``running`` before it stops it, and waits a stop through ``stopping``. Unlike Glue's run
slot, a stopped task starts again straight away.

**The status is a hint; the dates are the record.** DMS's reported status can lag the task by
twenty seconds and more, in either direction, so whether the task last started or last stopped is
read from its own dates when it reports them: ``ReplicationTaskStartDate``, when the last start was
accepted, and ``ReplicationTaskStats.StopDate``, when it last stopped.

Synchronous, like boto3. The app calls it through ``asyncio.to_thread``.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError

#: A task that has been created and never started. Its first start creates the slot.
NEVER_STARTED = "ready"
#: Statuses a task passes through on its way to running or stopped.
TRANSITIONAL = frozenset({"creating", "starting", "stopping", "modifying", "testing", "moving"})
#: Statuses in which a task is at rest and holds nothing but its slot.
PARKED = frozenset({"ready", "stopped", "failed"})

#: How long a start or a park waits for DMS, and how often it looks. A cold start reached
#: `running` in about 10 s in the spike and a stop took under a minute; the bound is what turns a
#: wedged task into a named failure instead of a hang.
TRANSITION_BOUND_SECONDS = 600.0
TRANSITION_POLL_SECONDS = 2.0
#: How long a task just asked to start may still report the status it had before.
START_GRACE_SECONDS = 30.0
#: How long a park believes a status at rest that the task's dates contradict, read the same the
#: whole time: a date DMS never wrote, not a read that lags (those last seconds, not a minute).
PARKED_CONFIRM_SECONDS = 60.0

_CLIENT_CONFIG = Config(retries={"mode": "standard", "max_attempts": 5})


class DmsLaneError(RuntimeError):
    """A DMS task could not be started, stopped, or read the way Round 6 needs."""


@dataclass(frozen=True)
class DmsTaskState:
    """One task, as DMS reports it."""

    status: str
    stop_reason: str = ""
    failure: str = ""
    #: Where a resume starts from; empty until the task has run once.
    recovery_checkpoint: str = ""
    #: When DMS accepted the task's last start, and when the task last stopped.
    start_accepted_at: datetime | None = None
    stopped_at: datetime | None = None

    @property
    def last_stopped(self) -> bool:
        """Whether the task's last stop came after its last start, by its own dates.

        True without the dates, so a status DMS reports none for still decides on its own.
        """

        if self.start_accepted_at is None or self.stopped_at is None:
            return self.start_accepted_at is None
        return self.stopped_at >= self.start_accepted_at

    @property
    def parked(self) -> bool:
        """At rest: never started, failed, or stopped since its last start.

        rc19 (2026-10-05): a stop was sent at 08:14:07 and DMS dated it 08:14:08, yet ten
        seconds later it was refusing the next bout's start and reading ``running`` again. A
        status at rest right after a start, the status the task had before it, is the same lag
        the other way round, so a read of ``stopped`` counts only when the dates agree.
        """

        if self.status in {"ready", "failed"}:
            return True
        return self.status == "stopped" and self.last_stopped

    @property
    def at_rest_by_status(self) -> bool:
        return self.status in PARKED

    @property
    def running(self) -> bool:
        return self.status == "running"

    @property
    def running_since_last_stop(self) -> bool:
        """Running, by its status and by its dates: its last start came after its last stop."""

        return self.running and (self.stopped_at is None or not self.last_stopped)

    @classmethod
    def from_api(cls, payload: dict[str, Any]) -> DmsTaskState:
        stats = payload.get("ReplicationTaskStats") or {}
        accepted = payload.get("ReplicationTaskStartDate")
        stopped = stats.get("StopDate")
        return cls(
            status=str(payload.get("Status") or "unknown"),
            stop_reason=str(payload.get("StopReason") or ""),
            failure=str(payload.get("LastFailureMessage") or ""),
            recovery_checkpoint=str(payload.get("RecoveryCheckpoint") or ""),
            start_accepted_at=accepted if isinstance(accepted, datetime) else None,
            stopped_at=stopped if isinstance(stopped, datetime) else None,
        )


def _error_code(error: ClientError) -> str:
    return str((error.response.get("Error") or {}).get("Code") or "")


class DmsCaptureTask:
    """One Round 6 change-capture task, addressed by its sealed ARN and nothing else."""

    def __init__(
        self,
        session: Any,
        task_arn: str,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not task_arn:
            raise DmsLaneError("A Round 6 capture task needs its sealed ARN")
        self.task_arn = task_arn
        self._dms = session.client("dms", config=_CLIENT_CONFIG)
        self._clock = clock
        self._sleep = sleep

    def state(self) -> DmsTaskState:
        try:
            response = self._dms.describe_replication_tasks(
                Filters=[{"Name": "replication-task-arn", "Values": [self.task_arn]}],
                WithoutSettings=True,
            )
        except ClientError as error:
            # DMS answers a filter that matches nothing with a fault, not an empty list.
            if _error_code(error) == "ResourceNotFoundFault":
                raise DmsLaneError(f"The DMS task {self.task_arn} does not exist") from error
            raise
        tasks = response.get("ReplicationTasks") or []
        if len(tasks) != 1 or tasks[0].get("ReplicationTaskArn") != self.task_arn:
            raise DmsLaneError(f"DMS did not return exactly the task {self.task_arn}")
        return DmsTaskState.from_api(tasks[0])

    def start(self) -> tuple[str, float]:
        """Start the task, returning how it started and how long DMS took to accept it.

        A task that has never run starts replication, which creates its slot; every other
        resumes from it. A task still stopping is waited out first, because DMS refuses to start
        it. Returns once DMS has accepted the start, not once the task is running: at the bell,
        that wait is on the clock and belongs to DMS.

        A read is never proof that the task is running. On 2026-10-03 (rc14) DMS answered
        ``running`` twelve seconds after it reported the task stopped; the bell skipped its start,
        and the lane waited out its grace on a task that never ran. So the start is always asked
        for, and the task was already running only if DMS refuses the start and the task then
        reads as running since its last stop, by its dates as well as its status.

        rc19 (2026-10-05) needed the dates: DMS refused a bell's start 21 seconds after the task
        had stopped and then read ``running``, and the bout went on with a task that never ran
        again. A refused start of a task that last stopped is asked again until DMS accepts it,
        on the bell's clock, which is DMS's time.
        """

        began = self._clock()
        deadline = began + TRANSITION_BOUND_SECONDS
        while True:
            state = self._settle()
            kind = "start-replication" if state.status == NEVER_STARTED else "resume-processing"
            try:
                self._dms.start_replication_task(
                    ReplicationTaskArn=self.task_arn,
                    StartReplicationTaskType=kind,
                )
                return kind, self._clock() - began
            except ClientError as error:
                refused = DmsLaneError(
                    f"DMS refused to start {self.task_arn} ({kind}): {_error_code(error) or error}"
                )
                if _error_code(error) != "InvalidResourceStateFault":
                    raise refused from error
                if self._settle().running_since_last_stop:
                    return "already-running", self._clock() - began
                if self._clock() >= deadline:
                    raise refused from error
            self._sleep(TRANSITION_POLL_SECONDS)

    def wait_running(self) -> DmsTaskState:
        """Wait for the task to run, failing with DMS's own reason if it stops instead.

        Just after a start is accepted, DMS can still report the status the task had before it,
        so a task at rest counts as having stopped only once it has been seen moving, or once
        ``START_GRACE_SECONDS`` have passed without it moving at all.
        """

        began = self._clock()
        deadline = began + TRANSITION_BOUND_SECONDS
        moved = False
        while True:
            state = self.state()
            if state.running:
                return state
            moved = moved or state.status in TRANSITIONAL
            if state.parked and (moved or self._clock() - began >= START_GRACE_SECONDS):
                raise DmsLaneError(
                    f"The DMS task {self.task_arn} stopped instead of running: "
                    f"{state.failure or state.stop_reason or state.status}"
                )
            if self._clock() >= deadline:
                raise DmsLaneError(
                    f"The DMS task {self.task_arn} did not run within "
                    f"{TRANSITION_BOUND_SECONDS:.0f}s; it is {state.status}"
                )
            self._sleep(TRANSITION_POLL_SECONDS)

    def park(self, notify: Callable[[str], None] | None = None) -> DmsTaskState:
        """Stop the task and wait for it to be stopped. Idempotent: a parked task costs one read.

        Raises if the task is not at rest when the bound passes, because a task that cannot be
        parked keeps reading its source and must say so.
        """

        say = notify or (lambda _message: None)
        deadline = self._clock() + TRANSITION_BOUND_SECONDS
        stop_sent = False
        contradicted_since: float | None = None
        while True:
            state = self.state()
            if state.parked:
                return state
            if state.at_rest_by_status:
                # At rest by its status, started since by its dates: a read that lags a start,
                # which the dates outrank, or a date DMS never wrote. Only the second outlasts
                # a minute of the same answer.
                now = self._clock()
                contradicted_since = now if contradicted_since is None else contradicted_since
                if now - contradicted_since >= PARKED_CONFIRM_SECONDS:
                    return state
            else:
                contradicted_since = None
            if state.running and not stop_sent:
                try:
                    self._dms.stop_replication_task(ReplicationTaskArn=self.task_arn)
                except ClientError as error:
                    # A task that stopped between the read and the stop is what the stop wanted.
                    if _error_code(error) != "InvalidResourceStateFault":
                        raise
                stop_sent = True
                say(f"Stopping the DMS task {self.task_arn}")
            if self._clock() >= deadline:
                raise DmsLaneError(
                    f"The DMS task {self.task_arn} did not stop within "
                    f"{TRANSITION_BOUND_SECONDS:.0f}s; it is {state.status}"
                )
            self._sleep(TRANSITION_POLL_SECONDS)

    def _settle(self) -> DmsTaskState:
        """Wait out a transition, so the task is at rest or running before it is asked to start."""

        deadline = self._clock() + TRANSITION_BOUND_SECONDS
        while True:
            state = self.state()
            if state.status not in TRANSITIONAL:
                return state
            if self._clock() >= deadline:
                raise DmsLaneError(
                    f"The DMS task {self.task_arn} stayed {state.status} for "
                    f"{TRANSITION_BOUND_SECONDS:.0f}s"
                )
            self._sleep(TRANSITION_POLL_SECONDS)
