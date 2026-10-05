"""Round 4's AWS lane: start a Glue writer at the bell, and park it after the bout.

The installer builds the jobs (``infra/aws/round4_glue.tf``); this is everything that is done to
them afterwards, by the installer's proof run and by the app. Two verbs and one question, on a
standing job, with Glue's own run history as the only record: nothing here keeps state between
calls, so a restarted process asks Glue exactly what it would have remembered.

**Parked is more than stopped.** Glue reports a run as ``STOPPED`` before it releases the job's
run slot. In the cold race on 2026-09-29 a start issued seconds after a stop was refused with
``ConcurrentRunsExceededException``. So a job counts as parked only once no run is active *and*
``RELEASE_SETTLE_SECONDS`` have passed since the last one ended, and a start still retries that
one refusal, within a bound, in case a release is slower than the settle. The job's concurrent-run
limit stays at one: two runs would race each other's writes.

Synchronous, like boto3. The app calls it through ``asyncio.to_thread``.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError

#: Run states in which a run holds, or is about to hold, the job's slot.
ACTIVE_STATES = frozenset({"STARTING", "RUNNING", "STOPPING", "WAITING"})
#: Run states a stop can be issued against.
STOPPABLE_STATES = frozenset({"STARTING", "RUNNING", "WAITING"})
#: Every state a run ends in.
TERMINAL_STATES = frozenset({"STOPPED", "SUCCEEDED", "FAILED", "TIMEOUT", "ERROR", "EXPIRED"})

#: The writer's ``--starting_version`` that opens its change feed with the table's snapshot: the
#: job's default, and what every run but a bout's uses (``glue/round4_writer.py``).
FROM_SNAPSHOT = "snapshot"


def starting_version(verified_version: int | None) -> str:
    """Where a run's change feed starts: just after the version its destination holds, if known."""

    return FROM_SNAPSHOT if verified_version is None else str(verified_version + 1)

#: How long after a run ends before its slot is taken as released. Measured: the refusal came
#: seconds after ``STOPPED``, and a start 30 s later was accepted in each of five cycles.
RELEASE_SETTLE_SECONDS = 30.0

#: How long a start keeps retrying Glue's refusal of an unreleased slot, and how often.
START_RETRY_BOUND_SECONDS = 120.0
START_RETRY_POLL_SECONDS = 2.0

#: How long a park waits for its runs to end, and how often it looks. A stop took 20-60 s in
#: the spike; the bound is what turns a wedged stop into a named failure instead of a hang.
STOP_BOUND_SECONDS = 300.0
STOP_POLL_SECONDS = 3.0

_CLIENT_CONFIG = Config(retries={"mode": "standard", "max_attempts": 5})


class GlueLaneError(RuntimeError):
    """A Glue writer could not be started, stopped, or read the way Round 4 needs."""


@dataclass(frozen=True)
class GlueRun:
    """One run of a writer job, as Glue reports it."""

    run_id: str
    state: str
    started_on: datetime | None
    completed_on: datetime | None
    error_message: str = ""
    arguments: Mapping[str, str] | None = None

    @property
    def active(self) -> bool:
        return self.state in ACTIVE_STATES

    @classmethod
    def from_api(cls, payload: Mapping[str, Any]) -> GlueRun:
        return cls(
            run_id=str(payload.get("Id") or ""),
            state=str(payload.get("JobRunState") or "UNKNOWN"),
            started_on=_aware(payload.get("StartedOn")),
            completed_on=_aware(payload.get("CompletedOn")),
            error_message=str(payload.get("ErrorMessage") or ""),
            arguments=dict(payload.get("Arguments") or {}),
        )


@dataclass(frozen=True)
class ParkState:
    """Whether a job is parked now, and if not, what is still in the way."""

    active: tuple[GlueRun, ...]
    #: Seconds left of the release settle after the most recent run ended; zero once passed.
    settle_remaining_seconds: float
    last_run: GlueRun | None

    @property
    def parked(self) -> bool:
        return not self.active and self.settle_remaining_seconds <= 0


def _aware(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _error_code(error: ClientError) -> str:
    return str((error.response.get("Error") or {}).get("Code") or "")


class GlueWriterJob:
    """One Round 4 writer job, addressed by its sealed name and nothing else."""

    def __init__(
        self,
        session: Any,
        job_name: str,
        *,
        bucket: str = "",
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not job_name:
            raise GlueLaneError("A Round 4 writer job needs its sealed name")
        self.job_name = job_name
        self._glue = session.client("glue", config=_CLIENT_CONFIG)
        self._s3 = session.client("s3", config=_CLIENT_CONFIG)
        self._bucket = bucket
        self._clock = clock
        self._sleep = sleep
        self._now = now

    def recent_runs(self, limit: int = 10) -> list[GlueRun]:
        """The job's newest runs, newest first, as Glue orders them."""

        response = self._glue.get_job_runs(JobName=self.job_name, MaxResults=limit)
        return [GlueRun.from_api(item) for item in response.get("JobRuns") or []]

    def run(self, run_id: str) -> GlueRun:
        response = self._glue.get_job_run(JobName=self.job_name, RunId=run_id)
        return GlueRun.from_api(response.get("JobRun") or {})

    def park_state(self) -> ParkState:
        runs = self.recent_runs()
        active = tuple(run for run in runs if run.active)
        ended = [run for run in runs if run.completed_on is not None]
        last = max(ended, key=lambda run: run.completed_on) if ended else None
        remaining = 0.0
        if last is not None and last.completed_on is not None:
            # Glue's clock against this host's: a host a little behind must not wait longer
            # than a whole settle.
            since = (self._now() - last.completed_on).total_seconds()
            remaining = min(RELEASE_SETTLE_SECONDS, max(0.0, RELEASE_SETTLE_SECONDS - since))
        return ParkState(active=active, settle_remaining_seconds=remaining, last_run=last)

    def start(self, arguments: Mapping[str, str]) -> tuple[str, float]:
        """Start one run, returning its ID and how long Glue's refusals delayed it.

        Only ``ConcurrentRunsExceededException`` is retried, and only within
        ``START_RETRY_BOUND_SECONDS``: it is the one refusal a slot still being released
        produces. Every other error is the answer.
        """

        began = self._clock()
        while True:
            try:
                response = self._glue.start_job_run(
                    JobName=self.job_name,
                    Arguments={key: str(value) for key, value in arguments.items()},
                )
            except ClientError as error:
                if _error_code(error) != "ConcurrentRunsExceededException":
                    raise
                if self._clock() - began >= START_RETRY_BOUND_SECONDS:
                    raise GlueLaneError(
                        f"Glue kept refusing to start {self.job_name} for "
                        f"{START_RETRY_BOUND_SECONDS:.0f}s because a previous run still holds "
                        "its slot"
                    ) from error
                self._sleep(START_RETRY_POLL_SECONDS)
                continue
            run_id = str(response.get("JobRunId") or "")
            if not run_id:
                raise GlueLaneError(f"Glue started {self.job_name} but returned no run ID")
            return run_id, self._clock() - began

    def park(self, notify: Callable[[str], None] | None = None) -> ParkState:
        """Stop every active run, wait for each to end, then wait out the release settle.

        Idempotent: a parked job costs one read. Raises if a run is still active when the bound
        passes, because a lane that cannot be parked is billing and must say so.
        """

        say = notify or (lambda _message: None)
        deadline = self._clock() + STOP_BOUND_SECONDS
        stop_sent: set[str] = set()
        while True:
            state = self.park_state()
            if not state.active:
                break
            stoppable = [
                run.run_id
                for run in state.active
                if run.state in STOPPABLE_STATES and run.run_id not in stop_sent
            ]
            if stoppable:
                self._stop(stoppable)
                stop_sent.update(stoppable)
                say(f"Stopping the Glue writer {self.job_name}")
            if self._clock() >= deadline:
                raise GlueLaneError(
                    f"The Glue writer {self.job_name} did not stop within "
                    f"{STOP_BOUND_SECONDS:.0f}s; it is still billing"
                )
            self._sleep(STOP_POLL_SECONDS)
        if state.settle_remaining_seconds > 0:
            say(
                f"Waiting {state.settle_remaining_seconds:.0f}s for Glue to release "
                f"{self.job_name}'s run slot"
            )
            self._sleep(state.settle_remaining_seconds)
            state = self.park_state()
        return state

    def _stop(self, run_ids: Sequence[str]) -> None:
        response = self._glue.batch_stop_job_run(JobName=self.job_name, JobRunIds=list(run_ids))
        for error in response.get("Errors") or []:
            detail = (error.get("ErrorDetail") or {}).get("ErrorCode") or ""
            # A run that ended between the read and the stop is already what the stop wanted.
            if detail not in {"InvalidInputException", "EntityNotFoundException"}:
                raise GlueLaneError(
                    f"Glue refused to stop {self.job_name} run {error.get('JobRunId')}: "
                    f"{detail or 'unknown error'}"
                )

    def marker(self, competitor: str, run_tag: str) -> Mapping[str, Any] | None:
        """What a run recorded about itself (stream start, first batch), if it got that far.

        Evidence only, read after a bout and never on its clock.
        """

        if not self._bucket:
            return None
        try:
            response = self._s3.get_object(
                Bucket=self._bucket,
                Key=f"markers/{competitor}/{run_tag}.json",
            )
        except ClientError as error:
            if _error_code(error) in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise
        payload = json.loads(response["Body"].read())
        return payload if isinstance(payload, dict) else None
