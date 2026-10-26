"""Rounds 4 and 6's Azure lane: start an ADF data flow pipeline at the bell, and park it after.

The Azure counterpart of `server/round4_glue.py`, with the same verbs and the same discipline: a
standing pipeline addressed by its sealed name, and ADF's own run history as the only record.
Nothing here keeps state between calls, so a restarted process asks ADF what it would have
remembered.

**Differences from Glue that Phase 0 measured** (2026-10-18):

* **The bell is one call.** The version a Round 4 run reads (the verified one, plus one) is known
  at Prepare, so the data flow is rewritten then (`server/azure_dataflows.py`), off the clock. At
  the bell the lane makes one ``createRun``, as Glue makes one ``start_job_run``.
* **No run slot to release.** ADF has no counterpart of Glue's ``ConcurrentRunsExceededException``
  after a stop, so parking is "cancel what is in flight and see it end", with no settle.
* **Cold every time.** The managed-VNet runtime's TTL is 0. Five Round 4 cold cycles took
  239-368 s, mostly the cluster starting. That is Azure's time, and the clock includes it.

Synchronous, like the ARM client. The app calls it through ``asyncio.to_thread``.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from .azure_arm import ArmClient

#: Run states in which a run is still going, or about to.
ACTIVE_STATES = frozenset({"Queued", "InProgress", "Canceling"})
TERMINAL_STATES = frozenset({"Succeeded", "Failed", "Cancelled"})
#: How long a park waits for cancelled runs to end, and how often it looks.
PARK_BOUND_SECONDS = 600.0
PARK_POLL_SECONDS = 3.0
#: How far back a park looks for runs still in flight.
HISTORY_WINDOW = timedelta(days=2)


class AdfLaneError(RuntimeError):
    """A data flow pipeline could not be started, cancelled, or read the way the round needs."""


@dataclass(frozen=True)
class AdfRun:
    run_id: str
    state: str
    started_on: datetime | None
    completed_on: datetime | None
    message: str = ""

    @property
    def active(self) -> bool:
        return self.state in ACTIVE_STATES

    @classmethod
    def from_api(cls, payload: Mapping[str, Any]) -> AdfRun:
        return cls(run_id=str(payload.get("runId") or ""),
                   state=str(payload.get("status") or "Unknown"),
                   started_on=_instant(payload.get("runStart")),
                   completed_on=_instant(payload.get("runEnd")),
                   message=str(payload.get("message") or "")[:500])


def _instant(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _utc(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


class AdfDataFlowJob:
    """One Round 4 or Round 6 pipeline, addressed by its sealed factory and pipeline names."""

    def __init__(
        self,
        arm: ArmClient,
        factory: str,
        pipeline: str,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not factory or not pipeline:
            raise AdfLaneError("A data flow lane needs its sealed factory and pipeline names")
        self.factory = factory
        self.pipeline = pipeline
        self._arm = arm
        self._clock = clock
        self._sleep = sleep
        self._now = now

    def active_runs(self) -> list[AdfRun]:
        now = self._now()
        runs = self._arm.pipeline_runs(self.factory, self.pipeline,
                                       since_utc=_utc(now - HISTORY_WINDOW),
                                       until_utc=_utc(now + timedelta(minutes=5)),
                                       statuses=ACTIVE_STATES)
        return [AdfRun.from_api(run) for run in runs]

    def run(self, run_id: str) -> AdfRun:
        return AdfRun.from_api(self._arm.get_pipeline_run(self.factory, run_id))

    def start(self, parameters: Mapping[str, Any] | None = None) -> str:
        """The bell's one call. Returns the run ID."""

        run_id = self._arm.create_pipeline_run(self.factory, self.pipeline, parameters)
        if not run_id:
            raise AdfLaneError(f"ADF started {self.pipeline} but returned no run ID")
        return run_id

    def park(self, notify: Callable[[str], None] | None = None) -> list[str]:
        """Cancel every run still in flight and wait for each to end. Returns those cancelled.

        Idempotent: a parked pipeline costs one query. Raises if a run is still going when the
        bound passes, because a lane that can't be parked is billing and must say so.
        """

        say = notify or (lambda _message: None)
        deadline = self._clock() + PARK_BOUND_SECONDS
        cancelled: list[str] = []
        while True:
            active = self.active_runs()
            if not active:
                return cancelled
            for run in active:
                if run.run_id not in cancelled and run.state != "Canceling":
                    self._arm.cancel_pipeline_run(self.factory, run.run_id)
                    cancelled.append(run.run_id)
                    say(f"Cancelling the ADF run {run.run_id} of {self.pipeline}")
            if self._clock() >= deadline:
                raise AdfLaneError(f"The ADF pipeline {self.pipeline} did not stop within "
                                   f"{PARK_BOUND_SECONDS:.0f}s; it is still billing")
            self._sleep(PARK_POLL_SECONDS)
