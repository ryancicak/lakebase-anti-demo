"""server/azure_adf_job.py against a fake ARM client: one call at the bell, parking cancels and
waits, and a run that won't stop is a named failure."""

from __future__ import annotations

from typing import Any

import pytest

from server.azure_adf_job import AdfDataFlowJob, AdfLaneError, AdfRun


class FakeArm:
    def __init__(self, runs: list[dict[str, str]]) -> None:
        self.runs = runs
        self.calls: list[str] = []

    def pipeline_runs(self, factory: str, pipeline: str, **kwargs: Any) -> list[dict[str, str]]:
        wanted = set(kwargs["statuses"])
        return [run for run in self.runs if run["status"] in wanted]

    def create_pipeline_run(self, factory: str, pipeline: str, parameters: Any) -> str:
        self.calls.append(f"create:{pipeline}")
        return "run-new"

    def cancel_pipeline_run(self, factory: str, run_id: str) -> None:
        self.calls.append(f"cancel:{run_id}")
        for run in self.runs:
            if run["runId"] == run_id:
                run["status"] = "Cancelled"

    def get_pipeline_run(self, factory: str, run_id: str) -> dict[str, str]:
        return next(run for run in self.runs if run["runId"] == run_id)


def job(arm: FakeArm, **kwargs: Any) -> AdfDataFlowJob:
    return AdfDataFlowJob(arm, "adf", "r4_pipeline", sleep=lambda _: None, **kwargs)  # type: ignore[arg-type]


def test_the_bell_is_one_create_run_call() -> None:
    arm = FakeArm([])
    assert job(arm).start() == "run-new"
    assert arm.calls == ["create:r4_pipeline"]


def test_park_cancels_runs_in_flight_and_returns_once_they_end() -> None:
    arm = FakeArm([{"runId": "a", "status": "InProgress"}, {"runId": "b", "status": "Queued"},
                   {"runId": "c", "status": "Succeeded"}])
    assert job(arm).park() == ["a", "b"]
    assert arm.calls == ["cancel:a", "cancel:b"]


def test_a_parked_pipeline_costs_one_query() -> None:
    arm = FakeArm([{"runId": "c", "status": "Succeeded"}])
    assert job(arm).park() == []
    assert arm.calls == []


def test_a_run_that_never_stops_is_a_named_failure() -> None:
    class Stubborn(FakeArm):
        def cancel_pipeline_run(self, factory: str, run_id: str) -> None:
            self.calls.append(f"cancel:{run_id}")  # accepted, never takes effect

    ticks = iter(range(0, 10_000, 100))
    lane = AdfDataFlowJob(Stubborn([{"runId": "a", "status": "InProgress"}]), "adf",  # type: ignore[arg-type]
                          "r4_pipeline", sleep=lambda _: None, clock=lambda: float(next(ticks)))
    with pytest.raises(AdfLaneError, match="still billing"):
        lane.park()


def test_a_run_is_read_from_adfs_own_record() -> None:
    run = AdfRun.from_api({"runId": "r", "status": "Succeeded",
                           "runStart": "2026-10-19T04:03:00.1234567Z",
                           "runEnd": "2026-10-19T04:07:22Z"})
    assert (run.active, run.state) == (False, "Succeeded")
    assert run.started_on is not None and run.completed_on is not None
