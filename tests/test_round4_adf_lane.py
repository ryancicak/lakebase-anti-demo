"""Round 4's Azure lane (server/round4_adf_lane.py) through the real Round4RaceEngine.

ADF and the target server are faked. Each run is a batch at one Delta version, and it carries the
source's row once that version holds it, after a cold start, as Phase 0's runs did (4-6 minutes
there, milliseconds here).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import pytest

from server.azure_adf_job import AdfRun
from server.round4_adf_lane import AdfWriterLane
from server.round4_race import COMPETITOR_LANE, LAKEBASE_LANE, Bell, Verdict
from tests.test_round4_race import BASELINE, FakeLane, FakeSource, engine, quiet, update

NOW = datetime(2026, 10, 19, 9, 0, tzinfo=UTC)


class FakeAdfJob:
    """`AdfDataFlowJob`'s verbs over a fake factory."""

    def __init__(self, source: FakeSource, *, delay: float = 0.05, ends: str = "Succeeded",
                 delivers: bool = True) -> None:
        self.source = source
        self.delay = delay
        self.ends = ends
        self.delivers = delivers
        self.runs: list[dict[str, Any]] = []
        self.destination = BASELINE
        self.parks = 0

    def start(self, parameters: dict[str, Any] | None = None) -> str:
        run_id = f"run-{len(self.runs) + 1}"
        self.runs.append({"id": run_id, "version": int((parameters or {})["version"]),
                          "table_id": (parameters or {}).get("table_id"),
                          "at": time.monotonic(), "cancelled": False})
        return run_id

    def _state(self, run: dict[str, Any]) -> str:
        if run["cancelled"]:
            return "Cancelled"
        return self.ends if time.monotonic() - run["at"] >= self.delay else "InProgress"

    def run(self, run_id: str) -> AdfRun:
        run = next(item for item in self.runs if item["id"] == run_id)
        return AdfRun(run_id=run_id, state=self._state(run), started_on=None, completed_on=None)

    def active_runs(self) -> list[AdfRun]:
        return [self.run(item["id"]) for item in self.runs if self._state(item) == "InProgress"]

    def park(self, notify: Any = None) -> list[str]:
        self.parks += 1
        cancelled = []
        for item in self.runs:
            if self._state(item) == "InProgress":
                item["cancelled"] = True
                cancelled.append(item["id"])
        return cancelled

    def read_target(self) -> Any:
        # A run that Succeeded wrote the source's row as of its version: the latest row, when
        # its version reaches the latest.
        for item in self.runs:
            if (self.delivers and self._state(item) == "Succeeded"
                    and item["version"] >= self.source.version):
                self.destination = self.source.row
        return self.destination


class TargetReader:
    def __init__(self, job: FakeAdfJob) -> None:
        self.job = job

    async def read(self, entity_id: str) -> Any:
        return self.job.read_target()

    async def aclose(self) -> None:
        return None


def adf_lane(source: FakeSource, **options: Any) -> tuple[AdfWriterLane, FakeAdfJob]:
    job = FakeAdfJob(source, **options)

    async def open_reader() -> TargetReader:
        return TargetReader(job)

    async def ledger(entity_id: str) -> dict[str, Any] | None:
        done = [item for item in job.runs if job._state(item) == "Succeeded"]
        return None if not done else {"delta_table_id": "table-1", "deleted": False,
                                      "delta_commit_version": done[-1]["version"],
                                      "applied_at": None}

    lane = AdfWriterLane(job, source, open_reader, ledger,  # type: ignore[arg-type]
                         entity_id="customer-0001")
    return lane, job


@pytest.mark.asyncio
async def test_the_bell_starts_one_run_at_the_version_after_prepares_and_it_verifies() -> None:
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    azure, job = adf_lane(source, delay=0.08)
    race = engine(source, lakebase, azure)

    arm = await race.prepare(quiet)
    result = await race.run(arm, update(), quiet)

    assert arm.source_version == 10
    assert [run["version"] for run in job.runs] == [11]  # one createRun, just after Prepare's
    assert job.runs[0]["table_id"] == "table-1"  # the bell's table, as Glue's run argument is
    outcome = result.outcomes[COMPETITOR_LANE]
    assert outcome.verified
    assert outcome.evidence["adf_version"] == 11
    assert outcome.evidence["ledger_version_covers_commit"] is True
    assert result.resolution.verdict is Verdict.WIN and result.resolution.winner == LAKEBASE_LANE


@pytest.mark.asyncio
async def test_a_failed_run_is_the_lanes_failure_with_adfs_words_kept_off_the_card() -> None:
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    azure, _ = adf_lane(source, delay=0.02, ends="Failed")
    race = engine(source, lakebase, azure)

    result = await race.run(await race.prepare(quiet), update(), quiet)

    outcome = result.outcomes[COMPETITOR_LANE]
    assert not outcome.verified and not outcome.timed_out
    assert outcome.failure == "The Data Factory run ended Failed"


@pytest.mark.asyncio
async def test_a_run_that_succeeded_without_the_row_ends_the_lane_rather_than_censor_it() -> None:
    # A batch that ended will never write the row: waiting out the bound would score Azure as a
    # censored lower bound for a run that already said it was done (adversarial review, 2026-10-19).
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    azure, _ = adf_lane(source, delay=0.02, delivers=False)
    race = engine(source, lakebase, azure)

    result = await race.run(await race.prepare(quiet), update(), quiet)

    outcome = result.outcomes[COMPETITOR_LANE]
    assert not outcome.verified and not outcome.timed_out
    assert outcome.failure == "The Data Factory run ended without delivering the row"


@pytest.mark.asyncio
async def test_settling_parks_a_run_pinned_to_an_earlier_version_and_reads_the_head() -> None:
    # The bout's run is still in flight at settle, but it reads the bout's version, never the
    # restore committed after it.
    source = FakeSource()
    azure, job = adf_lane(source, delay=0.05)
    await azure.start(Bell(bout_id="b", rung_at=NOW, source_table_id="t", source_version=9))
    source.version = 12  # the bout's commit, then the restore
    assert await azure.ensure_running(Bell(bout_id="settle", rung_at=NOW,
                                           source_table_id="t")) is True
    assert [run["version"] for run in job.runs] == [10, 12]
    assert job.runs[0]["cancelled"] and job.parks == 1
    # And a run of ours already at the head is kept, with the whole bound.
    assert await azure.ensure_running(Bell(bout_id="settle", rung_at=NOW,
                                           source_table_id="t")) is True
    assert len(job.runs) == 2
    # The table ID reaches the flow inside a quoted literal, so nothing that could close it does.
    with pytest.raises(ValueError, match="not a Delta table ID"):
        await azure.start(Bell(bout_id="b", rung_at=NOW, source_table_id="x'y", source_version=1))


@pytest.mark.asyncio
async def test_settling_a_destination_off_its_baseline_reads_the_head_version() -> None:
    source = FakeSource()
    lakebase = FakeLane(LAKEBASE_LANE, source, delay=0.01)
    azure, job = adf_lane(source, delay=0.02)
    job.destination = update().row  # left behind by an earlier bout
    race = engine(source, lakebase, azure)

    await race.prepare(quiet)

    # The engine knows no version when settling, so the lane read the source's head, off the
    # clock, and then the run was parked with the rest.
    assert [run["version"] for run in job.runs] == [source.version]
    assert job.destination == BASELINE
    assert job.parks >= 1
