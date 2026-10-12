"""A Round 4 bout through the real engine and the real Lakebase lane.

The control plane, the pipeline's status and the synced table are fakes on scaled real time. The
engine, the lane, the power history and the rebase ledger are production code over a fake
coordination table, so these pin what Lakebase's clock contains and what a settled bout leaves in
the rebase ledger.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from server import pipeline_power, round4_race_live
from server.model_score_live import PipelineSignals
from server.round4_race import (
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    Bell,
    Round4NotArmedError,
    Round4RaceEngine,
)
from server.round4_race_live import DeltaSource, LakebaseSyncLane
from tests.test_lifecycle import attach_round4, make_manifest
from tests.test_round4_race import BASELINE, CONTRACT, FakeLane, FakeSource, quiet, update
from tests.test_round4_rebase_owed import _FakeRebaseTable

#: One live second, in real seconds. The verifier's 250 ms read becomes 10 ms and the 3 s status
#: watch and park poll become 120 ms, so the two keep their live ratio.
SCALE = 0.04
#: When a started update first reads healthy, in live seconds after its start request.
HEALTHY_AFTER = 4.0


@pytest.fixture(autouse=True)
def _no_power_store_between_tests():
    pipeline_power.install_pipeline_power_store(None)
    yield
    pipeline_power.install_pipeline_power_store(None)


def _signals(pipeline: str, update: str, table: str, *, continuous: bool, update_id: str):
    return PipelineSignals(
        pipeline_state=pipeline,
        update_state=update,
        synced_table_state=table,
        continuous_reported=continuous,
        update_id=update_id,
    )


class _Pipeline:
    """The sealed pipeline as its control plane reports it, and every request made of it."""

    def __init__(self, timeline: list[str]) -> None:
        self.timeline = timeline
        self.bodies: list[dict | None] = []
        self.stops = 0
        self.update_id = ""
        self.started_at: float | None = None

    def do(self, method: str, path: str, body: dict | None = None) -> dict:
        if method == "POST" and path.endswith("/updates"):
            self.timeline.append("start request")
            self.bodies.append(body)
            self.update_id = f"update-{len(self.bodies)}"
            self.started_at = time.monotonic()
            return {"update_id": self.update_id}
        if method == "POST" and path.endswith("/stop"):
            self.stops += 1
            self.started_at = None
            return {}
        raise AssertionError(f"unexpected control-plane request {method} {path}")

    @property
    def running(self) -> bool:
        return self.started_at is not None

    def live_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        return (time.monotonic() - self.started_at) / SCALE

    def parked(self) -> PipelineSignals:
        return _signals(
            "IDLE",
            "CANCELED" if self.update_id else "",
            "SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
            continuous=False,
            update_id=self.update_id,
        )

    def starting(self) -> PipelineSignals:
        return _signals(
            "RUNNING",
            "INITIALIZING",
            "SYNCED_TABLE_PROVISIONING_INITIAL_SNAPSHOT",
            continuous=False,
            update_id=self.update_id,
        )

    def healthy(self) -> PipelineSignals:
        return _signals(
            "RUNNING",
            "RUNNING",
            "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
            continuous=True,
            update_id=self.update_id,
        )

    async def signals(self) -> PipelineSignals:
        if not self.running:
            return self.parked()
        return self.healthy() if self.live_seconds() >= HEALTHY_AFTER else self.starting()


class _SyncedTable:
    """Lakebase's destination: a running pipeline carries the source's row once it has had time."""

    def __init__(self, pipeline: _Pipeline, source: FakeSource, *, row_after: float) -> None:
        self.pipeline = pipeline
        self.source = source
        self.row_after = row_after
        self.row = BASELINE

    async def open(self) -> _SyncedTable:
        return self

    async def read(self, entity_id: str):
        del entity_id
        if self.pipeline.running and self.pipeline.live_seconds() >= self.row_after:
            self.row = self.source.row
        return self.row

    async def aclose(self) -> None:
        return None


def _manifest():
    manifest = make_manifest()
    attach_round4(manifest)
    return manifest


def _install_ledger(timeline: list[str] | None = None) -> _FakeRebaseTable:
    """The power history and rebase ledger, noting every coordination round trip."""

    table = _FakeRebaseTable()

    async def run(operation):
        if timeline is not None:
            timeline.append("coordination")
        return await table.run(operation)

    pipeline_power.install_pipeline_power_store(pipeline_power.DurablePipelinePowerStore(run))
    return table


def _lakebase(manifest, pipeline: _Pipeline, synced: _SyncedTable) -> LakebaseSyncLane:
    config = SimpleNamespace(pipeline_id=manifest.round4.pipeline_id)

    async def inspect_sync():
        return SimpleNamespace(last_sync_delta_version=None, sync_end_time=None)

    adapter = SimpleNamespace(
        workspace=SimpleNamespace(api_client=pipeline),
        config=config,
        inspect_sync=inspect_sync,
    )
    lane = LakebaseSyncLane(
        manifest,
        adapter,
        DeltaSource(SimpleNamespace(source_repaired=False, config=config)),
        sleep=lambda seconds: asyncio.sleep(seconds * SCALE),
    )
    lane._signals = pipeline.signals  # type: ignore[method-assign]
    lane.open_reader = synced.open  # type: ignore[method-assign]
    return lane


def _engine(source: FakeSource, *lanes, clock_ns=time.monotonic_ns) -> Round4RaceEngine:
    return Round4RaceEngine(
        source,
        lanes,
        contract=CONTRACT,
        poll_seconds=0.25 * SCALE,
        watch_seconds=3.0 * SCALE,
        lane_timeout_seconds=5.0,
        settle_timeout_seconds=5.0,
        settle_poll_seconds=0.25 * SCALE,
        carry_kick_seconds=600.0,
        clock_ns=clock_ns,
    )


def _bell() -> Bell:
    return Bell(bout_id="bout-1", rung_at=datetime.now(UTC), source_table_id="t", source_version=1)


async def _drain_persists() -> None:
    pending = list(round4_race_live._PENDING)
    if pending:
        await asyncio.gather(*pending)


async def test_no_coordination_round_trip_sits_on_lakebase_s_clock() -> None:
    """Lakebase's clock holds its start request and nothing else ahead of it.

    The rebase decision and the start's pair key are coordination round trips, a fresh connection
    each. Made after the bell they delayed Lakebase's start request alone, by more the longer the
    power history grew; AWS's lane makes no such call.
    """

    timeline: list[str] = []
    _install_ledger(timeline)
    manifest = _manifest()
    await pipeline_power.note_rebase_owed(manifest.round4.pipeline_id)
    pipeline = _Pipeline(timeline)
    source = FakeSource()
    lane = _lakebase(manifest, pipeline, _SyncedTable(pipeline, source, row_after=HEALTHY_AFTER))

    def clock_ns() -> int:
        timeline.append("clock")
        return time.monotonic_ns()

    race = _engine(source, lane, clock_ns=clock_ns)
    arm = await race.prepare(quiet)
    timeline.clear()
    result = await race.run(arm, update(), quiet)
    await race.settle()
    await _drain_persists()

    bell = timeline.index("clock")
    start = timeline.index("start request")
    assert bell < start
    assert "coordination" not in timeline[bell:start]
    assert "coordination" in timeline[:bell]
    assert pipeline.bodies[0] == {"full_refresh": True}
    assert result.outcomes[LAKEBASE_LANE].verified


@pytest.mark.parametrize("fault", ["rebase read", "pair reservation"])
async def test_a_ledger_failure_refuses_the_bell_instead_of_voiding_a_rung_bout(fault) -> None:
    table = _install_ledger()
    if fault == "rebase read":
        table.fail_read = True
    else:
        store = pipeline_power.installed_pipeline_power_store()
        assert store is not None
        append = store.append

        async def refuse_reservation(record):
            if record.get("intent") == "generation_reserved":
                raise ConnectionError("coordination write unavailable")
            return await append(record)

        store.append = refuse_reservation  # type: ignore[method-assign]
    manifest = _manifest()
    pipeline = _Pipeline([])
    source = FakeSource()
    lakebase = _lakebase(
        manifest, pipeline, _SyncedTable(pipeline, source, row_after=HEALTHY_AFTER)
    )
    competitor = FakeLane(COMPETITOR_LANE, source, delay=0.01)
    race = _engine(source, lakebase, competitor)
    arm = await race.prepare(quiet)
    heard: list[tuple] = []

    async def progress(*event) -> None:
        heard.append(event)

    with pytest.raises(Round4NotArmedError, match="Lakebase"):
        await race.run(arm, update(), progress)

    assert pipeline.bodies == []
    assert competitor.started == 0
    assert source.commits == []
    assert heard == []
    assert competitor.closed_readers == competitor.opened_readers


@pytest.mark.parametrize(
    "row_after_healthy",
    [0.0, 1.5, 7.0],
    ids=["row-with-first-healthy", "row-1.5s-after", "row-7s-after"],
)
async def test_a_verified_full_refresh_bout_clears_the_rebase_and_the_next_bell_resumes(
    row_after_healthy,
) -> None:
    """The race stops watching Lakebase once its row verifies, which within a watch interval of
    the update first reading healthy is before it has read healthy twice. Settling confirms the
    update before it parks it, so the obligation clears and the next bell does not full-refresh.
    """

    table = _install_ledger()
    manifest = _manifest()
    pipeline_id = manifest.round4.pipeline_id
    await pipeline_power.note_rebase_owed(pipeline_id)
    pipeline = _Pipeline([])
    source = FakeSource()
    synced = _SyncedTable(pipeline, source, row_after=HEALTHY_AFTER + row_after_healthy)

    first = _engine(source, _lakebase(manifest, pipeline, synced))
    result = await first.run(await first.prepare(quiet), update(), quiet)
    await first.settle()
    await _drain_persists()

    assert result.outcomes[LAKEBASE_LANE].verified
    assert pipeline.stops == 1
    assert [row[1] for row in table.rows] == ["rebase_owed", "rebase_satisfied"]
    assert await pipeline_power.rebase_is_owed(pipeline_id) is False

    second = _engine(source, _lakebase(manifest, pipeline, synced))
    await second.run(await second.prepare(quiet), update("round4-v1-" + "b" * 32), quiet)
    await second.settle()
    await _drain_persists()

    assert pipeline.bodies == [{"full_refresh": True}, {"full_refresh": False}]


@pytest.mark.parametrize("ending", ["FAILED", "CANCELED"])
async def test_a_full_refresh_that_ended_before_its_park_owes_the_rebase_again(ending) -> None:
    table = _install_ledger()
    manifest = _manifest()
    pipeline_id = manifest.round4.pipeline_id
    await pipeline_power.note_rebase_owed(pipeline_id)
    pipeline = _Pipeline([])
    lane = _lakebase(manifest, pipeline, _SyncedTable(pipeline, FakeSource(), row_after=0.0))
    await lane.before_bell()
    await lane.start(_bell())

    async def ended() -> PipelineSignals:
        return _signals(
            "IDLE",
            ending,
            "SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
            continuous=False,
            update_id=pipeline.update_id,
        )

    lane._signals = ended  # type: ignore[method-assign]
    await lane.park(quiet)
    await _drain_persists()

    assert [(row[1], row[4]) for row in table.rows] == [
        ("rebase_owed", "source-repair"),
        ("rebase_owed", "full-refresh-failed"),
    ]
    assert await pipeline_power.rebase_is_owed(pipeline_id) is True


@pytest.mark.parametrize("hung", ["satisfied write", "re-owed write"])
async def test_a_ledger_write_that_never_answers_cannot_hold_back_a_park_s_stop(
    hung, monkeypatch
) -> None:
    """Settling a full-refresh bout confirms its update before parking it, and confirming writes
    the rebase ledger. A write the coordination database never answers must not keep Managed Sync
    running: the stop still goes out within the confirmation's bound, and the rebase stays owed.
    """

    monkeypatch.setattr(round4_race_live, "REBASE_CONFIRM_SECONDS", 0.5)
    table = _install_ledger()
    manifest = _manifest()
    pipeline_id = manifest.round4.pipeline_id
    await pipeline_power.note_rebase_owed(pipeline_id)
    pipeline = _Pipeline([])
    source = FakeSource()
    race = _engine(
        source, _lakebase(manifest, pipeline, _SyncedTable(pipeline, source, row_after=0.0))
    )
    result = await race.run(await race.prepare(quiet), update(), quiet)
    assert result.outcomes[LAKEBASE_LANE].verified
    assert pipeline.running

    store = pipeline_power.installed_pipeline_power_store()
    assert store is not None
    asked: list[str] = []
    never = asyncio.Event()

    async def unanswered(*_args, **_kwargs):
        asked.append(hung)
        await never.wait()

    if hung == "satisfied write":
        store.satisfy_rebase = unanswered  # type: ignore[method-assign]
    else:
        # The started update reads CANCELED while its pipeline still runs, so the confirmation
        # owes the rebase again before the park's stop.
        store.append_rebase = unanswered  # type: ignore[method-assign]
        started = pipeline.update_id

        async def canceled() -> PipelineSignals:
            if not pipeline.running:
                return pipeline.parked()
            return _signals(
                "RUNNING",
                "CANCELED",
                "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
                continuous=True,
                update_id=started,
            )

        race.lanes[0]._signals = canceled  # type: ignore[method-assign]

    began = time.monotonic()
    await asyncio.wait_for(race.settle(), timeout=5.0)
    settled_in = time.monotonic() - began
    await _drain_persists()

    assert asked == [hung]
    assert pipeline.stops == 1
    assert not pipeline.running
    assert settled_in < round4_race_live.REBASE_CONFIRM_SECONDS + 1.0
    assert [row[1] for row in table.rows] == ["rebase_owed"]
    assert await pipeline_power.rebase_is_owed(pipeline_id) is True


async def test_one_healthy_reading_before_a_park_does_not_clear_the_rebase(monkeypatch) -> None:
    monkeypatch.setattr(round4_race_live, "REBASE_CONFIRM_SECONDS", 0.5)
    table = _install_ledger()
    manifest = _manifest()
    pipeline_id = manifest.round4.pipeline_id
    await pipeline_power.note_rebase_owed(pipeline_id)
    pipeline = _Pipeline([])
    lane = _lakebase(manifest, pipeline, _SyncedTable(pipeline, FakeSource(), row_after=0.0))
    await lane.before_bell()
    await lane.start(_bell())
    readings = 0

    async def healthy_once() -> PipelineSignals:
        nonlocal readings
        if not pipeline.running:
            return pipeline.parked()
        readings += 1
        return pipeline.healthy() if readings == 1 else pipeline.starting()

    lane._signals = healthy_once  # type: ignore[method-assign]
    await lane.park(quiet)
    await _drain_persists()

    assert readings > 2
    assert pipeline.stops == 1
    assert [row[1] for row in table.rows] == ["rebase_owed"]
    assert await pipeline_power.rebase_is_owed(pipeline_id) is True
