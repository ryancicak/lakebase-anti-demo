"""Unordered Round 4 resuming vs stop_owed appends.

Newest ``event_id`` is still ``latest()`` for other intents. An unmatched
``stop_owed`` is the owed-stop authority even when a same-start ``resuming``
row commits later. A ``stopped`` row, or a later start that is not that pair,
still ends the debt.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server import pipeline_power, round4_race_live
from server.coordination import InMemoryBoutLeaseStore, round_ring_key
from server.models import RoundId
from server.round4_race import Bell, Round4RaceEngine
from server.round4_race_live import DeltaSource, LakebaseSyncLane
from server.round4_stop_recovery import RECOVERY_PHASE, InheritedRound4StopRecovery
from tests.test_lifecycle import attach_round4, make_manifest
from tests.test_pipeline_power import _recording_api
from tests.test_round4_stop_recovery import HEALTHY, INSTALLATION_ID, PROVEN_STOPPED, Clock

PIPELINE_ID = "f2af6c88-7da3-40cf-881f-7971e50a6b18"


def _select_power_history(rows: list[tuple], sql: str, params: tuple) -> list[tuple]:
    matched = [row for row in rows if row[0] == params[0]]
    projected = [(row[1], row[2], row[3], row[4], row[5]) for row in reversed(matched)]
    if "status-snapshot-prefer-real" in sql:
        limits = [int(n) for n in re.findall(r"LIMIT\s+(\d+)", sql)]
        real_limit = limits[0] if limits else 20
        real = [row for row in projected if row[0] != "generation_reserved"][:real_limit]
        if real:
            return real
        fallback = limits[-1] if limits else 1
        return projected[:fallback]
    normalized = " ".join(sql.split())
    if "intent <>" in normalized:
        projected = [row for row in projected if row[0] != "generation_reserved"]
    elif "AND intent = %s" in normalized:
        projected = [row for row in projected if row[0] == params[1]]
    limit = re.search(r"LIMIT\s+(\d+)", sql)
    return projected[: int(limit.group(1))] if limit else projected


class _InvertingPowerTable:
    """Coordination runner whose ``_run`` commits ``stop_owed`` before ``resuming``.

    Each append opens its own connection in production. Concurrent fire-and-forget
    inserts can therefore commit in either order. This runner forces the losing
    order without deadlocking a sequential caller: a lone ``resuming`` waits
    briefly, then commits if no sibling ``stop_owed`` arrives.
    """

    def __init__(self) -> None:
        self.rows: list[tuple] = []
        self._result: list[tuple] = []
        self.commit_order: list[str] = []
        self._stop_owed = asyncio.Event()

    async def execute(self, sql: str, params: tuple = ()) -> None:
        if "round4_source_rebase" in sql:
            self._result = []
            return
        if "INSERT" in sql:
            intent = str(params[1])
            if intent == "resuming":
                try:
                    await asyncio.wait_for(self._stop_owed.wait(), timeout=0.2)
                except TimeoutError:
                    pass
            self.rows.append(tuple(params))
            self.commit_order.append(intent)
            if intent == "stop_owed":
                self._stop_owed.set()
            return
        if "SELECT" in sql:
            self._result = _select_power_history(self.rows, sql, params)

    async def fetchone(self):
        return self._result[0] if self._result else None

    async def fetchall(self):
        return list(self._result)

    async def run(self, operation):
        return await operation(self)


class _StopOwedAfterStoppedTable(_InvertingPowerTable):
    """``stop_owed`` waits until ``stopped`` commits, modelling a delayed persist."""

    def __init__(self) -> None:
        super().__init__()
        self._stopped = asyncio.Event()

    async def execute(self, sql: str, params: tuple = ()) -> None:
        if "round4_source_rebase" in sql:
            self._result = []
            return
        if "INSERT" in sql:
            intent = str(params[1])
            if intent == "stop_owed":
                await self._stopped.wait()
            self.rows.append(tuple(params))
            self.commit_order.append(intent)
            if intent == "stopped":
                self._stopped.set()
            if intent == "stop_owed":
                self._stop_owed.set()
            return
        if "SELECT" in sql:
            self._result = _select_power_history(self.rows, sql, params)


class _OlderStopOwedCommitsLastTable(_InvertingPowerTable):
    """The first bout's ``stop_owed`` stays in flight until a later ``stopped``."""

    def __init__(self) -> None:
        super().__init__()
        self._first_owed_held = False
        self._release_first_owed = asyncio.Event()

    async def execute(self, sql: str, params: tuple = ()) -> None:
        if "round4_source_rebase" in sql:
            self._result = []
            return
        if "INSERT" in sql:
            intent = str(params[1])
            if intent == "stop_owed" and not self._first_owed_held:
                self._first_owed_held = True
                await self._release_first_owed.wait()
            if intent == "resuming":
                try:
                    await asyncio.wait_for(self._stop_owed.wait(), timeout=0.2)
                except TimeoutError:
                    pass
            self.rows.append(tuple(params))
            self.commit_order.append(intent)
            if intent == "stop_owed":
                self._stop_owed.set()
            if intent == "stopped":
                self._release_first_owed.set()
            return
        if "SELECT" in sql:
            self._result = _select_power_history(self.rows, sql, params)


class _StoppedCommitsLastTable(_InvertingPowerTable):
    """The first ``stopped`` row waits until a later start's ``resuming`` commits."""

    def __init__(self) -> None:
        super().__init__()
        self._holding_stopped = False
        self._release_stopped = asyncio.Event()

    async def execute(self, sql: str, params: tuple = ()) -> None:
        if "round4_source_rebase" in sql:
            self._result = []
            return
        if "INSERT" in sql:
            intent = str(params[1])
            if intent == "stopped" and not self._holding_stopped:
                self._holding_stopped = True
                await self._release_stopped.wait()
            if intent == "resuming":
                try:
                    await asyncio.wait_for(self._stop_owed.wait(), timeout=0.2)
                except TimeoutError:
                    pass
            self.rows.append(tuple(params))
            self.commit_order.append(intent)
            if intent == "stop_owed":
                self._stop_owed.set()
            if intent == "resuming":
                self._release_stopped.set()
            return
        if "SELECT" in sql:
            self._result = _select_power_history(self.rows, sql, params)


@pytest.fixture(autouse=True)
def _reset_pipeline_power_store():
    pipeline_power.install_pipeline_power_store(None)
    yield
    pipeline_power.install_pipeline_power_store(None)


def _manifest():
    manifest = make_manifest()
    attach_round4(manifest)
    return manifest


def _lane(manifest) -> LakebaseSyncLane:
    lane = object.__new__(LakebaseSyncLane)
    lane._manifest = manifest
    lane._adapter = SimpleNamespace(workspace=None, config=SimpleNamespace(pipeline_id=PIPELINE_ID))
    lane._source = DeltaSource(SimpleNamespace(source_repaired=False, config=lane._adapter.config))
    lane._api = _recording_api([])
    lane._pipeline_id = PIPELINE_ID
    lane._sleep = asyncio.sleep
    lane._now = lambda: datetime(2026, 10, 9, 19, 0, tzinfo=UTC)
    lane._started_update = ""
    lane._started_full_refresh = False
    lane._rebase_event_id = None
    lane._rebase_healthy_streak = 0
    lane._power_pair_key = ""
    lane._reserved = None
    return lane


def _bell() -> Bell:
    return Bell(
        bout_id="bout-power-order",
        rung_at=datetime(2026, 10, 9, 19, 0, tzinfo=UTC),
        source_table_id="t",
        source_version=1,
    )


async def _start(lane: LakebaseSyncLane) -> None:
    """What the engine does for this lane at a bell: record its start, then make it."""

    await lane.before_bell()
    await lane.start(_bell())


async def _drain_persists() -> None:
    pending = list(round4_race_live._PENDING)
    if pending:
        await asyncio.gather(*pending)


async def _park_now(lane: LakebaseSyncLane, *, drain: bool = True) -> None:
    sequence = [HEALTHY, PROVEN_STOPPED]

    async def signals():
        return sequence.pop(0) if len(sequence) > 1 else sequence[0]

    async def no_wait(_seconds: float) -> None:
        await asyncio.sleep(0)

    async def quiet(_message: str) -> None:
        return None

    lane._signals = signals
    lane._sleep = no_wait
    await lane.park(quiet)
    if drain:
        await _drain_persists()


def _stopped_run_ids(table: _InvertingPowerTable) -> list[str]:
    return [str(row[2]) for row in table.rows if str(row[1]) == "stopped"]


def _start_pair_keys(table: _InvertingPowerTable) -> list[str]:
    return [str(row[2]) for row in table.rows if str(row[1]) == "resuming"]


def _recovery_manifest():
    return SimpleNamespace(
        manifest_version=7,
        installation_id=INSTALLATION_ID,
        run_id="run-1",
        owner="owner@example.com",
        round4=SimpleNamespace(
            pipeline_id=PIPELINE_ID,
            synced_table_id="storage.round4.model_scores",
            app_service_principal_client_id="app-client",
        ),
        databricks=SimpleNamespace(profile="demo", user="app-client"),
    )


def _sql_row(intent: str, run_id: str, minute: int = 0):
    return (intent, run_id, datetime(2026, 10, 9, 19, minute, tzinfo=UTC), "actor", "14.57")


def _power_record(intent: str, run_id: str, at: datetime) -> dict[str, str]:
    stamp = at.astimezone(UTC).isoformat()
    if intent == "stop_owed":
        return {
            "intent": intent,
            "pipeline_id": PIPELINE_ID,
            "run_id": run_id,
            "owed_at": stamp,
            "owed_by": "app-client",
            "usd_per_day_owed": "14.57",
        }
    if intent == "resuming":
        return {
            "intent": intent,
            "pipeline_id": PIPELINE_ID,
            "run_id": run_id,
            "resumed_at": stamp,
            "resumed_by": "app-client",
            "usd_per_day_resumed": "14.57",
        }
    if intent == "generation_reserved":
        return {
            "intent": intent,
            "pipeline_id": PIPELINE_ID,
            "run_id": run_id,
            "reserved_at": stamp,
            "reserved_by": "round4-start-generation",
        }
    return {
        "intent": intent,
        "pipeline_id": PIPELINE_ID,
        "run_id": run_id,
        "stopped_at": stamp,
        "stopped_by": "app-client",
        "usd_per_day_saved": "14.57",
    }


async def _run_owed_recovery(store, snapshot, clock: Clock):
    root = InMemoryBoutLeaseStore(clock=clock.now)
    ring = root.for_ring_key(round_ring_key(INSTALLATION_ID, RoundId.PUT_MODEL_SCORE_IN_APP.value))
    stops: list[datetime] = []

    async def stop_pipeline():
        held = await ring.current()
        assert held is not None and held.phase == RECOVERY_PHASE
        stops.append(clock.now())
        return pipeline_power.stopped_record(_recovery_manifest(), now=clock.now)

    recovery = InheritedRound4StopRecovery(
        _recovery_manifest(),
        ring,
        store,
        snapshot,
        read_signals=lambda: asyncio.sleep(0, result=HEALTHY),
        stop_pipeline=stop_pipeline,
        now=clock.now,
        sleep=clock.sleep,
        retry_base_seconds=0,
        retry_max_seconds=0.01,
    )
    await asyncio.wait_for(recovery.run(), timeout=5)
    return recovery, stops, ring


@pytest.mark.asyncio
async def test_an_inverted_append_does_not_hide_the_owed_stop_from_start():
    """The fake ``_run`` commits stop_owed before resuming; the start still owes its stop."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()

    await _start(_lane(manifest))
    await _drain_persists()

    assert table.commit_order[:3] == ["generation_reserved", "stop_owed", "resuming"]
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) == "resuming"
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert pipeline_power._intent(snapshot) == "stop_owed"


@pytest.mark.asyncio
async def test_an_inverted_latest_still_builds_inherited_recovery():
    """When resuming is newest, startup still has an owed-stop snapshot."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _start(_lane(manifest))
    await _drain_persists()

    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) == "resuming"
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert pipeline_power._intent(snapshot) == "stop_owed"
    due = pipeline_power._parse_stamp(snapshot["owed_at"])
    assert due is not None
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, ring = await _run_owed_recovery(store, snapshot, clock)
    assert len(stops) == 1
    assert table.commit_order[-1] == "stopped"
    assert recovery.status.state == "settled"
    assert pipeline_power.owed_stop_notice(now=clock.now) is None
    assert await ring.current() is None
    assert _stopped_run_ids(table)[-1] == snapshot["run_id"]
    assert ":" in _stopped_run_ids(table)[-1]
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(manifest) is None


@pytest.mark.asyncio
async def test_lease_loss_without_settlement_recovers_an_inverted_owed_stop():
    """No ``stopped`` row and an inverted append; startup still inherits the owed stop."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _start(_lane(manifest))
    await _drain_persists()
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) == "resuming"
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert pipeline_power._intent(snapshot) == "stop_owed"
    assert "stopped" not in {row[1] for row in table.rows}
    due = pipeline_power._parse_stamp(snapshot["owed_at"])
    assert due is not None
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, ring = await _run_owed_recovery(store, snapshot, clock)
    assert len(stops) == 1
    assert table.commit_order[-1] == "stopped"
    assert recovery.status.state == "settled"
    assert pipeline_power.owed_stop_notice(now=clock.now) is None
    assert await ring.current() is None
    assert _stopped_run_ids(table)[-1] == snapshot["run_id"]
    assert ":" in _stopped_run_ids(table)[-1]
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(manifest) is None


@pytest.mark.parametrize("invert_resuming", [True, False], ids=["inverted", "ordered"])
@pytest.mark.asyncio
async def test_start_then_park_leaves_no_inherited_owed_stop(invert_resuming, monkeypatch):
    """Production park must settle the start it paid, in either append order."""

    table = _InvertingPowerTable()
    if not invert_resuming:
        table._stop_owed.set()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    lane = _lane(manifest)
    await _start(lane)
    await _drain_persists()
    await _park_now(lane)
    pairs = _start_pair_keys(table)
    stopped = _stopped_run_ids(table)
    assert pairs and stopped
    assert stopped[-1] == pairs[-1]
    assert ":" in stopped[-1]
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(manifest) is None


@pytest.mark.asyncio
async def test_unpaired_newer_start_still_owes_when_older_stop_owed_sits_below():
    """A failed stop_owed append after a completed older bout still owes."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    older = datetime(2026, 10, 9, 18, 0, tzinfo=UTC)
    await store.append(
        _power_record("stop_owed", "ad-test-001:bout-a", older + timedelta(minutes=30))
    )
    await store.append(_power_record("resuming", "ad-test-001:bout-a", older))
    await store.append(
        _power_record("stopped", "ad-test-001:bout-a", older + timedelta(minutes=31))
    )
    await store.append(
        _power_record("resuming", "ad-test-001:bout-b", older + timedelta(hours=1))
    )
    pipeline_power.install_pipeline_power_store(store)
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    assert snapshot["run_id"] == "ad-test-001:bout-b"


def test_walk_stop_owed_newest_same_pair_stopped_still_settles():
    """Delayed stop_owed after stopped must not resurrect the pair."""

    pair = "run-1:same-pair"
    rows = [
        _sql_row("stop_owed", pair, 3),
        _sql_row("stopped", pair, 2),
        _sql_row("resuming", pair, 1),
    ]
    assert pipeline_power._unmatched_owed_stop(rows, PIPELINE_ID) is None


def test_walk_unpaired_newer_resume_owes_when_older_stop_owed_is_unsettled():
    """Must fail if the pair-B continue becomes return None."""

    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("resuming", "ad-test-001:bout-b", 3),
            _sql_row("stop_owed", "ad-test-001:bout-a", 2),
            _sql_row("resuming", "ad-test-001:bout-a", 1),
        ],
        PIPELINE_ID,
    )
    assert found is not None
    assert found["run_id"] == "ad-test-001:bout-b"


def test_walk_stopped_settles_only_its_own_pair_not_a_later_start():
    """A newer stopped row for pair A must not hide pair B's unmatched stop_owed."""

    rows = [
        _sql_row("stopped", "pair-a:a", 3),
        _sql_row("stop_owed", "pair-b:b", 2),
        _sql_row("resuming", "pair-b:b", 1),
    ]
    found = pipeline_power._unmatched_owed_stop(rows, PIPELINE_ID)
    assert found is not None
    assert found["intent"] == "stop_owed"
    assert found["run_id"] == "pair-b:b"


@pytest.mark.parametrize(
    "intents",
    list(itertools.permutations(("resuming", "stop_owed", "stopped"))),
    ids=lambda ints: "-".join(ints),
)
@pytest.mark.asyncio
async def test_stopped_row_settles_even_when_resuming_order_varies(intents):
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pair = "run-1:same-pair"
    stamps = {
        "stop_owed": datetime(2026, 10, 9, 19, 30, tzinfo=UTC),
        "resuming": datetime(2026, 10, 9, 19, 0, tzinfo=UTC),
        "stopped": datetime(2026, 10, 9, 19, 31, tzinfo=UTC),
    }
    for intent in intents:
        await store.append(_power_record(intent, pair, stamps[intent]))
    pipeline_power.install_pipeline_power_store(store)
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(_manifest()) is None


@pytest.mark.asyncio
async def test_unpaired_resuming_after_failed_stop_owed_still_owes_a_stop():
    """Crash or append failure between resuming and stop_owed must not settle the start."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    pair = f"ad-test-001:{'a' * 32}"
    resumed = datetime(2026, 10, 9, 18, 0, tzinfo=UTC)
    await store.append(_power_record("resuming", pair, resumed))
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    assert pipeline_power._intent(snapshot) == "stop_owed"
    assert snapshot["run_id"] == pair


@pytest.mark.asyncio
async def test_lane_start_keeps_owed_stop_when_stop_owed_append_fails(monkeypatch):
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    original = store.append

    async def refuse_owed(record):
        if record.get("intent") == "stop_owed":
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse_owed  # type: ignore[method-assign]
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _start(_lane(manifest))
    await _drain_persists()
    assert "stop_owed" not in table.commit_order
    assert "resuming" in table.commit_order
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert pipeline_power._intent(snapshot) == "stop_owed"


@pytest.mark.asyncio
async def test_unkeyed_legacy_rows_do_not_bind_a_later_stamped_start():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    due = datetime(2026, 10, 9, 18, 30, tzinfo=UTC)
    pair_b = f"ad-test-001:{'b' * 32}"
    await store.append(_power_record("stop_owed", "ad-test-001", due))
    await store.append(_power_record("resuming", pair_b, due + timedelta(minutes=1)))
    pipeline_power.install_pipeline_power_store(store)
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    assert snapshot["run_id"] == pair_b


@pytest.mark.asyncio
async def test_unkeyed_same_start_inversion_is_still_unmatched():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    due = datetime(2026, 10, 9, 18, 30, tzinfo=UTC)
    await store.append(_power_record("stop_owed", "ad-test-001", due))
    await store.append(_power_record("resuming", "ad-test-001", due - timedelta(minutes=30)))
    pipeline_power.install_pipeline_power_store(store)
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    assert snapshot["run_id"] == "ad-test-001"


def test_lane_start_stamps_a_distinct_pair_key_shared_by_both_rows():
    # Minted before the clock, so Lakebase's clock never waits on the reservation round trip.
    ready = inspect.getsource(LakebaseSyncLane.before_bell)
    assert "uuid.uuid4().hex" in ready
    assert "mint_power_pair_key" in ready
    run = inspect.getsource(Round4RaceEngine.run)
    assert run.index("before_bell()") < run.index("_Clock(")
    source = inspect.getsource(LakebaseSyncLane.start)
    assert "mint_power_pair_key" not in source
    assert "rebase_snapshot" not in source
    assert source.index("pair_key") < source.index("pipeline_power.start")
    assert 'record["run_id"] = pair_key' in source
    assert 'owed_record["run_id"] = pair_key' in source
    park = inspect.getsource(LakebaseSyncLane._park)
    assert "_power_pair_key" in park
    assert park.index("mint_power_pair_key") < park.index("pipeline_power.stop")
    recovery = inspect.getsource(InheritedRound4StopRecovery._act_under_fence)
    assert 'stopped["run_id"] = settled_run_id' in recovery
    assert pipeline_power.STOP_OWED_DUE_SECONDS == round4_race_live.BOUT_STOP_DUE_SECONDS


@pytest.mark.asyncio
async def test_two_lane_starts_get_distinct_pair_keys():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _start(_lane(manifest))
    await _drain_persists()
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    keys: dict[str, list[str]] = {}
    for row in table.rows:
        keys.setdefault(str(row[2]), []).append(str(row[1]))
    assert len(keys) == 2
    assert all("resuming" in intents and "stop_owed" in intents for intents in keys.values())
    assert all(":" in key for key in keys)


@pytest.mark.asyncio
async def test_unmatched_owed_stop_survives_more_than_lookback_intervening_rows():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    due = datetime(2026, 10, 9, 18, 0, tzinfo=UTC)
    await store.append(_power_record("stop_owed", "run-1:owed-pair", due))
    filler = due + timedelta(seconds=1)
    for index in range(pipeline_power._POWER_HISTORY_LOOKBACK + 1):
        await store.append(
            _power_record("filler", f"run-1:filler-{index}", filler + timedelta(seconds=index))
        )
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) != "stop_owed"
    pipeline_power.install_pipeline_power_store(store)
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    assert pipeline_power._intent(snapshot) == "stop_owed"
    assert snapshot["run_id"] == "run-1:owed-pair"


def test_power_intent_latest_sql_lives_on_the_history_reader():
    source = inspect.getsource(pipeline_power.DurablePipelinePowerStore._power_history_rows)
    assert "rebase" not in source.lower()
    assert "ORDER BY event_id DESC" in source
    assert "LIMIT" in source
    assert "skip_reserved" in source
    assert "AND intent <>" in source
    assert "_INTENT_GENERATION_RESERVED" in source


_BARE = "ad-test-001"
_A = f"{_BARE}:{'a' * 32}"
_B = f"{_BARE}:{'b' * 32}"


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([_sql_row("stopped", _BARE), _sql_row("stop_owed", _A), _sql_row("resuming", _A)], _A),
        ([_sql_row("stopped", _BARE), _sql_row("resuming", _A), _sql_row("stop_owed", _A)], _A),
        (
            [
                _sql_row("stop_owed", _B, 5),
                _sql_row("resuming", _B, 4),
                _sql_row("stopped", _BARE, 3),
                _sql_row("stop_owed", _A, 2),
            ],
            _B,
        ),
        (
            [
                _sql_row("resuming", _B, 5),
                _sql_row("stopped", _BARE, 3),
                _sql_row("stop_owed", _A, 2),
                _sql_row("resuming", _A, 1),
            ],
            _B,
        ),
        ([_sql_row("resuming", _BARE, 2), _sql_row("stop_owed", _BARE, 1)], _BARE),
    ],
)
def test_walk_production_and_legacy_settlement_shapes(rows, expected):
    found = pipeline_power._unmatched_owed_stop(rows, PIPELINE_ID)
    got = None if found is None else found["run_id"]
    assert got == expected


@pytest.mark.asyncio
async def test_completed_bout_then_failed_stop_owed_append_still_owes(monkeypatch):
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    lane = _lane(manifest)
    await _start(lane)
    await _drain_persists()
    await _park_now(lane)
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None

    original = store.append

    async def refuse_owed(record):
        if record.get("intent") == "stop_owed":
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse_owed  # type: ignore[method-assign]
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert snapshot["run_id"] == _start_pair_keys(table)[-1]


@pytest.mark.asyncio
async def test_lane_park_still_settles_when_stop_owed_commits_after_stopped():
    table = _StopOwedAfterStoppedTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    lane = _lane(manifest)
    await _start(lane)
    await _park_now(lane)
    await _drain_persists()
    stopped_at = table.commit_order.index("stopped")
    owed_at = table.commit_order.index("stop_owed")
    assert stopped_at < owed_at
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(manifest) is None


@pytest.mark.asyncio
async def test_later_start_then_park_does_not_resurrect_older_owed_stop():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    first = _lane(manifest)
    await _start(first)
    await _drain_persists()
    table._stop_owed = asyncio.Event()
    second = _lane(manifest)
    await _start(second)
    await _drain_persists()
    await _park_now(second)
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(manifest) is None


@pytest.mark.asyncio
async def test_unsettled_older_bout_then_failed_newer_stop_owed_still_owes():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _start(_lane(manifest))
    await _drain_persists()
    original = store.append

    async def refuse_owed(record):
        if record.get("intent") == "stop_owed":
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse_owed  # type: ignore[method-assign]
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert snapshot["run_id"] == _start_pair_keys(table)[-1]


async def _assert_recovery_does_not_stop(store, pair: str, due: datetime) -> None:
    stale = _power_record("stop_owed", pair, due)
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, ring = await _run_owed_recovery(store, stale, clock)
    assert stops == []
    assert recovery.status.state == "settled"
    assert await ring.current() is None


@pytest.mark.parametrize(
    "intents",
    list(itertools.permutations(("resuming", "stop_owed", "stopped"))),
    ids=lambda ints: "-".join(ints),
)
@pytest.mark.asyncio
async def test_recovery_does_not_act_on_same_pair_commit_permutations(intents):
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pair = "run-1:same-pair"
    stamps = {
        "stop_owed": datetime(2026, 10, 9, 19, 30, tzinfo=UTC),
        "resuming": datetime(2026, 10, 9, 19, 0, tzinfo=UTC),
        "stopped": datetime(2026, 10, 9, 19, 31, tzinfo=UTC),
    }
    for intent in intents:
        await store.append(_power_record(intent, pair, stamps[intent]))
    pipeline_power.install_pipeline_power_store(store)
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    await _assert_recovery_does_not_stop(store, pair, stamps["stop_owed"])
    assert table.commit_order.count("stopped") == 1


@pytest.mark.asyncio
async def test_recovery_does_not_act_when_stop_owed_commits_after_same_pair_stopped():
    table = _StopOwedAfterStoppedTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    lane = _lane(manifest)
    await _start(lane)
    await _park_now(lane)
    await _drain_persists()
    assert table.commit_order.index("stopped") < table.commit_order.index("stop_owed")
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) == "stop_owed"
    pair = _start_pair_keys(table)[-1]
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is None
    due = datetime(2026, 10, 9, 19, 30, tzinfo=UTC)
    await _assert_recovery_does_not_stop(store, pair, due)
    assert table.commit_order.count("stopped") == 1


def test_walk_highest_gen_keeps_its_own_resume_origin():
    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stop_owed", older, 4),
            _sql_row("stop_owed", newer, 3),
            _sql_row("resuming", older, 2),
            _sql_row("resuming", newer, 1),
        ],
        PIPELINE_ID,
    )
    assert found is not None
    assert found["run_id"] == newer
    assert found["resumed_at"] == "2026-10-09T19:01:00+00:00"


def test_walk_inverted_resuming_keeps_pair_resume_origin():
    pair = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("resuming", pair, 4),
            _sql_row("stop_owed", pair, 3),
        ],
        PIPELINE_ID,
    )
    assert found is not None
    assert found["run_id"] == pair
    assert found["resumed_at"] == "2026-10-09T19:04:00+00:00"


def test_status_history_keeps_newer_pair_resume_origin():
    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    marker = pipeline_power._marker_from_history_rows(
        [
            _sql_row("stop_owed", newer, 3),
            _sql_row("resuming", older, 2),
            _sql_row("resuming", newer, 1),
        ],
        PIPELINE_ID,
    )
    assert marker is not None
    assert marker["run_id"] == newer
    assert marker["resumed_at"] == "2026-10-09T19:01:00+00:00"


def test_walk_delayed_older_owed_does_not_hide_newer_unmatched_start():
    """Newest unmatched ``stop_owed(A)`` must not beat a higher-generation B."""

    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stop_owed", older, 4),
            _sql_row("stop_owed", newer, 3),
            _sql_row("resuming", newer, 2),
            _sql_row("resuming", older, 1),
        ],
        PIPELINE_ID,
    )
    assert found is not None
    assert found["run_id"] == newer


def test_walk_delayed_older_park_does_not_hide_newer_unmatched_start():
    """Newest ``stopped(A)`` must not ``return None`` before unmatched higher-gen B."""

    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stopped", older, 5),
            _sql_row("stop_owed", older, 4),
            _sql_row("stop_owed", newer, 3),
            _sql_row("resuming", newer, 2),
            _sql_row("resuming", older, 1),
        ],
        PIPELINE_ID,
    )
    assert found is not None
    assert found["run_id"] == newer


def test_walk_delayed_older_park_hides_older_unmatched_when_parked_gen_is_newer():
    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stopped", newer, 5),
            _sql_row("stop_owed", newer, 4),
            _sql_row("stop_owed", older, 3),
            _sql_row("resuming", older, 2),
            _sql_row("resuming", newer, 1),
        ],
        PIPELINE_ID,
    )
    assert found is None


def test_walk_stopped_older_does_not_hide_higher_generation_unmatched_pair():
    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stopped", older, 3),
            _sql_row("stop_owed", newer, 2),
            _sql_row("resuming", newer, 1),
        ],
        PIPELINE_ID,
    )
    assert found is not None
    assert found["run_id"] == newer


def test_walk_later_parked_generation_settles_older_stop_owed_even_when_newest():
    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stop_owed", older, 5),
            _sql_row("stopped", newer, 4),
            _sql_row("stop_owed", newer, 3),
            _sql_row("resuming", newer, 2),
            _sql_row("resuming", older, 1),
        ],
        PIPELINE_ID,
    )
    assert found is None


async def _wait_commit(table: _InvertingPowerTable, intent: str, count: int = 1) -> None:
    for _ in range(50):
        if table.commit_order.count(intent) >= count:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"{intent} did not commit {count} time(s): {table.commit_order}")


@pytest.mark.asyncio
async def test_later_park_settles_when_older_stop_owed_commits_last():
    table = _OlderStopOwedCommitsLastTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    first = _lane(manifest)
    await _start(first)
    await _wait_commit(table, "resuming", 1)
    assert "stop_owed" not in table.commit_order
    table._stop_owed = asyncio.Event()
    second = _lane(manifest)
    await _start(second)
    await _wait_commit(table, "resuming", 2)
    await _park_now(second)
    await _drain_persists()
    assert table.commit_order[-1] == "stop_owed"
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(manifest) is None
    older_pair = str(table.rows[-1][2])
    due = datetime(2026, 10, 9, 19, 30, tzinfo=UTC)
    await _assert_recovery_does_not_stop(store, older_pair, due)
    assert table.commit_order.count("stopped") == 1


async def _newer_bout_parks_after_failed(intent: str) -> None:
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await store.append(
        _power_record("stop_owed", manifest.run_id, datetime(2026, 10, 9, 18, 30, tzinfo=UTC))
    )
    original = store.append

    async def refuse(record):
        if record.get("intent") == intent:
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse  # type: ignore[method-assign]
    newer = _lane(manifest)
    await _start(newer)
    await _drain_persists()
    await _park_now(newer)
    assert intent not in [str(row[1]) for row in table.rows[1:]]
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None
    assert await pipeline_power.load_owed_stop_snapshot(manifest) is None


@pytest.mark.asyncio
async def test_newer_park_settles_when_its_stop_owed_append_failed():
    await _newer_bout_parks_after_failed("stop_owed")


@pytest.mark.asyncio
async def test_newer_park_settles_when_its_resuming_append_failed():
    await _newer_bout_parks_after_failed("resuming")


def test_walk_parked_newer_without_resuming_does_not_owe_older_pair():
    """A later parked generation settles older debt even without that pair's resume."""

    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stopped", newer, 4),
            _sql_row("stop_owed", newer, 3),
            _sql_row("stop_owed", older, 2),
            _sql_row("resuming", older, 1),
        ],
        PIPELINE_ID,
    )
    assert found is None


def test_walk_parked_newer_without_stop_owed_does_not_owe_older_pair():
    """A later parked generation settles older debt even without that pair's stop_owed."""

    older = "ad-test-001:1:" + ("a" * 32)
    newer = "ad-test-001:2:" + ("b" * 32)
    found = pipeline_power._unmatched_owed_stop(
        [
            _sql_row("stopped", newer, 4),
            _sql_row("resuming", newer, 3),
            _sql_row("stop_owed", older, 2),
            _sql_row("resuming", older, 1),
        ],
        PIPELINE_ID,
    )
    assert found is None


def _simulate_process_restart() -> None:
    """A new serving process has no in-memory generation clock (r5 restarts at 1)."""

    pipeline_power._POWER_START_GENERATION = itertools.count(1)
    reset = getattr(pipeline_power, "reset_power_start_generation", None)
    if reset is not None:
        reset()


async def _park_two_bouts(store, table, manifest) -> None:
    for _ in range(2):
        table._stop_owed = asyncio.Event()
        lane = _lane(manifest)
        await _start(lane)
        await _drain_persists()
        await _park_now(lane)
        assert await store.unmatched_owed_stop(PIPELINE_ID) is None


@pytest.mark.asyncio
async def test_restart_after_two_parks_still_owes_a_new_start():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _park_two_bouts(store, table, manifest)
    _simulate_process_restart()
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    due = pipeline_power._parse_stamp(snapshot["owed_at"])
    assert due is not None
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, ring = await _run_owed_recovery(store, snapshot, clock)
    assert len(stops) == 1
    assert recovery.status.state == "settled"
    assert await ring.current() is None


@pytest.mark.asyncio
async def test_restart_after_two_parks_inverted_latest_still_owes():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _park_two_bouts(store, table, manifest)
    _simulate_process_restart()
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) == "resuming"
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert pipeline_power._intent(snapshot) == "stop_owed"
    due = pipeline_power._parse_stamp(snapshot["owed_at"])
    assert due is not None
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, _ring = await _run_owed_recovery(store, snapshot, clock)
    assert len(stops) == 1
    assert recovery.status.state == "settled"


@pytest.mark.asyncio
async def test_restart_after_two_parks_resuming_only_still_owes():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _park_two_bouts(store, table, manifest)
    _simulate_process_restart()
    table._stop_owed = asyncio.Event()
    original = store.append

    async def refuse_owed(record):
        if record.get("intent") == "stop_owed":
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse_owed  # type: ignore[method-assign]
    await _start(_lane(manifest))
    await _drain_persists()
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    due = pipeline_power._parse_stamp(snapshot["owed_at"])
    assert due is not None
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, _ring = await _run_owed_recovery(store, snapshot, clock)
    assert len(stops) == 1
    assert recovery.status.state == "settled"


@pytest.mark.asyncio
async def test_prepare_park_then_start_still_owes_when_stopped_commits_last():
    table = _StoppedCommitsLastTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    preparer = _lane(manifest)
    assert preparer._power_pair_key == ""
    await _park_now(preparer, drain=False)
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    assert table.commit_order[-1] == "stopped"
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert ":" in str(snapshot.get("run_id") or "")
    stopped_keys = _stopped_run_ids(table)
    assert stopped_keys and ":" in stopped_keys[-1]


@pytest.mark.asyncio
async def test_clock_behind_history_still_mints_above_parked(monkeypatch):
    """Pins observed_max+1: a rolled-back clock with a healthy SELECT still owes."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _park_two_bouts(store, table, manifest)
    parked = max(pipeline_power._pair_generation(key) for key in _stopped_run_ids(table))
    _simulate_process_restart()
    monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 1)
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    minted = pipeline_power._pair_generation(_start_pair_keys(table)[-1])
    assert minted > parked
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert snapshot["run_id"] == _start_pair_keys(table)[-1]


@pytest.mark.asyncio
async def test_history_read_failure_does_not_mint_below_parked_generation(monkeypatch):
    """A failed history read plus a rolled-back clock must not hide a new start."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    await _park_two_bouts(store, table, manifest)
    rows_before = list(table.rows)
    _simulate_process_restart()
    monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 1)

    async def boom(_pipeline_id):
        del _pipeline_id
        raise ConnectionError("coordination read unavailable")

    monkeypatch.setattr(store, "newest_reserved_pair_key", boom)
    calls: list = []
    lane = _lane(manifest)
    lane._api = _recording_api(calls)
    with pytest.raises(pipeline_power.PipelinePowerError):
        await _start(lane)
    await _drain_persists()
    assert calls == []
    assert table.rows == rows_before
    monkeypatch.undo()
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None


@pytest.mark.asyncio
async def test_the_generation_floor_is_one_bounded_read_however_long_the_history(monkeypatch):
    """The floor is the newest reservation, read as one row, not a scan of every bout so far."""

    table = _InvertingPowerTable()
    table._stop_owed.set()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    at = datetime(2026, 10, 9, 18, 0, tzinfo=UTC)
    for generation in range(1, 41):
        pair = f"ad-test-001:{generation}:{generation:032x}"
        for intent in ("generation_reserved", "resuming", "stop_owed", "stopped"):
            await store.append(_power_record(intent, pair, at))
    # An older bout's fire-and-forget row that commits after the newest reservation.
    await store.append(_power_record("stop_owed", f"ad-test-001:7:{7:032x}", at))
    pipeline_power.install_pipeline_power_store(store)
    selects: list[tuple[str, int]] = []
    execute = table.execute

    async def recording(sql: str, params: tuple = ()) -> None:
        await execute(sql, params)
        if "SELECT" in sql:
            selects.append((" ".join(sql.split()), len(table._result)))

    table.execute = recording  # type: ignore[method-assign]
    pipeline_power.reset_power_start_generation()
    monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 1)

    pair = await pipeline_power.mint_power_pair_key("ad-test-001", PIPELINE_ID)

    assert pipeline_power._pair_generation(pair) == 41
    assert len(selects) == 1
    sql, rows = selects[0]
    assert re.search(r"LIMIT 1\b", sql), sql
    assert rows == 1


def test_same_tick_mints_stay_strictly_increasing(monkeypatch):
    pipeline_power.reset_power_start_generation()
    monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 7)
    first = pipeline_power.next_power_start_generation()
    second = pipeline_power.next_power_start_generation()
    assert first == 7
    assert second == 8


@pytest.mark.asyncio
async def test_in_flight_reserved_generation_is_visible_to_the_next_mint(monkeypatch):
    """A reserved gen 100 with no stopped row yet must still floor the next mint."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    pipeline_power.reset_power_start_generation()
    monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 90)
    table._stop_owed = asyncio.Event()
    first = _lane(manifest)
    await _start(first)
    await _drain_persists()
    await _park_now(first)
    parked = max(pipeline_power._pair_generation(key) for key in _stopped_run_ids(table))
    assert parked == 90
    monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 100)
    reserved = await pipeline_power.mint_power_pair_key(manifest.run_id, PIPELINE_ID)
    assert pipeline_power._pair_generation(reserved) == 100
    assert reserved not in _stopped_run_ids(table)
    _simulate_process_restart()
    monkeypatch.setattr(pipeline_power.time, "time_ns", lambda: 1)
    table._stop_owed = asyncio.Event()
    await _start(_lane(manifest))
    await _drain_persists()
    newer = _start_pair_keys(table)[-1]
    assert pipeline_power._pair_generation(newer) > 100
    await store.append(
        _power_record("stopped", reserved, datetime(2026, 10, 9, 19, 40, tzinfo=UTC))
    )
    snapshot = await pipeline_power.load_owed_stop_snapshot(manifest)
    assert snapshot is not None
    assert snapshot["run_id"] == newer
    assert await store.unmatched_owed_stop(PIPELINE_ID) is not None


@pytest.mark.asyncio
async def test_reservation_append_failure_starts_nothing():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    original = store.append

    async def refuse(record):
        if record.get("intent") == "generation_reserved":
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse  # type: ignore[method-assign]
    calls: list = []
    lane = _lane(_manifest())
    lane._api = _recording_api(calls)
    with pytest.raises(pipeline_power.PipelinePowerError):
        await _start(lane)
    await _drain_persists()
    assert calls == []
    assert table.rows == []


@pytest.mark.asyncio
async def test_latest_skips_generation_reserved_until_a_real_intent():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    reserved = "ad-test-001:5:" + ("c" * 32)
    await store.append(
        _power_record("generation_reserved", reserved, datetime(2026, 10, 9, 19, 2, tzinfo=UTC))
    )
    assert await store.latest(PIPELINE_ID) is None
    owed_at = datetime(2026, 10, 9, 18, 0, tzinfo=UTC)
    pair = "ad-test-001:4:" + ("b" * 32)
    await store.append(
        _power_record("resuming", pair, owed_at - timedelta(minutes=30))
    )
    await store.append(_power_record("stop_owed", pair, owed_at))
    await store.append(
        _power_record("generation_reserved", reserved, datetime(2026, 10, 9, 19, 3, tzinfo=UTC))
    )
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) == "stop_owed"
    assert latest["run_id"] == pair
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    assert snapshot["run_id"] == pair


@pytest.mark.asyncio
async def test_status_one_snapshot_keeps_a_real_intent():
    class _RaceStore:
        def __init__(self) -> None:
            self.calls = 0

        async def latest_for_status(self, pipeline_id: str):
            self.calls += 1
            return {
                "intent": "stopped",
                "pipeline_id": pipeline_id,
                "run_id": "ad-test-001:1:" + ("a" * 32),
                "stopped_at": "2026-10-09T19:40:00+00:00",
                "stopped_by": "app-client",
            }

        async def latest(self, pipeline_id: str, *, skip_reserved: bool = True):
            del pipeline_id, skip_reserved
            raise AssertionError("status must not take a second latest() snapshot")

    store = _RaceStore()
    marker = await pipeline_power._latest_for_status(store, PIPELINE_ID)
    assert store.calls == 1
    assert marker is not None
    assert pipeline_power._intent(marker) == "stopped"


@pytest.mark.asyncio
async def test_status_one_snapshot_does_not_keep_a_reservation_over_owed():
    """A newer reservation in the same snapshot must not hide stop_owed."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    owed_at = datetime(2026, 10, 9, 19, 0, tzinfo=UTC)
    pair = "ad-test-001:1:" + ("a" * 32)
    await store.append(_power_record("resuming", pair, owed_at - timedelta(minutes=30)))
    await store.append(_power_record("stop_owed", pair, owed_at))
    await store.append(
        _power_record(
            "generation_reserved",
            "ad-test-001:2:" + ("b" * 32),
            owed_at + timedelta(minutes=41),
        )
    )
    marker = await pipeline_power._latest_for_status(store, PIPELINE_ID)
    assert marker is not None
    assert pipeline_power._intent(marker) == "stop_owed"
    assert marker["run_id"] == pair


def test_status_snapshot_sql_is_one_statement():
    source = inspect.getsource(
        pipeline_power.DurablePipelinePowerStore._status_snapshot_rows
    )
    assert "status-snapshot-prefer-real" in source
    assert "UNION ALL" in source
    assert "NOT EXISTS" in source
    assert "AND intent <>" in source
    assert source.count("await cursor.execute") == 1
    assert source.rfind(") snap") < source.rfind("ORDER BY event_id DESC")
    assert source.rfind("NOT EXISTS") < source.rfind("ORDER BY event_id DESC")


@pytest.mark.asyncio
async def test_durable_status_uses_one_snapshot_query_not_latest():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    owed_at = datetime(2026, 10, 9, 19, 0, tzinfo=UTC)
    pair = "ad-test-001:1:" + ("a" * 32)
    await store.append(_power_record("resuming", pair, owed_at - timedelta(minutes=30)))
    await store.append(_power_record("stop_owed", pair, owed_at))
    await store.append(
        _power_record(
            "generation_reserved",
            "ad-test-001:2:" + ("b" * 32),
            owed_at + timedelta(minutes=41),
        )
    )
    selects: list[str] = []
    original = table.execute

    async def counting_execute(sql: str, params: tuple = ()) -> None:
        if "SELECT" in sql:
            selects.append(sql)
        await original(sql, params)

    table.execute = counting_execute  # type: ignore[method-assign]

    async def boom(*_args, **_kwargs):
        raise AssertionError("Durable status must not call latest()")

    store.latest = boom  # type: ignore[method-assign]
    marker = await pipeline_power._latest_for_status(store, PIPELINE_ID)
    assert marker is not None
    assert pipeline_power._intent(marker) == "stop_owed"
    assert len(selects) == 1
    sql = selects[0]
    assert "status-snapshot-prefer-real" in sql
    assert sql.rfind(") snap") < sql.rfind("ORDER BY event_id DESC")


def test_reservation_only_is_not_a_deliberate_stop_or_install_bill():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    manifest.created_at = datetime(2026, 8, 1, tzinfo=UTC)
    asyncio.run(
        store.append(
            _power_record(
                "generation_reserved",
                "ad-test-001:5:" + ("c" * 32),
                datetime(2026, 10, 9, 19, 2, tzinfo=UTC),
            )
        )
    )
    assert asyncio.run(store.latest(PIPELINE_ID)) is None
    idle = pipeline_power.power_state(manifest, lambda _i: {"state": "IDLE"})
    assert idle.stopped_deliberately is False
    assert "STOPPED ON PURPOSE" not in idle.summary()
    marker = pipeline_power.read_stop_marker(PIPELINE_ID)
    assert marker is None or pipeline_power._intent(marker) != "generation_reserved"
    running = pipeline_power.power_state(manifest, lambda _i: {"state": "RUNNING"})
    assert running.stop_owed is False
    assert "A STOP WAS OWED" not in running.summary()
    if running.accrual_basis:
        assert "installation was created" not in running.accrual_basis


def test_overdue_stop_owed_stays_visible_under_a_newer_reservation():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    manifest = _manifest()
    resumed = datetime(2026, 8, 21, 16, 12, tzinfo=UTC)
    due = datetime(2026, 8, 21, 16, 42, tzinfo=UTC)
    manifest.created_at = resumed - timedelta(days=1)
    pair = "ad-test-001:1:" + ("a" * 32)
    asyncio.run(store.append(_power_record("resuming", pair, resumed)))
    asyncio.run(store.append(_power_record("stop_owed", pair, due)))
    asyncio.run(
        store.append(
            _power_record(
                "generation_reserved",
                "ad-test-001:2:" + ("b" * 32),
                due + timedelta(minutes=10),
            )
        )
    )
    marker = pipeline_power.read_stop_marker(
        PIPELINE_ID, now=lambda: due + timedelta(minutes=5)
    )
    assert marker is not None
    assert pipeline_power._intent(marker) == "stop_owed"
    state = pipeline_power.power_state(
        manifest,
        lambda _i: {"state": "RUNNING"},
        now=lambda: due + timedelta(minutes=5),
    )
    assert "A STOP WAS OWED" in state.summary()
    assert "restarted at 2026-08-21T16:12Z" in state.accrual_basis
    assert "installation was created" not in state.accrual_basis


async def _seed_overdue_owed_under_reservations(store, reserved_count: int):
    resumed = datetime(2026, 8, 21, 16, 12, tzinfo=UTC)
    due = datetime(2026, 8, 21, 16, 42, tzinfo=UTC)
    pair = "ad-test-001:1:" + ("a" * 32)
    await store.append(_power_record("resuming", pair, resumed))
    await store.append(_power_record("stop_owed", pair, due))
    for index in range(reserved_count):
        await store.append(
            _power_record(
                "generation_reserved",
                f"ad-test-001:{index + 2}:" + ("b" * 32),
                due + timedelta(seconds=index + 1),
            )
        )
    return pair, due, resumed


@pytest.mark.parametrize("reserved_count", [20, 25])
@pytest.mark.asyncio
async def test_latest_sees_owed_under_a_full_window_of_reservations(reserved_count):
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pair, due, _resumed = await _seed_overdue_owed_under_reservations(store, reserved_count)
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    assert pipeline_power._intent(latest) == "stop_owed"
    assert latest["run_id"] == pair
    unmatched = await store.unmatched_owed_stop(PIPELINE_ID)
    assert unmatched is not None
    assert unmatched["run_id"] == pair
    pipeline_power.install_pipeline_power_store(store)
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    assert snapshot["run_id"] == pair
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, _ring = await _run_owed_recovery(store, snapshot, clock)
    assert len(stops) == 1
    assert recovery.status.state == "settled"
    assert table.commit_order[-1] == "stopped"
    assert await store.unmatched_owed_stop(PIPELINE_ID) is None


@pytest.mark.parametrize("reserved_count", [20, 25])
def test_doctor_still_sees_owed_under_a_full_window_of_reservations(reserved_count):
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    pair, due, resumed = asyncio.run(
        _seed_overdue_owed_under_reservations(store, reserved_count)
    )
    del resumed
    manifest = _manifest()
    manifest.created_at = due - timedelta(days=1)
    marker = pipeline_power.read_stop_marker(
        PIPELINE_ID, now=lambda: due + timedelta(minutes=5)
    )
    assert marker is not None
    assert pipeline_power._intent(marker) == "stop_owed"
    assert marker["run_id"] == pair
    state = pipeline_power.power_state(
        manifest,
        lambda _i: {"state": "RUNNING"},
        now=lambda: due + timedelta(minutes=5),
    )
    assert "A STOP WAS OWED" in state.summary()


@pytest.mark.asyncio
async def test_latest_and_recovery_see_owed_when_reserved_and_junk_fill_the_window():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pair, due, _resumed = await _seed_overdue_owed_under_reservations(store, 19)
    await store.append(
        _power_record("filler", "ad-test-001:junk", due + timedelta(seconds=30))
    )
    latest = await store.latest(PIPELINE_ID)
    assert latest is not None
    unmatched = await store.unmatched_owed_stop(PIPELINE_ID)
    assert unmatched is not None
    assert unmatched["run_id"] == pair
    pipeline_power.install_pipeline_power_store(store)
    snapshot = await pipeline_power.load_owed_stop_snapshot(_manifest())
    assert snapshot is not None
    clock = Clock(due + timedelta(minutes=1))
    recovery, stops, _ring = await _run_owed_recovery(store, snapshot, clock)
    assert len(stops) == 1
    assert recovery.status.state == "settled"


@pytest.mark.asyncio
async def test_park_still_stops_when_pair_reserve_fails():
    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    original = store.append

    async def refuse(record):
        if record.get("intent") == "generation_reserved":
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse  # type: ignore[method-assign]
    calls: list = []
    lane = _lane(_manifest())
    lane._api = _recording_api(calls)
    with pytest.raises(pipeline_power.PipelinePowerError):
        await _park_now(lane)
    assert any(path.endswith("/stop") for _method, path, _body in calls)
    assert "stopped" not in {row[1] for row in table.rows}
    assert round4_race_live._PENDING == set()


@pytest.mark.asyncio
async def test_a_refused_stop_is_what_a_park_reports_when_its_pair_reserve_also_failed():
    """A pipeline that may still be billing outranks the bookkeeping that failed before it."""

    table = _InvertingPowerTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    pipeline_power.install_pipeline_power_store(store)
    original = store.append

    async def refuse(record):
        if record.get("intent") == "generation_reserved":
            raise ConnectionError("coordination write unavailable")
        return await original(record)

    store.append = refuse  # type: ignore[method-assign]
    stops: list[str] = []

    def refusing_api(profile, method, path, *, body=None, timeout=600):
        del profile, body, timeout
        if method == "post" and path.endswith("/stop"):
            stops.append(path)
            raise RuntimeError("the workspace refused the stop")
        return {}

    lane = _lane(_manifest())
    lane._api = refusing_api
    with pytest.raises(RuntimeError, match="refused the stop"):
        await _park_now(lane)
    assert len(stops) == 1
    assert "stopped" not in {row[1] for row in table.rows}
