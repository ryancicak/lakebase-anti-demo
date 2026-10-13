"""A Delta-source repair owes a Managed Sync rebase across engines.

The repairing Prepare's in-memory ``DeltaSource.repaired`` flag dies with that
engine. The obligation itself must survive card cancel, expiry, failure before a
lane start, and process restart, and it must not be inferred by comparing the
sync cursor to the source head.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import re
import textwrap
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import psycopg
import pytest

from server import pipeline_power, round4_race_live
from server.manager import RunManager
from server.model_score_live import (
    DeltaPathDoesNotExistError,
    LiveModelScoreAdapter,
    PipelineSignals,
    WorkspaceStatementExecutionError,
)
from server.models import BoutOperator, SessionState
from server.pipeline_power import PipelinePowerError, PipelineUpdateConflictError
from server.round4_race import Bell
from server.round4_race_live import DeltaSource, LakebaseSyncLane
from tests.test_lifecycle import attach_round4, make_manifest
from tests.test_manager import FakeModelScoreEngine, round_four_request, wait_for_state
from tests.test_model_score_live import live_config
from tests.test_pipeline_power import _recording_api
from tests.test_round4_race import BASELINE as R4_BASELINE

PIPELINE_ID = "pipeline-1"
FOREIGN_PIPELINE_ID = "pipeline-other"


class _RepairingAdapter:
    """First preflight repairs; every later adapter finds the rebuilt path present."""

    table_rebuilt = False

    def __init__(self, pipeline_id: str = PIPELINE_ID) -> None:
        self.source_repaired = False
        self.config = SimpleNamespace(pipeline_id=pipeline_id)
        self.inspect_calls = 0

    async def preflight_source(self, entity_id):
        del entity_id
        if not _RepairingAdapter.table_rebuilt:
            _RepairingAdapter.table_rebuilt = True
            self.source_repaired = True
        return R4_BASELINE

    async def inspect_sync(self):
        self.inspect_calls += 1
        raise AssertionError(
            "inspect_sync is post-race evidence only; the rebase is carried, not re-derived"
        )


class _FakeRebaseTable:
    """Coordination rows for the rebase ledger, insertion-ordered like the power fake.

    Power-history inserts share the same runner in one test so a ``stop_owed`` or
    ``resuming`` row cannot be mistaken for a rebase event. Newest-row reads use
    the SQL ``ORDER BY event_id DESC LIMIT``, not a Python reverse of insertion.
    """

    def __init__(self) -> None:
        self.rows: list[tuple] = []
        self.power_rows: list[tuple] = []
        self._result: list[tuple] = []
        self.selects: list[str] = []
        self.fail_insert = False
        self.fail_read = False

    async def execute(self, sql: str, params: tuple = ()) -> None:
        rebase = "round4_source_rebase" in sql
        normalized = " ".join(sql.split())
        if sql.strip().upper() in {"BEGIN", "COMMIT", "ROLLBACK"}:
            return
        if "pg_advisory_xact_lock" in sql and "INSERT" not in sql:
            return
        if "INSERT" in sql:
            if rebase:
                if self.fail_insert:
                    raise ConnectionError("coordination write unavailable")
                args = params[1:] if "pg_advisory_xact_lock" in sql else params
                if "WHERE (" in sql:
                    observed = args[-1]
                    pipeline_id = args[0]
                    newest = None
                    for index, row in enumerate(self.rows):
                        if row[0] == pipeline_id:
                            newest = index + 1
                    if newest != observed:
                        self._result = []
                        return
                    self.rows.append(tuple(args[:5]))
                    self._result = [(len(self.rows),)]
                    return
                self.rows.append(tuple(args[:5]))
                self._result = [(len(self.rows),)]
            else:
                self.power_rows.append(tuple(params))
            return
        if "SELECT" in sql and rebase:
            if self.fail_read and "WHERE false" not in sql.lower():
                raise ConnectionError("coordination read unavailable after restart")
            self.selects.append(normalized)
            if "WHERE false" in sql.lower():
                self._result = []
                return
            assert "ORDER BY event_id DESC" in normalized, normalized
            assert re.search(r"LIMIT\s+1\b", normalized), normalized
            matched = [
                (index + 1, row) for index, row in enumerate(self.rows) if row[0] == params[0]
            ]
            matched.sort(key=lambda item: item[0], reverse=True)
            self._result = [
                (event_id, row[1], row[2], row[3], row[4]) for event_id, row in matched[:1]
            ]
            return
        if "SELECT" in sql:
            self._result = []

    async def fetchall(self):
        return list(self._result)

    async def fetchone(self):
        return self._result[0] if self._result else None

    async def run(self, operation):
        return await operation(self)


@pytest.fixture(autouse=True)
def _reset_pipeline_power_store():
    pipeline_power.install_pipeline_power_store(None)
    yield
    pipeline_power.install_pipeline_power_store(None)


def _install_rebase_store(table: _FakeRebaseTable | None = None) -> _FakeRebaseTable:
    table = table or _FakeRebaseTable()
    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(table.run)
    )
    return table


def _lane(source: DeltaSource, *, pipeline_id: str = PIPELINE_ID) -> LakebaseSyncLane:
    lane = object.__new__(LakebaseSyncLane)
    lane._manifest = SimpleNamespace(
        run_id="run-1",
        owner="tester",
        round4=SimpleNamespace(pipeline_id=pipeline_id),
    )
    lane._api = None
    lane._source = source
    lane._pipeline_id = pipeline_id
    lane._now = lambda: datetime.now(UTC)
    lane._sleep = asyncio.sleep
    lane._started_update = ""
    lane._started_full_refresh = False
    lane._rebase_event_id = None
    lane._rebase_healthy_streak = 0
    lane._power_pair_key = ""
    lane._reserved = None
    return lane


def _bell() -> Bell:
    return Bell(bout_id="bout-a", rung_at=datetime.now(UTC), source_table_id="t", source_version=1)


async def _start(lane: LakebaseSyncLane) -> None:
    """What the engine does for this lane at a bell: record its start, then make it."""

    await lane.before_bell()
    await lane.start(_bell())


def _claims_renew_provisions(text: str) -> bool:
    return any(f"{q}antidemo renew{q} provisions" in text for q in ("'", '"', "`"))


def _healthy_signals(update_id: str) -> PipelineSignals:
    return PipelineSignals(
        pipeline_state="RUNNING",
        update_state="RUNNING",
        synced_table_state="SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
        continuous_reported=True,
        update_id=update_id,
    )


def _terminal_signals(update_id: str, state: str) -> PipelineSignals:
    return PipelineSignals(
        pipeline_state="IDLE",
        update_state=state,
        synced_table_state="SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
        continuous_reported=False,
        update_id=update_id,
    )


async def _confirm_healthy_full_refresh(lane: LakebaseSyncLane, update_id: str) -> None:
    async def signals():
        return _healthy_signals(update_id)

    lane._signals = signals
    assert await lane.failure() is None
    assert await lane.failure() is None


async def _repair_then_discard_engine() -> DeltaSource:
    first = DeltaSource(_RepairingAdapter())
    await first.check_storage(R4_BASELINE.entity_id)
    assert first.repaired is True
    return first


class _RepairingRound4Engine(FakeModelScoreEngine):
    def __init__(self, source: DeltaSource, *, fail_after_repair: bool = False) -> None:
        super().__init__()
        self._source = source
        self.fail_after_repair = fail_after_repair

    async def prepare(self, notify):
        await self._source.check_storage(R4_BASELINE.entity_id)
        if self.fail_after_repair:
            raise RuntimeError("prepare failed after the source repair")
        return await super().prepare(notify)


@pytest.mark.asyncio
async def test_a_repair_whose_prepare_never_rings_still_rebases_the_next_bell(monkeypatch):
    """Engine B's in-memory flag is false; the bell still full-refreshes."""

    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-1", resuming_since="", full_refresh=full_refresh)

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})

    await _repair_then_discard_engine()

    second_adapter = _RepairingAdapter()
    second = DeltaSource(second_adapter)
    await second.check_storage(R4_BASELINE.entity_id)
    assert second.repaired is False
    assert second_adapter.inspect_calls == 0
    await _start(_lane(second))

    assert requested == [True]
    assert second_adapter.inspect_calls == 0


@pytest.mark.asyncio
async def test_rebase_owed_survives_a_new_store_and_engine_over_the_same_rows(monkeypatch):
    """After a restart, a new process instance must still request full_refresh."""

    _RepairingAdapter.table_rebuilt = False
    table = _install_rebase_store()
    await _repair_then_discard_engine()

    pipeline_power.install_pipeline_power_store(None)
    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(table.run)
    )

    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-restart", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})

    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    assert second.repaired is False
    await _start(_lane(second))
    assert requested == [True]


@pytest.mark.asyncio
async def test_cancel_arm_after_repair_keeps_rebase_owed(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    owner = BoutOperator(display_name="Owner", subject="rebase-cancel")
    source = DeltaSource(_RepairingAdapter())
    engine = _RepairingRound4Engine(source)
    manager = RunManager(model_score_factory=lambda _c: engine)
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, owner)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True
    cancelled = await manager.cancel_arm(created.id, owner)
    assert cancelled.state == SessionState.FAILED
    await manager.close()
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True

    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-after-cancel", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    later = DeltaSource(_RepairingAdapter())
    await later.check_storage(R4_BASELINE.entity_id)
    await _start(_lane(later))
    assert requested == [True]


@pytest.mark.asyncio
async def test_armed_expiry_after_repair_keeps_rebase_owed(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    monkeypatch.setenv("ANTI_DEMO_ARM_TTL_SECONDS", "0.05")
    owner = BoutOperator(display_name="Owner", subject="rebase-expire")
    source = DeltaSource(_RepairingAdapter())
    engine = _RepairingRound4Engine(source)
    manager = RunManager(model_score_factory=lambda _c: engine)
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, owner)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True
    failed = await wait_for_state(manager, created.id, SessionState.FAILED)
    assert failed.state == SessionState.FAILED
    await manager.close()
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True

    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-after-expire", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    later = DeltaSource(_RepairingAdapter())
    await later.check_storage(R4_BASELINE.entity_id)
    await _start(_lane(later))
    assert requested == [True]


@pytest.mark.asyncio
async def test_failed_arm_after_repair_keeps_rebase_owed(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    owner = BoutOperator(display_name="Owner", subject="rebase-fail")
    source = DeltaSource(_RepairingAdapter())
    engine = _RepairingRound4Engine(source, fail_after_repair=True)
    manager = RunManager(model_score_factory=lambda _c: engine)
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, owner)
    failed = await wait_for_state(manager, created.id, SessionState.FAILED)
    assert failed.state == SessionState.FAILED
    await manager.close()
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True

    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-after-fail", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    later = DeltaSource(_RepairingAdapter())
    await later.check_storage(R4_BASELINE.entity_id)
    await _start(_lane(later))
    assert requested == [True]


@pytest.mark.asyncio
async def test_a_confirmed_full_refresh_clears_owed_so_later_starts_resume(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id=f"update-{len(requested)}", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})

    await _repair_then_discard_engine()
    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    lane = _lane(second)
    await _start(lane)
    assert requested == [True]
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True
    await _confirm_healthy_full_refresh(lane, "update-1")
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is False

    third = DeltaSource(_RepairingAdapter())
    await third.check_storage(R4_BASELINE.entity_id)
    assert third.repaired is False
    await _start(_lane(third))
    assert requested == [True, False]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        PipelineUpdateConflictError("active update identity changed"),
        PipelinePowerError("The Round 4 pipeline start returned no update identity"),
        TimeoutError("pipeline start timed out"),
    ],
)
async def test_a_failed_refused_or_timed_out_start_leaves_rebase_owed(
    error: Exception, monkeypatch
):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    await _repair_then_discard_engine()

    def boom(*_a, **_k):
        raise error

    monkeypatch.setattr(pipeline_power, "start", boom)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    with pytest.raises(type(error)):
        await _start(_lane(second))
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True


def test_cli_pipeline_start_honors_durable_row_after_restart(tmp_path, monkeypatch):
    manifest = make_manifest()
    attach_round4(manifest)
    pipeline_id = manifest.round4.pipeline_id
    monkeypatch.setattr(
        pipeline_power,
        "stop_marker_path",
        lambda manifest_file=None: tmp_path / "stopped.json",
    )
    table = _FakeRebaseTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    asyncio.run(store.append_rebase(pipeline_id, "rebase_owed"))
    pipeline_power.install_pipeline_power_store(None)
    pipeline_power.install_pipeline_power_store(store)

    calls: list[tuple[str, str, dict | None]] = []
    power = pipeline_power.start(
        manifest, _recording_api(calls), marker_path=tmp_path / "power.json"
    )
    assert calls[-1][2] == {"full_refresh": True}
    assert power.full_refresh is True


@pytest.mark.asyncio
async def test_the_rebase_obligation_lives_only_in_the_durable_ledger(tmp_path, monkeypatch):
    """No laptop sidecar and no process cache: every reader asks the coordination ledger."""

    monkeypatch.setattr(
        pipeline_power,
        "stop_marker_path",
        lambda manifest_file=None: tmp_path / pipeline_power.STOP_MARKER_FILENAME,
    )
    _install_rebase_store()

    owed = await pipeline_power.note_rebase_owed(PIPELINE_ID)
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True
    assert await pipeline_power.note_rebase_satisfied(PIPELINE_ID, observed_event_id=owed)
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is False

    assert list(tmp_path.iterdir()) == []
    for retired in (
        "rebase_is_owed_sync",
        "note_rebase_owed_sync",
        "cli_rebase_refresh",
        "_rebase_owed_cache",
    ):
        assert not hasattr(pipeline_power, retired), retired


def test_cli_pipeline_start_without_repair_does_not_full_refresh(tmp_path):
    manifest = make_manifest()
    attach_round4(manifest)
    calls: list[tuple[str, str, dict | None]] = []
    power = pipeline_power.start(
        manifest, _recording_api(calls), marker_path=tmp_path / "power.json"
    )
    assert calls[-1][2] == {"full_refresh": False}
    assert power.full_refresh is False


def test_cli_without_durable_store_fails_closed(monkeypatch):
    pipeline_power.install_pipeline_power_store(None)

    async def no_store():
        return None

    monkeypatch.setattr(pipeline_power, "_coordination_store", lambda: None)
    with pytest.raises(pipeline_power.RebaseLedgerUnavailableError):
        asyncio.run(pipeline_power.install_cli_pipeline_power_store())


@pytest.mark.asyncio
async def test_no_repair_does_not_force_a_full_refresh(monkeypatch):
    _RepairingAdapter.table_rebuilt = True
    _install_rebase_store()
    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-normal", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    source = DeltaSource(_RepairingAdapter())
    await source.check_storage(R4_BASELINE.entity_id)
    assert source.repaired is False
    await _start(_lane(source))
    assert requested == [False]


def test_glue_ledger_still_applies_a_different_delta_table_id():
    from tests.test_round4_glue_writer import writer

    upsert, _tombstone = writer.ledger_statements("round4", "model_score_ledger")
    assert "delta_table_id IS DISTINCT FROM EXCLUDED.delta_table_id" in upsert


def test_prepare_never_compares_the_sync_cursor_with_the_source_head():
    """The repair obligation is carried, not inferred from the sync cursor and source head."""

    from server.round4_race import Round4RaceEngine

    prepare = inspect.getsource(Round4RaceEngine.prepare)
    module = inspect.getsource(round4_race_live)
    assert "inspect_sync" not in prepare
    assert "last_sync_delta_version" not in prepare
    assert "last_sync_delta_version" not in inspect.getsource(DeltaSource.check_storage)
    tree = ast.parse(textwrap.dedent(inspect.getsource(Round4RaceEngine.prepare)))
    calls = [
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]
    assert "inspect_sync" not in calls
    evidence = inspect.getsource(round4_race_live.LakebaseSyncLane.evidence)
    assert "inspect_sync" in evidence
    assert "inspect_sync" not in inspect.getsource(round4_race_live.LakebaseSyncLane.start)
    del module


@pytest.mark.asyncio
async def test_a_foreign_pipeline_or_power_intent_cannot_clear_rebase_owed():
    _RepairingAdapter.table_rebuilt = False
    table = _install_rebase_store()
    await _repair_then_discard_engine()
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True

    await pipeline_power.note_rebase_satisfied(FOREIGN_PIPELINE_ID, observed_event_id=1)
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True

    power = pipeline_power.DurablePipelinePowerStore(table.run)
    await power.append(
        {
            "intent": "stop_owed",
            "pipeline_id": PIPELINE_ID,
            "run_id": "foreign-bout",
            "owed_at": datetime.now(UTC).isoformat(),
            "owed_by": "stale-fence",
            "usd_per_day_owed": "1.00",
        }
    )
    await power.append(
        {
            "intent": "resuming",
            "pipeline_id": PIPELINE_ID,
            "run_id": "stale-bout",
            "resumed_at": datetime.now(UTC).isoformat(),
            "resumed_by": "stale-fence",
            "usd_per_day_resumed": "1.00",
        }
    )
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True


def test_cancel_expiry_fail_and_startup_cleanup_are_not_satisfy_writers():
    from server import manager, round4_stop_recovery

    for source in (
        inspect.getsource(manager.RunManager._cancel_armed_card),
        inspect.getsource(manager.RunManager._fail),
        inspect.getsource(round4_stop_recovery),
    ):
        assert "note_rebase_satisfied" not in source
        assert "rebase_satisfied" not in source


def test_power_intent_latest_sql_is_unchanged_by_this_batch():
    """Rebase history must not reorder resuming vs stop_owed."""

    source = inspect.getsource(pipeline_power.DurablePipelinePowerStore._power_history_rows)
    assert "rebase" not in source.lower()
    assert "ORDER BY event_id DESC" in source
    assert "LIMIT" in source


def test_rebase_latest_sql_orders_by_event_id_desc_with_limit():
    source = inspect.getsource(pipeline_power.DurablePipelinePowerStore.latest_rebase)
    assert "ORDER BY event_id DESC" in source
    assert "LIMIT 1" in source
    assert "SELECT event_id, intent" in source.replace("\n", " ")


CLOUD = {"path_missing": True, "repairs": 0}


def _table_payload(config):
    catalog, schema, name = config.source_table_full_name.split(".")
    return {
        "full_name": config.source_table_full_name,
        "catalog_name": catalog,
        "schema_name": schema,
        "name": name,
        "owner": config.setup_principal,
        "table_type": "MANAGED",
        "data_source_format": "DELTA",
        "columns": [
            {"name": "entity_id", "type_name": "STRING"},
            {"name": "score", "type_name": "DOUBLE"},
            {"name": "model_version", "type_name": "STRING"},
            {"name": "proof_nonce", "type_name": "STRING"},
            {"name": "updated_at", "type_name": "TIMESTAMP"},
        ],
        "table_id": "rebuilt-table-id",
        "storage_location": "s3://bucket/round4",
    }


def _transient(message="warehouse hiccup"):
    return WorkspaceStatementExecutionError(
        state="FAILED",
        error_code="TEMPORARILY_UNAVAILABLE",
        sql_state="XX000",
        provider_category="unknown",
        provider_message=message,
    )


def _live_repair_adapter(
    *, recheck_failure: str | None, repair: str = "succeed"
) -> LiveModelScoreAdapter:
    from dataclasses import replace

    config = replace(
        live_config(),
        pipeline_id=PIPELINE_ID,
        source_repair_job_id="123",
        source_repair_job_name="lakebase-anti-demo-owner-round4-source-repair",
        source_repair_notebook_path="/Shared/lakebase-anti-demo/owner/round4-source-repair",
    )
    adapter = LiveModelScoreAdapter(
        config,
        workspace_client=SimpleNamespace(api_client=None),
        statement_runner=SimpleNamespace(),
    )
    heads = {"calls": 0}

    async def get_json(_path):
        return _table_payload(config)

    async def source_head():
        heads["calls"] += 1
        if CLOUD["path_missing"]:
            raise DeltaPathDoesNotExistError(
                state="FAILED",
                error_code="BAD_REQUEST",
                sql_state="42000",
                provider_category="delta_path_does_not_exist",
                provider_message="[DELTA_PATH_DOES_NOT_EXIST]",
            )
        if recheck_failure == "head" and heads["calls"] == 2:
            raise _transient("DESCRIBE HISTORY failed right after the rebuild")
        return 1, datetime.now(UTC)

    async def run_source_repair():
        if repair == "cancel":
            raise asyncio.CancelledError()
        if repair == "die":
            raise RuntimeError("process died during the sealed repair job")
        CLOUD["repairs"] += 1
        CLOUD["path_missing"] = False

    async def cdf_enabled():
        if recheck_failure == "cdf":
            raise _transient("SHOW TBLPROPERTIES failed")
        return True

    async def read_source(_entity):
        if recheck_failure == "read":
            raise _transient("baseline SELECT failed")
        return R4_BASELINE

    adapter._get_json = get_json
    adapter._source_head = source_head
    adapter._run_source_repair = run_source_repair
    adapter._cdf_enabled = cdf_enabled
    adapter.read_source = read_source
    return adapter


@pytest.mark.asyncio
@pytest.mark.parametrize("how", ["cancel", "die"])
async def test_owed_is_recorded_before_repair_mutates_and_survives_death(how, monkeypatch):
    table = _install_rebase_store()
    CLOUD.update(path_missing=True, repairs=0)
    adapter = _live_repair_adapter(recheck_failure=None, repair=how)
    with pytest.raises((asyncio.CancelledError, RuntimeError)):
        await DeltaSource(adapter).check_storage(R4_BASELINE.entity_id)
    assert [row[1] for row in table.rows] == ["rebase_owed"]
    assert CLOUD["repairs"] == 0

    pipeline_power.install_pipeline_power_store(None)
    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(table.run)
    )
    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-after-death", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    CLOUD["path_missing"] = False
    later = DeltaSource(_live_repair_adapter(recheck_failure=None, repair="succeed"))
    await later.check_storage(R4_BASELINE.entity_id)
    await _start(_lane(later))
    assert requested == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("recheck_failure", ["head", "cdf", "read"])
async def test_failed_post_repair_recheck_still_owes_after_restart(
    recheck_failure, monkeypatch
):
    table = _install_rebase_store()
    CLOUD.update(path_missing=True, repairs=0)
    adapter = _live_repair_adapter(recheck_failure=recheck_failure)
    with pytest.raises(WorkspaceStatementExecutionError):
        await DeltaSource(adapter).check_storage(R4_BASELINE.entity_id)
    assert CLOUD["repairs"] == 1
    assert [row[1] for row in table.rows] == ["rebase_owed"]

    pipeline_power.install_pipeline_power_store(None)
    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(table.run)
    )
    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-after-recheck", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    later = DeltaSource(_live_repair_adapter(recheck_failure=None))
    await later.check_storage(R4_BASELINE.entity_id)
    await _start(_lane(later))
    assert requested == [True]


@pytest.mark.asyncio
async def test_failed_durable_owed_append_refuses_repair_and_blocks_incremental_start(
    monkeypatch,
):
    table = _FakeRebaseTable()
    table.fail_insert = True
    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(table.run)
    )
    CLOUD.update(path_missing=True, repairs=0)
    adapter = _live_repair_adapter(recheck_failure=None)
    with pytest.raises(pipeline_power.RebaseLedgerUnavailableError):
        await DeltaSource(adapter).check_storage(R4_BASELINE.entity_id)
    assert CLOUD["repairs"] == 0
    assert table.rows == []

    durable = _FakeRebaseTable()
    pipeline_power.install_pipeline_power_store(None)
    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(durable.run)
    )
    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-no-owed", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    CLOUD["path_missing"] = False
    later = DeltaSource(_live_repair_adapter(recheck_failure=None))
    await later.check_storage(R4_BASELINE.entity_id)
    await _start(_lane(later))
    assert requested == [False]


@pytest.mark.asyncio
async def test_durable_read_failure_fails_closed(monkeypatch):
    table = _FakeRebaseTable()
    table.rows.append(
        (PIPELINE_ID, "rebase_owed", None, datetime.now(UTC), "prior-process")
    )
    table.fail_read = True
    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(table.run)
    )
    with pytest.raises(pipeline_power.RebaseLedgerUnavailableError):
        await pipeline_power.rebase_is_owed(PIPELINE_ID)

    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id="update-read-failure", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    later = DeltaSource(_RepairingAdapter())
    _RepairingAdapter.table_rebuilt = True
    await later.check_storage(R4_BASELINE.entity_id)
    with pytest.raises(pipeline_power.RebaseLedgerUnavailableError):
        await _start(_lane(later))
    assert requested == []


@pytest.mark.asyncio
async def test_stale_start_cannot_satisfy_a_newer_owed_generation(monkeypatch):
    table = _install_rebase_store()
    first = await pipeline_power.note_rebase_owed(PIPELINE_ID, actor="first-repair")
    accepted = threading.Event()
    return_response = threading.Event()

    def delayed_start(*_args, full_refresh=False, **_kwargs):
        assert full_refresh is True
        accepted.set()
        assert return_response.wait(timeout=5)
        return SimpleNamespace(update_id="older-update", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", delayed_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    _RepairingAdapter.table_rebuilt = True
    stale = _lane(DeltaSource(_RepairingAdapter()))
    await stale.before_bell()
    start_task = asyncio.create_task(stale.start(_bell()))
    assert await asyncio.to_thread(accepted.wait, 5)
    second = await pipeline_power.note_rebase_owed(PIPELINE_ID, actor="newer-repair")
    assert second != first
    return_response.set()
    await start_task
    assert stale._rebase_event_id == first
    await _confirm_healthy_full_refresh(stale, "older-update")
    latest = await pipeline_power.DurablePipelinePowerStore(table.run).latest_rebase(
        PIPELINE_ID
    )
    assert latest is not None
    assert latest["intent"] == "rebase_owed"
    assert latest["event_id"] == second


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["FAILED", "CANCELED"])
async def test_accepted_full_refresh_that_later_ends_keeps_owed(ending, monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    await _repair_then_discard_engine()
    requested: list[bool] = []

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        requested.append(full_refresh)
        return SimpleNamespace(update_id=f"update-{len(requested)}", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    lane = _lane(second)
    await _start(lane)
    assert requested == [True]

    async def signals():
        return _terminal_signals("update-1", ending)

    lane._signals = signals
    message = await lane.failure()
    assert message is not None
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True

    third = DeltaSource(_RepairingAdapter())
    await third.check_storage(R4_BASELINE.entity_id)
    await _start(_lane(third))
    assert requested == [True, True]


class _ExistingInstallCursor:
    def __init__(self, present=("round4_pipeline_power",)):
        self.present = present
        self.ddl: list[str] = []
        self._rows: list[tuple] = []

    async def execute(self, sql, params=None):
        if "pg_class" in sql:
            wanted = set(params[1])
            self._rows = [(name,) for name in self.present if name in wanted]
            return
        if "pg_namespace" in sql:
            self._rows = [(1,)]
            return
        if "CREATE" in sql:
            self.ddl.append(" ".join(sql.split())[:70])
            raise psycopg.errors.InsufficientPrivilege(
                "permission denied for schema anti_demo_coordination"
            )
        raise AssertionError(sql[:80])

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return list(self._rows)


class _UngrantedCursor:
    def __init__(self, privileges=(False, False, False, True, True, True)):
        self._rows = []
        self.privileges = privileges

    async def execute(self, sql, params=None):
        if "pg_class" in sql:
            self._rows = [(n,) for n in params[1]]
            return
        if "pg_namespace" in sql:
            self._rows = [(1,)]
            return
        if "has_table_privilege" in sql or "has_sequence_privilege" in sql:
            self._rows = [self.privileges]
            return
        if "round4_source_rebase" in sql:
            raise psycopg.errors.InsufficientPrivilege(
                "permission denied for table round4_source_rebase"
            )
        raise AssertionError(sql[:80])

    async def fetchone(self):
        return self._rows[0] if self._rows else None

    async def fetchall(self):
        return list(self._rows)


def _runner(cursor):
    async def run(operation):
        return await operation(cursor)

    return run


@pytest.mark.asyncio
async def test_existing_install_missing_rebase_table_fails_initialize_and_boot():
    import app as app_module
    from server.coordination import CoordinationObjectsMissingError

    cursor = _ExistingInstallCursor()
    store = pipeline_power.DurablePipelinePowerStore(_runner(cursor))
    with pytest.raises(CoordinationObjectsMissingError) as caught:
        await store.initialize()
    message = str(caught.value)
    assert "round4_source_rebase" in message
    assert "coordination-upgrade" in message
    assert "resets database lanes" in message
    assert "cursor.ddl" not in message
    assert "provisions it" not in message

    class Lease:
        mode = "lakebase"

        def __init__(self) -> None:
            self._run = _runner(_ExistingInstallCursor())

    with pytest.raises(CoordinationObjectsMissingError):
        await app_module._open_pipeline_power_store(Lease())


@pytest.mark.asyncio
async def test_present_but_ungranted_rebase_table_fails_initialize_and_boot():
    import app as app_module
    from server.coordination import CoordinationObjectsMissingError

    store = pipeline_power.DurablePipelinePowerStore(_runner(_UngrantedCursor()))
    with pytest.raises(CoordinationObjectsMissingError, match="round4_source_rebase"):
        await store.initialize()

    class Lease:
        mode = "lakebase"

        def __init__(self) -> None:
            self._run = _runner(_UngrantedCursor())

    with pytest.raises(CoordinationObjectsMissingError):
        await app_module._open_pipeline_power_store(Lease())


@pytest.mark.asyncio
async def test_select_only_rebase_table_fails_initialize():
    from server.coordination import CoordinationObjectsMissingError

    cursor = _UngrantedCursor(privileges=(True, False, True, True, True, True))
    store = pipeline_power.DurablePipelinePowerStore(_runner(cursor))
    with pytest.raises(CoordinationObjectsMissingError, match="INSERT"):
        await store.initialize()


@pytest.mark.asyncio
async def test_missing_rebase_sequence_usage_fails_initialize():
    from server.coordination import CoordinationObjectsMissingError

    cursor = _UngrantedCursor(privileges=(True, True, False, True, True, True))
    store = pipeline_power.DurablePipelinePowerStore(_runner(cursor))
    with pytest.raises(CoordinationObjectsMissingError, match="USAGE"):
        await store.initialize()


@pytest.mark.asyncio
async def test_one_healthy_sample_does_not_clear_owed(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    await _repair_then_discard_engine()

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        return SimpleNamespace(update_id="update-1", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    lane = _lane(second)
    await _start(lane)

    async def signals():
        return _healthy_signals("update-1")

    lane._signals = signals
    assert await lane.failure() is None
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True
    assert await lane.failure() is None
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is False


@pytest.mark.asyncio
async def test_healthy_then_failed_keeps_owed(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    await _repair_then_discard_engine()

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        return SimpleNamespace(update_id="update-1", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    lane = _lane(second)
    await _start(lane)
    reads = {"n": 0}

    async def signals():
        reads["n"] += 1
        if reads["n"] == 1:
            return _healthy_signals("update-1")
        return _terminal_signals("update-1", "FAILED")

    lane._signals = signals
    assert await lane.failure() is None
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True
    message = await lane.failure()
    assert message is not None and "failed" in message
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True


@pytest.mark.asyncio
async def test_reowe_failure_is_returned_as_lane_failure(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    await _repair_then_discard_engine()

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        return SimpleNamespace(update_id="update-1", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    lane = _lane(second)
    await _start(lane)

    async def boom(*_a, **_k):
        raise pipeline_power.RebaseLedgerUnavailableError("coordination write unavailable")

    monkeypatch.setattr(pipeline_power, "note_rebase_owed", boom)

    async def signals():
        return _terminal_signals("update-1", "FAILED")

    lane._signals = signals
    message = await lane.failure()
    assert message is not None
    assert "could not re-record" in message


@pytest.mark.asyncio
async def test_bell_start_reads_ledger_once_on_the_app_loop(monkeypatch, tmp_path):
    reads: list[dict] = []
    main_thread = threading.get_ident()
    main_loop = id(asyncio.get_running_loop())

    class Cursor:
        def __init__(self) -> None:
            self._rows: list[tuple] = []

        async def execute(self, sql: str, params: tuple = ()) -> None:
            if "round4_source_rebase" in sql and "SELECT" in sql and "INSERT" not in sql:
                loop = asyncio.get_running_loop()
                reads.append(
                    {
                        "thread": threading.get_ident() == main_thread,
                        "loop": id(loop),
                    }
                )
            self._rows = []

        async def fetchall(self):
            return list(self._rows)

        async def fetchone(self):
            return None

    async def run(operation):
        return await operation(Cursor())

    pipeline_power.install_pipeline_power_store(
        pipeline_power.DurablePipelinePowerStore(run)
    )
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda manifest_file=None: tmp_path / "s.json"
    )
    manifest = make_manifest()
    attach_round4(manifest)
    posts: list[dict | None] = []

    def api(profile, method, path, *, body=None, timeout=600):
        del profile, timeout
        if method == "post":
            posts.append(body)
        return {"update_id": "update-bell"} if path.endswith("/updates") else {}

    _RepairingAdapter.table_rebuilt = True
    lane = _lane(DeltaSource(_RepairingAdapter()))
    lane._manifest = manifest
    lane._api = api
    lane._pipeline_id = manifest.round4.pipeline_id
    await _start(lane)
    assert posts == [{"full_refresh": False}]
    assert len(reads) == 1
    assert reads[0]["thread"] is True
    assert reads[0]["loop"] == main_loop


@pytest.mark.asyncio
async def test_event_loop_start_without_explicit_refresh_fails_closed():
    _install_rebase_store()
    manifest = make_manifest()
    attach_round4(manifest)
    with pytest.raises(pipeline_power.RebaseLedgerUnavailableError, match="explicit"):
        pipeline_power.start(manifest, _recording_api([]))


@pytest.mark.asyncio
async def test_activation_after_restarted_prepare_honors_durable_owed(
    monkeypatch, tmp_path
):
    from tests.test_model_score_live import FakePipelineApi, _activation

    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    await _repair_then_discard_engine()
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda manifest_file=None: tmp_path / "s.json"
    )
    api = FakePipelineApi(running=True)

    def tracking_api(profile, method, path, *, body=None, timeout=600):
        result = api(profile, method, path, body=body, timeout=timeout)
        if method == "get" and "/pipelines/" in path and api.update_bodies:
            result = dict(result)
            result["latest_updates"] = [{"state": "RUNNING", "update_id": "update-1"}]
        return result

    activation = _activation(tracking_api)
    await activation.ensure_running(lambda *_a, **_k: asyncio.sleep(0))
    assert api.update_bodies == [{"full_refresh": True}]
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is False


def test_lifecycle_sealed_start_consults_durable_owed(monkeypatch, tmp_path):
    from server import lifecycle

    manifest = make_manifest()
    attach_round4(manifest)
    _install_rebase_store()
    asyncio.run(pipeline_power.note_rebase_owed(manifest.round4.pipeline_id))
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda manifest_file=None: tmp_path / "s.json"
    )

    async def already_installed() -> None:
        return None

    monkeypatch.setattr(
        pipeline_power, "install_cli_pipeline_power_store", already_installed
    )
    calls: list[tuple[str, str, dict | None]] = []
    monkeypatch.setattr(lifecycle, "_databricks_api", _recording_api(calls))
    started = lifecycle._round4_pipeline_verb(
        manifest, manifest.round4.pipeline_id, "start"
    )
    assert started.full_refresh is True
    assert calls[-1][2] == {"full_refresh": True}


def test_cli_command_path_waits_and_satisfies(monkeypatch, tmp_path, capsys):
    from contextlib import nullcontext

    from server import cli

    manifest = make_manifest()
    attach_round4(manifest)
    table = _FakeRebaseTable()
    store = pipeline_power.DurablePipelinePowerStore(table.run)
    asyncio.run(store.append_rebase(manifest.round4.pipeline_id, "rebase_owed"))
    pipeline_power.install_pipeline_power_store(store)
    monkeypatch.setattr("server.manifest.load_manifest", lambda: manifest)
    monkeypatch.setattr(cli, "_mutating", lambda *_a, **_k: nullcontext())

    async def already_installed() -> None:
        return None

    monkeypatch.setattr(
        pipeline_power, "install_cli_pipeline_power_store", already_installed
    )
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda manifest_file=None: tmp_path / "s.json"
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api",
        lambda profile, method, path, *, body=None, timeout=600: {"update_id": "update-1"}
        if method == "post" and str(path).endswith("/updates")
        else {},
    )

    original_wait = pipeline_power.wait_and_settle_full_refresh

    async def inspect_ok() -> tuple[str, str, bool]:
        return ("update-1", "RUNNING", True)

    async def wait_fast(*args, **kwargs):
        kwargs["inspect"] = inspect_ok
        kwargs["poll_seconds"] = 0.01
        kwargs["timeout_seconds"] = 2
        return await original_wait(*args, **kwargs)

    monkeypatch.setattr(pipeline_power, "wait_and_settle_full_refresh", wait_fast)
    code = cli._pipeline_power("start")
    captured = capsys.readouterr()
    assert code == 0
    assert "full refresh" in captured.out
    assert asyncio.run(pipeline_power.rebase_is_owed(manifest.round4.pipeline_id)) is False


def test_cli_command_path_failed_update_stays_owed(monkeypatch, tmp_path, capsys):
    from contextlib import nullcontext

    from server import cli

    manifest = make_manifest()
    attach_round4(manifest)
    store = pipeline_power.DurablePipelinePowerStore(_FakeRebaseTable().run)
    asyncio.run(store.append_rebase(manifest.round4.pipeline_id, "rebase_owed"))
    pipeline_power.install_pipeline_power_store(store)
    monkeypatch.setattr("server.manifest.load_manifest", lambda: manifest)
    monkeypatch.setattr(cli, "_mutating", lambda *_a, **_k: nullcontext())

    async def already_installed() -> None:
        return None

    monkeypatch.setattr(
        pipeline_power, "install_cli_pipeline_power_store", already_installed
    )
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda manifest_file=None: tmp_path / "s.json"
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api",
        lambda profile, method, path, *, body=None, timeout=600: {"update_id": "update-1"}
        if method == "post" and str(path).endswith("/updates")
        else {},
    )

    original_wait = pipeline_power.wait_and_settle_full_refresh

    async def inspect_failed() -> tuple[str, str, bool]:
        return ("update-1", "FAILED", False)

    async def wait_fast(*args, **kwargs):
        kwargs["inspect"] = inspect_failed
        kwargs["poll_seconds"] = 0.01
        kwargs["timeout_seconds"] = 2
        return await original_wait(*args, **kwargs)

    monkeypatch.setattr(pipeline_power, "wait_and_settle_full_refresh", wait_fast)
    code = cli._pipeline_power("start")
    captured = capsys.readouterr()
    assert code == 1
    assert "did not stay healthy" in captured.out
    assert asyncio.run(pipeline_power.rebase_is_owed(manifest.round4.pipeline_id)) is True


def test_coordination_upgrade_is_a_cli_command():
    from server.cli import _parser

    parser = _parser()
    args = parser.parse_args(["coordination-upgrade"])
    assert args.command == "coordination-upgrade"


def test_missing_table_messages_name_coordination_upgrade_not_setup_resume():
    from pathlib import Path

    from server import lifecycle

    power = Path("server/pipeline_power.py").read_text(encoding="utf-8")
    create_error = power.split("except psycopg.errors.InsufficientPrivilege as exc:")[1]
    create_error = create_error.split("await self._probe_runtime_privileges")[0]
    assert "coordination-upgrade" in create_error
    assert "resets database lanes" in create_error
    assert "cursor.ddl" not in create_error
    assert not _claims_renew_provisions(create_error)
    grant = inspect.getsource(lifecycle._grant_round4_postgres)
    assert "coordination-upgrade" in grant
    assert "resets database lanes" in grant
    assert "cursor.ddl" not in grant
    footer = Path("bootstrap.sh").read_text(encoding="utf-8")
    block = footer.split("Last 60 log lines:", 1)[1][:2000]
    assert "./antidemo coordination-upgrade" in block
    assert "resets both database lanes" in block
    assert "cursor.ddl" not in block
    warning = inspect.getsource(pipeline_power._durable_ledger)
    assert "coordination-upgrade" in warning
    assert not _claims_renew_provisions(warning)
    owed = Path("tests/test_round4_rebase_owed.py").read_text(encoding="utf-8")
    assert ("or " + "cursor.ddl") not in owed


def test_lifecycle_pipeline_signals_carry_the_newest_update_id(monkeypatch):
    from server import lifecycle

    monkeypatch.setattr(
        lifecycle,
        "_round4_get_pipeline",
        lambda *_a, **_k: {
            "state": "RUNNING",
            "latest_updates": [{"state": "RUNNING", "update_id": "update-accepted"}],
        },
    )
    monkeypatch.setattr(
        lifecycle,
        "_round4_get_database_synced_table",
        lambda *_a, **_k: {
            "data_synchronization_status": {
                "detailed_state": "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
                "continuous_update_status": {"last_processed_commit_version": 1},
            }
        },
    )
    signals = lifecycle._round4_pipeline_signals(
        make_manifest(), {"synced_table_id": "t"}, "pipe-1"
    )
    assert signals.update_id == "update-accepted"


def test_lifecycle_start_satisfies_the_accepted_update_id(monkeypatch):
    from server import lifecycle

    satisfied: list[tuple] = []

    async def satisfy(pipeline_id, *, observed_event_id=None, actor=""):
        satisfied.append((pipeline_id, observed_event_id, actor))

    monkeypatch.setattr(pipeline_power, "note_rebase_satisfied", satisfy)
    monkeypatch.setattr(
        lifecycle,
        "_round4_pipeline_verb",
        lambda *_a, **_k: SimpleNamespace(
            full_refresh=True, update_id="update-accepted", rebase_event_id=11
        ),
    )
    monkeypatch.setattr(
        lifecycle,
        "_round4_get_pipeline",
        lambda *_a, **_k: {"latest_updates": [{"state": "RUNNING"}]},
    )
    samples = [_healthy_signals("update-accepted"), _healthy_signals("update-accepted")]
    monkeypatch.setattr(
        lifecycle,
        "_round4_pipeline_signals",
        lambda *_a, **_k: samples.pop(0) if samples else _healthy_signals("update-accepted"),
    )
    monkeypatch.setattr(lifecycle.time, "sleep", lambda _s: None)
    monkeypatch.setattr(lifecycle.time, "monotonic", lambda: 0.0)
    lifecycle._start_round4_pipeline(make_manifest(), {"synced_table_id": "t"}, "pipe-setup")
    assert satisfied == [("pipe-setup", 11, "pipeline-start")]


def test_lifecycle_start_reowes_the_accepted_failed_update(monkeypatch):
    from server import lifecycle

    reowed: list[tuple] = []

    async def owed(pipeline_id, *, actor=""):
        reowed.append((pipeline_id, actor))

    monkeypatch.setattr(pipeline_power, "note_rebase_owed", owed)
    monkeypatch.setattr(
        lifecycle,
        "_round4_pipeline_verb",
        lambda *_a, **_k: SimpleNamespace(
            full_refresh=True, update_id="update-accepted", rebase_event_id=11
        ),
    )
    monkeypatch.setattr(
        lifecycle,
        "_round4_get_pipeline",
        lambda *_a, **_k: {"latest_updates": [{"state": "RUNNING"}]},
    )
    monkeypatch.setattr(
        lifecycle,
        "_round4_pipeline_signals",
        lambda *_a, **_k: _terminal_signals("update-accepted", "FAILED"),
    )
    monkeypatch.setattr(lifecycle.time, "sleep", lambda _s: None)
    monkeypatch.setattr(lifecycle.time, "monotonic", lambda: 0.0)
    with pytest.raises(RuntimeError, match="FAILED"):
        lifecycle._start_round4_pipeline(make_manifest(), {"synced_table_id": "t"}, "pipe-setup")
    assert reowed == [("pipe-setup", "full-refresh-failed")]


def _script_lifecycle_start(monkeypatch, *, full_refresh: bool, samples, clock=None):
    from server import lifecycle

    satisfied: list[tuple] = []
    reowed: list[tuple] = []

    async def satisfy(pipeline_id, *, observed_event_id=None, actor=""):
        satisfied.append((pipeline_id, observed_event_id, actor))

    async def owed(pipeline_id, *, actor=""):
        reowed.append((pipeline_id, actor))

    monkeypatch.setattr(pipeline_power, "note_rebase_satisfied", satisfy)
    monkeypatch.setattr(pipeline_power, "note_rebase_owed", owed)
    monkeypatch.setattr(
        lifecycle,
        "_round4_pipeline_verb",
        lambda *_a, **_k: SimpleNamespace(
            full_refresh=full_refresh,
            update_id="update-accepted",
            rebase_event_id=11 if full_refresh else None,
        ),
    )
    monkeypatch.setattr(
        lifecycle,
        "_round4_get_pipeline",
        lambda *_a, **_k: {"latest_updates": [{"state": "RUNNING"}]},
    )
    leftover = list(samples)
    last = {"signals": leftover[0]}

    def signals(*_a, **_k):
        if leftover:
            last["signals"] = leftover.pop(0)
        return last["signals"]

    monkeypatch.setattr(lifecycle, "_round4_pipeline_signals", signals)
    if clock is not None:

        def sleep(seconds: float) -> None:
            clock["t"] += seconds

        monkeypatch.setattr(lifecycle.time, "monotonic", lambda: clock["t"])
        monkeypatch.setattr(lifecycle.time, "sleep", sleep)
    else:
        monkeypatch.setattr(lifecycle.time, "monotonic", lambda: 0.0)
        monkeypatch.setattr(lifecycle.time, "sleep", lambda _s: None)
    return satisfied, reowed


def test_lifecycle_incremental_start_does_not_succeed_on_foreign_healthies(monkeypatch):
    from server import lifecycle

    clock = {"t": 0.0}
    satisfied, reowed = _script_lifecycle_start(
        monkeypatch,
        full_refresh=False,
        samples=[_healthy_signals("foreign"), _healthy_signals("foreign")],
        clock=clock,
    )
    monkeypatch.setattr(lifecycle, "_ROUND4_PIPELINE_POWER_TIMEOUT_SECONDS", 5)

    def sleep(seconds):
        clock["t"] += 10

    monkeypatch.setattr(lifecycle.time, "sleep", sleep)
    with pytest.raises(RuntimeError, match="did not reach"):
        lifecycle._start_round4_pipeline(make_manifest(), {"synced_table_id": "t"}, "pipe-setup")
    assert satisfied == []
    assert reowed == []


@pytest.mark.parametrize("full_refresh", [True, False])
def test_lifecycle_mixed_update_waits_for_two_consecutive_accepted(
    monkeypatch, full_refresh
):
    from server import lifecycle

    samples = [
        _healthy_signals("update-accepted"),
        _healthy_signals("foreign"),
        _healthy_signals("update-accepted"),
        _healthy_signals("update-accepted"),
    ]
    satisfied, reowed = _script_lifecycle_start(
        monkeypatch, full_refresh=full_refresh, samples=samples
    )
    lifecycle._start_round4_pipeline(make_manifest(), {"synced_table_id": "t"}, "pipe-setup")
    if full_refresh:
        assert satisfied == [("pipe-setup", 11, "pipeline-start")]
    else:
        assert satisfied == []
    assert reowed == []


def _drive_unsealed_lifecycle_start(
    monkeypatch, *, samples, clock=None, post_update_id="accepted-by-post"
):
    """Start through the real unsealed verb; do not mock it closed."""

    from server import lifecycle

    manifest = make_manifest()
    assert manifest.round4 is None
    posts: list[tuple] = []
    polls = {"n": 0}
    leftover = list(samples)
    last = {"signals": leftover[0] if leftover else _healthy_signals(post_update_id)}
    satisfied: list[tuple] = []
    reowed: list[tuple] = []

    def api(profile, method, path, *, body=None, timeout=600):
        del profile, timeout
        posts.append((method, path, body))
        return {"update_id": post_update_id}

    def signals(*_a, **_k):
        polls["n"] += 1
        if leftover:
            last["signals"] = leftover.pop(0)
        return last["signals"]

    async def satisfy(pipeline_id, *, observed_event_id=None, actor=""):
        satisfied.append((pipeline_id, observed_event_id, actor))

    async def owed(pipeline_id, *, actor=""):
        reowed.append((pipeline_id, actor))

    monkeypatch.setattr(pipeline_power, "note_rebase_satisfied", satisfy)
    monkeypatch.setattr(pipeline_power, "note_rebase_owed", owed)
    monkeypatch.setattr(lifecycle, "_databricks_api", api)
    monkeypatch.setattr(
        lifecycle,
        "_round4_get_pipeline",
        lambda *_a, **_k: {"latest_updates": [{"state": "RUNNING"}]},
    )
    monkeypatch.setattr(lifecycle, "_round4_pipeline_signals", signals)
    if clock is not None:

        def sleep(seconds: float) -> None:
            clock["t"] += seconds

        monkeypatch.setattr(lifecycle.time, "monotonic", lambda: clock["t"])
        monkeypatch.setattr(lifecycle.time, "sleep", sleep)
    else:
        monkeypatch.setattr(lifecycle.time, "monotonic", lambda: 0.0)
        monkeypatch.setattr(lifecycle.time, "sleep", lambda _s: None)
    return manifest, posts, polls, leftover, satisfied, reowed


def test_unsealed_verb_returns_the_posted_update_id(monkeypatch):
    from server import lifecycle

    manifest = make_manifest()
    assert manifest.round4 is None
    calls: list[tuple] = []

    def api(profile, method, path, *, body=None, timeout=600):
        del profile, timeout
        calls.append((method, path, body))
        return {"update_id": "accepted-by-post"}

    monkeypatch.setattr(lifecycle, "_databricks_api", api)
    started = lifecycle._round4_pipeline_verb(manifest, "pipe-unsealed", "start")
    assert started is not None
    assert started.update_id == "accepted-by-post"
    assert started.full_refresh is False
    assert calls[-1][1].endswith("/updates")
    assert calls[-1][2] == {"full_refresh": False}


def test_unsealed_verb_raises_when_the_post_has_no_update_id(monkeypatch):
    from server import lifecycle

    monkeypatch.setattr(
        lifecycle, "_databricks_api", lambda *_a, **_k: {"state": "RUNNING"}
    )
    with pytest.raises(RuntimeError, match="no update identity"):
        lifecycle._round4_pipeline_verb(make_manifest(), "pipe-unsealed", "start")


def test_unsealed_start_does_not_succeed_on_foreign_healthies(monkeypatch):
    from server import lifecycle

    clock = {"t": 0.0}
    manifest, posts, polls, leftover, satisfied, reowed = _drive_unsealed_lifecycle_start(
        monkeypatch,
        samples=[_healthy_signals("foreign-1"), _healthy_signals("foreign-2")],
        clock=clock,
    )
    monkeypatch.setattr(lifecycle, "_ROUND4_PIPELINE_POWER_TIMEOUT_SECONDS", 5)

    def sleep(seconds):
        clock["t"] += 10

    monkeypatch.setattr(lifecycle.time, "sleep", sleep)
    with pytest.raises(RuntimeError, match="did not reach"):
        lifecycle._start_round4_pipeline(manifest, {"synced_table_id": "t"}, "pipe-unsealed")
    assert posts[-1][2] == {"full_refresh": False}
    assert leftover == []
    assert polls["n"] >= 2
    assert satisfied == []
    assert reowed == []


def test_unsealed_start_succeeds_on_two_consecutive_accepted(monkeypatch):
    from server import lifecycle

    manifest, posts, polls, leftover, satisfied, reowed = _drive_unsealed_lifecycle_start(
        monkeypatch,
        samples=[
            _healthy_signals("accepted-by-post"),
            _healthy_signals("accepted-by-post"),
        ],
    )
    lifecycle._start_round4_pipeline(manifest, {"synced_table_id": "t"}, "pipe-unsealed")
    assert posts[-1][2] == {"full_refresh": False}
    assert leftover == []
    assert polls["n"] == 2
    assert satisfied == []
    assert reowed == []


def test_unsealed_mixed_update_settles_on_the_fourth_sample(monkeypatch):
    from server import lifecycle

    manifest, posts, polls, leftover, satisfied, reowed = _drive_unsealed_lifecycle_start(
        monkeypatch,
        samples=[
            _healthy_signals("accepted-by-post"),
            _healthy_signals("foreign"),
            _healthy_signals("accepted-by-post"),
            _healthy_signals("accepted-by-post"),
        ],
    )
    lifecycle._start_round4_pipeline(manifest, {"synced_table_id": "t"}, "pipe-unsealed")
    assert posts[-1][2] == {"full_refresh": False}
    assert leftover == []
    assert polls["n"] == 4
    assert satisfied == []
    assert reowed == []


def test_unsealed_start_fails_on_accepted_failed(monkeypatch):
    from server import lifecycle

    manifest, posts, polls, leftover, satisfied, reowed = _drive_unsealed_lifecycle_start(
        monkeypatch,
        samples=[_terminal_signals("accepted-by-post", "FAILED")],
    )
    with pytest.raises(RuntimeError, match="FAILED"):
        lifecycle._start_round4_pipeline(manifest, {"synced_table_id": "t"}, "pipe-unsealed")
    assert posts[-1][2] == {"full_refresh": False}
    assert leftover == []
    assert polls["n"] == 1
    assert satisfied == []
    assert reowed == []


def test_durable_power_read_warning_names_coordination_upgrade_not_renew(
    caplog, monkeypatch
):
    class Boom:
        async def latest(self, pipeline_id):
            raise RuntimeError("missing table")

    pipeline_power.install_pipeline_power_store(Boom())
    with caplog.at_level("WARNING"):
        assert pipeline_power._durable_ledger("pipe-1") is None
    text = caplog.text
    assert "coordination-upgrade" in text
    assert not _claims_renew_provisions(text)
    assert "cursor.ddl" not in text
    assert "renew does not provision" in text


@pytest.mark.asyncio
async def test_wait_and_settle_does_not_satisfy_across_a_foreign_update():
    _install_rebase_store()
    await pipeline_power.note_rebase_owed(PIPELINE_ID)
    seq = [
        ("update-1", "RUNNING", True),
        ("foreign", "RUNNING", True),
        ("update-1", "RUNNING", True),
    ]
    n = {"i": 0}

    async def inspect():
        if n["i"] < len(seq):
            sample = seq[n["i"]]
            n["i"] += 1
            return sample
        return ("foreign", "RUNNING", True)

    with pytest.raises(PipelinePowerError, match="two consecutive"):
        await pipeline_power.wait_and_settle_full_refresh(
            PIPELINE_ID,
            "update-1",
            1,
            inspect=inspect,
            timeout_seconds=0.05,
            poll_seconds=0.01,
        )
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True


@pytest.mark.asyncio
async def test_wait_and_settle_satisfies_two_consecutive_accepted_after_a_gap():
    _install_rebase_store()
    event_id = await pipeline_power.note_rebase_owed(PIPELINE_ID)
    seq = [
        ("update-1", "RUNNING", True),
        ("foreign", "RUNNING", True),
        ("update-1", "RUNNING", True),
        ("update-1", "RUNNING", True),
    ]

    async def inspect():
        return seq.pop(0)

    await pipeline_power.wait_and_settle_full_refresh(
        PIPELINE_ID,
        "update-1",
        event_id,
        inspect=inspect,
        timeout_seconds=2,
        poll_seconds=0.01,
    )
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is False


@pytest.mark.asyncio
async def test_bell_mixed_update_streak_does_not_clear_owed(monkeypatch):
    _RepairingAdapter.table_rebuilt = False
    _install_rebase_store()
    await _repair_then_discard_engine()

    def fake_start(_manifest, _api, *, full_refresh=False, on_record=None, **_kwargs):
        return SimpleNamespace(update_id="update-1", resuming_since="")

    monkeypatch.setattr(pipeline_power, "start", fake_start)
    monkeypatch.setattr(pipeline_power, "owed_stop_record", lambda *_a, **_k: {})
    second = DeltaSource(_RepairingAdapter())
    await second.check_storage(R4_BASELINE.entity_id)
    lane = _lane(second)
    await _start(lane)
    seq = [
        _healthy_signals("update-1"),
        _healthy_signals("foreign"),
        _healthy_signals("update-1"),
    ]

    async def signals():
        return seq.pop(0)

    lane._signals = signals
    assert await lane.failure() is None
    assert await lane.failure() is None
    assert await lane.failure() is None
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is True


@pytest.mark.asyncio
async def test_activation_mixed_update_requires_two_consecutive_accepted(
    monkeypatch, tmp_path
):
    from tests.test_model_score_live import FakePipelineApi, _activation

    _install_rebase_store()
    await pipeline_power.note_rebase_owed(PIPELINE_ID)
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda manifest_file=None: tmp_path / "s.json"
    )
    api = FakePipelineApi(running=False)
    activation = _activation(api, wait_timeout_seconds=5)
    reads = [
        _healthy_signals("update-1"),
        _healthy_signals("foreign"),
        _healthy_signals("update-1"),
        _healthy_signals("update-1"),
    ]
    seen: list[str] = []

    async def scripted():
        signals = reads.pop(0)
        seen.append(signals.update_id)
        return signals

    activation._read_signals = scripted
    await activation._start_and_wait(
        _terminal_signals("old", "CANCELED"),
        lambda *_a, **_k: asyncio.sleep(0),
        by_arm=True,
    )
    assert seen == ["update-1", "foreign", "update-1", "update-1"]
    assert await pipeline_power.rebase_is_owed(PIPELINE_ID) is False


@pytest.mark.asyncio
async def test_activation_start_does_not_run_on_the_event_loop_thread(
    monkeypatch, tmp_path
):
    from tests.test_model_score_live import FakePipelineApi, _activation

    _install_rebase_store()
    monkeypatch.setattr(
        pipeline_power, "stop_marker_path", lambda manifest_file=None: tmp_path / "s.json"
    )
    loop_thread = threading.get_ident()
    seen: list[bool] = []
    real_start = pipeline_power.start

    def wrapped(*args, **kwargs):
        seen.append(threading.get_ident() == loop_thread)
        return real_start(*args, **kwargs)

    monkeypatch.setattr(pipeline_power, "start", wrapped)
    api = FakePipelineApi(running=False)
    activation = _activation(api)
    await activation.ensure_running(lambda *_a, **_k: asyncio.sleep(0))
    assert seen == [False]
