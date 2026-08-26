from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server import model_score_live
from server.model_score import (
    DeltaCommit,
    ManagedSyncState,
    ManagedSyncStatus,
    ModelScoreAdapter,
    ModelScoreContract,
    ModelScoreEngine,
    ModelScoreRow,
    ModelScoreUpdate,
)
from server.model_score_live import (
    LiveModelScoreAdapter,
    ModelScoreLiveConfig,
    ModelScoreLiveConfigurationError,
    ModelScoreLiveOperationError,
    SqlParameter,
)

NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
_COMMITTED_AT = NOW - timedelta(seconds=2)


def live_config() -> ModelScoreLiveConfig:
    return ModelScoreLiveConfig(
        profile="demo",
        warehouse_id="warehouse-1",
        setup_principal="operator@databricks.com",
        source_table_full_name="owned.round4.model_scores",
        storage_catalog="owned",
        storage_schema="round4_storage",
        synced_table_resource_name="synced_tables/storage.round4.model_scores",
        synced_table_id="storage.round4.model_scores",
        synced_table_uid="uid-1",
        pipeline_id="pipeline-1",
        physical_database="anti_demo",
        physical_schema="public",
        physical_table="model_scores",
        project_uid="project-uid-1",
        branch_uid="branch-uid-1",
        branch="projects/project-1/branches/round4",
        endpoint_name="projects/project-1/branches/round4/endpoints/primary",
    )


def postgres_synced_table_payload(*, state: str = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"):
    return {
        "name": "synced_tables/storage.round4.model_scores",
        "synced_table_id": "storage.round4.model_scores",
        "uid": "uid-1",
        "status": {
            "project": "projects/project-1",
            "pipeline_id": "pipeline-1",
            "detailed_state": state,
            "last_processed_commit_version": 11,
            "message": "healthy",
            "last_sync": {
                "sync_end_time": "2026-08-18T11:59:58.500000Z",
                "delta_table_sync_info": {
                    "delta_commit_version": 11,
                    "delta_commit_time": "2026-08-18T11:59:58Z",
                },
            },
        },
    }


def database_synced_table_payload(
    *,
    state: str = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE",
    processed_version: int = 12,
    delta_version: int = 12,
):
    return {
        "name": "storage.round4.model_scores",
        "effective_database_project_id": "project-uid-1",
        "effective_database_branch_id": "branch-uid-1",
        "effective_logical_database_name": "anti_demo",
        "spec": {
            "source_table_full_name": "owned.round4.model_scores",
            "primary_key_columns": ["entity_id"],
            "scheduling_policy": "CONTINUOUS",
        },
        "data_synchronization_status": {
            "pipeline_id": "pipeline-1",
            "detailed_state": state,
            "message": "healthy",
            "continuous_update_status": {
                "last_processed_commit_version": processed_version,
                "timestamp": "2026-08-18T11:59:59.500000Z",
            },
            "last_sync": {
                "sync_start_timestamp": "2026-08-18T11:59:58.750000Z",
                "sync_end_timestamp": "2026-08-18T11:59:59.500000Z",
                "delta_table_sync_info": {
                    "delta_commit_version": delta_version,
                    "delta_commit_timestamp": "2026-08-18T11:59:59Z",
                },
            },
        },
    }


def project_payload():
    return {
        "name": "projects/project-1",
        "project_id": "project-1",
        "uid": "project-uid-1",
        "status": {"project_id": "project-1"},
    }


def branch_payload():
    return {
        "name": "projects/project-1/branches/round4",
        "parent": "projects/project-1",
        "branch_id": "round4",
        "uid": "branch-uid-1",
        "status": {"branch_id": "round4", "current_state": "READY"},
    }


def pipeline_payload(state="RUNNING", latest_updates=None):
    return {
        "pipeline_id": "pipeline-1",
        "name": "managed-sync-pipeline",
        "creator_user_name": "operator@databricks.com",
        "state": state,
        "latest_updates": (
            [{"update_id": "update-1", "state": "RUNNING"}]
            if latest_updates is None
            else latest_updates
        ),
        "spec": {
            "catalog": "storage",
            "schema": "public",
            "continuous": True,
            "pipeline_type": "DATABASE_TABLE_SYNC",
            "managed_definition": {
                "database_table_sync": {
                    "sinks": [
                        {
                            "src_table": "owned.round4.model_scores",
                            "dest_table": "anti_demo.public.model_scores",
                            "dest_table_uc_name": "storage.round4.model_scores",
                            "dest_table_id": "uid-1",
                            "primary_key": ["entity_id"],
                            "creator": "operator@databricks.com",
                            "online_catalog_name": "storage",
                            "database_user": "managed-sync-user",
                            "query_federation_user": "managed-sync-query-user",
                        }
                    ]
                }
            },
        },
    }


def uc_schema_payload(full_name: str):
    return {
        "full_name": full_name,
        "owner": "operator@databricks.com",
        "created_by": "operator@databricks.com",
    }


class FakeApiClient:
    def __init__(self, responses=None):
        self.responses = responses or {
            "/api/2.0/postgres/synced_tables/storage.round4.model_scores": (
                postgres_synced_table_payload()
            ),
            "/api/2.0/database/synced_tables/storage.round4.model_scores": (
                database_synced_table_payload()
            ),
            "/api/2.0/postgres/projects/project-1": project_payload(),
            "/api/2.0/postgres/projects/project-1/branches/round4": branch_payload(),
            "/api/2.0/pipelines/pipeline-1": pipeline_payload(),
            "/api/2.1/unity-catalog/schemas/owned.round4": uc_schema_payload(
                "owned.round4"
            ),
            "/api/2.1/unity-catalog/schemas/owned.round4_storage": uc_schema_payload(
                "owned.round4_storage"
            ),
            "/api/2.1/unity-catalog/schemas/storage.public": uc_schema_payload(
                "storage.public"
            ),
            "/api/2.1/unity-catalog/tables?catalog_name=owned&schema_name=round4_storage": {
                "tables": []
            },
        }
        self.calls = []

    def do(self, method, path):
        self.calls.append((method, path))
        return self.responses[path]


def control_plane_workspace(api_client=None):
    return SimpleNamespace(api_client=api_client or FakeApiClient())


class FakeStatements:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[tuple[str, tuple[SqlParameter, ...]]] = []

    async def execute(self, statement, parameters=()):
        self.calls.append((statement, tuple(parameters)))
        return self.responses.pop(0)


async def test_inspect_sync_uses_sealed_spec_cdf_source_head_and_fail_closed_state(
    monkeypatch,
) -> None:
    statements = FakeStatements(
        [
            [{"version": "12", "timestamp": "2026-08-18T11:59:59Z"}],
            [{"key": "delta.enableChangeDataFeed", "value": "true"}],
        ]
    )
    workspace = control_plane_workspace()
    adapter = LiveModelScoreAdapter(
        live_config(),
        workspace_client=workspace,
        statement_runner=statements,
        now=lambda: NOW,
    )

    status = await adapter.inspect_sync()

    assert status.state == ManagedSyncState.RUNNING
    assert status.cdf_enabled is True
    assert status.source_version == status.last_processed_version == 12
    assert status.pipeline_id == "pipeline-1"
    assert statements.calls[0][0].startswith("DESCRIBE HISTORY")
    assert set(workspace.api_client.calls) == {
        (
            "GET",
            "/api/2.0/postgres/synced_tables/storage.round4.model_scores",
        ),
        (
            "GET",
            "/api/2.0/database/synced_tables/storage.round4.model_scores",
        ),
        ("GET", "/api/2.0/postgres/projects/project-1"),
        ("GET", "/api/2.0/postgres/projects/project-1/branches/round4"),
        ("GET", "/api/2.0/pipelines/pipeline-1"),
        ("GET", "/api/2.1/unity-catalog/schemas/owned.round4"),
        ("GET", "/api/2.1/unity-catalog/schemas/owned.round4_storage"),
        ("GET", "/api/2.1/unity-catalog/schemas/storage.public"),
        (
            "GET",
            "/api/2.1/unity-catalog/tables?catalog_name=owned&schema_name=round4_storage",
        ),
    }

    stopped_statements = FakeStatements(
        [
            [{"version": "12", "timestamp": "2026-08-18T11:59:59Z"}],
            [{"key": "delta.enableChangeDataFeed", "value": "true"}],
        ]
    )
    stopped_api = FakeApiClient()
    stopped_api.responses["/api/2.0/postgres/synced_tables/storage.round4.model_scores"] = (
        postgres_synced_table_payload(state="SYNCED_TABLE_PROVISIONING")
    )
    stopped = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(stopped_api),
        statement_runner=stopped_statements,
        now=lambda: NOW,
    )
    assert (await stopped.inspect_sync()).state == ManagedSyncState.STOPPED

    changed_api = FakeApiClient()
    changed_api.responses["/api/2.0/database/synced_tables/storage.round4.model_scores"][
        "effective_database_branch_id"
    ] = "replaced-branch-uid"
    mismatched = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(changed_api),
        statement_runner=FakeStatements([]),
        now=lambda: NOW,
    )
    with pytest.raises(ModelScoreLiveConfigurationError, match="effective target"):
        await mismatched.inspect_sync()

    contract = ModelScoreContract(
        pipeline_id="pipeline-1",
        source_table="owned.round4.model_scores",
        synced_table="anti_demo.public.model_scores",
    )
    resources = SimpleNamespace(
        warehouse_id="warehouse-1",
        setup_principal="operator@databricks.com",
        source_table_full_name=contract.source_table,
        storage_catalog="owned",
        storage_schema="round4_storage",
        synced_table_resource_name="synced_tables/storage.round4.model_scores",
        synced_table_id="storage.round4.model_scores",
        synced_table_uid="uid-1",
        pipeline_id=contract.pipeline_id,
        physical_database="anti_demo",
        physical_schema="public",
        physical_table="model_scores",
        project_uid="project-uid-1",
        branch_uid="branch-uid-1",
        branch="projects/project-1/branches/round4",
        endpoint_name="projects/project-1/branches/round4/endpoints/primary",
        app_service_principal_client_id="app-client-id",
        contract_sha256=contract.sha256,
    )
    manifest = SimpleNamespace(
        manifest_version=2,
        round4=resources,
        databricks=SimpleNamespace(profile="setup-profile", user="setup-user"),
    )
    monkeypatch.setenv("ANTI_DEMO_ENV", "databricks-app")
    monkeypatch.setattr(
        model_score_live,
        "WorkspaceClient",
        lambda: SimpleNamespace(),
    )
    engine = model_score_live.build_model_score_engine(manifest)
    assert engine.adapter.config.profile == ""
    assert engine.adapter.config.database_user == ""
    assert engine.adapter.config.expected_runtime_principal == "app-client-id"
    assert engine.inspect_timeout_seconds == 30.0
    manifest.manifest_version = 4
    assert model_score_live.build_model_score_engine(manifest).contract == engine.contract
    manifest.manifest_version = 5
    assert model_score_live.build_model_score_engine(manifest).contract == engine.contract
    manifest.manifest_version = 6
    assert model_score_live.build_model_score_engine(manifest).contract == engine.contract
    manifest.manifest_version = 7
    assert model_score_live.build_model_score_engine(manifest).contract == engine.contract


async def test_database_cursor_is_authoritative_and_ahead_of_source_fails_closed() -> None:
    caught_up = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(),
        statement_runner=FakeStatements(
            [
                [{"version": "12", "timestamp": "2026-08-18T11:59:59Z"}],
                [{"key": "delta.enableChangeDataFeed", "value": "true"}],
            ]
        ),
        now=lambda: NOW,
    )

    status = await caught_up.inspect_sync()

    assert status.last_processed_version == 12
    assert status.last_sync_delta_version == 12
    assert status.last_sync_delta_commit_time == datetime(2026, 8, 18, 11, 59, 59, tzinfo=UTC)

    waiting_api = FakeApiClient()
    waiting_api.responses["/api/2.0/database/synced_tables/storage.round4.model_scores"] = (
        database_synced_table_payload(processed_version=11, delta_version=11)
    )
    waiting = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(waiting_api),
        statement_runner=FakeStatements(
            [
                [{"version": "12", "timestamp": "2026-08-18T11:59:59Z"}],
                [{"key": "delta.enableChangeDataFeed", "value": "true"}],
            ]
        ),
    )
    waiting_status = await waiting.inspect_sync()
    assert waiting_status.source_version == 12
    assert waiting_status.last_processed_version == 11
    assert waiting_status.last_sync_delta_version == 11

    ahead_api = FakeApiClient()
    ahead_api.responses["/api/2.0/database/synced_tables/storage.round4.model_scores"] = (
        database_synced_table_payload(processed_version=13, delta_version=13)
    )
    ahead = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(ahead_api),
        statement_runner=FakeStatements(
            [
                [{"version": "12", "timestamp": "2026-08-18T11:59:59Z"}],
                [{"key": "delta.enableChangeDataFeed", "value": "true"}],
            ]
        ),
    )
    with pytest.raises(ModelScoreLiveOperationError, match="source head"):
        await ahead.inspect_sync()


async def test_pipeline_requires_exact_single_managed_sync_sink() -> None:
    api = FakeApiClient()
    pipeline = api.responses["/api/2.0/pipelines/pipeline-1"]
    sinks = pipeline["spec"]["managed_definition"]["database_table_sync"]["sinks"]
    sinks.append(dict(sinks[0]))
    adapter = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(api),
        statement_runner=FakeStatements([]),
    )

    with pytest.raises(ModelScoreLiveConfigurationError, match="sink contract"):
        await adapter.inspect_sync()


async def test_uc_schemas_and_empty_storage_are_part_of_the_runtime_contract() -> None:
    api = FakeApiClient()
    api.responses[
        "/api/2.1/unity-catalog/schemas/owned.round4_storage"
    ]["owner"] = "different-owner@databricks.com"
    adapter = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(api),
        statement_runner=FakeStatements([]),
    )
    with pytest.raises(ModelScoreLiveConfigurationError, match="ownership changed"):
        await adapter.inspect_sync()

    nonempty_api = FakeApiClient()
    nonempty_api.responses[
        "/api/2.1/unity-catalog/tables?catalog_name=owned&schema_name=round4_storage"
    ] = {"tables": [{"full_name": "owned.round4_storage.unexpected"}]}
    adapter = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(nonempty_api),
        statement_runner=FakeStatements([]),
    )
    with pytest.raises(ModelScoreLiveConfigurationError, match="exactly empty"):
        await adapter.inspect_sync()


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executions = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, statement, parameters):
        self.executions.append((statement, parameters))

    async def fetchall(self):
        return [self.row]


class FakeConnection:
    def __init__(self, row):
        self.cursor_instance = FakeCursor(row)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def cursor(self):
        return self.cursor_instance


async def test_source_merge_cdf_and_each_application_read_are_exact_and_fresh() -> None:
    expected = ModelScoreRow("customer-1", 0.81, "risk-v1", "nonce-1")
    statements = FakeStatements(
        [
            [
                {
                    "entity_id": expected.entity_id,
                    "score": str(expected.score),
                    "model_version": expected.model_version,
                    "proof_nonce": expected.proof_nonce,
                }
            ],
            [{"version": "12", "timestamp": "2026-08-18T11:59:58Z"}],
            [],
            [
                {
                    "entity_id": expected.entity_id,
                    "score": str(expected.score),
                    "model_version": expected.model_version,
                    "proof_nonce": expected.proof_nonce,
                    "_commit_version": "13",
                    "_commit_timestamp": "2026-08-18T11:59:59Z",
                    "_change_type": "update_postimage",
                }
            ],
        ]
    )
    credential_calls = 0

    def fresh_credential(_endpoint):
        nonlocal credential_calls
        credential_calls += 1
        return SimpleNamespace(token=f"token-{credential_calls}")

    workspace = SimpleNamespace(
        postgres=SimpleNamespace(
            get_endpoint=lambda _name: {"status": {"hosts": {"host": "db.example"}}},
            generate_database_credential=fresh_credential,
        ),
        current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name="ignored")),
    )
    connections = []

    async def connector(**arguments):
        connection = FakeConnection(
            (
                expected.entity_id,
                expected.score,
                expected.model_version,
                expected.proof_nonce,
            )
        )
        connections.append((arguments, connection))
        return connection

    adapter = LiveModelScoreAdapter(
        live_config(),
        workspace_client=workspace,
        statement_runner=statements,
        connector=connector,
    )

    assert await adapter.read_source(expected.entity_id) == expected
    commit = await adapter.commit_source_update(
        ModelScoreUpdate(
            expected.entity_id,
            expected.score,
            expected.model_version,
            expected.proof_nonce,
        )
    )
    assert commit.version == 13
    merge, merge_parameters = statements.calls[2]
    assert merge.startswith("MERGE INTO")
    assert ":proof_nonce" in merge
    assert {parameter.name for parameter in merge_parameters} == {
        "entity_id",
        "score",
        "model_version",
        "proof_nonce",
    }
    cdf, cdf_parameters = statements.calls[3]
    assert "table_changes" in cdf and "update_postimage" in cdf
    assert {parameter.name for parameter in cdf_parameters} == {
        "start_version",
        "proof_nonce",
    }

    assert await adapter.read_application_fresh(expected.entity_id) == expected
    assert await adapter.read_application_fresh(expected.entity_id) == expected
    assert credential_calls == 2
    assert len(connections) == 2
    assert connections[0][0]["password"] == "token-1"
    assert connections[1][0]["password"] == "token-2"
    assert connections[0][1] is not connections[1][1]
    assert connections[0][1].cursor_instance.executions[0][1] == (expected.entity_id,)


# ---------------------------------------------------------------------------
# GET /api/2.0/pipelines/{id} reports `state` from DEPLOYING, STARTING,
# RUNNING, STOPPING, DELETED, RECOVERING, FAILED, RESETTING, IDLE and a
# `latest_updates` array ordered newest first whose entries carry QUEUED,
# CREATED, WAITING_FOR_RESOURCES, INITIALIZING, RESETTING, SETTING_UP_TABLES,
# RUNNING, STOPPING, COMPLETED, FAILED, CANCELED.
#
# `state` alone cannot tell a stopped continuous pipeline from a healthy one,
# because a stopped one reports IDLE and the synced table stays on the healthy
# SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE. The newest update can: a continuous
# pipeline that is genuinely syncing always has one non-terminal update.
# ---------------------------------------------------------------------------


async def inspect_with_pipeline(pipeline, synced_state="SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"):
    statements = FakeStatements(
        [
            [{"version": "12", "timestamp": "2026-08-18T11:59:59Z"}],
            [{"key": "delta.enableChangeDataFeed", "value": "true"}],
        ]
    )
    api = FakeApiClient()
    api.responses["/api/2.0/pipelines/pipeline-1"] = pipeline
    api.responses["/api/2.0/postgres/synced_tables/storage.round4.model_scores"] = (
        postgres_synced_table_payload(state=synced_state)
    )
    api.responses["/api/2.0/database/synced_tables/storage.round4.model_scores"] = (
        database_synced_table_payload(state=synced_state)
    )
    adapter = LiveModelScoreAdapter(
        live_config(),
        workspace_client=control_plane_workspace(api),
        statement_runner=statements,
        now=lambda: NOW,
    )
    return await adapter.inspect_sync()


def _update(state: str, update_id: str = "update-9") -> dict:
    return {"update_id": update_id, "state": state}


async def test_what_the_pipeline_state_and_its_newest_update_classify_to() -> None:
    """The whole classification table, read through a live inspect_sync.

    The defect in one line: IDLE used to be accepted as healthy. A continuous
    pipeline that has been stopped reports IDLE, so the pipeline's own state is
    not sufficient on its own -- the newest update is what decides, and the
    array arrives newest-first.

    Rows to read together:

    * The three terminal updates against IDLE are the defect itself, and the
      refusal has to name all three signals rather than the first non-empty one.
    * The five provisioning updates are the cold start Round 4 must not measure,
      and STARTING is a different answer from both RUNNING and STOPPED.
    * A RUNNING update wins under either pipeline state, so the pair of rows is
      what pins that the update rather than the pipeline is authoritative.
    * The last row carries a terminal and a failed update behind a running one;
      classifying on anything but the head would read it as stopped or failed.
    """

    cases: tuple[tuple[str, str, list[dict], ManagedSyncState, tuple[str, ...]], ...] = (
        *(
            (
                f"idle pipeline, {terminal} update",
                "IDLE",
                [_update(terminal)],
                ManagedSyncState.STOPPED,
                ("pipeline IDLE", f"latest update {terminal}"),
            )
            for terminal in ("COMPLETED", "CANCELED", "STOPPING")
        ),
        ("idle pipeline, no update history at all", "IDLE", [], ManagedSyncState.STOPPED, ()),
        *(
            (
                f"starting pipeline, {provisioning} update",
                "STARTING",
                [_update(provisioning)],
                ManagedSyncState.STARTING,
                (),
            )
            for provisioning in (
                "QUEUED",
                "CREATED",
                "WAITING_FOR_RESOURCES",
                "INITIALIZING",
                "SETTING_UP_TABLES",
            )
        ),
        (
            "running pipeline, running update",
            "RUNNING",
            [_update("RUNNING")],
            ManagedSyncState.RUNNING,
            (),
        ),
        (
            "idle pipeline, running update",
            "IDLE",
            [_update("RUNNING")],
            ManagedSyncState.RUNNING,
            (),
        ),
        (
            "idle pipeline, failed update",
            "IDLE",
            [_update("FAILED")],
            ManagedSyncState.FAILED,
            (),
        ),
        (
            "only the newest update decides",
            "RUNNING",
            [
                _update("RUNNING", "update-9"),
                _update("COMPLETED", "update-8"),
                _update("FAILED", "update-7"),
            ],
            ManagedSyncState.RUNNING,
            (),
        ),
    )

    for name, pipeline_state, latest_updates, expected, fragments in cases:
        status = await inspect_with_pipeline(
            pipeline_payload(state=pipeline_state, latest_updates=latest_updates)
        )
        assert status.state is expected, name
        for fragment in fragments:
            assert fragment in status.failure, f"{name}: missing {fragment!r}"


def test_classification_is_lenient_only_where_the_warm_up_can_still_catch_it() -> None:
    """Ambiguity resolves to RUNNING and is left to the arm-time round trip.

    Refusing on a signal that might belong to a healthy pipeline would break a
    working demo; accepting one is safe because warmth is proven empirically
    before the bell either way.
    """

    healthy = {"SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"}
    classify = model_score_live.classify_managed_sync_state

    assert classify(healthy, "IDLE", "RUNNING") is ManagedSyncState.RUNNING
    assert classify(healthy, "IDLE", "COMPLETED") is ManagedSyncState.STOPPED
    assert classify(healthy, "STOPPING", "RUNNING") is ManagedSyncState.STOPPED
    assert classify(healthy, "IDLE", "") is ManagedSyncState.STOPPED
    assert (
        classify({"SYNCED_TABLE_ONLINE_PIPELINE_FAILED"}, "RUNNING", "RUNNING")
        is ManagedSyncState.FAILED
    )


async def test_a_pipeline_somebody_switched_off_is_stopped_and_not_a_failure() -> None:
    """A sanctioned power-off must not be reported as a fault nobody caused.

    The shape is measured, not imagined. A deliberately stopped pipeline reports
    ``IDLE`` with a newest update of ``CANCELED``, and its synced table reports
    ``SYNCED_TABLE_ONLINE_PIPELINE_FAILED`` -- a state whose own description is
    "Online Table is online, however latest pipeline update failed". So the
    table is *up* and the only complaint is an update somebody ended on purpose.

    STOPPED and FAILED both refuse, so this changes no control flow. What it
    changes is the sentence: ``ManagedSyncStatus.failure`` carried a pipeline
    failure into every surface that reads it, for an installation whose operator
    had deliberately switched the pipeline off to stop paying for it.
    """

    status = await inspect_with_pipeline(
        pipeline_payload(state="IDLE", latest_updates=[_update("CANCELED", "c22f27")]),
        synced_state="SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
    )

    assert status.state is ManagedSyncState.STOPPED
    # The refusal still names all three signals, so nothing is hidden -- it just
    # no longer claims the pipeline failed.
    assert "latest update CANCELED" in status.failure
    assert "pipeline IDLE" in status.failure


def test_the_stopped_pipeline_exemption_is_the_narrowest_one_that_closes_it() -> None:
    """Every way to be genuinely broken that the exemption must not swallow.

    Written in this direction deliberately. The tempting fix -- dropping
    ``SYNCED_TABLE_ONLINE_PIPELINE_FAILED`` from
    :data:`server.model_score_live.SYNCED_TABLE_FAILED_STATES` -- would trade a
    false alarm for a missed real one, and the offline-failed rows below are what
    fail if anyone tries it.

    **What this test does not guard, so that nobody reads it as covering more
    than it does.** Widening
    :data:`server.model_score_live.PIPELINE_UPDATE_CANCELLED_STATES` to admit
    ``FAILED`` does *not* break the ``FAILED`` row here, because this function
    refuses a failed newest update a second time through
    :data:`server.model_score_live.PIPELINE_UPDATE_FAILED_STATES`. That widening
    is caught in ``tests/test_lifecycle.py``, at the two waits which have no such
    second check -- verified by making the change and watching it fail there.
    """

    classify = model_score_live.classify_managed_sync_state
    online_failed = "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"

    # The one case that is a stop.
    assert classify({online_failed}, "IDLE", "CANCELED") is ManagedSyncState.STOPPED

    # An update that fell over on its own. `FAILED` is not a cancellation, and
    # this row is what fails first if it is ever added to the cancelled set.
    assert classify({online_failed}, "IDLE", "FAILED") is ManagedSyncState.FAILED
    # The pipeline itself is gone or broken, whatever its newest update says.
    assert classify({online_failed}, "FAILED", "CANCELED") is ManagedSyncState.FAILED
    assert classify({online_failed}, "DELETED", "CANCELED") is ManagedSyncState.FAILED
    # The table itself went offline and failed. No stop produces that, so a
    # cancelled update must not buy it an exemption -- on its own, or alongside
    # the online state that does qualify.
    assert (
        classify({"SYNCED_TABLE_OFFLINE_FAILED"}, "IDLE", "CANCELED") is ManagedSyncState.FAILED
    )
    assert (
        classify({"SYNCED_TABLE_OFFLINE_FAILED", online_failed}, "IDLE", "CANCELED")
        is ManagedSyncState.FAILED
    )
    # No newest update at all cannot exonerate a failed state either.
    assert classify({online_failed}, "IDLE", "") is ManagedSyncState.FAILED


def _activation_manifest() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="run-1",
        round4=SimpleNamespace(
            pipeline_id="pipeline-1",
            synced_table_id="storage.round4.model_scores",
        ),
        databricks=SimpleNamespace(profile="demo", user="setup-user"),
    )


class FakePipelineApi:
    """The two GETs and the two verbs, in the shapes the account actually returns.

    The stopped shape is measured rather than imagined. On 2026-08-24 the live
    sealed pipeline, deliberately stopped, reported ``state: IDLE`` with a newest
    update of ``CANCELED``, a synced table of
    ``SYNCED_TABLE_ONLINE_PIPELINE_FAILED``, and **no**
    ``continuous_update_status`` key at all.
    """

    def __init__(self, running: bool = True) -> None:
        self.running = running
        self.calls: list[tuple[str, str]] = []

    def __call__(self, profile, method, path, *, body=None, timeout=600):
        self.calls.append((method, path))
        if method == "post" and path.endswith("/stop"):
            self.running = False
            return {}
        if method == "post" and path.endswith("/updates"):
            assert body == {"full_refresh": False}, body
            self.running = True
            return {"update_id": "update-1"}
        if "/pipelines/" in path:
            return {
                "state": "RUNNING" if self.running else "IDLE",
                "latest_updates": [
                    {"state": "RUNNING" if self.running else "CANCELED", "update_id": "u1"}
                ],
            }
        status = {
            "detailed_state": (
                "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE"
                if self.running
                else "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"
            )
        }
        if self.running:
            status["continuous_update_status"] = {"last_processed_commit_version": 5}
        return {"data_synchronization_status": status}


def _activation(api: FakePipelineApi, **kwargs) -> model_score_live.Round4PipelineActivation:
    return model_score_live.Round4PipelineActivation(
        _activation_manifest(),
        api,
        pipeline_id="pipeline-1",
        poll_seconds=0,
        **kwargs,
    )


async def test_a_running_pipeline_costs_the_arm_one_read_and_no_verb() -> None:
    """The common case is on the audience-facing path and must stay cheap."""

    api = FakePipelineApi(running=True)
    notices: list[str] = []

    await _activation(api).ensure_running(lambda status: _record(notices, status))

    assert [method for method, _ in api.calls] == ["get", "get"]
    assert notices == []


async def test_a_stopped_pipeline_is_started_without_a_full_refresh_and_waited_for() -> None:
    """Resume, never re-seed, and never anything that could recreate the table.

    ``full_refresh: False`` is asserted on the wire because it is what makes a
    resumed pipeline pick up at the Delta version it stopped on --- confirmed
    live on 2026-08-24, where the cursor came back at the same version 5 it
    stopped on and the last sync timestamp was unchanged. The set of paths is
    asserted whole because the danger here is not a wrong verb but an extra one:
    anything addressing the synced table could recreate it, which mints a new
    ``pipeline_id``, demotes the manifest to v2 and destroys Rounds 5 and 6.
    """

    api = FakePipelineApi(running=False)
    notices: list[str] = []

    await _activation(api).ensure_running(lambda status: _record(notices, status))

    assert ("post", "/api/2.0/pipelines/pipeline-1/updates") in api.calls
    assert not any("synced_tables" in path for method, path in api.calls if method == "post")
    assert any("Starting it before the bell" in notice for notice in notices)
    assert any("pipeline is running" in notice for notice in notices)


async def test_a_pipeline_that_never_comes_back_refuses_the_arm_rather_than_hanging() -> None:
    """Refusing is recoverable. Waiting forever in front of a room is not."""

    class NeverStarts(FakePipelineApi):
        def __call__(self, profile, method, path, *, body=None, timeout=600):
            result = super().__call__(profile, method, path, body=body, timeout=timeout)
            self.running = False
            return result

    api = NeverStarts(running=False)
    clock = iter([0.0, 0.0, 400.0, 500.0, 600.0])
    with pytest.raises(ModelScoreLiveOperationError, match="did not reach a healthy"):
        await _activation(api, clock=lambda: next(clock)).ensure_running(
            lambda status: _record([], status)
        )


async def test_an_arm_cancels_a_release_the_previous_bout_scheduled() -> None:
    """The stop must never land inside the next bout.

    A fresh engine is built for every arm while the pipeline outlives all of
    them, so the activation is shared per pipeline rather than owned per engine.
    Without that, a release scheduled by bout one would still be asleep when
    bout two armed, and would wake up and stop a pipeline a round was using.
    """

    api = FakePipelineApi(running=True)
    activation = _activation(api, idle_seconds=0)
    activation.release_when_idle()
    await activation.ensure_running(lambda status: _record([], status))
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert not any(method == "post" and path.endswith("/stop") for method, path in api.calls)


async def test_a_release_stops_a_healthy_pipeline_and_leaves_a_broken_one_alone() -> None:
    """A deliberate-stop record written over a real failure hides the failure.

    Telling a deliberate stop from a pipeline that fell over is the only thing
    the power record is for, so the release refuses to stop anything that is not
    healthy rather than stamping its own explanation onto someone else's problem.
    """

    healthy = FakePipelineApi(running=True)
    activation = _activation(healthy, idle_seconds=0)
    await activation._release_after_idle(activation._generation)
    assert healthy.running is False

    broken = FakePipelineApi(running=False)
    quiet = _activation(broken, idle_seconds=0)
    await quiet._release_after_idle(quiet._generation)
    assert not any(path.endswith("/stop") for _, path in broken.calls)


class _BaselineManagedSync(ModelScoreAdapter):
    """A Delta source and continuous sync that never leave the sealed baseline.

    Enough to drive one whole towelled bout through the real engine: ``arm``
    round-trips its throwaway warm-up commit, ``run`` is never reached because
    the operator threw the towel, and settlement therefore finds the baseline
    already in place and takes the branch that only waits for the sync to catch
    up. Modelled on ``ReplayableManagedSync`` in ``tests/test_model_score.py``
    and deliberately no larger, because everything this test is about happens
    on the pipeline wire rather than in the adapter.
    """

    def __init__(self, contract: ModelScoreContract, *, version: int = 10) -> None:
        self.contract = contract
        self.row = contract.baseline
        self.version = version

    async def inspect_sync(self) -> ManagedSyncStatus:
        return ManagedSyncStatus(
            pipeline_id=self.contract.pipeline_id,
            source_table=self.contract.source_table,
            synced_table=self.contract.synced_table,
            state=ManagedSyncState.RUNNING,
            cdf_enabled=True,
            continuous=True,
            source_version=self.version,
            last_processed_version=self.version,
            last_sync_delta_version=self.version,
            last_sync_delta_commit_time=_COMMITTED_AT,
            observed_at=NOW,
            sync_end_time=_COMMITTED_AT + timedelta(milliseconds=500),
        )

    async def read_source(self, entity_id: str) -> ModelScoreRow | None:
        return self.row if self.row.entity_id == entity_id else None

    async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit:
        self.version += 1
        self.row = update.row
        return DeltaCommit(version=self.version, committed_at=_COMMITTED_AT)

    async def read_application_fresh(self, entity_id: str) -> ModelScoreRow | None:
        return self.row if self.row.entity_id == entity_id else None


def _towelled_engine(activation: model_score_live.Round4PipelineActivation) -> ModelScoreEngine:
    contract = ModelScoreContract(
        pipeline_id="pipeline-1",
        source_table="owned.round4.model_scores",
        synced_table="public.model_scores",
    )
    return ModelScoreEngine(
        _BaselineManagedSync(contract),
        contract=contract,
        poll_interval_seconds=0,
        now=lambda: NOW,
        clock_ns=_ticking_clock(),
        activation=activation,
    )


def _ticking_clock(step_ns: int = 3_000_000):
    value = 0

    def clock() -> int:
        nonlocal value
        value += step_ns
        return value

    return clock


async def test_a_towel_gives_back_the_pipeline_its_own_arm_started_and_nothing_else() -> None:
    """A towelled bout has no redo to protect, so it owes the pipeline nothing.

    The twenty-minute window `IDLE_STOP_SECONDS` documents is bought entirely by
    the redo affordance: a *verified* Round 4 leaves a ``RedoState.READY`` the
    presenter may take at any time, and a redo landing on a stopped pipeline is
    a round dying in front of an audience. A towel produces no such state --
    ``_finish_model_score`` returns early on a non-null towel, so no redo
    snapshot is ever published -- so the window protects nothing and the towel
    is left holding a pipeline that bills $14.57/day until somebody notices.

    The second half is the guard that makes the first half safe. Stopping "the
    pipeline" is the wrong contract; restoring what this arm changed is the
    right one. An operator who warmed the pipeline by hand, or a previous bout
    that left it up, must not lose it to somebody else's towel -- that path
    keeps the idle release it has always had.
    """

    started_by_arm = FakePipelineApi(running=False)
    activation = _activation(started_by_arm)
    engine = _towelled_engine(activation)

    await engine.arm()
    assert started_by_arm.running is True, "arm should have resumed the stopped pipeline"

    # No run(): the operator threw the towel, so the engine issued no result and
    # there is no redo anyone can still take.
    await engine.settle_and_restore_baseline()

    assert started_by_arm.running is False, (
        "the towelled bout left the Managed Sync pipeline RUNNING, and its own arm "
        "is what started it -- it bills until somebody remembers"
    )

    already_up = FakePipelineApi(running=True)
    borrowed = _activation(already_up)
    await _towelled_engine(borrowed).arm()
    await _towelled_engine(borrowed).settle_and_restore_baseline()

    assert already_up.running is True, (
        "a towel stopped a pipeline this bout did not start, destroying state the "
        "operator put there"
    )
    borrowed._cancel_release()


async def test_a_stop_that_fails_after_a_towel_says_so_and_keeps_its_fallback() -> None:
    """The reported defect was silence, not the stop itself.

    A towel that cannot stop the pipeline it started is a towel that did not do
    the one thing a towel is for, and the receipt read ``cleanup_failure: null``
    while the pipeline billed. Raising is what turns this into the diagnostic
    the towel already knows how to carry, and the message has to name the money
    and the manual way out, because the operator reading it is the only thing
    standing between the installation and a forgotten pipeline.

    Re-arming the idle release on the way out is the other half. Surfacing a
    failure must not also discard the delayed stop that would have caught it --
    that would trade a silent overspend for a loud one.
    """

    class StopRefused(FakePipelineApi):
        def __call__(self, profile, method, path, *, body=None, timeout=600):
            if method == "post" and path.endswith("/stop"):
                raise RuntimeError("PERMISSION_DENIED: CAN_RUN is required on this pipeline")
            return super().__call__(profile, method, path, body=body, timeout=timeout)

    api = StopRefused(running=False)
    activation = _activation(api)
    engine = _towelled_engine(activation)
    await engine.arm()

    with pytest.raises(ModelScoreLiveOperationError, match=r"still billing about \$\d") as raised:
        await engine.settle_and_restore_baseline()

    assert "antidemo pipeline stop" in str(raised.value)
    assert api.running is True
    # The fallback the failure did not throw away: a release is pending again,
    # so the money still comes down even though nobody read the diagnostic.
    assert activation._release is not None
    activation._cancel_release()


async def _armed_activation(api: FakePipelineApi, **kwargs):
    """An activation that started this pipeline, which is how a bout leaves one."""

    activation = _activation(api, **kwargs)
    await activation.ensure_running(lambda status: _record([], status))
    return activation


def _durable_records(monkeypatch) -> list[dict]:
    """Every power record this process actually landed in the durable store.

    Appended by the replacement for ``record_power_request`` itself, so a record
    only shows up here once its write has genuinely run. That matters: the
    writes are scheduled tasks, and a record that is merely *scheduled* when the
    loop goes away is lost exactly as silently as the stop was.
    """

    written: list[dict] = []

    async def capture(record) -> bool:
        written.append(dict(record))
        return True

    monkeypatch.setattr(model_score_live.pipeline_power, "record_power_request", capture)
    return written


async def _shutdown(activation) -> None:
    """Shut down through the entry point ``RunManager.close()`` actually calls."""

    model_score_live._ACTIVATIONS["pipeline-1"] = activation
    try:
        await model_score_live.aclose_activations()
    finally:
        model_score_live._ACTIVATIONS.pop("pipeline-1", None)


async def test_shutdown_performs_the_stop_the_delayed_release_had_only_scheduled(
    monkeypatch,
) -> None:
    """Quitting the server inside the twenty-minute window used to lose the stop.

    The delayed release is a bare in-memory ``asyncio`` task owned by the
    module-global activation registry, so nothing in ``RunManager.close()`` --
    which accounts for the eight tasks a session record owns -- could see it.
    Stopping the server inside the window cancelled the loop, the task went with
    it, no verb was issued and no record was written, so the next ``doctor`` read
    a pipeline billing $14.57/day as one nobody had ever meant to stop. The
    triggering behaviour is the most ordinary one there is: finish the demo,
    quit the server.

    Asserted against the pipeline rather than against the activation. A fake that
    reports "release was called" proves the call and never the effect; this
    ``FakePipelineApi`` goes down only when a ``/stop`` actually reaches it, so
    it is a thing that would notice the stop not happening.
    """

    written = _durable_records(monkeypatch)
    api = FakePipelineApi(running=False)
    activation = await _armed_activation(api)
    assert api.running is True, "arm should have started the stopped pipeline"

    # Exactly what the tail of a clean-finished Round 4 settlement does.
    activation.release_when_idle()

    await _shutdown(activation)

    assert api.running is False, (
        "shutdown discarded the stop a settled bout had scheduled; the pipeline is "
        "still billing and nothing anywhere says a stop was owed"
    )
    assert ("post", "/api/2.0/pipelines/pipeline-1/stop") in api.calls
    assert [record["intent"] for record in written][-1] == "stopped"
    assert activation._release is None


async def test_a_shutdown_that_cannot_stop_the_pipeline_records_the_stop_as_owed_now(
    monkeypatch,
) -> None:
    """The half that makes an ungraceful loss survivable: say a stop is owed.

    A bounded stop has to be allowed to expire -- shutdown must not hang on a
    control-plane round trip -- but expiring must not be silence, which is the
    whole shape of this defect. So the owed record is rewritten as due *now*
    rather than left due at the end of a window whose timer no longer exists,
    and the very next status read names the pipeline, the money and the command.
    """

    class StopRefused(FakePipelineApi):
        def __call__(self, profile, method, path, *, body=None, timeout=600):
            if method == "post" and path.endswith("/stop"):
                raise RuntimeError("PERMISSION_DENIED: CAN_RUN is required on this pipeline")
            return super().__call__(profile, method, path, body=body, timeout=timeout)

    written = _durable_records(monkeypatch)
    api = StopRefused(running=False)
    activation = await _armed_activation(api)
    activation.release_when_idle()

    # Never raises: a failure here must not stop a ring being released or a
    # coordination store being closed further down the shutdown path.
    await _shutdown(activation)

    assert api.running is True
    owed = [record for record in written if record["intent"] == "stop_owed"]
    assert owed, "a stop that could not be performed was not recorded as owed"
    due = datetime.fromisoformat(owed[-1]["owed_at"])
    assert due <= datetime.now(UTC), "the owed stop is not yet due, so no surface reports it"


async def test_shutdown_leaves_a_pipeline_this_process_did_not_start_alone(
    monkeypatch,
) -> None:
    """Shutdown owes a stop; it does not get to take somebody else's pipeline.

    The delayed release may stop any healthy pipeline because twenty idle
    minutes after a settled bout is itself evidence nobody is presenting.
    Shutdown fires the instant the operator quits and has no such evidence, so
    it keeps the narrower contract: restore what this process changed. An
    operator who warmed the pipeline by hand keeps it -- and still gets the owed
    record, so the obligation is visible rather than merely declined.
    """

    written = _durable_records(monkeypatch)
    api = FakePipelineApi(running=True)
    activation = await _armed_activation(api)
    activation.release_when_idle()

    await _shutdown(activation)

    assert api.running is True
    assert not any(path.endswith("/stop") for _, path in api.calls)
    assert [record["intent"] for record in written][-1] == "stop_owed"


async def test_a_scheduled_release_records_the_stop_as_owed_before_anything_can_lose_it(
    monkeypatch,
) -> None:
    """Written when the timer is scheduled, because a kill runs no handler.

    A graceful shutdown now performs the owed stop, but ``SIGKILL``, an OOM kill
    and a container eviction run nothing at all, and on a deployed replica
    eviction is ordinary. A record written only at shutdown is, in exactly those
    cases, a record never written.

    The second half is what keeps it from crying wolf after every single bout:
    the record is not *in effect* until its due time, because a settled bout
    deliberately holds the pipeline for twenty minutes so a redo can land on it,
    and a stop being owed during that window is the healthy state rather than a
    fault.
    """

    written = _durable_records(monkeypatch)
    api = FakePipelineApi(running=True)
    activation = _activation(api, idle_seconds=1200.0)

    activation.release_when_idle()
    await model_score_live.drain_power_records()

    assert [record["intent"] for record in written] == ["stop_owed"]
    owed = written[0]
    assert owed["pipeline_id"] == "pipeline-1"

    due = datetime.fromisoformat(owed["owed_at"])
    assert due > datetime.now(UTC), "an owed stop that is due immediately shouts every bout"
    assert (
        model_score_live.pipeline_power.read_stop_marker(
            "pipeline-1", path=None, now=lambda: due - timedelta(seconds=1), durable=False
        )
        is None
    ), "a stop still inside its redo window must not read as overdue"

    activation._cancel_release()


async def test_the_delayed_release_issues_its_stop_off_the_event_loop() -> None:
    """A blocking SDK POST on the loop stalls the SSE stream an audience is watching.

    ``_read_signals`` has always used ``asyncio.to_thread``; the stop did not,
    so the one call in this class that mutates the control plane was the one
    that could hold the whole server for the length of a round trip.

    Asserted by thread identity rather than by timing, because a timing
    assertion on a fake that returns instantly proves nothing at all.
    """

    loop_thread = threading.get_ident()
    seen: list[int] = []

    class ThreadWatchingApi(FakePipelineApi):
        def __call__(self, profile, method, path, *, body=None, timeout=600):
            if method == "post" and path.endswith("/stop"):
                seen.append(threading.get_ident())
            return super().__call__(profile, method, path, body=body, timeout=timeout)

    api = ThreadWatchingApi(running=True)
    activation = _activation(api, idle_seconds=0)

    await activation._release_after_idle(activation._generation)

    assert api.running is False
    assert seen and loop_thread not in seen, (
        "the delayed release issued its stop on the event loop, so a slow "
        "control plane would stall every request and every SSE stream with it"
    )


async def test_a_power_record_produced_off_the_loop_is_reported_rather_than_dropped(
    caplog,
) -> None:
    """The silent-drop trap that the ``to_thread`` refactor walks straight into.

    ``_persist`` schedules onto the running loop, so a caller in a worker thread
    raises ``RuntimeError`` -- which this used to catch and return on, losing the
    durable record that lets a later check tell a deliberate stop from a pipeline
    that fell over. Every caller now collects records and persists them back on
    the loop, so this path is a guard rather than a writer; it is kept loud
    because a future caller that forgets would otherwise lose the record with no
    trace at all, and this fix depends on that record being written.
    """

    activation = _activation(FakePipelineApi(running=True))
    record = {"intent": "stopped", "pipeline_id": "pipeline-1"}

    with caplog.at_level("ERROR"):
        await asyncio.to_thread(activation._persist, record)

    assert any("could not be persisted durably" in message for message in caplog.messages)


async def _record(sink: list[str], status: str) -> None:
    sink.append(status)
