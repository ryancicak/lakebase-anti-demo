from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import quote

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ExecuteStatementRequestOnWaitTimeout,
    StatementParameterListItem,
    StatementState,
)

from . import pipeline_power
from .manifest import DemoManifest
from .model_score import (
    DeltaCommit,
    ManagedSyncState,
    ManagedSyncStatus,
    ModelScoreAdapter,
    ModelScoreContract,
    ModelScoreEngine,
    ModelScoreError,
    ModelScoreRow,
    ModelScoreUpdate,
)
from .pipeline_power import RESTART_SECONDS_ESTIMATE, START_WAIT_TIMEOUT_SECONDS

LOGGER = logging.getLogger(__name__)

SYNCED_TABLE_HEALTHY_STATES = frozenset(
    {
        "SYNCED_TABLE_ONLINE",
        "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
        "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE",
    }
)
SYNCED_TABLE_FAILED_STATES = frozenset(
    {
        "SYNCED_TABLE_OFFLINE_FAILED",
        "SYNCED_TABLE_ONLINE_PIPELINE_FAILED",
    }
)

# GET /api/2.0/pipelines/{id} reports a top-level `state` drawn from
# DEPLOYING, STARTING, RUNNING, STOPPING, DELETED, RECOVERING, FAILED,
# RESETTING and IDLE, and a `latest_updates` array ordered newest first whose
# entries carry their own state drawn from QUEUED, CREATED,
# WAITING_FOR_RESOURCES, INITIALIZING, RESETTING, SETTING_UP_TABLES, RUNNING,
# STOPPING, COMPLETED, FAILED and CANCELED.
#
# The top-level state alone cannot answer the question Round 4 needs answered.
# A continuous pipeline that has been stopped reports IDLE, which is the same
# value a pipeline with no active update reports for any other reason, and the
# synced table is no help either because SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE
# is a healthy state. What does distinguish them is the newest update: a
# continuous pipeline that is actually syncing always has one non-terminal
# update in flight, so a newest update sitting in COMPLETED, CANCELED or
# STOPPING means the continuous run has ended no matter what `state` says.
PIPELINE_SETTLED_STATES = frozenset({"RUNNING", "IDLE"})
PIPELINE_STARTING_STATES = frozenset({"DEPLOYING", "STARTING", "RECOVERING", "RESETTING"})
PIPELINE_FAILED_STATES = frozenset({"DELETED", "FAILED"})
PIPELINE_UPDATE_ACTIVE_STATE = "RUNNING"
PIPELINE_UPDATE_STARTING_STATES = frozenset(
    {
        "QUEUED",
        "CREATED",
        "WAITING_FOR_RESOURCES",
        "INITIALIZING",
        "RESETTING",
        "SETTING_UP_TABLES",
    }
)
PIPELINE_UPDATE_FAILED_STATES = frozenset({"FAILED"})

#: The one failed synced-table state a *sanctioned power-off* also produces.
#:
#: `SYNCED_TABLE_ONLINE_PIPELINE_FAILED` describes itself as "Online Table is
#: online, however latest pipeline update failed", so by the state's own
#: definition the table is up and the entire complaint is about the update. A
#: stop ends the continuous update by cancelling it, so this state arrives
#: identically for a pipeline somebody deliberately switched off and for one that
#: fell over -- the same collision `app._open_pipeline_power_store` documents for
#: `doctor`, reached here by a different route.
#:
#: `SYNCED_TABLE_OFFLINE_FAILED` is deliberately absent, and its absence is what
#: keeps this from becoming a blanket exemption. That state says the table itself
#: went offline and failed, which no stop produces, so it stays terminal however
#: the update ended.
STOPPABLE_SYNCED_TABLE_FAILED_STATE = "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"

#: The newest-update states that mean somebody ended the update rather than the
#: update ending itself. `CANCELED` is the state a `/stop` leaves behind --
#: measured 2026-08-24, and again on 2026-08-25 where the pipeline's own event log
#: named the cancelled update as CANCELED by a user action.
#:
#: `FAILED` is pointedly not in here, which is the whole reason this is a set of
#: cancellations rather than a removal from :data:`SYNCED_TABLE_FAILED_STATES`: a
#: pipeline that genuinely fell over must still be caught, and trading a false
#: alarm for a missed real one is not a fix.
PIPELINE_UPDATE_CANCELLED_STATES = frozenset({"CANCELED"})


def synced_table_failure_is_a_stopped_pipeline(
    synced_table_states: Iterable[str],
    pipeline_state: str,
    pipeline_update_state: str,
) -> bool:
    """Whether a failed synced-table state is a switched-off pipeline, not a fault.

    The one place that question is answered, because the set it is answered
    against had three independent copies -- two inline in `lifecycle`'s Round 4
    waits, one here -- and the false alarm all three shared had to be fixed in
    every one of them.

    **The signal is the newest pipeline update's own state, read from the
    pipeline.** Deliberately not the durable stop record: that record is
    authoritative about *intent*, but a stop can be issued by anything holding
    `CAN_RUN` on the pipeline, so a rule that only worked when a local marker
    existed would miss exactly the stops nobody here wrote down. The update state
    is present however the stop arrived.
    """

    failed = {
        str(state).strip().upper() for state in synced_table_states if state
    } & SYNCED_TABLE_FAILED_STATES
    # An empty set has nothing to exempt, and a set holding anything *else* is
    # carrying a failure no stop explains. Both answer no.
    if failed != {STOPPABLE_SYNCED_TABLE_FAILED_STATE}:
        return False
    if pipeline_state.strip().upper() in PIPELINE_FAILED_STATES:
        return False
    return pipeline_update_state.strip().upper() in PIPELINE_UPDATE_CANCELLED_STATES


def classify_managed_sync_state(
    synced_table_states: frozenset[str] | set[str],
    pipeline_state: str,
    pipeline_update_state: str,
) -> ManagedSyncState:
    """Decide whether the sync is warm, coming up, stopped, or broken.

    Deliberately lenient in the ambiguous direction: anything that could be a
    healthy continuous pipeline is reported RUNNING and left for the arm-time
    warm-up round trip to prove empirically. Only the states that cannot belong
    to a running continuous pipeline refuse here, so that the operator gets
    "the pipeline is stopped" immediately instead of an opaque timeout.

    STOPPED and FAILED are both "not RUNNING", so this distinction changes no
    caller's control flow -- ``Round4PipelineActivation`` starts the pipeline
    either way. What it changes is what everything downstream *says*: FAILED puts
    a fault nobody caused into ``ManagedSyncStatus.failure``, which is the
    sentence an operator reads when a deliberate, money-saving stop is the only
    thing that happened.
    """

    table_failed = bool(set(synced_table_states) & SYNCED_TABLE_FAILED_STATES)
    if table_failed and synced_table_failure_is_a_stopped_pipeline(
        synced_table_states, pipeline_state, pipeline_update_state
    ):
        # Falls through to STOPPED below, which is the answer this function
        # already exists to give: the online table's only complaint is an update
        # somebody cancelled.
        table_failed = False
    if (
        table_failed
        or pipeline_state in PIPELINE_FAILED_STATES
        or pipeline_update_state in PIPELINE_UPDATE_FAILED_STATES
    ):
        return ManagedSyncState.FAILED
    if (
        pipeline_state in PIPELINE_STARTING_STATES
        or pipeline_update_state in PIPELINE_UPDATE_STARTING_STATES
    ):
        # Provisioning is exactly the cold start Round 4 must not measure.
        return ManagedSyncState.STARTING
    if (
        set(synced_table_states) <= SYNCED_TABLE_HEALTHY_STATES
        and pipeline_state in PIPELINE_SETTLED_STATES
        and pipeline_update_state == PIPELINE_UPDATE_ACTIVE_STATE
    ):
        return ManagedSyncState.RUNNING
    return ManagedSyncState.STOPPED


class ModelScoreLiveConfigurationError(ModelScoreError):
    """The live resources do not match the sealed Round 4 contract."""


class ModelScoreLiveOperationError(ModelScoreError):
    """A live Statement Execution or PostgreSQL operation was not exact."""


@dataclass(frozen=True)
class SqlParameter:
    name: str
    value: str
    type: str


class StatementRunner(Protocol):
    async def execute(
        self,
        statement: str,
        parameters: Sequence[SqlParameter] = (),
    ) -> Sequence[Mapping[str, str | None]]: ...


class WorkspaceStatementRunner:
    """Bounded, inline Statement Execution for the small Round 4 proof queries."""

    def __init__(self, workspace: Any, warehouse_id: str) -> None:
        self._workspace = workspace
        self._warehouse_id = warehouse_id

    async def execute(
        self,
        statement: str,
        parameters: Sequence[SqlParameter] = (),
    ) -> Sequence[Mapping[str, str | None]]:
        response = await asyncio.to_thread(
            self._workspace.statement_execution.execute_statement,
            statement,
            self._warehouse_id,
            parameters=[
                StatementParameterListItem(
                    name=parameter.name,
                    value=parameter.value,
                    type=parameter.type,
                )
                for parameter in parameters
            ],
            wait_timeout="50s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
        )
        state = response.status.state if response.status else None
        if state != StatementState.SUCCEEDED:
            state_name = getattr(state, "value", "UNKNOWN")
            raise ModelScoreLiveOperationError(f"Statement Execution did not succeed: {state_name}")
        if response.manifest is None and response.result is None:
            return []
        if response.manifest is None or response.result is None:
            raise ModelScoreLiveOperationError("Statement Execution omitted its inline result")
        if response.manifest.truncated:
            raise ModelScoreLiveOperationError("Statement Execution result was truncated")
        if response.result.next_chunk_index is not None:
            raise ModelScoreLiveOperationError("Statement Execution returned an unexpected chunk")
        schema = response.manifest.schema
        columns = schema.columns if schema else None
        if columns is None or any(not column.name for column in columns):
            raise ModelScoreLiveOperationError("Statement Execution omitted its result schema")
        names = [str(column.name) for column in columns]
        rows = response.result.data_array or []
        if any(len(row) != len(names) for row in rows):
            raise ModelScoreLiveOperationError("Statement Execution returned a malformed row")
        return [dict(zip(names, row, strict=True)) for row in rows]


@dataclass(frozen=True)
class ModelScoreLiveConfig:
    profile: str
    warehouse_id: str
    setup_principal: str
    source_table_full_name: str
    storage_catalog: str
    storage_schema: str
    synced_table_resource_name: str
    synced_table_id: str
    synced_table_uid: str
    pipeline_id: str
    physical_database: str
    physical_schema: str
    physical_table: str
    project_uid: str
    branch_uid: str
    branch: str
    endpoint_name: str
    database_user: str = ""
    expected_runtime_principal: str = ""
    port: int = 5432
    connect_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        required = {
            "warehouse_id": self.warehouse_id,
            "setup_principal": self.setup_principal,
            "source_table_full_name": self.source_table_full_name,
            "storage_catalog": self.storage_catalog,
            "storage_schema": self.storage_schema,
            "synced_table_resource_name": self.synced_table_resource_name,
            "synced_table_id": self.synced_table_id,
            "synced_table_uid": self.synced_table_uid,
            "pipeline_id": self.pipeline_id,
            "physical_database": self.physical_database,
            "physical_schema": self.physical_schema,
            "physical_table": self.physical_table,
            "project_uid": self.project_uid,
            "branch_uid": self.branch_uid,
            "branch": self.branch,
            "endpoint_name": self.endpoint_name,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ModelScoreLiveConfigurationError(
                "Missing sealed Round 4 binding: " + ", ".join(missing)
            )
        _source_identifier(self.source_table_full_name)
        _postgres_identifier(self.physical_schema, self.physical_table)
        if self.synced_table_resource_name != f"synced_tables/{self.synced_table_id}":
            raise ModelScoreLiveConfigurationError(
                "Round 4 Synced Table resource name does not match its sealed ID"
            )
        branch_parts = self.branch.split("/")
        if (
            len(branch_parts) != 4
            or branch_parts[0] != "projects"
            or branch_parts[2] != "branches"
            or not branch_parts[1]
            or not branch_parts[3]
            or not self.endpoint_name.startswith(f"{self.branch}/endpoints/")
        ):
            raise ModelScoreLiveConfigurationError(
                "Round 4 project, branch, and endpoint names are not hierarchical"
            )
        if self.port < 1 or self.connect_timeout_seconds <= 0:
            raise ModelScoreLiveConfigurationError(
                "Round 4 PostgreSQL port and connection timeout must be positive"
            )

    @property
    def application_table_name(self) -> str:
        return f"{self.physical_database}.{self.physical_schema}.{self.physical_table}"

    @property
    def project_name(self) -> str:
        return self.branch.rsplit("/branches/", 1)[0]

    @property
    def project_id(self) -> str:
        return self.project_name.removeprefix("projects/")

    @property
    def branch_id(self) -> str:
        return self.branch.rsplit("/", 1)[1]

    @property
    def destination_catalog(self) -> str:
        return self.synced_table_id.split(".", 1)[0]

    @property
    def source_catalog(self) -> str:
        return self.source_table_full_name.split(".", 1)[0]

    @property
    def source_schema(self) -> str:
        return self.source_table_full_name.split(".")[1]


PsycopgConnector = Callable[..., Awaitable[Any]]


class LiveModelScoreAdapter(ModelScoreAdapter):
    def __init__(
        self,
        config: ModelScoreLiveConfig,
        *,
        workspace_client: Any | None = None,
        statement_runner: StatementRunner | None = None,
        connector: PsycopgConnector = psycopg.AsyncConnection.connect,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.config = config
        self._workspace = workspace_client or (
            WorkspaceClient(profile=config.profile) if config.profile else WorkspaceClient()
        )
        self._statements = statement_runner or WorkspaceStatementRunner(
            self._workspace, config.warehouse_id
        )
        self._connector = connector
        self._now = now

    @property
    def workspace(self) -> Any:
        """The bound control-plane client, for collaborators that must share it."""

        return self._workspace

    async def inspect_sync(self) -> ManagedSyncStatus:
        (
            postgres_synced,
            database_synced,
            project,
            branch,
            pipeline,
            source_schema,
            storage_schema,
            online_schema,
            storage_tables,
        ) = await asyncio.gather(
            self._get_json(
                f"/api/2.0/postgres/{quote(self.config.synced_table_resource_name, safe='/')}"
            ),
            self._get_json(
                f"/api/2.0/database/synced_tables/{quote(self.config.synced_table_id, safe='')}"
            ),
            self._get_json(f"/api/2.0/postgres/{quote(self.config.project_name, safe='/')}"),
            self._get_json(f"/api/2.0/postgres/{quote(self.config.branch, safe='/')}"),
            self._get_json(f"/api/2.0/pipelines/{quote(self.config.pipeline_id, safe='')}"),
            self._get_json(
                "/api/2.1/unity-catalog/schemas/"
                + quote(
                    f"{self.config.source_catalog}.{self.config.source_schema}", safe=""
                )
            ),
            self._get_json(
                "/api/2.1/unity-catalog/schemas/"
                + quote(
                    f"{self.config.storage_catalog}.{self.config.storage_schema}", safe=""
                )
            ),
            self._get_json(
                "/api/2.1/unity-catalog/schemas/"
                + quote(
                    f"{self.config.destination_catalog}.{self.config.physical_schema}",
                    safe="",
                )
            ),
            self._get_json(
                "/api/2.1/unity-catalog/tables"
                f"?catalog_name={quote(self.config.storage_catalog, safe='')}"
                f"&schema_name={quote(self.config.storage_schema, safe='')}"
            ),
        )
        postgres_state, postgres_last_sync_version = self._validate_postgres_synced_table(
            postgres_synced
        )
        database_state, processed, database_last_sync = self._validate_database_synced_table(
            database_synced
        )
        self._validate_project(project)
        self._validate_branch(branch)
        pipeline_state, pipeline_update_state = self._validate_pipeline(pipeline)
        self._validate_uc_schema(
            source_schema,
            f"{self.config.source_catalog}.{self.config.source_schema}",
        )
        self._validate_uc_schema(
            storage_schema,
            f"{self.config.storage_catalog}.{self.config.storage_schema}",
        )
        self._validate_uc_schema(
            online_schema,
            f"{self.config.destination_catalog}.{self.config.physical_schema}",
        )
        tables = storage_tables.get("tables") or []
        if (
            not isinstance(tables, list)
            or tables
            or storage_tables.get("next_page_token")
        ):
            raise ModelScoreLiveConfigurationError(
                "Round 4 auxiliary storage schema is not exactly empty"
            )
        source_version, _ = await self._source_head()
        cdf_enabled = await self._cdf_enabled()
        delta_info = _required_mapping(
            database_last_sync.get("delta_table_sync_info"),
            "Database Synced Table last-sync Delta position",
        )
        _timestamp(database_last_sync.get("sync_start_timestamp"))
        database_delta_version = _integer(delta_info.get("delta_commit_version"))
        if (
            processed > source_version
            or database_delta_version > source_version
            or (
                postgres_last_sync_version is not None
                and postgres_last_sync_version > source_version
            )
        ):
            raise ModelScoreLiveOperationError(
                "Managed Sync cursor advanced beyond the authoritative Delta source head"
            )
        states = {postgres_state, database_state}
        state = classify_managed_sync_state(states, pipeline_state, pipeline_update_state)
        database_status = _required_mapping(
            database_synced.get("data_synchronization_status"),
            "Database Synced Table synchronization status",
        )
        return ManagedSyncStatus(
            pipeline_id=str(database_status.get("pipeline_id") or ""),
            source_table=self.config.source_table_full_name,
            synced_table=self.config.application_table_name,
            state=state,
            cdf_enabled=cdf_enabled,
            continuous=True,
            source_version=source_version,
            last_processed_version=processed,
            last_sync_delta_version=database_delta_version,
            last_sync_delta_commit_time=_timestamp(delta_info.get("delta_commit_timestamp")),
            observed_at=_timestamp(self._now()),
            sync_end_time=_timestamp(database_last_sync.get("sync_end_timestamp")),
            failure=(
                str(
                    database_status.get("message")
                    or database_state
                    or postgres_state
                    or pipeline_state
                    or "unknown state"
                )
                if state is ManagedSyncState.RUNNING
                # An unhealthy sync is refused by message, so name the three
                # signals the decision was made from rather than whichever one
                # happened to be non-empty.
                else (
                    f"pipeline {pipeline_state or 'UNKNOWN'}"
                    f" · latest update {pipeline_update_state or 'NONE'}"
                    f" · synced table {database_state or postgres_state or 'UNKNOWN'}"
                )
            ),
        )

    async def _get_json(self, path: str) -> Mapping[str, Any]:
        payload = await asyncio.to_thread(
            self._workspace.api_client.do,
            "GET",
            path,
        )
        if not isinstance(payload, Mapping):
            raise ModelScoreLiveConfigurationError(
                f"Round 4 control plane returned an invalid resource for {path}"
            )
        return payload

    async def read_source(self, entity_id: str) -> ModelScoreRow | None:
        rows = await self._statements.execute(
            "SELECT entity_id, score, model_version, proof_nonce "
            f"FROM {_source_identifier(self.config.source_table_full_name)} "
            "WHERE entity_id = :entity_id",
            [SqlParameter("entity_id", entity_id, "STRING")],
        )
        return _exact_model_row(rows, allow_missing=True)

    async def commit_source_update(self, update: ModelScoreUpdate) -> DeltaCommit:
        before_version, _ = await self._source_head()
        source = _source_identifier(self.config.source_table_full_name)
        await self._statements.execute(
            f"""MERGE INTO {source} AS target
USING (
  SELECT :entity_id AS entity_id,
         CAST(:score AS DOUBLE) AS score,
         :model_version AS model_version,
         :proof_nonce AS proof_nonce
) AS incoming
ON target.entity_id = incoming.entity_id
WHEN MATCHED THEN UPDATE SET
  score = incoming.score,
  model_version = incoming.model_version,
  proof_nonce = incoming.proof_nonce
WHEN NOT MATCHED THEN INSERT (entity_id, score, model_version, proof_nonce)
VALUES (incoming.entity_id, incoming.score, incoming.model_version, incoming.proof_nonce)""",
            [
                SqlParameter("entity_id", update.entity_id, "STRING"),
                SqlParameter("score", repr(update.score), "DOUBLE"),
                SqlParameter("model_version", update.model_version, "STRING"),
                SqlParameter("proof_nonce", update.proof_nonce, "STRING"),
            ],
        )
        escaped_source = self.config.source_table_full_name.replace("'", "''")
        rows = await self._statements.execute(
            "SELECT entity_id, score, model_version, proof_nonce, "
            "_commit_version, _commit_timestamp, _change_type "
            f"FROM table_changes('{escaped_source}', :start_version) "
            "WHERE proof_nonce = :proof_nonce "
            "AND _change_type IN ('insert', 'update_postimage')",
            [
                SqlParameter("start_version", str(before_version + 1), "BIGINT"),
                SqlParameter("proof_nonce", update.proof_nonce, "STRING"),
            ],
        )
        if len(rows) != 1:
            raise ModelScoreLiveOperationError(
                "Delta CDF did not return exactly one insert/update_postimage for the nonce"
            )
        row = rows[0]
        if _model_row(row) != update.row:
            raise ModelScoreLiveOperationError("Delta CDF row did not match the committed update")
        version = _integer(row.get("_commit_version"))
        if version <= before_version:
            raise ModelScoreLiveOperationError("Delta CDF commit version did not advance")
        return DeltaCommit(
            version=version,
            committed_at=_timestamp(row.get("_commit_timestamp")),
        )

    async def read_application_fresh(self, entity_id: str) -> ModelScoreRow | None:
        endpoint, credential, current_user = await asyncio.gather(
            asyncio.to_thread(
                self._workspace.postgres.get_endpoint,
                self.config.endpoint_name,
            ),
            asyncio.to_thread(
                self._workspace.postgres.generate_database_credential,
                self.config.endpoint_name,
            ),
            asyncio.to_thread(self._workspace.current_user.me),
        )
        endpoint_payload = endpoint.as_dict() if hasattr(endpoint, "as_dict") else endpoint
        status = _mapping(_mapping(endpoint_payload).get("status"))
        hosts = _mapping(status.get("hosts"))
        host = str(hosts.get("host") or "")
        token = str(getattr(credential, "token", None) or _mapping(credential).get("token") or "")
        returned_user = str(
            getattr(current_user, "user_name", None) or _mapping(current_user).get("userName") or ""
        )
        if (
            self.config.expected_runtime_principal
            and returned_user.casefold() != self.config.expected_runtime_principal.casefold()
        ):
            raise ModelScoreLiveConfigurationError(
                "Runtime principal does not match the sealed Round 4 application principal"
            )
        user = self.config.database_user or returned_user
        if not host or not token or not user:
            raise ModelScoreLiveConfigurationError(
                "Fresh Lakebase host, OAuth credential, or database user is missing"
            )
        connection = await self._connector(
            host=host,
            port=self.config.port,
            dbname=self.config.physical_database,
            user=user,
            password=token,
            sslmode="require",
            application_name="lakebase-anti-demo-round-4",
            connect_timeout=max(1, math.ceil(self.config.connect_timeout_seconds)),
            autocommit=True,
        )
        application_table = _postgres_identifier(
            self.config.physical_schema,
            self.config.physical_table,
        )
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT entity_id, score, model_version, proof_nonce "
                    f"FROM {application_table} "
                    "WHERE entity_id = %s",
                    (entity_id,),
                )
                rows = await cursor.fetchall()
        mappings = [
            dict(zip(("entity_id", "score", "model_version", "proof_nonce"), row, strict=True))
            for row in rows
        ]
        return _exact_model_row(mappings, allow_missing=True)

    async def _source_head(self) -> tuple[int, datetime]:
        rows = await self._statements.execute(
            f"DESCRIBE HISTORY {_source_identifier(self.config.source_table_full_name)} LIMIT 1"
        )
        if len(rows) != 1:
            raise ModelScoreLiveOperationError("Delta history did not return one source head")
        return _integer(rows[0].get("version")), _timestamp(rows[0].get("timestamp"))

    async def _cdf_enabled(self) -> bool:
        rows = await self._statements.execute(
            f"SHOW TBLPROPERTIES {_source_identifier(self.config.source_table_full_name)} "
            "('delta.enableChangeDataFeed')"
        )
        return len(rows) == 1 and str(rows[0].get("value") or "").casefold() == "true"

    def _validate_postgres_synced_table(
        self, payload: Mapping[str, Any]
    ) -> tuple[str, int | None]:
        status = _required_mapping(payload.get("status"), "Postgres Synced Table status")
        actual = {
            "name": str(payload.get("name") or ""),
            "synced_table_id": str(payload.get("synced_table_id") or ""),
            "uid": str(payload.get("uid") or ""),
            "project": str(status.get("project") or ""),
            "pipeline_id": str(status.get("pipeline_id") or ""),
        }
        expected = {
            "name": self.config.synced_table_resource_name,
            "synced_table_id": self.config.synced_table_id,
            "uid": self.config.synced_table_uid,
            "project": self.config.project_name,
            "pipeline_id": self.config.pipeline_id,
        }
        if actual != expected:
            raise ModelScoreLiveConfigurationError(
                "Postgres Synced Table identity or ownership changed"
            )
        last_sync = status.get("last_sync")
        if last_sync is None:
            return _enum_value(status.get("detailed_state")), None
        last_sync = _required_mapping(last_sync, "Postgres Synced Table last-sync position")
        delta_info = _required_mapping(
            last_sync.get("delta_table_sync_info"),
            "Postgres Synced Table last-sync Delta position",
        )
        for field in ("delta_commit_time",):
            if delta_info.get(field) is not None:
                _timestamp(delta_info.get(field))
        for field in ("sync_start_time", "sync_end_time"):
            if last_sync.get(field) is not None:
                _timestamp(last_sync.get(field))
        return (
            _enum_value(status.get("detailed_state")),
            _integer(delta_info.get("delta_commit_version")),
        )

    def _validate_database_synced_table(
        self, payload: Mapping[str, Any]
    ) -> tuple[str, int, Mapping[str, Any]]:
        spec = _required_mapping(payload.get("spec"), "Database Synced Table spec")
        status = _required_mapping(
            payload.get("data_synchronization_status"),
            "Database Synced Table synchronization status",
        )
        actual = {
            "name": str(payload.get("name") or ""),
            "source": str(spec.get("source_table_full_name") or ""),
            "project_uid": str(payload.get("effective_database_project_id") or ""),
            "branch_uid": str(payload.get("effective_database_branch_id") or ""),
            "database": str(payload.get("effective_logical_database_name") or ""),
            "pipeline_id": str(status.get("pipeline_id") or ""),
        }
        expected = {
            "name": self.config.synced_table_id,
            "source": self.config.source_table_full_name,
            "project_uid": self.config.project_uid,
            "branch_uid": self.config.branch_uid,
            "database": self.config.physical_database,
            "pipeline_id": self.config.pipeline_id,
        }
        if actual != expected:
            raise ModelScoreLiveConfigurationError(
                "Database Synced Table identity or effective target changed"
            )
        if spec.get("primary_key_columns") != ["entity_id"]:
            raise ModelScoreLiveConfigurationError("Database Synced Table primary key changed")
        if _enum_value(spec.get("scheduling_policy")) != "CONTINUOUS":
            raise ModelScoreLiveConfigurationError("Database Synced Table is not continuous")
        continuous = _required_mapping(
            status.get("continuous_update_status"),
            "Database Synced Table continuous update status",
        )
        _timestamp(continuous.get("timestamp"))
        last_sync = _required_mapping(
            status.get("last_sync"),
            "Database Synced Table last-sync position",
        )
        return (
            _enum_value(status.get("detailed_state")),
            _integer(continuous.get("last_processed_commit_version")),
            last_sync,
        )

    def _validate_project(self, payload: Mapping[str, Any]) -> None:
        status = _required_mapping(payload.get("status"), "Postgres project status")
        if (
            payload.get("name") != self.config.project_name
            or payload.get("project_id") != self.config.project_id
            or payload.get("uid") != self.config.project_uid
            or status.get("project_id") != self.config.project_id
        ):
            raise ModelScoreLiveConfigurationError(
                "Postgres project name, UID, or effective ID changed"
            )

    def _validate_branch(self, payload: Mapping[str, Any]) -> None:
        status = _required_mapping(payload.get("status"), "Postgres branch status")
        if (
            payload.get("name") != self.config.branch
            or payload.get("parent") != self.config.project_name
            or payload.get("branch_id") != self.config.branch_id
            or payload.get("uid") != self.config.branch_uid
            or status.get("branch_id") != self.config.branch_id
        ):
            raise ModelScoreLiveConfigurationError(
                "Postgres branch name, UID, parent, or effective ID changed"
            )
        if _enum_value(status.get("current_state")) != "READY":
            raise ModelScoreLiveConfigurationError("Postgres branch is not ready")

    def _validate_pipeline(self, payload: Mapping[str, Any]) -> tuple[str, str]:
        spec = _required_mapping(payload.get("spec"), "Managed Sync pipeline spec")
        managed = _required_mapping(
            spec.get("managed_definition"), "Managed Sync pipeline definition"
        )
        table_sync = _required_mapping(
            managed.get("database_table_sync"), "Managed Sync database-table definition"
        )
        sinks = table_sync.get("sinks")
        expected_sink = {
            "src_table": self.config.source_table_full_name,
            "dest_table": self.config.application_table_name,
            "dest_table_uc_name": self.config.synced_table_id,
            "dest_table_id": self.config.synced_table_uid,
            "primary_key": ["entity_id"],
            "creator": self.config.setup_principal,
        }
        if (
            not isinstance(sinks, list)
            or len(sinks) != 1
            or not isinstance(sinks[0], Mapping)
            or any(sinks[0].get(key) != value for key, value in expected_sink.items())
        ):
            raise ModelScoreLiveConfigurationError("Managed Sync pipeline sink contract changed")
        if (
            payload.get("pipeline_id") != self.config.pipeline_id
            or payload.get("creator_user_name") != self.config.setup_principal
            or spec.get("catalog") != self.config.destination_catalog
            or spec.get("schema") != self.config.physical_schema
            or spec.get("continuous") is not True
            or _enum_value(spec.get("pipeline_type")) != "DATABASE_TABLE_SYNC"
        ):
            raise ModelScoreLiveConfigurationError(
                "Managed Sync pipeline identity or immutable specification changed"
            )
        return _enum_value(payload.get("state")), latest_pipeline_update_state(payload)

    def _validate_uc_schema(self, payload: Mapping[str, Any], full_name: str) -> None:
        if (
            payload.get("full_name") != full_name
            or payload.get("owner") != self.config.setup_principal
            or payload.get("created_by") != self.config.setup_principal
        ):
            raise ModelScoreLiveConfigurationError(
                f"Unity Catalog schema {full_name} identity or ownership changed"
            )


#: How long the pipeline is left running after a bout has settled.
#:
#: **The redo affordance is what sets this, not the cost.** D20's third blocker
#: establishes that there is no moment after which a bout is definitively over:
#: a verified Round 4 leaves a ``RedoState.READY`` the presenter may take at any
#: time, and a redo does not re-arm --- it goes straight to a commit and a sync
#: wait --- so a redo landing on a stopped pipeline is a round dying in front of
#: an audience. This window has to outlast the realistic gap between "the number
#: is on screen" and "do it again", including questions.
#:
#: Twenty minutes buys that for about **$0.20** at the standing rate, which is
#: the whole reason it can be generous. The saving being chased is the fifteen
#: idle *hours* a day nobody is presenting, and shaving this window would trade
#: the last of the risk margin for a rounding error.
IDLE_STOP_SECONDS = 1200.0

#: How long an immediate release waits for the stop before giving up on it.
#:
#: **Sized against a towel rather than against the control plane.** A towel is
#: the operator saying "stop this now" in front of a room, and it already waits
#: for Round 4's settlement; a stop is one POST on top of that. Thirty seconds
#: is long enough that an ordinary control-plane hop cannot expire it and short
#: enough that a wedged one surfaces as a named cleanup failure instead of a
#: towel that appears to hang. Expiring is not silence: the caller turns it into
#: ``cleanup_failure`` and the idle release is re-armed behind it.
TOWEL_STOP_TIMEOUT_SECONDS = 30.0

#: How long shutdown waits for a stop it owes before writing it down instead.
#:
#: **Shorter than the towel's thirty seconds, and deliberately so.** A towel is
#: one operator waiting on one thing in front of a room; this sits inside
#: ``RunManager.close()``, behind task settlement and ahead of ring release and
#: the coordination stores, on a path an operator experiences as "the server did
#: not exit". Ten seconds covers an ordinary control-plane hop with room to
#: spare, and the cost of expiring is small and bounded now that expiring is not
#: silence: the owed record is rewritten as due immediately, so the next status
#: read names the pipeline, the money and the command that fixes it.
SHUTDOWN_STOP_TIMEOUT_SECONDS = 10.0

#: How often the start-and-wait re-reads the pipeline while it comes back.
#:
#: The measurement this is sized against saw the synced table reach
#: ``UPDATING_PIPELINE_RESOURCES`` at 6.8 s and full health by 19.2 s, so a
#: three-second cadence puts several honest progress lines in front of an
#: audience during a typical resume rather than one long silence.
START_POLL_SECONDS = 3.0

#: One activation per sealed pipeline per process.
#:
#: Keyed rather than owned by the engine because **a new engine is constructed
#: for every arm** while the pipeline outlives all of them. An activation owned
#: per engine would let a previous bout's pending release fire in the middle of
#: the next bout: the release task would be holding a timer nobody armed against.
#: Sharing one activation is what makes "an arm cancels the pending release" true
#: across bouts rather than only within one.
_ACTIVATIONS: dict[str, Round4PipelineActivation] = {}


class Round4PipelineActivation:
    """Start the sealed pipeline before an arm, and stop it once a bout is done.

    This is the narrow amendment to D9a and D20a, and the narrowness is the whole
    of its safety case. It can address exactly one pipeline --- every request goes
    through ``pipeline_power._require_pipeline_path`` --- and it can issue exactly
    two verbs against it, ``/stop`` and ``/updates``. It never reads or writes the
    synced table, never touches the manifest, and cannot reach any resealing path,
    so the failure D20's first blocker describes --- a recreated synced table
    minting a new ``pipeline_id``, demoting the manifest to v2 and destroying
    Rounds 5 and 6 --- has no route through here. Measured 2026-08-24: across a
    stop and a resume the ``pipeline_id``, project UID, branch UID, creator and
    ``scheduling_policy`` were all unchanged, and the sync cursor resumed at the
    same Delta version it stopped on rather than re-seeding.

    **Why starting at arm does not touch what Round 4 measures.** The headline
    number is ``sync_end - committed_at`` on the bout's own Delta commit, and
    ``_prove_update`` refuses unless the status commit timestamp equals that exact
    commit. Nothing reads a backfill watermark or any window before the bell. A
    pipeline started ninety seconds before the bell therefore measures the same
    thing a pipeline resident for a week measures --- and ``arm()``'s own
    ``_prove_pipeline_warm`` still round-trips a throwaway commit afterwards, so
    warmth remains proven rather than assumed no matter how the pipeline got here.

    **Why the stop is not the automatic stop D20a rejected.** D20a rejected a
    shutdown hook and an idle timer because both *guess* that a session is over,
    and guessing wrong left a round unable to arm. Both halves have moved. This
    fires only after Round 4's own settlement has finished and the engine has been
    idle since, so it is not guessing about a bout in flight; and the state it
    produces is one the next arm recovers from by itself in about twenty seconds,
    so guessing wrong now costs a slower arm rather than a dead round.
    """

    def __init__(
        self,
        manifest: DemoManifest,
        api: Callable[..., dict[str, Any]],
        *,
        pipeline_id: str,
        idle_seconds: float = IDLE_STOP_SECONDS,
        wait_timeout_seconds: float = START_WAIT_TIMEOUT_SECONDS,
        poll_seconds: float = START_POLL_SECONDS,
        stop_timeout_seconds: float = TOWEL_STOP_TIMEOUT_SECONDS,
        shutdown_stop_timeout_seconds: float = SHUTDOWN_STOP_TIMEOUT_SECONDS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._manifest = manifest
        self._api = api
        self._pipeline_id = pipeline_id
        self._idle_seconds = idle_seconds
        self._wait_timeout_seconds = wait_timeout_seconds
        self._poll_seconds = poll_seconds
        self._stop_timeout_seconds = stop_timeout_seconds
        self._shutdown_stop_timeout_seconds = shutdown_stop_timeout_seconds
        self._sleep = sleep
        self._clock = clock
        self._now = now
        self._release: asyncio.Task[None] | None = None
        # Bumped by every arm. The pending release compares the value it was
        # scheduled against with the value it wakes to, which is what makes "an
        # arm happened while I slept" a fact rather than a race.
        self._generation = 0
        # Whether the most recent arm is what put this pipeline up. Recorded
        # because "restore what I changed" is a contract this activation can
        # keep and "stop the pipeline" is not: an operator who warmed it by
        # hand, or a previous bout that left it up, is state somebody chose,
        # and :meth:`release_now` must not take it away. The delayed release
        # deliberately does not consult this -- see there.
        self._started_by_arm = False
        # The same claim widened from one arm to this whole process, and never
        # cleared. `_started_by_arm` is wrong for shutdown: a second bout that
        # arms onto a pipeline the *first* bout started clears it, and quitting
        # after that second bout would then leave running a pipeline this
        # process is entirely responsible for. See :meth:`aclose`.
        self._started_by_process = False
        # The timestamp of the most recent resume this activation requested,
        # carried into any owed-stop record so scheduling a stop does not cost
        # the accrued figure its origin. See `pipeline_power.owed_stop_record`.
        self._resumed_at = ""

    async def ensure_running(self, notify: Callable[[str], Awaitable[None]]) -> None:
        """Bring the pipeline to a healthy continuous sync, however long that takes.

        Returns immediately and costs one GET in the common case, which is the
        pipeline already running. That matters: this sits at the top of the arm
        path, and an audience is watching it.
        """

        self._generation += 1
        self._cancel_release()
        if self._healthy(await self._read_signals()):
            self._started_by_arm = False
            return

        await notify(
            "The Managed Sync pipeline is not running. Starting it before the bell — "
            f"this usually takes about {RESTART_SECONDS_ESTIMATE}s."
        )
        def remember(record: dict[str, Any]) -> None:
            self._resumed_at = str(record.get("resumed_at") or "")
            self._persist(record)

        pipeline_power.start(
            self._manifest,
            self._api,
            on_record=remember,
        )
        # Set on the request rather than on the successful wait. The claim this
        # records is "this arm asked for the pipeline that is now up", and that
        # is true the moment the verb is issued -- an arm that then times out
        # still left a resuming pipeline behind it.
        self._started_by_arm = True
        self._started_by_process = True
        deadline = self._clock() + self._wait_timeout_seconds
        while True:
            await self._sleep(self._poll_seconds)
            signals = await self._read_signals()
            if self._healthy(signals):
                await notify("The Managed Sync pipeline is running. Verifying the baseline.")
                return
            remaining = deadline - self._clock()
            if remaining <= 0:
                break
            # Named states rather than a spinner. An operator who can see
            # DEPLOYING knows the difference between slow and stuck, and this is
            # the one place in an arm where a wait can last a minute.
            await notify(
                f"Waiting for the Managed Sync pipeline: {signals.describe()} "
                f"({int(remaining)}s left before this arm gives up)."
            )
        raise ModelScoreLiveOperationError(
            "The Managed Sync pipeline did not reach a healthy continuous sync within "
            f"{self._wait_timeout_seconds:.0f}s of being started, so Round 4 refuses to "
            "arm rather than measure a pipeline that is still coming up. Check "
            "'antidemo pipeline status'."
        )

    async def release_now(self) -> None:
        """Give the pipeline back at once, for a bout that left no redo behind.

        :data:`IDLE_STOP_SECONDS` is bought entirely by the redo affordance --
        see its own note -- so a bout that can never be redone is owed none of
        it. A towelled bout is exactly that: ``_finish_model_score`` returns
        early on a non-null towel, so no ``RedoState.READY`` is ever published
        and there is nothing for twenty minutes of billing to protect.

        **It stops what this arm started and nothing else, which is the whole
        of its safety case.** The delayed release may stop any healthy pipeline,
        because twenty idle minutes after a settled bout is itself evidence
        nobody is mid-session. An immediate stop has no such evidence, so it
        keeps the narrower contract instead: restore what this arm changed. A
        pipeline that was already up when the bout armed was somebody's choice
        -- an operator warming it, or a previous bout that has not aged out --
        and it keeps the idle release it would have had.

        **Raises rather than logs.** A stop that did not happen is the one
        failure in this module that costs money silently, and the towel has a
        place to put it: the caller turns this into ``cleanup_failure``, which
        the receipt carries and the retry affordance keys on. The idle release
        is re-armed on the way out, so surfacing the failure does not also
        throw away the fallback that would have caught it.
        """

        if not self._started_by_arm:
            self.release_when_idle()
            return

        self._cancel_release()
        try:
            if not self._healthy(await self._read_signals()):
                # Already down, or down for a reason that is not ours. Same rule
                # the delayed release keeps, for the same reason: a
                # deliberate-stop record written over a genuine failure hides
                # the failure, and telling those apart is all the record is for.
                self._started_by_arm = False
                return
            async with asyncio.timeout(self._stop_timeout_seconds):
                await self._stop_off_loop()
        except Exception as exc:
            self.release_when_idle()
            raise ModelScoreLiveOperationError(
                "The Round 4 Managed Sync pipeline was started for this bout and could "
                f"not be stopped after the towel, so it is still billing about "
                f"${pipeline_power.PIPELINE_USD_PER_DAY:.2f}/day. Stop it with "
                "'antidemo pipeline stop', or retry cleanup."
            ) from exc
        self._started_by_arm = False
        LOGGER.info(
            "Stopped the Round 4 Managed Sync pipeline immediately after a towelled "
            "bout; it left no redo to protect and the next arm will start it again",
        )

    def release_when_idle(self) -> None:
        """Schedule the stop that ends this bout's billing. Never blocks a caller.

        Called from the tail of settlement, which is itself fire-and-forget after
        the terminal receipt. Scheduling rather than awaiting is not a shortcut:
        a stop awaited here would sit between a bout finishing and the settlement
        task completing, and D20's third blocker is that settlement needs the
        pipeline live.
        """

        self._cancel_release()
        generation = self._generation
        try:
            self._release = asyncio.create_task(
                self._release_after_idle(generation),
                name=f"round4-pipeline-release-{self._pipeline_id}",
            )
        except RuntimeError:
            # No running loop. Nothing to schedule against and nothing to save:
            # the process is not serving.
            self._release = None
            return
        self._record_stop_owed(self._now() + timedelta(seconds=self._idle_seconds))

    def _record_stop_owed(self, due_at: datetime) -> None:
        """Write down that a stop is owed, before anything can lose the timer.

        **Written when the timer is scheduled rather than only when shutdown
        fails**, and that is the design decision this whole path turns on. Three
        things settled it.

        The first is that a graceful handler cannot be the only answer. A
        ``SIGKILL``, an OOM kill or a container eviction runs no handler at all,
        and on a deployed replica eviction is ordinary rather than exotic. In
        that case a record written at shutdown is a record that is never
        written, and the pipeline goes on billing $14.57/day with nothing
        anywhere recording that anybody meant to stop it.

        The second is that shutdown is the worst possible moment to need the
        durable store. It is being torn down in the same sequence -- the app
        uninstalls it during ``_close_runtime`` -- so a write attempted there
        races the teardown that removes its destination. At schedule time both
        the store and the loop are certainly present, because a bout has just
        run through them.

        The third is that this project already solved the same problem in the
        same shape. ``_INTENT_RESUMING`` exists because an in-flight state with
        no record read as a failure; it is written when the intent is *formed*
        and given a grace window so it does not cry wolf while the thing is
        legitimately in progress. An owed stop is that rule run backwards, and
        :func:`pipeline_power.read_stop_marker` holds it to the same discipline:
        this record is not in effect until ``due_at`` has passed, so the twenty
        minutes a settled bout deliberately holds the pipeline for its redo stay
        silent, and only a stop that should have happened and did not can speak.

        The cost is one small write per settled bout and a record that is
        superseded moments later by the ``stopped`` record the release writes.
        That is the right trade against a pipeline that goes on billing
        $14.57/day with nothing anywhere recording that a stop was owed.
        """

        try:
            record = pipeline_power.owed_stop_record(
                self._manifest,
                due_at=due_at,
                resumed_at=self._resumed_at,
            )
        except Exception:
            # Never at the expense of the bout. This runs on the tail of
            # settlement, after the terminal receipt is already published, and a
            # bookkeeping write must not be the thing that fails a declared bout.
            LOGGER.warning(
                "Could not record that a Round 4 pipeline stop is owed",
                exc_info=True,
            )
            return
        self._persist(record)

    async def _release_after_idle(self, generation: int) -> None:
        try:
            await self._sleep(self._idle_seconds)
            if generation != self._generation:
                # An arm happened while this slept, so the bout this was
                # scheduled for is not the current one. The newer arm scheduled
                # its own release.
                return
            signals = await self._read_signals()
            if not self._healthy(signals):
                # Already down, or down for a reason that is not ours. Stopping a
                # pipeline that is failing would overwrite a genuine failure with
                # a deliberate-stop record, and the difference between those two
                # is the only thing the record is for.
                return
            # Off the loop, like every other call this class makes to the
            # control plane. `pipeline_power.stop` is a blocking SDK POST, and
            # awaiting it inline stalled the whole server -- including the SSE
            # stream an audience is watching -- for the duration of the round
            # trip. `_read_signals` above has always done this; the stop was the
            # one that did not.
            await self._stop_off_loop()
            LOGGER.info(
                "Stopped the Round 4 Managed Sync pipeline after %.0fs idle following "
                "settlement; the next arm will start it again",
                self._idle_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # A stop that did not happen costs money and nothing else. It must
            # never surface as a failure of the bout that has already been
            # declared and whose receipt is already published.
            LOGGER.warning(
                "Could not stop the Round 4 Managed Sync pipeline after settlement; it "
                "is still billing",
                exc_info=True,
            )
        finally:
            self._release = None

    async def aclose(self) -> None:
        """Perform a stop this process owes before the process goes away.

        The delayed release is an in-memory ``asyncio`` task, so before this
        existed a server stopped inside the twenty-minute window simply lost it:
        the task died with the loop, no verb was issued, no record was written,
        and the next ``doctor`` read a fully billing pipeline as one nobody had
        ever intended to stop. The operator behaviour that triggers it is the
        most ordinary one there is -- finish the demo, quit the server.

        **It performs the stop rather than cancelling the timer.** Cancelling
        silently is precisely the defect; a shutdown that tidies away the
        obligation without discharging it is what made this invisible.

        **It is bounded, and a stop it cannot perform is written down rather
        than swallowed.** A stop is a control-plane round trip and shutdown must
        not hang on one, so the whole attempt sits inside
        :data:`SHUTDOWN_STOP_TIMEOUT_SECONDS`. When it expires, the owed record
        is rewritten as due *now* so that the very next ``antidemo pipeline
        status`` or session notice says so, instead of staying quiet for the
        remainder of a window whose timer no longer exists. A shutdown that
        fails to stop the pipeline and says so plainly is a far better outcome
        than one that succeeds quietly nine times and on the tenth leaves the
        pipeline up at $14.57/day with nothing saying so.

        **It stops only a pipeline this process started.** The delayed release
        may stop any healthy pipeline because twenty idle minutes after a
        settled bout is itself evidence that nobody is presenting. Shutdown has
        no such evidence -- it fires the instant the operator quits -- so it
        keeps the narrower contract ``release_now`` keeps, one process wide: if
        this process ever brought the pipeline up, the stop is its own to
        perform. A pipeline that was already up when this process started is
        somebody else's state, and quitting is not a reason to take it away. The
        owed record is still written in that case, so the obligation is visible
        even where this refuses to act on it.
        """

        if self._release is None:
            # Nothing pending, so nothing is owed. Either no bout settled in
            # this process, or the release already fired and wrote its own
            # record.
            return
        self._cancel_release()
        if not self._started_by_process:
            LOGGER.warning(
                "Shutting down with a Round 4 pipeline stop still owed, but this "
                "process did not start that pipeline, so it is left running. It is "
                "billing about $%.2f/day; stop it with 'antidemo pipeline stop'.",
                pipeline_power.PIPELINE_USD_PER_DAY,
            )
            self._record_stop_owed(self._now())
            return
        try:
            async with asyncio.timeout(self._shutdown_stop_timeout_seconds):
                if not self._healthy(await self._read_signals()):
                    # Same rule as both other releases: a deliberate-stop record
                    # written over a genuine failure hides the failure.
                    return
                await self._stop_off_loop()
        except (asyncio.CancelledError, Exception) as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise
            LOGGER.error(
                "Shutdown could not stop the Round 4 Managed Sync pipeline, which is "
                "still billing about $%.2f/day. A stop is recorded as owed against it; "
                "'antidemo pipeline status' will say so and 'antidemo pipeline stop' "
                "will clear it.",
                pipeline_power.PIPELINE_USD_PER_DAY,
                exc_info=True,
            )
            self._record_stop_owed(self._now())
            return
        LOGGER.info(
            "Stopped the Round 4 Managed Sync pipeline during shutdown rather than "
            "losing the release this bout had scheduled",
        )

    async def _stop_off_loop(self) -> None:
        """Issue the stop without blocking the loop, and persist its record on it.

        ``pipeline_power.stop`` is a synchronous SDK POST by design -- the CLI
        calls it from a command -- so every caller in a serving process has to
        push it into a worker thread. Awaiting it inline stalls the whole event
        loop for the round trip, including the SSE stream an audience is
        watching.

        Records are collected and persisted by the caller's loop rather than
        handed to ``_persist`` inside the worker, because ``_persist`` schedules
        onto a running loop and a worker thread has none. That is the one place
        the durable record could be lost, and losing it puts `doctor` back to
        reading a deliberate stop as a pipeline         that fell over.

        Bounding is the caller's job, through ``asyncio.timeout``. Both bounded
        callers want a different number -- a towel waits thirty seconds, a
        shutdown ten -- and both want the collected records persisted even when
        the bound expires, which the ``finally`` here guarantees for either.
        """

        records: list[dict[str, Any]] = []
        try:
            await asyncio.to_thread(
                pipeline_power.stop,
                self._manifest,
                self._api,
                on_record=records.append,
            )
        finally:
            for record in records:
                self._persist(record)

    def _cancel_release(self) -> None:
        release = self._release
        self._release = None
        if release is not None and not release.done():
            release.cancel()

    def _persist(self, record: dict[str, Any]) -> None:
        """Push a power record at the durable store, if this process has one.

        Scheduled rather than awaited because :func:`pipeline_power.stop` and
        :func:`pipeline_power.start` are synchronous by design -- the CLI calls
        them from a command -- and because the cloud change has already
        happened by the time this runs.

        **Off the loop it says so rather than returning.** This used to swallow
        the ``RuntimeError`` and drop the record, which was silent in the one
        module whose entire purpose is that a deliberate pipeline change is
        never confused with a failure. No caller reaches it off-loop today --
        every one of them collects records and persists them back here on the
        loop, which is the correct fix and the reason this path stays a guard
        rather than becoming a second writer. It is kept loud because
        ``pipeline_power.stop`` is now called through ``asyncio.to_thread`` in
        three places, and a future fourth that forgets to collect would
        otherwise lose the record with no trace at all.
        """

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            LOGGER.error(
                "A Round 4 pipeline power record (%s) was produced off the event loop "
                "and could not be persisted durably, so a later check cannot tell this "
                "deliberate change from a failure. Collect records at the call site and "
                "persist them on the loop instead.",
                record.get("intent", "unknown"),
            )
            return
        task = asyncio.create_task(pipeline_power.record_power_request(record))
        _PENDING_RECORDS.add(task)
        task.add_done_callback(_PENDING_RECORDS.discard)

    async def _read_signals(self) -> _PipelineSignals:
        """The three states that decide whether a sync is healthy.

        Read here rather than through ``inspect_sync`` because ``inspect_sync``
        cannot answer this question: a stopped pipeline's synced table omits
        ``continuous_update_status`` entirely -- measured 2026-08-24 -- and
        ``_validate_database_synced_table`` requires it as a non-empty mapping,
        so the call raises before it can report that the pipeline is down. The
        classification below is the same ``classify_managed_sync_state`` the
        adapter uses; only the fetch is more forgiving.
        """

        pipeline = await asyncio.to_thread(
            self._api,
            self._manifest.databricks.profile,
            "get",
            f"/api/2.0/pipelines/{self._pipeline_id}",
        )
        synced = await asyncio.to_thread(
            self._api,
            self._manifest.databricks.profile,
            "get",
            f"/api/2.0/database/synced_tables/{quote(self._synced_table_id, safe='')}",
        )
        status = _mapping(synced.get("data_synchronization_status"))
        return _PipelineSignals(
            pipeline_state=_enum_value(pipeline.get("state")),
            update_state=latest_pipeline_update_state(pipeline),
            synced_table_state=_enum_value(status.get("detailed_state")),
            continuous_reported=bool(_mapping(status.get("continuous_update_status"))),
        )

    @property
    def _synced_table_id(self) -> str:
        sealed = self._manifest.round4
        return str(getattr(sealed, "synced_table_id", "") or "")

    @staticmethod
    def _healthy(signals: _PipelineSignals) -> bool:
        if not signals.continuous_reported:
            return False
        return (
            classify_managed_sync_state(
                {signals.synced_table_state},
                signals.pipeline_state,
                signals.update_state,
            )
            is ManagedSyncState.RUNNING
        )


@dataclass(frozen=True, slots=True)
class _PipelineSignals:
    pipeline_state: str
    update_state: str
    synced_table_state: str
    continuous_reported: bool

    def describe(self) -> str:
        table = self.synced_table_state or "no synced-table state"
        if not self.continuous_reported:
            table = f"{table}, no continuous update reported yet"
        return (
            f"pipeline {self.pipeline_state or 'UNKNOWN'} · "
            f"update {self.update_state or 'NONE'} · {table}"
        )


#: Strong references to in-flight durable power writes, for the same reason
#: `receipts._pending_writes` exists: a task referenced only by the event loop
#: may be collected mid-write.
_PENDING_RECORDS: set[asyncio.Task[bool]] = set()


def _pipeline_activation(manifest: DemoManifest, workspace: Any) -> Round4PipelineActivation | None:
    """The one activation for this manifest's sealed pipeline, built once."""

    sealed = manifest.round4
    pipeline_id = str(getattr(sealed, "pipeline_id", "") or "")
    if not pipeline_id:
        return None
    existing = _ACTIVATIONS.get(pipeline_id)
    if existing is not None:
        return existing
    activation = Round4PipelineActivation(
        manifest,
        pipeline_power.workspace_api(workspace),
        pipeline_id=pipeline_id,
    )
    _ACTIVATIONS[pipeline_id] = activation
    return activation


async def aclose_activations() -> None:
    """Discharge every pipeline stop this process still owes. Never raises.

    Module-level because :data:`_ACTIVATIONS` is: an activation is keyed by
    pipeline and outlives the engines and the session records, so there is no
    per-record handle for ``RunManager.close()`` to find one through. That is
    exactly why the pending release was missed -- ``close()`` accounts for the
    eight tasks a session record owns, and this task belongs to none of them.

    Never raising is the contract shutdown needs. This runs among the other
    cleanups and a failure here must not prevent a ring being released or a
    coordination store being closed; :meth:`Round4PipelineActivation.aclose`
    already turns its own failures into a durable owed record, so the loud
    outcome is preserved without the exception.
    """

    for activation in tuple(_ACTIVATIONS.values()):
        try:
            await activation.aclose()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.error(
                "Shutdown could not settle a Round 4 pipeline release",
                exc_info=True,
            )
    await drain_power_records()


async def drain_power_records() -> None:
    """Let the durable power writes finish before the loop goes away.

    ``_persist`` schedules rather than awaits, which is right on every other
    path -- the cloud change has already happened and a bookkeeping write must
    not sit in front of a bout. At shutdown it is exactly wrong: the record
    saying a stop happened, or that one is still owed, would be a task cancelled
    with the loop, which is the same shape of silent loss this whole change is
    about, one level down. ``receipts.drain_receipt_writes`` exists for the
    identical reason and is drained in the identical place.

    Bounded, because shutdown must not hang on a coordination round trip either.
    A write that does not land inside the bound is logged rather than waited on;
    the pipeline itself has already been dealt with by that point.
    """

    pending = tuple(_PENDING_RECORDS)
    if not pending:
        return
    done, unfinished = await asyncio.wait(pending, timeout=SHUTDOWN_STOP_TIMEOUT_SECONDS)
    for task in done:
        with suppress(asyncio.CancelledError, Exception):
            task.result()
    if unfinished:
        LOGGER.error(
            "%d Round 4 pipeline power record(s) did not reach the durable store before "
            "shutdown, so a later check may not be able to tell a deliberate stop from "
            "a failure",
            len(unfinished),
        )


def build_model_score_engine(manifest: DemoManifest) -> ModelScoreEngine:
    """Build the sealed live engine without performing any network operation."""

    resources = manifest.round4
    if manifest.manifest_version not in (2, 3, 4, 5, 6, 7) or resources is None:
        raise ModelScoreLiveConfigurationError(
            "A sealed manifest v2-v7 Round 4 contract is required"
        )
    contract = ModelScoreContract(
        pipeline_id=resources.pipeline_id,
        source_table=resources.source_table_full_name,
        synced_table=(
            f"{resources.physical_database}.{resources.physical_schema}.{resources.physical_table}"
        ),
    )
    if contract.sha256 != resources.contract_sha256:
        raise ModelScoreLiveConfigurationError("Round 4 contract hash does not match the manifest")
    is_databricks_app = os.environ.get("ANTI_DEMO_ENV", "").casefold() == "databricks-app" or bool(
        os.environ.get("DATABRICKS_APP_NAME", "")
    )
    config = ModelScoreLiveConfig(
        profile="" if is_databricks_app else manifest.databricks.profile,
        warehouse_id=resources.warehouse_id,
        setup_principal=resources.setup_principal,
        source_table_full_name=resources.source_table_full_name,
        storage_catalog=resources.storage_catalog,
        storage_schema=resources.storage_schema,
        synced_table_resource_name=resources.synced_table_resource_name,
        synced_table_id=resources.synced_table_id,
        synced_table_uid=resources.synced_table_uid,
        pipeline_id=resources.pipeline_id,
        physical_database=resources.physical_database,
        physical_schema=resources.physical_schema,
        physical_table=resources.physical_table,
        project_uid=resources.project_uid,
        branch_uid=resources.branch_uid,
        branch=resources.branch,
        endpoint_name=resources.endpoint_name,
        database_user="" if is_databricks_app else manifest.databricks.user,
        expected_runtime_principal=(
            resources.app_service_principal_client_id or "" if is_databricks_app else ""
        ),
    )
    adapter = LiveModelScoreAdapter(config)
    # A live inspection fans out across the sealed control-plane resources and
    # performs two bounded Statement Execution reads. Five seconds is a useful
    # fake/unit bound but is too short for a real warehouse/control-plane hop.
    return ModelScoreEngine(
        adapter,
        contract=contract,
        inspect_timeout_seconds=30.0,
        # Built from the adapter's own WorkspaceClient rather than a second one,
        # so the pipeline is powered by exactly the identity that inspects it. On
        # the deployed path that is the app's service principal, which now holds
        # CAN_RUN on this pipeline and nothing else.
        activation=_pipeline_activation(manifest, adapter.workspace),
    )


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "as_dict"):
        converted = value.as_dict()
        if isinstance(converted, Mapping):
            return converted
    return {}


def _required_mapping(value: object, label: str) -> Mapping[str, Any]:
    converted = _mapping(value)
    if not converted:
        raise ModelScoreLiveConfigurationError(f"{label} is missing or invalid")
    return converted


def _enum_value(value: object) -> str:
    return str(getattr(value, "value", value) or "")


def latest_pipeline_update_state(payload: Mapping[str, Any]) -> str:
    """State of the newest pipeline update, or "" when there is none.

    Public because `lifecycle`'s Round 4 waits read the same field from the same
    endpoint to answer the same question, and a second implementation of
    "newest-first, tolerate an absent array" is how the two would come to
    disagree about a stopped pipeline.

    ``latest_updates`` is documented as ordered with the newest update first.
    An empty or absent array means the pipeline has never produced an update,
    which cannot prove a continuous pipeline is syncing, so "" deliberately
    classifies as stopped rather than defaulting to healthy.
    """

    updates = payload.get("latest_updates")
    if not isinstance(updates, Sequence) or isinstance(updates, str | bytes):
        return ""
    for update in updates:
        mapping = _mapping(update)
        if mapping:
            return _enum_value(mapping.get("state"))
        return ""
    return ""


def _integer(value: object) -> int:
    if isinstance(value, bool):
        raise ModelScoreLiveOperationError("Expected an integer, received a boolean")
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ModelScoreLiveOperationError("Expected an integer in the live response") from exc


def _timestamp(value: object) -> datetime:
    if hasattr(value, "ToDatetime"):
        value = value.ToDatetime(tzinfo=UTC)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ModelScoreLiveOperationError("Live response timestamp was invalid") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ModelScoreLiveOperationError("Live response timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _source_identifier(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 3 or any(not part for part in parts):
        raise ModelScoreLiveConfigurationError(
            "Round 4 source table must be a three-part identifier"
        )
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


def _postgres_identifier(schema: str, table: str) -> str:
    if not schema or not table or "." in schema or "." in table:
        raise ModelScoreLiveConfigurationError(
            "Round 4 PostgreSQL schema and table must be separate identifiers"
        )
    return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in (schema, table))


def _model_row(row: Mapping[str, object]) -> ModelScoreRow:
    try:
        return ModelScoreRow(
            entity_id=str(row["entity_id"]),
            score=float(row["score"]),
            model_version=str(row["model_version"]),
            proof_nonce=str(row["proof_nonce"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ModelScoreLiveOperationError("Live model score row was malformed") from exc


def _exact_model_row(
    rows: Sequence[Mapping[str, object]],
    *,
    allow_missing: bool,
) -> ModelScoreRow | None:
    if not rows and allow_missing:
        return None
    if len(rows) != 1:
        raise ModelScoreLiveOperationError("Expected exactly one model score row")
    return _model_row(rows[0])
