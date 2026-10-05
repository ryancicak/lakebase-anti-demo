"""Round 4's live lanes: Lakebase's synced-table pipeline and AWS's Glue writer.

The protocol is ``server/round4_race.py``; this is how each lane does what it asks, against the
sealed resources and nothing else.

- **The shared source** is Round 4's Delta table, reached through the same Statement Execution
  the v1 proof used, so the change, its conflict retry and its change-feed lookup are unchanged.
- **Lakebase's lane** is the sealed synced table. Its pipeline is started at the bell and parked
  after the bout through ``pipeline_power``, which keeps the durable power record, so ``doctor``
  and startup recovery read every start and stop.
- **AWS's lane** is the sealed Glue job for the matchup's competitor, started and parked through
  ``server/round4_glue.py``.
- **The verifier** is one query shape on one client for both lanes: a persistent psycopg
  connection, TLS required, autocommit, reading the entity's four columns. On Lakebase that is the
  synced table; on AWS the view over the writer's ledger that hides its tombstones.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import threading
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg import sql

from . import pipeline_power
from .aws_auth import runtime_auth_from_environment
from .manifest import DemoManifest
from .model_score import DeltaCommit, ModelScoreRow, ModelScoreUpdate
from .model_score_live import (
    DELTA_CONCURRENT_TRANSACTION_SQLSTATE,
    DELTA_CONFLICT_ATTEMPTS,
    DELTA_CONFLICT_RETRY_SECONDS,
    PIPELINE_UPDATE_FAILED_STATES,
    LiveModelScoreAdapter,
    ModelScoreLiveConfigurationError,
    PipelineSignals,
    WorkspaceStatementExecutionError,
    build_model_score_engine,
    read_pipeline_signals,
)
from .models import CompetitorId
from .round4_glue import TERMINAL_STATES, GlueWriterJob, starting_version
from .round4_race import (
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    Bell,
    Notify,
    Round4RaceEngine,
    Round4RaceError,
)
from .targets import (
    AuroraCredentialProvider,
    ConnectionMaterial,
    RdsCredentialProvider,
    _runtime_aws_session,
)

LOGGER = logging.getLogger(__name__)

#: How long a park or a pre-bell wait may take before Prepare names it.
PARK_TIMEOUT_SECONDS = 300.0
PARK_POLL_SECONDS = 3.0
#: A connect wide enough for an Aurora cluster resuming from zero, which Prepare pays, not a clock.
CONNECT_TIMEOUT_SECONDS = 45
#: Newest-update states in which the pipeline is still doing something.
_ACTIVE_UPDATE_STATES = frozenset(
    {
        "QUEUED",
        "CREATED",
        "WAITING_FOR_RESOURCES",
        "INITIALIZING",
        "RESETTING",
        "SETTING_UP_TABLES",
        "RUNNING",
        "STOPPING",
    }
)
#: The owed stop a bell records, so a process that dies mid-bout leaves the stop on record.
BOUT_STOP_DUE_SECONDS = 1800.0

_READ = "SELECT entity_id, score, model_version, proof_nonce FROM {} WHERE entity_id = %s"


def _row(values: tuple[Any, ...] | None) -> ModelScoreRow | None:
    if values is None:
        return None
    entity, score, model_version, nonce = values
    return ModelScoreRow(
        entity_id=str(entity),
        score=float(score),
        model_version=str(model_version),
        proof_nonce=str(nonce),
    )


class PostgresReader:
    """The verifier's connection to one destination, opened before the bell.

    A destination can end that connection under it. Lakebase suspends a compute after a
    minute with no query, and an open but idle connection doesn't keep it awake: on
    2026-10-02 rc9 lost a Round 4 Prepare to AdminShutdown six seconds after its head-start
    connection opened. So a read that finds its connection gone opens a new one, which wakes
    the compute, and reads once more. Both lanes' verifiers are this class, so each gets the
    same retry.
    """

    def __init__(
        self,
        connection: Any,
        relation: sql.Composable,
        *,
        reconnect: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        self._connection = connection
        self._query = sql.SQL(_READ).format(relation)
        self._reconnect = reconnect

    async def read(self, entity_id: str) -> ModelScoreRow | None:
        try:
            rows = await self._fetch(entity_id)
        except (psycopg.OperationalError, psycopg.InterfaceError) as lost:
            if self._reconnect is None:
                raise
            LOGGER.info("Round 4 verifier connection was lost (%s); opening a new one", lost)
            try:
                await self._connection.close()
            except Exception:  # noqa: BLE001 - the connection is already gone
                pass
            self._connection = await self._reconnect()
            rows = await self._fetch(entity_id)
        if len(rows) > 1:
            raise Round4RaceError("A destination returned more than one row for one key")
        return _row(rows[0] if rows else None)

    async def _fetch(self, entity_id: str) -> list[tuple[Any, ...]]:
        async with self._connection.cursor() as cursor:
            await cursor.execute(self._query, (entity_id,))
            return await cursor.fetchall()

    async def aclose(self) -> None:
        await self._connection.close()


async def _connect(material: ConnectionMaterial, application_name: str) -> Any:
    return await psycopg.AsyncConnection.connect(
        host=material.host,
        port=material.port,
        dbname=material.database,
        user=material.user,
        password=material.password,
        sslmode="require",
        application_name=application_name,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        autocommit=True,
    )


class DeltaSource:
    """Round 4's shared Delta source, through the v1 adapter's Statement Execution."""

    def __init__(self, adapter: LiveModelScoreAdapter) -> None:
        self._adapter = adapter
        self.repaired = False

    async def check_storage(self, entity_id: str) -> ModelScoreRow | None:
        row = await self._adapter.preflight_source(entity_id)
        # A missing Delta path was rebuilt by the sealed repair job: its versions start again,
        # so Lakebase's next start must rebase rather than resume.
        self.repaired = self.repaired or self._adapter.source_repaired
        return row

    async def read(self, entity_id: str) -> ModelScoreRow | None:
        return await self._adapter.read_source(entity_id)

    async def probe_storage(self) -> None:
        """One Delta history read, repairing and starting nothing: the catalog's readiness probe."""

        await self._adapter.probe_source_storage()

    @property
    def repair_job_id(self) -> str:
        return self._adapter.config.source_repair_job_id

    async def head_version(self) -> int:
        version, _ = await self._adapter._source_head()
        return version

    async def table_id(self) -> str:
        rows = await self._adapter._statements.execute(
            "DESCRIBE DETAIL "
            + ".".join(
                f"`{part.replace('`', '``')}`"
                for part in self._adapter.config.source_table_full_name.split(".")
            )
        )
        if len(rows) != 1 or not rows[0].get("id"):
            raise Round4RaceError("DESCRIBE DETAIL did not return Round 4's Delta table ID")
        return str(rows[0]["id"])

    async def commit(
        self,
        update: ModelScoreUpdate,
        *,
        after_version: int,
        on_acknowledged: Callable[[], None] | None = None,
    ) -> DeltaCommit:
        """The bout's MERGE, retried only on Delta's concurrent-transaction abort.

        ``on_acknowledged`` fires the moment the MERGE returns, before the change-feed lookup
        that names its version, so the resolution's commit acknowledgment is the commit's own.
        """

        before = after_version
        for attempt in range(1, DELTA_CONFLICT_ATTEMPTS + 1):
            try:
                await self._adapter._merge_source_update(update)
                break
            except WorkspaceStatementExecutionError as exc:
                if (
                    exc.sql_state != DELTA_CONCURRENT_TRANSACTION_SQLSTATE
                    or attempt >= DELTA_CONFLICT_ATTEMPTS
                ):
                    raise
                await asyncio.sleep(DELTA_CONFLICT_RETRY_SECONDS * attempt)
                before, _ = await self._adapter._source_head()
        if on_acknowledged is not None:
            on_acknowledged()
        return await self._adapter._committed_change(update, before)


class LakebaseSyncLane:
    """Lakebase's lane: the sealed synced table, its pipeline parked between bouts."""

    lane_id = LAKEBASE_LANE
    label = "Lakebase"

    def __init__(
        self,
        manifest: DemoManifest,
        adapter: LiveModelScoreAdapter,
        source: DeltaSource,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._manifest = manifest
        self._adapter = adapter
        self._source = source
        self._api = pipeline_power.workspace_api(adapter.workspace)
        self._pipeline_id = adapter.config.pipeline_id
        self._sleep = sleep
        self._now = now
        self._started_update = ""

    async def _signals(self) -> PipelineSignals:
        return await read_pipeline_signals(self._manifest, self._api, pipeline_id=self._pipeline_id)

    @staticmethod
    def _parked(signals: PipelineSignals) -> bool:
        return (
            signals.pipeline_state.strip().upper() == "IDLE"
            and signals.update_state.strip().upper() not in _ACTIVE_UPDATE_STATES
        )

    def _persist(self, record: Mapping[str, Any]) -> None:
        task = asyncio.create_task(pipeline_power.record_power_request(dict(record)))
        _PENDING.add(task)
        task.add_done_callback(_PENDING.discard)

    async def confirm_parked(self, notify: Notify) -> None:
        await self._park(notify, stop_if_running=True)

    async def park(self, notify: Notify) -> None:
        await self._park(notify, stop_if_running=True)

    async def _park(self, notify: Notify, *, stop_if_running: bool) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + PARK_TIMEOUT_SECONDS
        requested = False
        while True:
            signals = await self._signals()
            if self._parked(signals):
                return
            update = signals.update_state.strip().upper()
            if stop_if_running and not requested and update != "STOPPING":
                await notify("Parking Lakebase's synced-table pipeline")
                records: list[dict[str, Any]] = []
                try:
                    await asyncio.to_thread(
                        pipeline_power.stop, self._manifest, self._api, on_record=records.append
                    )
                finally:
                    for record in records:
                        self._persist(record)
                requested = True
            if loop.time() >= deadline:
                raise Round4RaceError(
                    f"Lakebase's pipeline did not park within {PARK_TIMEOUT_SECONDS:.0f}s "
                    f"({signals.describe()})"
                )
            await self._sleep(PARK_POLL_SECONDS)

    async def open_reader(self) -> PostgresReader:
        config = self._adapter.config
        return PostgresReader(
            await self._connect_verifier(),
            sql.Identifier(config.physical_schema, config.physical_table),
            reconnect=self._connect_verifier,
        )

    async def _connect_verifier(self) -> Any:
        """A new verifier connection, on a credential minted for it."""

        config = self._adapter.config
        workspace = self._adapter.workspace
        endpoint, credential, current_user = await asyncio.gather(
            asyncio.to_thread(workspace.postgres.get_endpoint, config.endpoint_name),
            asyncio.to_thread(
                workspace.postgres.generate_database_credential, config.endpoint_name
            ),
            asyncio.to_thread(workspace.current_user.me),
        )
        payload = endpoint.as_dict() if hasattr(endpoint, "as_dict") else dict(endpoint)
        host = str(((payload.get("status") or {}).get("hosts") or {}).get("host") or "")
        token = str(getattr(credential, "token", None) or "")
        user = config.database_user or str(getattr(current_user, "user_name", None) or "")
        if not host or not token or not user:
            raise ModelScoreLiveConfigurationError(
                "Lakebase's host, credential, or database user is missing"
            )
        return await psycopg.AsyncConnection.connect(
            host=host,
            port=config.port,
            dbname=config.physical_database,
            user=user,
            password=token,
            sslmode="require",
            application_name="lakebase-anti-demo-round4-verifier",
            connect_timeout=max(1, math.ceil(config.connect_timeout_seconds)),
            autocommit=True,
        )

    async def start(self, bell: Bell) -> None:
        records: list[dict[str, Any]] = []
        try:
            started = await asyncio.to_thread(
                pipeline_power.start,
                self._manifest,
                self._api,
                full_refresh=self._source.repaired,
                on_record=records.append,
            )
        finally:
            for record in records:
                self._persist(record)
        self._source.repaired = False
        self._started_update = started.update_id
        # The stop this bout owes, on record before anything can lose it.
        try:
            self._persist(
                pipeline_power.owed_stop_record(
                    self._manifest,
                    due_at=self._now() + timedelta(seconds=BOUT_STOP_DUE_SECONDS),
                    resumed_at=started.resuming_since,
                )
            )
        except Exception:  # noqa: BLE001 - bookkeeping never fails a bell
            LOGGER.warning("Round 4 could not record the stop its bell owes", exc_info=True)

    async def ensure_running(self, bell: Bell) -> bool:
        signals = await self._signals()
        if not self._parked(signals):
            return False
        await self.start(bell)
        return True

    async def failure(self) -> str | None:
        signals = await self._signals()
        if not self._started_update or signals.update_id != self._started_update:
            return None
        if signals.update_state.strip().upper() in PIPELINE_UPDATE_FAILED_STATES:
            return f"Lakebase's pipeline update failed ({signals.describe()})"
        return None

    async def evidence(self, commit: DeltaCommit, bell: Bell) -> Mapping[str, Any]:
        """Lakebase's own figure, the sync's timestamps, read after the race and never compared."""

        status = await self._adapter.inspect_sync()
        evidence: dict[str, Any] = {
            "pipeline_update": self._started_update,
            "last_synced_delta_version": status.last_sync_delta_version,
        }
        if status.last_sync_delta_version == commit.version and status.sync_end_time is not None:
            evidence["managed_availability_ms"] = round(
                (status.sync_end_time - commit.committed_at).total_seconds() * 1000, 1
            )
        return evidence


class GlueWriterLane:
    """AWS's lane: the sealed Glue job for this matchup's competitor."""

    lane_id = COMPETITOR_LANE

    def __init__(
        self,
        manifest: DemoManifest,
        competitor: CompetitorId,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        sealed = manifest.round4_aws
        if sealed is None:
            raise ModelScoreLiveConfigurationError("Round 4's AWS lane is not sealed")
        self._competitor = "aurora" if competitor == CompetitorId.AURORA_SERVERLESS_V2 else "rds"
        self.label = (
            "AWS Glue → Aurora Serverless v2"
            if self._competitor == "aurora"
            else "AWS Glue → RDS PostgreSQL"
        )
        self._sealed = sealed
        self._lane = sealed.lane(self._competitor)
        self._region = manifest.aws.region
        environment = manifest.round_environment(4)
        # Built on first use, off the loop: the session assumes the runtime role, which is a
        # network call, and building an engine makes none.
        self._built: GlueWriterJob | None = None
        self._build_lock = threading.Lock()
        self._provider: AuroraCredentialProvider | RdsCredentialProvider
        if self._competitor == "aurora":
            assert environment.aurora is not None
            provider = AuroraCredentialProvider()
            provider.cluster_id = environment.aurora.cluster_id
            provider.secret_arn = environment.aurora.secret_arn
            provider.database = manifest.databricks.database
            self._provider = provider
        else:
            assert environment.rds is not None
            rds = RdsCredentialProvider(round_id=_round4())
            rds.instance_id = environment.rds.instance_id
            rds.secret_arn = environment.rds.secret_arn
            rds.database = manifest.databricks.database
            self._provider = rds
        self._sleep = sleep
        self._run_id = ""
        self._run_tag = ""

    @property
    def _job(self) -> GlueWriterJob:
        """The sealed job, built on first use. Only ever read inside ``asyncio.to_thread``."""

        with self._build_lock:
            if self._built is None:
                auth = runtime_auth_from_environment(os.environ)
                session = _runtime_aws_session(auth.mode, auth.profile, self._region)
                self._built = GlueWriterJob(
                    session, self._lane.job_name, bucket=self._sealed.bucket
                )
            return self._built

    def _threaded_notify(self, notify: Notify) -> Callable[[str], None]:
        loop = asyncio.get_running_loop()

        def say(message: str) -> None:
            asyncio.run_coroutine_threadsafe(notify(message), loop)

        return say

    async def confirm_parked(self, notify: Notify) -> None:
        say = self._threaded_notify(notify)
        await asyncio.to_thread(lambda: self._job.park(say))

    async def park(self, notify: Notify) -> None:
        say = self._threaded_notify(notify)
        await asyncio.to_thread(lambda: self._job.park(say))

    async def open_reader(self) -> PostgresReader:
        return PostgresReader(
            await self._connect_verifier(),
            sql.Identifier(self._sealed.target_schema, self._sealed.target_view),
            reconnect=self._connect_verifier,
        )

    async def _connect_verifier(self) -> Any:
        """A new verifier connection, on credentials fetched for it."""

        material = await self._provider.connection_material()
        return await _connect(material, "lakebase-anti-demo-round4-verifier")

    async def start(self, bell: Bell) -> None:
        self._run_tag = bell.bout_id
        arguments = {
            "--run_tag": bell.bout_id,
            "--source_table_id": bell.source_table_id,
            # At the bell, the change feed from just after the version Prepare verified, which
            # is what Lakebase's pipeline reads too. Settling starts from the snapshot.
            "--starting_version": starting_version(bell.source_version),
        }
        run_id, _ = await asyncio.to_thread(lambda: self._job.start(arguments))
        self._run_id = run_id

    async def ensure_running(self, bell: Bell) -> bool:
        state = await asyncio.to_thread(lambda: self._job.park_state())
        if any(run.state in {"STARTING", "RUNNING", "WAITING"} for run in state.active):
            return False
        if state.active:
            # Stopping: let it finish before a new run can take the slot.
            await asyncio.to_thread(lambda: self._job.park())
        await self.start(bell)
        return True

    async def failure(self) -> str | None:
        run_id = self._run_id
        if not run_id:
            return None
        run = await asyncio.to_thread(lambda: self._job.run(run_id))
        if run.state in TERMINAL_STATES:
            return f"The Glue run ended {run.state}: {run.error_message or 'no error message'}"
        return None

    async def evidence(self, commit: DeltaCommit, bell: Bell) -> Mapping[str, Any]:
        """The ledger's record of the bout's row, and when the run says it started and wrote."""

        evidence: dict[str, Any] = {"glue_run": self._run_id, "competitor": self._competitor}
        material = await self._provider.connection_material()
        connection = await _connect(material, "lakebase-anti-demo-round4-evidence")
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(
                        "SELECT delta_table_id, delta_commit_version, deleted, applied_at "
                        "FROM {} WHERE entity_id = %s"
                    ).format(sql.Identifier(self._sealed.target_schema, self._sealed.target_table)),
                    (commit_entity(commit),),
                )
                row = await cursor.fetchone()
        if row is not None:
            table_id, version, deleted, applied_at = row
            evidence.update(
                {
                    "ledger_delta_table_id": str(table_id),
                    "ledger_delta_commit_version": int(version),
                    "ledger_deleted": bool(deleted),
                    "ledger_applied_at": applied_at.isoformat() if applied_at else None,
                    "ledger_version_covers_commit": int(version) >= commit.version,
                }
            )
        run_id, run_tag, competitor = self._run_id, self._run_tag, self._competitor
        run = await asyncio.to_thread(lambda: self._job.run(run_id)) if run_id else None
        if run is not None and run.started_on is not None:
            evidence["glue_run_started_on"] = run.started_on.isoformat()
        marker = await asyncio.to_thread(lambda: self._job.marker(competitor, run_tag))
        if marker:
            for key in (
                "starting_version",
                "stream_started_at",
                "first_batch_applied_at",
                "first_batch_max_version",
            ):
                if key in marker:
                    evidence[f"glue_{key}"] = marker[key]
        return evidence


def _round4() -> Any:
    from .models import RoundId

    return RoundId.PUT_MODEL_SCORE_IN_APP


def commit_entity(commit: DeltaCommit) -> str:
    """The contracted key; the commit itself does not carry it."""

    from .lifecycle import ROUND4_BASELINE_ENTITY_ID

    return ROUND4_BASELINE_ENTITY_ID


#: Strong references to in-flight durable power writes; see ``model_score_live._PENDING_RECORDS``.
_PENDING: set[asyncio.Task[Any]] = set()


def build_round4_race_engine(
    manifest: DemoManifest,
    competitor: CompetitorId,
) -> Round4RaceEngine:
    """The live engine for one matchup, without any network operation beyond client setup.

    Lakebase races alone, with the competitor's lane marked not supported, on an installation
    whose AWS lane is not sealed.
    """

    v1 = build_model_score_engine(manifest)
    adapter = v1.adapter
    assert isinstance(adapter, LiveModelScoreAdapter)
    source = DeltaSource(adapter)
    lanes: list[Any] = [LakebaseSyncLane(manifest, adapter, source)]
    if manifest.round4_aws is not None:
        lanes.append(GlueWriterLane(manifest, competitor))
    return Round4RaceEngine(source, lanes, contract=v1.contract)
