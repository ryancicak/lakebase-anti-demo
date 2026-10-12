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
import uuid
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
    _PENDING_RECORDS,
    DELTA_CONCURRENT_TRANSACTION_SQLSTATE,
    DELTA_CONFLICT_ATTEMPTS,
    DELTA_CONFLICT_RETRY_SECONDS,
    PIPELINE_UPDATE_CANCELLED_STATES,
    PIPELINE_UPDATE_FAILED_STATES,
    LiveModelScoreAdapter,
    ModelScoreLiveConfigurationError,
    PipelineSignals,
    WorkspaceStatementExecutionError,
    build_model_score_engine,
    pipeline_signals_are_healthy,
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
#: How long a park waits for a full-refresh update to read healthy twice before cancelling it.
#: Settling parks once the restored row has been carried, so a healthy update needs only its two
#: readings, PARK_POLL_SECONDS apart; one that does not get there leaves the rebase owed.
REBASE_CONFIRM_SECONDS = 30.0
#: How long a park waits on the coordination database for the pair key its stop is recorded under.
#: The stop goes out either way: a pipeline left billing costs more than a stop left unrecorded.
PARK_LEDGER_SECONDS = 30.0
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
            LOGGER.info(
                "Round 4 verifier connection was lost (%s); opening a new one",
                type(lost).__name__,
            )
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
        if self.repaired:
            pipeline_id = str(
                getattr(getattr(self._adapter, "config", None), "pipeline_id", "") or ""
            )
            if pipeline_id:
                await pipeline_power.note_rebase_owed(pipeline_id)
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
        self._started_full_refresh = False
        self._rebase_event_id: int | None = None
        self._rebase_healthy_streak = 0
        self._power_pair_key = ""
        # The rebase decision and pair key ``before_bell`` read for the next start, used once.
        self._reserved: tuple[pipeline_power.RebaseSnapshot, str] | None = None

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
        if self._started_full_refresh:
            # Confirming writes the rebase ledger, and a write that never answers must not hold
            # back the stop below: past the bound the park goes ahead and the rebase stays owed.
            try:
                await _within(self._confirm_rebase(notify), REBASE_CONFIRM_SECONDS)
            except TimeoutError:
                LOGGER.warning(
                    "Round 4 could not confirm Lakebase's full refresh within %.0fs, so the next "
                    "bell rebases again",
                    REBASE_CONFIRM_SECONDS,
                )
            finally:
                self._started_full_refresh = False
                self._rebase_healthy_streak = 0
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
                pair_key = str(getattr(self, "_power_pair_key", "") or "")
                mint_error: BaseException | None = None
                if not pair_key:
                    try:
                        pair_key = await _within(
                            pipeline_power.mint_power_pair_key(
                                str(self._manifest.run_id),
                                str(getattr(self, "_pipeline_id", "") or ""),
                            ),
                            PARK_LEDGER_SECONDS,
                        )
                        self._power_pair_key = pair_key
                    except Exception as exc:
                        mint_error = exc
                # A refused stop outranks a failed pair key: the pipeline may still be billing.
                await asyncio.to_thread(
                    pipeline_power.stop, self._manifest, self._api, on_record=records.append
                )
                if mint_error is not None:
                    raise mint_error
                for record in records:
                    record["run_id"] = pair_key
                    self._persist(record)
                requested = True
            if loop.time() >= deadline:
                raise Round4RaceError(
                    f"Lakebase's pipeline did not park within {PARK_TIMEOUT_SECONDS:.0f}s "
                    f"({signals.describe()})"
                )
            await self._sleep(PARK_POLL_SECONDS)

    async def _confirm_rebase(self, notify: Notify) -> None:
        """Settle the rebase a full-refresh start owes, before its park cancels the update.

        The race stops watching a lane once its row verifies, which can be before the update has
        read healthy twice, so nothing cleared the obligation and every later bell full-refreshed
        again. Two consecutive healthy readings of the started update clear it here; a FAILED or
        CANCELED one owes it again; anything else leaves it owed. The park goes ahead regardless.
        """

        await notify("Confirming Lakebase's full refresh before parking its pipeline")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + REBASE_CONFIRM_SECONDS
        self._rebase_healthy_streak = 0
        try:
            while True:
                signals = await self._signals()
                ended = await self._observe_start(signals)
                if ended is not None:
                    LOGGER.warning("Round 4 still owes Lakebase's pipeline a rebase: %s", ended)
                    return
                if not self._started_full_refresh:
                    return
                if self._parked(signals) or loop.time() >= deadline:
                    LOGGER.warning(
                        "Lakebase's full-refresh update did not read healthy twice before its "
                        "park (%s), so the next bell rebases again",
                        signals.describe(),
                    )
                    return
                await self._sleep(PARK_POLL_SECONDS)
        except Exception:  # noqa: BLE001 - the park goes ahead whatever this could not read
            LOGGER.warning(
                "Round 4 could not confirm Lakebase's full refresh before parking it, so the "
                "next bell rebases again",
                exc_info=True,
            )

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

    async def before_bell(self) -> None:
        """Read whether a rebase is owed and reserve the start's pair key, off every clock."""

        pipeline_id = str(getattr(self, "_pipeline_id", "") or "")
        snapshot = (
            await pipeline_power.rebase_snapshot(pipeline_id)
            if pipeline_id
            else pipeline_power.RebaseSnapshot(owed=False, event_id=None)
        )
        pair_key = await pipeline_power.mint_power_pair_key(
            str(self._manifest.run_id),
            pipeline_id,
            bout_token=uuid.uuid4().hex,
        )
        self._reserved = (snapshot, pair_key)

    async def start(self, bell: Bell) -> None:
        reserved, self._reserved = self._reserved, None
        if reserved is None:
            raise Round4RaceError("Lakebase's start was not recorded before it was asked for")
        snapshot, pair_key = reserved
        owed = bool(self._source.repaired) or snapshot.owed
        observed_event_id = snapshot.event_id if snapshot.owed else None
        self._power_pair_key = pair_key
        records: list[dict[str, Any]] = []
        try:
            started = await asyncio.to_thread(
                pipeline_power.start,
                self._manifest,
                self._api,
                full_refresh=owed,
                rebase_event_id=observed_event_id,
                on_record=records.append,
            )
        finally:
            for record in records:
                record["run_id"] = pair_key
                self._persist(record)
        self._source.repaired = False
        self._started_update = started.update_id
        self._started_full_refresh = owed
        self._rebase_event_id = observed_event_id
        self._rebase_healthy_streak = 0
        # The stop this bout owes, on record before anything can lose it.
        try:
            owed_record = pipeline_power.owed_stop_record(
                self._manifest,
                due_at=self._now() + timedelta(seconds=BOUT_STOP_DUE_SECONDS),
                resumed_at=started.resuming_since,
            )
            owed_record["run_id"] = pair_key
            self._persist(owed_record)
        except Exception:  # noqa: BLE001 - bookkeeping never fails a bell
            LOGGER.warning("Round 4 could not record the stop its bell owes", exc_info=True)

    async def ensure_running(self, bell: Bell) -> bool:
        signals = await self._signals()
        if not self._parked(signals):
            return False
        await self.before_bell()
        await self.start(bell)
        return True

    async def failure(self) -> str | None:
        return await self._observe_start(await self._signals())

    async def _observe_start(self, signals: PipelineSignals) -> str | None:
        """The started update's own failure, settling a full refresh's rebase as it goes.

        Two consecutive healthy readings of that update clear the obligation, compare-and-set on
        the owed event its start observed; a FAILED or CANCELED one owes it again.
        """

        if not self._started_update:
            return None
        if signals.update_id != self._started_update:
            if getattr(self, "_started_full_refresh", False):
                self._rebase_healthy_streak = 0
            return None
        state = signals.update_state.strip().upper()
        pipeline_id = str(getattr(self, "_pipeline_id", "") or "")
        if state in PIPELINE_UPDATE_FAILED_STATES or state in PIPELINE_UPDATE_CANCELLED_STATES:
            self._rebase_healthy_streak = 0
            ended = (
                "was canceled"
                if state in PIPELINE_UPDATE_CANCELLED_STATES
                else "failed"
            )
            detail = f"Lakebase's pipeline update {ended} ({signals.describe()})"
            if getattr(self, "_started_full_refresh", False) and pipeline_id:
                try:
                    await pipeline_power.note_rebase_owed(
                        pipeline_id, actor="full-refresh-failed"
                    )
                except Exception as exc:
                    return (
                        f"{detail}; Round 4 could not re-record the rebase it still "
                        f"owes ({type(exc).__name__}: {exc})"
                    )
            return detail
        if getattr(self, "_started_full_refresh", False) and pipeline_signals_are_healthy(
            signals
        ):
            self._rebase_healthy_streak = getattr(self, "_rebase_healthy_streak", 0) + 1
            if self._rebase_healthy_streak >= 2 and pipeline_id:
                await pipeline_power.note_rebase_satisfied(
                    pipeline_id,
                    observed_event_id=getattr(self, "_rebase_event_id", None),
                )
                self._started_full_refresh = False
                self._rebase_healthy_streak = 0
            return None
        if getattr(self, "_started_full_refresh", False):
            self._rebase_healthy_streak = 0
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

    async def before_bell(self) -> None:
        """Nothing: a Glue run is tagged with the bell's own bout ID."""

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
            # This is the lane's failure on the card and in the receipt, and Glue's own
            # message can name the role, bucket or connection, so it is logged instead.
            LOGGER.warning(
                "Round 4's Glue run %s ended %s: %s",
                run_id,
                run.state,
                run.error_message or "no error message",
            )
            return f"The Glue run ended {run.state}"
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


#: Strong references to in-flight durable power writes. The same set as
#: ``model_score_live._PENDING_RECORDS``, so the shutdown drain that waits for v1's waits for
#: these too.
_PENDING: set[asyncio.Task[Any]] = _PENDING_RECORDS


async def _within[T](operation: Awaitable[T], seconds: float) -> T:
    """``operation``'s answer, or :class:`TimeoutError` once ``seconds`` have passed.

    A coordination call can wait on a statement its server never answers, and its cancel is not
    prompt either: psycopg waits on the same socket after sending one. So a call that runs out of
    time is cancelled and left to finish on its own, never waited for.
    """

    task = asyncio.ensure_future(operation)
    try:
        done, _ = await asyncio.wait({task}, timeout=seconds)
    finally:
        if not task.done():
            task.cancel()
            _PENDING.add(task)
            task.add_done_callback(_PENDING.discard)
            task.add_done_callback(lambda left: None if left.cancelled() else left.exception())
    if not done:
        raise TimeoutError(f"the coordination database did not answer within {seconds:.0f}s")
    return task.result()


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
