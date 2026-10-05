"""Round 6's live lanes: Lakebase's built-in change feed and AWS's DMS and Glue pipeline.

The protocol is ``server/round6_race.py``; this is how each lane does what it asks, against the
sealed resources and nothing else.

- **Lakebase's lane** is v1's: the sealed source table on Lakebase, its built-in change feed, and
  the Delta history table the feed writes, all through v1's own adapter
  (``server/live_orders.py``). The feed is always on, so starting and parking it are nothing;
  the card and the receipt say so (docs/design/v1.1-rounds-4-6-aws.md, section 2).
- **AWS's lane** is the sealed source table on the matchup's r6 database, reached with the round's
  own credential, and the sealed DMS task and Glue job for that competitor, started at the bell
  and parked after the bout through ``server/round6_dms.py`` and ``server/round4_glue.py``.
- **The verifier** is one question for both lanes, on the same SQL warehouse: is the bout's order
  in the lane's history as exactly one insert. Lakebase's feed names an insert ``insert`` in
  ``_pg_change_type``; DMS writes ``I`` in ``Op``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import psycopg
from psycopg import sql

from .aws_auth import runtime_auth_from_environment
from .live_orders import (
    LiveOrder,
    LiveOrdersLiveAdapter,
    build_live_orders_engine,
)
from .manifest import DemoManifest
from .model_score_live import SqlParameter, WorkspaceStatementRunner
from .models import CompetitorId
from .round4_glue import TERMINAL_STATES, GlueWriterJob
from .round6_dms import TRANSITIONAL, DmsCaptureTask, DmsTaskState
from .round6_race import (
    BOUT_NONCE_PREFIX,
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    Notify,
    Round6NotArmedError,
    Round6RaceEngine,
    Round6RaceError,
)
from .targets import (
    AuroraCredentialProvider,
    ConnectionMaterial,
    RdsCredentialProvider,
    _runtime_aws_session,
)

LOGGER = logging.getLogger(__name__)

#: A connect wide enough for any r6 database; Prepare pays it, never a clock.
CONNECT_TIMEOUT_SECONDS = 45
#: How long after the bell a DMS task may still report the status it had before it.
DMS_STALE_STATUS_SECONDS = 30.0
#: How long a just-stopped task may still hold its replication slot.
SLOT_RELEASE_SECONDS = 30.0


def _slot_prefix(task_arn: str) -> str:
    """How DMS names the slot a task makes: its resource ID's first 16 characters, lowercased,
    then a suffix of its own (read off the v1.1 test installation, 2026-09-29)."""

    return task_arn.rsplit(":", 1)[-1][:16].lower()


_COLUMNS = "order_id, sku, store, quantity, total_cents, status, proof_nonce"


def _order(values: tuple[Any, ...]) -> LiveOrder:
    order_id, sku, store, quantity, total_cents, status, nonce = values
    return LiveOrder(
        order_id=str(order_id),
        sku=str(sku),
        store=str(store),
        quantity=int(quantity),
        total_cents=int(total_cents),
        status=str(status),
        proof_nonce=str(nonce),
    )


def _uc(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 3 or any(not part or "`" in part for part in parts):
        raise Round6RaceError(f"not a three-part Unity Catalog name: {name!r}")
    return ".".join(f"`{part}`" for part in parts)


class LakebaseFeedLane:
    """Lakebase's lane: the sealed source, its built-in change feed and its history table."""

    lane_id = LAKEBASE_LANE
    label = "Lakebase built-in change feed"

    def __init__(self, adapter: LiveOrdersLiveAdapter, baseline: LiveOrder) -> None:
        self._adapter = adapter
        self._baseline = baseline

    async def confirm_parked(self, notify: Notify) -> None:
        """The feed is built in and always on, so "at rest" means streaming the sealed table,
        with the baseline in its history, as v1's Prepare checked."""

        feed = await self._adapter.inspect_feed()
        if feed.state != "CDF_STATE_STREAMING" or not feed.committed_lsn:
            raise Round6NotArmedError("Lakebase's change feed is not streaming the sealed table")
        history = await self._adapter.read_history(self._baseline)
        if history is None or history.order != self._baseline or history.change_type != "insert":
            raise Round6NotArmedError("Lakebase's history does not hold the exact baseline")

    async def probe_storage(self) -> None:
        """Read the baseline's history and nothing else: catalog readiness's check that the
        sealed Delta path is still there. Raises what the adapter raises when it is not."""

        await self._adapter.read_history(self._baseline)

    async def read_source(self, order_id: str) -> LiveOrder | None:
        return await self._adapter.read_checkout(order_id)

    async def commit(self, order: LiveOrder) -> None:
        await self._adapter.insert_checkout(order)

    async def read_history(self, order: LiveOrder) -> bool:
        history = await self._adapter.read_history(order)
        return (
            history is not None
            and history.order == order
            and history.change_type == "insert"
            and history.lsn >= 0
        )

    async def start(self) -> None:
        return None

    async def failure(self) -> str | None:
        feed = await self._adapter.inspect_feed()
        if feed.state != "CDF_STATE_STREAMING":
            return f"Lakebase's change feed left streaming: {feed.state}"
        return None

    async def park(self, notify: Notify) -> None:
        return None

    async def delete(self, order: LiveOrder) -> None:
        await self._adapter.delete_checkout_exact(order)

    async def residue(self) -> list[LiveOrder]:
        config = self._adapter.config
        connection = await self._adapter._connect()
        table = sql.Identifier(config.source_schema, config.source_table)
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(f"SELECT {_COLUMNS} FROM {{}} WHERE proof_nonce LIKE %s").format(table),
                    (f"{BOUT_NONCE_PREFIX}%",),
                )
                rows = await cursor.fetchall()
        return [_order(tuple(row)) for row in rows]

    async def evidence(self, order: LiveOrder) -> Mapping[str, Any]:
        history = await self._adapter.read_history(order)
        return {"history_lsn": history.lsn} if history is not None else {}


class AwsCaptureLane:
    """AWS's lane: the r6 source, and the sealed DMS task and Glue job for one competitor."""

    lane_id = COMPETITOR_LANE

    def __init__(
        self,
        manifest: DemoManifest,
        competitor: CompetitorId,
        statements: WorkspaceStatementRunner,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        sealed = manifest.round6_aws
        if sealed is None:
            raise Round6NotArmedError("Round 6's AWS lane is not sealed")
        self._competitor = "aurora" if competitor == CompetitorId.AURORA_SERVERLESS_V2 else "rds"
        self.label = (
            "AWS DMS + Glue from Aurora Serverless v2"
            if self._competitor == "aurora"
            else "AWS DMS + Glue from RDS PostgreSQL"
        )
        self._sealed = sealed
        self._lane = sealed.lane(self._competitor)
        self._region = manifest.aws.region
        self._statements = statements
        self._clock = clock
        environment = manifest.round_environment(6)
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
            rds = RdsCredentialProvider(round_id=_round6())
            rds.instance_id = environment.rds.instance_id
            rds.secret_arn = environment.rds.secret_arn
            rds.database = manifest.databricks.database
            self._provider = rds
        # Built on first use, off the loop: the session assumes the runtime role, which is a
        # network call, and building an engine makes none.
        self._built: tuple[DmsCaptureTask, GlueWriterJob] | None = None
        self._build_lock = threading.Lock()
        self._run_id = ""
        self._started_at: float | None = None
        self._dms_seen_moving = False

    @property
    def _pipeline(self) -> tuple[DmsCaptureTask, GlueWriterJob]:
        """The sealed task and job, built on first use. Only ever read inside a thread."""

        with self._build_lock:
            if self._built is None:
                auth = runtime_auth_from_environment(os.environ)
                session = _runtime_aws_session(auth.mode, auth.profile, self._region)
                self._built = (
                    DmsCaptureTask(session, self._lane.task_arn),
                    GlueWriterJob(session, self._lane.job_name, bucket=self._sealed.bucket),
                )
            return self._built

    def _threaded_notify(self, notify: Notify) -> Callable[[str], None]:
        loop = asyncio.get_running_loop()

        def say(message: str) -> None:
            asyncio.run_coroutine_threadsafe(notify(message), loop)

        return say

    async def _connect(self, application_name: str) -> Any:
        material: ConnectionMaterial = await self._provider.connection_material()
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

    def _table(self) -> sql.Composable:
        return sql.Identifier(self._sealed.source_schema, self._sealed.source_table)

    async def confirm_parked(self, notify: Notify) -> None:
        """Park the task and the job (waiting out a park in progress), and refuse a task that
        has never run, because it has no slot to resume from."""

        say = self._threaded_notify(notify)
        task, job = await asyncio.to_thread(lambda: self._pipeline)
        state, _ = await asyncio.gather(
            asyncio.to_thread(lambda: task.park(say)),
            asyncio.to_thread(lambda: job.park(say)),
        )
        if state.status == "ready":
            raise Round6NotArmedError(
                "Round 6's DMS task has never run, so it has no replication slot to resume from; "
                "run 'antidemo setup' again"
            )
        await self._rest_the_slot(notify)
        # The external table answers at all: the same warehouse, the same question shape.
        await self._history_rows(self._probe_statement())

    async def _rest_the_slot(self, notify: Notify) -> None:
        """Bring the parked task's replication slot up to now, or re-create one the source dropped.

        A stopped task's slot holds back everything its source has written since the task last
        ran, and an idle source keeps writing WAL. On the v1.1 test installation an idle r6 RDS
        instance added one 64 MB segment every 5 minutes (its `archive_timeout`), 768 MB an hour,
        while Aurora added a fraction of a megabyte (2026-09-29). Left alone, the bell's resume
        would first decode all of it, so AWS's clock would grow with how long the installation
        had sat idle. Past `max_slot_wal_keep_size` (`infra/aws/parameter_groups.tf`), about
        2.5 hours idle on RDS, PostgreSQL gives the slot up and the resume fails. Nothing written
        between bouts needs capturing: a bout's orders are deleted after it, and a per-bout
        nonce keeps any old row from matching. So Prepare, with
        the task parked, moves the slot up to the current WAL position, or re-creates a lost one
        under its own name and plugin, which a resume then reads from, before the bell and off
        either clock. Measured live: the step takes seconds, and DMS resumed and delivered the
        bout's order both from an advanced slot and from a re-created one, on Aurora and RDS.
        An advance moves where the resume starts at once; the WAL the slot holds falls only to
        its last restart point, a couple of segments back on an idle RDS.
        """

        prefix = _slot_prefix(self._lane.task_arn)
        connection = await self._connect("lakebase-anti-demo-round6-prepare")
        async with connection:
            async with connection.cursor() as cursor:
                deadline = self._clock() + SLOT_RELEASE_SECONDS
                while True:
                    await cursor.execute(
                        "SELECT slot_name, plugin, wal_status, active "
                        "FROM pg_catalog.pg_replication_slots "
                        "WHERE slot_type = 'logical' AND database = current_database() "
                        "AND slot_name LIKE %s",
                        (f"{prefix}%",),
                    )
                    rows = await cursor.fetchall()
                    if len(rows) != 1:
                        raise Round6NotArmedError(
                            f"Round 6's DMS task has {len(rows)} replication slots on its source, "
                            "not one; run 'antidemo setup' again"
                        )
                    name, plugin, status, active = rows[0]
                    if not active:
                        break
                    # A task that has just stopped can still hold its slot for a moment.
                    if self._clock() >= deadline:
                        raise Round6NotArmedError(
                            "Round 6's replication slot is still held after its DMS task stopped"
                        )
                    await asyncio.sleep(1.0)
                if status == "lost":
                    await notify(
                        "Re-creating the AWS lane's replication slot, which its idle source dropped"
                    )
                    await cursor.execute("SELECT pg_drop_replication_slot(%s)", (name,))
                    await cursor.execute(
                        "SELECT pg_create_logical_replication_slot(%s, %s)", (name, plugin)
                    )
                    return
                await cursor.execute(
                    "SELECT pg_replication_slot_advance(%s, pg_current_wal_lsn())", (name,)
                )

    def _probe_statement(self) -> str:
        return f"SELECT count(*) AS n FROM {_uc(self._lane.history_table_full_name)} LIMIT 1"

    async def read_source(self, order_id: str) -> LiveOrder | None:
        connection = await self._connect("lakebase-anti-demo-round6-checkout")
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(f"SELECT {_COLUMNS} FROM {{}} WHERE order_id = %s").format(
                        self._table()
                    ),
                    (order_id,),
                )
                rows = await cursor.fetchall()
        if len(rows) > 1:
            raise Round6RaceError("Checkout returned duplicate order rows")
        return _order(tuple(rows[0])) if rows else None

    async def commit(self, order: LiveOrder) -> None:
        connection = await self._connect("lakebase-anti-demo-round6-checkout")
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(
                        f"INSERT INTO {{}} ({_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    ).format(self._table()),
                    (
                        order.order_id,
                        order.sku,
                        order.store,
                        order.quantity,
                        order.total_cents,
                        order.status,
                        order.proof_nonce,
                    ),
                )

    async def read_history(self, order: LiveOrder) -> bool:
        rows = await self._history_rows(
            f"SELECT count(*) AS n FROM {_uc(self._lane.history_table_full_name)} "
            "WHERE order_id = :order_id AND proof_nonce = :proof_nonce AND Op = 'I'",
            order,
        )
        return bool(rows) and int(str(rows[0].get("n") or 0)) == 1

    async def _history_rows(self, statement: str, order: LiveOrder | None = None) -> list[Any]:
        parameters = (
            (
                SqlParameter("order_id", order.order_id, "STRING"),
                SqlParameter("proof_nonce", order.proof_nonce, "STRING"),
            )
            if order is not None
            else ()
        )
        return await self._statements.execute(statement, parameters)

    async def start(self) -> None:
        """Start DMS and Glue at once. Returns once both have accepted, not once they run."""

        self._started_at = self._clock()
        self._dms_seen_moving = False
        task, job = await asyncio.to_thread(lambda: self._pipeline)
        (kind, _), (run_id, _) = await asyncio.gather(
            asyncio.to_thread(task.start),
            asyncio.to_thread(lambda: job.start({})),
        )
        if kind == "already-running":
            # Prepare parks the task, so a running one at the bell means AWS started warm.
            LOGGER.warning(
                "Round 6's DMS task %s was already running at the bell", self._lane.task_arn
            )
        self._run_id = run_id

    async def failure(self) -> str | None:
        task, job = await asyncio.to_thread(lambda: self._pipeline)
        state: DmsTaskState = await asyncio.to_thread(task.state)
        if state.running or state.status in TRANSITIONAL:
            self._dms_seen_moving = True
        elif state.parked:
            # Just after the start, DMS can still report the status the task had before it.
            fresh = (
                self._started_at is not None
                and self._clock() - self._started_at < DMS_STALE_STATUS_SECONDS
            )
            if self._dms_seen_moving or not fresh:
                return f"The DMS task stopped: {state.failure or state.stop_reason or state.status}"
        run_id = self._run_id
        if run_id:
            run = await asyncio.to_thread(lambda: job.run(run_id))
            if run.state in TERMINAL_STATES:
                return f"The Glue run ended {run.state}: {run.error_message or 'no error message'}"
        return None

    async def park(self, notify: Notify) -> None:
        say = self._threaded_notify(notify)
        task, job = await asyncio.to_thread(lambda: self._pipeline)
        results = await asyncio.gather(
            asyncio.to_thread(lambda: job.park(say)),
            asyncio.to_thread(lambda: task.park(say)),
            return_exceptions=True,
        )
        failures = [str(result) for result in results if isinstance(result, BaseException)]
        if failures:
            raise Round6RaceError("; ".join(failures))

    async def delete(self, order: LiveOrder) -> None:
        connection = await self._connect("lakebase-anti-demo-round6-cleanup")
        predicate = (
            "order_id = %s AND proof_nonce = %s AND sku = %s AND store = %s "
            "AND quantity = %s AND total_cents = %s AND status = %s"
        )
        parameters = (
            order.order_id,
            order.proof_nonce,
            order.sku,
            order.store,
            order.quantity,
            order.total_cents,
            order.status,
        )
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(f"DELETE FROM {{}} WHERE {predicate} RETURNING order_id").format(
                        self._table()
                    ),
                    parameters,
                )
                if len(await cursor.fetchall()) > 1:
                    raise Round6RaceError("Exact checkout cleanup matched duplicate rows")

    async def residue(self) -> list[LiveOrder]:
        connection = await self._connect("lakebase-anti-demo-round6-cleanup")
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    sql.SQL(f"SELECT {_COLUMNS} FROM {{}} WHERE proof_nonce LIKE %s").format(
                        self._table()
                    ),
                    (f"{BOUT_NONCE_PREFIX}%",),
                )
                rows = await cursor.fetchall()
        return [_order(tuple(row)) for row in rows]

    async def evidence(self, order: LiveOrder) -> Mapping[str, Any]:
        """DMS's commit timestamp and Glue's apply time for the order, and the run that carried
        it. Read after the race, never on its clock."""

        evidence: dict[str, Any] = {"competitor": self._competitor, "glue_run": self._run_id}
        rows = await self._history_rows(
            "SELECT max(dms_commit_ts) AS dms_commit_ts, max(applied_at) AS applied_at "
            f"FROM {_uc(self._lane.history_table_full_name)} "
            "WHERE order_id = :order_id AND proof_nonce = :proof_nonce AND Op = 'I'",
            order,
        )
        if rows:
            evidence["dms_commit_ts"] = rows[0].get("dms_commit_ts")
            evidence["glue_applied_at"] = rows[0].get("applied_at")
        run_id = self._run_id
        if run_id:
            _, job = await asyncio.to_thread(lambda: self._pipeline)
            run = await asyncio.to_thread(lambda: job.run(run_id))
            if run.started_on is not None:
                evidence["glue_run_started_on"] = run.started_on.isoformat()
        return evidence


def _round6() -> Any:
    from .models import RoundId

    return RoundId.ANALYZE_LIVE_ORDERS


def build_round6_race_engine(manifest: DemoManifest, competitor: CompetitorId) -> Round6RaceEngine:
    """The live engine for one matchup, without any network operation beyond client setup.

    Lakebase races alone, with the competitor's lane marked not supported, on an installation
    whose AWS lane is not sealed.
    """

    v1 = build_live_orders_engine(manifest)
    adapter = v1.adapter
    assert isinstance(adapter, LiveOrdersLiveAdapter)
    baseline = v1.contract.baseline
    lanes: list[Any] = [LakebaseFeedLane(adapter, baseline)]
    if manifest.round6_aws is not None:
        # The same warehouse and client Lakebase's history is read on, so both lanes are read
        # by one reader.
        lanes.append(AwsCaptureLane(manifest, competitor, adapter._statements))
    return Round6RaceEngine(lanes, baseline=baseline)
