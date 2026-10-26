"""Round 6's Azure lane: the r6 source on Azure Database for PostgreSQL, and the installation's
standing ADF data flow that extracts new orders into Delta in ADLS, cold at the bell.

The Azure counterpart of `server/round6_race_live.AwsCaptureLane`, behind the `Lane` verbs of
`server/round6_race.py`. The verifier is the same warehouse question, on the Unity Catalog
external table over the flow's Delta history. What differs is what Phase 0 measured
(2026-10-18/19):

* **A watermark, not change capture.** ADF has no log-based capture for PostgreSQL, so the flow
  extracts the rows committed after a watermark: the source's ``committed_at``, which the database
  defaults, so the app's checkout writes nothing new.
  - Prepare reads the watermark from the history itself, its newest ``committed_at``. So a
    restarted process needs no memory of its own.
  - Prepare then writes the flow for that watermark, off the clock, and the bell is one
    ``createRun``.
  - Phase 0: after 25 more orders, a run for the old watermark appended exactly those 25, with no
    gap or duplicate.
* **Nothing to rest between bouts.** No replication slot holds the source's WAL back, unlike DMS.
* **Every extracted row is an insert,** so the history has no ``Op`` column to check.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from psycopg import sql

from .azure_adf_job import AdfDataFlowJob
from .live_orders import LiveOrder
from .model_score_live import SqlParameter
from .round6_race import BOUT_NONCE_PREFIX, COMPETITOR_LANE, Notify, Round6RaceError
from .round6_race_live import _COLUMNS, _order, _uc

LOGGER = logging.getLogger(__name__)

#: The watermark before any extract: everything the source holds.
EPOCH = "1970-01-01T00:00:00Z"
#: ADF's run states that end a run. Succeeded is one: a batch that ended has written all it will,
#: and the engine reads once more before it scores the lane, so a delivered order still verifies.
ENDED_STATES = frozenset({"Succeeded", "Failed", "Cancelled"})


class OrderStore(Protocol):
    """The lane's own r6 source: the app's checkout writes, and the cleanup after a bout."""

    async def read(self, order_id: str) -> LiveOrder | None: ...
    async def commit(self, order: LiveOrder) -> None: ...
    async def delete(self, order: LiveOrder) -> None: ...
    async def residue(self) -> list[LiveOrder]: ...


class History(Protocol):
    """The flow's Delta history, read through the SQL warehouse as both lanes' verifier is."""

    async def newest_committed_at(self) -> str | None: ...
    async def holds(self, order: LiveOrder) -> bool: ...
    async def copies(self, order: LiveOrder) -> int: ...
    async def committed_at(self, order: LiveOrder) -> str | None: ...


class AdfExtractLane:
    """Azure's lane: the r6 server, and the sealed ADF pipeline that extracts it into Delta."""

    lane_id = COMPETITOR_LANE
    label = "Azure Data Factory from Azure Database for PostgreSQL"

    def __init__(
        self,
        job: AdfDataFlowJob,
        orders: OrderStore,
        history: History,
        write_flow: Callable[[str], None],
    ) -> None:
        self._job = job
        self._orders = orders
        self._history = history
        self._write_flow = write_flow
        self._run_id = ""
        self.watermark: str | None = None

    def _threaded_notify(self, notify: Notify) -> Callable[[str], None]:
        loop = asyncio.get_running_loop()

        def say(message: str) -> None:
            asyncio.run_coroutine_threadsafe(notify(message), loop)

        return say

    async def confirm_parked(self, notify: Notify) -> None:
        """Park the pipeline, then write its flow for the history's newest order (Prepare)."""

        say = self._threaded_notify(notify)
        await asyncio.to_thread(lambda: self._job.park(say))
        watermark = await self._history.newest_committed_at() or EPOCH
        await asyncio.to_thread(lambda: self._write_flow(watermark))
        self.watermark = watermark

    async def read_source(self, order_id: str) -> LiveOrder | None:
        return await self._orders.read(order_id)

    async def commit(self, order: LiveOrder) -> None:
        await self._orders.commit(order)

    async def read_history(self, order: LiveOrder) -> bool:
        return await self._history.holds(order)

    async def start(self, *, on_refused: Callable[[BaseException], None] | None = None) -> None:
        """The bell's one ``createRun``. Returns once ADF has accepted it, not once it runs."""

        if self.watermark is None:
            raise Round6RaceError("Round 6's Azure lane was not prepared, so it has no watermark")
        try:
            self._run_id = await asyncio.to_thread(self._job.start)
        except Exception as refusal:
            if on_refused is not None:
                on_refused(refusal)
            raise

    async def failure(self) -> str | None:
        run_id = self._run_id
        if not run_id:
            return None
        run = await asyncio.to_thread(lambda: self._job.run(run_id))
        if run.state not in ENDED_STATES:
            return None
        if run.state == "Succeeded":
            return "The Data Factory run ended without delivering the order"
        # ADF's own message can name a linked service, a server or a storage account.
        LOGGER.warning("Round 6's ADF run %s ended %s: %s", run_id, run.state,
                       run.message or "no error message")
        return f"The Data Factory run ended {run.state}"

    async def park(self, notify: Notify) -> None:
        say = self._threaded_notify(notify)
        await asyncio.to_thread(lambda: self._job.park(say))

    async def delete(self, order: LiveOrder) -> None:
        await self._orders.delete(order)

    async def residue(self) -> list[LiveOrder]:
        return await self._orders.residue()

    async def evidence(self, order: LiveOrder) -> Mapping[str, Any]:
        """When the source committed the order, and the run that carried it, read after the race."""

        evidence: dict[str, Any] = {"competitor": "azure_postgres", "adf_run": self._run_id,
                                    "watermark": self.watermark,
                                    "committed_at": await self._history.committed_at(order),
                                    "history_copies": await self._history.copies(order)}
        run_id = self._run_id
        if run_id:
            run = await asyncio.to_thread(lambda: self._job.run(run_id))
            if run.started_on is not None:
                evidence["adf_run_started_on"] = run.started_on.isoformat()
            if run.completed_on is not None:
                evidence["adf_run_completed_on"] = run.completed_on.isoformat()
        return evidence


class PostgresOrderStore:
    """`OrderStore` on the r6 server: the AWS lane's SQL, on Azure's connection."""

    def __init__(self, connect: Callable[[str], Awaitable[Any]], schema: str, table: str) -> None:
        self._connect = connect
        self._table = sql.Identifier(schema, table)

    async def _rows(self, statement: sql.Composable, parameters: tuple[Any, ...],
                    application_name: str) -> list[tuple[Any, ...]]:
        connection = await self._connect(application_name)
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(statement, parameters)
                return list(await cursor.fetchall()) if cursor.description else []

    async def read(self, order_id: str) -> LiveOrder | None:
        rows = await self._rows(
            sql.SQL(f"SELECT {_COLUMNS} FROM {{}} WHERE order_id = %s").format(self._table),
            (order_id,), "lakebase-anti-demo-round6-checkout")
        if len(rows) > 1:
            raise Round6RaceError("Checkout returned duplicate order rows")
        return _order(tuple(rows[0])) if rows else None

    async def commit(self, order: LiveOrder) -> None:
        await self._rows(
            sql.SQL(f"INSERT INTO {{}} ({_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s, %s)").format(
                self._table),
            (order.order_id, order.sku, order.store, order.quantity, order.total_cents,
             order.status, order.proof_nonce), "lakebase-anti-demo-round6-checkout")

    async def delete(self, order: LiveOrder) -> None:
        rows = await self._rows(
            sql.SQL("DELETE FROM {} WHERE order_id = %s AND proof_nonce = %s AND sku = %s "
                    "AND store = %s AND quantity = %s AND total_cents = %s AND status = %s "
                    "RETURNING order_id").format(self._table),
            (order.order_id, order.proof_nonce, order.sku, order.store, order.quantity,
             order.total_cents, order.status), "lakebase-anti-demo-round6-cleanup")
        if len(rows) > 1:
            raise Round6RaceError("Exact checkout cleanup matched duplicate rows")

    async def residue(self) -> list[LiveOrder]:
        rows = await self._rows(
            sql.SQL(f"SELECT {_COLUMNS} FROM {{}} WHERE proof_nonce LIKE %s").format(self._table),
            (f"{BOUT_NONCE_PREFIX}%",), "lakebase-anti-demo-round6-cleanup")
        return [_order(tuple(row)) for row in rows]


class WarehouseHistory:
    """`History` through the workspace's SQL warehouse, the reader Lakebase's lane uses too."""

    def __init__(self, statements: Any, table_full_name: str) -> None:
        self._statements = statements
        self._table = _uc(table_full_name)

    async def _first(self, statement: str, order: LiveOrder | None = None) -> Mapping[str, Any]:
        parameters = () if order is None else (
            SqlParameter("order_id", order.order_id, "STRING"),
            SqlParameter("proof_nonce", order.proof_nonce, "STRING"))
        rows = await self._statements.execute(statement, parameters)
        return rows[0] if rows else {}

    async def newest_committed_at(self) -> str | None:
        """The history's newest `committed_at`, exact to the microsecond, as an ISO UTC instant.

        Read as an integer (``unix_micros``), not as the warehouse's rendering of a timestamp:
        a rendering that dropped the microseconds would make the last row extracted look newer
        than the watermark, so every run would extract it again.
        """

        row = await self._first(
            f"SELECT unix_micros(max(committed_at)) AS newest_us FROM {self._table}")
        newest = row.get("newest_us")
        if newest is None or str(newest) == "":
            return None
        instant = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=int(str(newest)))
        return instant.isoformat()

    async def holds(self, order: LiveOrder) -> bool:
        """The order is in the history. Once or more: the extract appends, so a second run for the
        same watermark would add it again, and that is still the order delivered (the count is
        in the evidence)."""

        return await self.copies(order) >= 1

    async def copies(self, order: LiveOrder) -> int:
        row = await self._first(
            f"SELECT count(*) AS n FROM {self._table} "
            "WHERE order_id = :order_id AND proof_nonce = :proof_nonce", order)
        return int(str(row.get("n") or 0))

    async def committed_at(self, order: LiveOrder) -> str | None:
        row = await self._first(
            f"SELECT max(committed_at) AS committed_at FROM {self._table} "
            "WHERE order_id = :order_id AND proof_nonce = :proof_nonce", order)
        value = row.get("committed_at")
        return str(value) if value else None
