from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

import psycopg
from databricks.sdk import WorkspaceClient

from .manifest import DemoManifest
from .model_score_live import SqlParameter, WorkspaceStatementRunner

LOGGER = logging.getLogger(__name__)


class LiveOrdersError(RuntimeError):
    """Base class for the native Lakebase CDF proof."""


class LiveOrdersNotArmedError(LiveOrdersError):
    pass


class LiveOrdersVerificationError(LiveOrdersError):
    pass


class LiveOrdersTimeoutError(LiveOrdersError):
    pass


class LiveOrdersPhase(StrEnum):
    PREFLIGHT = "preflight"
    ARMED = "armed"
    CHECKOUT = "checkout"
    WAITING_CDF = "waiting_cdf"
    READING_CHECKOUT = "reading_checkout"
    VERIFIED = "verified"
    FAILED = "failed"


@dataclass(frozen=True)
class LiveOrder:
    order_id: str
    sku: str
    store: str
    quantity: int
    total_cents: int
    status: str
    proof_nonce: str

    def __post_init__(self) -> None:
        if (
            not self.order_id
            or not self.sku
            or not self.store
            or self.quantity < 1
            or self.total_cents < 0
            or not self.status
            or not self.proof_nonce
        ):
            raise ValueError("order payload is incomplete")


@dataclass(frozen=True)
class LiveOrderHistory:
    order: LiveOrder
    change_type: str
    lsn: int


@dataclass(frozen=True)
class NativeCdfStatus:
    state: str
    committed_lsn: str
    last_sync_time: datetime
    source_table: str
    history_table: str


@dataclass(frozen=True)
class LiveOrdersContract:
    database_resource_name: str
    postgres_database: str
    source_schema: str
    source_table: str
    cdf_config_name: str
    cdf_status_name: str
    history_table: str
    baseline: LiveOrder = LiveOrder(
        order_id="00000000-0000-4000-8000-000000000006",
        sku="RED-GLOVE",
        store="CHICAGO",
        quantity=1,
        total_cents=8450,
        status="baseline",
        proof_nonce="round6-baseline",
    )
    sealed_sha256: str = ""

    @property
    def sha256(self) -> str:
        if self.sealed_sha256:
            return self.sealed_sha256
        values = (
            self.database_resource_name,
            self.postgres_database,
            self.source_schema,
            self.source_table,
            self.cdf_config_name,
            self.cdf_status_name,
            self.history_table,
            self.baseline.order_id,
            self.baseline.sku,
            self.baseline.store,
            str(self.baseline.quantity),
            str(self.baseline.total_cents),
            self.baseline.status,
            self.baseline.proof_nonce,
        )
        return hashlib.sha256("\0".join(values).encode()).hexdigest()


@dataclass(frozen=True)
class LiveOrdersArm:
    arm_id: str
    contract_sha256: str
    committed_lsn: str
    armed_at: datetime


@dataclass(frozen=True)
class LiveOrdersResult:
    order: LiveOrder
    checkout_guardrail_order: LiveOrder
    history_lsn: int
    matching_orders: int
    analytics_available_ms: float
    total_elapsed_ms: float
    checkout_commit_ms: float
    checkout_guardrail_commit_ms: float
    checkout_guardrail_read_ms: float
    checkout_verified: bool
    poll_attempts: int


@dataclass(frozen=True)
class LiveOrdersProgress:
    phase: LiveOrdersPhase
    status: str
    attempt: int | None = None
    elapsed_ms: float | None = None


class LiveOrdersAdapter(Protocol):
    async def inspect_feed(self) -> NativeCdfStatus: ...

    async def read_checkout(self, order_id: str) -> LiveOrder | None: ...

    async def insert_checkout(self, order: LiveOrder) -> None: ...

    async def delete_checkout_exact(self, order: LiveOrder) -> None: ...

    async def read_history(self, order: LiveOrder) -> LiveOrderHistory | None: ...


ProgressCallback = Callable[[LiveOrdersProgress], Awaitable[None]]


class LiveOrdersEngine:
    """One checkout row, one native-CDF history row, one checkout guardrail."""

    def __init__(
        self,
        adapter: LiveOrdersAdapter,
        *,
        contract: LiveOrdersContract,
        max_poll_attempts: int = 60,
        poll_interval_seconds: float = 1.0,
        operation_timeout_seconds: float = 60.0,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if max_poll_attempts < 1 or poll_interval_seconds < 0:
            raise ValueError("invalid CDF polling contract")
        self.adapter = adapter
        self.contract = contract
        self.max_poll_attempts = max_poll_attempts
        self.poll_interval_seconds = poll_interval_seconds
        self.operation_timeout_seconds = operation_timeout_seconds
        self._clock_ns = clock_ns
        self._now = now
        self._sleep = sleep
        self._used_arms: set[str] = set()
        self._checkout_commit_lock = asyncio.Lock()
        self._checkout_commit_tasks: dict[LiveOrder, asyncio.Task[None]] = {}
        self._settling_orders: set[LiveOrder] = set()

    async def arm(self, on_progress: ProgressCallback | None = None) -> LiveOrdersArm:
        await self._emit(on_progress, LiveOrdersPhase.PREFLIGHT, "Checking native CDF")
        feed = await self._bounded(self.adapter.inspect_feed(), "CDF preflight")
        self._require_streaming(feed, LiveOrdersNotArmedError)
        source = await self._bounded(
            self.adapter.read_checkout(self.contract.baseline.order_id),
            "checkout baseline read",
        )
        history = await self._bounded(
            self.adapter.read_history(self.contract.baseline),
            "CDF baseline read",
        )
        if source != self.contract.baseline:
            raise LiveOrdersNotArmedError("The exact checkout baseline is not present")
        if (
            history is None
            or history.order != self.contract.baseline
            or history.change_type != "insert"
        ):
            raise LiveOrdersNotArmedError("The exact baseline has not reached native CDF")
        arm = LiveOrdersArm(
            arm_id=uuid4().hex,
            contract_sha256=self.contract.sha256,
            committed_lsn=feed.committed_lsn,
            armed_at=self._aware(self._now()),
        )
        await self._emit(on_progress, LiveOrdersPhase.ARMED, "Native CDF is streaming")
        return arm

    async def run(
        self,
        arm: LiveOrdersArm,
        order: LiveOrder,
        checkout_guardrail_order: LiveOrder,
        on_progress: ProgressCallback | None = None,
    ) -> LiveOrdersResult:
        if arm.contract_sha256 != self.contract.sha256:
            raise LiveOrdersNotArmedError("The native CDF contract changed after arming")
        if arm.arm_id in self._used_arms:
            raise LiveOrdersVerificationError("This armed live-order proof was already used")
        if order == self.contract.baseline:
            raise LiveOrdersVerificationError("The live order must differ from the baseline")
        if (
            checkout_guardrail_order == order
            or checkout_guardrail_order == self.contract.baseline
            or checkout_guardrail_order.order_id == order.order_id
            or checkout_guardrail_order.proof_nonce == order.proof_nonce
        ):
            raise LiveOrdersVerificationError(
                "The checkout guardrail must be a separate order"
            )
        self._used_arms.add(arm.arm_id)
        await self._warm_checkout_endpoint(on_progress)
        total_started = self._clock_ns()
        await self._emit(on_progress, LiveOrdersPhase.CHECKOUT, "Committing one checkout order")
        commit_started = self._clock_ns()
        await self._commit_checkout(order, "checkout commit")
        commit_finished = self._clock_ns()
        checkout_commit_ms = self._elapsed(commit_started, commit_finished)
        analytics_started = commit_finished

        (history, attempts, analytics_finished), (
            guardrail_commit_ms,
            guardrail_read_ms,
        ) = await asyncio.gather(
            self._wait_for_history(order, analytics_started, on_progress),
            self._run_checkout_guardrail(checkout_guardrail_order, on_progress),
        )
        total_finished = self._clock_ns()
        result = LiveOrdersResult(
            order=order,
            checkout_guardrail_order=checkout_guardrail_order,
            history_lsn=history.lsn,
            matching_orders=1,
            analytics_available_ms=self._elapsed(analytics_started, analytics_finished),
            total_elapsed_ms=self._elapsed(total_started, total_finished),
            checkout_commit_ms=checkout_commit_ms,
            checkout_guardrail_commit_ms=guardrail_commit_ms,
            checkout_guardrail_read_ms=guardrail_read_ms,
            checkout_verified=True,
            poll_attempts=attempts,
        )
        await self._emit(
            on_progress,
            LiveOrdersPhase.VERIFIED,
            "Exact Delta answer and separate checkout committed",
            attempt=attempts,
            elapsed_ms=result.analytics_available_ms,
        )
        return result

    async def _warm_checkout_endpoint(self, on_progress: ProgressCallback | None) -> None:
        """Pay any endpoint wake before the clock starts rather than inside it.

        ``arm()`` opens a Postgres connection, so it leaves the endpoint awake --
        but it does not keep it awake until the bell, and the difference is what
        this exists for. The endpoint's suspend timer runs from arming's last
        Postgres touch, while the arm's own TTL only starts once the Delta
        history read has *also* returned. The suspend therefore comes due before
        the arm expires, and an operator who rings the bell late in the armed
        window would have the wake billed to ``checkout_commit_ms`` -- the very
        number that says this round did not slow checkout down.

        Reading the sealed baseline is the cheapest thing that reopens the
        connection. It writes nothing, advances no sequence, and returns a row
        that is a fixed part of the contract, so it cannot be confused with the
        bout's own order nor leave residue for the exact cleanup to find.

        A failure here is deliberately not fatal. The warm-up proves nothing that
        arming has not already proven, so refusing on it would end a live bout
        over an optimisation; and if the endpoint genuinely is unreachable, the
        checkout commit fails moments later with an error that names the real
        problem instead of blaming a preflight.
        """

        await self._emit(
            on_progress,
            LiveOrdersPhase.PREFLIGHT,
            "Proving the checkout endpoint is warm before the bell",
        )
        try:
            await self._bounded(
                self.adapter.read_checkout(self.contract.baseline.order_id),
                "checkout endpoint warm-up",
            )
        except Exception:
            LOGGER.warning(
                "Round 6 checkout endpoint warm-up failed; the bout continues and may "
                "pay an endpoint wake inside checkout_commit_ms",
                exc_info=True,
            )

    async def _wait_for_history(
        self,
        order: LiveOrder,
        analytics_started: int,
        on_progress: ProgressCallback | None,
    ) -> tuple[LiveOrderHistory, int, int]:
        source = await self._bounded(
            self.adapter.read_checkout(order.order_id), "committed checkout read"
        )
        if source != order:
            raise LiveOrdersVerificationError("Checkout did not return the exact committed order")
        history: LiveOrderHistory | None = None
        for attempts in range(1, self.max_poll_attempts + 1):
            elapsed = self._elapsed(analytics_started, self._clock_ns())
            await self._emit(
                on_progress,
                LiveOrdersPhase.WAITING_CDF,
                "Waiting for this order in Delta history",
                attempt=attempts,
                elapsed_ms=elapsed,
            )
            async def read_history_at_completion() -> tuple[LiveOrderHistory | None, int]:
                exact_history = await self._bounded(
                    self.adapter.read_history(order), "Delta history read"
                )
                return exact_history, self._clock_ns()

            feed, (history, history_read_finished) = await asyncio.gather(
                self._bounded(self.adapter.inspect_feed(), "CDF status read"),
                read_history_at_completion(),
            )
            self._require_streaming(feed, LiveOrdersVerificationError)
            if history is not None:
                if history.order != order or history.change_type != "insert" or history.lsn < 0:
                    raise LiveOrdersVerificationError(
                        "Delta history did not return one exact insert"
                    )
                return history, attempts, history_read_finished
            if attempts < self.max_poll_attempts:
                await self._sleep(self.poll_interval_seconds)
        if history is None:
            raise LiveOrdersTimeoutError("The exact order did not reach Delta history in time")
        raise LiveOrdersVerificationError("Delta history result was invalid")

    async def _run_checkout_guardrail(
        self,
        order: LiveOrder,
        on_progress: ProgressCallback | None,
    ) -> tuple[float, float]:
        await self._emit(
            on_progress,
            LiveOrdersPhase.CHECKOUT,
            "Committing a separate checkout guardrail order",
        )
        commit_started = self._clock_ns()
        await self._commit_checkout(order, "separate checkout guardrail commit")
        commit_finished = self._clock_ns()
        await self._emit(
            on_progress,
            LiveOrdersPhase.READING_CHECKOUT,
            "Separate checkout committed; verifying its exact row",
        )
        read_started = self._clock_ns()
        final_source = await self._bounded(
            self.adapter.read_checkout(order.order_id), "separate checkout guardrail read"
        )
        read_finished = self._clock_ns()
        if final_source != order:
            raise LiveOrdersVerificationError(
                "Separate checkout guardrail did not return the exact order"
            )
        return (
            self._elapsed(commit_started, commit_finished),
            self._elapsed(read_started, read_finished),
        )

    async def settle_and_cleanup_owned(
        self,
        order: LiveOrder,
        checkout_guardrail_order: LiveOrder | None,
    ) -> None:
        """Settle checkout commits, then remove only this run's exact source rows."""
        owned_orders = tuple(
            dict.fromkeys(
                candidate
                for candidate in (order, checkout_guardrail_order)
                if candidate is not None
            )
        )
        async with self._checkout_commit_lock:
            self._settling_orders.update(owned_orders)
            pending = tuple(
                task
                for owned_order in owned_orders
                if (task := self._checkout_commit_tasks.get(owned_order)) is not None
            )

        if pending:
            await asyncio.gather(
                *(asyncio.shield(task) for task in pending),
                return_exceptions=True,
            )

        for owned_order in owned_orders:
            await self._bounded(
                self.adapter.delete_checkout_exact(owned_order),
                "exact checkout cleanup",
            )

        async with self._checkout_commit_lock:
            for owned_order in owned_orders:
                task = self._checkout_commit_tasks.get(owned_order)
                if task is not None and task.done():
                    self._checkout_commit_tasks.pop(owned_order, None)

    async def _commit_checkout(self, order: LiveOrder, label: str) -> None:
        async with self._checkout_commit_lock:
            if order in self._settling_orders:
                raise LiveOrdersVerificationError(
                    "Checkout commit was stopped because exact cleanup has started"
                )
            if order in self._checkout_commit_tasks:
                raise LiveOrdersVerificationError("The exact checkout commit is already pending")
            task = asyncio.create_task(self.adapter.insert_checkout(order))
            self._checkout_commit_tasks[order] = task

        try:
            await self._bounded(asyncio.shield(task), label)
        finally:
            async with self._checkout_commit_lock:
                if self._checkout_commit_tasks.get(order) is task and task.done():
                    self._checkout_commit_tasks.pop(order, None)

    async def _bounded(self, operation: Awaitable[Any], label: str) -> Any:
        try:
            return await asyncio.wait_for(operation, timeout=self.operation_timeout_seconds)
        except TimeoutError as exc:
            raise LiveOrdersTimeoutError(f"{label} timed out") from exc

    def _require_streaming(self, feed: NativeCdfStatus, error: type[LiveOrdersError]) -> None:
        if (
            feed.state != "CDF_STATE_STREAMING"
            or not feed.committed_lsn
            or feed.source_table != self.contract.source_table
            or feed.history_table != self.contract.history_table
        ):
            raise error("Native CDF is not streaming the sealed live-order table")

    async def _emit(
        self,
        callback: ProgressCallback | None,
        phase: LiveOrdersPhase,
        status: str,
        *,
        attempt: int | None = None,
        elapsed_ms: float | None = None,
    ) -> None:
        if callback is not None:
            await callback(LiveOrdersProgress(phase, status, attempt, elapsed_ms))

    @staticmethod
    def _elapsed(started: int, finished: int) -> float:
        return max(0.0, (finished - started) / 1_000_000)

    @staticmethod
    def _aware(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise LiveOrdersVerificationError("CDF timestamps must be timezone-aware")
        return value.astimezone(UTC)


@dataclass(frozen=True)
class LiveOrdersLiveConfig:
    profile: str
    warehouse_id: str
    setup_principal: str
    app_service_principal_client_id: str
    endpoint_name: str
    database_resource_name: str
    postgres_database: str
    source_schema: str
    source_table: str
    cdf_config_name: str
    cdf_status_name: str
    destination_table_full_name: str
    database_user: str = ""


class LiveOrdersLiveAdapter(LiveOrdersAdapter):
    def __init__(
        self,
        config: LiveOrdersLiveConfig,
        *,
        workspace: Any | None = None,
        connector: Callable[..., Awaitable[Any]] = psycopg.AsyncConnection.connect,
    ) -> None:
        self.config = config
        self._workspace = workspace or (
            WorkspaceClient(profile=config.profile) if config.profile else WorkspaceClient()
        )
        self._statements = WorkspaceStatementRunner(self._workspace, config.warehouse_id)
        self._connector = connector

    async def inspect_feed(self) -> NativeCdfStatus:
        cdf_config, cdf_status = await asyncio.gather(
            asyncio.to_thread(
                self._workspace.postgres.get_cdf_config, self.config.cdf_config_name
            ),
            asyncio.to_thread(
                self._workspace.postgres.get_cdf_status, self.config.cdf_status_name
            ),
        )
        config = _mapping(cdf_config)
        status = _mapping(cdf_status)
        destination = self.config.destination_table_full_name.split(".")
        expected_config = {
            "name": self.config.cdf_config_name,
            "catalog": destination[0],
            "schema": destination[1],
            "postgres_schema": self.config.source_schema,
        }
        if {key: str(config.get(key) or "") for key in expected_config} != expected_config:
            raise LiveOrdersVerificationError("Native CDF configuration changed")
        if (
            str(status.get("name") or "") != self.config.cdf_status_name
            or str(status.get("postgres_table") or "") != self.config.source_table
            or str(status.get("uc_table") or "") != self.config.destination_table_full_name
            or str(status.get("status_detail") or "")
        ):
            raise LiveOrdersVerificationError("Native CDF table identity changed")
        last_sync = _timestamp(status.get("last_sync_time"))
        return NativeCdfStatus(
            state=_enum(status.get("state")),
            committed_lsn=str(status.get("committed_lsn") or ""),
            last_sync_time=last_sync,
            source_table=self.config.source_table,
            history_table=self.config.destination_table_full_name,
        )

    async def read_checkout(self, order_id: str) -> LiveOrder | None:
        connection = await self._connect()
        table = _postgres_identifier(self.config.source_schema, self.config.source_table)
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"SELECT order_id, sku, store, quantity, total_cents, status, proof_nonce "
                    f"FROM {table} WHERE order_id = %s",
                    (order_id,),
                )
                rows = await cursor.fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise LiveOrdersVerificationError("Checkout returned duplicate order rows")
        return LiveOrder(
            order_id=str(rows[0][0]),
            sku=str(rows[0][1]),
            store=str(rows[0][2]),
            quantity=int(rows[0][3]),
            total_cents=int(rows[0][4]),
            status=str(rows[0][5]),
            proof_nonce=str(rows[0][6]),
        )

    async def insert_checkout(self, order: LiveOrder) -> None:
        connection = await self._connect()
        table = _postgres_identifier(self.config.source_schema, self.config.source_table)
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    f"INSERT INTO {table} ("
                    "order_id, sku, store, quantity, total_cents, status, proof_nonce"
                    ") VALUES (%s, %s, %s, %s, %s, %s, %s)",
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
            await connection.commit()

    async def delete_checkout_exact(self, order: LiveOrder) -> None:
        connection = await self._connect()
        table = _postgres_identifier(self.config.source_schema, self.config.source_table)
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
                    f"DELETE FROM {table} WHERE {predicate} RETURNING order_id",
                    parameters,
                )
                deleted = await cursor.fetchall()
                if len(deleted) > 1:
                    raise LiveOrdersVerificationError(
                        "Exact checkout cleanup matched duplicate source rows"
                    )
                await connection.commit()
                await cursor.execute(
                    f"SELECT 1 FROM {table} WHERE {predicate}",
                    parameters,
                )
                if await cursor.fetchall():
                    raise LiveOrdersVerificationError(
                        "Exact checkout cleanup could not verify source-row absence"
                    )

    async def read_history(self, order: LiveOrder) -> LiveOrderHistory | None:
        table = _uc_identifier(self.config.destination_table_full_name)
        rows = await self._statements.execute(
            "SELECT order_id, sku, store, quantity, total_cents, status, proof_nonce, "
            f"_pg_change_type, _pg_lsn FROM {table} "
            "WHERE order_id = :order_id AND proof_nonce = :proof_nonce",
            (
                SqlParameter("order_id", order.order_id, "STRING"),
                SqlParameter("proof_nonce", order.proof_nonce, "STRING"),
            ),
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise LiveOrdersVerificationError("Delta history returned duplicate proof rows")
        row = rows[0]
        live_order = LiveOrder(
            order_id=str(row.get("order_id") or ""),
            sku=str(row.get("sku") or ""),
            store=str(row.get("store") or ""),
            quantity=int(str(row.get("quantity") or "-1")),
            total_cents=int(str(row.get("total_cents") or "-1")),
            status=str(row.get("status") or ""),
            proof_nonce=str(row.get("proof_nonce") or ""),
        )
        return LiveOrderHistory(
            order=live_order,
            change_type=str(row.get("_pg_change_type") or ""),
            lsn=int(str(row.get("_pg_lsn") or "-1")),
        )

    async def _connect(self) -> Any:
        endpoint, credential, current_user = await asyncio.gather(
            asyncio.to_thread(self._workspace.postgres.get_endpoint, self.config.endpoint_name),
            asyncio.to_thread(
                self._workspace.postgres.generate_database_credential,
                self.config.endpoint_name,
            ),
            asyncio.to_thread(self._workspace.current_user.me),
        )
        endpoint_payload = _mapping(endpoint)
        host = str(
            _mapping(_mapping(endpoint_payload.get("status")).get("hosts")).get("host")
            or ""
        )
        token = str(getattr(credential, "token", None) or _mapping(credential).get("token") or "")
        returned_user = str(
            getattr(current_user, "user_name", None) or _mapping(current_user).get("userName") or ""
        )
        if (
            not self.config.profile
            and returned_user.casefold()
            != self.config.app_service_principal_client_id.casefold()
        ):
            raise LiveOrdersVerificationError("Runtime principal changed")
        user = self.config.database_user or returned_user
        if not host or not token or not user:
            raise LiveOrdersVerificationError("Lakebase checkout connection is unavailable")
        return await self._connector(
            host=host,
            port=5432,
            dbname=self.config.postgres_database,
            user=user,
            password=token,
            sslmode="require",
            application_name="lakebase-anti-demo-round-6",
            connect_timeout=30,
        )


def build_live_orders_engine(manifest: DemoManifest) -> LiveOrdersEngine:
    resources = manifest.round6
    if resources is None:
        raise LiveOrdersNotArmedError("A sealed native CDF contract is required")
    contract = LiveOrdersContract(
        database_resource_name=resources.database_resource_name,
        postgres_database=resources.postgres_database,
        source_schema=resources.source_schema,
        source_table=resources.source_table,
        cdf_config_name=resources.cdf_config_name,
        cdf_status_name=resources.cdf_status_name,
        history_table=resources.destination_table_full_name,
        baseline=LiveOrder(
            order_id=resources.baseline_order_id,
            sku=resources.baseline_sku,
            store=resources.baseline_store,
            quantity=resources.baseline_quantity,
            total_cents=resources.baseline_total_cents,
            status=resources.baseline_status,
            proof_nonce=resources.baseline_proof_nonce,
        ),
        sealed_sha256=resources.contract_sha256,
    )
    deployed = os.environ.get("ANTI_DEMO_ENV") == "databricks-app" or bool(
        os.environ.get("DATABRICKS_APP_NAME")
    )
    config = LiveOrdersLiveConfig(
        profile="" if deployed else manifest.databricks.profile,
        warehouse_id=resources.warehouse_id,
        setup_principal=resources.setup_principal,
        app_service_principal_client_id=resources.app_service_principal_client_id,
        endpoint_name=resources.endpoint_name,
        database_resource_name=resources.database_resource_name,
        postgres_database=resources.postgres_database,
        source_schema=resources.source_schema,
        source_table=resources.source_table,
        cdf_config_name=resources.cdf_config_name,
        cdf_status_name=resources.cdf_status_name,
        destination_table_full_name=resources.destination_table_full_name,
        database_user="" if deployed else manifest.databricks.user,
    )
    return LiveOrdersEngine(LiveOrdersLiveAdapter(config), contract=contract)


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "as_dict"):
        converted = value.as_dict()
        if isinstance(converted, Mapping):
            return converted
    return {}


def _enum(value: object) -> str:
    return str(getattr(value, "value", value) or "")


def _timestamp(value: object) -> datetime:
    if hasattr(value, "ToDatetime"):
        value = value.ToDatetime(tzinfo=UTC)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise LiveOrdersVerificationError("Native CDF status omitted last_sync_time")
    return value.astimezone(UTC)


def _postgres_identifier(schema: str, table: str) -> str:
    if not schema or not table or "." in schema or "." in table:
        raise LiveOrdersVerificationError("Invalid checkout table identifier")
    return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in (schema, table))


def _uc_identifier(name: str) -> str:
    parts = name.split(".")
    if len(parts) != 3 or any(not part for part in parts):
        raise LiveOrdersVerificationError("Invalid Delta history table identifier")
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)
