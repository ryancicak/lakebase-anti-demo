from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Any, Protocol, TypeVar

import psycopg
from databricks.sdk import WorkspaceClient

from .coordination import CoordinationObjectsMissingError, read_coordination_objects

COORDINATION_SCHEMA = "anti_demo_coordination"
COST_LEDGER_TABLE = f"{COORDINATION_SCHEMA}.cost_ledger"
RECONCILIATION_TABLE = f"{COORDINATION_SCHEMA}.cost_reconciliation_snapshot"
CALIBRATION_TABLE = f"{COORDINATION_SCHEMA}.cost_calibration_profile"
_ESTIMATE_GUARD_TRIGGER = "protect_cost_estimate_update"

_RATE_QUANTUM = Decimal("0.000000000000000001")
_COST_QUANTUM = Decimal("0.000000000000000000000001")


class EstimateConflictError(RuntimeError):
    """The stable ledger ID already has a different original estimate."""


class IncompleteWindowError(ValueError):
    """A correction omitted a line from an earlier full-window snapshot."""


class BoutWindowConflictError(RuntimeError):
    """A closed bout window was asked to close with different evidence."""


def _required_text(value: str, name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _decimal(
    value: Decimal | None,
    *,
    quantum: Decimal,
    name: str,
    non_negative: bool = True,
) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal, not {type(value).__name__}")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if non_negative and value < 0:
        raise ValueError(f"{name} must be non-negative")
    if value.as_tuple().exponent < quantum.as_tuple().exponent:
        raise ValueError(f"{name} exceeds the supported scale")
    return value


@dataclass(frozen=True)
class CalibrationKey:
    provider: str
    region: str
    component: str
    attribution_method: str
    configuration_fingerprint: str
    unit: str

    def __post_init__(self) -> None:
        for field_name in (
            "provider",
            "region",
            "component",
            "attribution_method",
            "configuration_fingerprint",
            "unit",
        ):
            object.__setattr__(
                self, field_name, _required_text(getattr(self, field_name), field_name)
            )


@dataclass(frozen=True)
class CostEstimate:
    ledger_id: str
    installation_id: str
    bout_id: str
    session_id: str
    round_id: str
    lane_id: str
    resource_id: str
    resource_type: str
    resource_name: str | None
    resource_arn: str | None
    scope: str
    key: CalibrationKey
    original_quantity: Decimal | None
    original_unit_rate_usd: Decimal | None
    original_cost_usd: Decimal | None
    window_start: datetime
    created_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "ledger_id",
            "installation_id",
            "bout_id",
            "session_id",
            "round_id",
            "lane_id",
            "resource_id",
            "resource_type",
            "scope",
        ):
            object.__setattr__(
                self, field_name, _required_text(getattr(self, field_name), field_name)
            )
        for field_name in ("resource_name", "resource_arn"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _required_text(value, field_name))
        object.__setattr__(
            self,
            "original_quantity",
            _decimal(self.original_quantity, quantum=_RATE_QUANTUM, name="original_quantity"),
        )
        object.__setattr__(
            self,
            "original_unit_rate_usd",
            _decimal(
                self.original_unit_rate_usd,
                quantum=_RATE_QUANTUM,
                name="original_unit_rate_usd",
            ),
        )
        object.__setattr__(
            self,
            "original_cost_usd",
            _decimal(self.original_cost_usd, quantum=_COST_QUANTUM, name="original_cost_usd"),
        )
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "window_start", _utc(self.window_start, "window_start"))


@dataclass(frozen=True)
class PostedCost:
    ledger_id: str
    quantity: Decimal | None
    unit_rate_usd: Decimal | None
    cost_usd: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "ledger_id", _required_text(self.ledger_id, "ledger_id"))
        object.__setattr__(
            self,
            "quantity",
            _decimal(self.quantity, quantum=_RATE_QUANTUM, name="quantity"),
        )
        object.__setattr__(
            self,
            "unit_rate_usd",
            _decimal(self.unit_rate_usd, quantum=_RATE_QUANTUM, name="unit_rate_usd"),
        )
        checked_cost = _decimal(self.cost_usd, quantum=_COST_QUANTUM, name="cost_usd")
        assert checked_cost is not None
        object.__setattr__(self, "cost_usd", checked_cost)


@dataclass(frozen=True)
class FullWindowReconciliation:
    window_id: str
    provider: str
    region: str
    window_start: datetime
    window_end: datetime
    provider_watermark: datetime
    observed_at: datetime
    clean: bool
    items: tuple[PostedCost, ...]

    def __post_init__(self) -> None:
        for field_name in ("window_id", "provider", "region"):
            object.__setattr__(
                self, field_name, _required_text(getattr(self, field_name), field_name)
            )
        for field_name in ("window_start", "window_end", "provider_watermark", "observed_at"):
            object.__setattr__(self, field_name, _utc(getattr(self, field_name), field_name))
        if self.window_end <= self.window_start:
            raise ValueError("window_end must be after window_start")
        if self.provider_watermark < self.window_end:
            raise ValueError("provider_watermark must cover the complete window")
        if self.observed_at < self.provider_watermark:
            raise ValueError("observed_at cannot precede provider_watermark")
        if not self.items:
            raise ValueError("a full-window reconciliation requires at least one item")
        ids = [item.ledger_id for item in self.items]
        if len(ids) != len(set(ids)):
            raise ValueError("a full-window reconciliation cannot repeat a ledger_id")


@dataclass(frozen=True)
class LedgerRecord:
    estimate: CostEstimate
    posted_quantity: Decimal | None = None
    posted_unit_rate_usd: Decimal | None = None
    posted_cost_usd: Decimal | None = None
    variance_usd: Decimal | None = None
    reconciliation_revision: int = 0
    provider_watermark: datetime | None = None
    reconciliation_observed_at: datetime | None = None
    clean: bool | None = None
    window_end: datetime | None = None
    terminal_outcome: str | None = None


@dataclass(frozen=True)
class ReconciliationSnapshot:
    window_id: str
    revision: int
    window_start: datetime
    window_end: datetime
    provider_watermark: datetime
    observed_at: datetime
    clean: bool
    item: PostedCost
    variance_usd: Decimal | None


@dataclass(frozen=True)
class CalibrationProfile:
    key: CalibrationKey
    sample_count: int
    quantity_sum: Decimal
    cost_sum_usd: Decimal
    calibrated_unit_rate_usd: Decimal
    provider_watermark: datetime


class CostLedgerStore(Protocol):
    mode: str

    async def initialize(self) -> None: ...

    async def close(self) -> None: ...

    async def record_estimate(self, estimate: CostEstimate) -> LedgerRecord: ...

    async def record_estimates(
        self, estimates: Sequence[CostEstimate]
    ) -> tuple[LedgerRecord, ...]: ...

    async def close_bout(
        self,
        *,
        installation_id: str,
        bout_id: str,
        window_end: datetime,
        terminal_outcome: str,
    ) -> tuple[LedgerRecord, ...]: ...

    async def reconcile_window(
        self, reconciliation: FullWindowReconciliation
    ) -> tuple[LedgerRecord, ...]: ...

    async def get(self, ledger_id: str) -> LedgerRecord | None: ...

    async def snapshots(self, window_id: str) -> tuple[ReconciliationSnapshot, ...]: ...

    async def calibration_for(self, key: CalibrationKey) -> CalibrationProfile | None: ...


def _variance(posted: Decimal, original: Decimal | None) -> Decimal | None:
    if original is None:
        return None
    return posted - original


def _profile(records: Sequence[LedgerRecord], key: CalibrationKey) -> CalibrationProfile | None:
    samples = [
        record
        for record in records
        if record.estimate.key == key
        and record.clean is True
        and record.posted_quantity is not None
        and record.posted_quantity > 0
        and record.posted_cost_usd is not None
        and record.provider_watermark is not None
    ]
    if len(samples) < 3:
        return None
    quantity_sum = sum((sample.posted_quantity for sample in samples), Decimal(0))
    if quantity_sum <= 0:
        return None
    cost_sum = sum((sample.posted_cost_usd for sample in samples), Decimal(0))
    with localcontext() as context:
        context.prec = 76
        calibrated_rate = (cost_sum / quantity_sum).quantize(
            _RATE_QUANTUM, rounding=ROUND_HALF_EVEN
        )
    return CalibrationProfile(
        key=key,
        sample_count=len(samples),
        quantity_sum=quantity_sum,
        cost_sum_usd=cost_sum,
        calibrated_unit_rate_usd=calibrated_rate,
        provider_watermark=max(
            sample.provider_watermark for sample in samples if sample.provider_watermark is not None
        ),
    )


class InMemoryCostLedgerStore:
    mode = "memory"

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._records: dict[str, LedgerRecord] = {}
        self._snapshots: dict[str, list[ReconciliationSnapshot]] = {}
        self._profiles: dict[CalibrationKey, CalibrationProfile] = {}

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def record_estimate(self, estimate: CostEstimate) -> LedgerRecord:
        records = await self.record_estimates((estimate,))
        return records[0]

    async def record_estimates(
        self, estimates: Sequence[CostEstimate]
    ) -> tuple[LedgerRecord, ...]:
        if not estimates:
            raise ValueError("at least one cost estimate is required")
        async with self._lock:
            seen: set[str] = set()
            for estimate in estimates:
                if estimate.ledger_id in seen:
                    raise ValueError("a cost estimate batch cannot repeat a ledger_id")
                seen.add(estimate.ledger_id)
                existing = self._records.get(estimate.ledger_id)
                if existing is not None and existing.estimate != estimate:
                    raise EstimateConflictError(
                        f"ledger_id {estimate.ledger_id!r} already has an immutable estimate"
                    )
            records: list[LedgerRecord] = []
            for estimate in estimates:
                record = self._records.get(estimate.ledger_id)
                if record is None:
                    record = LedgerRecord(estimate=estimate)
                    self._records[estimate.ledger_id] = record
                records.append(record)
            return tuple(records)

    async def close_bout(
        self,
        *,
        installation_id: str,
        bout_id: str,
        window_end: datetime,
        terminal_outcome: str,
    ) -> tuple[LedgerRecord, ...]:
        installation_id = _required_text(installation_id, "installation_id")
        bout_id = _required_text(bout_id, "bout_id")
        terminal_outcome = _required_text(terminal_outcome, "terminal_outcome")
        closed_at = _utc(window_end, "window_end")
        async with self._lock:
            selected = [
                record
                for record in self._records.values()
                if record.estimate.installation_id == installation_id
                and record.estimate.bout_id == bout_id
            ]
            if not selected:
                raise KeyError(f"unknown cost bout {bout_id!r}")
            for record in selected:
                if closed_at <= record.estimate.window_start:
                    raise ValueError("window_end must be after window_start")
                if record.window_end is not None and (
                    record.window_end != closed_at
                    or record.terminal_outcome != terminal_outcome
                ):
                    raise BoutWindowConflictError(
                        f"cost bout {bout_id!r} already has different terminal evidence"
                    )
            updated: list[LedgerRecord] = []
            for record in selected:
                if record.window_end is None:
                    record = replace(
                        record,
                        window_end=closed_at,
                        terminal_outcome=terminal_outcome,
                    )
                    self._records[record.estimate.ledger_id] = record
                updated.append(record)
            return tuple(updated)

    async def reconcile_window(
        self, reconciliation: FullWindowReconciliation
    ) -> tuple[LedgerRecord, ...]:
        async with self._lock:
            prior = self._snapshots.get(reconciliation.window_id, [])
            if prior:
                first = prior[0]
                if (
                    first.window_start != reconciliation.window_start
                    or first.window_end != reconciliation.window_end
                ):
                    raise ValueError("window_id cannot be reused for different time bounds")
                latest_revision = max(snapshot.revision for snapshot in prior)
                latest = [snapshot for snapshot in prior if snapshot.revision == latest_revision]
                if reconciliation.provider_watermark < latest[0].provider_watermark:
                    raise ValueError("a correction cannot regress the provider watermark")
                prior_ids = {snapshot.item.ledger_id for snapshot in latest}
                current_ids = {item.ledger_id for item in reconciliation.items}
                if not prior_ids.issubset(current_ids):
                    raise IncompleteWindowError(
                        "a correction must include every line from the prior full-window revision"
                    )
                revision = latest_revision + 1
            else:
                revision = 1

            records: list[LedgerRecord] = []
            affected_keys: set[CalibrationKey] = set()
            pending_snapshots: list[ReconciliationSnapshot] = []
            for item in reconciliation.items:
                current = self._records.get(item.ledger_id)
                if current is None:
                    raise KeyError(f"unknown ledger_id {item.ledger_id!r}")
                if (
                    current.estimate.key.provider != reconciliation.provider
                    or current.estimate.key.region != reconciliation.region
                ):
                    raise ValueError("reconciliation provider/region does not match its estimate")
                variance = _variance(item.cost_usd, current.estimate.original_cost_usd)
                updated = replace(
                    current,
                    posted_quantity=item.quantity,
                    posted_unit_rate_usd=item.unit_rate_usd,
                    posted_cost_usd=item.cost_usd,
                    variance_usd=variance,
                    reconciliation_revision=revision,
                    provider_watermark=reconciliation.provider_watermark,
                    reconciliation_observed_at=reconciliation.observed_at,
                    clean=reconciliation.clean,
                )
                records.append(updated)
                affected_keys.add(current.estimate.key)
                pending_snapshots.append(
                    ReconciliationSnapshot(
                        window_id=reconciliation.window_id,
                        revision=revision,
                        window_start=reconciliation.window_start,
                        window_end=reconciliation.window_end,
                        provider_watermark=reconciliation.provider_watermark,
                        observed_at=reconciliation.observed_at,
                        clean=reconciliation.clean,
                        item=item,
                        variance_usd=variance,
                    )
                )

            for record in records:
                self._records[record.estimate.ledger_id] = record
            self._snapshots.setdefault(reconciliation.window_id, []).extend(pending_snapshots)
            for key in affected_keys:
                profile = _profile(tuple(self._records.values()), key)
                if profile is None:
                    self._profiles.pop(key, None)
                else:
                    self._profiles[key] = profile
            return tuple(records)

    async def get(self, ledger_id: str) -> LedgerRecord | None:
        async with self._lock:
            return self._records.get(ledger_id)

    async def snapshots(self, window_id: str) -> tuple[ReconciliationSnapshot, ...]:
        async with self._lock:
            return tuple(self._snapshots.get(window_id, ()))

    async def calibration_for(self, key: CalibrationKey) -> CalibrationProfile | None:
        async with self._lock:
            return self._profiles.get(key)


T = TypeVar("T")


class LakebaseCostLedgerStore:
    """Durable cost evidence stored on the dedicated coordination project."""

    mode = "lakebase"

    def __init__(
        self,
        *,
        endpoint_name: str,
        database: str,
        profile: str = "",
        host: str = "",
        user: str = "",
        port: int = 5432,
        connector: Callable[..., Awaitable[Any]] = psycopg.AsyncConnection.connect,
        workspace_client: WorkspaceClient | None = None,
    ) -> None:
        if not endpoint_name:
            raise ValueError("A dedicated Lakebase coordination endpoint is required")
        self.endpoint_name = endpoint_name
        self.database = database
        self.profile = profile
        self.host = host
        self.user = user
        self.port = port
        self._connector = connector
        self._workspace = workspace_client or (
            WorkspaceClient(profile=profile) if profile else WorkspaceClient()
        )
        self._material_lock = asyncio.Lock()
        self._cached_token = ""
        self._token_refresh_at = 0.0

    async def _connection_material(self, *, force_refresh: bool = False) -> tuple[str, str, str]:
        async with self._material_lock:
            if not self.host:
                endpoint = await asyncio.to_thread(
                    self._workspace.postgres.get_endpoint, self.endpoint_name
                )
                self.host = str(endpoint.status.hosts.host if endpoint.status else "")
            if not self.user:
                self.user = os.environ.get("PGUSER", "").strip() or str(
                    (await asyncio.to_thread(self._workspace.current_user.me)).user_name or ""
                )
            now = time.monotonic()
            if force_refresh or not self._cached_token or now >= self._token_refresh_at:
                credential = await asyncio.to_thread(
                    self._workspace.postgres.generate_database_credential,
                    self.endpoint_name,
                )
                self._cached_token = str(credential.token or "")
                self._token_refresh_at = now + 2700
            if not self.host or not self.user or not self._cached_token:
                raise RuntimeError("Lakebase coordination host, user, or credential is missing")
            return self.host, self.user, self._cached_token

    async def _run(self, operation: Callable[[Any], Awaitable[T]]) -> T:
        last_error: BaseException | None = None
        for attempt in range(2):
            try:
                host, user, token = await self._connection_material(force_refresh=attempt > 0)
                connection = await self._connector(
                    host=host,
                    port=self.port,
                    dbname=self.database,
                    user=user,
                    password=token,
                    sslmode="require",
                    application_name="lakebase-anti-demo-cost-ledger",
                    connect_timeout=15,
                )
                async with connection:
                    async with connection.cursor() as cursor:
                        return await operation(cursor)
            except psycopg.OperationalError as exc:
                last_error = exc
                self._cached_token = ""
                if attempt == 0:
                    await asyncio.sleep(0.25)
        assert last_error is not None
        raise last_error

    async def initialize(self) -> None:
        """Make the ledger usable, and create it only when it is genuinely absent.

        The deployed app is a *consumer* of ``anti_demo_coordination``: it holds
        no DDL on database ``anti_demo``, by design and to match every other way
        this repository treats that principal. Running the create sequence
        unconditionally therefore failed a start that had nothing wrong with it
        -- ``CREATE SCHEMA IF NOT EXISTS`` checks the ACL before the
        ``IF NOT EXISTS``, so the no-op was refused rather than skipped.

        Looking first is what makes tolerating that refusal safe. Tolerating it
        blindly would not be: it would hand back a store that has never
        confirmed a single one of its four objects exists, and the app would go
        on to serve a cost disclosure off it. So an absent object with no way to
        create it raises :class:`CoordinationObjectsMissingError` and the start
        dies, which on the deployed path is a crash with this reason in the log.
        """

        async def create_tables(cursor: Any, *, create_schema: bool) -> None:
            if create_schema:
                await cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {COORDINATION_SCHEMA}")
            await cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {COST_LEDGER_TABLE} (
                    ledger_id text PRIMARY KEY,
                    installation_id text NOT NULL,
                    bout_id text NOT NULL,
                    session_id text NOT NULL,
                    round_id text NOT NULL,
                    lane_id text NOT NULL,
                    resource_id text NOT NULL,
                    resource_type text NOT NULL,
                    resource_name text,
                    resource_arn text,
                    cost_scope text NOT NULL,
                    provider text NOT NULL,
                    region text NOT NULL,
                    component text NOT NULL,
                    attribution_method text NOT NULL,
                    configuration_fingerprint text NOT NULL,
                    unit text NOT NULL,
                    original_quantity NUMERIC(38,18),
                    original_unit_rate_usd NUMERIC(38,18),
                    original_cost_usd NUMERIC(38,24),
                    window_start timestamptz NOT NULL,
                    window_end timestamptz,
                    terminal_outcome text,
                    posted_quantity NUMERIC(38,18),
                    posted_unit_rate_usd NUMERIC(38,18),
                    posted_cost_usd NUMERIC(38,24),
                    variance_usd NUMERIC(38,24),
                    reconciliation_revision bigint NOT NULL DEFAULT 0,
                    provider_watermark timestamptz,
                    reconciliation_observed_at timestamptz,
                    clean boolean,
                    created_at timestamptz NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
                )
                """
            )
            await cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RECONCILIATION_TABLE} (
                    window_id text NOT NULL,
                    revision bigint NOT NULL,
                    ledger_id text NOT NULL REFERENCES {COST_LEDGER_TABLE}(ledger_id),
                    provider text NOT NULL,
                    region text NOT NULL,
                    window_start timestamptz NOT NULL,
                    window_end timestamptz NOT NULL,
                    provider_watermark timestamptz NOT NULL,
                    observed_at timestamptz NOT NULL,
                    clean boolean NOT NULL,
                    posted_quantity NUMERIC(38,18),
                    posted_unit_rate_usd NUMERIC(38,18),
                    posted_cost_usd NUMERIC(38,24) NOT NULL,
                    variance_usd NUMERIC(38,24),
                    PRIMARY KEY (window_id, revision, ledger_id)
                )
                """
            )
            await cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CALIBRATION_TABLE} (
                    provider text NOT NULL,
                    region text NOT NULL,
                    component text NOT NULL,
                    attribution_method text NOT NULL,
                    configuration_fingerprint text NOT NULL,
                    unit text NOT NULL,
                    sample_count bigint NOT NULL CHECK (sample_count >= 3),
                    quantity_sum NUMERIC(38,18) NOT NULL,
                    cost_sum_usd NUMERIC(38,24) NOT NULL,
                    calibrated_unit_rate_usd NUMERIC(38,18) NOT NULL,
                    provider_watermark timestamptz NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                    PRIMARY KEY (
                        provider, region, component, attribution_method,
                        configuration_fingerprint, unit
                    )
                )
                """
            )
            await cursor.execute(
                f"""
                CREATE OR REPLACE FUNCTION {COORDINATION_SCHEMA}.protect_cost_estimate()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF ROW(
                        NEW.installation_id, NEW.bout_id, NEW.round_id, NEW.lane_id,
                        NEW.session_id, NEW.resource_id, NEW.resource_type,
                        NEW.resource_name, NEW.resource_arn, NEW.cost_scope,
                        NEW.provider, NEW.region, NEW.component,
                        NEW.attribution_method, NEW.configuration_fingerprint, NEW.unit,
                        NEW.original_quantity, NEW.original_unit_rate_usd,
                        NEW.original_cost_usd, NEW.window_start, NEW.created_at
                    ) IS DISTINCT FROM ROW(
                        OLD.installation_id, OLD.bout_id, OLD.round_id, OLD.lane_id,
                        OLD.session_id, OLD.resource_id, OLD.resource_type,
                        OLD.resource_name, OLD.resource_arn, OLD.cost_scope,
                        OLD.provider, OLD.region, OLD.component,
                        OLD.attribution_method, OLD.configuration_fingerprint, OLD.unit,
                        OLD.original_quantity, OLD.original_unit_rate_usd,
                        OLD.original_cost_usd, OLD.window_start, OLD.created_at
                    ) THEN
                        RAISE EXCEPTION 'original cost estimate is immutable';
                    END IF;
                    RETURN NEW;
                END $$
                """
            )
            await cursor.execute(
                f"""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_trigger
                        WHERE tgname = '{_ESTIMATE_GUARD_TRIGGER}'
                          AND tgrelid = '{COST_LEDGER_TABLE}'::regclass
                    ) THEN
                        CREATE TRIGGER {_ESTIMATE_GUARD_TRIGGER}
                        BEFORE UPDATE ON {COST_LEDGER_TABLE}
                        FOR EACH ROW EXECUTE FUNCTION
                            {COORDINATION_SCHEMA}.protect_cost_estimate();
                    END IF;
                END $$
                """
            )

        async def ensure(cursor: Any) -> None:
            objects = await read_coordination_objects(
                cursor,
                (COST_LEDGER_TABLE, RECONCILIATION_TABLE, CALIBRATION_TABLE),
            )
            guard_present = objects.complete and await self._estimate_guard_present(cursor)
            if objects.complete and guard_present:
                return
            missing = (
                objects.describe_missing()
                if not objects.complete
                else f"the {_ESTIMATE_GUARD_TRIGGER} guard on {COST_LEDGER_TABLE}"
            )
            try:
                await create_tables(cursor, create_schema=not objects.schema_present)
            except psycopg.errors.InsufficientPrivilege as exc:
                raise CoordinationObjectsMissingError(
                    f"The cost ledger in {self.database!r} is missing {missing}, and this "
                    "identity may not create it. Provision the coordination schema with an "
                    "identity that owns it (`antidemo setup`), then grant this one the runtime "
                    "privileges in docs/DEPLOY.md."
                ) from exc

        await self._run(ensure)

    @staticmethod
    async def _estimate_guard_present(cursor: Any) -> bool:
        """Whether the trigger that makes an original estimate immutable is installed.

        Checked alongside the tables rather than assumed from them: the tables
        without the guard is a real, silent drift state -- estimates become
        editable and the ledger stops being evidence. The lookup joins through
        ``pg_class`` rather than casting to ``regclass`` so it answers ``False``
        for an absent table instead of raising.
        """

        schema, _, table = COST_LEDGER_TABLE.partition(".")
        await cursor.execute(
            """
            SELECT 1
            FROM pg_catalog.pg_trigger t
            JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
              AND t.tgname = %s AND NOT t.tgisinternal
            """,
            (schema, table, _ESTIMATE_GUARD_TRIGGER),
        )
        return await cursor.fetchone() is not None

    async def close(self) -> None:
        self._cached_token = ""
        self._token_refresh_at = 0.0

    @staticmethod
    def _columns() -> str:
        return (
            "ledger_id, installation_id, bout_id, session_id, round_id, lane_id, resource_id, "
            "resource_type, resource_name, resource_arn, cost_scope, "
            "provider, region, component, attribution_method, configuration_fingerprint, "
            "unit, original_quantity, original_unit_rate_usd, original_cost_usd, window_start, "
            "created_at, window_end, terminal_outcome, "
            "posted_quantity, posted_unit_rate_usd, posted_cost_usd, variance_usd, "
            "reconciliation_revision, provider_watermark, reconciliation_observed_at, clean"
        )

    @staticmethod
    def _row_to_record(row: Any) -> LedgerRecord:
        key = CalibrationKey(*map(str, row[11:17]))
        estimate = CostEstimate(
            ledger_id=str(row[0]),
            installation_id=str(row[1]),
            bout_id=str(row[2]),
            session_id=str(row[3]),
            round_id=str(row[4]),
            lane_id=str(row[5]),
            resource_id=str(row[6]),
            resource_type=str(row[7]),
            resource_name=str(row[8]) if row[8] is not None else None,
            resource_arn=str(row[9]) if row[9] is not None else None,
            scope=str(row[10]),
            key=key,
            original_quantity=row[17],
            original_unit_rate_usd=row[18],
            original_cost_usd=row[19],
            window_start=row[20],
            created_at=row[21],
        )
        return LedgerRecord(
            estimate=estimate,
            window_end=row[22],
            terminal_outcome=str(row[23]) if row[23] is not None else None,
            posted_quantity=row[24],
            posted_unit_rate_usd=row[25],
            posted_cost_usd=row[26],
            variance_usd=row[27],
            reconciliation_revision=int(row[28]),
            provider_watermark=row[29],
            reconciliation_observed_at=row[30],
            clean=row[31],
        )

    async def record_estimate(self, estimate: CostEstimate) -> LedgerRecord:
        records = await self.record_estimates((estimate,))
        return records[0]

    async def record_estimates(
        self, estimates: Sequence[CostEstimate]
    ) -> tuple[LedgerRecord, ...]:
        if not estimates:
            raise ValueError("at least one cost estimate is required")
        ids = [estimate.ledger_id for estimate in estimates]
        if len(ids) != len(set(ids)):
            raise ValueError("a cost estimate batch cannot repeat a ledger_id")

        async def insert(cursor: Any) -> tuple[LedgerRecord, ...]:
            records: list[LedgerRecord] = []
            for estimate in estimates:
                await cursor.execute(
                    f"""
                    INSERT INTO {COST_LEDGER_TABLE} (
                        ledger_id, installation_id, bout_id, session_id, round_id,
                        lane_id, resource_id, resource_type, resource_name, resource_arn,
                        cost_scope, provider, region, component, attribution_method,
                        configuration_fingerprint, unit, original_quantity,
                        original_unit_rate_usd, original_cost_usd, window_start, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (ledger_id) DO NOTHING
                    RETURNING {self._columns()}
                    """,
                    (
                        estimate.ledger_id,
                        estimate.installation_id,
                        estimate.bout_id,
                        estimate.session_id,
                        estimate.round_id,
                        estimate.lane_id,
                        estimate.resource_id,
                        estimate.resource_type,
                        estimate.resource_name,
                        estimate.resource_arn,
                        estimate.scope,
                        estimate.key.provider,
                        estimate.key.region,
                        estimate.key.component,
                        estimate.key.attribution_method,
                        estimate.key.configuration_fingerprint,
                        estimate.key.unit,
                        estimate.original_quantity,
                        estimate.original_unit_rate_usd,
                        estimate.original_cost_usd,
                        estimate.window_start,
                        estimate.created_at,
                    ),
                )
                row = await cursor.fetchone()
                if row is None:
                    await cursor.execute(
                        f"SELECT {self._columns()} FROM {COST_LEDGER_TABLE} "
                        "WHERE ledger_id = %s",
                        (estimate.ledger_id,),
                    )
                    row = await cursor.fetchone()
                assert row is not None
                record = self._row_to_record(row)
                if record.estimate != estimate:
                    raise EstimateConflictError(
                        f"ledger_id {estimate.ledger_id!r} already has an immutable estimate"
                    )
                records.append(record)
            return tuple(records)

        return await self._run(insert)

    async def close_bout(
        self,
        *,
        installation_id: str,
        bout_id: str,
        window_end: datetime,
        terminal_outcome: str,
    ) -> tuple[LedgerRecord, ...]:
        installation_id = _required_text(installation_id, "installation_id")
        bout_id = _required_text(bout_id, "bout_id")
        terminal_outcome = _required_text(terminal_outcome, "terminal_outcome")
        closed_at = _utc(window_end, "window_end")

        async def close(cursor: Any) -> tuple[LedgerRecord, ...]:
            await cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"{installation_id}:{bout_id}",),
            )
            await cursor.execute(
                f"SELECT {self._columns()} FROM {COST_LEDGER_TABLE} "
                "WHERE installation_id = %s AND bout_id = %s ORDER BY ledger_id FOR UPDATE",
                (installation_id, bout_id),
            )
            rows = await cursor.fetchall()
            if not rows:
                raise KeyError(f"unknown cost bout {bout_id!r}")
            records = tuple(self._row_to_record(row) for row in rows)
            for record in records:
                if closed_at <= record.estimate.window_start:
                    raise ValueError("window_end must be after window_start")
                if record.window_end is not None and (
                    record.window_end != closed_at
                    or record.terminal_outcome != terminal_outcome
                ):
                    raise BoutWindowConflictError(
                        f"cost bout {bout_id!r} already has different terminal evidence"
                    )
            if all(record.window_end is not None for record in records):
                return records
            await cursor.execute(
                f"""
                UPDATE {COST_LEDGER_TABLE}
                SET window_end = %s, terminal_outcome = %s,
                    updated_at = clock_timestamp()
                WHERE installation_id = %s AND bout_id = %s AND window_end IS NULL
                """,
                (closed_at, terminal_outcome, installation_id, bout_id),
            )
            await cursor.execute(
                f"SELECT {self._columns()} FROM {COST_LEDGER_TABLE} "
                "WHERE installation_id = %s AND bout_id = %s ORDER BY ledger_id",
                (installation_id, bout_id),
            )
            return tuple(self._row_to_record(row) for row in await cursor.fetchall())

        return await self._run(close)

    async def reconcile_window(
        self, reconciliation: FullWindowReconciliation
    ) -> tuple[LedgerRecord, ...]:
        async def reconcile(cursor: Any) -> tuple[LedgerRecord, ...]:
            await cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (reconciliation.window_id,),
            )
            await cursor.execute(
                f"""
                SELECT revision, window_start, window_end, provider, region, provider_watermark
                FROM {RECONCILIATION_TABLE}
                WHERE window_id = %s
                ORDER BY revision DESC LIMIT 1
                """,
                (reconciliation.window_id,),
            )
            prior = await cursor.fetchone()
            revision = 1
            if prior is not None:
                revision = int(prior[0]) + 1
                if prior[1] != reconciliation.window_start or prior[2] != reconciliation.window_end:
                    raise ValueError("window_id cannot be reused for different time bounds")
                if prior[3] != reconciliation.provider or prior[4] != reconciliation.region:
                    raise ValueError("window_id cannot be reused for a different provider/region")
                if prior[5] > reconciliation.provider_watermark:
                    raise ValueError("a correction cannot regress the provider watermark")
                await cursor.execute(
                    f"""
                    SELECT ledger_id FROM {RECONCILIATION_TABLE}
                    WHERE window_id = %s AND revision = %s
                    """,
                    (reconciliation.window_id, prior[0]),
                )
                prior_ids = {str(row[0]) for row in await cursor.fetchall()}
                current_ids = {item.ledger_id for item in reconciliation.items}
                if not prior_ids.issubset(current_ids):
                    raise IncompleteWindowError(
                        "a correction must include every line from the prior full-window revision"
                    )

            records: list[LedgerRecord] = []
            affected_keys: set[CalibrationKey] = set()
            for item in reconciliation.items:
                await cursor.execute(
                    f"SELECT {self._columns()} FROM {COST_LEDGER_TABLE} "
                    "WHERE ledger_id = %s FOR UPDATE",
                    (item.ledger_id,),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise KeyError(f"unknown ledger_id {item.ledger_id!r}")
                current = self._row_to_record(row)
                if (
                    current.estimate.key.provider != reconciliation.provider
                    or current.estimate.key.region != reconciliation.region
                ):
                    raise ValueError("reconciliation provider/region does not match its estimate")
                variance = _variance(item.cost_usd, current.estimate.original_cost_usd)
                await cursor.execute(
                    f"""
                    INSERT INTO {RECONCILIATION_TABLE} (
                        window_id, revision, ledger_id, provider, region, window_start,
                        window_end, provider_watermark, observed_at, clean, posted_quantity,
                        posted_unit_rate_usd, posted_cost_usd, variance_usd
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        reconciliation.window_id,
                        revision,
                        item.ledger_id,
                        reconciliation.provider,
                        reconciliation.region,
                        reconciliation.window_start,
                        reconciliation.window_end,
                        reconciliation.provider_watermark,
                        reconciliation.observed_at,
                        reconciliation.clean,
                        item.quantity,
                        item.unit_rate_usd,
                        item.cost_usd,
                        variance,
                    ),
                )
                await cursor.execute(
                    f"""
                    UPDATE {COST_LEDGER_TABLE} SET
                        posted_quantity = %s,
                        posted_unit_rate_usd = %s,
                        posted_cost_usd = %s,
                        variance_usd = %s,
                        reconciliation_revision = %s,
                        provider_watermark = %s,
                        reconciliation_observed_at = %s,
                        clean = %s,
                        updated_at = clock_timestamp()
                    WHERE ledger_id = %s
                    RETURNING {self._columns()}
                    """,
                    (
                        item.quantity,
                        item.unit_rate_usd,
                        item.cost_usd,
                        variance,
                        revision,
                        reconciliation.provider_watermark,
                        reconciliation.observed_at,
                        reconciliation.clean,
                        item.ledger_id,
                    ),
                )
                updated = await cursor.fetchone()
                assert updated is not None
                records.append(self._row_to_record(updated))
                affected_keys.add(current.estimate.key)

            for key in affected_keys:
                values = (
                    key.provider,
                    key.region,
                    key.component,
                    key.attribution_method,
                    key.configuration_fingerprint,
                    key.unit,
                )
                await cursor.execute(
                    f"""
                    DELETE FROM {CALIBRATION_TABLE}
                    WHERE provider = %s AND region = %s AND component = %s
                      AND attribution_method = %s AND configuration_fingerprint = %s
                      AND unit = %s
                    """,
                    values,
                )
                await cursor.execute(
                    f"""
                    INSERT INTO {CALIBRATION_TABLE} (
                        provider, region, component, attribution_method,
                        configuration_fingerprint, unit, sample_count, quantity_sum,
                        cost_sum_usd, calibrated_unit_rate_usd, provider_watermark, updated_at
                    )
                    SELECT provider, region, component, attribution_method,
                           configuration_fingerprint, unit, COUNT(*), SUM(posted_quantity),
                           SUM(posted_cost_usd),
                           CAST(SUM(posted_cost_usd) / NULLIF(SUM(posted_quantity), 0)
                                AS NUMERIC(38,18)),
                           MAX(provider_watermark), clock_timestamp()
                    FROM {COST_LEDGER_TABLE}
                    WHERE provider = %s AND region = %s AND component = %s
                      AND attribution_method = %s AND configuration_fingerprint = %s
                      AND unit = %s AND clean IS TRUE
                      AND posted_quantity IS NOT NULL AND posted_quantity > 0
                      AND posted_cost_usd IS NOT NULL AND provider_watermark IS NOT NULL
                    GROUP BY provider, region, component, attribution_method,
                             configuration_fingerprint, unit
                    HAVING COUNT(*) >= 3
                    """,
                    values,
                )
            return tuple(records)

        return await self._run(reconcile)

    async def get(self, ledger_id: str) -> LedgerRecord | None:
        async def select(cursor: Any) -> LedgerRecord | None:
            await cursor.execute(
                f"SELECT {self._columns()} FROM {COST_LEDGER_TABLE} WHERE ledger_id = %s",
                (ledger_id,),
            )
            row = await cursor.fetchone()
            return self._row_to_record(row) if row is not None else None

        return await self._run(select)

    async def snapshots(self, window_id: str) -> tuple[ReconciliationSnapshot, ...]:
        async def select(cursor: Any) -> tuple[ReconciliationSnapshot, ...]:
            await cursor.execute(
                f"""
                SELECT revision, window_start, window_end, provider_watermark, observed_at,
                       clean, ledger_id, posted_quantity, posted_unit_rate_usd,
                       posted_cost_usd, variance_usd
                FROM {RECONCILIATION_TABLE}
                WHERE window_id = %s ORDER BY revision, ledger_id
                """,
                (window_id,),
            )
            return tuple(
                ReconciliationSnapshot(
                    window_id=window_id,
                    revision=int(row[0]),
                    window_start=row[1],
                    window_end=row[2],
                    provider_watermark=row[3],
                    observed_at=row[4],
                    clean=bool(row[5]),
                    item=PostedCost(
                        ledger_id=str(row[6]),
                        quantity=row[7],
                        unit_rate_usd=row[8],
                        cost_usd=row[9],
                    ),
                    variance_usd=row[10],
                )
                for row in await cursor.fetchall()
            )

        return await self._run(select)

    async def calibration_for(self, key: CalibrationKey) -> CalibrationProfile | None:
        async def select(cursor: Any) -> CalibrationProfile | None:
            await cursor.execute(
                f"""
                SELECT sample_count, quantity_sum, cost_sum_usd,
                       calibrated_unit_rate_usd, provider_watermark
                FROM {CALIBRATION_TABLE}
                WHERE provider = %s AND region = %s AND component = %s
                  AND attribution_method = %s AND configuration_fingerprint = %s
                  AND unit = %s
                """,
                (
                    key.provider,
                    key.region,
                    key.component,
                    key.attribution_method,
                    key.configuration_fingerprint,
                    key.unit,
                ),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return CalibrationProfile(
                key=key,
                sample_count=int(row[0]),
                quantity_sum=row[1],
                cost_sum_usd=row[2],
                calibrated_unit_rate_usd=row[3],
                provider_watermark=row[4],
            )

        return await self._run(select)


def build_cost_ledger_store() -> CostLedgerStore:
    endpoint_name = os.environ.get("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", "").strip()
    if not endpoint_name:
        if os.environ.get("DATABRICKS_APP_NAME"):
            raise RuntimeError(
                "Databricks Apps require ANTI_DEMO_COORDINATION_ENDPOINT_NAME; "
                "process-local cost evidence is not allowed"
            )
        return InMemoryCostLedgerStore()
    measured_endpoint = os.environ.get("LAKEBASE_ENDPOINT_NAME", "").strip()
    if measured_endpoint and endpoint_name == measured_endpoint:
        raise RuntimeError(
            "The coordination endpoint must be separate from the measured Lakebase endpoint"
        )
    return LakebaseCostLedgerStore(
        endpoint_name=endpoint_name,
        database=os.environ.get("ANTI_DEMO_COORDINATION_DATABASE", "anti_demo"),
        profile=os.environ.get("DATABRICKS_PROFILE", ""),
        host=os.environ.get("ANTI_DEMO_COORDINATION_HOST", ""),
        user=os.environ.get("ANTI_DEMO_COORDINATION_USER", ""),
        port=int(os.environ.get("ANTI_DEMO_COORDINATION_PORT", "5432")),
    )
