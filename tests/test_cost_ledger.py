from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import psycopg
import pytest

from server.coordination import (
    CoordinationObjectsMissingError,
    is_transient_coordination_error,
)
from server.cost_ledger import (
    CalibrationKey,
    CostEstimate,
    EstimateConflictError,
    FullWindowReconciliation,
    IncompleteWindowError,
    InMemoryCostLedgerStore,
    LakebaseCostLedgerStore,
    PostedCost,
)
from server.manager import RunManager
from server.manifest import DemoManifest
from server.models import CompetitorId, Corner, RoundId, SessionCreate

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
KEY = CalibrationKey(
    provider="databricks",
    region="us-west-2",
    component="Lakebase compute",
    attribution_method="exact_endpoint_interval",
    configuration_fingerprint="autoscaling:0.5-2cu:promo-v1",
    unit="DBU",
)
INSTALLATION_ID = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"


def estimate(
    ledger_id: str,
    *,
    key: CalibrationKey = KEY,
    original_cost: str = "0.010000000000000000000000",
) -> CostEstimate:
    return CostEstimate(
        ledger_id=ledger_id,
        installation_id="install-a",
        bout_id=f"bout-{ledger_id}",
        session_id=f"session-{ledger_id}",
        round_id="wake_idle_app",
        lane_id="lakebase",
        resource_id=f"projects/round-1/endpoints/{ledger_id}",
        resource_type="lakebase_endpoint",
        resource_name=f"round-1-{ledger_id}",
        resource_arn=None,
        scope="bout_estimate",
        key=key,
        original_quantity=Decimal("0.100000000000000000"),
        original_unit_rate_usd=Decimal("0.260000000000000000"),
        original_cost_usd=Decimal(original_cost),
        window_start=NOW,
        created_at=NOW,
    )


def window(
    window_id: str,
    *items: PostedCost,
    clean: bool = True,
    watermark: datetime = NOW + timedelta(hours=3),
) -> FullWindowReconciliation:
    return FullWindowReconciliation(
        window_id=window_id,
        provider="databricks",
        region="us-west-2",
        window_start=NOW,
        window_end=NOW + timedelta(minutes=5),
        provider_watermark=watermark,
        observed_at=watermark + timedelta(minutes=1),
        clean=clean,
        items=tuple(items),
    )


def posted(ledger_id: str, cost: str) -> PostedCost:
    return PostedCost(
        ledger_id=ledger_id,
        quantity=Decimal("0.100000000000000000"),
        unit_rate_usd=Decimal("0.260000000000000000"),
        cost_usd=Decimal(cost),
    )


async def test_original_estimate_is_decimal_exact_and_immutable() -> None:
    store = InMemoryCostLedgerStore()
    original = estimate("line-1")

    assert (await store.record_estimate(original)).estimate == original
    assert (await store.record_estimate(original)).estimate == original

    changed = estimate("line-1", original_cost="0.020000000000000000000000")
    with pytest.raises(EstimateConflictError, match="immutable estimate"):
        await store.record_estimate(changed)
    with pytest.raises(TypeError, match="must be Decimal"):
        PostedCost(ledger_id="line-1", quantity=None, unit_rate_usd=None, cost_usd=0.01)  # type: ignore[arg-type]


async def test_bout_window_closes_once_with_half_open_utc_evidence() -> None:
    store = InMemoryCostLedgerStore()
    await store.record_estimates((estimate("line-1"), estimate("line-2")))
    ended_at = NOW + timedelta(seconds=9)

    closed = await store.close_bout(
        installation_id="install-a",
        bout_id="bout-line-1",
        window_end=ended_at,
        terminal_outcome="towelled",
    )
    repeated = await store.close_bout(
        installation_id="install-a",
        bout_id="bout-line-1",
        window_end=ended_at,
        terminal_outcome="towelled",
    )

    assert len(closed) == len(repeated) == 1
    assert closed[0].window_end == ended_at
    assert closed[0].terminal_outcome == "towelled"


async def test_v7_manager_uses_a_new_immutable_id_for_each_cost_bout() -> None:
    store = InMemoryCostLedgerStore()
    branch = "projects/install-r1/branches/production"
    environment = SimpleNamespace(
        lakebase=SimpleNamespace(
            project_id="install-r1",
            project_uid="project-uid-r1",
            branch_name=branch,
            branch_uid="branch-uid-r1",
            endpoint_name=f"{branch}/endpoints/primary",
            endpoint_uid="endpoint-uid-r1",
        ),
        aurora=SimpleNamespace(
            cluster_id="anti-demo-r1-aurora",
            cluster_resource_id="cluster-resource-r1",
            writer_instance_id="anti-demo-r1-writer",
            secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-r1",
        ),
        rds=SimpleNamespace(
            instance_id="anti-demo-r1-rds",
            resource_id="db-resource-r1",
            secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:rds-r1",
        ),
    )
    manifest = DemoManifest.model_construct(
        manifest_version=7,
        installation_id=INSTALLATION_ID,
        aws=SimpleNamespace(account_id="123456789012", region="us-west-2"),
        round_environments={RoundId.WAKE_IDLE_APP: environment},
    )
    manager = RunManager(
        round_isolation=True,
        installation_id=INSTALLATION_ID,
        cost_ledger_store=store,
        cost_manifest=manifest,
    )
    session = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    record = manager._records[session.id]

    first = await manager._open_cost_bout(record, kind="run", started_at=NOW)
    assert first is not None
    await manager._close_cost_bout(
        record,
        bout_id=first,
        ended_at=NOW + timedelta(seconds=5),
        outcome="verified",
    )
    second = await manager._open_cost_bout(
        record,
        kind="redo",
        started_at=NOW + timedelta(seconds=6),
    )

    assert second is not None and second != first
    assert record.snapshot.cost_receipt is not None
    assert record.snapshot.cost_receipt.reconciliation_status == "attribution_ambiguous"


async def test_corrections_append_full_revisions_without_changing_estimate() -> None:
    store = InMemoryCostLedgerStore()
    await store.record_estimate(estimate("line-1"))
    await store.record_estimate(estimate("line-2"))

    await store.reconcile_window(
        window("usage-2026-08-20", posted("line-1", "0.011000000000000000000000"))
    )
    corrected = await store.reconcile_window(
        window(
            "usage-2026-08-20",
            posted("line-1", "0.012000000000000000000000"),
            posted("line-2", "0.013000000000000000000000"),
            watermark=NOW + timedelta(hours=6),
        )
    )

    line_one = corrected[0]
    assert line_one.estimate.original_cost_usd == Decimal("0.010000000000000000000000")
    assert line_one.posted_cost_usd == Decimal("0.012000000000000000000000")
    assert line_one.variance_usd == Decimal("0.002000000000000000000000")
    assert line_one.reconciliation_revision == 2
    assert [snapshot.revision for snapshot in await store.snapshots("usage-2026-08-20")] == [
        1,
        2,
        2,
    ]

    with pytest.raises(IncompleteWindowError, match="every line"):
        await store.reconcile_window(
            window(
                "usage-2026-08-20",
                posted("line-2", "0.014000000000000000000000"),
                watermark=NOW + timedelta(hours=7),
            )
        )


async def test_calibration_requires_three_clean_exact_configuration_samples() -> None:
    store = InMemoryCostLedgerStore()
    for number in range(1, 4):
        ledger_id = f"clean-{number}"
        await store.record_estimate(estimate(ledger_id))
        await store.reconcile_window(
            window(
                f"window-{number}",
                posted(ledger_id, "0.026000000000000000000000"),
            )
        )
        profile = await store.calibration_for(KEY)
        if number < 3:
            assert profile is None

    profile = await store.calibration_for(KEY)
    assert profile is not None
    assert profile.sample_count == 3
    assert profile.calibrated_unit_rate_usd == Decimal("0.260000000000000000")

    different_key = CalibrationKey(
        provider=KEY.provider,
        region=KEY.region,
        component=KEY.component,
        attribution_method=KEY.attribution_method,
        configuration_fingerprint="autoscaling:0.5-4cu:promo-v1",
        unit=KEY.unit,
    )
    await store.record_estimate(estimate("different-config", key=different_key))
    await store.reconcile_window(
        window(
            "different-config-window",
            posted("different-config", "0.026000000000000000000000"),
        )
    )
    await store.record_estimate(estimate("dirty"))
    await store.reconcile_window(
        window(
            "dirty-window",
            posted("dirty", "9.000000000000000000000000"),
            clean=False,
        )
    )

    unchanged = await store.calibration_for(KEY)
    assert unchanged is not None
    assert unchanged.sample_count == 3
    assert await store.calibration_for(different_key) is None


# ---------------------------------------------------------------------------
# Startup on a schema this identity consumes but does not own
#
# The deployed app has no DDL on database `anti_demo`, deliberately. These pin
# the only two answers that are allowed: present-and-refused starts, absent-and-
# refused dies. The middle option -- shrugging at InsufficientPrivilege and
# handing back a store nothing has verified -- is the failure this project has
# now recorded five times, and it would be invisible until a cost disclosure
# rendered off an empty ledger.
# ---------------------------------------------------------------------------

COST_TABLES = ("cost_ledger", "cost_reconciliation_snapshot", "cost_calibration_profile")


class _CatalogCursor:
    """A cursor that answers pg_catalog and refuses every DDL statement.

    Modelled on the live failure rather than on a mock: Postgres checks the ACL
    before `IF NOT EXISTS`, so a `CREATE` is refused whether or not it would
    have done anything, while reads of `pg_catalog` are open to every role.
    """

    def __init__(self, *, tables: tuple[str, ...], guard: bool, schema: bool = True) -> None:
        self._tables = tables
        self._guard = guard
        self._schema = schema
        self.statements: list[str] = []
        self._pending: list[tuple] = []

    async def __aenter__(self) -> _CatalogCursor:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    @property
    def ddl(self) -> list[str]:
        return [
            statement
            for statement in self.statements
            if statement.upper().startswith(("CREATE", "ALTER", "DROP", "DO $$"))
        ]

    async def execute(self, sql: str, params: tuple = ()) -> None:
        statement = " ".join(sql.split())
        self.statements.append(statement)
        upper = statement.upper()
        if upper.startswith(("CREATE", "ALTER", "DROP", "DO $$")):
            raise psycopg.errors.InsufficientPrivilege("permission denied for database anti_demo")
        if "pg_catalog.pg_namespace WHERE nspname" in statement:
            self._pending = [("anti_demo_coordination",)] if self._schema else []
        elif "pg_catalog.pg_trigger" in statement:
            self._pending = [(1,)] if self._guard else []
        elif "pg_catalog.pg_class" in statement:
            wanted = set(params[1]) if len(params) > 1 else set()
            self._pending = [(name,) for name in self._tables if name in wanted]
        else:  # pragma: no cover - the stores under test issue nothing else
            raise AssertionError(f"unexpected statement: {statement}")

    async def fetchone(self) -> tuple | None:
        return self._pending[0] if self._pending else None

    async def fetchall(self) -> list[tuple]:
        return list(self._pending)


class _CatalogConnection:
    def __init__(self, cursor: _CatalogCursor) -> None:
        self._cursor = cursor

    async def __aenter__(self) -> _CatalogConnection:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def cursor(self) -> _CatalogCursor:
        return self._cursor


def _lakebase_cost_store(cursor: _CatalogCursor) -> LakebaseCostLedgerStore:
    async def connect(**_kwargs: object) -> _CatalogConnection:
        return _CatalogConnection(cursor)

    return LakebaseCostLedgerStore(
        endpoint_name="coordination-endpoint",
        database="anti_demo",
        host="coordination.example",
        user="app-service-principal",
        connector=connect,
        workspace_client=SimpleNamespace(
            postgres=SimpleNamespace(
                generate_database_credential=lambda _name: SimpleNamespace(token="tok")
            )
        ),
    )


async def test_a_consumer_starts_on_a_ledger_it_can_see_but_may_not_create() -> None:
    cursor = _CatalogCursor(tables=COST_TABLES, guard=True)
    await _lakebase_cost_store(cursor).initialize()
    # The point is not that it survived. It is that it looked, found all four
    # objects, and therefore never issued the statement that would be refused.
    assert cursor.ddl == []
    assert any("pg_catalog.pg_class" in statement for statement in cursor.statements)
    assert any("pg_catalog.pg_trigger" in statement for statement in cursor.statements)


@pytest.mark.parametrize(
    ("present", "guard", "schema", "expected"),
    [
        (COST_TABLES, False, True, "protect_cost_estimate_update"),
        (COST_TABLES[:2], True, True, "anti_demo_coordination.cost_calibration_profile"),
        ((), True, True, "anti_demo_coordination.cost_ledger"),
        ((), False, False, "schema anti_demo_coordination"),
    ],
)
async def test_an_absent_cost_ledger_is_fatal_rather_than_tolerated(
    present: tuple[str, ...],
    guard: bool,
    schema: bool,
    expected: str,
) -> None:
    """Every incomplete shape raises, and names the object an operator must fix.

    Including the guard trigger: tables without it is not a cosmetic gap, it is
    the ledger silently ceasing to be evidence, because original estimates stop
    being immutable.
    """
    cursor = _CatalogCursor(tables=present, guard=guard, schema=schema)
    with pytest.raises(CoordinationObjectsMissingError) as missing:
        await _lakebase_cost_store(cursor).initialize()

    message = str(missing.value)
    assert expected in message
    assert "'anti_demo'" in message
    assert "docs/DEPLOY.md" in message
    # The refusal it was raised from is preserved, so the log still shows the
    # Postgres error underneath the interpretation of it.
    assert isinstance(missing.value.__cause__, psycopg.errors.InsufficientPrivilege)
    # And it is on the non-transient side, so nothing retries a denial.
    assert is_transient_coordination_error(missing.value) is False
