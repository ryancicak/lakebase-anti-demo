from __future__ import annotations

import asyncio
import getpass
import json
import os
import re
import socket
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Branch,
    BranchSpec,
    CdfConfig,
    Endpoint,
    EndpointSpec,
    EndpointType,
)
from psycopg import sql

from .capacity import LAKEBASE_SUSPEND_SECONDS
from .manifest import DemoManifest, Round6Resources
from .model_score_live import WorkspaceStatementRunner
from .process_registry import state_dir_from_environ
from .round6_contract import round6_contract_sha256

# Round 6 scales to zero on the same vendor-minimum window as every other round.
ROUND6_SUSPEND_WINDOW = f"{LAKEBASE_SUSPEND_SECONDS}s"

ROUND6_DATABASE_ID = "databricks-postgres"
ROUND6_POSTGRES_DATABASE = "databricks_postgres"
ROUND6_SOURCE_TABLE = "live_orders"
ROUND6_BASELINE_ORDER_ID = "00000000-0000-4000-8000-000000000006"
ROUND6_BASELINE_NONCE = "round6-baseline"
ROUND6_BASELINE_SKU = "RED-GLOVE"
ROUND6_BASELINE_STORE = "CHICAGO"
ROUND6_BASELINE_QUANTITY = 1
ROUND6_BASELINE_TOTAL_CENTS = 8450
ROUND6_BASELINE_STATUS = "baseline"
ROUND6_OWNER_PROPERTY = "anti_demo_run_id"


def round6_names(manifest: DemoManifest) -> dict[str, str]:
    suffix = re.sub(r"[^a-z0-9]+", "_", manifest.run_id.casefold()).strip("_")
    if not suffix or len(suffix) > 43:
        raise RuntimeError("Run ID cannot be converted to safe Round 6 identifiers")
    per_round = getattr(manifest, "round_environments", None) is not None
    source_endpoint = (
        manifest.round_lakebase(6).endpoint_name if per_round else manifest.databricks.endpoint_name
    )
    project = source_endpoint.split("/branches/", 1)[0]
    production_branch = f"{project}/branches/production"
    branch_id = "production" if per_round else f"round6-{suffix.replace('_', '-')}"
    branch = production_branch if per_round else f"{project}/branches/{branch_id}"
    endpoint = source_endpoint if per_round else f"{branch}/endpoints/primary"
    database = f"{branch}/databases/{ROUND6_DATABASE_ID}"
    schema = f"anti_demo_r6_{suffix}"
    config_id = f"anti_demo_r6_{suffix}"
    return {
        "database_resource_name": database,
        "project_name": project,
        "production_branch_name": production_branch,
        "branch_name": branch,
        "branch_id": branch_id,
        "endpoint_name": endpoint,
        "endpoint_id": "primary",
        "source_schema": schema,
        "source_table": ROUND6_SOURCE_TABLE,
        "destination_schema": schema,
        "uses_production_endpoint": "true" if per_round else "false",
        "cdf_config_id": config_id,
        "cdf_config_name": f"{database}/cdf-configs/{config_id}",
        "cdf_status_id": ROUND6_SOURCE_TABLE,
        "cdf_status_name": (
            f"{database}/cdf-configs/{config_id}/cdf-statuses/{ROUND6_SOURCE_TABLE}"
        ),
    }


class Round6ContractMismatch(RuntimeError):
    """A Round 6 contract assertion failed.

    Carries the individual fields that did not match so an inventory can name
    them, instead of only reporting that something somewhere changed.
    """

    def __init__(
        self,
        message: str,
        findings: tuple[tuple[str, str, str], ...] = (),
    ) -> None:
        super().__init__(message)
        self.findings = findings


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "as_dict"):
        payload = value.as_dict()
        if isinstance(payload, Mapping):
            return payload
    return {}


def _enum(value: object) -> str:
    return str(getattr(value, "value", value) or "")


def _timestamp(value: object, label: str) -> datetime:
    if hasattr(value, "ToDatetime"):
        value = value.ToDatetime(tzinfo=UTC)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RuntimeError(f"Round 6 {label} is missing or not timezone-aware")
    return value.astimezone(UTC)


def _workspace(manifest: DemoManifest) -> WorkspaceClient:
    return WorkspaceClient(profile=manifest.databricks.profile)


def _validate_branch(
    payload: object, names: Mapping[str, str]
) -> tuple[Mapping[str, Any], datetime]:
    branch = _mapping(payload)
    spec = _mapping(branch.get("spec"))
    status = _mapping(branch.get("status"))
    source_branches = tuple(
        str(value) for value in (spec.get("source_branch"), status.get("source_branch")) if value
    )
    uses_production = names.get("uses_production_endpoint") == "true"
    source_is_exact = (
        not source_branches
        if uses_production
        else bool(source_branches)
        and all(
            source_branch == names["production_branch_name"] for source_branch in source_branches
        )
    )
    default_is_exact = (
        status.get("default") in (None, True)
        if uses_production
        else status.get("default") is not True
    )
    if (
        branch.get("name") != names["branch_name"]
        or branch.get("branch_id") != names["branch_id"]
        or branch.get("parent") not in {None, names["project_name"]}
        or not branch.get("uid")
        or not source_is_exact
        or status.get("branch_id") not in {None, names["branch_id"]}
        or not default_is_exact
    ):
        raise RuntimeError("Round 6 branch identity or production source changed")
    return branch, _timestamp(branch.get("create_time"), "branch create_time")


def _endpoint_contract_findings(
    endpoint: Mapping[str, Any], names: Mapping[str, str]
) -> tuple[tuple[str, str, str], ...]:
    """Every endpoint contract field that does not match, as (field, expected, found).

    Identity and provisioning convention are asserted as one contract: any
    non-empty result is a refusal. Enumerating the fields only changes what a
    refusal can say about itself, never which inputs are accepted.
    """

    spec = _mapping(endpoint.get("spec"))
    status = _mapping(endpoint.get("status"))
    endpoint_types = tuple(
        _enum(value) for value in (spec.get("endpoint_type"), status.get("endpoint_type")) if value
    )
    # Round 6 once ran always-on, on the theory that its change feed needed a
    # continuously live connection. A live experiment settled it the other way:
    # a CDF-replicating endpoint reporting CDF_STATE_STREAMING held IDLE for 24x
    # its suspend window, so the feed survives suspension and the endpoint is
    # required to scale to zero like every other round's.
    suspend_windows = tuple(
        str(value)
        for value in (
            spec.get("suspend_timeout_duration"),
            status.get("suspend_timeout_duration"),
        )
        if value
    )
    scales_to_zero = (
        spec.get("no_suspension") is not True
        and bool(suspend_windows)
        and all(window == ROUND6_SUSPEND_WINDOW for window in suspend_windows)
    )
    checks: tuple[tuple[str, bool, object, object], ...] = (
        (
            "name",
            endpoint.get("name") == names["endpoint_name"],
            names["endpoint_name"],
            endpoint.get("name"),
        ),
        (
            "endpoint_id",
            endpoint.get("endpoint_id") == names["endpoint_id"],
            names["endpoint_id"],
            endpoint.get("endpoint_id"),
        ),
        (
            "parent",
            endpoint.get("parent") in {None, names["branch_name"]},
            f"unset or {names['branch_name']}",
            endpoint.get("parent"),
        ),
        (
            "uid",
            bool(endpoint.get("uid")),
            "a non-empty server-assigned uid",
            endpoint.get("uid"),
        ),
        (
            "endpoint_type",
            bool(endpoint_types)
            and all(value == "ENDPOINT_TYPE_READ_WRITE" for value in endpoint_types),
            "ENDPOINT_TYPE_READ_WRITE on every returned copy",
            list(endpoint_types),
        ),
        (
            "spec.no_suspension/status.suspend_timeout_duration",
            scales_to_zero,
            f"suspension enabled with a {ROUND6_SUSPEND_WINDOW} window on every returned copy",
            f"spec.no_suspension={spec.get('no_suspension')!r}, "
            f"spec.suspend_timeout_duration={spec.get('suspend_timeout_duration')!r}, "
            f"status.suspend_timeout_duration={status.get('suspend_timeout_duration')!r}",
        ),
        (
            "status.endpoint_id",
            status.get("endpoint_id") in {None, names["endpoint_id"]},
            f"unset or {names['endpoint_id']}",
            status.get("endpoint_id"),
        ),
        (
            "status.disabled",
            status.get("disabled") is not True,
            "not True",
            status.get("disabled"),
        ),
    )
    return tuple(
        (field, str(expected), str(found)) for field, ok, expected, found in checks if not ok
    )


def _validate_endpoint(
    payload: object, names: Mapping[str, str]
) -> tuple[Mapping[str, Any], datetime]:
    endpoint = _mapping(payload)
    findings = _endpoint_contract_findings(endpoint, names)
    if findings:
        raise Round6ContractMismatch(
            "Round 6 endpoint identity or scale-to-zero contract changed", findings
        )
    return endpoint, _timestamp(endpoint.get("create_time"), "endpoint create_time")


def _get_or_create_branch_endpoint(
    workspace: Any, names: Mapping[str, str]
) -> tuple[Mapping[str, Any], datetime, Mapping[str, Any], datetime]:
    branches = [
        item
        for item in workspace.postgres.list_branches(parent=names["project_name"])
        if _mapping(item).get("branch_id") == names["branch_id"]
    ]
    if len(branches) > 1:
        raise RuntimeError("Round 6 branch identity is not unique")
    if not branches and names.get("uses_production_endpoint") == "true":
        raise RuntimeError("Round 6 production branch is missing")
    if not branches:
        workspace.postgres.create_branch(
            parent=names["project_name"],
            branch_id=names["branch_id"],
            branch=Branch(
                spec=BranchSpec(
                    source_branch=names["production_branch_name"],
                    no_expiry=True,
                )
            ),
        ).wait()
    branch, branch_created = _validate_branch(
        workspace.postgres.get_branch(names["branch_name"]), names
    )
    endpoints = [
        item
        for item in workspace.postgres.list_endpoints(parent=names["branch_name"])
        if _mapping(item).get("endpoint_id") == names["endpoint_id"]
    ]
    if len(endpoints) > 1:
        raise RuntimeError("Round 6 endpoint identity is not unique")
    if not endpoints and names.get("uses_production_endpoint") == "true":
        raise RuntimeError("Round 6 production endpoint is missing")
    if not endpoints:
        workspace.postgres.create_endpoint(
            parent=names["branch_name"],
            endpoint_id=names["endpoint_id"],
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    suspend_timeout_duration=ROUND6_SUSPEND_WINDOW,
                )
            ),
        ).wait()
    endpoint, endpoint_created = _validate_endpoint(
        workspace.postgres.get_endpoint(names["endpoint_name"]), names
    )
    return branch, branch_created, endpoint, endpoint_created


def _validate_database(workspace: Any, names: Mapping[str, str]) -> Mapping[str, Any]:
    database = _mapping(workspace.postgres.get_database(names["database_resource_name"]))
    status = _mapping(database.get("status"))
    if (
        database.get("name") != names["database_resource_name"]
        or database.get("database_id") != ROUND6_DATABASE_ID
        or status.get("database_id") != ROUND6_DATABASE_ID
        or status.get("postgres_database") != ROUND6_POSTGRES_DATABASE
    ):
        raise RuntimeError("Round 6 requires the existing databricks_postgres default SQL database")
    listed = [
        _mapping(item)
        for item in workspace.postgres.list_databases(
            parent=names["database_resource_name"].rsplit("/databases/", 1)[0]
        )
        if _mapping(item).get("database_id") == ROUND6_DATABASE_ID
    ]
    if len(listed) != 1 or listed[0].get("name") != names["database_resource_name"]:
        raise RuntimeError("Round 6 default SQL database identity is not unique")
    return database


def _validate_catalog(workspace: Any, catalog_name: str) -> Mapping[str, Any]:
    catalog = _mapping(workspace.catalogs.get(catalog_name))
    if catalog.get("name") != catalog_name or catalog.get("full_name") not in {
        None,
        catalog_name,
    }:
        raise RuntimeError("Round 6 destination catalog identity changed")
    if catalog.get("storage_root") or catalog.get("storage_location"):
        raise RuntimeError("Round 6 CDF catalog must not have catalog-level managed storage")
    return catalog


def _ensure_round6_app_role(
    manifest: DemoManifest,
    names: Mapping[str, str],
    app_client_id: str,
    *,
    timeout: float,
) -> None:
    # Lifecycle owns the validated Lakebase OAuth role API contract. Importing
    # here avoids a module cycle because lifecycle loads Round 6 lazily.
    from .lifecycle import _ensure_lakebase_app_roles

    _ensure_lakebase_app_roles(
        manifest,
        app_client_id,
        (names["branch_name"],),
        timeout=timeout,
    )


def _validate_schema(
    payload: object,
    *,
    catalog: str,
    schema: str,
    owner: str,
    run_id: str,
) -> Mapping[str, Any]:
    item = _mapping(payload)
    properties = _mapping(item.get("properties"))
    if (
        item.get("full_name") != f"{catalog}.{schema}"
        or item.get("catalog_name") != catalog
        or item.get("name") != schema
        or item.get("owner") != owner
        or item.get("created_by") != owner
        or not item.get("schema_id")
        or item.get("storage_root")
        or properties.get(ROUND6_OWNER_PROPERTY) != run_id
        or properties.get("managed_by") != "round6-lifecycle"
    ):
        raise RuntimeError("Round 6 destination schema ownership or identity changed")
    return item


async def _source_connection(workspace: Any, endpoint_name: str, user: str) -> Any:
    endpoint, credential = await asyncio.gather(
        asyncio.to_thread(workspace.postgres.get_endpoint, endpoint_name),
        asyncio.to_thread(workspace.postgres.generate_database_credential, endpoint_name),
    )
    endpoint_payload = _mapping(endpoint)
    host = str(_mapping(_mapping(endpoint_payload.get("status")).get("hosts")).get("host") or "")
    token = str(getattr(credential, "token", None) or _mapping(credential).get("token") or "")
    if not host or not token or not user:
        raise RuntimeError("Round 6 branch endpoint connection is unavailable")
    return await psycopg.AsyncConnection.connect(
        host=host,
        port=5432,
        dbname=ROUND6_POSTGRES_DATABASE,
        user=user,
        password=token,
        sslmode="require",
        application_name="lakebase-anti-demo-round6-setup",
        connect_timeout=30,
    )


async def ensure_round6_source(
    names: Mapping[str, str], *, workspace: Any, user: str, app_client_id: str
) -> int:
    connection = await _source_connection(workspace, names["endpoint_name"], user)
    async with connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT n.oid, pg_get_userbyid(n.nspowner), current_user "
                "FROM pg_namespace n WHERE n.nspname = %s",
                (names["source_schema"],),
            )
            schema_row = await cursor.fetchone()
            if schema_row is None:
                await cursor.execute(
                    sql.SQL("CREATE SCHEMA {} AUTHORIZATION CURRENT_USER").format(
                        sql.Identifier(names["source_schema"])
                    )
                )
            elif str(schema_row[1]) != str(schema_row[2]):
                raise RuntimeError("Round 6 source schema is not owned by the setup user")

            table = sql.SQL("{}.{}").format(
                sql.Identifier(names["source_schema"]),
                sql.Identifier(names["source_table"]),
            )
            await cursor.execute(
                "SELECT c.oid, c.relreplident, pg_get_userbyid(c.relowner), current_user "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r'",
                (names["source_schema"], names["source_table"]),
            )
            table_row = await cursor.fetchone()
            if table_row is None:
                await cursor.execute(
                    sql.SQL(
                        "CREATE TABLE {} ("
                        "order_id text PRIMARY KEY, sku text NOT NULL, store text NOT NULL, "
                        "quantity integer NOT NULL CHECK (quantity > 0), "
                        "total_cents integer NOT NULL CHECK (total_cents >= 0), "
                        "status text NOT NULL, "
                        "proof_nonce text NOT NULL UNIQUE)"
                    ).format(table)
                )
                await cursor.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(table))
            elif str(table_row[2]) != str(table_row[3]):
                raise RuntimeError("Round 6 source table is not owned by the setup user")
            elif str(table_row[1]) != "f":
                raise RuntimeError("Round 6 source table is not REPLICA IDENTITY FULL")

            await cursor.execute(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (names["source_schema"], names["source_table"]),
            )
            columns = [tuple(row) for row in await cursor.fetchall()]
            if columns != [
                ("order_id", "text", "NO"),
                ("sku", "text", "NO"),
                ("store", "text", "NO"),
                ("quantity", "integer", "NO"),
                ("total_cents", "integer", "NO"),
                ("status", "text", "NO"),
                ("proof_nonce", "text", "NO"),
            ]:
                raise RuntimeError("Round 6 source table columns are not exact")
            await cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(names["source_schema"]),
                    sql.Identifier(app_client_id),
                )
            )
            # DELETE belongs here with SELECT and INSERT: the round commits a
            # proof order through checkout and then settles by removing that
            # exact row again, so an app that can only insert leaves one row of
            # drift in the source table per bout and logs "Round 6 settlement
            # attempt n/4 failed ... InsufficientPrivilege: permission denied
            # for table live_orders" after a bout that otherwise verified. No
            # UPDATE: a proof row is written once and then withdrawn, never
            # amended, and the baseline row is never touched by the app at all.
            await cursor.execute(
                sql.SQL("GRANT SELECT, INSERT, DELETE ON TABLE {} TO {}").format(
                    table,
                    sql.Identifier(app_client_id),
                )
            )

            await cursor.execute(
                sql.SQL(
                    "SELECT order_id, sku, store, quantity, total_cents, status, "
                    "proof_nonce FROM {} "
                    "WHERE order_id = %s"
                ).format(table),
                (ROUND6_BASELINE_ORDER_ID,),
            )
            baseline = await cursor.fetchall()
            if not baseline:
                await cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (order_id, sku, store, quantity, total_cents, "
                        "status, proof_nonce) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    ).format(table),
                    (
                        ROUND6_BASELINE_ORDER_ID,
                        ROUND6_BASELINE_SKU,
                        ROUND6_BASELINE_STORE,
                        ROUND6_BASELINE_QUANTITY,
                        ROUND6_BASELINE_TOTAL_CENTS,
                        ROUND6_BASELINE_STATUS,
                        ROUND6_BASELINE_NONCE,
                    ),
                )
            elif len(baseline) != 1 or tuple(baseline[0]) != (
                ROUND6_BASELINE_ORDER_ID,
                ROUND6_BASELINE_SKU,
                ROUND6_BASELINE_STORE,
                ROUND6_BASELINE_QUANTITY,
                ROUND6_BASELINE_TOTAL_CENTS,
                ROUND6_BASELINE_STATUS,
                ROUND6_BASELINE_NONCE,
            ):
                raise RuntimeError("Round 6 source baseline is not exact")
            # Reset the dedicated proof table to its one-row baseline. No other
            # workload is permitted in this run-owned source schema.
            await cursor.execute(
                sql.SQL("DELETE FROM {} WHERE order_id <> %s").format(table),
                (ROUND6_BASELINE_ORDER_ID,),
            )
            await cursor.execute(
                sql.SQL(
                    "SELECT c.oid, c.relreplident, t.order_id::text, t.total_cents, "
                    "t.status, t.proof_nonce, t.sku, t.store, t.quantity FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "JOIN {} t ON true WHERE n.nspname = %s AND c.relname = %s"
                ).format(table),
                (names["source_schema"], names["source_table"]),
            )
            exact = await cursor.fetchall()
            if len(exact) != 1 or tuple(exact[0][1:]) != (
                "f",
                ROUND6_BASELINE_ORDER_ID,
                ROUND6_BASELINE_TOTAL_CENTS,
                ROUND6_BASELINE_STATUS,
                ROUND6_BASELINE_NONCE,
                ROUND6_BASELINE_SKU,
                ROUND6_BASELINE_STORE,
                ROUND6_BASELINE_QUANTITY,
            ):
                raise RuntimeError("Round 6 source baseline is not exact")
            oid = int(exact[0][0])
        await connection.commit()
    return oid


def _validate_cdf_config(
    payload: object,
    *,
    names: Mapping[str, str],
    catalog: str,
) -> tuple[Mapping[str, Any], datetime]:
    item = _mapping(payload)
    if (
        item.get("name") != names["cdf_config_name"]
        or item.get("cdf_config_id") != names["cdf_config_id"]
        or item.get("catalog") != catalog
        or item.get("schema") != names["destination_schema"]
        or item.get("postgres_schema") != names["source_schema"]
    ):
        raise RuntimeError("Round 6 CDF configuration identity or immutable spec changed")
    return item, _timestamp(item.get("create_time"), "CDF config create_time")


def _get_or_create_cdf_config(
    workspace: Any,
    *,
    names: Mapping[str, str],
    catalog: str,
) -> tuple[Mapping[str, Any], datetime]:
    matches = [
        item
        for item in workspace.postgres.list_cdf_configs(parent=names["database_resource_name"])
        if _mapping(item).get("cdf_config_id") == names["cdf_config_id"]
    ]
    if len(matches) > 1:
        raise RuntimeError("Round 6 CDF configuration identity is not unique")
    if matches:
        listed, listed_created = _validate_cdf_config(matches[0], names=names, catalog=catalog)
        fetched = workspace.postgres.get_cdf_config(names["cdf_config_name"])
        fetched_item, created = _validate_cdf_config(fetched, names=names, catalog=catalog)
        if (
            listed.get("name") != fetched_item.get("name")
            or listed.get("cdf_config_id") != fetched_item.get("cdf_config_id")
            or listed_created != created
        ):
            raise RuntimeError("Round 6 CDF config differs between list and get")
        return fetched_item, created
    operation = workspace.postgres.create_cdf_config(
        parent=names["database_resource_name"],
        cdf_config_id=names["cdf_config_id"],
        cdf_config=CdfConfig(
            catalog=catalog,
            schema=names["destination_schema"],
            postgres_schema=names["source_schema"],
        ),
    )
    operation.wait()
    return _validate_cdf_config(
        workspace.postgres.get_cdf_config(names["cdf_config_name"]),
        names=names,
        catalog=catalog,
    )


def _quoted_uc_table(full_name: str) -> str:
    parts = full_name.split(".")
    if len(parts) != 3 or any(not part for part in parts):
        raise RuntimeError("Round 6 CDF status returned an invalid UC table name")
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


async def _read_uc_baseline(workspace: Any, warehouse_id: str, table: str) -> list[dict[str, Any]]:
    return await WorkspaceStatementRunner(workspace, warehouse_id).execute(
        "SELECT order_id, sku, store, quantity, total_cents, status, proof_nonce, "
        "_pg_change_type "
        f"FROM {_quoted_uc_table(table)} "
        f"WHERE order_id = '{ROUND6_BASELINE_ORDER_ID}' "
        f"AND proof_nonce = '{ROUND6_BASELINE_NONCE}'"
    )


def _wait_for_cdf_baseline(
    workspace: Any,
    *,
    names: Mapping[str, str],
    catalog: str,
    warehouse_id: str,
    timeout: float,
) -> tuple[Mapping[str, Any], datetime, Mapping[str, Any]]:
    deadline = time.monotonic() + timeout
    final = "CDF status has not appeared"
    while True:
        statuses = [
            _mapping(item)
            for item in workspace.postgres.list_cdf_statuses(parent=names["cdf_config_name"])
            if _mapping(item).get("postgres_table") == names["source_table"]
        ]
        if len(statuses) == 1:
            status = statuses[0]
            full_name = str(status.get("uc_table") or "")
            parts = full_name.split(".")
            state = _enum(status.get("state"))
            final = f"state={state or 'UNKNOWN'}, uc_table={full_name or 'MISSING'}"
            expected_name = names["cdf_status_name"]
            if (
                status.get("name") == expected_name
                and state == "CDF_STATE_STREAMING"
                and not status.get("status_detail")
                and len(parts) == 3
                and parts[:2] == [catalog, names["destination_schema"]]
                and status.get("committed_lsn")
            ):
                fetched = _mapping(workspace.postgres.get_cdf_status(expected_name))
                if (
                    fetched.get("name") != expected_name
                    or fetched.get("postgres_table") != names["source_table"]
                    or fetched.get("uc_table") != full_name
                    or _enum(fetched.get("state")) != "CDF_STATE_STREAMING"
                    or fetched.get("status_detail")
                    or _timestamp(fetched.get("create_time"), "CDF status create_time")
                    != _timestamp(status.get("create_time"), "CDF status create_time")
                ):
                    raise RuntimeError("Round 6 CDF status differs between list and get")
                status = fetched
                rows = asyncio.run(_read_uc_baseline(workspace, warehouse_id, full_name))
                if len(rows) == 1 and (
                    str(rows[0].get("order_id") or "") == ROUND6_BASELINE_ORDER_ID
                    and str(rows[0].get("sku") or "") == ROUND6_BASELINE_SKU
                    and str(rows[0].get("store") or "") == ROUND6_BASELINE_STORE
                    and int(str(rows[0].get("quantity") or "-1")) == ROUND6_BASELINE_QUANTITY
                    and int(str(rows[0].get("total_cents") or "-1")) == ROUND6_BASELINE_TOTAL_CENTS
                    and str(rows[0].get("status") or "") == ROUND6_BASELINE_STATUS
                    and str(rows[0].get("proof_nonce") or "") == ROUND6_BASELINE_NONCE
                    and str(rows[0].get("_pg_change_type") or "").casefold() == "insert"
                ):
                    table = _mapping(workspace.tables.get(full_name))
                    if table.get("full_name") != full_name or not table.get("table_id"):
                        raise RuntimeError("Round 6 history table identity is incomplete")
                    return (
                        status,
                        _timestamp(status.get("create_time"), "CDF status create_time"),
                        table,
                    )
        elif len(statuses) > 1:
            raise RuntimeError("Round 6 CDF returned duplicate live_orders statuses")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Round 6 baseline canary timed out; final observation: {final}")
        time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))


def prepare_round6(
    manifest: DemoManifest,
    *,
    timeout: float,
    scale_zero_validator: Callable[[DemoManifest, float], None],
    workspace: Any | None = None,
) -> Round6Resources:
    """Provision and validate Round 6, but leave manifest persistence to lifecycle."""

    if not manifest.round5_ready or manifest.round4 is None:
        raise RuntimeError("Round 6 requires the complete manifest v5 baseline")
    names = round6_names(manifest)
    catalog = os.environ.get("DATABRICKS_CDF_CATALOG", "main").strip()
    if not catalog:
        raise RuntimeError("DATABRICKS_CDF_CATALOG cannot be empty")
    workspace = workspace or _workspace(manifest)
    current = _mapping(workspace.current_user.me())
    principal = str(current.get("userName") or current.get("user_name") or "")
    if principal != manifest.round4.setup_principal:
        raise RuntimeError("Round 6 setup principal differs from the sealed Round 4 identity")
    branch, branch_created, endpoint, endpoint_created = _get_or_create_branch_endpoint(
        workspace, names
    )
    _validate_database(workspace, names)
    _validate_catalog(workspace, catalog)
    app_client_id = manifest.round4.app_service_principal_client_id
    _ensure_round6_app_role(
        manifest,
        names,
        app_client_id,
        timeout=timeout,
    )
    source_oid = asyncio.run(
        ensure_round6_source(
            names,
            workspace=workspace,
            user=principal,
            app_client_id=app_client_id,
        )
    )
    full_schema = f"{catalog}.{names['destination_schema']}"
    try:
        schema_payload = workspace.schemas.get(full_schema)
    except Exception as exc:
        if (
            getattr(exc, "status_code", None) != 404
            and getattr(exc, "error_code", None) != "SCHEMA_DOES_NOT_EXIST"
        ):
            raise
        schema_payload = workspace.schemas.create(
            name=names["destination_schema"],
            catalog_name=catalog,
            comment=f"Owned Round 6 CDF destination for {manifest.run_id}",
            properties={
                ROUND6_OWNER_PROPERTY: manifest.run_id,
                "managed_by": "round6-lifecycle",
            },
        )
    schema = _validate_schema(
        schema_payload,
        catalog=catalog,
        schema=names["destination_schema"],
        owner=principal,
        run_id=manifest.run_id,
    )
    config, config_created = _get_or_create_cdf_config(workspace, names=names, catalog=catalog)
    status, status_created, table = _wait_for_cdf_baseline(
        workspace,
        names=names,
        catalog=catalog,
        warehouse_id=manifest.round4.warehouse_id,
        timeout=timeout,
    )
    # After the feed exists, because the destination table it creates is the
    # securable being granted and a GRANT cannot name a table that is not there
    # yet. Lifecycle owns the grant plan; imported here for the same reason
    # `_ensure_round6_app_role` imports the role helper.
    from .lifecycle import _grant_round6_unity_catalog

    _grant_round6_unity_catalog(
        manifest,
        str(status.get("uc_table") or ""),
        manifest.round4.warehouse_id,
        app_client_id,
    )
    values: dict[str, Any] = {
        "warehouse_id": manifest.round4.warehouse_id,
        "setup_principal": principal,
        "app_service_principal_client_id": (app_client_id),
        "branch_name": names["branch_name"],
        "branch_id": names["branch_id"],
        "branch_uid": str(branch.get("uid") or ""),
        "branch_create_time": branch_created,
        "endpoint_name": names["endpoint_name"],
        "endpoint_id": names["endpoint_id"],
        "endpoint_uid": str(endpoint.get("uid") or ""),
        "endpoint_create_time": endpoint_created,
        "database_resource_name": names["database_resource_name"],
        "database_resource_id": ROUND6_DATABASE_ID,
        "postgres_database": ROUND6_POSTGRES_DATABASE,
        "source_schema": names["source_schema"],
        "source_table": names["source_table"],
        "source_table_oid": source_oid,
        "cdf_config_name": names["cdf_config_name"],
        "cdf_config_id": str(config.get("cdf_config_id") or ""),
        "cdf_config_create_time": config_created,
        "cdf_status_name": names["cdf_status_name"],
        "cdf_status_id": names["cdf_status_id"],
        "cdf_status_create_time": status_created,
        "destination_catalog": catalog,
        "destination_schema": names["destination_schema"],
        "destination_schema_id": str(schema.get("schema_id") or ""),
        "destination_table_full_name": str(status.get("uc_table") or ""),
        "destination_table_id": str(table.get("table_id") or ""),
        "baseline_order_id": ROUND6_BASELINE_ORDER_ID,
        "baseline_proof_nonce": ROUND6_BASELINE_NONCE,
        "baseline_sku": ROUND6_BASELINE_SKU,
        "baseline_store": ROUND6_BASELINE_STORE,
        "baseline_quantity": ROUND6_BASELINE_QUANTITY,
        "baseline_total_cents": ROUND6_BASELINE_TOTAL_CENTS,
        "baseline_status": ROUND6_BASELINE_STATUS,
    }
    values["contract_sha256"] = round6_contract_sha256(
        branch_name=values["branch_name"],
        branch_id=values["branch_id"],
        branch_uid=values["branch_uid"],
        branch_create_time=branch_created.isoformat(),
        endpoint_name=values["endpoint_name"],
        endpoint_id=values["endpoint_id"],
        endpoint_uid=values["endpoint_uid"],
        endpoint_create_time=endpoint_created.isoformat(),
        database_resource_name=values["database_resource_name"],
        database_resource_id=values["database_resource_id"],
        postgres_database=values["postgres_database"],
        source_schema=values["source_schema"],
        source_table=values["source_table"],
        source_table_oid=values["source_table_oid"],
        cdf_config_name=values["cdf_config_name"],
        cdf_config_id=values["cdf_config_id"],
        cdf_config_create_time=config_created.isoformat(),
        cdf_status_name=values["cdf_status_name"],
        cdf_status_id=values["cdf_status_id"],
        cdf_status_create_time=status_created.isoformat(),
        destination_catalog=values["destination_catalog"],
        destination_schema=values["destination_schema"],
        destination_schema_id=values["destination_schema_id"],
        destination_table_full_name=values["destination_table_full_name"],
        destination_table_id=values["destination_table_id"],
        baseline_order_id=values["baseline_order_id"],
        baseline_proof_nonce=values["baseline_proof_nonce"],
        baseline_sku=values["baseline_sku"],
        baseline_store=values["baseline_store"],
        baseline_quantity=values["baseline_quantity"],
        baseline_total_cents=values["baseline_total_cents"],
        baseline_status=values["baseline_status"],
    )
    sealed = Round6Resources.model_validate(values)
    # The baseline above is the live CDF canary. Only a subsequent live
    # scale-zero observation authorizes lifecycle to persist READY manifest v6.
    scale_zero_validator(manifest, timeout)
    return sealed


def check_round6(manifest: DemoManifest, *, workspace: Any | None = None) -> tuple[bool, str]:
    ok, detail, _findings = _check_round6(manifest, workspace=workspace)
    return ok, detail


def _check_round6(
    manifest: DemoManifest, *, workspace: Any | None = None
) -> tuple[bool, str, tuple[tuple[str, str, str], ...]]:
    sealed = manifest.round6
    if not manifest.round6_ready or sealed is None:
        return False, "manifest has no complete sealed Round 6 resources", ()
    try:
        names = round6_names(manifest)
        workspace = workspace or _workspace(manifest)
        listed_branches = [
            _mapping(item)
            for item in workspace.postgres.list_branches(parent=names["project_name"])
            if _mapping(item).get("branch_id") == sealed.branch_id
        ]
        listed_endpoints = [
            _mapping(item)
            for item in workspace.postgres.list_endpoints(parent=sealed.branch_name)
            if _mapping(item).get("endpoint_id") == sealed.endpoint_id
        ]
        if len(listed_branches) != 1 or len(listed_endpoints) != 1:
            raise RuntimeError("Round 6 branch or endpoint identity is not unique")
        branch, branch_created = _validate_branch(
            workspace.postgres.get_branch(sealed.branch_name), names
        )
        endpoint, endpoint_created = _validate_endpoint(
            workspace.postgres.get_endpoint(sealed.endpoint_name), names
        )
        if (
            branch.get("uid") != sealed.branch_uid
            or branch_created != sealed.branch_create_time
            or endpoint.get("uid") != sealed.endpoint_uid
            or endpoint_created != sealed.endpoint_create_time
        ):
            raise RuntimeError("Round 6 branch or endpoint seal changed")
        _validate_database(workspace, names)
        _validate_catalog(workspace, sealed.destination_catalog)
        schema = _validate_schema(
            workspace.schemas.get(f"{sealed.destination_catalog}.{sealed.destination_schema}"),
            catalog=sealed.destination_catalog,
            schema=sealed.destination_schema,
            owner=sealed.setup_principal,
            run_id=manifest.run_id,
        )
        if schema.get("schema_id") != sealed.destination_schema_id:
            raise RuntimeError("Round 6 destination schema ID changed")
        config, created = _validate_cdf_config(
            workspace.postgres.get_cdf_config(sealed.cdf_config_name),
            names=names,
            catalog=sealed.destination_catalog,
        )
        if (
            config.get("cdf_config_id") != sealed.cdf_config_id
            or created != sealed.cdf_config_create_time
        ):
            raise RuntimeError("Round 6 CDF config seal changed")
        listed_configs = [
            _mapping(item)
            for item in workspace.postgres.list_cdf_configs(parent=sealed.database_resource_name)
            if _mapping(item).get("cdf_config_id") == sealed.cdf_config_id
        ]
        if len(listed_configs) != 1:
            raise RuntimeError("Round 6 CDF config identity is not unique")
        status = _mapping(workspace.postgres.get_cdf_status(sealed.cdf_status_name))
        if (
            status.get("name") != sealed.cdf_status_name
            or status.get("postgres_table") != sealed.source_table
            or status.get("uc_table") != sealed.destination_table_full_name
            or _enum(status.get("state")) != "CDF_STATE_STREAMING"
            or status.get("status_detail")
            or _timestamp(status.get("create_time"), "CDF status create_time")
            != sealed.cdf_status_create_time
        ):
            raise RuntimeError("Round 6 CDF status seal changed or is not STREAMING")
        listed_statuses = [
            _mapping(item)
            for item in workspace.postgres.list_cdf_statuses(parent=sealed.cdf_config_name)
            if _mapping(item).get("name") == sealed.cdf_status_name
        ]
        if len(listed_statuses) != 1:
            raise RuntimeError("Round 6 CDF status identity is not unique")
        table = _mapping(workspace.tables.get(sealed.destination_table_full_name))
        if (
            table.get("full_name") != sealed.destination_table_full_name
            or table.get("table_id") != sealed.destination_table_id
        ):
            raise RuntimeError("Round 6 history table ID changed")
        source_ok, source_detail = asyncio.run(_check_source(sealed, workspace))
        if not source_ok:
            raise RuntimeError(source_detail)
        rows = asyncio.run(
            _read_uc_baseline(workspace, sealed.warehouse_id, sealed.destination_table_full_name)
        )
        if len(rows) != 1 or str(rows[0].get("proof_nonce") or "") != sealed.baseline_proof_nonce:
            raise RuntimeError("Round 6 exact baseline is not visible in sealed history")
        return True, sealed.destination_table_full_name, ()
    except Exception as exc:
        return False, str(exc), getattr(exc, "findings", ())


async def _check_source(sealed: Round6Resources, workspace: Any) -> tuple[bool, str]:
    try:
        connection = await _source_connection(
            workspace, sealed.endpoint_name, sealed.setup_principal
        )
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT c.oid, c.relreplident, pg_get_userbyid(n.nspowner), "
                    "current_user FROM pg_class c JOIN pg_namespace n "
                    "ON n.oid = c.relnamespace WHERE n.nspname = %s "
                    "AND c.relname = %s AND c.relkind = 'r'",
                    (sealed.source_schema, sealed.source_table),
                )
                identity = await cursor.fetchall()
                await cursor.execute(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns WHERE table_schema = %s "
                    "AND table_name = %s ORDER BY ordinal_position",
                    (sealed.source_schema, sealed.source_table),
                )
                columns = [tuple(row) for row in await cursor.fetchall()]
                # DELETE is probed alongside SELECT and INSERT because the round
                # settles by removing the exact proof row it committed. This
                # check said "fine" for a release while settlement was refused
                # every bout, purely because it asked about two of the three
                # privileges the runtime actually uses.
                await cursor.execute(
                    "SELECT has_schema_privilege(%s, %s, 'USAGE'), "
                    "has_table_privilege(%s, %s, 'SELECT'), "
                    "has_table_privilege(%s, %s, 'INSERT'), "
                    "has_table_privilege(%s, %s, 'DELETE')",
                    (
                        sealed.app_service_principal_client_id,
                        sealed.source_schema,
                        sealed.app_service_principal_client_id,
                        f"{sealed.source_schema}.{sealed.source_table}",
                        sealed.app_service_principal_client_id,
                        f"{sealed.source_schema}.{sealed.source_table}",
                        sealed.app_service_principal_client_id,
                        f"{sealed.source_schema}.{sealed.source_table}",
                    ),
                )
                privileges = await cursor.fetchone()
                table = sql.SQL("{}.{}").format(
                    sql.Identifier(sealed.source_schema),
                    sql.Identifier(sealed.source_table),
                )
                await cursor.execute(
                    sql.SQL(
                        "SELECT order_id, sku, store, quantity, total_cents, status, "
                        "proof_nonce FROM {} "
                        "WHERE order_id = %s"
                    ).format(table),
                    (sealed.baseline_order_id,),
                )
                baseline = await cursor.fetchall()
        if (
            len(identity) != 1
            or int(identity[0][0]) != sealed.source_table_oid
            or str(identity[0][1]) != "f"
            or str(identity[0][2]) != str(identity[0][3])
            or columns
            != [
                ("order_id", "text", "NO"),
                ("sku", "text", "NO"),
                ("store", "text", "NO"),
                ("quantity", "integer", "NO"),
                ("total_cents", "integer", "NO"),
                ("status", "text", "NO"),
                ("proof_nonce", "text", "NO"),
            ]
            or tuple(privileges or ()) != (True, True, True, True)
            or len(baseline) != 1
            or tuple(baseline[0])
            != (
                sealed.baseline_order_id,
                sealed.baseline_sku,
                sealed.baseline_store,
                sealed.baseline_quantity,
                sealed.baseline_total_cents,
                sealed.baseline_status,
                sealed.baseline_proof_nonce,
            )
        ):
            return (
                False,
                "Round 6 source table OID, ownership, replica identity, or baseline changed",
            )
        return True, str(sealed.source_table_oid)
    except Exception as exc:
        return False, str(exc)


async def _delete_source_schema(sealed: Round6Resources, workspace: Any) -> None:
    """Drop the sealed source table and schema, refusing on anything unexpected.

    The tolerance is deliberately wrapped around the connection alone rather
    than the whole body. An endpoint that no longer exists holds no Postgres and
    therefore no source schema, so its absence means the work is done; but the
    two refusals further down are the source-schema ownership and contents
    checks, and a text match wide enough to reach them could swallow one. The
    schema being gone while the endpoint is still up needs nothing here -- the
    query below already returns no rows and stops.
    """

    try:
        connection = await _source_connection(
            workspace, sealed.endpoint_name, sealed.setup_principal
        )
    except Exception as exc:
        if not _absent(exc):
            raise
        print(
            f"FORCE   Postgres schema {sealed.source_schema}: its endpoint is already gone",
            flush=True,
        )
        return
    async with connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT c.oid, c.relname, c.relkind, pg_get_userbyid(n.nspowner), "
                "current_user FROM pg_namespace n LEFT JOIN pg_class c "
                "ON c.relnamespace = n.oid WHERE n.nspname = %s "
                "AND (c.relkind IS NULL OR c.relkind NOT IN ('i', 'I', 'S', 't'))",
                (sealed.source_schema,),
            )
            rows = await cursor.fetchall()
            if not rows:
                return
            if any(str(row[3]) != str(row[4]) for row in rows):
                raise RuntimeError("Cleanup refused: Round 6 source schema owner changed")
            objects = [(int(row[0]), str(row[1]), str(row[2])) for row in rows if row[0]]
            if objects != [(sealed.source_table_oid, sealed.source_table, "r")]:
                raise RuntimeError("Cleanup refused: Round 6 source schema has unexpected objects")
            table = sql.SQL("{}.{}").format(
                sql.Identifier(sealed.source_schema), sql.Identifier(sealed.source_table)
            )
            await cursor.execute(sql.SQL("DROP TABLE {}").format(table))
            await cursor.execute(
                sql.SQL("DROP SCHEMA {}").format(sql.Identifier(sealed.source_schema))
            )
        await connection.commit()


def force_round6_tokens(manifest: DemoManifest) -> tuple[str, ...]:
    """The confirmation tokens that authorise forcing *this* environment.

    Two are accepted because they protect against different mistakes. The run ID
    is on the operator's screen already, so it is the one they can type without
    opening the manifest. The branch UID is server-assigned and unguessable, and
    it is the field most likely to have drifted, so quoting it is evidence the
    operator has actually looked at the environment they are about to destroy.

    Either way the point is the same: a ``--force-round6`` recalled from shell
    history carries the *previous* environment's token, so it cannot destroy this
    one by accident. That is the entire threat this defends against -- not a
    hostile operator, who has console access anyway, but a tired one holding the
    up arrow.
    """

    sealed = manifest.round6
    tokens = [manifest.run_id]
    if sealed is not None:
        tokens.extend(value for value in (sealed.branch_uid, sealed.endpoint_uid) if value)
    return tuple(dict.fromkeys(token for token in tokens if token))


def authorize_force_round6(manifest: DemoManifest, token: str) -> str:
    """Accept an exact confirmation token, or refuse and change nothing.

    Refuses on mismatch even when the seal happens to verify. A wrong token means
    the operator believes they are pointed at a different environment, and being
    wrong about *which* environment is precisely the error worth stopping.
    """

    candidate = (token or "").strip()
    accepted = force_round6_tokens(manifest)
    if not candidate:
        raise RuntimeError(
            "Cleanup refused: --force-round6 requires a confirmation token; "
            f"pass one of {', '.join(accepted)}"
        )
    if candidate not in accepted:
        raise RuntimeError(
            f"Cleanup refused: --force-round6 token {candidate!r} does not name this "
            f"environment; this manifest accepts {', '.join(accepted)}"
        )
    return candidate


def _force_round6_manifest_lines(
    manifest: DemoManifest,
    sealed: Round6Resources,
    *,
    detail: str,
    findings: tuple[tuple[str, str, str], ...],
    branch_deletable: bool,
) -> tuple[str, ...]:
    """Exactly what is about to be destroyed, and exactly why the seal refused."""

    lines = [
        "FORCE ==============================================================",
        "FORCE --force-round6: the Round 6 seal check is being OVERRIDDEN.",
        f"FORCE run_id={manifest.run_id}",
        f"FORCE seal check failed with: {detail}",
    ]
    lines.extend(
        f"FORCE   mismatch field={field} expected={expected} found={found}"
        for field, expected, found in findings
    )
    if not findings:
        lines.append("FORCE   mismatch fields were not itemised by this failure")
    lines.append("FORCE WILL DESTROY, by sealed name only:")
    lines.append(f"FORCE   CDF config      {sealed.cdf_config_name}")
    lines.append(
        f"FORCE   UC schema       {sealed.destination_catalog}.{sealed.destination_schema} "
        "(force=true, cascading)"
    )
    lines.append(
        f"FORCE   Postgres schema {sealed.source_schema} "
        f"(table {sealed.source_table}, oid {sealed.source_table_oid})"
    )
    if branch_deletable:
        lines.append(f"FORCE   Lakebase branch {sealed.branch_name} (purge=true)")
    else:
        lines.append(
            f"FORCE   Lakebase branch {sealed.branch_name} WILL NOT be deleted "
            "(shared production branch, or its UID no longer matches the seal)"
        )
    lines.append("FORCE STILL ENFORCED, and this force does NOT bypass them:")
    lines.append(
        "FORCE   destination schema must hold nothing but the sealed table "
        "(unexpected-tables check)"
    )
    lines.append(
        "FORCE   source schema must hold nothing but the sealed table, and must "
        "still be owned by the setup user"
    )
    lines.append("FORCE   the CDF config must actually be gone after the forced delete")
    lines.append(
        "FORCE   every identity and ownership check in the enclosing cleanup is unchanged"
    )
    lines.append("FORCE ==============================================================")
    return tuple(lines)


def write_force_round6_audit(
    manifest: DemoManifest,
    *,
    token: str,
    detail: str,
    findings: tuple[tuple[str, str, str], ...],
    dry_run: bool,
    branch_deletable: bool,
    state_dir: Path | None = None,
) -> Path | None:
    """Record who overrode the gate, and against what mismatch. Best effort.

    Written before the first delete, so an override that then fails half way is
    still on the record. Failing to write the audit does not stop the cleanup:
    the same content has already gone to stdout, and refusing here would restore
    the exact deadlock the escape hatch exists to break.
    """

    resolved = state_dir_from_environ() if state_dir is None else state_dir
    if resolved is None or not resolved.is_dir():
        return None
    sealed = manifest.round6
    entry = {
        "event": "force_round6",
        "at": datetime.now(UTC).isoformat(),
        "dry_run": dry_run,
        "run_id": manifest.run_id,
        "confirmation_token": token,
        "seal_failure": detail,
        "mismatches": [
            {"field": field, "expected": expected, "found": found}
            for field, expected, found in findings
        ],
        "forced_by": {
            "os_user": _os_user(),
            "databricks_user": getattr(manifest.databricks, "user", ""),
            "pid": os.getpid(),
            "host": socket.gethostname(),
        },
        "branch_deletable": branch_deletable,
        "targets": {}
        if sealed is None
        else {
            "cdf_config_name": sealed.cdf_config_name,
            "destination_schema": f"{sealed.destination_catalog}.{sealed.destination_schema}",
            "source_schema": sealed.source_schema,
            "source_table_oid": sealed.source_table_oid,
            "branch_name": sealed.branch_name,
            "branch_uid": sealed.branch_uid,
            "endpoint_name": sealed.endpoint_name,
        },
    }
    path = resolved / "round6-force.jsonl"
    try:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")
        path.chmod(0o600)
    except OSError:
        return None
    return path


def _os_user() -> str:
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 - a nameless user is still worth recording
        return f"uid:{os.getuid()}"


def _round6_cleanup_report(
    manifest: DemoManifest,
    detail: str,
    findings: tuple[tuple[str, str, str], ...],
) -> tuple[str, ...]:
    lines = [f"DRIFT Round 6 cleanup would refuse: {detail}"]
    lines.extend(
        f"DRIFT   field={field} expected={expected} found={found}"
        for field, expected, found in findings
    )
    lines.append("DRIFT   Inventory only: a real cleanup still refuses until this matches")
    # Naming the escape hatch here is what keeps a genuinely drifted environment
    # from being stranded: the operator learns the sanctioned override, and the
    # exact token for it, from the refusal itself rather than from a console.
    tokens = force_round6_tokens(manifest)
    if tokens:
        lines.append(
            "DRIFT   If this drift is real and the environment is to be reaped anyway: "
            f"uv run antidemo cleanup --yes --force-round6 {tokens[0]}"
        )
    return tuple(lines)


#: How Unity Catalog says a name resolves to nothing. Matched against the text
#: of the failure, as `lifecycle._databricks_api_optional` does, because the
#: absence arrives as a server message. Narrow on purpose: a denial, a throttle
#: or a dropped connection says nothing about whether the schema is there, and
#: none of them may be read as an answer.
_ABSENT_MARKERS = ("not found", "does not exist", "resource_does_not_exist", "404")


def _absent(exc: BaseException) -> bool:
    """Whether the failure says the resource is not there, rather than unreadable.

    Textual, and so inexact in one known direction: a Unity Catalog denial
    phrased as "does not exist" reads here as an absence. That ambiguity is
    inherited from the marker idiom and is bounded by where it is used. Every
    caller below is either confirming the absence afterwards by a separate
    route, or deleting a resource it has already identified -- so a false
    absence costs a delete that does not happen, never a delete that does.
    """

    return any(marker in str(exc).lower() for marker in _ABSENT_MARKERS)


def _delete_if_present(step: str, delete: Callable[[], Any]) -> None:
    """Carry out a teardown step that an earlier failed teardown may have done.

    Every delete below this point is reachable in a state where a previous
    cleanup already performed it and then died further along, which is not an
    edge case but the state that a resumed teardown is always in. None of these
    APIs takes an ``allow_missing``, so the absence arrives as a failure and has
    to be read rather than prevented.

    Only the absence is read. A denial, a throttle or a dropped connection is a
    failure to act, not evidence the resource is gone, and each still refuses.
    The skip is printed because an operator watching a forced teardown needs to
    know which steps it found already done.
    """

    try:
        delete()
    except Exception as exc:
        if not _absent(exc):
            raise
        print(f"FORCE   {step}: already gone, nothing to delete", flush=True)


def _destination_tables(workspace: Any, sealed: Round6Resources) -> list[Mapping[str, Any]]:
    """The tables in the sealed destination schema, or none at all if it is gone.

    A schema that no longer exists holds no unexpected tables, so the check this
    feeds is satisfied vacuously. That is not a hypothetical state: a cleanup
    that deletes the schema and then fails further along leaves the seal
    unverifiable, and the ``force_token`` that exists to get past exactly that
    trap does not relax this check -- so a listing that raised on the absence
    stranded the environment behind the one check with nothing left to check.

    Only the absence is tolerated. Every other failure is a failure to look, and
    a failure to look still refuses, because this check is the only thing
    standing between a teardown and a schema someone else has put tables in.
    The listing is consumed here rather than returned lazily so that a paginated
    iterator raises inside this tolerance instead of past it.
    """

    try:
        return [
            _mapping(item)
            for item in workspace.tables.list(
                catalog_name=sealed.destination_catalog,
                schema_name=sealed.destination_schema,
            )
        ]
    except Exception as exc:
        if not _absent(exc):
            raise
        return []


def _remaining_cdf_configs(workspace: Any, sealed: Round6Resources) -> list[str]:
    """The CDF configs still present on the sealed database, for the proof below.

    This listing is what turns the forced delete's outcome into evidence, so it
    has to keep proving something once that delete can be skipped as already
    done. It is the stronger half of the pair on purpose: a successful listing
    that omits the sealed name is positive proof of absence, where the delete's
    own not-found is only the service's word for it.

    A database that is itself gone carries no configs, so its absence answers
    the question rather than dodging it. Anything else still refuses.
    """

    try:
        return [
            str(_mapping(item).get("name") or "")
            for item in workspace.postgres.list_cdf_configs(
                parent=sealed.database_resource_name
            )
        ]
    except Exception as exc:
        if not _absent(exc):
            raise
        return []


def cleanup_round6(
    manifest: DemoManifest,
    *,
    dry_run: bool,
    workspace: Any | None = None,
    force_token: str = "",
    state_dir: Path | None = None,
) -> tuple[str, ...]:
    """Delete the owned Round 6 resources, or report why a cleanup would refuse.

    A dry run reports a contract mismatch as a finding rather than raising it. A
    drifted environment is exactly the one an operator needs to inspect, so the
    drift must not be the thing that takes inspection away. A real cleanup still
    refuses on any mismatch, before anything is deleted.

    Every refusal is raised before the first delete, and that ordering is the
    whole contract. A refusal that fires afterwards leaves a half-torn-down
    environment whose seal can no longer be verified, so every later cleanup
    refuses at the gate and the branch and endpoint bill forever with no way
    to reap them.

    ``force_token`` is the sanctioned way out of exactly that trap, and it does
    one thing: it downgrades the seal *verification* from a refusal to a recorded
    override. It is empty unless an operator typed ``--force-round6 <token>``, so
    the paragraph above describes the default path unchanged. Nothing else is
    relaxed -- in particular the unexpected-tables check, the source-schema
    ownership and contents checks, and the post-delete confirmation all still
    run, in the same order, and still refuse.
    """

    sealed = manifest.round6
    if sealed is None:
        return ()
    forcing = bool((force_token or "").strip())
    if forcing:
        # Validated before the seal is even consulted. A token naming the wrong
        # environment is an operator error worth stopping whatever the seal says.
        authorize_force_round6(manifest, force_token)
    ok, detail, findings = _check_round6(manifest, workspace=workspace)
    if not ok and not forcing:
        if dry_run:
            return _round6_cleanup_report(manifest, detail, findings)
        raise RuntimeError(f"Cleanup refused: {detail}")
    if not forcing and dry_run:
        return ()
    workspace = workspace or _workspace(manifest)
    branch_deletable = getattr(manifest, "round_environments", None) is None
    forced_lines: tuple[str, ...] = ()
    if forcing:
        # A drifted UID means the branch standing at the sealed name is not the
        # branch that was sealed. Deleting it would destroy somebody else's
        # environment, which is worse than the leak being fixed, so the forced
        # path narrows here rather than widening.
        branch_deletable = branch_deletable and _forced_branch_is_still_ours(sealed, workspace)
        forced_lines = _force_round6_manifest_lines(
            manifest,
            sealed,
            detail=detail if not ok else "the seal verified; --force-round6 was passed anyway",
            findings=findings,
            branch_deletable=branch_deletable,
        )
        for line in forced_lines:
            print(line, flush=True)
        write_force_round6_audit(
            manifest,
            token=force_token.strip(),
            detail=detail if not ok else "seal verified",
            findings=findings,
            dry_run=dry_run,
            branch_deletable=branch_deletable,
            state_dir=state_dir,
        )
        if dry_run:
            return (
                *forced_lines,
                "FORCE Dry run: nothing was deleted. Re-run with --yes to act.",
            )
    # Asked before the CDF config is destroyed. The answer does not depend on
    # deleting it, and refusing afterwards would strand the environment.
    tables = _destination_tables(workspace, sealed)
    if [(item.get("full_name"), item.get("table_id")) for item in tables] not in (
        [],
        [(sealed.destination_table_full_name, sealed.destination_table_id)],
    ):
        raise RuntimeError("Cleanup refused: Round 6 destination schema has unexpected tables")
    _delete_if_present(
        f"CDF config {sealed.cdf_config_name}",
        lambda: workspace.postgres.delete_cdf_config(sealed.cdf_config_name, force=True).wait(),
    )
    # Unchanged in what it enforces: the config must be gone, however it went.
    if sealed.cdf_config_name in _remaining_cdf_configs(workspace, sealed):
        raise RuntimeError("Round 6 CDF config still exists after forced delete")
    destination_schema = f"{sealed.destination_catalog}.{sealed.destination_schema}"
    # No second opinion is taken on this one. Re-reading the schema would ask
    # the same service the same question under the same credentials, so it
    # would repeat a denial-phrased-as-absence rather than catch it, and the
    # only thing a false absence costs here is a schema left standing -- which
    # the project delete that follows this cleanup removes anyway. What the
    # schema *held* was settled above, strictly, before anything was deleted.
    _delete_if_present(
        f"UC schema {destination_schema}",
        lambda: workspace.schemas.delete(destination_schema, force=True),
    )
    asyncio.run(_delete_source_schema(sealed, workspace))
    if branch_deletable:
        # Absence is tolerated here on the narrowest terms of any step: the
        # branch was positively identified by its sealed UID moments ago, so
        # a not-found now means it went away between that read and this delete.
        # ``allow_missing`` stays False so that the identity check, not the
        # API's leniency, remains what permits this purge.
        _delete_if_present(
            f"Lakebase branch {sealed.branch_name}",
            lambda: workspace.postgres.delete_branch(
                sealed.branch_name, allow_missing=False, purge=True
            ).wait(),
        )
    return forced_lines if forcing else ()


def _forced_branch_is_still_ours(sealed: Round6Resources, workspace: Any) -> bool:
    """Is the branch standing at the sealed name the one that was sealed?

    Only a positively matching UID answers yes. A drifted UID, a missing branch,
    or an unreadable one all answer no, because ``purge=True`` on the wrong
    branch is not a mistake that can be walked back.
    """

    try:
        branch = _mapping(workspace.postgres.get_branch(sealed.branch_name))
    except Exception:  # noqa: BLE001 - unreadable is not the same as ours
        return False
    return bool(sealed.branch_uid) and branch.get("uid") == sealed.branch_uid
