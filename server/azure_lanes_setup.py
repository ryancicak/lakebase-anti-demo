"""The installer's Rounds 4 and 6 Azure lanes: built once, proven once cold, then sealed.

Kept apart from `server/azure_lifecycle.py` because each lane is its own short story, told in the
order Phase 0 (2026-10-18/19) learned it:

* **Round 4** writes into the r4 server as the AWS lane's writer role, owning the AWS lane's ledger
  and view (`round4_aws_lifecycle.ensure_writer_target`, unchanged), plus a staging table the data
  flow fills before its guarded upsert. The flow reads Round 4's Delta source straight from the
  `r4source` container, the version the run names (`versionAsOf`).
* **Round 6** extracts from the r6 server's `round6.live_orders`, the AWS lane's source exactly
  (`round6_aws_lifecycle.source_statements`), with one more column, `committed_at`, defaulted by
  the database, so the app's checkout writes nothing new. The factory reads it as a read-only
  role. The history is a Delta table ADF writes, and Unity Catalog then registers it as an
  external table: a table Unity Catalog made first couldn't be appended to by ADF's Delta 2.4.
* **Each lane runs once, cold, before it is sealed,** and the seal names only what that run used.
  A lane whose run didn't land is left unsealed, and its round races Lakebase alone, saying so.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import Callable
from typing import Any

from psycopg import sql

from . import lifecycle
from .azure_adf_job import AdfDataFlowJob
from .azure_arm import DATA_FACTORY_API, ArmClient
from .azure_dataflows import pipeline, round4_flow, round6_flow
from .azure_manifest import AzureDataFlowSeal
from .azure_runtime import (
    ADF_UNREADABLE_FEATURES,
    ADLS_LINKED_SERVICE,
    POSTGRES_LINKED_SERVICES,
    ROUND4_SCHEMA,
    ROUND4_STAGING_TABLE,
    ROUND6_SCHEMA,
    ROUND6_TABLE,
    history_table_full_name,
    server_connector,
)
from .manifest import DemoManifest

ROUND4_FLOW, ROUND4_PIPELINE = "r4_move", "r4_pipeline"
ROUND6_FLOW, ROUND6_PIPELINE = "r6_extract", "r6_pipeline"
#: The staging table the data flow fills on r4, before its guarded upsert into the ledger.
ROUND4_STAGING_QUALIFIED = "round4.model_score_staging"
ROUND6_FOLDER = "live_orders"
ROUND6_READER_ROLE = "round6_reader"
#: A managed-VNet data flow's cold start took up to about 7 minutes in Phase 0; a proof run waits
#: well past that before it calls the lane broken.
PROOF_BOUND_SECONDS = 1800.0
PROOF_POLL_SECONDS = 10.0
ENDED = frozenset({"Succeeded", "Failed", "Cancelled"})


class AzureLaneSetupError(RuntimeError):
    """A lane couldn't be built or proven, for the reason named."""


def _put(arm: ArmClient, factory: str, kind: str, name: str, body: dict[str, Any]) -> None:
    response = arm.request(
        "PUT", f"{arm.factory_path(factory)}/{kind}/{name}?api-version={DATA_FACTORY_API}", body)
    if response.status not in (200, 201):
        message = response.body if isinstance(response.body, str) else (response.body or {})
        raise AzureLaneSetupError(f"the factory refused {kind}/{name}: {response.status} "
                                  f"{str(message)[:300]}")


def postgres_linked_service(*, runtime: str, host: str, database: str, user: str,
                            password: str) -> dict[str, Any]:
    """A server's linked service, through the managed-VNet runtime and its private endpoint."""

    return {"properties": {
        "type": "AzurePostgreSql", "version": "2.0",
        "connectVia": {"referenceName": runtime, "type": "IntegrationRuntimeReference"},
        "typeProperties": {
            "server": host, "port": 5432, "database": database, "username": user,
            "password": {"type": "SecureString", "value": password},
            "sslMode": 2, "authenticationType": "Basic"}}}


def prove(job: AdfDataFlowJob, parameters: dict[str, Any] | None, label: str, *,
          sleep: Callable[[float], None] = time.sleep,
          clock: Callable[[], float] = time.monotonic) -> str:
    """One cold run to its end; its run ID once it Succeeded."""

    run_id = job.start(parameters)
    started = clock()
    print(f"PROVE {label}: run {run_id} started cold", flush=True)
    while True:
        run = job.run(run_id)
        if run.state in ENDED:
            break
        if clock() - started > PROOF_BOUND_SECONDS:
            job.park()
            raise AzureLaneSetupError(f"{label}'s proof run was still {run.state} after "
                                      f"{PROOF_BOUND_SECONDS:.0f}s")
        sleep(PROOF_POLL_SECONDS)
    if run.state != "Succeeded":
        reason = f": {run.message}" if run.message else ""
        raise AzureLaneSetupError(f"{label}'s proof run {run_id} ended {run.state}{reason}")
    print(f"OK    {label}: run {run_id} Succeeded in {clock() - started:.0f}s", flush=True)
    return run_id


def make_source_readable(profile: str, warehouse_id: str, table: str) -> tuple[str, ...]:
    """Drop each reader feature Data Factory's Delta 2.4 can't read from Round 4's source.

    Returns the features it dropped; none, once the table is readable.
    """

    quoted = ".".join(f"`{part}`" for part in table.split("."))
    rows = lifecycle._sql_rows(lifecycle._sql_statement(
        profile, warehouse_id, f"SHOW TBLPROPERTIES {quoted}"))
    present = {str(row.get("key") or "") for row in rows}
    dropped = tuple(feature for feature in ADF_UNREADABLE_FEATURES
                    if f"delta.feature.{feature}" in present)
    for feature in dropped:
        print(f"ALTER Round 4's source: drop {feature}, which Data Factory's Delta 2.4 can't read",
              flush=True)
        lifecycle._sql_statement(profile, warehouse_id,
                                 f"ALTER TABLE {quoted} DROP FEATURE {feature}")
    return dropped


async def _admin(manifest: DemoManifest, key: str, statements: list[tuple[Any, tuple[Any, ...]]],
                 ) -> list[tuple[Any, ...]]:
    """Run `statements` on one round's server as its admin; the last one's rows."""

    connection = await server_connector(manifest, key)(f"lakebase-anti-demo-setup-{key}")
    rows: list[tuple[Any, ...]] = []
    async with connection:
        async with connection.cursor() as cursor:
            for statement, parameters in statements:
                await cursor.execute(statement, parameters)
                rows = list(await cursor.fetchall()) if cursor.description else []
    return rows


def ensure_round4_lane(manifest: DemoManifest, arm: ArmClient, workspace: Any, *,
                       runtime: str, factory: str, warehouse_id: str,
                       writer_password: str) -> AzureDataFlowSeal:
    """Round 4's Azure lane, proven by one cold run that carries the baseline row into r4."""

    from .round4_aws_lifecycle import ensure_writer_target

    azure = manifest.azure
    if azure is None or manifest.round4 is None:
        raise AzureLaneSetupError("Round 4's Azure lane needs Round 4's Lakebase seal first")
    server = azure.servers["r4"]

    async def targets() -> None:
        connection = await server_connector(manifest, "r4")("lakebase-anti-demo-setup-r4")
        async with connection:
            async with connection.cursor() as cursor:
                await ensure_writer_target(cursor, database=manifest.databricks.database,
                                           password=writer_password)
                await cursor.execute(sql.SQL(
                    f"CREATE TABLE IF NOT EXISTS {ROUND4_STAGING_QUALIFIED} (entity_id text, "
                    "score double precision, model_version text, proof_nonce text, "
                    "updated_at timestamptz, delta_table_id text, delta_commit_version bigint)"))
                await cursor.execute(sql.SQL("ALTER TABLE {} OWNER TO {}").format(
                    sql.Identifier(ROUND4_SCHEMA, ROUND4_STAGING_TABLE),
                    sql.Identifier("round4_writer")))

    print("CREATE/VERIFY Round 4's writer role, ledger and staging table on r4", flush=True)
    asyncio.run(targets())
    table = workspace.tables.get(manifest.round4.source_table_full_name)
    location = str(table.storage_location or "")
    prefix = f"abfss://r4source@{azure.storage_account}.dfs.core.windows.net/"
    if not location.startswith(prefix):
        raise AzureLaneSetupError(f"Round 4's source isn't in the r4source container: {location}")
    folder = location[len(prefix):].rstrip("/")
    # Made readable once, before Round 4's setup; checked again here, because a drop is a commit
    # and the run reads the head.
    make_source_readable(manifest.databricks.profile, warehouse_id,
                         manifest.round4.source_table_full_name)
    head = int(lifecycle._sql_rows(lifecycle._sql_statement(
        manifest.databricks.profile, warehouse_id,
        f"DESCRIBE HISTORY {manifest.round4.source_table_full_name} LIMIT 1"))[0]["version"])
    _put(arm, factory, "linkedservices", POSTGRES_LINKED_SERVICES["r4"], postgres_linked_service(
        runtime=runtime, host=server.host, database=manifest.databricks.database,
        user="round4_writer", password=writer_password))
    _put(arm, factory, "dataflows", ROUND4_FLOW, round4_flow(
        adls_linked_service=ADLS_LINKED_SERVICE,
        postgres_linked_service=POSTGRES_LINKED_SERVICES["r4"],
        container="r4source", folder=folder, version=head, schema=ROUND4_SCHEMA,
        target="model_score_ledger", staging=ROUND4_STAGING_TABLE,
        delta_table_id=str(table.table_id)))
    _put(arm, factory, "pipelines", ROUND4_PIPELINE, pipeline(
        flow=ROUND4_FLOW, runtime=runtime,
        parameters={"version": ("int", head), "table_id": ("string", str(table.table_id))}))
    job = AdfDataFlowJob(arm, factory, ROUND4_PIPELINE)
    job.park()
    prove(job, {"version": head, "table_id": str(table.table_id)}, "Round 4's data flow")
    rows = asyncio.run(_admin(manifest, "r4", [(sql.SQL(
        "SELECT delta_commit_version FROM {} WHERE entity_id = %s").format(
        sql.Identifier(ROUND4_SCHEMA, "model_score_ledger")),
        (lifecycle.ROUND4_BASELINE_ENTITY_ID,))]))
    if not rows or int(rows[0][0]) != head:
        raise AzureLaneSetupError("Round 4's proof run didn't carry the baseline row into r4")
    return AzureDataFlowSeal(factory=factory, pipeline=ROUND4_PIPELINE, data_flow=ROUND4_FLOW,
                             runtime=runtime, server_endpoint="mpe-r4-postgres",
                             storage_endpoint="mpe-adls-dfs", container="r4source", folder=folder)


def ensure_round6_lane(manifest: DemoManifest, arm: ArmClient, *, runtime: str, factory: str,
                       warehouse_id: str, reader_password: str) -> AzureDataFlowSeal:
    """Round 6's Azure lane, proven by one cold run that extracts the baseline order into Delta."""

    from .round6_aws_lifecycle import (
        BASELINE_ROW,
        ROUND6_BASELINE_ORDER_ID,
        history_table_statement,
        source_statements,
    )

    azure = manifest.azure
    if azure is None or manifest.round6 is None:
        raise AzureLaneSetupError("Round 6's Azure lane needs Round 6's Lakebase seal first")
    table = sql.Identifier(ROUND6_SCHEMA, ROUND6_TABLE)
    reader = sql.Identifier(ROUND6_READER_ROLE)

    async def source() -> None:
        connection = await server_connector(manifest, "r6")("lakebase-anti-demo-setup-r6")
        async with connection:
            async with connection.cursor() as cursor:
                for statement in source_statements():
                    await cursor.execute(statement)
                # Azure's watermark column: the database stamps it, so a checkout writes nothing
                # new; clock_timestamp(), never now(), which is the transaction's start.
                await cursor.execute(sql.SQL(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS committed_at timestamptz NOT NULL "
                    "DEFAULT clock_timestamp()").format(table))
                await cursor.execute(sql.SQL(
                    "INSERT INTO {} (order_id, sku, store, quantity, total_cents, status, "
                    "proof_nonce) VALUES (%s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (order_id) DO NOTHING").format(table), BASELINE_ROW)
                await cursor.execute(sql.SQL("DELETE FROM {} WHERE order_id <> %s").format(table),
                                     (ROUND6_BASELINE_ORDER_ID,))
                await cursor.execute("SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s",
                                     (ROUND6_READER_ROLE,))
                verb = "ALTER" if await cursor.fetchone() else "CREATE"
                await cursor.execute(sql.SQL(f"{verb} ROLE {{}} LOGIN PASSWORD {{}}").format(
                    reader, sql.Literal(reader_password)))
                await cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(manifest.databricks.database), reader))
                await cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(ROUND6_SCHEMA), reader))
                await cursor.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(table, reader))

    print("CREATE/VERIFY Round 6's source, at its baseline, and its reader on r6", flush=True)
    asyncio.run(source())
    _put(arm, factory, "linkedservices", POSTGRES_LINKED_SERVICES["r6"], postgres_linked_service(
        runtime=runtime, host=azure.servers["r6"].host, database=manifest.databricks.database,
        user=ROUND6_READER_ROLE, password=reader_password))
    _put(arm, factory, "dataflows", ROUND6_FLOW, round6_flow(
        postgres_linked_service=POSTGRES_LINKED_SERVICES["r6"],
        adls_linked_service=ADLS_LINKED_SERVICE, table=ROUND6_TABLE,
        watermark_utc="1970-01-01T00:00:00Z", container="r6history", folder=ROUND6_FOLDER,
        schema=ROUND6_SCHEMA))
    _put(arm, factory, "pipelines", ROUND6_PIPELINE, pipeline(flow=ROUND6_FLOW, runtime=runtime))
    job = AdfDataFlowJob(arm, factory, ROUND6_PIPELINE)
    job.park()
    prove(job, None, "Round 6's data flow")
    history = history_table_full_name(manifest)
    location = f"abfss://r6history@{azure.storage_account}.dfs.core.windows.net/{ROUND6_FOLDER}"
    profile = manifest.databricks.profile
    lifecycle._sql_statement(profile, warehouse_id, history_table_statement(history, location))
    quoted = ".".join(f"`{part}`" for part in history.split("."))
    rows = lifecycle._sql_rows(lifecycle._sql_statement(
        profile, warehouse_id,
        f"SELECT count(*) AS n FROM {quoted} WHERE order_id = '{ROUND6_BASELINE_ORDER_ID}'"))
    if not rows or int(rows[0]["n"]) != 1:
        raise AzureLaneSetupError("Round 6's proof run didn't extract the baseline order once")
    # The App reads both histories through the warehouse as itself. Round 6's grant plan names
    # Lakebase's table; Azure's is granted here, the same privilege on the one table.
    app = manifest.round4.app_service_principal_client_id if manifest.round4 else ""
    if app:
        lifecycle._sql_statement(profile, warehouse_id,
                                 f"GRANT SELECT ON TABLE {quoted} TO `{app}`")
    return AzureDataFlowSeal(factory=factory, pipeline=ROUND6_PIPELINE, data_flow=ROUND6_FLOW,
                             runtime=runtime, server_endpoint="mpe-r6-postgres",
                             storage_endpoint="mpe-adls-dfs", container="r6history",
                             folder=ROUND6_FOLDER)


def new_role_password() -> str:
    return secrets.token_urlsafe(32)
