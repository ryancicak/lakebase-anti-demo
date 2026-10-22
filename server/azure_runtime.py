"""The serving App's Azure side: its ARM client, and the round engines built on it.

`server/safe_change_live.py` and `server/recovery_live.py` hand an Azure installation's manifest
here, so the AWS edition's builders are unchanged and nothing on this path makes a boto3 client.
What the engines race, bell, verify and clean up is the AWS edition's; only the competitor's
lane is Azure's (design section 6):

* **Rounds 2 and 3** restore the round's own server to a point in time, as RDS restores an
  instance: `AzurePostgresSafeChangeAdapter` and `AzurePostgresRecoveryAdapter`, built only through
  `config_from_manifest`, so the manifest's tag rules always apply.
* **ARM tokens:** the deployed App's come from its Unity Catalog service credential, the laptop's
  from `az login` (`server/azure_identity.py`). Neither is an Azure secret.
* **Rounds 4 and 6** race their sealed ADF pipelines, cold at the bell: `AdfWriterLane` into the
  r4 server, `AdfExtractLane` out of the r6 server into Delta. Round 4's settle bound is 900 s,
  sized for an ADF cold run (up to 408 s in Phase 0), not Glue's 420 s.
* **The servers' admin password** reaches the App as a secret binding (`app.yaml`), and the
  laptop from the installation's generation directory. It is never sealed in the manifest.
* **Every connection to an Azure server verifies its certificate** against the pinned roots and
  accepts SCRAM only (`verifying_connector`).
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable, Mapping
from datetime import UTC
from pathlib import Path
from typing import Any

import psycopg

from .azure_arm import ArmClient, AzureCredentials, token_reuse_seconds
from .azure_identity import az_cli_token_source, service_credential_token_source
from .azure_manifest import AzureManifest
from .azure_recovery import AzurePostgresRecoveryAdapter
from .azure_safe_change import RESTORES_SENT, AzurePostgresSafeChangeAdapter, config_from_manifest
from .manifest import PROJECT_ROOT, DemoManifest
from .models import CompetitorId

#: The App's binding of the servers' admin password (`app.yaml`), and the laptop's copy of it.
ADMIN_PASSWORD_ENV = "AZURE_POSTGRES_ADMIN_PASSWORD"
ADMIN_PASSWORD_FILE = PROJECT_ROOT / ".anti-demo-azure" / "admin-password"


class AzureRuntimeError(RuntimeError):
    """The Azure installation can't serve this round, for the reason named."""


def _app_mode(env: Mapping[str, str]) -> bool:
    return env.get("ANTI_DEMO_ENV", "").strip() == "databricks-app" or bool(
        env.get("DATABRICKS_APP_NAME", "").strip()
    )


def azure_section(manifest: DemoManifest) -> AzureManifest:
    if manifest.azure is None:
        raise AzureRuntimeError("this manifest is not an Azure installation's")
    return manifest.azure


def app_arm_client(
    manifest: DemoManifest,
    *,
    environment: Mapping[str, str] | None = None,
    workspace: Any | None = None,
) -> ArmClient:
    """The ARM client for this installation's resource group, as this process may hold one.

    Deployed, the token comes from the App's service credential: the App is the only
    principal with ACCESS on it. On the operator's laptop it comes from `az login`.
    """

    env = os.environ if environment is None else environment
    azure = azure_section(manifest)
    if _app_mode(env):
        sealed = azure.unity_catalog
        if sealed is None:
            raise AzureRuntimeError("the App's Azure service credential is not sealed yet")
        if workspace is None:
            from databricks.sdk import WorkspaceClient

            workspace = WorkspaceClient()
        source = service_credential_token_source(workspace, sealed.app_service_credential)
    else:
        source = az_cli_token_source(azure.tenant_id)
    return ArmClient(
        AzureCredentials(tenant_id=azure.tenant_id, subscription_id=azure.subscription_id),
        azure.resource_group,
        token_source=source,
    )


def admin_password(environment: Mapping[str, str] | None = None) -> Callable[[], str]:
    """The servers' admin password, read when a lane first connects, never at import."""

    env = os.environ if environment is None else environment

    def read() -> str:
        bound = env.get(ADMIN_PASSWORD_ENV, "").strip()
        if bound:
            return bound
        path = Path(ADMIN_PASSWORD_FILE)
        if not _app_mode(env) and path.is_file():
            return path.read_text(encoding="utf-8").strip()
        raise AzureRuntimeError(
            f"the Azure servers' admin password is not bound: {ADMIN_PASSWORD_ENV} is empty")

    return read


def build_azure_safe_change_engine(
    manifest: DemoManifest,
    *,
    cleanup_only: bool = False,
    round_number: int = 2,
    environment: Mapping[str, str] | None = None,
    databricks_runner: Any | None = None,
    connector: Any = psycopg.AsyncConnection.connect,
    sleep: Callable[[float], Any] = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
    arm: ArmClient | None = None,
) -> Any:
    """Round 2's engine (or Round 3's source half): Lakebase's branch against Azure's restore."""

    from .bout_limit import BOUT_TIME_LIMIT_SECONDS
    from .safe_change import SafeChangeEngine, SafeChangeOwnershipScope
    from .safe_change_live import (
        DEFAULT_POLL_TIMEOUT_SECONDS,
        DEFAULT_RUN_TIMEOUT_SECONDS,
        LakebaseSafeChangeAdapter,
        LakebaseSafeChangeConfig,
        SafeChangeLiveConfigurationError,
        _positive_float,
    )

    env = os.environ if environment is None else environment
    azure = azure_section(manifest)
    if round_number not in (2, 3):
        raise SafeChangeLiveConfigurationError("Safe-change source round must be 2 or 3")
    if not cleanup_only and manifest.status != "ready":
        raise SafeChangeLiveConfigurationError(
            f"Owned demo manifest is {manifest.status.upper()}, not READY")
    if manifest.round_environments is None:
        raise SafeChangeLiveConfigurationError("the Azure installation's rounds are not sealed")
    round_key = "r2" if round_number == 2 else "r3"
    if round_key not in azure.servers:
        raise SafeChangeLiveConfigurationError(f"Round {round_number}'s Azure server is not sealed")
    app_mode = _app_mode(env)
    if app_mode:
        if manifest.round4 is None:
            raise SafeChangeLiveConfigurationError(
                "Databricks App runtime requires a sealed manifest v2 identity")
        dbx_profile = ""
        lakebase_user = manifest.round4.app_service_principal_client_id
    else:
        dbx_profile = manifest.databricks.profile
        lakebase_user = manifest.databricks.user
    scope = SafeChangeOwnershipScope(
        run_id=manifest.run_id,
        owner=manifest.owner,
        expires_at=manifest.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        cloud="azure",
        azure_subscription_id=azure.subscription_id,
        azure_region=azure.region,
    )
    lakebase = LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile=dbx_profile,
            source_endpoint=manifest.round_lakebase(round_number).endpoint_name,
            database=manifest.databricks.database,
            user=lakebase_user,
            expected_region=azure.region,
            # The AWS edition's tuning, from the same settings.
            control_timeout_seconds=_positive_float(
                env, "ANTI_DEMO_SAFE_CHANGE_CONTROL_TIMEOUT_SECONDS", 120.0),
            poll_timeout_seconds=_positive_float(
                env, "ANTI_DEMO_SAFE_CHANGE_POLL_TIMEOUT_SECONDS", DEFAULT_POLL_TIMEOUT_SECONDS),
            poll_interval_seconds=_positive_float(
                env, "ANTI_DEMO_SAFE_CHANGE_POLL_SECONDS", 5.0),
            connect_timeout_seconds=_positive_float(
                env, "ANTI_DEMO_SAFE_CHANGE_CONNECT_TIMEOUT_SECONDS", 30.0),
        ),
        runner=databricks_runner,
        connector=connector,
        sleep=sleep,
        clock=clock,
    )
    adapter_type = (
        AzurePostgresSafeChangeAdapter if round_number == 2 else AzurePostgresRecoveryAdapter
    )
    competitor = adapter_type(
        config_from_manifest(azure, round_key, database=manifest.databricks.database),
        arm=arm or app_arm_client(manifest, environment=env),
        password=admin_password(env),
        connector=connector,
        sleep=sleep,
        clock=clock,
        # Shared: a towel's cleanup runs on an engine of its own, and must know a restore the
        # bout sent is still on its way.
        restores=RESTORES_SENT,
    )
    # The AWS edition's bounds, from the same settings (`safe_change_live`'s engine builder).
    # Without them the engine's own defaults applied: an arm older than 60 s refused at the bell
    # while the manager's card still read armed for 180 s, and a lane ran 1800 s, not the bout's
    # 900 s limit (a Round 3 bout ran 1,175 s, 2026-10-20). Round 3 copies these from here.
    return SafeChangeEngine(
        scope=scope,
        lakebase=lakebase,
        competitors={CompetitorId.AZURE_POSTGRES: competitor},
        arm_ttl_seconds=_positive_float(env, "ANTI_DEMO_ARM_TTL_SECONDS", 180.0),
        preflight_timeout_seconds=_positive_float(
            env, "ANTI_DEMO_SAFE_CHANGE_PREFLIGHT_TIMEOUT_SECONDS", 180.0),
        run_timeout_seconds=_positive_float(
            env, "ANTI_DEMO_SAFE_CHANGE_RUN_TIMEOUT_SECONDS", BOUT_TIME_LIMIT_SECONDS),
        reset_timeout_seconds=_positive_float(
            env, "ANTI_DEMO_SAFE_CHANGE_RESET_TIMEOUT_SECONDS", DEFAULT_RUN_TIMEOUT_SECONDS),
    )


def build_azure_recovery_engine(
    manifest: DemoManifest,
    *,
    cleanup_only: bool = False,
    **options: Any,
) -> Any:
    """Round 3's engine: Lakebase's point-in-time branch against Azure's point-in-time restore."""

    from .recovery import RecoveryEngine
    from .recovery_live import LakebaseRecoveryAdapter

    safe_change = build_azure_safe_change_engine(
        manifest, cleanup_only=cleanup_only, round_number=3, **options)
    return RecoveryEngine(
        scope=safe_change.scope,
        lakebase=LakebaseRecoveryAdapter(safe_change.lakebase),
        competitors={
            CompetitorId.AZURE_POSTGRES: safe_change.competitors[CompetitorId.AZURE_POSTGRES]
        },
        arm_ttl_seconds=safe_change.arm_ttl_seconds,
        run_timeout_seconds=safe_change.run_timeout_seconds,
        reset_timeout_seconds=safe_change.reset_timeout_seconds,
    )


#: Round 4's writer tables on the r4 server: the AWS lane's ledger and view
#: (`round4_aws_lifecycle`), and the staging table the data flow writes before its guarded upsert
#: (`azure_dataflows`).
ROUND4_SCHEMA = "round4"
ROUND4_STAGING_TABLE = "model_score_staging"
#: Round 4's Delta source, as Data Factory's Delta Lake 2.4 reader can read it:
#: - classic checkpoints, because 2.4 can't read v2 ones (Phase 0, S-Z4);
#: - none of the reader features below. This metastore makes every new managed table with
#:   catalog-managed commits and vacuumProtocolCheck, and 2.4 reads neither (2026-10-19).
#: They're dropped, in this order, before anything syncs from the table. It stays MANAGED, and
#: writes don't add them back.
ROUND4_SOURCE_PROPERTIES = (
    "'delta.enableChangeDataFeed' = 'true', 'delta.checkpointPolicy' = 'classic', "
    "'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false'")
ADF_UNREADABLE_FEATURES = ("catalogManaged", "vacuumProtocolCheck", "v2Checkpoint")
#: Round 6's source on the r6 server, and its history's Unity Catalog table beside Lakebase's.
ROUND6_SCHEMA = "round6"
ROUND6_TABLE = "live_orders"
ROUND6_HISTORY_TABLE = "live_orders_azure_history"
#: The factory's linked services: Terraform's ADLS one, and the installer's one per server.
ADLS_LINKED_SERVICE = "ls-adls-installation"
POSTGRES_LINKED_SERVICES = {"r4": "ls-postgres-r4", "r6": "ls-postgres-r6"}


def server_connector(
    manifest: DemoManifest, key: str, *, environment: Mapping[str, str] | None = None
) -> Callable[[str], Any]:
    """An async `connect(application_name)` to one round's server, verified and SCRAM-only."""

    from .azure_safe_change import AZURE_POSTGRES_ROOTS, verifying_connector

    seal = azure_section(manifest).servers[key]
    password = admin_password(environment)
    connect = verifying_connector(psycopg.AsyncConnection.connect, AZURE_POSTGRES_ROOTS)

    async def open_connection(application_name: str) -> Any:
        return await connect(host=seal.host, port=seal.port, dbname=manifest.databricks.database,
                             user=seal.admin_login, password=password(),
                             application_name=application_name, connect_timeout=15,
                             autocommit=True)

    return open_connection


def history_table_full_name(manifest: DemoManifest) -> str:
    """Round 6's Azure history, registered in Unity Catalog beside Lakebase's destination."""

    if manifest.round6 is None:
        raise AzureRuntimeError("Round 6's Lakebase destination is not sealed")
    return (f"{manifest.round6.destination_catalog}.{manifest.round6.destination_schema}."
            f"{ROUND6_HISTORY_TABLE}")


class _SourceHead:
    """Round 4's source head version, read off the clock when a settling lane asks for it."""

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    async def head_version(self) -> int:
        version, _ = await self._adapter._source_head()
        return int(version)


def build_azure_round4_race_engine(manifest: DemoManifest, competitor: CompetitorId) -> Any:
    """Round 4's engine: Lakebase's synced table against the sealed ADF data flow.

    Lakebase races alone, the competitor's lane marked not supported, until the lane is sealed.
    """

    from psycopg import sql

    from .azure_adf_job import AdfDataFlowJob
    from .model_score_live import LiveModelScoreAdapter, build_model_score_engine
    from .round4_adf_lane import ADF_SETTLE_TIMEOUT_SECONDS, AdfWriterLane
    from .round4_race import Round4RaceEngine
    from .round4_race_live import DeltaSource, LakebaseSyncLane, PostgresReader, commit_entity

    if competitor != CompetitorId.AZURE_POSTGRES:
        raise AzureRuntimeError(f"the Azure edition races Azure PostgreSQL, not {competitor}")
    v1 = build_model_score_engine(manifest)
    adapter = v1.adapter
    assert isinstance(adapter, LiveModelScoreAdapter)
    source = DeltaSource(adapter)
    lanes: list[Any] = [LakebaseSyncLane(manifest, adapter, source)]
    seal = azure_section(manifest).round4
    if seal is not None:
        connect = server_connector(manifest, "r4")
        view = sql.Identifier(ROUND4_SCHEMA, "model_scores")
        ledger_table = sql.Identifier(ROUND4_SCHEMA, "model_score_ledger")

        async def open_reader() -> Any:
            async def reconnect() -> Any:
                return await connect("lakebase-anti-demo-round4-verifier")

            return PostgresReader(await reconnect(), view, reconnect=reconnect)

        async def ledger(entity_id: str) -> Mapping[str, Any] | None:
            connection = await connect("lakebase-anti-demo-round4-evidence")
            async with connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        sql.SQL("SELECT delta_table_id, delta_commit_version, deleted, applied_at "
                                "FROM {} WHERE entity_id = %s").format(ledger_table),
                        (entity_id,))
                    row = await cursor.fetchone()
            if row is None:
                return None
            return dict(zip(("delta_table_id", "delta_commit_version", "deleted",
                             "applied_at"), row, strict=True))

        job = AdfDataFlowJob(app_arm_client(manifest), seal.factory, seal.pipeline)
        lanes.append(AdfWriterLane(job, _SourceHead(adapter), open_reader, ledger,
                                   entity_id=commit_entity(None)))  # type: ignore[arg-type]
    return Round4RaceEngine(source, lanes, contract=v1.contract,
                            settle_timeout_seconds=ADF_SETTLE_TIMEOUT_SECONDS)


def build_azure_round6_race_engine(manifest: DemoManifest, competitor: CompetitorId) -> Any:
    """Round 6's engine: Lakebase's built-in change feed against the sealed ADF extract."""

    from .azure_adf_job import AdfDataFlowJob
    from .azure_dataflows import round6_flow
    from .live_orders import LiveOrdersLiveAdapter, build_live_orders_engine
    from .round6_adf_lane import AdfExtractLane, PostgresOrderStore, WarehouseHistory
    from .round6_race import Round6RaceEngine
    from .round6_race_live import LakebaseFeedLane

    if competitor != CompetitorId.AZURE_POSTGRES:
        raise AzureRuntimeError(f"the Azure edition races Azure PostgreSQL, not {competitor}")
    v1 = build_live_orders_engine(manifest)
    adapter = v1.adapter
    assert isinstance(adapter, LiveOrdersLiveAdapter)
    baseline = v1.contract.baseline
    lanes: list[Any] = [LakebaseFeedLane(adapter, baseline)]
    seal = azure_section(manifest).round6
    if seal is not None:
        arm = app_arm_client(manifest)
        from .azure_arm import DATA_FACTORY_API

        def write_flow(watermark: str) -> None:
            # Off the clock: Prepare writes the flow for the history's newest order, and the bell
            # is then one createRun.
            response = arm.request("PUT", f"{arm.factory_path(seal.factory)}/dataflows/"
                                   f"{seal.data_flow}?api-version={DATA_FACTORY_API}",
                                   round6_flow(
                                       postgres_linked_service=POSTGRES_LINKED_SERVICES["r6"],
                                       adls_linked_service=ADLS_LINKED_SERVICE,
                                       table=ROUND6_TABLE, watermark_utc=watermark,
                                       container=seal.container, folder=seal.folder,
                                       schema=ROUND6_SCHEMA))
            if response.status not in (200, 201):
                raise AzureRuntimeError(f"Round 6's data flow was not rewritten: {response.status}")

        lanes.append(AdfExtractLane(
            AdfDataFlowJob(arm, seal.factory, seal.pipeline),
            PostgresOrderStore(server_connector(manifest, "r6"), ROUND6_SCHEMA, ROUND6_TABLE),
            WarehouseHistory(adapter._statements, history_table_full_name(manifest)),
            write_flow,
        ))
    return Round6RaceEngine(lanes, baseline=baseline)


#: How long the App reuses its Service Bus token. UC mints them for about an hour, and minting one
#: per control event would put a Databricks API call on the bell path.
SERVICE_BUS_TOKEN_REUSE_SECONDS = 1800.0


def reused_token(source: Callable[[], str], *, seconds: float = SERVICE_BUS_TOKEN_REUSE_SECONDS,
                 clock: Callable[[], float] = time.monotonic) -> Callable[[], str]:
    """`source`'s token, reused for `seconds`, or until shortly before it expires if that is
    sooner (`token_reuse_seconds`): `az` may hand back one that expires in minutes."""

    held: list[tuple[str, float, float]] = []

    def token() -> str:
        if not held or clock() - held[0][1] >= held[0][2]:
            minted = source()
            held[:] = [(minted, clock(), token_reuse_seconds(minted, ceiling=seconds))]
        return held[0][0]

    return token


def app_service_bus_token_source(manifest: DemoManifest, *,
                                 environment: Mapping[str, str] | None = None,
                                 workspace: Any | None = None) -> Callable[[], str]:
    """The App's Service Bus token (Round 5's control sends), as `app_arm_client` mints ARM's."""

    from .azure_identity import SERVICE_BUS_SCOPE

    env = os.environ if environment is None else environment
    azure = azure_section(manifest)
    if _app_mode(env):
        sealed = azure.unity_catalog
        if sealed is None:
            raise AzureRuntimeError("the App's Azure service credential is not sealed yet")
        if workspace is None:
            from databricks.sdk import WorkspaceClient

            workspace = WorkspaceClient()
        source = service_credential_token_source(workspace, sealed.app_service_credential,
                                                 scope=SERVICE_BUS_SCOPE)
    else:
        source = az_cli_token_source(azure.tenant_id, scope=SERVICE_BUS_SCOPE)
    return reused_token(source)


def azure_round5_publisher(manifest: DemoManifest, *,
                           environment: Mapping[str, str] | None = None) -> Callable[..., Any]:
    """`publish_control` for Round 5's dispatcher: each event to its lane's Service Bus queue."""

    from .azure_round5_runner import QUEUE_ADDRESS
    from .azure_round5_transport import service_bus_publisher

    resources = manifest.require_azure_round5_resources()
    queues = {}
    for lane, address in (("lakebase", resources.lakebase_control_queue),
                          ("competitor", resources.competitor_control_queue)):
        match = QUEUE_ADDRESS.fullmatch(address)
        if match is None or match.group("namespace") != resources.service_bus_namespace:
            raise AzureRuntimeError(f"Round 5's {lane} queue is not on the sealed namespace")
        queues[lane] = match.group("queue")
    return service_bus_publisher(resources.service_bus_namespace, queues,
                                 app_service_bus_token_source(manifest, environment=environment))


def build_azure_round5_engine(manifest: DemoManifest, *, journal: Any, fence: Any,
                              fresh_lakebase_host: Any, resident_transport: Any,
                              environment: Mapping[str, str] | None = None) -> Any:
    """Round 5's engine on Azure: AWS's engine with Azure's setup and lane adapters."""

    from .azure_pgbouncer import AzurePgBouncer
    from .azure_round5_engine import build_azure_connection_spike_engine
    from .azure_safe_change import AZURE_POSTGRES_ROOTS

    env = os.environ if environment is None else environment
    arm = app_arm_client(manifest, environment=env)
    server = azure_section(manifest).servers["r5"]
    # The gate's client 1 is the server's admin, through PgBouncer, verify-full against the
    # sealed Azure roots, as the runners connect.
    pgbouncer = AzurePgBouncer(arm, server=server.server, host=server.host,
                               database=manifest.databricks.database, user=server.admin_login,
                               password=admin_password(env), ca_file=str(AZURE_POSTGRES_ROOTS))
    return build_azure_connection_spike_engine(
        manifest, arm=arm, run_script=arm.run_shell_script, pgbouncer=pgbouncer,
        journal=journal, fence=fence, fresh_lakebase_host=fresh_lakebase_host,
        resident_transport=resident_transport)
