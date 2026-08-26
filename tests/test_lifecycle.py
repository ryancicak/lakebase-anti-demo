import ast
import asyncio
import json
import re
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote, unquote

import botocore.session
import psycopg
import pytest
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber

from server import lifecycle
from server.lifecycle import (
    _LAKEBASE_APP_PROJECT_PERMISSION,
    ROUND4_DEFAULT_CATALOG,
    ROUND5_SSM_COMMAND_TIMEOUT_SECONDS,
    Check,
    _aws_resources_from_outputs,
    _canonical_iam_policy,
    _commit_round4_reseal,
    _complete_provision,
    _connect,
    _coordination_lakebase_binding,
    _create_or_get_round4_synced_table,
    _delete_databricks_app,
    _delete_round4_pipeline,
    _delete_round4_resources,
    _enable_round5_lakebase_native_login,
    _ensure_lakebase_app_roles,
    _ensure_lakebase_database,
    _ensure_round4_app_roles,
    _expected_aws_state_addresses,
    _grant_round4_uc_and_warehouse,
    _hydrate_aws_resources,
    _lakebase_scale_zero_check,
    _prepare_and_reassert_round5_aws_credentials,
    _prepare_round4_source_artifacts,
    _rds_ingress,
    _read_round4_baseline,
    _reconcile_legacy_round5_partial_state,
    _reconcile_round5_failed_cleanups,
    _refresh_operator_cidr,
    _require_round4_catalog,
    _require_round5_runner_idle,
    _required_round5_outputs,
    _required_round_tags,
    _reset_under_ring_lease,
    _round4_check,
    _round4_names,
    _round4_survivor_lines,
    _round4_synced_spec,
    _round5_aurora_cluster_resource_id,
    _round5_cleanup_ring_key,
    _round5_runtime_tag_inventory,
    _round_lakebase_binding,
    _terraform_environment,
    _validate_partial_aws_destroy_retry,
    _validate_round4_database_synced_table,
    _validate_round4_pipeline,
    _validate_round4_synced_table,
    cleanup,
    resume_provision,
    seed_identical_schema,
    setup,
)
from server.manifest import (
    AwsManifest,
    AwsResources,
    DatabricksManifest,
    DemoManifest,
    Round3Anchor,
    Round3AnchorLane,
    Round4Resources,
    Round5Resources,
    manifest_path,
)
from server.model_score import ModelScoreContract
from server.pipeline_power import PIPELINE_RATE_TOLERANCE, PIPELINE_USD_PER_DAY
from server.reconcile import (
    AURORA_WRITER,
    EC2_RUNNER,
    ORPHAN_EPHEMERAL,
    ORPHAN_FOREIGN_RUN,
    ORPHAN_UNEXPECTED,
    RDS_INSTANCE,
    TAG_RUN_ID,
    ObservedResource,
    ephemeral_artifact_ids,
    reconcile,
)
from server.targets import ConnectionMaterial, TargetNotArmedError


@pytest.fixture(autouse=True)
def isolated_lifecycle_manifest(monkeypatch, tmp_path):
    """Never let a lifecycle unit test resolve the live default manifest."""

    target = tmp_path / "manifest.json"
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(target))
    return target


def test_lifecycle_tests_resolve_only_the_isolated_manifest(
    isolated_lifecycle_manifest,
) -> None:
    assert manifest_path() == isolated_lifecycle_manifest.resolve()


def test_fresh_v7_install_derives_seven_unique_workspace_projects() -> None:
    def staged(installation_id: str) -> SimpleNamespace:
        return SimpleNamespace(
            installation_id=installation_id,
            round_environments=None,
            coordination_lakebase=None,
            databricks=SimpleNamespace(
                project_id="legacy",
                endpoint_name=("projects/legacy/branches/production/endpoints/primary"),
            ),
        )

    first = staged("018f1f5a-7b2e-7ca4-9d21-8d2df0eb1201")
    second = staged("018f1f5a-7b2e-7ca4-9d21-8d2df0eb1202")
    first_projects = {
        *(_round_lakebase_binding(first, number).project_id for number in range(1, 7)),
        _coordination_lakebase_binding(first).project_id,
    }
    second_projects = {
        *(_round_lakebase_binding(second, number).project_id for number in range(1, 7)),
        _coordination_lakebase_binding(second).project_id,
    }

    assert len(first_projects) == 7
    assert first_projects.isdisjoint(second_projects)


def test_lakebase_database_setup_restores_round_provider_database(monkeypatch) -> None:
    observed_databases: list[str] = []

    class Provider:
        database = "anti_demo"

        async def connection_material(self) -> ConnectionMaterial:
            observed_databases.append(self.database)
            return ConnectionMaterial(
                host="lakebase.test",
                port=5432,
                database=self.database,
                user="setup-user",
                password="setup-password",
            )

    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def execute(self, *_):
            return None

        async def fetchone(self):
            return (1,)

    class Connection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def cursor(self):
            return Cursor()

    async def fake_connect(material, *, autocommit=False):
        assert material.database == "postgres"
        assert autocommit is True
        return Connection()

    provider = Provider()
    monkeypatch.setattr("server.lifecycle._connect", fake_connect)

    asyncio.run(_ensure_lakebase_database("anti_demo", provider))

    assert observed_databases == ["postgres"]
    assert provider.database == "anti_demo"


def test_round4_baseline_reads_from_the_round4_project(monkeypatch) -> None:
    requested: list[tuple[int, str]] = []

    class Provider:
        async def connection_material(self) -> ConnectionMaterial:
            return ConnectionMaterial(
                host="round4.test",
                port=5432,
                database="anti_demo",
                user="setup-user",
                password="setup-password",
            )

    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def execute(self, *_):
            return None

        async def fetchone(self):
            return ("customer-0001", 0.25, "risk-v0", "round4-baseline")

    class Connection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def cursor(self):
            return Cursor()

    def fake_provider(manifest, round_id, *, database=None):
        del manifest
        requested.append((round_id, database))
        return Provider()

    async def fake_connect(material, *, autocommit=False):
        assert material.host == "round4.test"
        assert autocommit is False
        return Connection()

    monkeypatch.setattr("server.lifecycle.apply_manifest_environment", lambda _: None)
    monkeypatch.setattr("server.lifecycle._round_lakebase_provider", fake_provider)
    monkeypatch.setattr("server.lifecycle._connect", fake_connect)

    row = asyncio.run(
        _read_round4_baseline(
            SimpleNamespace(),
            {"online_schema": "anti_demo_online_test"},
        )
    )

    assert requested == [(4, "anti_demo")]
    assert row == ("customer-0001", 0.25, "risk-v0", "round4-baseline")


def test_v7_seed_covers_each_round_scoped_database_arena(monkeypatch) -> None:
    requested: list[tuple[str, int]] = []
    applied: list[str] = []

    class Provider:
        def __init__(self, name: str) -> None:
            self.name = name

        async def connection_material(self) -> ConnectionMaterial:
            return ConnectionMaterial(
                host=f"{self.name}.test",
                port=5432,
                database="anti_demo",
                user="setup-user",
                password="setup-password",
            )

    def provider_factory(lane: str):
        def build(manifest, round_id):
            del manifest
            requested.append((lane, round_id))
            return Provider(f"{lane}-r{round_id}")

        return build

    async def fake_ensure(database, provider):
        assert database == "anti_demo"
        assert provider.name.startswith("lakebase-")

    async def fake_apply(material):
        applied.append(material.host.removesuffix(".test"))
        return "170000"

    monkeypatch.setattr("server.lifecycle.apply_manifest_environment", lambda _: None)
    monkeypatch.setattr("server.lifecycle._round_lakebase_provider", provider_factory("lakebase"))
    monkeypatch.setattr("server.lifecycle._round_aurora_provider", provider_factory("aurora"))
    monkeypatch.setattr("server.lifecycle._round_rds_provider", provider_factory("rds"))
    monkeypatch.setattr("server.lifecycle._ensure_lakebase_database", fake_ensure)
    monkeypatch.setattr("server.lifecycle._apply_schema", fake_apply)

    versions = asyncio.run(
        seed_identical_schema(
            SimpleNamespace(
                round_environments={},
                databricks=SimpleNamespace(database="anti_demo"),
            )
        )
    )

    # Lakebase and Aurora are seeded for all four AWS rounds. RDS is seeded for
    # three: Round 1 stands up no RDS instance, because its RDS lane refuses to
    # enter on engine semantics and is never timed, so there is nothing there to
    # seed. The three lanes are therefore no longer the same width, which is the
    # property the cumulative indexing in seed_identical_schema depends on.
    expected = [
        (lane, round_id) for lane in ("lakebase", "aurora") for round_id in (1, 2, 3, 5)
    ] + [("rds", round_id) for round_id in (2, 3, 5)]
    assert requested == expected
    assert ("rds", 1) not in requested
    assert applied == [f"{lane}-r{round_id}" for lane, round_id in expected]
    assert versions == ("170000", "170000", "170000")


def make_manifest(*, status: str = "ready") -> DemoManifest:
    return DemoManifest(
        run_id="ad-test-001",
        owner="operator@databricks.com",
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=24),
        status=status,
        aws=AwsManifest(
            profile="sandbox-admin",
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state="/tmp/anti-demo-test.tfstate",
            resources=AwsResources(
                aurora_cluster_id="anti-demo-aurora",
                aurora_writer_instance_id="anti-demo-aurora-writer",
                aurora_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:a",
                rds_instance_id="anti-demo-rds",
                rds_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:r",
                security_group_id="sg-aurora",
                rds_security_group_id="sg-rds",
                db_subnet_group_name="anti-demo-subnets",
            ),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-001",
            endpoint_name="projects/ad-test-001/branches/production/endpoints/primary",
            user="operator@databricks.com",
        ),
        schema_sha256="abc123",
    )


def ready_round5_stub(**updates) -> Round5Resources:
    values = {
        "aurora_direct_host": "aurora.example.com",
        "aurora_cluster_id": "anti-demo-aurora",
        "aurora_cluster_resource_id": "cluster-RESOURCE",
        "aurora_writer_instance_id": "anti-demo-aurora-writer",
        "aurora_master_secret_arn": ("arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora"),
        "aurora_credential_sha256": "a" * 64,
        "proxy_service_role_arn": ("arn:aws:iam::123456789012:role/anti-demo-round5-proxy"),
        "proxy_service_policy_name": "r5-proxy-secrets-test",
        "aurora_proxy_secret_arn": (
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-proxy"
        ),
        "rds_proxy_secret_arn": ("arn:aws:secretsmanager:us-west-2:123456789012:secret:rds-proxy"),
    }
    values.update(updates)
    return Round5Resources.model_construct(**values)


def setup_connection_material() -> ConnectionMaterial:
    return ConnectionMaterial(
        host="setup.test",
        port=5432,
        database="postgres",
        user="setup-user",
        password="setup-password",
    )


def attach_round4(manifest: DemoManifest) -> dict[str, str]:
    names = _round4_names(manifest)
    pipeline_id = "f2af6c88-7da3-40cf-881f-7971e50a6b18"
    manifest.round4 = Round4Resources(
        warehouse_id="0123456789abcdef",
        setup_principal=manifest.databricks.user,
        app_service_principal_client_id="11111111-2222-3333-4444-555555555555",
        source_table_full_name=names["source_table"],
        storage_catalog=names["catalog"],
        storage_schema=names["storage_schema"],
        synced_table_id=names["synced_table_id"],
        synced_table_resource_name=names["resource_name"],
        synced_table_uid="6fb5fb44-954a-49ce-a751-2a9fbc0686c2",
        pipeline_id=pipeline_id,
        project_uid="project-uid-001",
        branch_uid="branch-uid-001",
        physical_database="anti_demo",
        physical_schema=names["online_schema"],
        physical_table="model_scores",
        branch=names["branch"],
        endpoint_name=names["endpoint_name"],
        contract_sha256=ModelScoreContract(
            pipeline_id=pipeline_id,
            source_table=names["source_table"],
            synced_table=f"anti_demo.{names['online_schema']}.model_scores",
        ).sha256,
    )
    manifest.manifest_version = 2
    return names


def round4_payload(manifest: DemoManifest, names: dict[str, str]) -> dict[str, object]:
    assert manifest.round4 is not None
    return {
        "name": names["resource_name"],
        "synced_table_id": names["synced_table_id"],
        "uid": manifest.round4.synced_table_uid,
        "spec": _round4_synced_spec(names),
        "status": {
            "project": f"projects/{manifest.run_id}",
            "pipeline_id": manifest.round4.pipeline_id,
            "detailed_state": "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
            "last_processed_commit_version": 7,
        },
    }


def database_round4_payload(
    manifest: DemoManifest, names: dict[str, str], *, version: int = 7
) -> dict[str, object]:
    assert manifest.round4 is not None
    return {
        "name": names["synced_table_id"],
        "effective_database_project_id": manifest.round4.project_uid,
        "effective_database_branch_id": manifest.round4.branch_uid,
        "effective_logical_database_name": "anti_demo",
        "spec": {
            "source_table_full_name": names["source_table"],
            "primary_key_columns": ["entity_id"],
            "scheduling_policy": "CONTINUOUS",
        },
        "data_synchronization_status": {
            "pipeline_id": manifest.round4.pipeline_id,
            "detailed_state": "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE",
            "continuous_update_status": {
                "last_processed_commit_version": version,
                "timestamp": "2026-08-18T12:00:02Z",
            },
            "last_sync": {
                "sync_start_timestamp": "2026-08-18T12:00:00Z",
                "sync_end_timestamp": "2026-08-18T12:00:02Z",
                "delta_table_sync_info": {
                    "delta_commit_version": version,
                    "delta_commit_timestamp": "2026-08-18T11:59:59Z",
                },
            },
        },
    }


def pipeline_payload(manifest: DemoManifest, names: dict[str, str]) -> dict[str, object]:
    assert manifest.round4 is not None
    return {
        "pipeline_id": manifest.round4.pipeline_id,
        "creator_user_name": manifest.round4.setup_principal,
        "state": "RUNNING",
        "spec": {
            "id": manifest.round4.pipeline_id,
            "catalog": names["catalog"],
            "schema": names["online_schema"],
            "continuous": True,
            "pipeline_type": "DATABASE_TABLE_SYNC",
            "managed_definition": {
                "database_table_sync": {
                    "sinks": [
                        {
                            "src_table": names["source_table"],
                            "dest_table": (f"anti_demo.{names['online_schema']}.model_scores"),
                            "dest_table_uc_name": names["synced_table_id"],
                            "dest_table_id": manifest.round4.synced_table_uid,
                            "primary_key": ["entity_id"],
                            "creator": manifest.round4.setup_principal,
                            "online_catalog_name": names["catalog"],
                        }
                    ]
                }
            },
        },
    }


async def test_setup_connection_retries_transient_failure_then_preserves_autocommit(
    monkeypatch, capsys
) -> None:
    connection = object()
    connect_calls: list[dict[str, object]] = []
    sleeps: list[float] = []

    class FakeAsyncConnection:
        @staticmethod
        async def connect(**kwargs):
            connect_calls.append(kwargs)
            if len(connect_calls) == 1:
                raise psycopg.errors.CannotConnectNow("endpoint is starting")
            return connection

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("server.lifecycle.psycopg.AsyncConnection", FakeAsyncConnection)
    monkeypatch.setattr("server.lifecycle.asyncio.sleep", fake_sleep)

    result = await _connect(setup_connection_material(), autocommit=True)

    assert result is connection
    assert sleeps == [2]
    assert [call["connect_timeout"] for call in connect_calls] == [15, 15]
    assert all(call["autocommit"] is True for call in connect_calls)
    assert capsys.readouterr().out == (
        "WAIT PostgreSQL setup connection not ready; retrying in 2s\n"
    )


async def test_setup_connection_fatal_sqlstate_fails_without_retry(monkeypatch, capsys) -> None:
    connect_calls = 0

    class FakeAsyncConnection:
        @staticmethod
        async def connect(**_):
            nonlocal connect_calls
            connect_calls += 1
            raise psycopg.errors.InvalidPassword("password=must-not-leak")

    monkeypatch.setattr("server.lifecycle.psycopg.AsyncConnection", FakeAsyncConnection)

    with pytest.raises(
        RuntimeError,
        match=r"^PostgreSQL setup connection failed \(SQLSTATE 28P01\)$",
    ):
        await _connect(setup_connection_material())

    assert connect_calls == 1
    assert capsys.readouterr().out == ""


async def test_setup_connection_exhaustion_respects_monotonic_deadline(monkeypatch) -> None:
    now = 0.0
    connect_calls = 0
    sleeps: list[float] = []

    class FakeAsyncConnection:
        @staticmethod
        async def connect(**_):
            nonlocal connect_calls
            connect_calls += 1
            raise psycopg.OperationalError("host and password must not leak")

    async def fake_sleep(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay

    monkeypatch.setattr("server.lifecycle.psycopg.AsyncConnection", FakeAsyncConnection)

    class FakeTime:
        @staticmethod
        def monotonic() -> float:
            return now

    monkeypatch.setattr("server.lifecycle.time", FakeTime)
    monkeypatch.setattr("server.lifecycle.asyncio.sleep", fake_sleep)

    with pytest.raises(
        RuntimeError,
        match="^PostgreSQL setup connection did not become ready within 120 seconds$",
    ):
        await _connect(setup_connection_material())

    assert now == 120
    assert connect_calls == 14
    assert sleeps[:4] == [2, 4, 8, 10]
    assert max(sleeps) == 10


def attach_anchor(manifest: DemoManifest) -> None:
    reset_at = manifest.last_reset_at or datetime.now(UTC)
    manifest.last_reset_at = reset_at
    manifest.round3_anchor = Round3Anchor(
        run_id=manifest.run_id,
        owner=manifest.owner,
        aws_account_id=manifest.aws.account_id,
        aws_region=manifest.aws.region,
        contract_sha256="contract-hash",
        schema_sha256=manifest.schema_sha256,
        last_reset_at=reset_at,
        lakebase=Round3AnchorLane(
            provider="lakebase",
            source_id=manifest.databricks.endpoint_name,
            recovery_at=reset_at + timedelta(seconds=2),
        ),
        aurora=Round3AnchorLane(
            provider="aurora",
            source_id=manifest.aws.resources.aurora_cluster_id,
            recovery_at=reset_at + timedelta(seconds=3),
        ),
        rds=Round3AnchorLane(
            provider="rds",
            source_id=manifest.aws.resources.rds_instance_id,
            recovery_at=reset_at + timedelta(seconds=4),
        ),
    )


class FakeRdsClient:
    def __init__(self, *, publicly_accessible: bool = True) -> None:
        self.publicly_accessible = publicly_accessible

    def describe_db_instances(self, DBInstanceIdentifier: str):
        assert DBInstanceIdentifier == "anti-demo-rds"
        return {
            "DBInstances": [
                {
                    "PubliclyAccessible": self.publicly_accessible,
                    "VpcSecurityGroups": [{"VpcSecurityGroupId": "sg-rds", "Status": "active"}],
                }
            ]
        }


class FakeEc2Client:
    def __init__(self, *, ingress: list[dict[str, object]] | None = None) -> None:
        self.ingress = ingress or []

    def describe_security_groups(self, GroupIds: list[str]):
        assert GroupIds == ["sg-rds"]
        return {"SecurityGroups": [{"IpPermissions": self.ingress}]}


class FakeAwsSession:
    def __init__(
        self,
        *,
        ingress: list[dict[str, object]] | None = None,
        publicly_accessible: bool = True,
    ) -> None:
        self.rds = FakeRdsClient(publicly_accessible=publicly_accessible)
        self.ec2 = FakeEc2Client(ingress=ingress)

    def client(self, service: str):
        return {"rds": self.rds, "ec2": self.ec2}[service]


def test_rds_network_check_requires_public_instance_with_exact_operator_ingress(
    monkeypatch,
) -> None:
    ingress = [
        {
            "IpProtocol": "tcp",
            "FromPort": 5432,
            "ToPort": 5432,
            "IpRanges": [{"CidrIp": "203.0.113.10/32"}],
        }
    ]
    monkeypatch.setattr(
        "server.lifecycle.boto3.Session",
        lambda **kwargs: FakeAwsSession(ingress=ingress),
    )

    check = _rds_ingress(make_manifest())

    assert check.ok is True
    assert check.name == "rds_ingress"
    assert check.detail == "203.0.113.10/32"

    manifest = make_manifest()
    attach_round4(manifest)
    manifest.round5 = ready_round5_stub(runner_security_group_id="sg-runner")
    manifest.manifest_version = 5
    ingress[0]["UserIdGroupPairs"] = [{"GroupId": "sg-runner"}]
    check = _rds_ingress(manifest)
    assert check.ok is True

    ingress[0]["Ipv6Ranges"] = [{"CidrIpv6": "::/0"}]
    check = _rds_ingress(manifest)
    assert check.ok is False


def test_rds_network_check_rejects_broad_or_incomplete_ingress(monkeypatch) -> None:
    monkeypatch.setattr(
        "server.lifecycle.boto3.Session",
        lambda **kwargs: FakeAwsSession(
            ingress=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5432,
                    "ToPort": 5432,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                }
            ]
        ),
    )

    check = _rds_ingress(make_manifest())

    assert check.ok is False


def test_operator_cidr_refresh_rebinds_only_after_ownership_verification(monkeypatch) -> None:
    manifest = make_manifest()
    saved: list[str] = []
    monkeypatch.setattr(
        "server.lifecycle.detect_operator_cidr",
        lambda: "203.0.113.11/32",
    )
    monkeypatch.setattr(
        "server.lifecycle._aws_ownership",
        lambda candidate: Check("aws_ownership", candidate is manifest, "owned"),
    )
    monkeypatch.setattr(
        "server.lifecycle.save_manifest",
        lambda candidate: saved.append(candidate.aws.operator_cidr),
    )

    _refresh_operator_cidr(manifest)

    assert manifest.aws.operator_cidr == "203.0.113.11/32"
    assert saved == ["203.0.113.11/32"]


def test_operator_cidr_refresh_fails_closed_when_ownership_is_not_verified(
    monkeypatch,
) -> None:
    """Unverified ownership refuses -- and refuses on an answer, not on an error.

    `_refresh_operator_cidr` refuses when ownership fails *and*
    `_sealed_databases_absent` reports the sealed databases are still there. Only
    the first of those was stubbed here, so the second reached a real RDS client:
    this was the one test in the suite that got past every containment and out to
    the wire. What it did there decided nothing. `_sealed_databases_absent`
    catches bare `Exception` and answers False, which is the refusing answer, so
    a missing profile produced the same refusal a live present cluster would --
    and the assertion below held whatever the code under test did.

    Stubbed now, and the describes are counted, so the refusal has to come from
    AWS saying the cluster is still there. The count also pins the short-circuit:
    one surviving database is enough, and the second describe is never issued.
    """

    manifest = make_manifest()
    described: list[str] = []

    class PresentRdsClient:
        """An account that still holds every sealed database."""

        def describe_db_clusters(self, DBClusterIdentifier: str):
            described.append(DBClusterIdentifier)
            return {"DBClusters": [{"DBClusterIdentifier": DBClusterIdentifier}]}

        def describe_db_instances(self, DBInstanceIdentifier: str):
            described.append(DBInstanceIdentifier)
            return {"DBInstances": [{"DBInstanceIdentifier": DBInstanceIdentifier}]}

    monkeypatch.setattr(
        "server.lifecycle.detect_operator_cidr",
        lambda: "203.0.113.11/32",
    )
    monkeypatch.setattr(
        "server.lifecycle._aws_ownership",
        lambda candidate: Check("aws_ownership", False, "not owned"),
    )
    monkeypatch.setattr(
        "server.lifecycle._aws_session",
        lambda candidate: SimpleNamespace(client=lambda _service: PresentRdsClient()),
    )
    monkeypatch.setattr(
        "server.lifecycle.save_manifest",
        lambda candidate: pytest.fail("an unverified owner must not rebind"),
    )

    with pytest.raises(RuntimeError, match="Refusing to change database ingress"):
        _refresh_operator_cidr(manifest)

    assert described == [manifest.aws.resources.aurora_cluster_id]
    assert manifest.aws.operator_cidr == "203.0.113.10/32"


def test_terraform_outputs_must_fully_identify_owned_resources() -> None:
    outputs = {
        "aurora_cluster_id": "anti-demo-aurora",
        "aurora_writer_instance_id": "anti-demo-aurora-writer",
        "aurora_secret_arn": "aurora-secret",
        "rds_instance_id": "anti-demo-rds",
        "rds_secret_arn": "rds-secret",
        "aurora_security_group_id": "sg-aurora",
        "rds_security_group_id": "sg-rds",
        "db_subnet_group_name": "anti-demo-subnets",
    }

    resources = _aws_resources_from_outputs(outputs)

    assert resources.rds_instance_id == "anti-demo-rds"
    assert resources.rds_security_group_id == "sg-rds"
    # The Aurora mirror is still required: Round 1 races Aurora, so an installation
    # that cannot identify its cluster is not identifiable at all.
    with pytest.raises(RuntimeError, match="aurora_cluster_id"):
        _aws_resources_from_outputs({**outputs, "aurora_cluster_id": None})
    with pytest.raises(RuntimeError, match="security_group_id"):
        _aws_resources_from_outputs({**outputs, "aurora_security_group_id": None})
    # The RDS mirror is not, because Round 1 stands up no RDS instance and those
    # outputs are null by construction. They must come back empty rather than
    # borrowing another round's instance, which is what would make the seal
    # describe a resource Round 1 does not have.
    without_rds = _aws_resources_from_outputs(
        {
            **outputs,
            "rds_instance_id": None,
            "rds_secret_arn": None,
            "rds_security_group_id": None,
        }
    )
    assert without_rds.rds_instance_id == ""
    assert without_rds.rds_secret_arn == ""
    assert without_rds.rds_security_group_id == ""
    assert without_rds.aurora_cluster_id == "anti-demo-aurora"


def test_round5_outputs_require_static_proxy_role_and_secret_bindings() -> None:
    output_names = {
        "aurora_direct_host": "round5_aurora_direct_host",
        "aurora_cluster_id": "aurora_cluster_id",
        "aurora_cluster_resource_id": "round5_aurora_cluster_resource_id",
        "aurora_writer_instance_id": "aurora_writer_instance_id",
        "aurora_master_secret_arn": "aurora_secret_arn",
        "rds_direct_host": "round5_rds_direct_host",
        "rds_master_secret_arn": "rds_secret_arn",
        "rds_resource_id": "round5_rds_resource_id",
        "vpc_id": "vpc_id",
        "proxy_subnet_ids": "subnet_ids",
        "control_role_arn": "round5_control_role_arn",
        "control_role_trusted_principal_arn": "round5_app_principal_arn",
        "proxy_service_role_arn": "round5_proxy_service_role_arn",
        "proxy_service_policy_name": "round5_proxy_service_policy_name",
        "aurora_proxy_secret_arn": "round5_aurora_proxy_secret_arn",
        "rds_proxy_secret_arn": "round5_rds_proxy_secret_arn",
        "runner_permissions_boundary_arn": "round5_runner_permissions_boundary_arn",
        "runner_instance_id": "round5_runner_instance_id",
        "runner_instance_profile_arn": "round5_runner_instance_profile_arn",
        "runner_role_arn": "round5_runner_role_arn",
        "runner_subnet_id": "round5_runner_subnet_id",
        "runner_security_group_id": "round5_runner_security_group_id",
        "runner_egress_rule_id": "round5_runner_egress_rule_id",
        "bout_name_prefix": "round5_bout_name_prefix",
        "ownership_tags": "round5_bout_base_tags",
    }
    outputs = {name: f"value-{field}" for field, name in output_names.items()}
    outputs["subnet_ids"] = ["subnet-a", "subnet-b"]
    outputs["round5_bout_base_tags"] = {"managed-by": "round5-lifecycle"}

    required = _required_round5_outputs(outputs)

    assert required["proxy_service_role_arn"] == "value-proxy_service_role_arn"
    assert required["aurora_proxy_secret_arn"] == "value-aurora_proxy_secret_arn"
    assert "per_bout_role_boundary_arn" not in required
    assert "secret_name_prefix" not in required
    with pytest.raises(RuntimeError, match="rds_proxy_secret_arn"):
        _required_round5_outputs({**outputs, "round5_rds_proxy_secret_arn": None})


def test_round5_provisioning_tags_use_installation_scope_before_v7_commit() -> None:
    manifest = make_manifest().model_copy(
        update={
            "installation_id": "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1",
            "manifest_version": 2,
        }
    )

    tags = _required_round_tags(manifest, "r5")

    assert tags["anti-demo-installation-slug"] == "ib3beef6697cc1d6dce31-r5"
    assert tags["anti-demo-round"] == "r5"


def test_round5_inventory_distinguishes_static_terraform_role_from_bout_roles(
    monkeypatch,
) -> None:
    manifest = make_manifest().model_copy(
        update={"installation_id": "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"}
    )
    prefix = "ib3beef6697cc1d6dce31-r5-"
    control_role_arn = f"arn:aws:iam::123456789012:role/{prefix}exec-static"
    static_tags = _required_round_tags(manifest, "r5")
    static_tags.pop("Owner")
    policy_names = [f"{prefix}secret-terraform-static"]

    # One secret carrying this run's ownership tags and already inside its
    # deletion recovery window. `list_secrets` is called with
    # `IncludePlannedDeletion=True` so the name collision stays visible, but a
    # secret on its way out is not a live per-bout add-on -- and everything this
    # inventory returns makes `_require_round5_clean_baseline` raise, so counting
    # it would refuse every teardown of this installation until the window
    # elapsed, over a resource that no longer exists and is not billing.
    scheduled_for_deletion = {
        "Name": f"{prefix}{'f' * 16}",
        "ARN": f"arn:aws:secretsmanager:us-west-2:123456789012:secret:{prefix}ghost",
        "DeletedDate": "2026-08-23T20:17:00+00:00",
        "Tags": [
            {"Key": "anti-demo-run-id", "Value": manifest.run_id},
            {"Key": "managed-by", "Value": "round5-lifecycle"},
        ],
    }

    class EmptySecrets:
        def list_secrets(self, **kwargs):
            return {"SecretList": [scheduled_for_deletion]}

    class EmptyEc2:
        def describe_security_groups(self, **kwargs):
            return {"SecurityGroups": []}

        def describe_security_group_rules(self, **kwargs):
            return {"SecurityGroupRules": []}

    class EmptyRds:
        def describe_db_proxies(self, **kwargs):
            return {"DBProxies": []}

    class StaticIam:
        def list_roles(self, **kwargs):
            return {
                "Roles": [
                    {
                        "Arn": control_role_arn,
                        "RoleName": f"{prefix}exec-static",
                        "Tags": [
                            {"Key": key, "Value": value}
                            for key, value in static_tags.items()
                        ],
                    }
                ],
                "IsTruncated": False,
            }

        def list_role_policies(self, **kwargs):
            return {"PolicyNames": policy_names}

    clients = {
        "secretsmanager": EmptySecrets(),
        "ec2": EmptyEc2(),
        "rds": EmptyRds(),
        "iam": StaticIam(),
    }
    session = SimpleNamespace(client=lambda name: clients[name])
    ownership = SimpleNamespace(
        as_aws_tags=lambda: {**static_tags, "managed-by": "round5-lifecycle"}
    )
    sealed = SimpleNamespace(
        ownership_tags=ownership,
        bout_name_prefix=prefix,
        secret_name_prefix=None,
        vpc_id="vpc-0123456789abcdef0",
        runner_security_group_id="sg-0123456789abcdef0",
        control_role_arn=control_role_arn,
        runner_role_arn="arn:aws:iam::123456789012:role/static-runner",
        proxy_service_role_arn="arn:aws:iam::123456789012:role/static-proxy",
    )
    candidate = SimpleNamespace(
        round5_ready=True,
        require_round5_resources=lambda: sealed,
        run_id=manifest.run_id,
        owner=manifest.owner,
        expires_at=manifest.expires_at,
        installation_id=manifest.installation_id,
        aws=SimpleNamespace(
            resources=SimpleNamespace(rds_security_group_id="sg-1123456789abcdef0")
        ),
    )
    monkeypatch.setattr("server.lifecycle._aws_session", lambda _: session)

    assert _round5_runtime_tag_inventory(candidate) == []

    # And the same secret without the deletion date is residue, so the skip
    # above is reading `DeletedDate` and not simply failing to see the entry.
    live_secret = {key: value for key, value in scheduled_for_deletion.items()}
    del live_secret["DeletedDate"]
    scheduled_for_deletion.clear()
    scheduled_for_deletion.update(live_secret)
    with pytest.raises(RuntimeError, match="ownership tags differ"):
        _round5_runtime_tag_inventory(candidate)
    scheduled_for_deletion["DeletedDate"] = "2026-08-23T20:17:00+00:00"

    bout_policy = f"{prefix}{'0' * 16}-runner-secret"
    policy_names.append(bout_policy)
    assert _round5_runtime_tag_inventory(candidate) == [
        f"iam-inline:static-runner/{bout_policy}"
    ]
    policy_names.remove(bout_policy)

    static_tags.pop("anti-demo-round")
    with pytest.raises(RuntimeError, match="static Terraform ownership tags differ"):
        _round5_runtime_tag_inventory(candidate)


def test_round5_aws_credentials_are_prepared_before_static_secrets_are_reasserted(
    monkeypatch,
) -> None:
    requests: list[dict[str, object]] = []

    def request(*args, **kwargs):
        del args
        payload = kwargs["payload"]
        requests.append(payload)
        return (
            {"credential_sha256": ("a" if payload["lane_id"] == "aurora" else "b") * 64}
            if payload["action"] == "prepare_rds_baseline"
            else {}
        )

    monkeypatch.setattr("server.lifecycle._round5_setup_request", request)
    common = {
        "protocol": "connection-spike-setup-v1",
        "bout_id": "baseline-test",
        "port": 5432,
        "dbname": "anti_demo",
        "username": "anti_demo_burst",
        "trust_bundle_path": "/opt/lakebase-anti-demo/round5/round5-ca.pem",
        "trust_bundle_sha256": "c" * 64,
    }

    digests = _prepare_and_reassert_round5_aws_credentials(
        object(),
        runner_instance_id="i-runner",
        common=common,
        lanes=(
            ("aurora", "aurora.example", "master-a", "proxy-secret-a"),
            ("rds", "rds.example", "master-r", "proxy-secret-r"),
        ),
    )

    assert [item["action"] for item in requests] == [
        "prepare_rds_baseline",
        "reassert_rds_credentials",
        "prepare_rds_baseline",
        "reassert_rds_credentials",
    ]
    assert requests[1]["destination_secret_arn"] == "proxy-secret-a"
    assert requests[3]["destination_secret_arn"] == "proxy-secret-r"
    assert digests == {"aurora": "a" * 64, "rds": "b" * 64}


def test_the_cleanup_runner_idle_probe_is_a_command_ssm_would_accept(monkeypatch) -> None:
    """The gate on teardown, driven through the real SSM service model.

    This probe is the last thing standing between `antidemo cleanup` and a
    billing fleet, and it shipped asking for `TimeoutSeconds=15` against a
    parameter botocore models with `min=30`. Every call raised
    `ParamValidationError` before the request was signed, so cleanup could not
    finish on any installation with a Round 5 runner -- and the check it guards
    would have passed.

    Nothing caught it because a hand-written fake `send_command` takes whatever
    keyword arguments a test hands it and reports success. `Stubber` does not:
    it answers on `before-call`, which `BaseClient._make_api_call` reaches only
    after `_convert_to_request_dict` has run the same parameter validation a
    genuine call runs, so an out-of-range value raises here exactly as it would
    against AWS. No credential is resolved and no packet is built -- see the
    note in `conftest.py`.

    `TimeoutSeconds` is deliberately left unpinned. Pinning it would move the
    failure to `Stubber`'s expected-parameter assertion on
    `before-parameter-build`, which fires *before* validation, and the test
    would then be making a claim about one blessed number rather than about what
    the service accepts. The floor is read off botocore's own model below rather
    than copied from the documentation, so it tracks AWS if AWS moves it.
    """

    # A placeholder in this tree's sequential-hex convention. Nothing here reads
    # the value -- it is threaded from the manifest into `InstanceIds` and
    # compared against itself -- so only the length and the `i-` prefix matter,
    # and a real instance ID would be refused by
    # tests/test_no_live_identifiers_committed.py.
    instance_id = "i-0123456789abcdef0"
    command_id = "11111111-2222-4333-8444-555555555555"
    client = botocore.session.Session(
        session_vars={"profile": (None, None, None, None)}
    ).create_client(
        "ssm",
        region_name="us-west-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    manifest = SimpleNamespace(
        round5_ready=True,
        require_round5_resources=lambda: SimpleNamespace(runner_instance_id=instance_id),
    )
    monkeypatch.setattr(
        "server.lifecycle._aws_session",
        lambda _: SimpleNamespace(client=lambda _service: client),
    )

    with Stubber(client) as stubber:
        stubber.add_response(
            "send_command",
            {"Command": {"CommandId": command_id}},
            {
                "InstanceIds": [instance_id],
                "DocumentName": "AWS-RunShellScript",
                "TimeoutSeconds": ANY,
                "Parameters": {
                    "commands": [
                        "set -euo pipefail",
                        "flock -n /run/lock/lakebase-anti-demo-round5.lock "
                        "-c 'echo RUNNER_IDLE'",
                    ],
                    "executionTimeout": [ANY],
                },
                "CloudWatchOutputConfig": {"CloudWatchOutputEnabled": False},
            },
        )
        stubber.add_response(
            "get_command_invocation",
            {"Status": "Success", "StandardOutputContent": "RUNNER_IDLE\n"},
            {"CommandId": command_id, "InstanceId": instance_id},
        )

        _require_round5_runner_idle(manifest)

        stubber.assert_no_pending_responses()

    # Secondary, and only an explanation of the above: name the floor the
    # validation enforced, so a failure reads as a range violation rather than as
    # a puzzle about a stub.
    floor = (
        botocore.session.get_session()
        .get_service_model("ssm")
        .operation_model("SendCommand")
        .input_shape.members["TimeoutSeconds"]
        .metadata["min"]
    )
    assert ROUND5_SSM_COMMAND_TIMEOUT_SECONDS >= floor


def test_round5_aurora_seal_resolves_exact_owned_writer_resource() -> None:
    manifest = make_manifest()

    class FakeRds:
        def describe_db_clusters(self, **kwargs):
            assert kwargs == {"DBClusterIdentifier": "anti-demo-aurora"}
            return {
                "DBClusters": [
                    {
                        "DBClusterIdentifier": "anti-demo-aurora",
                        "DbClusterResourceId": "cluster-RESOURCE",
                        "Status": "available",
                        "Endpoint": "aurora.example.com",
                        "MasterUserSecret": {"SecretArn": manifest.aws.resources.aurora_secret_arn},
                        "DBClusterMembers": [
                            {
                                "DBInstanceIdentifier": "anti-demo-aurora-writer",
                                "IsClusterWriter": True,
                            }
                        ],
                    }
                ]
            }

        def describe_db_instances(self, **kwargs):
            assert kwargs == {"DBInstanceIdentifier": "anti-demo-aurora-writer"}
            return {
                "DBInstances": [
                    {
                        "DBInstanceIdentifier": "anti-demo-aurora-writer",
                        "DBClusterIdentifier": "anti-demo-aurora",
                        "DBInstanceStatus": "available",
                    }
                ]
            }

    assert (
        _round5_aurora_cluster_resource_id(
            manifest,
            FakeRds(),
            direct_host="aurora.example.com",
            cluster_id="anti-demo-aurora",
            writer_instance_id="anti-demo-aurora-writer",
            master_secret_arn=manifest.aws.resources.aurora_secret_arn,
            expected_resource_id="cluster-RESOURCE",
        )
        == "cluster-RESOURCE"
    )

    with pytest.raises(RuntimeError, match="owned AWS resources"):
        _round5_aurora_cluster_resource_id(
            manifest,
            FakeRds(),
            direct_host="aurora.example.com",
            cluster_id="anti-demo-aurora",
            writer_instance_id="anti-demo-aurora-writer",
            master_secret_arn=("arn:aws:secretsmanager:us-west-2:123456789012:secret:someone-else"),
            expected_resource_id="cluster-RESOURCE",
        )


def test_terraform_uses_only_manifest_selected_environment_credentials(monkeypatch) -> None:
    manifest = make_manifest()
    manifest.aws.auth_mode = "environment"
    manifest.aws.profile = ""
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test-token")
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::999999999999:role/wrong")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/tmp/wrong")

    environment = _terraform_environment(manifest)

    assert environment["AWS_ACCESS_KEY_ID"] == "test-access"
    assert environment["AWS_SECRET_ACCESS_KEY"] == "test-secret"
    assert environment["AWS_SESSION_TOKEN"] == "test-token"
    assert "AWS_PROFILE" not in environment
    assert "AWS_ROLE_ARN" not in environment
    assert "AWS_WEB_IDENTITY_TOKEN_FILE" not in environment


def test_an_iam_policy_proves_ownership_with_the_tag_iam_can_actually_hold(monkeypatch) -> None:
    """A destroy must not be gated on a tag AWS refuses to store.

    IAM treats tag keys case-insensitively and rejects ``Owner`` and ``owner``
    as a duplicate pair -- `infra/aws/locals.tf` says so and tags every IAM
    resource from the lowercase-only set because of it. This validator exempted
    roles and instance profiles from the capital key but not policies, so a
    cleanup forced onto the partial-retry path refused on a tag that could never
    have been there, with every database in the installation still billing.

    Both directions are here because only one of them is a safety property. What
    is excused is the key IAM cannot hold; ownership itself is still proven by
    the lowercase ``owner``, and a policy carrying somebody else's is refused.
    """

    manifest = make_manifest(status="cleanup_failed")
    address = "aws_iam_policy.round5_runner_boundary"
    expires_at = manifest.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def tagged_to(owner: str) -> dict[str, dict[str, object]]:
        return {
            address: {
                "tags_all": {
                    "anti-demo-run-id": manifest.run_id,
                    # No capital "Owner" key, because IAM would not accept one
                    # alongside this. That absence is the whole point.
                    "owner": owner,
                    "expires-at": expires_at,
                    "managed-by": "terraform",
                }
            }
        }

    def state_is(values):
        monkeypatch.setattr(
            "server.lifecycle._terraform_state_resource_values", lambda *_a, **_k: values
        )

    monkeypatch.setattr(
        "server.lifecycle._aws_session",
        lambda _: SimpleNamespace(client=lambda _name: SimpleNamespace()),
    )

    state_is(tagged_to(manifest.owner))
    _validate_partial_aws_destroy_retry(manifest, {address})

    state_is(tagged_to("someone.else@databricks.com"))
    with pytest.raises(RuntimeError, match="ownership tags differ"):
        _validate_partial_aws_destroy_retry(manifest, {address})


def test_an_inline_role_policy_is_recognised_by_the_name_terraform_gives_it(monkeypatch) -> None:
    """An untaggable resource has to be named correctly to be recognised.

    IAM has no tagging for inline role policies, so `aws_iam_role_policy` can
    never satisfy the tag check and is instead recognised by name. The name
    recorded for the proxy secret-reader policy was singular while Terraform
    declares it plural at `infra/aws/round5_secrets.tf`, so the one address that
    needed the exemption was the one address that did not get it.

    The refusal it fell through to is the check that stops a cleanup destroying
    an untagged resource nobody has shown to be ours, so that direction is
    asserted too: an address that is not a known-untaggable child is still
    refused, and this policy's own ownership still rests on its parent role.
    """

    manifest = make_manifest(status="cleanup_failed")
    inline_policy = "aws_iam_role_policy.round5_proxy_secrets"

    def state_is(values):
        monkeypatch.setattr(
            "server.lifecycle._terraform_state_resource_values", lambda *_a, **_k: values
        )

    monkeypatch.setattr(
        "server.lifecycle._aws_session",
        lambda _: SimpleNamespace(client=lambda _name: SimpleNamespace()),
    )

    state_is({inline_policy: {"tags_all": {}}})
    _validate_partial_aws_destroy_retry(manifest, {inline_policy})

    state_is({"aws_security_group.round5_proxy": {"tags_all": {}}})
    with pytest.raises(RuntimeError, match="no ownership tags"):
        _validate_partial_aws_destroy_retry(manifest, {"aws_security_group.round5_proxy"})


def test_round5_legacy_partial_restores_default_before_obsolete_state_can_leave(
    monkeypatch,
) -> None:
    manifest = make_manifest()
    addresses = {
        "aws_db_parameter_group.rds_round5",
        "aws_secretsmanager_secret.round5_lakebase_credentials",
        "aws_security_group.round5_proxy",
        "aws_security_group.round5_runner",
        "aws_vpc_security_group_ingress_rule.round5_runner_to_proxy",
    }
    required_tags = {
        "anti-demo-run-id": manifest.run_id,
        "Owner": manifest.owner,
        "owner": manifest.owner,
        "expires-at": manifest.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "managed-by": "terraform",
    }
    state = {
        address: {
            "name": "owned-scram" if address == "aws_db_parameter_group.rds_round5" else "",
            "tags_all": required_tags,
        }
        for address in addresses
    }
    state["aws_security_group.round5_proxy"]["id"] = "sg-proxy"
    state["aws_security_group.round5_runner"]["id"] = "sg-runner"
    state["aws_vpc_security_group_ingress_rule.round5_runner_to_proxy"].update(
        {
            "security_group_id": "sg-proxy",
            "referenced_security_group_id": "sg-runner",
            "ip_protocol": "tcp",
            "from_port": 5432,
            "to_port": 5432,
        }
    )
    calls: list[str] = []

    class FakeRds:
        phase = "custom"

        def describe_db_parameter_groups(self, **kwargs):
            assert kwargs == {"DBParameterGroupName": "owned-scram"}
            return {
                "DBParameterGroups": [
                    {
                        "DBParameterGroupName": "owned-scram",
                        "DBParameterGroupArn": "arn:aws:rds:us-west-2:123456789012:pg:owned",
                    }
                ]
            }

        def list_tags_for_resource(self, **kwargs):
            assert kwargs["ResourceName"].endswith(":pg:owned")
            return {
                "TagList": [{"Key": key, "Value": value} for key, value in required_tags.items()]
            }

        def describe_db_instances(self, **kwargs):
            assert kwargs == {"DBInstanceIdentifier": manifest.aws.resources.rds_instance_id}
            if self.phase == "restored":
                calls.append("available-in-sync")
                return {
                    "DBInstances": [
                        {
                            "DBInstanceStatus": "available",
                            "DBParameterGroups": [
                                {
                                    "DBParameterGroupName": "default.postgres17",
                                    "ParameterApplyStatus": "in-sync",
                                }
                            ],
                            "PendingModifiedValues": {},
                        }
                    ]
                }
            if self.phase == "pending-reboot":
                return {
                    "DBInstances": [
                        {
                            "DBInstanceStatus": "available",
                            "DBParameterGroups": [
                                {
                                    "DBParameterGroupName": "default.postgres17",
                                    "ParameterApplyStatus": "pending-reboot",
                                }
                            ],
                            "PendingModifiedValues": {},
                        }
                    ]
                }
            return {
                "DBInstances": [{"DBParameterGroups": [{"DBParameterGroupName": "owned-scram"}]}]
            }

        def modify_db_instance(self, **kwargs):
            assert kwargs == {
                "DBInstanceIdentifier": manifest.aws.resources.rds_instance_id,
                "DBParameterGroupName": "default.postgres17",
                "ApplyImmediately": True,
            }
            calls.append("restore-default")
            self.phase = "pending-reboot"

        def reboot_db_instance(self, **kwargs):
            assert kwargs == {
                "DBInstanceIdentifier": manifest.aws.resources.rds_instance_id,
            }
            calls.append("reboot")
            self.phase = "restored"

    class FakeSession:
        def client(self, name):
            assert name == "rds"
            return FakeRds()

    monkeypatch.setattr("server.lifecycle._terraform_state_resource_values", lambda *_: state)
    monkeypatch.setattr("server.lifecycle._aws_ownership", lambda _: Check("owned", True, ""))
    monkeypatch.setattr("server.lifecycle._aws_session", lambda _: FakeSession())

    assert _reconcile_legacy_round5_partial_state(manifest, addresses) is True
    assert calls == ["restore-default", "reboot", "available-in-sync"]

    expected_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    reordered_and_encoded = quote(
        json.dumps(
            {
                "Statement": [
                    {
                        "Action": ["sts:AssumeRole"],
                        "Principal": {"Service": ["ec2.amazonaws.com"]},
                        "Effect": "Allow",
                    }
                ],
                "Version": "2012-10-17",
            }
        )
    )
    assert _canonical_iam_policy(reordered_and_encoded) == _canonical_iam_policy(expected_trust)
    with_extra_statement = {
        **expected_trust,
        "Statement": [
            *expected_trust["Statement"],
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "sts:AssumeRole",
            },
        ],
    }
    assert _canonical_iam_policy(with_extra_statement) != _canonical_iam_policy(expected_trust)

    class PrefixFirstSecrets:
        def list_secrets(self, **kwargs):
            assert kwargs["IncludePlannedDeletion"] is True
            assert kwargs["Filters"] == [{"Key": "name", "Values": ["anti-demo/r5/secret/"]}]
            return {
                "SecretList": [
                    {
                        "ARN": "arn:aws:secretsmanager:us-west-2:123456789012:secret:drifted",
                        "Name": "anti-demo/r5/secret/0123456789abcdef",
                        "DeletedDate": datetime.now(UTC),
                        "Tags": [],
                    }
                ]
            }

    class PrefixFirstSession:
        def client(self, name):
            assert name == "secretsmanager"
            return PrefixFirstSecrets()

    ownership = SimpleNamespace(
        as_aws_tags=lambda: {
            "anti-demo-run-id": manifest.run_id,
            "Owner": manifest.owner,
            "owner": manifest.owner,
            "expires-at": manifest.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "managed-by": "round5-lifecycle",
        }
    )
    sealed = SimpleNamespace(
        ownership_tags=ownership,
        bout_name_prefix="anti-demo-r5-bout",
        secret_name_prefix="anti-demo/r5/secret",
        vpc_id="vpc-0123456789abcdef0",
        runner_security_group_id="sg-0123456789abcdef0",
    )
    prefix_manifest = SimpleNamespace(
        round5_ready=True,
        require_round5_resources=lambda: sealed,
        run_id=manifest.run_id,
        owner=manifest.owner,
        expires_at=manifest.expires_at,
        aws=SimpleNamespace(
            resources=SimpleNamespace(rds_security_group_id="sg-1123456789abcdef0")
        ),
    )
    monkeypatch.setattr("server.lifecycle._aws_session", lambda _: PrefixFirstSession())
    with pytest.raises(RuntimeError, match="ownership tags differ"):
        _round5_runtime_tag_inventory(prefix_manifest)


def test_waiting_provision_clears_legacy_anchor_without_recreating_resources(
    monkeypatch,
) -> None:
    manifest = make_manifest(status="waiting_for_zero")
    attach_anchor(manifest)
    calls: list[str] = []

    async def fake_wait(candidate: DemoManifest, timeout_seconds: float) -> None:
        assert candidate is manifest
        assert timeout_seconds == 123
        calls.append("wait")

    monkeypatch.setattr("server.lifecycle.wait_for_scale_zero", fake_wait)
    monkeypatch.setattr(
        "server.lifecycle.ensure_coordination",
        lambda candidate: calls.append("coordination") or candidate,
    )
    monkeypatch.setattr("server.lifecycle.save_manifest", lambda candidate: calls.append("save"))
    monkeypatch.setattr(
        "server.lifecycle._terraform_init",
        lambda candidate: pytest.fail("resume must not recreate AWS resources"),
    )
    monkeypatch.setattr(
        "server.lifecycle.seed_identical_schema",
        lambda candidate: pytest.fail("resume must not wake an already sealed environment"),
    )

    recovered = _complete_provision(manifest, 123)

    assert recovered.status == "ready"
    assert recovered.round3_anchor is None
    assert calls == ["save", "coordination", "wait", "save"]


def test_interrupted_seeding_resumes_without_staging_recovery_points(monkeypatch) -> None:
    manifest = make_manifest(status="seeding")
    calls: list[str] = []

    async def fake_seed(candidate: DemoManifest) -> None:
        assert candidate is manifest
        calls.append("seed")

    async def fake_wait(candidate: DemoManifest, timeout_seconds: float) -> None:
        assert candidate is manifest
        assert timeout_seconds == 123
        calls.append("wait")

    monkeypatch.setattr("server.lifecycle.seed_identical_schema", fake_seed)
    monkeypatch.setattr("server.lifecycle.wait_for_scale_zero", fake_wait)
    monkeypatch.setattr(
        "server.lifecycle.ensure_coordination",
        lambda candidate: calls.append("coordination") or candidate,
    )
    monkeypatch.setattr(
        "server.lifecycle.save_manifest",
        lambda candidate: calls.append(
            f"save:{candidate.status}:{candidate.round3_anchor is not None}"
        ),
    )
    monkeypatch.setattr(
        "server.lifecycle._terraform_init",
        lambda candidate: pytest.fail("resume must not reprovision AWS resources"),
    )

    recovered = _complete_provision(manifest, 123)

    assert recovered.status == "ready"
    assert recovered.round3_anchor is None
    assert calls == [
        "save:seeding:False",
        "seed",
        "coordination",
        "save:waiting_for_zero:False",
        "coordination",
        "wait",
        "save:ready:False",
    ]


def test_interrupted_first_provision_leaves_a_findable_owned_record(
    monkeypatch, isolated_lifecycle_manifest
) -> None:
    """A provision killed mid-apply must still name its owner, region and run tag.

    The record that survives this is the manifest, which `provision` writes
    before the first billable apply. Nothing else in the suite pins that
    ordering: were the write ever moved after `_complete_provision`, an
    interrupted provision would bill for tagged AWS resources whose only record
    never reached disk, and the tag-driven reconciliation would have no run ID
    to search the account for.
    """
    seen: dict[str, object] = {}

    def dying_apply(candidate: DemoManifest, plan_path: Path) -> None:
        del plan_path
        seen["run_id"] = candidate.run_id
        seen["on_disk"] = json.loads(isolated_lifecycle_manifest.read_text(encoding="utf-8"))
        raise RuntimeError("provision interrupted mid-apply")

    monkeypatch.setattr(
        "server.lifecycle.select_setup_auth",
        lambda environment, requested: SimpleNamespace(mode="profile", profile="sandbox-admin"),
    )
    monkeypatch.setattr(
        "server.lifecycle._verify_databricks_identity",
        lambda profile: "operator@databricks.com",
    )
    monkeypatch.setattr("server.lifecycle._verify_aws_identity", lambda *args: None)
    monkeypatch.setattr("server.lifecycle._terraform_init", lambda candidate: None)
    monkeypatch.setattr("server.lifecycle._run", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "server.lifecycle._terraform_plan",
        lambda candidate, targets=(): Path("/tmp/anti-demo-test-create.tfplan"),
    )
    monkeypatch.setattr("server.lifecycle._terraform_apply", dying_apply)

    with pytest.raises(RuntimeError, match="interrupted mid-apply"):
        lifecycle.provision(
            databricks_profile="fe-vm-test",
            aws_profile="sandbox-admin",
            aws_region="us-west-2",
            expected_account="123456789012",
            owner="operator@databricks.com",
            operator_cidr="203.0.113.10/32",
            ttl_hours=72,
            zero_timeout_seconds=1,
        )

    record = seen["on_disk"]
    assert record["status"] == "provisioning"
    assert record["owner"] == "operator@databricks.com"
    assert record["aws"]["region"] == "us-west-2"
    assert record["created_at"]
    # The same run ID Terraform stamps on every resource it creates, so a
    # half-built fleet stays findable by tag even if state was never written.
    assert record["run_id"] == seen["run_id"]
    # The failure must not take the record with it.
    assert json.loads(isolated_lifecycle_manifest.read_text(encoding="utf-8")) == record


async def test_reset_clears_legacy_anchor_and_never_stages_recovery_points(
    monkeypatch,
) -> None:
    manifest = make_manifest(status="ready")
    attach_anchor(manifest)
    calls: list[str] = []
    main_thread = threading.get_ident()
    main_loop = asyncio.get_running_loop()
    coordination_started = asyncio.Event()
    release_coordination = threading.Event()
    heartbeat_sleeps = 0

    class FastLifecycleAsyncio:
        FIRST_COMPLETED = asyncio.FIRST_COMPLETED
        create_task = staticmethod(asyncio.create_task)
        gather = staticmethod(asyncio.gather)
        to_thread = staticmethod(asyncio.to_thread)
        wait = staticmethod(asyncio.wait)

        @staticmethod
        async def sleep(delay: float) -> None:
            nonlocal heartbeat_sleeps
            assert delay == 15
            heartbeat_sleeps += 1
            if heartbeat_sleeps == 1:
                await coordination_started.wait()
                await asyncio.sleep(0)
                return
            await asyncio.Event().wait()

    class FakeLeaseStore:
        async def initialize(self) -> None:
            calls.append("lease:init")

        async def claim(self, **_):
            calls.append("lease:claim")
            return object()

        async def renew(self, lease, ttl):
            assert threading.get_ident() == main_thread
            calls.append("lease:renew")
            release_coordination.set()
            return lease

        async def release(self, lease) -> None:
            calls.append("lease:release")

        async def close(self) -> None:
            calls.append("lease:close")

    async def fake_cleanup(candidate: DemoManifest) -> None:
        assert candidate is manifest
        calls.append("cleanup")

    async def fake_seed(candidate: DemoManifest) -> None:
        assert candidate is manifest
        calls.append("seed")

    async def fake_wait(candidate: DemoManifest, timeout_seconds: float) -> None:
        assert candidate is manifest
        calls.append("wait")

    def fake_round4(candidate: DemoManifest, *, timeout: float) -> DemoManifest:
        assert candidate is manifest
        assert timeout == 123
        assert threading.get_ident() != main_thread
        calls.append("round4")
        candidate.status = "waiting_for_zero"
        calls.append("save:waiting_for_zero:False")
        return candidate

    def fake_ensure_coordination(candidate: DemoManifest) -> DemoManifest:
        assert candidate is manifest
        assert threading.get_ident() != main_thread
        calls.append("coordination:start")
        asyncio.run(asyncio.sleep(0))
        main_loop.call_soon_threadsafe(coordination_started.set)
        assert release_coordination.wait(timeout=2)
        calls.append("coordination")
        return candidate

    monkeypatch.setattr("server.coordination.build_lease_store", FakeLeaseStore)
    monkeypatch.setattr("server.lifecycle.asyncio", FastLifecycleAsyncio)
    monkeypatch.setattr("server.lifecycle.reset_safe_change_artifacts", fake_cleanup)
    monkeypatch.setattr("server.lifecycle.seed_identical_schema", fake_seed)
    monkeypatch.setattr("server.lifecycle.wait_for_scale_zero", fake_wait)
    monkeypatch.setattr("server.lifecycle._ensure_round4", fake_round4)
    monkeypatch.setattr(
        "server.lifecycle.ensure_coordination",
        fake_ensure_coordination,
    )
    monkeypatch.setattr(
        "server.lifecycle.save_manifest",
        lambda candidate: calls.append(
            f"save:{candidate.status}:{candidate.round3_anchor is not None}"
        ),
    )

    result = await _reset_under_ring_lease(manifest, 123)

    assert result.status == "ready"
    assert result.round3_anchor is None
    assert calls == [
        "lease:init",
        "lease:claim",
        "cleanup",
        "save:seeding:False",
        "seed",
        "coordination:start",
        "lease:renew",
        "coordination",
        "round4",
        "save:waiting_for_zero:False",
        "wait",
        "save:ready:False",
        "lease:release",
        "lease:close",
    ]


async def test_round5_restart_cleanup_uses_fresh_exact_bout_fences(monkeypatch) -> None:
    manifest = make_manifest(status="ready")
    calls: list[str] = []

    class Lease:
        def __init__(self, session_id: str, fencing_token: int) -> None:
            self.session_id = session_id
            self.fencing_token = fencing_token

    class DurableStore:
        mode = "lakebase"
        ring_key = "round5"

        async def _run(self, operation):
            return await operation(None)

        async def current(self):
            return None

        async def claim(self, **values):
            bout_id = values["session_id"]
            token = len([call for call in calls if call.startswith("claim:")]) + 101
            calls.append(
                f"claim:{bout_id}:{token}:{values['competitor_id']}:{values['competitor_name']}"
            )
            return Lease(bout_id, token)

        async def renew(self, lease, ttl):
            pytest.fail("short recovery must not need a lease renewal")

        async def release(self, lease) -> None:
            calls.append(f"release:{lease.session_id}:{lease.fencing_token}")

    competitors = {
        "bout-a": "aurora_serverless_v2",
        "bout-b": "rds_postgres",
    }

    class Journal:
        def __init__(self, run, *, authority_ring_key):
            assert run is not None
            assert authority_ring_key == DurableStore.ring_key

        async def scopes(self, bout_id):
            return (SimpleNamespace(bout_id=bout_id, fencing_token=7, runtime_seal_sha256="s"),)

        async def events(self, scope):
            selected = competitors[scope.bout_id]
            selected_values = selected if isinstance(selected, tuple) else (selected,)
            return tuple(
                SimpleNamespace(
                    bout_id=scope.bout_id,
                    fencing_token=scope.fencing_token,
                    runtime_seal_sha256=scope.runtime_seal_sha256,
                    metadata=(
                        {"competitor_id": competitor_id} if competitor_id is not None else {}
                    ),
                )
                for competitor_id in selected_values
            )

    class Engine:
        def __init__(self, competitor_id):
            self.competitor_id = competitor_id

        async def reconcile_failed_cleanup(self, bout_id, current_fencing_token):
            calls.append(f"recover:{bout_id}:{current_fencing_token}:{self.competitor_id}")

    monkeypatch.setattr(
        "server.connection_spike_live.LakebaseCreationJournalStore",
        Journal,
    )

    monkeypatch.setattr(
        "server.connection_spike_live.build_connection_spike_live_engine",
        lambda *args, competitor_id, **kwargs: (
            calls.append(f"build:{competitor_id}") or Engine(competitor_id)
        ),
    )
    monkeypatch.setattr(
        "server.lifecycle._require_round5_clean_baseline",
        lambda candidate: calls.append("clean") if candidate is manifest else None,
    )

    await _reconcile_round5_failed_cleanups(
        manifest,
        DurableStore(),
        ("bout-a", "bout-b"),
    )

    assert calls == [
        "build:aurora_serverless_v2",
        "claim:bout-a:101:aurora_serverless_v2:Amazon Aurora PostgreSQL Serverless v2",
        "recover:bout-a:101:aurora_serverless_v2",
        "release:bout-a:101",
        "build:rds_postgres",
        "claim:bout-b:102:rds_postgres:Amazon RDS for PostgreSQL",
        "recover:bout-b:102:rds_postgres",
        "release:bout-b:102",
        "clean",
    ]

    competitors["bout-missing"] = None
    with pytest.raises(RuntimeError, match="missing exact competitor metadata"):
        await _reconcile_round5_failed_cleanups(
            manifest,
            DurableStore(),
            ("bout-missing",),
        )
    competitors["bout-mixed"] = ("rds_postgres", "aurora_serverless_v2")
    with pytest.raises(RuntimeError, match="mixes competitor metadata"):
        await _reconcile_round5_failed_cleanups(
            manifest,
            DurableStore(),
            ("bout-mixed",),
        )


def test_round5_cleanup_ring_key_tracks_manifest_generation() -> None:
    legacy = SimpleNamespace(manifest_version=6, installation_id=None)
    assert _round5_cleanup_ring_key(legacy) == "round5"

    current = SimpleNamespace(manifest_version=7, installation_id="install-a")
    assert _round5_cleanup_ring_key(current) == (
        "installation:install-a:round:survive_connection_spike:cleanup"
    )


def test_one_command_setup_resets_and_checks_both_opponents(monkeypatch, tmp_path) -> None:
    manifest = make_manifest(status="ready")
    owned_manifest = tmp_path / "manifest.json"
    owned_manifest.touch()
    calls: list[str] = []

    monkeypatch.setattr("server.lifecycle.manifest_path", lambda: owned_manifest)
    monkeypatch.setattr("server.lifecycle.load_manifest", lambda: manifest)
    monkeypatch.setattr(
        "server.lifecycle.reconcile_infrastructure",
        lambda candidate: calls.append("reconcile") or candidate,
    )
    monkeypatch.setattr(
        "server.lifecycle.reset",
        lambda timeout: calls.append(f"reset:{timeout}") or manifest,
    )
    monkeypatch.setattr(
        "server.lifecycle.resume_provision",
        lambda timeout: pytest.fail("a ready environment must reset, not resume"),
    )
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round5",
        lambda candidate, *, timeout: calls.append(f"round5:{timeout}") or candidate,
    )
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round6",
        lambda candidate, *, timeout: calls.append(f"round6:{timeout}") or candidate,
    )
    monkeypatch.setattr(
        "server.lifecycle.doctor",
        lambda competitor, *, timeout_seconds: (
            calls.append(f"doctor:{competitor}:{timeout_seconds}")
            or [Check("ready", True, "ready")]
        ),
    )

    prepared = setup(
        databricks_profile="",
        aws_profile="",
        aws_region="",
        expected_account="",
        owner="",
        operator_cidr=None,
        # An existing installation refuses an explicit TTL and points at
        # `antidemo renew`; saying nothing is what lets setup proceed.
        ttl_hours=None,
        timeout_seconds=321,
    )

    assert prepared is manifest
    assert calls == [
        "reconcile",
        "reset:321",
        "round5:321",
        "round6:321",
        "doctor:aurora:321",
        "doctor:rds:321",
    ]


def test_resume_reseals_round5_when_existing_manifest_needs_upgrade(monkeypatch) -> None:
    manifest = make_manifest(status="ready")
    calls: list[str] = []

    monkeypatch.setattr("server.lifecycle.load_manifest", lambda: manifest)
    monkeypatch.setattr(
        "server.lifecycle._verify_databricks_identity",
        lambda profile: manifest.databricks.user,
    )
    monkeypatch.setattr("server.lifecycle._verify_aws_identity", lambda *args: None)
    monkeypatch.setattr("server.lifecycle.detect_operator_cidr", lambda: manifest.aws.operator_cidr)
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round4",
        lambda candidate, *, timeout: calls.append(f"round4:{timeout}") or candidate,
    )
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round5",
        lambda candidate, *, timeout: calls.append(f"round5:{timeout}") or candidate,
    )
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round6",
        lambda candidate, *, timeout: calls.append(f"round6:{timeout}") or candidate,
    )

    assert resume_provision(321) is manifest
    assert calls == ["round4:321", "round5:321", "round6:321"]


def test_resume_retries_interrupted_round6_without_reseeding_base(monkeypatch) -> None:
    manifest = make_manifest(status="seeding")
    attach_round4(manifest)
    manifest.round5 = ready_round5_stub()
    manifest.manifest_version = 5
    calls: list[str] = []

    monkeypatch.setattr("server.lifecycle.load_manifest", lambda: manifest)
    monkeypatch.setattr(
        "server.lifecycle._verify_databricks_identity",
        lambda profile: manifest.databricks.user,
    )
    monkeypatch.setattr("server.lifecycle._verify_aws_identity", lambda *args: None)
    monkeypatch.setattr("server.lifecycle.detect_operator_cidr", lambda: manifest.aws.operator_cidr)
    monkeypatch.setattr(
        "server.lifecycle._complete_provision",
        lambda *args: pytest.fail("Round 6 retry must not repeat base seeding"),
    )
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round4",
        lambda *args, **kwargs: pytest.fail("Round 6 retry must not reseal Round 4"),
    )
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round5",
        lambda *args, **kwargs: pytest.fail("Round 6 retry must not reseal Round 5"),
    )
    monkeypatch.setattr(
        "server.lifecycle._prepare_and_reseal_round6",
        lambda candidate, *, timeout: calls.append(f"round6:{timeout}") or candidate,
    )

    assert resume_provision(321) is manifest
    assert calls == ["round6:321"]


@pytest.mark.parametrize(
    ("states", "expected_ok", "expected_calls"),
    [
        (["ACTIVE", "IDLE"], True, 2),
        (["ACTIVE", "ACTIVE", "ACTIVE"], False, 3),
    ],
)
async def test_lakebase_scale_zero_doctor_polls_to_idle_or_reports_final_timeout(
    monkeypatch, states, expected_ok, expected_calls
) -> None:
    manifest = make_manifest()
    clock = [0.0]

    class FakeProvider:
        def __init__(self) -> None:
            self.calls = 0

        async def assert_armed(self):
            state = states[self.calls]
            self.calls += 1
            if state != "IDLE":
                raise TargetNotArmedError(f"Lakebase endpoint is {state}, not IDLE")
            return {"state": "IDLE", "disabled": False}

    provider = FakeProvider()

    async def advance(delay: float) -> None:
        clock[0] += delay

    monkeypatch.setattr("server.lifecycle.apply_manifest_environment", lambda candidate: None)
    monkeypatch.setattr("server.lifecycle.LakebaseCredentialProvider", lambda: provider)
    monkeypatch.setattr("server.lifecycle.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("server.lifecycle.asyncio.sleep", advance)

    result = await _lakebase_scale_zero_check(manifest, timeout_seconds=10)

    assert result.ok is expected_ok
    assert provider.calls == expected_calls
    assert "last_active_time" not in result.detail
    if expected_ok:
        assert result.detail == "state=IDLE, disabled=False"
    else:
        assert result.detail.endswith("Lakebase endpoint is ACTIVE, not IDLE")


def test_round4_names_take_an_unsealed_catalog_from_the_environment(monkeypatch) -> None:
    manifest = make_manifest()
    assert manifest.round4 is None

    monkeypatch.delenv("ROUND4_CATALOG", raising=False)
    assert _round4_names(manifest)["catalog"] == ROUND4_DEFAULT_CATALOG

    # Deliberately not the module default, so this proves the environment was
    # read rather than that the default happened to agree with it.
    assert ROUND4_DEFAULT_CATALOG != "customer_catalog"
    monkeypatch.setenv("ROUND4_CATALOG", "customer_catalog")
    names = _round4_names(manifest)
    assert names["catalog"] == "customer_catalog"
    assert names["source_table"].startswith("customer_catalog.")
    assert names["synced_table_id"].startswith("customer_catalog.")
    assert (
        _round4_synced_spec(names)["new_pipeline_spec"]["storage_catalog"] == "customer_catalog"
    )


def test_round4_names_prefer_the_sealed_catalog_over_a_missing_environment(monkeypatch) -> None:
    """A provisioned installation must keep working with ROUND4_CATALOG unset.

    The catalog reaches CREATE SCHEMA, GRANT and cleanup DELETE statements, so a
    later run that fell back to the compiled-in default would operate on a
    different catalog than the one it provisioned.
    """

    manifest = make_manifest()
    monkeypatch.setenv("ROUND4_CATALOG", "customer_catalog")
    attach_round4(manifest)
    assert manifest.round4 is not None
    assert manifest.round4.storage_catalog == "customer_catalog"

    monkeypatch.delenv("ROUND4_CATALOG", raising=False)
    assert _round4_names(manifest)["catalog"] == "customer_catalog"


def test_round4_refuses_an_environment_catalog_that_contradicts_the_seal(monkeypatch) -> None:
    manifest = make_manifest()
    monkeypatch.delenv("ROUND4_CATALOG", raising=False)
    attach_round4(manifest)

    monkeypatch.setenv("ROUND4_CATALOG", "somewhere_else")
    with pytest.raises(RuntimeError, match="disagrees with the Unity Catalog"):
        _round4_names(manifest)


def test_round4_refuses_a_catalog_name_that_could_escape_a_quoted_identifier(
    monkeypatch,
) -> None:
    manifest = make_manifest()
    for hostile in ("main`.`evil", "main.other", "drop schema", ""):
        monkeypatch.setenv("ROUND4_CATALOG", hostile)
        if hostile == "":
            assert _round4_names(manifest)["catalog"] == ROUND4_DEFAULT_CATALOG
            continue
        with pytest.raises(RuntimeError, match="bare Unity Catalog name"):
            _round4_names(manifest)


def test_round4_refuses_an_absent_catalog_before_writing_and_names_the_fix(monkeypatch) -> None:
    """A stranger's workspace need not have the default catalog, and must be told so.

    Without this the absence lands as a raw Databricks error out of the first
    `CREATE SCHEMA`, part-way through provisioning, naming neither the guessed
    default nor the variable that overrides it -- which is a round dying mid-demo
    and looking like a bug in the project.
    """

    manifest = make_manifest()
    monkeypatch.setattr(
        "server.lifecycle._round4_get_uc_object",
        lambda candidate, kind, full_name: None,
    )

    with pytest.raises(RuntimeError) as absent_default:
        _require_round4_catalog(manifest, ROUND4_DEFAULT_CATALOG)
    message = str(absent_default.value)
    assert ROUND4_DEFAULT_CATALOG in message
    assert "ROUND4_CATALOG" in message
    assert "cleanup does not delete one" in message

    with pytest.raises(RuntimeError, match="ROUND4_CATALOG"):
        _require_round4_catalog(manifest, "customer_catalog")

    monkeypatch.setattr(
        "server.lifecycle._round4_get_uc_object",
        lambda candidate, kind, full_name: {"name": full_name},
    )
    _require_round4_catalog(manifest, ROUND4_DEFAULT_CATALOG)


def test_round4_provisioning_sql_and_grants_follow_a_foreign_catalog(monkeypatch) -> None:
    """Round 4 must be usable in a workspace that does not have the default catalog."""

    monkeypatch.setenv("ROUND4_CATALOG", "customer_catalog")
    manifest = make_manifest()
    names = attach_round4(manifest)
    assert manifest.round4 is not None

    statements: list[str] = []
    monkeypatch.setattr(
        "server.lifecycle._sql_statement",
        lambda profile, warehouse_id, statement: statements.append(statement)
        or {"result": {"data_array": [["7"]]}, "manifest": {"schema": {"columns": []}}},
    )
    monkeypatch.setattr("server.lifecycle._sql_rows", lambda payload: [{"version": 7}])
    monkeypatch.setattr("server.lifecycle._run", lambda arguments, **kwargs: None)

    _prepare_round4_source_artifacts(manifest, names, manifest.round4.warehouse_id)
    _grant_round4_uc_and_warehouse(
        manifest,
        names,
        manifest.round4.warehouse_id,
        manifest.round4.pipeline_id,
        manifest.round4.app_service_principal_client_id,
    )

    assert statements
    assert all(f"`{ROUND4_DEFAULT_CATALOG}`" not in statement for statement in statements)
    assert all(f"{ROUND4_DEFAULT_CATALOG}." not in statement for statement in statements)
    assert (
        f"CREATE SCHEMA IF NOT EXISTS `customer_catalog`.`{names['source_schema']}`" in statements
    )
    assert "GRANT USE CATALOG ON CATALOG `customer_catalog` TO " in "\n".join(statements)


def test_round4_resume_reuses_only_an_exact_owned_synced_table(monkeypatch) -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    existing = round4_payload(manifest, names)
    monkeypatch.setattr(
        "server.lifecycle._round4_get_synced_table",
        lambda candidate, expected: existing,
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api",
        lambda *args, **kwargs: pytest.fail("an exact existing synced table must not be recreated"),
    )

    reused = _create_or_get_round4_synced_table(manifest, names, timeout=10)

    assert reused is existing
    assert _validate_round4_synced_table(manifest, reused, names, sealed=manifest.round4) == (
        manifest.round4.synced_table_uid,
        manifest.round4.pipeline_id,
    )

    manifest.round5 = Round5Resources.model_construct()
    manifest.manifest_version = 4

    def reached_v4_validation(candidate):
        assert candidate is manifest
        raise RuntimeError("reached sealed v4 validation")

    monkeypatch.setattr("server.lifecycle._round4_names", reached_v4_validation)
    check = _round4_check(manifest)
    assert check.detail == "reached sealed v4 validation"


def test_round4_reseal_preserves_only_an_unchanged_v5_baseline() -> None:
    manifest = make_manifest()
    attach_round4(manifest)
    original_round4 = manifest.round4
    assert original_round4 is not None
    round5 = ready_round5_stub()
    manifest.round5 = round5
    manifest.manifest_version = 5

    _commit_round4_reseal(manifest, original_round4.model_copy())

    assert manifest.manifest_version == 5
    assert manifest.round5 is round5

    changed_round4 = original_round4.model_copy(update={"pipeline_id": "changed-pipeline"})
    _commit_round4_reseal(manifest, changed_round4)

    assert manifest.manifest_version == 2
    assert manifest.round5 is None


def test_round4_validation_refuses_a_different_project_or_spec() -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    payload = round4_payload(manifest, names)
    payload["status"] = {
        **payload["status"],
        "project": "projects/someone-elses-project",
    }

    with pytest.raises(RuntimeError, match="not owned by the manifest"):
        _validate_round4_synced_table(manifest, payload, names, sealed=manifest.round4)


def test_round4_dual_views_allow_omitted_create_fields_but_validate_effective_ids() -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    postgres = round4_payload(manifest, names)
    postgres.pop("spec")

    assert _validate_round4_synced_table(manifest, postgres, names, sealed=manifest.round4) == (
        manifest.round4.synced_table_uid,
        manifest.round4.pipeline_id,
    )

    database = database_round4_payload(manifest, names)
    _validate_round4_database_synced_table(
        database,
        names,
        project_uid=manifest.round4.project_uid,
        branch_uid=manifest.round4.branch_uid,
        pipeline_id=manifest.round4.pipeline_id,
    )
    database["effective_database_branch_id"] = "someone-elses-branch"
    with pytest.raises(RuntimeError, match="effective_database_branch_id"):
        _validate_round4_database_synced_table(
            database,
            names,
            project_uid=manifest.round4.project_uid,
            branch_uid=manifest.round4.branch_uid,
            pipeline_id=manifest.round4.pipeline_id,
        )


def test_round4_pipeline_requires_exactly_one_exact_sink(monkeypatch) -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    payload = pipeline_payload(manifest, names)

    _validate_round4_pipeline(
        payload,
        pipeline_id=manifest.round4.pipeline_id,
        synced_table_uid=manifest.round4.synced_table_uid,
        setup_principal=manifest.round4.setup_principal,
        names=names,
    )
    sinks = payload["spec"]["managed_definition"]["database_table_sync"]["sinks"]
    sinks.append(dict(sinks[0]))
    with pytest.raises(RuntimeError, match="exactly one sink"):
        _validate_round4_pipeline(
            payload,
            pipeline_id=manifest.round4.pipeline_id,
            synced_table_uid=manifest.round4.synced_table_uid,
            setup_principal=manifest.round4.setup_principal,
            names=names,
        )

    permission_calls: list[list[str]] = []
    sql_grants: list[str] = []
    monkeypatch.setattr(
        "server.lifecycle._run",
        lambda arguments, **kwargs: permission_calls.append(arguments),
    )
    monkeypatch.setattr(
        "server.lifecycle._sql_statement",
        lambda profile, warehouse_id, statement: sql_grants.append(statement),
    )
    _grant_round4_uc_and_warehouse(
        manifest,
        names,
        manifest.round4.warehouse_id,
        manifest.round4.pipeline_id,
        manifest.round4.app_service_principal_client_id,
    )

    assert [call[3:5] for call in permission_calls] == [
        ["warehouses", manifest.round4.warehouse_id],
        ["pipelines", manifest.round4.pipeline_id],
    ]
    assert '"permission_level": "CAN_USE"' in permission_calls[0][6]
    # CAN_RUN, because the app now starts this pipeline at arm and stops it once
    # a bout has settled, and CAN_VIEW cannot do either. The negative half is the
    # half worth keeping: CAN_MANAGE would also let the app edit the pipeline's
    # spec, and a `scheduling_policy` edit recreates the synced table, mints a
    # new `pipeline_id`, demotes the manifest to v2 and destroys Rounds 5 and 6.
    # A cost optimisation on Round 4 silently deleting two other rounds is the
    # failure this assertion exists to keep out.
    assert '"permission_level": "CAN_RUN"' in permission_calls[1][6]
    assert "CAN_MANAGE" not in permission_calls[1][6]
    principal = manifest.round4.app_service_principal_client_id
    assert sql_grants == [
        f"GRANT USE CATALOG ON CATALOG `{names['catalog']}` TO `{principal}`",
        "GRANT USE SCHEMA ON SCHEMA "
        f"`{names['catalog']}`.`{names['source_schema']}` TO `{principal}`",
        "GRANT USE SCHEMA ON SCHEMA "
        f"`{names['catalog']}`.`{names['storage_schema']}` TO `{principal}`",
        "GRANT USE SCHEMA ON SCHEMA "
        f"`{names['catalog']}`.`{names['online_schema']}` TO `{principal}`",
        "GRANT SELECT, MODIFY ON TABLE "
        f"`{names['source_table'].replace('.', '`.`')}` TO `{principal}`",
        "GRANT SELECT ON TABLE "
        f"`{names['synced_table_id'].replace('.', '`.`')}` TO `{principal}`",
    ]

    role_reads: dict[str, int] = {}
    role_posts: list[tuple[str, dict[str, object]]] = []
    role_resource_id = f"app-{principal}"

    def fake_role_get(profile: str, path: str):
        role_reads[path] = role_reads.get(path, 0) + 1
        if role_reads[path] == 1:
            return None
        role_name = unquote(path.removeprefix("/api/2.0/postgres/"))
        branch = role_name.rsplit("/roles/", 1)[0]
        return {
            "name": role_name,
            "parent": branch,
            "role_id": role_resource_id,
            "status": {
                "role_id": role_resource_id,
                "identity_type": "SERVICE_PRINCIPAL",
                "auth_method": "LAKEBASE_OAUTH_V1",
                "postgres_role": principal,
                "membership_roles": [],
                "attributes": {},
            },
        }

    def fake_role_create(profile: str, method: str, path: str, **kwargs):
        assert method == "post"
        role_posts.append((path, kwargs["body"]))
        return {"done": True}

    monkeypatch.setattr("server.lifecycle._databricks_api_optional", fake_role_get)
    monkeypatch.setattr("server.lifecycle._databricks_api", fake_role_create)
    monkeypatch.setattr("server.lifecycle._wait_round4_operation", lambda *args, **kwargs: None)

    _ensure_round4_app_roles(manifest, principal, timeout=30)

    round6_branch = "projects/install-r6/branches/production"
    _ensure_lakebase_app_roles(
        manifest,
        principal,
        (round6_branch,),
        timeout=30,
    )
    _ensure_lakebase_app_roles(
        manifest,
        principal,
        (round6_branch,),
        timeout=30,
    )

    assert [path for path, _body in role_posts] == [
        f"/api/2.0/postgres/projects/{manifest.run_id}/branches/production/roles"
        f"?role_id={role_resource_id}",
        f"/api/2.0/postgres/projects/{manifest.run_id}/branches/coordination/roles"
        f"?role_id={role_resource_id}",
        f"/api/2.0/postgres/{round6_branch}/roles?role_id={role_resource_id}",
    ]
    assert all(body["spec"]["postgres_role"] == principal for _path, body in role_posts)


#: `CREATE TABLE IF NOT EXISTS <target> (`, where the target is either a literal
#: name or a single f-string placeholder naming a module-level constant.
_DURABLE_TABLE_DDL = re.compile(r"CREATE TABLE IF NOT EXISTS\s+(\S+)\s*\(")
#: `NAME = "value"` or `NAME = f"{COORDINATION_SCHEMA}.value"`, module level.
_TABLE_CONSTANT = r'^{name} = f?"([^"]+)"$'


def _balanced_parentheses(source: str, opening: int) -> str:
    depth = 0
    for index in range(opening, len(source)):
        if source[index] == "(":
            depth += 1
        elif source[index] == ")":
            depth -= 1
            if depth == 0:
                return source[opening + 1 : index]
    raise AssertionError(f"unbalanced CREATE TABLE body at offset {opening}")


def _coordination_tables_created_in(
    segment: str,
    module_source: str,
    *,
    origin: str,
) -> dict[str, tuple[str, ...]]:
    """Coordination tables `segment` creates, mapped to their sequence columns.

    ``segment`` is a slice of ``module_source`` -- a whole file, a class body, or
    one function -- so the same scan answers both "what does `server/` create"
    and "what does this one initializer create". Constants are resolved against
    the enclosing module, because that is where a table name is defined.
    """

    from server.coordination import COORDINATION_SCHEMA

    tables: dict[str, tuple[str, ...]] = {}
    for match in _DURABLE_TABLE_DDL.finditer(segment):
        target = match.group(1)
        if target.startswith("{") and target.endswith("}"):
            constant = re.search(
                _TABLE_CONSTANT.format(name=re.escape(target[1:-1])),
                module_source,
                re.MULTILINE,
            )
            resolved = (
                constant.group(1).replace("{COORDINATION_SCHEMA}", COORDINATION_SCHEMA)
                if constant is not None
                else None
            )
        else:
            resolved = target
        if resolved is None:
            # Postgres never quotes with backticks, so a backticked target is
            # Unity Catalog and cannot be a coordination table. Anything else
            # is a Postgres table this scan could not name, which would let it
            # be missed -- so it fails here rather than silently.
            assert "`" in target, (
                f"{origin} creates {target}, whose name this scan cannot resolve. "
                "Assign it to a module-level constant so the grant plan in "
                "server/lifecycle.py can be checked against it."
            )
            continue
        if not resolved.startswith(f"{COORDINATION_SCHEMA}."):
            continue
        body = _balanced_parentheses(segment, match.end() - 1)
        tables[resolved] = tuple(
            column
            for column, kind in re.findall(r"^\s+(\w+)\s+(\w+)", body, re.MULTILINE)
            if kind.endswith("serial")
        )
    return tables


def _durable_coordination_tables() -> dict[str, tuple[str, ...]]:
    """Every coordination table `server/` creates, mapped to its sequence columns.

    Read out of the `CREATE TABLE` statements themselves, because the alternative
    -- a list of table names kept next to the grants -- is what let
    `startup_readiness` ship with no ACL entry for a release: it agreed with
    itself, and a test written against it would have agreed too.
    """

    tables: dict[str, tuple[str, ...]] = {}
    for path in sorted((Path(__file__).resolve().parents[1] / "server").glob("*.py")):
        source = path.read_text(encoding="utf-8")
        tables.update(
            _coordination_tables_created_in(source, source, origin=path.name)
        )
    return tables


def _setup_initialized_coordination_tables() -> set[str]:
    """Every coordination table `ensure_coordination`'s initializer actually creates.

    The other half of the anti-drift pair. `_durable_coordination_tables` asks
    "is every table that exists granted"; this asks "is every table that is
    granted created by setup". Nothing asked the second question, and
    `round4_pipeline_power` entered the grant plan while no initializer created
    it: the `GRANT` then failed mid-provision, which is the most expensive
    possible moment to learn it -- an installation half-built, and the live
    completeness probe in `initialize_table` firing after Terraform has run.

    Derived, not written down. The initializer's own source is walked for the
    stores whose `initialize()` it awaits, each of those stores' `CREATE TABLE`
    statements are read out of its class body, and the inline DDL in the
    initializer is read from the initializer. A store that is added to the grant
    plan but never initialized therefore fails here, and one that is renamed
    moves with its class.

    The resolution is deliberately the `initialize()` receiver rather than every
    name the initializer mentions. Mentioning is not creating: a store that is
    imported and constructed but never initialized runs no DDL, and a scan that
    counted the mention would report the table as created and pass while the
    provision still died at the GRANT.
    """

    server = Path(__file__).resolve().parents[1] / "server"
    lifecycle_source = (server / "lifecycle.py").read_text(encoding="utf-8")
    initializer = next(
        (
            node
            for node in ast.walk(ast.parse(lifecycle_source))
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "initialize_table"
        ),
        None,
    )
    assert initializer is not None, (
        "server/lifecycle.py no longer defines the `initialize_table` coroutine that "
        "provisions the coordination schema, so this guard is checking nothing."
    )
    segment = ast.get_source_segment(lifecycle_source, initializer) or ""
    created = set(
        _coordination_tables_created_in(
            segment, lifecycle_source, origin="lifecycle.initialize_table"
        )
    )

    def _class_of(node: ast.expr) -> str | None:
        """The class name a constructor expression refers to, if it is one."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    # Stores bound to a name first, so `store = LakebaseBoutLeaseStore(...)`
    # followed by `await store.initialize()` resolves to the class.
    constructed = {
        target.id: _class_of(node.value.func)
        for node in ast.walk(initializer)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    # The stores the initializer actually *initializes*, resolved from the
    # receiver of each `initialize()` call rather than from every name the
    # function mentions. Matching on the mention was the weaker test: a store
    # that is imported and constructed but never initialized creates no table,
    # which is exactly the `round4_pipeline_power` shape -- the name was there,
    # the DDL never ran, and the GRANT failed mid-provision.
    named: set[str] = set()
    for node in ast.walk(initializer):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "initialize":
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Call):
            resolved = _class_of(receiver.func)
        elif isinstance(receiver, ast.Name):
            resolved = constructed.get(receiver.id, receiver.id)
        else:
            resolved = _class_of(receiver)
        if resolved is not None:
            named.add(resolved)
    assert named, (
        "`initialize_table` awaits no store's `initialize()`, so this scan "
        "resolved no stores and the parity assertions prove nothing."
    )
    for path in sorted(server.glob("*.py")):
        source = path.read_text(encoding="utf-8")
        for node in ast.parse(source).body:
            if not isinstance(node, ast.ClassDef) or node.name not in named:
                continue
            body = ast.get_source_segment(source, node) or ""
            created.update(
                _coordination_tables_created_in(
                    body, source, origin=f"{path.name}::{node.name}"
                )
            )
    return created


def _deploy_doc_grants() -> dict[tuple[str, str], frozenset[str]]:
    """The runtime privilege set `docs/DEPLOY.md` publishes as authoritative."""

    heading = "### Coordination-database grants — the complete runtime set"
    document = (
        Path(__file__).resolve().parents[1] / "docs" / "DEPLOY.md"
    ).read_text(encoding="utf-8")
    assert heading in document, "docs/DEPLOY.md no longer publishes the runtime grant set"
    block = document.split(heading, 1)[1].split("```sql", 1)[1].split("```", 1)[0]
    sql_only = "\n".join(
        line for line in block.splitlines() if not line.strip().startswith("--")
    )
    grants: dict[tuple[str, str], frozenset[str]] = {}
    for raw in sql_only.split(";"):
        statement = " ".join(raw.split())
        if not statement:
            continue
        grants.update(_parse_grant(statement))
    return grants


def _parse_grant(statement: str) -> dict[tuple[str, str], frozenset[str]]:
    match = re.fullmatch(
        r'GRANT (?P<privileges>[A-Z, ]+) ON (?:(?P<kind>DATABASE|SCHEMA|SEQUENCE|TABLE) )?'
        r'(?P<name>[\w."]+) TO "(?P<role>[^"]+)"',
        statement,
    )
    assert match is not None, f"unparsed grant: {statement}"
    kind = match.group("kind") or "TABLE"
    name = match.group("name").replace('"', "")
    privileges = frozenset(part.strip() for part in match.group("privileges").split(","))
    return {(kind, name): privileges}


#: Every way the measured rounds name a relation, mapped to the privilege
#: PostgreSQL checks for it. `ALTER` is included so the scan can *find* Round 2's
#: migration and the test can assert it is knowingly excluded, rather than the plan
#: silently not covering it.
_MEASURED_SQL_VERBS = (
    (r"INSERT\s+INTO\s+(public\.\w+)", "INSERT"),
    (r"DELETE\s+FROM\s+(public\.\w+)", "DELETE"),
    (r"UPDATE\s+(public\.\w+)\s+SET", "UPDATE"),
    (r"FROM\s+(public\.\w+)", "SELECT"),
    (r"ALTER\s+TABLE\s+(public\.\w+)", "ALTER"),
)


def _measured_lakebase_sql() -> dict[str, str]:
    """The SQL each measured round issues against its Lakebase database.

    Read out of the objects that issue it -- the two frozen contracts and the
    probe attempt's own source -- so a statement added to any of them is picked up
    without this list being edited.
    """

    import inspect

    from server import targets
    from server.recovery import RecoveryContract
    from server.safe_change import SafeChangeContract
    from server.targets import PsycopgPreparedTarget

    # `attempt` interpolates its relation from a module constant rather than
    # spelling it, which is the whole point of `PROBE_TABLE` -- so the scan has to
    # resolve the placeholder the same way the f-string does. Resolved from the
    # module's own attribute, so a renamed constant makes this substitution stop
    # happening and the assertions below fail rather than silently matching nothing.
    probe_source = inspect.getsource(PsycopgPreparedTarget.attempt)
    for name in ("PROBE_TABLE",):
        probe_source = probe_source.replace("{" + name + "}", getattr(targets, name))
    sources: dict[str, str] = {"targets.PsycopgPreparedTarget.attempt": probe_source}
    for label, contract in (
        ("safe_change.SafeChangeContract", SafeChangeContract()),
        ("recovery.RecoveryContract", RecoveryContract()),
    ):
        statements = [
            value
            for name, value in vars(contract).items()
            if name.endswith("_sql") and isinstance(value, str)
        ]
        assert statements, f"{label} exposed no *_sql statements to scan"
        sources[label] = "\n".join(statements)
    return sources


def _measured_relation_verbs() -> dict[str, set[str]]:
    """Every ``(relation, privilege)`` pair the measured rounds actually use."""

    used: dict[str, set[str]] = {}
    for source in _measured_lakebase_sql().values():
        for pattern, privilege in _MEASURED_SQL_VERBS:
            for relation in re.findall(pattern, source):
                used.setdefault(relation, set()).add(privilege)
        # `ON CONFLICT ... DO UPDATE` updates the table the INSERT named, and no
        # `UPDATE <table> SET` appears anywhere for it. Missing this is what would
        # have left the probe upsert with INSERT but not UPDATE.
        if re.search(r"ON\s+CONFLICT[\s\S]{0,80}?DO\s+UPDATE", source):
            for relation in re.findall(r"INSERT\s+INTO\s+(public\.\w+)", source):
                used.setdefault(relation, set()).add("UPDATE")
    return used


def test_the_measured_lakebase_grant_plan_covers_every_relation_and_verb() -> None:
    """The fifth instance of one defect, and the first test that can see it.

    Round 1 was refused at the probe INSERT with SQLSTATE 42501 *after* the bell,
    and Round 3 at arm with `permission denied for table orders`, on a deployed app
    whose `/api/catalog` had published both as ready. The app's principal held
    `CONNECT` on every measured database and `USAGE` on `public` -- which is what
    made it look configured -- and no table privilege anywhere.

    This walks the SQL the rounds actually issue rather than comparing one hardcoded
    list to another, which is the only reason it can catch the Round 4 variant of
    this bug, where the plan named a *plausible* table that was not the one the code
    read. A statement added to either contract, a renamed relation, or a widened
    verb fails here.
    """

    from server.lifecycle import _measured_lakebase_runtime_grants
    from server.safe_change import ORDERS_TABLE
    from server.targets import PROBE_TABLE

    used = _measured_relation_verbs()
    # The scan found both relations, or every assertion below is vacuous.
    assert set(used) == {PROBE_TABLE, ORDERS_TABLE}, sorted(used)

    planned = {grant.table: set(grant.privileges) for grant in _measured_lakebase_runtime_grants()}
    assert set(planned) == set(used), (sorted(planned), sorted(used))

    # PostgreSQL has no grantable ALTER: it requires ownership. Round 2's migration
    # is therefore knowingly outside this plan, and this assertion is what stops
    # somebody "fixing" that by inventing a privilege that does not exist.
    assert used[ORDERS_TABLE] & {"ALTER"} == {"ALTER"}, used[ORDERS_TABLE]
    grantable = {relation: verbs - {"ALTER"} for relation, verbs in used.items()}

    for relation, verbs in grantable.items():
        # Every verb the code issues is granted ...
        assert verbs - planned[relation] == set(), (
            relation,
            sorted(verbs - planned[relation]),
        )
        # ... and nothing beyond them is, which is what keeps this least-privilege
        # and stops a future edit reaching for ALL.
        assert planned[relation] - verbs == set(), (
            relation,
            sorted(planned[relation] - verbs),
        )


#: What the `database-projects` permissions API offers, quoted from its own
#: `permissionLevels` response, and which of the two authorizes branch creation.
#: `CAN_USE` reads "the permission to create catalogs and tables within the
#: database project" -- catalogs and tables, not branches. There is no third,
#: narrower level to reach for.
_LAKEBASE_LEVEL_THAT_CREATES_BRANCHES = "CAN_MANAGE"


def _lakebase_lanes_that_create_a_branch() -> set[str]:
    """Every Lakebase adapter method that creates a branch on the control plane.

    Read out of the adapter's own source rather than listed, because the whole
    question this answers -- which project permission the app needs -- is decided
    by whether any lane still issues this write. Remove the write and the
    requirement below relaxes on its own; keep it and no edit can quietly
    downgrade the permission.
    """

    import inspect

    from server.safe_change_live import LakebaseSafeChangeAdapter

    found: set[str] = set()
    for name, member in vars(LakebaseSafeChangeAdapter).items():
        if not callable(member):
            continue
        try:
            source = inspect.getsource(member)
        except (OSError, TypeError):  # pragma: no cover - defensive
            continue
        if re.search(r'_lakebase_create_path\(\s*[^)]*?"branches"', source):
            found.add(name)
    return found


def test_the_app_holds_the_lakebase_project_permission_its_own_lanes_require() -> None:
    """Round 2 and Round 3 died on a permission nobody had compared to the code.

    The deployed app held `CAN_USE` on every Lakebase project. Its Round 2 lane
    answered "Databricks control-plane request was refused" and its Round 3 lane
    could not verify a recovery, while the *local* server passed both -- because
    the local server authenticates as a principal holding `CAN_MANAGE` and the
    deployed app does not. Same source, different identity, and nothing in the
    tree connected the level to the write that needs it.

    Both halves are derived, which is the only reason this can catch a
    regression:

    * the required level comes from whether an adapter still creates a branch,
      not from a second copy of the answer;
    * the projects come from the sealed environments the runtime resolves its own
      connections through, so the pairing that left r1, r2, r3 and r5 with no ACL
      entry at all cannot come back by adding a round.
    """

    from server.lifecycle import _lakebase_app_project_ids
    from server.manifest import LakebaseEnvironmentSeal, RoundEnvironmentSeal, RoundId

    # The scan found the write, or the requirement below is vacuous. Round 3
    # reuses Round 2's adapter, so this one method is both rounds' isolation step.
    creators = _lakebase_lanes_that_create_a_branch()
    assert creators == {"create_isolated"}, sorted(creators)
    assert _LAKEBASE_APP_PROJECT_PERMISSION == _LAKEBASE_LEVEL_THAT_CREATES_BRANCHES

    def seal(project_id: str) -> RoundEnvironmentSeal:
        branch = f"projects/{project_id}/branches/production"
        return RoundEnvironmentSeal(
            lakebase=LakebaseEnvironmentSeal(
                project_id=project_id,
                project_uid=f"uid-{project_id}",
                branch_name=branch,
                branch_uid=f"branch-{project_id}",
                endpoint_name=f"{branch}/endpoints/primary",
                endpoint_uid=f"endpoint-{project_id}",
                direct_host=f"{project_id}.database.example.com",
                pooled_host=f"{project_id}-pooler.database.example.com",
            )
        )

    round_ids = tuple(RoundId)
    projects = {round_id: f"anti-demo-test-r{index}" for index, round_id in enumerate(round_ids, 1)}
    coordination_project = "anti-demo-test-coord"
    manifest = make_manifest().model_copy(
        update={
            "round_environments": {
                round_id: seal(project) for round_id, project in projects.items()
            },
            "coordination_environment": seal(coordination_project).lakebase,
        }
    )

    granted = _lakebase_app_project_ids(manifest)
    # Every sealed round, plus coordination. Coordination is the one an
    # enumeration over `round_environments` alone would silently omit, and the
    # app reads it on every request that touches a ring lease or a receipt.
    assert set(granted) == set(projects.values()) | {coordination_project}, sorted(granted)
    assert len(granted) == len(set(granted)) == len(round_ids) + 1, granted


#: Every statement class PostgreSQL checks against relation OWNERSHIP rather than
#: against a grantable privilege. No `GRANT` satisfies any of them -- which is the
#: whole reason this test exists apart from its privilege-side sibling. Kept wider
#: than the code currently issues so that *adding* one of these to a contract is
#: what fails, rather than being silently uncovered.
_OWNERSHIP_REQUIRING_DDL = (
    (r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(public\.\w+)", "ALTER TABLE"),
    (r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(public\.\w+)", "DROP TABLE"),
    (
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
        r"[\w.]+\s+ON\s+(public\.\w+)",
        "CREATE INDEX",
    ),
    (r"TRUNCATE\s+(?:TABLE\s+)?(public\.\w+)", "TRUNCATE"),
    (r"COMMENT\s+ON\s+TABLE\s+(public\.\w+)", "COMMENT"),
    (r"CREATE\s+TRIGGER\s+[\s\S]{0,80}?ON\s+(public\.\w+)", "CREATE TRIGGER"),
    (r"REINDEX\s+(?:TABLE\s+)?(public\.\w+)", "REINDEX"),
)

#: Which round each measured contract belongs to, so the scan below can say *which
#: lane* needs ownership rather than only which relation does.
_MEASURED_CONTRACTS = ((2, "safe_change.SafeChangeContract"), (3, "recovery.RecoveryContract"))


def _measured_ownership_ddl() -> dict[int, set[str]]:
    """Per round, the relations its own SQL issues ownership-requiring DDL against.

    Reuses `_measured_lakebase_sql`, which reads the contracts and the probe's
    source rather than a list, so this inherits that function's property: a
    statement added to either contract is picked up without this test being
    edited.
    """

    sources = _measured_lakebase_sql()
    per_round: dict[int, set[str]] = {}
    for number, label in _MEASURED_CONTRACTS:
        source = sources[label]
        for pattern, _verb in _OWNERSHIP_REQUIRING_DDL:
            for relation in re.findall(pattern, source, re.IGNORECASE):
                per_round.setdefault(number, set()).add(relation)
    return per_round


def test_every_relation_the_rounds_run_ddl_against_is_owned_by_a_role_the_app_is_in() -> None:
    """"must be owner of table orders", and why no GRANT could ever have fixed it.

    The privilege plan was complete and correct -- SELECT, INSERT and DELETE on
    `orders`, all granted, all verified -- and Round 2 still failed inside its
    branch, because `ALTER TABLE` is an ownership check and ownership is not a
    privilege. `_apply_schema` seeds these tables as the operator, a Lakebase
    branch inherits its parent's ownership, so the app owned nothing in a branch
    it had just created itself.

    Derived on both axes, which is what makes it a check rather than a restatement:

    * the relations come from scanning the contracts for the whole
      ownership-requiring DDL class, not from the plan, so a `TRUNCATE` or a
      `CREATE INDEX` added to either contract fails here;
    * the coverage is asserted per round, so Round 3 gaining a migration -- it
      already reuses Round 2's branch-creating adapter -- cannot quietly rely on
      a plan that only names Round 2.
    """

    from server.lifecycle import _measured_lakebase_owned_relations
    from server.safe_change import ORDERS_TABLE

    needed = _measured_ownership_ddl()
    # The scan found Round 2's migration, or every assertion below is vacuous.
    assert needed == {2: {ORDERS_TABLE}}, needed

    planned = {number: set(relations) for number, relations in
               _measured_lakebase_owned_relations().items()}

    # Every round that issues ownership-requiring DDL is covered ...
    assert set(needed) - set(planned) == set(), sorted(set(needed) - set(planned))
    # ... and no round is handed ownership it has no DDL to justify, because
    # membership of the owning role is effectively full access to the table.
    assert set(planned) - set(needed) == set(), sorted(set(planned) - set(needed))
    for number, relations in needed.items():
        assert relations == planned[number], (number, sorted(relations), sorted(planned[number]))


def test_a_fresh_install_grants_the_complete_coordination_runtime_set(monkeypatch) -> None:
    """The four-table gap that held a deployed app at `/readyz` 503, closed.

    The app role had `arw` on `ring_lease`, `ar` on the journal, `rU` on the
    journal's sequence and no ACL entry at all on `startup_readiness`,
    `cost_ledger`, `cost_reconciliation_snapshot` or `cost_calibration_profile`,
    because those four statements were never issued. `StartupReadinessStore.read()`
    was refused, the readiness gate classified the refusal permanent, and the
    replica never came back into rotation.

    Three independent things have to agree for that to be closed, and none of
    them is a list of table names typed out next to another list of table names:

    * every table `server/` creates in the coordination schema appears in the
      plan, derived from the `CREATE TABLE` statements rather than from the plan;
    * the privileges match `docs/DEPLOY.md`, which is the authoritative set;
    * `_grant_round4_postgres` actually issues the whole plan, checked against
      the SQL it composes rather than against the plan it read;
    * and every table in the plan is created by setup's own initializer, which
      is the direction nothing checked. `round4_pipeline_power` was granted and
      initialized nowhere, so the `GRANT` failed mid-provision -- the plan and
      the DDL scan both agreed, because both were looking at the same
      `CREATE TABLE` statement in a store setup never called.

    A FOURTH THING, AND THE REASON THIS TEST DRIVES A CALLER. Every assertion
    above was already true while nothing checked that a fresh install *reaches*
    the grant at all: this test used to invoke `_grant_round4_postgres` directly,
    so deleting its one call site left the whole suite green. That absence is
    invisible at startup -- the store initializers probe `pg_catalog` for the
    relations, not the ACL, so `initialize()` returns cleanly with zero
    privileges -- and it surfaces as an HTTP 409 at the bell, forty minutes of
    provisioning after the mistake. So the entry point here is
    `_prepare_and_reseal_round4`, the function `setup` calls on the from-scratch
    path, and the grant is reached the way a provision reaches it.

    WHAT THIS STILL CANNOT CATCH, stated so the next reader does not over-trust
    it:

    * the last hop, `setup` -> `_prepare_and_reseal_round4`, is asserted
      structurally rather than executed, because `setup`'s fresh path runs
      Terraform. A structural check sees the call; it cannot see that
      `round4_prepared` is still False when it is reached;
    * the Round 4 control-plane work before the grant is stubbed, so this says
      nothing about whether any of it is correct -- only that a provision that
      got through it issues the grants;
    * the SQL is composed, not executed, so a privilege PostgreSQL would refuse
      to grant looks identical here to one it accepts;
    * nothing here checks that the *sequences* the plan grants on exist. That is
      safe today only because every one of them is created implicitly by the
      `bigserial` column of a table this test does check for.
    """

    from server.coordination import COORDINATION_SCHEMA
    from server.lifecycle import _coordination_runtime_grants

    documented = _deploy_doc_grants()
    durable = _durable_coordination_tables()
    planned = {grant.table for grant in _coordination_runtime_grants()}
    initialized = _setup_initialized_coordination_tables()

    # The scan found something, or the two assertions below prove nothing.
    assert initialized, "the initializer scan resolved no coordination tables"
    # A granted table setup never creates is a GRANT on an absent relation: a
    # hard error partway through a provision, or -- if the grant is issued
    # before the completeness probe -- an app holding privileges on nothing.
    assert planned - initialized == set(), sorted(planned - initialized)
    # And the other way: a table setup creates but nothing grants is a table the
    # deployed app cannot read. Kept in the same assertion pair so neither side
    # can drift without the other noticing.
    assert initialized - planned == set(), sorted(initialized - planned)

    # Eight tables, or the DDL scan is broken and proves nothing below. The
    # eighth is `round4_pipeline_power`: the app now stops the Round 4 pipeline
    # once a bout has settled, and a stop it could not record is one a later
    # check reports as a failure rather than as the deliberate saving it is.
    assert len(durable) == 8, durable

    # Every durable coordination table is granted. This is the assertion that
    # fails when someone adds a table to a store and forgets the grant -- the
    # failure mode that produced tonight's 503 weeks after the code shipped.
    assert {name for kind, name in documented if kind == "TABLE"} == set(durable)
    # ... and every sequence behind a `serial` column, since an INSERT that omits
    # such a column reads the sequence and INSERT alone is not enough.
    assert {name for kind, name in documented if kind == "SEQUENCE"} == {
        f"{table}_{column}_seq"
        for table, columns in durable.items()
        for column in columns
    }

    manifest = make_manifest()
    names = _round4_names(manifest)
    principal = "11111111-2222-3333-4444-555555555555"
    issued: dict[str, list[str]] = {}

    class Cursor:
        def __init__(self, statements: list[str]) -> None:
            self._statements = statements
            self._rows: list[tuple[object, ...]] = []

        async def __aenter__(self) -> "Cursor":
            return self

        async def __aexit__(self, *exc_info: object) -> bool:
            return False

        async def execute(self, statement, parameters=None) -> None:
            if isinstance(statement, str):
                # The `pg_catalog` probe that refuses to grant on an absent
                # relation. Answered as though the schema setup provisions is
                # fully there, which is the state this test is about.
                self._rows = (
                    [(name,) for name in (parameters or ((), ()))[-1]]
                    if "pg_class" in statement
                    else [(1,)]
                )
                return
            self._statements.append(statement.as_string())

        async def fetchone(self) -> tuple[object, ...] | None:
            return self._rows[0] if self._rows else None

        async def fetchall(self) -> list[tuple[object, ...]]:
            return list(self._rows)

    class Connection:
        def __init__(self, host: str) -> None:
            self.statements = issued.setdefault(host, [])

        async def __aenter__(self) -> "Connection":
            return self

        async def __aexit__(self, *exc_info: object) -> bool:
            return False

        def cursor(self) -> Cursor:
            return Cursor(self.statements)

        async def commit(self) -> None:
            return None

    class Provider:
        async def connection_material(self) -> ConnectionMaterial:
            return ConnectionMaterial(
                host="round4.test",
                port=5432,
                database="anti_demo",
                user=manifest.databricks.user,
                password="round4-token",
            )

    def fake_databricks_json(profile: str, *arguments: str):
        if arguments[1] == "get-endpoint":
            return {
                "name": arguments[2],
                "status": {"hosts": {"host": "coordination.test"}},
            }
        return {"token": "coordination-token"}

    monkeypatch.setattr("server.lifecycle.apply_manifest_environment", lambda _: None)
    monkeypatch.setattr(
        "server.lifecycle._round_lakebase_provider",
        lambda candidate, round_id, **kwargs: Provider(),
    )
    monkeypatch.setattr("server.lifecycle._databricks_json", fake_databricks_json)

    async def fake_connect(material, **kwargs) -> Connection:
        return Connection(material.host)

    monkeypatch.setattr("server.lifecycle._connect", fake_connect)

    # Fail closed rather than open. Everything below stubs the control-plane work
    # `_ensure_round4` does before the grant; if a refactor adds a step these
    # stubs do not cover, the test has to say so rather than quietly reaching for
    # a Databricks workspace or an AWS account from a unit run.
    def refuse(label: str):
        def refused(*arguments: object, **keywords: object):
            raise AssertionError(
                f"this test reached un-stubbed {label}, which would be a live "
                "control-plane call from a unit run. Stub the new step rather "
                "than letting the guard depend on credentials."
            )

        return refused

    for chokepoint in ("_run", "_databricks_api", "_databricks_api_optional", "_sql_statement"):
        monkeypatch.setattr(f"server.lifecycle.{chokepoint}", refuse(chokepoint))
    monkeypatch.setattr("subprocess.run", refuse("subprocess.run"))

    # The state a from-scratch install reaches the grant in: no sealed Round 4, so
    # `_ensure_round4` takes its create-and-validate path and reads both
    # identifiers out of the environment.
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "warehouse-id")
    monkeypatch.setenv("DATABRICKS_APP_CLIENT_ID", principal)
    assert manifest.round4 is None, "this must exercise the from-scratch path, not a reseal"

    # Every control-plane read and validation ahead of the grant, answered with the
    # shape a converged provision hands it. None of these is what this test is
    # about; they exist so that the real `_grant_round4_postgres` call site is
    # reached through its real caller instead of being invoked by hand.
    for helper, result in (
        ("_verify_databricks_identity", manifest.databricks.user),
        ("_require_round4_catalog", None),
        ("_get_lakebase_project_or_none", {"uid": "project-uid"}),
        ("_round4_get_branch", {"uid": "branch-uid"}),
        ("_validate_round4_project_and_branch", ("project-uid", "branch-uid")),
        ("_round4_get_synced_table", None),
        ("_round4_get_database_synced_table", None),
        ("_prepare_round4_source_artifacts", None),
        ("_round4_get_uc_object", {}),
        ("_validate_round4_uc_object", {}),
        ("_create_or_get_round4_synced_table", {}),
        ("_wait_round4_cross_endpoint_table", ({}, {})),
        ("_validate_round4_synced_table", ("synced-uid", "pipeline-id")),
        ("_validate_round4_database_synced_table", None),
        ("_round4_get_pipeline", {}),
        ("_validate_round4_pipeline", None),
        ("_validate_round4_uc_contract", None),
        ("_repair_round4_baseline", 1),
        ("_wait_round4_baseline", ({"name": names["resource_name"]}, {})),
        ("_grant_lakebase_app_projects", ()),
        ("_ensure_round4_app_roles", None),
        ("_grant_round4_uc_and_warehouse", None),
    ):
        monkeypatch.setattr(
            f"server.lifecycle.{helper}",
            lambda *arguments, _result=result, **keywords: _result,
        )

    async def no_failures(*arguments: object, **keywords: object) -> list[str]:
        return []

    # The measured lanes and the scale-zero wait: different databases and a real
    # sleep, both sequenced after the grant this test is about.
    #
    # `_ensure_lakebase_app_roles` is stubbed here rather than in the block above
    # because `_ensure_round4` now calls it a second time, directly and *after*
    # `_grant_round4_postgres`, to create the app's Postgres role on the measured
    # branches that the two stubbed lines below then grant inside. Stubbing
    # `_ensure_round4_app_roles` only covers the earlier call, which reaches this
    # function through that wrapper; the measured-lane call has no wrapper, so it
    # walked into the un-stubbed `_run` guard. Nothing is lost by stubbing it:
    # the function's own behaviour -- the role it creates per branch, the project
    # permission it pairs with that role, and the fact that a `create=False`
    # verification pass grants nothing -- is asserted directly by
    # `test_every_lakebase_branch_role_comes_with_its_project_permission` below.
    monkeypatch.setattr(
        "server.lifecycle._ensure_lakebase_app_roles", lambda *arguments, **keywords: None
    )
    monkeypatch.setattr("server.lifecycle._own_measured_lakebase_relations", no_failures)
    monkeypatch.setattr("server.lifecycle._grant_measured_lakebase_postgres", no_failures)

    async def already_at_zero(*arguments: object, **keywords: object) -> None:
        return None

    monkeypatch.setattr("server.lifecycle.wait_for_scale_zero", already_at_zero)

    lifecycle._prepare_and_reseal_round4(manifest, timeout=1.0)

    assert "coordination.test" in issued, (
        "a from-scratch install sealed Round 4 without issuing one coordination "
        "GRANT: `_prepare_and_reseal_round4` -> `_ensure_round4` no longer reaches "
        "`_grant_round4_postgres`. The deployed app then holds no privilege on the "
        "coordination schema, `initialize()` still returns cleanly because its "
        "probes read `pg_catalog` rather than the ACL, and the first symptom is an "
        "HTTP 409 at the bell -- forty minutes of provisioning after the mistake."
    )

    coordination: dict[tuple[str, str], frozenset[str]] = {}
    for statement in issued["coordination.test"]:
        assert f'TO "{principal}"' in statement, statement
        coordination.update(_parse_grant(statement))
    assert coordination == documented

    # The Round 4 production endpoint is a different database on a different
    # connection, and keeps its own narrow read grant. Asserted so that a future
    # loop over the coordination plan cannot start issuing coordination grants
    # against the measured endpoint without failing here.
    assert _parse_grant(issued["round4.test"][2]) == {
        ("TABLE", f"{names['online_schema']}.model_scores"): frozenset({"SELECT"})
    }
    assert not any(COORDINATION_SCHEMA in statement for statement in issued["round4.test"])

    # The one hop above `_prepare_and_reseal_round4` that cannot be executed here,
    # because `setup`'s from-scratch path runs Terraform. Asserted structurally so
    # that dropping the call -- the same mutation the behavioural half catches one
    # level down -- is still a red test. See the docstring for what this cannot see.
    lifecycle_source = Path(lifecycle.__file__).resolve().read_text(encoding="utf-8")
    setup_definition = next(
        node
        for node in ast.walk(ast.parse(lifecycle_source))
        if isinstance(node, ast.FunctionDef) and node.name == "setup"
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_prepare_and_reseal_round4"
        for node in ast.walk(setup_definition)
    ), (
        "`setup` no longer calls `_prepare_and_reseal_round4`, so nothing on the "
        "from-scratch install path reaches the coordination grants at all and every "
        "assertion above is testing a function no installer runs."
    )


def _inspect_sync_urls(manifest: DemoManifest, monkeypatch) -> list[str]:
    """Every control-plane path `LiveModelScoreAdapter.inspect_sync` requests.

    Built through `build_model_score_engine` rather than by assembling a config
    by hand, so the chain under test is the real one: `_round4_names` ->
    `Round4Resources` -> `ModelScoreLiveConfig` -> the URL the adapter GETs.
    """

    from server import model_score_live

    monkeypatch.setattr(model_score_live, "WorkspaceClient", lambda **kwargs: object())
    adapter = model_score_live.build_model_score_engine(manifest).adapter
    requested: list[str] = []

    async def record(path: str) -> dict[str, object]:
        requested.append(unquote(path))
        return {}

    monkeypatch.setattr(adapter, "_get_json", record)
    # `inspect_sync` gathers every read before validating any of them, so the
    # paths are all recorded and then the first empty payload is rejected.
    with pytest.raises(Exception):  # noqa: B017 - the refusal itself is not the subject
        asyncio.run(adapter.inspect_sync())
    return requested


def test_the_app_holds_select_on_every_unity_catalog_object_round4_reads() -> None:
    """The grant plan is a covering set of what the adapter reads, not a guess.

    The defect: `_grant_round4_uc_and_warehouse` granted `SELECT, MODIFY` on
    `<catalog>.<source schema>.model_scores_source` and stopped, while
    `inspect_sync` reads `<catalog>.<online schema>.model_scores` -- a real,
    adjacent, plausible-looking table that nothing granted. The app held
    `USE SCHEMA` on the online schema and no privilege at all on the one table
    in it, so the deployed app died at arm with `PermissionDenied: User does not
    have SELECT on Table '<catalog>.<online schema>.model_scores'`.

    A test comparing the plan's names to names typed out here would have passed
    against the broken plan, because both sides would have said
    `model_scores_source`. So this asserts coverage instead: any sealed Unity
    Catalog name that shows up in a URL `inspect_sync` actually requests must
    have an entry in `_round4_unity_catalog_grants`.
    """

    manifest = make_manifest()
    names = attach_round4(manifest)
    with pytest.MonkeyPatch.context() as patch:
        requested = _inspect_sync_urls(manifest, patch)

    granted = {grant.name for grant in lifecycle._round4_unity_catalog_grants(names)}
    candidates = {
        names["catalog"],
        f"{names['catalog']}.{names['source_schema']}",
        f"{names['catalog']}.{names['storage_schema']}",
        f"{names['catalog']}.{names['online_schema']}",
        names["source_table"],
        names["synced_table_id"],
    }
    read = {
        candidate
        for candidate in candidates
        if any(
            re.search(re.escape(candidate) + r"(?![A-Za-z0-9_])", path) for path in requested
        )
    }

    # Guard the scan itself: if the adapter stops naming the synced table in a
    # URL, this test would otherwise start passing by finding nothing to check.
    assert names["synced_table_id"] in read, requested
    assert read - granted == set(), sorted(read - granted)

    # And the privileges are the least each read needs. The synced table is
    # written by the Managed Sync pipeline and only ever read by the app.
    privileges = {
        grant.name: set(grant.privileges)
        for grant in lifecycle._round4_unity_catalog_grants(names)
    }
    assert privileges[names["synced_table_id"]] == {"SELECT"}
    assert privileges[names["source_table"]] == {"SELECT", "MODIFY"}


def test_round6_grants_the_catalog_and_schema_its_own_feed_wrote_to() -> None:
    """Round 6 had no Unity Catalog grants at all, and arming said so.

    Setup created the app's Lakebase branch role and stopped, so the deployed
    app was refused inside `get_cdf_config` with `PermissionDenied: User does
    not have USE SCHEMA on Schema '<catalog>.<destination schema>'`.

    The destination is created by the native CDF feed rather than by setup, so
    the only name that can be right is the feed's own `uc_table` -- the same
    string `prepare_round6` seals as `destination_table_full_name` and
    `LiveOrdersLiveAdapter.read_history` selects from. Splitting the catalog and
    schema back out of it, rather than re-reading `DATABRICKS_CDF_CATALOG` and
    `round6_names`, is what makes a grant on a different schema impossible.
    """

    destination = "main.anti_demo_r6_install_001.lb_live_orders_history"
    grants = lifecycle._round6_unity_catalog_grants(destination)

    assert [(grant.securable, grant.name, grant.privileges) for grant in grants] == [
        ("CATALOG", "main", ("USE CATALOG",)),
        ("SCHEMA", "main.anti_demo_r6_install_001", ("USE SCHEMA",)),
        ("TABLE", destination, ("SELECT",)),
    ]
    # Read-only throughout: the CDF feed writes the history table, never the app.
    assert all("MODIFY" not in grant.privileges for grant in grants)

    with pytest.raises(RuntimeError, match="three-level"):
        lifecycle._round6_unity_catalog_grants("main.anti_demo_r6_install_001")


def test_every_lakebase_branch_role_comes_with_its_project_permission(monkeypatch) -> None:
    """A branch role the app cannot reach is the Round 6 refusal, in one place.

    `_ensure_lakebase_app_roles` created the app's Lakebase OAuth role on each
    branch and granted nothing on the project above it, so Round 6 arming died
    on its first `/api/2.0/postgres/...` read with "please contact the workspace
    admin to assign the user <app> 'Can Use' or 'Can Manage' for Database
    project <uid>". Both rounds reach the app's Lakebase identity through this
    one function, so the pairing is asserted here rather than at each caller.
    """

    manifest = make_manifest()
    principal = "11111111-2222-3333-4444-555555555555"
    branches = (
        "projects/install-r6/branches/production",
        "projects/install-coord/branches/coordination",
    )
    permission_calls: list[list[str]] = []
    monkeypatch.setattr(
        "server.lifecycle._run",
        lambda arguments, **kwargs: permission_calls.append(arguments),
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_optional",
        lambda profile, path: {
            "name": unquote(path.removeprefix("/api/2.0/postgres/")),
            "role_id": f"app-{principal}",
            "status": {
                "role_id": f"app-{principal}",
                "identity_type": "SERVICE_PRINCIPAL",
                "auth_method": "LAKEBASE_OAUTH_V1",
                "postgres_role": principal,
                "membership_roles": [],
                "attributes": {},
            },
        },
    )

    _ensure_lakebase_app_roles(manifest, principal, branches, timeout=30)

    assert [call[2:5] for call in permission_calls] == [
        ["update", "database-projects", "install-r6"],
        ["update", "database-projects", "install-coord"],
    ]
    for call in permission_calls:
        entry = json.loads(call[6])["access_control_list"][0]
        assert entry == {
            "service_principal_name": principal,
            # Read from the constant rather than spelled again. This assertion
            # used to hardcode `CAN_USE` beside the constant's own `CAN_USE`,
            # which made it a copy of the answer instead of a check on it -- so
            # the level and the branch creation it has to authorize were never
            # compared. `..._require` above is the test that compares them.
            "permission_level": _LAKEBASE_APP_PROJECT_PERMISSION,
        }

    # Verification passes (`create=False`) must not grant anything: `antidemo
    # doctor` runs it against an installation it is only inspecting.
    permission_calls.clear()
    _ensure_lakebase_app_roles(manifest, principal, branches, timeout=30, create=False)
    assert permission_calls == []


def test_cleanup_deletes_synced_table_then_schemas_before_project(monkeypatch, tmp_path) -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    payload = round4_payload(manifest, names)
    database_payload = database_round4_payload(manifest, names)
    managed_pipeline = pipeline_payload(manifest, names)
    manifest.round4 = None
    manifest.manifest_version = 1
    owned_manifest = tmp_path / "manifest.json"
    owned_manifest.write_text("{}")
    calls: list[str] = []

    monkeypatch.setattr("server.lifecycle.load_manifest", lambda: manifest)
    monkeypatch.setattr("server.lifecycle.manifest_path", lambda: owned_manifest)
    monkeypatch.setattr(
        "server.lifecycle._verify_databricks_identity",
        lambda profile: manifest.databricks.user,
    )
    monkeypatch.setattr("server.lifecycle._verify_aws_identity", lambda *args: None)
    monkeypatch.setattr("server.lifecycle._terraform_init", lambda candidate: None)
    monkeypatch.setattr("server.lifecycle._terraform_managed_addresses", lambda candidate: set())
    # An empty Terraform state now has to be corroborated by the account before
    # cleanup will act on it, so the AWS side of this installation is stated to
    # be genuinely gone rather than left to an unstubbed `_aws_session` failing.
    # Relying on that failure made this test pass for the wrong reason, and on a
    # machine that happened to own a profile named `sandbox-admin` it would have
    # aimed a describe-* sweep at a real account.
    monkeypatch.setattr(
        "server.lifecycle.reconcile_live", lambda candidate, factory: reconcile(candidate, ())
    )
    monkeypatch.setattr(
        "server.lifecycle._get_lakebase_project_or_none",
        lambda candidate: {
            "project_id": manifest.run_id,
            "name": f"projects/{manifest.run_id}",
            "uid": "project-uid-001",
            "status": {"pg_version": 17},
        },
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_branch",
        lambda candidate, expected: {
            "name": names["branch"],
            "branch_id": "production",
            "parent": names["project"],
            "uid": "branch-uid-001",
        },
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_synced_table",
        lambda candidate, expected: payload,
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_database_synced_table",
        lambda candidate, expected: database_payload,
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_pipeline",
        lambda candidate, pipeline_id: managed_pipeline,
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_optional",
        lambda profile, path: {
            "full_name": unquote(path.rsplit("/", 1)[-1]),
            "owner": manifest.databricks.user,
            "created_by": manifest.databricks.user,
        },
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_list_uc_tables",
        lambda candidate, schema, *, catalog: {
            names["source_schema"]: [{"full_name": names["source_table"]}],
            names["storage_schema"]: [],
            names["online_schema"]: [{"full_name": names["synced_table_id"]}],
        }[schema]
        if catalog == names["catalog"]
        else pytest.fail(f"cleanup listed tables in the wrong catalog: {catalog}"),
    )

    def fake_api(profile, method, path, **kwargs):
        assert method == "delete"
        calls.append("synced_table")
        return {"done": True}

    def fake_uc_delete(profile, path):
        full_name = unquote(path.split("?", 1)[0].rsplit("/", 1)[-1])
        calls.append(f"schema:{full_name.split('.', 1)[1]}")

    def fake_run(arguments, **kwargs):
        assert "delete-project" in arguments
        calls.append("project")
        return None

    monkeypatch.setattr("server.lifecycle._databricks_api", fake_api)
    monkeypatch.setattr("server.lifecycle._databricks_api_delete_no_response", fake_uc_delete)
    monkeypatch.setattr("server.lifecycle._run", fake_run)

    cleaned = cleanup(dry_run=False)

    assert cleaned is manifest
    assert calls == [
        "synced_table",
        f"schema:{names['online_schema']}",
        f"schema:{names['storage_schema']}",
        f"schema:{names['source_schema']}",
        "project",
    ]


def _stub_round6_drifted_cleanup(monkeypatch, tmp_path):
    """A v6 manifest whose live endpoint carries the pre-always-on convention."""

    from test_round6_lifecycle import aug19_drifted_endpoint, round6_manifest, round6_workspace

    from server.round6_lifecycle import round6_names

    manifest = round6_manifest(tmp_path)
    workspace = round6_workspace(
        manifest.round6,
        round6_names(manifest),
        aug19_drifted_endpoint(manifest.round6),
        [],
    )
    monkeypatch.setattr("server.round6_lifecycle._workspace", lambda candidate: workspace)
    monkeypatch.setattr("server.lifecycle.load_manifest", lambda: manifest)
    monkeypatch.setattr(
        "server.lifecycle._verify_databricks_identity",
        lambda profile: manifest.databricks.user,
    )
    monkeypatch.setattr("server.lifecycle._verify_aws_identity", lambda *args: None)
    monkeypatch.setattr("server.lifecycle._terraform_init", lambda candidate: None)
    monkeypatch.setattr("server.lifecycle._terraform_managed_addresses", lambda candidate: set())
    monkeypatch.setattr("server.lifecycle._get_lakebase_project_or_none", lambda candidate: None)
    monkeypatch.setattr(
        "server.lifecycle._inspect_round4_for_cleanup",
        lambda candidate: (_round4_names(candidate), None, {}),
    )
    return manifest


def test_cleanup_dry_run_inventories_a_drifted_round6_instead_of_dying(
    monkeypatch, tmp_path, capsys, isolated_lifecycle_manifest
) -> None:
    manifest = _stub_round6_drifted_cleanup(monkeypatch, tmp_path)
    # The two Databricks-side survivors. Neither is a Terraform resource, so
    # `_terraform_managed_addresses` -- which this stub returns empty -- cannot
    # see either one, and before they were inventoried explicitly a dry run
    # printed a clean sheet while the pipeline billed the largest standing line
    # in the installation.
    #
    # The dollar figure, deliberately, and not the share it used to quote. "63%
    # of the standing cost" was never derived from anything, and neither was
    # D15a's "56% of the Databricks side": they are two wrong fractions against
    # two different denominators. The share the panel actually renders is a share
    # of the *Databricks half* and is derived at render time in
    # `standing_cost._continuous`. A hardcoded fraction here would be one more
    # number for the same quantity -- and so would a hardcoded rate, which is why
    # the expected line below is built from the constant the code prints from.
    monkeypatch.setattr(
        "server.lifecycle._round4_get_pipeline",
        lambda candidate, identifier: {"state": "RUNNING"},
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_app_record",
        lambda candidate: {"app_name": "lakebase-anti-demo"},
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_optional",
        lambda profile, path: {"compute_status": {"state": "ACTIVE"}},
    )

    assert cleanup(dry_run=True) is manifest

    printed = capsys.readouterr().out
    assert "OWNED Lakebase project: projects/ad-test-v3" in printed
    # `_aws_session` is left unstubbed here, so the reconciliation lands on
    # `unavailable` -- and that is the case worth keeping rather than papering
    # over, because it is the gap in miniature. Terraform state is empty and the
    # account was never read, so this dry run is entitled to say only that, and
    # never the "OWNED AWS resources: already removed" that used to stand here.
    # It says it and keeps going: `--yes` refuses on this same finding, a dry run
    # must not, or the inspection the operator came for disappears at the first
    # surprise on the installation most likely to surprise them.
    assert "OWNED AWS resources: already removed" not in printed
    assert "DRIFT Terraform state lists no AWS resources for ad-test-v3" in printed
    assert "the account could not be read" in printed
    assert f"--filters Name=tag:{TAG_RUN_ID},Values=ad-test-v3" in printed
    # A dry run must name the pipeline, price it, and say the app outlives the
    # teardown. An operator told nothing at all about these two reads the sheet
    # as an empty account.
    assert f"OWNED Round 4 Managed Sync pipeline: {manifest.round4.pipeline_id}" in printed
    assert f"RUNNING · ${PIPELINE_USD_PER_DAY:.2f}/day {PIPELINE_RATE_TOLERANCE}" in printed
    assert "/month" not in printed
    assert "OWNED Databricks app: lakebase-anti-demo · ACTIVE · SURVIVES THIS CLEANUP" in printed
    assert "databricks apps delete lakebase-anti-demo" in printed
    assert (
        "DRIFT Round 6 cleanup would refuse: "
        "Round 6 endpoint identity or scale-to-zero contract changed"
    ) in printed
    assert (
        "DRIFT   field=spec.no_suspension/status.suspend_timeout_duration "
        "expected=suspension enabled with a 60s window on every returned copy "
        "found=spec.no_suspension=None, spec.suspend_timeout_duration=None, "
        "status.suspend_timeout_duration='86400s'"
    ) in printed
    assert manifest.status == "ready"
    assert not isolated_lifecycle_manifest.exists()


def test_a_billing_app_is_named_even_with_no_successful_deploy_recorded(
    monkeypatch, tmp_path, isolated_lifecycle_manifest
) -> None:
    """The billing miss this inventory exists to prevent, on the path that hit it.

    `bootstrap.sh` writes `app-deploy.json` only when a deploy succeeds, so an
    installation whose app was created but never successfully deployed to has no
    record -- and that is precisely the installation whose app is running with
    nothing served. The inventory used to print "no deploy record beside this
    manifest" there and stop, which reads as an all-clear over ACTIVE compute.
    """

    manifest = _stub_round6_drifted_cleanup(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "server.lifecycle._round4_get_pipeline",
        lambda candidate, identifier: {"state": "RUNNING"},
    )
    # No app-deploy.json beside the manifest -- only the record every locked
    # bootstrap run writes, successful or not. It carries the app's client ID as
    # well as its name, which is what lets this path prove ownership and so
    # delete rather than merely report.
    (isolated_lifecycle_manifest.parent / "bootstrap.json").write_text(
        json.dumps(
            {
                "databricks_app_name": "lakebase-anti-demo",
                "databricks_app_client_id": "spn-owned",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    asked: list[str] = []

    def workspace(profile: str, path: str) -> dict[str, object]:
        asked.append(path)
        return {
            "compute_status": {"state": "ACTIVE"},
            "service_principal_client_id": "spn-owned",
        }

    monkeypatch.setattr("server.lifecycle._databricks_api_optional", workspace)

    lines = _round4_survivor_lines(manifest)

    assert any(
        "OWNED Databricks app: lakebase-anti-demo · ACTIVE · DELETED BY THIS CLEANUP" in line
        for line in lines
    )
    assert any("Named from bootstrap.json" in line for line in lines)
    # The name is derived; the compute state is not. It came from the workspace
    # being asked about that exact app, which is what entitles the line above to
    # say ACTIVE.
    assert asked == ["/api/2.0/apps/lakebase-anti-demo"]


def test_an_app_this_installation_cannot_prove_it_owns_is_reported_never_deleted(
    monkeypatch, tmp_path, isolated_lifecycle_manifest
) -> None:
    """The safety direction of the same decision, and the reason it is not by name.

    `DEFAULT_APP_NAME` is a convention every installation of this repo shares,
    so on a machine with no deploy record the name alone resolves to whatever
    app currently holds it -- which may be another operator's, running. Deleting
    on a name match would make one teardown remove somebody else's app: the
    mirror image of the miss being fixed, and the worse of the two.
    """

    manifest = _stub_round6_drifted_cleanup(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "server.lifecycle._round4_get_pipeline",
        lambda candidate, identifier: {"state": "RUNNING"},
    )
    # Neither record exists, so the name falls back to the documented default
    # and no client ID is recorded anywhere.
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_optional",
        lambda profile, path: {
            "compute_status": {"state": "ACTIVE"},
            "service_principal_client_id": "spn-somebody-else",
        },
    )
    deleted: list[str] = []
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_delete_no_response",
        lambda profile, path: deleted.append(path),
    )

    lines = _round4_survivor_lines(manifest)

    assert any(
        "OWNED Databricks app: lakebase-anti-demo · ACTIVE · SURVIVES THIS CLEANUP" in line
        for line in lines
    )
    assert any("cannot be shown to be ours" in line for line in lines)
    assert any("databricks apps delete lakebase-anti-demo" in line for line in lines)

    _delete_databricks_app(manifest)

    assert deleted == []


def test_a_pipeline_that_outlived_its_synced_table_is_deleted_not_assumed_gone(
    monkeypatch,
) -> None:
    """Defect 4's actual blind spot: the branch that deletes nothing.

    The Managed Sync pipeline is removed as a side effect of deleting the synced
    table that owns it, so `_delete_round4_resources` deleted the table and said
    nothing more. But that branch is guarded by ``synced_table is not None`` --
    an installation whose table was already gone (a half-finished earlier
    teardown, or a table dropped by hand) never entered it, and its pipeline,
    the single largest standing line here, survived a teardown that reported
    success.
    """

    manifest = make_manifest()
    manifest.round4 = SimpleNamespace(pipeline_id="pipe-1")
    looked: list[str] = []
    deleted: list[str] = []
    alive = {"pipe-1"}

    def optional(profile: str, path: str):
        looked.append(path)
        return {"pipeline_id": "pipe-1"} if "pipe-1" in alive else None

    monkeypatch.setattr("server.lifecycle._databricks_api_optional", optional)

    def delete(profile: str, path: str) -> None:
        deleted.append(path)
        alive.clear()

    monkeypatch.setattr("server.lifecycle._databricks_api_delete_no_response", delete)

    # Through the real teardown entry point, with the inventory that used to
    # skip everything: the synced table is already absent. Calling
    # `_delete_round4_pipeline` directly here would pass even with the call site
    # removed, which is the shape of a guard that proves nothing.
    names = {
        "resource_name": "synced_tables/unused",
        "catalog": "unused",
        "online_schema": "unused",
        "storage_schema": "unused",
        "source_schema": "unused",
    }
    _delete_round4_resources(manifest, (names, None, {}))

    assert deleted == ["/api/2.0/pipelines/pipe-1"]
    # Asked again after deleting. A delete call that returns is not a pipeline
    # that is gone, and this is the last point at which anything still knows
    # the ID.
    assert looked == ["/api/2.0/pipelines/pipe-1", "/api/2.0/pipelines/pipe-1"]


def test_a_pipeline_that_survives_its_own_deletion_refuses_the_teardown(monkeypatch) -> None:
    """"Delete returned" is not "gone", and the difference is the whole bill."""

    manifest = make_manifest()
    manifest.round4 = SimpleNamespace(pipeline_id="pipe-1")
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_optional",
        lambda profile, path: {"pipeline_id": "pipe-1"},
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_delete_no_response", lambda profile, path: None
    )

    with pytest.raises(RuntimeError) as refusal:
        _delete_round4_pipeline(manifest)

    assert "still in the workspace and still billing" in str(refusal.value)
    assert "databricks pipelines delete pipe-1" in str(refusal.value)


def test_cleanup_still_refuses_the_same_drifted_round6_for_real(
    monkeypatch, tmp_path, isolated_lifecycle_manifest
) -> None:
    manifest = _stub_round6_drifted_cleanup(monkeypatch, tmp_path)
    # The same stub, and the opposite mode. Its dry-run twin above leaves
    # `_aws_session` unstubbed and reaches the Round 6 gate anyway, because a
    # dry run reports an unreadable account and continues; `--yes` refuses on it
    # outright, which is the difference between the two modes and would
    # otherwise stop this test short of the gate it exists to pin. So the
    # account is stated to be readable and empty, leaving Round 6 as the only
    # thing wrong here.
    monkeypatch.setattr(
        "server.lifecycle.reconcile_live", lambda candidate, factory: reconcile(candidate, ())
    )

    with pytest.raises(RuntimeError) as refusal:
        cleanup(dry_run=False)

    assert str(refusal.value) == (
        "Cleanup refused: Round 6 endpoint identity or scale-to-zero contract changed"
    )
    assert manifest.status == "cleanup_failed"
    assert json.loads(isolated_lifecycle_manifest.read_text())["status"] == "cleanup_failed"


def _stub_refusable_cleanup(monkeypatch, manifest, reconciliation, *, addresses=frozenset()):
    """A cleanup against a chosen Terraform state and a chosen account read.

    Every destructive call is a failure rather than a recording, because the
    whole question in both refusal tests is whether cleanup stops before
    reaching one. `addresses` empty is the lost-state-file path; the manifest's
    full expected set is the ordinary path, where Terraform state is populated
    and the destroy would really have run.
    """

    monkeypatch.setattr("server.lifecycle.load_manifest", lambda: manifest)
    monkeypatch.setattr(
        "server.lifecycle._verify_databricks_identity",
        lambda profile: manifest.databricks.user,
    )
    monkeypatch.setattr("server.lifecycle._verify_aws_identity", lambda *args: None)
    monkeypatch.setattr("server.lifecycle._terraform_init", lambda candidate: None)
    monkeypatch.setattr(
        "server.lifecycle._terraform_managed_addresses", lambda candidate: set(addresses)
    )
    monkeypatch.setattr(
        "server.lifecycle._hydrate_aws_resources", lambda candidate, **kwargs: None
    )
    monkeypatch.setattr(
        "server.lifecycle._aws_ownership", lambda candidate: Check("aws_ownership", True, "owned")
    )
    monkeypatch.setattr("server.lifecycle._get_lakebase_project_or_none", lambda candidate: None)
    monkeypatch.setattr(
        "server.lifecycle._inspect_round4_for_cleanup",
        lambda candidate: (_round4_names(candidate), None, {}),
    )
    monkeypatch.setattr(
        "server.lifecycle.reconcile_live",
        lambda candidate, factory: reconciliation,
    )
    monkeypatch.setattr(
        "server.lifecycle._run",
        lambda *args, **kwargs: pytest.fail("a refused cleanup must run no command"),
    )
    monkeypatch.setattr(
        "server.lifecycle._terraform_plan",
        lambda candidate, destroy: pytest.fail("a refused cleanup must plan no destroy"),
    )
    monkeypatch.setattr(
        "server.lifecycle._delete_round4_resources",
        lambda candidate, inventory: pytest.fail("a refused cleanup must delete nothing"),
    )


def test_cleanup_refuses_the_teardown_the_account_says_it_never_did(
    monkeypatch, isolated_lifecycle_manifest
) -> None:
    """An empty Terraform state is not evidence of an empty account.

    `terraform state list` yields no rows for a state file that was lost, moved
    or never written, which is indistinguishable from a finished teardown until
    the account itself is asked by tag. That answer was already being collected
    here -- and was being collected *after* the all-clear had been printed, where
    its verdict gated nothing. So this path told the operator
    "OWNED AWS resources: already removed", skipped the destroy plan, wrote a
    cleanup receipt and unlinked the manifest: the fleet kept billing, the
    operator was told it was gone, and the only local record of what to tear
    down was deleted on the way out. Terraform cannot destroy what its state
    does not list, so the only correct move is to refuse and say why.
    """

    manifest = make_manifest()
    isolated_lifecycle_manifest.write_text("{}")
    # Composed by the real reconciler rather than hand-built, so the report
    # cleanup reads is one `reconcile_live` could actually have returned.
    reconciliation = reconcile(
        manifest,
        (
            ObservedResource(
                AURORA_WRITER,
                manifest.aws.resources.aurora_writer_instance_id,
                "available",
                run_id=manifest.run_id,
                public_ipv4=True,
            ),
            ObservedResource(
                RDS_INSTANCE,
                manifest.aws.resources.rds_instance_id,
                "available",
                run_id=manifest.run_id,
                public_ipv4=True,
            ),
            ObservedResource(
                EC2_RUNNER,
                "i-0123456789abcdef0",
                "running",
                run_id=manifest.run_id,
                public_ipv4=True,
            ),
            # Already on its way out, and belonging to a neighbouring run that
            # the orphan lines report separately. Neither is residue this
            # refusal is entitled to claim, so neither may appear in it.
            ObservedResource(
                RDS_INSTANCE, "anti-demo-rds-retiring", "deleting", run_id=manifest.run_id
            ),
            ObservedResource(
                RDS_INSTANCE, "anti-demo-rds-neighbour", "available", run_id="ad-other-999"
            ),
        ),
    )
    assert not reconciliation.unavailable
    _stub_refusable_cleanup(monkeypatch, manifest, reconciliation)

    with pytest.raises(RuntimeError) as refusal:
        cleanup(dry_run=False)

    message = str(refusal.value)
    assert message.startswith("Cleanup refused: ")
    for identifier in (
        manifest.aws.resources.aurora_writer_instance_id,
        manifest.aws.resources.rds_instance_id,
        "i-0123456789abcdef0",
    ):
        assert identifier in message
    assert "anti-demo-rds-retiring" not in message
    assert "anti-demo-rds-neighbour" not in message
    # The two things the old all-clear destroyed on its way past.
    assert isolated_lifecycle_manifest.exists()
    assert not (isolated_lifecycle_manifest.parent / "cleanup-receipt.json").exists()


def test_cleanup_refuses_to_tear_down_around_a_leaked_per_bout_clone(
    monkeypatch, isolated_lifecycle_manifest
) -> None:
    """The same rule as its sibling above, reached down the populated-state path.

    Terraform destroys what its state lists, and a Round 2 or 3 clone was never
    in that state. So a teardown removed the sealed fleet, wrote a receipt
    saying every owned resource was removed, and unlinked the manifest while the
    clone billed on -- and because `doctor` and `cleanup` both load the
    manifest, the unlink took away the operator's handle for ever finding it
    again. The refusal is deliberately narrow: `ORPHAN_EPHEMERAL` for this run
    only, which is the same set `reap.py` is allowed to delete.
    """

    manifest = make_manifest()
    isolated_lifecycle_manifest.write_text("{}")
    leaked = sorted(ephemeral_artifact_ids(manifest.run_id))
    round2_writer = next(name for name in leaked if name.startswith("adsc-") and "writer" in name)
    round3_rds = next(name for name in leaked if name.startswith("adrc-") and name.endswith("rds"))
    sealed = manifest.aws.resources
    reconciliation = reconcile(
        manifest,
        (
            # The sealed fleet, alive and about to be destroyed by Terraform.
            # `expected_resources` reads the round seals rather than
            # `aws.resources`, so on a manifest sealing no `round_environments`
            # these are classified ORPHAN_UNEXPECTED -- a healthy installation
            # reporting its own fleet as residue. Refusing on that code would
            # block every ordinary teardown, which is why this refusal does not.
            ObservedResource(
                AURORA_WRITER, sealed.aurora_writer_instance_id, "available",
                run_id=manifest.run_id, public_ipv4=True,
            ),
            ObservedResource(
                RDS_INSTANCE, sealed.rds_instance_id, "available",
                run_id=manifest.run_id, public_ipv4=True,
            ),
            # The two that actually outlived their bouts.
            ObservedResource(
                AURORA_WRITER, round2_writer, "available", run_id=manifest.run_id, public_ipv4=True
            ),
            ObservedResource(
                RDS_INSTANCE, round3_rds, "available", run_id=manifest.run_id, public_ipv4=True
            ),
            # A neighbour's residue in this shared account. Refusing over it
            # would wedge this teardown behind a resource the operator neither
            # created nor can remove, so it is reported and never refused on.
            ObservedResource(
                RDS_INSTANCE, "anti-demo-rds-neighbour", "available", run_id="ad-other-999"
            ),
        ),
    )
    assert {finding.code for finding in reconciliation.orphans} == {
        ORPHAN_EPHEMERAL,
        ORPHAN_UNEXPECTED,
        ORPHAN_FOREIGN_RUN,
    }
    _stub_refusable_cleanup(
        monkeypatch,
        manifest,
        reconciliation,
        addresses=_expected_aws_state_addresses(manifest),
    )

    with pytest.raises(RuntimeError) as refusal:
        cleanup(dry_run=False)

    message = str(refusal.value)
    assert message.startswith("Cleanup refused: ")
    assert round2_writer in message
    assert round3_rds in message
    # Neither the fleet Terraform was about to destroy nor the neighbour's.
    assert sealed.aurora_writer_instance_id not in message
    assert sealed.rds_instance_id not in message
    assert "anti-demo-rds-neighbour" not in message
    # The two things the teardown destroyed on its way past, and the reason the
    # owner chose refusal: the manifest is the only local record of this run ID.
    assert isolated_lifecycle_manifest.exists()
    assert not (isolated_lifecycle_manifest.parent / "cleanup-receipt.json").exists()


def test_a_dry_run_denied_the_round5_read_leaves_the_status_on_disk_alone(
    monkeypatch, isolated_lifecycle_manifest
) -> None:
    """A dry run may fail. It may not rewrite the file it was only reading.

    `README.md` sends a newcomer to `antidemo cleanup --dry-run` first, and the
    app-runtime IAM user is not authorized for the `secretsmanager:ListSecrets`
    that `_require_round5_clean_baseline` reaches through
    `_round5_runtime_tag_inventory`. That call sat inside cleanup's teardown
    `try:`, above the `if dry_run: return manifest` guard, so the denial was
    caught by the handler that writes `status = "cleanup_failed"` and saves --
    the one and only writer of that value in the tree. An inspection that
    deleted nothing therefore wedged the installation, because
    `require_ready_manifest` refuses all six rounds on that status.

    The read still runs, and the denial still propagates: an inventory that
    cannot complete must say so rather than print a clean sheet. What it must
    not do is mutate. The disk assertion is the load-bearing one -- the
    in-memory check below it would pass on its own even while the file on disk
    said `cleanup_failed`, because the handler mutated the object *and* saved it.
    """

    manifest = make_manifest()
    # A real file written by the real writer, so the assertions below read what
    # an operator's manifest would actually say rather than a fixture's echo.
    lifecycle.save_manifest(manifest)
    assert json.loads(isolated_lifecycle_manifest.read_text())["status"] == "ready"

    # The full expected address set, which is what makes `complete_baseline`
    # true and so is what reaches the Round 5 baseline call at all. The empty
    # set its dry-run sibling above uses never gets there.
    _stub_refusable_cleanup(
        monkeypatch,
        manifest,
        reconcile(manifest, ()),
        addresses=_expected_aws_state_addresses(manifest),
    )
    # Clean, and reached first: on the installation this reproduces the journal
    # half answered `[]` and the denial landed on the AWS half immediately after.
    monkeypatch.setattr("server.lifecycle._round5_active_journal_addons", lambda candidate: [])
    denial = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": (
                    "User: arn:aws:iam::123456789012:user/anti-demo-app-runtime is not "
                    "authorized to perform: secretsmanager:ListSecrets"
                ),
            }
        },
        "ListSecrets",
    )

    class DeniedSecretsManager:
        def list_secrets(self, **kwargs):
            raise denial

    # Faked at the boto client boundary, which is where the real principal was
    # refused. Stubbing `_require_round5_clean_baseline` itself would assert
    # against the test's own fake instead of the code path that broke.
    monkeypatch.setattr(
        "server.lifecycle._aws_session",
        lambda candidate: SimpleNamespace(
            client=lambda name: {"secretsmanager": DeniedSecretsManager()}[name]
        ),
    )

    with pytest.raises(ClientError) as denied:
        cleanup(dry_run=True)

    assert "secretsmanager:ListSecrets" in str(denied.value)
    assert json.loads(isolated_lifecycle_manifest.read_text())["status"] == "ready"
    assert manifest.status == "ready"
    # Nothing was torn down, so no receipt and no unlink either.
    assert not (isolated_lifecycle_manifest.parent / "cleanup-receipt.json").exists()


def test_aws_hydration_can_inventory_without_writing_the_manifest(monkeypatch) -> None:
    manifest = make_manifest()
    manifest.aws.resources = AwsResources()
    hydrated = AwsResources(aurora_cluster_id="anti-demo-aurora")
    saved: list[str] = []
    monkeypatch.setattr(
        "server.lifecycle._aws_resources_from_outputs", lambda outputs: hydrated
    )
    monkeypatch.setattr(
        "server.lifecycle.save_manifest",
        lambda candidate, path=None: saved.append(candidate.run_id),
    )

    _hydrate_aws_resources(manifest, {}, persist=False)

    assert manifest.aws.resources is hydrated
    assert saved == []

    _hydrate_aws_resources(manifest, {}, persist=True)

    assert saved == [manifest.run_id]


def test_cleanup_retries_an_exact_owned_partial_terraform_destroy(monkeypatch, tmp_path) -> None:
    manifest = make_manifest(status="cleanup_failed")
    owned_manifest = tmp_path / "manifest.json"
    owned_manifest.write_text("{}")
    addresses = {"aws_db_subnet_group.round1", "aws_security_group.aurora"}
    expected_tags = {
        "anti-demo-run-id": manifest.run_id,
        "Owner": manifest.owner,
        "owner": manifest.owner,
        "expires-at": manifest.expires_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "managed-by": "terraform",
    }
    tag_list = [{"Key": key, "Value": value} for key, value in expected_tags.items()]
    calls: list[str] = []

    class FakeRds:
        def describe_db_subnet_groups(self, **kwargs):
            assert kwargs == {"DBSubnetGroupName": manifest.aws.resources.db_subnet_group_name}
            return {
                "DBSubnetGroups": [
                    {
                        "DBSubnetGroupName": manifest.aws.resources.db_subnet_group_name,
                        "DBSubnetGroupArn": "arn:aws:rds:us-west-2:123456789012:subgrp:owned",
                    }
                ]
            }

        def list_tags_for_resource(self, **kwargs):
            assert kwargs["ResourceName"].endswith(":subgrp:owned")
            return {"TagList": tag_list}

    class FakeEc2:
        def describe_security_groups(self, **kwargs):
            assert kwargs == {"GroupIds": [manifest.aws.resources.security_group_id]}
            return {
                "SecurityGroups": [
                    {"GroupId": manifest.aws.resources.security_group_id, "Tags": tag_list}
                ]
            }

    class FakeSession:
        def client(self, name):
            return FakeRds() if name == "rds" else FakeEc2()

    monkeypatch.setattr("server.lifecycle.load_manifest", lambda: manifest)
    monkeypatch.setattr("server.lifecycle.manifest_path", lambda: owned_manifest)
    monkeypatch.setattr(
        "server.lifecycle._verify_databricks_identity",
        lambda profile: manifest.databricks.user,
    )
    monkeypatch.setattr("server.lifecycle._verify_aws_identity", lambda *args: None)
    monkeypatch.setattr("server.lifecycle._terraform_init", lambda candidate: None)
    monkeypatch.setattr(
        "server.lifecycle._terraform_managed_addresses", lambda candidate: addresses
    )
    monkeypatch.setattr(
        "server.lifecycle._terraform_state_resource_values",
        lambda candidate, expected: {
            "aws_db_subnet_group.round1": {"name": manifest.aws.resources.db_subnet_group_name},
            "aws_security_group.aurora": {"id": manifest.aws.resources.security_group_id},
        },
    )
    monkeypatch.setattr("server.lifecycle._aws_session", lambda candidate: FakeSession())
    monkeypatch.setattr(
        "server.lifecycle._hydrate_aws_resources",
        lambda candidate: pytest.fail("partial retry must not require deleted outputs"),
    )
    monkeypatch.setattr(
        "server.lifecycle._aws_ownership",
        lambda candidate: pytest.fail("partial retry must inspect only remaining resources"),
    )
    monkeypatch.setattr(
        "server.lifecycle._terraform_plan",
        lambda candidate, destroy: calls.append("destroy_plan") or tmp_path / "destroy.tfplan",
    )
    monkeypatch.setattr("server.lifecycle._get_lakebase_project_or_none", lambda candidate: None)
    monkeypatch.setattr(
        "server.lifecycle._inspect_round4_for_cleanup",
        lambda candidate: (_round4_names(candidate), None, {}),
    )

    async def fake_safe_change_cleanup(candidate):
        assert candidate is manifest
        calls.append("safe_change_cleanup")

    monkeypatch.setattr(
        "server.lifecycle.reset_safe_change_only_artifacts",
        fake_safe_change_cleanup,
    )
    monkeypatch.setattr(
        "server.lifecycle.reset_safe_change_artifacts",
        lambda candidate: pytest.fail("partial retry must not open deleted sources"),
    )
    monkeypatch.setattr(
        "server.lifecycle._terraform_apply",
        lambda candidate, plan: calls.append("terraform_apply"),
    )
    # The Databricks app. It is not a Terraform resource and never has been, so
    # nothing above this line can see it -- and cleanup used to print "SURVIVES
    # THIS CLEANUP" over compute that stays ACTIVE and billing for as long as
    # the workspace exists. Owned here by the recorded service-principal client
    # ID matching the live app's, which is the only thing that makes deletion
    # safe: the name is a convention every installation of this repo shares.
    monkeypatch.setattr(
        "server.lifecycle._round4_app_record",
        lambda candidate: {"app_name": "lakebase-anti-demo", "app_client_id": "spn-owned"},
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_optional",
        lambda profile, path: {
            "compute_status": {"state": "ACTIVE"},
            "service_principal_client_id": "spn-owned",
        },
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_api_delete_no_response",
        lambda profile, path: calls.append(f"delete {path}"),
    )

    assert cleanup(dry_run=False) is manifest
    # Ordering is the assertion, not just membership. The app assumes the
    # runtime IAM role the destroy removes, so deleting it after
    # `terraform_apply` would leave an app that is broken *and* still billing,
    # with a successful destroy scrolling past to say otherwise.
    assert calls == [
        "destroy_plan",
        "delete /api/2.0/apps/lakebase-anti-demo",
        "safe_change_cleanup",
        "terraform_apply",
    ]


def test_round5_native_login_uses_only_returned_pooled_host(monkeypatch) -> None:
    manifest = make_manifest()
    commands: list[list[str]] = []
    native_login = {"enabled": True}

    def fake_run(arguments, **kwargs):
        del kwargs
        commands.append(arguments)
        native_login["enabled"] = True
        return None

    monkeypatch.setattr("server.lifecycle._run", fake_run)
    monkeypatch.setattr(
        "server.lifecycle._get_lakebase_project_or_none",
        lambda candidate: {
            "project_id": candidate.run_id,
            "name": f"projects/{candidate.run_id}",
            "status": {
                "project_id": candidate.run_id,
                "pg_version": 17,
                "enable_pg_native_login": native_login["enabled"],
            },
        },
    )
    monkeypatch.setattr(
        "server.lifecycle._databricks_json",
        lambda *args: {
            "name": manifest.databricks.endpoint_name,
            "status": {
                "hosts": {
                    "host": "direct.database.us-west-2.cloud.databricks.com",
                    "read_write_pooled_host": "opaque.pooler.us-west-2.example",
                }
            },
        },
    )

    assert _enable_round5_lakebase_native_login(manifest) == (
        "direct.database.us-west-2.cloud.databricks.com",
        "opaque.pooler.us-west-2.example",
    )
    assert commands == []

    native_login["enabled"] = False
    assert _enable_round5_lakebase_native_login(manifest) == (
        "direct.database.us-west-2.cloud.databricks.com",
        "opaque.pooler.us-west-2.example",
    )
    assert commands[0][1:5] == [
        "postgres",
        "update-project",
        f"projects/{manifest.run_id}",
        "spec.enable_pg_native_login",
    ]
    assert json.loads(commands[0][commands[0].index("--json") + 1]) == {
        "spec": {"enable_pg_native_login": True}
    }


def sql_payload(columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> dict[str, object]:
    return {
        "status": {"state": "SUCCEEDED"},
        "manifest": {"schema": {"columns": [{"name": name} for name in columns]}},
        "result": {"data_array": [list(row) for row in rows]},
    }


class FakeRound4SourceTable:
    """A stand-in for the real Delta source table, not for the statements about it.

    The readiness bug this guards was invisible to fixture-shaped tests: every
    fake returned the baseline, so nothing ever described the table as a
    completed round actually leaves it.
    """

    def __init__(self, row: tuple[str, float, str, str]) -> None:
        self.row = row
        self.version = 7
        self.statements: list[str] = []

    def __call__(self, profile: str, warehouse_id: str, statement: str, **kwargs) -> dict:
        self.statements.append(statement)
        if statement.startswith("DESCRIBE DETAIL"):
            return sql_payload(
                ("properties",), [({"delta.enableChangeDataFeed": "true"},)]
            )
        if statement.startswith("DESCRIBE HISTORY"):
            return sql_payload(("version",), [(self.version,)])
        if statement.startswith("SELECT"):
            return sql_payload(
                ("entity_id", "score", "model_version", "proof_nonce"), [self.row]
            )
        if statement.startswith("MERGE INTO"):
            self.row = ("customer-0001", 0.25, "risk-v0", "round4-baseline")
            self.version += 1
            return sql_payload((), [])
        raise AssertionError(f"unexpected statement {statement}")

    @property
    def merges(self) -> list[str]:
        return [item for item in self.statements if item.startswith("MERGE INTO")]


def stub_round4_control_plane(monkeypatch, manifest, names, table) -> None:
    monkeypatch.setattr("server.lifecycle._sql_statement", table)
    monkeypatch.setattr(
        "server.lifecycle._get_lakebase_project_or_none",
        lambda *args, **kwargs: {"name": names["project"]},
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_branch",
        lambda *args, **kwargs: {"name": names["branch"]},
    )
    monkeypatch.setattr(
        "server.lifecycle._validate_round4_project_and_branch",
        lambda *args, **kwargs: ("project-uid-001", "branch-uid-001"),
    )
    monkeypatch.setattr(
        "server.lifecycle._ensure_round4_app_roles", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_synced_table",
        lambda *args, **kwargs: round4_payload(manifest, names),
    )
    monkeypatch.setattr(
        "server.lifecycle._validate_round4_synced_table",
        lambda *args, **kwargs: ("uid", "pipeline"),
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_database_synced_table",
        lambda *args, **kwargs: database_round4_payload(
            manifest, names, version=table.version
        ),
    )
    monkeypatch.setattr(
        "server.lifecycle._validate_round4_database_synced_table",
        lambda *args, **kwargs: None,
    )
    # A real pipeline GET always carries its own `state`, and the readiness check
    # now reads the standing cost off that field rather than making a second
    # call. A stub that omits it would let a check which silently lost the price
    # keep passing.
    monkeypatch.setattr(
        "server.lifecycle._round4_get_pipeline",
        lambda *args, **kwargs: {"state": "RUNNING"},
    )
    monkeypatch.setattr("server.lifecycle._validate_round4_pipeline", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "server.lifecycle._validate_round4_uc_contract", lambda *args, **kwargs: None
    )


def test_round4_readiness_restores_the_baseline_a_completed_round_consumed(monkeypatch) -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    # Exactly what a finished Round 4 leaves behind: its own run-owned v1 proof.
    table = FakeRound4SourceTable(
        ("customer-0001", 0.81, "risk-v1", "round4-v1-3422c47774ab4dddababb2a41527d939")
    )
    stub_round4_control_plane(monkeypatch, manifest, names, table)

    check = _round4_check(manifest, timeout_seconds=5)

    assert check.ok
    assert "baseline restored after a completed run" in check.detail
    assert table.row == ("customer-0001", 0.25, "risk-v0", "round4-baseline")
    assert len(table.merges) == 1


def test_round4_readiness_leaves_an_untouched_baseline_alone(monkeypatch) -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    table = FakeRound4SourceTable(("customer-0001", 0.25, "risk-v0", "round4-baseline"))
    stub_round4_control_plane(monkeypatch, manifest, names, table)

    check = _round4_check(manifest, timeout_seconds=5)

    assert check.ok
    assert check.detail.startswith(manifest.round4.synced_table_resource_name)
    # A healthy answer must still carry the price. This pipeline is the largest
    # standing line in the installation, and `doctor` is the screen an operator
    # looks at before walking away from it; a green line that omits the daily
    # number is what makes forgetting to stop cost anything at all. Built from
    # the constant, never written down: this rate had three independent recorded
    # values before it was derived from its own meter. The daily rate is also the
    # longest horizon this line may state -- see the absence assertion below.
    assert (
        f"RUNNING · ${PIPELINE_USD_PER_DAY:.2f}/day {PIPELINE_RATE_TOLERANCE}"
    ) in check.detail
    assert "/month" not in check.detail
    assert "antidemo pipeline stop" in check.detail
    assert table.merges == []


def test_round4_readiness_still_names_an_overdue_stop_on_a_healthy_pipeline(
    monkeypatch, tmp_path
) -> None:
    """The one power state that leaves the pipeline up, and therefore green.

    Money-spent, under D18. `_round4_check` answered a deliberate stop before
    running the full check and then rebuilt a bare `PipelinePower` for the
    healthy line, and the two branches turned out to be mutually exclusive with
    the state that actually costs money: `PipelinePower.stop_owed` requires
    `running`, the early return requires `not running`. So an overdue stop --- a
    settled bout scheduled a release, the process died before performing it, and
    the pipeline has been billing ever since --- fell through to a completely
    green line that said `RUNNING · $14.57/day` and nothing about the stop. Every
    sync check was correct and the money was silent.

    The marker is written by the real writer into a real state directory, so
    `read_stop_marker`'s due gate and `power_state`'s ledger read both run for
    real. Both directions are asserted: inside the redo window the pipeline is
    supposed to be up, and a `doctor` that shouted then would shout after every
    single bout and be learned away.
    """

    from server.pipeline_power import owed_stop_record, owed_stop_sentence

    manifest = make_manifest()
    names = attach_round4(manifest)
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / "manifest.json"))

    def check_now() -> object:
        table = FakeRound4SourceTable(("customer-0001", 0.25, "risk-v0", "round4-baseline"))
        stub_round4_control_plane(monkeypatch, manifest, names, table)
        return _round4_check(manifest, timeout_seconds=5)

    owed_stop_record(manifest, due_at=datetime.now(UTC) + timedelta(minutes=20))
    pending = check_now()
    assert pending.ok
    assert "A STOP WAS OWED" not in pending.detail

    due = datetime.now(UTC) - timedelta(minutes=5)
    owed_stop_record(manifest, due_at=due)
    overdue = check_now()

    assert overdue.ok
    # Still the full healthy check -- sync health is genuinely fine -- but the
    # money is no longer silent, and in the same sentence every other surface
    # uses rather than a fourth phrasing of it.
    assert f"RUNNING · ${PIPELINE_USD_PER_DAY:.2f}/day" in overdue.detail
    assert owed_stop_sentence(due.isoformat()) in overdue.detail


def test_round4_readiness_refuses_to_overwrite_a_row_the_demo_did_not_write(monkeypatch) -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    foreign = ("customer-0001", 0.91, "someone-elses-model", "not-a-demo-nonce")
    table = FakeRound4SourceTable(foreign)
    stub_round4_control_plane(monkeypatch, manifest, names, table)

    check = _round4_check(manifest, timeout_seconds=5)

    assert not check.ok
    assert check.detail == "Round 4 source table does not contain the exact baseline"
    assert table.row == foreign
    assert table.merges == []


def test_round4_readiness_waits_for_managed_sync_to_apply_the_restore(monkeypatch) -> None:
    manifest = make_manifest()
    names = attach_round4(manifest)
    table = FakeRound4SourceTable(
        ("customer-0001", 0.33, "risk-v2", "round4-v2-76cc7b187f3342b7a96cea54f3597817")
    )
    stub_round4_control_plane(monkeypatch, manifest, names, table)
    # Managed Sync stays one commit behind the restore for the whole window.
    monkeypatch.setattr(
        "server.lifecycle._round4_get_database_synced_table",
        lambda *args, **kwargs: database_round4_payload(manifest, names, version=7),
    )
    monkeypatch.setattr("server.lifecycle.time.sleep", lambda seconds: None)

    check = _round4_check(manifest, timeout_seconds=0)

    assert not check.ok
    assert check.detail == (
        "Round 4 baseline restore did not reach the synced table before timeout"
    )


# --------------------------------------------------------------------------
# The Terraform backend: local by default, S3 only for a generation that
# recorded the choice. `bootstrap.sh` refuses `--state-backend s3` until this
# code exists, so these cover a path no Terraform command is run against.
# --------------------------------------------------------------------------


def _init_harness(monkeypatch, tmp_path):
    """Capture the `terraform init` argv without running anything."""

    override = tmp_path / "infra" / "backend_override.tf"
    override.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(lifecycle, "BACKEND_OVERRIDE", override)
    monkeypatch.setattr(lifecycle, "_terraform_environment", lambda candidate: {})
    invocations: list[list[str]] = []
    monkeypatch.setattr(
        lifecycle,
        "_run",
        lambda command, **_kwargs: invocations.append(list(command)),
    )
    manifest = make_manifest()
    manifest.aws.terraform_state = str(tmp_path / "generation" / "terraform.tfstate")
    return manifest, override, invocations


def _write_backend_record(payload: dict) -> None:
    record = manifest_path().parent / lifecycle.BACKEND_RECORD_NAME
    record.parent.mkdir(parents=True, exist_ok=True)
    record.write_text(json.dumps(payload), encoding="utf-8")


def test_terraform_init_without_a_backend_record_stays_local(monkeypatch, tmp_path) -> None:
    """The property that matters most: no existing installation changes."""
    manifest, override, invocations = _init_harness(monkeypatch, tmp_path)

    lifecycle._terraform_init(manifest)

    state = Path(manifest.aws.terraform_state)
    assert invocations == [
        [
            "terraform",
            f"-chdir={lifecycle.AWS_INFRA_DIR}",
            "init",
            "-input=false",
            "-reconfigure",
            f"-backend-config=path={state}",
        ]
    ]
    assert state.parent.is_dir()
    assert not override.exists()


def test_terraform_init_removes_another_generations_override(monkeypatch, tmp_path) -> None:
    """`infra/aws/` is shared between generations.

    A leftover override would silently point a local-backend generation at the
    S3 state of whichever generation was initialised last -- at which point plan
    proposes a second copy of every billed resource and the first copy is
    invisible. Deleting rather than ignoring it is the whole point.
    """
    manifest, override, invocations = _init_harness(monkeypatch, tmp_path)
    override.write_text('terraform {\n  backend "s3" {}\n}\n', encoding="utf-8")

    lifecycle._terraform_init(manifest)

    assert not override.exists()
    assert "-backend-config=path=" in invocations[0][-1]


def test_terraform_init_generates_the_s3_override_from_the_record(monkeypatch, tmp_path) -> None:
    manifest, override, invocations = _init_harness(monkeypatch, tmp_path)
    _write_backend_record(
        {
            "backend": "s3",
            "bucket": "my-tfstate-bucket",
            "key": "anti-demo/.anti-demo-v7/terraform.tfstate",
            "region": "us-west-2",
            "use_lockfile": True,
            "encrypt": True,
        }
    )

    lifecycle._terraform_init(manifest)

    assert override.read_text(encoding="utf-8") == (
        "terraform {\n"
        '  backend "s3" {\n'
        '    bucket       = "my-tfstate-bucket"\n'
        '    key          = "anti-demo/.anti-demo-v7/terraform.tfstate"\n'
        '    region       = "us-west-2"\n'
        "    use_lockfile = true\n"
        "    encrypt      = true\n"
        "  }\n"
        "}\n"
    )
    arguments = invocations[0]
    assert arguments[-3:] == [
        "-backend-config=bucket=my-tfstate-bucket",
        "-backend-config=key=anti-demo/.anti-demo-v7/terraform.tfstate",
        "-backend-config=region=us-west-2",
    ]
    # `path` is a local-backend argument; the S3 backend rejects an unknown one
    # outright, which is the failure this whole branch exists to avoid.
    assert not any(argument.startswith("-backend-config=path=") for argument in arguments)
    # `-reconfigure` is what makes switching generations safe: it discards the
    # previous backend's cached configuration instead of offering to migrate.
    assert "-reconfigure" in arguments


def test_an_unsupported_or_incomplete_backend_record_is_refused(monkeypatch, tmp_path) -> None:
    """Falling back to local here is the expensive mistake, so it refuses."""
    manifest, override, invocations = _init_harness(monkeypatch, tmp_path)

    _write_backend_record({"backend": "gcs", "bucket": "b", "key": "k", "region": "r"})
    with pytest.raises(RuntimeError, match="unsupported backend 'gcs'"):
        lifecycle._terraform_init(manifest)

    _write_backend_record({"backend": "s3", "bucket": "b", "key": "k"})
    with pytest.raises(RuntimeError, match="missing 'region'"):
        lifecycle._terraform_init(manifest)

    assert invocations == []
    assert not override.exists()


def test_bootstrap_can_find_the_string_it_gates_the_s3_backend_on() -> None:
    """`bootstrap.sh` greps this module for `terraform-backend.json`.

    That is a text-level contract between two files. Renaming the constant would
    silently reopen the refusal on a tree that has the feature, so it is pinned.
    """
    source = (Path(lifecycle.__file__)).read_text(encoding="utf-8")
    assert lifecycle.BACKEND_RECORD_NAME == "terraform-backend.json"
    assert "terraform-backend.json" in source


# ---------------------------------------------------------------------------
# A deliberately stopped Round 4 pipeline, and the reset that refused it.
#
# Measured on the live installation on 2026-08-25. The pipeline had been switched
# off on purpose to stop paying the standing rate. The synced table then read
# `unity_catalog_provisioning_state=ACTIVE` with
# `detailed_state=SYNCED_TABLE_ONLINE_PIPELINE_FAILED` -- whose own description is
# "Online Table is online, however latest pipeline update failed" -- and the
# pipeline's events named update `c22f27` as CANCELED by a user action.
#
# `antidemo reset` read that as a terminal failure and aborted, having already
# flipped the manifest to `seeding` on the way in, so an installation that was
# doing exactly what it had been told refused every round until it was recovered
# by hand.
# ---------------------------------------------------------------------------

_STOPPED_PIPELINE_SYNCED_STATE = "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"


def _round4_wait_harness(
    monkeypatch,
    *,
    synced_state: str,
    pipeline_state: str = "IDLE",
    update_state: str = "CANCELED",
    pipeline_readable: bool = True,
) -> list[str]:
    """Answer both Round 4 waits from one synced-table and pipeline pair.

    Everything the waits do before the failed-state check is stubbed and nothing
    after it is, so a wait that stopped raising would fall through to its own
    timeout error rather than hanging this test.
    """

    postgres = {"status": {"detailed_state": synced_state}}
    database = {
        "unity_catalog_provisioning_state": "ACTIVE",
        "data_synchronization_status": {"detailed_state": synced_state},
    }
    pipeline_reads: list[str] = []

    def fake_pipeline(manifest, pipeline_id):
        del manifest
        pipeline_reads.append(pipeline_id)
        if not pipeline_readable:
            raise RuntimeError("pipeline read refused")
        return {
            "state": pipeline_state,
            "latest_updates": [{"update_id": "c22f27", "state": update_state}],
        }

    for name, result in (
        ("_round4_get_synced_table", postgres),
        ("_round4_get_database_synced_table", database),
        ("_validate_round4_synced_table", ("synced-uid", "pipeline-1")),
        ("_validate_round4_database_synced_table", None),
    ):
        monkeypatch.setattr(
            f"server.lifecycle.{name}",
            lambda *arguments, _result=result, **keywords: _result,
        )
    monkeypatch.setattr("server.lifecycle._round4_get_pipeline", fake_pipeline)
    return pipeline_reads


def _drive_round4_wait(wait: str) -> None:
    """Call one of the two waits with the arguments its caller passes it."""

    manifest = SimpleNamespace(databricks=SimpleNamespace(profile="demo"))
    names = {"resource_name": "managed-sync", "online_schema": "anti_demo_online"}
    if wait == "baseline":
        lifecycle._wait_round4_baseline(
            manifest,
            names,
            7,
            project_uid="project-uid",
            branch_uid="branch-uid",
            pipeline_id="pipeline-1",
            timeout=0.25,
        )
        return
    lifecycle._wait_round4_sync_position(
        manifest,
        names,
        7,
        pipeline_id="pipeline-1",
        timeout=0.25,
    )


@pytest.mark.parametrize("wait", ("baseline", "sync_position"))
def test_a_deliberately_stopped_round4_pipeline_is_refused_as_a_stop_not_a_failure(
    monkeypatch, wait
) -> None:
    """The defect, at both waits: a power-off refused as a pipeline failure.

    Both waits are driven because both held their own copy of the terminal set
    and their own copy of the message, and fixing one of them would have left the
    other free to make the identical claim on the next reset.

    What is asserted is the shape of the answer rather than its wording: no claim
    that anything failed, the cancelled update named as the reason, and the
    command that fixes it present -- because the operator's alternative was a
    manifest stuck in `seeding` and a hand recovery.
    """

    reads = _round4_wait_harness(monkeypatch, synced_state=_STOPPED_PIPELINE_SYNCED_STATE)

    with pytest.raises(RuntimeError) as raised:
        _drive_round4_wait(wait)

    message = str(raised.value)
    assert "entered terminal state" not in message, message
    assert "switched off rather than broken" in message, message
    assert "CANCELED" in message, message
    assert lifecycle.ROUND4_PIPELINE_START_COMMAND in message, message
    # The signal is read from the pipeline itself, once, and only after a failed
    # state has actually been seen. A durable stop record would have been
    # cheaper and would have missed every stop made outside this tooling.
    assert reads == ["pipeline-1"], reads


@pytest.mark.parametrize("wait", ("baseline", "sync_position"))
@pytest.mark.parametrize(
    ("synced_state", "pipeline_state", "update_state"),
    (
        # An update that fell over on its own. This is the row that fails first
        # if FAILED is ever admitted to the cancelled set.
        (_STOPPED_PIPELINE_SYNCED_STATE, "IDLE", "FAILED"),
        # The pipeline itself is broken or gone, whatever its newest update says.
        (_STOPPED_PIPELINE_SYNCED_STATE, "FAILED", "CANCELED"),
        (_STOPPED_PIPELINE_SYNCED_STATE, "DELETED", "CANCELED"),
        # No newest update at all cannot exonerate a failed state.
        (_STOPPED_PIPELINE_SYNCED_STATE, "IDLE", ""),
        # The table itself went offline and failed. No stop produces that, so a
        # cancelled update must not buy it an exemption.
        ("SYNCED_TABLE_OFFLINE_FAILED", "IDLE", "CANCELED"),
    ),
)
def test_a_genuinely_broken_round4_pipeline_is_still_terminal(
    monkeypatch, wait, synced_state, pipeline_state, update_state
) -> None:
    """The direction that is easy to break while fixing the other one.

    A false alarm traded for a missed real failure is not a fix, so each row here
    is a way of being genuinely broken that the exemption must not swallow. Every
    one of them fails if the exemption is widened by a single state.
    """

    _round4_wait_harness(
        monkeypatch,
        synced_state=synced_state,
        pipeline_state=pipeline_state,
        update_state=update_state,
    )

    with pytest.raises(RuntimeError) as raised:
        _drive_round4_wait(wait)

    message = str(raised.value)
    assert "entered terminal state" in message, message
    assert synced_state in message, message
    assert lifecycle.ROUND4_PIPELINE_START_COMMAND not in message, message


@pytest.mark.parametrize("wait", ("baseline", "sync_position"))
def test_an_unreadable_pipeline_cannot_exonerate_a_failed_synced_table(monkeypatch, wait) -> None:
    """No answer from the pipeline is not the same as a cancelled update.

    The exemption needs a positive reading of the newest update. A control plane
    that will not answer would otherwise be the widest exemption in the set --
    every failure, excused by an outage.
    """

    reads = _round4_wait_harness(
        monkeypatch,
        synced_state=_STOPPED_PIPELINE_SYNCED_STATE,
        pipeline_readable=False,
    )

    with pytest.raises(RuntimeError) as raised:
        _drive_round4_wait(wait)

    assert "entered terminal state" in str(raised.value)
    assert reads == ["pipeline-1"], reads


def test_doctor_names_a_switched_off_round4_pipeline_rather_than_a_broken_one(
    monkeypatch, tmp_path
) -> None:
    """The third site: `doctor`'s own health gate, reached with no stop recorded.

    `_round4_check` already answers a *recorded* stop before it asks the account
    anything, so this is the case that slips past it: a stop whose record lives
    somewhere this process cannot read. On a laptop that is any stop the deployed
    app made, because `pipeline_power`'s file marker resolves through
    `manifest_path()`, which raises inside a Databricks App.

    It stays red, deliberately. A pipeline that is down with nothing recorded is
    `PipelinePower.summary`'s "a failure rather than a choice", and this check has
    no basis to call it healthy. What it must not do is blame a pipeline failure
    that never happened, so the verdict is unchanged and the sentence is not.
    """

    manifest = make_manifest()
    names = attach_round4(manifest)
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / "manifest.json"))
    table = FakeRound4SourceTable(("customer-0001", 0.25, "risk-v0", "round4-baseline"))
    stub_round4_control_plane(monkeypatch, manifest, names, table)

    postgres = round4_payload(manifest, names)
    postgres["status"]["detailed_state"] = _STOPPED_PIPELINE_SYNCED_STATE
    database = database_round4_payload(manifest, names)
    database["data_synchronization_status"]["detailed_state"] = _STOPPED_PIPELINE_SYNCED_STATE
    monkeypatch.setattr("server.lifecycle._round4_get_synced_table", lambda *a, **k: postgres)
    monkeypatch.setattr(
        "server.lifecycle._round4_get_database_synced_table", lambda *a, **k: database
    )
    monkeypatch.setattr(
        "server.lifecycle._round4_get_pipeline",
        lambda *a, **k: {
            "state": "IDLE",
            "latest_updates": [{"update_id": "c22f27", "state": "CANCELED"}],
        },
    )

    check = _round4_check(manifest, timeout_seconds=5)

    assert not check.ok
    assert "is not healthy" not in check.detail, check.detail
    assert "switched off, not broken" in check.detail, check.detail
    assert "cancelled rather than failed" in check.detail, check.detail
    # Both commands, because either one settles it: put the pipeline back up, or
    # record the stop so the next check reads it as the choice it was.
    assert lifecycle.ROUND4_PIPELINE_START_COMMAND in check.detail, check.detail
    assert lifecycle.ROUND4_PIPELINE_STOP_COMMAND in check.detail, check.detail


def test_doctor_still_calls_a_genuinely_unhealthy_round4_synced_table_unhealthy(
    monkeypatch, tmp_path
) -> None:
    """The same gate, with a pipeline that really is broken. No exemption."""

    manifest = make_manifest()
    names = attach_round4(manifest)
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(tmp_path / "manifest.json"))
    table = FakeRound4SourceTable(("customer-0001", 0.25, "risk-v0", "round4-baseline"))
    stub_round4_control_plane(monkeypatch, manifest, names, table)

    postgres = round4_payload(manifest, names)
    postgres["status"]["detailed_state"] = "SYNCED_TABLE_OFFLINE_FAILED"
    monkeypatch.setattr("server.lifecycle._round4_get_synced_table", lambda *a, **k: postgres)
    monkeypatch.setattr(
        "server.lifecycle._round4_get_pipeline",
        lambda *a, **k: {
            "state": "IDLE",
            "latest_updates": [{"update_id": "c22f27", "state": "CANCELED"}],
        },
    )

    check = _round4_check(manifest, timeout_seconds=5)

    assert not check.ok
    assert check.detail == "Round 4 synced table is not healthy: SYNCED_TABLE_OFFLINE_FAILED"
