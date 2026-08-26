from __future__ import annotations

import asyncio
import json
import pathlib
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qsl, unquote

import botocore.session
import psycopg
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from databricks.sdk.errors import NotFound, PermissionDenied

from server import safe_change_live
from server.manager import operator_diagnosis
from server.manifest import (
    AwsManifest,
    AwsResources,
    DatabricksManifest,
    DemoManifest,
    Round4Resources,
)
from server.models import CompetitorId, RoundId
from server.safe_change import (
    SafeChangeOwnershipScope,
    SafeChangePlan,
    SafeChangeProvider,
    UnsafeCleanupError,
    deterministic_artifact_id,
)
from server.safe_change_live import (
    CURRENT_USER_PATH,
    DEFAULT_CANCEL_TEARDOWN_SECONDS,
    DEFAULT_POLL_TIMEOUT_SECONDS,
    DEFAULT_RUN_TIMEOUT_SECONDS,
    LAKEBASE_API_ROOT,
    AuroraSafeChangeAdapter,
    AuroraSafeChangeConfig,
    AwsSafeChangeConfig,
    ControlPlaneCommandError,
    DatabricksRestRunner,
    LakebaseSafeChangeAdapter,
    LakebaseSafeChangeConfig,
    RdsSafeChangeAdapter,
    RdsSafeChangeConfig,
    SafeChangeControlPlaneError,
    SafeChangeLiveConfigurationError,
    _lakebase_create_path,
    build_safe_change_engine,
    lakebase_resource_path,
)

ACCOUNT = "123456789012"
REGION = "us-west-2"
RUN_ID = "ad-test-001"
OWNER = "operator@databricks.com"
SUBNET_GROUP = "anti-demo-db-subnets"
AURORA_SG = "sg-00000000000000001"
RDS_SG = "sg-00000000000000002"
AURORA_SOURCE = "anti-demo-aurora"
AURORA_WRITER = "anti-demo-aurora-writer"
RDS_SOURCE = "anti-demo-rds"
AURORA_SECRET = f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:aurora-managed-AbCdEf"
RDS_SECRET = f"arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:rds-managed-AbCdEf"
LAKEBASE_SOURCE = "projects/ad-test-001/branches/production/endpoints/primary"


async def test_the_lakebase_lanes_never_shell_out_to_a_databricks_binary() -> None:
    """The defect that refused all six rounds in the deployed app.

    The readiness gate runs Round 2's backstage cleanup before it will arm
    anything, and that cleanup's Lakebase lane reached the control plane by
    spawning `databricks postgres get-branch`. In the Apps container that binary
    is whatever the base image ships -- the app declares only `databricks-sdk`
    and nothing installs a CLI -- and an image CLI predating the `postgres`
    command group rejects the command. `/readyz` sat at 503 with every round,
    including the two that touch no AWS, unavailable behind one gate.

    So this asserts on the *source*: no module on the Lakebase control-plane
    path may name the executable or spawn a process. A behavioural test cannot
    catch this, because a `databricks` binary exists on every machine that runs
    the suite.
    """

    server = pathlib.Path(safe_change_live.__file__).parent
    # `targets.py` is the third module on this path and the one the brief did not
    # name: `TargetResolver` builds a Lakebase credential provider for every
    # bout, so a CLI here refuses the round at arm time rather than at startup.
    for module in ("safe_change_live.py", "recovery_live.py", "targets.py"):
        source = (server / module).read_text(encoding="utf-8")
        for forbidden in (
            'create_subprocess_exec',
            'subprocess.run',
            '"databricks"',
            "'databricks'",
        ):
            assert forbidden not in source, f"{module} still reaches a CLI via {forbidden}"


async def test_a_refused_control_plane_request_keeps_the_refusal_in_its_chain() -> None:
    """The swallowed stderr, which is what turned minutes into hours.

    The runner this replaces discarded command output deliberately, for secret
    safety, and so reported "Databricks control-plane command failed" and
    nothing else. That string is in no named class, so the gate classified it
    permanent, latched after one attempt, and the banner named the manifest seal
    for a container problem.

    The message here is still generic -- nothing is dumped -- and the workspace's
    own answer travels as the cause, where `manager.operator_diagnosis` is the
    one place that decides how much of it may reach a screen.
    """

    refusal = PermissionDenied("assign the user 'Can Use' for Database project abc")

    class RefusingApi:
        def __call__(self, profile, method, path, *, body=None, timeout=600):
            del profile, method, path, body, timeout
            raise refusal

    runner = DatabricksRestRunner(workspace_client=SimpleNamespace())
    runner._api = RefusingApi()

    with pytest.raises(ControlPlaneCommandError) as raised:
        await runner.json("GET", "/api/2.0/postgres/projects/p", timeout_seconds=5)

    assert raised.value.not_found is False
    # Generic, as before. The evidence is one link down.
    assert "Can Use" not in str(raised.value)
    assert raised.value.__cause__ is refusal
    assert "Can Use" in operator_diagnosis(raised.value)

    # A 404 is the one refusal the lanes act on rather than report: an absent
    # branch is the goal state for cleanup and for an abandoned lane.
    class MissingApi:
        def __call__(self, profile, method, path, *, body=None, timeout=600):
            del profile, method, path, body, timeout
            raise NotFound("branch id not found")

    missing = DatabricksRestRunner(workspace_client=SimpleNamespace())
    missing._api = MissingApi()
    with pytest.raises(ControlPlaneCommandError) as absent:
        await missing.json("GET", "/api/2.0/postgres/projects/p/branches/b", timeout_seconds=5)
    assert absent.value.not_found is True


def scope() -> SafeChangeOwnershipScope:
    return SafeChangeOwnershipScope(
        run_id=RUN_ID,
        owner=OWNER,
        aws_account_id=ACCOUNT,
        aws_region=REGION,
    )


def plan(provider: SafeChangeProvider, source_id: str) -> SafeChangePlan:
    return SafeChangePlan(
        lane_id=provider.value,
        name=provider.value,
        provider=provider,
        source_id=source_id,
        artifact_id=deterministic_artifact_id(RUN_ID, provider),
        scope=scope(),
    )


class FakeRawConnection:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class RecordingConnector:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.connections: list[FakeRawConnection] = []

    async def __call__(self, **arguments: Any) -> FakeRawConnection:
        self.calls.append(arguments)
        connection = FakeRawConnection()
        self.connections.append(connection)
        return connection


class FlakyConnector(RecordingConnector):
    """A connector that refuses `failures` times before it succeeds.

    `refusal` builds the exception so a test can choose the SQLSTATE, which is
    the only thing the connect path uses to tell a transport fault worth
    retrying from a credential fault that will never improve. The default is a
    bare `OperationalError`, whose SQLSTATE is `None` -- the shape psycopg
    produces when a name does not resolve yet.
    """

    def __init__(
        self,
        failures: int,
        refusal: Callable[[], psycopg.OperationalError] | None = None,
    ) -> None:
        super().__init__()
        self.failures = failures
        self._refusal = refusal or (
            lambda: psycopg.OperationalError("endpoint DNS is not ready")
        )

    async def __call__(self, **arguments: Any) -> FakeRawConnection:
        self.calls.append(arguments)
        if self.failures:
            self.failures -= 1
            raise self._refusal()
        connection = FakeRawConnection()
        self.connections.append(connection)
        return connection


class FakeDatabricksRunner:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.commands: list[list[str]] = []
        self.timeouts: list[float] = []
        self.project = {"name": "projects/ad-test-001"}
        self.branches: dict[str, dict[str, Any]] = {
            "projects/ad-test-001/branches/production": {
                "name": "projects/ad-test-001/branches/production",
                "status": {"current_state": "READY"},
            }
        }
        self.endpoints: dict[str, dict[str, Any]] = {
            LAKEBASE_SOURCE: {
                "name": LAKEBASE_SOURCE,
                "spec": {"endpoint_type": "ENDPOINT_TYPE_READ_WRITE"},
                "status": {
                    "current_state": "ACTIVE",
                    "disabled": False,
                    "hosts": {"host": "source.database.us-west-2.cloud.databricks.com"},
                },
            }
        }

    @staticmethod
    def _resource(path: str) -> tuple[str, dict[str, str]]:
        """Split a REST path back into its resource name and query values."""

        route, _, query = path.partition("?")
        assert route.startswith(f"{LAKEBASE_API_ROOT}/"), path
        return (
            unquote(route[len(LAKEBASE_API_ROOT) + 1 :]),
            {key: value for key, value in parse_qsl(query)},
        )

    async def json(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        assert timeout_seconds > 0
        self.commands.append([method, path])
        if method == "GET" and path == CURRENT_USER_PATH:
            return {"userName": OWNER}
        if method == "POST" and path == f"{LAKEBASE_API_ROOT}/credentials":
            assert body is not None
            return {"token": f"oauth-for-{body['endpoint']}"}
        name, query = self._resource(path)
        if method == "GET":
            if name == self.project["name"]:
                return self.project
            if name in self.branches:
                return self.branches[name]
            if name in self.endpoints:
                return self.endpoints[name]
            raise ControlPlaneCommandError("missing", not_found=True)
        if method == "POST" and name.endswith("/branches"):
            self.events.append("create-branch")
            assert body is not None
            parent = name[: -len("/branches")]
            branch = f"{parent}/branches/{query['branch_id']}"
            self.branches[branch] = {
                "name": branch,
                "spec": body["spec"],
                "status": {
                    "current_state": "READY",
                    "source_branch": body["spec"]["source_branch"],
                },
            }
            endpoint_name = f"{branch}/endpoints/primary"
            self.endpoints[endpoint_name] = {
                "name": endpoint_name,
                "status": {
                    "current_state": "ACTIVE",
                    # Matches the live Autoscaling get-endpoint response: spec is
                    # omitted and endpoint_type is reported in status.
                    "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
                    "hosts": {"host": "isolated.database.us-west-2.cloud.databricks.com"},
                },
            }
            return self.branches[branch]
        if method == "POST" and name.endswith("/endpoints"):
            self.events.append("create-endpoint")
            assert body is not None
            parent = name[: -len("/endpoints")]
            endpoint = f"{parent}/endpoints/{query['endpoint_id']}"
            self.endpoints[endpoint] = {
                "name": endpoint,
                "spec": body["spec"],
                "status": {
                    "current_state": "ACTIVE",
                    "hosts": {"host": "isolated.database.us-west-2.cloud.databricks.com"},
                },
            }
            return self.endpoints[endpoint]
        raise AssertionError((method, path))

    async def run(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        timeout_seconds: float,
    ) -> None:
        assert timeout_seconds > 0
        assert method == "DELETE", (method, path)
        self.commands.append([method, path])
        self.timeouts.append(timeout_seconds)
        branch_name, _ = self._resource(path)
        self.events.append("delete-branch")
        if branch_name not in self.branches:
            raise ControlPlaneCommandError("missing", not_found=True)
        self.branches.pop(branch_name)
        for endpoint in list(self.endpoints):
            if endpoint.startswith(f"{branch_name}/endpoints/"):
                self.endpoints.pop(endpoint)


async def no_sleep(_: float) -> None:
    return None


async def test_lakebase_uses_native_branch_oauth_and_fresh_tls_connections() -> None:
    runner = FakeDatabricksRunner()
    connector = RecordingConnector()
    adapter = LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile="fe-vm-test",
            source_endpoint=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
            expected_region=REGION,
            poll_interval_seconds=0.01,
        ),
        runner=runner,
        connector=connector,
        sleep=no_sleep,
    )
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    reports: list[tuple[str, str | None]] = []

    async def report(message: str, wire_call: str | None = None) -> None:
        runner.events.append(f"report:{message}")
        reports.append((message, wire_call))

    evidence = await adapter.preflight(lakebase_plan)
    artifact = await adapter.create_isolated(lakebase_plan, report)
    source_connection = await adapter.connect_source(lakebase_plan)
    isolated_connection = await adapter.connect_isolated(lakebase_plan, artifact)

    assert evidence["capability"] == "native_branch"
    assert artifact.source_id == LAKEBASE_SOURCE
    assert artifact.owner == OWNER
    assert artifact.metadata["ownership_marker_valid"] is True
    assert runner.events.index("create-branch") < runner.events.index(
        "report:Lakebase branch created from production"
    )
    assert "create-endpoint" not in runner.events
    assert connector.calls[0]["host"] == ("source.database.us-west-2.cloud.databricks.com")
    assert connector.calls[1]["host"] == ("isolated.database.us-west-2.cloud.databricks.com")
    assert connector.calls[0]["sslmode"] == "require"
    assert connector.calls[0]["password"].startswith("oauth-for-")
    assert connector.calls[0] is not connector.calls[1]
    assert reports == [
        ("Lakebase branch created from production", None),
        (
            "Waiting for the Lakebase branch and endpoint to become ready",
            f"GET {LAKEBASE_API_ROOT}/<branch> + <endpoint>",
        ),
    ]
    # The branch is requested by POST against the project's branch collection --
    # no `databricks` process, which is what the Apps container could not run.
    assert [
        "POST",
        _lakebase_create_path(
            "projects/ad-test-001", "branches", "branch_id", lakebase_plan.artifact_id
        ),
    ] in runner.commands

    await source_connection.close()
    await isolated_connection.close()
    await adapter.delete_isolated(lakebase_plan, artifact, report)
    assert await adapter.inspect_artifact(lakebase_plan) is None


async def test_lakebase_source_endpoint_type_is_required_and_may_come_from_status() -> None:
    runner = FakeDatabricksRunner()
    source = runner.endpoints[LAKEBASE_SOURCE]
    source.pop("spec")
    source["status"]["endpoint_type"] = "ENDPOINT_TYPE_READ_WRITE"
    adapter = LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile="fe-vm-test",
            source_endpoint=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
            expected_region=REGION,
        ),
        runner=runner,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)

    assert (await adapter.preflight(lakebase_plan))["capability"] == "native_branch"
    source["status"].pop("endpoint_type")
    with pytest.raises(SafeChangeLiveConfigurationError, match="not read-write"):
        await adapter.preflight(lakebase_plan)


async def test_lakebase_isolated_endpoint_type_missing_or_read_only_fails_closed() -> None:
    runner = FakeDatabricksRunner()
    adapter = LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile="fe-vm-test",
            source_endpoint=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
            expected_region=REGION,
        ),
        runner=runner,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    artifact = await adapter.create_isolated(lakebase_plan, quiet_report)
    endpoint = runner.endpoints[str(artifact.metadata["ownership_endpoint"])]

    endpoint["status"].pop("endpoint_type")
    missing = await adapter.inspect_artifact(lakebase_plan)
    assert missing is not None
    assert missing.metadata["ownership_marker_valid"] is False
    with pytest.raises(SafeChangeLiveConfigurationError, match="no verified read-write"):
        await adapter.connect_isolated(lakebase_plan, missing)

    endpoint["status"]["endpoint_type"] = "ENDPOINT_TYPE_READ_ONLY"
    read_only = await adapter.inspect_artifact(lakebase_plan)
    assert read_only is not None
    assert read_only.metadata["ownership_marker_valid"] is False


async def test_aws_inspection_reconciles_pending_mutation_before_reporting_absent() -> None:
    session = FakeAwsSession()
    adapter = rds_adapter(session)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    reconciled = False

    async def late_mutation() -> None:
        nonlocal reconciled
        await asyncio.sleep(0.01)
        reconciled = True

    task = asyncio.create_task(late_mutation())
    adapter._pending_mutations.add(task)

    assert await adapter.inspect_artifact(rds_plan) is None
    assert reconciled is True
    assert not adapter._pending_mutations


async def test_aws_inspection_refuses_success_while_late_mutation_is_unresolved() -> None:
    session = FakeAwsSession()
    base = rds_adapter(session).rds_config
    adapter = RdsSafeChangeAdapter(
        replace(base, control_timeout_seconds=0.001),
        session=session,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )
    task = asyncio.create_task(asyncio.sleep(60))
    adapter._pending_mutations.add(task)

    with pytest.raises(SafeChangeControlPlaneError, match="cannot report success"):
        await adapter.inspect_artifact(plan(SafeChangeProvider.RDS, RDS_SOURCE))

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_lakebase_cleanup_recovers_a_branch_created_before_its_endpoint() -> None:
    runner = FakeDatabricksRunner()
    adapter = LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile="fe-vm-test",
            source_endpoint=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
            expected_region=REGION,
        ),
        runner=runner,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)

    async def report(_: str, wire_call: str | None = None) -> None:
        del wire_call
        return None

    artifact = await adapter.create_isolated(lakebase_plan, report)
    runner.endpoints.pop(str(artifact.metadata["ownership_endpoint"]))
    incomplete = await adapter.inspect_artifact(lakebase_plan)
    assert incomplete is not None
    assert incomplete.owner == OWNER
    assert incomplete.metadata["ownership_marker_valid"] is False

    await adapter.delete_isolated(lakebase_plan, incomplete, report)
    assert await adapter.inspect_artifact(lakebase_plan) is None


async def test_lakebase_cleanup_refuses_a_branch_with_different_source_lineage() -> None:
    runner = FakeDatabricksRunner()
    adapter = LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile="fe-vm-test",
            source_endpoint=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
            expected_region=REGION,
        ),
        runner=runner,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    artifact = await adapter.create_isolated(lakebase_plan, quiet_report)
    branch_name = str(artifact.metadata["branch_name"])
    runner.branches[branch_name]["status"]["source_branch"] = (
        "projects/ad-test-001/branches/not-production"
    )
    unowned = await adapter.inspect_artifact(lakebase_plan)
    assert unowned is not None

    with pytest.raises(UnsafeCleanupError, match="ownership mismatch"):
        await adapter.delete_isolated(lakebase_plan, unowned, quiet_report)


def missing_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "missing"}}, operation)


class FakeStsClient:
    def __init__(self, account: str) -> None:
        self.account = account

    def get_caller_identity(self) -> dict[str, str]:
        return {"Account": self.account}


class FakeSecretsClient:
    def get_secret_value(self, SecretId: str) -> dict[str, str]:
        if SecretId == AURORA_SECRET:
            payload = {
                "engine": "postgres",
                "host": "source.cluster.us-west-2.rds.amazonaws.com",
                "port": 5432,
                "username": "antidemo_admin",
                "password": "source-aurora-password",
                "dbClusterIdentifier": AURORA_SOURCE,
            }
        elif SecretId == RDS_SECRET:
            payload = {
                "engine": "postgres",
                "host": "source.rds.us-west-2.rds.amazonaws.com",
                "port": 5432,
                "username": "antidemo_admin",
                "password": "source-rds-password",
                "dbInstanceIdentifier": RDS_SOURCE,
            }
        else:
            raise AssertionError(SecretId)
        return {"ARN": SecretId, "SecretString": json.dumps(payload)}


class FakeRdsClient:
    def __init__(self) -> None:
        now = datetime.now(UTC)
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.clusters: dict[str, dict[str, Any]] = {
            AURORA_SOURCE: {
                "DBClusterIdentifier": AURORA_SOURCE,
                "DBClusterArn": f"arn:aws:rds:{REGION}:{ACCOUNT}:cluster:{AURORA_SOURCE}",
                "Engine": "aurora-postgresql",
                "EngineVersion": "17.10",
                "Status": "available",
                "Endpoint": "source.cluster.us-west-2.rds.amazonaws.com",
                "Port": 5432,
                "DBSubnetGroup": SUBNET_GROUP,
                "VpcSecurityGroups": [{"VpcSecurityGroupId": AURORA_SG}],
                "BackupRetentionPeriod": 1,
                "LatestRestorableTime": now,
                "MasterUserSecret": {"SecretArn": AURORA_SECRET},
                "DBClusterMembers": [
                    {
                        "DBInstanceIdentifier": AURORA_WRITER,
                        "IsClusterWriter": True,
                    }
                ],
                "ServerlessV2ScalingConfiguration": {
                    "MinCapacity": 0,
                    "MaxCapacity": 2,
                    "SecondsUntilAutoPause": 300,
                },
                "NetworkType": "IPV4",
            }
        }
        self.instances: dict[str, dict[str, Any]] = {
            AURORA_WRITER: {
                "DBInstanceIdentifier": AURORA_WRITER,
                "DBInstanceArn": f"arn:aws:rds:{REGION}:{ACCOUNT}:db:{AURORA_WRITER}",
                "DBClusterIdentifier": AURORA_SOURCE,
                "DBInstanceClass": "db.serverless",
                "DBInstanceStatus": "available",
                "Engine": "aurora-postgresql",
                "PubliclyAccessible": True,
            },
            RDS_SOURCE: {
                "DBInstanceIdentifier": RDS_SOURCE,
                "DBInstanceArn": f"arn:aws:rds:{REGION}:{ACCOUNT}:db:{RDS_SOURCE}",
                "DBInstanceClass": "db.t4g.medium",
                "DBInstanceStatus": "available",
                "Engine": "postgres",
                "EngineVersion": "17.10",
                "Endpoint": {
                    "Address": "source.rds.us-west-2.rds.amazonaws.com",
                    "Port": 5432,
                },
                "DBSubnetGroup": {"DBSubnetGroupName": SUBNET_GROUP},
                "VpcSecurityGroups": [{"VpcSecurityGroupId": RDS_SG}],
                "BackupRetentionPeriod": 1,
                "LatestRestorableTime": now,
                "MasterUserSecret": {"SecretArn": RDS_SECRET},
                "PubliclyAccessible": True,
                "NetworkType": "IPV4",
            },
        }
        self.tags: dict[str, list[dict[str, str]]] = {}

    def describe_db_clusters(self, **arguments: Any) -> dict[str, Any]:
        cluster_id = arguments["DBClusterIdentifier"]
        if cluster_id not in self.clusters:
            raise missing_error("DBClusterNotFoundFault", "DescribeDBClusters")
        return {"DBClusters": [self.clusters[cluster_id]]}

    def describe_db_instances(self, **arguments: Any) -> dict[str, Any]:
        instance_id = arguments["DBInstanceIdentifier"]
        if instance_id not in self.instances:
            raise missing_error("DBInstanceNotFound", "DescribeDBInstances")
        return {"DBInstances": [self.instances[instance_id]]}

    def list_tags_for_resource(self, **arguments: Any) -> dict[str, Any]:
        return {"TagList": self.tags.get(arguments["ResourceName"], [])}

    def restore_db_cluster_to_point_in_time(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("restore_db_cluster_to_point_in_time", arguments))
        target = arguments["DBClusterIdentifier"]
        source = self.clusters[AURORA_SOURCE]
        arn = f"arn:aws:rds:{REGION}:{ACCOUNT}:cluster:{target}"
        self.clusters[target] = {
            **source,
            "DBClusterIdentifier": target,
            "DBClusterArn": arn,
            "Status": "available",
            "Endpoint": f"{target}.cluster.us-west-2.rds.amazonaws.com",
            "DBClusterMembers": [],
            "MasterUserSecret": {},
        }
        self.tags[arn] = arguments["Tags"]
        return {"DBCluster": self.clusters[target]}

    def create_db_instance(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("create_db_instance", arguments))
        instance_id = arguments["DBInstanceIdentifier"]
        cluster_id = arguments["DBClusterIdentifier"]
        arn = f"arn:aws:rds:{REGION}:{ACCOUNT}:db:{instance_id}"
        self.instances[instance_id] = {
            "DBInstanceIdentifier": instance_id,
            "DBInstanceArn": arn,
            "DBClusterIdentifier": cluster_id,
            "DBInstanceClass": arguments["DBInstanceClass"],
            "DBInstanceStatus": "available",
            "Engine": arguments["Engine"],
            "PubliclyAccessible": arguments["PubliclyAccessible"],
        }
        self.clusters[cluster_id]["DBClusterMembers"] = [
            {"DBInstanceIdentifier": instance_id, "IsClusterWriter": True}
        ]
        self.tags[arn] = arguments["Tags"]
        return {"DBInstance": self.instances[instance_id]}

    def restore_db_instance_to_point_in_time(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("restore_db_instance_to_point_in_time", arguments))
        target = arguments["TargetDBInstanceIdentifier"]
        source = self.instances[RDS_SOURCE]
        arn = f"arn:aws:rds:{REGION}:{ACCOUNT}:db:{target}"
        self.instances[target] = {
            **source,
            "DBInstanceIdentifier": target,
            "DBInstanceArn": arn,
            "DBInstanceStatus": "available",
            "Endpoint": {
                "Address": f"{target}.rds.us-west-2.rds.amazonaws.com",
                "Port": 5432,
            },
            "MasterUserSecret": {},
        }
        self.tags[arn] = arguments["Tags"]
        return {"DBInstance": self.instances[target]}

    def delete_db_instance(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("delete_db_instance", arguments))
        instance_id = arguments["DBInstanceIdentifier"]
        if instance_id not in self.instances:
            raise missing_error("DBInstanceNotFound", "DeleteDBInstance")
        instance = self.instances.pop(instance_id)
        cluster_id = str(instance.get("DBClusterIdentifier") or "")
        if cluster_id and cluster_id in self.clusters:
            self.clusters[cluster_id]["DBClusterMembers"] = []
        return {"DBInstance": instance}

    def delete_db_cluster(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("delete_db_cluster", arguments))
        cluster_id = arguments["DBClusterIdentifier"]
        if cluster_id not in self.clusters:
            raise missing_error("DBClusterNotFoundFault", "DeleteDBCluster")
        # Real RDS refuses to delete a cluster that still has members, which is
        # the ordinary state immediately after a writer delete is issued.
        if self.clusters[cluster_id].get("DBClusterMembers"):
            raise missing_error("InvalidDBClusterStateFault", "DeleteDBCluster")
        return {"DBCluster": self.clusters.pop(cluster_id)}


class FakeAwsSession:
    region_name = REGION

    def __init__(self, *, account: str = ACCOUNT) -> None:
        self.rds = FakeRdsClient()
        self.sts = FakeStsClient(account)
        self.secrets = FakeSecretsClient()

    def client(self, service: str) -> Any:
        return {
            "rds": self.rds,
            "sts": self.sts,
            "secretsmanager": self.secrets,
        }[service]


def aurora_adapter(
    session: FakeAwsSession,
    connector: RecordingConnector | None = None,
) -> AuroraSafeChangeAdapter:
    return AuroraSafeChangeAdapter(
        AuroraSafeChangeConfig(
            profile="anti-demo-admin",
            region=REGION,
            account_id=ACCOUNT,
            database="anti_demo",
            secret_arn=AURORA_SECRET,
            db_subnet_group_name=SUBNET_GROUP,
            security_group_id=AURORA_SG,
            source_cluster_id=AURORA_SOURCE,
            source_writer_instance_id=AURORA_WRITER,
            poll_interval_seconds=0.01,
        ),
        session=session,
        connector=connector or RecordingConnector(),
        sleep=no_sleep,
    )


def rds_adapter(
    session: FakeAwsSession,
    connector: RecordingConnector | None = None,
) -> RdsSafeChangeAdapter:
    return RdsSafeChangeAdapter(
        RdsSafeChangeConfig(
            profile="anti-demo-admin",
            region=REGION,
            account_id=ACCOUNT,
            database="anti_demo",
            secret_arn=RDS_SECRET,
            db_subnet_group_name=SUBNET_GROUP,
            security_group_id=RDS_SG,
            source_instance_id=RDS_SOURCE,
            poll_interval_seconds=0.01,
        ),
        session=session,
        connector=connector or RecordingConnector(),
        sleep=no_sleep,
    )


async def quiet_report(_: str, wire_call: str | None = None) -> None:
    del wire_call
    return None


async def test_aurora_creates_copy_on_write_clone_and_serverless_writer() -> None:
    session = FakeAwsSession()
    connector = RecordingConnector()
    adapter = aurora_adapter(session, connector)
    aurora_plan = plan(SafeChangeProvider.AURORA, AURORA_SOURCE)

    evidence = await adapter.preflight(aurora_plan)
    artifact = await adapter.create_isolated(aurora_plan, quiet_report)
    connection = await adapter.connect_isolated(aurora_plan, artifact)

    restore = next(
        arguments
        for method, arguments in session.rds.calls
        if method == "restore_db_cluster_to_point_in_time"
    )
    writer = next(
        arguments for method, arguments in session.rds.calls if method == "create_db_instance"
    )
    assert evidence["capability"] == "copy_on_write_pitr_clone"
    assert restore["RestoreType"] == "copy-on-write"
    assert restore["UseLatestRestorableTime"] is True
    assert restore["SourceDBClusterIdentifier"] == AURORA_SOURCE
    assert writer["DBInstanceClass"] == "db.serverless"
    assert connector.calls[-1]["host"] == (
        f"{aurora_plan.artifact_id}.cluster.us-west-2.rds.amazonaws.com"
    )
    assert connector.calls[-1]["password"] == "source-aurora-password"
    assert connector.calls[-1]["sslmode"] == "require"

    await connection.close()
    await adapter.delete_isolated(aurora_plan, artifact, quiet_report)
    assert await adapter.inspect_artifact(aurora_plan) is None


async def test_rds_creates_real_pitr_restore_and_uses_target_control_plane_host() -> None:
    session = FakeAwsSession()
    connector = RecordingConnector()
    adapter = rds_adapter(session, connector)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)

    evidence = await adapter.preflight(rds_plan)
    artifact = await adapter.create_isolated(rds_plan, quiet_report)
    connection = await adapter.connect_isolated(rds_plan, artifact)

    restore = next(
        arguments
        for method, arguments in session.rds.calls
        if method == "restore_db_instance_to_point_in_time"
    )
    assert evidence["capability"] == "native_pitr_restore"
    assert restore["UseLatestRestorableTime"] is True
    assert restore["SourceDBInstanceIdentifier"] == RDS_SOURCE
    assert connector.calls[-1]["host"] == (
        f"{rds_plan.artifact_id}.rds.us-west-2.rds.amazonaws.com"
    )
    assert connector.calls[-1]["host"] != ("source.rds.us-west-2.rds.amazonaws.com")
    assert connector.calls[-1]["password"] == "source-rds-password"

    await connection.close()
    await adapter.delete_isolated(rds_plan, artifact, quiet_report)
    assert await adapter.inspect_artifact(rds_plan) is None


async def test_aws_cleanup_revalidates_current_ownership_tags() -> None:
    session = FakeAwsSession()
    adapter = rds_adapter(session)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    artifact = await adapter.create_isolated(rds_plan, quiet_report)
    target_arn = str(artifact.metadata["instance_arn"])
    for tag in session.rds.tags[target_arn]:
        if tag["Key"] == "owner":
            tag["Value"] = "someone-else@databricks.com"

    with pytest.raises(UnsafeCleanupError, match="ownership mismatch"):
        await adapter.delete_isolated(rds_plan, artifact, quiet_report)
    assert rds_plan.artifact_id in session.rds.instances


async def test_rds_teardown_deletes_a_restore_still_reporting_backing_up() -> None:
    # Teardown waits for the resource to be *absent*, never for it to report a
    # particular status. Widening readiness to accept `backing-up` means a clone
    # can reach cleanup mid-transition, and a teardown that waited for
    # `available` first would sit there until the poll timeout while the
    # instance billed. Pinned so that lesson is not relearned from a bill.
    session = FakeAwsSession()
    adapter = rds_adapter(session)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    artifact = await adapter.create_isolated(rds_plan, quiet_report)
    session.rds.instances[rds_plan.artifact_id]["DBInstanceStatus"] = "backing-up"

    await adapter.delete_isolated(rds_plan, artifact, quiet_report)

    assert rds_plan.artifact_id not in session.rds.instances


async def test_aurora_teardown_deletes_a_writer_still_reporting_backing_up() -> None:
    session = FakeAwsSession()
    adapter = aurora_adapter(session)
    aurora_plan = plan(SafeChangeProvider.AURORA, AURORA_SOURCE)
    artifact = await adapter.create_isolated(aurora_plan, quiet_report)
    writer_id = str(artifact.metadata["writer_id"])
    session.rds.instances[writer_id]["DBInstanceStatus"] = "backing-up"

    await adapter.delete_isolated(aurora_plan, artifact, quiet_report)

    assert writer_id not in session.rds.instances
    assert await adapter.inspect_artifact(aurora_plan) is None


async def test_aws_adapter_rejects_credentials_for_another_account() -> None:
    adapter = aurora_adapter(FakeAwsSession(account="999999999999"))

    with pytest.raises(SafeChangeLiveConfigurationError, match="999999999999"):
        await adapter.preflight(plan(SafeChangeProvider.AURORA, AURORA_SOURCE))


async def test_aws_preflight_requires_pitr_to_include_the_seed_commit() -> None:
    session = FakeAwsSession()
    base = aurora_adapter(session).aurora_config
    adapter = AuroraSafeChangeAdapter(
        replace(base, source_seeded_at=datetime.now(UTC) + timedelta(minutes=5)),
        session=session,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )

    with pytest.raises(SafeChangeLiveConfigurationError, match="seeded proof schema"):
        await adapter.preflight(plan(SafeChangeProvider.AURORA, AURORA_SOURCE))


class StubbedAwsSession:
    """A session of real botocore clients under `Stubber`, for shape claims.

    `FakeRdsClient` hands back whatever dict a test writes, so a response key
    RDS does not actually have still reads as a passing test -- the same
    forgiveness that let `RestoreToTime` reach a live demo. `Stubber` validates
    every response against the real service model, which is what makes the
    `EarliestRestorableTime` claim below worth anything: it is a member of
    `DBCluster` and, notably, not of `DBInstance`.
    """

    region_name = REGION

    def __init__(self, *responses: tuple[str, str, dict[str, Any], dict[str, Any]]) -> None:
        session = botocore_session()
        self._clients: dict[str, Any] = {}
        self._stubbers: dict[str, Stubber] = {}
        for service, method, parameters, response in responses:
            if service not in self._clients:
                self._clients[service] = session.create_client(
                    service,
                    region_name=REGION,
                    aws_access_key_id="testing",
                    aws_secret_access_key="testing",
                    aws_session_token="testing",
                )
                self._stubbers[service] = Stubber(self._clients[service])
                self._stubbers[service].activate()
            # Queued in call order, so an adapter that skips or reorders a
            # control-plane call fails here rather than reading a stale answer.
            self._stubbers[service].add_response(method, response, parameters)

    def client(self, service: str) -> Any:
        return self._clients[service]

    def assert_every_call_was_made(self) -> None:
        for stubber in self._stubbers.values():
            stubber.assert_no_pending_responses()


CALLER_IDENTITY = (
    "sts",
    "get_caller_identity",
    {},
    {
        "Account": ACCOUNT,
        "Arn": f"arn:aws:iam::{ACCOUNT}:user/anti-demo-admin",
        "UserId": "AIDAEXAMPLEEXAMPLE",
    },
)


def aurora_source_cluster(**restorable: datetime) -> dict[str, Any]:
    return {
        "DBClusterIdentifier": AURORA_SOURCE,
        "DBClusterArn": f"arn:aws:rds:{REGION}:{ACCOUNT}:cluster:{AURORA_SOURCE}",
        "Engine": "aurora-postgresql",
        "EngineVersion": "17.10",
        "Status": "available",
        "Endpoint": "source.cluster.us-west-2.rds.amazonaws.com",
        "Port": 5432,
        "DBSubnetGroup": SUBNET_GROUP,
        "VpcSecurityGroups": [{"VpcSecurityGroupId": AURORA_SG}],
        "BackupRetentionPeriod": 1,
        "MasterUserSecret": {"SecretArn": AURORA_SECRET},
        "DBClusterMembers": [
            {"DBInstanceIdentifier": AURORA_WRITER, "IsClusterWriter": True}
        ],
        **restorable,
    }


AURORA_SOURCE_WRITER = {
    "DBInstanceIdentifier": AURORA_WRITER,
    "DBInstanceArn": f"arn:aws:rds:{REGION}:{ACCOUNT}:db:{AURORA_WRITER}",
    "DBClusterIdentifier": AURORA_SOURCE,
    "DBInstanceClass": "db.serverless",
    "DBInstanceStatus": "available",
    "Engine": "aurora-postgresql",
}


def seeded_aurora_adapter(
    cluster: dict[str, Any],
    seeded_at: datetime,
) -> AuroraSafeChangeAdapter:
    session = StubbedAwsSession(
        CALLER_IDENTITY,
        (
            "rds",
            "describe_db_clusters",
            {"DBClusterIdentifier": AURORA_SOURCE},
            {"DBClusters": [cluster]},
        ),
        (
            "rds",
            "describe_db_instances",
            {"DBInstanceIdentifier": AURORA_WRITER},
            {"DBInstances": [AURORA_SOURCE_WRITER]},
        ),
    )
    return AuroraSafeChangeAdapter(
        AuroraSafeChangeConfig(
            profile="anti-demo-admin",
            region=REGION,
            account_id=ACCOUNT,
            database="anti_demo",
            secret_arn=AURORA_SECRET,
            db_subnet_group_name=SUBNET_GROUP,
            security_group_id=AURORA_SG,
            source_cluster_id=AURORA_SOURCE,
            source_writer_instance_id=AURORA_WRITER,
            source_seeded_at=seeded_at,
            poll_interval_seconds=0.01,
        ),
        session=session,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )


async def test_round2_preflight_accepts_a_seed_aged_out_of_the_restore_window() -> None:
    """A seed older than the window is the steady state, not a fault.

    `backup_retention_period = 1`, so any installation seeded over a day ago has
    `EarliestRestorableTime` past its seed. Round 2 restores with
    `UseLatestRestorableTime`, so the schema is still present at the point it
    lands on. This pins the deliberate decision not to add a lower-bound check:
    it would refuse every long-lived demo for a condition neither round cares
    about. Round 3 checks both bounds against its recovery point, where they
    genuinely bind, in `recovery_live.wait_recovery_point`.
    """

    now = datetime.now(UTC)
    seeded_at = now - timedelta(days=3)
    cluster = aurora_source_cluster(
        EarliestRestorableTime=now - timedelta(hours=1),
        LatestRestorableTime=now - timedelta(minutes=5),
    )
    assert cluster["EarliestRestorableTime"] > seeded_at, "seed must be outside the window"
    adapter = seeded_aurora_adapter(cluster, seeded_at)

    preflight = await adapter.preflight(plan(SafeChangeProvider.AURORA, AURORA_SOURCE))

    assert preflight["capability"] == "copy_on_write_pitr_clone"
    adapter._session.assert_every_call_was_made()


async def test_round2_restore_window_refusal_names_the_bound_it_checked() -> None:
    """The guard may only claim the one bound it reads.

    It was `_assert_restore_freshness`, refusing with "PITR window has not
    captured...", which reads as a statement about the whole window while only
    ever comparing the upper one. Both the name and the wording now say
    `LatestRestorableTime`, so no reader takes a lower-bound guarantee from a
    check that does not make one -- and on the RDS lane could not, since
    `DBInstance` carries no `EarliestRestorableTime` to compare against.
    """

    now = datetime.now(UTC)
    cluster = aurora_source_cluster(
        EarliestRestorableTime=now - timedelta(hours=1),
        LatestRestorableTime=now,
    )
    adapter = seeded_aurora_adapter(cluster, now + timedelta(minutes=5))

    assert not hasattr(adapter, "_assert_restore_freshness")

    with pytest.raises(
        SafeChangeLiveConfigurationError,
        match="latest restorable time predates the seeded proof schema",
    ):
        await adapter.preflight(plan(SafeChangeProvider.AURORA, AURORA_SOURCE))


async def test_aws_connection_retries_transient_endpoint_readiness() -> None:
    session = FakeAwsSession()
    connector = FlakyConnector(failures=2)
    adapter = rds_adapter(session, connector)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    artifact = await adapter.create_isolated(rds_plan, quiet_report)

    connection = await adapter.connect_isolated(rds_plan, artifact)

    assert len(connector.calls) == 3
    assert connector.calls[-1]["host"] == (
        f"{rds_plan.artifact_id}.rds.us-west-2.rds.amazonaws.com"
    )
    await connection.close()


async def test_a_refused_connection_reaches_the_lane_named_by_sqlstate_not_by_dsn() -> None:
    """The other half of the same `except`: a refusal that will never improve.

    This is the one Round 2 path that puts a third-party exception message into
    `SafeChangeLaneResult.error`, and that field is quoted verbatim onto the SSE
    lane update, the bout record and the receipt without passing through
    `manager._message_is_ours_to_quote`. psycopg names the endpoint, a routable
    address and the login role in a connect refusal, so what is asserted here is
    that the SQLSTATE survives and the DSN does not.

    The host is assembled from fragments for the reason
    `test_no_live_identifiers_committed.test_the_guard_can_fail` gives: the
    `rds_endpoint` *shape* has to exist at run time for the assertions to mean
    anything, while no searchable value may be published in a tracked file.
    """

    host = "anti-demo-src." + "b4mq7xz10kdp" + ".us-west-2.rds.amazonaws.com"
    role = "anti_demo_app"

    def refused() -> psycopg.OperationalError:
        # `28P01` is invalid_password: not transient, so the connect path must
        # not spend its readiness budget retrying it.
        return psycopg.errors.lookup("28P01")(
            f'connection failed: connection to server at "{host}" '
            f'(203.0.113.24), port 5432 failed: FATAL:  password '
            f'authentication failed for user "{role}"'
        )

    session = FakeAwsSession()
    connector = FlakyConnector(failures=99, refusal=refused)
    adapter = rds_adapter(session, connector)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    artifact = await adapter.create_isolated(rds_plan, quiet_report)

    with pytest.raises(safe_change_live.DataPlaneConnectionRefusedError) as caught:
        await adapter.connect_isolated(rds_plan, artifact)

    # Refused on the first attempt: a bad credential is not a readiness problem.
    assert len(connector.calls) == 1

    # What `_run_lane` turns into `SafeChangeLaneResult.error`, verbatim.
    lane_error = str(caught.value) or type(caught.value).__name__
    assert "28P01" in lane_error, "the actionable half of the refusal was dropped"
    assert host not in lane_error, (
        "the endpoint hostname reaches the screen, the bout record and the receipt"
    )
    assert role not in lane_error, "the login role reaches the same three surfaces"
    assert "203.0.113" not in lane_error

    # The raw refusal really did carry all three, so the assertions above are
    # measuring the wrapper and not a message that was harmless to begin with.
    cause = str(caught.value.__cause__)
    assert host in cause and role in cause and "203.0.113" in cause

    # And the chain stays walkable, so `operator_diagnosis` still reports the
    # psycopg type for a log line without quoting its words.
    assert "InvalidPassword" in operator_diagnosis(caught.value)


def owned_manifest() -> DemoManifest:
    return DemoManifest(
        run_id=RUN_ID,
        owner=OWNER,
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(days=1),
        status="ready",
        aws=AwsManifest(
            profile="anti-demo-admin",
            account_id=ACCOUNT,
            region=REGION,
            operator_cidr="203.0.113.7/32",
            terraform_state="/tmp/anti-demo.tfstate",
            resources=AwsResources(
                aurora_cluster_id=AURORA_SOURCE,
                aurora_writer_instance_id=AURORA_WRITER,
                aurora_secret_arn=AURORA_SECRET,
                rds_instance_id=RDS_SOURCE,
                rds_secret_arn=RDS_SECRET,
                security_group_id=AURORA_SG,
                rds_security_group_id=RDS_SG,
                db_subnet_group_name=SUBNET_GROUP,
            ),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id=RUN_ID,
            endpoint_name=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
        ),
    )


def test_builder_exposes_all_three_live_adapters_without_cloud_calls() -> None:
    sessions: list[FakeAwsSession] = []

    def session_factory(**_: Any) -> FakeAwsSession:
        session = FakeAwsSession()
        sessions.append(session)
        return session

    engine = build_safe_change_engine(
        owned_manifest(),
        environment={},
        databricks_runner=FakeDatabricksRunner(),
        session_factory=session_factory,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )

    assert isinstance(engine.lakebase, LakebaseSafeChangeAdapter)
    assert isinstance(
        engine.competitors[CompetitorId.AURORA_SERVERLESS_V2],
        AuroraSafeChangeAdapter,
    )
    assert isinstance(
        engine.competitors[CompetitorId.RDS_POSTGRES],
        RdsSafeChangeAdapter,
    )
    assert len(sessions) == 2
    assert engine.run_timeout_seconds == DEFAULT_RUN_TIMEOUT_SECONDS
    assert engine.reset_timeout_seconds == DEFAULT_RUN_TIMEOUT_SECONDS
    assert engine.lakebase.config.control_timeout_seconds == 120.0
    assert engine.lakebase.config.poll_timeout_seconds == DEFAULT_POLL_TIMEOUT_SECONDS

    # A passed TTL is reported, not enforced: it says nothing about whether the
    # Round 2/3 sources are healthy. `status` is a real readiness signal and is
    # still refused.
    past_ttl = owned_manifest()
    past_ttl.expires_at = datetime.now(UTC) - timedelta(minutes=1)
    assert build_safe_change_engine(
        past_ttl,
        environment={},
        databricks_runner=FakeDatabricksRunner(),
        session_factory=session_factory,
        connector=RecordingConnector(),
        sleep=no_sleep,
    ).scope.run_id == RUN_ID

    expired = owned_manifest()
    expired.status = "cleanup_failed"
    expired.expires_at = datetime.now(UTC) - timedelta(minutes=1)
    with pytest.raises(RuntimeError, match="CLEANUP_FAILED"):
        build_safe_change_engine(
            expired,
            environment={},
            databricks_runner=FakeDatabricksRunner(),
            session_factory=session_factory,
            connector=RecordingConnector(),
            sleep=no_sleep,
        )
    cleanup_engine = build_safe_change_engine(
        expired,
        cleanup_only=True,
        environment={},
        databricks_runner=FakeDatabricksRunner(),
        session_factory=session_factory,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )
    assert cleanup_engine.scope.run_id == RUN_ID
    assert cleanup_engine.lakebase.source_id == LAKEBASE_SOURCE


def test_builder_uses_the_round_specific_aurora_writer() -> None:
    manifest = owned_manifest()
    manifest.round_environments = {
        RoundId.RECOVER_DELETED_ORDER: SimpleNamespace(
            lakebase=SimpleNamespace(endpoint_name="projects/r3/branches/production/endpoints/primary"),
            aurora=SimpleNamespace(
                cluster_id="round3-aurora",
                writer_instance_id="round3-aurora-writer",
                secret_arn=AURORA_SECRET,
                security_group_id=AURORA_SG,
                db_subnet_group_name=SUBNET_GROUP,
            ),
            rds=SimpleNamespace(
                instance_id="round3-rds",
                secret_arn=RDS_SECRET,
                security_group_id=RDS_SG,
            ),
        )
    }

    engine = build_safe_change_engine(
        manifest,
        cleanup_only=True,
        round_number=3,
        environment={},
        databricks_runner=FakeDatabricksRunner(),
        session_factory=lambda **_: FakeAwsSession(),
        connector=RecordingConnector(),
        sleep=no_sleep,
    )
    aurora = engine.competitors[CompetitorId.AURORA_SERVERLESS_V2]

    assert aurora.source_id == "round3-aurora"
    assert aurora.aurora_config.source_writer_instance_id == "round3-aurora-writer"


def test_builder_uses_profileless_aws_sessions_in_databricks_app_mode() -> None:
    calls: list[dict[str, str]] = []
    app_client_id = "11111111-2222-3333-4444-555555555555"
    manifest = owned_manifest()
    manifest.round4 = Round4Resources(
        warehouse_id="warehouse-1",
        setup_principal=OWNER,
        app_service_principal_client_id=app_client_id,
        source_table_full_name="catalog.source.model_scores_source",
        storage_catalog="catalog",
        storage_schema="storage",
        synced_table_id="catalog.online.model_scores",
        synced_table_resource_name="synced_tables/catalog.online.model_scores",
        synced_table_uid="synced-uid",
        pipeline_id="pipeline-1",
        project_uid="project-uid",
        branch_uid="branch-uid",
        physical_database="anti_demo",
        physical_schema="online",
        physical_table="model_scores",
        branch=f"projects/{RUN_ID}/branches/production",
        endpoint_name=LAKEBASE_SOURCE,
        contract_sha256="0" * 64,
    )
    manifest.manifest_version = 2

    def session_factory(**kwargs: str) -> FakeAwsSession:
        calls.append(kwargs)
        return FakeAwsSession()

    engine = build_safe_change_engine(
        manifest,
        environment={
            "ANTI_DEMO_ENV": "databricks-app",
            "AWS_AUTH_MODE": "environment",
            "AWS_ACCESS_KEY_ID": "test-access",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
        },
        databricks_runner=FakeDatabricksRunner(),
        session_factory=session_factory,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )

    assert calls == [{"region_name": REGION}, {"region_name": REGION}]
    assert engine.competitors[
        CompetitorId.AURORA_SERVERLESS_V2
    ].config.auth_mode == "environment"
    assert engine.lakebase.config.profile == ""
    assert engine.lakebase.config.user == app_client_id


def test_builder_refuses_environment_that_redirects_the_manifest() -> None:
    with pytest.raises(SafeChangeLiveConfigurationError, match="AWS_REGION"):
        build_safe_change_engine(
            owned_manifest(),
            environment={"AWS_REGION": "us-east-1"},
            databricks_runner=FakeDatabricksRunner(),
            session_factory=lambda **_: FakeAwsSession(),
            connector=RecordingConnector(),
            sleep=no_sleep,
        )


# --- Round 2 restore readiness -------------------------------------------------
#
# A PITR-restored RDS instance runs a mandatory automated snapshot immediately
# after its engine starts, and reports `backing-up` for the whole of it. It
# accepts client connections throughout. Waiting for `available` put that
# snapshot on the critical path and timed the round out on 2026-08-20.


async def instant_sleep(_: float) -> None:
    return None


def restore_lands_in(session: FakeAwsSession, status: str) -> None:
    original = session.rds.restore_db_instance_to_point_in_time

    def restore(**arguments: Any) -> dict[str, Any]:
        result = original(**arguments)
        target = arguments["TargetDBInstanceIdentifier"]
        session.rds.instances[target]["DBInstanceStatus"] = status
        return result

    session.rds.restore_db_instance_to_point_in_time = restore


def impatient_rds_adapter(session: FakeAwsSession) -> RdsSafeChangeAdapter:
    return RdsSafeChangeAdapter(
        RdsSafeChangeConfig(
            profile="anti-demo-admin",
            region=REGION,
            account_id=ACCOUNT,
            database="anti_demo",
            secret_arn=RDS_SECRET,
            db_subnet_group_name=SUBNET_GROUP,
            security_group_id=RDS_SG,
            source_instance_id=RDS_SOURCE,
            poll_timeout_seconds=0.05,
            poll_interval_seconds=0.01,
        ),
        session=session,
        connector=RecordingConnector(),
        sleep=instant_sleep,
    )


async def test_rds_pitr_restore_is_ready_while_backing_up() -> None:
    session = FakeAwsSession()
    restore_lands_in(session, "backing-up")
    connector = RecordingConnector()
    adapter = rds_adapter(session, connector)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)

    artifact = await adapter.create_isolated(rds_plan, quiet_report)

    assert artifact.state == "BACKING-UP"
    assert artifact.artifact_id == rds_plan.artifact_id

    # The whole point of accepting the earlier state: the copy is usable now.
    await adapter.connect_isolated(rds_plan, artifact)
    assert connector.calls[-1]["host"] == f"{rds_plan.artifact_id}.rds.us-west-2.rds.amazonaws.com"


async def test_rds_pitr_restore_still_ready_when_already_available() -> None:
    session = FakeAwsSession()
    adapter = rds_adapter(session)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)

    artifact = await adapter.create_isolated(rds_plan, quiet_report)

    assert artifact.state == "AVAILABLE"


@pytest.mark.parametrize("status", ["creating", "modifying", "starting", "stopped"])
async def test_rds_pitr_restore_keeps_waiting_for_states_that_do_not_serve(status: str) -> None:
    session = FakeAwsSession()
    restore_lands_in(session, status)
    adapter = impatient_rds_adapter(session)

    with pytest.raises(SafeChangeControlPlaneError, match="RDS PITR restore availability"):
        await adapter.create_isolated(plan(SafeChangeProvider.RDS, RDS_SOURCE), quiet_report)


@pytest.mark.parametrize(
    "status",
    ["failed", "incompatible-restore", "inaccessible-encryption-credentials"],
)
async def test_rds_pitr_restore_fails_fast_on_terminal_states(status: str) -> None:
    session = FakeAwsSession()
    restore_lands_in(session, status)
    adapter = impatient_rds_adapter(session)

    with pytest.raises(SafeChangeControlPlaneError, match="entered"):
        await adapter.create_isolated(plan(SafeChangeProvider.RDS, RDS_SOURCE), quiet_report)


async def test_aurora_writer_readiness_is_unchanged_and_still_requires_available() -> None:
    # Aurora restores via a copy-on-write clone and never takes the RDS restore
    # path. Widening the RDS predicate must not widen Aurora's.
    session = FakeAwsSession()
    original = session.rds.create_db_instance

    def create(**arguments: Any) -> dict[str, Any]:
        result = original(**arguments)
        session.rds.instances[arguments["DBInstanceIdentifier"]]["DBInstanceStatus"] = "backing-up"
        return result

    session.rds.create_db_instance = create
    adapter = AuroraSafeChangeAdapter(
        AuroraSafeChangeConfig(
            profile="anti-demo-admin",
            region=REGION,
            account_id=ACCOUNT,
            database="anti_demo",
            secret_arn=AURORA_SECRET,
            db_subnet_group_name=SUBNET_GROUP,
            security_group_id=AURORA_SG,
            source_cluster_id=AURORA_SOURCE,
            source_writer_instance_id=AURORA_WRITER,
            poll_timeout_seconds=0.05,
            poll_interval_seconds=0.01,
        ),
        session=session,
        connector=RecordingConnector(),
        sleep=instant_sleep,
    )

    with pytest.raises(SafeChangeControlPlaneError, match="Aurora clone writer availability"):
        await adapter.create_isolated(plan(SafeChangeProvider.AURORA, AURORA_SOURCE), quiet_report)


def test_timeout_defaults_are_single_sourced_and_correctly_ordered() -> None:
    assert DEFAULT_POLL_TIMEOUT_SECONDS == 900.0
    assert DEFAULT_RUN_TIMEOUT_SECONDS == 1080.0
    # A per-wait budget at or above the whole-lane budget is unreachable: the
    # lane deadline would always fire first and the poll value would be a lie.
    assert DEFAULT_RUN_TIMEOUT_SECONDS > DEFAULT_POLL_TIMEOUT_SECONDS

    # The dataclass default must be the same constant the builder uses, so it
    # can never quietly drift into a value nothing honours.
    field_default = AwsSafeChangeConfig.__dataclass_fields__["poll_timeout_seconds"].default
    assert field_default == DEFAULT_POLL_TIMEOUT_SECONDS
    assert (
        RdsSafeChangeConfig(
            profile="anti-demo-admin",
            region=REGION,
            account_id=ACCOUNT,
            database="anti_demo",
            secret_arn=RDS_SECRET,
            db_subnet_group_name=SUBNET_GROUP,
            security_group_id=RDS_SG,
            source_instance_id=RDS_SOURCE,
        ).poll_timeout_seconds
        == DEFAULT_POLL_TIMEOUT_SECONDS
    )


def test_builder_applies_the_shared_timeout_defaults() -> None:
    engine = build_safe_change_engine(
        owned_manifest(),
        environment={},
        databricks_runner=FakeDatabricksRunner(),
        session_factory=lambda **_: FakeAwsSession(),
        connector=RecordingConnector(),
        sleep=no_sleep,
    )

    assert engine.run_timeout_seconds == DEFAULT_RUN_TIMEOUT_SECONDS
    # Every whole-operation budget must stay above the per-wait budget it wraps.
    assert engine.reset_timeout_seconds > DEFAULT_POLL_TIMEOUT_SECONDS
    for adapter in engine.competitors.values():
        assert adapter.config.poll_timeout_seconds == DEFAULT_POLL_TIMEOUT_SECONDS
    assert engine.lakebase.config.poll_timeout_seconds == DEFAULT_POLL_TIMEOUT_SECONDS


# ---------------------------------------------------------------------------
# Abandoning a cancelled lane.
#
# `abandon_isolated` is the adapter half of the cancellation-path teardown. It
# has one job -- get the delete requests accepted -- and two prohibitions: it
# must not wait for the resource to disappear, and it must not raise because the
# resource is absent or already going. Cancellation can land before creation,
# between the cluster and its writer, or part way through an ordinary teardown,
# and all three have to be safe.
# ---------------------------------------------------------------------------


def botocore_session():
    """A session that ignores ambient AWS config.

    Other tests in the suite set `AWS_PROFILE`, and a plain `get_session()`
    would fail resolving a profile that does not exist on this machine. Nothing
    here needs credentials.
    """

    return botocore.session.Session(session_vars={"profile": (None, None, None, None)})


def refuse_waiting(adapter: Any) -> None:
    """Make any absence wait a hard failure for the duration of a test."""

    async def forbidden(*args: Any, **kwargs: Any):
        del args, kwargs
        raise AssertionError("the cancellation path must not wait for absence")

    adapter._wait_for = forbidden  # type: ignore[method-assign]


def deletes(session: FakeAwsSession) -> list[tuple[str, dict[str, Any]]]:
    return [call for call in session.rds.calls if call[0].startswith("delete_")]


async def created_aurora_clone() -> tuple[FakeAwsSession, AuroraSafeChangeAdapter, SafeChangePlan]:
    session = FakeAwsSession()
    adapter = aurora_adapter(session)
    aurora_plan = plan(SafeChangeProvider.AURORA, AURORA_SOURCE)
    await adapter.create_isolated(aurora_plan, quiet_report)
    session.rds.calls.clear()
    return session, adapter, aurora_plan


async def test_aurora_abandon_issues_both_deletes_and_waits_for_neither() -> None:
    session, adapter, aurora_plan = await created_aurora_clone()
    refuse_waiting(adapter)
    writer_id = f"{aurora_plan.artifact_id}-writer"

    await adapter.abandon_isolated(aurora_plan)

    assert deletes(session) == [
        (
            "delete_db_instance",
            {
                "DBInstanceIdentifier": writer_id,
                "SkipFinalSnapshot": True,
                "DeleteAutomatedBackups": True,
            },
        ),
        (
            "delete_db_cluster",
            {
                "DBClusterIdentifier": aurora_plan.artifact_id,
                "SkipFinalSnapshot": True,
                "DeleteAutomatedBackups": True,
            },
        ),
    ]
    # The writer is the compute that made the original leak expensive, so its
    # delete has to be the first thing that goes out.
    assert deletes(session)[0][0] == "delete_db_instance"
    assert session.rds.calls == deletes(session), "no describes belong on this path"


async def test_aurora_abandon_tolerates_a_clone_that_was_never_created() -> None:
    """Cancellation can beat the restore. Deleting nothing must be quiet."""

    session = FakeAwsSession()
    adapter = aurora_adapter(session)
    refuse_waiting(adapter)
    aurora_plan = plan(SafeChangeProvider.AURORA, AURORA_SOURCE)

    await adapter.abandon_isolated(aurora_plan)

    assert [call[0] for call in deletes(session)] == [
        "delete_db_instance",
        "delete_db_cluster",
    ]


async def test_aurora_abandon_tolerates_a_writer_that_is_already_deleting() -> None:
    session, adapter, aurora_plan = await created_aurora_clone()
    refuse_waiting(adapter)
    original = session.rds.delete_db_instance

    def already_deleting(**arguments: Any) -> dict[str, Any]:
        session.rds.calls.append(("delete_db_instance", arguments))
        raise missing_error("InvalidDBInstanceState", "DeleteDBInstance")

    session.rds.delete_db_instance = already_deleting  # type: ignore[method-assign]
    try:
        await adapter.abandon_isolated(aurora_plan)
    finally:
        session.rds.delete_db_instance = original  # type: ignore[method-assign]

    assert [call[0] for call in deletes(session)] == [
        "delete_db_instance",
        "delete_db_cluster",
    ]


async def test_aurora_abandon_reports_a_cluster_it_could_not_delete_yet(caplog) -> None:
    """The ordinary outcome: the writer delete lands, the cluster cannot follow.

    RDS refuses to delete a cluster whose writer is still going, and the
    cancellation path is not allowed to wait for that. The cluster does survive,
    so its identifier has to reach the log or the orphan is invisible -- which is
    what made the original incident expensive.
    """

    session, adapter, aurora_plan = await created_aurora_clone()
    refuse_waiting(adapter)

    def accepted_but_still_a_member(**arguments: Any) -> dict[str, Any]:
        # RDS accepts the delete and moves the writer to `deleting`; it does not
        # leave the cluster instantly, which is why the cluster delete then
        # cannot be accepted.
        session.rds.calls.append(("delete_db_instance", arguments))
        return {"DBInstance": session.rds.instances[arguments["DBInstanceIdentifier"]]}

    session.rds.delete_db_instance = accepted_but_still_a_member  # type: ignore[method-assign]

    with caplog.at_level("ERROR", logger="server.safe_change_live"):
        await adapter.abandon_isolated(aurora_plan)

    assert [call[0] for call in deletes(session)] == [
        "delete_db_instance",
        "delete_db_cluster",
    ]
    assert any(
        "ORPHAN RISK" in record.getMessage() and aurora_plan.artifact_id in record.getMessage()
        for record in caplog.records
    )
    assert aurora_plan.artifact_id in session.rds.clusters, "the cluster really does survive"


async def test_aurora_abandon_attempts_the_cluster_even_when_the_writer_errors() -> None:
    session, adapter, aurora_plan = await created_aurora_clone()
    refuse_waiting(adapter)

    def denied(**arguments: Any) -> dict[str, Any]:
        session.rds.calls.append(("delete_db_instance", arguments))
        raise missing_error("AccessDenied", "DeleteDBInstance")

    session.rds.delete_db_instance = denied  # type: ignore[method-assign]

    with pytest.raises(ClientError):
        await adapter.abandon_isolated(aurora_plan)

    # An unexpected failure on the writer must not cost us the attempt on the
    # cluster, and it must still surface rather than being swallowed here.
    assert [call[0] for call in deletes(session)] == [
        "delete_db_instance",
        "delete_db_cluster",
    ]


async def test_rds_abandon_issues_exactly_one_delete() -> None:
    session = FakeAwsSession()
    adapter = rds_adapter(session)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    await adapter.create_isolated(rds_plan, quiet_report)
    session.rds.calls.clear()
    refuse_waiting(adapter)

    await adapter.abandon_isolated(rds_plan)

    assert deletes(session) == [
        (
            "delete_db_instance",
            {
                "DBInstanceIdentifier": rds_plan.artifact_id,
                "SkipFinalSnapshot": True,
                "DeleteAutomatedBackups": True,
            },
        )
    ]
    assert rds_plan.artifact_id not in session.rds.instances


async def test_rds_abandon_tolerates_a_restore_that_was_never_created() -> None:
    session = FakeAwsSession()
    adapter = rds_adapter(session)
    refuse_waiting(adapter)

    await adapter.abandon_isolated(plan(SafeChangeProvider.RDS, RDS_SOURCE))

    assert [call[0] for call in deletes(session)] == ["delete_db_instance"]


@pytest.mark.parametrize("provider", [SafeChangeProvider.AURORA, SafeChangeProvider.RDS])
async def test_abandon_refuses_an_identifier_it_did_not_derive(provider) -> None:
    """The ownership guard is the whole safety argument for deleting blind.

    Nothing is described before the delete, so the only thing standing between
    this path and someone else's database is the check that the identifier is
    this run's deterministic per-bout artifact in this account and region.
    """

    session = FakeAwsSession()
    adapter = (
        aurora_adapter(session)
        if provider == SafeChangeProvider.AURORA
        else rds_adapter(session)
    )
    source = AURORA_SOURCE if provider == SafeChangeProvider.AURORA else RDS_SOURCE
    foreign = replace(plan(provider, source), artifact_id="anti-demo-aurora")

    with pytest.raises(SafeChangeLiveConfigurationError):
        await adapter.abandon_isolated(foreign)

    assert session.rds.calls == []


async def test_abandon_delete_arguments_satisfy_the_real_rds_service_model() -> None:
    """Push the argument dicts production builds through botocore's validator.

    `FakeRdsClient` records keyword arguments without checking them, so a
    misspelled parameter reads as a passing test -- which is exactly how
    `RestoreToTime`/`RestoreTime` survived into a live demo. `Stubber` applies
    the same client-side validation a real call would, without credentials or
    network access.
    """

    session, adapter, aurora_plan = await created_aurora_clone()
    refuse_waiting(adapter)
    session.rds.clusters[aurora_plan.artifact_id]["DBClusterMembers"] = []
    await adapter.abandon_isolated(aurora_plan)

    recorded = dict(deletes(session))
    assert set(recorded) == {"delete_db_instance", "delete_db_cluster"}
    for method, arguments in recorded.items():
        client = botocore_session().create_client(
            "rds",
            region_name=REGION,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )
        with Stubber(client) as stubber:
            stubber.add_response(method, {}, arguments)
            getattr(client, method)(**arguments)
            stubber.assert_no_pending_responses()


def lakebase_adapter(runner: FakeDatabricksRunner) -> LakebaseSafeChangeAdapter:
    return LakebaseSafeChangeAdapter(
        LakebaseSafeChangeConfig(
            profile="fe-vm-test",
            source_endpoint=LAKEBASE_SOURCE,
            database="anti_demo",
            user=OWNER,
            expected_region=REGION,
            poll_interval_seconds=0.01,
        ),
        runner=runner,
        connector=RecordingConnector(),
        sleep=no_sleep,
    )


async def test_lakebase_abandon_issues_the_branch_delete_under_the_teardown_bound() -> None:
    """A cancelled lane still has to release the branch, and quickly.

    `delete_isolated` may wait out the full control timeout because it proves
    absence. Cancellation cannot: the teardown bound is what stops a cancelled
    round from holding compute, so the request goes out under the short deadline
    and somebody else proves the outcome.
    """

    runner = FakeDatabricksRunner()
    adapter = lakebase_adapter(runner)
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    await adapter.preflight(lakebase_plan)
    await adapter.create_isolated(lakebase_plan, quiet_report)
    runner.commands.clear()
    runner.timeouts.clear()

    await adapter.abandon_isolated(lakebase_plan)

    branch = f"projects/ad-test-001/branches/{lakebase_plan.artifact_id}"
    assert runner.commands == [["DELETE", lakebase_resource_path(branch)]]
    assert runner.timeouts == [DEFAULT_CANCEL_TEARDOWN_SECONDS]
    assert branch not in runner.branches


async def test_lakebase_abandon_tolerates_a_branch_that_was_never_created() -> None:
    runner = FakeDatabricksRunner()
    adapter = lakebase_adapter(runner)
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    await adapter.preflight(lakebase_plan)

    await adapter.abandon_isolated(lakebase_plan)

    assert runner.events == ["delete-branch"]


async def test_lakebase_abandon_surfaces_a_refusal_it_cannot_interpret() -> None:
    """Only "already gone" is the goal state; everything else is an orphan risk
    the engine has to hear about."""

    runner = FakeDatabricksRunner()
    adapter = lakebase_adapter(runner)
    lakebase_plan = plan(SafeChangeProvider.LAKEBASE, LAKEBASE_SOURCE)
    await adapter.preflight(lakebase_plan)

    async def refuse(*_args: Any, **_kwargs: Any) -> None:
        raise ControlPlaneCommandError("permission denied", not_found=False)

    runner.run = refuse  # type: ignore[method-assign]

    with pytest.raises(ControlPlaneCommandError):
        await adapter.abandon_isolated(lakebase_plan)


# ---------------------------------------------------------------------------
# A towel thrown mid-restore: the cooldown's delete, not the cancellation's.
#
# Round 2 *is* in the manager's cancel set, so a towel cancels the lane task and
# the tolerant `abandon_isolated` above runs first. That does not save it:
# `_reset_safe_change` runs `delete_isolated` straight afterwards, and that is
# the intolerant path, and the one whose success decides whether the ring lease
# is released. RDS will not delete a clone that is still `creating` -- it
# answers `InvalidDBClusterStateFault` or `InvalidDBInstanceState` -- and
# `reap.py` deliberately refuses to sweep any resource whose round still holds
# a lease. So a `delete_isolated` that lets the refusal escape wedges the ring
# and bills an Aurora cluster and its Serverless v2 writer until somebody reads
# a bill: the wedge disarms the very safety net meant to catch it.
#
# These drive the real Round 2 adapters against the fake RDS control plane on a
# virtual clock, raising the fault codes RDS actually raises. Round 3's twin of
# this leak, and the `_delete_when_deletable` helper both rounds now share, are
# covered in `tests/test_recovery_live.py`.
# ---------------------------------------------------------------------------

#: How long the fake clone stays in `creating`. An Aurora clone takes minutes,
#: so a towel lands well inside the window; this value is several poll
#: intervals wide, which is all the retry needs to be exercised.
CREATING_SECONDS = 90.0

#: How long the fake cluster stays `modifying` after its writer is deleted.
MODIFYING_SECONDS = 30.0

VIRTUAL_POLL_INTERVAL = 5.0


class VirtualClock:
    """A monotonic clock that only advances when the adapter sleeps.

    The retry loop is bounded in wall-clock seconds, so a real clock would make
    these either slow or flaky. Advancing time only in `sleep` makes the number
    of retries, and the budget exhaustion, exact.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


def mid_restore_aurora_adapter(
    session: FakeAwsSession,
    clock: VirtualClock,
) -> AuroraSafeChangeAdapter:
    return AuroraSafeChangeAdapter(
        replace(
            aurora_adapter(session).aurora_config,
            poll_interval_seconds=VIRTUAL_POLL_INTERVAL,
        ),
        session=session,
        connector=RecordingConnector(),
        sleep=clock.sleep,
        clock=clock,
    )


def mid_restore_rds_adapter(
    session: FakeAwsSession,
    clock: VirtualClock,
) -> RdsSafeChangeAdapter:
    return RdsSafeChangeAdapter(
        replace(
            rds_adapter(session).rds_config,
            poll_interval_seconds=VIRTUAL_POLL_INTERVAL,
        ),
        session=session,
        connector=RecordingConnector(),
        sleep=clock.sleep,
        clock=clock,
    )


def refuses_delete_while_creating(
    session: FakeAwsSession,
    clock: VirtualClock,
    *,
    instances: tuple[str, ...] = (),
    clusters: tuple[str, ...] = (),
    creating_seconds: float = CREATING_SECONDS,
) -> SimpleNamespace:
    """Hold the named clones in `creating` and refuse every delete until they leave.

    Both halves matter. Real RDS reports `creating` for the whole restore *and*
    answers the delete with `InvalidDBInstanceState` /
    `InvalidDBClusterStateFault` throughout it, so a fix that only reads the
    status would pass against a fake that only refuses, and vice versa.
    """

    tally = SimpleNamespace(refusals=0)
    described_instances = session.rds.describe_db_instances
    described_clusters = session.rds.describe_db_clusters
    delete_instance = session.rds.delete_db_instance
    delete_cluster = session.rds.delete_db_cluster

    def creating() -> bool:
        return clock.now < creating_seconds

    def describe_db_instances(**arguments: Any) -> dict[str, Any]:
        response = described_instances(**arguments)
        if arguments["DBInstanceIdentifier"] in instances and creating():
            instance = {**response["DBInstances"][0], "DBInstanceStatus": "creating"}
            return {"DBInstances": [instance]}
        return response

    def describe_db_clusters(**arguments: Any) -> dict[str, Any]:
        response = described_clusters(**arguments)
        if arguments["DBClusterIdentifier"] in clusters and creating():
            return {"DBClusters": [{**response["DBClusters"][0], "Status": "creating"}]}
        return response

    def delete_db_instance(**arguments: Any) -> dict[str, Any]:
        if arguments["DBInstanceIdentifier"] in instances and creating():
            session.rds.calls.append(("delete_db_instance", arguments))
            tally.refusals += 1
            raise missing_error("InvalidDBInstanceState", "DeleteDBInstance")
        return delete_instance(**arguments)

    def delete_db_cluster(**arguments: Any) -> dict[str, Any]:
        if arguments["DBClusterIdentifier"] in clusters and creating():
            session.rds.calls.append(("delete_db_cluster", arguments))
            tally.refusals += 1
            raise missing_error("InvalidDBClusterStateFault", "DeleteDBCluster")
        return delete_cluster(**arguments)

    session.rds.describe_db_instances = describe_db_instances  # type: ignore[method-assign]
    session.rds.describe_db_clusters = describe_db_clusters  # type: ignore[method-assign]
    session.rds.delete_db_instance = delete_db_instance  # type: ignore[method-assign]
    session.rds.delete_db_cluster = delete_db_cluster  # type: ignore[method-assign]
    return tally


def cluster_modifies_after_its_writer_goes(
    session: FakeAwsSession,
    clock: VirtualClock,
    cluster_id: str,
    *,
    seconds: float = MODIFYING_SECONDS,
) -> SimpleNamespace:
    """Put the cluster into `modifying` for a while once its writer is deleted."""

    tally = SimpleNamespace(refusals=0)
    window = SimpleNamespace(until=None)
    described_clusters = session.rds.describe_db_clusters
    delete_instance = session.rds.delete_db_instance
    delete_cluster = session.rds.delete_db_cluster

    def modifying() -> bool:
        return window.until is not None and clock.now < window.until

    def describe_db_clusters(**arguments: Any) -> dict[str, Any]:
        response = described_clusters(**arguments)
        if arguments["DBClusterIdentifier"] == cluster_id and modifying():
            return {"DBClusters": [{**response["DBClusters"][0], "Status": "modifying"}]}
        return response

    def delete_db_instance(**arguments: Any) -> dict[str, Any]:
        result = delete_instance(**arguments)
        window.until = clock.now + seconds
        return result

    def delete_db_cluster(**arguments: Any) -> dict[str, Any]:
        if arguments["DBClusterIdentifier"] == cluster_id and modifying():
            session.rds.calls.append(("delete_db_cluster", arguments))
            tally.refusals += 1
            raise missing_error("InvalidDBClusterStateFault", "DeleteDBCluster")
        return delete_cluster(**arguments)

    session.rds.describe_db_clusters = describe_db_clusters  # type: ignore[method-assign]
    session.rds.delete_db_instance = delete_db_instance  # type: ignore[method-assign]
    session.rds.delete_db_cluster = delete_db_cluster  # type: ignore[method-assign]
    return tally


async def test_aurora_teardown_waits_out_a_clone_that_is_still_creating() -> None:
    """The towel beat the writer: the clone itself is the thing mid-restore."""

    session = FakeAwsSession()
    clock = VirtualClock()
    adapter = mid_restore_aurora_adapter(session, clock)
    aurora_plan = plan(SafeChangeProvider.AURORA, AURORA_SOURCE)
    artifact = await adapter.create_isolated(aurora_plan, quiet_report)
    session.rds.instances.pop(str(artifact.metadata["writer_id"]))
    session.rds.clusters[aurora_plan.artifact_id]["DBClusterMembers"] = []
    tally = refuses_delete_while_creating(
        session,
        clock,
        clusters=(aurora_plan.artifact_id,),
    )

    await adapter.delete_isolated(aurora_plan, artifact, quiet_report)

    assert tally.refusals > 0, "the fake never reproduced the creating window"
    assert clock.now >= CREATING_SECONDS
    assert await adapter.inspect_artifact(aurora_plan) is None


async def test_aurora_teardown_waits_out_a_writer_that_is_still_creating() -> None:
    """The expensive half. The writer is the Serverless v2 compute.

    The ordering is the other half of the assertion: the writer's delete must
    be issued at or before the cluster's, or the compute outlives the storage
    it was attached to.
    """

    session = FakeAwsSession()
    clock = VirtualClock()
    adapter = mid_restore_aurora_adapter(session, clock)
    aurora_plan = plan(SafeChangeProvider.AURORA, AURORA_SOURCE)
    artifact = await adapter.create_isolated(aurora_plan, quiet_report)
    writer_id = str(artifact.metadata["writer_id"])
    tally = refuses_delete_while_creating(session, clock, instances=(writer_id,))

    await adapter.delete_isolated(aurora_plan, artifact, quiet_report)

    assert tally.refusals > 0, "the fake never reproduced the creating window"
    # Re-issued rather than status-allowlisted: RDS is the authority on when a
    # resource is deletable, so the delete goes back out on every poll until it
    # is accepted, and no list of deletable statuses has to be maintained here.
    assert tally.refusals > 1
    assert clock.now >= CREATING_SECONDS
    issued = [method for method, _ in deletes(session)]
    assert issued.index("delete_db_instance") < issued.index("delete_db_cluster")
    assert writer_id not in session.rds.instances
    assert await adapter.inspect_artifact(aurora_plan) is None


async def test_aurora_teardown_waits_out_a_cluster_the_writer_delete_left_modifying() -> None:
    """The state the fix itself provokes.

    Deleting the Serverless v2 writer is a modification of its cluster, so the
    cluster can be `modifying` at exactly the moment the ordered pair reaches
    its second half. The old code raised on anything that was not `deleting`,
    which made the correct deletion order able to wedge the cleanup by itself,
    with no mid-restore towel needed.
    """

    session = FakeAwsSession()
    clock = VirtualClock()
    adapter = mid_restore_aurora_adapter(session, clock)
    aurora_plan = plan(SafeChangeProvider.AURORA, AURORA_SOURCE)
    artifact = await adapter.create_isolated(aurora_plan, quiet_report)
    tally = cluster_modifies_after_its_writer_goes(session, clock, aurora_plan.artifact_id)

    await adapter.delete_isolated(aurora_plan, artifact, quiet_report)

    assert tally.refusals > 0, "the fake never reproduced the modifying window"
    assert clock.now >= MODIFYING_SECONDS
    assert await adapter.inspect_artifact(aurora_plan) is None


async def test_rds_teardown_waits_out_a_restore_that_is_still_creating() -> None:
    session = FakeAwsSession()
    clock = VirtualClock()
    adapter = mid_restore_rds_adapter(session, clock)
    rds_plan = plan(SafeChangeProvider.RDS, RDS_SOURCE)
    artifact = await adapter.create_isolated(rds_plan, quiet_report)
    tally = refuses_delete_while_creating(session, clock, instances=(rds_plan.artifact_id,))

    await adapter.delete_isolated(rds_plan, artifact, quiet_report)

    assert tally.refusals > 0, "the fake never reproduced the creating window"
    assert clock.now >= CREATING_SECONDS
    assert rds_plan.artifact_id not in session.rds.instances


@pytest.mark.parametrize("provider", [SafeChangeProvider.AURORA, SafeChangeProvider.RDS])
async def test_a_clone_that_never_becomes_deletable_stays_loud(
    provider: SafeChangeProvider,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Giving up must name the resource, not swallow the refusal.

    A clone that never leaves `creating` is a real orphan. The standing
    convention for "this may still be billing" is the `ORPHAN RISK` line, and
    the raise is what keeps the towel `failed` and retryable instead of
    releasing the ring over a clone nobody deleted.
    """

    session = FakeAwsSession()
    clock = VirtualClock()
    adapter: AuroraSafeChangeAdapter | RdsSafeChangeAdapter
    if provider is SafeChangeProvider.AURORA:
        adapter = mid_restore_aurora_adapter(session, clock)
        change_plan = plan(provider, AURORA_SOURCE)
        artifact = await adapter.create_isolated(change_plan, quiet_report)
        session.rds.instances.pop(str(artifact.metadata["writer_id"]))
        session.rds.clusters[change_plan.artifact_id]["DBClusterMembers"] = []
        refuses_delete_while_creating(
            session,
            clock,
            clusters=(change_plan.artifact_id,),
            creating_seconds=float("inf"),
        )
    else:
        adapter = mid_restore_rds_adapter(session, clock)
        change_plan = plan(provider, RDS_SOURCE)
        artifact = await adapter.create_isolated(change_plan, quiet_report)
        refuses_delete_while_creating(
            session,
            clock,
            instances=(change_plan.artifact_id,),
            creating_seconds=float("inf"),
        )

    with caplog.at_level("ERROR", logger="server.safe_change_live"):
        with pytest.raises(SafeChangeControlPlaneError):
            await adapter.delete_isolated(change_plan, artifact, quiet_report)

    orphan_lines = [
        record.getMessage() for record in caplog.records if "ORPHAN RISK" in record.getMessage()
    ]
    assert orphan_lines, "a clone that may still be billing was not reported"
    assert any(change_plan.artifact_id in line for line in orphan_lines)
    # It spent its whole budget before saying so, and the budget is the
    # per-wait one, deliberately under the lane's own reset deadline.
    assert clock.now >= adapter.config.poll_timeout_seconds
