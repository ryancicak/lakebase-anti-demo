from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from runner import connection_spike_runner as runner
from server.connection_fanin import (
    ConnectionSpikeContract as FanInContract,
)
from server.connection_fanin import (
    capacity_model_sha256 as fanin_capacity_model_sha256,
)
from server.connection_fanin import (
    fanin_config_sha256,
    fanin_generator_sha256,
    fanin_run_request,
)
from server.connection_spike_live import (
    ConnectionSpikeLiveConfig,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeTarget,
    LiveConnectionSpikeAdapter,
    _proxy_target_set_matches,
)

ACCOUNT = "123456789012"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/sealed-round5-execution"
INSTANCE_ID = "i-0123456789abcdef0"


class FakeSts:
    def __init__(self, *, role_name: str = "sealed-round5-execution") -> None:
        self.role_name = role_name
        self.calls: list[dict[str, object]] = []

    def assume_role(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "AssumedRoleUser": {
                "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/{self.role_name}/test"
            },
            "Credentials": {
                "AccessKeyId": "temporary-access-key",
                "SecretAccessKey": "temporary-secret-key",
                "SessionToken": "temporary-session-token",
                "Expiration": datetime.now(UTC) + timedelta(minutes=15),
            },
        }


class FakeRds:
    def describe_db_proxies(self, **kwargs):
        assert kwargs == {"DBProxyName": "sealed-proxy"}
        return {
            "DBProxies": [
                {
                    "DBProxyName": "sealed-proxy",
                    "DBProxyArn": f"arn:aws:rds:us-west-2:{ACCOUNT}:db-proxy:prx-1",
                    "Endpoint": "proxy.example.test",
                    "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/proxy-role",
                    "RequireTLS": True,
                    "Status": "available",
                    "Auth": [
                        {
                            "SecretArn": (f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds"),
                            "IAMAuth": "DISABLED",
                            "UserName": "anti_demo_burst",
                            "ClientPasswordAuthType": "POSTGRES_SCRAM_SHA_256",
                        }
                    ],
                }
            ]
        }

    def describe_db_proxy_targets(self, **kwargs):
        assert kwargs == {"DBProxyName": "sealed-proxy"}
        return {
            "Targets": [
                {
                    "RdsResourceId": "sealed-instance",
                    "Type": "RDS_INSTANCE",
                    "TargetHealth": {"State": "AVAILABLE"},
                }
            ]
        }

    def describe_db_instances(self, **kwargs):
        assert kwargs == {"DBInstanceIdentifier": "sealed-instance"}
        return {
            "DBInstances": [
                {
                    "DBInstanceIdentifier": "sealed-instance",
                    "DbiResourceId": "db-SEALEDTARGET",
                    "Endpoint": {"Address": "direct-rds.example.test"},
                }
            ]
        }

    def describe_db_proxy_target_groups(self, **kwargs):
        assert kwargs == {"DBProxyName": "sealed-proxy"}
        return {
            "TargetGroups": [
                {
                    "TargetGroupName": "default",
                    "ConnectionPoolConfig": {
                        "MaxConnectionsPercent": 90,
                        "ConnectionBorrowTimeout": 120,
                    },
                }
            ]
        }


class FakeCloudWatch:
    def get_metric_statistics(self, **kwargs):
        del kwargs
        return {"Datapoints": []}


class FakeSsm:
    def __init__(self) -> None:
        self.sent = asyncio.Event()
        self.send_calls: list[dict[str, object]] = []
        self.cancel_calls: list[dict[str, object]] = []
        self.cancelled = False

    def send_command(self, **kwargs):
        self.send_calls.append(kwargs)
        self.sent.set()
        return {"Command": {"CommandId": "command-exact-1"}}

    def get_command_invocation(self, **kwargs):
        assert kwargs == {
            "CommandId": "command-exact-1",
            "InstanceId": INSTANCE_ID,
        }
        if self.cancelled:
            return {
                "Status": "Cancelled",
                "StandardOutputContent": (
                    "CLEANUP_CONFIRMED:test-run\nRUNNER_FLOCK_RELEASED:test-run\n"
                ),
            }
        return {"Status": "InProgress", "StandardOutputContent": ""}

    def cancel_command(self, **kwargs):
        self.cancel_calls.append(kwargs)
        self.cancelled = True
        return {}

    def describe_instance_information(self, **kwargs):
        assert kwargs == {"Filters": [{"Key": "InstanceIds", "Values": [INSTANCE_ID]}]}
        return {
            "InstanceInformationList": [
                {
                    "InstanceId": INSTANCE_ID,
                    "PingStatus": "Online",
                    "PlatformType": "Linux",
                }
            ]
        }


class FakeEc2:
    def describe_instances(self, **kwargs):
        assert kwargs == {"InstanceIds": [INSTANCE_ID]}
        return {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "InstanceId": INSTANCE_ID,
                            "State": {"Name": "running"},
                            "InstanceType": "c7i.2xlarge",
                            "SubnetId": "subnet-sealed",
                            "IamInstanceProfile": {
                                "Arn": f"arn:aws:iam::{ACCOUNT}:instance-profile/runner"
                            },
                            "SecurityGroups": [{"GroupId": "sg-sealed"}],
                            "MetadataOptions": {"HttpTokens": "required"},
                            "PublicIpAddress": "203.0.113.10",
                        }
                    ]
                }
            ]
        }

    def describe_security_groups(self, **kwargs):
        assert kwargs == {"GroupIds": ["sg-sealed"]}
        return {"SecurityGroups": [{"GroupId": "sg-sealed", "IpPermissions": []}]}


class FakeSessionFactory:
    def __init__(self, sts: FakeSts, ssm: FakeSsm | None = None) -> None:
        self.sts = sts
        self.ssm = ssm or FakeSsm()
        self.rds = FakeRds()
        self.cloudwatch = FakeCloudWatch()
        self.ec2 = FakeEc2()
        self.calls: list[dict[str, object]] = []
        self.client_origins: list[tuple[str, str]] = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        assumed = "aws_access_key_id" in kwargs
        origin = "assumed" if assumed else "ambient"
        factory = self

        class Session:
            def client(self, name, **client_kwargs):
                assert client_kwargs == {"region_name": "us-west-2"}
                factory.client_origins.append((origin, name))
                if origin == "ambient":
                    assert name == "sts"
                    return factory.sts
                return {
                    "ssm": factory.ssm,
                    "rds": factory.rds,
                    "cloudwatch": factory.cloudwatch,
                    "ec2": factory.ec2,
                }[name]

        return Session()


def live_config() -> ConnectionSpikeLiveConfig:
    return ConnectionSpikeLiveConfig(
        region="us-west-2",
        expected_account_id=ACCOUNT,
        execution_role_arn=ROLE_ARN,
        runner_instance_id=INSTANCE_ID,
        runner_instance_profile_arn=(f"arn:aws:iam::{ACCOUNT}:instance-profile/runner"),
        runner_subnet_id="subnet-sealed",
        runner_security_group_id="sg-sealed",
        trust_bundle_sha256="a" * 64,
        targets=(
            ConnectionSpikeTarget(
                lane_id="lakebase",
                secret_arn="",
                endpoint_host="pooled.example.test",
                credential_host="direct.example.test",
                credential_sha256="c" * 64,
            ),
            ConnectionSpikeTarget(
                lane_id="competitor",
                secret_arn=f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds",
                endpoint_host="proxy.example.test",
                credential_host="direct-rds.example.test",
                competitor_id="rds_postgres",
                competitor_target_id="sealed-instance",
                competitor_resource_id="db-SEALEDTARGET",
                rds_proxy_name="sealed-proxy",
                rds_proxy_arn=f"arn:aws:rds:us-west-2:{ACCOUNT}:db-proxy:prx-1",
                rds_proxy_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-role",
                rds_proxy_max_connections_percent=90,
                rds_proxy_borrow_timeout_seconds=120,
                database_user="anti_demo_burst",
                credential_sha256="b" * 64,
            ),
        ),
    )


def fanin_request(run_id: str) -> dict[str, object]:
    """The request the adapter dispatches, built by the builder the adapter uses.

    Not a hand-written dict. These tests are about what happens to a command in flight --
    cancellation, settlement, flock release -- and a hand-written request would let them
    keep passing while the real one became undispatchable.
    """

    return fanin_run_request(
        run_id=run_id,
        contract_sha256=FanInContract().sha256,
        config_sha256=fanin_config_sha256(),
        generator_sha256=fanin_generator_sha256(),
        capacity_model_sha256=fanin_capacity_model_sha256(),
        trust_bundle_sha256="a" * 64,
        lakebase_credential_sha256="c" * 64,
        lakebase_observer_credential_sha256="d" * 64,
        competitor_credential_sha256="b" * 64,
        competitor_observer_credential_sha256="e" * 64,
        competitor_credential_id="rds",
        targets=[
            target.runner_value() for target in live_config().targets
        ],
    )


async def test_sts_role_enforcement_and_exact_command_cancellation_cleanup() -> None:
    available_target = FakeRds().describe_db_proxy_targets(DBProxyName="sealed-proxy")["Targets"]
    assert _proxy_target_set_matches(
        "rds_postgres",
        "sealed-instance",
        "db-SEALEDTARGET",
        available_target,
    )
    assert _proxy_target_set_matches(
        "rds_postgres",
        "sealed-instance",
        "db-SEALEDTARGET",
        available_target,
        require_available=True,
    )
    assert not _proxy_target_set_matches(
        "rds_postgres",
        "sealed-instance",
        "db-SEALEDTARGET",
        [{**available_target[0], "RdsResourceId": "foreign-instance"}],
        require_available=True,
    )

    wrong_factory = FakeSessionFactory(FakeSts(role_name="wrong-role"))
    wrong_adapter = LiveConnectionSpikeAdapter(live_config(), session_factory=wrong_factory)
    with pytest.raises(
        ConnectionSpikeLiveConfigurationError,
        match="sealed Round 5 assumed role",
    ):
        await wrong_adapter.check()
    assert wrong_factory.calls == [{"region_name": "us-west-2"}]
    assert wrong_factory.client_origins == [("ambient", "sts")]

    ssm = FakeSsm()
    factory = FakeSessionFactory(FakeSts(), ssm)

    async def poll(_: float) -> None:
        await asyncio.sleep(0)

    adapter = LiveConnectionSpikeAdapter(
        live_config(),
        session_factory=factory,
        sleep=poll,
    )
    task = asyncio.create_task(adapter.execute("test-run", fanin_request("test-run")))
    await asyncio.wait_for(ssm.sent.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert factory.calls[0] == {"region_name": "us-west-2"}
    assert factory.calls[1] == {
        "aws_access_key_id": "temporary-access-key",
        "aws_secret_access_key": "temporary-secret-key",
        "aws_session_token": "temporary-session-token",
        "region_name": "us-west-2",
    }
    assert factory.client_origins == [
        ("ambient", "sts"),
        ("assumed", "ssm"),
        ("assumed", "rds"),
        ("assumed", "cloudwatch"),
        ("assumed", "ec2"),
    ]
    assert len(ssm.send_calls) == 1
    assert ssm.send_calls[0]["TimeoutSeconds"] == 660
    assert len(ssm.send_calls[0]["Parameters"]["commands"]) == 1
    command = ssm.send_calls[0]["Parameters"]["commands"][0]
    assert len(command.encode()) < 24_000
    decoded_run_id, decoded_targets, decoded_trust, decoded = runner._decode_fanin_request(
        command.rsplit(" ", 1)[1]
    )
    assert decoded_run_id == "test-run"
    assert decoded["action"] == "run"
    assert {target.lane_id for target in decoded_targets} == {
        "lakebase",
        "competitor",
    }
    assert {target.lane_id: target.baseline_credential_id for target in decoded_targets} == {
        "lakebase": "lakebase",
        "competitor": "rds",
    }
    # Both credentials per lane survive the round trip. The observer one is what proves
    # multiplexing, so a request that reached the runner without it would run 10,000
    # clients with nothing watching the backend sessions behind them.
    assert {target.lane_id: target.observer_sha256 for target in decoded_targets} == {
        "lakebase": "d" * 64,
        "competitor": "e" * 64,
    }
    # The shape the capacity model was calibrated on. An empty value here is not a
    # missing field on the wire, it is a capacity gate the runner fails by name.
    assert decoded["runner_instance_type"] == "c7i.2xlarge"
    assert decoded_trust == "a" * 64
    assert ssm.cancel_calls == [{"CommandId": "command-exact-1", "InstanceIds": [INSTANCE_ID]}]


async def test_live_preflight_accepts_exact_aurora_cluster_proxy_binding() -> None:
    secret_arn = f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:aurora"
    target = ConnectionSpikeTarget(
        lane_id="competitor",
        secret_arn=secret_arn,
        endpoint_host="aurora-proxy.example.test",
        credential_host="aurora-direct.example.test",
        competitor_id="aurora_serverless_v2",
        competitor_target_id="sealed-aurora",
        competitor_resource_id="cluster-SEALEDTARGET",
        rds_proxy_name="sealed-aurora-proxy",
        rds_proxy_arn=f"arn:aws:rds:us-west-2:{ACCOUNT}:db-proxy:prx-aurora",
        rds_proxy_role_arn=f"arn:aws:iam::{ACCOUNT}:role/proxy-role",
        rds_proxy_max_connections_percent=90,
        rds_proxy_borrow_timeout_seconds=120,
        database_user="anti_demo_burst",
    )

    class AuroraRds:
        def describe_db_proxies(self, **kwargs):
            assert kwargs == {"DBProxyName": "sealed-aurora-proxy"}
            return {
                "DBProxies": [
                    {
                        "DBProxyName": "sealed-aurora-proxy",
                        "DBProxyArn": target.rds_proxy_arn,
                        "Status": "available",
                        "Endpoint": target.endpoint_host,
                        "RoleArn": target.rds_proxy_role_arn,
                        "RequireTLS": True,
                        "Auth": [
                            {
                                "SecretArn": secret_arn,
                                "IAMAuth": "DISABLED",
                                "UserName": "anti_demo_burst",
                                "ClientPasswordAuthType": "POSTGRES_SCRAM_SHA_256",
                            }
                        ],
                    }
                ]
            }

        def describe_db_clusters(self, **kwargs):
            assert kwargs == {"DBClusterIdentifier": "sealed-aurora"}
            return {
                "DBClusters": [
                    {
                        "DBClusterIdentifier": "sealed-aurora",
                        "DbClusterResourceId": "cluster-SEALEDTARGET",
                        "Endpoint": "aurora-direct.example.test",
                    }
                ]
            }

        def describe_db_proxy_targets(self, **kwargs):
            assert kwargs == {"DBProxyName": "sealed-aurora-proxy"}
            return {
                "Targets": [
                    {
                        "Type": "TRACKED_CLUSTER",
                        "RdsResourceId": "sealed-aurora",
                    },
                    {
                        "Type": "RDS_INSTANCE",
                        "TrackedClusterId": "sealed-aurora",
                        "RdsResourceId": "db-WRITER",
                        "TargetHealth": {"State": "AVAILABLE"},
                    },
                ]
            }

        def describe_db_proxy_target_groups(self, **kwargs):
            assert kwargs == {"DBProxyName": "sealed-aurora-proxy"}
            return {
                "TargetGroups": [
                    {
                        "TargetGroupName": "default",
                        "ConnectionPoolConfig": {
                            "MaxConnectionsPercent": 90,
                            "ConnectionBorrowTimeout": 120,
                        },
                    }
                ]
            }

    await LiveConnectionSpikeAdapter(live_config())._preflight_targets(
        AuroraRds(), targets=(target,)
    )


class FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...], bool]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *unused):
        return None

    async def execute(self, statement, parameters, *, prepare):
        self.calls.append((statement, parameters, prepare))

    async def fetchone(self):
        return self.rows.pop(0)


class FakeConnection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.cursor_value = FakeCursor(rows)
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_value

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        return None

    async def close(self):
        self.closed = True




class WitnessCursor:
    def __init__(self, backend_pid: int) -> None:
        self.backend_pid = backend_pid
        self.parameters: tuple[object, ...] = ()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *unused):
        return None

    async def execute(self, statement, parameters, *, prepare):
        assert "%s::text" in statement
        assert prepare is False
        self.parameters = parameters

    async def fetchone(self):
        return self.parameters[0], self.backend_pid


class WitnessConnection:
    def __init__(self, backend_pid: int) -> None:
        self.backend_pid = backend_pid
        self.closed = False

    def cursor(self):
        return WitnessCursor(self.backend_pid)

    async def commit(self):
        return None

    async def close(self):
        self.closed = True


class CleanupCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...], bool]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *unused):
        return None

    async def execute(self, statement, parameters, *, prepare):
        self.calls.append((statement, parameters, prepare))

    async def fetchone(self):
        return (0,)


class CleanupConnection:
    def __init__(self) -> None:
        self.cursor_value = CleanupCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_value

    async def commit(self):
        return None

    async def close(self):
        self.closed = True


async def test_runner_retains_real_witness_clients_and_deletes_only_owned_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_connects = 0
    peak_connects = 0
    connections: list[WitnessConnection] = []

    async def witness_connect(database, application_name):
        nonlocal active_connects, peak_connects
        del database
        assert application_name == "anti-demo-r5-test-run-witness"
        active_connects += 1
        peak_connects = max(peak_connects, active_connects)
        await asyncio.sleep(0)
        connection = WitnessConnection(100 + len(connections) % 8)
        connections.append(connection)
        active_connects -= 1
        return connection

    runtime = runner.LaneRuntime(
        runner.Target("lakebase", "secret", "pooled", "direct"),
        {},
        {},
        [],
        [],
    )
    monkeypatch.setattr(runner, "_connect", witness_connect)
    await runner._open_witness_clients(runtime, "test-run")
    await runner._verify_witness_clients(runtime)
    assert len(runtime.witness_connections) == 64
    assert len(runtime.witness_clients) == 64
    assert peak_connects <= 8
    assert all(client["retained"] and client["verified"] for client in runtime.witness_clients)
    assert len({client["backend_pid"] for client in runtime.witness_clients}) < 64

    observer_calls: list[tuple[str, tuple[object, ...], bool]] = []
    observer_stop = asyncio.Event()

    class ObserverCursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, statement, parameters, *, prepare):
            observer_calls.append((statement, parameters, prepare))
            observer_stop.set()

        async def fetchone(self):
            return (8,)

    class ObserverConnection:
        def cursor(self):
            return ObserverCursor()

        async def commit(self):
            return None

        async def close(self):
            return None

    async def observer_connect(database, application_name):
        assert database == runtime.direct_database
        assert application_name == "anti-demo-r5-test-run-observer"
        return ObserverConnection()

    monkeypatch.setattr(runner, "_connect", observer_connect)
    await runner._observe_backend_peak(runtime, "test-run", observer_stop)
    assert len(observer_calls) == 1
    statement, parameters, prepare = observer_calls[0]
    assert "application_name = %s" in statement
    assert "LIKE" not in statement
    assert parameters == ("anti-demo-r5-test-run-witness",)
    assert prepare is False
    assert runtime.peak_backend_sessions == 8

    cleanup_connection = CleanupConnection()

    async def cleanup_connect(database, application_name):
        assert database == {"host": "direct"}
        assert application_name == "anti-demo-r5-cleanup"
        return cleanup_connection

    monkeypatch.setattr(runner, "_connect", cleanup_connect)
    owned = [
        runner.Attempt(
            lane_id="lakebase",
            kind="warmup",
            ordinal=index,
            worker_slot=index,
            row_uuid=uuid4(),
            value="owned",
            attempt_id=uuid4(),
            scheduled_at_ns=0,
        )
        for index in range(4)
    ]
    runtime.direct_database = {"host": "direct"}
    prepare_connection = FakeConnection([(len(owned),)])

    async def prepare_connect(database, application_name):
        assert database == {"host": "direct"}
        assert application_name == "anti-demo-r5-prepare"
        return prepare_connection

    monkeypatch.setattr(runner, "_connect", prepare_connect)
    await runner._prepare_rows((runtime,), owned)
    prepare_calls = prepare_connection.cursor_value.calls
    assert len(prepare_calls) == len(owned) + 1
    assert all(
        "INSERT INTO public.anti_demo_probe" in call[0]
        and call[1] == (item.row_uuid, item.value)
        and call[2] is False
        for call, item in zip(prepare_calls[:-1], owned, strict=True)
    )

    monkeypatch.setattr(runner, "_connect", cleanup_connect)
    await runner._cleanup_rows((runtime,), owned)
    delete, verify = cleanup_connection.cursor_value.calls
    assert "DELETE FROM public.anti_demo_probe" in delete[0]
    assert delete[1] == ([item.row_uuid for item in owned],)
    assert delete[2] is False
    assert "SELECT count(*)" in verify[0]
    assert verify[1] == delete[1]
    assert verify[2] is False
    assert cleanup_connection.closed is True




async def test_prepare_rows_cancels_and_settles_siblings_before_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sibling_started = asyncio.Event()
    events: list[str] = []

    class PrepareCursor:
        def __init__(self, lane: str) -> None:
            self.lane = lane

        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, statement, parameters, *, prepare):
            del statement, parameters, prepare
            if self.lane == "failing":
                await sibling_started.wait()
                raise runner.RunnerContractError("prepare_rows_failed")
            sibling_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                events.append("sibling-cancelled")
                raise

        async def fetchone(self):
            return (1,)

    class PrepareConnection:
        def __init__(self, lane: str) -> None:
            self.lane = lane

        def cursor(self):
            return PrepareCursor(self.lane)

        async def commit(self):
            events.append(f"commit:{self.lane}")

        async def close(self):
            events.append(f"closed:{self.lane}")

    async def connect(database, application_name):
        assert application_name == "anti-demo-r5-prepare"
        return PrepareConnection(database["lane"])

    monkeypatch.setattr(runner, "_connect", connect)
    runtimes = tuple(
        runner.LaneRuntime(
            runner.Target(lane, "secret", "pooled", "direct"),
            {},
            {"lane": lane},
            [],
            [],
        )
        for lane in ("failing", "sibling")
    )
    attempts = tuple(
        runner.Attempt(
            lane_id=lane,
            kind="warmup",
            ordinal=0,
            worker_slot=0,
            row_uuid=uuid4(),
            value="owned",
            attempt_id=uuid4(),
            scheduled_at_ns=0,
        )
        for lane in ("failing", "sibling")
    )

    with pytest.raises(runner.RunnerContractError, match="prepare_rows_failed"):
        await runner._prepare_rows(runtimes, attempts)
        events.append("prepare-returned")

    assert events == ["closed:failing", "sibling-cancelled", "closed:sibling"]
    assert not any(event.startswith("commit:") for event in events)
