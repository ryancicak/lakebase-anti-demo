from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from runner import connection_spike_runner as runner
from server.connection_spike import build_schedule
from server.connection_spike_live import (
    ConnectionSpikeLiveConfig,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeTarget,
    LiveConnectionSpikeAdapter,
    _proxy_target_set_matches,
    _serialize_schedule,
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
                            "InstanceType": "m6i.large",
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
    schedule = build_schedule(("lakebase", "competitor"), scheduled_at_ns=0)
    serialized = _serialize_schedule(
        SimpleNamespace(schedule=schedule)  # type: ignore[arg-type]
    )
    task = asyncio.create_task(adapter.execute("test-run", serialized))
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
    assert ssm.send_calls[0]["TimeoutSeconds"] == 120
    assert len(ssm.send_calls[0]["Parameters"]["commands"]) == 1
    command = ssm.send_calls[0]["Parameters"]["commands"][0]
    assert len(command.encode()) < 24_000
    decoded_run_id, decoded_targets, decoded_attempts, decoded_trust = runner._decode_request(
        command.rsplit(" ", 1)[1]
    )
    assert decoded_run_id == "test-run"
    assert {target.lane_id for target in decoded_targets} == {
        "lakebase",
        "competitor",
    }
    assert {target.lane_id: target.baseline_credential_id for target in decoded_targets} == {
        "lakebase": "lakebase",
        "competitor": "rds",
    }
    assert len(decoded_attempts) == 264
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


async def test_runner_exact_probe_commit_barrier_and_frozen_connection_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row_uuid = uuid4()
    attempt_id = uuid4()
    attempt = runner.Attempt(
        lane_id="lakebase",
        kind="scored",
        ordinal=0,
        worker_slot=0,
        row_uuid=row_uuid,
        value=f"round5-{row_uuid}",
        attempt_id=attempt_id,
        scheduled_at_ns=0,
    )
    connection = FakeConnection(
        [
            (row_uuid, attempt.value, attempt_id, 321),
            (row_uuid, attempt.value),
        ]
    )
    connect_calls: list[tuple[object, str]] = []
    actual_connect = runner._connect

    async def fake_connect(database, application_name):
        connect_calls.append((database, application_name))
        return connection

    monkeypatch.setattr(runner, "_connect", fake_connect)
    observation = await runner._execute_attempt(
        attempt,
        {"host": "sealed"},
        "anti-demo-r5-test",
    )
    assert observation["status"] == "success"
    assert observation["exact"] is True
    assert connection.commits == 1
    select_call = connection.cursor_value.calls[0]
    assert "FROM public.anti_demo_probe" in select_call[0]
    assert "INSERT" not in select_call[0]
    assert select_call[1] == (attempt_id, row_uuid, attempt.value)
    assert select_call[2] is False
    assert connection.closed is True

    malformed = FakeConnection(
        [
            (row_uuid, "wrong-value", attempt_id, 322),
            (row_uuid, "wrong-value"),
        ]
    )

    async def malformed_connect(database, application_name):
        del database, application_name
        return malformed

    monkeypatch.setattr(runner, "_connect", malformed_connect)
    malformed_observation = await runner._execute_attempt(
        attempt,
        {"host": "sealed"},
        "anti-demo-r5-test",
    )
    assert malformed_observation == {
        "attempt_id": str(attempt_id),
        "status": "error",
        "completed_ns": malformed_observation["completed_ns"],
        "error": "probe_contract_failed",
    }

    low_level_calls: list[dict[str, object]] = []

    async def low_level_connect(**kwargs):
        low_level_calls.append(kwargs)
        return object()

    monkeypatch.setattr(
        runner.psycopg,
        "AsyncConnection",
        SimpleNamespace(connect=low_level_connect),
    )
    monkeypatch.setattr(runner, "_connect", actual_connect)
    await runner._connect({"host": "sealed"}, "anti-demo-r5-test")
    assert low_level_calls == [
        {
            "host": "sealed",
            "sslmode": "verify-full",
            "sslrootcert": "/opt/lakebase-anti-demo/round5/round5-ca.pem",
            "connect_timeout": 10,
            "prepare_threshold": None,
            "application_name": "anti-demo-r5-test",
        }
    ]

    schedule = build_schedule(("lakebase", "competitor"), scheduled_at_ns=0)
    raw = _serialize_schedule(
        SimpleNamespace(schedule=schedule)  # type: ignore[arg-type]
    )
    scored = [
        runner.Attempt(
            lane_id=str(item["lane_id"]),
            kind=str(item["kind"]),
            ordinal=int(item["ordinal"]),
            worker_slot=int(item["worker_slot"]),
            row_uuid=UUID(str(item["proof"]["row_uuid"])),
            value=str(item["proof"]["value"]),
            attempt_id=UUID(str(item["proof"]["attempt_id"])),
            scheduled_at_ns=int(item["scheduled_at_ns"]),
        )
        for item in raw
        if item["kind"] == "scored"
    ]

    async def instant(item, database, application_name):
        del database, application_name
        await asyncio.sleep(0)
        return {
            "attempt_id": str(item.attempt_id),
            "status": "success",
            "completed_ns": runner.time.monotonic_ns(),
        }

    monkeypatch.setattr(runner, "_execute_attempt", instant)
    runtimes = [
        runner.LaneRuntime(
            runner.Target(lane, "secret", "endpoint", "direct"),
            {},
            {},
            [],
            [],
        )
        for lane in ("lakebase", "competitor")
    ]
    results, release_ns, first_launch = await runner._run_scored(
        runtimes,
        scored,
        "test-run",
        asyncio.Event(),
    )
    assert len(results) == 256
    assert all(int(value["completed_ns"]) >= release_ns for value in results)
    assert set(first_launch) == {"lakebase", "competitor"}
    assert (max(first_launch.values()) - min(first_launch.values())) / 1_000_000 <= 10

    worst_case_lanes = []
    for lane in schedule.lane_ids:
        worst_case_lanes.append(
            {
                "lane_id": lane,
                "observations": [
                    {
                        "attempt_id": str(item.attempt_id),
                        "status": "success",
                        "completed_ns": 9_999_999_999_999_999,
                        "exact": True,
                        "backend_pid": 2_147_483_647,
                    }
                    for item in schedule.lane_attempts(lane)
                ],
                "witness": {
                    "clients": [
                        {
                            "client_id": f"w{index:02d}",
                            "retained": True,
                            "verified": True,
                            "backend_pid": 2_147_483_647 - index,
                        }
                        for index in range(64)
                    ],
                    "peak_backend_sessions": 63,
                },
            }
        )
    encoded_result = runner._encode_result(
        {
            "protocol": runner.PROTOCOL,
            "run_id": "r5-" + "x" * 32,
            "release_ns": 9_999_999_999_999_990,
            "first_launch_ns_by_lane": {
                "lakebase": 9_999_999_999_999_991,
                "competitor": 9_999_999_999_999_992,
            },
            "lanes": worst_case_lanes,
            "contracts_verified": True,
        }
    )
    assert len(encoded_result) < 23_500


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


async def test_runner_lifecycle_starts_witness_only_after_scored_settlement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    events: list[str] = []
    targets = (
        runner.Target(
            "lakebase",
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:lakebase",
            "lakebase-pooled",
            "lakebase-direct",
        ),
        runner.Target(
            "competitor",
            f"arn:aws:secretsmanager:us-west-2:{ACCOUNT}:secret:rds",
            "rds-proxy",
            "rds-direct",
        ),
    )
    attempts = tuple(
        runner.Attempt(
            lane_id=lane_id,
            kind=kind,
            ordinal=0,
            worker_slot=0,
            row_uuid=uuid4(),
            value="owned",
            attempt_id=uuid4(),
            scheduled_at_ns=0,
        )
        for kind in ("warmup", "scored")
        for lane_id in ("lakebase", "competitor")
    )

    monkeypatch.setattr(
        runner.boto3,
        "Session",
        lambda: SimpleNamespace(client=lambda service, **kwargs: object()),
    )

    async def database_config(client, target):
        del client
        return ({"lane": target.lane_id}, {"direct": target.lane_id})

    async def prepare_rows(runtimes, supplied_attempts):
        assert len(runtimes) == 2 and supplied_attempts == attempts
        events.append("prepare")

    async def warmup(runtime, supplied_attempts, run_id):
        assert run_id == "test-run" and len(supplied_attempts) == 1
        events.append(f"warmup:{runtime.target.lane_id}")
        return [{"status": "success", "attempt_id": str(supplied_attempts[0].attempt_id)}]

    async def run_scored(runtimes, supplied_attempts, run_id, cancelled):
        assert run_id == "test-run" and not cancelled.is_set()
        assert all(not runtime.witness_connections for runtime in runtimes)
        assert all(not runtime.witness_clients for runtime in runtimes)
        events.append("scored-settled")
        return (
            [
                {
                    "status": "success",
                    "attempt_id": str(item.attempt_id),
                    "completed_ns": 101,
                }
                for item in supplied_attempts
            ],
            100,
            {"lakebase": 100, "competitor": 100},
        )

    async def open_witness(runtime, run_id):
        assert run_id == "test-run"
        events.append(f"open:{runtime.target.lane_id}")
        runtime.witness_clients.append(
            {"client_id": "w00", "retained": True, "verified": False, "backend_pid": 0}
        )

    async def observe(runtime, run_id, stop):
        assert run_id == "test-run"
        events.append(f"observer:{runtime.target.lane_id}")
        await stop.wait()
        events.append(f"observer-stopped:{runtime.target.lane_id}")

    async def verify_witness(runtime):
        assert any(event == f"observer:{runtime.target.lane_id}" for event in events)
        events.append(f"verify:{runtime.target.lane_id}")
        runtime.witness_clients[0]["verified"] = True
        runtime.witness_clients[0]["backend_pid"] = 123

    async def close_witness(runtimes):
        assert len(runtimes) == 2
        events.append("close-witness")

    async def cleanup_rows(runtimes, supplied_attempts):
        assert len(runtimes) == 2 and supplied_attempts == attempts
        events.append("cleanup-rows")

    monkeypatch.setattr(runner, "_database_config", database_config)
    monkeypatch.setattr(runner, "_prepare_rows", prepare_rows)
    monkeypatch.setattr(runner, "_warmup", warmup)
    monkeypatch.setattr(runner, "_run_scored", run_scored)
    monkeypatch.setattr(runner, "_open_witness_clients", open_witness)
    monkeypatch.setattr(runner, "_observe_backend_peak", observe)
    monkeypatch.setattr(runner, "_verify_witness_clients", verify_witness)
    monkeypatch.setattr(runner, "_close_witness_clients", close_witness)
    monkeypatch.setattr(runner, "_cleanup_rows", cleanup_rows)
    monkeypatch.setattr(
        runner,
        "_cleanup_owned",
        lambda run_id, path: events.append("cleanup-owned"),
    )

    result, was_cancelled = await runner._lifecycle(
        "test-run",
        targets,
        attempts,
        asyncio.Event(),
        tmp_path / "run",
    )

    assert was_cancelled is False
    assert result is not None
    assert set(result) == {
        "protocol",
        "run_id",
        "release_ns",
        "first_launch_ns_by_lane",
        "lanes",
        "contracts_verified",
    }
    scored_index = events.index("scored-settled")
    assert all(scored_index < events.index(f"open:{lane}") for lane in ("lakebase", "competitor"))
    assert all(
        events.index(f"observer:{lane}") < events.index(f"verify:{lane}")
        for lane in ("lakebase", "competitor")
    )


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
