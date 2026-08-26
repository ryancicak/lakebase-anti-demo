from __future__ import annotations

import asyncio
import base64
import gzip
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from runner import connection_spike_runner as runner


def _encode_request(request: dict[str, object]) -> str:
    return base64.urlsafe_b64encode(
        gzip.compress(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode(),
            mtime=0,
        )
    ).decode()


def test_runtime_competitor_selects_fixed_physical_credential_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "WARMUP_ATTEMPTS", 1)
    monkeypatch.setattr(runner, "SCORED_ATTEMPTS", 2)
    monkeypatch.setattr(runner, "MAX_CONCURRENCY", 2)
    schedule = []
    for lane_id in ("lakebase", "competitor"):
        for kind, count in (("warmup", 1), ("scored", 2)):
            for ordinal in range(count):
                row_uuid = uuid4()
                schedule.append(
                    {
                        "lane_id": lane_id,
                        "kind": kind,
                        "ordinal": ordinal,
                        "worker_slot": ordinal,
                        "scheduled_at_ns": 0,
                        "proof": {
                            "row_uuid": str(row_uuid),
                            "value": f"round5-{row_uuid}",
                            "attempt_id": str(uuid4()),
                        },
                    }
                )
    stored = {
        "host": "aurora.example.test",
        "port": 5432,
        "dbname": "anti_demo",
        "username": runner.BASELINE_ROLE,
        "password": "never-emitted",
        "master_secret_arn": (
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-master"
        ),
    }
    request = {
        "protocol": runner.PROTOCOL,
        "run_id": "run-1",
        "trust_bundle_path": str(runner.TRUST_BUNDLE_PATH),
        "trust_bundle_sha256": "a" * 64,
        "baseline_auth": {
            "lakebase": {"credential_sha256": "b" * 64},
            "competitor": {
                "credential_sha256": runner.hashlib.sha256(
                    runner._canonical_json(stored)
                ).hexdigest(),
                "credential_id": "aurora",
            },
        },
        "targets": [
            {
                "lane_id": "lakebase",
                "secret_arn": "",
                "endpoint_host": "lakebase-pool.example.test",
                "credential_host": "lakebase-direct.example.test",
            },
            {
                "lane_id": "competitor",
                "secret_arn": (
                    "arn:aws:secretsmanager:us-west-2:123456789012:secret:bout-proxy"
                ),
                "endpoint_host": "proxy.example.test",
                "credential_host": "aurora.example.test",
            },
        ],
        "schedule": schedule,
    }

    _, targets, _, _ = runner._decode_request(_encode_request(request))
    competitor = next(target for target in targets if target.lane_id == "competitor")
    assert competitor.baseline_credential_id == "aurora"

    def read_root_json(path, keys):
        assert path == runner.BASELINE_CREDENTIAL_PATHS["aurora"]
        assert keys == runner.RDS_BASELINE_KEYS
        return stored

    monkeypatch.setattr(runner, "_read_root_json", read_root_json)
    assert runner._load_baseline_database(competitor)["password"] == "never-emitted"

    del request["baseline_auth"]["competitor"]["credential_id"]  # type: ignore[index]
    _, legacy_targets, _, _ = runner._decode_request(_encode_request(request))
    legacy = next(target for target in legacy_targets if target.lane_id == "competitor")
    assert legacy.baseline_credential_id == "rds"


async def test_aurora_backstage_verify_retries_resume_timeout_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored_by_lane = {
        lane_id: {
            "host": f"{lane_id}.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "username": runner.BASELINE_ROLE,
            "password": "never-emitted",
            "master_secret_arn": (
                "arn:aws:secretsmanager:us-west-2:123456789012:"
                f"secret:{lane_id}-master"
            ),
        }
        for lane_id in ("aurora", "rds")
    }

    def setup_request(lane_id: str) -> dict[str, object]:
        stored = stored_by_lane[lane_id]
        return {
            "protocol": runner.SETUP_PROTOCOL,
            "action": "verify",
            "nonce": f"nonce-{lane_id}",
            "bout_id": "baseline-run-1",
            "lane_id": lane_id,
            "endpoint_host": f"{lane_id}.example.test",
            "credential_host": f"{lane_id}.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "username": runner.BASELINE_ROLE,
            "trust_bundle_path": str(runner.TRUST_BUNDLE_PATH),
            "trust_bundle_sha256": "a" * 64,
            "credential_sha256": runner.hashlib.sha256(
                runner._canonical_json(stored)
            ).hexdigest(),
        }

    def read_root_json(path, keys):
        assert keys == runner.RDS_BASELINE_KEYS
        lane_id = next(
            lane
            for lane, expected_path in runner.BASELINE_CREDENTIAL_PATHS.items()
            if path == expected_path
        )
        return stored_by_lane[lane_id]

    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, statement, parameters, *, prepare):
            assert statement == "SELECT %s::text, current_user"
            assert prepare is False
            self.nonce = parameters[0]

        async def fetchone(self):
            return self.nonce, runner.BASELINE_ROLE

    class Connection:
        def __init__(self):
            self.closed = False

        def cursor(self):
            return Cursor()

        async def commit(self):
            return None

        async def close(self):
            self.closed = True

    connect_calls: list[str] = []
    connections: list[Connection] = []

    async def connect(database, application_name):
        assert application_name == "anti-demo-r5-setup-verify"
        host = str(database["host"])
        connect_calls.append(host)
        if host == "aurora.example.test" and connect_calls.count(host) == 1:
            raise TimeoutError("Aurora is resuming")
        if host == "rds.example.test":
            raise TimeoutError("RDS timeout is not retried")
        connection = Connection()
        connections.append(connection)
        return connection

    retry_delays: list[float] = []

    async def retry_sleep(delay):
        retry_delays.append(delay)

    monkeypatch.setattr(runner, "_validate_trust_bundle", lambda digest: None)
    monkeypatch.setattr(runner, "_read_root_json", read_root_json)
    monkeypatch.setattr(runner, "_connect", connect)
    monkeypatch.setattr(runner.asyncio, "sleep", retry_sleep)

    result = await runner._execute_setup(setup_request("aurora"))

    assert result["status"] == "verified"
    assert connect_calls == ["aurora.example.test", "aurora.example.test"]
    assert retry_delays == [1.0]
    assert len(connections) == 1 and connections[0].closed

    with pytest.raises(runner.RunnerContractError, match="setup_verify_failed"):
        await runner._execute_setup(setup_request("rds"))
    assert connect_calls[-1] == "rds.example.test"
    assert connect_calls.count("rds.example.test") == 1
    assert retry_delays == [1.0]


@pytest.mark.parametrize(
    ("setup_lane_id", "credential_id"),
    (("competitor", "rds"), ("rds", "rds"), ("aurora", "aurora")),
)
async def test_revised_aws_gate_reuses_source_password_and_keeps_receipt_secret_free(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
    setup_lane_id: str,
    credential_id: str,
) -> None:
    request = {
        "protocol": runner.SETUP_PROTOCOL,
        "action": "reassert_rds_credentials",
        "nonce": "nonce-1",
        "bout_id": "bout-1",
        "lane_id": setup_lane_id,
        "endpoint_host": "proxy.example.test",
        "credential_host": "rds.example.test",
        "port": 5432,
        "dbname": "anti_demo",
        "username": runner.BASELINE_ROLE,
        "trust_bundle_path": str(runner.TRUST_BUNDLE_PATH),
        "trust_bundle_sha256": "a" * 64,
        "master_secret_arn": (
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:baseline-master"
        ),
        "destination_secret_arn": (
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:bout-proxy"
        ),
        "credential_sha256": "b" * 64,
    }
    encoded = base64.urlsafe_b64encode(
        gzip.compress(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode(),
            mtime=0,
        )
    ).decode()
    decoded = runner._decode_setup_request(encoded)

    class MinimalMasterSecret:
        def get_secret_value(self, **kwargs):
            assert kwargs == {
                "SecretId": request["master_secret_arn"],
                "VersionStage": "AWSCURRENT",
            }
            return {
                "SecretString": json.dumps(
                    {"username": "master", "password": "admin-not-emitted"}
                )
            }

    assert await runner._read_master_database(
        MinimalMasterSecret(),
        secret_arn=str(request["master_secret_arn"]),
        expected_host="rds.example.test",
        expected_port=5432,
        expected_database="anti_demo",
    ) == {
        "host": "rds.example.test",
        "port": 5432,
        "dbname": "anti_demo",
        "user": "master",
        "password": "admin-not-emitted",
    }

    stored = {
        "host": "rds.example.test",
        "port": 5432,
        "dbname": "anti_demo",
        "username": runner.BASELINE_ROLE,
        "password": "same-baseline-password",
        "master_secret_arn": request["master_secret_arn"],
    }
    def read_root_json(path, keys):
        assert path == runner.BASELINE_CREDENTIAL_PATHS[credential_id]
        assert keys == runner.RDS_BASELINE_KEYS
        return stored

    monkeypatch.setattr(runner, "_read_root_json", read_root_json)
    monkeypatch.setattr(
        runner.hashlib,
        "sha256",
        lambda value: SimpleNamespace(hexdigest=lambda: "b" * 64),
    )
    monkeypatch.setattr(runner, "_validate_trust_bundle", lambda digest: None)

    statements: list[str] = []

    class Cursor:
        rows = [(True, False, False, False, False, False)]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, statement, parameters=None, *, prepare):
            del parameters
            assert prepare is False
            statements.append(str(statement))

        async def fetchone(self):
            return self.rows.pop(0)

    class Connection:
        def cursor(self):
            return Cursor()

        async def commit(self):
            return None

        async def rollback(self):
            return None

        async def close(self):
            return None

    async def connect(database, application_name):
        assert database["user"] == "master"
        assert application_name == "anti-demo-r5-role-setup"
        return Connection()

    async def master(*unused, **kwargs):
        assert kwargs["secret_arn"] == request["master_secret_arn"]
        return {
            "host": "rds.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": "master",
            "password": "admin-not-emitted",
        }

    writes: list[dict[str, object]] = []

    class Secrets:
        def put_secret_value(self, **kwargs):
            writes.append(kwargs)

    monkeypatch.setattr(runner, "_connect", connect)
    monkeypatch.setattr(runner, "_read_master_database", master)
    monkeypatch.setattr(
        runner.boto3,
        "Session",
        lambda: SimpleNamespace(
            client=lambda name, *, region_name: (
                Secrets()
                if name == "secretsmanager" and region_name == "us-west-2"
                else pytest.fail("runner secret client was not region-bound")
            )
        ),
    )

    result = await runner._execute_setup(decoded)

    assert statements[0] == "SET LOCAL password_encryption = 'scram-sha-256'"
    alter = next(statement for statement in statements if "ALTER ROLE" in statement)
    assert all(
        clause not in alter
        for clause in (
            "NOSUPERUSER",
            "NOCREATEDB",
            "NOCREATEROLE",
            "NOREPLICATION",
            "NOBYPASSRLS",
        )
    )
    assert all("pg_authid" not in statement for statement in statements)
    assert len(writes) == 1
    destination = json.loads(str(writes[0]["SecretString"]))
    assert destination["password"] == stored["password"]
    assert writes[0]["SecretId"] == request["destination_secret_arn"]
    assert result == {
        "protocol": runner.SETUP_PROTOCOL,
        "action": "reassert_rds_credentials",
        "bout_id": "bout-1",
        "lane_id": setup_lane_id,
        "nonce": "nonce-1",
        "status": "verified",
    }
    assert stored["password"] not in json.dumps(result)

    restart_connections = []
    retry_delays: list[float] = []

    class RestartCursor:
        def __init__(self, fail: bool):
            self.fail = fail
            self.rows = [(True, False, False, False, False, False)]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, statement, parameters=None, *, prepare):
            del statement, parameters
            assert prepare is False
            if self.fail:
                self.fail = False
                raise runner.psycopg.errors.AdminShutdown("endpoint restarting")

        async def fetchone(self):
            return self.rows.pop(0)

    class RestartConnection:
        def __init__(self, fail: bool):
            self.events: list[str] = []
            self._cursor = RestartCursor(fail)

        def cursor(self):
            return self._cursor

        async def commit(self):
            self.events.append("commit")

        async def rollback(self):
            self.events.append("rollback")

        async def close(self):
            self.events.append("close")

    async def restart_connect(*unused):
        connection = RestartConnection(fail=not restart_connections)
        restart_connections.append(connection)
        return connection

    async def retry_sleep(delay):
        retry_delays.append(delay)

    with monkeypatch.context() as retry_patch:
        retry_patch.setattr(runner, "_connect", restart_connect)
        retry_patch.setattr(runner.asyncio, "sleep", retry_sleep)
        await runner._configure_ordinary_role(
            {"user": "admin"},
            {"dbname": "anti_demo", "password": "ordinary-secret"},
            create_if_missing=True,
            retry_transient_restart=True,
        )

    assert retry_delays == [1.0]
    assert [connection.events for connection in restart_connections] == [
        ["rollback", "close"],
        ["commit", "close"],
    ]

    timing_ticks = iter((100, 300))

    async def settled_attempt(attempt, database, application_name):
        del database, application_name
        return {
            "attempt_id": str(attempt.attempt_id),
            "status": "success" if attempt.ordinal == 0 else "error",
            "completed_ns": 200 if attempt.ordinal == 0 else 400,
        }

    monkeypatch.setattr(runner.time, "monotonic_ns", lambda: next(timing_ticks))
    monkeypatch.setattr(runner, "_execute_attempt", settled_attempt)
    timed = []
    for ordinal in range(2):
        row_uuid = uuid4()
        attempt = runner.Attempt(
            lane_id="competitor",
            kind="scored",
            ordinal=ordinal,
            worker_slot=ordinal,
            row_uuid=row_uuid,
            value=f"round5-{row_uuid}",
            attempt_id=uuid4(),
            scheduled_at_ns=0,
        )
        timed.append(await runner._execute_service_attempt(attempt, {}, "timed"))
    assert [(value["started_ns"], value["completed_ns"]) for value in timed] == [
        (100, 200),
        (300, 400),
    ]

    setup_started = asyncio.Event()
    setup_stopped = asyncio.Event()

    async def hanging_setup(unused):
        del unused
        setup_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            setup_stopped.set()

    monkeypatch.setattr(runner, "_execute_setup", hanging_setup)
    cancelled = asyncio.Event()
    bounded = asyncio.create_task(runner._run_setup_bounded(decoded, cancelled))
    await setup_started.wait()
    cancelled.set()
    assert await bounded == (None, True)
    assert setup_stopped.is_set()

    async def failed_setup(unused):
        del unused
        raise runner.RunnerContractError("setup_failed_safely")

    installed_signals: list[int] = []
    monkeypatch.setattr(runner, "_execute_setup", failed_setup)
    monkeypatch.setattr(runner, "_validate_runtime", lambda: None)
    monkeypatch.setattr(runner, "LOCK_PATH", tmp_path / "runner.lock")
    monkeypatch.setattr(
        runner.signal,
        "signal",
        lambda signal_number, handler: installed_signals.append(signal_number),
    )
    monkeypatch.setattr(runner.sys, "argv", ["runner", encoded])

    assert await asyncio.to_thread(runner.main) == 1
    output = capsys.readouterr().out.splitlines()
    assert installed_signals == [runner.signal.SIGTERM, runner.signal.SIGINT]
    assert output[-3:] == [
        "RUNNER_ERROR:setup_failed_safely",
        "SETUP_SETTLED:nonce-1",
        "RUNNER_FLOCK_RELEASED:bout-1",
    ]
