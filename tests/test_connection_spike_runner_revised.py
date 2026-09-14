from __future__ import annotations

import asyncio
import base64
import gzip
import inspect
import json
import queue
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from runner import connection_spike_runner as runner
from server.connection_spike_live import SETUP_SSM_TIMEOUT_SECONDS


def test_production_preflight_executes_four_process_affinity_probe() -> None:
    source = inspect.getsource(runner.main)
    assert "shard_preflight = shard_process_preflight()" in source
    assert 'RunnerContractError("fanin_shard_preflight_failed")' in source


class _FakeValue:
    def __init__(self, value: int) -> None:
        self.value = value
        self._lock = threading.Lock()

    def get_lock(self):
        return self._lock


class _FakeProcess:
    def __init__(self, *, name: str, crash: bool = False, **unused) -> None:
        self.name = name
        self.exitcode = None
        self.alive = True
        self.crash = crash
        self.terminated = False

    def start(self) -> None:
        if self.crash:
            self.exitcode = 1
            self.alive = False

    def join(self, unused_timeout: float) -> None:
        pass

    def is_alive(self) -> bool:
        return self.alive

    def terminate(self) -> None:
        self.terminated = True
        self.alive = False
        self.exitcode = -15


class _FakeProcessContext:
    def __init__(self, *, crash_first: bool = False) -> None:
        self.processes: list[_FakeProcess] = []
        self.crash_first = crash_first

    def Queue(self):
        return queue.Queue()

    def Event(self):
        return threading.Event()

    def Value(self, unused_kind: str, value: int):
        return _FakeValue(value)

    def Process(self, **kwargs):
        process = _FakeProcess(
            name=kwargs["name"],
            crash=self.crash_first and not self.processes,
        )
        self.processes.append(process)
        return process


def _encode_request(request: dict[str, object]) -> str:
    return base64.urlsafe_b64encode(
        gzip.compress(
            json.dumps(request, sort_keys=True, separators=(",", ":")).encode(),
            mtime=0,
        )
    ).decode()


def _setup_verify_material(lane_id: str) -> tuple[dict[str, object], dict[str, object]]:
    stored: dict[str, object] = {
        "host": f"{lane_id}.example.test",
        "port": 5432,
        "dbname": "anti_demo",
        "username": runner.BASELINE_ROLE,
        "password": "credential-must-never-appear",
    }
    if lane_id != "lakebase":
        stored["master_secret_arn"] = "provider-identifier-must-never-appear"
    request = {
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
        "credential_sha256": runner.hashlib.sha256(runner._canonical_json(stored)).hexdigest(),
    }
    return request, stored


def test_bounded_main_emits_an_object_instead_of_nested_lifecycle_tuple(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    encoded_request = _encode_request({"protocol": runner.BOUNDED_PROTOCOL})
    expected = {
        "protocol": runner.BOUNDED_PROTOCOL,
        "run_id": "run-1",
        "lanes": [],
        "contracts_verified": True,
    }

    monkeypatch.setattr(runner.sys, "argv", ["connection_spike_runner.py", encoded_request])
    monkeypatch.setattr(
        runner,
        "_decode_request",
        lambda unused: ("run-1", (), "a" * 64, ()),
    )
    monkeypatch.setattr(runner, "_validate_runtime", lambda: None)
    monkeypatch.setattr(runner, "_validate_trust_bundle", lambda unused: None)
    monkeypatch.setattr(runner, "RUN_ROOT", tmp_path / "runs")
    monkeypatch.setattr(runner, "LOCK_PATH", tmp_path / "runner.lock")

    async def bounded_lifecycle(*unused):
        return expected, False

    monkeypatch.setattr(runner, "_lifecycle", bounded_lifecycle)

    assert runner.main() == 0
    payloads = [
        line.removeprefix("RESULT_GZIP_BASE64:")
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("RESULT_GZIP_BASE64:")
    ]
    assert len(payloads) == 1
    decoded = json.loads(gzip.decompress(base64.urlsafe_b64decode(payloads[0])))
    assert decoded == expected
    assert isinstance(decoded, dict)


class _VerifiedCursor:
    nonce = ""

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


class _VerifiedConnection:
    def __init__(self) -> None:
        self.closed = False

    def cursor(self):
        return _VerifiedCursor()

    async def commit(self):
        return None

    async def close(self):
        self.closed = True


class _DeadlineClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.now += delay


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
        "master_secret_arn": ("arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-master"),
    }
    request = {
        "protocol": runner.BOUNDED_PROTOCOL,
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
                "secret_arn": ("arn:aws:secretsmanager:us-west-2:123456789012:secret:bout-proxy"),
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

    request["protocol"] = runner.PROTOCOL
    with pytest.raises(runner.RunnerContractError, match="^protocol_invalid$"):
        runner._decode_request(_encode_request(request))


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
                f"arn:aws:secretsmanager:us-west-2:123456789012:secret:{lane_id}-master"
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
            "credential_sha256": runner.hashlib.sha256(runner._canonical_json(stored)).hexdigest(),
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


async def test_aurora_direct_verify_survives_the_76_to_80_second_resume_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sixth attempt is the proof: the retired fixed limit stopped after five."""

    request, stored = _setup_verify_material("aurora")
    clock = _DeadlineClock()
    available_at = 78.0
    attempts = 0
    connections: list[_VerifiedConnection] = []
    sleeps: list[float] = []

    def read_root_json(path, keys):
        assert path == runner.BASELINE_CREDENTIAL_PATHS["aurora"]
        assert keys == runner.RDS_BASELINE_KEYS
        return stored

    async def connect(database, application_name):
        nonlocal attempts
        assert database["host"] == request["endpoint_host"]
        assert application_name == "anti-demo-r5-setup-verify"
        attempts += 1
        attempt_end = clock.now + runner.CONNECT_TIMEOUT_SECONDS
        if available_at <= attempt_end:
            clock.now = available_at
            connection = _VerifiedConnection()
            connections.append(connection)
            return connection
        clock.now = attempt_end
        raise TimeoutError(
            "raw provider message with endpoint.example.test and credential-must-never-appear"
        )

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        await clock.sleep(delay)

    monkeypatch.setattr(runner, "_validate_trust_bundle", lambda digest: None)
    monkeypatch.setattr(runner, "_read_root_json", read_root_json)
    monkeypatch.setattr(runner, "_connect", connect)
    monkeypatch.setattr(runner.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(runner.asyncio, "sleep", sleep)

    result = await runner._execute_setup(request)

    assert result["status"] == "verified"
    assert attempts == len(runner.BASELINE_RESTART_RETRY_DELAYS) + 2 == 6
    assert sleeps == [1.0, 2.0, 4.0, 8.0, 8.0]
    assert clock.now == available_at
    assert len(connections) == 1 and connections[0].closed


async def test_aurora_direct_verify_deadline_fails_closed_with_sanitized_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, stored = _setup_verify_material("aurora")
    clock = _DeadlineClock()
    attempts = 0
    sleeps: list[float] = []
    raw_detail = (
        "host=endpoint.example.test user=operator password=credential-must-never-appear "
        "arn=provider-identifier-must-never-appear account=123456789012"
    )

    monkeypatch.setattr(runner, "_read_root_json", lambda path, keys: stored)

    async def connect(*unused):
        nonlocal attempts
        attempts += 1
        clock.now += min(runner.CONNECT_TIMEOUT_SECONDS, 100.0 - clock.now)
        raise runner.psycopg.OperationalError(raw_detail)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        await clock.sleep(delay)

    monkeypatch.setattr(runner, "_connect", connect)
    with pytest.raises(runner.RunnerContractError) as raised:
        await runner._verify_setup_transaction(
            request,
            retry_transient_restart=True,
            _monotonic=clock.monotonic,
            _sleep=sleep,
        )

    diagnostic = str(raised.value)
    assert diagnostic == "setup_verify_deadline_state_none_attempts_7_elapsed_100s"
    assert attempts == 7
    assert sleeps == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]
    assert len(diagnostic) <= 64
    assert all(
        forbidden not in diagnostic
        for forbidden in (
            "endpoint.example.test",
            "operator",
            "credential-must-never-appear",
            "provider-identifier-must-never-appear",
            "123456789012",
        )
    )


@pytest.mark.parametrize(
    ("failure", "sqlstate"),
    (
        (runner.psycopg.errors.InvalidPassword("raw password and endpoint"), "28p01"),
        (runner.psycopg.errors.InvalidCatalogName("raw database configuration"), "3d000"),
    ),
)
async def test_aurora_direct_verify_auth_and_configuration_fail_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    sqlstate: str,
) -> None:
    request, stored = _setup_verify_material("aurora")
    calls = 0
    sleeps: list[float] = []

    monkeypatch.setattr(runner, "_read_root_json", lambda path, keys: stored)

    async def connect(*unused):
        nonlocal calls
        calls += 1
        raise failure

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(runner, "_connect", connect)
    with pytest.raises(runner.RunnerContractError) as raised:
        await runner._verify_setup_transaction(
            request,
            retry_transient_restart=True,
            _monotonic=lambda: 0.0,
            _sleep=sleep,
        )

    assert str(raised.value) == (
        f"setup_verify_nonretryable_state_{sqlstate}_attempts_1_elapsed_0s"
    )
    assert calls == 1
    assert sleeps == []
    assert "raw " not in str(raised.value)


async def test_aurora_direct_verify_digest_mismatch_never_connects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, stored = _setup_verify_material("aurora")
    request["credential_sha256"] = "0" * 64
    monkeypatch.setattr(runner, "_read_root_json", lambda path, keys: stored)

    async def connect(*unused):
        pytest.fail("digest refusal must happen before any database connection")

    monkeypatch.setattr(runner, "_connect", connect)
    with pytest.raises(runner.RunnerContractError, match="^baseline_auth_hash_invalid$"):
        await runner._verify_setup_transaction(request, retry_transient_restart=True)


async def test_aurora_direct_verify_cancellation_interrupts_retry_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, stored = _setup_verify_material("aurora")
    sleeping = asyncio.Event()
    calls = 0

    monkeypatch.setattr(runner, "_read_root_json", lambda path, keys: stored)

    async def connect(*unused):
        nonlocal calls
        calls += 1
        raise TimeoutError("transient wake")

    async def sleep(unused):
        sleeping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runner, "_connect", connect)
    verification = asyncio.create_task(
        runner._verify_setup_transaction(
            request,
            retry_transient_restart=True,
            _sleep=sleep,
        )
    )
    await sleeping.wait()
    verification.cancel()
    with pytest.raises(asyncio.CancelledError):
        await verification
    assert calls == 1


@pytest.mark.parametrize("lane_id", ("lakebase", "rds"))
async def test_non_aurora_setup_verify_remains_single_attempt(
    monkeypatch: pytest.MonkeyPatch,
    lane_id: str,
) -> None:
    request, stored = _setup_verify_material(lane_id)
    calls = 0
    sleeps: list[float] = []

    monkeypatch.setattr(runner, "_read_root_json", lambda path, keys: stored)

    async def connect(*unused):
        nonlocal calls
        calls += 1
        raise TimeoutError("not retried outside Aurora direct wake")

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(runner, "_connect", connect)
    with pytest.raises(runner.RunnerContractError, match="^setup_verify_failed$"):
        await runner._verify_setup_transaction(
            request,
            retry_transient_restart=False,
            _sleep=sleep,
        )
    assert calls == 1
    assert sleeps == []


def test_aurora_verify_budget_preserves_the_ssm_completion_margin() -> None:
    assert SETUP_SSM_TIMEOUT_SECONDS == runner.SSM_COMMAND_TIMEOUT_SECONDS == 120.0
    assert runner.SETUP_VERIFY_DEADLINE_SECONDS == 100.0
    assert runner.SETUP_VERIFY_SSM_SAFETY_MARGIN_SECONDS == 20.0
    assert (
        runner.SETUP_VERIFY_DEADLINE_SECONDS + runner.SETUP_VERIFY_SSM_SAFETY_MARGIN_SECONDS
        == SETUP_SSM_TIMEOUT_SECONDS
    )
    assert runner.SETUP_VERIFY_DEADLINE_SECONDS < SETUP_SSM_TIMEOUT_SECONDS


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
                "SecretString": json.dumps({"username": "master", "password": "admin-not-emitted"})
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
        runner,
        "secrets_manager_for_runner_operation",
        lambda arns: (
            Secrets()
            if set(arns)
            == {
                request["master_secret_arn"],
                request["destination_secret_arn"],
            }
            else pytest.fail("runner secret client was not descriptor-bound")
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


async def test_sharded_fanin_cancellation_terminates_every_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _FakeProcessContext()
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)
    cancelled = asyncio.Event()
    cancelled.set()

    with pytest.raises(runner.RunnerCancelled, match="fanin_cancelled"):
        await runner._execute_sharded_fanin({}, cancelled)

    assert len(context.processes) == runner.fanin.WORKER_COUNT
    assert all(process.terminated for process in context.processes)
    assert not any(process.is_alive() for process in context.processes)


async def test_sharded_fanin_detects_crash_and_cleans_remaining_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _FakeProcessContext(crash_first=True)
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)

    with pytest.raises(runner.RunnerContractError, match="worker_crashed"):
        await runner._execute_sharded_fanin({}, asyncio.Event())

    assert len(context.processes) == runner.fanin.WORKER_COUNT
    assert not any(process.is_alive() for process in context.processes)


def test_worker_normal_completion_does_not_wait_for_unset_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def complete(
        unused_request,
        *,
        await_release,
        await_hold,
        **unused,
    ):
        del unused_request, unused
        await await_release()
        await await_hold()
        return {"worker_index": 0}

    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda unused: 0)
    monkeypatch.setattr(runner.fanin, "execute_fanin", complete)
    control_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    release_event = threading.Event()
    hold_event = threading.Event()
    cancel_event = threading.Event()
    release_event.set()
    hold_event.set()
    baseline_threads = set(threading.enumerate())
    started = time.monotonic()

    runner._fanin_worker_process(
        {},
        0,
        control_queue,
        result_queue,
        release_event,
        _FakeValue(1),
        hold_event,
        _FakeValue(2),
        cancel_event,
    )

    assert time.monotonic() - started < 1.0
    assert result_queue.get_nowait() == ("ok", 0, {"worker_index": 0})
    assert result_queue.empty()
    assert set(threading.enumerate()) == baseline_threads


def test_worker_cancellation_is_bounded_and_publishes_one_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def await_cancel(
        unused_request,
        *,
        cancelled,
        await_release,
        **unused,
    ):
        del unused_request, unused
        await await_release()
        await cancelled.wait()
        raise runner.RunnerCancelled("fanin_cancelled")

    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda unused: 0)
    monkeypatch.setattr(runner.fanin, "execute_fanin", await_cancel)
    control_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    release_event = threading.Event()
    release_event.set()
    cancel_event = threading.Event()
    trigger = threading.Timer(0.02, cancel_event.set)
    baseline_threads = set(threading.enumerate())
    started = time.monotonic()
    trigger.start()
    runner._fanin_worker_process(
        {},
        0,
        control_queue,
        result_queue,
        release_event,
        _FakeValue(1),
        threading.Event(),
        _FakeValue(0),
        cancel_event,
    )
    trigger.join()

    assert time.monotonic() - started < 1.0
    assert result_queue.get_nowait() == ("error", 0, "fanin_cancelled")
    assert result_queue.empty()
    assert set(threading.enumerate()) == baseline_threads


def test_worker_event_lifecycle_never_blocks_in_default_executor() -> None:
    source = inspect.getsource(runner._fanin_worker_process)
    assert "to_thread(cancel_event.wait" not in source
    assert "to_thread(release_event.wait" not in source
    assert "to_thread(hold_event.wait" not in source


def test_worker_crash_envelope_retains_sanitized_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def crash(*unused, **unused_keywords):
        raise IndexError("list index out of range")

    context = {
        "worker_id": 2,
        "worker_cpu": 2,
        "phase": "hold_sample",
        "operation": "sample_group_2",
        "wave": 26,
        "partition_start": 5_000,
        "partition_end_exclusive": 7_500,
        "lanes": {
            "lakebase": {"initiated": 2_500, "authenticated": 2_500, "held": 2_500},
            "competitor": {"initiated": 2_500, "authenticated": 2_500, "held": 2_500},
        },
    }
    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda unused: 2)
    monkeypatch.setattr(runner.fanin, "execute_fanin", crash)
    monkeypatch.setattr(runner.fanin, "worker_crash_context", lambda: context)
    control_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()

    runner._fanin_worker_process(
        {},
        2,
        control_queue,
        result_queue,
        threading.Event(),
        _FakeValue(1),
        threading.Event(),
        _FakeValue(1),
        threading.Event(),
    )

    result = result_queue.get_nowait()
    assert result[:3] == ("error", 2, "IndexError")
    envelope = result[3]
    assert envelope["phase"] == "hold_sample"
    assert envelope["operation"] == "sample_group_2"
    assert envelope["partition_start"] == 5_000
    assert envelope["partition_end_exclusive"] == 7_500
    assert envelope["lanes"] == context["lanes"]
    assert envelope["error_type"] == "IndexError"
    assert all(set(frame) == {"file", "function", "line"} for frame in envelope["frames"])
    flattened = json.dumps(envelope).lower()
    assert "password" not in flattened
    assert "host" not in flattened
    assert "secret" not in flattened


async def test_parent_capacity_failure_names_observed_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = runner.Target(
        lane_id="lakebase",
        secret_arn="unused",
        endpoint_host="lakebase.example.test",
        credential_host="lakebase.example.test",
    )
    monkeypatch.setattr(
        runner,
        "_load_baseline_database",
        lambda unused: {"user": runner.fanin.CLIENT_ROLE, "password": "unused"},
    )
    monkeypatch.setattr(
        runner,
        "_load_observer_database",
        lambda unused: {"user": runner.fanin.OBSERVER_ROLE, "password": "unused"},
    )

    async def insufficient(unused_instance_type):
        return {"sufficient": False, "failures": ["event_loop_microbatch_pressure"]}

    async def must_not_spawn(*unused_args, **unused_kwargs):
        raise AssertionError("workers must not spawn after a failed parent preflight")

    monkeypatch.setattr(runner.fanin, "capacity_preflight", insufficient)
    monkeypatch.setattr(runner, "_execute_sharded_fanin", must_not_spawn)

    with pytest.raises(
        runner.RunnerContractError,
        match="runner_capacity_insufficient_event_loop_microbatch_pressure",
    ):
        await runner._execute_fanin_request(
            {"runner_instance_type": "m6i.xlarge"},
            (target,),
            asyncio.Event(),
        )


async def test_sharded_worker_ready_timeout_is_not_capacity_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _FakeProcessContext()
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)
    monkeypatch.setattr(runner, "FANIN_WORKER_READY_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(runner.RunnerContractError, match="fanin_worker_ready_timeout"):
        await runner._execute_sharded_fanin({}, asyncio.Event())

    assert all(process.terminated for process in context.processes)
    assert not any(process.is_alive() for process in context.processes)


def test_fanin_workers_are_pinned_to_distinct_cpus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    affinity = {0, 1, 2, 3}
    pinned: list[set[int]] = []
    monkeypatch.setattr(
        runner.os,
        "sched_getaffinity",
        lambda unused_pid: pinned[-1] if pinned else affinity,
        raising=False,
    )
    monkeypatch.setattr(
        runner.os,
        "sched_setaffinity",
        lambda unused_pid, cpus: pinned.append(set(cpus)),
        raising=False,
    )

    assert runner._pin_fanin_worker(2) == 2
    assert pinned == [{2}]
