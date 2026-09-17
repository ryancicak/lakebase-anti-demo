from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import inspect
import json
import queue
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from runner import connection_spike_runner as runner
from server.connection_spike_live import SETUP_SSM_TIMEOUT_SECONDS
from tests.test_connection_fanin import worker_result as exact_worker_result


def test_affinity_probe_runs_only_during_explicit_preflight() -> None:
    source = inspect.getsource(runner.main)
    assert source.count("shard_process_preflight()") == 1
    assert 'fanin_request["action"] == "preflight"' in source
    assert "shard_preflight = shard_process_preflight()" in source


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


def test_v3_job_registry_atomically_owns_or_rejoins_one_logical_job(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = {
        "job_id": "a" * 64,
        "prepared_request_digest": "b" * 64,
    }

    job, first_owner, first_lock = runner._claim_job(request)
    same_job, second_owner, second_lock = runner._claim_job(request)

    assert first_owner is True
    assert second_owner is False
    assert same_job == job
    # The contender never acquires the ownership lock; the owner holds it.
    assert second_lock is None
    assert runner._read_job_value(job, "state") == "claimed"
    runner._release_job_lock(first_lock)


def test_v3_job_registry_refuses_same_job_id_with_different_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    runner._claim_job(
        {
            "job_id": "a" * 64,
            "prepared_request_digest": "b" * 64,
        }
    )

    with pytest.raises(runner.RunnerContractError, match="fanin_job_identity_conflict"):
        runner._claim_job(
            {
                "job_id": "a" * 64,
                "prepared_request_digest": "c" * 64,
            }
        )


def test_v3_job_registry_rejoins_the_persisted_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    request = {
        "job_id": "a" * 64,
        "prepared_request_digest": "b" * 64,
    }
    job, _owner, lock = runner._claim_job(request)
    runner._atomic_job_write(job, "result_gzip_base64", "encoded-result")
    runner._atomic_job_write(job, "state", "completed")
    runner._atomic_job_write(job, "settled", "true")
    # The owner exited after settling; a rejoining invocation replays its result.
    runner._release_job_lock(lock)

    rejoin = runner._rejoin_or_takeover_job(job, "ignored-run", request)
    assert rejoin.disposition == "replay"
    assert rejoin.encoded_result == "encoded-result"
    assert rejoin.was_cancelled is False


def test_v3_job_registry_returns_a_large_result_in_bounded_chunks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(runner, "JOB_ROOT", tmp_path / "jobs")
    job, _owner, lock = runner._claim_job(
        {
            "job_id": "a" * 64,
            "prepared_request_digest": "b" * 64,
        }
    )
    runner._release_job_lock(lock)
    encoded = "A" * 23_500
    runner._atomic_job_write(job, "result_gzip_base64", encoded)
    runner._atomic_job_write(job, "state", "completed")
    runner._atomic_job_write(job, "settled", "true")
    expected_digest = hashlib.sha256(encoded.encode("ascii")).hexdigest()
    rebuilt: list[str] = []

    for chunk_index in range(4):
        runner._job_control(
            {
                "protocol": runner.JOB_PROTOCOL,
                "schema_version": 3,
                "action": "job_result",
                "job_id": "a" * 64,
                "chunk_index": chunk_index,
            }
        )
        document = json.loads(
            capsys.readouterr().out.split(runner.JOB_RESULT_PREFIX, 1)[1]
        )
        assert document["chunk_index"] == chunk_index
        assert document["chunk_count"] == 4
        assert document["result_sha256"] == expected_digest
        rebuilt.append(document["payload"])

    assert "".join(rebuilt) == encoded


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


async def test_safety_sampling_failure_still_terminates_every_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Process(_FakeProcess):
        pid = 12345

    class Context(_FakeProcessContext):
        def Process(self, **kwargs):
            process = Process(name=kwargs["name"])
            self.processes.append(process)
            return process

    context = Context()
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)
    monkeypatch.setattr(
        runner.fanin,
        "host_safety_telemetry",
        lambda unused: (_ for _ in ()).throw(
            runner.RunnerContractError("safety_sampling_failed")
        ),
    )

    with pytest.raises(
        runner.RunnerContractError,
        match="safety_sampling_failed",
    ):
        await runner._execute_sharded_fanin({}, asyncio.Event())
    assert all(process.terminated for process in context.processes)
    assert not any(process.is_alive() for process in context.processes)


async def test_four_fresh_hold_prepares_commit_one_shared_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result_run_id = {"value": "run"}

    class BarrierProcess:
        def __init__(self, args, name: str) -> None:
            self.args = args
            self.name = name
            self.exitcode = None
            self.alive = False
            self.terminated = False
            self.thread: threading.Thread | None = None
            self.error: BaseException | None = None

        def start(self) -> None:
            self.alive = True

            def run() -> None:
                (
                    request,
                    index,
                    control_queue,
                    result_queue,
                    release_event,
                    release_ns,
                    hold_prepare_event,
                    hold_epoch_event,
                    hold_ns,
                    sample_release_event,
                    teardown_event,
                    cancel_event,
                ) = self.args
                try:
                    control_queue.put(("ready", index))
                    assert release_event.wait(2)
                    proof = {
                        "lakebase": {
                            "initiated": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                            "authenticated": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                            "held": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                            "terminal_failures": 0,
                            "cancelled": 0,
                            "target_elapsed_ns": 1,
                            "run_id": request["run_id"],
                            "lane_id": "lakebase",
                            "worker_index": index,
                            "release_ns": release_ns.value,
                        }
                    }
                    for milestone, offset in (
                        ("first_socket_initiated", 10 + index),
                        ("first_client_authenticated", 20 + index),
                    ):
                        control_queue.put(
                            (
                                "milestone",
                                index,
                                {
                                    "protocol": runner.fanin.PROTOCOL,
                                    "schema_version": runner.fanin.SCHEMA_VERSION,
                                    "lane_id": "lakebase",
                                    "milestone": milestone,
                                    "milestone_monotonic_ns": (
                                        release_ns.value + offset
                                    ),
                                },
                            )
                        )
                    control_queue.put(
                        (
                            "progress",
                            index,
                            {
                                "protocol": runner.fanin.PROTOCOL,
                                "schema_version": runner.fanin.SCHEMA_VERSION,
                                "lane_id": "lakebase",
                                "phase": "ramp",
                                "initiated_clients": 2_500,
                                "authenticated_clients": 2_500,
                                "held_clients": 2_500,
                                "terminal_failures": 0,
                                "sampled_queries_succeeded": 0,
                                "sampled_queries_failed": 0,
                                "time_to_target_ms": 0.000001,
                                "elapsed_ms": 99_000.0,
                                "release_ns": release_ns.value,
                                "hold_ns": None,
                                "snapshot_epoch": "ramp:2500:0",
                            },
                        )
                    )
                    control_queue.put(("ramp_ready", index, proof))
                    assert hold_prepare_event.wait(2)
                    assert not cancel_event.is_set()
                    control_queue.put(("hold_prepared", index, proof))
                    assert hold_epoch_event.wait(2)
                    assert not cancel_event.is_set()
                    control_queue.put(
                        ("hold_committed", index, proof, hold_ns.value)
                    )
                    assert sample_release_event.wait(2)
                    assert not cancel_event.is_set()
                    control_queue.put(("teardown_ready", index))
                    assert teardown_event.wait(2)
                    result = exact_worker_result(index, release_ns=release_ns.value)
                    result["lanes"] = [
                        lane for lane in result["lanes"]
                        if lane["lane_id"] == "lakebase"
                    ]
                    result["hold_ns"] = hold_ns.value
                    result["run_id"] = result_run_id["value"]
                    result["lanes"][0]["time_to_target_ns"] = 1
                    result["lanes"][0]["time_to_target_ms"] = 0.000001
                    result["lanes"][0]["achieved_elapsed_ms"] = (
                        (hold_ns.value - release_ns.value) / 1_000_000
                        + runner.fanin.HOLD_SECONDS * 1_000
                        + 1
                    )
                    result_queue.put(("completed", index, result))
                    self.exitcode = 0
                except BaseException as exc:
                    self.error = exc
                    self.exitcode = 1
                finally:
                    self.alive = False

            self.thread = threading.Thread(target=run, daemon=True)
            self.thread.start()

        def join(self, timeout: float) -> None:
            if self.thread is not None:
                self.thread.join(timeout)

        def is_alive(self) -> bool:
            return self.alive

        def terminate(self) -> None:
            self.terminated = True
            self.alive = False
            self.exitcode = -15

    class BarrierContext:
        def __init__(self) -> None:
            self.processes: list[BarrierProcess] = []
            self.events: list[threading.Event] = []
            self.values: list[_FakeValue] = []

        def Queue(self):
            return queue.Queue()

        def Event(self):
            value = threading.Event()
            self.events.append(value)
            return value

        def Value(self, unused_kind: str, value: int):
            result = _FakeValue(value)
            self.values.append(result)
            return result

        def Process(self, **kwargs):
            process = BarrierProcess(kwargs["args"], kwargs["name"])
            self.processes.append(process)
            return process

    context = BarrierContext()
    published: list[dict[str, object]] = []
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)
    monkeypatch.setattr(
        runner.fanin,
        "_progress_callback",
        lambda value: published.append(dict(value)),
    )

    try:
        aggregate = await runner._execute_sharded_fanin(
            {
                "run_id": "run",
                "targets": [{"lane_id": "lakebase"}],
            },
            asyncio.Event(),
        )
    except runner.RunnerContractError as exc:
        pytest.fail(
            f"parent barrier failed: {exc}; worker errors="
            f"{[repr(process.error) for process in context.processes]}"
        )

    assert aggregate["barrier_state"] == "COMPLETE"
    assert aggregate["hold_commit_count"] == 1
    assert aggregate["lanes"][0]["time_to_target_ms"] == 0.000001
    assert context.values[0].value > 0
    assert context.values[1].value > context.values[0].value
    assert all(process.exitcode == 0 for process in context.processes)
    assert not any(process.terminated for process in context.processes)
    ramp = next(value for value in published if value["phase"] == "ramping")
    assert ramp["initiated_clients"] == 10_000
    assert ramp["held_clients"] == 10_000
    assert ramp["time_to_target_ms"] is None
    assert ramp["sampled_queries_succeeded"] == 0
    assert ramp["elapsed_ms"] < 1_000
    assert ramp["first_socket_initiated_ms"] > 0
    assert ramp["first_socket_initiated_ms"] == pytest.approx(0.00001)
    assert ramp["first_client_authenticated_ms"] == pytest.approx(0.00002)
    assert (
        ramp["first_client_authenticated_ms"]
        > ramp["first_socket_initiated_ms"]
    )

    result_run_id["value"] = "other-run"
    mismatched = BarrierContext()
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: mismatched)
    with pytest.raises(
        runner.RunnerContractError,
        match="result_epoch_identity",
    ):
        await runner._execute_sharded_fanin(
            {
                "run_id": "run",
                "targets": [{"lane_id": "lakebase"}],
            },
            asyncio.Event(),
        )


async def test_resident_pool_executes_real_parent_worker_and_fanin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import multiprocessing as multiprocessing_module

    fork_context = multiprocessing_module.get_context("fork")
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: fork_context)
    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda index: index)

    class SslContext:
        check_hostname = True
        verify_mode = 2

    class Observer:
        def __init__(self, *unused):
            self.backend_pids = {101}

        async def open_and_preflight(self):
            return None

    class Diagnostics:
        def gc_callback(self, *unused):
            return None

        def install_selector_probe(self, unused_loop):
            return None

        def install_ready_batch_limit(self, unused_loop):
            return None

        def install_execution_probes(self, unused_loop):
            return None

        def restore_selector_probe(self):
            return None

        def restore_ready_batch_limit(self):
            return None

        def restore_execution_probes(self):
            return None

        def record(self, unused_phase, unused_started_ns):
            return None

        def public_dict(self):
            return {}

    class ControlledGC:
        def __init__(self, unused):
            pass

        def start(self):
            return None

        def stop(self):
            return None

    class Client:
        ready = True

        def __init__(self):
            self.closed = asyncio.get_running_loop().create_future()

    telemetry = {
        "physical_memory_bytes": 16 * 1024**3,
        "available_memory_bytes": 8 * 1024**3,
        "rss_bytes": 256 * 1024**2,
        "fd_soft_limit": 65_535,
        "open_fds": 2_600,
        "ephemeral_port_count": 28_232,
        "ephemeral_ports_in_use": 2_500,
        "ephemeral_ports_remaining": 25_732,
        "event_loop_p99_ms": 0.0,
        "cpu_capacity_fraction": 0.1,
    }
    parent_safety_samples: list[float] = []
    opened = fork_context.Value("i", 0)

    async def open_wave(lanes, unused_wave, t0_ns, **kwargs):
        del kwargs
        with opened.get_lock():
            opened.value += 1
        for lane in lanes:
            lane.initiated = runner.fanin.PARTITION_CLIENTS_PER_LANE
            lane.authenticated = runner.fanin.PARTITION_CLIENTS_PER_LANE
            lane.target_elapsed_ns = 1
            lane.first_launch_ns = t0_ns + 1
            lane.clients = [Client()] * runner.fanin.PARTITION_CLIENTS_PER_LANE

    async def telemetry_off_loop(*unused):
        return dict(telemetry)

    async def hold_and_sample(lanes, unused_observers, **kwargs):
        worker_index = int(runner.fanin._worker_execution_context["worker_id"])
        expected = len(
            tuple(
                range(
                    worker_index,
                    runner.fanin.SAMPLE_GROUPS,
                    runner.fanin.WORKER_COUNT,
                )
            )
        ) * runner.fanin.CLIENTS_PER_SAMPLE_GROUP
        for lane in lanes:
            lane.sample_attempted = expected
            lane.sample_succeeded = expected
            lane.sample_failed = 0
        kwargs["telemetry_summary"].observe(dict(telemetry))
        return runner.fanin.HOLD_SECONDS * 1_000, True

    def lane_result(lane, unused_observer, **kwargs):
        worker_index = int(runner.fanin._worker_execution_context["worker_id"])
        raw = dict(exact_worker_result(worker_index)["lanes"][0])
        raw.update(
            {
                "initiated_clients": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                "authenticated_clients": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                "held_clients_at_gate": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                "peak_authenticated_clients": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                "peak_held_clients": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                "sampled_queries_attempted": lane.sample_attempted,
                "sampled_queries_succeeded": lane.sample_succeeded,
                "sampled_queries_failed": 0,
                "first_launch_ns": lane.first_launch_ns,
                "time_to_target_ns": 1,
                "time_to_target_ms": 0.000001,
                "hold_elapsed_ms": runner.fanin.HOLD_SECONDS * 1_000,
                "achieved_elapsed_ms": (
                    kwargs["hold_elapsed_ms"]
                    + 1_000
                ),
                "telemetry_verified": True,
            }
        )
        return raw

    async def close_everything(unused_lanes, unused_observers):
        return True

    monkeypatch.setattr(runner.fanin.ssl, "create_default_context", lambda **unused: SslContext())
    monkeypatch.setattr(runner.fanin, "DirectObserver", Observer)
    monkeypatch.setattr(runner.fanin, "RuntimeDiagnostics", Diagnostics)
    monkeypatch.setattr(runner.fanin, "ControlledGC", ControlledGC)
    monkeypatch.setattr(runner.fanin, "_network_bytes", lambda: (0, 0))
    monkeypatch.setattr(runner.fanin, "_open_equal_wave_guarded", open_wave)
    monkeypatch.setattr(runner.fanin, "_telemetry_off_loop", telemetry_off_loop)
    monkeypatch.setattr(runner.fanin, "_hold_and_sample", hold_and_sample)
    monkeypatch.setattr(runner.fanin, "_lane_result", lane_result)
    monkeypatch.setattr(runner.fanin, "_close_everything", close_everything)
    def host_safety(unused):
        parent_safety_samples.append(time.monotonic())
        return {
            **telemetry,
            "fd_soft_limit": telemetry["fd_soft_limit"] * runner.fanin.WORKER_COUNT,
            "open_fds": 10_400,
            "ephemeral_ports_in_use": 10_000,
            "ephemeral_ports_remaining": 18_232,
        }

    monkeypatch.setattr(runner.fanin, "host_safety_telemetry", host_safety)

    request = {
        "schema_version": runner.fanin.SCHEMA_VERSION,
        "protocol": runner.fanin.PROTOCOL,
        "action": "run_lane_v3",
        "run_id": "resident-composed-run",
        "contract_sha256": runner.fanin.contract_sha256(),
        "config_sha256": runner.fanin.config_sha256(),
        "generator_sha256": runner.fanin.generator_sha256(),
        "capacity_model_sha256": runner.fanin.capacity_model_sha256(),
        "runner_instance_type": runner.fanin.RUNNER_INSTANCE_TYPE,
        "targets": [
            {
                "lane_id": "lakebase",
                "database": {
                    "host": "127.0.0.1",
                    "port": 5432,
                    "dbname": "anti_demo",
                    "user": runner.fanin.CLIENT_ROLE,
                    "password": "unused",
                    "credential_sha256": "a" * 64,
                },
                "observer_database": {
                    "host": "127.0.0.1",
                    "port": 5432,
                    "dbname": "anti_demo",
                    "user": runner.fanin.OBSERVER_ROLE,
                    "password": "unused",
                    "credential_sha256": "b" * 64,
                },
            }
        ],
    }
    pool = await asyncio.to_thread(runner.ResidentShardPool.start)
    release_gate = asyncio.Event()
    prepared = asyncio.Event()
    execution = asyncio.create_task(
        runner._execute_sharded_fanin(
            request,
            asyncio.Event(),
            resident_pool=pool,
            resident_release_gate=release_gate,
            on_resident_prepared=lambda: asyncio.sleep(
                0,
                result=prepared.set(),
            ),
        )
    )
    await asyncio.wait_for(prepared.wait(), timeout=2)
    assert opened.value == 0
    assert not execution.done()
    release_gate.set()
    result = await asyncio.wait_for(execution, timeout=5)

    assert result["barrier_state"] == "COMPLETE"
    assert result["hold_commit_count"] == 1
    assert result["lanes"][0]["authenticated_clients"] == 10_000
    assert result["lanes"][0]["sampled_queries_succeeded"] == 64
    assert opened.value == runner.fanin.WORKER_COUNT
    assert result["telemetry"]["telemetry_peak_cpu_capacity_fraction"] == 0.1
    assert len(parent_safety_samples) >= 3


async def test_parent_fails_closed_on_retired_telemetry_stop_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScriptedContext(_FakeProcessContext):
        def __init__(self) -> None:
            super().__init__()
            self.queues: list[queue.Queue] = []
            self.events: list[threading.Event] = []

        def Queue(self):
            value: queue.Queue = queue.Queue()
            self.queues.append(value)
            if len(self.queues) == 1:
                for index in range(runner.fanin.WORKER_COUNT):
                    value.put(("ready", index))
                value.put(
                    (
                        "telemetry_failed",
                        2,
                        "fanin_worker_telemetry_failed",
                        {
                            "worker_id": 2,
                            "worker_cpu": 2,
                            "outcome": "telemetry_failed",
                            "lanes": [],
                            "telemetry_failures": ["event_loop_pressure"],
                            "telemetry_peak_generator_owned_loop_lag_ms": 60.499,
                            "worst_stall": None,
                        },
                    )
                )
            return value

        def Event(self):
            value = threading.Event()
            self.events.append(value)
            return value

    context = ScriptedContext()
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)

    with pytest.raises(
        runner.RunnerContractError,
        match="fanin_worker_barrier_invalid",
    ):
        await runner._execute_sharded_fanin(
            {"targets": [{"lane_id": "lakebase"}]},
            asyncio.Event(),
        )

    cancel_event = context.events[4]
    assert cancel_event.is_set()
    assert all(process.terminated for process in context.processes)


@pytest.mark.parametrize(
    "code",
    [
        "available_memory_reserve_exhausted",
        "file_descriptor_reserve_exhausted",
        "ephemeral_port_reserve_exhausted",
    ],
)
async def test_parent_preserves_each_typed_hard_safety_cause(
    monkeypatch: pytest.MonkeyPatch,
    code: str,
) -> None:
    class HardFailureContext(_FakeProcessContext):
        def __init__(self) -> None:
            super().__init__()
            self.queues: list[queue.Queue] = []
            self.events: list[threading.Event] = []

        def Queue(self):
            value: queue.Queue = queue.Queue()
            self.queues.append(value)
            if len(self.queues) == 1:
                for index in range(runner.fanin.WORKER_COUNT):
                    value.put(("ready", index))
                value.put(
                    (
                        "hard_safety_failed",
                        2,
                        "fanin_worker_hard_safety_failed",
                        {
                            "worker_id": 2,
                            "worker_cpu": 2,
                            "outcome": "hard_safety_failed",
                            "lanes": [],
                            "telemetry_failures": [code],
                            "telemetry_peak_generator_owned_loop_lag_ms": 0.0,
                            "worst_stall": None,
                        },
                    )
                )
            return value

        def Event(self):
            value = threading.Event()
            self.events.append(value)
            return value

    context = HardFailureContext()
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)

    with pytest.raises(runner.RunnerContractError, match=f"^{code}$"):
        await runner._execute_sharded_fanin(
            {"run_id": "run", "targets": [{"lane_id": "lakebase"}]},
            asyncio.Event(),
        )

    assert context.events[4].is_set()


async def test_7134_partial_topology_never_commits_hold_or_publishes_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PartialContext(_FakeProcessContext):
        def __init__(self) -> None:
            super().__init__()
            self.queues: list[queue.Queue] = []
            self.events: list[threading.Event] = []
            self.values: list[_FakeValue] = []

        def Queue(self):
            value: queue.Queue = queue.Queue()
            self.queues.append(value)
            if len(self.queues) == 1:
                for index in range(runner.fanin.WORKER_COUNT):
                    value.put(("ready", index))
                counts = (
                    (2_500, 2_500),
                    (2_500, 2_500),
                    (1_081, 1_067),
                    (1_081, 1_067),
                )
                for index, (initiated, held) in enumerate(counts):
                    value.put(
                        (
                            "progress",
                            index,
                            {
                                "protocol": runner.fanin.PROTOCOL,
                                "schema_version": runner.fanin.SCHEMA_VERSION,
                                "lane_id": "lakebase",
                                "phase": "ramping",
                                "initiated_clients": initiated,
                                "authenticated_clients": held,
                                "held_clients": held,
                                "terminal_failures": 0,
                                "sampled_queries_succeeded": 0,
                                "sampled_queries_failed": 0,
                                "elapsed_ms": 99_000.0 - index * 10_000,
                                "time_to_target_ms": (
                                    40_000.0 + index if held == 2_500 else None
                                ),
                            },
                        )
                    )
                value.put(
                    (
                        "partial_result",
                        2,
                        "fanin_worker_partial_result",
                        {
                            "worker_id": 2,
                            "outcome": "partial_result",
                            "lanes": [
                                {
                                    "lane_id": "lakebase",
                                    "initiated": 7_162,
                                    "authenticated": 7_134,
                                    "held": 7_134,
                                    "samples_succeeded": 32,
                                }
                            ],
                        },
                    )
                )
            return value

        def Event(self):
            value = threading.Event()
            self.events.append(value)
            return value

        def Value(self, unused_kind: str, value: int):
            result = _FakeValue(value)
            self.values.append(result)
            return result

    context = PartialContext()
    published: list[dict[str, object]] = []
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)
    monkeypatch.setattr(
        runner.fanin,
        "_progress_callback",
        lambda value: published.append(dict(value)),
    )

    with pytest.raises(
        runner.RunnerContractError,
        match="fanin_worker_partial_result",
    ):
        await runner._execute_sharded_fanin(
            {"run_id": "run", "targets": [{"lane_id": "lakebase"}]},
            asyncio.Event(),
        )

    assert published == []
    assert context.values[1].value == 0


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
        release_ns = await await_release()
        await await_hold(
            lambda: {
                "lakebase": {
                    "initiated": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                    "authenticated": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                    "held": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                    "terminal_failures": 0,
                    "cancelled": 0,
                    "target_elapsed_ns": 1,
                    "run_id": "run",
                    "lane_id": "lakebase",
                    "worker_index": 0,
                    "release_ns": release_ns,
                }
            }
        )
        return {"worker_index": 0, "worker_outcome": "completed"}

    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda unused: 0)
    monkeypatch.setattr(runner.fanin, "execute_fanin", complete)
    control_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    release_event = threading.Event()
    hold_prepare_event = threading.Event()
    hold_epoch_event = threading.Event()
    sample_release_event = threading.Event()
    teardown_event = threading.Event()
    cancel_event = threading.Event()
    release_event.set()
    hold_prepare_event.set()
    hold_epoch_event.set()
    sample_release_event.set()
    teardown_event.set()
    baseline_threads = set(threading.enumerate())
    started = time.monotonic()

    runner._fanin_worker_process(
        {"run_id": "run"},
        0,
        control_queue,
        result_queue,
        release_event,
        _FakeValue(1),
        hold_prepare_event,
        hold_epoch_event,
        _FakeValue(2),
        sample_release_event,
        teardown_event,
        cancel_event,
    )

    assert time.monotonic() - started < 1.0
    assert result_queue.get_nowait() == (
        "completed",
        0,
        {"worker_index": 0, "worker_outcome": "completed"},
    )
    assert result_queue.empty()
    controls = []
    while not control_queue.empty():
        controls.append(control_queue.get_nowait())
    assert [message[0] for message in controls] == [
        "ready",
        "ramp_ready",
        "hold_prepared",
        "hold_committed",
    ]
    assert set(threading.enumerate()) == baseline_threads


def test_disconnect_between_ramp_ready_and_hold_prepare_prevents_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def disconnect_before_prepare(
        unused_request,
        *,
        await_release,
        await_hold,
        **unused,
    ):
        del unused_request, unused
        release_ns = await await_release()
        reads = 0

        def proof():
            nonlocal reads
            reads += 1
            held = runner.fanin.PARTITION_CLIENTS_PER_LANE - (reads > 1)
            return {
                "lakebase": {
                    "initiated": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                    "authenticated": held,
                    "held": held,
                    "terminal_failures": 0,
                    "cancelled": 0,
                    "target_elapsed_ns": 1,
                    "run_id": "run",
                    "lane_id": "lakebase",
                    "worker_index": 0,
                    "release_ns": release_ns,
                }
            }

        await await_hold(proof)
        raise AssertionError("hold must not commit after the socket disappears")

    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda unused: 0)
    monkeypatch.setattr(runner.fanin, "execute_fanin", disconnect_before_prepare)
    control_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    release_event = threading.Event()
    hold_prepare_event = threading.Event()
    hold_epoch_event = threading.Event()
    sample_release_event = threading.Event()
    release_event.set()
    hold_prepare_event.set()
    hold_epoch_event.set()
    sample_release_event.set()

    runner._fanin_worker_process(
        {"run_id": "run"},
        0,
        control_queue,
        result_queue,
        release_event,
        _FakeValue(1),
        hold_prepare_event,
        hold_epoch_event,
        _FakeValue(2),
        sample_release_event,
        threading.Event(),
        threading.Event(),
    )

    controls = []
    while not control_queue.empty():
        controls.append(control_queue.get_nowait())
    assert [message[0] for message in controls] == ["ready", "ramp_ready", "crashed"]
    assert all(message[0] != "hold_prepared" for message in controls)
    assert result_queue.get_nowait()[:3] == (
        "crashed",
        0,
        "fanin_worker_ramp_proof_invalid",
    )


def test_socket_loss_after_hold_prepared_prevents_commit_and_sampling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sampled = False

    async def disconnect_before_commit(
        unused_request,
        *,
        await_release,
        await_hold,
        **unused,
    ):
        nonlocal sampled
        del unused_request, unused
        release_ns = await await_release()
        reads = 0

        def proof():
            nonlocal reads
            reads += 1
            held = runner.fanin.PARTITION_CLIENTS_PER_LANE - (reads > 2)
            return {
                "lakebase": {
                    "initiated": runner.fanin.PARTITION_CLIENTS_PER_LANE,
                    "authenticated": held,
                    "held": held,
                    "terminal_failures": 0,
                    "cancelled": 0,
                    "target_elapsed_ns": 1,
                    "run_id": "run",
                    "lane_id": "lakebase",
                    "worker_index": 0,
                    "release_ns": release_ns,
                }
            }

        await await_hold(proof)
        sampled = True
        return {"worker_index": 0, "worker_outcome": "completed"}

    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda unused: 0)
    monkeypatch.setattr(runner.fanin, "execute_fanin", disconnect_before_commit)
    control_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    release_event = threading.Event()
    hold_prepare_event = threading.Event()
    hold_epoch_event = threading.Event()
    sample_release_event = threading.Event()
    for event in (
        release_event,
        hold_prepare_event,
        hold_epoch_event,
        sample_release_event,
    ):
        event.set()

    runner._fanin_worker_process(
        {"run_id": "run"},
        0,
        control_queue,
        result_queue,
        release_event,
        _FakeValue(1),
        hold_prepare_event,
        hold_epoch_event,
        _FakeValue(2),
        sample_release_event,
        threading.Event(),
        threading.Event(),
    )

    controls = []
    while not control_queue.empty():
        controls.append(control_queue.get_nowait())
    assert [message[0] for message in controls] == [
        "ready",
        "ramp_ready",
        "hold_prepared",
        "crashed",
    ]
    assert sampled is False
    assert result_queue.get_nowait()[:3] == (
        "crashed",
        0,
        "fanin_worker_ramp_proof_invalid",
    )


def test_retired_telemetry_outcome_is_partial_and_never_announces_ramp_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def telemetry_stop(
        unused_request,
        *,
        await_release,
        **unused,
    ):
        del unused_request, unused
        await await_release()
        return {
            "worker_outcome": "telemetry_failed",
            "lanes": [
                {
                    "lane_id": "lakebase",
                    "initiated_clients": 2_162,
                    "authenticated_clients": 2_134,
                    "cancelled_clients": 28,
                    "held_clients_at_gate": 2_134,
                    "terminal_failures": 0,
                    "sampled_queries_succeeded": 0,
                }
            ],
            "telemetry": {"telemetry_failures": ["event_loop_pressure"]},
            "runtime_diagnostics": {
                "peak_generator_owned_loop_lag_ms": 60.499,
                "significant_stall_envelopes": [],
            },
        }

    monkeypatch.setattr(runner, "_pin_fanin_worker", lambda unused: 0)
    monkeypatch.setattr(runner.fanin, "execute_fanin", telemetry_stop)
    control_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()
    release_event = threading.Event()
    release_event.set()

    runner._fanin_worker_process(
        {},
        0,
        control_queue,
        result_queue,
        release_event,
        _FakeValue(1),
        threading.Event(),
        threading.Event(),
        threading.Event(),
        _FakeValue(0),
        threading.Event(),
        threading.Event(),
    )

    controls = []
    while not control_queue.empty():
        controls.append(control_queue.get_nowait())
    assert [message[0] for message in controls] == ["ready", "partial_result"]
    assert all(message[0] != "ramp_ready" for message in controls)
    stopped = result_queue.get_nowait()
    assert stopped[:3] == (
        "partial_result",
        0,
        "fanin_worker_partial_result",
    )
    assert stopped[3]["lanes"][0]["cancelled"] == 28


async def test_cancellation_wins_when_cleanup_also_releases_a_barrier() -> None:
    released = threading.Event()
    cancelled = threading.Event()
    released.set()
    cancelled.set()

    with pytest.raises(runner.RunnerCancelled, match="fanin_cancelled"):
        await runner._await_process_event(released, cancel_event=cancelled)


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
        threading.Event(),
        _FakeValue(0),
        threading.Event(),
        threading.Event(),
        cancel_event,
    )
    trigger.join()

    assert time.monotonic() - started < 1.0
    assert result_queue.get_nowait() == ("cancelled", 0, "fanin_cancelled")
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
        threading.Event(),
        threading.Event(),
        _FakeValue(1),
        threading.Event(),
        threading.Event(),
    )

    result = result_queue.get_nowait()
    assert result[:3] == ("crashed", 2, "IndexError")
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


async def test_run_requires_boot_bound_capacity_receipt_without_rerunning_preflight(
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

    async def must_not_run_preflight(unused_instance_type):
        raise AssertionError("post-bell capacity benchmark must not run")

    async def must_not_spawn(*unused_args, **unused_kwargs):
        raise AssertionError("workers must not spawn after a failed parent preflight")

    monkeypatch.setattr(runner.fanin, "capacity_preflight", must_not_run_preflight)
    monkeypatch.setattr(runner, "_execute_sharded_fanin", must_not_spawn)

    with pytest.raises(
        runner.RunnerContractError,
        match="fanin_capacity_receipt_invalid",
    ):
        await runner._execute_fanin_request(
            {"runner_instance_type": "m6i.xlarge"},
            (target,),
            asyncio.Event(),
        )


async def test_valid_boot_receipt_uses_only_live_hard_safety_before_workers(
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
    monkeypatch.setattr(runner.fanin, "_runner_boot_id", lambda: "boot-one")
    monkeypatch.setattr(runner.fanin, "_network_bytes", lambda: (0, 0))

    async def live_safety(*unused_args, **unused_kwargs):
        return {
            "physical_memory_bytes": 15 * 1024**3,
            "available_memory_bytes": 7 * 1024**3,
            "rss_bytes": 100 * 1024**2,
            "fd_soft_limit": 65_535,
            "open_fds": 7,
            "ephemeral_port_count": 28_232,
            "ephemeral_ports_in_use": 100,
            "ephemeral_ports_remaining": 28_132,
            "event_loop_p99_ms": 71.721,
            "cpu_capacity_fraction": 0.99,
        }

    async def must_not_run_preflight(unused_instance_type):
        raise AssertionError("post-bell capacity benchmark must not run")

    async def execute(expanded, unused_cancelled, **unused_kwargs):
        del unused_kwargs
        assert expanded["targets"][0]["lane_id"] == "lakebase"
        return {"raw": "available"}

    monkeypatch.setattr(runner.fanin, "_telemetry_off_loop", live_safety)
    monkeypatch.setattr(runner.fanin, "capacity_preflight", must_not_run_preflight)
    monkeypatch.setattr(runner, "_execute_sharded_fanin", execute)

    result = await runner._execute_fanin_request(
        {
            "run_id": "run",
            "runner_instance_type": runner.fanin.RUNNER_INSTANCE_TYPE,
            "capacity_receipt": {
                "protocol": runner.fanin.PROTOCOL,
                "schema_version": runner.fanin.SCHEMA_VERSION,
                "safety_evidence_version": runner.fanin.SAFETY_EVIDENCE_VERSION,
                "boot_id": "boot-one",
                "capacity_model_sha256": runner.fanin.capacity_model_sha256(),
                    "runner_harness_sha256": runner.runner_harness_sha256(),
                "hard_safety_verified": True,
            },
        },
        (target,),
        asyncio.Event(),
    )

    assert result["raw"] == "available"
    assert result["runner_harness_sha256"] == runner.runner_harness_sha256()
    assert set(result["runner_asset_sha256s"]) == set(runner.RUNNER_HARNESS_ASSETS)
    assert result["runner_boot_id"] == "boot-one"


async def test_sharded_worker_ready_timeout_is_not_capacity_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _FakeProcessContext()
    monkeypatch.setattr(runner.mp, "get_context", lambda unused: context)
    monkeypatch.setattr(runner, "FANIN_WORKER_RUN_TIMEOUT_SECONDS", 0.01)

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
