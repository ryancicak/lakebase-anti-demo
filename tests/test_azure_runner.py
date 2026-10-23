"""runner/azure_runner.py: the AWS Round 5 harness, unchanged, on Service Bus and Key Vault.

The harness reaches AWS through two boto3 clients alone. The Azure entry point stands in for both,
so the harness files stay exactly as AWS seals them (Ryan, 2026-10-20). These pin that the stand-ins
answer in the shapes the harness reads, that the harness's own checks accept Azure's identifiers,
and that nothing else of boto3 can be reached.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

RUNNER = Path(__file__).resolve().parent.parent / "runner"
sys.path.insert(0, str(RUNNER))

import azure_runner  # noqa: E402

VAULT = "https://kv-test.vault.azure.net"
QUEUE = "sb://sb-test-r5.servicebus.windows.net/queue-r5-runner-0"


def test_a_key_vault_secret_rides_the_harness_arn_checks() -> None:
    import connection_spike_runner
    import external_io

    identifier = azure_runner.secret_identifier("westus2", "round5-competitor-master")
    assert identifier == ("arn:aws:secretsmanager:westus2:000000000000:secret:"
                          "round5-competitor-master")
    # The harness's own two checks, untouched, take it as one region and one account.
    assert connection_spike_runner._secrets_manager_region([identifier], "invalid") == "westus2"
    parts = identifier.split(":", 5)
    assert parts[2] == "secretsmanager" and parts[5].startswith("secret:")
    with pytest.raises(ValueError):
        azure_runner.secret_identifier("westus2", "not_a_vault_name")
    assert external_io is not None


def test_get_secret_value_reads_key_vault_and_waits_out_an_unwritten_secret() -> None:
    reads: list[tuple[str, str]] = []

    def read(vault: str, name: str, credential: Any) -> str | None:
        reads.append((vault, name))
        return {"round5-lakebase-control": "postgresql://control"}.get(name)

    secrets = azure_runner.KeyVaultSecrets(VAULT, object(), read=read)
    identifier = azure_runner.secret_identifier("westus2", "round5-lakebase-control")
    assert secrets.get_secret_value(SecretId=identifier) == {"SecretString": "postgresql://control"}
    assert reads == [(VAULT, "round5-lakebase-control")]
    # The harness waits on its client's own ResourceNotFoundException while the installer has
    # not written the value yet (connection_spike_runner._resident_agent).
    missing = azure_runner.secret_identifier("westus2", "round5-competitor-control")
    with pytest.raises(secrets.exceptions.ResourceNotFoundException):
        secrets.get_secret_value(SecretId=missing, VersionStage="AWSCURRENT")
    with pytest.raises(ValueError):
        secrets.get_secret_value(SecretId="arn:aws:secretsmanager:us-west-2:123456789012:secret:x")


def test_a_vault_that_refuses_the_vm_for_now_is_waited_out_and_noted_once(capsys) -> None:
    # A fresh install's role assignment reaches Key Vault minutes after the VM is up.
    answers: list[Any] = [403, 403, 500]

    def read(vault: str, name: str, credential: Any) -> str | None:
        code = answers.pop(0)
        raise urllib.error.HTTPError(f"{vault}/secrets/{name}", code, "refused", None, None)

    secrets = azure_runner.KeyVaultSecrets(VAULT, object(), read=read)
    identifier = azure_runner.secret_identifier("westus2", "round5-lakebase-control")
    for _ in range(2):
        with pytest.raises(secrets.exceptions.ResourceNotFoundException):
            secrets.get_secret_value(SecretId=identifier)
    assert capsys.readouterr().err.count("KEY_VAULT_REFUSED_WAITING") == 1
    with pytest.raises(urllib.error.HTTPError):  # anything else is not waited out
        secrets.get_secret_value(SecretId=identifier)


class Message:
    def __init__(self, body: str) -> None:
        self.body, self.acknowledged = body, False
        self.left: list[float] = []

    def acknowledge(self) -> None:
        self.acknowledged = True

    def leave(self, visibility_timeout: float) -> None:
        self.left.append(visibility_timeout)


class Queue:
    def __init__(self, namespace: str, queue: str, credential: Any) -> None:
        self.address = (namespace, queue)
        self.stage, self.release = Message("stage"), Message("release")
        self.pending = [self.stage, self.release]
        self.waits: list[float] = []

    def receive(self, wait_seconds: float) -> list[Message]:
        self.waits.append(wait_seconds)
        return [self.pending.pop(0)] if self.pending else []


def test_receive_and_delete_message_settle_the_service_bus_message_they_name() -> None:
    made: list[Queue] = []

    def factory(namespace: str, queue: str, credential: Any) -> Queue:
        made.append(Queue(namespace, queue, credential))
        return made[-1]

    sqs = azure_runner.ServiceBusSqs(object(), queue_factory=factory)
    # The agent's own call (connection_spike_runner._resident_agent).
    first = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=1, WaitTimeSeconds=1,
                                VisibilityTimeout=720)
    (message,) = first["Messages"]
    assert message["Body"] == "stage" and message["ReceiptHandle"]
    sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=message["ReceiptHandle"])
    assert made[0].address == ("sb-test-r5", "queue-r5-runner-0")
    second = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(made) == 1  # one queue per address, kept
    assert sqs.receive_message(QueueUrl=QUEUE, WaitTimeSeconds=1) == {}
    assert second["Messages"][0]["Body"] == "release"
    with pytest.raises(ValueError):
        sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle="never-issued")
    with pytest.raises(ValueError):
        sqs.receive_message(QueueUrl="https://sqs.us-west-2.amazonaws.com/1/q.fifo")
    with pytest.raises(ValueError):
        sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=10)


def test_a_message_still_held_at_the_next_receive_goes_back_as_left() -> None:
    # The agent deletes before it next receives, or never (RESIDENT_CONTROL_TRANSIENT, say): SQS
    # then hands the message out again after the visibility timeout it was received under.
    made: list[Queue] = []

    def factory(namespace: str, queue: str, credential: Any) -> Queue:
        made.append(Queue(namespace, queue, credential))
        return made[-1]

    other = "sb://sb-test-r5.servicebus.windows.net/queue-r5-runner-1"
    sqs = azure_runner.ServiceBusSqs(object(), queue_factory=factory)
    first = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=1, WaitTimeSeconds=1,
                                VisibilityTimeout=720)
    assert first["Messages"][0]["Body"] == "stage"
    sqs.receive_message(QueueUrl=other, WaitTimeSeconds=1)  # another queue's receive leaves nothing
    assert made[0].stage.left == []
    second = sqs.receive_message(QueueUrl=QUEUE, WaitTimeSeconds=1, VisibilityTimeout=720)
    assert made[0].stage.left == [720.0] and not made[0].stage.acknowledged
    assert second["Messages"][0]["Body"] == "release"
    with pytest.raises(ValueError):  # its handle went with it
        sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=first["Messages"][0]["ReceiptHandle"])
    with pytest.raises(ValueError):  # a handle is good only on the queue that issued it
        sqs.delete_message(QueueUrl=other, ReceiptHandle=second["Messages"][0]["ReceiptHandle"])
    sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=second["Messages"][0]["ReceiptHandle"])
    assert made[0].release.acknowledged and made[0].release.left == []


def test_nothing_else_of_boto3_exists_on_an_azure_runner() -> None:
    clients = azure_runner.AzureClients(VAULT, SimpleNamespace())
    assert isinstance(clients.client("sqs"), azure_runner.ServiceBusSqs)
    assert isinstance(clients.client("secretsmanager", region_name="westus2"),
                      azure_runner.KeyVaultSecrets)
    with pytest.raises(RuntimeError, match="nothing here calls AWS"):
        clients.client("s3")


def test_the_unchanged_harness_runs_on_the_stand_ins(tmp_path: Path) -> None:
    # In a fresh interpreter, as the runner starts: the stand-ins are registered before the
    # harness is imported, so its module-level `import boto3` and external_io's lazy one both
    # bind them, and no real boto3 is ever reached.
    script = textwrap.dedent(f"""
        import sys
        sys.path.insert(0, {str(RUNNER)!r})
        import azure_runner
        azure_runner.install({VAULT!r}, object())
        import connection_spike_runner, external_io
        assert connection_spike_runner.boto3 is sys.modules["boto3"]
        assert type(connection_spike_runner.boto3.client("sqs")).__name__ == "ServiceBusSqs"
        arn = azure_runner.secret_identifier("westus2", "round5-competitor-master")
        client = external_io.secrets_manager_for_runner_operation([arn])
        assert type(client).__name__ == "KeyVaultSecrets"
        try:
            connection_spike_runner.boto3.client("ec2")
        except RuntimeError:
            print("ok")
    """)
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                            timeout=120, cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_a_spawned_fan_in_worker_finds_the_same_stand_ins(tmp_path: Path) -> None:
    # As the runner starts: `python3.12 -I azure_runner.py ...` keeps the script's own directory off
    # sys.path, and the harness's fan-in workers are spawned interpreters (multiprocessing's
    # "spawn"), which run azure_runner.py again as __mp_main__ before they import the harness. A
    # worker that missed the stand-ins would import a boto3 of its own, or fail to.
    (tmp_path / "probe.py").write_text(textwrap.dedent("""
        def boto3_in_worker(results):
            try:
                import connection_spike_runner
                results.put(type(connection_spike_runner.boto3.client("sqs")).__name__)
            except Exception as error:
                results.put(type(error).__name__)
    """))
    driver = tmp_path / "driver.py"
    driver.write_text(textwrap.dedent(f"""
        import importlib.util, multiprocessing, sys, types
        path = {str(RUNNER / "azure_runner.py")!r}
        spec = importlib.util.spec_from_file_location("azure_runner", path)
        runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(runner)
        # What `python3.12 -I azure_runner.py` leaves as __main__, short of running the harness.
        main = types.ModuleType("__main__")
        main.__file__ = path
        sys.modules["__main__"] = main
        runner.install({VAULT!r}, object())
        sys.path.append({str(tmp_path)!r})
        import probe
        context = multiprocessing.get_context("spawn")
        results = context.Queue()
        worker = context.Process(target=probe.boto3_in_worker, args=(results,))
        worker.start()
        print(results.get(timeout=60))
        worker.join(60)
    """))
    environment = {**os.environ, "ANTI_DEMO_KEY_VAULT_URI": VAULT}
    result = subprocess.run([sys.executable, "-I", str(driver)], capture_output=True, text=True,
                            timeout=180, cwd=tmp_path, env=environment)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ServiceBusSqs"


def test_the_azure_files_are_outside_the_digest_aws_seals() -> None:
    # Leaving AWS untouched means AWS's sealed harness is exactly its five files: nothing Azure
    # adds is read into the digest an AWS installation compares (`runner refresh` would follow).
    import connection_spike_runner

    assert connection_spike_runner.RUNNER_HARNESS_ASSETS == (
        "connection_spike_runner.py", "round5_fanin.py", "external_io.py",
        "run_connection_spike.sh", "requirements-round5.txt")


def test_the_entry_point_refuses_a_vault_that_is_not_key_vault(monkeypatch) -> None:
    monkeypatch.setenv("ANTI_DEMO_KEY_VAULT_URI", "https://example.test")
    assert azure_runner.main() == 2


@pytest.mark.parametrize(("current", "raised"), [
    ((1024, 524288), 65535),   # Ubuntu's systemd and Run Command: raised to AWS's 65,535
    ((1024, 4096), 4096),      # never past the hard limit
    ((100000, 524288), 100000),  # never lowered
])
def test_the_runner_raises_its_open_file_limit_as_amazon_linux_gives_aws(
        monkeypatch, current: tuple[int, int], raised: int) -> None:
    limits = {"value": current}
    monkeypatch.setattr(azure_runner.resource, "getrlimit", lambda _which: limits["value"])

    def setrlimit(_which: int, value: tuple[int, int]) -> None:
        assert value[1] == current[1]  # the hard limit is left alone
        limits["value"] = value

    monkeypatch.setattr(azure_runner.resource, "setrlimit", setrlimit)
    assert azure_runner.raise_open_files() == raised


def test_the_entry_point_refuses_to_run_without_the_pooled_port(monkeypatch) -> None:
    # Without it the competitor's 10,000 would go to the server's 5432, past PgBouncer.
    monkeypatch.setenv("ANTI_DEMO_KEY_VAULT_URI", VAULT)
    monkeypatch.delenv(azure_runner.POOLED_PORT_ENVIRONMENT, raising=False)
    assert azure_runner.main() == 2


def test_only_the_competitors_bout_clients_get_the_pooled_port(monkeypatch) -> None:
    # Ryan, 2026-10-20: the Azure shim, not a harness change. Against the real harness module.
    import asyncio
    import hashlib

    import connection_spike_runner as harness

    for name in ("_load_baseline_database", "_database_config"):
        monkeypatch.setattr(harness, name, getattr(harness, name))  # restored after the test
    host = "srv-r5.postgres.database.azure.com"
    sealed = {"rds": {"host": host, "port": 5432, "dbname": "postgres",
                      "username": "anti_demo_burst", "password": "p", "master_secret_arn": "a"},
              "lakebase": {"host": "lakebase.test", "port": 5432, "dbname": "databricks_postgres",
                           "username": "anti_demo_burst", "password": "p"}}
    monkeypatch.setattr(harness, "_read_root_json",
                        lambda path, keys: sealed["rds" if "rds" in str(path) else "lakebase"])

    def target(lane: str, credential_id: str, value: dict[str, object]) -> Any:
        return harness.Target(
            lane_id=lane, secret_arn="", endpoint_host=str(value["host"]),
            credential_host=str(value["host"]), baseline_credential_id=credential_id,
            baseline_sha256=hashlib.sha256(harness._canonical_json(value)).hexdigest())

    competitor = target("competitor", "rds", sealed["rds"])
    lakebase = target("lakebase", "lakebase", sealed["lakebase"])
    observer_loader = harness._load_observer_database
    azure_runner.point_bouts_at_the_pooler(harness, 6432)
    assert harness._load_baseline_database(competitor)["port"] == 6432
    assert harness._load_baseline_database(lakebase)["port"] == 5432
    # The observer keeps its own credential and loader: the server's 5432.
    assert harness._load_observer_database is observer_loader
    # Fails loudly if the harness changes how it reads these: a bout reads its client port
    # through the wrapped loader; setup verification reads the credential file itself.
    assert "_load_baseline_database" in harness._execute_fanin_request.__code__.co_names
    assert "_load_baseline_database" not in harness._verify_setup_transaction.__code__.co_names
    with pytest.raises(harness.RunnerContractError, match="bounded_path_unavailable_on_azure"):
        asyncio.run(harness._database_config(None, competitor))


def test_the_agents_sqs_calls_ride_the_pump_in_order() -> None:
    # The stand-in the agent calls, on the pumped queue, on a fake SDK: FIFO's order, delete as
    # settle, and an event left undeleted holding its job while another job's events flow.
    import azure_io
    from test_azure_round5_transport import Sdk

    sdk = Sdk([("lakebase-job-1", ["stage", "release"]), ("lakebase-job-2", ["preload"])])
    pumps: list[Any] = []

    def pumped(namespace: str, queue: str, credential: Any) -> Any:
        pumps.append(azure_io.PumpedControlQueue(
            azure_io.ServiceBusControlQueue(namespace, queue, credential, sdk=sdk),
            wait_seconds=0.05))
        return pumps[-1]

    sqs = azure_runner.ServiceBusSqs(object(), queue_factory=pumped)
    try:
        _ride_the_pump(sqs, sdk)
    finally:
        for pump in pumps:
            pump.close()


def _ride_the_pump(sqs: Any, sdk: Any) -> None:
    import time

    def receive() -> dict[str, Any]:
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            response = sqs.receive_message(QueueUrl=QUEUE, MaxNumberOfMessages=1,
                                           WaitTimeSeconds=1)
            if response.get("Messages"):
                (message,) = response["Messages"]
                return message
        raise AssertionError("nothing was received")

    stage = receive()
    assert stage["Body"] == "stage"
    sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=stage["ReceiptHandle"])
    release = receive()
    assert release["Body"] == "release"
    # Not deleted: at the next receive it is left, and job 1 waits behind it; job 2 flows.
    preload = receive()
    assert preload["Body"] == "preload"
    sqs.delete_message(QueueUrl=QUEUE, ReceiptHandle=preload["ReceiptHandle"])
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and sdk.receivers[-1].completed != ["preload"]:
        time.sleep(0.02)
    assert sdk.receivers[0].completed == ["stage"]
    assert sdk.receivers[-1].completed == ["preload"]


def test_a_held_clients_end_is_journaled_with_how_it_ended() -> None:
    # The harness counts a disconnect during the hold and drops its exception. Twice on
    # 2026-10-21 the Lakebase lane held 2,499 of a worker's 2,500 with no failure code.
    import inspect

    import round5_fanin

    # Fails loudly if the harness renames what this reads.
    source = inspect.getsource(round5_fanin.PostgresClient.connection_lost)
    assert "self.intentional_close" in source and "self.ready" in source
    lines: list[str] = []
    ended: list[object] = []

    class Client:
        def __init__(self, *, ready: bool, intentional: bool) -> None:
            self.ready, self.intentional_close, self.lane_id = ready, intentional, "lakebase"

        def connection_lost(self, exc: BaseException | None) -> None:
            ended.append(exc)

    fake = SimpleNamespace(PostgresClient=Client)
    azure_runner.journal_held_client_losses(fake, report=lines.append)
    reset = ConnectionResetError("Connection reset by peer")
    Client(ready=True, intentional=False).connection_lost(reset)
    Client(ready=True, intentional=False).connection_lost(None)
    # Closed by the harness itself, or before it was held: not journaled.
    Client(ready=True, intentional=True).connection_lost(None)
    Client(ready=False, intentional=False).connection_lost(None)
    assert lines == [
        "FANIN_HELD_CLIENT_LOST:lakebase:ConnectionResetError:Connection reset by peer",
        "FANIN_HELD_CLIENT_LOST:lakebase:EOF:",
    ]
    # Each still does exactly what it did.
    assert ended == [reset, None, None, None]


def test_a_control_plane_connect_that_fails_in_passing_is_tried_again() -> None:
    # 2026-10-21, 08:43 UTC: one heartbeat's connect to the coordination Lakebase failed ("SSL
    # connection has been closed unexpectedly"). The harness let it end the agent, and the App
    # re-warmed Round 5 for the restarted one.
    import inspect

    import connection_spike_runner as harness

    # Fails loudly if the harness stops reaching its control plane through its module's psycopg,
    # or connects that way for anything else.
    source = inspect.getsource(harness)
    assert source.count("psycopg.connect(") == len(azure_runner.CONTROL_PLANE_CALLS)
    for name in azure_runner.CONTROL_PLANE_CALLS:
        assert "psycopg.connect(control_dsn" in inspect.getsource(getattr(harness, name)), name

    class OperationalError(Exception):
        pass

    failures = [OperationalError("SSL connection has been closed unexpectedly")]
    connected: list[tuple[Any, ...]] = []

    def connect(*args: Any, **kwargs: Any) -> str:
        connected.append((args, kwargs))
        if failures:
            raise failures.pop(0)
        return "connection"

    errors = SimpleNamespace(InsufficientPrivilege=type("InsufficientPrivilege", (Exception,), {}))
    real = SimpleNamespace(__name__="psycopg", connect=connect, OperationalError=OperationalError,
                           errors=errors)
    fake = SimpleNamespace(psycopg=real)
    lines: list[str] = []
    slept: list[float] = []
    azure_runner.retry_control_plane_connects(fake, sleep=slept.append, report=lines.append)
    assert fake.psycopg.connect("dsn", autocommit=True) == "connection"
    assert connected == [(("dsn",), {"autocommit": True})] * 2 and slept == [0.5]
    assert lines == ["CONTROL_DB_CONNECT_RETRY:1:SSL connection has been closed unexpectedly"]
    # The rest is psycopg's own, and so is what the harness catches.
    assert fake.psycopg.errors is errors and fake.psycopg.OperationalError is OperationalError
    # Three failures in a row end the call with the last, as before.
    failures.extend(OperationalError(f"down {n}") for n in range(3))
    with pytest.raises(OperationalError, match="down 2"):
        fake.psycopg.connect("dsn")
    assert slept == [0.5, 0.5, 1.0]
    # Anything but a failed connect is never tried again.
    failures.append(ValueError("not a connect failure"))
    with pytest.raises(ValueError):
        fake.psycopg.connect("dsn")
    assert slept == [0.5, 0.5, 1.0]


def test_a_slow_control_plane_call_is_journaled_by_name() -> None:
    # azure_io journals a slow Service Bus call; this is the coordination database's side. The
    # first Azure bout's Lakebase runner acted on its RELEASE 22 s late, saying nothing.
    import inspect

    import connection_spike_runner as harness

    # Fails loudly if the harness renames one or stops calling it from its agent by name.
    agent = inspect.getsource(harness._resident_agent)
    for name in azure_runner.CONTROL_PLANE_CALLS:
        assert callable(getattr(harness, name)) and name in agent, name
    now, lines, written = [0.0], [], []

    def write(*args: Any, **kwargs: Any) -> str:
        now[0] += 2.5
        written.append((args, kwargs))
        return "written"

    def refuse(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("refused")

    fake = SimpleNamespace(_write_resident_event=write,
                           _last_resident_event_sequence=lambda _dsn: 7,
                           _resident_event_disposition=refuse)
    azure_runner.time_the_control_plane(fake, clock=lambda: now[0], report=lines.append)
    # Each does exactly what it did: same arguments, same answer, same error.
    assert fake._write_resident_event("dsn", sequence=1) == "written"
    assert written == [(("dsn",), {"sequence": 1})]
    assert fake._last_resident_event_sequence("dsn") == 7
    with pytest.raises(RuntimeError, match="refused"):
        fake._resident_event_disposition("dsn")
    assert lines == ["CONTROL_DB_SLOW:_write_resident_event:2.500"]
