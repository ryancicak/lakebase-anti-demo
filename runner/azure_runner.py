"""Round 5's runner on Azure: the AWS harness, byte for byte, on Service Bus and Key Vault.

The harness (`connection_spike_runner.py` and the files `RUNNER_HARNESS_ASSETS` names) reaches AWS
through boto3 alone, and only for two things: its control queue (SQS ``ReceiveMessage`` and
``DeleteMessage``) and its secrets (Secrets Manager ``GetSecretValue``). This entry point registers
a stand-in ``boto3`` (and the ``botocore.config`` it is handed) before the harness is imported, so
the same calls land on the lane's Service Bus session queue and the installation's Key Vault, as the
VM's managed identity (`azure_io`). Nothing else of boto3 exists here: a call the stand-in doesn't
know fails at once rather than reaching for AWS.

The harness files are left exactly as AWS seals them (Ryan, 2026-10-20: "leave AWS untouched"), so
no AWS installation's sealed digest moves. Two consequences of riding its contract unchanged:

- **A secret is named by an ARN-shaped identifier**, as the harness checks:
  ``arn:aws:secretsmanager:<region>:000000000000:secret:<name>``. The name is the Key Vault
  secret's; the region and account are placeholders that only have to agree across one request.
- **The queue is named by its address**, ``sb://<namespace>.servicebus.windows.net/<queue>``, in
  the argument the harness passes to ``ReceiveMessage`` as ``QueueUrl``.

    python3 azure_runner.py <the harness's own arguments>

with ``ANTI_DEMO_KEY_VAULT_URI`` set to the installation's vault (``https://<name>.vault.azure.net``).

The harness runs its fan-in workers as fresh interpreters (multiprocessing's "spawn"). Each one runs
this file again, as ``__mp_main__``, before it imports the harness, and installs the same stand-ins
then, from the same environment.

A resident agent attests what it loaded twice: the harness writes its own attestation, and this
writes one beside it with the digest of the Azure files (`AZURE_RUNNER_ASSETS`), so the installer
proves both halves of the running process, as SSM's install proves the harness on AWS.

**The pooled port** (Ryan, 2026-10-20: "Azure shim"). AWS's pooled path is a host of its own on
5432 (the bout's Proxy endpoint); Azure's is the server's own host on 6432 (its PgBouncer). The
harness takes a bout's client port from the lane's sealed credential, the one its setup
verification and its observer use too, so on its own it would send Azure's 10,000 clients straight
to the server. `point_bouts_at_the_pooler` gives the competitor lane's bout clients the pooled
port, and only them: the observer and setup verification stay on the server's 5432. The bounded
path, which builds a direct address from that same credential, is refused here; the Azure App
never sends it.
"""

from __future__ import annotations

import functools
import hashlib
import json
import os
import re
import resource
import secrets
import sys
import time
import types
import urllib.error
from collections.abc import Callable
from pathlib import Path
from typing import Any

# run_connection_spike_azure.sh runs this with -I, which keeps the script's own directory off
# sys.path. azure_io and the harness are installed beside it, and the harness resolves its own
# siblings the same way.
_RUNNER_DIRECTORY = str(Path(__file__).resolve().parent)
if _RUNNER_DIRECTORY not in sys.path:
    sys.path.insert(0, _RUNNER_DIRECTORY)

from azure_io import (  # noqa: E402
    SLOW_CALL_SECONDS,
    ControlMessage,
    ImdsCredential,
    key_vault_secret,
    pumped_control_queue,
    report_slow,
)

#: The identifier the harness's secret checks accept, carrying a Key Vault secret's name.
SECRET_ID = re.compile(
    r"^arn:aws:secretsmanager:(?P<region>[a-z0-9-]+):000000000000:secret:(?P<name>[A-Za-z0-9-]{1,127})$"
)
QUEUE_ADDRESS = re.compile(
    r"^sb://(?P<namespace>[A-Za-z0-9-]{6,50})\.servicebus\.windows\.net/(?P<queue>[A-Za-z0-9._-]{1,260})$"
)
#: The AWS edition's control queues' visibility timeout, which the agent also asks for.
VISIBILITY_TIMEOUT_SECONDS = 720
VAULT_URI = re.compile(r"^https://[A-Za-z0-9-]{3,24}\.vault\.azure\.net/?$")
#: The Azure half of the runner, digested as the harness digests its own files.
AZURE_RUNNER_ASSETS = (
    "azure_runner.py",
    "azure_io.py",
    "run_connection_spike_azure.sh",
    "requirements-round5-azure.txt",
)
ATTESTATION_DIRECTORY = Path("/run")
_LANE_ID = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
#: The open-file limit this raises its process to before the harness loads. A lane holds 10,000
#: sockets, and the harness's capacity preflight refuses a limit below its projection
#: (`file_descriptor_projection`). AWS's runners get 65,535 from Amazon Linux's systemd; Ubuntu's
#: gives a service, and the scripts Run Command starts, 1,024. Spawned workers inherit it.
OPEN_FILES = 65_535
#: The competitor lane's pooled port, from the unit (server/azure_round5_runner.py).
POOLED_PORT_ENVIRONMENT = "ANTI_DEMO_POOLED_PORT"
#: The credential the harness keeps for the competitor lane. It knows two, "rds" and "aurora",
#: and Azure's Flexible Server rides as "rds".
POOLED_CREDENTIAL_IDS = frozenset({"rds"})


def point_bouts_at_the_pooler(harness: Any, pooled_port: int) -> None:
    """Give the competitor lane's bout clients `pooled_port`; refuse the bounded path."""

    if isinstance(pooled_port, bool) or not 1 <= pooled_port <= 65535:
        raise ValueError("not a port")
    load = harness._load_baseline_database

    def load_pooled(target: Any) -> dict[str, object]:
        value = load(target)
        if (target.baseline_credential_id or target.lane_id) in POOLED_CREDENTIAL_IDS:
            return {**value, "port": pooled_port}
        return value

    async def bounded_refused(_client: Any, _target: Any) -> Any:
        raise harness.RunnerContractError("bounded_path_unavailable_on_azure")

    harness._load_baseline_database = load_pooled
    harness._database_config = bounded_refused


def journal_held_client_losses(fanin: Any, *,
                               report: Callable[[str], None] = report_slow) -> None:
    """Journal how a held client's connection ended, which the harness counts and lets go.

    The harness records a disconnect during the hold by its count alone: its
    `PostgresClient.connection_lost` drops the exception. Twice on 2026-10-21 the Lakebase lane
    held 2,499 of a worker's 2,500, with no failure code. This journals each such end as the
    peer's EOF, a reset, or a TLS error, which says whether a pooler, a proxy or the network
    closed it. The harness file stays as AWS seals it: its class's method is wrapped here, in this
    process, and does exactly what it did.
    """

    client = fanin.PostgresClient
    original = client.connection_lost

    @functools.wraps(original)
    def connection_lost(self: Any, exc: BaseException | None) -> None:
        if not self.intentional_close and self.ready:
            reason = "EOF" if exc is None else type(exc).__name__
            report(f"FANIN_HELD_CLIENT_LOST:{self.lane_id}:{reason}:{str(exc or '')[:160]}")
        original(self, exc)

    client.connection_lost = connection_lost


#: The harness's control-plane database calls, each a fresh connection to the coordination
#: Lakebase from a thread of the resident agent's loop.
CONTROL_PLANE_CALLS = ("_write_resident_event", "_last_resident_event_sequence",
                       "_resident_event_disposition")


def time_the_control_plane(harness: Any, *, clock: Callable[[], float] = time.monotonic,
                           report: Callable[[str], None] = report_slow) -> None:
    """Journal any of the harness's control-plane database calls that takes `SLOW_CALL_SECONDS`.

    `azure_io` journals a slow Service Bus call the same way, so a wait between a control event
    and the agent acting on it says which side it was on. The harness file stays as AWS seals it:
    its functions are wrapped here, in this process, and do exactly what they did.
    """

    for name in CONTROL_PLANE_CALLS:
        call = getattr(harness, name)

        @functools.wraps(call)
        def timed(*args: Any, _call: Callable[..., Any] = call, _name: str = name,
                  **kwargs: Any) -> Any:
            started = clock()
            try:
                return _call(*args, **kwargs)
            finally:
                seconds = clock() - started
                if seconds >= SLOW_CALL_SECONDS:
                    report(f"CONTROL_DB_SLOW:{_name}:{seconds:.3f}")

        setattr(harness, name, timed)


#: How many times the harness's control-plane connect is tried, and the pause before each retry
#: (times the try's number). The resident agent opens a fresh connection to the coordination
#: Lakebase for every event it writes, a heartbeat every 2 s, and the harness lets a heartbeat's
#: failure end the agent (it catches only a revoked attempt). One connect failed in passing on
#: 2026-10-21, 08:43 UTC ("SSL connection has been closed unexpectedly"): the agent exited,
#: systemd started another, and the App re-warmed Round 5 for the new identity.
CONTROL_CONNECT_ATTEMPTS = 3
CONTROL_CONNECT_RETRY_SECONDS = 0.5


class _RetryingPsycopg(types.ModuleType):
    """The harness's `psycopg`, with a `connect` that tries again; the rest is psycopg's own."""

    def __init__(self, real: Any, connect: Callable[..., Any]) -> None:
        super().__init__(real.__name__)
        self._real = real
        self.connect = connect

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def retry_control_plane_connects(harness: Any, *, attempts: int = CONTROL_CONNECT_ATTEMPTS,
                                 pause: float = CONTROL_CONNECT_RETRY_SECONDS,
                                 sleep: Callable[[float], None] = time.sleep,
                                 report: Callable[[str], None] = report_slow) -> None:
    """Try the harness's psycopg connects again when they fail, and only its connects.

    Its only ones are its control-plane calls (`CONTROL_PLANE_CALLS`). A connect that failed sent
    nothing, so trying it again is safe, where repeating a whole write is not: its event ID
    carries the moment it was made. Nothing after a connection opens is retried. The harness
    file stays as AWS seals it: its module's `psycopg` is swapped here, in this process.
    """

    real = harness.psycopg

    def connect(*args: Any, **kwargs: Any) -> Any:
        for attempt in range(1, attempts):
            try:
                return real.connect(*args, **kwargs)
            except real.OperationalError as error:
                report(f"CONTROL_DB_CONNECT_RETRY:{attempt}:{str(error)[:160]}")
                sleep(pause * attempt)
        return real.connect(*args, **kwargs)

    harness.psycopg = _RetryingPsycopg(real, connect)


def pooled_port() -> int | None:
    value = os.environ.get(POOLED_PORT_ENVIRONMENT, "")
    return int(value) if value.isdigit() and 1 <= int(value) <= 65535 else None


def raise_open_files(limit: int = OPEN_FILES) -> int:
    """Raise the soft open-file limit to `limit`, within the hard one; never lower it."""

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = limit if hard == resource.RLIM_INFINITY else min(limit, hard)
    if soft != resource.RLIM_INFINITY and soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    return resource.getrlimit(resource.RLIMIT_NOFILE)[0]


def azure_runner_sha256(root: Path | None = None) -> str:
    digest = hashlib.sha256()
    for name in AZURE_RUNNER_ASSETS:
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(((root or Path(_RUNNER_DIRECTORY)) / name).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def azure_attestation_path(lane_id: str) -> Path:
    if _LANE_ID.fullmatch(lane_id) is None:
        raise ValueError("not a lane ID")
    return ATTESTATION_DIRECTORY / f"lakebase-anti-demo-round5-{lane_id}.azure-attestation.json"


def attest(lane_id: str) -> None:
    """Record this process and the Azure files it loaded, before the harness attests its own."""

    path = azure_attestation_path(lane_id)
    temporary = path.with_suffix(f".tmp-{os.getpid()}")
    temporary.write_text(json.dumps({"pid": os.getpid(),
                                     "azure_runner_sha256": azure_runner_sha256()},
                                    sort_keys=True, separators=(",", ":")), encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


def secret_identifier(region: str, name: str) -> str:
    """The ARN-shaped identifier the harness is handed for Key Vault secret `name`."""

    identifier = f"arn:aws:secretsmanager:{region}:000000000000:secret:{name}"
    if SECRET_ID.fullmatch(identifier) is None:
        raise ValueError("not a Key Vault secret name, or not a region")
    return identifier


class ResourceNotFoundException(Exception):  # noqa: N818 - Secrets Manager's own name
    """A secret with no value yet, which the harness waits out (it is written by the installer)."""


class KeyVaultSecrets:
    """Secrets Manager's ``GetSecretValue``, as the harness calls it, on Key Vault.

    A secret the VM's identity may not read yet (403) is waited out like one not written yet. Its
    role assignment is made by the same apply that makes the VM, and reaches Key Vault's data
    plane minutes later. Raised, the refusal would end the agent and systemd would restart it every
    2 s, and the install's attestation check would race those restarts. It is noted once on
    stderr (the journal), and the install's transport proof is what catches a runner that never
    gets the role.
    """

    def __init__(self, vault_uri: str, credential: Any, *,
                 read: Callable[..., str | None] = key_vault_secret) -> None:
        self._vault_uri = vault_uri
        self._credential = credential
        self._read = read
        self._refusal_noted = False
        self.exceptions = types.SimpleNamespace(ResourceNotFoundException=ResourceNotFoundException)

    def get_secret_value(self, *, SecretId: str, VersionStage: str = "AWSCURRENT",  # noqa: N803
                         **_ignored: Any) -> dict[str, str]:
        match = SECRET_ID.fullmatch(SecretId)
        if match is None or VersionStage != "AWSCURRENT":
            raise ValueError("not a Key Vault secret identifier this runner was given")
        try:
            value = self._read(self._vault_uri, match.group("name"), self._credential)
        except urllib.error.HTTPError as error:
            if error.code != 403:
                raise
            if not self._refusal_noted:
                self._refusal_noted = True
                print("KEY_VAULT_REFUSED_WAITING: the VM's identity may not read the vault yet",
                      file=sys.stderr, flush=True)
            raise ResourceNotFoundException(match.group("name")) from None
        if not value:
            raise ResourceNotFoundException(match.group("name"))
        return {"SecretString": value}


class ServiceBusSqs:
    """SQS's ``ReceiveMessage`` and ``DeleteMessage``, as the resident agent calls them.

    One message at a time, as the agent asks. Its receipt handle names the message held here
    until the agent deletes it, which completes it on Service Bus. A session's lock is renewed
    while the agent works (`ServiceBusControlQueue`), because the agent deletes only once it is done
    and SQS gave it a 720 s visibility timeout where a Service Bus lock lasts 300 s.

    The agent deletes each message before it next receives, or not at all: a message still held at
    the next receive is one it left (connection_spike_runner._resident_agent leaves an event it
    could not finish, for SQS to hand out again). It is handed back to the queue as left, with the
    visibility timeout it was received under.
    """

    def __init__(self, credential: Any, *,
                 queue_factory: Callable[[str, str, Any], Any] = pumped_control_queue) -> None:
        self._credential = credential
        self._factory = queue_factory
        self._queues: dict[str, Any] = {}
        self._held: dict[str, tuple[str, ControlMessage, float]] = {}

    def _queue(self, address: str) -> Any:
        if address not in self._queues:
            match = QUEUE_ADDRESS.fullmatch(address)
            if match is None:
                raise ValueError("not a Service Bus queue address")
            self._queues[address] = self._factory(match.group("namespace"), match.group("queue"),
                                                  self._credential)
        return self._queues[address]

    def receive_message(self, *, QueueUrl: str, MaxNumberOfMessages: int = 1,  # noqa: N803
                        WaitTimeSeconds: int = 1,  # noqa: N803
                        VisibilityTimeout: int = VISIBILITY_TIMEOUT_SECONDS,  # noqa: N803
                        **_ignored: Any) -> dict[str, Any]:
        if MaxNumberOfMessages != 1:
            raise ValueError("the resident agent takes one control message at a time")
        queue = self._queue(QueueUrl)
        for handle in [handle for handle, (address, _, _) in self._held.items()
                       if address == QueueUrl]:
            _address, left, visibility_timeout = self._held.pop(handle)
            left.leave(visibility_timeout)
        received = queue.receive(float(max(WaitTimeSeconds, 1)))
        messages = []
        for message in received:
            handle = secrets.token_hex(16)
            self._held[handle] = (QueueUrl, message, float(VisibilityTimeout))
            messages.append({"Body": message.body, "ReceiptHandle": handle})
        return {"Messages": messages} if messages else {}

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str, **_ignored: Any) -> dict:  # noqa: N803
        held = self._held.get(ReceiptHandle)
        if held is None or held[0] != QueueUrl:
            raise ValueError("unknown receipt handle")
        del self._held[ReceiptHandle]
        held[1].acknowledge()
        return {}


class AzureClients:
    """The two boto3 clients the harness makes, and nothing else of boto3."""

    def __init__(self, vault_uri: str, credential: Any) -> None:
        self._made = {"sqs": lambda: ServiceBusSqs(credential),
                      "secretsmanager": lambda: KeyVaultSecrets(vault_uri, credential)}

    def client(self, service_name: str, *_args: Any, **_kwargs: Any) -> Any:
        if service_name not in self._made:
            raise RuntimeError(f"the Azure runner has no {service_name}: nothing here calls AWS")
        return self._made[service_name]()


def install(vault_uri: str, credential: Any) -> None:
    """Register the stand-in ``boto3`` and ``botocore.config`` before the harness is imported."""

    clients = AzureClients(vault_uri, credential)
    boto3 = types.ModuleType("boto3")
    boto3.client = clients.client  # type: ignore[attr-defined]
    boto3.Session = lambda **_kwargs: types.SimpleNamespace(client=clients.client)  # type: ignore[attr-defined]
    botocore = types.ModuleType("botocore")
    config = types.ModuleType("botocore.config")
    config.Config = lambda **kwargs: types.SimpleNamespace(**kwargs)  # type: ignore[attr-defined]
    botocore.config = config  # type: ignore[attr-defined]
    sys.modules.update({"boto3": boto3, "botocore": botocore, "botocore.config": config})


def main() -> int:
    vault_uri = os.environ.get("ANTI_DEMO_KEY_VAULT_URI", "")
    if VAULT_URI.fullmatch(vault_uri) is None:
        print("ANTI_DEMO_KEY_VAULT_URI must be the installation's Key Vault", file=sys.stderr)
        return 2
    port = pooled_port()
    if port is None:
        print(f"{POOLED_PORT_ENVIRONMENT} must be the competitor's pooled port", file=sys.stderr)
        return 2
    raise_open_files()
    if len(sys.argv) == 6 and sys.argv[1] == "--resident-agent":
        attest(sys.argv[2])
    install(vault_uri, ImdsCredential())
    import connection_spike_runner

    # Before the harness runs anything: its fan-in reads the client port through this, in this
    # process. Its workers are handed their addresses, so they need no such step.
    point_bouts_at_the_pooler(connection_spike_runner, port)
    time_the_control_plane(connection_spike_runner)
    retry_control_plane_connects(connection_spike_runner)
    import round5_fanin

    journal_held_client_losses(round5_fanin)
    return connection_spike_runner.main()


if __name__ == "__main__":
    raise SystemExit(main())
if __name__ == "__mp_main__":
    # A fan-in worker, before it imports the harness. Its parent passed main()'s check on the same
    # environment, and a worker reads no secret: what it must not do is find a boto3 of its own.
    install(os.environ.get("ANTI_DEMO_KEY_VAULT_URI", ""), ImdsCredential())
    # The held clients live in the workers, so their ends are journaled here too.
    import round5_fanin

    journal_held_client_losses(round5_fanin)
