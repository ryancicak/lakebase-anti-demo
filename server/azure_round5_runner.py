"""Round 5's runners on Azure: the harness installed over Run Command, as SSM installs it on AWS.

`lifecycle._configure_round5_runner` installs an AWS runner in nine SSM commands. A Run Command
call takes about 32 s however small its script, so the same work is done here in the
fewest calls that still keep each step's failure its own, on both VMs at once:

1. **Prepare.** Refuse while a job is active; stop the old resident agent, keeping its PID and
   process boot ID so the new one is provably new; Python's venv package; the archive's first
   chunk.
2. **Chunks,** as many as the archive needs between its first and last (none at today's size).
3. **Install.** The last chunk; unpack; a venv from `requirements-round5-azure.txt` (three tries);
   every file moved in at once; the trust bundle; every file's checksum.
4. **Start.** The systemd unit; wait for both attestations; check them.

**What is installed:** AWS's sealed harness byte for byte (`RUNNER_ASSETS`, its wrapper too, which
the harness digests though the unit runs the Azure one); the Azure entry point, its I/O, wrapper and
requirements (`AZURE_RUNNER_ASSETS`, digested the same way); and the Azure Postgres roots. The
trust bundle is the VM's own CA bundle and those roots: Lakebase's certificate chains to a public
root, and the Flexible Server's to DigiCert Global Root G2 or Microsoft RSA Root CA 2017. Nothing
is downloaded, so nothing outside the archive moves the bundle.

**Every script is POSIX sh,** so it runs the same under sh or bash, and prints its exit status last
(`ANTI_DEMO_EXIT=`), where Run Command's 4,096-byte output limit keeps it. A failure names its
stage, as the AWS install does (`ANTI_DEMO_STAGE=`). Package and pip output goes to a log on the VM,
so it can't push the checksums out of that limit. Run Command can't be cancelled, so each step that
waits on something outside the VM is bounded on the VM.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import re
import tarfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .azure_pgbouncer import POOLED_PORT
from .connection_spike_live import (
    RUNNER_ASSETS,
    RUNNER_PATH,
    TRUST_BUNDLE_PATH,
    runner_asset_sha256s,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNNER_ROOT = PROJECT_ROOT / "runner"
INSTALL_ROOT = str(Path(RUNNER_PATH).parent)
AZURE_RUNNER_PATH = f"{INSTALL_ROOT}/run_connection_spike_azure.sh"
#: The Azure half of the runner (runner/azure_runner.py's own list, which its attestation digests).
AZURE_RUNNER_ASSETS = (
    "azure_runner.py",
    "azure_io.py",
    "run_connection_spike_azure.sh",
    "requirements-round5-azure.txt",
)
ROOTS_NAME = "azure-postgres-roots.pem"
ROOTS_PATH = PROJECT_ROOT / "server" / "certs" / ROOTS_NAME
#: Ubuntu's CA bundle, the runner image's (infra/azure/round5_runners.tf: Ubuntu 24.04 LTS).
SYSTEM_CA_BUNDLE = "/etc/ssl/certs/ca-certificates.crt"
INSTALL_LOG = "/var/log/lakebase-anti-demo-round5-install.log"
JOB_ROOT = "/var/lib/lakebase-anti-demo/round5-jobs"

#: One chunk of the archive per Run Command. SSM's chunks are 60,000 characters, under its 100 KB
#: document limit; Run Command documents no script limit, so these stay smaller still. Three
#: chunks carry today's archive, and the ceiling bounds the install however the runner grows.
RUN_COMMAND_CHUNK_CHARS = 48_000
RUN_COMMAND_MAX_CHUNKS = 16
#: Each call's bound, from the laptop. A call is never shorter than about 32 s.
PREPARE_BOUND_SECONDS = 900.0  # a VM's first boot can hold apt for minutes
CHUNK_BOUND_SECONDS = 240.0
INSTALL_BOUND_SECONDS = 1200.0  # three bounded pip attempts
START_BOUND_SECONDS = 300.0
#: The same bounds on the VM, inside the scripts: Run Command can't be cancelled.
CLOUD_INIT_WAIT_SECONDS = 300
APT_SECONDS = 420
PIP_ATTEMPT_SECONDS = 300
ATTESTATION_WAIT_SECONDS = 60

_LANE_ID = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_VM_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
#: runner/azure_runner.py's SECRET_ID, QUEUE_ADDRESS and VAULT_URI: what its stand-ins accept.
SECRET_ID = re.compile(
    r"^arn:aws:secretsmanager:(?P<region>[a-z0-9-]+):000000000000:secret:(?P<name>[A-Za-z0-9-]{1,127})$"
)
QUEUE_ADDRESS = re.compile(
    r"^sb://(?P<namespace>[A-Za-z0-9-]{6,50})\.servicebus\.windows\.net/(?P<queue>[A-Za-z0-9._-]{1,260})$"
)
VAULT_URI = re.compile(r"^https://[A-Za-z0-9-]{3,24}\.vault\.azure\.net/?$")


#: Terraform's runners and queues, by the lane each serves (infra/azure/round5_runners.tf and
#: round5_control.tf), and the Key Vault secret holding each lane's control DSN.
LANE_RUNNERS = {"lakebase": "runner-0", "competitor": "runner-1"}
LANE_QUEUES = {"lakebase": "r5-runner-0", "competitor": "r5-runner-1"}
CONTROL_SECRET_NAMES = {"lakebase": "round5-lakebase-control",
                        "competitor": "round5-competitor-control"}


class RunnerInstallError(RuntimeError):
    """A runner's install or restart failed. The message names the VM and stage, nothing else."""


def secret_identifier(region: str, name: str) -> str:
    """The ARN-shaped identifier the harness is handed for Key Vault secret `name`."""

    identifier = f"arn:aws:secretsmanager:{region}:000000000000:secret:{name}"
    if SECRET_ID.fullmatch(identifier) is None:
        raise ValueError("not a Key Vault secret name, or not a region")
    return identifier


def queue_address(namespace: str, queue: str) -> str:
    """The address the resident agent is handed as its queue URL."""

    address = f"sb://{namespace}.servicebus.windows.net/{queue}"
    if QUEUE_ADDRESS.fullmatch(address) is None:
        raise ValueError("not a Service Bus namespace and queue")
    return address


@dataclass(frozen=True)
class RunnerTarget:
    """One runner VM and the lane its resident agent serves."""

    vm_name: str
    lane_id: str
    queue_address: str
    control_secret: str
    vault_uri: str

    def __post_init__(self) -> None:
        if _VM_NAME.fullmatch(self.vm_name) is None:
            raise ValueError("not a VM name")
        if _LANE_ID.fullmatch(self.lane_id) is None:
            raise ValueError("not a lane ID")
        if QUEUE_ADDRESS.fullmatch(self.queue_address) is None:
            raise ValueError("not a Service Bus queue address")
        if SECRET_ID.fullmatch(self.control_secret) is None:
            raise ValueError("not a Key Vault secret identifier")
        if VAULT_URI.fullmatch(self.vault_uri) is None:
            raise ValueError("not a Key Vault URI")

    @property
    def unit_name(self) -> str:
        return f"lakebase-anti-demo-round5-{self.lane_id}.service"

    @property
    def attestation_path(self) -> str:
        return f"/run/lakebase-anti-demo-round5-{self.lane_id}.attestation.json"

    @property
    def azure_attestation_path(self) -> str:
        return f"/run/lakebase-anti-demo-round5-{self.lane_id}.azure-attestation.json"

    @property
    def active_registry(self) -> str:
        return f"{JOB_ROOT}/resident-{self.lane_id}-active.json"


def runner_targets(outputs: Mapping[str, Any], *, region: str) -> list[RunnerTarget]:
    """Both lanes' runners, from Terraform's outputs (`round5_runners`, `service_bus`,
    `key_vault`). `region` only has to be the same in every identifier one request carries."""

    runners = outputs.get("round5_runners") or {}
    service_bus = outputs.get("service_bus") or {}
    queues = service_bus.get("queues") or {}
    vault = (outputs.get("key_vault") or {}).get("uri", "")
    if set(LANE_RUNNERS.values()) - set(runners) or set(LANE_QUEUES.values()) - set(queues):
        raise RunnerInstallError(
            "Terraform's outputs name no Round 5 runners: is round5_runners on?")
    return [
        RunnerTarget(
            vm_name=str(runners[LANE_RUNNERS[lane]]["name"]),
            lane_id=lane,
            queue_address=queue_address(str(service_bus.get("namespace_name", "")),
                                        str(queues[LANE_QUEUES[lane]])),
            control_secret=secret_identifier(region, CONTROL_SECRET_NAMES[lane]),
            vault_uri=str(vault),
        )
        for lane in ("lakebase", "competitor")
    ]


@dataclass(frozen=True)
class RunnerInstall:
    """What one runner proved: its trust bundle, and the resident agent now running."""

    vm_name: str
    trust_bundle_sha256: str
    pid: int
    process_boot_id: str
    trust_bundle_kept: bool


def installed_files() -> tuple[str, ...]:
    return (*RUNNER_ASSETS, *AZURE_RUNNER_ASSETS, ROOTS_NAME)


def _source(name: str) -> Path:
    return ROOTS_PATH if name == ROOTS_NAME else RUNNER_ROOT / name


def azure_asset_sha256s() -> dict[str, str]:
    return {name: hashlib.sha256(_source(name).read_bytes()).hexdigest()
            for name in AZURE_RUNNER_ASSETS}


def azure_runner_sha256() -> str:
    """The Azure half's digest, as runner/azure_runner.py attests what it loaded."""

    digest = hashlib.sha256()
    for name in AZURE_RUNNER_ASSETS:
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(_source(name).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def runner_archive() -> str:
    """Every installed file, as one base64 tar.gz. Deterministic: a retry sends the same bytes."""

    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name in installed_files():
            data = _source(name).read_bytes()
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.mtime = len(data), 0o644, 0
            archive.addfile(info, io.BytesIO(data))
    return base64.b64encode(gzip.compress(stream.getvalue(), mtime=0)).decode("ascii")


def archive_chunks(archive: str) -> list[str]:
    chunks = [archive[index:index + RUN_COMMAND_CHUNK_CHARS]
              for index in range(0, len(archive), RUN_COMMAND_CHUNK_CHARS)]
    if not chunks or len(chunks) > RUN_COMMAND_MAX_CHUNKS:
        raise RunnerInstallError("the Round 5 runner archive exceeds the bounded install payload")
    return chunks


def script(commands: Iterable[str]) -> list[str]:
    """A step's script: POSIX sh, its exit status printed last, its failing stage named."""

    return [
        "#!/bin/sh",
        "set -eu",
        "stage=bootstrap",
        ('trap \'status=$?; if [ "$status" -ne 0 ]; then echo "ANTI_DEMO_STAGE=$stage" >&2; fi; '
         'echo "ANTI_DEMO_EXIT=$status"\' EXIT'),
        *commands,
    ]


def _values(stdout: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in stdout.splitlines() if "=" in line)


#: `ArmClient.run_shell_script`: (vm, script, *, bound_seconds) -> RunCommandOutput.
RunScript = Callable[..., Any]


def _run(run: RunScript, vm_name: str, commands: Iterable[str], bound_seconds: float) -> str:
    output = run(vm_name, script(commands), bound_seconds=bound_seconds)
    status = _values(output.stdout).get("ANTI_DEMO_EXIT")
    if status == "0":
        return output.stdout
    stages = re.findall(r"ANTI_DEMO_STAGE=([a-z_]{1,32})", output.stderr)
    stage = f" at stage {stages[-1]}" if stages else ""
    if status is None:
        raise RunnerInstallError(f"Round 5 runner {vm_name}'s command returned no exit status")
    raise RunnerInstallError(f"Round 5 runner {vm_name}'s configuration command failed{stage}")


def _prepare_commands(target: RunnerTarget, first_chunk: str, chunk_count: int) -> list[str]:
    unit, attestation = target.unit_name, target.attestation_path
    archive_path = f"{INSTALL_ROOT}/.runner-assets.tar.gz.b64"
    return [
        "stage=idle",
        f"test ! -e {target.active_registry}",
        "stage=stop",
        f"if systemctl cat {unit} >/dev/null 2>&1; then",
        f'  echo "OLD_PID=$(systemctl show {unit} -p MainPID --value)"',
        f"  if [ -e {attestation} ]; then",
        ("    python3 -c \"import json; "
         f"print('OLD_PROCESS_BOOT=' + str(json.load(open('{attestation}'))"
         ".get('runner_process_boot_id', '')))\""),
        "  fi",
        f"  systemctl stop {unit}",
        f"  if systemctl is-active --quiet {unit}; then exit 1; fi",
        "else",
        "  echo OLD_PID=0",
        "fi",
        f"rm -f {attestation} {target.azure_attestation_path}",
        "stage=packages",
        # A first boot's cloud-init and apt timers hold dpkg for a while; wait for them, bounded.
        f"timeout {CLOUD_INIT_WAIT_SECONDS} cloud-init status --wait >/dev/null 2>&1 || true",
        "if ! python3.12 -c 'import ensurepip' >/dev/null 2>&1; then",
        (f"  DEBIAN_FRONTEND=noninteractive timeout {APT_SECONDS} apt-get "
         f"-o DPkg::Lock::Timeout={APT_SECONDS} -qq update >>{INSTALL_LOG} 2>&1"),
        (f"  DEBIAN_FRONTEND=noninteractive timeout {APT_SECONDS} apt-get "
         f"-o DPkg::Lock::Timeout={APT_SECONDS} -y -qq install python3.12-venv "
         f">>{INSTALL_LOG} 2>&1"),
        "fi",
        "python3.12 -c 'import ensurepip'",
        "stage=updates",
        # Ubuntu's daily unattended upgrade restarts a service holding an updated library, and
        # restarted the competitor's resident agent mid-bar on 2026-10-21 (Round 5 left READY
        # while it re-established). Amazon Linux, AWS's runners, upgrades nothing by itself;
        # these don't either. A running upgrade is left to finish; there is no next one.
        "systemctl disable --now apt-daily-upgrade.timer apt-daily.timer >/dev/null 2>&1 || true",
        "stage=directory",
        f"install -d -m 0755 {INSTALL_ROOT}",
        f"install -m 0600 /dev/null {archive_path}",
        f"printf '%s' '{first_chunk}' >> {archive_path}",
        f"echo ARCHIVE_CHUNK=1/{chunk_count}",
    ]


def _chunk_commands(chunk: str, index: int, chunk_count: int) -> list[str]:
    archive_path = f"{INSTALL_ROOT}/.runner-assets.tar.gz.b64"
    return [
        "stage=archive",
        f"test -f {archive_path}",
        f"printf '%s' '{chunk}' >> {archive_path}",
        f"echo ARCHIVE_CHUNK={index + 1}/{chunk_count}",
    ]


def _trust_commands(keep_trust_bundle_sha256: str | None) -> list[str]:
    bundle = str(TRUST_BUNDLE_PATH)
    rebuild = [
        f"test -s {SYSTEM_CA_BUNDLE}",
        f"grep -q -- '-----BEGIN CERTIFICATE-----' {INSTALL_ROOT}/{ROOTS_NAME}",
        f"cat {SYSTEM_CA_BUNDLE} {INSTALL_ROOT}/{ROOTS_NAME} > {bundle}.tmp",
        f"chmod 0644 {bundle}.tmp",
        f"mv {bundle}.tmp {bundle}",
    ]
    if keep_trust_bundle_sha256 is None:
        return ["stage=trust", *rebuild, "echo TRUST_BUNDLE=rebuilt"]
    return [
        "stage=trust",
        (f'if [ -s {bundle} ] && [ "$(sha256sum {bundle} | cut -d " " -f 1)" = '
         f"'{keep_trust_bundle_sha256}' ]; then"),
        "  echo TRUST_BUNDLE=kept",
        "else",
        *(f"  {command}" for command in rebuild),
        "  echo TRUST_BUNDLE=rebuilt",
        "fi",
    ]


def _install_commands(last_chunk: str | None, keep_trust_bundle_sha256: str | None) -> list[str]:
    archive_path = f"{INSTALL_ROOT}/.runner-assets.tar.gz.b64"
    stage_root = f"{INSTALL_ROOT}.next"
    names = installed_files()
    modes = {name: "0755" if name.endswith((".sh", ".py")) else "0644" for name in names}
    return [
        "stage=archive",
        f"test -f {archive_path}",
        *([f"printf '%s' '{last_chunk}' >> {archive_path}"] if last_chunk is not None else []),
        f"rm -rf {stage_root}",
        f"install -d -m 0755 {stage_root}",
        f"base64 -d < {archive_path} > {stage_root}/.assets.tar.gz",
        f"tar -xzf {stage_root}/.assets.tar.gz -C {stage_root}",
        f"rm -f {stage_root}/.assets.tar.gz {archive_path}",
        "stage=venv",
        f"python3.12 -m venv {stage_root}/venv",
        "stage=dependencies",
        "attempt=1",
        (f"until timeout {PIP_ATTEMPT_SECONDS} {stage_root}/venv/bin/pip install "
         "--disable-pip-version-check --no-cache-dir --retries 2 --timeout 15 "
         f"-r {stage_root}/requirements-round5-azure.txt >>{INSTALL_LOG} 2>&1; do"),
        '  [ "$attempt" -lt 3 ]',
        "  sleep $((attempt * 5))",
        "  attempt=$((attempt + 1))",
        "done",
        "stage=permissions",
        *(f"install -m {modes[name]} {stage_root}/{name} {INSTALL_ROOT}/{name}.next"
          for name in names),
        *(f"mv {INSTALL_ROOT}/{name}.next {INSTALL_ROOT}/{name}" for name in names),
        f"rm -rf {INSTALL_ROOT}/venv.old",
        f"if [ -e {INSTALL_ROOT}/venv ]; then mv {INSTALL_ROOT}/venv {INSTALL_ROOT}/venv.old; fi",
        f"mv {stage_root}/venv {INSTALL_ROOT}/venv",
        f"rm -rf {stage_root} {INSTALL_ROOT}/venv.old",
        *_trust_commands(keep_trust_bundle_sha256),
        "stage=checksums",
        # Digests only: no path, host, credential or provider message comes back.
        f"{INSTALL_ROOT}/venv/bin/python3.12 -I - <<'PY'",
        "import hashlib, pathlib",
        f"root = pathlib.Path('{INSTALL_ROOT}')",
        "def digest(names):",
        "    total = hashlib.sha256()",
        "    for name in names:",
        "        value = (root / name).read_bytes()",
        "        total.update(name.encode()); total.update(b'\\0')",
        "        total.update(value); total.update(b'\\0')",
        "        print('ASSET=' + name + ':' + hashlib.sha256(value).hexdigest())",
        "    return total.hexdigest()",
        f"print('HARNESS=' + digest({RUNNER_ASSETS!r}))",
        f"print('AZURE=' + digest({AZURE_RUNNER_ASSETS!r}))",
        f"print('TRUST=' + hashlib.sha256(pathlib.Path('{TRUST_BUNDLE_PATH}').read_bytes())"
        ".hexdigest())",
        "PY",
    ]


def unit_text(target: RunnerTarget) -> str:
    return "\n".join((
        "[Unit]",
        "Description=Lakebase Anti-Demo Round 5 resident runner (Azure)",
        "After=network-online.target",
        "Wants=network-online.target",
        "",
        "[Service]",
        "Type=simple",
        # The vault's address, not a secret: the agent reads its control DSN there as the VM's
        # managed identity. A spawned fan-in worker inherits it (runner/azure_runner.py).
        f"Environment=ANTI_DEMO_KEY_VAULT_URI={target.vault_uri}",
        # Where the competitor lane's bout clients connect: PgBouncer, on the server's own host.
        f"Environment=ANTI_DEMO_POOLED_PORT={POOLED_PORT}",
        (f"ExecStart={AZURE_RUNNER_PATH} --resident-agent {target.lane_id} 0 "
         f"{target.queue_address} {target.control_secret}"),
        "Restart=always",
        "RestartSec=2",
        "",
        "[Install]",
        "WantedBy=multi-user.target",
        "",
    ))


def _start_commands(target: RunnerTarget, old_pid: int) -> list[str]:
    unit = target.unit_name
    encoded_unit = base64.b64encode(unit_text(target).encode()).decode("ascii")
    return [
        "stage=unit",
        f"printf '%s' '{encoded_unit}' | base64 -d > /etc/systemd/system/{unit}",
        f"chmod 0644 /etc/systemd/system/{unit}",
        "systemctl daemon-reload",
        f"systemctl enable --now {unit} >>{INSTALL_LOG} 2>&1",
        "stage=attestation",
        f"deadline=$(( $(date +%s) + {ATTESTATION_WAIT_SECONDS} ))",
        (f"until systemctl is-active --quiet {unit} && [ -s {target.attestation_path} ] "
         f"&& [ -s {target.azure_attestation_path} ]; do"),
        '  [ "$(date +%s)" -lt "$deadline" ]',
        "  sleep 1",
        "done",
        f"new_pid=$(systemctl show {unit} -p MainPID --value)",
        '[ "$new_pid" -gt 0 ]',
        f"[ \"$new_pid\" != '{old_pid}' ]",
        f"{INSTALL_ROOT}/venv/bin/python3.12 -I - \"$new_pid\" <<'PY'",
        "import json, sys",
        "pid = int(sys.argv[1])",
        f"harness = json.load(open('{target.attestation_path}'))",
        f"azure = json.load(open('{target.azure_attestation_path}'))",
        "assert int(harness.get('pid', 0)) == pid and int(azure.get('pid', 0)) == pid",
        "print('NEW_PID=' + str(pid))",
        "print('NEW_PROCESS_BOOT=' + str(harness.get('runner_process_boot_id', '')))",
        "print('LOADED_HARNESS=' + str(harness.get('runner_harness_sha256', '')))",
        "print('LOADED_AZURE=' + str(azure.get('azure_runner_sha256', '')))",
        "PY",
    ]


def _checked_install(stdout: str, vm_name: str, expected_harness_sha256: str) -> tuple[str, bool]:
    assets: dict[str, str] = {}
    for line in stdout.splitlines():
        if line.startswith("ASSET="):
            name, separator, checksum = line.removeprefix("ASSET=").partition(":")
            if separator and _SHA256.fullmatch(checksum):
                assets[name] = checksum
    values = _values(stdout)
    trust = values.get("TRUST", "")
    if _SHA256.fullmatch(trust) is None:
        raise RunnerInstallError(f"Round 5 runner {vm_name} returned no trust bundle checksum")
    if assets != {**runner_asset_sha256s(), **azure_asset_sha256s()}:
        raise RunnerInstallError(
            f"Round 5 runner {vm_name}'s per-file checksums differ from the local assets")
    if values.get("HARNESS") != expected_harness_sha256:
        raise RunnerInstallError(
            f"Round 5 runner {vm_name}'s harness differs from the sealed local harness")
    if values.get("AZURE") != azure_runner_sha256():
        raise RunnerInstallError(
            f"Round 5 runner {vm_name}'s Azure files differ from the local ones")
    return trust, values.get("TRUST_BUNDLE") == "kept"


def configure_runner(
    run: RunScript,
    target: RunnerTarget,
    *,
    expected_harness_sha256: str,
    keep_trust_bundle_sha256: str | None = None,
    report: Callable[[str], None] = lambda _message: None,
) -> RunnerInstall:
    """Install the runner on `target`, restart its resident agent, and prove what now runs.

    `run` is `ArmClient.run_shell_script`. `keep_trust_bundle_sha256` is the sealed bundle's
    checksum, where there is a seal: a runner still holding that bundle keeps it, as on AWS.
    """

    if _SHA256.fullmatch(expected_harness_sha256) is None:
        raise ValueError("the sealed harness checksum is not a sha256")
    if keep_trust_bundle_sha256 is not None and _SHA256.fullmatch(keep_trust_bundle_sha256) is None:
        raise ValueError("the sealed trust bundle checksum is not a sha256")
    chunks = archive_chunks(runner_archive())
    vm = target.vm_name
    report(f"Round 5 runner {vm}: stopping the old agent and preparing the install")
    stopped = _values(_run(run, vm, _prepare_commands(target, chunks[0], len(chunks)),
                           PREPARE_BOUND_SECONDS))
    old_pid_text = stopped.get("OLD_PID", "0")
    old_pid = int(old_pid_text) if old_pid_text.isdigit() else 0
    old_process_boot = stopped.get("OLD_PROCESS_BOOT", "")
    for index in range(1, len(chunks) - 1):
        _run(run, vm, _chunk_commands(chunks[index], index, len(chunks)), CHUNK_BOUND_SECONDS)
    report(f"Round 5 runner {vm}: installing the harness")
    installed = _run(run, vm, _install_commands(chunks[-1] if len(chunks) > 1 else None,
                                                keep_trust_bundle_sha256),
                     INSTALL_BOUND_SECONDS)
    trust, kept = _checked_install(installed, vm, expected_harness_sha256)
    report(f"Round 5 runner {vm}: starting the {target.lane_id} lane's resident agent")
    started = _values(_run(run, vm, _start_commands(target, old_pid), START_BOUND_SECONDS))
    pid = started.get("NEW_PID", "")
    process_boot = started.get("NEW_PROCESS_BOOT", "")
    if (
        not pid.isdigit()
        or int(pid) <= 0
        or process_boot in {"", old_process_boot}
        or started.get("LOADED_HARNESS") != expected_harness_sha256
        or started.get("LOADED_AZURE") != azure_runner_sha256()
    ):
        raise RunnerInstallError(
            f"Round 5 runner {vm}'s resident agent did not attest a new loaded harness")
    return RunnerInstall(vm_name=vm, trust_bundle_sha256=trust, pid=int(pid),
                         process_boot_id=process_boot, trust_bundle_kept=kept)


#: SSM's 120 s execution bound for one runner request, plus Run Command's own ~32 s, with room.
RUNNER_REQUEST_BOUND_SECONDS = 300.0
#: The longest compressed stdout one call carries back: Run Command keeps the last 4,096 bytes of
#: output, and the exit marker and status line come after it.
RUNNER_OUTPUT_MAX_CHARS = 3_800


def runner_request_commands(vault_uri: str, encoded: str) -> list[str]:
    """One request to the harness through the Azure wrapper, as SSM sends `RUNNER_PATH <request>`.

    The harness caps a setup or preflight result at 4,096 bytes of JSON, and prints settlement
    lines around it. With this script's own lines that can pass Run Command's 4,096-byte limit,
    which keeps the end and cuts the start. So the runner's stdout comes back gzipped and in
    base64, on one line, and is refused here, not cut, if it would not fit.
    """

    if VAULT_URI.fullmatch(vault_uri) is None:
        raise ValueError("not a Key Vault URI")
    if re.fullmatch(r"[A-Za-z0-9+/=_-]{1,60000}", encoded) is None:
        raise ValueError("not an encoded runner request")
    return [
        "stage=request",
        'out=$(mktemp)',
        'err=$(mktemp)',
        "status=0",
        (f"ANTI_DEMO_KEY_VAULT_URI='{vault_uri}' ANTI_DEMO_POOLED_PORT={POOLED_PORT} "
         f"{AZURE_RUNNER_PATH} '{encoded}' >\"$out\" 2>\"$err\" || status=$?"),
        'echo "RUNNER_STATUS=$status"',
        "blob=$(gzip -9c \"$out\" | base64 | tr -d '\\n')",
        f'if [ "${{#blob}}" -gt {RUNNER_OUTPUT_MAX_CHARS} ]; then',
        '  echo "RUNNER_OUTPUT_TOO_LARGE=${#blob}"',
        "else",
        '  echo "RUNNER_STDOUT_GZIP_BASE64=$blob"',
        "fi",
        'tail -c 1024 "$err" >&2 || true',
        'rm -f "$out" "$err"',
    ]


def run_runner_request(run: RunScript, vm_name: str, *, vault_uri: str, encoded: str,
                       bound_seconds: float = RUNNER_REQUEST_BOUND_SECONDS) -> tuple[int, str]:
    """The harness's exit status and stdout for one request, over Run Command."""

    values = _values(_run(run, vm_name, runner_request_commands(vault_uri, encoded),
                          bound_seconds))
    if "RUNNER_OUTPUT_TOO_LARGE" in values:
        raise RunnerInstallError(
            f"Round 5 runner {vm_name}'s answer is too large for Run Command "
            f"({values['RUNNER_OUTPUT_TOO_LARGE']} compressed characters)")
    status, blob = values.get("RUNNER_STATUS", ""), values.get("RUNNER_STDOUT_GZIP_BASE64")
    if not status.isdigit() or blob is None:
        raise RunnerInstallError(f"Round 5 runner {vm_name} returned no runner output")
    return int(status), gzip.decompress(base64.b64decode(blob)).decode("utf-8")


def configure_runners(
    run: RunScript,
    targets: Sequence[RunnerTarget],
    *,
    expected_harness_sha256: str,
    keep_trust_bundle_sha256: str | None = None,
    report: Callable[[str], None] = lambda _message: None,
) -> Mapping[str, RunnerInstall]:
    """Both runners at once, by lane. They must end holding the same trust bundle, as on AWS."""

    lanes = [target.lane_id for target in targets]
    if sorted(lanes) != ["competitor", "lakebase"]:
        raise ValueError("Round 5 needs exactly one runner per lane")
    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = {
            target.lane_id: pool.submit(
                configure_runner, run, target,
                expected_harness_sha256=expected_harness_sha256,
                keep_trust_bundle_sha256=keep_trust_bundle_sha256, report=report)
            for target in targets
        }
        installs = {lane: future.result() for lane, future in futures.items()}
    if len({install.trust_bundle_sha256 for install in installs.values()}) != 1:
        raise RunnerInstallError("Round 5's two runners installed different trust bundles")
    return installs
