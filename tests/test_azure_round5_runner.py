"""server/azure_round5_runner.py: the Round 5 runner installed over Run Command, on a fake VM.

Run Command costs about 32 s a call, so the scripts are proven here first: each one runs under dash
(Ubuntu's /bin/sh) with the VM's paths moved under a temporary root, systemd, apt and pip stubbed,
and the output cut to Azure's last 4,096 bytes. The fake agent imports the files the install put
down and attests them as the real one does, so a wrong byte anywhere shows as a wrong digest.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from server import azure_round5_runner as installer
from server.azure_arm import RunCommandOutput
from server.connection_spike_live import runner_harness_sha256

VAULT = "https://kv-test-r5.vault.azure.net/"
SECRET = installer.secret_identifier("westus2", "round5-lakebase-control")
QUEUE = installer.queue_address("sb-test-r5", "queue-r5-runner-0")
DASH = shutil.which("dash")
#: Absolute paths the scripts write, moved under the fake VM's root.
VM_PATHS = ("/opt/lakebase-anti-demo", "/var/lib/lakebase-anti-demo", "/var/log/lakebase-anti-demo",
            "/etc/systemd/system", "/run/lakebase-anti-demo", "/etc/ssl/certs")

FAKE_AGENT = textwrap.dedent("""
    # systemctl enable --now, on the fake VM: start the agent the unit names, as far as attesting.
    import json, os, secrets, sys
    from pathlib import Path

    root, unit = Path(sys.argv[1]), sys.argv[2]
    lines = (root / "etc/systemd/system" / unit).read_text().splitlines()
    arguments = next(line for line in lines if line.startswith("ExecStart=")).split()
    vault = next(line for line in lines
                 if line.startswith("Environment=ANTI_DEMO_KEY_VAULT_URI=")).split("=", 2)[2]
    lane = arguments[2]
    sys.path.insert(0, str(root / "opt/lakebase-anti-demo/round5"))
    import azure_runner  # the installed copy
    azure_runner.ATTESTATION_DIRECTORY = root / "run"
    if os.environ.get("FAKE_AGENT") != "silent":
        azure_runner.attest(lane)
    azure_runner.install(vault, object())
    import connection_spike_runner  # the installed copy, on the stand-ins
    loaded = connection_spike_runner.LOADED_RUNNER_HARNESS_SHA256
    if os.environ.get("FAKE_AGENT") == "stale":
        loaded = "0" * 64
    if os.environ.get("FAKE_AGENT") != "silent":
        attestation = root / "run" / f"lakebase-anti-demo-round5-{lane}.attestation.json"
        attestation.write_text(json.dumps({
            "pid": os.getpid(), "runner_boot_id": "boot",
            "runner_process_boot_id": f"process-{secrets.token_hex(16)}",
            "runner_harness_sha256": loaded}))
    (root / "systemd" / f"{unit}.pid").write_text(str(os.getpid()))
""")


def executable(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(0o755)


class FakeVm:
    """One runner VM on the local disk, answering as `ArmClient.run_shell_script` does."""

    def __init__(self, root: Path, name: str = "vm-r5-0", *, shell: str | None = DASH) -> None:
        self.root, self.name, self.shell = root, name, shell
        self.scripts: list[list[str]] = []
        self.bounds: list[float] = []
        self.environment: dict[str, str] = {}
        for path in VM_PATHS:
            (root / path.lstrip("/")).mkdir(parents=True, exist_ok=True)
        for directory in ("bin", "run", "systemd"):
            (root / directory).mkdir(exist_ok=True)
        (root / "etc/ssl/certs/ca-certificates.crt").write_text(
            "-----BEGIN CERTIFICATE-----\nsystem\n-----END CERTIFICATE-----\n")
        (root / "fake_agent.py").write_text(FAKE_AGENT)
        python = sys.executable
        stubs = {
            "timeout": 'shift\nexec "$@"\n',
            "cloud-init": "exit 0\n",
            "apt-get": "echo apt-get must not run here >&2\nexit 1\n",
            "python3": f'exec {python} "$@"\n',
            # A venv with no network: its python is this one, its pip succeeds.
            "python3.12": textwrap.dedent(f"""\
                if [ "$1" = -m ] && [ "$2" = venv ]; then
                  mkdir -p "$3/bin"
                  ln -s {python} "$3/bin/python3.12"
                  printf '#!/bin/sh\\nexit 0\\n' > "$3/bin/pip"
                  chmod 0755 "$3/bin/pip"
                  exit 0
                fi
                exec {python} "$@"
            """),
            "systemctl": textwrap.dedent(f"""\
                state={root}/systemd
                case "$1" in
                  cat) [ -f {root}/etc/systemd/system/"$2" ] ;;
                  show)
                    if [ -f "$state/$2.pid" ]; then read -r pid < "$state/$2.pid"; echo "$pid"
                    else echo 0; fi ;;
                  stop) rm -f "$state/$2.pid" ;;
                  is-active) [ -f "$state/$3.pid" ] ;;
                  daemon-reload) exit 0 ;;
                  disable) echo "$@" >> "$state/disabled" ;;
                  enable) exec {python} {root}/fake_agent.py {root} "$3" ;;
                  *) exit 1 ;;
                esac
            """),
        }
        for name, body in stubs.items():
            executable(root / "bin" / name, "#!/bin/sh\n" + body)

    def path(self, real: str) -> Path:
        return self.root / real.lstrip("/")

    def __call__(self, vm: str, script: list[str], *, bound_seconds: float) -> RunCommandOutput:
        assert vm == self.name
        self.scripts.append(script)
        self.bounds.append(bound_seconds)
        text = "\n".join(script) + "\n"
        for real in VM_PATHS:
            text = text.replace(real, str(self.path(real)))
        environment = {"PATH": f"{self.root / 'bin'}:/usr/bin:/bin:/usr/sbin:/sbin",
                       **self.environment}
        result = subprocess.run([self.shell, "-c", text], capture_output=True, text=True,
                                timeout=120, env=environment)
        return RunCommandOutput(stdout=result.stdout[-4096:], stderr=result.stderr[-4096:])


pytestmark = pytest.mark.skipif(DASH is None, reason="the fake VM runs its scripts under dash")


def target(lane: str = "lakebase", vm: str = "vm-r5-0") -> installer.RunnerTarget:
    return installer.RunnerTarget(vm_name=vm, lane_id=lane, queue_address=QUEUE,
                                  control_secret=SECRET, vault_uri=VAULT)


def test_a_fresh_install_puts_down_both_halves_and_proves_the_agent_loaded_them(
        tmp_path: Path) -> None:
    vm = FakeVm(tmp_path)
    install = installer.configure_runner(vm, target(),
                                         expected_harness_sha256=runner_harness_sha256())
    # Prepare with the first chunk, one call per middle chunk, install with the last, then start.
    chunks = installer.archive_chunks(installer.runner_archive())
    assert len(vm.scripts) == len(chunks) + 1
    assert vm.bounds == [installer.PREPARE_BOUND_SECONDS,
                         *[installer.CHUNK_BOUND_SECONDS] * (len(chunks) - 2),
                         installer.INSTALL_BOUND_SECONDS, installer.START_BOUND_SECONDS]
    root = vm.path(installer.INSTALL_ROOT)
    for name in installer.installed_files():
        source = installer.ROOTS_PATH if name == installer.ROOTS_NAME else (
            installer.RUNNER_ROOT / name)
        assert (root / name).read_bytes() == source.read_bytes(), name
        executable_bit = bool((root / name).stat().st_mode & stat.S_IXUSR)
        assert executable_bit == name.endswith((".sh", ".py")), name
    # The trust bundle: the VM's own CA bundle, then the Azure roots, nothing downloaded.
    bundle = vm.path(str(installer.TRUST_BUNDLE_PATH)).read_bytes()
    assert bundle == (vm.path(installer.SYSTEM_CA_BUNDLE).read_bytes()
                      + installer.ROOTS_PATH.read_bytes())
    assert install.trust_bundle_sha256 == hashlib.sha256(bundle).hexdigest()
    assert not install.trust_bundle_kept
    # The unit runs the Azure wrapper with the harness's own five arguments.
    unit = vm.path("/etc/systemd/system/lakebase-anti-demo-round5-lakebase.service").read_text()
    assert f"Environment=ANTI_DEMO_KEY_VAULT_URI={VAULT}" in unit
    assert "Environment=ANTI_DEMO_POOLED_PORT=6432" in unit
    assert (f"ExecStart={installer.AZURE_RUNNER_PATH} --resident-agent lakebase 0 {QUEUE} {SECRET}"
            in unit)
    attested = json.loads(vm.path(
        "/run/lakebase-anti-demo-round5-lakebase.attestation.json").read_text())
    assert attested["pid"] == install.pid and attested["runner_process_boot_id"] == (
        install.process_boot_id)
    assert not vm.path(f"{installer.INSTALL_ROOT}/.runner-assets.tar.gz.b64").exists()
    assert not vm.path(f"{installer.INSTALL_ROOT}.next").exists()
    # Ubuntu's daily upgrade restarted a resident agent mid-bar (2026-10-21): no next one.
    assert (tmp_path / "systemd" / "disabled").read_text().split() == [
        "disable", "--now", "apt-daily-upgrade.timer", "apt-daily.timer"]


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_the_scripts_run_the_same_under_bash(tmp_path: Path) -> None:
    # Whichever shell Run Command hands the script to.
    vm = FakeVm(tmp_path, shell=shutil.which("bash"))
    install = installer.configure_runner(vm, target(),
                                         expected_harness_sha256=runner_harness_sha256())
    chunks = installer.archive_chunks(installer.runner_archive())
    assert install.pid > 0 and len(vm.scripts) == len(chunks) + 1


def test_a_refresh_keeps_the_sealed_trust_bundle_and_starts_a_provably_new_agent(
        tmp_path: Path) -> None:
    vm = FakeVm(tmp_path)
    first = installer.configure_runner(vm, target(),
                                       expected_harness_sha256=runner_harness_sha256())
    # The VM's CA bundle moved since (an apt upgrade): the sealed bundle is kept, not rebuilt.
    vm.path(installer.SYSTEM_CA_BUNDLE).write_text("-----BEGIN CERTIFICATE-----\nnewer\n")
    second = installer.configure_runner(vm, target(),
                                        expected_harness_sha256=runner_harness_sha256(),
                                        keep_trust_bundle_sha256=first.trust_bundle_sha256)
    assert second.trust_bundle_kept and second.trust_bundle_sha256 == first.trust_bundle_sha256
    assert second.process_boot_id != first.process_boot_id
    # With no seal to keep, it is rebuilt from what the VM holds now.
    third = installer.configure_runner(vm, target(),
                                       expected_harness_sha256=runner_harness_sha256())
    assert not third.trust_bundle_kept and third.trust_bundle_sha256 != first.trust_bundle_sha256


def test_an_active_job_refuses_the_install_before_anything_stops(tmp_path: Path) -> None:
    vm = FakeVm(tmp_path)
    installer.configure_runner(vm, target(), expected_harness_sha256=runner_harness_sha256())
    running = vm.path("/var/lib/lakebase-anti-demo/round5-jobs/resident-lakebase-active.json")
    running.parent.mkdir(parents=True, exist_ok=True)
    running.write_text("{}")
    with pytest.raises(installer.RunnerInstallError, match="failed at stage idle"):
        installer.configure_runner(vm, target(), expected_harness_sha256=runner_harness_sha256())
    assert vm.path("/run/lakebase-anti-demo-round5-lakebase.attestation.json").exists()


def test_an_agent_that_never_attests_fails_the_start(tmp_path: Path,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(installer, "ATTESTATION_WAIT_SECONDS", 2)
    vm = FakeVm(tmp_path)
    vm.environment["FAKE_AGENT"] = "silent"
    with pytest.raises(installer.RunnerInstallError, match="failed at stage attestation"):
        installer.configure_runner(vm, target(), expected_harness_sha256=runner_harness_sha256())


def test_an_agent_that_loaded_another_harness_is_refused(tmp_path: Path) -> None:
    vm = FakeVm(tmp_path)
    vm.environment["FAKE_AGENT"] = "stale"
    with pytest.raises(installer.RunnerInstallError, match="did not attest a new loaded harness"):
        installer.configure_runner(vm, target(), expected_harness_sha256=runner_harness_sha256())


def test_a_harness_other_than_the_sealed_one_is_refused_before_the_agent_starts(
        tmp_path: Path) -> None:
    vm = FakeVm(tmp_path)
    with pytest.raises(installer.RunnerInstallError, match="differs from the sealed"):
        installer.configure_runner(vm, target(), expected_harness_sha256="f" * 64)
    # Every chunk's call, and no start.
    assert len(vm.scripts) == len(installer.archive_chunks(installer.runner_archive()))
    assert not vm.path("/etc/systemd/system/lakebase-anti-demo-round5-lakebase.service").exists()


def test_both_runners_install_at_once_and_must_agree_on_the_trust_bundle(tmp_path: Path) -> None:
    vms = {"vm-r5-0": FakeVm(tmp_path / "zero", "vm-r5-0"),
           "vm-r5-1": FakeVm(tmp_path / "one", "vm-r5-1")}

    def run(vm: str, script: list[str], *, bound_seconds: float) -> Any:
        return vms[vm](vm, script, bound_seconds=bound_seconds)

    targets = [target("lakebase", "vm-r5-0"), target("competitor", "vm-r5-1")]
    installs = installer.configure_runners(run, targets,
                                           expected_harness_sha256=runner_harness_sha256())
    assert set(installs) == {"lakebase", "competitor"}
    assert vms["vm-r5-1"].path(
        "/etc/systemd/system/lakebase-anti-demo-round5-competitor.service").exists()
    vms["vm-r5-1"].path(installer.SYSTEM_CA_BUNDLE).write_text("-----BEGIN CERTIFICATE-----\nx\n")
    with pytest.raises(installer.RunnerInstallError, match="different trust bundles"):
        installer.configure_runners(run, targets, expected_harness_sha256=runner_harness_sha256())
    with pytest.raises(ValueError, match="one runner per lane"):
        installer.configure_runners(run, targets[:1], expected_harness_sha256="0" * 64)


def test_the_installer_and_the_runner_agree_on_identifiers_and_the_azure_digest() -> None:
    sys.path.insert(0, str(installer.RUNNER_ROOT))
    import azure_runner

    assert installer.AZURE_RUNNER_ASSETS == azure_runner.AZURE_RUNNER_ASSETS
    assert installer.azure_runner_sha256() == azure_runner.azure_runner_sha256()
    for name in ("SECRET_ID", "QUEUE_ADDRESS", "VAULT_URI"):
        assert getattr(installer, name).pattern == getattr(azure_runner, name).pattern, name
    assert installer.secret_identifier("westus2", "x-y") == azure_runner.secret_identifier(
        "westus2", "x-y")
    with pytest.raises(ValueError):
        installer.RunnerTarget(vm_name="vm", lane_id="lakebase", queue_address=QUEUE,
                               control_secret="arn:aws:secretsmanager:us-west-2:123456789012:"
                                              "secret:real", vault_uri=VAULT)


def stub_harness(vm: FakeVm, body: str) -> None:
    wrapper = vm.path(installer.AZURE_RUNNER_PATH)
    wrapper.parent.mkdir(parents=True, exist_ok=True)
    executable(wrapper, "#!/bin/sh\n" + body)


def test_a_runner_request_comes_back_whole_through_run_commands_limit(tmp_path: Path) -> None:
    # A preflight's JSON may be 4,096 bytes on its own; with its settlement lines it would lose its
    # head to Run Command's limit if sent back as it is.
    vm = FakeVm(tmp_path)
    stub_harness(vm, textwrap.dedent("""\
        [ "$ANTI_DEMO_POOLED_PORT" = 6432 ] && [ -n "$ANTI_DEMO_KEY_VAULT_URI" ] || exit 9
        echo "REQUEST:$1"
        printf 'PREFLIGHT_RESULT:{"pad":"%s"}\\n' "$(head -c 4000 /dev/zero | tr '\\0' 'a')"
        echo CLEANUP_CONFIRMED:run-1
        echo RUNNER_FLOCK_RELEASED:run-1
        echo 'a warning' >&2
    """))
    status, output = installer.run_runner_request(vm, "vm-r5-0", vault_uri=VAULT,
                                                  encoded="H4sI-request_body=")
    assert status == 0
    lines = output.splitlines()
    assert lines[0] == "REQUEST:H4sI-request_body="
    assert lines[1].startswith('PREFLIGHT_RESULT:{"pad":"aaa') and len(lines[1]) > 4000
    assert lines[-2:] == ["CLEANUP_CONFIRMED:run-1", "RUNNER_FLOCK_RELEASED:run-1"]


def test_a_runner_answer_too_large_for_run_command_is_refused_not_cut(tmp_path: Path) -> None:
    vm = FakeVm(tmp_path)
    stub_harness(vm, "head -c 12000 /dev/urandom | base64\n")
    with pytest.raises(installer.RunnerInstallError, match="too large for Run Command"):
        installer.run_runner_request(vm, "vm-r5-0", vault_uri=VAULT, encoded="H4sI")


def test_a_runner_refusal_keeps_its_status_and_code(tmp_path: Path) -> None:
    vm = FakeVm(tmp_path)
    stub_harness(vm, "echo RUNNER_ERROR:setup_binding_invalid\nexit 65\n")
    status, output = installer.run_runner_request(vm, "vm-r5-0", vault_uri=VAULT, encoded="H4sI")
    assert (status, output.strip()) == (65, "RUNNER_ERROR:setup_binding_invalid")
    with pytest.raises(ValueError):
        installer.runner_request_commands(VAULT, "a'; rm -rf /")


def test_the_targets_come_from_terraforms_outputs_one_lane_per_runner() -> None:
    outputs = {
        "round5_runners": {"runner-0": {"name": "vm-t-r5-runner-0"},
                           "runner-1": {"name": "vm-t-r5-runner-1"}},
        "service_bus": {"namespace_name": "sb-test-r5",
                        "queues": {"r5-runner-0": "queue-r5-runner-0",
                                   "r5-runner-1": "queue-r5-runner-1"}},
        "key_vault": {"uri": VAULT},
    }
    lakebase, competitor = installer.runner_targets(outputs, region="westus2")
    assert (lakebase.lane_id, lakebase.vm_name) == ("lakebase", "vm-t-r5-runner-0")
    assert lakebase.queue_address == "sb://sb-test-r5.servicebus.windows.net/queue-r5-runner-0"
    assert competitor.control_secret.endswith(":secret:round5-competitor-control")
    with pytest.raises(installer.RunnerInstallError, match="round5_runners"):
        installer.runner_targets({**outputs, "round5_runners": {}}, region="westus2")


def test_the_archive_is_the_same_bytes_every_time_and_fits_the_bound() -> None:
    archive = installer.runner_archive()
    assert archive == installer.runner_archive()
    chunks = installer.archive_chunks(archive)
    assert "".join(chunks) == archive
    assert all(len(chunk) <= installer.RUN_COMMAND_CHUNK_CHARS for chunk in chunks)
    assert "'" not in archive  # each chunk rides inside single quotes
    assert os.path.basename(installer.AZURE_RUNNER_PATH) in installer.AZURE_RUNNER_ASSETS
