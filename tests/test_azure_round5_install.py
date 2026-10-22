"""server/azure_round5_install.py: Round 5 on Azure installed, its runners set up, and sealed.

Run Command, the runners and Lakebase are fakes: the harness's setup protocol is answered as the
real one answers it (SETUP_RESULT and SETUP_SETTLED lines), and the payloads it is sent are read.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import json
from types import SimpleNamespace
from typing import Any

import pytest

from server import azure_round5_install as install
from server.azure_round5_runner import RunnerInstall, RunnerInstallError

VAULT = "https://kv-test-r5.vault.azure.net/"
KEY = base64.b64encode(b"k" * 32).decode()
KEY_SHA = hashlib.sha256(b"k" * 32).hexdigest()

OUTPUTS = {
    "round5_runners": {
        "runner-0": {"name": "vm-test-r5-runner-0", "id": "/subscriptions/x/vm0",
                     "size": "Standard_D8ls_v7",
                     "principal_id": "11111111-1111-1111-1111-111111111111"},
        "runner-1": {"name": "vm-test-r5-runner-1", "id": "/subscriptions/x/vm1",
                     "size": "Standard_D8ls_v7",
                     "principal_id": "22222222-2222-2222-2222-222222222222"},
    },
    "round5_runner_egress_ip": "203.0.113.7",
    "service_bus": {"namespace_name": "sb-test-r5",
                    "queues": {"r5-runner-0": "queue-r5-runner-0",
                               "r5-runner-1": "queue-r5-runner-1"}},
    "key_vault": {"uri": VAULT},
}


class Vault:
    def __init__(self) -> None:
        self.secrets: dict[str, str] = {}

    def set(self, name: str, value: str) -> None:
        self.secrets[name] = value


def manifest() -> Any:
    server = SimpleNamespace(
        server="psql-test-r5", host="psql-test-r5.postgres.database.azure.com",
        resource_id=("/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/"
                     "providers/Microsoft.DBforPostgreSQL/flexibleServers/psql-test-r5"),
        admin_login="antidemo_admin")
    return SimpleNamespace(azure=SimpleNamespace(region="westus2", servers={"r5": server}),
                           databricks=SimpleNamespace(database="anti_demo"),
                           run_id="ad-test", round5_azure=None)


class Harness:
    """The runners' setup protocol: answers each request as the harness would."""

    def __init__(self, refuse: str | None = None) -> None:
        self.payloads: list[tuple[str, dict[str, Any]]] = []
        self.refuse = refuse

    def __call__(self, run: Any, vm_name: str, *, vault_uri: str, encoded: str) -> Any:
        payload = json.loads(gzip.decompress(base64.b64decode(encoded)))
        self.payloads.append((vm_name, payload))
        if self.refuse and payload["action"] == self.refuse:
            return 65, "RUNNER_ERROR:setup_binding_invalid\n"
        result = {"protocol": payload["protocol"], "action": payload["action"],
                  "nonce": payload["nonce"], "status": "verified"}
        if payload["action"] == "public_key":
            result.update(public_key=KEY, public_key_sha256=KEY_SHA)
        else:
            tag = "a" if payload["lane_id"] == "lakebase" else "c"
            result.update(credential_sha256=tag * 64, observer_credential_sha256="e" * 64)
        return 0, (f"SETUP_RESULT:{json.dumps(result)}\nSETUP_SETTLED:{payload['nonce']}\n"
                   "RUNNER_FLOCK_RELEASED:x\n")


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> Harness:
    responder = Harness()
    monkeypatch.setattr(install, "run_runner_request", responder)
    monkeypatch.setattr(install, "configure_runners", lambda run, targets, **_: {
        target.lane_id: RunnerInstall(vm_name=target.vm_name, trust_bundle_sha256="f" * 64,
                                      pid=10, process_boot_id="process-1",
                                      trust_bundle_kept=False)
        for target in targets})
    monkeypatch.setattr(install.lifecycle, "_enable_round5_lakebase_native_login",
                        lambda _manifest: ("lakebase-direct.test", "lakebase-pooled.test"))
    monkeypatch.setattr(install, "apply_manifest_environment", lambda _manifest: None)
    material = SimpleNamespace(host="lakebase-direct.test")

    async def connection_material() -> Any:
        return material

    monkeypatch.setattr(install.lifecycle, "_round_lakebase_provider",
                        lambda _manifest, _round: SimpleNamespace(
                            connection_material=connection_material))
    monkeypatch.setattr(install.lifecycle, "_seal_round5_admin",
                        lambda key, admin: f"sealed-for-{key[:4]}")
    return responder


def test_the_install_sets_up_both_lanes_as_aws_does_and_seals_what_they_attested(
        harness: Harness) -> None:
    vault = Vault()
    seal = install.ensure_round5(manifest(), run=object(), outputs=OUTPUTS, vault=vault,  # type: ignore[arg-type]
                                 admin_password="admin-pw", report=lambda _: None)
    master = json.loads(vault.secrets[install.MASTER_SECRET_NAME])
    assert master == {"host": "psql-test-r5.postgres.database.azure.com", "port": 5432,
                      "dbname": "anti_demo", "username": "antidemo_admin",
                      "password": "admin-pw"}
    actions = [(vm, payload["action"]) for vm, payload in harness.payloads]
    assert actions == [("vm-test-r5-runner-0", "public_key"),
                       ("vm-test-r5-runner-1", "public_key"),
                       ("vm-test-r5-runner-0", "prepare_lakebase"),
                       ("vm-test-r5-runner-1", "prepare_rds_baseline")]
    lakebase, competitor = harness.payloads[2][1], harness.payloads[3][1]
    assert lakebase["sealed_admin"] == f"sealed-for-{KEY[:4]}"
    assert (lakebase["endpoint_host"], lakebase["credential_host"]) == (
        "lakebase-pooled.test", "lakebase-direct.test")
    # The competitor's roles are made on the server itself: port 5432, as `rds` to the harness.
    assert (competitor["lane_id"], competitor["port"]) == ("rds", 5432)
    assert competitor["master_secret_arn"].endswith(":secret:round5-competitor-master")
    assert competitor["trust_bundle_sha256"] == "f" * 64
    assert seal.lakebase_credential_sha256 == "a" * 64
    assert seal.competitor_credential_sha256 == "c" * 64
    assert seal.competitor_runner_vm == "vm-test-r5-runner-1"
    assert seal.competitor_control_queue.endswith("/queue-r5-runner-1")
    assert seal.runner_public_key_sha256 == KEY_SHA
    assert seal.config_sha256 == install.azure_round5_config_sha256(seal.baseline_sha256,
                                                                    seal.contract_sha256)


def test_a_refused_setup_names_the_runners_code(harness: Harness) -> None:
    harness.refuse = "prepare_rds_baseline"
    with pytest.raises(RunnerInstallError, match="refused with setup_binding_invalid"):
        install.ensure_round5(manifest(), run=object(), outputs=OUTPUTS, vault=Vault(),  # type: ignore[arg-type]
                              admin_password="pw", report=lambda _: None)


async def test_each_lanes_resident_login_is_kept_and_its_dsn_written_to_key_vault(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # A published password is re-applied, never replaced: a running agent holds the DSN it read.
    rotated: list[tuple[str, str, str]] = []

    async def rotate_login(cursor: Any, *, database: str, role: str, password: str,
                           lane_id: str) -> None:
        rotated.append((lane_id, role, password))

    async def retire(cursor: Any, *, database: str, role: str) -> None:
        rotated.append(("retired", role, ""))

    monkeypatch.setattr(install.lifecycle, "_rotate_round5_resident_login", rotate_login)
    monkeypatch.setattr(install.lifecycle, "_retire_round5_shared_resident_login", retire)

    class Store:
        async def _run(self, operation: Any) -> None:
            await operation(object())

    class FullVault(Vault):
        def get(self, name: str) -> str | None:
            return self.secrets.get(name)

    built = manifest()
    built.installation_id = "install-1"
    digest = hashlib.sha256(b"install-1").hexdigest()[:16]
    vault = FullVault()
    vault.secrets["round5-lakebase-control"] = (
        f"postgresql://anti_demo_r5_lakebase_{digest}:kept-pw@coord.test/anti_demo")
    await install.publish_resident_logins(built, Store(), "coord.test", vault)  # type: ignore[arg-type]
    lakebase, competitor, retired = rotated
    assert lakebase == ("lakebase", f"anti_demo_r5_lakebase_{digest}", "kept-pw")
    assert competitor[0] == "competitor" and competitor[2] not in ("", "kept-pw")
    assert retired == ("retired", f"anti_demo_r5_{digest}", "")
    written = vault.secrets["round5-competitor-control"]
    assert written.startswith(f"postgresql://anti_demo_r5_competitor_{digest}:")
    assert "sslrootcert=" in written and "coord.test" in written
    assert install.published_password(written, f"anti_demo_r5_competitor_{digest}") == (
        competitor[2])
    assert install.published_password(written, "another_role") is None


def test_a_result_for_another_request_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    def stale(run: Any, vm: str, *, vault_uri: str, encoded: str) -> Any:
        payload = json.loads(gzip.decompress(base64.b64decode(encoded)))
        result = {"protocol": payload["protocol"], "action": payload["action"],
                  "nonce": "an-older-nonce", "status": "verified"}
        return 0, f"SETUP_RESULT:{json.dumps(result)}\nSETUP_SETTLED:{payload['nonce']}\n"

    monkeypatch.setattr(install, "run_runner_request", stale)
    target = SimpleNamespace(vm_name="vm-test-r5-runner-0", vault_uri=VAULT)
    with pytest.raises(RunnerInstallError, match="did not match its request"):
        install.setup_request(object(), target,  # type: ignore[arg-type]
                              {"protocol": install.SETUP_PROTOCOL, "action": "public_key",
                               "nonce": "n1"})
