"""server/azure_lifecycle.py: the Azure installer's own rules, without Azure or Databricks.

Identifiers are placeholders and addresses documentation space (RFC 5737), as the identifier guard
requires. The live run is the release bar's; this is what that run can rely on.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from server.azure_arm import Response as ArmResponse
from server.azure_lifecycle import (
    ADMIN_PASSWORD_SECRET,
    TERRAFORM_ATTEMPTS,
    VM_REFUSED,
    AzureInputs,
    AzureSetupError,
    Generation,
    Terraform,
    clear_refused_runners,
    inputs_from_environment,
    new_admin_password,
    new_manifest,
    only_retryable,
    seal_servers,
    storage_account_name,
    terraform_vars,
)

TENANT = "aaaaaaaa-0000-0000-0000-000000000000"
SUB = "00000000-1111-2222-3333-444444444444"
GROUP = f"/subscriptions/{SUB}/resourceGroups/rg-test/providers/"
NOW = datetime(2026, 10, 19, 21, 5, tzinfo=UTC)


def inputs(**changes: Any) -> AzureInputs:
    values: dict[str, Any] = {
        "databricks_profile": "anti-demo-azure-test", "tenant_id": TENANT,
        "subscription_id": SUB, "resource_group": "rg-test", "region": "westus2",
        "owner": "operator@example.com", "operator_cidr": "192.0.2.10/32"}
    return AzureInputs(**{**values, **changes})


def manifest(**changes: Any):
    return new_manifest(
        inputs(**changes), databricks_user="operator@example.com",
        principal_object_id="bbbbbbbb-0000-0000-0000-000000000000",
        egress=(("198.51.100.0/24", "198.18.0.0/23"), 1_790_000_000),
        run_id="az-20990101-0000-test", installation_id="018f6f50-7d3a-4cc1-9d5d-4d9ac8d107a1",
        now=NOW)


def test_the_storage_account_is_named_as_terraform_names_it() -> None:
    # Terraform's own expression (infra/azure/locals.tf), which the Python must reproduce: on
    # 2026-10-19 `terraform plan` and this function gave a live installation the same name.
    locals_tf = (Path(__file__).resolve().parents[1] / "infra/azure/locals.tf").read_text()
    assert ('storage_account_name = "st${substr(sha256(local.stable_id_base), 0, 20)}'
            '${substr(var.run_id, 0, 2)}"') in locals_tf
    installation = "018f6f50-7d3a-4cc1-9d5d-4d9ac8d107a1"
    digest = hashlib.sha256(installation.encode()).hexdigest()[:20]
    assert storage_account_name(installation, "az-20990101-0000-test") == f"st{digest}az"


def test_a_first_manifest_is_v8_azure_and_builds_nothing_yet() -> None:
    sealed = manifest()
    assert (sealed.manifest_version, sealed.cloud, sealed.status) == (8, "azure", "provisioning")
    assert sealed.aws is None and sealed.azure is not None and sealed.azure.servers == {}
    assert sealed.azure.storage_account == storage_account_name(
        "018f6f50-7d3a-4cc1-9d5d-4d9ac8d107a1", "az-20990101-0000-test")
    assert sealed.databricks.project_id == "anti-demo-018f6f507d3a4cc19d5d4d9ac8d107a1-r1"
    assert sealed.databricks.coordination_endpoint_name.startswith(
        "projects/anti-demo-018f6f507d3a4cc19d5d4d9ac8d107a1-coord/")
    assert (sealed.expires_at - NOW).total_seconds() == 72 * 3600


def test_terraform_is_given_the_manifests_identity_and_only_the_settings_it_has() -> None:
    values = terraform_vars(manifest(), inputs(), ssh_public_key="ssh-ed25519 AAAA test")
    assert values["run_id"] == "az-20990101-0000-test"
    assert values["installation_id"] == "018f6f50-7d3a-4cc1-9d5d-4d9ac8d107a1"
    assert values["expires_at"] == "2026-10-22T21:05:00Z"
    assert values["databricks_egress_cidrs"] == ["198.51.100.0/24", "198.18.0.0/23"]
    assert values["zone"] == "1" and values["manage_role_assignments"] is True
    # No policy, no policy variables: the file a hand-run apply wrote matches it key for key.
    assert "policy_tags" not in values and "lease_date_tag" not in values
    tagged = terraform_vars(manifest(policy_tags={"cost-center": "demo"},
                                     lease_date_tag="review-by"),
                            inputs(), ssh_public_key="ssh-ed25519 AAAA test")
    assert tagged["policy_tags"] == {"cost-center": "demo"}
    assert tagged["lease_date_tag"] == "review-by"


def test_a_virgin_install_stands_round_5s_runners() -> None:
    # The installer writes only what isn't Terraform's default, so the default is what an install
    # from nothing gets. It was off until 2026-10-21, from before Round 5 raced on Azure: the live
    # installation's runners stood only because a hand-run apply had turned them on.
    values = terraform_vars(manifest(), inputs(), ssh_public_key="ssh-ed25519 AAAA test")
    assert "round5_runners" not in values
    hcl = (Path(__file__).resolve().parents[1] / "infra/azure/variables.tf").read_text()
    block = re.search(r'variable "round5_runners" \{(.*?)\n\}', hcl, re.S)
    assert block is not None
    default = re.search(r"default\s*=\s*(\w+)", block.group(1))
    assert default is not None and default.group(1) == "true"


def _server(key: str, sku: str = "B_Standard_B2s") -> dict[str, Any]:
    name = f"server-test-{key}"
    return {"id": f"{GROUP}Microsoft.DBforPostgreSQL/flexibleServers/{name}",
            "fqdn": f"{name}.postgres.database.azure.com", "name": name,
            "admin_login": "antidemo_admin", "port": 5432, "sku_name": sku}


def test_each_rounds_server_is_sealed_from_terraforms_output() -> None:
    outputs = {"postgresql_servers": {"value": {
        key: _server(key, "GP_Standard_D2ds_v5" if key == "r5" else "B_Standard_B2s")
        for key in ("r2", "r3", "r4", "r5", "r6")}}}
    sealed = seal_servers(outputs)
    assert sorted(sealed) == ["r2", "r3", "r4", "r5", "r6"]
    assert sealed["r5"].sku == "GP_Standard_D2ds_v5"
    assert {seal.password_secret for seal in sealed.values()} == {ADMIN_PASSWORD_SECRET}
    with pytest.raises(AzureSetupError, match="expected"):
        seal_servers({"postgresql_servers": {"value": {"r2": _server("r2")}}})


@pytest.mark.parametrize("log, retry", [
    ("Error: creating Firewall Rule: ServerIsBusy: the server is busy", True),
    ("│ Error: deleting Server: PrivateEndpointServerWithActiveConnectionCannotBeDropped", True),
    ("Error: ServerIsBusy\n\nError: creating Storage Account: StorageAccountAlreadyTaken", False),
    ("Apply complete! Resources: 0 added, 0 changed, 0 destroyed.", False),
])
def test_a_run_is_asked_again_only_when_every_error_is_one_a_second_run_gets_past(
    log: str, retry: bool
) -> None:
    assert only_retryable(log) is retry


class FakeRun:
    """Terraform's answers in order; every call is recorded, with no environment secret kept."""

    def __init__(self, answers: list[tuple[int, str]]) -> None:
        self.answers = list(answers)
        self.calls: list[list[str]] = []

    def __call__(self, args, **kwargs) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(args))
        assert kwargs["env"]["TF_VAR_admin_password"] == "pw"
        code, out = self.answers.pop(0) if self.answers else (0, "")
        return subprocess.CompletedProcess(args, code, out, "")


def _terraform(tmp_path, answers, **changes):
    generation = Generation(tmp_path)
    generation.ensure()
    generation.admin_password.write_text("pw")
    run = FakeRun(answers)
    slept: list[float] = []
    terraform = Terraform(generation, inputs(), run=run, sleep=slept.append,
                          **{"echo": lambda _: None, **changes})
    return terraform, run, slept


#: Terraform's own words, from the runners' refused create (2026-10-20, 7:18 PM) and the apply
#: after it, with placeholders for the subscription and names, and long lines wrapped.
REFUSED_LOG = """
Error: creating Linux Virtual Machine (Subscription: "00000000-1111-2222-3333-444444444444"
Resource Group Name: "rg-test"
Virtual Machine Name: "vm-t-r5-runner-1"): polling after CreateOrUpdate: polling failed:
the Azure API returned the following error:

Status: "ZonalAllocationFailed"
Message: "Allocation failed. We do not have sufficient capacity for the requested VM size in
this zone."
"""
LEFT_BEHIND_LOG = (
    'Error: a resource with the ID "/subscriptions/00000000-1111-2222-3333-444444444444/'
    'resourceGroups/rg-test/providers/Microsoft.Compute/virtualMachines/vm-t-r5-runner-0" already '
    "exists - to be managed via Terraform this resource needs to be imported into the State.")


@pytest.mark.parametrize("log, retry", [
    (REFUSED_LOG, True),
    (LEFT_BEHIND_LOG, True),
    ('Error: a resource with the ID "/subscriptions/x/resourceGroups/rg-test/providers/'
     'Microsoft.Storage/storageAccounts/sttest" already exists', False),
    (REFUSED_LOG + "\nError: creating Storage Account: StorageAccountAlreadyTaken", False),
])
def test_an_apply_is_asked_again_when_azure_had_no_room_for_a_vm(log: str, retry: bool) -> None:
    assert only_retryable(log, also=VM_REFUSED) is retry
    assert only_retryable(log) is False  # a destroy never meets them, and doesn't retry them


def test_a_runner_azure_had_no_room_for_is_cleared_and_made_again(tmp_path) -> None:
    lines: list[str] = []
    cleared = [[], ["vm-t-r5-runner-1"]]  # what each attempt's clear found
    terraform, run, slept = _terraform(tmp_path, [
        (0, "initialized"), (2, "Plan: 3 to add"), (1, REFUSED_LOG), (0, "Apply complete!")],
        echo=lines.append, clear_refused=lambda: cleared.pop(0))
    terraform.apply()
    assert [call[1] for call in run.calls] == ["init", "plan", "apply", "apply"]
    assert slept == [60.0] and cleared == []
    assert any(line.startswith("WAIT  Azure had no room") for line in lines)
    assert any(line.startswith("CLEAR vm-t-r5-runner-1:") for line in lines)


def test_a_zone_with_no_room_through_every_attempt_says_to_run_it_again_later(tmp_path) -> None:
    terraform, run, _ = _terraform(tmp_path, [(0, "initialized"), (2, "Plan: 3 to add")] + [
        (1, REFUSED_LOG)] * TERRAFORM_ATTEMPTS)
    with pytest.raises(AzureSetupError, match="Run the same command again later"):
        terraform.apply()
    assert [call[1] for call in run.calls].count("apply") == TERRAFORM_ATTEMPTS


class FakeArm:
    """The group's VMs as ARM lists them, and every DELETE sent; deletes finish at once."""

    resource_group_path = f"/subscriptions/{SUB}/resourceGroups/rg-test"

    def __init__(self, vms: list[dict[str, Any]]) -> None:
        self.vms, self.deleted = vms, []

    def request(self, method: str, url: str, body: Any = None, **_: Any) -> ArmResponse:
        if method == "GET":
            assert "/providers/Microsoft.Compute/virtualMachines?api-version=" in url
            return ArmResponse(200, {"value": self.vms}, {})
        assert method == "DELETE"
        self.deleted.append(url.split("?", 1)[0].rsplit("/", 1)[-1])
        return ArmResponse(202, None, {"Azure-AsyncOperation": "https://operation"})

    def wait_operation(self, response: ArmResponse, *, bound_seconds: float) -> tuple[str, Any]:
        return "Succeeded", {}


def _vm(name: str, state: str, *, run: str = "az-20990101-0000-test") -> dict[str, Any]:
    group = f"/subscriptions/{SUB}/resourceGroups/rg-test/providers"
    return {"id": f"{group}/Microsoft.Compute/virtualMachines/{name}", "name": name,
            "tags": {"Anti-Demo-Run-Id": run},
            "properties": {"provisioningState": state, "storageProfile": {"osDisk": {
                "managedDisk": {"id": f"{group}/Microsoft.Compute/disks/{name}-osdisk"}}}}}


def test_only_a_refused_runner_outside_terraforms_state_is_deleted_with_its_disk() -> None:
    arm = FakeArm([
        _vm("vm-t-r5-runner-0", "Failed"),                            # refused: cleared
        _vm("vm-t-r5-runner-1", "Failed"),                            # Terraform's: kept
        _vm("vm-t-r5-runner-2", "Succeeded"),                         # running: kept
        _vm("vm-t-r5-runner-3", "Failed", run="az-20990101-0000-x"),  # another run's: kept
        _vm("vm-t-other", "Failed"),                                  # not a runner: kept
    ])
    managed = frozenset({'azurerm_linux_virtual_machine.runner["runner-1"]'})
    cleared = clear_refused_runners(arm, "az-20990101-0000-test", lambda: managed)  # type: ignore[arg-type]
    assert cleared == ["vm-t-r5-runner-0"]
    assert arm.deleted == ["vm-t-r5-runner-0", "vm-t-r5-runner-0-osdisk"]


def test_with_no_failed_runner_terraforms_state_is_never_read() -> None:
    arm = FakeArm([_vm("vm-t-r5-runner-0", "Succeeded")])

    def managed() -> frozenset[str]:
        raise AssertionError("read the state with nothing to clear")

    assert clear_refused_runners(arm, "az-20990101-0000-test", managed) == []  # type: ignore[arg-type]
    assert arm.deleted == []


def test_terraform_applies_again_after_a_busy_server_and_then_succeeds(tmp_path) -> None:
    terraform, run, slept = _terraform(tmp_path, [
        (0, "Terraform has been successfully initialized"),
        (2, "Plan: 158 to add, 0 to change, 0 to destroy."),
        (1, "Error: creating Firewall Rule: ServerIsBusy"),
        (0, "Apply complete!")])
    terraform.apply()
    assert [call[1] for call in run.calls] == ["init", "plan", "apply", "apply"]
    assert slept == [60.0]


def test_terraform_stops_at_once_on_any_other_error_and_names_a_zone_remedy(tmp_path) -> None:
    terraform, run, slept = _terraform(tmp_path, [
        (0, "initialized"), (2, "Plan: 1 to add"),
        (1, "Error: creating Server: SkuNotAvailable: not in zone 3")])
    with pytest.raises(AzureSetupError, match="Set AZURE_ZONE to another"):
        terraform.apply()
    assert [call[1] for call in run.calls] == ["init", "plan", "apply"] and slept == []


def test_terraform_gives_up_after_its_attempts(tmp_path) -> None:
    terraform, run, _ = _terraform(tmp_path, [(0, "initialized"), (2, "Plan: 1 to add")] + [
        (1, "Error: ServerIsBusy")] * TERRAFORM_ATTEMPTS)
    with pytest.raises(AzureSetupError, match="terraform apply failed"):
        terraform.apply()
    assert [call[1] for call in run.calls].count("apply") == TERRAFORM_ATTEMPTS


def test_matching_state_is_left_alone(tmp_path) -> None:
    terraform, run, _ = _terraform(tmp_path, [(0, "initialized"), (0, "No changes.")])
    terraform.apply()
    assert [call[1] for call in run.calls] == ["init", "plan"]


def test_the_state_and_variables_are_the_generations(tmp_path) -> None:
    terraform, run, _ = _terraform(tmp_path, [
        (0, "initialized"), (2, "Plan: 1 to add"), (0, "Apply complete!")])
    terraform.apply()
    init, _plan, apply = run.calls
    assert f"-backend-config=path={tmp_path / 'terraform.tfstate'}" in init
    assert f"-var-file={tmp_path / 'terraform.tfvars.json'}" in apply


def test_an_admin_password_meets_azures_rule_and_is_never_the_same() -> None:
    first, second = new_admin_password(), new_admin_password()
    assert first != second and len(first) == 32
    assert any(c.isupper() for c in first) and any(c.islower() for c in first)
    assert any(c.isdigit() for c in first) and not first.isalnum()


def _environment(**changes: str) -> dict[str, str]:
    values = {"DATABRICKS_PROFILE": "anti-demo-azure-test", "AZURE_TENANT_ID": TENANT,
              "AZURE_SUBSCRIPTION_ID": SUB, "AZURE_RESOURCE_GROUP": "rg-test",
              "AZURE_REGION": "westus2"}
    return {**values, **changes}


def test_the_inputs_come_from_the_environment(monkeypatch) -> None:
    environment = _environment(AZURE_POLICY_TAGS="cost-center=demo, team = fe",
                               AZURE_LEASE_DATE_TAG="review-by", AZURE_ZONE="2")
    monkeypatch.setattr("server.lifecycle._require_operator_cidr", lambda: "192.0.2.10/32")
    read = inputs_from_environment(environment)
    assert (read.region, read.zone, read.operator_cidr) == ("westus2", "2", "192.0.2.10/32")
    assert read.policy_tags == {"cost-center": "demo", "team": "fe"}
    assert read.lease_date_tag == "review-by"


def test_the_inputs_refuse_a_missing_setting() -> None:
    with pytest.raises(AzureSetupError, match="needs AZURE_REGION"):
        inputs_from_environment(_environment(AZURE_REGION=""))


def test_an_ambient_aws_setting_is_not_an_input(monkeypatch) -> None:
    # bootstrap.sh refuses one in the env file; one the operator's shell exported is ignored.
    monkeypatch.setattr("server.lifecycle._require_operator_cidr", lambda: "192.0.2.10/32")
    read = inputs_from_environment(_environment(AWS_PROFILE="sandbox"))
    assert not [name for name in vars(read) if "aws" in name.lower()]


class FakeApps:
    """`databricks apps get` and `create` as `subprocess.run` answers. A create makes the App, then
    exits non-zero if `create_fails`, as it did on 2026-10-22."""

    def __init__(self, *, create_fails: bool, made_by: str = "operator@example.com",
                 made_at: str = "2026-10-19T21:30:00Z") -> None:
        self.app: dict[str, Any] | None = None
        self.create_fails, self.made_by, self.made_at = create_fails, made_by, made_at
        self.calls: list[str] = []

    def __call__(self, arguments: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        verb = arguments[2]
        self.calls.append(verb)
        if verb == "get":
            if self.app is None:
                return subprocess.CompletedProcess(arguments, 1, "", "Error: does not exist")
            return subprocess.CompletedProcess(arguments, 0, json.dumps(self.app), "")
        assert verb == "create"
        self.app = {"name": arguments[3], "creator": self.made_by, "create_time": self.made_at,
                    "description": arguments[5], "service_principal_client_id": "app-client-1"}
        if self.create_fails:
            return subprocess.CompletedProcess(
                arguments, 1, "", "Error: the App's compute is starting\nTo view app logs, run:\n"
                "  databricks apps logs anti-demo-test")
        return subprocess.CompletedProcess(arguments, 0, json.dumps(self.app), "")


def test_an_app_whose_create_failed_after_making_it_is_adopted(tmp_path, monkeypatch) -> None:
    # 2026-10-22: `apps create` exited non-zero about 25 s in, while the App's compute was still
    # starting, and the compute came up by itself. The install's rerun then refused the App as a
    # stranger's, and an uninstall would have left it running.
    from server.azure_lifecycle import ensure_app

    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    apps = FakeApps(create_fails=True)
    monkeypatch.setattr("server.azure_lifecycle.subprocess.run", apps)
    assert ensure_app(generation, "p", "anti-demo-test", manifest()) == "app-client-1"
    assert json.loads((generation.root / "app.json").read_text()) == {
        "name": "anti-demo-test", "created": True, "client_id": "app-client-1"}
    # Run again, it is this installation's by its record, and nothing else is made.
    assert ensure_app(generation, "p", "anti-demo-test", manifest()) == "app-client-1"
    assert apps.calls.count("create") == 1


@pytest.mark.parametrize(("made_by", "made_at"), [
    ("someone@example.com", "2026-10-19T21:30:00Z"),  # another user's
    ("operator@example.com", "2026-10-18T09:00:00Z"),  # from before this installation was sealed
])
def test_an_app_this_installation_cannot_prove_it_made_is_never_adopted(
    tmp_path, monkeypatch, made_by: str, made_at: str,
) -> None:
    from server.azure_lifecycle import ensure_app

    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    monkeypatch.setattr("server.azure_lifecycle.subprocess.run",
                        FakeApps(create_fails=True, made_by=made_by, made_at=made_at))
    with pytest.raises(AzureSetupError, match="could not create the Databricks App anti-demo-test"):
        ensure_app(generation, "p", "anti-demo-test", manifest())
    with pytest.raises(AzureSetupError, match="this installation didn't make it"):
        ensure_app(generation, "p", "anti-demo-test", manifest())


def test_an_install_resumes_itself_after_a_transient_error_and_stops_on_any_other(
    monkeypatch, capsys,
) -> None:
    # 2026-10-22: a Lakebase INTERNAL_ERROR, a CLI call's EOF and an expired token stopped one
    # night's installs at three different steps. Each stage resumes where a run stopped, so the
    # install resumes itself, as running it again would.
    from server.azure_lifecycle import SETUP_ATTEMPTS, AzureSetup

    outcomes: list[Any] = []
    calls: list[int] = []

    def run_once(self: Any) -> Any:
        calls.append(1)
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(AzureSetup, "_run_once", run_once)
    monkeypatch.setattr("server.azure_lifecycle.time.sleep", lambda seconds: None)
    setup = AzureSetup(inputs=inputs())

    outcomes[:] = [RuntimeError(
        'Error: Patch "https://example.test/api/2.0/postgres/projects/p/branches/production/'
        'endpoints/primary?update_mask=spec.suspension": EOF'), "installed"]
    assert setup.run() == "installed" and len(calls) == 2
    assert "RESUME the run stopped on a transient error" in capsys.readouterr().out

    # A refusal that isn't transient stops it at once.
    calls.clear()
    outcomes[:] = [AzureSetupError("a Databricks App named x already exists and this "
                                   "installation didn't make it"), "installed"]
    with pytest.raises(AzureSetupError, match="didn't make it"):
        setup.run()
    assert len(calls) == 1

    # And a transient one that never clears stops it after its attempts.
    calls.clear()
    outcomes[:] = [RuntimeError("Error: INTERNAL_ERROR: Unhandled error in API call")] * (
        SETUP_ATTEMPTS)
    with pytest.raises(RuntimeError, match="INTERNAL_ERROR"):
        setup.run()
    assert len(calls) == SETUP_ATTEMPTS


def test_a_workspace_path_that_never_existed_reads_absent_and_a_refusal_does_not() -> None:
    # 2026-10-22: an install that had made its App, but not yet synced the App's source, stopped
    # its uninstall at "Path (...) doesn't exist.", the SDK's ResourceDoesNotExist.
    from databricks.sdk.errors import PermissionDenied, ResourceDoesNotExist

    from server.azure_lifecycle import _get_or_none

    def missing() -> Any:
        raise ResourceDoesNotExist(
            "Path (/Workspace/Users/operator@example.com/anti-demo-test) doesn't exist.")

    def missing_in_words() -> Any:  # the same answer, as plain text
        raise RuntimeError("Path (/Workspace/Users/operator@example.com/x) doesn't exist.")

    def refused() -> Any:
        raise PermissionDenied("User does not have READ permission on the path")

    assert _get_or_none(missing) is None
    assert _get_or_none(missing_in_words) is None
    with pytest.raises(PermissionDenied):
        _get_or_none(refused)


def test_a_manifest_and_its_variables_round_trip_through_the_generation(tmp_path) -> None:
    from server.manifest import load_manifest, save_manifest

    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    path = save_manifest(manifest(), generation.manifest)
    generation.tfvars.write_text(json.dumps(terraform_vars(
        manifest(), inputs(), ssh_public_key="ssh-ed25519 AAAA test")))
    assert load_manifest(path).azure == manifest().azure
    assert json.loads(generation.tfvars.read_text())["run_id"] == "az-20990101-0000-test"
