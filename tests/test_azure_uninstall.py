"""server/azure_uninstall.py's dry run: it lists only what is there, and removes nothing.

The live `--yes` path is the release bar's (design section 13): it deletes real resources. This
holds the part an operator runs first, against fakes for the workspace, ARM and the CLI.
"""

from __future__ import annotations

import json
from typing import Any

from test_manifest_v8 import azure_manifest

from server import azure_uninstall
from server.azure_lifecycle import AzureInputs, Generation, inputs_from_manifest
from server.azure_uninstall import AzureUninstall
from server.manifest import save_manifest


class Missing(Exception):
    """The SDK's NotFound, as `_get_or_none` reads it: by its words."""


def _absent(*_args: Any, **_kwargs: Any) -> Any:
    raise Missing("does not exist")


class Workspace:
    """Nothing in Unity Catalog yet, and no secret scope: the state before a first install's UC."""

    def __init__(self) -> None:
        self.deleted: list[str] = []
        for name in ("catalogs", "external_locations", "storage_credentials", "credentials",
                     "secrets"):
            setattr(self, name, type(name, (), {
                "get": staticmethod(_absent), "get_credential": staticmethod(_absent),
                "list_secrets": staticmethod(_absent),
                "delete": staticmethod(lambda *a, **k: self.deleted.append(str(a))),
            })())


def test_a_dry_run_lists_only_what_is_there_and_removes_nothing(tmp_path, monkeypatch) -> None:
    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    manifest = azure_manifest()
    save_manifest(manifest, generation.manifest)
    (generation.root / "app.json").write_text(json.dumps(
        {"name": "lakebase-anti-demo", "created": True, "client_id": "app-client"}))
    workspace = Workspace()
    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda **_k: workspace)
    monkeypatch.setattr(azure_uninstall, "arm_client", lambda _inputs: object())
    monkeypatch.setattr(azure_uninstall, "inventory", lambda _arm, _run: [])
    present = {manifest.round_lakebase(1).project_id}
    monkeypatch.setattr(
        "server.lifecycle._get_lakebase_project_or_none",
        lambda _manifest, project_id: {"name": project_id} if project_id in present else None)
    inputs = AzureInputs(databricks_profile="p", tenant_id="t", subscription_id="s",
                         resource_group="rg-test", region="westus2", owner="o",
                         operator_cidr="192.0.2.10/32")
    report = AzureUninstall(inputs, generation).run(yes=False)
    assert report.found == [
        "Databricks App lakebase-anti-demo",
        f"Lakebase project {manifest.round_lakebase(1).project_id}",
    ]
    assert report.removed == [] and workspace.deleted == []
    assert generation.manifest.exists()


def test_the_uninstaller_takes_the_settings_its_installation_sealed(tmp_path) -> None:
    # `./antidemo azure-cleanup` refused to run ("needs DATABRICKS_PROFILE, AZURE_TENANT_ID, ..."):
    # it read the installer's environment, which only bootstrap.sh exports (2026-10-19).
    manifest = azure_manifest()
    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    generation.tfvars.write_text(json.dumps({"zone": "2", "lease_date_tag": "RemoveAfter"}),
                                 encoding="utf-8")
    (generation.root / "app.json").write_text(
        json.dumps({"name": "anti-demo-test", "created": True, "client_id": "app-client"}),
        encoding="utf-8")
    inputs = inputs_from_manifest(manifest, generation)
    assert inputs.databricks_profile == manifest.databricks.profile
    assert (inputs.tenant_id, inputs.subscription_id) == (
        manifest.azure.tenant_id, manifest.azure.subscription_id)
    assert (inputs.resource_group, inputs.region) == (
        manifest.azure.resource_group, manifest.azure.region)
    assert inputs.operator_cidr == manifest.azure.operator_cidr
    assert (inputs.zone, inputs.lease_date_tag, inputs.app_name) == (
        "2", "RemoveAfter", "anti-demo-test")


def test_the_installers_factory_objects_go_before_terraforms_in_reference_order() -> None:
    # The first uninstall's `terraform destroy` stopped on "The document cannot be deleted since it
    # is referenced by r4_move": the installer's data flow named Terraform's ADLS linked service.
    deleted: list[str] = []

    class Arm:
        def factory_path(self, factory: str) -> str:
            return f"/factories/{factory}"

        def request(self, method: str, path: str) -> Any:
            assert method == "DELETE"
            deleted.append(path.split("?")[0].removeprefix("/factories/adf-test/"))
            # One already gone (an install that stopped early) is as good as deleted.
            return type("Response", (), {"status": 204 if "r6" in path else 200})()

    azure_uninstall.remove_factory_objects(Arm(), "adf-test")
    assert deleted == [
        "pipelines/r4_pipeline", "pipelines/r6_pipeline",
        "dataflows/r4_move", "dataflows/r6_extract",
        "linkedservices/ls-postgres-r4", "linkedservices/ls-postgres-r6",
    ]


def test_an_uninstall_run_again_skips_what_an_earlier_run_removed(tmp_path, monkeypatch) -> None:
    # The second and third runs on 2026-10-19 stopped on Round 6's cleanup refusing an environment
    # whose project was gone, then on deleting a project that wasn't there.
    from test_manifest import _round5_round4_resources, _v7_round6

    base = azure_manifest()
    lakebase4 = base.round_lakebase(4)
    round4 = _round5_round4_resources().model_copy(update={
        "project_uid": lakebase4.project_uid, "branch_uid": lakebase4.branch_uid,
        "branch": lakebase4.branch_name, "endpoint_name": lakebase4.endpoint_name})
    manifest = azure_manifest(round4=round4, round6=_v7_round6(base.round_lakebase(6)))
    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    save_manifest(manifest, generation.manifest)
    workspace = Workspace()
    deleted_pipelines: list[str] = []
    workspace.pipelines = type("Pipelines", (), {
        "get": staticmethod(lambda pipeline_id: {"pipeline_id": pipeline_id}),
        "delete": staticmethod(deleted_pipelines.append),
    })()

    def never(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a step an earlier run finished was asked again")

    async def settled(*_args: Any, **_kwargs: Any) -> None:
        return None

    class Arm:
        def factory_path(self, factory: str) -> str:
            return f"/factories/{factory}"

        def request(self, *_args: Any, **_kwargs: Any) -> Any:
            return type("Response", (), {"status": 204, "body": {}})()

    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda **_k: workspace)
    monkeypatch.setattr(azure_uninstall, "arm_client", lambda _inputs: Arm())
    monkeypatch.setattr(azure_uninstall, "inventory", lambda _arm, _run: [])
    monkeypatch.setattr(azure_uninstall, "settle", settled)
    monkeypatch.setattr(azure_uninstall, "release_private_endpoints", lambda *a, **k: None)
    monkeypatch.setattr(AzureUninstall, "_destroy", lambda self: None)
    monkeypatch.setattr("server.lifecycle._get_lakebase_project_or_none",
                        lambda _manifest, project_id: None)
    monkeypatch.setattr("server.lifecycle._delete_lakebase_project", never)
    monkeypatch.setattr("server.lifecycle._inspect_round4_for_cleanup", never)
    monkeypatch.setattr("server.round6_lifecycle.cleanup_round6", never)
    inputs = AzureInputs(databricks_profile="p", tenant_id="t", subscription_id="s",
                         resource_group="rg-test", region="westus2", owner="o",
                         operator_cidr="192.0.2.10/32")
    report = AzureUninstall(inputs, generation).run(yes=True)
    assert deleted_pipelines == [manifest.round4.pipeline_id]
    assert "Round 4's pipeline" in report.removed and report.survivors == []
    # ALL GONE: the generation is archived, never deleted.
    assert not generation.root.exists()
    assert list(tmp_path.glob(".anti-demo-azure.removed-*"))


def test_an_app_whose_create_never_confirmed_is_removed_only_if_provably_this_installations(
    tmp_path, monkeypatch,
) -> None:
    # Its create can fail after making it (2026-10-22), and the record then says only "pending".
    import subprocess
    from datetime import timedelta

    from server.azure_lifecycle import APP_DESCRIPTION

    base = azure_manifest()
    manifest = azure_manifest(
        databricks=base.databricks.model_copy(update={"user": "operator@example.com"}))
    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    (generation.root / "app.json").write_text(
        json.dumps({"name": "lakebase-anti-demo", "creation_pending": True}))
    live = {"name": "lakebase-anti-demo", "creator": "operator@example.com",
            "create_time": (manifest.created_at + timedelta(minutes=20)).isoformat(),
            "description": APP_DESCRIPTION, "service_principal_client_id": "app-client"}
    monkeypatch.setattr(azure_uninstall, "_databricks", lambda _profile, *args, **_k: (
        subprocess.CompletedProcess(args, 0, json.dumps(live), "")))
    inputs = AzureInputs(databricks_profile="p", tenant_id="t", subscription_id="s",
                         resource_group="rg-test", region="westus2", owner="o",
                         operator_cidr="192.0.2.10/32")
    uninstall = AzureUninstall(inputs, generation)
    assert uninstall._app(manifest) == ("lakebase-anti-demo", "app-client")
    live["creator"] = "someone@example.com"  # not this installation's to remove
    assert uninstall._app(manifest) is None


def test_all_gone_lets_azures_lists_catch_up_with_a_finished_delete(monkeypatch) -> None:
    # 2026-10-22: right after `terraform destroy`, the group's list still showed a Round 2 server
    # Terraform had just destroyed, which read ResourceNotFound moments later.
    from server.azure_inventory import Standing

    server = Standing("server-r2", "Microsoft.DBforPostgreSQL/flexibleServers", "run tag")
    answers = [[server], [server], []]
    monkeypatch.setattr(azure_uninstall, "inventory", lambda _arm, _run: answers.pop(0))
    now = [0.0]
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    assert azure_uninstall.standing_after_destroy(object(), "run", sleep=sleep,
                                                  clock=lambda: now[0]) == []
    assert slept == [azure_uninstall.ALL_GONE_POLL_SECONDS] * 2

    # One that really stands is reported once the bound has passed.
    monkeypatch.setattr(azure_uninstall, "inventory", lambda _arm, _run: [server])
    now[0] = 0.0
    assert azure_uninstall.standing_after_destroy(object(), "run", sleep=sleep,
                                                  clock=lambda: now[0]) == [server]
    assert now[0] >= azure_uninstall.ALL_GONE_SETTLE_SECONDS


def test_the_factory_is_found_in_terraforms_state_when_no_lane_sealed(tmp_path) -> None:
    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    assert azure_uninstall._factory_of(generation) is None
    generation.state.write_text(json.dumps(
        {"outputs": {"data_factory": {"value": {"name": "adf-test"}}}}), encoding="utf-8")
    assert azure_uninstall._factory_of(generation) == "adf-test"


def _unsealed_uninstall(
    tmp_path, monkeypatch, *, deletes: bool
) -> tuple[Any, list[str], list[str]]:
    """An uninstall of an install that stopped before it sealed its Lakebase projects, all seven of
    which Lakebase made. `deletes` is whether a delete takes a project away."""

    manifest = azure_manifest(status="provisioning", round_environments=None,
                              coordination_environment=None)
    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    save_manifest(manifest, generation.manifest)
    compact = str(manifest.installation_id).replace("-", "")
    made = [*(f"anti-demo-{compact}-r{number}" for number in range(1, 7)),
            f"anti-demo-{compact}-coord"]
    standing, deleted = set(made), []

    def delete(_manifest: Any, _profile: str, project_id: str) -> None:
        deleted.append(project_id)
        if deletes:
            standing.discard(project_id)

    async def settled(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr("databricks.sdk.WorkspaceClient", lambda **_k: Workspace())
    monkeypatch.setattr(azure_uninstall, "arm_client", lambda _inputs: object())
    monkeypatch.setattr(azure_uninstall, "inventory", lambda _arm, _run: [])
    monkeypatch.setattr(azure_uninstall, "settle", settled)
    monkeypatch.setattr(AzureUninstall, "_destroy", lambda self: None)
    monkeypatch.setattr(
        "server.lifecycle._get_lakebase_project_or_none",
        lambda _manifest, project_id: {"name": project_id} if project_id in standing else None)
    monkeypatch.setattr("server.lifecycle._delete_lakebase_project", delete)
    inputs = AzureInputs(databricks_profile="p", tenant_id="t", subscription_id="s",
                         resource_group="rg-test", region="westus2", owner="o",
                         operator_cidr="192.0.2.10/32")
    return (lambda: AzureUninstall(inputs, generation).run(yes=True)), made, deleted


def test_an_install_that_stopped_before_its_seals_has_its_lakebase_projects_removed(
    tmp_path, monkeypatch
) -> None:
    # 2026-10-22: an install stopped on a transient error as it made the coordination project,
    # before it sealed any. Reading the seals alone, the uninstall would have left all seven
    # standing and said ALL GONE.
    run, made, deleted = _unsealed_uninstall(tmp_path, monkeypatch, deletes=True)
    report = run()
    assert deleted == made
    assert report.survivors == []
    assert list(tmp_path.glob(".anti-demo-azure.removed-*"))


def test_all_gone_needs_the_databricks_side_gone_too(tmp_path, monkeypatch) -> None:
    import pytest

    from server.azure_lifecycle import AzureSetupError

    run, made, _ = _unsealed_uninstall(tmp_path, monkeypatch, deletes=False)
    with pytest.raises(AzureSetupError, match="7 resources of run .* survive: not ALL GONE"):
        run()
    # Not archived: the next run finds the installation and asks again.
    assert (tmp_path / ".anti-demo-azure" / "manifest.json").exists()
    assert not list(tmp_path.glob(".anti-demo-azure.removed-*"))
