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


def test_the_factory_is_found_in_terraforms_state_when_no_lane_sealed(tmp_path) -> None:
    generation = Generation(tmp_path / ".anti-demo-azure")
    generation.ensure()
    assert azure_uninstall._factory_of(generation) is None
    generation.state.write_text(json.dumps(
        {"outputs": {"data_factory": {"value": {"name": "adf-test"}}}}), encoding="utf-8")
    assert azure_uninstall._factory_of(generation) == "adf-test"
