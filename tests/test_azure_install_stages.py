"""The Azure installer's post-READY stages against fakes: what each makes, and in what order.

Unity Catalog's objects, a lane's proof run, and the App's secrets and resources. Nothing here
reaches Azure or Databricks; the live install is the release bar's.
"""

from __future__ import annotations

import base64
import subprocess
from types import SimpleNamespace
from typing import Any

import pytest
from test_manifest_v8 import azure_manifest

from server import lifecycle
from server.azure_deploy import (
    ADMIN_PASSWORD_SECRET,
    MANIFEST_SECRET,
    bind_resources,
    publish_secrets,
    secret_scope_name,
)
from server.azure_lanes_setup import AzureLaneSetupError, make_source_readable, prove
from server.azure_lifecycle import ensure_unity_catalog, uc_names
from server.azure_runtime import ROUND4_SOURCE_PROPERTIES

GROUP = "/subscriptions/00000000-1111-2222-3333-444444444444/resourceGroups/rg-test/providers/"
OUTPUTS = {
    "access_connector": {"value": {"id": f"{GROUP}Microsoft.Databricks/accessConnectors/ac-uc"}},
    "app_access_connector": {
        "value": {"id": f"{GROUP}Microsoft.Databricks/accessConnectors/ac-app"}},
}


class NotFound(Exception):
    """What the SDK raises for an absent object, by name."""


class Collection:
    def __init__(self, calls: list[tuple[Any, ...]], kind: str) -> None:
        self.calls, self.kind, self.items = calls, kind, set()

    def get(self, name: str) -> Any:
        if name not in self.items:
            raise NotFound(f"{self.kind} {name} does not exist")
        return {"name": name}

    def get_credential(self, name: str) -> Any:
        return self.get(name)

    def create(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((self.kind, name, args, kwargs))
        self.items.add(name)
        return {"name": name}

    def create_credential(self, *, name: str, **kwargs: Any) -> Any:
        return self.create(name, **kwargs)


class Workspace:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.storage_credentials = Collection(self.calls, "storage_credential")
        self.external_locations = Collection(self.calls, "external_location")
        self.catalogs = Collection(self.calls, "catalog")
        self.credentials = Collection(self.calls, "credential")


def test_unity_catalog_is_made_on_the_installations_own_adls_and_sealed() -> None:
    manifest = azure_manifest()
    workspace = Workspace()
    ensure_unity_catalog(manifest, OUTPUTS, workspace)
    kinds = [call[0] for call in workspace.calls]
    # The credential first, every container's location on it, the catalog, then the App's own.
    assert kinds == ["storage_credential", *["external_location"] * 4, "catalog", "credential"]
    account = manifest.azure.storage_account
    urls = {call[2][0] for call in workspace.calls if call[0] == "external_location"}
    assert urls == {f"abfss://{container}@{account}.dfs.core.windows.net/"
                    for container in ("r4source", "r6history", "change-feed", "uc-managed")}
    catalog = next(call for call in workspace.calls if call[0] == "catalog")
    assert catalog[3]["storage_root"] == f"abfss://uc-managed@{account}.dfs.core.windows.net/catalog"
    app = next(call for call in workspace.calls if call[0] == "credential")
    assert app[3]["azure_managed_identity"].access_connector_id.endswith("/ac-app")
    sealed = manifest.azure.unity_catalog
    names = uc_names(manifest)
    assert sealed is not None and sealed.catalog == names["catalog"]
    assert sealed.app_service_credential == names["app_service_credential"]
    assert sealed.external_locations["r4source"] == names["r4_source"]


def test_unity_catalog_objects_already_there_are_kept() -> None:
    manifest = azure_manifest()
    workspace = Workspace()
    ensure_unity_catalog(manifest, OUTPUTS, workspace)
    made = len(workspace.calls)
    ensure_unity_catalog(manifest, OUTPUTS, workspace)
    assert len(workspace.calls) == made


class Job:
    def __init__(self, states: list[str], message: str = "") -> None:
        self.states, self.parked, self.message = list(states), 0, message

    def start(self, parameters: Any) -> str:
        self.parameters = parameters
        return "run-1"

    def run(self, run_id: str) -> Any:
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return type("Run", (), {"state": state, "message": self.message})()

    def park(self) -> None:
        self.parked += 1


def test_a_proof_run_is_waited_to_its_end() -> None:
    job = Job(["Queued", "InProgress", "Succeeded"])
    assert prove(job, {"version": 3}, "Round 4", sleep=lambda _: None) == "run-1"  # type: ignore[arg-type]
    assert job.parameters == {"version": 3}


@pytest.mark.parametrize("state", ["Failed", "Cancelled"])
def test_a_proof_run_that_did_not_succeed_seals_nothing(state: str) -> None:
    with pytest.raises(AzureLaneSetupError, match=f"ended {state}"):
        prove(Job([state]), None, "Round 6", sleep=lambda _: None)  # type: ignore[arg-type]


def test_a_failed_proof_run_names_data_factorys_reason() -> None:
    # The 2026-10-19 install's first Round 4 run, as Data Factory reported it.
    reason = ("Operation on target move failed: Unable to read table because it requires reader "
              "table feature(s) that are unsupported by this version of Delta Lake 2.4")
    with pytest.raises(AzureLaneSetupError) as caught:
        prove(Job(["Failed"], reason), None, "Round 4", sleep=lambda _: None)  # type: ignore[arg-type]
    assert str(caught.value) == f"Round 4's proof run run-1 ended Failed: {reason}"


def test_a_proof_run_past_its_bound_is_parked() -> None:
    moments = iter([0.0, 0.0, 10_000.0])
    job = Job(["InProgress"])
    with pytest.raises(AzureLaneSetupError, match="still InProgress"):
        prove(job, None, "Round 6", sleep=lambda _: None,  # type: ignore[arg-type]
              clock=lambda: next(moments))
    assert job.parked == 1


class Secrets:
    def __init__(self) -> None:
        self.scopes: list[str] = []
        self.put: dict[tuple[str, str], str] = {}

    def list_scopes(self) -> list[Any]:
        return [type("Scope", (), {"name": name})() for name in self.scopes]

    def create_scope(self, scope: str) -> None:
        self.scopes.append(scope)

    def put_secret(self, scope: str, key: str, *, string_value: str) -> None:
        self.put[(scope, key)] = string_value


def test_the_secrets_are_the_manifest_and_the_password_and_the_app_reads_them_alone() -> None:
    manifest = azure_manifest()
    secrets = Secrets()
    updates: list[Any] = []
    apps = type("Apps", (), {"update": lambda _self, name, app: updates.append((name, app))})()
    workspace = type("Workspace", (), {"secrets": secrets, "apps": apps})()
    scope = secret_scope_name("lakebase-anti-demo", manifest.run_id)
    publish_secrets(workspace, scope, manifest, "pw")
    assert secrets.scopes == [scope]
    assert set(secrets.put) == {(scope, MANIFEST_SECRET), (scope, ADMIN_PASSWORD_SECRET)}
    assert '"cloud":"azure"' in secrets.put[(scope, MANIFEST_SECRET)]
    bind_resources(workspace, "lakebase-anti-demo", scope)
    (name, app), = updates
    assert name == "lakebase-anti-demo"
    assert sorted(resource.name for resource in app.resources) == sorted(
        [MANIFEST_SECRET, ADMIN_PASSWORD_SECRET])
    assert {resource.secret.permission.value for resource in app.resources} == {"READ"}


@pytest.mark.parametrize("arrived", [True, False])
def test_the_deploy_syncs_the_built_frontend_and_asks_the_workspace_it_arrived(
    monkeypatch, tmp_path, arrived: bool
) -> None:
    from server import azure_deploy

    for name in ("publish_secrets", "bind_resources", "build_frontend", "build_stamp"):
        monkeypatch.setattr(azure_deploy, name, lambda *args, **kwargs: None)
    staged = tmp_path / "stage" / "src"
    staged.mkdir(parents=True)
    monkeypatch.setattr(azure_deploy, "stage_source", lambda *args, **kwargs: staged)
    calls: list[list[str]] = []

    def run(args: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, '{"url": "https://app.example"}', "")

    asked: list[list[str]] = []

    def status(args: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        asked.append(args)
        return subprocess.CompletedProcess(args, 0 if arrived else 1, "", "")

    monkeypatch.setattr(azure_deploy, "_run", run)
    monkeypatch.setattr(azure_deploy.subprocess, "run", status)

    def deploy() -> str:
        return azure_deploy.deploy(object(), profile="p", app_name="app", manifest=azure_manifest(),
                                   admin_password="pw", principal="operator@example.com")

    if arrived:
        assert deploy() == "https://app.example"
    else:
        with pytest.raises(azure_deploy.AzureDeployError, match="no frontend/dist/index.html"):
            deploy()
    sync = next(call for call in calls if call[:2] == ["databricks", "sync"])
    assert sync[sync.index("--include") + 1] == "frontend/dist"
    index = "/Workspace/Users/operator@example.com/app/frontend/dist/index.html"
    assert asked == [["databricks", "workspace", "get-status", index, "-p", "p"]]
    # A tree without the frontend is never deployed.
    assert any(call[:3] == ["databricks", "apps", "deploy"] for call in calls) is arrived


class Warehouse:
    """The SQL warehouse as `lifecycle._sql_statement` reaches it: one table's features."""

    def __init__(self, features: list[str]) -> None:
        self.features, self.statements = list(features), []

    def statement(self, profile: str, warehouse_id: str, statement: str) -> Any:
        self.statements.append(statement)
        if statement.startswith("ALTER TABLE"):
            self.features.remove(statement.rsplit(" ", 1)[1])
        return [{"key": f"delta.feature.{name}", "value": "supported"} for name in self.features]


def test_round4s_source_loses_only_the_features_data_factory_cant_read(monkeypatch) -> None:
    # A new managed table's features in the metastore of the first install (2026-10-19).
    warehouse = Warehouse(["appendOnly", "catalogManaged", "deletionVectors", "rowTracking",
                           "vacuumProtocolCheck"])
    monkeypatch.setattr(lifecycle, "_sql_statement", warehouse.statement)
    monkeypatch.setattr(lifecycle, "_sql_rows", lambda rows: rows)
    table = "cat.sch.model_scores_source"
    assert make_source_readable("p", "w", table) == ("catalogManaged", "vacuumProtocolCheck")
    # catalogManaged first, the order tested live; deletion vectors stay, which ADF reads.
    assert warehouse.statements[1:] == [
        "ALTER TABLE `cat`.`sch`.`model_scores_source` DROP FEATURE catalogManaged",
        "ALTER TABLE `cat`.`sch`.`model_scores_source` DROP FEATURE vacuumProtocolCheck"]
    assert warehouse.features == ["appendOnly", "deletionVectors", "rowTracking"]
    # Once it's readable, nothing more is committed to it.
    warehouse.statements.clear()
    assert make_source_readable("p", "w", table) == ()
    assert warehouse.statements == ["SHOW TBLPROPERTIES `cat`.`sch`.`model_scores_source`"]


def repair_notebook(monkeypatch, manifest: Any) -> str:
    """The notebook Round 4's repair job is given, caught as it's imported."""

    class Imported(Exception):
        pass

    def api(profile: str, method: str, path: str, body: Any = None, **_: Any) -> Any:
        if path == "/api/2.0/workspace/import":
            raise Imported(base64.b64decode(body["content"]).decode())
        return {}

    monkeypatch.setattr(lifecycle, "_databricks_api", api)
    with pytest.raises(Imported) as caught:
        lifecycle._ensure_round4_source_repair_job(
            manifest, {"source_table": "cat.sch.model_scores_source"},
            setup_principal="operator@example.com", app_client_id="app-client")
    source = str(caught.value)
    compile(source, "round4-source-repair", "exec")
    return source


def test_the_repair_rebuilds_azures_source_as_data_factory_reads_it(monkeypatch) -> None:
    source = repair_notebook(monkeypatch, azure_manifest())
    assert f"TBLPROPERTIES ({ROUND4_SOURCE_PROPERTIES})" in source
    # Dropped once the table is rebuilt, before its baseline row is written.
    assert (source.index("CREATE OR REPLACE TABLE") < source.index("DROP FEATURE")
            < source.index("INSERT INTO"))


def test_the_repair_on_aws_rebuilds_the_source_as_it_always_has(monkeypatch) -> None:
    aws = SimpleNamespace(installation_id="018f6f50-7d3a-4cc1-9d5d-4d9ac8d107a1",
                          run_id="run-test", databricks=SimpleNamespace(profile="p"), round4=None)
    source = repair_notebook(monkeypatch, aws)
    assert ("TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')\"\"\")\n"
            "    spark.sql(f\"\"\"INSERT INTO") in source
    assert "DROP FEATURE" not in source and "checkpointPolicy" not in source
