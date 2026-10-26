"""The Azure edition's uninstall (design section 10): what the installation made, removed in the
reverse of its making, then proof that none of it is left (ALL GONE).

`./antidemo azure-cleanup` lists what it would remove; `--yes` removes it. Every object is found by
the name the manifest seals or the run ID derives, never by a pattern a stranger's could match:

1. **The per-bout leftovers** (`azure_cleanup.settle`): both data flows parked, and Rounds 2 and
   3's copies deleted.
2. **Databricks:** the App this installation made and its source; Round 6's change feed and
   schemas (`round6_lifecycle.cleanup_round6`); Round 4's synced table, pipeline and schemas; the
   seven Lakebase projects; the catalog, the external locations and both credentials; the
   secret scope.
3. **Azure:** the factory's private endpoints released (a server can't go while one holds it), then
   `terraform destroy`, asked again on the refusals a second run gets past.
4. **ALL GONE:** nothing in the subscription carries the run ID, and no Databricks object named for
   it survives. Only then is the generation archived, never deleted: its record outlives it.

The resource group is never deleted. It is the operator's, and deleting it would prove nothing
about what lived outside it.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from . import lifecycle
from .azure_arm import DATA_FACTORY_API
from .azure_cleanup import release_private_endpoints, settle
from .azure_deploy import secret_scope_name
from .azure_inventory import all_gone, inventory
from .azure_lanes_setup import ROUND4_FLOW, ROUND4_PIPELINE, ROUND6_FLOW, ROUND6_PIPELINE
from .azure_lifecycle import (
    CONTAINERS,
    FACTORY_ENDPOINTS,
    AzureInputs,
    AzureSetupError,
    Generation,
    Terraform,
    _get_or_none,
    _tail,
    app_made_by_installation,
    arm_client,
    only_retryable,
    uc_names,
)
from .azure_runtime import POSTGRES_LINKED_SERVICES
from .manifest import DemoManifest, load_manifest


@dataclass
class Removal:
    """What one uninstall found, and what it removed."""

    found: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    survivors: list[str] = field(default_factory=list)


#: How long ALL GONE gives Azure's resource lists to catch up with deletes that have finished.
#: Right after `terraform destroy`, the group's list still showed a Round 2 server Terraform had
#: just destroyed, and the server read ResourceNotFound moments later (2026-10-22).
ALL_GONE_SETTLE_SECONDS = 300.0
ALL_GONE_POLL_SECONDS = 15.0


def standing_after_destroy(
    arm: Any,
    run_id: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[Any]:
    """What still stands of the run, read again until nothing does or the settle bound passes."""

    deadline = clock() + ALL_GONE_SETTLE_SECONDS
    while True:
        standing = inventory(arm, run_id)
        if not standing or clock() >= deadline:
            return standing
        sleep(ALL_GONE_POLL_SECONDS)


def _databricks(profile: str, *args: str, timeout: float = 900) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["databricks", *args, "-p", profile, "-o", "json"],
                          capture_output=True, text=True, timeout=timeout, check=False)


#: What the installer puts in the factory outside Terraform (`azure_lanes_setup`), in the order it
#: must go: a pipeline names its data flow, and a data flow names its linked services, Terraform's
#: ADLS one among them, which Data Factory won't delete while anything still names it. Without
#: this, `terraform destroy` stopped on "The document cannot be deleted since it is referenced by
#: r4_move" (2026-10-19, the first uninstall).
FACTORY_OBJECTS = (
    *(("pipelines", name) for name in (ROUND4_PIPELINE, ROUND6_PIPELINE)),
    *(("dataflows", name) for name in (ROUND4_FLOW, ROUND6_FLOW)),
    *(("linkedservices", name) for name in POSTGRES_LINKED_SERVICES.values()),
)


def remove_factory_objects(arm: Any, factory: str) -> None:
    """Delete the installer's factory objects. One already gone (204, or 404) is fine."""

    for kind, name in FACTORY_OBJECTS:
        response = arm.request(
            "DELETE", f"{arm.factory_path(factory)}/{kind}/{name}?api-version={DATA_FACTORY_API}")
        if response.status not in (200, 204, 404):
            raise AzureSetupError(
                f"could not delete the factory's {kind}/{name}: HTTP {response.status}")
        if response.status == 200:
            print(f"DELETE factory {kind}/{name}", flush=True)


def _factory_of(generation: Generation) -> str | None:
    """The factory's name in Terraform's own state, or None without one."""

    if not generation.state.exists():
        return None
    outputs = json.loads(generation.state.read_text(encoding="utf-8")).get("outputs") or {}
    value = (outputs.get("data_factory") or {}).get("value") or {}
    return str(value.get("name") or "") or None


def _project_standing(manifest: DemoManifest, round_number: int) -> bool:
    """Whether that round's Lakebase project still stands."""

    project = manifest.round_lakebase(round_number).project_id
    return lifecycle._get_lakebase_project_or_none(manifest, project_id=project) is not None


def _project_ids(manifest: DemoManifest) -> list[str]:
    """The installation's seven Lakebase projects, whether or not it got as far as sealing them.

    An install that stopped before its seals has still made some, under IDs derived from its
    installation ID, as AWS's cleanup derives them. Reading the seals alone would have left all
    seven standing and still said ALL GONE (2026-10-22: an install stopped on a transient error
    while it created the coordination project).
    """

    if manifest.round_environments is None and manifest.installation_id is None:
        return []
    return [*(lifecycle._round_lakebase_binding(manifest, number).project_id
              for number in range(1, 7)),
            lifecycle._coordination_lakebase_binding(manifest).project_id]


class AzureUninstall:
    def __init__(self, inputs: AzureInputs, generation: Generation | None = None) -> None:
        self.inputs = inputs
        self.generation = generation or Generation()

    def _app(self, manifest: DemoManifest) -> tuple[str, str] | None:
        """The App this installation made, by its record, or None.

        A record that says only that a create was pending names an App that may stand anyway:
        its create can fail after making it (`app_made_by_installation`). Such an App is removed
        only on the same proof that adopts it.
        """

        path = self.generation.root / "app.json"
        record = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        if record.get("created") and record.get("client_id"):
            return str(record["name"]), str(record["client_id"])
        if not record.get("creation_pending") or not record.get("name"):
            return None
        live = _databricks(self.inputs.databricks_profile, "apps", "get", str(record["name"]))
        if live.returncode != 0:
            return None
        app = json.loads(live.stdout)
        client_id = str(app.get("service_principal_client_id") or "")
        if not client_id or not app_made_by_installation(app, manifest):
            return None
        return str(record["name"]), client_id

    def run(self, *, yes: bool) -> Removal:
        from databricks.sdk import WorkspaceClient

        os.environ["ANTI_DEMO_MANIFEST"] = str(self.generation.manifest)
        manifest = load_manifest(self.generation.manifest)
        if manifest.azure is None:
            raise AzureSetupError(f"{self.generation.manifest} is not an Azure installation's")
        profile = self.inputs.databricks_profile
        workspace = WorkspaceClient(profile=profile)
        arm = arm_client(self.inputs)
        names = uc_names(manifest)
        app = self._app(manifest)
        report = Removal()
        scope = secret_scope_name(self.inputs.app_name, manifest.run_id)
        # Only what is there: each one asked for by its own name.
        present: list[tuple[str, Callable[[], Any]]] = [
            *((f"Lakebase project {project}",
               lambda project=project: lifecycle._get_lakebase_project_or_none(
                   manifest, project_id=project)) for project in _project_ids(manifest)),
            (f"catalog {names['catalog']} and its schemas",
             lambda: workspace.catalogs.get(names["catalog"])),
            *((f"external location {names[key]}",
               lambda key=key: workspace.external_locations.get(names[key])) for key in CONTAINERS),
            (f"storage credential {names['storage_credential']}",
             lambda: workspace.storage_credentials.get(names["storage_credential"])),
            (f"service credential {names['app_service_credential']}",
             lambda: workspace.credentials.get_credential(names["app_service_credential"])),
            (f"secret scope {scope}", lambda: workspace.secrets.list_secrets(scope)),
        ]
        report.found = [
            *([f"Databricks App {app[0]}"] if app else []),
            *(label for label, read in present if _get_or_none(read) is not None),
            *(f"Azure {item.type} {item.name}" for item in inventory(arm, manifest.run_id)),
        ]
        for line in report.found:
            print(f"FOUND {line}", flush=True)
        if not yes:
            print("DRY RUN: nothing was removed. Run again with --yes to remove all of it.")
            return report

        factory = (manifest.azure.round4 or manifest.azure.round6)
        # Terraform's own record names it too, for an install that stopped before a lane sealed:
        # the installer may have put its objects in the factory by then.
        factory_name = factory.factory if factory else _factory_of(self.generation)
        asyncio.run(settle(arm, run_id=manifest.run_id, factory=factory_name,
                           pipelines=(ROUND4_PIPELINE, ROUND6_PIPELINE), reset_pgbouncer=None))
        report.removed.append("the per-bout leftovers")
        if app is not None:
            self._remove_app(workspace, profile, *app, manifest=manifest)
            report.removed.append(f"Databricks App {app[0]}")
        # An uninstall that stopped part way is run again, so each round's Databricks objects are
        # removed only while its Lakebase project stands. Once it's gone, what they were lived in
        # it or in the catalog, which goes below with `force`. Round 6's own cleanup refuses an
        # environment it can no longer verify, by design (`--force-round6`).
        if manifest.round6 is not None and _project_standing(manifest, 6):
            from .round6_lifecycle import cleanup_round6

            for line in cleanup_round6(manifest, dry_run=False, workspace=workspace):
                print(line, flush=True)
            report.removed.append("Round 6's change feed and schemas")
        if manifest.round4 is not None and _project_standing(manifest, 4):
            inventory_round4 = lifecycle._inspect_round4_for_cleanup(manifest)
            lifecycle._delete_round4_resources(manifest, inventory_round4)
            report.removed.append("Round 4's synced table, pipeline and schemas")
        elif manifest.round4 is not None:
            # A workspace object, outside the project and the catalog: removed by its sealed ID.
            pipeline_id = manifest.round4.pipeline_id
            if _get_or_none(lambda: workspace.pipelines.get(pipeline_id)) is not None:
                print(f"DELETE Round 4's pipeline {pipeline_id}", flush=True)
                workspace.pipelines.delete(pipeline_id)
            report.removed.append("Round 4's pipeline")
        for project in _project_ids(manifest):
            # Each asked for first, as the dry run does: a rerun finds some already gone, and
            # deleting a project that isn't there is refused ("Project ... not found").
            if lifecycle._get_lakebase_project_or_none(manifest, project_id=project) is None:
                continue
            print(f"DELETE Lakebase project {project}", flush=True)
            lifecycle._delete_lakebase_project(manifest, profile, project)
        report.removed.append("the Lakebase projects")
        self._remove_unity_catalog(workspace, names)
        report.removed.append("the Unity Catalog objects")
        if _get_or_none(lambda: workspace.secrets.list_secrets(scope)) is not None:
            workspace.secrets.delete_scope(scope)
        report.removed.append(f"secret scope {scope}")
        if factory_name:
            remove_factory_objects(arm, factory_name)
            report.removed.append("the factory's pipelines, data flows and server linked services")
            release_private_endpoints(
                arm, factory_name, FACTORY_ENDPOINTS.values(),
                [manifest.azure.servers[key].server for key in ("r4", "r6")
                 if key in manifest.azure.servers])
        self._destroy()
        report.removed.append("infra/azure")
        # ALL GONE asks again, by name, for everything it removed, not only for what Azure tags:
        # a Lakebase project left standing still bills.
        standing = standing_after_destroy(arm, manifest.run_id)
        report.survivors = [
            *(f"Azure {item.type} {item.name}" for item in standing),
            *(label for label, read in present if _get_or_none(read) is not None),
        ]
        if report.survivors or not all_gone(standing):
            for line in report.survivors:
                print(f"SURVIVES {line}", flush=True)
            raise AzureSetupError(f"{len(report.survivors)} resources of run "
                                  f"{manifest.run_id} survive: not ALL GONE")
        archive = self.generation.root.with_name(
            f"{self.generation.root.name}.removed-{datetime.now(UTC):%Y%m%d-%H%M%S}")
        self.generation.root.rename(archive)
        print(f"ALL GONE: run {manifest.run_id}. Its record is kept at {archive}.", flush=True)
        return report

    @staticmethod
    def _remove_app(workspace: Any, profile: str, name: str, client_id: str, *,
                    manifest: DemoManifest, sleep: Callable[[float], None] = time.sleep) -> None:
        live = _databricks(profile, "apps", "get", name)
        if live.returncode != 0:
            return
        if json.loads(live.stdout).get("service_principal_client_id") != client_id:
            raise AzureSetupError(f"the Databricks App {name} isn't the one this installation "
                                  "made: it is left alone")
        print(f"DELETE Databricks App {name}", flush=True)
        deleted = _databricks(profile, "apps", "delete", name)
        if deleted.returncode != 0:
            raise AzureSetupError(
                f"could not delete the Databricks App {name}: {_tail(deleted, 3)}")
        source = f"/Workspace/Users/{manifest.databricks.user}/{name}"
        if _get_or_none(lambda: workspace.workspace.get_status(source)) is not None:
            workspace.workspace.delete(source, recursive=True)

    @staticmethod
    def _remove_unity_catalog(workspace: Any, names: dict[str, str]) -> None:
        if _get_or_none(lambda: workspace.catalogs.get(names["catalog"])) is not None:
            print(f"DELETE catalog {names['catalog']}", flush=True)
            workspace.catalogs.delete(names["catalog"], force=True)
        credential = names["app_service_credential"]
        if _get_or_none(lambda: workspace.credentials.get_credential(credential)) is not None:
            workspace.credentials.delete_credential(credential, force=True)
        for key in CONTAINERS:
            location = names[key]
            if _get_or_none(lambda name=location: workspace.external_locations.get(name)):
                workspace.external_locations.delete(location, force=True)
        storage = names["storage_credential"]
        if _get_or_none(lambda: workspace.storage_credentials.get(storage)) is not None:
            workspace.storage_credentials.delete(storage, force=True)

    def _destroy(self) -> None:
        terraform = Terraform(self.generation, self.inputs)
        terraform.init()
        for attempt in range(1, 4):
            print(f"DESTROY infra/azure (attempt {attempt} of 3)", flush=True)
            result = terraform._terraform(
                "destroy", "-input=false", "-no-color", "-auto-approve",
                f"-var-file={self.generation.tfvars}", timeout=5400)
            if result.returncode == 0:
                return
            log = f"{result.stdout}\n{result.stderr}"
            if attempt == 3 or not only_retryable(log):
                raise AzureSetupError(f"terraform destroy failed: {_tail(result)}")
            time.sleep(60)
