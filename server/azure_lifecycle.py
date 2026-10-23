"""The Azure edition's installer (design section 8): one subscription, one resource group, and
nothing AWS's.

`./antidemo azure-setup` runs it. It resumes as the AWS installer does: the manifest's `status`
says how far a run got, and each step proves what an earlier one left before it builds on it.

**What it calls from the AWS installer as it is** (`server/lifecycle.py`), because it is
Databricks' and the same on both clouds:
* Lakebase's seven projects, their seals and the region check (`_ensure_v7_lakebase_projects`);
* the base schema and its proof (`_apply_schema`), on Lakebase and on each Azure server alike;
* the coordination store (`ensure_coordination`) and the wait for scale zero;
* Round 4's synced table, and Round 6's change feed, over a catalog this installer owns.

**What is Azure's:**
* `infra/azure`, applied with the busy retry, its servers sealed from its outputs;
* the factory's managed private endpoints, approved on their targets (Terraform's provider can't);
* Unity Catalog on the installation's own ADLS: a storage credential on the Access Connector, an
  external location per container, and a catalog whose managed location is one of them;
* the App's Azure identity, a Unity Catalog service credential on its own Access Connector,
  so the App holds no Azure secret.

**The generation directory** (`.anti-demo-azure/`) holds the manifest, Terraform's state and
variables, the servers' admin password and the runners' SSH key, each written once at first
provision and read on every resume. A directory with Terraform variables and no manifest is a
provision whose manifest was lost: the new manifest keeps the variables' IDs, so Terraform's names
don't move.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import string
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from . import lifecycle
from .azure_arm import COMPUTE_API, POSTGRES_API, ArmClient, AzureCredentials
from .azure_identity import az_cli_token_source
from .azure_inventory import RUN_TAG
from .azure_manifest import AzureManifest, AzurePostgresEnvironmentSeal
from .home_cloud import AZURE
from .manifest import (
    PROJECT_ROOT,
    DatabricksManifest,
    DemoManifest,
    RoundEnvironmentSeal,
    apply_manifest_environment,
    load_manifest,
    save_manifest,
)
from .models import RoundId
from .targets import ConnectionMaterial

GENERATION = PROJECT_ROOT / ".anti-demo-azure"
TERRAFORM_ROOT = PROJECT_ROOT / "infra" / "azure"
#: The servers' admin login (infra/azure/flexible_servers.tf) and the secret-scope key that holds
#: its password. One password serves all five servers; Rounds 2 and 3's copies restore it.
ADMIN_LOGIN = "antidemo_admin"
ADMIN_PASSWORD_SECRET = "azure-postgres-admin-password"
ROUND_KEYS = ("r2", "r3", "r4", "r5", "r6")
#: The rounds that race on the orders schema, and so get it seeded. Round 1 stands no Azure
#: server up (design 6.1); Rounds 4 and 6 hold their own lanes' tables instead.
SEEDED_LAKEBASE_ROUNDS = (1, 2, 3, 5)
SEEDED_AZURE_ROUNDS = ("r2", "r3", "r5")
#: Storage's API for approving the factory's endpoint on the ADLS account (Phase 0).
STORAGE_API = "2023-05-01"
#: The factory's managed private endpoints (infra/azure/adf.tf), by what each reaches.
FACTORY_ENDPOINTS = {"r4": "mpe-r4-postgres", "r6": "mpe-r6-postgres", "adls": "mpe-adls-dfs"}

#: A Terraform run that stopped only on these is asked again: the subscription's
#: policy write makes a new server refuse a firewall rule for a while, and a server can't be
#: deleted until a private endpoint's connection to it clears.
TERRAFORM_RETRYABLE = re.compile(
    r"ServerIsBusy|PrivateEndpointServerWithActiveConnectionCannotBeDropped"
)
TERRAFORM_ATTEMPTS = 3
TERRAFORM_RETRY_SECONDS = 60.0
#: How many times one install resumes itself after a transient error (`AzureSetup.run`), and how
#: long it waits first.
SETUP_ATTEMPTS = 3
SETUP_RESUME_SECONDS = 30.0
#: Azure refusing a VM's create for want of room in its zone at that moment: Round 5's runners
#: (D8ls_v6, westus2 zone 1) twice on 2026-10-20. The refused VM is left Failed, with its OS disk,
#: outside Terraform's state, and the next apply stops on "already exists" for it. Every apply
#: first clears such a VM (`clear_refused_runners`), so both are refusals a second run gets past.
VM_REFUSED = re.compile(
    r"ZonalAllocationFailed|\bAllocationFailed\b|Overconstrained(?:Zonal)?AllocationRequest"
    r"|virtualMachines/[^\"/\s]+\" already exists"
)
#: A Round 5 runner VM's name (infra/azure/round5_runners.tf), with the number its key carries.
RUNNER_VM_NAME = re.compile(r"-r5-runner-(\d+)$")
#: Managed disks' API, for a refused runner's OS disk.
DISK_API = "2024-03-02"


class AzureSetupError(RuntimeError):
    """The Azure installer stopped on something it names, with what to do about it."""


@dataclass(frozen=True)
class AzureInputs:
    """What an Azure installation is built from (design 8.1), all of it settings, none secret."""

    databricks_profile: str
    tenant_id: str
    subscription_id: str
    resource_group: str
    region: str
    owner: str
    operator_cidr: str
    zone: str | None = "1"
    policy_tags: Mapping[str, str] = field(default_factory=dict)
    lease_date_tag: str | None = None
    ttl_hours: float = 72.0
    #: The SQL warehouse Rounds 4 and 6 read through, and the App's name in the workspace.
    warehouse_id: str = ""
    app_name: str = "lakebase-anti-demo"


@dataclass(frozen=True)
class Generation:
    """The installation's files on the operator's machine."""

    root: Path = GENERATION

    @property
    def manifest(self) -> Path:
        return self.root / "manifest.json"

    @property
    def state(self) -> Path:
        return self.root / "terraform.tfstate"

    @property
    def tfvars(self) -> Path:
        return self.root / "terraform.tfvars.json"

    @property
    def admin_password(self) -> Path:
        return self.root / "admin-password"

    @property
    def runner_key(self) -> Path:
        return self.root / "runner_ed25519"

    def ensure(self) -> None:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)


def _private(path: Path, text: str) -> None:
    """Write `text` readable by this user alone, as the manifest is."""

    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, path)


def new_run_id(now: datetime | None = None) -> str:
    """The AWS installer's shape, ``az-`` for ``ad-``: when it began, and four hex digits."""

    stamp = (now or datetime.now(UTC)).strftime("%Y%m%d-%H%M")
    return f"az-{stamp}-{secrets.token_hex(2)}"


def storage_account_name(installation_id: str, run_id: str) -> str:
    """The ADLS account's name, as `infra/azure/locals.tf` derives it from the installation."""

    digest = hashlib.sha256(installation_id.encode("utf-8")).hexdigest()
    return f"st{digest[:20]}{run_id[:2]}"


def new_admin_password() -> str:
    """28 random letters and digits, and one of each class Azure's password rule asks for."""

    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(28)) + "Aa1-"


# --- The Azure side's identity and client --------------------------------------------------------


def arm_client(inputs: AzureInputs) -> ArmClient:
    """The operator's ARM client: `az login` in the installation's tenant, no service principal."""

    return ArmClient(
        AzureCredentials(tenant_id=inputs.tenant_id, subscription_id=inputs.subscription_id),
        inputs.resource_group,
        token_source=az_cli_token_source(inputs.tenant_id),
    )


def operator_object_id(
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    """The signed-in operator's object ID in the tenant, sealed so a later run as anyone else stops.

    Read by object ID, because a guest's sign-in name isn't its name in the tenant.
    """

    result = run(["az", "ad", "signed-in-user", "show", "--query", "id", "-o", "tsv"],
                 capture_output=True, text=True, timeout=60, check=False)
    object_id = (result.stdout or "").strip()
    if result.returncode != 0 or not re.fullmatch(r"[0-9a-f-]{36}", object_id):
        raise AzureSetupError("az could not read the signed-in operator: run 'az login --tenant "
                              "<tenant>' as the operator who installs, then try again")
    return object_id


# --- The manifest ---------------------------------------------------------------------------------


def new_manifest(
    inputs: AzureInputs,
    *,
    databricks_user: str,
    principal_object_id: str,
    egress: tuple[tuple[str, ...], int],
    run_id: str | None = None,
    installation_id: str | None = None,
    expires_at: datetime | None = None,
    now: datetime | None = None,
) -> DemoManifest:
    """A first provision's manifest: the installation's identity, and nothing built yet."""

    created_at = now or datetime.now(UTC)
    run_id = run_id or new_run_id(created_at)
    installation_id = installation_id or str(uuid4())
    compact = installation_id.replace("-", "")
    round1 = f"anti-demo-{compact}-r1"
    coordination = f"anti-demo-{compact}-coord"
    cidrs, published_at = egress
    return DemoManifest(
        manifest_version=8,
        cloud="azure",
        installation_id=installation_id,
        run_id=run_id,
        owner=inputs.owner or databricks_user,
        created_at=created_at,
        expires_at=expires_at or created_at + timedelta(hours=inputs.ttl_hours),
        status="provisioning",
        azure=AzureManifest(
            tenant_id=inputs.tenant_id,
            subscription_id=inputs.subscription_id,
            region=inputs.region,
            resource_group=inputs.resource_group,
            principal_object_id=principal_object_id,
            operator_cidr=inputs.operator_cidr,
            databricks_egress_cidrs=cidrs,
            databricks_egress_published_at=published_at,
            storage_account=storage_account_name(installation_id, run_id),
            policy_tags=dict(inputs.policy_tags),
            lease_date_tag=inputs.lease_date_tag,
            servers={},
        ),
        databricks=DatabricksManifest(
            profile=inputs.databricks_profile,
            project_id=round1,
            endpoint_name=f"projects/{round1}/branches/production/endpoints/primary",
            coordination_endpoint_name=(
                f"projects/{coordination}/branches/production/endpoints/primary"
            ),
            user=databricks_user,
        ),
        schema_sha256=lifecycle._schema_sha256(),
    )


def _rfc3339(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def terraform_vars(
    manifest: DemoManifest, inputs: AzureInputs, *, ssh_public_key: str
) -> dict[str, Any]:
    """`infra/azure`'s variables for this installation. Only what isn't a default is written."""

    azure = _azure(manifest)
    values: dict[str, Any] = {
        "location": azure.region,
        "resource_group_name": azure.resource_group,
        "run_id": manifest.run_id,
        "installation_id": manifest.installation_id,
        "owner": manifest.owner,
        "expires_at": _rfc3339(manifest.expires_at),
        "operator_cidr": azure.operator_cidr,
        "databricks_egress_cidrs": list(azure.databricks_egress_cidrs or ()),
        "runner_ssh_public_key": ssh_public_key,
        "manage_role_assignments": True,
        "key_vault_authorization": "rbac",
        "zone": inputs.zone,
    }
    if azure.policy_tags:
        values["policy_tags"] = dict(azure.policy_tags)
    if azure.lease_date_tag:
        values["lease_date_tag"] = azure.lease_date_tag
    return values


def _azure(manifest: DemoManifest) -> AzureManifest:
    if manifest.azure is None:
        raise AzureSetupError("this manifest is not an Azure installation's")
    return manifest.azure


def seal_servers(outputs: Mapping[str, Any]) -> dict[str, AzurePostgresEnvironmentSeal]:
    """Each round's server, from `infra/azure`'s `postgresql_servers` output."""

    servers = (outputs.get("postgresql_servers") or {}).get("value") or {}
    if set(servers) != set(ROUND_KEYS):
        raise AzureSetupError(f"infra/azure output servers for {sorted(servers)}, expected "
                              f"{list(ROUND_KEYS)}")
    return {
        key: AzurePostgresEnvironmentSeal(
            server=str(item["name"]), resource_id=str(item["id"]), host=str(item["fqdn"]),
            port=int(item.get("port") or 5432), admin_login=str(item["admin_login"]),
            sku=str(item["sku_name"]), password_secret=ADMIN_PASSWORD_SECRET,
        )
        for key, item in sorted(servers.items())
    }


# --- Terraform -----------------------------------------------------------------------------------


def only_retryable(log: str, *, also: re.Pattern[str] | None = None) -> bool:
    """True when the run printed errors and every one names a refusal a second run gets past."""

    errors = re.split(r"(?m)^[│ ]*Error: ", log)[1:]
    return bool(errors) and all(
        TERRAFORM_RETRYABLE.search(error) or (also is not None and also.search(error))
        for error in errors)


class Terraform:
    """`infra/azure`, with its state and variables in the generation directory."""

    def __init__(
        self,
        generation: Generation,
        inputs: AzureInputs,
        *,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        sleep: Callable[[float], None] = time.sleep,
        echo: Callable[[str], None] = lambda line: print(line, flush=True),
        clear_refused: Callable[[], list[str]] = lambda: [],
    ) -> None:
        self._generation = generation
        self._inputs = inputs
        self._run = run
        self._sleep = sleep
        self._echo = echo
        self._clear_refused = clear_refused

    def _environment(self) -> dict[str, str]:
        password = self._generation.admin_password.read_text(encoding="utf-8").strip()
        return {**os.environ, "ARM_SUBSCRIPTION_ID": self._inputs.subscription_id,
                "ARM_TENANT_ID": self._inputs.tenant_id, "TF_VAR_admin_password": password,
                "TF_IN_AUTOMATION": "1"}

    def _terraform(self, *args: str, timeout: float) -> subprocess.CompletedProcess[str]:
        return self._run(["terraform", *args], cwd=TERRAFORM_ROOT, env=self._environment(),
                         capture_output=True, text=True, timeout=timeout, check=False)

    def init(self) -> None:
        result = self._terraform("init", "-input=false", "-no-color", "-reconfigure",
                                 f"-backend-config=path={self._generation.state}", timeout=600)
        if result.returncode != 0:
            raise AzureSetupError(f"terraform init failed: {_tail(result)}")

    def apply(self) -> None:
        """Apply, asked again only when every error is a refusal a second run gets past.

        Planned first: state that already matches its configuration is left alone, so a resumed
        run changes nothing it doesn't have to. Before each attempt, a runner VM an earlier
        attempt's refused create left behind is cleared (`clear_refused`), so the apply can make
        it again.
        """

        self.init()
        plan = self._terraform("plan", "-input=false", "-no-color", "-detailed-exitcode",
                               f"-var-file={self._generation.tfvars}", timeout=1800)
        if plan.returncode == 0:
            self._echo("OK    infra/azure matches its state: nothing to apply")
            return
        if plan.returncode != 2:
            raise AzureSetupError(f"terraform plan failed: {_tail(plan)}")
        for attempt in range(1, TERRAFORM_ATTEMPTS + 1):
            for name in self._clear_refused():
                self._echo(f"CLEAR {name}: Azure refused its create and left it Failed, outside "
                           "Terraform's state; it and its OS disk are deleted")
            self._echo(f"APPLY infra/azure (attempt {attempt} of {TERRAFORM_ATTEMPTS})")
            result = self._terraform("apply", "-input=false", "-no-color", "-auto-approve",
                                     f"-var-file={self._generation.tfvars}", timeout=5400)
            log = f"{result.stdout}\n{result.stderr}"
            if result.returncode == 0:
                return
            if attempt == TERRAFORM_ATTEMPTS or not only_retryable(log, also=VM_REFUSED):
                raise AzureSetupError(f"terraform apply failed: {_tail(result)}"
                                      + _sku_remedy(log) + _room_remedy(log))
            reason = ("Azure had no room in the zone for a VM" if VM_REFUSED.search(log)
                      else "a new server is still busy")
            self._echo(f"WAIT  {reason}; applying again in {TERRAFORM_RETRY_SECONDS:.0f}s, for "
                       "only what the last run missed")
            self._sleep(TERRAFORM_RETRY_SECONDS)

    def state_addresses(self) -> frozenset[str]:
        """Every resource address in Terraform's state; none before the first apply."""

        if not self._generation.state.exists():
            return frozenset()
        result = self._terraform("state", "list", "-no-color", timeout=300)
        if result.returncode != 0:
            raise AzureSetupError(f"terraform state list failed: {_tail(result)}")
        return frozenset(line.strip() for line in result.stdout.splitlines() if line.strip())

    def outputs(self) -> dict[str, Any]:
        result = self._terraform("output", "-json", "-no-color", timeout=300)
        if result.returncode != 0:
            raise AzureSetupError(f"terraform output failed: {_tail(result)}")
        return json.loads(result.stdout or "{}")


def _tail(result: subprocess.CompletedProcess[str], lines: int = 12) -> str:
    text = f"{result.stdout or ''}\n{result.stderr or ''}".strip().splitlines()
    return "\n".join(text[-lines:])


def _sku_remedy(log: str) -> str:
    """No read predicts a zone's refusal, so the create's refusal says what to do."""

    if "SkuNotAvailable" not in log:
        return ""
    return ("\nThe server size isn't offered in this zone right now. Set AZURE_ZONE to another of "
            "1, 2 or 3 (every server and runner moves with it), or move to a General Purpose size.")


def _room_remedy(log: str) -> str:
    if not VM_REFUSED.search(log):
        return ""
    return ("\nAzure had no room in this zone for a runner VM of this size, which comes and goes. "
            "Run the same command again later: it clears the VMs Azure refused and applies only "
            "what is missing.")


def clear_refused_runners(arm: ArmClient, run_id: str,
                          managed: Callable[[], frozenset[str]], *,
                          bound_seconds: float = 900.0) -> list[str]:
    """Delete the runner VMs a refused create left behind, with their OS disks; their names.

    Only a VM that carries the run's tag, reads Failed, is named as a runner, and isn't in
    Terraform's state (`managed`, read only when there is such a VM). That's one Azure accepted
    and then had no room for. A runner Terraform made is never touched, whatever it
    reads.
    """

    failed: list[tuple[Mapping[str, Any], str]] = []
    url = (f"{arm.resource_group_path}/providers/Microsoft.Compute/virtualMachines"
           f"?api-version={COMPUTE_API}")
    while url:
        response = arm.request("GET", url)
        if response.status != 200 or not isinstance(response.body, dict):
            raise AzureSetupError(f"listing the group's VMs answered HTTP {response.status}")
        for vm in response.body.get("value", []):
            tags = {str(key).lower(): str(value) for key, value in (vm.get("tags") or {}).items()}
            named = RUNNER_VM_NAME.search(str(vm.get("name") or ""))
            if (named and tags.get(RUN_TAG) == run_id
                    and (vm.get("properties") or {}).get("provisioningState") == "Failed"):
                failed.append((vm, named.group(1)))
        url = response.body.get("nextLink") or ""
    if not failed:
        return []
    addresses = managed()
    cleared: list[str] = []
    for vm, number in failed:
        if f'azurerm_linux_virtual_machine.runner["runner-{number}"]' in addresses:
            continue
        disk = (((vm.get("properties") or {}).get("storageProfile") or {}).get("osDisk")
                or {}).get("managedDisk") or {}
        _delete_and_wait(arm, f"{vm['id']}?api-version={COMPUTE_API}", str(vm["name"]),
                         bound_seconds=bound_seconds)
        if disk.get("id"):
            _delete_and_wait(arm, f"{disk['id']}?api-version={DISK_API}",
                             str(disk["id"]).rsplit("/", 1)[-1], bound_seconds=bound_seconds)
        cleared.append(str(vm["name"]))
    return cleared


def _delete_and_wait(arm: ArmClient, path: str, name: str, *, bound_seconds: float) -> None:
    response = arm.request("DELETE", path)
    if response.status in (204, 404):
        return
    if response.status not in (200, 202):
        raise AzureSetupError(f"deleting {name} answered HTTP {response.status}")
    status, _ = arm.wait_operation(response, bound_seconds=bound_seconds)
    if status != "Succeeded":
        raise AzureSetupError(f"deleting {name} ended {status}")


# --- Steps ---------------------------------------------------------------------------------------


def approve_factory_endpoints(
    arm: ArmClient, manifest: DemoManifest, outputs: Mapping[str, Any]
) -> None:
    """Approve the factory's three endpoints on their targets; wait until each reads Approved."""

    azure = _azure(manifest)
    factory = str(((outputs.get("data_factory") or {}).get("value") or {}).get("name") or "")
    if not factory:
        raise AzureSetupError("infra/azure output no data factory")
    storage_id = (f"{arm.resource_group_path}/providers/Microsoft.Storage/storageAccounts/"
                  f"{azure.storage_account}")
    targets = (
        (azure.servers["r4"].resource_id, POSTGRES_API, FACTORY_ENDPOINTS["r4"]),
        (azure.servers["r6"].resource_id, POSTGRES_API, FACTORY_ENDPOINTS["r6"]),
        (storage_id, STORAGE_API, FACTORY_ENDPOINTS["adls"]),
    )
    for target_id, api_version, endpoint in targets:
        state = arm.approve_private_endpoint_connection(
            target_id, api_version, factory=factory, endpoint=endpoint)
        print(f"OK    factory endpoint {endpoint}: {state}", flush=True)


def seal_lakebase(manifest: DemoManifest) -> None:
    """Lakebase's seven projects, sealed as on AWS, with no AWS database beside any of them."""

    rounds, coordination = lifecycle._ensure_v7_lakebase_projects(manifest)
    manifest.round_environments = {
        round_id: RoundEnvironmentSeal(lakebase=rounds[round_id]) for round_id in RoundId
    }
    manifest.coordination_environment = coordination
    manifest.databricks.coordination_endpoint_name = coordination.endpoint_name
    DemoManifest.model_validate(manifest.model_dump())


def azure_material(
    manifest: DemoManifest, key: str, password: str, *, database: str
) -> ConnectionMaterial:
    seal = _azure(manifest).servers[key]
    return ConnectionMaterial(host=seal.host, port=seal.port, database=database,
                              user=seal.admin_login, password=password)


async def _ensure_azure_database(manifest: DemoManifest, key: str, password: str) -> None:
    """The installation's database on one server. Azure creates only `postgres`."""

    database = manifest.databricks.database
    connection = await lifecycle._connect(
        azure_material(manifest, key, password, database="postgres"), autocommit=True)
    async with connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if await cursor.fetchone() is None:
                from psycopg import sql

                await cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))


async def seed(manifest: DemoManifest, password: str) -> None:
    """The identical base schema on every Lakebase round and Azure server that races on it."""

    apply_manifest_environment(manifest)
    await asyncio.gather(*(_ensure_azure_database(manifest, key, password) for key in ROUND_KEYS))
    providers = [lifecycle._round_lakebase_provider(manifest, number)
                 for number in SEEDED_LAKEBASE_ROUNDS]
    await asyncio.gather(*(
        lifecycle._ensure_lakebase_database(manifest.databricks.database, provider)
        for provider in providers))
    lakebase = await asyncio.gather(*(provider.connection_material() for provider in providers))
    azure = [azure_material(manifest, key, password, database=manifest.databricks.database)
             for key in SEEDED_AZURE_ROUNDS]
    versions = await asyncio.gather(*(lifecycle._apply_schema(material)
                                      for material in (*lakebase, *azure)))
    print(f"OK    base schema on Lakebase rounds {list(SEEDED_LAKEBASE_ROUNDS)} and Azure "
          f"servers {list(SEEDED_AZURE_ROUNDS)} (server_version_num {sorted(set(versions))})",
          flush=True)


# --- Unity Catalog, the App, and the rounds' Lakebase halves -------------------------------------

#: The ADLS containers (infra/azure/locals.tf), by what each holds.
CONTAINERS = {"r4_source": "r4source", "r6_history": "r6history",
              "change_feed_dest": "change-feed", "uc_managed_loc": "uc-managed"}
#: How long Unity Catalog's validation of a new external location may keep failing while the Access
#: Connector's new role assignment propagates. Azure documents up to 10 minutes.
UC_VALIDATION_BOUND_SECONDS = 900.0
UC_VALIDATION_RETRY_SECONDS = 20.0


def uc_names(manifest: DemoManifest) -> dict[str, str]:
    """The Unity Catalog objects an installation makes, each named for its run."""

    token = manifest.run_id.replace("-", "_")
    names = {"catalog": f"anti_demo_{token}", "storage_credential": f"anti_demo_{token}_uc",
             "app_service_credential": f"anti_demo_{token}_app"}
    names.update({key: f"anti_demo_{token}_{key}" for key in CONTAINERS})
    return names


def abfss(account: str, container: str, path: str = "") -> str:
    return f"abfss://{container}@{account}.dfs.core.windows.net/{path}"


def _output(outputs: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = (outputs.get(name) or {}).get("value")
    if not isinstance(value, dict):
        raise AzureSetupError(f"infra/azure has no {name} output")
    return value


def _absent(error: BaseException) -> bool:
    # Any of the SDK's NotFound family is absent, whatever it says. Its `ResourceDoesNotExist`
    # for a workspace path says "Path (...) doesn't exist.", which the words below missed: it
    # stopped an uninstall of an install that had made its App but not yet synced the App's
    # source (2026-10-22). The words still serve errors that reach here as text (a gateway's 404).
    from databricks.sdk.errors import NotFound

    if isinstance(error, NotFound):
        return True
    text = f"{type(error).__name__} {error}".lower()
    return any(marker in text for marker in ("notfound", "not found", "does not exist",
                                              "doesn't exist", "resourcedoesnotexist",
                                              "resource_does_not_exist"))


def _get_or_none(read: Callable[[], Any]) -> Any:
    try:
        return read()
    except Exception as error:  # the SDK's NotFound, or a gateway's 404 in its words
        if _absent(error):
            return None
        raise


def _validated(create: Callable[[], Any], what: str, *, sleep: Callable[[float], None] = time.sleep,
               clock: Callable[[], float] = time.monotonic) -> Any:
    """Create, retrying while Unity Catalog can't yet use the connector's new role."""

    deadline = clock() + UC_VALIDATION_BOUND_SECONDS
    while True:
        try:
            return create()
        except Exception as error:
            text = str(error).lower()
            denied = any(marker in text for marker in ("permission", "authoriz", "403",
                                                      "forbidden", "not authorized"))
            if not denied or clock() + UC_VALIDATION_RETRY_SECONDS >= deadline:
                raise AzureSetupError(f"Unity Catalog refused {what}: {error}") from error
            print(f"WAIT  {what}: the Access Connector's role is still reaching the storage "
                  f"account; again in {UC_VALIDATION_RETRY_SECONDS:.0f}s", flush=True)
            sleep(UC_VALIDATION_RETRY_SECONDS)


def ensure_unity_catalog(
    manifest: DemoManifest, outputs: Mapping[str, Any], workspace: Any
) -> None:
    """The installation's own Unity Catalog objects, all on its own ADLS account."""

    from databricks.sdk.service.catalog import (
        AzureManagedIdentity,
        AzureManagedIdentityRequest,
        CredentialPurpose,
    )

    from .azure_manifest import AzureUnityCatalogSeal

    azure = _azure(manifest)
    names = uc_names(manifest)
    connector = _output(outputs, "access_connector")["id"]
    app_connector = _output(outputs, "app_access_connector")["id"]
    comment = f"Lakebase Anti-Demo {manifest.run_id}"
    if _get_or_none(lambda: workspace.storage_credentials.get(names["storage_credential"])) is None:
        print(f"CREATE storage credential {names['storage_credential']}", flush=True)
        _validated(lambda: workspace.storage_credentials.create(
            names["storage_credential"], comment=comment,
            azure_managed_identity=AzureManagedIdentityRequest(access_connector_id=connector)),
            f"storage credential {names['storage_credential']}")
    locations: dict[str, str] = {}
    for key, container in CONTAINERS.items():
        name = names[key]
        locations[container] = name
        if _get_or_none(lambda name=name: workspace.external_locations.get(name)) is None:
            print(f"CREATE external location {name} on {container}", flush=True)
            _validated(lambda name=name, container=container: workspace.external_locations.create(
                name, abfss(azure.storage_account, container), names["storage_credential"],
                comment=comment), f"external location {name}")
    if _get_or_none(lambda: workspace.catalogs.get(names["catalog"])) is None:
        print(f"CREATE catalog {names['catalog']} on {CONTAINERS['uc_managed_loc']}", flush=True)
        workspace.catalogs.create(
            names["catalog"], comment=comment,
            storage_root=abfss(azure.storage_account, CONTAINERS["uc_managed_loc"], "catalog"))
    credential = names["app_service_credential"]
    if _get_or_none(lambda: workspace.credentials.get_credential(credential)) is None:
        print(f"CREATE the App's service credential {credential}", flush=True)
        workspace.credentials.create_credential(
            name=credential, purpose=CredentialPurpose.SERVICE, comment=comment,
            azure_managed_identity=AzureManagedIdentity(access_connector_id=app_connector))
    manifest.azure = azure.model_copy(update={"unity_catalog": AzureUnityCatalogSeal(
        storage_credential=names["storage_credential"], external_locations=locations,
        catalog=names["catalog"], app_service_credential=credential)})


#: What every App this installer makes says about itself (`databricks apps create --description`).
APP_DESCRIPTION = "Lakebase: The Anti-Demo (Azure edition)"


def app_made_by_installation(live: Mapping[str, Any], manifest: DemoManifest) -> bool:
    """Whether a live App is the one this installation asked for, though its create never said so.

    `databricks apps create` waits for the App's compute, and it can exit non-zero after the App
    exists. On 2026-10-22 it failed about 25 s in, while the compute was still starting, and the
    compute came up by itself. The install's record then said only that a create was pending: a
    rerun refused the App as a stranger's, and an uninstall would have left it running.

    Such an App is this installation's when three things hold. This installation's Databricks user
    created it, with the installer's own description, and after the installation was sealed. An
    App of the same name from an earlier installation was created before that.
    """

    try:
        created = datetime.fromisoformat(str(live.get("create_time") or ""))
    except ValueError:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=UTC)
    user = manifest.databricks.user.strip().casefold()
    return (
        bool(user)
        and str(live.get("creator") or "").strip().casefold() == user
        and live.get("description") == APP_DESCRIPTION
        and created >= manifest.created_at - timedelta(minutes=1)
    )


def ensure_app(generation: Generation, profile: str, app_name: str, manifest: DemoManifest) -> str:
    """The Databricks App, made by this installation, and its service principal's client ID.

    An App of that name that this installation didn't make is refused, never adopted: its
    principal would be handed every grant below. One whose create didn't confirm is adopted only
    if `app_made_by_installation` proves it this installation's.
    """

    record_path = generation.root / "app.json"
    record = json.loads(record_path.read_text(encoding="utf-8")) if record_path.exists() else {}

    def live_app() -> dict[str, Any] | None:
        found = subprocess.run(["databricks", "apps", "get", app_name, "-p", profile, "-o", "json"],
                               capture_output=True, text=True, timeout=120, check=False)
        return json.loads(found.stdout) if found.returncode == 0 else None

    def adopt(app: Mapping[str, Any]) -> str:
        client_id = str(app.get("service_principal_client_id") or "")
        if not client_id:
            raise AzureSetupError("the new Databricks App returned no service principal")
        _private(record_path, json.dumps({"name": app_name, "created": True,
                                          "client_id": client_id}) + "\n")
        return client_id

    existing = live_app()
    if existing is not None:
        client_id = str(existing.get("service_principal_client_id") or "")
        if record.get("name") == app_name and record.get("client_id") == client_id:
            return client_id
        if (record.get("name") == app_name and record.get("creation_pending")
                and app_made_by_installation(existing, manifest)):
            print(f"ADOPT Databricks App {app_name}: this installation made it, though its "
                  "create didn't confirm", flush=True)
            return adopt(existing)
        raise AzureSetupError(f"a Databricks App named {app_name} already exists and this "
                              "installation didn't make it: set DATABRICKS_APP_NAME to another")
    _private(record_path, json.dumps({"name": app_name, "creation_pending": True}) + "\n")
    print(f"CREATE Databricks App {app_name} (its compute starts now)", flush=True)
    created = subprocess.run(
        ["databricks", "apps", "create", app_name, "--description", APP_DESCRIPTION,
         "-p", profile, "-o", "json"],
        capture_output=True, text=True, timeout=1800, check=False)
    if created.returncode != 0:
        made = live_app()
        if made is not None and app_made_by_installation(made, manifest):
            print(f"ADOPT Databricks App {app_name}: `apps create` failed after making it "
                  f"({_tail(created, 8).replace(chr(10), ' | ')})", flush=True)
            return adopt(made)
        raise AzureSetupError(
            f"could not create the Databricks App {app_name}: {_tail(created, 8)}")
    return adopt(json.loads(created.stdout))


def prepare_round4_source(manifest: DemoManifest, warehouse_id: str) -> None:
    """Round 4's Delta source, made where Data Factory can read it, before Round 4's setup.

    Round 4's own setup (`lifecycle._ensure_round4`) creates the schema and table if they're
    missing, in the catalog's managed location. Made here first:
    - the schema's managed location is the `r4source` container, which the factory's private
      endpoint reaches;
    - the table is one ADF's Delta 2.4 reader can read (`azure_runtime.ROUND4_SOURCE_PROPERTIES`),
      from before Round 4's synced table first reads it.
    Nothing else may commit to it between Prepare and the bell, so predictive optimization is off
    for the schema.
    """

    from .azure_lanes_setup import make_source_readable
    from .azure_runtime import ROUND4_SOURCE_PROPERTIES

    names = lifecycle._round4_names(manifest)
    azure = _azure(manifest)
    catalog, schema = names["catalog"], names["source_schema"]
    location = abfss(azure.storage_account, CONTAINERS["r4_source"], schema)
    profile = manifest.databricks.profile
    for statement in (
        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` MANAGED LOCATION '{location}'",
        f"ALTER SCHEMA `{catalog}`.`{schema}` DISABLE PREDICTIVE OPTIMIZATION",
        f"""CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{lifecycle.ROUND4_SOURCE_TABLE}` (
  entity_id STRING NOT NULL,
  score DOUBLE NOT NULL,
  model_version STRING NOT NULL,
  proof_nonce STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ({ROUND4_SOURCE_PROPERTIES})""",
    ):
        lifecycle._sql_statement(profile, warehouse_id, statement)
    make_source_readable(profile, warehouse_id,
                         f"{catalog}.{schema}.{lifecycle.ROUND4_SOURCE_TABLE}")


def grant_app_service_credential(
    manifest: DemoManifest, workspace: Any, app_client_id: str
) -> None:
    """ACCESS on the App's service credential, for the App's principal alone."""

    from databricks.sdk.service.catalog import PermissionsChange, Privilege

    sealed = _azure(manifest).unity_catalog
    if sealed is None:
        raise AzureSetupError("the App's service credential is not sealed yet")
    workspace.grants.update("credential", sealed.app_service_credential, changes=[
        PermissionsChange(principal=app_client_id, add=[Privilege.ACCESS])])


# --- The run -------------------------------------------------------------------------------------


@dataclass
class AzureSetup:
    """One run of the installer, resumable from whatever status its manifest last saved."""

    inputs: AzureInputs
    generation: Generation = field(default_factory=Generation)
    zero_timeout_seconds: float = 900.0

    def _ensure_secrets(self) -> str:
        """The servers' admin password and the runners' SSH key, made once and kept."""

        self.generation.ensure()
        if not self.generation.admin_password.exists():
            _private(self.generation.admin_password, new_admin_password())
        if not self.generation.runner_key.exists():
            result = subprocess.run(
                ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "anti-demo-runner",
                 "-f", str(self.generation.runner_key)],
                capture_output=True, text=True, timeout=60, check=False)
            if result.returncode != 0:
                raise AzureSetupError(f"ssh-keygen failed: {_tail(result)}")
        return self.generation.runner_key.with_suffix(".pub").read_text(encoding="utf-8").strip()

    def _first_manifest(self, databricks_user: str) -> DemoManifest:
        """A new manifest, keeping the IDs of Terraform variables a lost manifest left behind."""

        kept: dict[str, Any] = {}
        if self.generation.tfvars.exists():
            values = json.loads(self.generation.tfvars.read_text(encoding="utf-8"))
            expires_at = datetime.fromisoformat(values["expires_at"].replace("Z", "+00:00"))
            kept = {"run_id": values["run_id"], "installation_id": values["installation_id"],
                    "expires_at": expires_at}
            print(f"KEEP  run {kept['run_id']}: Terraform's variables outlived its manifest",
                  flush=True)
        print("CHECK the Databricks published egress for this region", flush=True)
        egress = lifecycle.fetch_serverless_egress_cidrs(self.inputs.region, home=AZURE)
        return new_manifest(self.inputs, databricks_user=databricks_user,
                            principal_object_id=operator_object_id(), egress=egress, **kept)

    def run(self) -> DemoManifest:
        """The install, resumed by itself after a transient error, as running it again would be.

        Every stage picks up where the last run stopped (the manifest's `status`, then each seal),
        so an attempt after a transient control-plane or network error repeats nothing that had
        finished. Three different ones stopped one night's installs at three different steps
        (2026-10-22: a Lakebase INTERNAL_ERROR, a CLI call's EOF, an `az` token). Anything else
        stops the install at once, as before.
        """

        for attempt in range(1, SETUP_ATTEMPTS):
            try:
                return self._run_once()
            except Exception as exc:
                text = f"{type(exc).__name__} {exc}"
                if not lifecycle._sql_statement_error_is_transient(text):
                    raise
                print(f"RESUME the run stopped on a transient error ({text.splitlines()[0][:200]});"
                      f" resuming in {SETUP_RESUME_SECONDS:.0f}s (attempt {attempt + 1} of "
                      f"{SETUP_ATTEMPTS})", flush=True)
                time.sleep(SETUP_RESUME_SECONDS)
        return self._run_once()

    def _run_once(self) -> DemoManifest:
        # Nothing to do with AWS by construction: no AWS setting reaches anything this runs.
        for name in [name for name in os.environ if name.startswith("AWS_")]:
            del os.environ[name]
        os.environ["ANTI_DEMO_MANIFEST"] = str(self.generation.manifest)
        print("CHECK Databricks identity and Lakebase API", flush=True)
        databricks_user = lifecycle._verify_databricks_identity(self.inputs.databricks_profile)
        ssh_public_key = self._ensure_secrets()
        if self.generation.manifest.exists():
            manifest = load_manifest(self.generation.manifest)
            if manifest.cloud != "azure":
                raise AzureSetupError(f"{self.generation.manifest} is an AWS installation's")
            if manifest.databricks.user != databricks_user:
                raise AzureSetupError(f"this installation was set up by {manifest.databricks.user},"
                                      f" not {databricks_user}")
            print(f"RESUME run {manifest.run_id} from {manifest.status}", flush=True)
        else:
            manifest = self._first_manifest(databricks_user)
            save_manifest(manifest, self.generation.manifest)
            print(f"SEAL  run {manifest.run_id}, installation {manifest.installation_id}",
                  flush=True)
        if not self.generation.tfvars.exists():
            _private(self.generation.tfvars, json.dumps(
                terraform_vars(manifest, self.inputs, ssh_public_key=ssh_public_key), indent=2)
                + "\n")
        password = self.generation.admin_password.read_text(encoding="utf-8").strip()
        arm = arm_client(self.inputs)

        if manifest.status == "provisioning":
            run_id = manifest.run_id

            def clear_refused() -> list[str]:
                return clear_refused_runners(arm, run_id, terraform.state_addresses)

            terraform = Terraform(self.generation, self.inputs, clear_refused=clear_refused)
            terraform.apply()
            outputs = terraform.outputs()
            _azure(manifest).servers.update(seal_servers(outputs))
            manifest.azure = AzureManifest.model_validate(_azure(manifest).model_dump())
            save_manifest(manifest, self.generation.manifest)
            approve_factory_endpoints(arm, manifest, outputs)
            seal_lakebase(manifest)
            manifest.status = "seeding"
            save_manifest(manifest, self.generation.manifest)

        if manifest.status == "seeding":
            asyncio.run(seed(manifest, password))
            manifest = lifecycle.ensure_coordination(manifest)
            manifest.status = "waiting_for_zero"
            save_manifest(manifest, self.generation.manifest)

        if manifest.status == "waiting_for_zero":
            lifecycle.ensure_coordination(manifest)
            print("WAIT  for Round 1's Lakebase endpoint to scale to zero", flush=True)
            asyncio.run(lifecycle.wait_for_scale_zero(manifest, self.zero_timeout_seconds))
            manifest.status = "ready"
            save_manifest(manifest, self.generation.manifest)
        print(f"READY run {manifest.run_id}: infrastructure, Lakebase and the seeds", flush=True)
        return self._rounds(manifest, password)

    def _secret(self, name: str) -> str:
        """A password made once for one of the factory's roles, and kept in the generation."""

        from .azure_lanes_setup import new_role_password

        path = self.generation.root / name
        if not path.exists():
            _private(path, new_role_password())
        return path.read_text(encoding="utf-8").strip()

    @staticmethod
    def _round5_stale(manifest: DemoManifest) -> bool:
        """No Round 5 seal yet, or the runners run other files than this tree's."""

        from .azure_round5_runner import azure_runner_sha256
        from .connection_spike_live import runner_harness_sha256

        sealed = manifest.round5_azure
        return (sealed is None or sealed.runner_harness_sha256 != runner_harness_sha256()
                or sealed.azure_runner_sha256 != azure_runner_sha256())

    def _round5(self, manifest: DemoManifest, arm: ArmClient, outputs: dict[str, Any],
                password: str) -> None:
        """Round 5: both runners installed and set up over Run Command, then sealed, then each
        lane's control DSN in Key Vault (server/azure_round5_install.py)."""

        from .azure_identity import az_cli_token_source
        from .azure_key_vault import KEY_VAULT_SCOPE, KeyVault
        from .azure_round5_install import ensure_round5

        vault = KeyVault(str((outputs.get("key_vault") or {}).get("uri") or ""),
                         az_cli_token_source(_azure(manifest).tenant_id, scope=KEY_VAULT_SCOPE))
        manifest.round5_azure = ensure_round5(
            manifest, run=arm.run_shell_script, outputs=outputs, vault=vault,
            admin_password=password, report=lambda line: print(f"R5    {line}", flush=True))
        save_manifest(manifest, self.generation.manifest)
        lifecycle.ensure_coordination(manifest)
        print("SEAL  Round 5: both runners set up, their control DSNs in Key Vault", flush=True)

    def _rounds(self, manifest: DemoManifest, password: str) -> DemoManifest:
        """After READY: Unity Catalog, the App, each round's seals, and the deploy.

        Each stage is skipped once its seal is in the manifest, as the AWS setup's are, so a run
        that stopped resumes at the stage that stopped it.
        """

        from databricks.sdk import WorkspaceClient

        from .azure_deploy import deploy
        from .azure_lanes_setup import ensure_round4_lane, ensure_round6_lane

        if not self.inputs.warehouse_id:
            raise AzureSetupError("DATABRICKS_WAREHOUSE_ID is required for Rounds 4 and 6")
        profile = self.inputs.databricks_profile
        workspace = WorkspaceClient(profile=profile)
        outputs = Terraform(self.generation, self.inputs).outputs()
        if _azure(manifest).unity_catalog is None:
            ensure_unity_catalog(manifest, outputs, workspace)
            save_manifest(manifest, self.generation.manifest)
        sealed_uc = _azure(manifest).unity_catalog
        assert sealed_uc is not None
        app_client_id = ensure_app(self.generation, profile, self.inputs.app_name, manifest)
        os.environ.update({"DATABRICKS_WAREHOUSE_ID": self.inputs.warehouse_id,
                           "DATABRICKS_APP_CLIENT_ID": app_client_id,
                           lifecycle.ROUND4_CATALOG_ENV: sealed_uc.catalog,
                           "DATABRICKS_CDF_CATALOG": sealed_uc.catalog})
        if manifest.round4 is None:
            prepare_round4_source(manifest, self.inputs.warehouse_id)
            manifest = lifecycle._prepare_and_reseal_round4(manifest, timeout=1800)
        grant_app_service_credential(manifest, workspace, app_client_id)
        if manifest.round6 is None:
            manifest = lifecycle._prepare_and_reseal_round6(manifest, timeout=1800)
        arm = arm_client(self.inputs)
        factory = str(_output(outputs, "data_factory")["name"])
        runtime = str(_output(outputs, "data_factory")["integration_runtime_name"])
        if _azure(manifest).round4 is None:
            seal = ensure_round4_lane(manifest, arm, workspace, runtime=runtime, factory=factory,
                                      warehouse_id=self.inputs.warehouse_id,
                                      writer_password=self._secret("round4-writer-password"))
            manifest.azure = _azure(manifest).model_copy(update={"round4": seal})
            save_manifest(manifest, self.generation.manifest)
        if _azure(manifest).round6 is None:
            seal = ensure_round6_lane(manifest, arm, runtime=runtime, factory=factory,
                                      warehouse_id=self.inputs.warehouse_id,
                                      reader_password=self._secret("round6-reader-password"))
            manifest.azure = _azure(manifest).model_copy(update={"round6": seal})
            save_manifest(manifest, self.generation.manifest)
        values = {name: (item or {}).get("value") for name, item in outputs.items()}
        if values.get("round5_runners") and self._round5_stale(manifest):
            # First install, or this tree's runner files differ from those the runners run: the
            # Azure counterpart of AWS's `runner refresh` (reinstall, set up, reseal).
            self._round5(manifest, arm, values, password)
        elif manifest.round5_azure is not None:
            # Every run republishes the lanes' control DSNs (their passwords are kept), so a run
            # that sealed Round 5 and stopped before publishing them is repaired by the next one.
            lifecycle.ensure_coordination(manifest)
        url = deploy(workspace, profile=profile, app_name=self.inputs.app_name,
                     manifest=manifest, admin_password=password,
                     principal=manifest.databricks.user)
        print(f"DEPLOYED {self.inputs.app_name}: {url}", flush=True)
        return manifest


def inputs_from_environment(environ: Mapping[str, str] | None = None) -> AzureInputs:
    """The installer's settings from the environment (`.env.bootstrap` on the Azure path)."""

    env = dict(os.environ if environ is None else environ)
    required = ("DATABRICKS_PROFILE", "AZURE_TENANT_ID", "AZURE_SUBSCRIPTION_ID",
                "AZURE_RESOURCE_GROUP", "AZURE_REGION")
    missing = [name for name in required if not env.get(name, "").strip()]
    if missing:
        raise AzureSetupError(f"the Azure installer needs {', '.join(missing)}")
    # An AWS value in the env file is refused by bootstrap.sh, which names its line; one the
    # operator's shell exported is never read here, and `AzureSetup.run` drops it.
    policy: dict[str, str] = {}
    pairs = (item.strip() for item in env.get("AZURE_POLICY_TAGS", "").split(","))
    for pair in filter(None, pairs):
        name, _, value = pair.partition("=")
        policy[name.strip()] = value.strip()
    return AzureInputs(
        databricks_profile=env["DATABRICKS_PROFILE"].strip(),
        tenant_id=env["AZURE_TENANT_ID"].strip(),
        subscription_id=env["AZURE_SUBSCRIPTION_ID"].strip(),
        resource_group=env["AZURE_RESOURCE_GROUP"].strip(),
        region=env["AZURE_REGION"].strip(),
        owner=env.get("ANTI_DEMO_OWNER", "").strip(),
        # ANTI_DEMO_OPERATOR_IP names it outright, as on AWS; otherwise it's detected.
        operator_cidr=lifecycle._validate_operator_cidr(lifecycle._require_operator_cidr()),
        zone=env.get("AZURE_ZONE", "1").strip() or None,
        policy_tags=policy,
        lease_date_tag=env.get("AZURE_LEASE_DATE_TAG", "").strip() or None,
        warehouse_id=env.get("DATABRICKS_WAREHOUSE_ID", "").strip(),
        app_name=env.get("DATABRICKS_APP_NAME", "").strip() or "lakebase-anti-demo",
    )


def inputs_from_manifest(manifest: DemoManifest, generation: Generation) -> AzureInputs:
    """The settings a sealed installation was built with, for a command that acts on it.

    `./antidemo azure-cleanup` removes exactly the installation in the generation, so it takes the
    subscription, group and profile that installation sealed, never ones exported since. The
    launcher carries only credentials from `.env.bootstrap`, and installation settings are the
    manifest's (the AWS edition's rule).
    """

    azure = _azure(manifest)
    variables = (json.loads(generation.tfvars.read_text(encoding="utf-8"))
                 if generation.tfvars.exists() else {})
    record_path = generation.root / "app.json"
    record = json.loads(record_path.read_text(encoding="utf-8")) if record_path.exists() else {}
    return AzureInputs(
        databricks_profile=manifest.databricks.profile,
        tenant_id=azure.tenant_id,
        subscription_id=azure.subscription_id,
        resource_group=azure.resource_group,
        region=azure.region,
        owner=manifest.owner,
        operator_cidr=azure.operator_cidr,
        zone=variables.get("zone", "1"),
        policy_tags=dict(azure.policy_tags),
        lease_date_tag=variables.get("lease_date_tag"),
        warehouse_id=str(getattr(manifest.round4, "warehouse_id", "") or ""),
        app_name=str(record.get("name") or "lakebase-anti-demo"),
    )


__all__ = [
    "AzureInputs",
    "AzureSetup",
    "AzureSetupError",
    "Generation",
    "Terraform",
    "inputs_from_environment",
    "inputs_from_manifest",
    "new_manifest",
    "only_retryable",
    "seal_servers",
    "storage_account_name",
    "terraform_vars",
]

