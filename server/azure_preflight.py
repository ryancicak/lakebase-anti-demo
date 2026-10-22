"""The Azure edition's preflight: what the subscription and resource group would refuse, found
before anything is built (design section 8.2). Read-only.

Each check here is one Phase 0 met the hard way (2026-10-18), and each names its own remedy:

* **Role assignments.** The managed identities for ADF, the Access Connector and the runners need
  role assignments, and Contributor can't make them. The operator's effective permissions on the
  group read ``*`` with ``Microsoft.Authorization/*/Write`` excluded, so the install would fail
  at its first grant. The check reads the permissions API, not role names, because a custom role
  or an inherited group grant can allow or deny it too.
* **The tags the subscription's policy requires.** A subscription can require tags of its own on
  every resource, such as a cost center or a date. The installer can't know which, so they are
  settings (``AZURE_POLICY_TAGS``, ``AZURE_LEASE_DATE_TAG``), and the check confirms the group
  carries them and that a date tag hasn't passed.
* **Quota:** 16 Dlsv7 vCPUs for the two runners, Flexible Server cores for five servers plus a Round
  2 and a Round 3 copy, and one storage account in a region that held 234 of 250.
* **Resource providers,** each Registered: a Contributor can't register one at subscription scope.

**What it can't check.** The Flexible Server capabilities API offered B2s in all three eastus2
zones, and every create there failed ``SkuNotAvailable``. No read predicts that, so the apply
reports it with its remedy (another region, or General Purpose) instead.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from .azure_arm import ArmClient

ROLE_ASSIGNMENT_WRITE = "Microsoft.Authorization/roleAssignments/write"
REQUIRED_PROVIDERS = (
    "Microsoft.DBforPostgreSQL", "Microsoft.Network", "Microsoft.Compute", "Microsoft.Storage",
    "Microsoft.DataFactory", "Microsoft.ServiceBus", "Microsoft.KeyVault",
    "Microsoft.ManagedIdentity", "Microsoft.Databricks",
)
#: Two of Round 5's runners (`capacity.AZURE_ROUND5_RUNNER_SIZE`, 8 vCPUs each), from that size's
#: family quota.
RUNNER_VCPUS = 16
RUNNER_FAMILY = "StandardDlsv7Family"
#: Five servers of 2 vCores, plus a Round 2 copy and a Round 3 copy alive at once.
POSTGRES_CORES = 14


@dataclass(frozen=True)
class Finding:
    check: str
    ok: bool
    detail: str
    remedy: str | None = None


def action_allowed(permissions: Sequence[Mapping[str, Any]], action: str) -> bool:
    """Whether ARM's effective permissions allow `action`.

    Each entry allows what its ``actions`` match and its own ``notActions`` don't (wildcards,
    case-insensitive); entries combine as a union, as Azure evaluates them.
    """

    wanted = action.lower()

    def matches(patterns: Iterable[str]) -> bool:
        return any(fnmatch.fnmatchcase(wanted, str(pattern).lower()) for pattern in patterns)

    return any(matches(entry.get("actions") or []) and not matches(entry.get("notActions") or [])
               for entry in permissions)


def check_role_assignments(arm: ArmClient) -> Finding:
    response = arm.request(
        "GET", f"{arm.resource_group_path}/providers/Microsoft.Authorization/permissions"
               "?api-version=2022-04-01")
    entries = response.body.get("value", []) if isinstance(response.body, dict) else []
    if response.status != 200:
        return Finding("role assignments", False, f"permissions unreadable ({response.status})",
                       "Give the installer's principal Reader on the resource group at least.")
    if action_allowed(entries, ROLE_ASSIGNMENT_WRITE):
        return Finding("role assignments", True, "the principal can assign roles on the group")
    return Finding(
        "role assignments", False,
        "the principal cannot assign roles on the resource group (Contributor excludes "
        "Microsoft.Authorization/*/Write)",
        "Grant Role Based Access Control Administrator (or User Access Administrator) on the "
        "resource group, or set manage_role_assignments = false and have an administrator make "
        "the assignments Terraform lists.")


def check_resource_group_tags(
    arm: ArmClient,
    *,
    required: Iterable[str] = (),
    date_tag: str | None = None,
    today: date | None = None,
) -> Finding:
    """The group carries every tag the subscription's policy requires, and a date tag is ahead.

    `required` and `date_tag` come from the installation's settings; with neither, there is
    nothing to check. Azure compares tag names without case, and so does this.
    """

    wanted = [*required, *([date_tag] if date_tag else [])]
    if not wanted:
        return Finding("group tags", True, "no tags required by the subscription's policy")
    response = arm.request("GET", f"{arm.resource_group_path}?api-version=2021-04-01")
    tags = {str(key).lower(): str(value) for key, value in
            ((response.body or {}).get("tags") or {}).items()} if isinstance(response.body,
                                                                               dict) else {}
    missing = [name for name in wanted if name.lower() not in tags]
    if missing:
        return Finding("group tags", False, f"missing {', '.join(missing)}",
                       f"Tag the resource group with {', '.join(missing)}, as the "
                       "subscription's policy requires.")
    if not date_tag:
        return Finding("group tags", True, f"{', '.join(wanted)} set")
    value = tags[date_tag.lower()]
    try:
        deadline = date.fromisoformat(value)
    except ValueError:
        return Finding("group tags", False, f"{date_tag} is not YYYY-MM-DD: {value}",
                       f"Set {date_tag} to a date in YYYY-MM-DD form.")
    if deadline <= (today or datetime.now(UTC).date()):
        return Finding("group tags", False, f"{date_tag} {deadline} has passed",
                       f"Move {date_tag} forward before installing.")
    return Finding("group tags", True, f"{', '.join(wanted)} set; {date_tag} {deadline}")


def check_providers(arm: ArmClient, namespaces: Iterable[str] = REQUIRED_PROVIDERS) -> Finding:
    subscription = arm.resource_group_path.split("/resourceGroups/")[0]
    unregistered = []
    for namespace in namespaces:
        path = f"{subscription}/providers/{namespace}?api-version=2021-04-01"
        response = arm.request("GET", path)
        state = response.body.get("registrationState") if isinstance(response.body, dict) else None
        if state != "Registered":
            unregistered.append(f"{namespace} ({state or response.status})")
    if unregistered:
        return Finding("resource providers", False, f"not registered: {', '.join(unregistered)}",
                       "Ask a subscription owner to register them (az provider register).")
    return Finding("resource providers", True, "all registered")


def _usage(rows: Iterable[Mapping[str, Any]], name: str) -> tuple[int, int] | None:
    for row in rows:
        if str((row.get("name") or {}).get("value", "")).lower() == name.lower():
            return int(row.get("currentValue") or 0), int(row.get("limit") or 0)
    return None


def check_quota(arm: ArmClient, region: str) -> list[Finding]:
    subscription = arm.resource_group_path.split("/resourceGroups/")[0]
    wanted = (
        ("runner vCPUs (Dlsv7)",
         f"{subscription}/providers/Microsoft.Compute/locations/{region}/usages"
         "?api-version=2024-07-01", RUNNER_FAMILY, RUNNER_VCPUS),
        ("Flexible Server cores",
         f"{subscription}/providers/Microsoft.DBforPostgreSQL/locations/{region}/resourceType/"
         "flexibleServers/usages?api-version=2024-08-01", "cores", POSTGRES_CORES),
        ("storage accounts",
         f"{subscription}/providers/Microsoft.Storage/locations/{region}/usages"
         "?api-version=2023-05-01", "StorageAccounts", 1),
    )
    findings = []
    for label, path, name, need in wanted:
        response = arm.request("GET", path)
        rows = response.body.get("value", []) if isinstance(response.body, dict) else []
        usage = _usage(rows, name)
        if usage is None:
            findings.append(Finding(label, False, f"no '{name}' usage reported in {region}",
                                    "Check the region offers it to this subscription."))
            continue
        used, limit = usage
        free = limit - used
        findings.append(Finding(
            label, free >= need, f"{used} of {limit} used, {free} free, {need} needed",
            None if free >= need else f"Request {need - free} more in {region}, or pick another "
                                      "region."))
    return findings


def run(
    arm: ArmClient,
    region: str,
    *,
    required_tags: Iterable[str] = (),
    lease_date_tag: str | None = None,
) -> list[Finding]:
    return [check_role_assignments(arm),
            check_resource_group_tags(arm, required=required_tags, date_tag=lease_date_tag),
            check_providers(arm), *check_quota(arm, region)]
