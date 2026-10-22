"""What an Azure installation still has standing: the read-only half of cleanup and of ALL GONE.

The design (section 10) proves ALL GONE by what is left, never by having deleted a resource group.
An installation's resources are found two ways, because Phase 0 showed tags alone are not enough:

* **By tag.** Everything Terraform and the lanes create carries ``anti-demo-run-id``.
* **By deterministic name.** A point-in-time restore gives its copy the *source's* tags, so a
  Round 2 or Round 3 copy is untagged until the adapter merges its own tags on. A crash between
  the two leaves a copy findable only by its name, ``adsc-<run>-azure`` or ``adrc-<run>-azure``.

**Nothing else in the group is the installation's.** A subscription's policy can attach resources
of its own beside an installation's (to a new VNet, for example), and they carry none of the run's
tags. They are neither counted against ALL GONE nor deleted.

Read-only: nothing here deletes.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .azure_arm import ArmClient, ArmError
from .recovery import deterministic_recovery_artifact_id
from .safe_change import SafeChangeProvider, deterministic_artifact_id

RUN_TAG = "anti-demo-run-id"


@dataclass(frozen=True)
class Standing:
    name: str
    type: str
    reason: str  # "run tag" or "copy name"


def copy_names(run_id: str) -> frozenset[str]:
    """The only names a Round 2 or Round 3 copy of this run can have."""

    provider = SafeChangeProvider.AZURE_POSTGRES
    return frozenset({deterministic_artifact_id(run_id, provider),
                      deterministic_recovery_artifact_id(run_id, provider)})


def classify(resources: Iterable[Mapping[str, Any]], run_id: str) -> list[Standing]:
    copies = copy_names(run_id)
    found: list[Standing] = []
    for resource in resources:
        name = str(resource.get("name") or "")
        kind = str(resource.get("type") or "")
        tags = {str(key).lower(): str(value) for key, value in (resource.get("tags") or {}).items()}
        if tags.get(RUN_TAG) == run_id:
            found.append(Standing(name, kind, "run tag"))
        elif (kind.lower() == "microsoft.dbforpostgresql/flexibleservers"
              and name.lower() in copies):
            found.append(Standing(name, kind, "copy name"))
    return found


def _list(arm: ArmClient, url: str) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    while url:
        response = arm.request("GET", url)
        if response.status != 200 or not isinstance(response.body, dict):
            raise ArmError(f"listing the resource group: {response.status}", status=response.status)
        found.extend(response.body.get("value", []))
        url = response.body.get("nextLink") or ""
    return found


def inventory(arm: ArmClient, run_id: str) -> list[Standing]:
    """Everything in the installation's resource group that belongs to `run_id`, or rides on it.

    The group's generic resource list does not show a Flexible Server that is still being
    restored (Phase 0: a Round 2 copy in Provisioning was missing from it and present in the
    provider's own list), so servers are read from the provider too. Without that, ALL GONE could
    be declared while a copy was still on its way.
    """

    generic = _list(arm, f"{arm.resource_group_path}/resources?api-version=2021-04-01")
    servers = [
        {**server, "type": "Microsoft.DBforPostgreSQL/flexibleServers"}
        for server in _list(arm, f"{arm.resource_group_path}/providers/Microsoft.DBforPostgreSQL/"
                                 "flexibleServers?api-version=2025-08-01")]
    seen = {(str(r.get("type", "")).lower(), str(r.get("name", "")).lower()) for r in generic}
    extra = [s for s in servers
             if (str(s["type"]).lower(), str(s.get("name", "")).lower()) not in seen]
    return classify([*generic, *extra], run_id)


def all_gone(standing: Iterable[Standing]) -> bool:
    """ALL GONE: nothing of the run's left, by tag or by copy name."""

    return not list(standing)
