"""Keep an Azure installation's lease moving while people use it: the Azure half of server/lease.py.

The rules are `server/lease.py`'s, and its `LeaseKeeper` runs this unchanged (`azure_lease_keeper`).
Only discovery and the tagging call are Azure's:

* **Only what Terraform made.** A resource is a lease target when it carries this run's
  ``anti-demo-run-id`` and ``managed-by = terraform``. Round 2's and Round 3's copies carry their
  own sealed expiry and are left as they are, as on AWS. So is the resource group, which the
  installation never creates, and whatever the subscription's policy attached beside it.
* **One call per resource, keeping its other tags.** The ARM Tags API's ``Merge`` writes only the
  lease's keys: ``expires-at``, and the subscription's date tag when the installation names one
  (`AzureLeaseScope.lease_date_tag`), holding the lease's date.
* **Never backwards.** ``expires-at`` moves only to a later value. The date tag is written only when
  the new date is later than the one the resource carries, so a date someone moved further out by
  hand stays where they put it.
* **Data Factory rewrites the lease.** A factory hands its tags back with any ISO timestamp
  reformatted: ``2026-10-22T08:05:18Z`` was read back as ``10/22/2026 08:05:18`` (Phase 0,
  2026-10-19), still UTC. A plain date is left alone. Read only as the ISO form, the factory's lease
  would be "unknown" forever and retagged on every pass, so both forms are read.
* **A server's new tags show late.** A Flexible Server's merged tags appeared in reads about a
  minute after the merge. A renewal's own result is the record of what it moved, never a re-read.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from .azure_arm import ArmClient, ArmError
from .azure_inventory import copy_names
from .lease import (
    LEASE_TAG,
    MANAGED_BY_TAG,
    RUN_ID_TAG,
    TERRAFORM_MANAGED,
    LeaseInventory,
    LeaseKeeper,
    LeaseRenewal,
    LeaseTarget,
    format_lease,
    parse_lease,
)
from .manifest import DemoManifest

RESOURCES_API = "2021-04-01"


@dataclass(frozen=True)
class AzureLeaseScope:
    run_id: str
    #: The tag that carries the lease's date (YYYY-MM-DD) for the subscription's policy, if any.
    lease_date_tag: str | None = None


@dataclass(frozen=True)
class AzureLeaseInventory(LeaseInventory):
    #: Each target's current date tag, by resource ID, where it carries one.
    date_tags: Mapping[str, str] = field(default_factory=dict)


def parse_azure_lease(value: object) -> datetime | None:
    """The lease in either form Azure hands back: ours, or Data Factory's rewrite of it (UTC)."""

    lease = parse_lease(value)
    if lease is not None or not isinstance(value, str):
        return lease
    try:
        return datetime.strptime(value.strip(), "%m/%d/%Y %H:%M:%S").replace(tzinfo=UTC)
    except ValueError:
        return None


def _date_tag_behind(current: str | None, target: date) -> bool:
    """Whether the date tag should move to `target`: it's missing, or a date before it.

    Read as ISO or as the US form Azure services rewrite dates into. A value that is neither is
    someone's own and is left exactly as it is, never pulled back.
    """

    if current is None:
        return True
    for form in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(current.strip()[:10], form).date() < target
        except ValueError:
            continue
    return False


def _tags(resource: Mapping[str, Any]) -> dict[str, str]:
    # Azure compares tag names without case; so does this.
    return {str(key).lower(): str(value) for key, value in (resource.get("tags") or {}).items()}


def _list(arm: ArmClient, url: str) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    while url:
        response = arm.request("GET", url)
        if response.status != 200 or not isinstance(response.body, dict):
            raise ArmError(f"listing the resource group: {response.status}", status=response.status)
        found.extend(response.body.get("value", []))
        url = response.body.get("nextLink") or ""
    return found


def discover_azure_lease_targets(arm: ArmClient, scope: AzureLeaseScope) -> AzureLeaseInventory:
    """Every Terraform-made resource of this installation in its group, with its lease. Reads only.

    A group that can't be listed is reported in `unreadable`, never passed off as a group with
    nothing in it.
    """

    try:
        resources = _list(arm, f"{arm.resource_group_path}/resources?api-version={RESOURCES_API}")
    except ArmError as error:
        return AzureLeaseInventory((), (f"resource group: {error.code or error.status}",))
    targets: list[LeaseTarget] = []
    date_tags: dict[str, str] = {}
    copies = copy_names(scope.run_id)
    for resource in resources:
        tags = _tags(resource)
        if tags.get(RUN_ID_TAG) != scope.run_id or tags.get(MANAGED_BY_TAG) != TERRAFORM_MANAGED:
            continue
        if (str(resource.get("type") or "").lower() == "microsoft.dbforpostgresql/flexibleservers"
                and str(resource.get("name") or "").lower() in copies):
            # A Round 2 or 3 copy wears its source's Terraform tags until its own merge shows,
            # about 20 minutes later (Phase 0). Its adapter seals its expiry, not the lease.
            continue
        identifier = str(resource.get("id") or "")
        targets.append(LeaseTarget(str(resource.get("type") or ""), identifier,
                                   parse_azure_lease(tags.get(LEASE_TAG))))
        if scope.lease_date_tag and scope.lease_date_tag.lower() in tags:
            date_tags[identifier] = tags[scope.lease_date_tag.lower()]
    return AzureLeaseInventory(tuple(targets), date_tags=date_tags)


def renew_azure_lease(
    arm: ArmClient,
    scope: AzureLeaseScope,
    target: datetime,
    *,
    minimum_gain: timedelta = timedelta(0),
    inventory: AzureLeaseInventory | None = None,
) -> LeaseRenewal:
    """Move the lease on every Terraform-made resource to `target`, never back.

    A resource is retagged only when `target` is at least `minimum_gain` past its current lease,
    or it carries no lease this can read. Never raises for a single resource: each refusal is
    collected, so one resource Azure will not tag can't keep the rest on the old lease.
    """

    target = target.astimezone(UTC).replace(microsecond=0)
    inventory = inventory if inventory is not None else discover_azure_lease_targets(arm, scope)
    value = format_lease(target)
    lagging = [
        item for item in inventory.targets
        if item.lease is None or target - item.lease >= max(minimum_gain, timedelta(seconds=1))
    ]
    renewed = 0
    vanished = 0
    failed: list[str] = []
    after: list[datetime | None] = [item.lease for item in inventory.targets if item not in lagging]
    for item in lagging:
        tags = {LEASE_TAG: value}
        if scope.lease_date_tag and _date_tag_behind(inventory.date_tags.get(item.identifier),
                                                     target.date()):
            tags[scope.lease_date_tag] = value[:10]
        try:
            arm.merge_tags(item.identifier, tags)
        except ArmError as error:
            if error.status == 404:
                vanished += 1
                continue
            failed.append(f"{item.service} {error.code or error.status}")
            after.append(item.lease)
            continue
        renewed += 1
        after.append(target)
    earliest = (None if not after or any(lease is None for lease in after)
                else min(lease for lease in after if lease is not None))
    return LeaseRenewal(target=target, resources=len(inventory.targets), renewed=renewed,
                        vanished=vanished, failed=tuple(failed), unreadable=inventory.unreadable,
                        earliest=earliest)


def azure_lease_keeper(
    manifest: DemoManifest,
    scope: AzureLeaseScope,
    arm_factory: Callable[[], ArmClient],
    **keeper_options: Any,
) -> LeaseKeeper:
    """`server/lease.py`'s keeper, renewing through ARM instead of boto3."""

    def renew(arm: ArmClient, _manifest: DemoManifest, target: datetime,
              **options: Any) -> LeaseRenewal:
        return renew_azure_lease(arm, scope, target, **options)

    return LeaseKeeper(manifest, lambda _manifest: arm_factory(), renew=renew, **keeper_options)
