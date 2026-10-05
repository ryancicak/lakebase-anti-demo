"""Is the installation's `expires-at` lease kept alive, and is Terraform still quiet?

    lease_check.py CHECKOUT [--plan]

Uses the installation's own code (CHECKOUT's `server/lease.py`) to read the lease
on every resource its Terraform made, and fails unless:

  * the app's `/readyz` reports `lease_state: current`, after one request to the
    app so that it has a use to renew for;
  * every resource could be read and carries a lease;
  * the earliest lease is at least `window - 7h` away. The app retags once the
    lease lags `last use + window` by 6 h, so just after use nothing may be
    further behind than that, plus an hour for its check interval and the retag;
  * once the installation is more than 7 h old, the lease has moved past the
    expiry sealed at install. Nothing in the release bar moves it but the app,
    so this is the proof that the app renewed it by itself.

With `--plan` it then runs `terraform plan`, which applies nothing, and fails
on any proposed resource or output change: the app's retag must be invisible to
Terraform. Two changes are set aside, because nothing the app does makes them:
a resource whose only change is losing tags someone else added to it, and an
output whose list holds the same items in another order. Needs
`ANTI_DEMO_APP_URL` and `ANTI_DEMO_PROFILE`, and uses the AWS key in
CHECKOUT's `.env.bootstrap`. Prints counts and times, never identifiers.
Exit 0 when every check passed, 1 when one failed.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import AppClient, use_checkout  # noqa: E402

#: Allowed on top of the app's own 6 h hysteresis: its 10-minute check, the
#: retag itself, and the minutes between the last use and this check.
SLACK = timedelta(hours=1)
READYZ_WAIT_SECONDS = 12 * 60


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


_TAG_ATTRIBUTES = ("tags", "tags_all")
#: The tag keys the installation's Terraform sets, beside every key under `anti-demo`.
_OWN_TAG_KEYS = frozenset({"Name", "Owner", "owner", "expires-at", "managed-by"})


def _own_tag(key: str) -> bool:
    return key in _OWN_TAG_KEYS or key.startswith("anti-demo")


def _unknown(flag: Any) -> bool:
    """Whether an ``after_unknown`` entry marks anything unknown, at any depth: Terraform writes
    a known nested block as a structure of falses, which reads as true taken whole."""

    if isinstance(flag, bool):
        return flag
    if isinstance(flag, Mapping):
        return any(_unknown(value) for value in flag.values())
    if isinstance(flag, list | tuple):
        return any(_unknown(value) for value in flag)
    return False


def only_loses_foreign_tags(change: Mapping[str, Any]) -> bool:
    """Whether a planned update does nothing but drop tags someone else added.

    rc19 (2026-10-05): the account's own automation tagged two of the installation's VPC
    endpoints at 04:00Z, and the plan after the bar proposed taking the tag off again. So every
    tag the plan keeps must keep its value, and every tag it drops must have a key the
    installation never uses. A tag of its own that is missing, changed or dropped is still
    drift, and so is any other attribute.
    """

    if change.get("actions") != ["update"]:
        return False
    before = change.get("before") or {}
    after = change.get("after") or {}
    if _unknown(change.get("after_unknown")):
        return False
    others = {key for key in {*before, *after} if key not in _TAG_ATTRIBUTES}
    if any(before.get(key) != after.get(key) for key in others):
        return False
    dropped_some = False
    for attribute in _TAG_ATTRIBUTES:
        had = before.get(attribute) or {}
        keeps = after.get(attribute) or {}
        if not isinstance(had, Mapping) or not isinstance(keeps, Mapping):
            return False
        if any(had.get(key) != value for key, value in keeps.items()):
            return False
        dropped = set(had) - set(keeps)
        if any(_own_tag(key) for key in dropped):
            return False
        dropped_some = dropped_some or bool(dropped)
    return dropped_some


def only_reorders(change: Mapping[str, Any]) -> bool:
    """Whether a planned output change holds the same list items in another order.

    rc19 (2026-10-05): AWS listed the same three subnets in another order, and `subnet_ids`
    changed with nothing in it changing.
    """

    before, after = change.get("before"), change.get("after")
    if _unknown(change.get("after_unknown")):
        return False
    if not isinstance(before, list) or not isinstance(after, list):
        return False

    def items(values: list[Any]) -> list[str]:
        return sorted(json.dumps(value, sort_keys=True) for value in values)

    return before != after and items(before) == items(after)


def plan_drift(document: Mapping[str, Any]) -> tuple[list[tuple[str, list[str]]], list[str], int]:
    """A plan's resource and output changes, less the ones set aside, and how many were."""

    changes: list[tuple[str, list[str]]] = []
    set_aside = 0
    for entry in document.get("resource_changes") or []:
        change = entry.get("change") or {}
        if change.get("actions") in (["no-op"], ["read"], None):
            continue
        if only_loses_foreign_tags(change):
            set_aside += 1
            continue
        changes.append((str(entry.get("address")), list(change.get("actions") or [])))
    outputs = []
    for name, change in sorted((document.get("output_changes") or {}).items()):
        if change.get("actions") in (["no-op"], None):
            continue
        if only_reorders(change):
            set_aside += 1
            continue
        outputs.append(name)
    return changes, outputs, set_aside


def wait_for_current_lease(client: AppClient) -> tuple[bool, dict[str, object]]:
    # `/readyz` is a probe and does not count as use; the board does.
    client.call("GET", "/api/bout/all", timeout=60)
    deadline = time.monotonic() + READYZ_WAIT_SECONDS
    ready: dict[str, object] = {}
    while True:
        _, ready = client.call("GET", "/readyz", timeout=60)
        if ready.get("lease_state") == "current" or time.monotonic() >= deadline:
            return ready.get("lease_state") == "current", ready
        time.sleep(15)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if use_checkout(args.checkout) is None:
        raise SystemExit(f"no installation manifest in {args.checkout}")
    from server import lease, lifecycle

    failures: list[str] = []
    current, ready = wait_for_current_lease(AppClient())
    for key in ("lease_state", "lease_expires_at", "lease_renewed_at", "lease_window_hours"):
        print(f"readyz {key}: {ready.get(key)}")
    if not current:
        failures.append(
            f"the app's lease is {ready.get('lease_state')}: {ready.get('lease_detail')}"
        )

    manifest = lifecycle.load_manifest()
    inventory = lease.discover_lease_targets(lifecycle._aws_session(manifest), manifest)
    window = lease.lease_window(manifest)
    sealed = _aware(manifest.expires_at)
    now = datetime.now(UTC)
    counts = collections.Counter(target.service for target in inventory.targets)
    leases = collections.Counter(
        lease.format_lease(target.lease) if target.lease else "none" for target in inventory.targets
    )
    earliest = inventory.earliest
    print("resources by service:", dict(sorted(counts.items())))
    print("unreadable:", list(inventory.unreadable))
    print("window:", window)
    print("sealed at install:", lease.format_lease(sealed))
    print("earliest lease:", lease.format_lease(earliest) if earliest else None)
    print("distinct leases:", dict(leases))
    if not inventory.targets:
        failures.append("no Terraform-made resources were found")
    if inventory.unreadable:
        failures.append(f"leases could not be read for: {', '.join(inventory.unreadable)}")
    floor = now + window - lease.RENEW_HYSTERESIS - SLACK
    if earliest is None:
        failures.append("some resource carries no readable lease")
    elif earliest < floor:
        failures.append(
            f"the earliest lease {lease.format_lease(earliest)} is before "
            f"{lease.format_lease(floor)}: the app is not keeping it alive"
        )
    age = now - _aware(manifest.created_at)
    if earliest is not None and age > lease.RENEW_HYSTERESIS + SLACK:
        if earliest <= sealed:
            failures.append(
                f"the installation is {age} old and its lease never moved past the "
                "expiry sealed at install"
            )
    elif earliest is not None:
        print(f"too young ({age}) for a renewal to be due; not checking that one happened")

    if args.plan:
        lifecycle._terraform_init(manifest)
        document = lifecycle._terraform_plan_json(manifest, lifecycle._terraform_plan(manifest))
        changes, outputs, set_aside = plan_drift(document)
        print(
            "terraform plan changes set aside (foreign tags dropped, outputs reordered):",
            set_aside,
        )
        print("terraform plan resource changes:", len(changes))
        for address, actions in changes:
            print("  ", "+".join(actions or []), address)
        print("terraform plan output changes:", outputs)
        if changes or outputs:
            failures.append(f"terraform plan proposes {len(changes)} resource changes, {outputs}")

    for failure in failures:
        print("FAIL:", failure)
    print("LEASE OK" if not failures else "LEASE CHECK FAILED")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
