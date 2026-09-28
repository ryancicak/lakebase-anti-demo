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
Terraform. Needs `ANTI_DEMO_APP_URL` and `ANTI_DEMO_PROFILE`, and uses the AWS
key in CHECKOUT's `.env.bootstrap`. Prints counts and times, never identifiers.
Exit 0 when every check passed, 1 when one failed.
"""

from __future__ import annotations

import argparse
import collections
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import AppClient, use_checkout  # noqa: E402

#: Allowed on top of the app's own 6 h hysteresis: its 10-minute check, the
#: retag itself, and the minutes between the last use and this check.
SLACK = timedelta(hours=1)
READYZ_WAIT_SECONDS = 12 * 60


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


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
        changes = [
            (entry.get("address"), entry.get("change", {}).get("actions"))
            for entry in document.get("resource_changes") or []
            if entry.get("change", {}).get("actions") not in (["no-op"], ["read"], None)
        ]
        outputs = sorted(
            name
            for name, change in (document.get("output_changes") or {}).items()
            if change.get("actions") not in (["no-op"], None)
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
