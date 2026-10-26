"""Is an Azure installation's `expires-at` lease kept alive? The Azure edition's `lease` step.

    azure_lease_check.py CHECKOUT

`lease_check.py`'s rules, read through the Azure edition's own discovery
(CHECKOUT's `server/azure_lease.py`: every resource in the installation's group that carries its
run ID and `managed-by = terraform`, Rounds 2 and 3's copies set aside). It fails unless:

  * the app's `/readyz` reports `lease_state: current`, after one request to the app so that it
    has a use to renew for;
  * the group could be listed, and every resource carries a lease;
  * the earliest lease is at least `window - 7h` away;
  * once the installation is more than 7 h old, the lease has moved past the expiry sealed at
    install: nothing in the release bar moves it but the app.

Terraform's half of `lease` is the operator's own `terraform plan` of `infra/azure` (docs/AZURE.md),
which must propose nothing. Needs `ANTI_DEMO_APP_URL`, `ANTI_DEMO_PROFILE` and `az login` in the
installation's tenant. Prints counts and times, never identifiers. Exit 0 when every check passed,
1 when one failed.
"""

from __future__ import annotations

import argparse
import collections
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import AppClient  # noqa: E402
from lease_check import SLACK, _aware, wait_for_current_lease  # noqa: E402

#: How long a lease behind the floor is read again before it counts.
TAG_SETTLE_SECONDS = 300.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    args = parser.parse_args()
    checkout = args.checkout.resolve()
    sys.path.insert(0, str(checkout))
    from server import lease
    from server.azure_arm import ArmClient, AzureCredentials
    from server.azure_identity import az_cli_token_source
    from server.azure_lease import AzureLeaseScope, discover_azure_lease_targets
    from server.manifest import load_manifest

    manifest_path = checkout / ".anti-demo-azure" / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"no Azure installation manifest in {checkout}")
    manifest = load_manifest(manifest_path)
    azure = manifest.azure
    if azure is None:
        raise SystemExit("not an Azure installation's manifest")

    failures: list[str] = []
    current, ready = wait_for_current_lease(AppClient())
    for key in ("lease_state", "lease_expires_at", "lease_renewed_at", "lease_window_hours"):
        print(f"readyz {key}: {ready.get(key)}")
    if not current:
        failures.append(
            f"the app's lease is {ready.get('lease_state')}: {ready.get('lease_detail')}"
        )

    arm = ArmClient(AzureCredentials(tenant_id=azure.tenant_id,
                                     subscription_id=azure.subscription_id),
                    azure.resource_group, token_source=az_cli_token_source(azure.tenant_id))
    window = lease.lease_window(manifest)
    sealed = _aware(manifest.expires_at)
    # The App reports `current` from its own write, and a server's merged tags show in reads
    # about a minute later (server/azure_lease.py), so a lease behind the floor is read again.
    settle_by = time.monotonic() + TAG_SETTLE_SECONDS
    while True:
        inventory = discover_azure_lease_targets(arm, AzureLeaseScope(run_id=manifest.run_id))
        now = datetime.now(UTC)
        floor = now + window - lease.RENEW_HYSTERESIS - SLACK
        settled = inventory.earliest is not None and inventory.earliest >= floor
        if settled or inventory.unreadable or time.monotonic() >= settle_by:
            break
        time.sleep(30)
    counts = collections.Counter(target.service for target in inventory.targets)
    leases = collections.Counter(
        lease.format_lease(target.lease) if target.lease else "none" for target in inventory.targets
    )
    earliest = inventory.earliest
    print("resources by type:", dict(sorted(counts.items())))
    print("unreadable:", list(inventory.unreadable))
    print("window:", window)
    print("sealed at install:", lease.format_lease(sealed))
    print("earliest lease:", lease.format_lease(earliest) if earliest else None)
    print("distinct leases:", dict(leases))
    if not inventory.targets:
        failures.append("no Terraform-made resources were found")
    if inventory.unreadable:
        failures.append(f"leases could not be read for: {', '.join(inventory.unreadable)}")
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

    for failure in failures:
        print("FAIL:", failure)
    print("LEASE OK" if not failures else "LEASE CHECK FAILED")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
