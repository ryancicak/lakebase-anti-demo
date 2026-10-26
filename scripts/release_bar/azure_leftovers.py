"""The Azure edition's `leaks` step: no per-bout copy left standing, and no data flow still running.

The AWS edition's `leftovers.py` counts per-bout AWS resources by their tags. On Azure a bout's
resources are:
- Rounds 2 and 3's point-in-time copies, found by their deterministic names
  (`server/azure_inventory.copy_names`), because a restore takes its source's tags;
- Rounds 4 and 6's data flow runs, which bill while they run;
- Round 5's PgBouncer on the r5 server, which a bout switches on and its cleanup switches off: on
  outside a bout, it is AWS's leaked Proxy.

Nothing is changed. A copy can still be finishing a delete that began before READY, so it waits.

Usage: azure_leftovers.py CHECKOUT [--wait SECONDS]
Exit 0 with nothing left, 1 with something still there when the wait ends.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path


def leftovers(arm, manifest) -> list[str]:  # noqa: ANN001 - an ArmClient and a DemoManifest
    from server.azure_inventory import copy_names
    from server.azure_lanes_setup import ROUND4_PIPELINE, ROUND6_PIPELINE

    found = [f"copy {name}" for name in sorted(copy_names(manifest.run_id))
             if arm.get_server(name) is not None]
    seal = manifest.azure.round4 or manifest.azure.round6
    if seal is not None:
        now = datetime.now(UTC)
        window = {"since_utc": (now - timedelta(days=1)).isoformat(),
                  "until_utc": (now + timedelta(minutes=5)).isoformat()}
        for pipeline in (ROUND4_PIPELINE, ROUND6_PIPELINE):
            found.extend(f"{pipeline} run {run.get('runId')} {run.get('status')}"
                         for run in arm.pipeline_runs(seal.factory, pipeline, **window))
    sealed_round5 = getattr(manifest, "round5_azure", None)
    if sealed_round5 is not None:
        enabled = arm.get_configuration(sealed_round5.server_name, "pgbouncer.enabled")
        if str(enabled).lower() == "true":
            found.append(f"PgBouncer still on at {sealed_round5.server_name}")
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("--wait", type=float, default=0.0)
    args = parser.parse_args()
    sys.path.insert(0, str(args.checkout.resolve()))
    from server.azure_arm import ArmClient, AzureCredentials
    from server.azure_identity import az_cli_token_source
    from server.manifest import load_manifest

    manifest = load_manifest(args.checkout / ".anti-demo-azure" / "manifest.json")
    azure = manifest.azure
    if azure is None:
        print("not an Azure installation's manifest")
        return 2
    arm = ArmClient(AzureCredentials(tenant_id=azure.tenant_id,
                                     subscription_id=azure.subscription_id),
                    azure.resource_group, token_source=az_cli_token_source(azure.tenant_id))
    deadline = time.monotonic() + args.wait
    while True:
        found = leftovers(arm, manifest)
        if not found or time.monotonic() >= deadline:
            break
        time.sleep(30)
    print(json.dumps({"run_id": manifest.run_id, "leftovers": found}))
    print("no leftovers" if not found else f"{len(found)} leftover(s): {'; '.join(found)}")
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
