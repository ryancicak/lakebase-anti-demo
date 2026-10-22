"""Settle an Azure installation's per-bout leftovers, before Terraform destroys the rest.

Design section 10, step 1 and 2: what a bout can leave behind that Terraform did not make. Each
step is idempotent and safe to repeat, and each acts only on what `server/azure_inventory.py`
proves is this run's:

1. **Park both data flows.** Cancel any Round 4 or Round 6 pipeline run still in flight.
2. **Delete Round 2's and Round 3's copies.** Found by tag *or* deterministic name, because a
   restore gives its copy the source's tags until the adapter merges its own.
3. **Turn Round 5's PgBouncer off** the way the baseline is proved: the client limit back to
   Azure's default first, then the switch, then 6432 refusing (server/azure_pgbouncer.py).

Nothing here deletes a resource group, and nothing touches what the subscription's own policy
may have attached beside the installation's resources.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from .azure_arm import ArmClient
from .azure_inventory import Standing, copy_names, inventory

SERVER_TYPE = "microsoft.dbforpostgresql/flexibleservers"


@dataclass
class CleanupReport:
    cancelled_runs: list[str] = field(default_factory=list)
    deleted_copies: list[str] = field(default_factory=list)
    pgbouncer_reset: bool | None = None


def _window() -> tuple[str, str]:
    now = datetime.now(UTC)
    return (now - timedelta(days=2)).isoformat().replace("+00:00", "Z"), \
        (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")


def park_pipelines(arm: ArmClient, factory: str, pipelines: Iterable[str]) -> list[str]:
    since, until = _window()
    cancelled = []
    for pipeline in pipelines:
        for run in arm.pipeline_runs(factory, pipeline, since_utc=since, until_utc=until):
            arm.cancel_pipeline_run(factory, str(run["runId"]))
            cancelled.append(str(run["runId"]))
    return cancelled


def release_private_endpoints(
    arm: ArmClient,
    factory: str,
    endpoints: Iterable[str],
    servers: Iterable[str],
    *,
    bound_seconds: float = 900.0,
    sleep: Callable[[float], None] | None = None,
) -> None:
    """Remove ADF's managed private endpoints, then wait until no server still holds a connection.

    Azure refuses to delete a Flexible Server with an active private endpoint connection
    (``PrivateEndpointServerWithActiveConnectionCannotBeDropped``, Phase 0), so this runs before
    the r4 and r6 servers go. In Phase 0 the connection cleared 11 s after its endpoint was deleted.
    """

    pause = sleep or time.sleep
    for name in endpoints:
        arm.request("DELETE", f"{arm.factory_path(factory)}/managedVirtualNetworks/default/"
                              f"managedPrivateEndpoints/{name}?api-version=2018-06-01")
    deadline = time.monotonic() + bound_seconds
    for server in servers:
        while True:
            response = arm.request("GET", f"{arm.server_path(server)}/privateEndpointConnections"
                                          "?api-version=2025-08-01")
            rows = response.body.get("value", []) if isinstance(response.body, dict) else []
            if response.status == 404 or not rows:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"{server} still holds {len(rows)} private endpoint "
                                   "connection(s), so it cannot be deleted yet")
            pause(5.0)


def copies_to_delete(standing: Iterable[Standing], run_id: str) -> list[str]:
    """Round 2 and Round 3 copies of this run: servers whose name is a copy name for the run."""

    names = copy_names(run_id)
    return sorted({item.name for item in standing
                   if item.type.lower() == SERVER_TYPE and item.name.lower() in names})


async def settle(
    arm: ArmClient,
    *,
    run_id: str,
    factory: str | None,
    pipelines: Iterable[str],
    reset_pgbouncer: Callable[[], Awaitable[object]] | None,
) -> CleanupReport:
    """Cancel in-flight data flows, delete this run's copies, and reset PgBouncer. Repeatable."""

    report = CleanupReport()
    if factory:
        report.cancelled_runs = await asyncio.to_thread(park_pipelines, arm, factory, pipelines)
    standing = await asyncio.to_thread(inventory, arm, run_id)
    for name in copies_to_delete(standing, run_id):
        await asyncio.to_thread(arm.delete_server, name)
        report.deleted_copies.append(name)
    if reset_pgbouncer is not None:
        await reset_pgbouncer()
        report.pgbouncer_reset = True
    return report
