"""One failed renewal does not end the lease heartbeat under a held Round 5 operation.

``_run_holding_lease`` renews the coordinator lease on a third of its TTL for the whole of a
held warm, refresh or cleanup reconcile, which can run for minutes. Its beat handled only a lost
or conflicting fence. Any other error, such as the coordination store's own
``psycopg.OperationalError`` once its retry is spent, ended the beat silently, so the lease lapsed
under work that still owned it and the next authority guard aborted that work.
"""

from __future__ import annotations

import asyncio

import psycopg

from tests.test_round5_cleanup_lease_heartbeat_and_wake import (
    _cleaning_coordinator,
    _HeartbeatCountingStore,
    _PausableReconcileProvider,
)
from tests.test_round5_warm import Clock


class _OneRefusedHeartbeatStore(_HeartbeatCountingStore):
    async def heartbeat_coordinator(self, slot, **kwargs):
        if self.heartbeat_calls == 0:
            self.heartbeat_calls += 1
            raise psycopg.OperationalError("connection refused")
        return await super().heartbeat_coordinator(slot, **kwargs)


async def test_a_refused_renewal_is_retried_on_the_next_beat_of_a_held_reconcile() -> None:
    clock = Clock()
    store = _OneRefusedHeartbeatStore()
    provider = _PausableReconcileProvider(clock)
    provider.reconcile_result = True
    manager, claim_id = await _cleaning_coordinator(
        clock, store, provider, coordinator_ttl_seconds=0.09
    )

    task = asyncio.create_task(manager.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)
    await asyncio.sleep(0.25)

    # Renewed again after the refusal, while the reconcile still runs.
    assert store.heartbeat_calls >= 3
    assert not task.done()

    provider.release_reconcile.set()
    await asyncio.wait_for(task, timeout=1)
    assert provider.reconcile_completed_normally
    assert not provider.reconcile_cancelled
