"""B12 / G09: a cancelled startup cleanup stops before `main` is released (COORD-LEASE-6.a, 6.b).

Cancelling the main readiness gate mid-cleanup used to leave the reset operation
running while ``_lead_cleanup`` released ``main``, so the deletes ran unfenced.
"""

from __future__ import annotations

import asyncio

from server.readiness import ShowtimeReadinessGate
from tests.test_readiness import DurableFakeLeaseStore, FakeReadinessStore, manifest


class BlockingCleanup:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.cancelled = False
        self.finished = False

    async def reset_all(self) -> None:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        finally:
            self.finished = True


class Done:
    async def reset_all(self) -> None:
        return None


class RecordingLeaseStore(DurableFakeLeaseStore):
    def __init__(self, cleanup: BlockingCleanup) -> None:
        super().__init__()
        self.cleanup = cleanup
        self.release_saw_cleanup_running: list[bool] = []

    async def release(self, lease):
        self.release_saw_cleanup_running.append(not self.cleanup.finished)
        return await super().release(lease)


async def test_cancelling_the_gate_stops_cleanup_before_main_is_released() -> None:
    cleanup = BlockingCleanup()
    leases = RecordingLeaseStore(cleanup)
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: cleanup,
        recovery_factory=lambda _manifest: Done(),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
        heartbeat_seconds=0.01,
        lease_seconds=1,
    )
    run = asyncio.create_task(gate._lead_cleanup())
    await asyncio.wait_for(cleanup.entered.wait(), timeout=1)
    run.cancel()
    await asyncio.gather(run, return_exceptions=True)
    # 6.a: the operation was cancelled and has finished.
    assert cleanup.cancelled and cleanup.finished
    orphans = [
        task
        for task in asyncio.all_tasks()
        if task.get_name() == "startup-cleanup-operation" and not task.done()
    ]
    assert orphans == []
    # 6.b: `main` was released only after the deletes stopped.
    assert leases.release_saw_cleanup_running
    assert not any(leases.release_saw_cleanup_running)
    assert await leases.current() is None


class ShieldedDeleteAdapter:
    """A live adapter's delete: a shielded call that outlives its caller's cancel."""

    def __init__(self) -> None:
        self.land = asyncio.Event()
        self.landed = False
        self.pending: set[asyncio.Task[None]] = set()

    async def _wire_delete(self) -> None:
        await self.land.wait()
        self.landed = True

    async def delete(self) -> None:
        task = asyncio.create_task(self._wire_delete())
        self.pending.add(task)
        await asyncio.shield(task)

    async def settle_pending_mutations(self) -> None:
        await asyncio.gather(*(asyncio.shield(task) for task in self.pending))


class ShieldedDeleteCleanup:
    def __init__(self) -> None:
        self.lakebase = ShieldedDeleteAdapter()
        self.competitors = {"aurora": ShieldedDeleteAdapter()}
        self.entered = asyncio.Event()

    async def reset_all(self) -> None:
        deletes = [self.lakebase.delete(), *(a.delete() for a in self.competitors.values())]
        self.entered.set()
        await asyncio.gather(*deletes)


class DeleteRecordingLeaseStore(DurableFakeLeaseStore):
    def __init__(self, cleanup: ShieldedDeleteCleanup) -> None:
        super().__init__()
        self.cleanup = cleanup
        self.release_saw_delete_running: list[bool] = []

    async def release(self, lease):
        adapters = [self.cleanup.lakebase, *self.cleanup.competitors.values()]
        self.release_saw_delete_running.append(not all(a.landed for a in adapters))
        return await super().release(lease)


async def test_cancel_holds_main_until_shielded_deletes_land() -> None:
    cleanup = ShieldedDeleteCleanup()
    leases = DeleteRecordingLeaseStore(cleanup)
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: cleanup,
        recovery_factory=lambda _manifest: Done(),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
        heartbeat_seconds=0.01,
        lease_seconds=1,
    )
    run = asyncio.create_task(gate._lead_cleanup())
    await asyncio.wait_for(cleanup.entered.wait(), timeout=1)
    await asyncio.sleep(0)
    run.cancel()
    await asyncio.sleep(0.05)
    assert not run.done()
    assert leases.release_saw_delete_running == []
    for adapter in (cleanup.lakebase, *cleanup.competitors.values()):
        adapter.land.set()
    await asyncio.gather(run, return_exceptions=True)
    assert leases.release_saw_delete_running
    assert not any(leases.release_saw_delete_running)
    assert await leases.current() is None


async def test_a_second_cancel_during_drain_still_holds_main_until_deletes_land() -> None:
    cleanup = ShieldedDeleteCleanup()
    leases = DeleteRecordingLeaseStore(cleanup)
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: cleanup,
        recovery_factory=lambda _manifest: Done(),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
        heartbeat_seconds=0.01,
        lease_seconds=1,
    )
    run = asyncio.create_task(gate._lead_cleanup())
    await asyncio.wait_for(cleanup.entered.wait(), timeout=1)
    await asyncio.sleep(0)
    run.cancel()
    await asyncio.sleep(0.05)
    assert not run.done()
    run.cancel()
    await asyncio.sleep(0.05)
    assert not run.done()
    assert leases.release_saw_delete_running == []
    for adapter in (cleanup.lakebase, *cleanup.competitors.values()):
        adapter.land.set()
    await asyncio.gather(run, return_exceptions=True)
    assert leases.release_saw_delete_running == [False]
    assert await leases.current() is None


class TimedOutSettleAdapter(ShieldedDeleteAdapter):
    async def settle_pending_mutations(self) -> None:
        raise TimeoutError("AWS mutation is still unresolved")


class UnhookedDeleteAdapter:
    """A delete whose adapter has no settlement hook."""

    def __init__(self) -> None:
        self.land = asyncio.Event()
        self.landed = False

    async def delete(self) -> None:
        await self.land.wait()
        self.landed = True


async def cancelled_mid_delete(cleanup: ShieldedDeleteCleanup):
    leases = DeleteRecordingLeaseStore(cleanup)
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: cleanup,
        recovery_factory=lambda _manifest: Done(),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
        heartbeat_seconds=0.01,
        lease_seconds=1,
    )
    run = asyncio.create_task(gate._lead_cleanup())
    await asyncio.wait_for(cleanup.entered.wait(), timeout=1)
    await asyncio.sleep(0)
    run.cancel()
    return leases, run


async def test_a_settle_that_times_out_leaves_main_to_expire_unreleased() -> None:
    cleanup = ShieldedDeleteCleanup()
    cleanup.competitors["aurora"] = TimedOutSettleAdapter()
    cleanup.lakebase.land.set()
    leases, run = await cancelled_mid_delete(cleanup)
    await asyncio.gather(run, return_exceptions=True)
    assert leases.release_saw_delete_running == []
    held = await leases.current()
    assert held is not None and held.phase == "startup_cleanup"


async def test_an_adapter_without_a_settle_hook_leaves_main_to_expire_unreleased() -> None:
    cleanup = ShieldedDeleteCleanup()
    cleanup.lakebase = UnhookedDeleteAdapter()
    for adapter in cleanup.competitors.values():
        adapter.land.set()
    leases, run = await cancelled_mid_delete(cleanup)
    await asyncio.gather(run, return_exceptions=True)
    assert leases.release_saw_delete_running == []
    held = await leases.current()
    assert held is not None and held.phase == "startup_cleanup"


async def test_a_cleanup_that_finishes_still_releases_main() -> None:
    leases = DurableFakeLeaseStore()
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: Done(),
        recovery_factory=lambda _manifest: Done(),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
        heartbeat_seconds=0.01,
        lease_seconds=1,
    )
    await gate._lead_cleanup()
    assert await leases.current() is None
