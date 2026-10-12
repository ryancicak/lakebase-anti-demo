"""A Round 1 cooldown timeout's local lease pointer ends with its durable row.

`_pin_round_one_cooldown_failure` stops the heartbeat and leaves a finite
`cooldown_failed` row, but the local pointer used to outlive it, so `_releasable`
stayed False and retention never evicted the record.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from itertools import pairwise

from server import manager as manager_module
from server.coordination import InMemoryBoutLeaseStore
from server.manager import RunManager
from server.models import CompetitorId, CooldownState, Corner, SessionCreate, SessionState
from tests.test_coordination import MutableClock
from tests.test_manager import (
    HangingCooldownResolver,
    make_verifier,
    wait_for_cooldown,
    wait_for_state,
)


class RefusingTransitionStore(InMemoryBoutLeaseStore):
    async def transition(self, *args, **kwargs):
        if kwargs.get("phase") == "cooldown_failed":
            raise ConnectionError("coordinator unreachable")
        return await super().transition(*args, **kwargs)


async def timed_out_round_one(lease_store=None, *, running_lease_ttl: float = 0.3):
    manager = RunManager(
        resolver=HangingCooldownResolver(),
        verifier=make_verifier(),
        lease_store=lease_store,
    )
    manager._arm_poll = 0.001
    manager._cooldown_poll_timeout = 0.005
    manager._cooldown_timeout = 0.025
    manager._active_lease_ttl = 0.05
    manager._running_lease_ttl = running_lease_ttl
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await wait_for_state(manager, created.id, SessionState.VERIFIED)
    await manager.start_cooldown(created.id)
    await wait_for_cooldown(manager, created.id, CooldownState.FAILED)
    return manager, created, manager._records[created.id]


async def wait_until(predicate, within: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    while not predicate():
        assert loop.time() < deadline, "condition never held"
        await asyncio.sleep(0.005)


async def test_cooldown_timeout_pointer_ends_with_its_durable_row() -> None:
    manager, _, record = await timed_out_round_one()
    assert record.lease is not None and record.lease.phase == "cooldown_failed"
    assert not RunManager._releasable(record)

    await wait_until(lambda: record.lease is None)
    assert await manager._lease_store.current() is None
    assert RunManager._unfinished_operations(record) == ()
    assert RunManager._releasable(record)
    await manager.close()


async def test_unpersisted_fence_pointer_still_ends_with_the_original_row() -> None:
    manager, _, record = await timed_out_round_one(RefusingTransitionStore())
    assert record.lease is not None and record.lease.phase == "cooldown"
    assert record.lease_heartbeat_task is None

    await wait_until(lambda: record.lease is None)
    assert RunManager._releasable(record)
    await manager.close()


async def test_owner_retry_after_expiry_claims_a_fresh_cooldown() -> None:
    manager, created, record = await timed_out_round_one()
    await wait_until(lambda: record.lease is None)
    await manager.start_cooldown(created.id)
    assert record.lease is not None and record.lease.phase == "cooldown"
    await wait_for_cooldown(manager, created.id, CooldownState.FAILED)
    await wait_until(lambda: record.lease is None)
    await manager.close()


async def test_a_store_clock_behind_this_host_keeps_the_pointer_while_the_row_lives() -> None:
    store_clock = MutableClock(datetime.now(UTC) - timedelta(hours=1))
    manager, _, record = await timed_out_round_one(InMemoryBoutLeaseStore(clock=store_clock))
    fence = record.lease
    assert fence is not None and fence.phase == "cooldown_failed"
    assert fence.expires_at < datetime.now(UTC)

    await asyncio.sleep(0.2)
    assert record.lease is fence
    assert not RunManager._releasable(record)

    store_clock.now = fence.expires_at + timedelta(seconds=1)
    await wait_until(lambda: record.lease is None)
    await manager.close()


class CountingStore(InMemoryBoutLeaseStore):
    def __init__(self, *, hang: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self.read_at: list[float] = []
        self.hang = hang

    async def current(self):
        self.read_at.append(time.monotonic())
        if self.hang:
            await asyncio.Event().wait()
        return await super().current()


async def test_a_fence_still_held_after_local_expiry_is_checked_once_per_ttl() -> None:
    store_clock = MutableClock(datetime.now(UTC) - timedelta(hours=1))
    store = CountingStore(clock=store_clock)
    manager, _, record = await timed_out_round_one(store)
    fence = record.lease
    assert fence is not None and fence.phase == "cooldown_failed"
    await wait_until(lambda: len(store.read_at) >= 4)
    gaps = [later - earlier for earlier, later in pairwise(store.read_at)]
    # One read per 0.05 s lease TTL, not one per 10 ms.
    assert min(gaps[1:]) >= 0.04
    assert record.lease is fence
    await manager.close()


async def test_a_hung_fence_read_is_cut_off_logged_and_asked_again(monkeypatch, caplog) -> None:
    monkeypatch.setattr(manager_module, "FENCE_EXPIRY_READ_SECONDS", 0.05)
    store_clock = MutableClock(datetime.now(UTC) - timedelta(hours=1))
    store = CountingStore(clock=store_clock)
    with caplog.at_level("WARNING", logger="server.manager"):
        manager, _, record = await timed_out_round_one(store)
        store.hang = True
        reads = len(store.read_at)
        await wait_until(lambda: len(store.read_at) >= reads + 2)
    assert any(
        "cooldown fence check could not read the ring" in entry.getMessage()
        and "TimeoutError" in entry.getMessage()
        for entry in caplog.records
    )
    assert record.lease is not None
    store.hang = False
    await manager.close()


async def test_shutdown_leaves_an_unexpired_cooldown_fence_in_the_ring() -> None:
    store_clock = MutableClock(datetime.now(UTC) - timedelta(hours=1))
    store = InMemoryBoutLeaseStore(clock=store_clock)
    manager, _, record = await timed_out_round_one(store)
    fence = record.lease
    assert fence is not None and fence.phase == "cooldown_failed"
    await manager.close()
    held = await store.current()
    assert held is not None and held.phase == "cooldown_failed"
    assert held.lease_id == fence.lease_id


async def test_shutdown_leaves_an_unpersisted_fences_original_row_in_the_ring() -> None:
    store = RefusingTransitionStore(clock=MutableClock(datetime.now(UTC) - timedelta(hours=1)))
    manager, _, record = await timed_out_round_one(store)
    fence = record.lease
    assert fence is not None and fence.phase == "cooldown"
    await manager.close()
    held = await store.current()
    assert held is not None and held.lease_id == fence.lease_id


class HangingTransitionStore(InMemoryBoutLeaseStore):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pinning = asyncio.Event()

    async def transition(self, *args, **kwargs):
        if kwargs.get("phase") == "cooldown_failed":
            self.pinning.set()
            await asyncio.Event().wait()
        return await super().transition(*args, **kwargs)


async def test_shutdown_during_a_hung_pin_leaves_the_original_row_in_the_ring() -> None:
    store = HangingTransitionStore(clock=MutableClock(datetime.now(UTC) - timedelta(hours=1)))
    manager, _, record = await timed_out_round_one(store)
    await asyncio.wait_for(store.pinning.wait(), 1)
    fence = record.lease
    assert fence is not None and fence.phase == "cooldown"
    await manager.close()
    held = await store.current()
    assert held is not None and held.lease_id == fence.lease_id


class LostTransitionAnswerStore(InMemoryBoutLeaseStore):
    async def transition(self, *args, **kwargs):
        moved = await super().transition(*args, **kwargs)
        if kwargs.get("phase") == "cooldown_failed":
            raise ConnectionError("answer lost after commit")
        return moved


async def test_a_lost_transition_answer_still_ends_with_the_committed_row() -> None:
    manager, _, record = await timed_out_round_one(
        LostTransitionAnswerStore(), running_lease_ttl=60.0
    )
    assert record.lease is not None and record.lease.phase == "cooldown"
    await wait_until(lambda: record.lease is None)
    await manager.close()


async def test_retire_never_drops_a_newer_lease() -> None:
    manager, _, record = await timed_out_round_one()
    pinned = record.lease
    assert pinned is not None
    newer = replace(pinned, lease_id="newer-bout", fencing_token=pinned.fencing_token + 1)
    async with record.lease_lock:
        record.lease = newer
    await asyncio.sleep(0.1)
    assert record.lease is newer
    async with record.lease_lock:
        record.lease = None
    await manager.close()


async def test_shutdown_while_the_failure_is_still_being_announced_keeps_the_row(
    monkeypatch,
) -> None:
    announcing = asyncio.Event()
    publish = manager_module.EventLog.publish

    async def hang_on_the_failure(self, kind, payload, *args, **kwargs):
        if kind == "cooldown_update" and payload["cooldown"]["state"] == CooldownState.FAILED:
            announcing.set()
            await asyncio.Event().wait()
        return await publish(self, kind, payload, *args, **kwargs)

    monkeypatch.setattr(manager_module.EventLog, "publish", hang_on_the_failure)
    store = InMemoryBoutLeaseStore()
    manager, _, record = await timed_out_round_one(store, running_lease_ttl=60.0)
    await asyncio.wait_for(announcing.wait(), 1)
    fence = record.lease
    assert fence is not None and fence.phase == "cooldown"
    await manager.close()
    held = await store.current()
    assert held is not None and held.lease_id == fence.lease_id
