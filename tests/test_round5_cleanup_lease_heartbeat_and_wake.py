"""Production-path proofs for two review gaps in ``Round5WarmCoordinator``:

1. ``_converge_cleanup_once`` must run the (possibly long) provider reconcile
   UNDER the lease-heartbeat wrapper (``_run_holding_lease``) for the reconcile's
   WHOLE duration, so a coordinator-lease takeover mid-reconcile cancels the
   stale operation promptly instead of letting it run to completion under a
   fence this process no longer holds, and so that a subsequent mutation by the
   now-stale owner is refused.

2. The durable-store-driving coordinator methods ``begin_cleanup`` and
   ``finish_cleanup_and_rewarm`` must each wake the supervised loop
   (``self._wake.set()``) so a manager-initiated cleanup transition is picked
   up promptly rather than waiting out whatever sleep the loop last scheduled
   (the "latency guarantee").

Both proofs exercise the REAL ``Round5WarmCoordinator`` methods end to end
(never an extracted/copied fragment), against the real ``InMemoryRound5WarmStore``
CAS semantics that mirror the durable store's fencing contract.
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import timedelta

import pytest

from server.round5_warm import (
    InMemoryRound5WarmStore,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmState,
    WarmFenceLostError,
)
from tests.test_round5_warm import Clock, Provider, coordinator

# ---------------------------------------------------------------------------
# Shared fixture: a CLEANING slot with a claim, owned by ``process-one``.
# ---------------------------------------------------------------------------


async def _cleaning_coordinator(
    clock: Clock,
    store: InMemoryRound5WarmStore,
    provider: Provider,
    *,
    coordinator_ttl_seconds: float = 90.0,
) -> tuple[Round5WarmCoordinator, str]:
    manager = Round5WarmCoordinator(
        installation_id="install-one",
        warm_contract_sha256="a" * 64,
        store=store,
        provider=provider,
        process_epoch="process-one",
        broker_epoch="broker-one",
        clock=clock,
        monotonic_ns=clock.monotonic,
        coordinator_ttl_seconds=coordinator_ttl_seconds,
    )
    # ``run_one_cycle``'s WARMING branch calls ``provider.reconcile`` (settling
    # any leaked prior residents) BEFORE ``provider.prepare`` -- let that
    # warm-up-time reconcile through immediately; only the CLEANUP-time
    # reconcile below is meant to pause.
    release_reconcile = getattr(provider, "release_reconcile", None)
    if release_reconcile is not None:
        release_reconcile.set()
    task = asyncio.create_task(manager.run_one_cycle())
    await asyncio.wait_for(provider.prepare_started.wait(), timeout=1)
    provider.release_prepare.set()
    await asyncio.wait_for(task, timeout=1)
    if release_reconcile is not None:
        release_reconcile.clear()
        provider.reconcile_started.clear()
        provider.reconcile_calls = 0
        provider.reconcile_completed_normally = False
    claimed, _capsule = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id
    await manager.accept_bell(claim_id)
    await manager.begin_cleanup(claim_id)
    return manager, claim_id


# ---------------------------------------------------------------------------
# 1. ``_run_holding_lease`` wraps the cleanup reconcile for its whole duration.
# ---------------------------------------------------------------------------


class _PausableReconcileProvider(Provider):
    """A provider whose ``reconcile`` blocks until the test releases it, so the
    test can observe -- and later cancel -- an in-flight reconcile."""

    def __init__(self, clock: Clock) -> None:
        super().__init__(clock)
        self.reconcile_started = asyncio.Event()
        self.release_reconcile = asyncio.Event()
        self.reconcile_completed_normally = False
        self.reconcile_cancelled = False

    async def reconcile(self, slot) -> bool:
        self.reconcile_calls += 1
        self.reconcile_started.set()
        try:
            await self.release_reconcile.wait()
        except asyncio.CancelledError:
            self.reconcile_cancelled = True
            raise
        self.reconcile_completed_normally = True
        return self.reconcile_result


class _HeartbeatCountingStore(InMemoryRound5WarmStore):
    def __init__(self) -> None:
        super().__init__()
        self.heartbeat_calls = 0

    async def heartbeat_coordinator(self, slot, **kwargs):
        self.heartbeat_calls += 1
        return await super().heartbeat_coordinator(slot, **kwargs)


async def test_cleanup_reconcile_stays_inside_the_lease_heartbeat_wrapper() -> None:
    """The wrapper must (a) actually heartbeat DURING the still-pending reconcile,
    (b) cancel that reconcile PROMPTLY -- not let it run to completion -- once a
    competing coordinator wins the lease mid-flight, and (c) refuse the stale
    owner's next mutation afterward.

    This fails outright if ``_converge_cleanup_once`` stops calling
    ``self._run_holding_lease(holder, do_reconcile)`` at its call site: without
    the wrapper there is no heartbeat (assertion (a) fails) and no cancellation
    mechanism at all, so the paused reconcile never resolves and the
    ``asyncio.wait_for`` below times out instead of observing
    ``WarmFenceLostError`` (assertion (b) fails through a different, but still
    red, exception type).
    """

    # A short REAL-wallclock TTL keeps the wrapper's background heartbeat
    # cadence (``max(0.02, ttl/3)`` seconds of REAL asyncio time) fast enough to
    # observe multiple heartbeats within a fraction of a second, without any
    # sleep-based flakiness in the pass/fail assertions themselves.
    clock = Clock()
    store = _HeartbeatCountingStore()
    provider = _PausableReconcileProvider(clock)
    provider.reconcile_result = True
    manager, claim_id = await _cleaning_coordinator(
        clock, store, provider, coordinator_ttl_seconds=0.09
    )

    task = asyncio.create_task(manager.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)
    assert not task.done()

    # Let the real background beat task fire a few times while the reconcile is
    # still paused -- this is only OBSERVABLE if the reconcile is running INSIDE
    # the wrapper for its whole (still ongoing) duration.
    await asyncio.sleep(0.25)
    assert store.heartbeat_calls >= 1, (
        "no heartbeat occurred while the cleanup reconcile was still pending; "
        "the reconcile is not running inside the lease-heartbeat wrapper"
    )
    assert not task.done()
    assert provider.reconcile_calls == 1

    # A second replica's coordinator wins the coordinator lease. This directly
    # invokes the same durable, fenced CAS the supervised loop's own
    # ``acquire_coordinator`` cycle uses -- simulating a real takeover after this
    # process's lease has gone stale.
    current = await store.read(manager.installation_id)
    assert current is not None and current.coordinator_lease_expires_at is not None
    takeover = await store.acquire_coordinator(
        installation_id=manager.installation_id,
        process_epoch="process-two",
        broker_epoch="broker-two",
        now=current.coordinator_lease_expires_at + timedelta(seconds=1),
        ttl=timedelta(seconds=90),
    )
    assert takeover.coordinator_owner == "process-two"
    assert takeover.coordinator_fence != current.coordinator_fence

    # The manager's own next heartbeat attempt (still real-wallclock driven,
    # ~0.03s out) must now observe the stolen fence and CANCEL the in-flight
    # reconcile -- promptly, not "eventually at the final commit".
    with pytest.raises(WarmFenceLostError, match="fence lost"):
        await asyncio.wait_for(task, timeout=1)

    assert provider.reconcile_cancelled is True
    assert provider.reconcile_completed_normally is False
    assert provider.reconcile_calls == 1  # no retry snuck through under the stale fence

    # No partial mutation: still CLEANING at generation 1, now owned by the
    # winner -- the loser never reached ``finish_cleanup_and_rewarm``.
    final = await store.read(manager.installation_id)
    assert final is not None
    assert final.state == Round5WarmState.CLEANING
    assert final.generation == 1
    assert final.coordinator_owner == "process-two"

    # Takeover/fence loss prevents the NEXT mutation by the stale owner.
    with pytest.raises(WarmFenceLostError, match="ownership is not current"):
        await manager.finish_cleanup_and_rewarm(claim_id)

    # Belt-and-suspenders structural check: lock the exact call site so a
    # future refactor that silently drops the wrapper (while somehow still
    # passing the behavioral assertions above under a different timing) is
    # still caught immediately.
    source = inspect.getsource(Round5WarmCoordinator._converge_cleanup_once)
    assert "await self._run_holding_lease(holder, do_reconcile)" in source


async def test_the_supervised_loop_joining_a_held_cleanup_does_not_cancel_it() -> None:
    """Only another owner or fence may cancel a held cleanup; this process may not.

    Live on 2026-09-26 (05:02-05:04Z) a 75 s Round 5 towel lost three 30 s
    cleanup attempts in a row to ``coordinator fence lost during held provider
    operation`` with no second process anywhere. The manager's ``begin_cleanup``
    woke the supervised loop, whose ``acquire_coordinator`` advanced the slot
    revision after the manager's cleanup task had read it, and the task's next
    heartbeat refused that revision and cancelled the Proxy reconcile. This
    replays the same order with the real loop cycle.
    """

    clock = Clock()
    store = _HeartbeatCountingStore()
    provider = _PausableReconcileProvider(clock)
    provider.reconcile_result = True
    manager, claim_id = await _cleaning_coordinator(
        clock, store, provider, coordinator_ttl_seconds=0.09
    )

    cleanup = asyncio.create_task(manager.converge_cleanup(claim_id))
    await asyncio.wait_for(provider.reconcile_started.wait(), timeout=1)
    held = await store.read(manager.installation_id)
    assert held is not None

    loop_cycle = asyncio.create_task(manager.run_one_cycle())
    await asyncio.sleep(0.25)

    moved = await store.read(manager.installation_id)
    assert moved is not None and moved.revision > held.revision
    assert store.heartbeat_calls >= 2
    assert not cleanup.done(), "this process's own loop cancelled the held cleanup"
    assert provider.reconcile_cancelled is False

    provider.release_reconcile.set()
    warmed = await asyncio.wait_for(cleanup, timeout=1)
    await asyncio.wait_for(loop_cycle, timeout=1)

    assert warmed.state == Round5WarmState.WARMING
    assert warmed.generation == 2
    assert provider.reconcile_calls == 1
    assert provider.reconcile_completed_normally is True


# ---------------------------------------------------------------------------
# 2. ``begin_cleanup``/``finish_cleanup_and_rewarm`` wake the supervised loop.
# ---------------------------------------------------------------------------


async def test_finish_cleanup_and_rewarm_wakes_the_supervised_loop() -> None:
    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider = Provider(clock)
    provider.reconcile_result = True
    manager, claim_id = await _cleaning_coordinator(clock, store, provider)
    manager._verified_clean_claim_ids.add(claim_id)

    manager._wake.clear()
    assert not manager._wake.is_set()

    await manager.finish_cleanup_and_rewarm(claim_id)

    assert manager._wake.is_set(), (
        "finish_cleanup_and_rewarm must wake the supervised loop so the next "
        "cycle runs promptly instead of waiting out its last scheduled sleep"
    )


async def test_begin_cleanup_wakes_the_supervised_loop() -> None:
    """A manager-initiated ``begin_cleanup`` (the no-bell abandon path, the
    towel-cleanup path, and the crashed-bell-recovery path in ``manager.py`` all
    call this directly, off the supervised loop) must wake the loop so its
    generic ``CLEANING`` branch converges the cleanup promptly. Without the
    wake, the loop only notices on its NEXT already-scheduled sleep, which can
    be far later than the fast in-place cadence this method's sibling,
    ``finish_cleanup_and_rewarm``, already guarantees.
    """

    clock = Clock()
    store = InMemoryRound5WarmStore()
    provider = Provider(clock)
    manager = coordinator(clock, provider, store)
    task = asyncio.create_task(manager.run_one_cycle())
    await asyncio.wait_for(provider.prepare_started.wait(), timeout=1)
    provider.release_prepare.set()
    await asyncio.wait_for(task, timeout=1)
    claimed, _capsule = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=1,
    )
    assert claimed.claim is not None
    claim_id = claimed.claim.claim_id

    manager._wake.clear()
    assert not manager._wake.is_set()

    await manager.begin_cleanup(claim_id)

    assert manager._wake.is_set(), (
        "begin_cleanup must wake the supervised loop the same way "
        "finish_cleanup_and_rewarm does, or a manager-initiated cleanup "
        "transition waits out the loop's last scheduled sleep before it is "
        "picked up -- a latency regression against the pre-refactor behavior "
        "that woke on every CLAIMED->CLEANING/WARMING release path"
    )
