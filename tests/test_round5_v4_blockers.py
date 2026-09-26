"""Deterministic race/failure tests for the Round 5 V4 warm blockers.

Each test targets one verified blocker:

1. the 90-second coordinator lease is heartbeaten through an hour-long
   ``provider.reconcile``/``provider.prepare``;
2. a transient store exception does not kill the supervised warm loop;
3. a warm claim held by an active operator is renewed through ARM instead of
   being released, while an abandoned claim still expires;
4. a failed pre-bell resident stage records its ownership before staging so a
   cancellation can still find it; and
5. an ambiguous ``accept_bell_with_leases`` commit reads the committed rows back
   and rejoins exactly rather than reporting a false rollback.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server.connection_spike_live import LiveConnectionSpikeEngine
from server.round5_control import Round5ControlBinding, Round5ControlEvent, Round5ControlKind
from server.round5_warm import (
    DEFAULT_CLAIM_TTL_SECONDS,
    InMemoryRound5WarmStore,
    LakebaseRound5WarmStore,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmState,
    WarmCoordinatorHeldError,
    WarmFenceLostError,
)
from tests.test_round5_warm import Clock, Provider, coordinator, warm_ready

# --------------------------------------------------------------------------- #
# Blocker 1: heartbeat the coordinator lease through a long preparation.
# --------------------------------------------------------------------------- #


async def test_coordinator_lease_is_heartbeaten_through_a_long_preparation() -> None:
    clock = Clock()
    provider = Provider(clock)
    # A short TTL makes the heartbeat interval (ttl/3) fire on real time inside
    # the test; the fake clock is what advances past the *original* lease.
    manager = Round5WarmCoordinator(
        installation_id="install-one",
        warm_contract_sha256="a" * 64,
        store=InMemoryRound5WarmStore(),
        provider=provider,
        process_epoch="process-one",
        broker_epoch="broker-one",
        clock=clock,
        monotonic_ns=clock.monotonic,
        coordinator_ttl_seconds=0.09,
    )
    await manager.store.initialize()

    cycle = asyncio.create_task(manager.run_one_cycle())
    await asyncio.wait_for(provider.prepare_started.wait(), timeout=1)

    during_prepare = await manager.store.read("install-one")
    assert during_prepare is not None
    revision_at_attempt = during_prepare.revision
    lease_at_attempt = during_prepare.coordinator_lease_expires_at
    assert lease_at_attempt is not None

    # The provider work takes far longer than the 0.09s lease; advance the fake
    # clock well past it so that, without heartbeating, the lease would be dead.
    clock.advance(10)
    await asyncio.sleep(0.12)  # several ttl/3 real-time heartbeat intervals

    beaten = await manager.store.read("install-one")
    assert beaten is not None
    assert beaten.revision > revision_at_attempt  # the beat wrote new revisions
    assert beaten.coordinator_lease_expires_at is not None
    assert beaten.coordinator_lease_expires_at > lease_at_attempt

    # A different process must not be able to seize coordination: the lease is
    # still live because it was renewed against the advanced clock.
    with pytest.raises(WarmCoordinatorHeldError):
        await manager.store.acquire_coordinator(
            installation_id="install-one",
            process_epoch="process-two",
            broker_epoch="broker-two",
            now=clock.now,
            ttl=timedelta(seconds=0.09),
        )

    provider.release_prepare.set()
    await asyncio.wait_for(cycle, timeout=1)
    ready = await manager.store.read("install-one")
    assert ready is not None and ready.state == Round5WarmState.READY
    await manager.close()


# --------------------------------------------------------------------------- #
# Blocker 2: a transient store exception must not kill the warm loop.
# --------------------------------------------------------------------------- #


class _FlakyStore(InMemoryRound5WarmStore):
    """Raise a transient error on the first acquire, then behave normally."""

    def __init__(self, fail_times: int = 1) -> None:
        super().__init__()
        self._remaining_failures = fail_times

    async def acquire_coordinator(self, **kwargs: object):  # type: ignore[override]
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise RuntimeError("transient store connection dropped")
        return await super().acquire_coordinator(**kwargs)


async def test_transient_store_exception_does_not_kill_the_warm_loop() -> None:
    clock = Clock()
    provider = Provider(clock)
    store = _FlakyStore(fail_times=1)
    manager = Round5WarmCoordinator(
        installation_id="install-one",
        warm_contract_sha256="a" * 64,
        store=store,
        provider=provider,
        process_epoch="process-one",
        broker_epoch="broker-one",
        clock=clock,
        monotonic_ns=clock.monotonic,
        sleep=lambda _delay: asyncio.sleep(0),
    )

    task = await manager.start()
    # If the transient error had killed the loop, prepare would never start.
    await asyncio.wait_for(provider.prepare_started.wait(), timeout=1)
    assert not task.done()
    assert store._remaining_failures == 0

    provider.release_prepare.set()
    for _ in range(200):
        slot = await manager.store.read("install-one")
        if slot is not None and slot.state == Round5WarmState.READY:
            break
        await asyncio.sleep(0)
    assert slot is not None and slot.state == Round5WarmState.READY
    await manager.close()


# --------------------------------------------------------------------------- #
# Blocker 3: renew an active claim through ARM; still expire an abandoned one.
# --------------------------------------------------------------------------- #


async def test_active_claim_is_renewed_through_arm() -> None:
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)

    claimed, _capsule = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    assert claimed.state == Round5WarmState.CLAIMED
    original_expiry = claimed.claim.claim_expires_at

    # ARM outlives the claim TTL; the loop must renew rather than release.
    clock.advance(DEFAULT_CLAIM_TTL_SECONDS + 5)
    await manager.run_one_cycle()

    renewed = await manager.store.read("install-one")
    assert renewed is not None
    assert renewed.state == Round5WarmState.CLAIMED
    assert renewed.claim is not None
    assert renewed.claim.claim_id == claimed.claim.claim_id
    assert renewed.claim.claim_expires_at > original_expiry
    await manager.close()


async def test_abandoned_expired_claim_fences_into_cleaning_with_claim_retained() -> None:
    # The generic coordinator loop must fail SAFE: an expired, no-longer-active
    # claim may have staged ARM residents whose exact job IDs live only in the
    # durable claim, so it is fenced into CLEANING with the claim RETAINED rather
    # than normalized back to READY/WARMING (which would abandon those residents
    # and race the manager's begin_cleanup -- the live no-bell wedge). The fast
    # pre-bell return-to-READY path still exists via coordinator.abandon_claim /
    # store.release_claim; it is only this generic supervised-loop release that now
    # fences. (Was: released to READY/WARMING with the claim dropped.)
    clock = Clock()
    provider = Provider(clock)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)

    claimed, _capsule = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    # The operator goes away: stop backstage renewal.
    manager.release_claim_active(claimed.claim.claim_id)

    clock.advance(DEFAULT_CLAIM_TTL_SECONDS + 5)
    await manager.run_one_cycle()

    fenced = await manager.store.read("install-one")
    assert fenced is not None
    assert fenced.state == Round5WarmState.CLEANING
    assert fenced.claim is not None
    assert fenced.claim.claim_id == claimed.claim.claim_id
    await manager.close()


# --------------------------------------------------------------------------- #
# Blocker 4: record resident stage ownership before staging.
# --------------------------------------------------------------------------- #


async def test_failed_lakebase_stage_records_binding_before_staging() -> None:
    engine = object.__new__(LiveConnectionSpikeEngine)

    class _Orchestrator:
        async def prepare(self, bout_id: str, fencing_token: int) -> None:
            del bout_id, fencing_token

    class _Transport:
        def __init__(self) -> None:
            self.stage_calls = 0

        async def stage(self, *, binding: object, request: object) -> None:
            del binding, request
            self.stage_calls += 1
            raise RuntimeError("resident stage wait_prepared failed")

    transport = _Transport()
    sentinel_binding = object()

    engine._setup_orchestrator = _Orchestrator()
    engine._bound_claim = SimpleNamespace(claim_id="claim-one")
    engine._armed = object()
    engine._adapter = SimpleNamespace(
        config=SimpleNamespace(targets=[SimpleNamespace(lane_id="lakebase")])
    )
    engine._job_ids = {"lakebase": "a" * 64}
    engine._lane_adapters = {
        "lakebase": SimpleNamespace(_resident_transport=transport)
    }
    engine._resident_bindings = {}
    engine._fanin_request = lambda *args, **kwargs: {"job_id": "a" * 64}
    engine._resident_binding = lambda lane_id, request: sentinel_binding

    with pytest.raises(RuntimeError, match="resident stage"):
        await engine.prepare("bout-one", 7)

    # The stage was attempted, and ownership was recorded *before* it failed so a
    # cancellation can still find and settle the resident.
    assert transport.stage_calls == 1
    assert engine._resident_bindings.get("lakebase") is sentinel_binding


# --------------------------------------------------------------------------- #
# Blocker 5: exact read-back / rejoin on an ambiguous bell commit.
# --------------------------------------------------------------------------- #


class _AmbiguousCursor:
    """Return no rows for the commit CTE, then a chosen read-back row."""

    def __init__(self, read_back_row: tuple | None) -> None:
        self._read_back_row = read_back_row
        self._calls = 0

    async def execute(self, sql: str, params: object = None) -> None:
        del sql, params
        self._calls += 1

    async def fetchone(self):
        # First execute is the commit CTE (ambiguous -> no row); second is the
        # read-back SELECT.
        return None if self._calls <= 1 else self._read_back_row


async def _claimed_slot_and_release(manager: Round5WarmCoordinator):
    claimed, _capsule = await manager.claim(
        session_id="session-one",
        bout_id="bout-one",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    claim = claimed.claim
    assert claim is not None
    bell_at = datetime.now(UTC)
    release = Round5ControlEvent.create(
        binding=Round5ControlBinding(
            installation_id=claimed.installation_id,
            lane_id="lakebase",
            generation=claimed.generation,
            warm_attempt_token=claim.warm_attempt_token,
            claim_id=claim.claim_id,
            bout_id=claim.bout_id,
            bell_id=claim.bell_id,
            fence=claim.bout_fence,
            job_id=claim.lakebase_job_id,
            runner_boot_id="runner-boot-one",
            runner_process_boot_id="process-current",
            runner_harness_sha256="b" * 64,
            request_sha256="c" * 64,
        ),
        sequence=2,
        kind=Round5ControlKind.RELEASE,
        created_at=bell_at,
        payload={"bell_id": claim.bell_id},
    )
    return claimed, claim, bell_at, release


def _matching_read_back_row(
    claimed,
    claim,
    main_lease,
    cleanup_lease,
) -> tuple:
    now = datetime.now(UTC)
    main_updated, main_expires = now, now + timedelta(seconds=30)
    cleanup_updated, cleanup_expires = now, now + timedelta(seconds=30)
    return (
        "run_committed",
        main_updated,
        main_expires,
        str(main_lease.lease_id),
        main_lease.fencing_token,
        main_lease.session_id,
        main_lease.owner_subject,
        "run_committed",
        cleanup_updated,
        cleanup_expires,
        str(cleanup_lease.lease_id),
        cleanup_lease.fencing_token,
        cleanup_lease.session_id,
        cleanup_lease.owner_subject,
        Round5WarmState.RUNNING.value,
        claimed.generation,
        claimed.revision + 1,
        claimed.coordinator_fence,
        claim.bell_id,
        claim.claim_id,
        True,
    )


async def test_ambiguous_bell_commit_reads_back_and_rejoins() -> None:
    clock = Clock()
    provider = Provider(clock)
    inmemory = coordinator(clock, provider)
    await warm_ready(inmemory, provider)
    claimed, claim, bell_at, release = await _claimed_slot_and_release(inmemory)

    main_lease = SimpleNamespace(
        lease_id="11111111-1111-1111-1111-111111111111",
        fencing_token=8,
        session_id="session-one",
        owner_subject="owner-subject-one",
    )
    cleanup_lease = SimpleNamespace(
        lease_id="22222222-2222-2222-2222-222222222222",
        fencing_token=9,
        session_id="session-one",
        owner_subject="owner-subject-one",
    )
    row = _matching_read_back_row(claimed, claim, main_lease, cleanup_lease)
    cursor = _AmbiguousCursor(row)

    async def run(fn):
        return await fn(cursor)

    store = LakebaseRound5WarmStore(run=run)
    running, main_updated, main_expires, cleanup_updated, cleanup_expires = (
        await store.accept_bell_with_leases(
            claimed,
            claim_id=claim.claim_id,
            bell_id=claim.bell_id,
            bell_at_utc=bell_at,
            main_ring_key="ring-main",
            main_lease=main_lease,
            cleanup_ring_key="ring-cleanup",
            cleanup_lease=cleanup_lease,
            ttl=timedelta(seconds=30),
            release_event=release,
        )
    )
    assert running.state == Round5WarmState.RUNNING
    assert running.bell_id == claim.bell_id
    assert (main_updated, main_expires) == (row[1], row[2])
    assert (cleanup_updated, cleanup_expires) == (row[8], row[9])


async def test_ambiguous_bell_commit_rejects_a_mismatched_read_back() -> None:
    clock = Clock()
    provider = Provider(clock)
    inmemory = coordinator(clock, provider)
    await warm_ready(inmemory, provider)
    claimed, claim, bell_at, release = await _claimed_slot_and_release(inmemory)

    main_lease = SimpleNamespace(
        lease_id="11111111-1111-1111-1111-111111111111",
        fencing_token=8,
        session_id="session-one",
        owner_subject="owner-subject-one",
    )
    cleanup_lease = SimpleNamespace(
        lease_id="22222222-2222-2222-2222-222222222222",
        fencing_token=9,
        session_id="session-one",
        owner_subject="owner-subject-one",
    )
    row = list(_matching_read_back_row(claimed, claim, main_lease, cleanup_lease))
    row[18] = "bell-someone-else"  # committed bell_id does not match this call
    cursor = _AmbiguousCursor(tuple(row))

    async def run(fn):
        return await fn(cursor)

    store = LakebaseRound5WarmStore(run=run)
    with pytest.raises(WarmFenceLostError):
        await store.accept_bell_with_leases(
            claimed,
            claim_id=claim.claim_id,
            bell_id=claim.bell_id,
            bell_at_utc=bell_at,
            main_ring_key="ring-main",
            main_lease=main_lease,
            cleanup_ring_key="ring-cleanup",
            cleanup_lease=cleanup_lease,
            ttl=timedelta(seconds=30),
            release_event=release,
        )
