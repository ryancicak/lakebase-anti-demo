"""The bell is refused when the Lakebase dispatch could not be accepted after it.

A claimed capsule is not refreshed, so an armed wait can age the Lakebase dispatch
credential into its safety margin, and the bell's own durable commit can age it further.
Accepting such a bell launched the competitor and had the Lakebase dispatch refused a
moment later.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from server import connection_spike_live as live
from server.connection_spike_live import (
    DISPATCH_CAPSULE_SAFETY_SECONDS,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    LiveConnectionSpikeAdapter,
)
from server.coordination import ROUND5_RING_KEY, InMemoryBoutLeaseStore
from server.manager import RunManager
from server.models import SessionState
from server.round5_warm import (
    BELL_DISPATCH_HEADROOM_SECONDS,
    DISPATCH_MARGIN_SECONDS,
    RUNNER_DEADLINE_SECONDS,
    InMemoryRound5WarmStore,
    Round5LaunchCapsule,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmState,
    WarmBellMarginError,
    WarmClaimUnavailableError,
    WarmFenceLostError,
)
from tests.test_round5_pre_deploy_acceptance import (
    _arm_via_http,
    _asgi,
    _BlockingPlan,
    _coordinator,
    _EngineProvider,
    _make_manager,
    _session_body,
    _warm_to_ready,
)
from tests.test_round5_warm import DIGEST, Clock, Provider, coordinator, preparation, warm_ready

INSTALLATION = "install-one"
ACCEPTANCE_INSTALLATION = "install-acceptance"


def _with_lakebase_life(
    capsule: Round5LaunchCapsule, now: datetime, seconds: int
) -> Round5LaunchCapsule:
    return replace(
        capsule,
        dispatch_expires_at={
            "lakebase": now + timedelta(seconds=seconds),
            "competitor": now + timedelta(seconds=900),
        },
        expires_at=now + timedelta(seconds=seconds),
        renew_by=now + timedelta(seconds=seconds - 90),
    )


def lakebase_dispatch_for(seconds: int):
    def factory(clock, **kwargs):
        prep = preparation(clock, **kwargs)
        return replace(prep, capsule=_with_lakebase_life(prep.capsule, clock.now, seconds))

    return factory


async def claimed_after_wait(seconds_left: int, wait: int, store=None):
    clock = Clock()
    fake = Provider(clock)
    fake.preparation_factory = lakebase_dispatch_for(seconds_left)
    manager = coordinator(clock, fake, store)
    await warm_ready(manager, fake)
    claim_at = clock.now
    claimed, capsule = await manager.claim(
        session_id="session-bell-margin",
        bout_id="bout-bell-margin",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    assert claimed.claim is not None
    manager.mark_claim_active(
        claimed.claim.claim_id, armed_deadline=claim_at + timedelta(seconds=240)
    )
    for _ in range(wait // 10):
        clock.advance(10)
        await manager.run_one_cycle()
    assert capsule.meets_launch_margin(claim_at) is True
    return manager, claimed.claim.claim_id, capsule, clock


async def test_a_bell_past_the_dispatch_margin_is_refused_before_running() -> None:
    manager, claim_id, capsule, clock = await claimed_after_wait(790, 120)
    assert capsule.meets_launch_margin(clock.now) is False
    with pytest.raises(WarmClaimUnavailableError, match="safety margin") as refused:
        await manager.accept_bell(claim_id)
    assert isinstance(refused.value, WarmBellMarginError)
    assert refused.value.remaining_seconds == pytest.approx(-50.0)
    assert refused.value.armed_wait_seconds == pytest.approx(120.0)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state != Round5WarmState.RUNNING
    assert slot.bell_at_utc is None


async def test_a_bell_exactly_at_the_dispatch_cutoff_is_refused() -> None:
    # 720 s left is what the live dispatch refuses (`<=`), and dispatch runs after the bell.
    manager, claim_id, capsule, clock = await claimed_after_wait(840, 120)
    assert capsule.meets_launch_margin(clock.now) is True
    with pytest.raises(WarmClaimUnavailableError, match="safety margin"):
        await manager.accept_bell(claim_id)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state != Round5WarmState.RUNNING


async def test_a_bell_leaving_exactly_the_margin_at_its_headroom_is_refused() -> None:
    # 725 s left at the bell is 720 s at +5 s: what the live dispatch refuses (`<=`).
    assert BELL_DISPATCH_HEADROOM_SECONDS == 5.0
    manager, claim_id, _capsule, _clock = await claimed_after_wait(845, 120)
    with pytest.raises(WarmBellMarginError, match="safety margin.*prepare the bout again"):
        await manager.accept_bell(claim_id)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state != Round5WarmState.RUNNING


async def test_a_bell_one_second_past_that_boundary_is_accepted() -> None:
    manager, claim_id, _capsule, _clock = await claimed_after_wait(846, 120)
    await manager.accept_bell(claim_id)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state == Round5WarmState.RUNNING


async def test_a_bell_well_clear_of_the_dispatch_margin_is_accepted() -> None:
    manager, claim_id, capsule, clock = await claimed_after_wait(900, 120)
    assert capsule.meets_launch_margin(clock.now) is True
    context = await manager.accept_bell(claim_id)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state == Round5WarmState.RUNNING
    assert context.claim_id == claim_id


async def test_a_bell_is_refused_once_this_process_no_longer_holds_the_capsule() -> None:
    manager, claim_id, _capsule, _clock = await claimed_after_wait(900, 0)
    manager._capsule = None
    with pytest.raises(WarmFenceLostError, match="launch capsule"):
        await manager.accept_bell(claim_id)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state == Round5WarmState.CLAIMED


async def test_a_bell_is_refused_when_the_held_capsule_is_another_generation() -> None:
    manager, claim_id, capsule, _clock = await claimed_after_wait(900, 0)
    manager._capsule = replace(capsule, generation=capsule.generation + 1)
    with pytest.raises(WarmFenceLostError, match="launch capsule"):
        await manager.accept_bell(claim_id)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.state == Round5WarmState.CLAIMED


async def test_the_dispatch_check_writes_nothing() -> None:
    manager, _claim_id, _capsule, _clock = await claimed_after_wait(790, 120)
    slot = await manager.store.read(INSTALLATION)
    assert slot is not None and slot.claim is not None
    events = await manager.store.events(INSTALLATION)
    with pytest.raises(WarmBellMarginError):
        manager.require_dispatchable(slot.claim)
    assert await manager.store.read(INSTALLATION) == slot
    assert await manager.store.events(INSTALLATION) == events


def test_the_bell_and_the_dispatch_refuse_the_same_credential_life() -> None:
    assert RUNNER_DEADLINE_SECONDS + DISPATCH_MARGIN_SECONDS == DISPATCH_CAPSULE_SAFETY_SECONDS


async def test_the_dispatch_gate_refuses_exactly_where_the_launch_margin_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capsule = lakebase_dispatch_for(800)(Clock(), generation=1, fence=1).capsule
    cliff = capsule.launch_margin_cliff()

    class FrozenDatetime(datetime):
        current = cliff

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(live, "datetime", FrozenDatetime)
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._prepared_clients = SimpleNamespace(expires_at=capsule.dispatch_expires_at["lakebase"])
    adapter._resident_transport = None
    request = {"targets": [{"lane_id": "lakebase"}], "resident_generation": 1}

    FrozenDatetime.current = cliff - timedelta(seconds=1)
    assert capsule.meets_launch_margin(FrozenDatetime.current) is True
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="not configured"):
        await adapter.execute_prepared("a" * 64, request)
    FrozenDatetime.current = cliff
    with pytest.raises(ConnectionSpikeLiveOperationError, match="inside its safety margin"):
        await adapter.execute_prepared("a" * 64, request)


async def test_a_margin_refused_bell_tells_the_presenter_to_prepare_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    provider = _EngineProvider(clock, _BlockingPlan)
    warm = _coordinator(clock, provider)
    await _warm_to_ready(warm, provider)
    manager = _make_manager(warm, clock)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=_asgi(manager)), base_url="http://anti-demo.test"
        ) as client:
            created = await client.post("/api/sessions", json=_session_body())
            session_id = created.json()["id"]
            await _arm_via_http(client, session_id)

            async def margin_refused(*_args: object, **_kwargs: object) -> object:
                raise WarmBellMarginError(remaining_seconds=-50.0, armed_wait_seconds=120.0)

            monkeypatch.setattr(warm, "accept_bell_with_leases", margin_refused)
            monkeypatch.setattr(warm, "accept_bell", margin_refused)
            run = await client.post(f"/api/sessions/{session_id}/run")
            assert run.status_code == 409, run.text
            assert "prepare the bout again" in run.json()["detail"]
            snapshot = (await client.get(f"/api/sessions/{session_id}")).json()
            assert snapshot["state"] != SessionState.RUNNING.value
    finally:
        await manager.close()
        await warm.close()


class AtomicBellStore(InMemoryRound5WarmStore):
    """Stands in for the Lakebase store's four-row bell transaction."""

    def __init__(self) -> None:
        super().__init__()
        self.atomic_calls = 0

    async def accept_bell_with_leases(self, *args, **kwargs):
        self.atomic_calls += 1
        raise AssertionError("the bell transaction must not run past the margin")


async def test_the_atomic_bell_path_refuses_before_its_transaction() -> None:
    store = AtomicBellStore()
    manager, claim_id, _capsule, _clock = await claimed_after_wait(790, 120, store)
    with pytest.raises(WarmClaimUnavailableError, match="safety margin"):
        await manager.accept_bell_with_leases(
            claim_id,
            main_store=SimpleNamespace(ring_key="main"),
            main_lease=object(),
            cleanup_store=SimpleNamespace(ring_key="cleanup"),
            cleanup_lease=object(),
            ttl=timedelta(seconds=90),
        )
    assert store.atomic_calls == 0


# ---------------------------------------------------------------------------
# Through the real manager and HTTP route
# ---------------------------------------------------------------------------


class _ShortLakebaseProvider(_EngineProvider):
    """Warms capsules whose Lakebase dispatch credential lives ``lakebase_seconds``."""

    def __init__(self, clock: Clock, lakebase_seconds: int) -> None:
        super().__init__(clock, _BlockingPlan)
        self._lakebase_seconds = lakebase_seconds

    async def prepare(self, **kwargs):
        prep = await super().prepare(**kwargs)
        return replace(
            prep,
            capsule=_with_lakebase_life(prep.capsule, self.clock.now, self._lakebase_seconds),
        )


class _SlowBellStore(InMemoryRound5WarmStore):
    """The bell's durable commit answers ``delay`` seconds after it was sent."""

    def __init__(self, clock: Clock, delay: float) -> None:
        super().__init__()
        self._clock = clock
        self._delay = delay

    async def accept_bell(self, slot, **kwargs):
        running = await super().accept_bell(slot, **kwargs)
        self._clock.advance(self._delay)
        return running


class _AtomicSlowBellStore(_SlowBellStore):
    """The durable store's four-row bell transaction, answered ``delay`` seconds late."""

    def __init__(
        self, clock: Clock, delay: float, lease_stores: tuple[InMemoryBoutLeaseStore, ...]
    ) -> None:
        super().__init__(clock, delay)
        self._lease_stores = {store.ring_key: store for store in lease_stores}
        self.atomic_calls = 0

    async def accept_bell_with_leases(
        self,
        slot,
        *,
        claim_id,
        bell_id,
        bell_at_utc,
        main_ring_key,
        main_lease,
        cleanup_ring_key,
        cleanup_lease,
        ttl,
        release_event=None,
    ):
        del release_event
        self.atomic_calls += 1
        main, cleanup = [
            await self._lease_stores[ring_key].transition(
                lease,
                operator=lease.operator,
                expected_phase="armed",
                phase="run_committed",
                session_state=SessionState.RUNNING,
                ttl=ttl,
            )
            for ring_key, lease in ((main_ring_key, main_lease), (cleanup_ring_key, cleanup_lease))
        ]
        running = await self.accept_bell(
            slot, claim_id=claim_id, bell_id=bell_id, bell_at_utc=bell_at_utc
        )
        return running, main.updated_at, main.expires_at, cleanup.updated_at, cleanup.expires_at


@asynccontextmanager
async def _armed_bout(
    *,
    lakebase_seconds: int,
    wait: int,
    bell_delay: float = 0.0,
    atomic: bool = False,
) -> AsyncIterator[SimpleNamespace]:
    clock = Clock()
    provider = _ShortLakebaseProvider(clock, lakebase_seconds)
    main = InMemoryBoutLeaseStore()
    round5 = InMemoryBoutLeaseStore(ring_key=ROUND5_RING_KEY)
    store = (
        _AtomicSlowBellStore(clock, bell_delay, (main, round5))
        if atomic
        else _SlowBellStore(clock, bell_delay)
    )
    warm = Round5WarmCoordinator(
        installation_id=ACCEPTANCE_INSTALLATION,
        warm_contract_sha256=DIGEST,
        store=store,
        provider=provider,
        process_epoch="process-acceptance",
        broker_epoch="broker-acceptance",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await _warm_to_ready(warm, provider)
    manager = RunManager(
        connection_spike_factory=lambda _competitor: object(),
        round5_warm_coordinator=warm,
        installation_id=ACCEPTANCE_INSTALLATION,
        clock_ns=lambda: clock.monotonic_ns + 50_000_000,
        lease_store=main,
        round5_lease_store=round5,
    )
    try:
        async with AsyncClient(
            transport=ASGITransport(app=_asgi(manager)), base_url="http://anti-demo.test"
        ) as client:
            session_id = (await client.post("/api/sessions", json=_session_body())).json()["id"]
            await _arm_via_http(client, session_id)
            for _ in range(wait // 10):
                clock.advance(10)
                await warm.run_one_cycle()
            yield SimpleNamespace(
                client=client,
                session_id=session_id,
                warm=warm,
                store=store,
                manager=manager,
                clock=clock,
                engine=provider.engines[-1],
            )
    finally:
        await manager.close()
        await warm.close()


def _margin_refusals(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.getMessage().startswith("round5_bell_refused_dispatch_margin")
    ]


async def test_a_bell_refused_before_its_precommit_leaves_no_create_intent() -> None:
    async with _armed_bout(lakebase_seconds=790, wait=120) as bout:
        run = await bout.client.post(f"/api/sessions/{bout.session_id}/run")
        assert run.status_code == 409, run.text
        assert "prepare the bout again" in run.json()["detail"]
        assert bout.engine.launch_intent_calls == 0
        assert bout.engine.bound_bell is None
        slot = await bout.warm.store.read(ACCEPTANCE_INSTALLATION)
        assert slot is not None and slot.state == Round5WarmState.CLEANING
        assert slot.bell_id is None and slot.bell_at_utc is None
        events = await bout.warm.store.events(ACCEPTANCE_INSTALLATION)
        assert not any(event.event_type == "bell_accepted" for event in events)


@pytest.mark.parametrize("atomic", [False, True], ids=["bell", "four-row-bell"])
async def test_a_bell_whose_commit_outlasts_the_margin_launches_nothing(
    atomic: bool, caplog: pytest.LogCaptureFixture
) -> None:
    # 31 s of launch life at the bell clears every check made before the commit. The commit
    # answers 35 s later, when the Lakebase dispatch would refuse the credential.
    caplog.set_level(logging.WARNING)
    async with _armed_bout(
        lakebase_seconds=871, wait=120, bell_delay=35, atomic=atomic
    ) as bout:
        run = await bout.client.post(f"/api/sessions/{bout.session_id}/run")
        assert run.status_code == 409, run.text
        assert "prepare the bout again" in run.json()["detail"]
        assert getattr(bout.store, "atomic_calls", 1) == 1
        snapshot = (await bout.client.get(f"/api/sessions/{bout.session_id}")).json()
        assert snapshot["state"] == SessionState.FAILED.value
        engine = bout.engine
        assert engine.launch_intent_calls == 1
        assert engine.bound_bell is None
        assert engine.setup_calls == 0
        if atomic:
            # The other bell path commits the ring lease, and starts its heartbeat,
            # before the Round 5 bell runs at all.
            assert bout.manager._records[bout.session_id].lease_heartbeat_task is None
        (refusal,) = _margin_refusals(caplog)
        assert "stage=after_commit" in refusal.getMessage()

        # Committed, so cleanup runs the post-bell teardown before the next generation.
        slot = await bout.warm.store.read(ACCEPTANCE_INSTALLATION)
        assert slot is not None and slot.state == Round5WarmState.CLEANING
        assert slot.bell_id is not None
        generation = slot.generation
        for _ in range(10):
            bout.clock.advance(61)
            await bout.warm.run_one_cycle()
            slot = await bout.warm.store.read(ACCEPTANCE_INSTALLATION)
            assert slot is not None
            if slot.state == Round5WarmState.READY:
                break
        assert slot.state == Round5WarmState.READY
        assert slot.generation > generation
        assert engine.reconcile_claim_calls == 1


async def test_a_margin_refusal_logs_one_warning_and_no_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    async with _armed_bout(lakebase_seconds=790, wait=120) as bout:
        caplog.clear()
        run = await bout.client.post(f"/api/sessions/{bout.session_id}/run")
        assert run.status_code == 409, run.text
        records = list(caplog.records)
        refusals = _margin_refusals(caplog)
    assert len(refusals) == 1
    (refusal,) = refusals
    assert refusal.levelno == logging.WARNING
    assert refusal.exc_info is None
    message = refusal.getMessage()
    assert "stage=before_precommit" in message
    assert "remaining_s=-50.0" in message
    assert "armed_wait_s=120.0" in message
    assert [record.getMessage() for record in records if record.levelno >= logging.ERROR] == []
    assert not [record for record in records if "runner_handoff_incomplete" in record.getMessage()]
