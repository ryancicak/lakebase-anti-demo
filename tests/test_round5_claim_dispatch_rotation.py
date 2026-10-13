"""A held Round 5 claim keeps its dispatch sessions fresh through the armed wait.

A capsule is claimed with 60-180 s of launch life left on its 900 s dispatch sessions,
and the READY refresh never runs under a claim. A presenter who talked for a minute or
two between Ready and Ring therefore had the bell refused ("prepare the bout again").
The coordinator now rotates those sessions while the claim is held, and the bell's
margin check keeps refusing exactly what the dispatch gate would.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from server import connection_spike_live as live
from server import round5_warm
from server.connection_spike_live import (
    DISPATCH_CAPSULE_SAFETY_SECONDS,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    LiveConnectionSpikeAdapter,
    LiveRound5WarmProvider,
)
from server.models import SessionState
from server.round5_warm import (
    BELL_DISPATCH_HEADROOM_SECONDS,
    Round5LaunchCapsule,
    Round5Variant,
    Round5WarmState,
    WarmBellMarginError,
)
from tests.test_round5_bell_margin import (
    INSTALLATION,
    _armed_bout,
    _ShortLakebaseProvider,
    lakebase_dispatch_for,
)
from tests.test_round5_warm import Clock, Provider, coordinator, preparation, warm_ready

#: Longer than the 180 s armed window, so the whole window is held.
ARMED_WAIT_SECONDS = 175.0


def _rotated(capsule: Round5LaunchCapsule, now: datetime) -> Round5LaunchCapsule:
    """What a fresh 900 s mint of both dispatch sessions makes of ``capsule``."""

    dispatch = {lane: now + timedelta(seconds=900) for lane in ("lakebase", "competitor")}
    return replace(
        capsule,
        dispatch_expires_at=dispatch,
        expires_at=min(capsule.control_expires_at, *dispatch.values()),
        renew_by=min(dispatch.values()) - timedelta(seconds=DISPATCH_CAPSULE_SAFETY_SECONDS),
    )


class _RotatingProvider(Provider):
    def __init__(self, clock: Clock) -> None:
        super().__init__(clock)
        self.rotations = 0
        self.rotation_error: Exception | None = None
        self.rotation_hang: asyncio.Event | None = None
        self.rotation_generation_offset = 0

    async def refresh_claimed_dispatch(self, slot, capsule):
        del slot
        self.rotations += 1
        if self.rotation_hang is not None:
            await self.rotation_hang.wait()
        if self.rotation_error is not None:
            raise self.rotation_error
        rotated = _rotated(capsule, self.clock.now)
        return replace(rotated, generation=capsule.generation + self.rotation_generation_offset)


async def _claimed(provider: Provider, clock: Clock, *, seconds_left: int = 790, factory=None):
    provider.preparation_factory = factory or lakebase_dispatch_for(seconds_left)
    manager = coordinator(clock, provider)
    await warm_ready(manager, provider)
    claim_at = clock.now
    claimed, capsule = await manager.claim(
        session_id="session-rotation",
        bout_id="bout-rotation",
        selected_variant=Round5Variant.AURORA,
        bout_fence=7,
    )
    assert claimed.claim is not None
    manager.mark_claim_active(
        claimed.claim.claim_id, armed_deadline=claim_at + timedelta(seconds=240)
    )
    return manager, claimed.claim, capsule


async def _hold(manager, clock: Clock, claim, seconds: float) -> None:
    """Run the supervised loop on its own schedule, ringing in thought at every wake."""

    elapsed = 0.0
    while elapsed < seconds:
        # Just before the loop wakes is the latest a bell could land on this schedule.
        manager.require_dispatchable(claim)
        delay = await manager.run_one_cycle()
        manager.require_dispatchable(claim)
        step = min(delay, seconds - elapsed)
        clock.advance(step)
        elapsed += step


async def test_a_held_claim_is_dispatchable_through_the_whole_armed_window() -> None:
    clock = Clock()
    provider = _RotatingProvider(clock)
    manager, claim, capsule = await _claimed(provider, clock)
    # Claimed with 70 s of launch life: the bell used to be refused about a minute in.
    assert (capsule.launch_margin_cliff() - clock.now).total_seconds() == 70
    try:
        await _hold(manager, clock, claim, ARMED_WAIT_SECONDS)
        assert provider.rotations >= 2
        held = manager._capsule
        assert held is not None and held.generation == capsule.generation
        remaining = (held.launch_margin_cliff() - clock.now).total_seconds()
        assert remaining > BELL_DISPATCH_HEADROOM_SECONDS
        context = await manager.accept_bell(claim.claim_id)
        assert context.claim_id == claim.claim_id
        slot = await manager.store.read(INSTALLATION)
        assert slot is not None and slot.state == Round5WarmState.RUNNING
    finally:
        await manager.close()


async def test_the_rotation_never_delays_the_claim_renewal() -> None:
    clock = Clock()
    provider = _RotatingProvider(clock)
    manager, claim, _capsule = await _claimed(provider, clock)
    try:
        clock.advance(15)
        before = await manager.store.read(INSTALLATION)
        await manager.run_one_cycle()
        after = await manager.store.read(INSTALLATION)
        assert provider.rotations == 1
        assert before is not None and after is not None and after.claim is not None
        assert after.claim.claim_expires_at > before.claim.claim_expires_at
        assert after.state == Round5WarmState.CLAIMED
    finally:
        await manager.close()


@pytest.mark.parametrize(
    "failure",
    [RuntimeError("sts unavailable"), ConnectionSpikeLiveConfigurationError("short session")],
    ids=["transient", "configuration"],
)
async def test_a_failed_rotation_keeps_the_capsule_and_the_bell_still_refuses(
    failure: Exception, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    clock = Clock()
    provider = _RotatingProvider(clock)
    provider.rotation_error = failure
    manager, claim, capsule = await _claimed(provider, clock)
    try:
        for _ in range(12):
            clock.advance(10)
            await manager.run_one_cycle()
        assert provider.rotations >= 1
        assert manager._capsule is capsule
        with pytest.raises(WarmBellMarginError):
            await manager.accept_bell(claim.claim_id)
        slot = await manager.store.read(INSTALLATION)
        assert slot is not None and slot.state == Round5WarmState.CLAIMED
        assert any(
            record.getMessage().startswith("round5_claim_dispatch_rotation_failed")
            for record in caplog.records
        )
    finally:
        await manager.close()


async def test_a_hung_rotation_is_abandoned_and_the_capsule_kept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(round5_warm, "CLAIM_DISPATCH_ROTATION_TIMEOUT_SECONDS", 0.05)
    clock = Clock()
    provider = _RotatingProvider(clock)
    provider.rotation_hang = asyncio.Event()
    manager, claim, capsule = await _claimed(provider, clock)
    try:
        clock.advance(15)
        delay = await asyncio.wait_for(manager.run_one_cycle(), timeout=5)
        assert delay == round5_warm.CLAIM_DISPATCH_ROTATION_RETRY_SECONDS
        assert provider.rotations == 1
        assert manager._capsule is capsule
        slot = await manager.store.read(INSTALLATION)
        assert slot is not None and slot.state == Round5WarmState.CLAIMED
    finally:
        provider.rotation_hang.set()
        await manager.close()


async def test_a_rotation_for_another_warm_is_not_adopted() -> None:
    clock = Clock()
    provider = _RotatingProvider(clock)
    provider.rotation_generation_offset = 1
    manager, claim, capsule = await _claimed(provider, clock)
    try:
        for _ in range(12):
            clock.advance(10)
            await manager.run_one_cycle()
        assert provider.rotations >= 1
        assert manager._capsule is capsule
        with pytest.raises(WarmBellMarginError):
            await manager.accept_bell(claim.claim_id)
    finally:
        await manager.close()


async def test_a_claim_far_from_its_margin_rotates_nothing() -> None:
    clock = Clock()
    provider = _RotatingProvider(clock)
    manager, _claim, capsule = await _claimed(provider, clock, seconds_left=900)
    try:
        delay = await manager.run_one_cycle()
        assert provider.rotations == 0
        assert manager._capsule is capsule
        # Woken for the rotation itself, before the renewal cadence would have.
        assert delay <= (capsule.launch_margin_cliff() - clock.now).total_seconds() - 60
    finally:
        await manager.close()


def _control_bound(clock: Clock, **kwargs):
    # The control session, not a dispatch session, reaches its margin first: 30 s away.
    prep = preparation(clock, **kwargs)
    now = clock.now
    return replace(
        prep,
        capsule=replace(
            prep.capsule,
            control_expires_at=now + timedelta(seconds=30 * 60 + 60 + 30),
            dispatch_expires_at={
                "lakebase": now + timedelta(seconds=900),
                "competitor": now + timedelta(seconds=900),
            },
            expires_at=now + timedelta(seconds=900),
            renew_by=now + timedelta(seconds=100),
        ),
    )


async def test_a_bound_the_rotation_cannot_move_does_not_spin_the_loop() -> None:
    clock = Clock()
    provider = _RotatingProvider(clock)
    manager, _claim, _capsule = await _claimed(provider, clock, factory=_control_bound)
    try:
        delay = await manager.run_one_cycle()
        assert provider.rotations == 1
        assert delay == round5_warm.CLAIM_DISPATCH_ROTATION_RETRY_SECONDS
    finally:
        await manager.close()


# ---------------------------------------------------------------------------
# The live provider rotates only the sessions, and reports what the adapters hold
# ---------------------------------------------------------------------------


def _adapter(expires_at: datetime, minted: list[str], *, fresh_life: float = 900):
    adapter = object.__new__(LiveConnectionSpikeAdapter)
    adapter._prepared_clients = SimpleNamespace(expires_at=expires_at)
    adapter._capsule_refresh_lock = asyncio.Lock()
    adapter._resident_transport = None

    async def assumed(context_id: str):
        minted.append(context_id)
        return SimpleNamespace(expires_at=datetime.now(UTC) + timedelta(seconds=fresh_life))

    async def no_preflight(_clients):
        raise AssertionError("a session rotation must not rerun the runner preflight")

    adapter._assumed_clients = assumed
    adapter._preflight_runner = no_preflight
    return adapter


def _claimed_capsule(adapters: dict[str, LiveConnectionSpikeAdapter]) -> Round5LaunchCapsule:
    # The two variant engines share one pair of lane adapters, as a warm leaves them.
    engines = {
        Round5Variant.AURORA: SimpleNamespace(_lane_adapters=adapters),
        Round5Variant.RDS: SimpleNamespace(_lane_adapters=adapters),
    }
    base = preparation(Clock(), generation=4, fence=2).capsule
    now = datetime.now(UTC)
    dispatch = {lane: adapter._prepared_clients.expires_at for lane, adapter in adapters.items()}
    return replace(
        base,
        variant_contexts=engines,
        control_expires_at=now + timedelta(seconds=3600),
        dispatch_expires_at=dispatch,
        expires_at=min(dispatch.values()),
        renew_by=min(dispatch.values()) - timedelta(seconds=DISPATCH_CAPSULE_SAFETY_SECONDS),
    )


#: 50 s of launch life left: inside the rotation lead, so both sessions are due.
AGED_LIFE_SECONDS = 770


async def test_the_live_rotation_re_mints_each_dispatch_session_once() -> None:
    aged = datetime.now(UTC) + timedelta(seconds=AGED_LIFE_SECONDS)
    minted: list[str] = []
    adapters = {
        "lakebase": _adapter(aged, minted),
        "competitor": _adapter(aged, minted),
    }
    capsule = _claimed_capsule(adapters)
    provider = LiveRound5WarmProvider(SimpleNamespace(), lambda _competitor: None)
    rotated = await provider.refresh_claimed_dispatch(SimpleNamespace(generation=4), capsule)

    assert len(minted) == 2
    assert all(context.startswith("claim-4-") for context in minted)
    for lane, adapter in adapters.items():
        assert adapter._prepared_clients.expires_at > aged
        assert rotated.dispatch_expires_at[lane] == adapter._prepared_clients.expires_at
    assert (
        rotated.generation,
        rotated.coordinator_fence,
        rotated.broker_epoch,
        rotated.warm_attempt_token,
        rotated.variant_contexts,
    ) == (
        capsule.generation,
        capsule.coordinator_fence,
        capsule.broker_epoch,
        capsule.warm_attempt_token,
        capsule.variant_contexts,
    )
    assert rotated.launch_margin_cliff() > capsule.launch_margin_cliff()


async def test_after_a_rotation_the_bell_and_the_dispatch_gate_still_agree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aged = datetime.now(UTC) + timedelta(seconds=AGED_LIFE_SECONDS)
    minted: list[str] = []
    adapters = {"lakebase": _adapter(aged, minted), "competitor": _adapter(aged, minted)}
    provider = LiveRound5WarmProvider(SimpleNamespace(), lambda _competitor: None)
    rotated = await provider.refresh_claimed_dispatch(
        SimpleNamespace(generation=4), _claimed_capsule(adapters)
    )
    assert len(minted) == 2
    cliff = rotated.launch_margin_cliff()

    class FrozenDatetime(datetime):
        current = cliff

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(live, "datetime", FrozenDatetime)
    request = {"targets": [{"lane_id": "lakebase"}], "resident_generation": 1}
    FrozenDatetime.current = cliff - timedelta(seconds=1)
    assert rotated.meets_launch_margin(FrozenDatetime.current) is True
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="not configured"):
        await adapters["lakebase"].execute_prepared("a" * 64, request)
    FrozenDatetime.current = cliff
    with pytest.raises(ConnectionSpikeLiveOperationError, match="inside its safety margin"):
        await adapters["lakebase"].execute_prepared("a" * 64, request)


async def test_a_session_that_cannot_be_re_minted_fails_the_rotation() -> None:
    aged = datetime.now(UTC) + timedelta(seconds=AGED_LIFE_SECONDS)
    minted: list[str] = []
    adapters = {
        "lakebase": _adapter(aged, minted),
        # STS answered with a session shorter than the dispatch margin.
        "competitor": _adapter(aged, minted, fresh_life=600),
    }
    provider = LiveRound5WarmProvider(SimpleNamespace(), lambda _competitor: None)
    with pytest.raises(ConnectionSpikeLiveOperationError, match="safety margin"):
        await provider.refresh_claimed_dispatch(
            SimpleNamespace(generation=4), _claimed_capsule(adapters)
        )
    # The short session was not adopted: that adapter still holds what it had.
    assert adapters["competitor"]._prepared_clients.expires_at == aged


async def test_a_failed_rotation_waits_for_the_slower_lane_before_it_answers() -> None:
    # One lane refused at once while the other was still minting. Answering then left the
    # slow one to install its session after a later refresh had installed a newer one.
    aged = datetime.now(UTC) + timedelta(seconds=AGED_LIFE_SECONDS)
    minted: list[str] = []
    slow = _adapter(aged, minted)
    sts_answers = asyncio.Event()
    mint = slow._assumed_clients

    async def slow_mint(context_id: str):
        await sts_answers.wait()
        return await mint(context_id)

    slow._assumed_clients = slow_mint
    adapters = {"lakebase": slow, "competitor": _adapter(aged, minted, fresh_life=600)}
    provider = LiveRound5WarmProvider(SimpleNamespace(), lambda _competitor: None)
    rotation = asyncio.create_task(
        provider.refresh_claimed_dispatch(SimpleNamespace(generation=4), _claimed_capsule(adapters))
    )
    for _ in range(5):
        await asyncio.sleep(0)
    assert not rotation.done()

    sts_answers.set()
    with pytest.raises(ConnectionSpikeLiveOperationError, match="safety margin"):
        await rotation
    assert slow._prepared_clients.expires_at > aged


# ---------------------------------------------------------------------------
# Through the real manager and HTTP route
# ---------------------------------------------------------------------------


class _RotatingShortLakebaseProvider(_ShortLakebaseProvider):
    async def refresh_claimed_dispatch(self, slot, capsule):
        del slot
        return _rotated(capsule, self.clock.now)


async def test_a_presenter_who_talks_for_two_minutes_still_rings_the_bell() -> None:
    async with _armed_bout(
        lakebase_seconds=790, wait=150, provider_cls=_RotatingShortLakebaseProvider
    ) as bout:
        run = await bout.client.post(f"/api/sessions/{bout.session_id}/run")
        assert run.status_code == 200, run.text
        snapshot = (await bout.client.get(f"/api/sessions/{bout.session_id}")).json()
        assert snapshot["state"] == SessionState.RUNNING.value
        slot = await bout.warm.store.read("install-acceptance")
        assert slot is not None and slot.state == Round5WarmState.RUNNING
