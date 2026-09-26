"""Server-seam integration for the post-bout launch-capsule re-establishment.

Distinct from the attested IDENTITY-change loop (``test_round5_identity_reestablish_seam.py``):
that loop was driven by ``validate_ready`` reading an ATTESTED ``IDENTITY_CHANGED``. This
class was reproduced LIVE on the deployed identity-reestablish child (63e6698): after a real
bout + AWS towel@~75s, cleanup advanced the generation N->N+1, then the rewarm STORMED because
the READY keep-alive found the launch capsule no longer ``_capsule_belongs`` to the slot
(``launch_capsule_missing``). The old path did ``freshness_lost`` and rewarmed; because that
rewarm SUCCEEDS, the ``MAX_TRANSIENT_WARM_ATTEMPTS`` bound (checked only in the rewarm ERROR
path) never fired, and each transient ``publish_ready`` cleared ``last_error`` -> an invisible,
unbounded ``identity-refresh <-> rewarming`` flood (frozen gen, err=None, 20+ flips, ring
un-claimable) that only a process restart cleared. Evidence:
``~/Documents/round5-chaos-evidence-*/STORM_RECURRENCE_63e6698.md``.

The reproduction here is faithful and self-contained: ``publish_ready`` validates the capsule's
generation/coordinator_fence/warm_attempt_token but NOT its ``broker_epoch``, while
``_capsule_belongs``/``_capsule_current`` DO. A capsule whose ``broker_epoch`` has drifted
therefore PUBLISHES READY yet fails ``_capsule_belongs`` on the very next probe -- exactly the
``launch_capsule_missing`` condition, produced without any private hooks.

These drive the REAL ``Round5WarmCoordinator`` over the REAL transport + control store + warm
store (reusing the identity seam's doubles) and assert the fix end to end:

* a persistent non-belonging capsule does NOT loop forever -- it is BOUNDED to
  ``MAX_CAPSULE_REESTABLISH_ATTEMPTS`` rewarms and escalates to the named, SELF-VERIFIABLE
  block ``warm_capsule_unrecoverable`` (observable, never terminal-latched);
* the recovery is OBSERVABLE across the transient READY the rewarm publishes -- a durable named
  ``last_error`` and an un-claimable ring, the exact window the pre-fix code blanked to
  ``err=None``; and
* once the capsule belongs again (the drift clears), the SAME process self-recovers to a
  claimable READY without a restart, and the bounded budget resets.

Each assertion is mutation-sensitive: it FAILS on 63e6698 (no ``_reestablish_capsule``: an
unbounded ``launch_capsule_missing`` flood that never blocks and whose ``last_error`` is wiped
by the transient ``publish_ready``) and PASSES on the child.
"""

from __future__ import annotations

import dataclasses

import pytest

from server.round5_control import InMemoryRound5ControlStore
from server.round5_warm import (
    InMemoryRound5WarmStore,
    Round5WarmCoordinator,
    Round5WarmState,
    _blocked_is_terminal,
)

try:  # The bound is new on the child; keep the import resilient so the PRE-FIX run
    # exercises the BEHAVIOR (an unbounded flood that never blocks) and fails on the
    # assertions below rather than at collection.
    from server.round5_warm import MAX_CAPSULE_REESTABLISH_ATTEMPTS
except ImportError:  # pragma: no cover - pre-fix (63e6698) only
    MAX_CAPSULE_REESTABLISH_ATTEMPTS = 5
from tests.test_round5_identity_reestablish_seam import (
    DIGEST,
    INSTALLATION,
    Clock,
    RunnerDouble,
    SeamProvider,
)

pytestmark = pytest.mark.asyncio


class DriftingEpochProvider(SeamProvider):
    """A SeamProvider whose prepared capsule can carry a DRIFTED ``broker_epoch``.

    ``publish_ready`` does not validate ``broker_epoch`` (only generation, fence, and
    warm_attempt_token), but ``_capsule_belongs``/``_capsule_current`` do -- so a drifted
    capsule reaches READY yet fails the very next keep-alive probe. That is exactly the
    live ``launch_capsule_missing`` condition, with no private coordinator hooks. Toggle
    ``drift`` to model the condition clearing (a rewarm's capsule belongs again).
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.drift = False

    async def prepare(self, **kwargs: object):  # type: ignore[override]
        preparation = await super().prepare(**kwargs)
        if not self.drift:
            return preparation
        drifted = dataclasses.replace(
            preparation.capsule,
            broker_epoch=preparation.capsule.broker_epoch + "-DRIFT",
        )
        return dataclasses.replace(preparation, capsule=drifted)


async def _new_drift_seam():
    clock = Clock()
    store = InMemoryRound5ControlStore()
    runner = RunnerDouble(store, clock)
    provider = DriftingEpochProvider(clock, store, runner)
    manager = Round5WarmCoordinator(
        installation_id=INSTALLATION,
        warm_contract_sha256=DIGEST,
        store=InMemoryRound5WarmStore(),
        provider=provider,
        process_epoch="process-capsule-seam",
        broker_epoch="broker-capsule-seam",
        clock=clock,
        monotonic_ns=clock.monotonic,
    )
    await manager.store.initialize()
    return clock, store, runner, provider, manager


async def _drive_to_claimable(manager, provider, limit: int = 40):
    for _ in range(limit):
        await provider.runner.beat()
        await manager.run_one_cycle()
        if manager.ring_ready:
            return await manager.store.read(INSTALLATION)
        provider.clock.advance(1)
    return await manager.store.read(INSTALLATION)


async def test_persistent_non_belonging_capsule_is_bounded_and_named_block() -> None:
    # A launch capsule that never belongs (drifted broker_epoch every rewarm) must NOT
    # loop forever. It is bounded to MAX_CAPSULE_REESTABLISH_ATTEMPTS rewarms, then
    # escalates to the named, SELF-VERIFIABLE block ``warm_capsule_unrecoverable``.
    clock, _store, runner, provider, manager = await _new_drift_seam()

    # Reach a healthy claimable READY first (belonging capsule).
    ready = await _drive_to_claimable(manager, provider)
    assert ready is not None and ready.state == Round5WarmState.READY
    assert manager.ring_ready

    # From now on, every rewarm's capsule has a drifted broker_epoch: it publishes
    # READY (publish_ready ignores broker_epoch) but fails _capsule_belongs.
    provider.drift = True
    # Nudge the READY slot out of steady state so the next keep-alive rebuilds the
    # (now-drifting) capsule: a single freshness probe over a discarded capsule.
    manager._capsule = None

    blocked = None
    prepares_at_block = 0
    for _ in range(60):
        slot = await manager.store.read(INSTALLATION)
        if slot is not None and slot.state == Round5WarmState.BLOCKED:
            blocked = slot
            prepares_at_block = len(provider.attempt_tokens)
            break
        await runner.beat()
        await manager.run_one_cycle()
        clock.advance(1)

    assert blocked is not None, "the non-belonging capsule loop never bounded (pre-fix flood)"
    assert blocked.last_error_code == "warm_capsule_unrecoverable"
    # Bounded, observable, and NOT terminal-latched (self-verifiable: rechecks on a
    # bounded interval and self-recovers once a rewarm's capsule belongs).
    assert _blocked_is_terminal(blocked) is False
    assert not manager.ring_ready
    status = manager.public_status_cached()
    assert status["round5_warm_last_error_code"] == "warm_capsule_unrecoverable"
    # Bounded rewarm flood: the drifting rewarms before the block are on the order of
    # the cap, never one-per-cycle-forever. (Pre-fix, this grows without bound.)
    assert prepares_at_block <= MAX_CAPSULE_REESTABLISH_ATTEMPTS + 3


async def test_non_belonging_capsule_is_observable_across_the_transient_ready() -> None:
    # The exact regression: the pre-fix code cleared last_error at the rewarm's
    # transient publish_ready, so an idle sampler saw err=None while the ring churned.
    # The fix keeps a durable NAMED error across the transient READY the rewarm
    # publishes, until a belonging capsule + CURRENT probe clears it.
    clock, _store, runner, provider, manager = await _new_drift_seam()
    await _drive_to_claimable(manager, provider)
    assert manager.ring_ready

    provider.drift = True
    manager._capsule = None

    named_at_transient_ready = False
    saw_transient_ready = False
    for _ in range(30):
        await runner.beat()
        await manager.run_one_cycle()
        slot = await manager.store.read(INSTALLATION)
        if slot is not None and slot.state == Round5WarmState.READY:
            saw_transient_ready = True
            status = manager.public_status_cached()
            # A transient READY during the churn is NEVER claimable and NEVER blank.
            assert status["round5_ring_ready"] is False
            if status["round5_warm_last_error_code"] in (
                "warm_capsule_reestablishing",
                "warm_capsule_unrecoverable",
            ):
                named_at_transient_ready = True
        clock.advance(1)

    assert saw_transient_ready, "expected at least one transient READY during the churn"
    assert named_at_transient_ready, (
        "transient publish_ready blanked last_error to None during the capsule churn "
        "(pre-fix regression)"
    )


async def test_non_belonging_capsule_then_belongs_self_recovers_without_restart() -> None:
    # Once the capsule belongs again (the drift clears), the SAME process must
    # self-recover to a claimable READY -- no restart -- and reset the bounded budget.
    clock, _store, runner, provider, manager = await _new_drift_seam()
    ready_a = await _drive_to_claimable(manager, provider)
    token_a = ready_a.warm_attempt_token

    provider.drift = True
    manager._capsule = None
    # Churn until it escalates to the bounded named block.
    for _ in range(40):
        slot = await manager.store.read(INSTALLATION)
        if slot is not None and slot.state == Round5WarmState.BLOCKED:
            break
        await runner.beat()
        await manager.run_one_cycle()
        clock.advance(1)
    blocked = await manager.store.read(INSTALLATION)
    assert blocked is not None and blocked.last_error_code == "warm_capsule_unrecoverable"
    assert manager._capsule_missing_failures >= MAX_CAPSULE_REESTABLISH_ATTEMPTS

    # The drift clears (a rewarm's capsule belongs again). Advance past the
    # self-verifiable recheck interval so the block re-attempts the warm.
    provider.drift = False
    clock.advance(90)
    recovered = await _drive_to_claimable(manager, provider, limit=80)
    assert recovered is not None and recovered.state == Round5WarmState.READY
    assert manager.ring_ready
    assert recovered.warm_attempt_token != token_a
    # The bounded budget was reset once the capsule belonged again.
    assert manager._capsule_missing_failures == 0
    status = manager.public_status_cached()
    assert status["round5_warm_last_error_code"] is None
