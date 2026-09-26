"""Root-cause guard: the credential/receipt refresh must PRESERVE the slot broker_epoch.

Reproduced LIVE on the capsule-bound child (82628a0): after ~45 min idle, the
credential/receipt refresh fired and Round 5 fell into the launch_capsule_missing churn
(bounded+named by 82628a0 to warm_capsule_reestablishing -> warm_capsule_unrecoverable, but
still an idle Temporarily-Unavailable window). Root cause:
``LiveRound5WarmProvider.refresh_preparation`` / ``refresh_capsule`` minted a FRESH RANDOM
``broker_epoch`` (``broker-{uuid4()}``) on every refresh. ``broker_epoch`` is a per-process
identity used ONLY by the capsule-belonging checks (``_capsule_belongs``/``_capsule_current``)
and has NO role on the resident control wire. The coordinator stamps every rewarm's capsule with
its STABLE ``self.broker_epoch``; once a refresh rotated the slot's broker_epoch to a random
value, the next freshness_lost->rewarm published a capsule whose broker_epoch no longer matched
the slot -> ``launch_capsule_missing`` -> the idle rewarm storm.

The fix preserves ``slot.broker_epoch`` across a refresh so refresh and rewarm produce belonging
capsules interchangeably. These tests capture the ``broker_epoch`` the refresh hands to the
capsule builders and assert it is the slot's, not a fresh random one -- FAILS on 82628a0
(random), PASSES on the fix.
"""

from __future__ import annotations

import pytest

from server.connection_spike_live import LiveRound5WarmProvider

pytestmark = pytest.mark.asyncio

STABLE_BROKER_EPOCH = "broker-stable-epoch-0"


class _FakeEngine:
    async def refresh_warm(self, generation: int) -> object:
        del generation
        return object()

    async def warm_with_physical_runners_from(self, other: object, generation: int) -> object:
        del other, generation
        return object()


class _FakeSlot:
    generation = 5
    coordinator_fence = 7
    warm_attempt_token = "attempt-fixed"
    broker_epoch = STABLE_BROKER_EPOCH


def _provider(capture: dict) -> LiveRound5WarmProvider:
    provider = LiveRound5WarmProvider(object(), lambda _competitor: _FakeEngine())
    provider._engines = {
        "aurora_serverless_v2": _FakeEngine(),
        "rds_postgres": _FakeEngine(),
    }
    provider._receipts = {}

    def _capture(**kwargs: object) -> object:
        capture.clear()
        capture.update(kwargs)
        return object()

    # Capture exactly the arguments the refresh hands the capsule builders; the
    # returned object is opaque to these tests.
    provider._assemble_preparation = _capture  # type: ignore[assignment]
    provider._capsule = _capture  # type: ignore[assignment]
    return provider


async def test_refresh_preparation_preserves_broker_epoch() -> None:
    capture: dict = {}
    provider = _provider(capture)
    slot = _FakeSlot()
    await provider.refresh_preparation(slot, object())
    assert capture["broker_epoch"] == slot.broker_epoch, (
        "refresh_preparation minted a fresh broker_epoch; it must preserve slot.broker_epoch "
        "or a post-refresh rewarm publishes a non-belonging capsule (launch_capsule_missing)"
    )
    # The other belonging fields are already preserved and must stay so.
    assert capture["generation"] == slot.generation
    assert capture["coordinator_fence"] == slot.coordinator_fence
    assert capture["warm_attempt_token"] == slot.warm_attempt_token


async def test_refresh_capsule_preserves_broker_epoch() -> None:
    capture: dict = {}
    provider = _provider(capture)
    slot = _FakeSlot()
    await provider.refresh_capsule(slot, object())
    assert capture["broker_epoch"] == slot.broker_epoch, (
        "refresh_capsule minted a fresh broker_epoch; it must preserve slot.broker_epoch"
    )
    assert capture["generation"] == slot.generation
    assert capture["coordinator_fence"] == slot.coordinator_fence
