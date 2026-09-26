"""Round 5 remediation C.2: a duplicate /run that returns the CACHED launch
intent must still re-mint the process-local bell capability under the current
fence, so a legitimate same-owner retry does not present a capability that aged
out of the setup-deadline window and get its timed CreateDBProxy refused.

Exercised as an unbound method over a SimpleNamespace (the pattern in
test_round5_bell_dispatch.py) so the cached path is tested without standing up a
full orchestrator: the cached branch touches only the bout maps, the config
deadline, the monotonic clock and ``arm_bell_capability``.
"""

from __future__ import annotations

import itertools
from types import SimpleNamespace

from server.connection_spike_live import LiveConnectionSpikeSetupOrchestrator

BOUT = "bout0123456789abcdef0123456789ab"
FENCE = 7


def _fake_with_cached_intent():
    intent = object()
    ticks = itertools.count(1_000_000_000, step=1_000_000_000)  # +1s per read
    fake = SimpleNamespace(
        _prepared={BOUT: FENCE},
        _scopes={BOUT: SimpleNamespace(bout_id=BOUT, fencing_token=FENCE)},
        _coordinators={BOUT: object()},
        _prepared_specs={BOUT: (SimpleNamespace(resource_kind="rds_proxy"),)},
        _promoted_proxy_intents={BOUT: intent},
        _bell_capabilities={},
        config=SimpleNamespace(deadline_seconds=600.0),
        _monotonic_ns=lambda: next(ticks),
    )
    fake.arm_bell_capability = lambda b, f, ttl_seconds=None: (
        LiveConnectionSpikeSetupOrchestrator.arm_bell_capability(
            fake, b, f, ttl_seconds=ttl_seconds
        )
    )
    return fake, intent


async def test_cached_intent_mints_bell_capability_under_current_fence() -> None:
    fake, intent = _fake_with_cached_intent()

    result = await LiveConnectionSpikeSetupOrchestrator.precommit_launch_intent(fake, BOUT, FENCE)

    assert result is intent  # the cached intent is returned unchanged
    capability = fake._bell_capabilities[BOUT]
    assert capability.fencing_token == FENCE
    assert capability.bout_id == BOUT
    assert capability.expires_ns > capability.minted_ns


async def test_duplicate_run_refreshes_the_capability_expiry() -> None:
    fake, intent = _fake_with_cached_intent()

    await LiveConnectionSpikeSetupOrchestrator.precommit_launch_intent(fake, BOUT, FENCE)
    first = fake._bell_capabilities[BOUT]

    # A second /run for the same bout/fence returns the cached intent AGAIN and
    # must move the capability's mint/expiry forward (fresh window), not reuse
    # the first one -- this is the regression the fix closes.
    result = await LiveConnectionSpikeSetupOrchestrator.precommit_launch_intent(fake, BOUT, FENCE)
    second = fake._bell_capabilities[BOUT]

    assert result is intent
    assert second.minted_ns > first.minted_ns
    assert second.expires_ns > first.expires_ns
    assert second.fencing_token == FENCE


async def test_unprepared_bell_is_refused_even_with_a_cached_intent() -> None:
    # Fence-drift / never-prepared still fails closed: the guard runs before the
    # cached-intent return, so a token that does not match the prepared fence is
    # refused rather than served a stale capability.
    fake, _ = _fake_with_cached_intent()
    import pytest

    from server.connection_spike_live import ConnectionSpikeLiveOperationError

    with pytest.raises(ConnectionSpikeLiveOperationError):
        await LiveConnectionSpikeSetupOrchestrator.precommit_launch_intent(fake, BOUT, FENCE + 1)
    # and no capability was minted for the wrong fence
    assert FENCE + 1 not in {c.fencing_token for c in fake._bell_capabilities.values()}
