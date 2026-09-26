"""Round 5 warm-slot source availability is transient, identity drift is terminal.

These tests pin the root cause of the 2026-09-23 outage: a competitor source that
matched its sealed identity but was momentarily ``status != available`` was folded
into a terminal ``warm_baseline_invalid`` block, latching the fight card
``blocked_terminal=true`` with ``next_retry_at=null`` forever.

The invariants proved here:

* true identifier / resource-id / host / VPC / security-group drift stays terminal
  (``warm_baseline_invalid``) and does NOT auto-unlock;
* matching identity with a non-available status -- or a describe that momentarily
  returned no matching row -- is transient (``warm_source_unavailable``), retries
  on a bounded backoff, and auto-returns to READY with no app restart;
* a transient that persists past the escalation count becomes a *self-verifiable*
  block (``warm_source_unavailable_persistent``), never a terminal identity block.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

# Reuse the in-file coordinator/store/provider harness from the warm-slot suite.
from test_round5_warm import Clock, Provider, coordinator  # noqa: E402

from server.connection_spike_live import (
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveSourceUnavailableError,
    ConnectionSpikeLiveSourceUnresolvedError,
    ConnectionSpikeLiveTransientError,
    LiveRound5WarmProvider,
    _CompetitorSource,
    _require_warm_source_identity,
)
from server.round5_warm import (
    MAX_TRANSIENT_WARM_ATTEMPTS,
    SELF_VERIFIABLE_BLOCK_CODES,
    SELF_VERIFIABLE_BLOCK_RETRY_SECONDS,
    BlockedWarmError,
    RetryableWarmError,
    Round5WarmState,
    _blocked_is_terminal,
)

IDENT = "anti-demo-competitor"
RESID = "cluster-sealed"
HOST = "competitor.example.invalid"
VPC = "vpc-sealed"
SG = "sg-competitor"

# A representative, non-exhaustive set of RDS/Aurora statuses that are transient:
# the source is present and its identity is intact, it is simply not yet usable.
TRANSIENT_STATUSES = [
    "backing-up",
    "modifying",
    "failing-over",
    "rebooting",
    "creating",
    "upgrading",
    "maintenance",
    "starting",
    "stopping",
    "renaming",
    "configuring-enhanced-monitoring",
    "storage-optimization",
]


def _source(**overrides: object) -> _CompetitorSource:
    base: dict[str, object] = dict(
        identifier=IDENT,
        resource_id=RESID,
        direct_host=HOST,
        status="available",
        vpc_id=VPC,
        security_group_ids=(SG,),
    )
    base.update(overrides)
    return _CompetitorSource(**base)  # type: ignore[arg-type]


def _classify(source: _CompetitorSource, *, sg: str | None = SG) -> None:
    _require_warm_source_identity(
        source,
        expected_identifier=IDENT,
        expected_resource_id=RESID,
        expected_direct_host=HOST,
        expected_vpc_id=VPC,
        expected_security_group_id=sg,
    )


# --------------------------------------------------------------------------- #
# 1. Pure classifier: identity (terminal) vs availability/absence (transient)
# --------------------------------------------------------------------------- #


def test_matching_identity_and_available_passes() -> None:
    _classify(_source())  # exact-security-group contract
    _classify(_source(), sg=None)  # preflight derives the group, wants exactly one


@pytest.mark.parametrize("status", TRANSIENT_STATUSES)
def test_non_available_status_is_transient_not_terminal(status: str) -> None:
    with pytest.raises(ConnectionSpikeLiveSourceUnavailableError):
        _classify(_source(status=status))


@pytest.mark.parametrize(
    "override",
    [
        {"identifier": "someone-elses-cluster"},
        {"resource_id": "cluster-wrong"},
        {"direct_host": "attacker.example.invalid"},
        {"vpc_id": "vpc-wrong"},
        {"security_group_ids": ("sg-wrong",)},
    ],
)
def test_each_identity_field_mismatch_is_terminal(override: dict[str, object]) -> None:
    with pytest.raises(ConnectionSpikeLiveConfigurationError):
        _classify(_source(**override))


def test_empty_describe_is_unresolved_bounded_then_terminal() -> None:
    # Req #4: a describe that returned no matching row collapses every field to "".
    # It is classified as UNRESOLVED -- retried boundedly under eventual
    # consistency, but escalating to a TERMINAL block if it persists (an unfindable
    # source needs an operator), never a self-verifiable recheck forever.
    empty = _source(
        identifier="",
        resource_id="",
        direct_host="",
        status="",
        vpc_id="",
        security_group_ids=(),
    )
    with pytest.raises(ConnectionSpikeLiveSourceUnresolvedError):
        _classify(empty)
    # It remains a transient (subclass) so the FIRST attempts still retry, not
    # latch warm_baseline_invalid.
    assert issubclass(
        ConnectionSpikeLiveSourceUnresolvedError, ConnectionSpikeLiveSourceUnavailableError
    )


def test_present_but_busy_source_is_unavailable_not_unresolved() -> None:
    # Req #4: a present-but-non-available source (identity intact) stays the
    # self-verifiable "unavailable" taxonomy, NOT the terminal-escalating
    # "unresolved" one -- a busy DB recovers on its own.
    with pytest.raises(ConnectionSpikeLiveSourceUnavailableError) as raised:
        _classify(_source(status="modifying"))
    assert not isinstance(raised.value, ConnectionSpikeLiveSourceUnresolvedError)


def test_unresolved_persistent_escalation_is_terminal_not_self_verifiable() -> None:
    # Req #4: the persistent unresolved block is TERMINAL (operator attention),
    # unlike the self-verifiable persistent unavailable (busy DB) block, so an
    # unfindable source does not recheck forever.
    assert "warm_source_unresolved_persistent" not in SELF_VERIFIABLE_BLOCK_CODES
    assert "warm_source_unavailable_persistent" in SELF_VERIFIABLE_BLOCK_CODES


def test_wrong_security_group_count_is_terminal_when_group_derived() -> None:
    # preflight path (expected_security_group_id=None) requires exactly one group.
    with pytest.raises(ConnectionSpikeLiveConfigurationError):
        _classify(_source(security_group_ids=(SG, "sg-extra")), sg=None)


def test_source_unavailable_is_a_transient_error_subclass() -> None:
    assert issubclass(
        ConnectionSpikeLiveSourceUnavailableError, ConnectionSpikeLiveTransientError
    )


# --------------------------------------------------------------------------- #
# 2. Provider.prepare() taxonomy mapping
# --------------------------------------------------------------------------- #


class _FakeEngine:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def warm(self, generation: int, warm_attempt_token: str) -> object:
        raise self._exc

    async def warm_with_physical_runners_from(
        self, other: object, generation: int
    ) -> object:  # pragma: no cover - the aurora lane raises first
        raise AssertionError("RDS lane must not be reached when the aurora lane fails")


async def _prepare_with(exc: BaseException) -> object:
    provider = LiveRound5WarmProvider(
        manifest=cast(Any, object()),
        engine_factory=lambda competitor_id: cast(Any, _FakeEngine(exc)),
    )
    return await provider.prepare(
        generation=8,
        coordinator_fence=1,
        process_epoch="proc",
        broker_epoch="broker",
        warm_attempt_token="attempt",
        requires_cleaned_bout=False,
    )


async def test_prepare_maps_source_unavailable_to_retryable_taxonomy() -> None:
    with pytest.raises(RetryableWarmError) as raised:
        await _prepare_with(
            ConnectionSpikeLiveSourceUnavailableError("source is backing-up")
        )
    assert raised.value.code == "warm_source_unavailable"


async def test_prepare_maps_source_unresolved_to_distinct_retryable_code() -> None:
    # Req #4: an empty/unresolved describe maps to its OWN retryable code, whose
    # persistent escalation is terminal (not the self-verifiable unavailable one).
    with pytest.raises(RetryableWarmError) as raised:
        await _prepare_with(
            ConnectionSpikeLiveSourceUnresolvedError("did not resolve to one row")
        )
    assert raised.value.code == "warm_source_unresolved"


async def test_prepare_maps_identity_drift_to_terminal_baseline_invalid() -> None:
    with pytest.raises(BlockedWarmError) as raised:
        await _prepare_with(
            ConnectionSpikeLiveConfigurationError("Round 5 warm source identity changed")
        )
    assert raised.value.code == "warm_baseline_invalid"


async def test_prepare_maps_generic_transient_read_to_provider_retryable() -> None:
    with pytest.raises(RetryableWarmError) as raised:
        await _prepare_with(TimeoutError("aws read timed out"))
    assert raised.value.code == "warm_provider_retryable"


async def test_prepare_fails_closed_on_unexpected_error() -> None:
    with pytest.raises(BlockedWarmError) as raised:
        await _prepare_with(ValueError("unexpected"))
    assert raised.value.code == "warm_baseline_unexpected"


# --------------------------------------------------------------------------- #
# 3. Taxonomy: the persistent escalation is self-verifiable, not terminal
# --------------------------------------------------------------------------- #


def test_persistent_source_unavailable_is_self_verifiable() -> None:
    assert "warm_source_unavailable_persistent" in SELF_VERIFIABLE_BLOCK_CODES
    non_terminal = SimpleNamespace(
        state=Round5WarmState.BLOCKED,
        last_error_code="warm_source_unavailable_persistent",
    )
    terminal = SimpleNamespace(
        state=Round5WarmState.BLOCKED,
        last_error_code="warm_baseline_invalid",
    )
    assert _blocked_is_terminal(non_terminal) is False
    assert _blocked_is_terminal(terminal) is True


# --------------------------------------------------------------------------- #
# 4. End-to-end self-heal through the real coordinator (the outage scenario)
# --------------------------------------------------------------------------- #


async def test_source_unavailable_retries_and_self_heals_to_ready() -> None:
    clock = Clock()
    provider = Provider(clock)
    provider.release_prepare.set()
    provider.prepare_error = RetryableWarmError("warm_source_unavailable")
    manager = coordinator(clock, provider)

    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.WARMING  # NOT blocked
    assert slot.next_retry_at is not None and slot.next_retry_at > clock.now
    assert slot.last_error_code == "warm_source_unavailable"
    assert manager.public_status_cached()["round5_warm_blocked_terminal"] is False

    # The source returns to available; the next attempt reaches READY with no
    # restart and no sticky error.
    clock.advance(120)
    provider.prepare_error = None
    await manager.run_one_cycle()
    slot = await manager.store.read("install-one")
    assert slot is not None
    assert slot.state == Round5WarmState.READY
    assert slot.last_error_code is None


async def test_persistent_source_unavailable_escalates_self_verifiable_not_terminal() -> None:
    clock = Clock()
    provider = Provider(clock)
    provider.release_prepare.set()
    provider.prepare_error = RetryableWarmError("warm_source_unavailable")
    manager = coordinator(clock, provider)

    slot = None
    for _ in range(MAX_TRANSIENT_WARM_ATTEMPTS + 5):
        await manager.run_one_cycle()
        slot = await manager.store.read("install-one")
        assert slot is not None
        if (
            slot.state == Round5WarmState.BLOCKED
            and slot.last_error_code == "warm_source_unavailable_persistent"
        ):
            break
        clock.advance(120)

    assert slot is not None
    assert slot.state == Round5WarmState.BLOCKED
    assert slot.last_error_code == "warm_source_unavailable_persistent"
    # Crucially NOT terminal: the catalog/readyz must not promise operator-only
    # unlock -- the coordinator re-verifies this itself on a bounded interval.
    assert manager.public_status_cached()["round5_warm_blocked_terminal"] is False

    # And it does self-heal without a human: clear the transient, cross the
    # self-verifiable recheck interval, and it returns to READY on its own.
    clock.advance(SELF_VERIFIABLE_BLOCK_RETRY_SECONDS + 1)
    provider.prepare_error = None
    for _ in range(3):
        await manager.run_one_cycle()
        slot = await manager.store.read("install-one")
        assert slot is not None
        if slot.state == Round5WarmState.READY:
            break
        clock.advance(SELF_VERIFIABLE_BLOCK_RETRY_SECONDS + 1)
    assert slot is not None
    assert slot.state == Round5WarmState.READY
