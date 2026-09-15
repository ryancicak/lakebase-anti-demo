"""The fight card must not offer rounds it is about to refuse.

Ryan found this live during a Round 5 bout: every one of the six rounds reported BOUT IN PROGRESS,
each with the sentence "Other rounds remain available", while every one of them returned
`can_start: false`. The card said go somewhere else and then refused everywhere, which reads as a
bug in the ring fencing rather than what it is.

The lock itself is real and correct. Per-round rings require a v7 seal, and an installation whose
Round 6 is unsealed is pinned to v5 with one ring for the whole installation, so one bout does
hold all six rounds. What was wrong was only what the card said while refusing.

These tests cover both moments, because cleanup is the one that lasts minutes and is the one most
likely to be on screen.
"""

from __future__ import annotations

from datetime import UTC, datetime

from server.api import _fight_card_round_status
from server.models import Availability, BoutStatus, FightCardState, RoundId

HOLDER = "Ready a pooled application path"

READY_AVAILABILITY = type(
    "Availability",
    (),
    {
        "availability": Availability.READY,
        "availability_reason_code": None,
        "availability_headline": None,
    },
)()


def bout(*, phase: str) -> BoutStatus:
    return BoutStatus(
        scope="global",
        round_id=None,
        active=True,
        can_start=False,
        phase=phase,
        round_title="Ready a pooled application path",
        updated_at=datetime.now(UTC),
    )


def status(*, phase: str, shared: bool, holder: str | None):
    return _fight_card_round_status(
        RoundId.WAKE_IDLE_APP,
        READY_AVAILABILITY,
        bout(phase=phase),
        rounds_share_one_ring=shared,
        ring_holder=holder,
    )


def test_a_shared_ring_does_not_claim_other_rounds_are_available() -> None:
    """The exact sentence Ryan saw on all six rounds at once."""

    result = status(phase="run_committed", shared=True, holder=HOLDER)
    detail = result.detail
    assert detail is not None
    assert "Other rounds remain available" not in detail


def test_a_shared_ring_names_the_round_holding_it() -> None:
    """The one thing an operator can act on: wait for that round.

    Taken from the lease's own `round_title`, so the card cannot name a different round than the
    one that actually holds the ring.
    """

    result = status(phase="run_committed", shared=True, holder=HOLDER)
    detail = result.detail
    assert detail is not None
    assert "Ready a pooled application path" in detail
    assert "one bout at a time" in detail


def test_cleanup_on_a_shared_ring_is_truthful_too() -> None:
    """Cleanup is the part that takes minutes, so it is the part most likely to be read."""

    result = status(phase="round5_cleanup", shared=True, holder=HOLDER)
    assert result.state == FightCardState.CLEANUP_IN_PROGRESS
    assert result.detail is not None
    assert "Other rounds remain available" not in result.detail
    assert "releasing the ring" in result.detail


def test_independent_rings_keep_the_sentence_that_is_true_for_them() -> None:
    """With a v7 seal the rounds really are independent, and the card should say so.

    This is why the wording is conditional rather than simply removed: on an installation with
    per-round fences, one bout genuinely leaves the other five available.
    """

    for phase in ("run_committed", "round5_cleanup"):
        detail = status(phase=phase, shared=False, holder=None).detail
        assert detail is not None
        assert "Other rounds remain available" in detail


def test_an_unnamed_holder_still_says_what_is_happening() -> None:
    """A lease with no round title is a lease this card cannot name, not a lie it should tell."""

    cases = (("run_committed", "a bout is already"), ("round5_cleanup", "A bout is"))
    for phase, expected in cases:
        detail = status(phase=phase, shared=True, holder=None).detail
        assert detail is not None
        assert expected in detail
        assert "Other rounds remain available" not in detail
