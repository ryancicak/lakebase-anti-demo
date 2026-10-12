"""A Round 4 or 6 stoppage names only the margin floor it proves.

The loser ran out its bound from the bell and the winner delivered at U, so the
proven floor is the bound minus U, not the whole bound.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from server import manager
from server.models import ComparisonKind
from server.round4_race import COMPETITOR_LANE, LAKEBASE_LANE, Resolution, Verdict

WINNER_U_MS = 30_000.0


def stoppage(winner: str) -> SimpleNamespace:
    loser = COMPETITOR_LANE if winner == LAKEBASE_LANE else LAKEBASE_LANE
    return SimpleNamespace(
        resolution=Resolution(Verdict.LOWER_BOUND, winner=winner),
        outcomes={
            winner: SimpleNamespace(lane_id=winner, verified=True, elapsed_ms=WINNER_U_MS),
            loser: SimpleNamespace(lane_id=loser, verified=False, elapsed_ms=None),
        },
    )


@pytest.mark.parametrize(
    ("compare", "bound_seconds"),
    [
        (manager._round_four_comparison, manager.LANE_TIMEOUT_SECONDS),
        (manager._round_six_comparison, manager.ROUND6_LANE_TIMEOUT_SECONDS),
    ],
)
@pytest.mark.parametrize("winner", [LAKEBASE_LANE, COMPETITOR_LANE])
def test_stoppage_detail_states_the_bound_less_the_winners_time(
    compare, bound_seconds, winner
) -> None:
    comparison, measured = compare(stoppage(winner), "AWS DMS", has_competitor=True)
    proven_floor_s = bound_seconds - WINNER_U_MS / 1000
    assert measured is True
    assert comparison.kind == ComparisonKind.ADJUDICATED_STOPPAGE
    assert comparison.winner_lane_id == winner
    assert f"at least {proven_floor_s:.2f} s" in comparison.detail
    assert f"within {bound_seconds:.0f} s of the bell" in comparison.detail
    assert "so the margin is at least that" not in comparison.detail
    assert "not a measurement" in comparison.detail


def test_the_printed_floor_never_rounds_above_what_is_proven() -> None:
    clause = manager._margin_floor_clause(30_004.9, 900)
    assert "at least 869.99 s" in clause


@pytest.mark.parametrize(
    ("winner_ms", "printed"),
    [(244_930, "655.07"), (30_000, "870.00"), (899_999, "0.00"), (950_000, "0.00")],
)
def test_the_printed_floor_is_not_pulled_down_by_float_noise(winner_ms, printed) -> None:
    assert f"at least {printed} s" in manager._margin_floor_clause(winner_ms, 900)
