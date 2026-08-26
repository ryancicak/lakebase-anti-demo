from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest

from server.models import (
    ComparisonKind,
    ComparisonSnapshot,
    LaneSnapshot,
    LaneState,
    MetricValue,
    RoundFiveSetupLaneSnapshot,
    RoundFiveSetupState,
    RoundId,
    TowelSnapshot,
    TowelState,
)
from server.towel import adjudicate_round_five_towel, adjudicate_towel


def _lane(lane_id: str, state: LaneState, elapsed_ms: float | None = None) -> LaneSnapshot:
    return LaneSnapshot(
        id=lane_id,
        name="Lakebase" if lane_id == "lakebase" else "Opponent",
        state=state,
        elapsed_ms=elapsed_ms,
        status=state.value,
        evidence={"sealed_fact": lane_id},
    )


def test_round_five_towel_uses_only_shared_t0_setup_evidence() -> None:
    lanes = {
        "lakebase": _lane("lakebase", LaneState.CONNECTING),
        "competitor": _lane("competitor", LaneState.CONNECTING),
    }
    setup_lanes = {
        "lakebase": RoundFiveSetupLaneSnapshot(
            id="lakebase",
            name="Lakebase",
            state=RoundFiveSetupState.VERIFIED,
            setup_elapsed_ms=375.25,
            status="Built-in Lakebase pool verified",
        ),
        "competitor": RoundFiveSetupLaneSnapshot(
            id="competitor",
            name="RDS PostgreSQL + RDS Proxy",
            state=RoundFiveSetupState.RUNNING,
            setup_elapsed_ms=500.0,
            status="AWS is creating a new RDS Proxy",
        ),
    }
    original_lanes = deepcopy(lanes)
    original_setup = deepcopy(setup_lanes)

    result = adjudicate_round_five_towel(
        lanes=lanes,
        setup_lanes=setup_lanes,
        # The competitor kept running for 222.77 s after its last published
        # progress. An exact setup stop is never revised by a cutoff.
        elapsed_at_cutoff_ms={"lakebase": 999_999.0, "competitor": 223_270.0},
    )

    assert lanes == original_lanes
    assert setup_lanes == original_setup
    assert result.cutoff_ms is None
    assert result.censored_lower_bounds_ms == {"competitor": 223_270.0}
    assert result.lanes["lakebase"].state == LaneState.VERIFIED
    assert result.lanes["lakebase"].elapsed_ms == 375.25
    assert result.lanes["competitor"].state == LaneState.TOWELLED
    assert result.lanes["competitor"].elapsed_ms is None
    assert result.lanes["competitor"].evidence == {
        "censored": True,
        "lower_bound_ms": 223_270.0,
        "display_value": ">223.27s",
    }
    assert result.comparison.kind == ComparisonKind.NOT_COMPARABLE
    assert result.comparison.winner_lane_id is None
    assert result.comparison.margin is None
    assert "No winner · Margin N/A" in result.public_result


def test_round_five_towel_will_not_time_a_lane_that_never_published_progress() -> None:
    lanes = {
        "lakebase": _lane("lakebase", LaneState.CONNECTING),
        "competitor": _lane("competitor", LaneState.CONNECTING),
    }
    setup_lanes = {
        lane_id: RoundFiveSetupLaneSnapshot(
            id=lane_id,
            name=lanes[lane_id].name,
            state=RoundFiveSetupState.RUNNING,
            setup_elapsed_ms=None,
        )
        for lane_id in lanes
    }

    # No lane reported anything, so no lane has an origin to measure from, and a
    # cutoff figure must not become the first number either lane ever shows.
    result = adjudicate_round_five_towel(
        lanes=lanes,
        setup_lanes=setup_lanes,
        elapsed_at_cutoff_ms={"lakebase": 223_270.0, "competitor": 223_270.0},
    )

    assert result.censored_lower_bounds_ms == {}
    for lane in result.lanes.values():
        assert lane.state == LaneState.TOWELLED
        assert lane.elapsed_ms is None
        assert lane.evidence == {"censored": True, "display_value": "NOT TIMED"}


@pytest.mark.parametrize(
    (
        "round_id",
        "lakebase_state",
        "competitor_state",
        "normal_comparison",
        "expected_kind",
        "expected_winner",
        "expected_censored",
        "retains_normal",
    ),
    [
        (
            RoundId.WAKE_IDLE_APP,
            LaneState.CONNECTING,
            LaneState.VERIFYING,
            None,
            ComparisonKind.NOT_COMPARABLE,
            None,
            {"lakebase", "competitor"},
            False,
        ),
        (
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            LaneState.VERIFIED,
            LaneState.CONNECTING,
            None,
            ComparisonKind.ADJUDICATED_STOPPAGE,
            "lakebase",
            {"competitor"},
            False,
        ),
        (
            RoundId.RECOVER_DELETED_ORDER,
            LaneState.CONNECTING,
            LaneState.VERIFIED,
            None,
            ComparisonKind.ADJUDICATED_STOPPAGE,
            "competitor",
            {"lakebase"},
            False,
        ),
        (
            RoundId.PUT_MODEL_SCORE_IN_APP,
            LaneState.VERIFIED,
            LaneState.NOT_SUPPORTED,
            None,
            ComparisonKind.CAPABILITY_GAP,
            "lakebase",
            set(),
            False,
        ),
        (
            RoundId.SURVIVE_CONNECTION_SPIKE,
            LaneState.VERIFIED,
            LaneState.VERIFIED,
            ComparisonSnapshot(
                kind=ComparisonKind.MEASURED,
                winner_lane_id="lakebase",
                margin=MetricValue(spec_id="setup_elapsed_ms", value=1200, display_value="1.20 s"),
                detail="Existing verified setup result",
            ),
            ComparisonKind.MEASURED,
            "lakebase",
            set(),
            True,
        ),
        (
            RoundId.ANALYZE_LIVE_ORDERS,
            LaneState.VERIFYING,
            LaneState.NOT_SUPPORTED,
            None,
            ComparisonKind.NOT_COMPARABLE,
            None,
            {"lakebase"},
            False,
        ),
    ],
)
def test_universal_towel_adjudication_table(
    round_id: RoundId,
    lakebase_state: LaneState,
    competitor_state: LaneState,
    normal_comparison: ComparisonSnapshot | None,
    expected_kind: ComparisonKind,
    expected_winner: str | None,
    expected_censored: set[str],
    retains_normal: bool,
) -> None:
    lanes = {
        "lakebase": _lane("lakebase", lakebase_state, 1_250.0),
        "competitor": _lane("competitor", competitor_state, 2_500.0),
    }
    original = deepcopy(lanes)

    result = adjudicate_towel(
        round_id=round_id,
        lanes=lanes,
        cutoff_ms=10_000.0,
        normal_comparison=normal_comparison,
    )

    assert lanes == original
    assert result.comparison.kind == expected_kind
    assert result.comparison.winner_lane_id == expected_winner
    assert result.comparison.margin == (
        normal_comparison.margin if retains_normal and normal_comparison is not None else None
    )
    assert result.retains_normal_result is retains_normal
    assert set(result.censored_lower_bounds_ms) == expected_censored
    assert "Toweled" in result.public_result
    assert "Towelled" not in result.public_result
    for lane_id, lane in result.lanes.items():
        if lane_id in expected_censored:
            assert lane.state == LaneState.TOWELLED
            assert lane.elapsed_ms is None
            assert lane.evidence == {
                "censored": True,
                "lower_bound_ms": 10_000.0,
                "display_value": ">10.00s",
            }
        elif original[lane_id].state == LaneState.VERIFIED:
            assert lane == original[lane_id]
        elif original[lane_id].state == LaneState.NOT_SUPPORTED:
            assert lane.state == LaneState.NOT_SUPPORTED
            assert lane.elapsed_ms is None


def test_towel_snapshot_reads_legacy_round_three_cutoff() -> None:
    snapshot = TowelSnapshot.model_validate(
        {
            "state": TowelState.CLEANING,
            "requested_at": datetime.now(UTC),
            "active_lane": "competitor",
            "lower_bound_ms": 12_345.0,
            "lakebase_verified_ms": 900.0,
        }
    )

    assert snapshot.cutoff_ms == 12_345.0
    assert snapshot.censored_lower_bounds_ms == {}
