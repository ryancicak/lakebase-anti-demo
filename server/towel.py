from __future__ import annotations

import math
from collections.abc import Mapping

from pydantic import BaseModel

from .models import (
    ComparisonKind,
    ComparisonSnapshot,
    LaneActivity,
    LaneSnapshot,
    LaneState,
    RoundFiveSetupLaneSnapshot,
    RoundFiveSetupState,
    RoundId,
)

_ACTIVE_STATES = {LaneState.CONNECTING, LaneState.VERIFYING}
_CAPABILITY_ROUNDS = {
    RoundId.PUT_MODEL_SCORE_IN_APP,
    RoundId.ANALYZE_LIVE_ORDERS,
}


class TowelAdjudication(BaseModel):
    """Pure presentation result for a towel cutoff; cleanup is intentionally external."""

    cutoff_ms: float | None
    lanes: dict[str, LaneSnapshot]
    censored_lower_bounds_ms: dict[str, float]
    comparison: ComparisonSnapshot
    retains_normal_result: bool = False
    public_result: str


def _cutoff_display(cutoff_ms: float) -> str:
    return f">{cutoff_ms / 1000:.2f}s"


def _censor_active_lane(lane: LaneSnapshot, cutoff_ms: float) -> LaneSnapshot:
    censored = lane.model_copy(deep=True)
    display = _cutoff_display(cutoff_ms)
    censored.state = LaneState.TOWELLED
    # A cutoff is a lower bound, never an exact completion observation.
    censored.elapsed_ms = None
    censored.status = f"Toweled · unfinished at cutoff · {display} lower bound"
    censored.error = None
    censored.verified_at = None
    censored.activity = LaneActivity(phase=LaneState.TOWELLED)
    censored.evidence = {
        "censored": True,
        "lower_bound_ms": cutoff_ms,
        "display_value": display,
    }
    return censored


def _finite_ms(value: float | None) -> float | None:
    if value is None or not math.isfinite(value) or value < 0:
        return None
    return value


def _setup_elapsed_ms(lane: RoundFiveSetupLaneSnapshot) -> float | None:
    return _finite_ms(lane.setup_elapsed_ms)


def adjudicate_round_five_towel(
    *,
    lanes: Mapping[str, LaneSnapshot],
    setup_lanes: Mapping[str, RoundFiveSetupLaneSnapshot],
    elapsed_at_cutoff_ms: Mapping[str, float] | None = None,
) -> TowelAdjudication:
    """Freeze only scored setup evidence already published from the shared T0.

    The manager does not own the setup orchestrator's private monotonic T0, so a
    Round 5 towel must never derive timing from the earlier, preflight-inclusive
    run start. Exact setup stops remain exact.

    ``setup_elapsed_ms`` is a latch that only advances when the orchestrator
    reports progress, and a lane can sit for minutes inside one silent phase --
    ``_wait_proxy_available`` polls without reporting at all. Publishing that
    latch as the lower bound of a towelled lane understates the floor by the
    whole silent interval: one recorded bout ran 230 s on a lane whose last
    published progress was 7.29 s, and the result screen said ">7.29s".

    ``elapsed_at_cutoff_ms`` is the caller's answer to that: the lane's elapsed
    time at the towel, carried forward from its own last published progress on
    the same T0 origin. It is only ever accepted when it is at least the latch,
    so this can raise a floor and never lower one, and a lane that published
    nothing still has no timing to state.
    """

    expected_lanes = {"lakebase", "competitor"}
    if set(lanes) != expected_lanes or set(setup_lanes) != expected_lanes:
        raise ValueError("Round 5 towel adjudication requires both setup lanes")
    if any(lane_id != lane.id for lane_id, lane in lanes.items()) or any(
        lane_id != lane.id for lane_id, lane in setup_lanes.items()
    ):
        raise ValueError("Round 5 towel lane keys must match lane IDs")

    at_cutoff = elapsed_at_cutoff_ms or {}
    frozen = {lane_id: lane.model_copy(deep=True) for lane_id, lane in lanes.items()}
    censored: dict[str, float] = {}
    exact_lanes: list[str] = []
    for lane_id, setup_lane in setup_lanes.items():
        lane = frozen[lane_id]
        elapsed_ms = _setup_elapsed_ms(setup_lane)
        if setup_lane.state == RoundFiveSetupState.VERIFIED and elapsed_ms is not None:
            exact_lanes.append(lane_id)
            lane.state = LaneState.VERIFIED
            lane.elapsed_ms = elapsed_ms
            lane.status = setup_lane.status
            lane.error = None
            lane.activity = LaneActivity(phase="setup_stop")
            continue

        # A lane that never published progress has no T0-anchored origin to
        # carry forward from, so it stays untimed rather than borrowing one.
        cutoff_elapsed_ms = _finite_ms(at_cutoff.get(lane_id))
        if elapsed_ms is not None and cutoff_elapsed_ms is not None:
            elapsed_ms = max(elapsed_ms, cutoff_elapsed_ms)

        lane.state = LaneState.TOWELLED
        lane.elapsed_ms = None
        lane.error = None
        lane.verified_at = None
        lane.activity = LaneActivity(phase=LaneState.TOWELLED)
        if elapsed_ms is None:
            lane.status = "Toweled · setup unfinished · no exact timing observed"
            lane.evidence = {"censored": True, "display_value": "NOT TIMED"}
            continue
        censored[lane_id] = elapsed_ms
        display = _cutoff_display(elapsed_ms)
        lane.status = f"Toweled · setup unfinished · {display} observed lower bound"
        lane.evidence = {
            "censored": True,
            "lower_bound_ms": elapsed_ms,
            "display_value": display,
        }

    if exact_lanes:
        exact = " and ".join(frozen[lane_id].name for lane_id in exact_lanes)
        public_result = (
            f"Toweled · Exact setup stop preserved for {exact} · "
            "Downstream proof incomplete · No winner · Margin N/A"
        )
    else:
        public_result = (
            "Toweled · No exact setup stop verified · "
            "Downstream proof incomplete · No winner · Margin N/A"
        )

    return TowelAdjudication(
        cutoff_ms=None,
        lanes=frozen,
        censored_lower_bounds_ms=censored,
        comparison=ComparisonSnapshot(
            kind=ComparisonKind.NOT_COMPARABLE,
            winner_lane_id=None,
            margin=None,
            detail=(
                "Round 5 stopped before the complete setup and downstream proof; "
                "no winner or speed margin was declared."
            ),
        ),
        public_result=public_result,
    )


def _normal_two_lane_comparison(
    comparison: ComparisonSnapshot | None,
    lane_ids: set[str],
) -> ComparisonSnapshot:
    if comparison is None:
        raise ValueError("both verified lanes require their valid normal comparison")
    if comparison.kind == ComparisonKind.MEASURED:
        if comparison.winner_lane_id not in lane_ids or comparison.margin is None:
            raise ValueError("measured normal comparison is incomplete")
    elif comparison.kind == ComparisonKind.TIE:
        if comparison.winner_lane_id is not None or comparison.margin is not None:
            raise ValueError("tie normal comparison is invalid")
    else:
        raise ValueError("both verified lanes require a measured or tie comparison")
    return comparison.model_copy(deep=True)


def adjudicate_towel(
    *,
    round_id: RoundId,
    lanes: Mapping[str, LaneSnapshot],
    cutoff_ms: float,
    normal_comparison: ComparisonSnapshot | None = None,
) -> TowelAdjudication:
    """Adjudicate one two-lane bout at an authoritative monotonic cutoff.

    The caller remains responsible for stopping work, cleaning owned artifacts,
    and releasing the ring. This function only freezes truthful public evidence.
    Its inputs are never mutated.
    """

    if not math.isfinite(cutoff_ms) or cutoff_ms < 0:
        raise ValueError("cutoff_ms must be finite and non-negative")
    if set(lanes) != {"lakebase", "competitor"}:
        raise ValueError("towel adjudication requires lakebase and competitor lanes")
    if any(lane_id != lane.id for lane_id, lane in lanes.items()):
        raise ValueError("towel lane keys must match lane IDs")

    frozen = {lane_id: lane.model_copy(deep=True) for lane_id, lane in lanes.items()}
    censored: dict[str, float] = {}
    for lane_id, lane in tuple(frozen.items()):
        if lane.state in _ACTIVE_STATES:
            frozen[lane_id] = _censor_active_lane(lane, cutoff_ms)
            censored[lane_id] = cutoff_ms
        elif lane.state == LaneState.NOT_SUPPORTED:
            # Unsupported is a capability N/A, never a censored timer.
            lane.elapsed_ms = None

    verified = [lane_id for lane_id, lane in lanes.items() if lane.state == LaneState.VERIFIED]
    cutoff = _cutoff_display(cutoff_ms)

    if len(verified) == 2:
        comparison = _normal_two_lane_comparison(normal_comparison, set(lanes))
        return TowelAdjudication(
            cutoff_ms=cutoff_ms,
            lanes=frozen,
            censored_lower_bounds_ms=censored,
            comparison=comparison,
            retains_normal_result=True,
            public_result=(
                "Toweled after both exact lane proofs verified · Normal result retained"
            ),
        )

    lakebase_verified_with_unsupported_aws = (
        round_id in _CAPABILITY_ROUNDS
        and lanes["lakebase"].state == LaneState.VERIFIED
        and lanes["competitor"].state == LaneState.NOT_SUPPORTED
    )
    if lakebase_verified_with_unsupported_aws:
        detail = (
            normal_comparison.detail
            if normal_comparison is not None
            and normal_comparison.kind == ComparisonKind.CAPABILITY_GAP
            and normal_comparison.winner_lane_id == "lakebase"
            and normal_comparison.margin is None
            else "Lakebase exact proof verified; the AWS capability lane remained N/A."
        )
        return TowelAdjudication(
            cutoff_ms=cutoff_ms,
            lanes=frozen,
            censored_lower_bounds_ms=censored,
            comparison=ComparisonSnapshot(
                kind=ComparisonKind.CAPABILITY_GAP,
                winner_lane_id="lakebase",
                margin=None,
                detail=detail,
            ),
            public_result=(
                "Toweled after Lakebase exact proof verified · "
                "AWS capability lane N/A · Lakebase wins the capability round"
            ),
        )

    if len(verified) == 1:
        winner = verified[0]
        opponent = "competitor" if winner == "lakebase" else "lakebase"
        opponent_result = (
            f"{opponent} unfinished and censored {cutoff}"
            if opponent in censored
            else f"{opponent} had no exact verified result"
        )
        return TowelAdjudication(
            cutoff_ms=cutoff_ms,
            lanes=frozen,
            censored_lower_bounds_ms=censored,
            comparison=ComparisonSnapshot(
                kind=ComparisonKind.ADJUDICATED_STOPPAGE,
                winner_lane_id=winner,
                margin=None,
                detail=(
                    f"{winner} exact proof preserved; {opponent_result}. "
                    "No speed margin was calculated."
                ),
            ),
            public_result=(
                f"Toweled · {winner} exact proof preserved · {opponent_result} · "
                "Adjudicated stoppage · Margin N/A"
            ),
        )

    return TowelAdjudication(
        cutoff_ms=cutoff_ms,
        lanes=frozen,
        censored_lower_bounds_ms=censored,
        comparison=ComparisonSnapshot(
            kind=ComparisonKind.NOT_COMPARABLE,
            winner_lane_id=None,
            margin=None,
            detail="No lane had an exact verified result at the towel cutoff.",
        ),
        public_result=(f"Toweled at {cutoff} · No exact result verified · No winner · Margin N/A"),
    )
