"""A Round 2 or 3 lane still running at its bound loses to the lane that finished.

rc13 (2026-10-03): AWS took 14.6 minutes to clone Aurora inside its own backup, and the bout
ended "could not verify" while the clone's writer was still starting. Ryan's rule since: let
the clock run to a bound, then the lane that finished wins and cleanup removes the rest. That
is Rounds 4 and 6's rule, so Rounds 2 and 3 now decide such a bout the same way: the finished
lane wins, the stopped lane's clock is a floor, and the margin is a lower bound. A lane that
errored measured nothing, so nobody wins over it, and a stuck Lakebase is never handed a win.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from server.bout_limit import BOUT_TIME_LIMIT_SECONDS
from server.manager import BoutOperator, RunManager
from server.models import (
    ComparisonKind,
    CompetitorId,
    CooldownState,
    Corner,
    LaneState,
    ResetMode,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.receipts import derive_receipt
from server.recovery import (
    RecoveryLaneResult,
    RecoveryPhase,
    RecoveryProgress,
    RecoveryRunResult,
)
from server.round4_race import COMPETITOR_LANE, LaneOutcome
from server.round4_race import LANE_TIMEOUT_SECONDS as ROUND4_LANE_TIMEOUT_SECONDS
from server.round4_race import resolve as resolve_round4
from server.round6_race import LANE_TIMEOUT_SECONDS as ROUND6_LANE_TIMEOUT_SECONDS
from server.safe_change import (
    SafeChangeLaneResult,
    SafeChangeLaneState,
    SafeChangeProvider,
    SafeChangeResetLaneResult,
    SafeChangeResetResult,
)
from server.safe_change_live import (
    DEFAULT_POLL_TIMEOUT_SECONDS,
    DEFAULT_RUN_TIMEOUT_SECONDS,
    build_safe_change_engine,
)
from tests.test_manager import (
    FakeModelScoreEngine,
    FakeSafeChangeEngine,
    verified_round_four,
    wait_for_cooldown,
    wait_for_state,
)
from tests.test_recovery import build_engine as build_recovery_engine
from tests.test_safe_change import make_engine as make_safe_change_engine
from tests.test_safe_change_live import (
    FakeAwsSession,
    FakeDatabricksRunner,
    RecordingConnector,
    no_sleep,
    owned_manifest,
)

FLOOR_MS = 720_004.0

# --- the engines mark a lane that ran out its own bound -----------------------------------------


async def test_round2_a_lane_still_running_at_its_bound_is_timed_out() -> None:
    engine, _, _, _ = make_safe_change_engine(competitor_delay=30.0)
    engine.run_timeout_seconds = 0.2
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm)

    assert result.lanes["lakebase"].state == SafeChangeLaneState.VERIFIED
    assert result.lanes["lakebase"].timed_out is False
    stopped = result.lanes["competitor"]
    assert stopped.state == SafeChangeLaneState.FAILED
    assert stopped.timed_out is True
    assert stopped.error is not None and stopped.error.startswith("Safe-change lane exceeded")
    assert stopped.elapsed_ms >= 200


async def test_round2_a_timeout_inside_a_lane_is_an_error_not_its_bound() -> None:
    engine, _, aurora, _ = make_safe_change_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    async def connect_times_out(plan, artifact):
        raise TimeoutError("connect timed out")

    aurora.connect_isolated = connect_times_out  # type: ignore[method-assign]
    result = await engine.run(arm)

    assert result.lanes["competitor"].timed_out is False
    assert result.lanes["competitor"].error == "connect timed out"


async def test_round3_a_lane_still_running_at_its_bound_is_timed_out() -> None:
    engine, _, aurora, _, _ = build_recovery_engine()
    engine.run_timeout_seconds = 0.2
    aurora.hold_create = asyncio.Event()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm)

    assert result.lanes["lakebase"].ok is True
    stopped = result.lanes["competitor"]
    assert stopped.ok is False
    assert stopped.timed_out is True
    assert stopped.error is not None and stopped.error.startswith("Recovery lane exceeded")


async def test_round3_a_timeout_inside_a_lane_is_an_error_not_its_bound() -> None:
    engine, _, aurora, _, _ = build_recovery_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    async def connect_times_out(plan, artifact):
        raise TimeoutError("connect timed out")

    aurora.connect_recovery = connect_times_out  # type: ignore[method-assign]
    result = await engine.run(arm)

    assert result.lanes["competitor"].timed_out is False
    assert result.lanes["competitor"].error == "connect timed out"


# --- Round 2's verdict ----------------------------------------------------------------------------


class BoundSafeChangeEngine(FakeSafeChangeEngine):
    """Each lane ends as ``outcomes`` says: ``verified``, ``timed_out`` or ``errored``."""

    def __init__(self, **outcomes: str) -> None:
        super().__init__()
        self.outcomes = {"lakebase": "verified", "competitor": "verified", **outcomes}

    async def run(self, arm, on_progress):
        result = await super().run(arm, on_progress)
        lanes = dict(result.lanes)
        for lane_id, outcome in self.outcomes.items():
            if outcome == "verified":
                continue
            plan = self.plans[lane_id]
            lanes[lane_id] = SafeChangeLaneResult(
                lane_id=lane_id,
                name=plan.name,
                provider=plan.provider,
                state=SafeChangeLaneState.FAILED,
                elapsed_ms=FLOOR_MS if outcome == "timed_out" else 6.0,
                first_action_ns=1,
                completed_ns=2,
                artifact_id=plan.artifact_id,
                error=(
                    "Safe-change lane exceeded 720 seconds"
                    if outcome == "timed_out"
                    else "isolated endpoint contract rejected"
                ),
                timed_out=outcome == "timed_out",
            )
        return replace(result, lanes=lanes)


async def finish_round2(engine: FakeSafeChangeEngine, state: SessionState):
    manager = RunManager(safe_change_factory=lambda: engine)
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    return manager, created.id, await wait_for_state(manager, created.id, state)


async def test_round2_the_lane_that_finished_wins_over_one_stopped_at_its_bound(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        manager, session_id, finished = await finish_round2(
            BoundSafeChangeEngine(competitor="timed_out"), SessionState.VERIFIED
        )

    comparison = finished.comparison
    assert comparison is not None
    assert comparison.kind == ComparisonKind.ADJUDICATED_STOPPAGE
    assert comparison.winner_lane_id == "lakebase"
    assert comparison.margin is None
    assert comparison.detail == (
        "Lakebase verified the isolated schema change; Aurora Serverless v2 was still "
        "running 720.00s after the bell, so the margin is a lower bound and not a measurement."
    )
    assert finished.remembered_result == "LAKEBASE WINS · MARGIN IS A LOWER BOUND"
    assert finished.failure is None
    assert finished.lanes["lakebase"].state == LaneState.VERIFIED
    stopped = finished.lanes["competitor"]
    assert stopped.state == LaneState.FAILED
    assert stopped.elapsed_ms == FLOOR_MS
    assert stopped.error is None
    assert stopped.evidence == {
        "censored": True,
        "lower_bound_ms": FLOOR_MS,
        "display_value": ">720.00s",
    }
    logged = [record.getMessage() for record in caplog.records]
    assert any(
        "still running at its bound" in line and "lane=competitor" in line for line in logged
    ), logged
    # The bout's own cleanup removes what the stopped lane left, as after any result.
    assert finished.cooldown is not None
    assert finished.cooldown.mode == ResetMode.DELETE_ISOLATED_ENVIRONMENT
    await wait_for_cooldown(manager, session_id, CooldownState.READY)


async def test_round2_a_stopped_lane_is_a_floor_on_the_receipt() -> None:
    _, _, finished = await finish_round2(
        BoundSafeChangeEngine(competitor="timed_out"), SessionState.VERIFIED
    )

    receipt = derive_receipt(finished, "run_finished")

    assert receipt.outcome == "declared"
    assert receipt.lakebase.lower_bound is False
    assert receipt.opponent_lane.lower_bound is True
    assert receipt.opponent_lane.ms == FLOOR_MS
    assert receipt.margin_ms is None
    assert receipt.remembered_result == "LAKEBASE WINS · MARGIN IS A LOWER BOUND"


async def test_round2_a_stuck_lakebase_lane_is_not_handed_a_win() -> None:
    _, _, finished = await finish_round2(
        BoundSafeChangeEngine(lakebase="timed_out"), SessionState.VERIFIED
    )

    assert finished.comparison is not None
    assert finished.comparison.winner_lane_id == "competitor"
    assert finished.remembered_result == (
        f"{finished.competitor.short_name.upper()} WINS · MARGIN IS A LOWER BOUND"
    )
    assert finished.lanes["lakebase"].evidence["lower_bound_ms"] == FLOOR_MS


@pytest.mark.parametrize(
    "outcomes",
    [
        {"competitor": "errored"},
        {"lakebase": "timed_out", "competitor": "timed_out"},
        {"lakebase": "errored", "competitor": "timed_out"},
    ],
    ids=["other-errored", "both-stopped", "errored-and-stopped"],
)
async def test_round2_without_one_finished_lane_over_a_stopped_one_the_bout_fails(
    outcomes,
) -> None:
    _, _, failed = await finish_round2(BoundSafeChangeEngine(**outcomes), SessionState.FAILED)

    assert failed.comparison is None
    assert failed.remembered_result is None
    assert failed.failure == "One or more isolated schema changes could not be verified."


# --- Round 3's verdict ----------------------------------------------------------------------------


class BoundRecoveryEngine:
    """Round 3 with each lane ending as ``outcomes`` says, and a cleanup that succeeds."""

    def __init__(self, **outcomes: str) -> None:
        self.outcomes = {"lakebase": "verified", "competitor": "verified", **outcomes}
        self.names = {"lakebase": "Lakebase", "competitor": "RDS PostgreSQL"}

    async def arm(self, competitor, on_progress):
        lanes = {}
        for lane_id, name in self.names.items():
            await on_progress(
                RecoveryProgress(
                    lane_id=lane_id,
                    lane_name=name,
                    phase=RecoveryPhase.PREPARING_INCIDENT,
                    status="Committing and aging the exact incident row",
                    occurred_at=datetime.now(UTC),
                )
            )
            lanes[lane_id] = SimpleNamespace(evidence={"exact_incident_committed": True})
        return SimpleNamespace(competitor=competitor, lanes=lanes)

    async def run(self, arm, on_progress, on_started, stop_control=None):
        await on_started()
        lanes = {}
        for lane_id, name in self.names.items():
            outcome = self.outcomes[lane_id]
            ok = outcome == "verified"
            elapsed = 40.0 if ok else FLOOR_MS if outcome == "timed_out" else 6.0
            error = (
                None
                if ok
                else "Recovery lane exceeded 1030 seconds"
                if outcome == "timed_out"
                else "recovery endpoint contract rejected"
            )
            await on_progress(
                RecoveryProgress(
                    lane_id=lane_id,
                    lane_name=name,
                    phase=RecoveryPhase.VERIFIED if ok else RecoveryPhase.FAILED,
                    status="Exact recovered order verified" if ok else "Not verified",
                    occurred_at=datetime.now(UTC),
                    elapsed_ms=elapsed,
                    error=error,
                )
            )
            lanes[lane_id] = RecoveryLaneResult(
                lane_id=lane_id,
                name=name,
                provider=(
                    SafeChangeProvider.LAKEBASE if lane_id == "lakebase" else SafeChangeProvider.RDS
                ),
                elapsed_ms=elapsed,
                first_action_ns=1,
                completed_ns=2,
                artifact_id=f"recovery-{lane_id}",
                ok=ok,
                error=error,
                timed_out=outcome == "timed_out",
            )
        return RecoveryRunResult(
            competitor=arm.competitor,
            started_ns=1,
            completed_ns=2,
            launch_skew_ms=0.01,
            contract_sha256="contract",
            lanes=lanes,
        )

    async def reset(self, competitor, on_progress):
        return SafeChangeResetResult(
            competitor=competitor,
            lanes={
                lane_id: SafeChangeResetLaneResult(
                    lane_id=lane_id,
                    name=name,
                    provider=(
                        SafeChangeProvider.LAKEBASE
                        if lane_id == "lakebase"
                        else SafeChangeProvider.RDS
                    ),
                    artifact_id=f"recovery-{lane_id}",
                    ok=True,
                )
                for lane_id, name in self.names.items()
            },
        )


async def finish_round3(engine: BoundRecoveryEngine, state: SessionState):
    manager = RunManager(recovery_factory=lambda: engine)
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="software_engineer",
            secondary_personas=[],
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.RECOVER_DELETED_ORDER,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    return manager, created.id, await wait_for_state(manager, created.id, state)


async def test_round3_the_lane_that_finished_wins_over_one_stopped_at_its_bound() -> None:
    manager, session_id, finished = await finish_round3(
        BoundRecoveryEngine(competitor="timed_out"), SessionState.VERIFIED
    )

    assert finished.comparison is not None
    assert finished.comparison.kind == ComparisonKind.ADJUDICATED_STOPPAGE
    assert finished.comparison.winner_lane_id == "lakebase"
    assert finished.remembered_result == "LAKEBASE WINS · MARGIN IS A LOWER BOUND"
    stopped = finished.lanes["competitor"]
    assert stopped.state == LaneState.FAILED
    assert stopped.error is None
    assert stopped.evidence["lower_bound_ms"] == FLOOR_MS
    assert finished.cooldown is not None
    assert finished.cooldown.mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT
    await wait_for_cooldown(manager, session_id, CooldownState.READY)


async def test_round3_a_lane_that_errored_still_fails_the_bout() -> None:
    _, _, failed = await finish_round3(
        BoundRecoveryEngine(lakebase="errored"), SessionState.FAILED
    )

    assert failed.comparison is None
    assert failed.failure == "One or more recovered orders could not be verified."


# --- Rounds 4 and 6 show the same floor ---------------------------------------------------------


class BoundModelScoreEngine(FakeModelScoreEngine):
    """Round 4 with the AWS lane still running at its 900 s bound."""

    async def run(self, arm, update, on_progress):
        result = await super().run(arm, update, on_progress)
        outcomes = dict(result.outcomes)
        outcomes[COMPETITOR_LANE] = LaneOutcome(
            lane_id=COMPETITOR_LANE,
            verified=False,
            elapsed_ms=None,
            last_negative_ms=899_750.0,
            reads=3600,
            max_read_gap_ms=250.0,
            failure="AWS Glue did not deliver the row within 900s of the bell",
            timed_out=True,
            bound_ms=900_000.0,
        )
        result = replace(result, outcomes=outcomes, resolution=resolve_round4(outcomes))
        self.initial_result = result
        return result


async def test_round4_a_lane_stopped_at_its_bound_shows_its_floor_not_an_error() -> None:
    # Ryan (2026-10-03): without the floor, "it looks like the app crapped out and that's
    # not the case".
    engine = BoundModelScoreEngine()
    manager = RunManager(model_score_factory=lambda _competitor: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-4")

    _, verified = await verified_round_four(manager, operator)

    assert verified.comparison is not None
    assert verified.comparison.kind == ComparisonKind.ADJUDICATED_STOPPAGE
    assert verified.comparison.winner_lane_id == "lakebase"
    assert verified.remembered_result == "LAKEBASE WINS · MARGIN IS A LOWER BOUND"
    stopped = verified.lanes["competitor"]
    assert stopped.state == LaneState.FAILED
    assert stopped.status == "Did not deliver the row within its bound"
    assert stopped.error is None
    assert stopped.elapsed_ms is None
    assert stopped.evidence["censored"] is True
    assert stopped.evidence["lower_bound_ms"] == 900_000.0
    assert stopped.evidence["display_value"] == ">900.00s"
    receipt = derive_receipt(verified, "run_finished")
    assert receipt.outcome == "declared"
    assert receipt.opponent_lane.lower_bound is True
    assert receipt.opponent_lane.ms == 900_000.0
    assert receipt.margin_ms is None


# --- the one maximum --------------------------------------------------------------------------


def test_every_racing_round_has_the_one_15_minute_maximum() -> None:
    # Ryan (2026-10-03): "or 900 seconds? and then whoever completes before that 900 seconds
    # is deemed the winner!"
    assert BOUT_TIME_LIMIT_SECONDS == 900.0
    assert ROUND4_LANE_TIMEOUT_SECONDS == BOUT_TIME_LIMIT_SECONDS
    assert ROUND6_LANE_TIMEOUT_SECONDS == BOUT_TIME_LIMIT_SECONDS
    # rc13's backup-delayed clone (14.6 minutes) is decided at the limit instead of the room
    # waiting it out; cleanup still outlasts it.
    assert 14.6 * 60 < DEFAULT_POLL_TIMEOUT_SECONDS < DEFAULT_RUN_TIMEOUT_SECONDS


@pytest.mark.parametrize("round_number", [2, 3])
def test_the_builder_gives_rounds_2_and_3_the_maximum(round_number: int) -> None:
    bound = BOUT_TIME_LIMIT_SECONDS
    engine = build_safe_change_engine(
        owned_manifest(),
        round_number=round_number,
        environment={},
        databricks_runner=FakeDatabricksRunner(),
        session_factory=lambda **_: FakeAwsSession(),
        connector=RecordingConnector(),
        sleep=no_sleep,
    )

    assert engine.run_timeout_seconds == bound
    # Cleanup keeps its own budget, which outlasts a clone AWS is still creating.
    assert engine.reset_timeout_seconds == DEFAULT_RUN_TIMEOUT_SECONDS
