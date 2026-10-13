from __future__ import annotations

import ast
import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from databricks.sdk.errors.platform import PermissionDenied
from fastapi.testclient import TestClient

from server import receipts as receipts_module
from server.api import router
from server.coordination import CoordinationObjectsMissingError
from server.manager import EventLog, RunManager
from server.models import (
    BoutOperator,
    ComparisonKind,
    ComparisonSnapshot,
    CompetitorId,
    Corner,
    LaneState,
    MetricValue,
    RoundId,
    SessionCreate,
    SessionSnapshot,
    SessionState,
    TowelSnapshot,
    TowelState,
)
from server.receipts import (
    BOUT_RECEIPT_TABLE,
    SEALING_EVENTS,
    SETTLED_TOWEL_STATES,
    TOWEL_SETTLING_EVENTS,
    DurableReceiptStore,
    artifact_root,
    derive_receipt,
    drain_receipt_writes,
    install_receipt_store,
    load_receipts,
    load_receipts_async,
    receipt_id,
    receipts_root,
    record_sealed_bout,
    write_receipt,
)
from server.targets import TargetConfigurationError
from server.verifier import NeutralVerifier, RetryPolicy
from tests.test_manager import (
    FailingArmTarget,
    FakeLiveTarget,
    FakeResolver,
    HangingArmResolver,
    drain_record_operations,
    wait_for_state,
)


def make_verifier() -> NeutralVerifier:
    return NeutralVerifier(
        RetryPolicy(
            overall_timeout_seconds=1,
            attempt_timeout_seconds=0.5,
            initial_delay_seconds=0.001,
            maximum_delay_seconds=0.001,
        )
    )


@pytest.fixture(autouse=True)
def no_durable_receipt_store():
    """The write hook is process-global, so it may not survive a test.

    A leaked store would point the next test's bouts at a fake cursor that has
    gone out of scope, and the symptom -- a warning, because a souvenir may never
    fail a bout -- is exactly the shape of failure this file is here to catch.
    """

    install_receipt_store(None)
    yield
    install_receipt_store(None)


def deployed_shaped_runtime(monkeypatch) -> None:
    """The exact filesystem a Databricks App container has: none of one.

    `ANTI_DEMO_ARTIFACT_ROOT` is unset (the suite's autouse fixture sets it) and
    no manifest state directory resolves, so `artifact_root()` correctly answers
    None -- which is what produced "there is nowhere to keep a bout receipt" on
    the deployed app, twice, on two different rounds.
    """

    monkeypatch.delenv("ANTI_DEMO_ARTIFACT_ROOT", raising=False)
    monkeypatch.setattr("server.receipts.state_dir_from_environ", lambda *_a, **_k: None)


class FakeCoordinationCursor:
    """Enough of the coordination database to hold receipts, and nothing more.

    Deliberately keyed the same way the real SQL is -- primary key
    (session_id, round_id, sealing_event), with a latest-state upsert for the
    cleanup overlay -- so a test that passes here is testing the store's rules
    rather than the fake's.
    """

    def __init__(self, *, table_present: bool = True, may_create: bool = True) -> None:
        self.schema_present = True
        self.table_present = table_present
        self.may_create = may_create
        self.statements: list[str] = []
        self.rows: dict[tuple[str, str, str], tuple] = {}
        self._pending: list[tuple] = []

    async def execute(self, sql: str, params: tuple = ()) -> None:
        statement = " ".join(sql.split())
        self.statements.append(statement)
        self._pending = []
        # The relation probe joins pg_namespace too, so it has to be matched
        # first -- a looser namespace test would answer it with schema names.
        if "pg_catalog.pg_class" in statement:
            self._pending = [("bout_receipt",)] if self.table_present else []
        elif "pg_catalog.pg_namespace WHERE nspname" in statement:
            self._pending = [("anti_demo_coordination",)] if self.schema_present else []
        elif statement.upper().startswith("CREATE"):
            if not self.may_create:
                raise psycopg.errors.InsufficientPrivilege(
                    "permission denied for schema anti_demo_coordination"
                )
            self.schema_present = True
            self.table_present = True
        elif "bout_receipt_cleanup_upsert_v1(" in statement:
            # The SECURITY DEFINER cleanup overlay (sql/round5_receipt_least_privilege.sql).
            # Same guard as the INSERT branch below -- never move a newer overlay, only
            # replace it when the diagnostic changed -- with the sealing event fixed to
            # 'cleanup_update' by the function rather than supplied by the caller.
            session_id, round_id, receipt, run_id, outcome, sealed_at, document = params
            key = (session_id, round_id, "cleanup_update")
            row = (
                session_id, round_id, "cleanup_update", receipt,
                run_id, outcome, sealed_at, document,
            )
            current = self.rows.get(key)
            if current is None:
                self.rows[key] = row
            else:
                current_failure = json.loads(current[7])["receipt"].get("cleanup_failure")
                new_failure = json.loads(document)["receipt"].get("cleanup_failure")
                if current[6] <= sealed_at and current_failure != new_failure:
                    self.rows[key] = row
        elif statement.startswith(f"INSERT INTO {BOUT_RECEIPT_TABLE}"):
            key = (params[0], params[1], params[2])
            current = self.rows.get(key)
            if current is None:
                self.rows[key] = params
            elif params[2] == "cleanup_update":
                current_failure = json.loads(current[7])["receipt"].get(
                    "cleanup_failure"
                )
                new_failure = json.loads(params[7])["receipt"].get("cleanup_failure")
                if current[6] <= params[6] and current_failure != new_failure:
                    self.rows[key] = params
        elif statement.startswith("WITH annotated_receipts AS"):
            best: dict[tuple[str, str, bool], tuple] = {}
            latest_event: dict[tuple[str, str, bool], str] = {}
            latest_non_cancelled_at: dict[tuple[str, str, bool], datetime] = {}
            for row in self.rows.values():
                is_cleanup = str(row[2]).startswith("cleanup_update")
                key = (row[0], row[1], is_cleanup)
                current = max(
                    (
                        candidate
                        for candidate in self.rows.values()
                        if (
                            candidate[0],
                            candidate[1],
                            str(candidate[2]).startswith("cleanup_update"),
                        )
                        == key
                    ),
                    key=lambda candidate: candidate[6],
                )
                latest_event[key] = str(current[2])
                non_cancelled = [
                    candidate
                    for candidate in self.rows.values()
                    if (
                        candidate[0],
                        candidate[1],
                        str(candidate[2]).startswith("cleanup_update"),
                    )
                    == key
                    and not str(candidate[2]).startswith("session_cancelled")
                ]
                if non_cancelled:
                    latest_non_cancelled_at[key] = max(
                        candidate[6] for candidate in non_cancelled
                    )
            for row in self.rows.values():
                is_cleanup = str(row[2]).startswith("cleanup_update")
                key = (row[0], row[1], is_cleanup)
                rank = (
                    row[6] if is_cleanup else row[5] == "declared",
                    (
                        False
                        if is_cleanup
                        else latest_event[key].startswith("session_cancelled")
                        and str(row[2]).startswith("run_finished")
                        and row[6] == latest_non_cancelled_at[key]
                    ),
                    row[6],
                )
                current = best.get(key)
                current_rank = (
                    (
                        current[6]
                        if is_cleanup
                        else current[5] == "declared"
                    ),
                    (
                        False
                        if is_cleanup
                        else latest_event[key].startswith("session_cancelled")
                        and str(current[2]).startswith("run_finished")
                        and current[6] == latest_non_cancelled_at[key]
                    ),
                    current[6],
                ) if current is not None else None
                if current_rank is None or rank > current_rank:
                    best[key] = row
            selected = list(best.values())
            selected.sort(key=lambda row: (row[0], row[1], row[6]))
            self._pending = [(json.loads(row[7]),) for row in selected]
        else:  # pragma: no cover - the store issues nothing else
            raise AssertionError(f"unexpected statement: {statement}")

    async def fetchone(self) -> tuple | None:
        return self._pending[0] if self._pending else None

    async def fetchall(self) -> list[tuple]:
        return list(self._pending)


def durable_store(cursor: FakeCoordinationCursor) -> DurableReceiptStore:
    async def run(operation):
        return await operation(cursor)

    return DurableReceiptStore(run)


async def verified_round_one_snapshot() -> SessionSnapshot:
    """A real terminal snapshot, produced by the in-memory harness."""
    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    snapshot = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    await manager.close()
    return snapshot


# ---------------------------------------------------------------- derivation


async def test_receipt_id_matches_the_frontend_convention() -> None:
    assert receipt_id("8c4a0ca3deadbeefcafe0123456789ab") == "8C4A0CA3"


async def test_derives_a_declared_bout_with_a_margin_from_both_lanes() -> None:
    snapshot = await verified_round_one_snapshot()

    receipt = derive_receipt(snapshot, "run_finished")

    assert receipt.outcome == "declared"
    assert receipt.metric == "bout_elapsed_ms"
    assert receipt.lakebase.state == "verified"
    assert receipt.opponent_lane.state == "verified"
    assert receipt.has_measurements is True
    assert receipt.receipt == snapshot.id[:8].upper()
    assert receipt.round_id == RoundId.WAKE_IDLE_APP
    # Both lanes verified, so a margin is legitimate and must equal the gap.
    assert receipt.margin_ms == pytest.approx(
        snapshot.lanes["competitor"].elapsed_ms - snapshot.lanes["lakebase"].elapsed_ms
    )


async def test_a_failed_opponent_lane_never_becomes_a_margin() -> None:
    """The Round 2 and Round 3 shape: our lane verified, theirs did not.

    The opponent's elapsed value is a lower bound -- where the clock stood when the
    bout stopped -- so subtracting it would manufacture a verdict the orchestrator
    explicitly refused to declare by leaving remembered_result null.
    """
    snapshot = await verified_round_one_snapshot()
    snapshot.state = SessionState.FAILED
    snapshot.remembered_result = None
    snapshot.comparison = None
    snapshot.failure = "One or more recovered orders could not be verified."
    snapshot.lanes["competitor"].state = LaneState.FAILED
    snapshot.lanes["competitor"].elapsed_ms = 845602.452125
    snapshot.lanes["competitor"].status = "Could not verify the recovered order"

    receipt = derive_receipt(snapshot, "run_finished")

    assert receipt.outcome == "stopped_short"
    assert receipt.opponent_lane.state == "failed"
    assert receipt.opponent_lane.ms == pytest.approx(845602.452125)
    assert receipt.margin_ms is None
    assert receipt.remembered_result is None
    assert receipt.failure == "One or more recovered orders could not be verified."


async def test_a_capability_gap_records_no_margin_and_no_shared_start() -> None:
    """The Round 4 and Round 6 shape: the AWS lane was never launched.

    A null start skew is meaningful, not missing -- there was no simultaneous start
    to measure. Recording 0.0 would claim a fairness property that never happened.
    """
    snapshot = await verified_round_one_snapshot()
    snapshot.lanes["competitor"].state = LaneState.NOT_SUPPORTED
    snapshot.lanes["competitor"].elapsed_ms = None
    snapshot.lanes["competitor"].status = "AWS lane not timed for this Managed Sync proof"
    snapshot.fairness.launch_skew_ms = None
    snapshot.comparison = ComparisonSnapshot(
        kind=ComparisonKind.CAPABILITY_GAP,
        winner_lane_id="lakebase",
        detail="LAKEBASE CAPABILITY WIN",
    )

    receipt = derive_receipt(snapshot, "run_finished")

    assert receipt.outcome == "declared"
    assert receipt.opponent_lane.state == "not_supported"
    assert receipt.opponent_lane.ms is None
    assert receipt.opponent_lane.reason == "AWS lane not timed for this Managed Sync proof"
    assert receipt.margin_ms is None
    assert receipt.start_skew_ms is None
    assert receipt.has_measurements is True


async def test_a_declared_tie_records_no_margin_from_the_two_clocks() -> None:
    """The Round 4 and Round 6 shape: both lanes verified, within the verifiers' resolution.

    The two clocks differ, but by less than the verifiers can show, so the server declares
    a tie and gives no margin. Subtracting the clocks anyway made the receipt read the
    tie as a win for the lower one.
    """

    snapshot = await verified_round_one_snapshot()
    snapshot.comparison = ComparisonSnapshot(
        kind=ComparisonKind.TIE,
        detail="Both orders arrived within the verifiers' measurement resolution.",
    )
    snapshot.remembered_result = "TIE · WITHIN MEASUREMENT RESOLUTION"

    receipt = derive_receipt(snapshot, "run_finished")

    assert receipt.outcome == "declared"
    assert receipt.lakebase.state == receipt.opponent_lane.state == "verified"
    assert receipt.margin_ms is None
    assert receipt.remembered_result == "TIE · WITHIN MEASUREMENT RESOLUTION"


async def test_prefers_the_orchestrators_own_margin_over_arithmetic() -> None:
    """Round 5 is judged on setup, and its margin comes from the comparison."""
    snapshot = await verified_round_one_snapshot()
    snapshot.comparison = ComparisonSnapshot(
        kind=ComparisonKind.MEASURED,
        winner_lane_id="lakebase",
        margin=MetricValue(
            spec_id="setup_elapsed_ms",
            lane_id="lakebase",
            value=615178.707459,
            display_value="615178.71 ms",
        ),
        detail="Lakebase wins primary setup by 615178.71 ms",
    )

    receipt = derive_receipt(snapshot, "run_finished")

    assert receipt.margin_ms == pytest.approx(615178.707459)


async def test_a_thrown_towel_records_its_censored_lower_bounds() -> None:
    """A towel moves both elapsed values into the towel snapshot as floors.

    Reading only lane.elapsed_ms would report has_measurements=False and drop the
    bout off the scoreboard, silently discarding an honest non-completion.
    """
    snapshot = await verified_round_one_snapshot()
    snapshot.state = SessionState.TOWELLED
    snapshot.remembered_result = None
    snapshot.comparison = None
    for lane in snapshot.lanes.values():
        lane.state = LaneState.TOWELLED
        lane.elapsed_ms = None
    snapshot.towel = TowelSnapshot(
        state=TowelState.READY,
        requested_at=snapshot.updated_at,
        cutoff_ms=2140.0,
        censored_lower_bounds_ms={"lakebase": 2110.0, "competitor": 2140.0},
        public_result="STOPPED BEFORE EITHER LANE VERIFIED",
    )

    receipt = derive_receipt(snapshot, "towel_finished")

    assert receipt.outcome == "stopped_short"
    assert receipt.has_measurements is True
    assert receipt.lakebase.ms == pytest.approx(2110.0)
    assert receipt.lakebase.lower_bound is True
    assert receipt.opponent_lane.ms == pytest.approx(2140.0)
    assert receipt.opponent_lane.lower_bound is True
    # Two floors 30 ms apart are not a 30 ms margin.
    assert receipt.margin_ms is None


async def test_v4_bell_towel_receipt_reads_the_runtime_not_the_setup_stop() -> None:
    """A durable V4 towel receipt must carry the bell_to_10000 evidence and its
    reached-but-not-verified distinction, never the promoted ~6ms setup stop."""
    from server.models import (
        RoundFiveRuntimeLaneSnapshot,
        RoundFiveRuntimeSnapshot,
    )

    snapshot = await verified_round_one_snapshot()
    snapshot.round = snapshot.round.model_copy(
        update={
            "id": RoundId.SURVIVE_CONNECTION_SPIKE,
            "title": "Ready a pooled application path",
        }
    )
    snapshot.state = SessionState.TOWELLED
    snapshot.comparison = None
    snapshot.remembered_result = None
    # The poison: the generic lakebase lane was promoted VERIFIED at ~6ms setup.
    snapshot.lanes["lakebase"].state = LaneState.TOWELLED
    snapshot.lanes["lakebase"].elapsed_ms = None
    snapshot.lanes["competitor"].state = LaneState.TOWELLED
    snapshot.lanes["competitor"].elapsed_ms = None
    snapshot.round5_runtime = RoundFiveRuntimeSnapshot(
        protocol="round5-bell-to-10k-v4",
        warm_generation=1,
        bell_id="bell-one",
        revision=14,
        state="towelled",
        bell_at_utc=snapshot.updated_at,
        lanes={
            "lakebase": RoundFiveRuntimeLaneSnapshot(
                id="lakebase",
                phase="cancelled",
                elapsed_at_snapshot_ms=14_150.0,
                bell_to_10000_observed_ms=14_150.0,
                clients_initiated=10_000,
                clients_authenticated=10_000,
                held_clients=10_000,
                peak_clients_authenticated=10_000,
                peak_held_clients=10_000,
                sampled_queries_succeeded=41,
                status="Towel thrown during the 30-second hold",
            ),
            "competitor": RoundFiveRuntimeLaneSnapshot(
                id="competitor",
                phase="cancelled",
                elapsed_at_snapshot_ms=23_890.0,
                bell_to_10000_observed_ms=None,
                clients_initiated=8_400,
                clients_authenticated=8_400,
                held_clients=8_400,
                peak_clients_authenticated=8_400,
                peak_held_clients=8_400,
                sampled_queries_succeeded=0,
                status="Towel thrown before reaching 10,000",
            ),
        },
    )
    snapshot.towel = TowelSnapshot(
        state=TowelState.CLEANING,
        requested_at=snapshot.updated_at,
        censored_lower_bounds_ms={"competitor": 23_890.0},
        public_result="Toweled · No exact verified result · No declared winner",
    )

    receipt = derive_receipt(snapshot, "towel_finished")

    assert receipt.metric == "bell_to_10000_observed_ms"
    # Lakebase reached 10,000 -> its exact bell time is evidence, but a floor
    # (lower bound), never a verified time, and never the 6ms setup stop.
    assert receipt.lakebase.ms == pytest.approx(14_150.0)
    assert receipt.lakebase.state != "verified"
    assert receipt.lakebase.lower_bound is True
    # Aurora never reached 10,000 -> censored lower bound.
    assert receipt.opponent_lane.ms == pytest.approx(23_890.0)
    assert receipt.opponent_lane.lower_bound is True
    assert receipt.margin_ms is None
    # The receipt round-trips through its own schema unchanged.
    from server.receipts import BoutReceipt

    assert BoutReceipt.model_validate(receipt.model_dump(mode="json")) == receipt


async def test_stopped_short_bout_with_both_lanes_verified_records_no_margin() -> None:
    """Replays the live 2026-09-17 bout eb2b79b7173544fc92ff3a0da2ecec9f: both
    runtime lanes reached the exact 10,000-client gate and both session lanes are
    ``VERIFIED``, yet the orchestrator declared no comparison (a setup-phase gate
    failed) so the session is ``FAILED``/stopped_short. The receipt must not
    subtract the two verified lane times into a margin the bout never declared."""
    from server.models import (
        RoundFiveRuntimeLaneSnapshot,
        RoundFiveRuntimeSnapshot,
    )

    snapshot = await verified_round_one_snapshot()
    snapshot.round = snapshot.round.model_copy(
        update={
            "id": RoundId.SURVIVE_CONNECTION_SPIKE,
            "title": "Ready a pooled application path",
        }
    )
    snapshot.state = SessionState.FAILED
    snapshot.failure = "Round 5 contract gate failed; no comparison was declared."
    snapshot.comparison = None
    snapshot.remembered_result = None
    # Both lanes verified their own load/witness/cleanup gates.
    snapshot.lanes["lakebase"].state = LaneState.VERIFIED
    snapshot.lanes["competitor"].state = LaneState.VERIFIED
    snapshot.round5_runtime = RoundFiveRuntimeSnapshot(
        protocol="round5-bell-to-10k-v4",
        warm_generation=1,
        bell_id="bell-4ad57f21842b425aa58eff5be9220f66",
        revision=31,
        state="failed",
        bell_at_utc=snapshot.updated_at,
        lanes={
            "lakebase": RoundFiveRuntimeLaneSnapshot(
                id="lakebase",
                phase="verified",
                elapsed_at_snapshot_ms=13_740.241493,
                bell_to_10000_observed_ms=13_740.241493,
                clients_initiated=10_000,
                clients_authenticated=10_000,
                held_clients=10_000,
                peak_clients_authenticated=10_000,
                peak_held_clients=10_000,
                sampled_queries_succeeded=64,
                status="Exact 10,000-client retained gate verified",
            ),
            "competitor": RoundFiveRuntimeLaneSnapshot(
                id="competitor",
                phase="verified",
                elapsed_at_snapshot_ms=642_472.948717,
                bell_to_10000_observed_ms=642_472.948717,
                clients_initiated=10_000,
                clients_authenticated=10_000,
                held_clients=10_000,
                peak_clients_authenticated=10_000,
                peak_held_clients=10_000,
                sampled_queries_succeeded=64,
                status="Exact 10,000-client retained gate verified",
            ),
        },
    )

    receipt = derive_receipt(snapshot, "session_failed")

    assert receipt.outcome == "stopped_short"
    assert receipt.margin_ms is None


async def test_a_verified_lane_is_never_flagged_as_a_lower_bound() -> None:
    snapshot = await verified_round_one_snapshot()

    receipt = derive_receipt(snapshot, "run_finished")

    assert receipt.lakebase.lower_bound is False
    assert receipt.opponent_lane.lower_bound is False


async def test_an_attempt_with_no_timings_is_flagged_not_dropped() -> None:
    """An arm failure leaves no measurement but its evidence is still worth keeping."""
    snapshot = await verified_round_one_snapshot()
    snapshot.state = SessionState.FAILED
    snapshot.failure = "The sealed start state could not be verified."
    for lane in snapshot.lanes.values():
        lane.state = LaneState.FAILED
        lane.elapsed_ms = None

    receipt = derive_receipt(snapshot, "session_failed")

    assert receipt.has_measurements is False
    assert receipt.outcome == "stopped_short"
    assert receipt.sealing_event == "session_failed"


# ---------------------------------------------------------------- writing


async def test_writes_one_atomic_file_per_bout(tmp_path: Path) -> None:
    snapshot = await verified_round_one_snapshot()
    receipt = derive_receipt(snapshot, "run_finished")

    target = write_receipt(receipt, snapshot, tmp_path)

    day = snapshot.updated_at.astimezone(UTC).date().isoformat()
    assert target == tmp_path / day / f"{receipt.receipt}-{receipt.round_id.value}.json"
    assert target.exists()
    # No .tmp survives a successful write.
    assert list(target.parent.glob("*.tmp")) == []
    assert oct(target.stat().st_mode)[-3:] == "600"
    assert oct(target.parent.stat().st_mode)[-3:] == "700"

    document = json.loads(target.read_text())
    # The derived view for cheap rendering, plus the raw snapshot so nothing is
    # lost if the derivation later turns out to be wrong.
    assert document["receipt"]["receipt"] == receipt.receipt
    assert document["snapshot"]["id"] == snapshot.id
    assert document["snapshot"]["round"]["id"] == RoundId.WAKE_IDLE_APP.value


async def test_a_later_environment_failure_does_not_erase_a_declared_result(
    tmp_path: Path,
) -> None:
    """A cleanup that will not verify is not evidence the transaction did not."""
    snapshot = await verified_round_one_snapshot()
    declared = derive_receipt(snapshot, "run_finished")
    target = write_receipt(declared, snapshot, tmp_path)

    later = snapshot.model_copy(deep=True)
    later.state = SessionState.FAILED
    later.remembered_result = None
    later.failure = "Automatic cleanup verification is in progress"
    for lane in later.lanes.values():
        lane.state = LaneState.FAILED

    kept = write_receipt(derive_receipt(later, "session_failed"), later, tmp_path)

    assert kept == target
    document = json.loads(target.read_text())
    assert document["receipt"]["outcome"] == "declared"
    assert document["receipt"]["remembered_result"] == declared.remembered_result

    # A second declared write still lands, so the guard is narrow.
    redeclared = write_receipt(declared, snapshot, tmp_path)
    assert json.loads(redeclared.read_text())["receipt"]["outcome"] == "declared"


async def test_a_later_cancellation_cannot_replace_a_run_receipt_on_disk(
    tmp_path: Path,
) -> None:
    """An accepted bell is stronger evidence than cancel-arm."""

    snapshot = await verified_round_one_snapshot()
    snapshot.state = SessionState.FAILED
    snapshot.remembered_result = None
    snapshot.comparison = None
    snapshot.failure = "The accepted run stopped short."
    snapshot.lanes["competitor"].state = LaneState.FAILED
    run = derive_receipt(snapshot, "run_finished")
    assert run.outcome == "stopped_short"
    target = write_receipt(run, snapshot, tmp_path)

    cancelled = snapshot.model_copy(deep=True)
    cancelled.run_started_at = None
    cancelled.updated_at += timedelta(seconds=1)
    cancelled.failure = "Fight card cancelled by its owner before the bell."
    cancellation = derive_receipt(cancelled, "session_cancelled")
    write_receipt(cancellation, cancelled, tmp_path)

    document = json.loads(target.read_text())
    assert document["receipt"]["sealing_event"] == "run_finished"
    assert document["receipt"]["failure"] == run.failure


async def test_a_later_cancellation_cannot_replace_a_durable_run_receipt() -> None:
    """The durable reader applies the same run-over-cancel authority as disk."""

    snapshot = await verified_round_one_snapshot()
    snapshot.state = SessionState.FAILED
    snapshot.remembered_result = None
    snapshot.comparison = None
    snapshot.failure = "The accepted run stopped short."
    snapshot.lanes["competitor"].state = LaneState.FAILED
    run = derive_receipt(snapshot, "run_finished")
    assert run.outcome == "stopped_short"

    cancelled = snapshot.model_copy(deep=True)
    cancelled.run_started_at = None
    cancelled.updated_at += timedelta(seconds=1)
    cancelled.failure = "Fight card cancelled by its owner before the bell."
    cancellation = derive_receipt(cancelled, "session_cancelled")

    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    await store.append(run, snapshot)
    await store.append(cancellation, cancelled)

    found = await store.load()
    assert [item.sealing_event for item in found] == ["run_finished"]
    assert found[0].failure == run.failure


async def test_a_newer_accepted_failure_replaces_an_older_stopped_run_in_both_stores(
    tmp_path: Path,
) -> None:
    """Run-over-cancel precedence must not hide a later accepted bout."""

    snapshot = await verified_round_one_snapshot()
    snapshot.state = SessionState.FAILED
    snapshot.remembered_result = None
    snapshot.comparison = None
    snapshot.failure = "The first accepted run stopped short."
    snapshot.lanes["competitor"].state = LaneState.FAILED
    run = derive_receipt(snapshot, "run_finished")

    later = snapshot.model_copy(deep=True)
    later.receipt_bout_id = "later-accepted-bout"
    later.updated_at += timedelta(seconds=1)
    later.failure = "The later accepted run failed."
    failure = derive_receipt(later, "session_failed")

    target = write_receipt(run, snapshot, tmp_path)
    write_receipt(failure, later, tmp_path)
    assert json.loads(target.read_text())["receipt"]["sealing_event"] == "session_failed"

    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    await store.append(run, snapshot)
    await store.append(failure, later)
    found = await store.load()
    assert [item.sealing_event for item in found] == ["session_failed"]
    assert found[0].failure == failure.failure

    cancelled = later.model_copy(deep=True)
    cancelled.receipt_bout_id = "later-no-bell-cancel"
    cancelled.run_started_at = None
    cancelled.updated_at += timedelta(seconds=1)
    cancelled.failure = "The third bout was cancelled before the bell."
    cancellation = derive_receipt(cancelled, "session_cancelled")
    write_receipt(cancellation, cancelled, tmp_path)
    await store.append(cancellation, cancelled)

    assert json.loads(target.read_text())["receipt"]["sealing_event"] == "session_cancelled"
    found = await store.load()
    assert [item.sealing_event for item in found] == ["session_cancelled"]
    assert found[0].failure == cancellation.failure


async def test_a_late_older_file_write_cannot_clobber_a_newer_attempt(
    tmp_path: Path,
) -> None:
    snapshot = await verified_round_one_snapshot()
    snapshot.state = SessionState.FAILED
    snapshot.run_started_at = None
    snapshot.failure = "First attempt cancelled."
    older = derive_receipt(snapshot, "session_cancelled")

    newer_snapshot = snapshot.model_copy(deep=True)
    newer_snapshot.receipt_bout_id = "newer-cancelled-attempt"
    newer_snapshot.updated_at += timedelta(seconds=1)
    newer_snapshot.failure = "Second attempt cancelled."
    newer = derive_receipt(newer_snapshot, "session_cancelled")

    target = write_receipt(newer, newer_snapshot, tmp_path)
    write_receipt(older, snapshot, tmp_path)
    stored = json.loads(target.read_text())["receipt"]
    assert datetime.fromisoformat(stored["sealed_at"]) == newer.sealed_at
    assert stored["failure"] == newer.failure


async def test_a_write_failure_cannot_fail_a_bout(tmp_path: Path, caplog) -> None:
    """The load-bearing guarantee: a souvenir must never take a bout down with it."""
    snapshot = await verified_round_one_snapshot()
    payload = {"state": snapshot.state, "session": snapshot.model_dump(mode="json")}

    # A file where the date directory needs to be makes mkdir raise.
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    day = snapshot.updated_at.astimezone(UTC).date().isoformat()
    (blocked / day).write_text("not a directory")

    assert record_sealed_bout("run_finished", payload, blocked) is None
    assert "Could not write a bout receipt" in caplog.text

    # And the direct writer does raise, so the swallowing is provably in record_*.
    with pytest.raises(OSError):
        write_receipt(derive_receipt(snapshot, "run_finished"), snapshot, blocked)


async def test_a_malformed_payload_is_ignored_rather_than_raising() -> None:
    assert record_sealed_bout("run_finished", {}) is None
    assert record_sealed_bout("run_finished", {"session": "not a dict"}) is None
    assert record_sealed_bout("run_finished", {"session": {"nonsense": True}}) is None


async def test_the_sealing_event_set_is_deliberate() -> None:
    """Pinned so adding a round path with a new terminal event is a conscious choice.

    Every one of these is a publish site that assigns a terminal SessionState. The
    only such site left out is `towel_started`, because `towel_finished` follows it
    with the settled truth.

    A towel is the exception to the event-name rule, and the second set is why.
    `towel_finished` is published from exactly one site and only when cleanup
    succeeded on a round that is not Round 5; every other way a towel can end --
    a cleanup that failed, a cleanup that was cancelled, a lost cleanup lease,
    and every Round 5 towel including the ones that worked -- arrives as
    `towel_update` or `cleanup_update` instead. Those two cannot simply join the
    set above because they are also published while the towel is still moving,
    so they seal on the towel's settled state rather than on their name.
    """
    assert SEALING_EVENTS == {
        "run_finished",
        "towel_finished",
        "session_failed",
        "session_cancelled",
        "redo_finished",
        "redo_failed",
    }
    assert TOWEL_SETTLING_EVENTS == {"towel_update", "cleanup_update"}
    assert SETTLED_TOWEL_STATES == {TowelState.READY, TowelState.FAILED}


async def test_only_terminal_events_write_receipts(tmp_path: Path) -> None:
    snapshot = await verified_round_one_snapshot()
    payload = {"session": snapshot.model_dump(mode="json")}
    store = tmp_path / "store"
    day = snapshot.updated_at.astimezone(UTC).date().isoformat()
    expected = store / day / f"{receipt_id(snapshot.id)}-{RoundId.WAKE_IDLE_APP.value}.json"

    for event in ("run_started", "lane_update", "armed", "cooldown_ready", "towel_update"):
        assert record_sealed_bout(event, payload, store) is None
    assert not expected.exists()

    def towel_payload(state: TowelState) -> dict:
        towelled = snapshot.model_copy(deep=True)
        towelled.state = SessionState.TOWELLED
        towelled.towel = TowelSnapshot(state=state, requested_at=snapshot.updated_at)
        return {"session": towelled.model_dump(mode="json")}

    # A towel still cleaning is not a settled towel, on either event its
    # settlement can arrive on. Sealing here would be worse than not sealing: the
    # durable store is keyed on (session, round, sealing_event) and inserts
    # ON CONFLICT DO NOTHING, so a receipt written mid-cleanup would displace the
    # settled truth that follows it.
    for event in sorted(TOWEL_SETTLING_EVENTS):
        assert record_sealed_bout(event, towel_payload(TowelState.CLEANING), store) is None
    assert not expected.exists()

    # Settled, and each one seals. `cleanup_update` is the only event a Round 5
    # towel ever reaches, and `towel_update` is the only one a towel whose
    # cleanup failed ever reaches.
    for event in sorted(TOWEL_SETTLING_EVENTS):
        for state in sorted(SETTLED_TOWEL_STATES):
            assert record_sealed_bout(event, towel_payload(state), store) is not None
    assert expected.exists()

    for event in sorted(SEALING_EVENTS):
        assert record_sealed_bout(event, payload, store) is not None
    assert expected.exists()


# ---------------------------------------------------------------- reading


async def test_loads_receipts_oldest_first_and_filters_by_date(tmp_path: Path) -> None:
    snapshot = await verified_round_one_snapshot()
    base = snapshot.updated_at

    written = []
    for index, (offset, round_id) in enumerate(
        [
            (timedelta(days=-2), RoundId.WAKE_IDLE_APP),
            (timedelta(days=-1), RoundId.RECOVER_DELETED_ORDER),
            (timedelta(0), RoundId.PUT_MODEL_SCORE_IN_APP),
        ]
    ):
        moment = snapshot.model_copy(deep=True)
        moment.id = f"{index:08x}" + snapshot.id[8:]
        moment.updated_at = base + offset
        moment.round.id = round_id
        receipt = derive_receipt(moment, "run_finished")
        write_receipt(receipt, moment, tmp_path)
        written.append(receipt)

    everything = load_receipts(None, tmp_path)
    assert [item.receipt for item in everything] == [item.receipt for item in written]
    assert [item.sealed_at for item in everything] == sorted(
        item.sealed_at for item in everything
    )

    cutoff = (base - timedelta(days=1)).astimezone(UTC).date()
    recent = load_receipts(cutoff, tmp_path)
    assert [item.receipt for item in recent] == [item.receipt for item in written[1:]]


async def test_missing_root_reads_as_empty_not_an_error(tmp_path: Path) -> None:
    assert load_receipts(None, tmp_path / "never-created") == []


async def test_one_corrupt_receipt_does_not_hide_the_others(tmp_path: Path) -> None:
    snapshot = await verified_round_one_snapshot()
    receipt = derive_receipt(snapshot, "run_finished")
    write_receipt(receipt, snapshot, tmp_path)
    day = snapshot.updated_at.astimezone(UTC).date().isoformat()
    (tmp_path / day / "AAAAAAAA-wake_idle_app.json").write_text("{ truncated")

    found = load_receipts(None, tmp_path)

    assert [item.receipt for item in found] == [receipt.receipt]


# ---------------------------------------------------------------- end to end


async def test_a_real_bout_leaves_a_receipt_on_disk(tmp_path: Path, monkeypatch) -> None:
    """The failure mode to rule out: unit tests pass but nothing lands in practice.

    This drives a real round through RunManager and asserts against the filesystem,
    with no direct call to the receipt writer anywhere in the test.
    """
    root = tmp_path / "artifacts"
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(root))

    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    await manager.close()

    expected = receipts_root() / verified.updated_at.astimezone(UTC).date().isoformat()
    landed = list(expected.glob("*.json"))
    assert landed, f"no receipt landed under {expected}"

    document = json.loads(landed[0].read_text())
    assert document["receipt"]["session_id"] == created.id
    assert document["receipt"]["receipt"] == created.id[:8].upper()
    assert document["receipt"]["outcome"] == "declared"
    assert document["receipt"]["round_id"] == RoundId.WAKE_IDLE_APP.value
    assert document["receipt"]["lakebase"]["ms"] is not None
    assert document["snapshot"]["state"] == SessionState.VERIFIED.value


async def test_the_receipts_route_returns_what_a_real_bout_wrote(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path / "artifacts"))

    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    await manager.close()

    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        response = client.get("/api/receipts")
        assert response.status_code == 200
        receipts = response.json()["receipts"]
        assert [item["session_id"] for item in receipts] == [created.id]
        assert receipts[0]["outcome"] == "declared"

        today = verified.updated_at.astimezone(UTC).date().isoformat()
        assert len(client.get(f"/api/receipts?since={today}").json()["receipts"]) == 1

        future = (verified.updated_at.astimezone(UTC).date() + timedelta(days=1)).isoformat()
        assert client.get(f"/api/receipts?since={future}").json()["receipts"] == []

        assert client.get("/api/receipts?since=not-a-date").status_code == 422


# ------------------------------------------------- the durable coordination store


async def test_a_bout_with_nowhere_to_keep_a_receipt_warns_rather_than_failing(
    monkeypatch, caplog
) -> None:
    """The guarantee that survives every change to where receipts live.

    A process with no artifact directory and no durable store has genuinely
    nowhere to put a souvenir. Two things must both hold, and both were nearly
    lost the night the deployed app was first driven:

    * the bout does not fail -- `record_sealed_bout` returns instead of raising,
      which is what kept the app usable while its history was empty;
    * the loss reaches the log, because a silently dropped receipt is how a blank
      recap goes unnoticed until somebody is standing in front of an audience.

    The read side is asserted in the same test on purpose: nowhere to *write* is
    a fact worth logging, and nowhere to *read* is just an empty history. If a
    GET ever starts raising for the same condition, this catches it.
    """

    deployed_shaped_runtime(monkeypatch)
    snapshot = await verified_round_one_snapshot()
    payload = {"session": snapshot.model_dump(mode="json")}

    assert artifact_root() is None
    assert receipts_root() is None

    with caplog.at_level(logging.WARNING):
        assert record_sealed_bout("run_finished", payload) is None
    assert "Could not write a bout receipt" in caplog.text

    assert load_receipts() == []
    assert await load_receipts_async() == []


async def test_a_deployed_shaped_runtime_keeps_and_reads_back_a_receipt(
    monkeypatch, caplog
) -> None:
    """The gap this store exists to close, driven end to end.

    No manifest state directory and no `ANTI_DEMO_ARTIFACT_ROOT`, which is the
    exact shape of a deployed container: `artifact_root()` correctly resolves to
    nothing, so before this store there was no receipt at all and
    `GET /api/receipts` answered `{"receipts": []}` on the one surface an
    audience looks at.

    A real round is driven through `RunManager` and nothing in the test calls the
    receipt writer, so this fails if the hook stops reaching the durable store for
    any reason -- the same reason `test_a_real_bout_leaves_a_receipt_on_disk`
    drives a round instead of calling `write_receipt`.
    """

    deployed_shaped_runtime(monkeypatch)
    cursor = FakeCoordinationCursor(table_present=True)
    install_receipt_store(durable_store(cursor))

    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    with caplog.at_level(logging.WARNING):
        created = await manager.create(
            SessionCreate(
                competitor=CompetitorId.AURORA_SERVERLESS_V2,
                primary_persona="software_engineer",
                corners=[Corner.PERFORMANCE],
                round_id=RoundId.WAKE_IDLE_APP,
            )
        )
        await manager.start_arm(created.id)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id)
        verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)
        await manager.close()
        await drain_receipt_writes()

    # Nothing on disk, by construction -- so everything below came from the store.
    assert receipts_root() is None
    # And the absent directory is not reported as a lost souvenir: on this runtime
    # it is the normal shape, and a warning per bout would train its reader to
    # ignore the one that matters.
    assert "Could not write a bout receipt" not in caplog.text

    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        receipts = client.get("/api/receipts").json()["receipts"]
        assert [item["session_id"] for item in receipts] == [created.id]
        assert receipts[0]["receipt"] == created.id[:8].upper()
        assert receipts[0]["outcome"] == "declared"
        assert receipts[0]["round_id"] == RoundId.WAKE_IDLE_APP.value
        assert receipts[0]["lakebase"]["ms"] is not None

        today = verified.updated_at.astimezone(UTC).date().isoformat()
        assert len(client.get(f"/api/receipts?since={today}").json()["receipts"]) == 1
        future = (verified.updated_at.astimezone(UTC).date() + timedelta(days=1)).isoformat()
        assert client.get(f"/api/receipts?since={future}").json()["receipts"] == []

    # The whole snapshot is kept, not just the derived view, exactly as on disk.
    stored = list(cursor.rows.values())
    assert len(stored) == 1
    document = json.loads(stored[0][7])
    assert document["snapshot"]["id"] == created.id
    assert document["snapshot"]["state"] == SessionState.VERIFIED.value


async def test_a_later_environment_failure_does_not_erase_a_declared_row() -> None:
    """The on-disk guard, kept without giving the app UPDATE on its own history.

    A bout can publish a second terminal event after it has already been declared
    -- a cleanup that will not verify, a lost lease. That is a fact about the
    environment, not about whether the transaction verified, so it may not become
    the receipt. On disk that is a read-modify-write; here the row is simply
    appended and the *read* prefers the declared one, which keeps the later
    failure as evidence and keeps the app append-only.
    """

    snapshot = await verified_round_one_snapshot()
    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    declared = derive_receipt(snapshot, "run_finished")
    await store.append(declared, snapshot)

    later = snapshot.model_copy(deep=True)
    later.state = SessionState.FAILED
    later.remembered_result = None
    later.failure = "Automatic cleanup verification is in progress"
    for lane in later.lanes.values():
        lane.state = LaneState.FAILED
    await store.append(derive_receipt(later, "session_failed"), later)

    # Both terminal events are kept ...
    assert len(cursor.rows) == 2
    # ... and the declared one is the bout's receipt.
    found = await store.load()
    assert [item.outcome for item in found] == ["declared"]
    assert found[0].remembered_result == declared.remembered_result


async def test_recording_the_same_terminal_event_twice_is_one_row() -> None:
    """A re-published terminal event must not duplicate a bout in the recap."""
    snapshot = await verified_round_one_snapshot()
    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    receipt = derive_receipt(snapshot, "run_finished")

    await store.append(receipt, snapshot)
    await store.append(receipt, snapshot)

    assert len(cursor.rows) == 1
    assert [item.session_id for item in await store.load()] == [snapshot.id]


async def failed_then_verified_round_one_bouts() -> tuple[SessionSnapshot, SessionSnapshot]:
    """One card's two bouts: one that rang and did not verify, then its re-arm that did.

    Built directly rather than by two arms, so neither carries an attempt identity
    and the durable key tells them apart by ``armed_at`` alone.
    """

    verified = await verified_round_one_snapshot()
    verified.receipt_bout_id = None
    assert verified.armed_at is not None and verified.run_started_at is not None
    earlier = timedelta(minutes=5)
    failed = verified.model_copy(deep=True)
    failed.state = SessionState.FAILED
    failed.armed_at = verified.armed_at - earlier
    failed.run_started_at = verified.run_started_at - earlier
    failed.updated_at = verified.updated_at - earlier
    failed.remembered_result = None
    failed.comparison = None
    failed.metrics = []
    failed.failure = "One or more lanes could not be verified."
    failed.lanes["lakebase"].state = LaneState.FAILED
    return failed, verified


async def test_a_rearmed_bout_that_verifies_is_kept_over_the_failed_bout_before_it() -> None:
    """One session, two bouts, one terminal event name: the verified bout is the receipt.

    A FAILED card in any round but Round 5 can be armed again, and its next bout seals
    the same `run_finished` the failed one did. A key of session, round and event alone
    made the verified receipt collide with the stopped-short one, and `ON CONFLICT DO
    NOTHING` kept the first.
    """

    failed, verified = await failed_then_verified_round_one_bouts()
    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    stopped_short = derive_receipt(failed, "run_finished")
    declared = derive_receipt(verified, "run_finished")
    assert (stopped_short.session_id, stopped_short.round_id) == (
        declared.session_id,
        declared.round_id,
    )
    assert (stopped_short.outcome, declared.outcome) == ("stopped_short", "declared")

    await store.append(stopped_short, failed)
    await store.append(declared, verified)

    found = await store.load()
    assert [(item.session_id, item.outcome) for item in found] == [(verified.id, "declared")]
    assert found[0].remembered_result == declared.remembered_result
    assert found[0].sealed_at == declared.sealed_at

    # Both bouts stay on the record, and each one's terminal event is still one row.
    await store.append(stopped_short, failed)
    await store.append(declared, verified)
    assert len(cursor.rows) == 2
    assert [item.outcome for item in await store.load()] == ["declared"]


class FailsItsFirstBoutResolver:
    """Round 1 lanes whose first bout fails at the bell and whose next one verifies.

    The opponent's lane is the one that fails: Round 1 gives the ring back only after
    a verified Lakebase lane has proved its return to idle.
    """

    def __init__(self) -> None:
        self.bouts = 0

    def resolve(self, competitor: CompetitorId):
        self.bouts += 1
        return (
            FakeLiveTarget("lakebase", "Lakebase", 0.005),
            FakeLiveTarget("competitor", "Aurora Serverless v2", 0.025, fatal=self.bouts == 1),
        )


async def test_a_card_rearmed_after_a_failed_bout_reports_its_verified_receipt(
    monkeypatch,
) -> None:
    """The same, driven through RunManager on the deployed shape: no artifact directory.

    The owner arms and rings a Round 1 card that fails, then arms and rings the same
    card again and it verifies. Both bouts seal `run_finished` for one session and one
    round, and `/api/receipts` has to answer with the verified one.
    """

    from fastapi import FastAPI

    deployed_shaped_runtime(monkeypatch)
    cursor = FakeCoordinationCursor(table_present=True)
    install_receipt_store(durable_store(cursor))

    manager = RunManager(resolver=FailsItsFirstBoutResolver(), verifier=make_verifier())
    manager._arm_poll = 0.001
    owner = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    for outcome in (SessionState.FAILED, SessionState.VERIFIED):
        await manager.start_arm(created.id, owner)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id, owner)
        await wait_for_state(manager, created.id, outcome)
        # Round 1 holds the ring through its return to idle, so the re-arm waits it out.
        await drain_record_operations(manager, manager._records[created.id])
    await manager.close()
    await drain_receipt_writes()

    sealed = sorted(
        (document["receipt"]["sealing_event"], document["receipt"]["outcome"])
        for document in (json.loads(row[7]) for row in cursor.rows.values())
    )
    assert sealed == [("run_finished", "declared"), ("run_finished", "stopped_short")]
    # Each bout's row is keyed on its own arm, never on a fallback.
    assert all(key[2].startswith("run_finished#") for key in cursor.rows)

    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        receipts = client.get("/api/receipts").json()["receipts"]
    assert [(item["session_id"], item["outcome"]) for item in receipts] == [
        (created.id, "declared")
    ]


def round_one_card() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="software_engineer",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.WAKE_IDLE_APP,
    )


def stored_receipts(cursor: FakeCoordinationCursor) -> list[dict[str, object]]:
    return [json.loads(row[7])["receipt"] for row in cursor.rows.values()]


async def test_two_cancelled_checks_of_one_card_are_two_receipts(monkeypatch) -> None:
    """Re-arms that end before ARMED have no `armed_at`, and are still two bouts.

    The owner arms the same Round 1 card twice and cancels each fight-card check.
    Both seal `session_cancelled` with `armed_at` unset, so a key that falls back to
    the bare event name kept the first cancellation and dropped the second.
    """

    deployed_shaped_runtime(monkeypatch)
    cursor = FakeCoordinationCursor(table_present=True)
    install_receipt_store(durable_store(cursor))
    resolver = HangingArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    owner = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(round_one_card())

    attempts: list[str | None] = []
    cancelled: list[SessionSnapshot] = []
    for _ in range(2):
        await manager.start_arm(created.id, owner)
        await resolver.lakebase.entered.wait()
        attempts.append(manager._records[created.id].snapshot.receipt_bout_id)
        cancelled.append(await manager.cancel_arm(created.id, owner))
    await manager.close()
    await drain_receipt_writes()

    assert [snapshot.armed_at for snapshot in cancelled] == [None, None]
    assert None not in attempts and len(set(attempts)) == 2
    assert sorted(key[2] for key in cursor.rows) == sorted(
        f"session_cancelled#{attempt}" for attempt in attempts
    )
    assert sorted(item["sealing_event"] for item in stored_receipts(cursor)) == [
        "session_cancelled",
        "session_cancelled",
    ]
    found = await durable_store(cursor).load()
    assert [(item.session_id, item.sealed_at) for item in found] == [
        (created.id, cancelled[-1].updated_at)
    ]


async def test_a_cancellation_published_after_a_rearm_keeps_its_own_attempt(
    monkeypatch,
) -> None:
    """A late terminal publish belongs to the arm that produced it, not the current one.

    `cancel_arm` publishes after it lets go of the record lock, so the owner can re-arm
    the card before the first cancellation's receipt is sealed. Here that publish is
    held until the second attempt has been armed and cancelled. Taking the identity
    from record-wide state at publish time gave the late row the second attempt's key,
    and `ON CONFLICT DO NOTHING` dropped one of the two cancellations.
    """

    deployed_shaped_runtime(monkeypatch)
    cursor = FakeCoordinationCursor(table_present=True)
    install_receipt_store(durable_store(cursor))
    resolver = HangingArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    owner = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(round_one_card())
    record = manager._records[created.id]

    publish = record.event_log.publish
    held = asyncio.Event()
    release = asyncio.Event()

    async def hold_the_first_cancellation(event, payload, **kwargs):
        if event == "session_cancelled" and not held.is_set():
            held.set()
            await release.wait()
        return await publish(event, payload, **kwargs)

    monkeypatch.setattr(record.event_log, "publish", hold_the_first_cancellation)

    await manager.start_arm(created.id, owner)
    await resolver.lakebase.entered.wait()
    first_attempt = record.snapshot.receipt_bout_id
    first_cancel = asyncio.create_task(manager.cancel_arm(created.id, owner))
    await held.wait()

    resolver.lakebase.entered.clear()
    await manager.start_arm(created.id, owner)
    await resolver.lakebase.entered.wait()
    second_attempt = record.snapshot.receipt_bout_id
    second = await asyncio.wait_for(
        manager.cancel_arm(created.id, owner),
        timeout=2,
    )

    release.set()
    first = await asyncio.wait_for(first_cancel, timeout=2)
    await manager.close()
    await drain_receipt_writes()

    assert None not in (first_attempt, second_attempt)
    assert first_attempt != second_attempt
    assert first.updated_at < second.updated_at
    assert sorted(key[2] for key in cursor.rows) == sorted(
        [f"session_cancelled#{first_attempt}", f"session_cancelled#{second_attempt}"]
    )
    sealed_at = {
        key[2]: datetime.fromisoformat(json.loads(row[7])["receipt"]["sealed_at"])
        for key, row in cursor.rows.items()
    }
    assert sealed_at == {
        f"session_cancelled#{first_attempt}": first.updated_at,
        f"session_cancelled#{second_attempt}": second.updated_at,
    }
    found = await durable_store(cursor).load()
    assert [(item.session_id, item.sealed_at) for item in found] == [
        (created.id, second.updated_at)
    ]
    assert record.checking_cancel_tasks == {}


_RECEIPT_EVENTS = SEALING_EVENTS | TOWEL_SETTLING_EVENTS


def _possible_events(
    expr: ast.expr,
    scope: ast.AST | None,
    parents: dict[ast.AST, ast.AST],
    seen: frozenset[str] = frozenset(),
) -> set[str] | None:
    """Every event name ``expr`` can evaluate to, or None when that cannot be proven.

    Literals, conditional expressions of them, and local names bound only by plain
    assignments in the enclosing function are resolved. Anything else -- a parameter,
    a loop target, a call, an attribute -- is unproven, so its publisher has to bind.
    """

    if isinstance(expr, ast.Constant):
        return {expr.value} if isinstance(expr.value, str) else None
    if isinstance(expr, ast.IfExp):
        body = _possible_events(expr.body, scope, parents, seen)
        orelse = _possible_events(expr.orelse, scope, parents, seen)
        return None if body is None or orelse is None else body | orelse
    if not isinstance(expr, ast.Name) or scope is None or expr.id in seen:
        return None
    values: set[str] = set()
    bound = False
    for node in ast.walk(scope):
        if isinstance(node, ast.arg) and node.arg == expr.id:
            return None
        if not (isinstance(node, ast.Name) and node.id == expr.id):
            continue
        if not isinstance(node.ctx, ast.Store):
            continue
        binder = parents[node]
        if isinstance(binder, ast.Assign) and node in binder.targets:
            value = binder.value
        elif isinstance(binder, ast.AnnAssign) and binder.value is not None:
            value = binder.value
        else:
            return None
        resolved = _possible_events(value, scope, parents, seen | {expr.id})
        if resolved is None:
            return None
        values |= resolved
        bound = True
    return values if bound else None


def unbound_receipt_publishes(source: str) -> tuple[list[str], int]:
    """Publishers that may seal a receipt without binding their snapshot's attempt.

    Returns the problems and how many sealing publishers were proven correct. A
    publisher is exempt only when every event it can publish is a proven literal that
    seals nothing; otherwise it must carry ``"session": X.model_dump(...)`` and pass
    ``receipt_bout_id=X.receipt_bout_id`` from that same ``X``.
    """

    tree = ast.parse(source)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    problems: list[str] = []
    checked = 0
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "publish"
        ):
            continue
        where = f"manager.py:{node.lineno}"
        keywords = {keyword.arg: keyword.value for keyword in node.keywords}
        event = node.args[0] if node.args else keywords.get("event")
        payload = node.args[1] if len(node.args) > 1 else keywords.get("payload")
        if event is None:
            problems.append(f"{where} publishes an event this check cannot find")
            continue
        scope = parents.get(node)
        while scope is not None and not isinstance(
            scope, ast.FunctionDef | ast.AsyncFunctionDef
        ):
            scope = parents.get(scope)
        events = _possible_events(event, scope, parents)
        if events is not None and not events & _RECEIPT_EVENTS:
            continue
        label = sorted(events & _RECEIPT_EVENTS) if events is not None else "an unproven event"
        session = None
        if isinstance(payload, ast.Dict):
            for key, value in zip(payload.keys, payload.values, strict=True):
                if isinstance(key, ast.Constant) and key.value == "session":
                    session = value
        if not (
            isinstance(session, ast.Call)
            and isinstance(session.func, ast.Attribute)
            and session.func.attr == "model_dump"
        ):
            problems.append(f"{where} may publish {label} without a session snapshot dump")
            continue
        snapshot = ast.unparse(session.func.value)
        bound = keywords.get("receipt_bout_id")
        if bound is None or ast.unparse(bound) != f"{snapshot}.receipt_bout_id":
            problems.append(
                f"{where} may publish {label} without receipt_bout_id={snapshot}.receipt_bout_id"
            )
            continue
        checked += 1
    return problems, checked


def manager_source() -> str:
    from server import manager as manager_module

    return Path(manager_module.__file__).read_text()


def test_every_manager_sealing_publish_binds_the_attempt_of_the_snapshot_it_sends() -> None:
    """No sealing publisher in the manager may leave the attempt identity to a fallback.

    A sealing event published without ``receipt_bout_id`` would be keyed on ``armed_at``
    or the bare event name, which is the collision this identity exists to prevent, and
    nothing at runtime would say so. The identity also has to come from the very
    snapshot the payload carries, so a delayed publish cannot pick up a later arm's.
    """

    problems, checked = unbound_receipt_publishes(manager_source())
    assert problems == []
    assert checked >= 25


def test_the_publish_check_fails_a_computed_sealing_event_that_does_not_bind() -> None:
    """Mutate the manager's own computed, session-less publishers to seal, and be caught.

    The cooldown publishers pick between two non-sealing names and carry no session,
    which is the one shape the check may exempt. Turn one branch into
    ``session_failed`` and the same publisher must now be reported.
    """

    tree = ast.parse(manager_source())
    mutated: list[int] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "publish"
            and node.args
            and isinstance(node.args[0], ast.IfExp)
            and isinstance(node.args[0].orelse, ast.Constant)
            and not node.keywords
        ):
            node.args[0].orelse = ast.Constant("session_failed")
            mutated.append(node.lineno)
    assert mutated, "the manager no longer has a computed session-less publisher to mutate"

    problems, _ = unbound_receipt_publishes(ast.unparse(tree))
    assert len(problems) == len(mutated)
    assert all("['session_failed'] without a session snapshot dump" in item for item in problems)


def test_the_publish_check_fails_a_sealing_publisher_that_drops_its_binding() -> None:
    tree = ast.parse(manager_source())
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "publish"
            and any(keyword.arg == "receipt_bout_id" for keyword in node.keywords)
        ):
            node.keywords = [k for k in node.keywords if k.arg != "receipt_bout_id"]
            break
    else:
        raise AssertionError("no bound sealing publisher to mutate")

    problems, _ = unbound_receipt_publishes(ast.unparse(tree))
    assert len(problems) == 1 and "without receipt_bout_id=" in problems[0]


@pytest.mark.parametrize(
    ("body", "flagged"),
    [
        # Proven to publish only names that seal nothing: exempt.
        ('event = "cooldown_update"\n    await log.publish(event, {})', False),
        ('await log.publish("a" if ready else "cooldown_update", {})', False),
        # Could seal, with no session: flagged however the name is computed.
        ('event = "session_failed"\n    await log.publish(event, {})', True),
        (
            'event = "arm_started"\n    event = "run_finished"\n    await log.publish(event, {})',
            True,
        ),
        ('await log.publish("x" if ready else "towel_update", {})', True),
        ("await log.publish(name_for(ready), {})", True),
        ("for event in names:\n        await log.publish(event, {})", True),
        ('await log.publish(event="session_cancelled", payload={})', True),
        # Bound to a different snapshot than the one it sends: flagged.
        (
            'await log.publish("run_finished", {"session": a.model_dump(mode="json")},'
            " receipt_bout_id=b.receipt_bout_id)",
            True,
        ),
        # Correctly bound: proven.
        (
            'await log.publish("run_finished", {"session": a.model_dump(mode="json")},'
            " receipt_bout_id=a.receipt_bout_id)",
            False,
        ),
    ],
)
def test_the_publish_check_only_exempts_proven_non_sealing_events(body, flagged) -> None:
    source = f"async def publisher(log, ready, names):\n    {body}\n"
    problems, _ = unbound_receipt_publishes(source)
    assert bool(problems) is flagged, problems


def test_a_parameter_event_name_is_unproven() -> None:
    source = "async def publisher(log, event):\n    await log.publish(event, {})\n"
    problems, _ = unbound_receipt_publishes(source)
    assert problems == [
        "manager.py:2 may publish an unproven event without a session snapshot dump"
    ]


class RefusesEachArmDifferentlyResolver:
    """Round 1 lanes whose start state is refused before ARMED, for a new reason each arm."""

    def __init__(self) -> None:
        self.refusals = iter(
            [
                TargetConfigurationError("Lakebase project is not configured"),
                PermissionDenied("User does not have USE on project 'lakebase'"),
            ]
        )

    def resolve(self, competitor: CompetitorId):
        error = next(self.refusals)
        return (
            FailingArmTarget("lakebase", "Lakebase", error),
            FailingArmTarget("competitor", "Aurora Serverless v2", error),
        )


async def test_two_failed_arms_of_one_card_keep_the_latest_refusal(monkeypatch) -> None:
    """Two pre-ARM `session_failed` bouts with different facts: the latest one answers."""

    deployed_shaped_runtime(monkeypatch)
    cursor = FakeCoordinationCursor(table_present=True)
    install_receipt_store(durable_store(cursor))
    manager = RunManager(resolver=RefusesEachArmDifferentlyResolver(), verifier=make_verifier())
    manager._arm_poll = 0.001
    owner = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(round_one_card())

    failed: list[SessionSnapshot] = []
    for _ in range(2):
        await manager.start_arm(created.id, owner)
        failed.append(await wait_for_state(manager, created.id, SessionState.FAILED))
        await drain_record_operations(manager, manager._records[created.id])
    await manager.close()
    await drain_receipt_writes()

    assert [snapshot.armed_at for snapshot in failed] == [None, None]
    assert failed[0].failure != failed[1].failure
    stored = stored_receipts(cursor)
    assert sorted(item["sealing_event"] for item in stored) == ["session_failed", "session_failed"]
    assert all(key[2].startswith("session_failed#") for key in cursor.rows)
    assert {item["failure"] for item in stored} == {failed[0].failure, failed[1].failure}
    found = await durable_store(cursor).load()
    assert [(item.session_id, item.failure) for item in found] == [
        (created.id, failed[1].failure)
    ]


async def test_one_attempts_terminal_event_is_one_row_however_often_it_is_written() -> None:
    """The attempt identity keys the row, so a rewrite of one attempt is still one row."""

    failed, _ = await failed_then_verified_round_one_bouts()
    failed.armed_at = None
    failed.run_started_at = None
    first = failed.model_copy(deep=True, update={"receipt_bout_id": "attempt-one"})
    second = failed.model_copy(
        deep=True,
        update={
            "receipt_bout_id": "attempt-two",
            "updated_at": failed.updated_at + timedelta(minutes=1),
            "failure": "The start state could not be verified.",
        },
    )
    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    for snapshot in (first, second, first, second):
        await store.append(derive_receipt(snapshot, "session_failed"), snapshot)

    assert len(cursor.rows) == 2
    found = await store.load()
    assert [(item.failure, item.sealed_at) for item in found] == [
        (second.failure, second.updated_at)
    ]


async def test_the_attempt_identity_never_reaches_a_published_snapshot() -> None:
    resolver = HangingArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    owner = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(round_one_card())
    await manager.start_arm(created.id, owner)
    await resolver.lakebase.entered.wait()
    cancelled = await manager.cancel_arm(created.id, owner)
    await manager.close()

    assert manager._records[created.id].snapshot.receipt_bout_id is not None
    assert "receipt_bout_id" not in cancelled.model_dump(mode="json")
    for event in manager._records[created.id].event_log.events:
        assert "receipt_bout_id" not in json.dumps(event.payload)


async def test_durable_auto_cleanup_failure_then_recovery_folds_clean() -> None:
    snapshot = await verified_round_one_snapshot()
    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    declared = derive_receipt(snapshot, "run_finished")
    await store.append(declared, snapshot)

    failed = declared.model_copy(
        update={
            "outcome": "stopped_short",
            "sealing_event": "cleanup_update",
            "sealed_at": declared.sealed_at + timedelta(seconds=1),
            "cleanup_failure": "RDS Proxy deletion is still retrying.",
        }
    )
    recovered = failed.model_copy(
        update={
            "sealed_at": declared.sealed_at + timedelta(seconds=12),
            "cleanup_failure": None,
        }
    )
    for offset in range(1, 12):
        await store.append(
            failed.model_copy(
                update={"sealed_at": declared.sealed_at + timedelta(seconds=offset)}
            ),
            snapshot,
        )
    assert (await store.load())[0].cleanup_failure == failed.cleanup_failure

    await store.append(recovered, snapshot)

    # One immutable declaration plus one bounded latest-cleanup overlay, even
    # after repeated pending retries.
    assert len(cursor.rows) == 2
    found = await store.load()
    assert len(found) == 1
    assert found[0].outcome == "declared"
    assert found[0].remembered_result == declared.remembered_result
    assert found[0].cleanup_failure is None
    assert found[0].sealed_at == declared.sealed_at


async def test_cleanup_recovery_does_not_make_an_older_bout_latest() -> None:
    first_snapshot = await verified_round_one_snapshot()
    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    first = derive_receipt(first_snapshot, "run_finished")
    await store.append(first, first_snapshot)

    second_snapshot = first_snapshot.model_copy(deep=True)
    second_snapshot.id = "b" * 32
    second_snapshot.updated_at = first.sealed_at + timedelta(seconds=2)
    second = derive_receipt(second_snapshot, "run_finished")
    await store.append(second, second_snapshot)

    recovered = first.model_copy(
        update={
            "sealing_event": "cleanup_update",
            "sealed_at": first.sealed_at + timedelta(seconds=3),
            "cleanup_failure": None,
        }
    )
    await store.append(recovered, first_snapshot)

    found = await store.load()

    assert [item.session_id for item in found] == [
        first.session_id,
        second.session_id,
    ]
    assert found[0].sealed_at == first.sealed_at
    assert found[-1].session_id == second.session_id


async def test_the_receipt_table_is_confirmed_rather_than_created_when_present() -> None:
    """The deployed app holds no DDL, and a present table must not need any.

    `CREATE TABLE IF NOT EXISTS` checks the ACL before the `IF NOT EXISTS`, so an
    unconditional create is refused for a statement that would have done nothing.
    That refusal is what took the cost ledger and the readiness row out of
    rotation, once each.
    """

    cursor = FakeCoordinationCursor(table_present=True, may_create=False)
    await durable_store(cursor).initialize()
    assert [s for s in cursor.statements if s.upper().startswith("CREATE")] == []


async def test_an_absent_receipt_table_names_the_identity_that_can_create_it() -> None:
    cursor = FakeCoordinationCursor(table_present=False, may_create=False)
    with pytest.raises(CoordinationObjectsMissingError) as missing:
        await durable_store(cursor).initialize()
    assert BOUT_RECEIPT_TABLE in str(missing.value)
    assert "antidemo setup" in str(missing.value)
    assert "docs/DEPLOY.md" in str(missing.value)


async def test_a_bout_held_in_both_stores_is_reported_once(tmp_path: Path, monkeypatch) -> None:
    """An operator's laptop legitimately has both, and has to see one history.

    Files from before this change plus rows written after it. Reading only one
    store would hide either tonight's bouts or last week's, and reporting the
    union blindly would show the same bout twice in the recap.
    """

    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path))
    snapshot = await verified_round_one_snapshot()
    shared = derive_receipt(snapshot, "run_finished")
    write_receipt(shared, snapshot, receipts_root())

    older = snapshot.model_copy(deep=True)
    older.id = "0badcafe" + snapshot.id[8:]
    older.updated_at = snapshot.updated_at - timedelta(days=1)
    write_receipt(derive_receipt(older, "run_finished"), older, receipts_root())

    cursor = FakeCoordinationCursor()
    store = durable_store(cursor)
    await store.append(shared, snapshot)
    install_receipt_store(store)

    found = await load_receipts_async()

    assert [item.session_id for item in found] == [older.id, snapshot.id]


async def test_a_run_receipt_on_disk_outranks_a_later_durable_cancellation(
    tmp_path: Path, monkeypatch
) -> None:
    """Each store keeps a run over a later cancel of the same card, and so must the merge.

    Here the run reached only the disk and the cancellation only the durable table,
    so neither store saw both and only the merge can apply that authority.
    """

    snapshot = await verified_round_one_snapshot()
    # Away from the receipt the harness's own verified bout just wrote.
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path))
    snapshot.state = SessionState.FAILED
    snapshot.remembered_result = None
    snapshot.comparison = None
    snapshot.failure = "The accepted run stopped short."
    snapshot.lanes["competitor"].state = LaneState.FAILED
    run = derive_receipt(snapshot, "run_finished")
    write_receipt(run, snapshot, receipts_root())

    cancelled = snapshot.model_copy(deep=True)
    cancelled.run_started_at = None
    cancelled.updated_at += timedelta(seconds=1)
    cancelled.failure = "Fight card cancelled by its owner before the bell."
    cancellation = derive_receipt(cancelled, "session_cancelled")
    store = durable_store(FakeCoordinationCursor())
    await store.append(cancellation, cancelled)
    install_receipt_store(store)

    found = await load_receipts_async()

    assert [item.sealing_event for item in found] == ["run_finished"]
    assert found[0].failure == run.failure


async def test_a_broken_durable_read_still_shows_what_is_on_disk(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    """A history that cannot be fully read is not a reason to render nothing."""
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path))
    snapshot = await verified_round_one_snapshot()
    write_receipt(derive_receipt(snapshot, "run_finished"), snapshot, receipts_root())

    async def refuse(_operation):
        raise psycopg.OperationalError("coordination endpoint is waking up")

    install_receipt_store(DurableReceiptStore(refuse))

    with caplog.at_level(logging.WARNING):
        found = await load_receipts_async()

    assert [item.session_id for item in found] == [snapshot.id]
    assert "durable bout receipt history" in caplog.text


async def test_a_process_that_cannot_keep_receipts_still_serves(caplog) -> None:
    """A souvenir may not cost a demo, and it may not fail quietly either.

    Three shapes, one rule. A process-local ring keeps its files and wants no
    durable store; a durable ring gets one; and a durable ring whose table is
    absent -- the state a fresh install is in until setup provisions it -- gets
    none, loudly. If any of those ever became an exception it would take down the
    whole app for the least important thing it does.
    """

    import app as app_module

    class ProcessLocal:
        mode = "memory"

    assert await app_module._open_receipt_store(ProcessLocal()) is None

    class Durable:
        mode = "lakebase"

        def __init__(self, cursor: FakeCoordinationCursor) -> None:
            self._cursor = cursor

        async def _run(self, operation):
            return await operation(self._cursor)

    assert await app_module._open_receipt_store(Durable(FakeCoordinationCursor())) is not None

    absent = Durable(FakeCoordinationCursor(table_present=False, may_create=False))
    with caplog.at_level(logging.ERROR):
        assert await app_module._open_receipt_store(absent) is None
    assert "cannot keep durable bout receipts" in caplog.text


async def test_publish_writes_a_receipt_without_holding_the_condition(
    tmp_path: Path, monkeypatch
) -> None:
    """The hook must not do disk I/O while subscribers are blocked on the lock."""
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    snapshot = await verified_round_one_snapshot()
    log = EventLog()

    observed: list[bool] = []

    async def watcher() -> None:
        async for _ in log.stream():
            # Reaching here at all means the publisher released the condition.
            observed.append(True)
            return

    task = asyncio.create_task(watcher())
    await asyncio.sleep(0)
    await log.publish("run_finished", {"session": snapshot.model_dump(mode="json")})
    await asyncio.wait_for(task, timeout=1)

    day = snapshot.updated_at.astimezone(UTC).date().isoformat()
    assert (
        receipts_root() / day / f"{receipt_id(snapshot.id)}-{snapshot.round.id.value}.json"
    ).exists()
    assert observed == [True]


# 2026-09-27: setup never created the cleanup overlay's definer function, so every
# fresh install logged "function ... bout_receipt_cleanup_upsert_v1(...) does not
# exist" on each bout that reached cleanup. Setup now creates it from the one SQL file
# that defines it, and an install that predates that says so once, with the fix.


def test_setup_creates_the_cleanup_function_from_its_one_definition() -> None:
    ddl = receipts_module.bout_receipt_cleanup_function_ddl()

    assert ddl.startswith(
        "CREATE OR REPLACE FUNCTION anti_demo_coordination.bout_receipt_cleanup_upsert_v1("
    )
    assert ddl.endswith("$function$;")
    assert "SECURITY DEFINER" in ddl
    # Only the function: the psql-variable grants in the same file are setup's own.
    assert ":\"app_role\"" not in ddl


async def test_the_owner_path_creates_the_function_and_revokes_public() -> None:
    executed: list[str] = []

    class Cursor:
        async def execute(self, statement, parameters=None) -> None:
            executed.append(statement)

    async def run(operation):
        return await operation(Cursor())

    await DurableReceiptStore(run).ensure_cleanup_function()

    assert executed == [
        receipts_module.bout_receipt_cleanup_function_ddl(),
        f"REVOKE ALL ON FUNCTION {receipts_module.BOUT_RECEIPT_CLEANUP_SIGNATURE} FROM PUBLIC",
    ]


async def test_a_missing_cleanup_function_is_reported_once_with_its_fix(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    monkeypatch.setenv("ANTI_DEMO_ARTIFACT_ROOT", str(tmp_path))
    monkeypatch.setattr(receipts_module, "_missing_cleanup_function_reported", False)
    snapshot = await verified_round_one_snapshot()
    payload = {"session": snapshot.model_dump(mode="json")}

    async def undefined(_operation):
        raise psycopg.errors.UndefinedFunction("function does not exist")

    install_receipt_store(DurableReceiptStore(undefined))

    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            record_sealed_bout("run_finished", payload)
            await drain_receipt_writes()

    reports = [r for r in caplog.records if "./antidemo setup" in r.getMessage()]
    assert len(reports) == 1
    assert reports[0].exc_info is None
    assert "Could not record bout receipt" not in caplog.text


async def test_the_app_is_granted_the_cleanup_function_by_name() -> None:
    """The grant a ready install re-running setup reaches (`ensure_coordination`)."""

    executed: list[str] = []

    class Cursor:
        async def execute(self, statement, parameters=None) -> None:
            executed.append(statement.as_string())

    async def run(operation):
        return await operation(Cursor())

    await DurableReceiptStore(run).grant_cleanup_function("app-client-stub")

    assert executed == [
        f"GRANT EXECUTE ON FUNCTION {receipts_module.BOUT_RECEIPT_CLEANUP_SIGNATURE} "
        'TO "app-client-stub"'
    ]
