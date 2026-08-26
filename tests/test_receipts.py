from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, timedelta
from pathlib import Path

import psycopg
import pytest
from fastapi.testclient import TestClient

from server.api import router
from server.coordination import CoordinationObjectsMissingError
from server.manager import EventLog, RunManager
from server.models import (
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
from server.verifier import NeutralVerifier, RetryPolicy
from tests.test_manager import FakeResolver, wait_for_state


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

    Deliberately keyed and ranked the same way the real SQL is -- primary key
    (session_id, round_id, sealing_event), `ON CONFLICT DO NOTHING`, and a
    declared row outranking a later terminal event -- so a test that passes here
    is testing the store's rules rather than the fake's.
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
        elif statement.startswith(f"INSERT INTO {BOUT_RECEIPT_TABLE}"):
            self.rows.setdefault((params[0], params[1], params[2]), params)
        elif "SELECT DISTINCT ON" in statement:
            floor = params[0]
            best: dict[tuple[str, str], tuple] = {}
            for (session_id, round_id, _event), row in self.rows.items():
                if floor is not None and row[6] < floor:
                    continue
                rank = (row[5] == "declared", row[6])
                current = best.get((session_id, round_id))
                if current is None or rank > current[0]:
                    best[(session_id, round_id)] = (rank, row)
            self._pending = [(json.loads(row[7]),) for _key, (_rank, row) in sorted(best.items())]
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
