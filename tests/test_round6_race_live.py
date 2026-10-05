"""Round 6's live lanes against scripted providers: what each one asks, and what it concludes.

The protocol's own tests (``tests/test_round6_race.py``) drive scripted lanes. These drive the
real lanes, with the provider calls scripted underneath: Lakebase's change feed through v1's
adapter, and AWS's DMS task, Glue job, source database and history table.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from server.live_orders import LiveOrder, LiveOrderHistory, LiveOrdersLiveAdapter
from server.models import CompetitorId
from server.round4_glue import GlueRun
from server.round6_dms import DmsTaskState
from server.round6_race import (
    BOUT_NONCE_PREFIX,
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    Round6NotArmedError,
    Round6RaceError,
)
from server.round6_race_live import (
    DMS_STALE_STATUS_SECONDS,
    AwsCaptureLane,
    LakebaseFeedLane,
    _uc,
    build_round6_race_engine,
)
from tests.test_round6_race import BASELINE

ORDER = LiveOrder(
    order_id="00000000-0000-4000-8000-00000000b0a7",
    sku=BASELINE.sku,
    store=BASELINE.store,
    quantity=BASELINE.quantity,
    total_cents=BASELINE.total_cents,
    status="checkout",
    proof_nonce=f"{BOUT_NONCE_PREFIX}0123456789abcdef",
)
HISTORY = "main.round6_aws.aurora_history"


async def _quiet(_message: str) -> None:
    return None


# -- Lakebase: the built-in change feed ------------------------------------------------------


class FeedAdapter:
    """v1's adapter surface, scripted: a feed state and the history rows it holds."""

    def __init__(self, *, state: str = "CDF_STATE_STREAMING", history=None) -> None:
        self.state = state
        self.history: dict[str, LiveOrderHistory] = history or {
            BASELINE.proof_nonce: LiveOrderHistory(BASELINE, "insert", 7)
        }
        self.reads: list[LiveOrder] = []

    async def inspect_feed(self):
        return SimpleNamespace(state=self.state, committed_lsn="0/1A2B" if self.state else "")

    async def read_history(self, order: LiveOrder):
        self.reads.append(order)
        return self.history.get(order.proof_nonce)


def feed_lane(adapter: FeedAdapter) -> LakebaseFeedLane:
    return LakebaseFeedLane(adapter, BASELINE)  # type: ignore[arg-type]


class TestLakebaseFeedLane:
    def test_at_rest_means_streaming_with_the_exact_baseline_in_history(self):
        asyncio.run(feed_lane(FeedAdapter()).confirm_parked(_quiet))

    def test_a_feed_that_is_not_streaming_is_not_at_rest(self):
        with pytest.raises(Round6NotArmedError, match="not streaming"):
            asyncio.run(feed_lane(FeedAdapter(state="CDF_STATE_FAILED")).confirm_parked(_quiet))

    def test_a_history_without_the_exact_baseline_is_refused(self):
        drifted = LiveOrderHistory(
            LiveOrder(**{**BASELINE.__dict__, "total_cents": 1}), "insert", 7
        )
        adapter = FeedAdapter(history={BASELINE.proof_nonce: drifted})
        with pytest.raises(Round6NotArmedError, match="exact baseline"):
            asyncio.run(feed_lane(adapter).confirm_parked(_quiet))

    def test_the_order_counts_only_as_its_own_exact_insert(self):
        lane = feed_lane(FeedAdapter())
        assert asyncio.run(lane.read_history(ORDER)) is False
        lane._adapter.history[ORDER.proof_nonce] = LiveOrderHistory(ORDER, "update_postimage", 9)
        assert asyncio.run(lane.read_history(ORDER)) is False
        other = LiveOrder(**{**ORDER.__dict__, "status": "refunded"})
        lane._adapter.history[ORDER.proof_nonce] = LiveOrderHistory(other, "insert", 9)
        assert asyncio.run(lane.read_history(ORDER)) is False
        lane._adapter.history[ORDER.proof_nonce] = LiveOrderHistory(ORDER, "insert", 9)
        assert asyncio.run(lane.read_history(ORDER)) is True

    def test_the_feed_leaving_streaming_is_the_lanes_failure(self):
        adapter = FeedAdapter()
        lane = feed_lane(adapter)
        assert asyncio.run(lane.failure()) is None
        adapter.state = "CDF_STATE_STOPPED"
        assert "CDF_STATE_STOPPED" in (asyncio.run(lane.failure()) or "")

    def test_start_and_park_are_nothing_because_the_feed_is_built_in(self):
        lane = feed_lane(FeedAdapter())
        assert asyncio.run(lane.start()) is None
        assert asyncio.run(lane.park(_quiet)) is None

    def test_the_storage_probe_reads_the_baseline_and_nothing_else(self):
        adapter = FeedAdapter()
        asyncio.run(feed_lane(adapter).probe_storage())
        assert adapter.reads == [BASELINE]

    def test_evidence_is_the_history_lsn(self):
        adapter = FeedAdapter()
        adapter.history[ORDER.proof_nonce] = LiveOrderHistory(ORDER, "insert", 42)
        assert asyncio.run(feed_lane(adapter).evidence(ORDER)) == {"history_lsn": 42}


# -- AWS: DMS and Glue -----------------------------------------------------------------------


class FakeTask:
    def __init__(self, *statuses: str) -> None:
        self.statuses = list(statuses) or ["stopped"]
        self.calls: list[str] = []

    def state(self) -> DmsTaskState:
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return DmsTaskState(status=status, stop_reason="STOPPED_BY_USER", failure="")

    def start(self):
        self.calls.append("start")
        return ("resume-processing", 0.0)

    def park(self, notify=None):
        self.calls.append("park")
        return self.state()


class FakeJob:
    def __init__(self, *, run_state: str = "RUNNING", park_error: Exception | None = None):
        self.run_state = run_state
        self.park_error = park_error
        self.calls: list[str] = []

    def start(self, arguments):
        self.calls.append("start")
        assert arguments == {}
        return ("jr_1", 0.0)

    def run(self, run_id):
        assert run_id == "jr_1"
        return GlueRun(
            run_id=run_id,
            state=self.run_state,
            started_on=datetime(2026, 9, 29, 12, 0, tzinfo=UTC),
            completed_on=None,
            error_message="",
        )

    def park(self, notify=None):
        self.calls.append("park")
        if self.park_error:
            raise self.park_error


class Statements:
    def __init__(self, rows=None) -> None:
        self.rows = rows if rows is not None else [{"n": "0"}]
        self.calls: list[tuple[str, tuple]] = []

    async def execute(self, statement, parameters=()):
        self.calls.append((statement, tuple(parameters)))
        return self.rows


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class FakeSource:
    """The r6 source database, as Prepare's slot step sees it: one scripted slot row per read."""

    def __init__(self, *slots) -> None:
        # Each read of pg_replication_slots returns the next scripted row set (the last repeats).
        self.slots = list(slots) or [[("aurora_00016412_abc", "test_decoding", "reserved", False)]]
        self.statements: list[tuple[str, tuple]] = []
        self._rows: list = []

    async def connect(self, application_name):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    def cursor(self):
        return self

    async def execute(self, statement, parameters=()):
        self.statements.append((statement, tuple(parameters)))
        if "FROM pg_catalog.pg_replication_slots" in statement:
            self._rows = self.slots.pop(0) if len(self.slots) > 1 else self.slots[0]

    async def fetchall(self):
        return self._rows

    def calls(self, needle: str) -> list[tuple]:
        return [parameters for statement, parameters in self.statements if needle in statement]


def manifest(sealed: bool = True) -> SimpleNamespace:
    seal = SimpleNamespace(
        task_arn="arn:aws:dms:us-west-2:000000000000:task:AURORA",
        job_name="lakebase-anti-demo-r6-aurora",
        history_table_full_name=HISTORY,
    )
    environment = SimpleNamespace(
        aurora=SimpleNamespace(cluster_id="r6-aurora", secret_arn="aurora-secret"),
        rds=SimpleNamespace(instance_id="r6-rds", secret_arn="rds-secret"),
    )
    return SimpleNamespace(
        round6_aws=(
            SimpleNamespace(
                bucket="lakebase-anti-demo-r6-cdc",
                source_schema="round6",
                source_table="live_orders",
                lane=lambda competitor: seal,
            )
            if sealed
            else None
        ),
        aws=SimpleNamespace(region="us-west-2"),
        databricks=SimpleNamespace(database="anti_demo"),
        round_environment=lambda number: environment,
    )


def aws_lane(task=None, job=None, statements=None, clock=None, source=None) -> AwsCaptureLane:
    lane = AwsCaptureLane(
        manifest(),  # type: ignore[arg-type]
        CompetitorId.AURORA_SERVERLESS_V2,
        statements or Statements(),  # type: ignore[arg-type]
        clock=clock or Clock(),
    )
    lane._built = (task or FakeTask(), job or FakeJob())  # type: ignore[assignment]
    lane._connect = (source or FakeSource()).connect  # type: ignore[method-assign]
    return lane


class TestAwsCaptureLane:
    def test_an_unsealed_lane_cannot_be_built(self):
        with pytest.raises(Round6NotArmedError, match="not sealed"):
            AwsCaptureLane(
                manifest(sealed=False),  # type: ignore[arg-type]
                CompetitorId.RDS_POSTGRES,
                Statements(),  # type: ignore[arg-type]
            )

    def test_each_competitor_is_named_for_what_moves_its_data(self):
        aurora = aws_lane()
        rds = AwsCaptureLane(
            manifest(),  # type: ignore[arg-type]
            CompetitorId.RDS_POSTGRES,
            Statements(),  # type: ignore[arg-type]
        )
        assert aurora.lane_id == rds.lane_id == COMPETITOR_LANE
        assert aurora.label == "AWS DMS + Glue from Aurora Serverless v2"
        assert rds.label == "AWS DMS + Glue from RDS PostgreSQL"

    def test_at_rest_parks_both_and_asks_the_history_table_a_question(self):
        task, job, statements = FakeTask("stopped"), FakeJob(), Statements()
        asyncio.run(aws_lane(task, job, statements).confirm_parked(_quiet))
        assert task.calls == ["park"] and job.calls == ["park"]
        assert statements.calls == [
            ("SELECT count(*) AS n FROM `main`.`round6_aws`.`aurora_history` LIMIT 1", ())
        ]

    def test_a_task_that_has_never_run_has_no_slot_to_resume_from(self):
        with pytest.raises(Round6NotArmedError, match="never run"):
            asyncio.run(aws_lane(FakeTask("ready")).confirm_parked(_quiet))

    def test_prepare_brings_the_parked_slot_up_to_now(self):
        # Whatever the idle source wrote since the last bout is not the bell's to decode.
        source = FakeSource()
        asyncio.run(aws_lane(source=source).confirm_parked(_quiet))
        # Its own slot only: DMS names it after the task.
        (query,) = source.calls("FROM pg_catalog.pg_replication_slots")
        assert query == ("aurora%",)
        assert source.calls("pg_replication_slot_advance") == [("aurora_00016412_abc",)]
        assert not source.calls("pg_drop_replication_slot")

    def test_a_slot_the_source_dropped_is_re_created_under_its_name_and_plugin(self):
        messages: list[str] = []

        async def notify(message):
            messages.append(message)

        source = FakeSource([("aurora_00016412_abc", "test_decoding", "lost", False)])
        asyncio.run(aws_lane(source=source).confirm_parked(notify))
        assert source.calls("pg_drop_replication_slot") == [("aurora_00016412_abc",)]
        assert source.calls("pg_create_logical_replication_slot") == [
            ("aurora_00016412_abc", "test_decoding")
        ]
        assert not source.calls("pg_replication_slot_advance")
        assert any("Re-creating the AWS lane's replication slot" in item for item in messages)

    def test_a_slot_still_held_just_after_the_stop_is_waited_for(self):
        held = [("aurora_00016412_abc", "test_decoding", "reserved", True)]
        free = [("aurora_00016412_abc", "test_decoding", "reserved", False)]
        source = FakeSource(held, free)
        asyncio.run(aws_lane(source=source).confirm_parked(_quiet))
        assert len(source.calls("FROM pg_catalog.pg_replication_slots")) == 2
        assert source.calls("pg_replication_slot_advance") == [("aurora_00016412_abc",)]

    def test_a_slot_held_past_the_bound_is_refused(self):
        clock = Clock()
        held = [("aurora_00016412_abc", "test_decoding", "reserved", True)]

        class Stuck(FakeSource):
            async def execute(self, statement, parameters=()):
                await super().execute(statement, parameters)
                clock.now += 31.0

        source = Stuck(held)
        with pytest.raises(Round6NotArmedError, match="still held"):
            asyncio.run(aws_lane(clock=clock, source=source).confirm_parked(_quiet))
        assert not source.calls("pg_replication_slot_advance")

    @pytest.mark.parametrize(
        "slots", [[], [("a", "p", "reserved", False), ("b", "p", "reserved", False)]]
    )
    def test_a_source_without_exactly_one_slot_for_the_task_is_refused(self, slots):
        with pytest.raises(Round6NotArmedError, match="replication slots on its source"):
            asyncio.run(aws_lane(source=FakeSource(slots)).confirm_parked(_quiet))

    def test_the_order_counts_only_as_exactly_one_insert(self):
        statements = Statements([{"n": "1"}])
        lane = aws_lane(statements=statements)
        assert asyncio.run(lane.read_history(ORDER)) is True
        statement, parameters = statements.calls[-1]
        assert "WHERE order_id = :order_id AND proof_nonce = :proof_nonce AND Op = 'I'" in (
            statement
        )
        assert [(item.name, item.value) for item in parameters] == [
            ("order_id", ORDER.order_id),
            ("proof_nonce", ORDER.proof_nonce),
        ]
        for rows in ([{"n": "0"}], [{"n": "2"}], []):
            statements.rows = rows
            assert asyncio.run(lane.read_history(ORDER)) is False

    def test_the_bell_starts_the_task_and_the_job_together(self):
        task, job = FakeTask(), FakeJob()
        lane = aws_lane(task, job)
        asyncio.run(lane.start())
        assert task.calls == ["start"] and job.calls == ["start"]
        assert lane._run_id == "jr_1"

    def test_a_task_already_running_at_the_bell_is_in_the_operator_log(self, caplog):
        # Prepare parks the task, so one DMS says is running at the bell means AWS started warm.
        class Running(FakeTask):
            def start(self):
                self.calls.append("start")
                return ("already-running", 0.0)

        with caplog.at_level("WARNING", logger="server.round6_race_live"):
            asyncio.run(aws_lane(Running("running"), FakeJob()).start())

        logged = [record.getMessage() for record in caplog.records]
        assert any("already running at the bell" in line for line in logged), logged

    def test_a_stale_stopped_status_just_after_the_bell_is_not_a_failure(self):
        clock = Clock()
        lane = aws_lane(FakeTask("stopped"), clock=clock)
        asyncio.run(lane.start())
        clock.now += DMS_STALE_STATUS_SECONDS - 1
        assert asyncio.run(lane.failure()) is None
        clock.now += 2
        assert "The DMS task stopped" in (asyncio.run(lane.failure()) or "")

    def test_a_task_that_ran_and_then_stopped_has_failed_at_once(self):
        lane = aws_lane(FakeTask("starting", "running", "stopped"))
        asyncio.run(lane.start())
        assert asyncio.run(lane.failure()) is None
        assert asyncio.run(lane.failure()) is None
        assert "The DMS task stopped" in (asyncio.run(lane.failure()) or "")

    def test_a_glue_run_that_ended_is_the_lanes_failure(self):
        lane = aws_lane(FakeTask("running"), FakeJob(run_state="FAILED"))
        asyncio.run(lane.start())
        assert "The Glue run ended FAILED" in (asyncio.run(lane.failure()) or "")

    def test_park_parks_both_and_names_every_refusal(self):
        task, job = FakeTask(), FakeJob(park_error=RuntimeError("slot not released"))
        with pytest.raises(Round6RaceError, match="slot not released"):
            asyncio.run(aws_lane(task, job).park(_quiet))
        assert task.calls == ["park"] and job.calls == ["park"]

    def test_evidence_is_read_after_the_race_from_dms_and_glue(self):
        statements = Statements(
            [{"dms_commit_ts": "2026-09-29 12:00:01", "applied_at": "2026-09-29 12:01:05"}]
        )
        lane = aws_lane(statements=statements)
        asyncio.run(lane.start())
        evidence = asyncio.run(lane.evidence(ORDER))
        assert evidence == {
            "competitor": "aurora",
            "glue_run": "jr_1",
            "dms_commit_ts": "2026-09-29 12:00:01",
            "glue_applied_at": "2026-09-29 12:01:05",
            "glue_run_started_on": "2026-09-29T12:00:00+00:00",
        }


def test_unity_catalog_names_are_quoted_part_by_part_and_never_injected():
    assert _uc("main.round6_aws.aurora_history") == "`main`.`round6_aws`.`aurora_history`"
    for bad in ("main.history", "a.b.c.d", "main.`x`.history", "main..history"):
        with pytest.raises(Round6RaceError):
            _uc(bad)


def test_the_engine_races_lakebase_alone_until_the_aws_lane_is_sealed(monkeypatch):
    from server import round6_race_live

    adapter = object.__new__(LiveOrdersLiveAdapter)
    adapter._statements = Statements()  # type: ignore[attr-defined]
    monkeypatch.setattr(
        round6_race_live,
        "build_live_orders_engine",
        lambda manifest: SimpleNamespace(
            adapter=adapter, contract=SimpleNamespace(baseline=BASELINE)
        ),
    )

    alone = build_round6_race_engine(manifest(sealed=False), CompetitorId.RDS_POSTGRES)  # type: ignore[arg-type]
    assert alone.lane_ids == (LAKEBASE_LANE,)
    both = build_round6_race_engine(manifest(), CompetitorId.RDS_POSTGRES)  # type: ignore[arg-type]
    assert both.lane_ids == (LAKEBASE_LANE, COMPETITOR_LANE)
    assert both.baseline == BASELINE
    # One reader for both histories: the AWS lane asks on Lakebase's own warehouse client.
    assert both.lanes[1]._statements is adapter._statements  # type: ignore[attr-defined]
