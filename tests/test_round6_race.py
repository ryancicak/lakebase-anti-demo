"""Round 6's two-lane protocol, against scripted lanes on a real event loop.

These pin what makes the round fair and honest: both checkouts are committed at the bell and the
AWS pipeline starts then and not before, each lane is timed by its own verifier, the winner is
decided by Round 4's evidence rule, a failed or censored lane is never scored as a loss, and
settling removes exactly the bout's rows and always parks.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from server.live_orders import LiveOrder
from server.round6_race import (
    BOUT_NONCE_PREFIX,
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    Round6NotArmedError,
    Round6Phase,
    Round6RaceEngine,
    Round6RaceError,
    Round6SettleError,
    Verdict,
    new_bout_orders,
)

BASELINE = LiveOrder(
    order_id="00000000-0000-4000-8000-000000000006",
    sku="RED-GLOVE",
    store="CHICAGO",
    quantity=1,
    total_cents=8450,
    status="baseline",
    proof_nonce="round6-baseline",
)


@dataclass
class FakeLane:
    lane_id: str
    label: str
    #: The history read on which the order first appears (1-based), or None for never.
    arrives_on_read: int | None = 3
    failure_after_start: str | None = None
    delete_error: Exception | None = None
    park_error: Exception | None = None
    source: dict[str, LiveOrder] = field(default_factory=lambda: {BASELINE.order_id: BASELINE})
    events: list[str] = field(default_factory=list)
    history_reads: int = 0
    started: bool = False

    async def confirm_parked(self, notify):
        self.events.append("confirm_parked")

    async def read_source(self, order_id):
        return self.source.get(order_id)

    async def commit(self, order):
        self.events.append(f"commit:{order.status}")
        await asyncio.sleep(0)
        self.source[order.order_id] = order

    async def read_history(self, order):
        self.history_reads += 1
        return self.arrives_on_read is not None and self.history_reads >= self.arrives_on_read

    async def start(self):
        self.events.append("start")
        self.started = True

    async def failure(self):
        return self.failure_after_start if self.started else None

    async def park(self, notify):
        self.events.append("park")
        if self.park_error:
            raise self.park_error

    async def delete(self, order):
        self.events.append(f"delete:{order.status}")
        if self.delete_error:
            raise self.delete_error
        self.source.pop(order.order_id, None)

    async def residue(self):
        return [order for order in self.source.values() if order.order_id != BASELINE.order_id]

    async def evidence(self, order):
        return {"history_reads": self.history_reads}


def engine(*lanes: FakeLane, **options) -> Round6RaceEngine:
    options.setdefault("poll_seconds", 0.001)
    options.setdefault("watch_seconds", 0.001)
    options.setdefault("lane_timeout_seconds", 1.0)
    return Round6RaceEngine(lanes, baseline=BASELINE, **options)


async def quiet(*_arguments):
    return None


def ring(race: Round6RaceEngine, progress=quiet):
    async def bout():
        arm = await race.prepare(quiet)
        order, guardrail = new_bout_orders(BASELINE)
        return await race.run(arm, order, guardrail, progress)

    return asyncio.run(bout())


class TestPrepare:
    def test_prepare_confirms_every_pipeline_at_rest_and_starts_nothing(self):
        lakebase, aws = FakeLane(LAKEBASE_LANE, "Lakebase"), FakeLane(COMPETITOR_LANE, "AWS")
        arm = asyncio.run(engine(lakebase, aws).prepare(quiet))
        assert arm.lanes == (LAKEBASE_LANE, COMPETITOR_LANE)
        assert lakebase.events == ["confirm_parked"] and aws.events == ["confirm_parked"]

    def test_residue_a_crash_left_is_removed_before_the_baseline_is_checked(self):
        aws = FakeLane(COMPETITOR_LANE, "AWS")
        left, _ = new_bout_orders(BASELINE)
        aws.source[left.order_id] = left
        asyncio.run(engine(FakeLane(LAKEBASE_LANE, "Lakebase"), aws).prepare(quiet))
        assert left.order_id not in aws.source
        assert "delete:checkout" in aws.events

    def test_residue_this_demo_did_not_write_is_refused_and_left_alone(self):
        aws = FakeLane(COMPETITOR_LANE, "AWS")
        stranger = LiveOrder("x", "SKU", "STORE", 1, 1, "real", "someone-elses")
        aws.source["x"] = stranger
        with pytest.raises(Round6NotArmedError, match="did not write"):
            asyncio.run(engine(FakeLane(LAKEBASE_LANE, "Lakebase"), aws).prepare(quiet))
        assert aws.source["x"] == stranger

    def test_a_source_off_its_baseline_refuses(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase")
        lakebase.source[BASELINE.order_id] = LiveOrder(
            BASELINE.order_id, "SKU", "STORE", 2, 1, "changed", "changed"
        )
        with pytest.raises(Round6NotArmedError, match="not at its sealed baseline"):
            asyncio.run(engine(lakebase, FakeLane(COMPETITOR_LANE, "AWS")).prepare(quiet))

    def test_lakebase_is_always_the_first_lane(self):
        with pytest.raises(ValueError):
            Round6RaceEngine([FakeLane(COMPETITOR_LANE, "AWS")], baseline=BASELINE)


class TestTheBell:
    def test_the_first_lane_to_deliver_wins_by_evidence(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=2)
        aws = FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=40)
        result = ring(engine(lakebase, aws))
        assert result.resolution.verdict is Verdict.WIN
        assert result.resolution.winner == LAKEBASE_LANE
        assert result.resolution.margin_ms is not None and result.resolution.margin_ms > 0
        assert result.outcomes[LAKEBASE_LANE].verified and result.outcomes[COMPETITOR_LANE].verified

    def test_both_checkouts_commit_and_the_aws_pipeline_starts_at_the_bell(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase")
        aws = FakeLane(COMPETITOR_LANE, "AWS")
        result = ring(engine(lakebase, aws))
        for lane in (lakebase, aws):
            # Prepare starts nothing; the bell starts the pipeline and commits the checkout.
            assert lane.events.index("confirm_parked") < lane.events.index("start")
            assert "commit:checkout" in lane.events
            assert "commit:checkout-guardrail" in lane.events
        assert set(result.commit_ack_ms) == {LAKEBASE_LANE, COMPETITOR_LANE}
        assert result.commit_skew_ms is not None and result.skew_within_bound

    def test_the_guardrail_is_a_separate_order_read_back_on_each_source(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase")
        aws = FakeLane(COMPETITOR_LANE, "AWS")
        result = ring(engine(lakebase, aws))
        assert set(result.guardrails) == {LAKEBASE_LANE, COMPETITOR_LANE}
        assert result.guardrail_order.order_id in aws.source
        assert result.order.order_id != result.guardrail_order.order_id

    def test_a_pipeline_that_fails_is_no_result_never_a_loss(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=2)
        aws = FakeLane(
            COMPETITOR_LANE, "AWS", arrives_on_read=None, failure_after_start="task failed: slot"
        )
        result = ring(engine(lakebase, aws))
        assert result.outcomes[COMPETITOR_LANE].failure == "task failed: slot"
        assert not result.outcomes[COMPETITOR_LANE].timed_out
        assert result.outcomes[COMPETITOR_LANE].bound_ms is None
        assert result.resolution.verdict is Verdict.NO_RESULT

    def test_a_lane_that_runs_out_its_bound_gives_a_lower_bound_not_a_margin(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=2)
        aws = FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=None)
        result = ring(engine(lakebase, aws, lane_timeout_seconds=0.05))
        assert result.outcomes[COMPETITOR_LANE].timed_out
        # The bound it ran out is its clock's floor; a lane that delivered has none.
        assert result.outcomes[COMPETITOR_LANE].bound_ms == 50.0
        assert result.outcomes[LAKEBASE_LANE].bound_ms is None
        assert result.resolution.verdict is Verdict.LOWER_BOUND
        assert result.resolution.winner == LAKEBASE_LANE
        assert result.resolution.margin_ms is None

    def test_a_failed_history_read_is_a_negative_read(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=3)
        calls = {"n": 0}
        original = lakebase.read_history

        async def flaky(order):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("warehouse hiccup")
            return await original(order)

        lakebase.read_history = flaky
        result = ring(engine(lakebase, FakeLane(COMPETITOR_LANE, "AWS", arrives_on_read=20)))
        assert result.outcomes[LAKEBASE_LANE].verified

    def test_progress_reaches_the_observer_and_a_failing_observer_changes_nothing(self):
        seen: list[tuple[str, Round6Phase]] = []

        async def progress(lane_id, phase, status, elapsed_ms):
            seen.append((lane_id, phase))
            raise RuntimeError("observer went away")

        result = ring(
            engine(FakeLane(LAKEBASE_LANE, "Lakebase"), FakeLane(COMPETITOR_LANE, "AWS")),
            progress,
        )
        assert (LAKEBASE_LANE, Round6Phase.VERIFIED) in seen
        assert result.resolution.verdict in {Verdict.WIN, Verdict.WITHIN_RESOLUTION}

    def test_lakebase_alone_is_no_comparison(self):
        result = ring(engine(FakeLane(LAKEBASE_LANE, "Lakebase")))
        assert result.outcomes[LAKEBASE_LANE].verified
        assert result.resolution.verdict is Verdict.NO_RESULT
        assert result.commit_skew_ms is None

    def test_an_arm_rings_once(self):
        race = engine(FakeLane(LAKEBASE_LANE, "Lakebase"))

        async def twice():
            arm = await race.prepare(quiet)
            await race.run(arm, *new_bout_orders(BASELINE), quiet)
            await race.run(arm, *new_bout_orders(BASELINE), quiet)

        with pytest.raises(Round6RaceError, match="already been rung"):
            asyncio.run(twice())

    @pytest.mark.parametrize(
        "bad",
        [
            "baseline",
            "no-bout-nonce",
            "same-order",
        ],
    )
    def test_a_bout_order_must_be_new_and_the_demos_own(self, bad):
        race = engine(FakeLane(LAKEBASE_LANE, "Lakebase"))
        order, guardrail = new_bout_orders(BASELINE)
        if bad == "baseline":
            order = BASELINE
        elif bad == "no-bout-nonce":
            order = LiveOrder("id-1", "S", "T", 1, 1, "checkout", "nonce-without-prefix")
        else:
            guardrail = order

        async def bout():
            arm = await race.prepare(quiet)
            await race.run(arm, order, guardrail, quiet)

        with pytest.raises(Round6RaceError):
            asyncio.run(bout())

    def test_every_bout_order_carries_the_bout_prefix(self):
        order, guardrail = new_bout_orders(BASELINE)
        assert order.proof_nonce.startswith(BOUT_NONCE_PREFIX)
        assert guardrail.proof_nonce.startswith(BOUT_NONCE_PREFIX)
        assert order.total_cents == BASELINE.total_cents == 8450


class TestSettle:
    def test_settle_removes_exactly_the_bouts_orders_and_parks_every_lane(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase")
        aws = FakeLane(COMPETITOR_LANE, "AWS")
        race = engine(lakebase, aws)
        ring(race)
        asyncio.run(race.settle())
        for lane in (lakebase, aws):
            assert lane.source == {BASELINE.order_id: BASELINE}
            assert lane.events[-1] == "park"

    def test_a_delete_that_fails_still_parks_and_says_so(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase")
        aws = FakeLane(COMPETITOR_LANE, "AWS", delete_error=RuntimeError("connection refused"))
        race = engine(lakebase, aws)
        ring(race)
        with pytest.raises(Round6SettleError, match="connection refused"):
            asyncio.run(race.settle())
        assert aws.events[-1] == "park" and lakebase.events[-1] == "park"

    def test_a_lane_that_will_not_park_fails_the_settle(self):
        aws = FakeLane(COMPETITOR_LANE, "AWS", park_error=RuntimeError("DMS task stuck"))
        race = engine(FakeLane(LAKEBASE_LANE, "Lakebase"), aws)
        with pytest.raises(Round6SettleError, match="DMS task stuck"):
            asyncio.run(race.settle())

    def test_a_second_settle_has_nothing_left_to_delete(self):
        lakebase = FakeLane(LAKEBASE_LANE, "Lakebase")
        race = engine(lakebase)
        ring(race)
        asyncio.run(race.settle())
        deletes = [event for event in lakebase.events if event.startswith("delete:")]
        asyncio.run(race.settle())
        assert [event for event in lakebase.events if event.startswith("delete:")] == deletes
