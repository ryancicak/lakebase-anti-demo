"""Round 6's Azure lane (server/round6_adf_lane.py) through the real Round6RaceEngine.

ADF, the r6 server and the Delta history are faked. A run extracts the source's rows committed
after the watermark the lane prepared, into the history, after a cold start, as Phase 0's runs did.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from server.azure_adf_job import AdfRun
from server.live_orders import LiveOrder
from server.round6_adf_lane import EPOCH, AdfExtractLane
from server.round6_race import COMPETITOR_LANE, LAKEBASE_LANE, Verdict, new_bout_orders
from tests.test_round6_race import BASELINE, FakeLane, engine, quiet


class World:
    """The r6 server (orders with their committed_at) and the Delta history they extract into."""

    def __init__(self) -> None:
        self.clock = 0
        self.source: dict[str, tuple[LiveOrder, int]] = {BASELINE.order_id: (BASELINE, 0)}
        self.history: dict[str, tuple[LiveOrder, int]] = {}
        self.flows: list[str] = []


class FakeOrders:
    def __init__(self, world: World) -> None:
        self.world = world

    async def read(self, order_id: str) -> LiveOrder | None:
        held = self.world.source.get(order_id)
        return held[0] if held else None

    async def commit(self, order: LiveOrder) -> None:
        self.world.clock += 1
        self.world.source[order.order_id] = (order, self.world.clock)

    async def delete(self, order: LiveOrder) -> None:
        self.world.source.pop(order.order_id, None)

    async def residue(self) -> list[LiveOrder]:
        return [order for order, _ in self.world.source.values() if order != BASELINE]


class FakeHistory:
    def __init__(self, world: World) -> None:
        self.world = world

    async def newest_committed_at(self) -> str | None:
        stamps = [at for _, at in self.world.history.values()]
        return str(max(stamps)) if stamps else None

    async def holds(self, order: LiveOrder) -> bool:
        return self.world.history.get(order.order_id, (None, 0))[0] == order

    async def copies(self, order: LiveOrder) -> int:
        return int(await self.holds(order))

    async def committed_at(self, order: LiveOrder) -> str | None:
        held = self.world.history.get(order.order_id)
        return str(held[1]) if held else None


class FakeAdfJob:
    """A run extracts what was committed after the flow's watermark, once its cold start ends."""

    def __init__(self, world: World, *, delay: float = 0.05, ends: str = "Succeeded",
                 delivers: bool = True) -> None:
        self.world = world
        self.delay = delay
        self.ends = ends
        self.delivers = delivers
        self.runs: list[dict[str, Any]] = []
        self.parks = 0

    def start(self, parameters: dict[str, Any] | None = None) -> str:
        run = {"id": f"run-{len(self.runs) + 1}", "at": time.monotonic(),
               "watermark": self.world.flows[-1], "cancelled": False, "done": False}
        self.runs.append(run)
        return run["id"]

    def _state(self, run: dict[str, Any]) -> str:
        if run["cancelled"]:
            return "Cancelled"
        if time.monotonic() - run["at"] < self.delay:
            return "InProgress"
        if self.ends == "Succeeded" and not run["done"] and self.delivers:
            after = -1 if run["watermark"] == EPOCH else int(run["watermark"])
            for order_id, (order, at) in self.world.source.items():
                if at > after:
                    self.world.history[order_id] = (order, at)
            run["done"] = True
        return self.ends

    def run(self, run_id: str) -> AdfRun:
        run = next(item for item in self.runs if item["id"] == run_id)
        return AdfRun(run_id=run_id, state=self._state(run), started_on=None, completed_on=None)

    def active_runs(self) -> list[AdfRun]:
        return [self.run(item["id"]) for item in self.runs if self._state(item) == "InProgress"]

    def park(self, notify: Any = None) -> list[str]:
        self.parks += 1
        for item in self.runs:
            if self._state(item) == "InProgress":
                item["cancelled"] = True
        return []


class WatchedHistory(FakeHistory):
    """The verifier's reads also let a finished run land, as ADF's would on its own."""

    def __init__(self, world: World, job: FakeAdfJob) -> None:
        super().__init__(world)
        self.job = job

    async def holds(self, order: LiveOrder) -> bool:
        for run in self.job.runs:
            self.job._state(run)
        return await super().holds(order)


def azure_lane(**options: Any) -> tuple[AdfExtractLane, World, FakeAdfJob]:
    world = World()
    job = FakeAdfJob(world, **options)
    lane = AdfExtractLane(job, FakeOrders(world), WatchedHistory(world, job),  # type: ignore[arg-type]
                          write_flow=world.flows.append)
    return lane, world, job


def bout(race: Any) -> Any:
    async def run() -> Any:
        arm = await race.prepare(quiet)
        order, guardrail = new_bout_orders(BASELINE)
        return await race.run(arm, order, guardrail, quiet)

    return asyncio.run(run())


def test_prepare_writes_the_flow_for_the_historys_newest_order_and_starts_nothing() -> None:
    lane, world, job = azure_lane()
    world.history["old"] = (BASELINE, 7)
    asyncio.run(engine(FakeLane(LAKEBASE_LANE, "Lakebase"), lane).prepare(quiet))  # type: ignore[arg-type]
    assert world.flows == ["7"] and lane.watermark == "7"
    assert job.runs == [] and job.parks == 1


def test_a_bout_extracts_the_new_order_and_verifies() -> None:
    lane, world, job = azure_lane(delay=0.02)
    race = engine(FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1), lane,  # type: ignore[arg-type]
                  lane_timeout_seconds=2.0)
    result = bout(race)
    assert world.flows == [EPOCH]  # an empty history: everything after the epoch
    assert len(job.runs) == 1  # the bell's one createRun
    outcome = result.outcomes[COMPETITOR_LANE]
    assert outcome.verified
    assert outcome.evidence["adf_run"] == "run-1" and outcome.evidence["watermark"] == EPOCH
    assert result.resolution.verdict is Verdict.WIN


def test_a_failed_run_is_the_lanes_failure() -> None:
    lane, _, _ = azure_lane(delay=0.01, ends="Failed")
    result = bout(engine(FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1), lane,  # type: ignore[arg-type]
                         lane_timeout_seconds=2.0))
    outcome = result.outcomes[COMPETITOR_LANE]
    assert not outcome.verified and outcome.failure == "The Data Factory run ended Failed"


def test_a_run_that_succeeded_without_the_order_ends_the_lane_rather_than_censoring_it() -> None:
    lane, _, _ = azure_lane(delay=0.01, delivers=False)
    result = bout(engine(FakeLane(LAKEBASE_LANE, "Lakebase", arrives_on_read=1), lane,  # type: ignore[arg-type]
                         lane_timeout_seconds=2.0))
    outcome = result.outcomes[COMPETITOR_LANE]
    assert not outcome.verified and not outcome.timed_out
    assert outcome.failure == "The Data Factory run ended without delivering the order"


def test_the_watermark_is_read_exact_to_the_microsecond() -> None:
    # A rendering that dropped the microseconds would re-extract the last row on every run.
    from server.round6_adf_lane import WarehouseHistory

    class Statements:
        def __init__(self, value: object) -> None:
            self.value, self.seen = value, ""

        async def execute(self, statement: str, parameters: object = ()) -> list[dict]:
            self.seen = statement
            return [{"newest_us": self.value}]

    statements = Statements("1792381000325818")
    history = WarehouseHistory(statements, "main.anti_demo.azure_live_orders_history")
    assert asyncio.run(history.newest_committed_at()) == "2026-10-19T03:36:40.325818+00:00"
    assert "unix_micros(max(committed_at))" in statements.seen
    assert asyncio.run(WarehouseHistory(Statements(None), "a.b.c").newest_committed_at()) is None


def test_start_without_prepare_is_refused() -> None:
    lane, _, _ = azure_lane()
    with pytest.raises(Exception, match="no watermark"):
        asyncio.run(lane.start())
