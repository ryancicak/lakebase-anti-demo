"""A graceful shutdown waits for the Round 4 race lane's power writes, as it does for v1's.

The race lane schedules each power record (a start, the stop its bell owes, a stop) rather than
awaiting it. Shutdown drained only v1's writes, so one still in flight was cancelled with the loop,
and a stop the bell owed could vanish with it.
"""

from __future__ import annotations

import asyncio

from server import model_score_live, pipeline_power, round4_race_live
from server.round4_race_live import LakebaseSyncLane


async def test_shutdown_waits_for_a_power_write_the_race_lane_still_has_in_flight(monkeypatch):
    landed: list[dict] = []

    async def slow_write(record):
        await asyncio.sleep(0.05)
        landed.append(record)
        return True

    monkeypatch.setattr(pipeline_power, "record_power_request", slow_write)
    lane = object.__new__(LakebaseSyncLane)
    lane._persist({"intent": "stop_owed", "run_id": "pair-1"})
    try:
        await model_score_live.drain_power_records()

        assert landed == [{"intent": "stop_owed", "run_id": "pair-1"}]
        assert not round4_race_live._PENDING
    finally:
        await asyncio.gather(*round4_race_live._PENDING, return_exceptions=True)
