import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

import pytest

from server.models import LaneState
from server.verifier import (
    FatalProbeError,
    NeutralVerifier,
    RetryPolicy,
    VerifierStopped,
    compute_launch_skew_ms,
)


@dataclass
class FakePreparedTarget:
    id: str
    name: str
    delay: float
    failures_before_success: int = 0
    fatal: bool = False
    attempts: int = 0

    async def attempt(self, nonce: str, expected_value: str, timeout_seconds: float) -> None:
        assert nonce
        assert expected_value
        assert timeout_seconds > 0
        self.attempts += 1
        await asyncio.sleep(self.delay)
        if self.fatal:
            raise FatalProbeError("Nonce verification failed.")
        if self.attempts <= self.failures_before_success:
            raise RuntimeError("not awake yet")


async def test_verifier_starts_both_lanes_together_and_records_authoritative_times() -> None:
    events: list[tuple[str, str, dict[str, object]]] = []

    async def on_event(lane: str, state: str, payload: dict[str, object]) -> None:
        events.append((lane, state, payload))

    verifier = NeutralVerifier(
        RetryPolicy(
            overall_timeout_seconds=1,
            attempt_timeout_seconds=0.5,
            initial_delay_seconds=0.001,
            maximum_delay_seconds=0.001,
        )
    )
    lakebase = FakePreparedTarget("lakebase", "Lakebase", 0.01)
    competitor = FakePreparedTarget("competitor", "Aurora", 0.03)

    result = await verifier.run(
        (lakebase, competitor),
        nonce="00000000-0000-0000-0000-000000000001",
        expected_value="proof",
        on_event=on_event,
    )

    assert result.lanes["lakebase"].state == LaneState.VERIFIED
    assert result.lanes["competitor"].state == LaneState.VERIFIED
    assert result.lanes["lakebase"].elapsed_ms < result.lanes["competitor"].elapsed_ms
    assert result.launch_skew_ms is not None
    assert result.launch_skew_ms < 10
    assert {event[0] for event in events} == {"lakebase", "competitor"}
    verified_events = {
        lane: payload
        for lane, state, payload in events
        if state == LaneState.VERIFIED
    }
    assert set(verified_events) == {"lakebase", "competitor"}
    for lane_id, payload in verified_events.items():
        assert isinstance(payload["connection_closed_at"], datetime)
        assert payload["connection_closed_at"] == result.lanes[lane_id].connection_closed_at


async def test_correctness_failure_is_visible_and_not_retried() -> None:
    async def on_event(lane: str, state: str, payload: dict[str, object]) -> None:
        return None

    verifier = NeutralVerifier(
        RetryPolicy(
            overall_timeout_seconds=0.2,
            attempt_timeout_seconds=0.1,
            initial_delay_seconds=0.001,
            maximum_delay_seconds=0.001,
        )
    )
    bad = FakePreparedTarget("lakebase", "Lakebase", 0, fatal=True)
    good = FakePreparedTarget("competitor", "Aurora", 0)

    result = await verifier.run(
        (bad, good),
        nonce="00000000-0000-0000-0000-000000000002",
        expected_value="proof",
        on_event=on_event,
    )

    assert result.lanes["lakebase"].state == LaneState.FAILED
    assert result.lanes["lakebase"].elapsed_ms is None
    assert result.lanes["lakebase"].attempts == 1
    assert result.lanes["competitor"].state == LaneState.VERIFIED


async def test_verifier_accepts_one_eligible_lane_without_inventing_skew() -> None:
    async def on_event(lane: str, state: str, payload: dict[str, object]) -> None:
        return None

    verifier = NeutralVerifier()
    lakebase = FakePreparedTarget("lakebase", "Lakebase", 0)

    result = await verifier.run(
        (lakebase,),
        nonce="00000000-0000-0000-0000-000000000003",
        expected_value="proof",
        on_event=on_event,
    )

    assert set(result.lanes) == {"lakebase"}
    assert result.lanes["lakebase"].state == LaneState.VERIFIED
    # Not 0.0. One lane cannot evidence a simultaneous start, and a zero here
    # reached the proof modal as "Start gap 0.000ms" -- a fairness guarantee
    # manufactured from a single lane.
    assert result.launch_skew_ms is None


def test_a_single_launch_point_has_no_skew_to_report() -> None:
    assert compute_launch_skew_ms([1_000_000_000]) is None


def test_no_launch_points_have_no_skew_to_report() -> None:
    assert compute_launch_skew_ms([]) is None


def test_two_launch_points_still_report_their_real_difference() -> None:
    assert compute_launch_skew_ms([1_000_000_000, 1_002_500_000]) == 2.5
    # Order must not matter, and two lanes that genuinely started together
    # still report a true zero.
    assert compute_launch_skew_ms([1_002_500_000, 1_000_000_000]) == 2.5
    assert compute_launch_skew_ms([1_000_000_000, 1_000_000_000]) == 0.0


def test_the_widest_pair_sets_the_skew_across_more_than_two_lanes() -> None:
    assert compute_launch_skew_ms([1_000_000_000, 1_001_000_000, 1_004_000_000]) == 4.0


async def test_stop_cancels_and_settles_both_live_lane_attempts() -> None:
    entered = {"lakebase": asyncio.Event(), "competitor": asyncio.Event()}
    closed = {"lakebase": asyncio.Event(), "competitor": asyncio.Event()}
    release = asyncio.Event()

    @asynccontextmanager
    async def connection(lane_id: str) -> AsyncIterator[None]:
        entered[lane_id].set()
        try:
            yield
        finally:
            closed[lane_id].set()

    @dataclass
    class LiveTarget:
        id: str
        name: str

        async def attempt(self, nonce: str, expected_value: str, timeout_seconds: float) -> None:
            async with connection(self.id):
                await release.wait()

    events: list[tuple[str, str]] = []

    async def on_event(lane: str, state: str, payload: dict[str, object]) -> None:
        events.append((lane, state))

    stop_event = asyncio.Event()
    verifier = NeutralVerifier(RetryPolicy(overall_timeout_seconds=30))
    run = asyncio.create_task(
        verifier.run(
            (LiveTarget("lakebase", "Lakebase"), LiveTarget("competitor", "Aurora")),
            nonce="00000000-0000-0000-0000-000000000004",
            expected_value="proof",
            on_event=on_event,
            stop_event=stop_event,
        )
    )
    await asyncio.gather(*(ready.wait() for ready in entered.values()))

    stop_event.set()

    with pytest.raises(VerifierStopped):
        await asyncio.wait_for(run, timeout=1)
    assert all(settled.is_set() for settled in closed.values())
    assert all(state == LaneState.CONNECTING for _, state in events)
