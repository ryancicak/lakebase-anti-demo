"""Success-path settlement for Rounds 4 and 6, and the end-states that leak.

Rounds 4 and 6 write a proof row into a real source table. For a long time only
the towel path took it back out, so an ordinary win left its row behind and a
failure left one too — while clearing the identity that would have been needed
to find it again. These tests pin the settlement to every terminal route a bout
can take, and pin the property that matters most about it: settlement is cleanup,
so it may never be the thing that breaks a bout.
"""

from __future__ import annotations

import asyncio

import pytest

from server.live_orders import LiveOrder, LiveOrdersResult, LiveOrdersVerificationError
from server.manager import RunManager
from server.models import (
    CompetitorId,
    Corner,
    RoundId,
    SessionCreate,
    SessionState,
)


def _result(order: LiveOrder, guardrail: LiveOrder) -> LiveOrdersResult:
    return LiveOrdersResult(
        order=order,
        checkout_guardrail_order=guardrail,
        history_lsn=42,
        matching_orders=1,
        analytics_available_ms=12.5,
        total_elapsed_ms=25.0,
        checkout_commit_ms=3.0,
        checkout_guardrail_commit_ms=4.0,
        checkout_guardrail_read_ms=5.0,
        checkout_verified=True,
        poll_attempts=1,
    )


class _LiveOrdersEngine:
    """A Round 6 engine that records exactly which rows it was asked to settle."""

    def __init__(self, *, fail: bool = False, settle_failures: int = 0) -> None:
        self.fail = fail
        self.settle_failures = settle_failures
        self.settled: list[tuple[LiveOrder, LiveOrder | None]] = []
        self.settle_attempts = 0
        self.run_entered = asyncio.Event()

    async def arm(self, on_progress=None):
        from types import SimpleNamespace

        return SimpleNamespace(committed_lsn=1)

    async def run(self, arm, order, checkout_guardrail_order, on_progress=None):
        self.run_entered.set()
        if self.fail:
            raise LiveOrdersVerificationError("native CDF proof did not arrive")
        return _result(order, checkout_guardrail_order)

    async def settle_and_cleanup_owned(self, order, checkout_guardrail_order):
        self.settle_attempts += 1
        if self.settle_attempts <= self.settle_failures:
            raise LiveOrdersVerificationError("statement execution is briefly unavailable")
        self.settled.append((order, checkout_guardrail_order))


def _round_six() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="data_analyst",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.ANALYZE_LIVE_ORDERS,
    )


async def _reach(manager: RunManager, session_id: str, state: SessionState):
    for _ in range(200):
        snapshot = await manager.get(session_id)
        if snapshot.state == state:
            return snapshot
        await asyncio.sleep(0.005)
    raise AssertionError(f"session never reached {state}")


async def _settled(engine: _LiveOrdersEngine) -> None:
    for _ in range(200):
        if engine.settled:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("settlement never ran")


async def _drive(manager: RunManager, engine: _LiveOrdersEngine, state: SessionState):
    created = await manager.create(_round_six())
    await manager.start_arm(created.id)
    await _reach(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await _reach(manager, created.id, state)
    return created


async def test_a_won_round_six_removes_its_own_proof_rows() -> None:
    """The defect in one sentence: a win used to leave its row behind."""

    engine = _LiveOrdersEngine()
    manager = RunManager(live_orders_factory=lambda: engine)

    await _drive(manager, engine, SessionState.VERIFIED)
    await _settled(engine)

    order, guardrail = engine.settled[0]
    assert order.order_id
    # The guardrail checkout is a second owned row and leaks the same way.
    assert guardrail is not None
    assert guardrail.order_id != order.order_id
    await manager.close()


async def test_a_failed_round_six_still_removes_the_row_it_already_committed() -> None:
    """The worse half of the defect: failure cleared the identity without using it.

    A run can commit its checkout row and then fail verification. The row is
    real, it is owned, and until settlement was added here the failure path
    dropped the only handle anyone had on it.
    """

    engine = _LiveOrdersEngine(fail=True)
    manager = RunManager(live_orders_factory=lambda: engine)

    await _drive(manager, engine, SessionState.FAILED)
    await _settled(engine)

    assert len(engine.settled) == 1
    await manager.close()


async def test_settlement_retries_a_transient_failure_instead_of_leaving_residue() -> None:
    engine = _LiveOrdersEngine(settle_failures=2)
    manager = RunManager(live_orders_factory=lambda: engine)
    manager._cleanup_retry_initial = 0.001
    manager._cleanup_retry_max = 0.001

    await _drive(manager, engine, SessionState.VERIFIED)
    await _settled(engine)

    assert engine.settle_attempts == 3
    await manager.close()


async def test_settlement_that_never_succeeds_gives_up_without_failing_the_bout() -> None:
    """Residue is a cost problem; a terminal path that throws is a broken bout."""

    engine = _LiveOrdersEngine(settle_failures=99)
    manager = RunManager(live_orders_factory=lambda: engine)
    manager._cleanup_retry_initial = 0.001
    manager._cleanup_retry_max = 0.001

    created = await _drive(manager, engine, SessionState.VERIFIED)
    for _ in range(200):
        task = manager._records[created.id].settlement_task
        if task is None or task.done():
            break
        await asyncio.sleep(0.005)

    snapshot = await manager.get(created.id)
    assert snapshot.state == SessionState.VERIFIED
    assert snapshot.failure is None
    assert engine.settle_attempts == manager._settlement_attempts
    assert engine.settled == []
    await manager.close()


async def test_an_engine_that_cannot_settle_is_skipped_not_raised_on() -> None:
    """A terminal path must never fail because cleanup was unavailable."""

    class _Unsettleable(_LiveOrdersEngine):
        settle_and_cleanup_owned = None  # type: ignore[assignment]

    engine = _Unsettleable()
    manager = RunManager(live_orders_factory=lambda: engine)

    created = await _drive(manager, engine, SessionState.VERIFIED)

    snapshot = await manager.get(created.id)
    assert snapshot.state == SessionState.VERIFIED
    assert snapshot.failure is None
    await manager.close()


async def test_one_settlement_runs_at_a_time_for_a_session() -> None:
    """Scheduling is idempotent, so a redo cannot stack duplicate cleanups."""

    engine = _LiveOrdersEngine()
    manager = RunManager(live_orders_factory=lambda: engine)
    created = await manager.create(_round_six())
    record = manager._records[created.id]

    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow() -> None:
        entered.set()
        await release.wait()

    manager._schedule_round_settlement(record, slow, label="Round 6")
    await asyncio.wait_for(entered.wait(), timeout=1)
    first = record.settlement_task
    manager._schedule_round_settlement(record, slow, label="Round 6")

    assert record.settlement_task is first
    release.set()
    await asyncio.wait_for(asyncio.shield(first), timeout=1)
    assert record.settlement_task is None
    await manager.close()


async def test_settlement_is_not_scheduled_once_the_manager_is_closing() -> None:
    """Shutdown settles what is running; it must not start new work."""

    engine = _LiveOrdersEngine()
    manager = RunManager(live_orders_factory=lambda: engine)
    created = await manager.create(_round_six())
    record = manager._records[created.id]
    await manager.close()

    calls: list[int] = []

    async def settle() -> None:
        calls.append(1)

    manager._schedule_round_settlement(record, settle, label="Round 6")

    assert record.settlement_task is None
    assert calls == []


async def test_a_cancelled_settlement_propagates_and_clears_its_slot() -> None:
    """Shutdown cancels settlement; it must not be swallowed as a retryable error."""

    engine = _LiveOrdersEngine()
    manager = RunManager(live_orders_factory=lambda: engine)
    created = await manager.create(_round_six())
    record = manager._records[created.id]

    entered = asyncio.Event()

    async def blocked() -> None:
        entered.set()
        await asyncio.Event().wait()

    manager._schedule_round_settlement(record, blocked, label="Round 6")
    await asyncio.wait_for(entered.wait(), timeout=1)
    task = record.settlement_task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert record.settlement_task is None
    await manager.close()
