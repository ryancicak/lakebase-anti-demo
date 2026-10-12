"""No Round 3 cleanup certifies absence over an unresolved mutation.

RE-DO / Retry cleanup reset Round 3 without settling at all, and automatic
cleanup settled but reset anyway when the settle refused. Either way a shielded
create could land after the ring was released.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from types import SimpleNamespace

from server.coordination import InMemoryBoutLeaseStore
from server.manager import RunManager
from server.models import (
    CompetitorId,
    CooldownState,
    Corner,
    ResetMode,
    RoundId,
    SessionCreate,
    SessionState,
)
from server.recovery import (
    RecoveryLaneResult,
    RecoveryPhase,
    RecoveryProgress,
    RecoveryRunResult,
)
from server.safe_change import (
    SafeChangeProvider,
    SafeChangeResetLaneResult,
    SafeChangeResetResult,
)
from server.safe_change_live import SafeChangeControlPlaneError
from tests.test_manager import drain_record_operations, wait_for_cooldown, wait_for_state


def round3_request() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.RECOVER_DELETED_ORDER,
    )


class RecordingRecoveryEngine:
    """Round 3 engine whose settle refuses ``refusals`` times before it resolves."""

    def __init__(
        self,
        *,
        refusals: int = 0,
        run_forever: bool = False,
        child_cancels: int = 0,
    ) -> None:
        self.timeline: list[str] = []
        self.refusals = refusals
        self.run_forever = run_forever
        self.child_cancels = child_cancels
        self.running = asyncio.Event()

    async def arm(self, competitor, on_progress):
        del on_progress
        return SimpleNamespace(
            competitor=competitor,
            lanes={
                lane_id: SimpleNamespace(evidence={"exact_incident_committed": True})
                for lane_id in ("lakebase", "competitor")
            },
        )

    async def run(self, arm, on_progress, on_started, stop_control=None):
        del on_progress
        if stop_control is not None:
            stop_control.started_ns = time.monotonic_ns()
        await on_started()
        self.running.set()
        if self.run_forever:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.timeline.append("run_cancelled")
                raise
        return RecoveryRunResult(
            competitor=arm.competitor,
            started_ns=1,
            completed_ns=2,
            launch_skew_ms=0.001,
            contract_sha256="contract",
            lanes={
                "lakebase": RecoveryLaneResult(
                    lane_id="lakebase",
                    name="Lakebase",
                    provider=SafeChangeProvider.LAKEBASE,
                    elapsed_ms=9_000.0,
                    first_action_ns=1,
                    completed_ns=2,
                    artifact_id="recovery-ad-test-001",
                    ok=True,
                ),
                "competitor": RecoveryLaneResult(
                    lane_id="competitor",
                    name="RDS PostgreSQL",
                    provider=SafeChangeProvider.RDS,
                    elapsed_ms=900_000.0,
                    first_action_ns=1,
                    completed_ns=2,
                    artifact_id="adrc-ad-test-001-rds",
                    ok=False,
                    error="Recovery lane exceeded 900 seconds",
                    timed_out=True,
                ),
            },
        )

    async def settle_pending_mutations(self, competitor):
        del competitor
        if self.child_cancels:
            self.child_cancels -= 1
            self.timeline.append("settle_child_cancelled")
            child = asyncio.create_task(asyncio.Event().wait())
            await asyncio.sleep(0)
            child.cancel()
            await child
        if self.refusals:
            self.refusals -= 1
            self.timeline.append("settle_refused")
            raise SafeChangeControlPlaneError(
                "AWS mutation is still unresolved; cleanup cannot report success"
            )
        self.timeline.append("settle")

    async def reset(self, competitor, on_progress=None):
        self.timeline.append("reset")
        lanes = {
            lane_id: SafeChangeResetLaneResult(
                lane_id=lane_id,
                name=name,
                provider=provider,
                artifact_id=f"recovery-{lane_id}",
                ok=True,
                already_absent=True,
            )
            for lane_id, name, provider in (
                ("lakebase", "Lakebase", SafeChangeProvider.LAKEBASE),
                ("competitor", "RDS PostgreSQL", SafeChangeProvider.RDS),
            )
        }
        if on_progress is not None:
            for lane_id, lane in lanes.items():
                await on_progress(
                    RecoveryProgress(
                        lane_id=lane_id,
                        lane_name=lane.name,
                        phase=RecoveryPhase.RESET,
                        status="Owned recovery environment and synthetic order cleared",
                        occurred_at=datetime.now(UTC),
                    )
                )
        return SafeChangeResetResult(competitor=competitor, lanes=lanes)


def assert_reset_only_after_a_resolved_settle(timeline: list[str]) -> None:
    for index, step in enumerate(timeline):
        if step == "reset":
            assert index > 0 and timeline[index - 1] == "settle", timeline


async def failed_by_lost_lease(engine, lease_store, *, auto_retry: float = 0.01):
    manager = RunManager(recovery_factory=lambda: engine, lease_store=lease_store)
    manager._cleanup_retry_initial = auto_retry
    created = await manager.create(round3_request())
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.running.wait(), 1)
    record = manager._records[created.id]
    lost = record.lease
    assert lost is not None
    assert await lease_store.release(lost)
    await manager._handle_lost_lease(record, lost)
    failed = await wait_for_state(manager, created.id, SessionState.FAILED)
    assert failed.cooldown is None
    for _ in range(100):
        if "run_cancelled" in engine.timeline:
            break
        await asyncio.sleep(0.005)
    return manager, created, record


async def test_redo_after_a_lost_lease_settles_before_it_resets() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(run_forever=True)
    manager, created, record = await failed_by_lost_lease(engine, lease_store)
    await manager.start_cooldown(created.id)
    await wait_for_cooldown(manager, created.id, CooldownState.READY)
    await drain_record_operations(manager, record)
    assert engine.timeline == ["run_cancelled", "settle", "reset"]
    assert await lease_store.current() is None
    await manager.close()


async def test_redo_whose_settle_refuses_holds_the_ring_and_never_resets() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(run_forever=True, refusals=10_000)
    manager, created, record = await failed_by_lost_lease(engine, lease_store)
    await manager.start_cooldown(created.id)
    for _ in range(40):
        await asyncio.sleep(0.01)
    assert "settle_refused" in engine.timeline
    assert "reset" not in engine.timeline
    snapshot = await manager.get(created.id)
    assert snapshot.cooldown is not None
    assert snapshot.cooldown.state != CooldownState.READY
    assert await lease_store.current() is not None
    await manager.close()


class HooklessRecoveryEngine(RecordingRecoveryEngine):
    settle_pending_mutations = None


async def test_cleanup_with_no_settle_hook_never_resets_or_releases() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = HooklessRecoveryEngine(run_forever=True)
    manager, created, record = await failed_by_lost_lease(engine, lease_store)
    await manager.start_cooldown(created.id)
    for _ in range(40):
        await asyncio.sleep(0.01)
    assert "reset" not in engine.timeline
    snapshot = await manager.get(created.id)
    assert snapshot.cooldown is not None
    assert snapshot.cooldown.state != CooldownState.READY
    assert await lease_store.current() is not None
    await manager.close()


async def test_cleanup_whose_settle_meets_a_child_cancel_keeps_retrying() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(child_cancels=2)
    manager = RunManager(recovery_factory=lambda: engine, lease_store=lease_store)
    manager._cleanup_retry_initial = 0.01
    created = await manager.create(round3_request())
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await wait_for_cooldown(manager, created.id, CooldownState.READY)
    record = manager._records[created.id]
    await drain_record_operations(manager, record)
    assert engine.timeline == [
        "settle_child_cancelled",
        "settle_child_cancelled",
        "settle",
        "reset",
    ]
    assert await lease_store.current() is None
    await manager.close()


async def test_cleanup_run_from_an_already_cancelled_task_still_fails_a_child_cancel() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(run_forever=True)
    manager, created, record = await failed_by_lost_lease(engine, lease_store, auto_retry=60)
    await drain_record_operations(manager, record)
    record.snapshot.cooldown = manager._new_cooldown(
        record, ResetMode.DELETE_RECOVERY_ENVIRONMENT
    )
    engine.child_cancels = 1

    async def from_a_cancelled_finally() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        await manager._reset_recovery(record)

    runner = asyncio.create_task(from_a_cancelled_finally())
    await asyncio.sleep(0)
    runner.cancel()
    await runner
    assert record.snapshot.cooldown.state == CooldownState.FAILED
    assert "reset" not in engine.timeline
    await manager.close()


class HangingSettleEngine(RecordingRecoveryEngine):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.hang = False
        self.settling = asyncio.Event()

    async def settle_pending_mutations(self, competitor):
        if self.hang:
            self.settling.set()
            await asyncio.Event().wait()
        await super().settle_pending_mutations(competitor)


async def test_cleanup_that_is_itself_cancelled_propagates_and_never_fails() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = HangingSettleEngine(run_forever=True)
    manager, created, record = await failed_by_lost_lease(engine, lease_store, auto_retry=60)
    await drain_record_operations(manager, record)
    cooldown = manager._new_cooldown(record, ResetMode.DELETE_RECOVERY_ENVIRONMENT)
    record.snapshot.cooldown = cooldown
    engine.hang = True
    cleanup = asyncio.create_task(manager._reset_recovery(record))
    await asyncio.wait_for(engine.settling.wait(), 1)
    cleanup.cancel()
    try:
        await cleanup
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("an own cancel was swallowed")
    assert record.snapshot.cooldown.state != CooldownState.FAILED
    assert "reset" not in engine.timeline
    engine.hang = False
    await manager.close()


async def test_shutdown_whose_settle_meets_a_child_cancel_still_finishes_closing() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(run_forever=True)
    manager, created, record = await failed_by_lost_lease(engine, lease_store, auto_retry=60)
    await drain_record_operations(manager, record)
    engine.child_cancels = 1
    assert await manager._cleanup_for_shutdown(record) is False
    assert "reset" not in engine.timeline
    await manager.close()


async def test_close_whose_shutdown_settle_meets_a_child_cancel_keeps_the_ring() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(refusals=1)
    manager = RunManager(recovery_factory=lambda: engine, lease_store=lease_store)
    manager._cleanup_retry_initial = 60
    created = await manager.create(round3_request())
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    for _ in range(100):
        if "settle_refused" in engine.timeline:
            break
        await asyncio.sleep(0.005)
    assert engine.timeline == ["settle_refused"]
    heartbeat = manager._records[created.id].lease_heartbeat_task
    assert heartbeat is not None and not heartbeat.done()
    engine.child_cancels = 1
    await manager.close()
    assert engine.timeline == ["settle_refused", "settle_child_cancelled"]
    assert await lease_store.current() is not None
    assert heartbeat.done()


async def test_auto_cleanup_whose_settle_refuses_never_resets_or_releases() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(refusals=10_000)
    manager = RunManager(recovery_factory=lambda: engine, lease_store=lease_store)
    manager._cleanup_retry_initial = 0.01
    created = await manager.create(round3_request())
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    for _ in range(40):
        await asyncio.sleep(0.01)
    assert engine.timeline.count("settle_refused") >= 2
    assert "reset" not in engine.timeline
    snapshot = await manager.get(created.id)
    assert snapshot.cooldown is None or snapshot.cooldown.state != CooldownState.READY
    assert await lease_store.current() is not None
    await manager.close()


async def test_auto_cleanup_resets_once_a_retried_settle_resolves() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = RecordingRecoveryEngine(refusals=2)
    manager = RunManager(recovery_factory=lambda: engine, lease_store=lease_store)
    manager._cleanup_retry_initial = 0.01
    created = await manager.create(round3_request())
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await wait_for_cooldown(manager, created.id, CooldownState.READY)
    record = manager._records[created.id]
    await drain_record_operations(manager, record)
    assert engine.timeline[:2] == ["settle_refused", "settle_refused"]
    assert_reset_only_after_a_resolved_settle(engine.timeline)
    assert engine.timeline[-1] == "reset"
    assert await lease_store.current() is None
    await manager.close()
