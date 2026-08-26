from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from server.models import CompetitorId
from server.recovery import (
    RecoveryEngine,
    RecoveryNotArmedError,
    RecoveryPhase,
    RecoveryResetError,
    RecoveryStopControl,
    RecoveryStoppedResult,
)
from server.safe_change import (
    ArtifactInspection,
    SafeChangeOwnershipScope,
    SafeChangeProvider,
)


class FakeRecoveryConnection:
    def __init__(self, adapter: FakeRecoveryAdapter, *, recovered: bool = False) -> None:
        self.adapter = adapter
        self.recovered = recovered
        self.ordinal = len(adapter.connections) + 1
        self.closed = False
        adapter.connections.append(self)

    @property
    def label(self) -> str:
        kind = "recovered" if self.recovered else "source"
        return f"{kind}-{self.ordinal}"

    async def execute(self, statement, parameters=()) -> None:
        if "INSERT INTO public.orders" in statement:
            self.adapter.timeline.append((self.adapter.provider.value, "insert"))
            self.adapter.source_row = tuple(parameters[1:])
            self.adapter.historical_row = self.adapter.source_row
            return
        if "DELETE FROM public.orders" in statement:
            self.adapter.timeline.append((self.adapter.provider.value, "reset_delete"))
            expected = tuple(parameters[1:])
            if self.adapter.source_row == expected:
                self.adapter.source_row = None
            return
        raise AssertionError(statement)

    async def fetch_one(self, statement, parameters=()):
        if "WITH boundary_clock AS MATERIALIZED" in statement:
            self.adapter.timeline.append((self.adapter.provider.value, "delete_cte"))
            expected = tuple(parameters[1:])
            if self.adapter.source_row != expected:
                return None
            observed_at = self.adapter.database_now + timedelta(seconds=1, microseconds=250_000)
            self.adapter.database_now = observed_at
            recovery_at = observed_at.replace(microsecond=0) - timedelta(seconds=1)
            deleted = self.adapter.source_row
            self.adapter.source_row = None
            return (observed_at, recovery_at, *deleted)
        if "clock_timestamp" in statement:
            self.adapter.database_now += timedelta(seconds=1)
            self.adapter.clock_reads.append(self.adapter.database_now)
            return (self.adapter.database_now,)
        if "FROM public.orders" in statement:
            event = "recovered_select" if self.recovered else f"source_select:{self.label}"
            self.adapter.timeline.append((self.adapter.provider.value, event))
            return self.adapter.recovery_row if self.recovered else self.adapter.source_row
        raise AssertionError(statement)

    async def commit(self) -> None:
        self.adapter.timeline.append((self.adapter.provider.value, f"commit:{self.label}"))

    async def rollback(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True
        self.adapter.timeline.append((self.adapter.provider.value, f"close:{self.label}"))


class FakeRecoveryAdapter:
    def __init__(
        self,
        provider: SafeChangeProvider,
        name: str,
        timeline: list[tuple[str, str]],
    ) -> None:
        self.provider = provider
        self.name = name
        self.source_id = f"source-{provider.value}"
        self.timeline = timeline
        self.source_row = None
        self.historical_row = None
        self.recovery_row = None
        self.artifact = None
        self.database_now = datetime(2026, 8, 18, 15, 0, 0, 250_000, tzinfo=UTC)
        self.requested_at = None
        self.delete_calls = 0
        self.clock_reads: list[datetime] = []
        self.wait_calls = 0
        self.create_calls = 0
        self.eligibility_calls = 0
        self.inspect_calls = 0
        self.connections: list[FakeRecoveryConnection] = []
        # Stands in for the provider account: anything in here is billing.
        self.provider_account: set[str] = set()
        self.abandon_calls = 0
        self.abandoned: list[str] = []
        # "ok" | "raise" | "hang"
        self.abandon_behaviour = "ok"
        self.reached_readiness_wait = asyncio.Event()
        self.hold_create: asyncio.Event | None = None

    async def connect_source(self, plan):
        self.timeline.append((self.provider.value, "connect_source"))
        return FakeRecoveryConnection(self)

    async def inspect_recovery(self, plan):
        self.inspect_calls += 1
        return self.artifact

    async def wait_recovery_point(self, plan, recovery_at, report):
        self.wait_calls += 1
        self.timeline.append((self.provider.value, "wait_recovery_point"))
        self.recovery_row = self.historical_row
        await report("Recovery point eligible", f"{self.provider.value}.describe")
        return {"latest_restorable_time": recovery_at.isoformat()}

    async def recovery_point_eligible(self, plan, recovery_at):
        self.eligibility_calls += 1
        raise AssertionError("arm must not pre-wait recovery eligibility")

    async def create_recovery(self, plan, recovery_at, report):
        self.create_calls += 1
        self.requested_at = recovery_at
        self.timeline.append((self.provider.value, "create_recovery"))
        # The shielded restore has landed; the environment now exists and bills.
        self.provider_account.add(plan.artifact_id)
        if self.hold_create is not None:
            # The unshielded readiness poll, which is where the leak happens.
            self.reached_readiness_wait.set()
            await self.hold_create.wait()
        self.artifact = ArtifactInspection(
            artifact_id=plan.artifact_id,
            provider=plan.provider,
            source_id=plan.source_id,
            run_id=plan.scope.run_id,
            owner=plan.scope.owner,
            state="READY",
        )
        await report("Recovery request accepted", "provider.restore")
        return self.artifact

    async def connect_recovery(self, plan, artifact):
        self.timeline.append((self.provider.value, "connect_recovery"))
        return FakeRecoveryConnection(self, recovered=True)

    async def delete_recovery(self, plan, artifact, report):
        self.delete_calls += 1
        self.artifact = None
        self.provider_account.discard(plan.artifact_id)
        await report("Recovery environment deleted", "provider.describe")

    async def abandon_recovery(self, plan) -> None:
        self.abandon_calls += 1
        self.abandoned.append(plan.artifact_id)
        if self.abandon_behaviour == "hang":
            await asyncio.Event().wait()
        if self.abandon_behaviour == "raise":
            raise RuntimeError("the control plane refused the delete")
        self.provider_account.discard(plan.artifact_id)


def build_engine(*, cancel_teardown_timeout_seconds: float = 5.0):
    timeline: list[tuple[str, str]] = []
    scope = SafeChangeOwnershipScope(
        run_id="ad-test-003",
        owner="operator@databricks.com",
        aws_account_id="123456789012",
        aws_region="us-west-2",
    )
    lakebase = FakeRecoveryAdapter(SafeChangeProvider.LAKEBASE, "Lakebase", timeline)
    aurora = FakeRecoveryAdapter(
        SafeChangeProvider.AURORA,
        "Aurora Serverless v2",
        timeline,
    )
    rds = FakeRecoveryAdapter(SafeChangeProvider.RDS, "RDS PostgreSQL", timeline)
    tick = 0

    def clock_ns() -> int:
        nonlocal tick
        tick += 1_000_000
        return tick

    engine = RecoveryEngine(
        scope=scope,
        lakebase=lakebase,
        competitors={
            CompetitorId.AURORA_SERVERLESS_V2: aurora,
            CompetitorId.RDS_POSTGRES: rds,
        },
        cancel_teardown_timeout_seconds=cancel_teardown_timeout_seconds,
        clock_ns=clock_ns,
    )
    return engine, lakebase, aurora, rds, timeline


async def test_arm_commits_and_ages_only_selected_exact_rows_without_prewait() -> None:
    engine, lakebase, aurora, rds, _ = build_engine()

    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    assert lakebase.source_row == engine.contract.row
    assert aurora.source_row == engine.contract.row
    assert rds.source_row is None
    assert set(arm.lanes) == {"lakebase", "competitor"}
    assert all("recovery_at" not in lane.evidence for lane in arm.lanes.values())
    assert all(lane.evidence["exact_incident_committed"] is True for lane in arm.lanes.values())
    assert all(len(adapter.clock_reads) >= 2 for adapter in (lakebase, aurora))
    assert all(
        connection.closed
        for adapter in (lakebase, aurora)
        for connection in adapter.connections
    )
    assert all(adapter.wait_calls == 0 for adapter in (lakebase, aurora, rds))
    assert all(adapter.eligibility_calls == 0 for adapter in (lakebase, aurora, rds))
    assert all(adapter.create_calls == 0 for adapter in (lakebase, aurora, rds))
    assert rds.inspect_calls == 0 and rds.connections == []


def test_timed_delete_uses_one_materialized_provider_clock_read() -> None:
    engine, *_ = build_engine()
    statement = engine.contract.delete_with_boundary_sql

    assert statement.count("clock_timestamp()") == 1
    assert "WITH boundary_clock AS MATERIALIZED" in statement
    assert "date_trunc('second', observed_at)" in statement
    assert "DELETE FROM public.orders" in statement


async def test_run_times_delete_eligibility_restore_and_both_verified_reads() -> None:
    engine, lakebase, aurora, _, timeline = build_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    timeline.clear()
    progress = []

    async def on_started() -> None:
        timeline.append(("shared", "run_started"))

    async def on_progress(item) -> None:
        progress.append(item)
        timeline.append((item.lane_id, f"phase:{item.phase.value}"))

    result = await engine.run(arm, on_progress=on_progress, on_started=on_started)

    assert result.all_verified is True
    assert [len(adapter.connections) for adapter in (lakebase, aurora)] == [4, 4]
    assert all(
        connection.closed
        for adapter in (lakebase, aurora)
        for connection in adapter.connections
    )
    assert lakebase.requested_at == result.lanes["lakebase"].recovery_at
    assert aurora.requested_at == result.lanes["competitor"].recovery_at
    assert all(lane.recovery_at is not None for lane in result.lanes.values())
    assert all(lane.recovery_at.microsecond == 0 for lane in result.lanes.values())

    started_index = timeline.index(("shared", "run_started"))
    for lane_id, provider in (
        ("lakebase", SafeChangeProvider.LAKEBASE.value),
        ("competitor", SafeChangeProvider.AURORA.value),
    ):
        expected = [
            (lane_id, f"phase:{RecoveryPhase.DELETING_INCIDENT.value}"),
            (provider, "delete_cte"),
            (provider, "commit:source-2"),
            (provider, "close:source-2"),
            (lane_id, f"phase:{RecoveryPhase.WAITING_RECOVERY_POINT.value}"),
            (provider, "wait_recovery_point"),
            (lane_id, f"phase:{RecoveryPhase.RESTORING.value}"),
            (provider, "create_recovery"),
            (provider, "connect_recovery"),
            (provider, "recovered_select"),
            (provider, "connect_source"),
            (provider, "source_select:source-4"),
        ]
        positions = [timeline.index(entry, started_index + 1) for entry in expected]
        assert positions == sorted(positions)
        assert started_index < positions[0]

        lane_progress = [item for item in progress if item.lane_id == lane_id]
        boundary_progress = [
            item
            for item in lane_progress
            if item.phase
            in {
                RecoveryPhase.WAITING_RECOVERY_POINT,
                RecoveryPhase.RESTORING,
                RecoveryPhase.CONNECTING,
                RecoveryPhase.VERIFYING_RECOVERED_ORDER,
                RecoveryPhase.VERIFYING_SOURCE,
                RecoveryPhase.VERIFIED,
            }
        ]
        assert boundary_progress
        assert all(
            item.recovery_at == result.lanes[lane_id].recovery_at
            for item in boundary_progress
        )
        deleting = next(
            item
            for item in lane_progress
            if item.phase == RecoveryPhase.DELETING_INCIDENT
        )
        source_proof = next(
            item
            for item in lane_progress
            if item.phase == RecoveryPhase.VERIFYING_SOURCE
        )
        assert deleting.wire_call == "PostgreSQL DELETE + clock_timestamp() → COMMIT"
        assert source_proof.wire_call == (
            "PostgreSQL TLS reconnect → SELECT public.orders"
        )


async def test_towel_stops_only_unfinished_recovery_lane_during_eligibility() -> None:
    engine, lakebase, aurora, _, _ = build_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    eligibility_started = asyncio.Event()
    keep_waiting = asyncio.Event()

    async def blocked_eligibility(plan, recovery_at, report):
        eligibility_started.set()
        await keep_waiting.wait()

    aurora.wait_recovery_point = blocked_eligibility
    stop = RecoveryStopControl()
    run = asyncio.create_task(engine.run(arm, stop_control=stop))
    await asyncio.wait_for(eligibility_started.wait(), timeout=1)
    for _ in range(100):
        if "lakebase" in stop.completed_lanes:
            break
        await asyncio.sleep(0)
    assert stop.completed_lanes["lakebase"].ok is True

    stop.request((stop.started_ns or 0) + 90_005_678_901)
    result = await asyncio.wait_for(run, timeout=1)

    assert isinstance(result, RecoveryStoppedResult)
    assert set(result.lanes) == {"lakebase"}
    assert result.active_lane == "competitor"
    assert result.restore_started is False
    assert result.cutoff_ns == (stop.started_ns or 0) + 90_005_678_901
    assert all(connection.closed for connection in aurora.connections)
    assert all(connection.closed for connection in lakebase.connections)


async def test_towel_stops_competitor_during_connect_after_restore_started() -> None:
    """A towel thrown after the restore landed must also stop the billing.

    Stopping the lane is only half of it. The restore exists by this point, and
    the cooldown's `reset` is the only thing that removes it -- so this carries
    on past `run` and asserts the provider account is empty, because a towel
    that reports cleanly over a live PITR clone is the expensive failure.
    """

    engine, lakebase, aurora, _, timeline = build_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    connect_started = asyncio.Event()
    connect_cancelled = asyncio.Event()
    keep_connecting = asyncio.Event()
    progress = []

    async def blocked_connect(plan, artifact):
        timeline.append((aurora.provider.value, "connect_recovery"))
        connect_started.set()
        try:
            await keep_connecting.wait()
        finally:
            connect_cancelled.set()

    async def on_progress(item) -> None:
        progress.append(item)

    aurora.connect_recovery = blocked_connect
    stop = RecoveryStopControl()
    run = asyncio.create_task(
        engine.run(arm, on_progress=on_progress, stop_control=stop)
    )
    await asyncio.wait_for(connect_started.wait(), timeout=1)
    for _ in range(100):
        if "lakebase" in stop.completed_lanes:
            break
        await asyncio.sleep(0)
    assert stop.completed_lanes["lakebase"].ok is True

    cutoff_ns = (stop.started_ns or 0) + 90_005_678_901
    stop.request(cutoff_ns)
    result = await asyncio.wait_for(run, timeout=1)

    assert isinstance(result, RecoveryStoppedResult)
    assert set(result.lanes) == {"lakebase"}
    assert result.active_lane == "competitor"
    assert result.restore_started is True
    assert result.cutoff_ns == cutoff_ns
    assert connect_cancelled.is_set()
    competitor_phases = [
        item.phase for item in progress if item.lane_id == "competitor"
    ]
    assert RecoveryPhase.CONNECTING in competitor_phases
    assert RecoveryPhase.VERIFYING_RECOVERED_ORDER not in competitor_phases
    assert all(connection.closed for connection in aurora.connections)
    assert all(connection.closed for connection in lakebase.connections)

    # The competitor lane was cancelled mid-connect, so its abandon fired; the
    # Lakebase lane finished normally and its branch is still live. Either way
    # the cooldown has to leave nothing behind, and it has to say so.
    reset = await engine.reset(CompetitorId.AURORA_SERVERLESS_V2)

    assert reset.ok is True
    assert lakebase.provider_account == set()
    assert aurora.provider_account == set()


async def test_verified_competitor_is_authoritative_before_connection_cleanup() -> None:
    engine, lakebase, aurora, _, _ = build_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    original_connect_source = aurora.connect_source
    progress = []

    async def connect_source_with_blocked_verification_cleanup(plan):
        connection = await original_connect_source(plan)
        if len(aurora.connections) == 4:
            original_close = connection.close

            async def blocked_close() -> None:
                cleanup_started.set()
                await allow_cleanup.wait()
                await original_close()

            connection.close = blocked_close
        return connection

    async def on_progress(item) -> None:
        progress.append(item)

    aurora.connect_source = connect_source_with_blocked_verification_cleanup
    stop = RecoveryStopControl()
    run = asyncio.create_task(engine.run(arm, on_progress=on_progress, stop_control=stop))

    await asyncio.wait_for(cleanup_started.wait(), timeout=1)

    assert stop.completed_lanes["competitor"].ok is True
    assert any(
        item.lane_id == "competitor" and item.phase == RecoveryPhase.VERIFIED
        for item in progress
    )

    allow_cleanup.set()
    result = await asyncio.wait_for(run, timeout=1)
    assert result.lanes["competitor"].ok is True
    assert all(connection.closed for connection in aurora.connections)
    assert all(connection.closed for connection in lakebase.connections)


async def test_failed_competitor_is_authoritative_before_failed_progress() -> None:
    engine, _, aurora, _, _ = build_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    failed_progress_started = asyncio.Event()
    allow_failed_progress = asyncio.Event()
    original_wait = aurora.wait_recovery_point

    async def corrupt_recovery(plan, recovery_at, report):
        evidence = await original_wait(plan, recovery_at, report)
        aurora.recovery_row = ("wrong@example.com", 1, "wrong", datetime.now(UTC))
        return evidence

    async def on_progress(item) -> None:
        if item.lane_id == "competitor" and item.phase == RecoveryPhase.FAILED:
            failed_progress_started.set()
            await allow_failed_progress.wait()

    aurora.wait_recovery_point = corrupt_recovery
    stop = RecoveryStopControl()
    run = asyncio.create_task(engine.run(arm, on_progress=on_progress, stop_control=stop))

    await asyncio.wait_for(failed_progress_started.wait(), timeout=1)
    assert "competitor" in stop.terminal_lanes
    assert "competitor" not in stop.completed_lanes

    allow_failed_progress.set()
    result = await asyncio.wait_for(run, timeout=1)
    assert result.lanes["competitor"].ok is False


async def test_arm_accepts_exact_existing_row_but_rejects_mismatched_payload() -> None:
    engine, lakebase, aurora, _, _ = build_engine()
    lakebase.source_row = engine.contract.row
    lakebase.historical_row = engine.contract.row
    aurora.source_row = (
        "someone-else@example.com",
        1,
        "foreign",
        engine.contract.created_at,
    )

    with pytest.raises(RecoveryNotArmedError, match="different data"):
        await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    assert not any(
        event == "insert"
        for provider, event in lakebase.timeline
        if provider == "lakebase"
    )
    assert aurora.source_row[0] == "someone-else@example.com"


async def test_default_recovery_order_is_deterministic_and_run_owned() -> None:
    first, *_ = build_engine()
    repeated, *_ = build_engine()
    different, *_ = build_engine()
    different.scope = SafeChangeOwnershipScope(
        run_id="ad-test-004",
        owner=different.scope.owner,
        aws_account_id=different.scope.aws_account_id,
        aws_region=different.scope.aws_region,
    )
    different = RecoveryEngine(
        scope=different.scope,
        lakebase=different.lakebase,
        competitors=different.competitors,
    )

    assert first.contract.order_id == repeated.contract.order_id
    assert first.contract.order_id != different.contract.order_id
    assert first.contract.parameters[0] == first.contract.order_id


async def test_reset_refuses_mismatched_row_before_deleting_artifact() -> None:
    engine, lakebase, _, _, _ = build_engine()
    plan, _ = engine.plans_for(CompetitorId.AURORA_SERVERLESS_V2)
    lakebase.artifact = ArtifactInspection(
        artifact_id=plan.artifact_id,
        provider=plan.provider,
        source_id=plan.source_id,
        run_id=engine.scope.run_id,
        owner=engine.scope.owner,
        state="READY",
    )
    lakebase.source_row = (
        "someone-else@example.com",
        1,
        "foreign",
        engine.contract.created_at,
    )

    with pytest.raises(RecoveryResetError):
        await engine.reset(CompetitorId.AURORA_SERVERLESS_V2)

    assert lakebase.delete_calls == 0
    assert lakebase.artifact is not None


# ---------------------------------------------------------------------------
# Cancellation.
#
# Round 3 has the same shape as Round 2: `create_recovery` shields the restore
# and the writer creation but not the readiness waits between them, and
# `delete_recovery` runs from the cooldown task after `run` returns. A lane
# cancelled during a readiness wait therefore leaks its whole PITR restore.
# ---------------------------------------------------------------------------


async def cancel_recovery_during_readiness_wait(engine, adapters):
    gate = asyncio.Event()
    for adapter in adapters:
        adapter.hold_create = gate
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    run_task = asyncio.create_task(engine.run(arm))
    await asyncio.gather(
        *(
            asyncio.wait_for(adapter.reached_readiness_wait.wait(), timeout=1)
            for adapter in adapters
        )
    )
    run_task.cancel()
    return run_task


async def test_recovery_cancelled_during_the_readiness_wait_issues_the_delete() -> None:
    engine, lakebase, aurora, _, _ = build_engine()

    run_task = await cancel_recovery_during_readiness_wait(engine, (lakebase, aurora))

    with pytest.raises(asyncio.CancelledError):
        await run_task
    assert lakebase.provider_account == set()
    assert aurora.provider_account == set()
    assert lakebase.abandoned == ["recovery-ad-test-003"]
    assert aurora.abandoned == ["adrc-ad-test-003-aurora"]


async def test_recovery_cancellation_propagates_when_the_teardown_raises() -> None:
    engine, lakebase, aurora, _, _ = build_engine()
    for adapter in (lakebase, aurora):
        adapter.abandon_behaviour = "raise"

    run_task = await cancel_recovery_during_readiness_wait(engine, (lakebase, aurora))

    with pytest.raises(asyncio.CancelledError):
        await run_task
    assert run_task.cancelled() is True
    assert lakebase.abandon_calls == 1
    assert aurora.abandon_calls == 1


async def test_recovery_teardown_cannot_stall_the_cancellation() -> None:
    engine, lakebase, aurora, _, _ = build_engine(cancel_teardown_timeout_seconds=0.05)
    for adapter in (lakebase, aurora):
        adapter.abandon_behaviour = "hang"

    run_task = await cancel_recovery_during_readiness_wait(engine, (lakebase, aurora))

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(run_task), timeout=2)
    assert lakebase.abandon_calls == 1
    assert aurora.abandon_calls == 1


async def test_a_verified_recovery_run_never_reaches_the_cancellation_teardown() -> None:
    engine, lakebase, aurora, _, _ = build_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm)

    assert result.all_verified is True
    assert lakebase.abandon_calls == 0
    assert aurora.abandon_calls == 0
    assert lakebase.provider_account == {"recovery-ad-test-003"}
    assert aurora.provider_account == {"adrc-ad-test-003-aurora"}


async def test_a_failed_recovery_lane_never_reaches_the_cancellation_teardown() -> None:
    engine, lakebase, aurora, _, _ = build_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    async def refuse(plan, artifact):
        raise RuntimeError("recovery endpoint contract rejected")

    aurora.connect_recovery = refuse  # type: ignore[method-assign]
    result = await engine.run(arm)

    assert result.lanes["competitor"].ok is False
    assert aurora.abandon_calls == 0
    assert lakebase.abandon_calls == 0
