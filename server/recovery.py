from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from .models import CompetitorId
from .safe_change import (
    DEFAULT_CANCEL_TEARDOWN_SECONDS,
    ArtifactInspection,
    SafeChangeOwnershipScope,
    SafeChangeProvider,
    SafeChangeResetLaneResult,
    SafeChangeResetResult,
    SafeChangeSqlConnection,
    abandon_on_cancel,
    reset_lane_causes,
)


class RecoveryError(RuntimeError):
    pass


class RecoveryNotArmedError(RecoveryError):
    pass


class RecoveryResetError(RecoveryError):
    """Same fan-in shape as :class:`SafeChangeResetError`; see its docstring."""

    def __init__(self, result: SafeChangeResetResult) -> None:
        self.result = result
        super().__init__("One or more recovery environments could not be reset")

    def underlying_causes(self) -> tuple[BaseException, ...]:
        return reset_lane_causes(self.result)


class RecoveryPhase(StrEnum):
    PREPARING_INCIDENT = "preparing_incident"
    DELETING_INCIDENT = "deleting_incident"
    WAITING_RECOVERY_POINT = "waiting_recovery_point"
    RESTORING = "restoring"
    CONNECTING = "connecting"
    VERIFYING_RECOVERED_ORDER = "verifying_recovered_order"
    VERIFYING_SOURCE = "verifying_source"
    VERIFIED = "verified"
    FAILED = "failed"
    RESETTING = "resetting"
    RESET = "reset"


@dataclass(frozen=True)
class RecoveryContract:
    order_id: str = "00000000-0000-4000-8000-000000000003"
    customer_email: str = "round3-recovery@example.com"
    total_cents: int = 7319
    status: str = "recovery-proof"
    created_at: datetime = datetime(2026, 8, 18, 12, 0, 0, tzinfo=UTC)

    select_sql: str = """
        SELECT customer_email, total_cents, status, created_at
        FROM public.orders
        WHERE order_id = %s
    """
    insert_sql: str = """
        INSERT INTO public.orders (
            order_id, customer_email, total_cents, status, created_at
        ) VALUES (%s, %s, %s, %s, %s)
    """
    delete_exact_sql: str = """
        DELETE FROM public.orders
        WHERE order_id = %s
          AND customer_email = %s
          AND total_cents = %s
          AND status = %s
          AND created_at = %s
    """
    delete_with_boundary_sql: str = """
        WITH boundary_clock AS MATERIALIZED (
         SELECT clock_timestamp() AS observed_at
        ),
        boundary AS MATERIALIZED (
         SELECT observed_at,
                date_trunc('second', observed_at) - interval '1 second' AS recovery_at
         FROM boundary_clock
        ),
        deleted AS (
         DELETE FROM public.orders
         WHERE order_id = %s
           AND customer_email = %s
           AND total_cents = %s
           AND status = %s
           AND created_at = %s
         RETURNING customer_email, total_cents, status, created_at
        )
        SELECT boundary.observed_at, boundary.recovery_at,
               deleted.customer_email, deleted.total_cents,
               deleted.status, deleted.created_at
        FROM boundary CROSS JOIN deleted
    """
    clock_sql: str = "SELECT clock_timestamp()"

    @property
    def row(self) -> tuple[object, ...]:
        return (self.customer_email, self.total_cents, self.status, self.created_at)

    @property
    def parameters(self) -> tuple[object, ...]:
        return (
            self.order_id,
            self.customer_email,
            self.total_cents,
            self.status,
            self.created_at,
        )

    @property
    def sha256(self) -> str:
        values = (
            self.order_id,
            self.customer_email,
            str(self.total_cents),
            self.status,
            self.created_at.isoformat(),
            self.select_sql,
            self.insert_sql,
            self.delete_exact_sql,
            self.delete_with_boundary_sql,
            self.clock_sql,
        )
        return hashlib.sha256("\0".join(values).encode()).hexdigest()


@dataclass(frozen=True)
class RecoveryPlan:
    lane_id: str
    name: str
    provider: SafeChangeProvider
    source_id: str
    artifact_id: str
    scope: SafeChangeOwnershipScope


def deterministic_recovery_artifact_id(
    run_id: str,
    provider: SafeChangeProvider,
) -> str:
    if provider == SafeChangeProvider.LAKEBASE:
        return f"recovery-{run_id}"
    return f"adrc-{run_id}-{provider.value}"


@dataclass(frozen=True)
class RecoveryProgress:
    lane_id: str
    lane_name: str
    phase: RecoveryPhase
    status: str
    occurred_at: datetime
    elapsed_ms: float | None = None
    error: str | None = None
    wire_call: str | None = None
    recovery_at: datetime | None = None


@dataclass(frozen=True)
class RecoveryLaneArm:
    plan: RecoveryPlan
    evidence: Mapping[str, object]


@dataclass(frozen=True)
class RecoveryArm:
    competitor: CompetitorId
    armed_at: datetime
    armed_at_monotonic_ns: int
    contract_sha256: str
    scope: SafeChangeOwnershipScope
    lanes: Mapping[str, RecoveryLaneArm]


@dataclass(frozen=True)
class RecoveryLaneResult:
    lane_id: str
    name: str
    provider: SafeChangeProvider
    elapsed_ms: float
    first_action_ns: int
    completed_ns: int
    artifact_id: str
    ok: bool
    recovery_at: datetime | None = None
    error: str | None = None


@dataclass(frozen=True)
class RecoveryRunResult:
    competitor: CompetitorId
    started_ns: int
    completed_ns: int
    launch_skew_ms: float
    contract_sha256: str
    lanes: Mapping[str, RecoveryLaneResult]

    @property
    def all_verified(self) -> bool:
        return bool(self.lanes) and all(lane.ok for lane in self.lanes.values())


@dataclass(frozen=True)
class RecoveryStoppedResult:
    competitor: CompetitorId
    started_ns: int
    cutoff_ns: int
    launch_skew_ms: float
    contract_sha256: str
    lanes: Mapping[str, RecoveryLaneResult]
    active_lane: str
    restore_started: bool


@dataclass
class RecoveryStopControl:
    event: asyncio.Event = field(default_factory=asyncio.Event)
    cutoff_ns: int | None = None
    started_ns: int | None = None
    active_lane: str = "competitor"
    completed_lanes: dict[str, RecoveryLaneResult] = field(default_factory=dict)
    terminal_lanes: set[str] = field(default_factory=set)
    first_action_ns: dict[str, int] = field(default_factory=dict)
    restore_started_lanes: set[str] = field(default_factory=set)

    def request(self, cutoff_ns: int, active_lane: str = "competitor") -> None:
        if self.cutoff_ns is None:
            self.cutoff_ns = cutoff_ns
            self.active_lane = active_lane
        self.event.set()


class RecoveryReporter(Protocol):
    async def __call__(
        self,
        status: str,
        wire_call: str | None = None,
    ) -> None: ...


class RecoveryAdapter(Protocol):
    provider: SafeChangeProvider
    name: str
    source_id: str

    async def inspect_recovery(self, plan: RecoveryPlan) -> ArtifactInspection | None: ...

    async def create_recovery(
        self,
        plan: RecoveryPlan,
        recovery_at: datetime,
        report: RecoveryReporter,
    ) -> ArtifactInspection: ...

    async def connect_source(self, plan: RecoveryPlan) -> SafeChangeSqlConnection: ...

    async def connect_recovery(
        self,
        plan: RecoveryPlan,
        artifact: ArtifactInspection,
    ) -> SafeChangeSqlConnection: ...

    async def wait_recovery_point(
        self,
        plan: RecoveryPlan,
        recovery_at: datetime,
        report: RecoveryReporter,
    ) -> Mapping[str, object]: ...

    async def delete_recovery(
        self,
        plan: RecoveryPlan,
        artifact: ArtifactInspection,
        report: RecoveryReporter,
    ) -> None: ...

    async def abandon_recovery(self, plan: RecoveryPlan) -> None:
        """Issue teardown for a cancelled lane and return without waiting.

        Optional, and subject to the same contract as
        :meth:`server.safe_change.SafeChangeAdapter.abandon_isolated`: derive
        identifiers from ``plan``, issue the delete requests, do not wait for
        absence, and do not raise when the target is absent or already going.
        """

    async def settle_pending_mutations(self) -> None: ...


ProgressCallback = Callable[[RecoveryProgress], Awaitable[None]]
StartedCallback = Callable[[], Awaitable[None]]


class RecoveryEngine:
    def __init__(
        self,
        *,
        scope: SafeChangeOwnershipScope,
        lakebase: RecoveryAdapter,
        competitors: Mapping[CompetitorId, RecoveryAdapter],
        contract: RecoveryContract | None = None,
        arm_ttl_seconds: float = 60.0,
        run_timeout_seconds: float = 1800.0,
        reset_timeout_seconds: float = 1800.0,
        progress_timeout_seconds: float = 1.0,
        cancel_teardown_timeout_seconds: float = DEFAULT_CANCEL_TEARDOWN_SECONDS,
        clock_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if cancel_teardown_timeout_seconds <= 0:
            raise ValueError("cancel_teardown_timeout_seconds must be positive")
        self.scope = scope
        self.lakebase = lakebase
        self.competitors = dict(competitors)
        self.contract = contract or RecoveryContract(
            order_id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"lakebase-anti-demo:recovery:{scope.run_id}",
                )
            )
        )
        self.arm_ttl_seconds = arm_ttl_seconds
        self.run_timeout_seconds = run_timeout_seconds
        self.reset_timeout_seconds = reset_timeout_seconds
        self.progress_timeout_seconds = progress_timeout_seconds
        self.cancel_teardown_timeout_seconds = cancel_teardown_timeout_seconds
        self._clock_ns = clock_ns

    def plans_for(self, competitor: CompetitorId) -> tuple[RecoveryPlan, RecoveryPlan]:
        challenger = self.competitors[competitor]
        return self._plan("lakebase", self.lakebase), self._plan("competitor", challenger)

    def _plan(self, lane_id: str, adapter: RecoveryAdapter) -> RecoveryPlan:
        return RecoveryPlan(
            lane_id=lane_id,
            name=adapter.name,
            provider=adapter.provider,
            source_id=adapter.source_id,
            artifact_id=deterministic_recovery_artifact_id(
                self.scope.run_id,
                adapter.provider,
            ),
            scope=self.scope,
        )

    def _adapter(self, plan: RecoveryPlan) -> RecoveryAdapter:
        if plan.provider == SafeChangeProvider.LAKEBASE:
            return self.lakebase
        return next(
            adapter
            for adapter in self.competitors.values()
            if adapter.provider == plan.provider
        )

    async def arm(
        self,
        competitor: CompetitorId,
        on_progress: ProgressCallback | None = None,
    ) -> RecoveryArm:
        plans = self.plans_for(competitor)
        results = await asyncio.gather(
            *(self._arm_lane(plan, on_progress) for plan in plans),
            return_exceptions=True,
        )
        errors = [
            f"{plan.name}: {result}"
            for plan, result in zip(plans, results, strict=True)
            if isinstance(result, BaseException)
        ]
        if errors:
            raise RecoveryNotArmedError("Could not arm recovery: " + " · ".join(errors))
        now_ns = self._clock_ns()
        lanes = {
            plan.lane_id: RecoveryLaneArm(
                plan=plan,
                evidence=result,
            )
            for plan, result in zip(plans, results, strict=True)
            if isinstance(result, Mapping)
        }
        return RecoveryArm(
            competitor=competitor,
            armed_at=datetime.now(UTC),
            armed_at_monotonic_ns=now_ns,
            contract_sha256=self.contract.sha256,
            scope=self.scope,
            lanes=lanes,
        )

    async def _arm_lane(
        self,
        plan: RecoveryPlan,
        on_progress: ProgressCallback | None,
    ) -> Mapping[str, object]:
        adapter = self._adapter(plan)
        await self._emit(
            on_progress,
            plan,
            RecoveryPhase.PREPARING_INCIDENT,
            "Committing and aging the exact incident row",
            wire_call="PostgreSQL SELECT → INSERT → COMMIT → clock_timestamp()",
        )
        if await adapter.inspect_recovery(plan) is not None:
            raise RecoveryNotArmedError("Owned recovery environment already exists")
        source = await adapter.connect_source(plan)
        try:
            row = await source.fetch_one(
                self.contract.select_sql,
                (self.contract.order_id,),
            )
            inserted = row is None
            if inserted:
                await source.execute(self.contract.insert_sql, self.contract.parameters)
                await source.commit()
            elif tuple(row) != self.contract.row:
                raise RecoveryNotArmedError(
                    "The deterministic recovery order exists with different data"
                )

            initial_clock = await self._provider_clock(source)
            aged_through = initial_clock.replace(microsecond=0) + timedelta(seconds=2)
            current_clock = initial_clock
            while current_clock < aged_through:
                await asyncio.sleep(0.1)
                current_clock = await self._provider_clock(source)

            row = await source.fetch_one(
                self.contract.select_sql,
                (self.contract.order_id,),
            )
            if tuple(row or ()) != self.contract.row:
                raise RecoveryNotArmedError(
                    "The exact incident row changed while the recovery boundary aged"
                )
            return {
                "artifact_absent": True,
                "exact_incident_committed": True,
                "incident_inserted": inserted,
                "provider_clock_initial": initial_clock.isoformat(),
                "provider_clock_aged_through": current_clock.isoformat(),
            }
        finally:
            await self._close(source)

    async def _provider_clock(
        self,
        connection: SafeChangeSqlConnection,
    ) -> datetime:
        row = await connection.fetch_one(self.contract.clock_sql)
        if not row or not isinstance(row[0], datetime):
            raise RecoveryNotArmedError("Source clock_timestamp() was unavailable")
        observed = row[0]
        return observed.replace(tzinfo=UTC) if observed.tzinfo is None else observed

    async def _delete_exact_row(self, connection: SafeChangeSqlConnection) -> None:
        await connection.execute(self.contract.delete_exact_sql, self.contract.parameters)
        await connection.commit()

    async def run(
        self,
        arm: RecoveryArm,
        on_progress: ProgressCallback | None = None,
        on_started: StartedCallback | None = None,
        stop_control: RecoveryStopControl | None = None,
    ) -> RecoveryRunResult | RecoveryStoppedResult:
        plans = self._validate_arm(arm)
        checks = await asyncio.gather(
            *(self._revalidate(arm.lanes[plan.lane_id]) for plan in plans),
            return_exceptions=True,
        )
        errors = [str(item) for item in checks if isinstance(item, BaseException)]
        if errors:
            await asyncio.gather(
                *(self._close(item) for item in checks if not isinstance(item, BaseException))
            )
            raise RecoveryNotArmedError(
                "Start state changed before the bell: " + " · ".join(errors)
            )
        sources = {
            plan.lane_id: source
            for plan, source in zip(plans, checks, strict=True)
            if not isinstance(source, BaseException)
        }
        barrier = asyncio.Event()
        holder: dict[str, int] = {}
        tasks = {
            plan.lane_id: asyncio.create_task(
                self._run_lane(
                    arm.lanes[plan.lane_id],
                    sources[plan.lane_id],
                    barrier,
                    holder,
                    on_progress,
                    stop_control,
                )
            )
            for plan in plans
        }
        await asyncio.sleep(0)
        started_ns = self._clock_ns()
        holder["started_ns"] = started_ns
        if stop_control is not None:
            stop_control.started_ns = started_ns
        try:
            if on_started is not None:
                await on_started()
        except BaseException:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            await asyncio.gather(*(self._close(source) for source in sources.values()))
            raise
        barrier.set()
        if stop_control is not None:
            stop_wait = asyncio.create_task(stop_control.event.wait())
            pending = set(tasks.values())
            while pending and not stop_control.event.is_set():
                done, _ = await asyncio.wait(
                    [*pending, stop_wait],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                pending.difference_update(task for task in done if task is not stop_wait)
            if stop_control.event.is_set():
                for task in tasks.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks.values(), return_exceptions=True)
                stop_wait.cancel()
                await asyncio.gather(stop_wait, return_exceptions=True)
                cutoff_ns = stop_control.cutoff_ns or self._clock_ns()
                first_actions = list(stop_control.first_action_ns.values())
                launch_skew_ms = (
                    (max(first_actions) - min(first_actions)) / 1_000_000
                    if len(first_actions) > 1
                    else 0.0
                )
                return RecoveryStoppedResult(
                    competitor=arm.competitor,
                    started_ns=started_ns,
                    cutoff_ns=cutoff_ns,
                    launch_skew_ms=launch_skew_ms,
                    contract_sha256=self.contract.sha256,
                    lanes=dict(stop_control.completed_lanes),
                    active_lane=stop_control.active_lane,
                    restore_started=(
                        stop_control.active_lane in stop_control.restore_started_lanes
                    ),
                )
            stop_wait.cancel()
            await asyncio.gather(stop_wait, return_exceptions=True)
        lanes = await asyncio.gather(*tasks.values())
        completed_ns = self._clock_ns()
        first = [lane.first_action_ns for lane in lanes]
        return RecoveryRunResult(
            competitor=arm.competitor,
            started_ns=started_ns,
            completed_ns=completed_ns,
            launch_skew_ms=(max(first) - min(first)) / 1_000_000,
            contract_sha256=self.contract.sha256,
            lanes={lane.lane_id: lane for lane in lanes},
        )

    def _validate_arm(self, arm: RecoveryArm) -> tuple[RecoveryPlan, RecoveryPlan]:
        if arm.scope != self.scope or arm.contract_sha256 != self.contract.sha256:
            raise RecoveryNotArmedError("Recovery arm contract changed")
        age = self._clock_ns() - arm.armed_at_monotonic_ns
        if age < 0 or age > int(self.arm_ttl_seconds * 1e9):
            raise RecoveryNotArmedError("Recovery arm expired; prepare it again")
        plans = self.plans_for(arm.competitor)
        if any(arm.lanes.get(plan.lane_id, None) is None for plan in plans):
            raise RecoveryNotArmedError("Recovery arm does not contain both lanes")
        if any(arm.lanes[plan.lane_id].plan != plan for plan in plans):
            raise RecoveryNotArmedError("Recovery plan changed after arming")
        return plans

    async def _revalidate(
        self,
        armed: RecoveryLaneArm,
    ) -> SafeChangeSqlConnection:
        adapter = self._adapter(armed.plan)
        if await adapter.inspect_recovery(armed.plan) is not None:
            raise RecoveryNotArmedError("Owned recovery environment appeared after arming")
        source = await adapter.connect_source(armed.plan)
        try:
            row = await source.fetch_one(
                self.contract.select_sql,
                (self.contract.order_id,),
            )
            if tuple(row or ()) != self.contract.row:
                raise RecoveryNotArmedError(
                    "Exact incident row changed before the bell"
                )
        except Exception:
            await self._close(source)
            raise
        return source

    async def _run_lane(
        self,
        armed: RecoveryLaneArm,
        source: SafeChangeSqlConnection,
        barrier: asyncio.Event,
        holder: Mapping[str, int],
        on_progress: ProgressCallback | None,
        stop_control: RecoveryStopControl | None,
    ) -> RecoveryLaneResult:
        await barrier.wait()
        started_ns = holder["started_ns"]
        first_action_ns = self._clock_ns()
        plan = armed.plan
        if stop_control is not None:
            stop_control.first_action_ns[plan.lane_id] = first_action_ns
        adapter = self._adapter(plan)
        delete_source: SafeChangeSqlConnection | None = source
        recovered: SafeChangeSqlConnection | None = None
        verification_source: SafeChangeSqlConnection | None = None
        recovery_at: datetime | None = None
        completed_ns: int | None = None
        failure: str | None = None
        verified_result: RecoveryLaneResult | None = None

        async def wait_report(status: str, wire_call: str | None = None) -> None:
            await self._emit(
                on_progress,
                plan,
                RecoveryPhase.WAITING_RECOVERY_POINT,
                status,
                started_ns,
                wire_call=wire_call,
                recovery_at=recovery_at,
            )

        async def restore_report(status: str, wire_call: str | None = None) -> None:
            await self._emit(
                on_progress,
                plan,
                RecoveryPhase.RESTORING,
                status,
                started_ns,
                wire_call=wire_call,
                recovery_at=recovery_at,
            )

        try:
            async with asyncio.timeout(self.run_timeout_seconds):
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.DELETING_INCIDENT,
                    "Deleting the exact incident at the timed boundary",
                    started_ns,
                    wire_call="PostgreSQL DELETE + clock_timestamp() → COMMIT",
                )
                boundary = await delete_source.fetch_one(
                    self.contract.delete_with_boundary_sql,
                    self.contract.parameters,
                )
                recovery_at = self._validate_boundary(boundary)
                await delete_source.commit()
                await self._close(delete_source)
                delete_source = None
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.WAITING_RECOVERY_POINT,
                    "Waiting for the recovery point to become eligible",
                    started_ns,
                    recovery_at=recovery_at,
                )
                await adapter.wait_recovery_point(plan, recovery_at, wait_report)
                if stop_control is not None:
                    stop_control.restore_started_lanes.add(plan.lane_id)
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.RESTORING,
                    "Requesting the point-in-time recovery environment",
                    started_ns,
                    recovery_at=recovery_at,
                )
                artifact = await adapter.create_recovery(
                    plan,
                    recovery_at,
                    restore_report,
                )
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.CONNECTING,
                    "Connecting to the recovery environment",
                    started_ns,
                    wire_call="PostgreSQL TLS connect",
                    recovery_at=recovery_at,
                )
                recovered = await adapter.connect_recovery(plan, artifact)
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.VERIFYING_RECOVERED_ORDER,
                    "Reading back the exact recovered order",
                    started_ns,
                    wire_call="PostgreSQL SELECT public.orders",
                    recovery_at=recovery_at,
                )
                row = await recovered.fetch_one(
                    self.contract.select_sql,
                    (self.contract.order_id,),
                )
                if tuple(row or ()) != self.contract.row:
                    raise RecoveryError("Recovery environment did not contain the exact order")
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.VERIFYING_SOURCE,
                    "Proving the source still reflects the deletion",
                    started_ns,
                    wire_call="PostgreSQL TLS reconnect → SELECT public.orders",
                    recovery_at=recovery_at,
                )
                verification_source = await adapter.connect_source(plan)
                if await verification_source.fetch_one(
                    self.contract.select_sql,
                    (self.contract.order_id,),
                ):
                    raise RecoveryError("Deleted order reappeared in the source")
                completed_ns = self._clock_ns()
                verified_result = RecoveryLaneResult(
                    lane_id=plan.lane_id,
                    name=plan.name,
                    provider=plan.provider,
                    elapsed_ms=(completed_ns - started_ns) / 1_000_000,
                    first_action_ns=first_action_ns,
                    completed_ns=completed_ns,
                    artifact_id=plan.artifact_id,
                    ok=True,
                    recovery_at=recovery_at,
                )
                if stop_control is not None:
                    stop_control.completed_lanes[plan.lane_id] = verified_result
                    stop_control.terminal_lanes.add(plan.lane_id)
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.VERIFIED,
                    "Exact recovered order verified; source deletion preserved",
                    started_ns,
                    elapsed_ns=completed_ns,
                    recovery_at=recovery_at,
                )
        except asyncio.CancelledError:
            # `create_recovery` shields its restore and writer mutations but not
            # the readiness polls between them, so cancellation here can leave a
            # restore that AWS was never told to delete. `delete_recovery` runs
            # from the cooldown task after `run` returns, which a cancelled lane
            # never reaches, so this is the last in-process chance to issue it.
            await self._abandon_lane(plan, adapter)
            raise
        except Exception as exc:
            completed_ns = self._clock_ns()
            failure = str(exc) or type(exc).__name__
            if stop_control is not None:
                # Publish the authoritative terminal outcome before any cleanup or
                # progress callback can block on the manager's session lock.
                stop_control.terminal_lanes.add(plan.lane_id)
        finally:
            await asyncio.gather(
                *(
                    asyncio.shield(self._close(connection))
                    for connection in (recovered, verification_source, delete_source)
                    if connection is not None
                ),
                return_exceptions=True,
            )

        assert completed_ns is not None
        if failure is not None:
            await self._emit(
                on_progress,
                plan,
                RecoveryPhase.FAILED,
                "The recovered order could not be verified",
                started_ns,
                elapsed_ns=completed_ns,
                error=failure,
                recovery_at=recovery_at,
            )
            return RecoveryLaneResult(
                lane_id=plan.lane_id,
                name=plan.name,
                provider=plan.provider,
                elapsed_ms=(completed_ns - started_ns) / 1_000_000,
                first_action_ns=first_action_ns,
                completed_ns=completed_ns,
                artifact_id=plan.artifact_id,
                ok=False,
                recovery_at=recovery_at,
                error=failure,
            )
        assert verified_result is not None
        return verified_result

    async def _abandon_lane(
        self,
        plan: RecoveryPlan,
        adapter: RecoveryAdapter,
    ) -> None:
        abandon = getattr(adapter, "abandon_recovery", None)
        if abandon is None:
            return
        await abandon_on_cancel(
            lambda: abandon(plan),
            identifier=f"{plan.name} recovery environment {plan.artifact_id}",
            timeout_seconds=self.cancel_teardown_timeout_seconds,
        )

    def _validate_boundary(
        self,
        row: Sequence[object] | None,
    ) -> datetime:
        values = tuple(row or ())
        if len(values) != 6:
            raise RecoveryError("The exact incident row was not deleted")
        observed_at, recovery_at, *deleted = values
        if not isinstance(observed_at, datetime) or not isinstance(recovery_at, datetime):
            raise RecoveryError("The provider recovery boundary was not a timestamp")
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        if recovery_at.tzinfo is None:
            recovery_at = recovery_at.replace(tzinfo=UTC)
        expected = observed_at.replace(microsecond=0) - timedelta(seconds=1)
        if recovery_at != expected or recovery_at.microsecond != 0:
            raise RecoveryError("The provider recovery boundary was not the prior full second")
        if tuple(deleted) != self.contract.row:
            raise RecoveryError("The deleted incident payload did not match the contract")
        return recovery_at

    async def reset(
        self,
        competitor: CompetitorId,
        on_progress: ProgressCallback | None = None,
    ) -> SafeChangeResetResult:
        return await self._reset_plans(
            self.plans_for(competitor),
            competitor,
            on_progress,
        )

    async def settle_pending_mutations(self, competitor: CompetitorId) -> None:
        await asyncio.gather(
            *(
                self._settle_adapter(self._adapter(plan))
                for plan in self.plans_for(competitor)
            )
        )

    @staticmethod
    async def _settle_adapter(adapter: RecoveryAdapter) -> None:
        settle = getattr(adapter, "settle_pending_mutations", None)
        if settle is not None:
            await settle()

    async def reset_all(
        self,
        on_progress: ProgressCallback | None = None,
    ) -> SafeChangeResetResult:
        plans = [self._plan("lakebase", self.lakebase)]
        plans.extend(
            self._plan(f"competitor-{competitor.value}", adapter)
            for competitor, adapter in self.competitors.items()
        )
        return await self._reset_plans(tuple(plans), None, on_progress)

    async def _reset_plans(
        self,
        plans: tuple[RecoveryPlan, ...],
        competitor: CompetitorId | None,
        on_progress: ProgressCallback | None,
    ) -> SafeChangeResetResult:
        lanes = await asyncio.gather(
            *(self._reset_lane(plan, on_progress) for plan in plans)
        )
        result = SafeChangeResetResult(
            competitor=competitor,
            lanes={lane.lane_id: lane for lane in lanes},
        )
        if not result.ok:
            raise RecoveryResetError(result)
        return result

    async def _reset_lane(
        self,
        plan: RecoveryPlan,
        on_progress: ProgressCallback | None,
    ) -> SafeChangeResetLaneResult:
        adapter = self._adapter(plan)
        try:
            async with asyncio.timeout(self.reset_timeout_seconds):
                artifact = await adapter.inspect_recovery(plan)

                async def report(status: str, wire_call: str | None = None) -> None:
                    await self._emit(
                        on_progress,
                        plan,
                        RecoveryPhase.RESETTING,
                        status,
                        wire_call=wire_call,
                    )

                source = await adapter.connect_source(plan)
                try:
                    row = await source.fetch_one(
                        self.contract.select_sql,
                        (self.contract.order_id,),
                    )
                    if row is not None and tuple(row) != self.contract.row:
                        raise RecoveryError("Recovery row cleanup refused: payload mismatch")
                    if row is not None:
                        await self._delete_exact_row(source)
                        if await source.fetch_one(
                            self.contract.select_sql,
                            (self.contract.order_id,),
                        ):
                            raise RecoveryError(
                                "Owned synthetic recovery row is still present"
                            )
                finally:
                    await self._close(source)
                if artifact is not None:
                    await adapter.delete_recovery(plan, artifact, report)
                await self._emit(
                    on_progress,
                    plan,
                    RecoveryPhase.RESET,
                    "Owned recovery environment and synthetic order cleared",
                )
                return SafeChangeResetLaneResult(
                    lane_id=plan.lane_id,
                    name=plan.name,
                    provider=plan.provider,
                    artifact_id=plan.artifact_id,
                    ok=True,
                    already_absent=artifact is None,
                )
        except Exception as exc:
            await self._emit(
                on_progress,
                plan,
                RecoveryPhase.FAILED,
                "Recovery cleanup refused or incomplete",
                error=str(exc) or type(exc).__name__,
            )
            return SafeChangeResetLaneResult(
                lane_id=plan.lane_id,
                name=plan.name,
                provider=plan.provider,
                artifact_id=plan.artifact_id,
                ok=False,
                error=str(exc) or type(exc).__name__,
                cause=exc,
            )

    async def _emit(
        self,
        callback: ProgressCallback | None,
        plan: RecoveryPlan,
        phase: RecoveryPhase,
        status: str,
        started_ns: int | None = None,
        *,
        elapsed_ns: int | None = None,
        error: str | None = None,
        wire_call: str | None = None,
        recovery_at: datetime | None = None,
    ) -> None:
        if callback is None:
            return
        current = elapsed_ns if elapsed_ns is not None else self._clock_ns()
        elapsed_ms = (current - started_ns) / 1_000_000 if started_ns is not None else None
        try:
            async with asyncio.timeout(self.progress_timeout_seconds):
                await callback(
                    RecoveryProgress(
                        lane_id=plan.lane_id,
                        lane_name=plan.name,
                        phase=phase,
                        status=status,
                        occurred_at=datetime.now(UTC),
                        elapsed_ms=elapsed_ms,
                        error=error,
                        wire_call=wire_call,
                        recovery_at=recovery_at,
                    )
                )
        except Exception:
            return

    @staticmethod
    async def _close(connection: SafeChangeSqlConnection) -> None:
        try:
            await connection.close()
        except Exception:
            return
