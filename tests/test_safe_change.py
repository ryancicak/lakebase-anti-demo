import asyncio
import copy
import logging
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from server.models import CompetitorId
from server.safe_change import (
    SAFE_CHANGE_MIGRATION_PATH,
    ArtifactInspection,
    SafeChangeAdapter,
    SafeChangeContract,
    SafeChangeEngine,
    SafeChangeLaneState,
    SafeChangeNotArmedError,
    SafeChangeOwnershipScope,
    SafeChangePhase,
    SafeChangePlan,
    SafeChangeProvider,
    SafeChangeResetError,
    abandon_on_cancel,
)


@dataclass
class FakeDatabase:
    baseline_email: str = "ringside@example.com"
    baseline_total: int = 4299
    baseline_status: str = "ready"
    has_delivery_column: bool = False
    rows: dict[str, tuple[str, int, str, str | None]] = field(default_factory=dict)


class FakeConnection:
    def __init__(self, database: FakeDatabase, contract: SafeChangeContract) -> None:
        self.database = database
        self.contract = contract
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False
        self._before_transaction: FakeDatabase | None = None

    async def execute(self, statement: str, parameters=()) -> None:
        values = tuple(parameters)
        self.executed.append((statement, values))
        if self._before_transaction is None:
            self._before_transaction = copy.deepcopy(self.database)
        if statement == self.contract.migration_sql:
            self.database.has_delivery_column = True
            return
        if statement == self.contract.application_insert_sql:
            nonce, email, total, status, instructions = values
            if not self.database.has_delivery_column:
                raise RuntimeError("column does not exist")
            self.database.rows[str(nonce)] = (
                str(email),
                int(total),
                str(status),
                str(instructions),
            )
            return
        raise AssertionError("Unexpected SQL statement")

    async def fetch_one(self, statement: str, parameters=()):
        values = tuple(parameters)
        if statement == self.contract.baseline_sql:
            return (
                self.database.baseline_email,
                self.database.baseline_total,
                self.database.baseline_status,
            )
        if statement == self.contract.column_sql:
            return ("text", "YES") if self.database.has_delivery_column else None
        if statement == self.contract.migrated_baseline_sql:
            return (None,) if self.database.has_delivery_column else None
        if statement == self.contract.application_readback_sql:
            return self.database.rows.get(str(values[0]))
        if statement == self.contract.nonce_count_sql:
            return (int(str(values[0]) in self.database.rows),)
        raise AssertionError("Unexpected SQL query")

    async def commit(self) -> None:
        self._before_transaction = None

    async def rollback(self) -> None:
        if self._before_transaction is not None:
            restored = self._before_transaction
            self.database.baseline_email = restored.baseline_email
            self.database.baseline_total = restored.baseline_total
            self.database.baseline_status = restored.baseline_status
            self.database.has_delivery_column = restored.has_delivery_column
            self.database.rows = restored.rows
            self._before_transaction = None

    async def close(self) -> None:
        self.closed = True


class FakeAdapter(SafeChangeAdapter):
    def __init__(
        self,
        *,
        provider: SafeChangeProvider,
        name: str,
        source_id: str,
        scope: SafeChangeOwnershipScope,
        contract: SafeChangeContract,
        create_delay: float = 0,
    ) -> None:
        self.provider = provider
        self.name = name
        self.source_id = source_id
        self.scope = scope
        self.contract = contract
        self.create_delay = create_delay
        self.source = FakeDatabase()
        self.isolated: FakeDatabase | None = None
        self.artifact: ArtifactInspection | None = None
        self.create_started: float | None = None
        self.create_calls = 0
        self.delete_calls = 0
        self.source_connections: list[FakeConnection] = []
        self.isolated_connections: list[FakeConnection] = []
        self.mutate_source_after_create = False
        self.settle_calls = 0
        self.pending_mutation = False
        # Stands in for the provider account: whatever is in here is costing
        # money. `create_isolated` fills it before its readiness wait, exactly
        # as the live adapters do, so a cancelled lane that fails to tear down
        # leaves a non-empty set.
        self.provider_account: set[str] = set()
        self.abandon_calls = 0
        self.abandoned: list[str] = []
        # "ok" | "raise" | "hang"
        self.abandon_behaviour = "ok"
        self.reached_readiness_wait = asyncio.Event()
        self.entered_create = asyncio.Event()
        self.hold_create: asyncio.Event | None = None
        self.hold_before_create = False

    async def settle_pending_mutations(self) -> None:
        self.settle_calls += 1
        self.pending_mutation = False

    async def abandon_isolated(self, plan: SafeChangePlan) -> None:
        self.abandon_calls += 1
        self.abandoned.append(plan.artifact_id)
        if self.abandon_behaviour == "hang":
            await asyncio.Event().wait()
        if self.abandon_behaviour == "raise":
            raise RuntimeError("the control plane refused the delete")
        # `discard`, not `remove`: deleting something that was never created is
        # the normal case when cancellation beats creation.
        self.provider_account.discard(plan.artifact_id)

    async def preflight(self, plan: SafeChangePlan) -> dict[str, object]:
        return {"source": plan.source_id, "ready": True}

    async def inspect_artifact(self, plan: SafeChangePlan) -> ArtifactInspection | None:
        if self.pending_mutation:
            raise AssertionError("artifact inspected before pending mutation settled")
        return self.artifact

    async def create_isolated(self, plan: SafeChangePlan, report) -> ArtifactInspection:
        self.create_calls += 1
        self.create_started = asyncio.get_running_loop().time()
        self.entered_create.set()
        if self.hold_create is not None and self.hold_before_create:
            await self.hold_create.wait()
        await report("Control-plane create submitted")
        await asyncio.sleep(self.create_delay)
        # The shielded mutation has landed; the resource now exists and bills.
        self.provider_account.add(plan.artifact_id)
        if self.hold_create is not None and not self.hold_before_create:
            # The unshielded readiness poll, which is where the leak happens.
            self.reached_readiness_wait.set()
            await self.hold_create.wait()
        self.isolated = copy.deepcopy(self.source)
        self.artifact = self._inspection(plan)
        if self.mutate_source_after_create:
            self.source.has_delivery_column = True
        return self.artifact

    async def connect_source(self, plan: SafeChangePlan) -> FakeConnection:
        connection = FakeConnection(self.source, self.contract)
        self.source_connections.append(connection)
        return connection

    async def connect_isolated(
        self,
        plan: SafeChangePlan,
        artifact: ArtifactInspection,
    ) -> FakeConnection:
        assert self.isolated is not None
        connection = FakeConnection(self.isolated, self.contract)
        self.isolated_connections.append(connection)
        return connection

    async def delete_isolated(self, plan: SafeChangePlan, artifact, report) -> None:
        self.delete_calls += 1
        await report("Control-plane delete submitted")
        self.artifact = None
        self.isolated = None

    def _inspection(self, plan: SafeChangePlan) -> ArtifactInspection:
        return ArtifactInspection(
            artifact_id=plan.artifact_id,
            provider=self.provider,
            source_id=self.source_id,
            run_id=self.scope.run_id,
            owner=self.scope.owner,
            state="available",
            aws_account_id=(
                None
                if self.provider == SafeChangeProvider.LAKEBASE
                else self.scope.aws_account_id
            ),
            aws_region=(
                None if self.provider == SafeChangeProvider.LAKEBASE else self.scope.aws_region
            ),
        )


def make_engine(
    *,
    lakebase_delay: float = 0,
    competitor_delay: float = 0,
    cancel_teardown_timeout_seconds: float = 5.0,
):
    scope = SafeChangeOwnershipScope(
        run_id="ad-test-001",
        owner="operator@databricks.com",
        aws_account_id="123456789012",
        aws_region="us-west-2",
    )
    contract = SafeChangeContract()
    lakebase = FakeAdapter(
        provider=SafeChangeProvider.LAKEBASE,
        name="Lakebase",
        source_id="projects/ad-test-001/branches/production",
        scope=scope,
        contract=contract,
        create_delay=lakebase_delay,
    )
    aurora = FakeAdapter(
        provider=SafeChangeProvider.AURORA,
        name="Aurora Serverless v2",
        source_id="anti-demo-aurora",
        scope=scope,
        contract=contract,
        create_delay=competitor_delay,
    )
    rds = FakeAdapter(
        provider=SafeChangeProvider.RDS,
        name="RDS PostgreSQL",
        source_id="anti-demo-rds",
        scope=scope,
        contract=contract,
        create_delay=competitor_delay,
    )
    engine = SafeChangeEngine(
        scope=scope,
        lakebase=lakebase,
        competitors={
            CompetitorId.AURORA_SERVERLESS_V2: aurora,
            CompetitorId.RDS_POSTGRES: rds,
        },
        contract=contract,
        cancel_teardown_timeout_seconds=cancel_teardown_timeout_seconds,
        nonce_factory=lambda: "11111111-1111-1111-1111-111111111111",
    )
    return engine, lakebase, aurora, rds


def test_contract_uses_the_canonical_round_two_migration_file() -> None:
    contract = SafeChangeContract()

    assert SAFE_CHANGE_MIGRATION_PATH == (
        Path(__file__).resolve().parents[1] / "sql" / "003_add_delivery_instructions.sql"
    )
    assert contract.migration_sql == SAFE_CHANGE_MIGRATION_PATH.read_text(encoding="utf-8")
    assert contract.baseline_order_id == "00000000-0000-4000-8000-000000000001"


async def test_arm_preflights_both_sources_and_builds_deterministic_plans() -> None:
    engine, lakebase, aurora, _ = make_engine()
    progress = []

    async def capture(event) -> None:
        progress.append(event)

    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2, capture)

    assert set(arm.lanes) == {"lakebase", "competitor"}
    assert arm.contract_sha256 == engine.contract.sha256
    assert arm.lanes["lakebase"].plan.artifact_id == "safe-change-ad-test-001"
    assert arm.lanes["competitor"].plan.artifact_id == "adsc-ad-test-001-aurora"
    assert lakebase.source_connections[-1].closed is True
    assert aurora.source_connections[-1].closed is True
    assert [event.phase for event in progress].count(SafeChangePhase.ARMED) == 2


async def test_reset_settles_both_lanes_before_cleanup_inspection() -> None:
    engine, lakebase, aurora, _ = make_engine()
    lakebase_plan, aurora_plan = engine.plans_for(CompetitorId.AURORA_SERVERLESS_V2)
    lakebase.artifact = lakebase._inspection(lakebase_plan)
    aurora.artifact = aurora._inspection(aurora_plan)
    lakebase.pending_mutation = True
    aurora.pending_mutation = True

    await engine.reset(CompetitorId.AURORA_SERVERLESS_V2)

    assert lakebase.settle_calls == 1
    assert aurora.settle_calls == 1
    assert lakebase.artifact is None
    assert aurora.artifact is None


async def test_arm_clears_only_owned_stale_artifacts_before_preflight() -> None:
    engine, lakebase, aurora, _ = make_engine()
    lakebase_plan, aurora_plan = engine.plans_for(CompetitorId.AURORA_SERVERLESS_V2)
    lakebase.artifact = lakebase._inspection(lakebase_plan)
    aurora.artifact = aurora._inspection(aurora_plan)
    progress = []

    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2, progress.append)

    assert set(arm.lanes) == {"lakebase", "competitor"}
    assert lakebase.delete_calls == 1
    assert aurora.delete_calls == 1
    assert lakebase.artifact is None
    assert aurora.artifact is None
    assert [event.phase for event in progress].count(SafeChangePhase.RESET) == 2


async def test_run_uses_one_barrier_and_the_identical_full_sql_proof() -> None:
    engine, lakebase, aurora, _ = make_engine(
        lakebase_delay=0.005,
        competitor_delay=0.025,
    )
    progress = []

    async def capture(event) -> None:
        progress.append(event)

    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm, capture)

    assert result.all_verified is True
    assert result.lanes["lakebase"].state == SafeChangeLaneState.VERIFIED
    assert result.lanes["competitor"].state == SafeChangeLaneState.VERIFIED
    assert result.lanes["lakebase"].elapsed_ms < result.lanes["competitor"].elapsed_ms
    assert result.launch_skew_ms < 20
    assert result.nonce == "11111111-1111-1111-1111-111111111111"
    assert lakebase.create_started is not None and aurora.create_started is not None
    assert abs(lakebase.create_started - aurora.create_started) < 0.02

    lakebase_sql = lakebase.isolated_connections[-1].executed
    aurora_sql = aurora.isolated_connections[-1].executed
    assert lakebase_sql == aurora_sql
    assert [statement for statement, _ in lakebase_sql] == [
        engine.contract.migration_sql,
        engine.contract.application_insert_sql,
    ]
    assert lakebase.source.has_delivery_column is False
    assert aurora.source.has_delivery_column is False
    assert result.nonce not in lakebase.source.rows
    assert result.nonce not in aurora.source.rows
    assert any(event.phase == SafeChangePhase.VERIFYING_SOURCE for event in progress)


async def test_lane_fails_if_the_source_changes_during_the_round() -> None:
    engine, _, aurora, _ = make_engine()
    aurora.mutate_source_after_create = True
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm)

    assert result.lanes["lakebase"].state == SafeChangeLaneState.VERIFIED
    assert result.lanes["competitor"].state == SafeChangeLaneState.FAILED
    assert "source already contains" in result.lanes["competitor"].error.lower()
    assert result.all_verified is False


async def test_lane_error_is_reported_before_the_other_lane_finishes() -> None:
    engine, lakebase, _, _ = make_engine(competitor_delay=0.05)
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    original_connect = lakebase.connect_isolated

    async def fail_connect(*_args):
        raise RuntimeError("isolated endpoint contract rejected")

    lakebase.connect_isolated = fail_connect  # type: ignore[method-assign]
    failure_seen = asyncio.Event()
    failures = []

    async def capture(progress) -> None:
        if progress.phase == SafeChangePhase.FAILED:
            failures.append(progress)
            failure_seen.set()

    run_task = asyncio.create_task(engine.run(arm, capture))
    await asyncio.wait_for(failure_seen.wait(), timeout=0.1)

    assert run_task.done() is False
    assert failures[0].lane_id == "lakebase"
    assert failures[0].error == "isolated endpoint contract rejected"
    result = await run_task
    assert result.lanes["lakebase"].error == failures[0].error
    lakebase.connect_isolated = original_connect  # type: ignore[method-assign]


async def test_failed_run_can_rearm_by_cleaning_only_owned_artifacts() -> None:
    engine, lakebase, aurora, _ = make_engine(competitor_delay=0.01)
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    original_connect = lakebase.connect_isolated

    async def fail_connect(*_args):
        raise RuntimeError("one-time isolated connection failure")

    lakebase.connect_isolated = fail_connect  # type: ignore[method-assign]
    failed = await engine.run(arm)
    assert failed.all_verified is False
    assert lakebase.artifact is not None
    assert aurora.artifact is not None

    lakebase.connect_isolated = original_connect  # type: ignore[method-assign]
    retry_arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    assert set(retry_arm.lanes) == {"lakebase", "competitor"}
    assert lakebase.delete_calls == 1
    assert aurora.delete_calls == 1
    assert lakebase.artifact is None
    assert aurora.artifact is None


async def test_run_revalidates_and_refuses_an_artifact_created_after_arm() -> None:
    engine, _, aurora, _ = make_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    plan = arm.lanes["competitor"].plan
    aurora.artifact = aurora._inspection(plan)

    with pytest.raises(SafeChangeNotArmedError, match="Start state changed before the bell"):
        await engine.run(arm)

    assert aurora.create_calls == 0


async def test_reset_deletes_only_deterministic_owned_artifacts_and_is_idempotent() -> None:
    engine, lakebase, _, rds = make_engine()
    arm = await engine.arm(CompetitorId.RDS_POSTGRES)
    result = await engine.run(arm)
    assert result.all_verified is True

    first = await engine.reset(CompetitorId.RDS_POSTGRES)
    second = await engine.reset(CompetitorId.RDS_POSTGRES)

    assert first.ok is True
    assert lakebase.delete_calls == 1
    assert rds.delete_calls == 1
    assert all(lane.already_absent for lane in second.lanes.values())
    assert lakebase.delete_calls == 1
    assert rds.delete_calls == 1


async def test_reset_refuses_wrong_ownership_without_calling_delete() -> None:
    engine, _, aurora, _ = make_engine()
    plan = engine.plans_for(CompetitorId.AURORA_SERVERLESS_V2)[1]
    valid = aurora._inspection(plan)
    aurora.artifact = ArtifactInspection(
        **{**valid.__dict__, "owner": "someone-else@databricks.com"}
    )

    with pytest.raises(SafeChangeResetError) as raised:
        await engine.reset(CompetitorId.AURORA_SERVERLESS_V2)

    result = raised.value.result
    assert result.lanes["lakebase"].ok is True
    assert result.lanes["competitor"].ok is False
    assert "owner" in result.lanes["competitor"].error.lower()
    assert aurora.delete_calls == 0


async def test_arm_rejects_a_source_that_already_contains_the_change() -> None:
    engine, _, _, rds = make_engine()
    rds.source.has_delivery_column = True

    with pytest.raises(SafeChangeNotArmedError, match="proposed schema change"):
        await engine.arm(CompetitorId.RDS_POSTGRES)


# ---------------------------------------------------------------------------
# Cancellation.
#
# Creation mutations are shielded, the readiness waits between them are not,
# and ordinary teardown runs from the cooldown task after `run` returns. A lane
# cancelled during a readiness wait therefore used to unwind without anyone
# ever issuing a delete, which is how one Aurora writer lived fifty-seven
# minutes instead of six.
#
# The hazard in fixing that is the opposite failure: a shutdown that will not
# finish. A leak costs money and is recoverable; a stop that appears to hang
# gets killed, which loses the teardown and the log line naming the orphan.
# Every test below that asserts a delete was issued is paired with one
# asserting the cancellation still arrives, promptly.
# ---------------------------------------------------------------------------


async def cancel_during_readiness_wait(engine, adapters):
    """Cancel a run once every lane is parked in its readiness wait."""

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


async def test_cancellation_during_the_readiness_wait_issues_the_delete() -> None:
    engine, lakebase, aurora, _ = make_engine()

    run_task = await cancel_during_readiness_wait(engine, (lakebase, aurora))

    with pytest.raises(asyncio.CancelledError):
        await run_task
    # The whole point: nothing is left behind billing.
    assert lakebase.provider_account == set()
    assert aurora.provider_account == set()
    assert lakebase.abandoned == ["safe-change-ad-test-001"]
    assert aurora.abandoned == ["adsc-ad-test-001-aurora"]


async def test_cancellation_before_creation_issues_nothing_that_can_fail() -> None:
    """Cancellation can beat the create call; deleting nothing must be quiet.

    The lane cannot know whether its shielded mutation landed, so it asks for
    teardown regardless. That request has to be safe when the answer is "there
    was never anything there".
    """

    engine, lakebase, aurora, _ = make_engine()
    gate = asyncio.Event()
    for adapter in (lakebase, aurora):
        adapter.hold_create = gate
        adapter.hold_before_create = True
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    run_task = asyncio.create_task(engine.run(arm))
    await asyncio.gather(
        *(
            asyncio.wait_for(adapter.entered_create.wait(), timeout=1)
            for adapter in (lakebase, aurora)
        )
    )

    run_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await run_task
    assert lakebase.provider_account == set()
    assert aurora.provider_account == set()
    assert lakebase.abandon_calls == 1
    assert aurora.abandon_calls == 1


async def test_cancellation_propagates_even_when_the_teardown_raises() -> None:
    engine, lakebase, aurora, _ = make_engine()
    for adapter in (lakebase, aurora):
        adapter.abandon_behaviour = "raise"

    run_task = await cancel_during_readiness_wait(engine, (lakebase, aurora))

    with pytest.raises(asyncio.CancelledError):
        await run_task
    assert run_task.cancelled() is True
    assert lakebase.abandon_calls == 1
    assert aurora.abandon_calls == 1


async def test_an_unresponsive_control_plane_cannot_stall_the_cancellation() -> None:
    """The bound, which is the requirement that makes this change safe."""

    engine, lakebase, aurora, _ = make_engine(cancel_teardown_timeout_seconds=0.05)
    for adapter in (lakebase, aurora):
        adapter.abandon_behaviour = "hang"

    run_task = await cancel_during_readiness_wait(engine, (lakebase, aurora))

    # Generous next to the 0.05s bound, unreachable if the shield is unbounded.
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(run_task), timeout=2)
    assert lakebase.abandon_calls == 1
    assert aurora.abandon_calls == 1


async def test_a_lane_cancelled_while_a_sibling_hangs_still_tears_down() -> None:
    """Both lanes get their teardown; one slow adapter does not starve the other."""

    engine, lakebase, aurora, _ = make_engine(cancel_teardown_timeout_seconds=0.05)
    lakebase.abandon_behaviour = "hang"

    run_task = await cancel_during_readiness_wait(engine, (lakebase, aurora))

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(run_task), timeout=2)
    assert lakebase.provider_account == {"safe-change-ad-test-001"}
    assert aurora.provider_account == set()


async def test_a_verified_run_never_reaches_the_cancellation_teardown() -> None:
    engine, lakebase, aurora, _ = make_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm)

    assert result.all_verified is True
    assert lakebase.abandon_calls == 0
    assert aurora.abandon_calls == 0
    # Teardown still belongs to reset, exactly as before.
    assert lakebase.provider_account == {"safe-change-ad-test-001"}
    assert aurora.provider_account == {"adsc-ad-test-001-aurora"}


async def test_a_failed_run_never_reaches_the_cancellation_teardown() -> None:
    engine, _, aurora, _ = make_engine()
    aurora.mutate_source_after_create = True
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    result = await engine.run(arm)

    assert result.lanes["competitor"].state == SafeChangeLaneState.FAILED
    assert aurora.abandon_calls == 0
    assert aurora.artifact is not None


async def test_an_adapter_without_a_teardown_hook_cancels_cleanly() -> None:
    """The hook is optional, and a missing one must not become a shutdown error.

    `None` on the instance is how an adapter that never declared the hook looks
    to the engine's lookup, and the surviving lane proves the skip is per-adapter
    rather than an early return out of the whole teardown.
    """

    engine, lakebase, aurora, _ = make_engine()
    lakebase.abandon_isolated = None  # type: ignore[assignment]

    run_task = await cancel_during_readiness_wait(engine, (lakebase, aurora))

    with pytest.raises(asyncio.CancelledError):
        await run_task
    assert lakebase.abandon_calls == 0
    assert aurora.provider_account == set()


# ---------------------------------------------------------------------------
# `abandon_on_cancel` now has four callers and only three of them abandon a
# provider resource. Round 4 abandons a ring lease and a Delta row, neither of
# which anybody should be told to go and confirm the absence of, so the closing
# sentence has to come from the caller.
# ---------------------------------------------------------------------------


async def _orphan_message(caplog: pytest.LogCaptureFixture, **kwargs: object) -> str:
    async def slower_than_the_bound() -> None:
        await asyncio.sleep(0.2)

    caplog.set_level(logging.ERROR, logger="server.safe_change")
    confirmed = await abandon_on_cancel(
        slower_than_the_bound,
        identifier="isolated environment adsc-ad-test-001-aurora",
        timeout_seconds=0.01,
        **kwargs,  # type: ignore[arg-type]
    )
    assert confirmed is False
    message = next(
        record.getMessage() for record in caplog.records if "ORPHAN RISK" in record.getMessage()
    )
    # Let the abandoned task finish so it cannot outlive the event loop.
    await asyncio.sleep(0.25)
    return message


async def test_the_default_orphan_remedy_is_the_wording_the_resource_callers_rely_on(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Characterization: three call sites abandon resources and must not move."""

    message = await _orphan_message(caplog)

    assert "isolated environment adsc-ad-test-001-aurora" in message
    assert "The delete request may yet land; verify the resource is gone." in message


async def test_a_caller_can_supply_a_remedy_for_something_that_is_not_a_resource(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = await _orphan_message(
        caplog, remedy="the next arm re-seeds the sealed baseline, so nothing needs repairing"
    )

    assert "the next arm re-seeds the sealed baseline, so nothing needs repairing." in message
    assert "verify the resource is gone" not in message
