"""A provider's own words never reach a Round 2 or 3 card, a receipt or a 409.

botocore spells a refusal as ``User: arn:aws:iam::<account>:... is not authorized to
perform: rds:DeleteDBCluster on resource: arn:aws:rds:...``, and psycopg a lost
connection with the host and role. Rounds 2 and 3 quoted ``str(exc)`` as a lane's
error, a cooldown lane's status and a towel's cleanup failure, and a towel's cost-close
failure kept the cost ledger's raw psycopg text.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import psycopg
import pytest
from botocore.exceptions import ClientError

from server.coordination import InMemoryBoutLeaseStore
from server.manager import RunManager, audience_error_text
from server.models import CompetitorId, RoundId
from server.recovery import RecoveryResetError
from server.safe_change import SafeChangeResetError
from tests.test_manager import FailingCostLedgerStore, TowelRecoveryEngine, running_round_three
from tests.test_recovery import _owned_artifact
from tests.test_recovery import build_engine as build_recovery_engine
from tests.test_safe_change import make_engine as make_safe_change_engine

ACCOUNT = "123456789012"


def refused(operation: str) -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": (
                    f"User: arn:aws:iam::{ACCOUNT}:user/operator is not authorized to "
                    f"perform: rds:{operation} on resource: "
                    f"arn:aws:rds:us-west-2:{ACCOUNT}:cluster:anti-demo-clone"
                ),
            }
        },
        operation,
    )


def assert_redacted(text: str | None) -> None:
    assert text is not None
    assert ACCOUNT not in text and "arn:aws" not in text
    assert "ClientError[AccessDenied]" in text


def test_audience_error_text_keeps_our_sentences_and_drops_provider_messages() -> None:
    assert audience_error_text(RuntimeError("the delete was refused")) == "the delete was refused"
    assert_redacted(audience_error_text(refused("DeleteDBCluster")))
    lost = psycopg.OperationalError(
        'connection to server at "coordination.example.com" (10.0.0.1), port 5432 '
        'failed: FATAL: password authentication failed for user "anti_demo"'
    )
    text = audience_error_text(lost)
    assert "coordination.example.com" not in text and "anti_demo" not in text
    assert text.startswith("OperationalError")


async def test_round2_reset_lane_names_no_account_or_arn() -> None:
    engine, _, _, rds = make_safe_change_engine()
    arm = await engine.arm(CompetitorId.RDS_POSTGRES)
    assert (await engine.run(arm)).all_verified is True

    async def delete_isolated(plan, artifact, report) -> None:
        raise refused("DeleteDBInstance")

    rds.delete_isolated = delete_isolated
    with pytest.raises(SafeChangeResetError) as raised:
        await engine.reset(CompetitorId.RDS_POSTGRES)

    assert_redacted(raised.value.result.lanes["competitor"].error)


async def test_round2_run_lane_names_no_account_or_arn() -> None:
    engine, _, _, rds = make_safe_change_engine()
    arm = await engine.arm(CompetitorId.RDS_POSTGRES)

    async def create_isolated(plan, report):
        raise refused("CreateDBInstance")

    rds.create_isolated = create_isolated
    result = await engine.run(arm)

    assert_redacted(result.lanes["competitor"].error)


async def test_round3_reset_lane_names_no_account_or_arn() -> None:
    engine, _, aurora, _, _ = build_recovery_engine()
    await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)
    plan = engine.plans_for(CompetitorId.AURORA_SERVERLESS_V2)[1]
    aurora.artifact = _owned_artifact(engine, plan)

    async def delete_recovery(plan, artifact, report) -> None:
        raise refused("DeleteDBCluster")

    aurora.delete_recovery = delete_recovery
    with pytest.raises(RecoveryResetError) as raised:
        await engine.reset(CompetitorId.AURORA_SERVERLESS_V2)

    assert_redacted(raised.value.result.lanes["competitor"].error)


async def test_round3_run_lane_names_no_account_or_arn() -> None:
    engine, _, aurora, _, _ = build_recovery_engine()
    arm = await engine.arm(CompetitorId.AURORA_SERVERLESS_V2)

    async def create_recovery(plan, recovery_at, report):
        raise refused("RestoreDBClusterToPointInTime")

    aurora.create_recovery = create_recovery
    result = await engine.run(arm)

    assert_redacted(result.lanes["competitor"].error)


class DriverRefusingCostLedger(FailingCostLedgerStore):
    """The real ledger's `_run` re-raises psycopg's own error after its one retry."""

    async def close_bout(self, **_: object) -> None:
        self.close_calls += 1
        raise psycopg.OperationalError(
            'connection to server at "coordination.example.com" (10.0.0.1), port 5432 '
            'failed: FATAL: password authentication failed for user "anti_demo"'
        )


async def test_a_towel_cost_close_failure_names_no_host_or_role() -> None:
    engine = TowelRecoveryEngine()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=InMemoryBoutLeaseStore(),
        clock_ns=lambda: 100_005_678_901,
    )
    try:
        created = await running_round_three(manager, engine)
        record = manager._records[created.id]
        manager._cost_ledger_store = DriverRefusingCostLedger()
        record.cost_bout_id = "bout-under-test"
        record.cost_bout_started_at = datetime.now(UTC)
        record.cost_bout_kind = "run"

        accepted = await manager.start_towel(created.id)
    finally:
        await manager.close()

    failure = accepted.towel.cost_close_failure if accepted.towel else None
    assert failure is not None and failure.startswith("OperationalError")
    assert "coordination.example.com" not in failure and "anti_demo" not in failure


async def test_round2_towel_cleanup_failure_names_no_account_or_arn() -> None:
    # The towel's failure is the card's, the receipt's and every later claim's 409.
    async def settle_pending_mutations(competitor) -> None:
        raise refused("DeleteDBCluster")

    competitor = SimpleNamespace(id=CompetitorId.RDS_POSTGRES)
    record = SimpleNamespace(
        safe_change_engine=SimpleNamespace(settle_pending_mutations=settle_pending_mutations),
        snapshot=SimpleNamespace(id="s-1", competitor=competitor),
    )
    manager = RunManager()
    try:
        failure = await manager._settle_towel_once(
            record, RoundId.MAKE_SCHEMA_CHANGE_SAFELY, None, wait_for_run=False
        )
    finally:
        await manager.close()

    assert_redacted(failure)
