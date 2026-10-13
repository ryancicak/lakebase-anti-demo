"""A provider's own words never reach a Round 2 or 3 card, a receipt or a 409.

botocore spells a refusal as ``User: arn:aws:iam::<account>:... is not authorized to
perform: rds:DeleteDBCluster on resource: arn:aws:rds:...``, and psycopg a lost
connection with the host and role. Rounds 2 and 3 quoted ``str(exc)`` as a lane's
error, a cooldown lane's status and a towel's cleanup failure, and a towel's cost-close
failure kept the cost ledger's raw psycopg text.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from botocore.exceptions import ClientError
from databricks.sdk.errors.platform import PermissionDenied

from server.coordination import InMemoryBoutLeaseStore
from server.manager import RunManager, audience_diagnosis, audience_error_text
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
    # Create a server-package exception to keep its text
    class OurError(RuntimeError):
        __module__ = "server.manager"

    assert audience_error_text(OurError("the delete was refused")) == "the delete was refused"
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


def test_allow_list_redacts_all_provider_packages_not_just_deny_list() -> None:
    """Verify allow-list redacts provider exceptions beyond the old deny-list.

    psycopg_pool, urllib3, httpx, socket, and other provider packages should all
    be redacted, not just boto3, botocore, and psycopg.
    """

    # Create fake exception classes that mimic real provider packages.
    class FakePsycopgPoolError(Exception):
        """Mimic psycopg_pool.PoolTimeout - top-level package is psycopg_pool, not psycopg."""
        __module__ = "psycopg_pool.exceptions"

    class FakeUrllib3Error(Exception):
        """Mimic urllib3.exceptions.HTTPError with host info."""
        __module__ = "urllib3.exceptions"

    class FakeHttpxError(Exception):
        """Mimic httpx.ConnectError with host info."""
        __module__ = "httpx._exceptions"

    class FakeSocketError(OSError):
        """Mimic socket.error naming a host."""
        __module__ = "socket"

    # psycopg_pool error: not in old deny-list, but should be redacted
    psycopg_pool_err = FakePsycopgPoolError(
        "connection timeout at psycopg_pool.example.com (10.0.0.5)"
    )
    text = audience_error_text(psycopg_pool_err)
    assert "psycopg_pool.example.com" not in text
    assert "10.0.0.5" not in text
    assert text.startswith("FakePsycopgPoolError")

    # urllib3 error: not in old deny-list, but should be redacted
    urllib3_err = FakeUrllib3Error(
        "Failed to connect to host https://internal-database.corp:5432"
    )
    text = audience_error_text(urllib3_err)
    assert "internal-database.corp" not in text
    assert "5432" not in text
    assert text.startswith("FakeUrllib3Error")

    # httpx error: not in old deny-list, but should be redacted
    httpx_err = FakeHttpxError("Connection refused at api.internal.example.com")
    text = audience_error_text(httpx_err)
    assert "api.internal.example.com" not in text
    assert text.startswith("FakeHttpxError")

    # socket error: not in old deny-list, but should be redacted
    socket_err = FakeSocketError("Name or service not known: 192.168.1.100")
    text = audience_error_text(socket_err)
    assert "192.168.1.100" not in text
    assert text.startswith("FakeSocketError")


def test_app_exception_keeps_text_in_allow_list() -> None:
    """Verify app's own exceptions keep their text (they're on the allow-list)."""

    class AppError(RuntimeError):
        """Our own exception."""
        __module__ = "server.my_module"

    err = AppError("this is our own error message with a table name")
    text = audience_error_text(err)
    assert text == "this is our own error message with a table name"


def test_databricks_refusal_keeps_text_in_allow_list() -> None:
    """Verify Databricks refusals keep their text (they're on the allow-list)."""

    msg = (
        "PERMISSION_DENIED: User does not have permission to read table main.table1"
    )
    refusal = PermissionDenied(msg)
    text = audience_error_text(refusal)
    assert "User does not have permission" in text
    assert "main.table1" in text


def test_wrapped_provider_error_still_redacted() -> None:
    """Verify wrapped provider errors are still redacted at the outer level."""

    class FakeUrlllib3Error(Exception):
        __module__ = "urllib3.exceptions"

    # Wrap a provider error in our own exception
    class AppWrapperError(RuntimeError):
        __module__ = "server.my_module"

    provider_err = FakeUrlllib3Error("Failed to connect to db.internal.corp (10.0.0.1)")
    wrapper = AppWrapperError("Could not reach backend")
    wrapper.__cause__ = provider_err

    # The outer app error is ours and keeps its text
    text = audience_error_text(wrapper)
    assert text == "Could not reach backend"


def test_provider_error_in_chain_is_redacted_by_diagnosis() -> None:
    """Verify when we ask for audience_diagnosis, provider errors are fully redacted."""

    class FakeUrllib3Error(Exception):
        __module__ = "urllib3.exceptions"

    # Wrap a provider error in our own exception
    class AppWrapperError(RuntimeError):
        __module__ = "server.my_module"

    provider_err = FakeUrllib3Error("Failed to connect to db.internal.corp (10.0.0.1)")
    wrapper = AppWrapperError("Could not reach backend")
    wrapper.__cause__ = provider_err

    # audience_diagnosis shows the chain of types only (no messages, not even our own)
    # because it's for audience-facing text
    text = audience_diagnosis(wrapper)
    # Should show both error types in the chain
    assert "AppWrapperError" in text
    assert "FakeUrllib3Error" in text
    # But NOT the provider's sensitive info or any messages
    assert "db.internal.corp" not in text
    assert "10.0.0.1" not in text
    assert "Failed to connect" not in text


def _raised_in(filename: str, error: BaseException) -> BaseException:
    """``error``, raised from a frame whose code lives at ``filename``."""

    namespace = {"__name__": "boom", "error": error}
    exec(compile("def boom():\n    raise error\n", filename, "exec"), namespace)
    try:
        namespace["boom"]()
    except BaseException as raised:
        return raised
    raise AssertionError("boom() did not raise")


def test_a_builtin_a_library_raised_shows_only_its_type() -> None:
    """asyncio's connect failure names the host and port; a built-in type alone is no pass."""

    refused = _raised_in(
        "/usr/lib/python3.14/asyncio/base_events.py",
        ConnectionRefusedError(61, "Connect call failed ('10.0.1.23', 5432)"),
    )

    text = audience_error_text(refused)

    assert "10.0.1.23" not in text and "5432" not in text
    assert "ConnectionRefusedError" in text


def test_a_builtin_this_server_raised_keeps_its_text() -> None:
    """The server raises built-ins with its own sentences, nearly a thousand times."""

    timeout = _raised_in(
        str(Path(__file__).resolve().parents[1] / "server" / "round4_race_live.py"),
        TimeoutError("the coordination database did not answer within 30s"),
    )

    assert audience_error_text(timeout) == "the coordination database did not answer within 30s"


def test_a_builtin_this_server_built_but_never_raised_keeps_its_text() -> None:
    assert audience_error_text(TimeoutError("Round 4 did not settle in 300s")) == (
        "Round 4 did not settle in 300s"
    )

