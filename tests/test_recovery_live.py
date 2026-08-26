from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import botocore.session
import pytest
from botocore.exceptions import ClientError
from botocore.loaders import Loader
from botocore.model import ServiceModel
from botocore.stub import Stubber
from botocore.validate import ParamValidator

from server.recovery import RecoveryPlan
from server.recovery_live import (
    AuroraRecoveryAdapter,
    LakebaseRecoveryAdapter,
    RdsRecoveryAdapter,
)
from server.safe_change import (
    DEFAULT_CANCEL_TEARDOWN_SECONDS,
    ArtifactInspection,
    SafeChangeOwnershipScope,
    SafeChangeProvider,
)
from server.safe_change_live import (
    LAKEBASE_API_ROOT,
    ControlPlaneCommandError,
    SafeChangeControlPlaneError,
    SafeChangeLiveConfigurationError,
    _AwsSafeChangeAdapter,
    _lakebase_create_path,
    lakebase_resource_path,
)

SCOPE = SafeChangeOwnershipScope(
    run_id="ad-test-003",
    owner="operator@databricks.com",
    aws_account_id="123456789012",
    aws_region="us-west-2",
)
RECOVERY_AT = datetime(2026, 8, 18, 15, 0, 2, tzinfo=UTC)


def plan(provider: SafeChangeProvider, source_id: str, artifact_id: str) -> RecoveryPlan:
    return RecoveryPlan(
        lane_id=provider.value,
        name=provider.value,
        provider=provider,
        source_id=source_id,
        artifact_id=artifact_id,
        scope=SCOPE,
    )


def artifact(recovery_plan: RecoveryPlan, **metadata) -> ArtifactInspection:
    return ArtifactInspection(
        artifact_id=recovery_plan.artifact_id,
        provider=recovery_plan.provider,
        source_id=recovery_plan.source_id,
        run_id=SCOPE.run_id,
        owner=SCOPE.owner,
        state="READY/ACTIVE",
        aws_account_id=SCOPE.aws_account_id,
        aws_region=SCOPE.aws_region,
        metadata={
            "recovery_contract": "recovery-v1",
            "topology_valid": True,
            **metadata,
        },
    )


async def wait_for(check, **_):
    return await check()


async def test_lakebase_wait_reports_exact_preflight_and_acceptance_contract() -> None:
    source = "projects/ad-test/branches/production/endpoints/primary"
    underlying = SimpleNamespace(
        name="Lakebase",
        source_id=source,
        _project="projects/ad-test",
        _source_branch="projects/ad-test/branches/production",
        preflight=AsyncMock(),
    )
    recovery_plan = plan(
        SafeChangeProvider.LAKEBASE,
        source,
        "recovery-ad-test-003",
    )
    adapter = LakebaseRecoveryAdapter(underlying)  # type: ignore[arg-type]
    reports: list[tuple[str, str | None]] = []

    async def report(status: str, wire_call: str | None = None) -> None:
        reports.append((status, wire_call))

    evidence = await adapter.wait_recovery_point(recovery_plan, RECOVERY_AT, report)

    assert evidence == {"source_branch_time": RECOVERY_AT.isoformat()}
    underlying.preflight.assert_awaited_once()
    assert reports == [
        (
            (
                "Validating the Lakebase source; branch-request acceptance enforces "
                "recovery-point eligibility"
            ),
            (
                f"GET {LAKEBASE_API_ROOT}/projects/ad-test + "
                f"{LAKEBASE_API_ROOT}/projects/ad-test/branches/production + "
                f"{LAKEBASE_API_ROOT}/projects/ad-test/branches/production/endpoints/primary "
                "→ the branch POST's acceptance enforces source_branch_time eligibility"
            ),
        )
    ]


async def test_lakebase_create_mutation_settles_after_towel_cancellation() -> None:
    underlying = SimpleNamespace(
        name="Lakebase",
        source_id="projects/ad-test/branches/production/endpoints/primary",
        config=SimpleNamespace(control_timeout_seconds=1.0),
    )
    adapter = LakebaseRecoveryAdapter(underlying)  # type: ignore[arg-type]
    mutation_started = asyncio.Event()
    allow_mutation = asyncio.Event()
    mutation_completed = asyncio.Event()

    async def accepted_mutation():
        mutation_started.set()
        await allow_mutation.wait()
        mutation_completed.set()
        return {"accepted": True}

    create = asyncio.create_task(adapter._run_mutation(accepted_mutation()))
    await asyncio.wait_for(mutation_started.wait(), timeout=1)
    create.cancel()
    await asyncio.gather(create, return_exceptions=True)
    assert adapter._pending_mutations

    settlement = asyncio.create_task(adapter.settle_pending_mutations())
    await asyncio.sleep(0)
    assert not settlement.done()
    allow_mutation.set()
    await asyncio.wait_for(settlement, timeout=1)

    assert mutation_completed.is_set()
    assert not adapter._pending_mutations


async def test_provider_recovery_requests_use_explicit_restore_time() -> None:
    runner = SimpleNamespace(json=AsyncMock())
    lakebase_source = "projects/ad-test/branches/production/endpoints/primary"
    lakebase_underlying = SimpleNamespace(
        name="Lakebase",
        source_id=lakebase_source,
        _project="projects/ad-test",
        _source_branch="projects/ad-test/branches/production",
        _runner=runner,
        _get_or_none=AsyncMock(return_value=None),
        _resource_name=lambda resource, expected, _: (
            None if resource["name"] == expected else (_ for _ in ()).throw(AssertionError())
        ),
        _wait_branch_present=AsyncMock(),
        _clock=lambda: 0.0,
        _sleep=AsyncMock(),
        config=SimpleNamespace(control_timeout_seconds=120.0, poll_timeout_seconds=1.0),
    )
    lakebase_plan = plan(
        SafeChangeProvider.LAKEBASE,
        lakebase_source,
        "recovery-ad-test-003",
    )
    lakebase = LakebaseRecoveryAdapter(lakebase_underlying)  # type: ignore[arg-type]
    lakebase.inspect_recovery = AsyncMock(  # type: ignore[method-assign]
        return_value=artifact(
            lakebase_plan,
            branch_name="projects/ad-test/branches/recovery-ad-test-003",
            source_branch="projects/ad-test/branches/production",
            ownership_endpoint=(
                "projects/ad-test/branches/recovery-ad-test-003/endpoints/primary"
            ),
            ownership_marker_valid=True,
        )
    )
    lakebase_reports: list[tuple[str, str | None]] = []

    async def lakebase_report(status: str, wire_call: str | None = None) -> None:
        lakebase_reports.append((status, wire_call))

    await lakebase.create_recovery(lakebase_plan, RECOVERY_AT, lakebase_report)
    assert runner.json.await_count == 2
    branch_path = _lakebase_create_path(
        "projects/ad-test", "branches", "branch_id", "recovery-ad-test-003"
    )
    endpoint_path = _lakebase_create_path(
        "projects/ad-test/branches/recovery-ad-test-003",
        "endpoints",
        "endpoint_id",
        "primary",
    )
    # The recovery point travels in the branch request body, over REST. Nothing
    # on this path spawns a `databricks` process any more.
    assert [(call.args[0], call.args[1]) for call in runner.json.await_args_list] == [
        ("POST", branch_path),
        ("POST", endpoint_path),
    ]
    branch_spec = runner.json.await_args_list[0].kwargs["body"]
    assert branch_spec["spec"]["source_branch_time"] == RECOVERY_AT.isoformat()
    lakebase_wire_calls = [wire_call for _, wire_call in lakebase_reports]
    assert (
        f"POST {branch_path} source_branch_time={RECOVERY_AT.isoformat()}"
    ) in lakebase_wire_calls
    assert f"POST {endpoint_path}" in lakebase_wire_calls

    aurora_calls = []
    aurora_source = "anti-demo-aurora"
    aurora_underlying = SimpleNamespace(
        name="Aurora Serverless v2",
        source_id=aurora_source,
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            db_subnet_group_name="subnets",
            security_group_id="sg-123",
        ),
        _source=AsyncMock(return_value=({"Port": 5432}, {"PubliclyAccessible": True})),
        _tags=lambda _: [],
        _call=AsyncMock(side_effect=lambda *args, **kwargs: aurora_calls.append((args, kwargs))),
        _describe_cluster=AsyncMock(return_value={"Status": "available"}),
        _describe_instance=AsyncMock(return_value={"DBInstanceStatus": "available"}),
        _wait_for=AsyncMock(side_effect=wait_for),
    )
    aurora_plan = plan(
        SafeChangeProvider.AURORA,
        aurora_source,
        "adrc-ad-test-003-aurora",
    )
    aurora = AuroraRecoveryAdapter(aurora_underlying)  # type: ignore[arg-type]
    aurora._recovery_source = {
        "Port": 5432,
        "ServerlessV2ScalingConfiguration": {
            "MinCapacity": 0,
            "MaxCapacity": 2,
            "SecondsUntilAutoPause": 300,
        },
        "NetworkType": "IPV4",
    }
    aurora._recovery_writer = {"PubliclyAccessible": True}
    aurora.inspect_recovery = AsyncMock(  # type: ignore[method-assign]
        return_value=artifact(aurora_plan)
    )
    aurora_reports: list[tuple[str, str | None]] = []

    async def aurora_report(status: str, wire_call: str | None = None) -> None:
        aurora_reports.append((status, wire_call))

    await aurora.create_recovery(aurora_plan, RECOVERY_AT, aurora_report)
    restore_cluster = next(
        kwargs
        for args, kwargs in aurora_calls
        if args[1] == "restore_db_cluster_to_point_in_time"
    )
    assert restore_cluster["RestoreToTime"] == RECOVERY_AT
    assert "RestoreTime" not in restore_cluster
    assert "RestoreType" not in restore_cluster
    assert restore_cluster["ServerlessV2ScalingConfiguration"] == {
        "MinCapacity": 0,
        "MaxCapacity": 2,
        "SecondsUntilAutoPause": 300,
    }
    assert restore_cluster["NetworkType"] == "IPV4"
    assert "UseLatestRestorableTime" not in restore_cluster
    aurora_wire_calls = [wire_call for _, wire_call in aurora_reports]
    assert (
        "RDS.RestoreDBClusterToPointInTime("
        "SourceDBClusterIdentifier=anti-demo-aurora, "
        "DBClusterIdentifier=adrc-ad-test-003-aurora, "
        f"RestoreToTime={RECOVERY_AT.isoformat()})"
    ) in aurora_wire_calls
    assert (
        "RDS.CreateDBInstance("
        "DBInstanceIdentifier=adrc-ad-test-003-aurora-writer, "
        "DBClusterIdentifier=adrc-ad-test-003-aurora, "
        "DBInstanceClass=db.serverless)"
    ) in aurora_wire_calls

    rds_calls = []
    rds_source = "anti-demo-rds"
    rds_underlying = SimpleNamespace(
        name="RDS PostgreSQL",
        source_id=rds_source,
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            db_subnet_group_name="subnets",
            security_group_id="sg-456",
        ),
        _source=AsyncMock(return_value={"DBInstanceClass": "db.t4g.medium"}),
        _tags=lambda _: [],
        _call=AsyncMock(side_effect=lambda *args, **kwargs: rds_calls.append((args, kwargs))),
        _describe_instance=AsyncMock(return_value={"DBInstanceStatus": "available"}),
        _wait_for=AsyncMock(side_effect=wait_for),
    )
    rds_plan = plan(SafeChangeProvider.RDS, rds_source, "adrc-ad-test-003-rds")
    rds = RdsRecoveryAdapter(rds_underlying)  # type: ignore[arg-type]
    rds._recovery_source = {
        "DBInstanceClass": "db.t4g.medium",
        "NetworkType": "IPV4",
    }
    rds.inspect_recovery = AsyncMock(  # type: ignore[method-assign]
        return_value=artifact(rds_plan)
    )
    rds_reports: list[tuple[str, str | None]] = []

    async def rds_report(status: str, wire_call: str | None = None) -> None:
        rds_reports.append((status, wire_call))

    await rds.create_recovery(rds_plan, RECOVERY_AT, rds_report)
    restore_instance = next(
        kwargs
        for args, kwargs in rds_calls
        if args[1] == "restore_db_instance_to_point_in_time"
    )
    assert restore_instance["RestoreTime"] == RECOVERY_AT
    assert "RestoreToTime" not in restore_instance
    assert restore_instance["NetworkType"] == "IPV4"
    assert "UseLatestRestorableTime" not in restore_instance
    assert (
        "RDS.RestoreDBInstanceToPointInTime("
        "SourceDBInstanceIdentifier=anti-demo-rds, "
        "TargetDBInstanceIdentifier=adrc-ad-test-003-rds, "
        f"RestoreTime={RECOVERY_AT.isoformat()})"
    ) in [wire_call for _, wire_call in rds_reports]


async def test_aws_timed_wait_checks_both_restore_window_bounds_and_reports_call() -> None:
    source = {
        "EarliestRestorableTime": RECOVERY_AT - timedelta(minutes=1),
        "LatestRestorableTime": RECOVERY_AT + timedelta(minutes=1),
    }
    underlying = SimpleNamespace(
        name="Aurora Serverless v2",
        source_id="anti-demo-aurora",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            poll_timeout_seconds=10.0,
            poll_interval_seconds=0.01,
        ),
        _source=AsyncMock(return_value=(source, {})),
        _clock=lambda: 0.0,
        _sleep=AsyncMock(),
    )
    recovery_plan = plan(
        SafeChangeProvider.AURORA,
        "anti-demo-aurora",
        "adrc-ad-test-003-aurora",
    )
    adapter = AuroraRecoveryAdapter(underlying)  # type: ignore[arg-type]
    reports: list[tuple[str, str | None]] = []

    async def report(status: str, wire_call: str | None = None) -> None:
        reports.append((status, wire_call))

    evidence = await adapter.wait_recovery_point(recovery_plan, RECOVERY_AT, report)
    assert evidence["earliest_restorable_time"] == source[
        "EarliestRestorableTime"
    ].isoformat()
    assert reports == [
        (
            "Waiting for the AWS restore window to include the recovery point",
            (
                "RDS.DescribeDBClusters(DBClusterIdentifier=anti-demo-aurora) "
                "→ LatestRestorableTime"
            ),
        )
    ]

    source["EarliestRestorableTime"] = RECOVERY_AT + timedelta(seconds=1)
    with pytest.raises(
        SafeChangeLiveConfigurationError,
        match="no longer includes",
    ):
        await adapter.wait_recovery_point(recovery_plan, RECOVERY_AT, report)


def window_bound_adapter(
    earliest: datetime,
    latest: datetime,
    poll_timeout_seconds: float,
) -> tuple[AuroraRecoveryAdapter, RecoveryPlan]:
    underlying = SimpleNamespace(
        name="Aurora Serverless v2",
        source_id="anti-demo-aurora",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            poll_timeout_seconds=poll_timeout_seconds,
            poll_interval_seconds=0.01,
        ),
        _source=AsyncMock(
            return_value=(
                {"EarliestRestorableTime": earliest, "LatestRestorableTime": latest},
                {},
            )
        ),
        _clock=lambda: 0.0,
        _sleep=AsyncMock(),
    )
    recovery_plan = plan(
        SafeChangeProvider.AURORA,
        "anti-demo-aurora",
        "adrc-ad-test-003-aurora",
    )
    return AuroraRecoveryAdapter(underlying), recovery_plan  # type: ignore[arg-type]


# Both bound failures are diagnosed entirely by the arithmetic between the
# requested point and the window, and the restore API's own `InvalidRestoreFault`
# reports none of it. An operator reading only the harness message has to be able
# to tell "the source clock is a day off" from "the window was four minutes
# short", so the timestamps are part of the contract, not decoration.
async def test_restore_window_floor_error_names_the_request_and_both_bounds() -> None:
    earliest = RECOVERY_AT + timedelta(hours=2)
    latest = RECOVERY_AT + timedelta(hours=26)
    adapter, recovery_plan = window_bound_adapter(earliest, latest, 10.0)

    with pytest.raises(SafeChangeLiveConfigurationError) as caught:
        await adapter.wait_recovery_point(
            recovery_plan, RECOVERY_AT, quiet_recovery_report
        )

    message = str(caught.value)
    assert RECOVERY_AT.isoformat() in message
    assert earliest.isoformat() in message
    assert latest.isoformat() in message


async def test_restore_window_timeout_error_names_the_remaining_lag() -> None:
    latest = RECOVERY_AT - timedelta(seconds=270)
    adapter, recovery_plan = window_bound_adapter(
        RECOVERY_AT - timedelta(days=1),
        latest,
        0.0,
    )

    with pytest.raises(SafeChangeControlPlaneError) as caught:
        await adapter.wait_recovery_point(
            recovery_plan, RECOVERY_AT, quiet_recovery_report
        )

    message = str(caught.value)
    assert RECOVERY_AT.isoformat() in message
    assert latest.isoformat() in message
    assert "270s" in message


async def test_rds_restorable_window_falls_back_to_exact_active_automated_backup() -> None:
    latest = RECOVERY_AT + timedelta(minutes=1)
    earliest = RECOVERY_AT - timedelta(minutes=1)
    source = {
        "DBInstanceArn": "arn:aws:rds:us-west-2:123456789012:db:anti-demo-rds",
        "DBInstanceIdentifier": "anti-demo-rds",
        "DbiResourceId": "db-EXACTRESOURCE",
        "EarliestRestorableTime": None,
        "LatestRestorableTime": latest,
    }
    response = {
        "DBInstanceAutomatedBackups": [
            {
                "DBInstanceArn": source["DBInstanceArn"],
                "DBInstanceIdentifier": "anti-demo-rds",
                "DbiResourceId": source["DbiResourceId"],
                "Region": SCOPE.aws_region,
                "Status": "active",
                "RestoreWindow": {
                    "EarliestTime": earliest,
                    "LatestTime": latest,
                },
            }
        ]
    }
    underlying = SimpleNamespace(
        name="RDS PostgreSQL",
        source_id="anti-demo-rds",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
        ),
        _source=AsyncMock(return_value=source),
        _call=AsyncMock(return_value=response),
    )
    recovery_plan = plan(
        SafeChangeProvider.RDS,
        "anti-demo-rds",
        "adrc-ad-test-003-rds",
    )
    adapter = RdsRecoveryAdapter(underlying)  # type: ignore[arg-type]
    reports: list[tuple[str, str | None]] = []

    async def report(status: str, wire_call: str | None = None) -> None:
        reports.append((status, wire_call))

    assert await adapter._restorable_window(recovery_plan, report) == (earliest, latest)
    underlying._call.assert_awaited_once_with(
        "rds",
        "describe_db_instance_automated_backups",
        DBInstanceIdentifier="anti-demo-rds",
    )
    assert reports == [
        (
            "Using the exact active automated-backup restore-window fallback",
            (
                "RDS.DescribeDBInstanceAutomatedBackups("
                "DBInstanceIdentifier=anti-demo-rds) → RestoreWindow"
            ),
        )
    ]


@pytest.mark.parametrize(
    "invalid_case",
    [
        "paginated",
        "duplicate",
        "inactive",
        "identifier",
        "arn",
        "resource_id",
        "region",
        "naive_bound",
        "reversed_window",
        "latest_mismatch",
    ],
)
async def test_rds_restorable_window_fallback_fails_closed(invalid_case: str) -> None:
    latest = RECOVERY_AT + timedelta(minutes=1)
    source = {
        "DBInstanceArn": "arn:aws:rds:us-west-2:123456789012:db:anti-demo-rds",
        "DbiResourceId": "db-EXACTRESOURCE",
        "EarliestRestorableTime": None,
        "LatestRestorableTime": latest,
    }
    backup = {
        "DBInstanceArn": source["DBInstanceArn"],
        "DBInstanceIdentifier": "anti-demo-rds",
        "DbiResourceId": source["DbiResourceId"],
        "Region": SCOPE.aws_region,
        "Status": "active",
        "RestoreWindow": {
            "EarliestTime": RECOVERY_AT - timedelta(minutes=1),
            "LatestTime": latest,
        },
    }
    response = {"DBInstanceAutomatedBackups": [backup]}
    if invalid_case == "paginated":
        response["Marker"] = "next-page"
    elif invalid_case == "duplicate":
        response["DBInstanceAutomatedBackups"].append(dict(backup))
    elif invalid_case == "inactive":
        backup["Status"] = "retained"
    elif invalid_case == "identifier":
        backup["DBInstanceIdentifier"] = "other-rds"
    elif invalid_case == "arn":
        backup["DBInstanceArn"] = "arn:aws:rds:us-west-2:123456789012:db:other-rds"
    elif invalid_case == "resource_id":
        backup["DbiResourceId"] = "db-OTHERRESOURCE"
    elif invalid_case == "region":
        backup["Region"] = "us-east-1"
    elif invalid_case == "naive_bound":
        backup["RestoreWindow"]["EarliestTime"] = RECOVERY_AT.replace(tzinfo=None)
    elif invalid_case == "reversed_window":
        backup["RestoreWindow"]["EarliestTime"] = latest + timedelta(seconds=1)
    elif invalid_case == "latest_mismatch":
        backup["RestoreWindow"]["LatestTime"] = latest - timedelta(seconds=1)

    underlying = SimpleNamespace(
        name="RDS PostgreSQL",
        source_id="anti-demo-rds",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
        ),
        _source=AsyncMock(return_value=source),
        _call=AsyncMock(return_value=response),
    )
    recovery_plan = plan(
        SafeChangeProvider.RDS,
        "anti-demo-rds",
        "adrc-ad-test-003-rds",
    )
    adapter = RdsRecoveryAdapter(underlying)  # type: ignore[arg-type]

    with pytest.raises(SafeChangeLiveConfigurationError, match="window is unavailable"):
        await adapter._restorable_window(recovery_plan)


# --- Round 3 restore readiness -------------------------------------------------
#
# Round 3 shares the RDS PITR restore path with Round 2, so it shares the same
# hazard: a restored instance reports `backing-up` for the whole of its
# mandatory post-restore snapshot while already serving connections. Round 3
# never gates on AVAILABLE after the wait -- `inspect_recovery` and
# `_assert_owned` check topology and ownership tags only -- so accepting the
# earlier state is safe here for exactly the same reason it is in Round 2.


async def wait_or_timeout(check, *, description: str = "", **_):
    """Mirror `_wait_for` semantics with a single poll."""

    result = await check()
    if result is None:
        raise SafeChangeControlPlaneError(f"Timed out waiting for {description}")
    return result


def rds_recovery_adapter(status: str) -> tuple[RdsRecoveryAdapter, RecoveryPlan]:
    underlying = SimpleNamespace(
        name="RDS PostgreSQL",
        source_id="anti-demo-rds",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            db_subnet_group_name="subnets",
            security_group_id="sg-456",
        ),
        _source=AsyncMock(return_value={"DBInstanceClass": "db.t4g.medium"}),
        _tags=lambda _: [],
        _call=AsyncMock(),
        _describe_instance=AsyncMock(return_value={"DBInstanceStatus": status}),
        _wait_for=AsyncMock(side_effect=wait_or_timeout),
    )
    recovery_plan = plan(
        SafeChangeProvider.RDS,
        "anti-demo-rds",
        "adrc-ad-test-003-rds",
    )
    adapter = RdsRecoveryAdapter(underlying)  # type: ignore[arg-type]
    adapter._recovery_source = {"DBInstanceClass": "db.t4g.medium", "NetworkType": "IPV4"}
    adapter.inspect_recovery = AsyncMock(  # type: ignore[method-assign]
        return_value=artifact(recovery_plan)
    )
    return adapter, recovery_plan


async def quiet_recovery_report(_: str, wire_call: str | None = None) -> None:
    del wire_call
    return None


@pytest.mark.parametrize("status", ["available", "backing-up"])
async def test_rds_recovery_restore_is_ready_once_the_instance_serves(status: str) -> None:
    adapter, recovery_plan = rds_recovery_adapter(status)

    result = await adapter.create_recovery(recovery_plan, RECOVERY_AT, quiet_recovery_report)

    assert result.artifact_id == recovery_plan.artifact_id


@pytest.mark.parametrize("status", ["creating", "modifying", "starting", "stopped"])
async def test_rds_recovery_restore_keeps_waiting_when_not_serving(status: str) -> None:
    adapter, recovery_plan = rds_recovery_adapter(status)

    with pytest.raises(SafeChangeControlPlaneError, match="RDS recovery restore"):
        await adapter.create_recovery(recovery_plan, RECOVERY_AT, quiet_recovery_report)


# ---------------------------------------------------------------------------
# Real-service-model parameter validation.
#
# The fakes above record `_call` arguments without validating them, so a wrong
# parameter name reads as a passing test. These tests push the argument dicts
# that production actually builds through botocore's own validator and through
# `Stubber`, which is the same validation an AWS call performs client-side.
# Neither needs credentials or network access.
#
# RestoreDBInstanceToPointInTime takes `RestoreTime`; RestoreDBClusterToPointInTime
# takes `RestoreToTime`. That difference is deliberate on AWS's side, so these
# tests pin both names and fail loudly on a "consistency" edit that unifies them.
# ---------------------------------------------------------------------------

RDS_RESTORE_OPERATIONS = {
    "restore_db_instance_to_point_in_time": "RestoreDBInstanceToPointInTime",
    "restore_db_cluster_to_point_in_time": "RestoreDBClusterToPointInTime",
}


def botocore_session():
    """A session that ignores ambient AWS config.

    Other tests in the suite set `AWS_PROFILE`, and a plain `get_session()` would
    fail resolving a profile that does not exist on this machine. Nothing here
    needs credentials, so the profile is pinned off.
    """
    return botocore.session.Session(session_vars={"profile": (None, None, None, None)})


def rds_input_shape(method: str):
    service_model = ServiceModel(
        Loader().load_service_model("rds", "service-2"), service_name="rds"
    )
    return service_model.operation_model(RDS_RESTORE_OPERATIONS[method]).input_shape


def assert_valid_rds_parameters(method: str, arguments: dict) -> None:
    """Validate against the real RDS model exactly as botocore would."""
    report = ParamValidator().validate(arguments, rds_input_shape(method))
    assert not report.has_errors(), f"{method}: {report.generate_report()}"


def stub_rds_parameters(method: str, arguments: dict) -> None:
    """Serialize through a real botocore client, with the wire send stubbed out."""
    client = botocore_session().create_client(
        "rds",
        region_name=SCOPE.aws_region,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    with Stubber(client) as stubber:
        stubber.add_response(method, {}, arguments)
        getattr(client, method)(**arguments)
        stubber.assert_no_pending_responses()


async def captured_restore_arguments() -> dict[str, dict]:
    """Run both AWS recovery adapters and return the restore kwargs they build."""
    captured: dict[str, dict] = {}

    def record(*args, **kwargs):
        if args[1] in RDS_RESTORE_OPERATIONS:
            captured[args[1]] = dict(kwargs)

    aurora_underlying = SimpleNamespace(
        name="Aurora Serverless v2",
        source_id="anti-demo-aurora",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            db_subnet_group_name="subnets",
            security_group_id="sg-123",
        ),
        _source=AsyncMock(return_value=({"Port": 5432}, {"PubliclyAccessible": True})),
        _tags=lambda _: [{"Key": "managed-by", "Value": "anti-demo"}],
        _call=AsyncMock(side_effect=record),
        _describe_cluster=AsyncMock(return_value={"Status": "available"}),
        _describe_instance=AsyncMock(return_value={"DBInstanceStatus": "available"}),
        _wait_for=AsyncMock(side_effect=wait_for),
    )
    aurora_plan = plan(
        SafeChangeProvider.AURORA,
        "anti-demo-aurora",
        "adrc-ad-test-003-aurora",
    )
    aurora = AuroraRecoveryAdapter(aurora_underlying)  # type: ignore[arg-type]
    aurora._recovery_source = {
        "Port": 5432,
        "ServerlessV2ScalingConfiguration": {
            "MinCapacity": 0,
            "MaxCapacity": 2,
            "SecondsUntilAutoPause": 300,
        },
        "NetworkType": "IPV4",
    }
    aurora._recovery_writer = {"PubliclyAccessible": True}
    aurora.inspect_recovery = AsyncMock(  # type: ignore[method-assign]
        return_value=artifact(aurora_plan)
    )
    await aurora.create_recovery(aurora_plan, RECOVERY_AT, quiet_recovery_report)

    rds_underlying = SimpleNamespace(
        name="RDS PostgreSQL",
        source_id="anti-demo-rds",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            db_subnet_group_name="subnets",
            security_group_id="sg-456",
        ),
        _source=AsyncMock(return_value={"DBInstanceClass": "db.t4g.medium"}),
        _tags=lambda _: [{"Key": "managed-by", "Value": "anti-demo"}],
        _call=AsyncMock(side_effect=record),
        _describe_instance=AsyncMock(return_value={"DBInstanceStatus": "available"}),
        _wait_for=AsyncMock(side_effect=wait_for),
    )
    rds_plan = plan(SafeChangeProvider.RDS, "anti-demo-rds", "adrc-ad-test-003-rds")
    rds = RdsRecoveryAdapter(rds_underlying)  # type: ignore[arg-type]
    rds._recovery_source = {
        "DBInstanceClass": "db.t4g.medium",
        "PubliclyAccessible": True,
        "NetworkType": "IPV4",
    }
    rds.inspect_recovery = AsyncMock(  # type: ignore[method-assign]
        return_value=artifact(rds_plan)
    )
    await rds.create_recovery(rds_plan, RECOVERY_AT, quiet_recovery_report)

    assert set(captured) == set(RDS_RESTORE_OPERATIONS)
    return captured


@pytest.mark.parametrize("method", sorted(RDS_RESTORE_OPERATIONS))
async def test_recovery_restore_arguments_pass_botocore_validation(method: str) -> None:
    captured = await captured_restore_arguments()

    arguments = captured[method]
    arguments.pop("mutation", None)
    assert_valid_rds_parameters(method, arguments)
    stub_rds_parameters(method, arguments)


async def test_recovery_restore_uses_the_recovery_point_each_api_actually_accepts() -> None:
    captured = await captured_restore_arguments()

    instance = captured["restore_db_instance_to_point_in_time"]
    cluster = captured["restore_db_cluster_to_point_in_time"]

    assert instance["RestoreTime"] == RECOVERY_AT
    assert "RestoreToTime" not in instance
    assert cluster["RestoreToTime"] == RECOVERY_AT
    assert "RestoreTime" not in cluster


# An out-of-window `RestoreTime` is rejected synchronously by
# RestoreDBInstanceToPointInTime rather than accepted and failed during
# readiness polling. That is what makes the pre-restore window check in
# `wait_recovery_point` sufficient and a re-check immediately before the call
# redundant: there is no minutes-long wait hiding behind a bad recovery point.
# `InvalidRestoreFault` is asserted against the real service model, so this stops
# being true the moment AWS changes the contract.
async def test_out_of_window_restore_time_is_rejected_before_any_readiness_wait() -> None:
    client = botocore_session().create_client(
        "rds",
        region_name=SCOPE.aws_region,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    waited = False

    async def never_reached(*_args, **_kwargs):
        nonlocal waited
        waited = True

    async def call(_service: str, method: str, *, mutation: bool = False, **arguments):
        del mutation
        return await asyncio.to_thread(getattr(client, method), **arguments)

    underlying = SimpleNamespace(
        name="RDS PostgreSQL",
        source_id="anti-demo-rds",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            db_subnet_group_name="subnets",
            security_group_id="sg-456",
        ),
        _tags=lambda _: [{"Key": "managed-by", "Value": "anti-demo"}],
        _call=call,
        _wait_for=never_reached,
        _describe_instance=AsyncMock(side_effect=AssertionError("must not describe")),
    )
    recovery_plan = plan(SafeChangeProvider.RDS, "anti-demo-rds", "adrc-ad-test-003-rds")
    adapter = RdsRecoveryAdapter(underlying)  # type: ignore[arg-type]
    adapter._recovery_source = {"DBInstanceClass": "db.t4g.medium"}

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "restore_db_instance_to_point_in_time",
            service_error_code="InvalidRestoreFault",
            service_message=(
                "Cannot restore to a time before the earliest restorable time"
            ),
            http_status_code=400,
        )
        with pytest.raises(ClientError) as caught:
            await adapter.create_recovery(
                recovery_plan, RECOVERY_AT, quiet_recovery_report
            )

    assert caught.value.response["Error"]["Code"] == "InvalidRestoreFault"
    assert not waited, "restore rejection must surface before readiness polling"


@pytest.mark.parametrize(
    ("method", "accepted", "rejected"),
    [
        ("restore_db_instance_to_point_in_time", "RestoreTime", "RestoreToTime"),
        ("restore_db_cluster_to_point_in_time", "RestoreToTime", "RestoreTime"),
    ],
)
def test_rds_service_model_keeps_the_two_restore_apis_asymmetric(
    method: str, accepted: str, rejected: str
) -> None:
    members = rds_input_shape(method).members

    assert accepted in members
    assert rejected not in members


# ---------------------------------------------------------------------------
# Abandoning a cancelled Round 3 lane.
#
# Same contract as Round 2: issue the deletes, wait for nothing, and stay quiet
# about a resource that is absent or already going. These bind the real
# `_issue_delete` onto a recording double so the tolerated-error handling under
# test is production's, not the double's.
# ---------------------------------------------------------------------------


def recording_aws_double(**extra):
    """An underlying safe-change adapter double that records `_call` and really
    runs `_issue_delete`."""

    calls: list[tuple[str, dict]] = []
    errors: dict[str, BaseException] = {}

    async def record(_service, method, **kwargs):
        kwargs.pop("mutation", None)
        calls.append((method, dict(kwargs)))
        if method in errors:
            raise errors[method]
        return {}

    double = SimpleNamespace(
        name="double",
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
        ),
        _call=record,
        _wait_for=AsyncMock(
            side_effect=AssertionError("the cancellation path must not wait for absence")
        ),
        calls=calls,
        errors=errors,
        **extra,
    )
    double._issue_delete = _AwsSafeChangeAdapter._issue_delete.__get__(double)
    return double


def client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "n/a"}}, operation)


async def test_aurora_recovery_abandon_issues_both_deletes_and_waits_for_neither() -> None:
    double = recording_aws_double(source_id="anti-demo-aurora")
    adapter = AuroraRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-aurora"
    recovery_plan = plan(
        SafeChangeProvider.AURORA,
        "anti-demo-aurora",
        "adrc-ad-test-003-aurora",
    )

    await adapter.abandon_recovery(recovery_plan)

    assert double.calls == [
        (
            "delete_db_instance",
            {
                "DBInstanceIdentifier": "adrc-ad-test-003-aurora-writer",
                "SkipFinalSnapshot": True,
                "DeleteAutomatedBackups": True,
            },
        ),
        (
            "delete_db_cluster",
            {
                "DBClusterIdentifier": "adrc-ad-test-003-aurora",
                "SkipFinalSnapshot": True,
                "DeleteAutomatedBackups": True,
            },
        ),
    ]


async def test_aurora_recovery_abandon_tolerates_a_restore_that_never_happened() -> None:
    double = recording_aws_double(source_id="anti-demo-aurora")
    double.errors["delete_db_instance"] = client_error(
        "DBInstanceNotFound", "DeleteDBInstance"
    )
    double.errors["delete_db_cluster"] = client_error(
        "DBClusterNotFoundFault", "DeleteDBCluster"
    )
    adapter = AuroraRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-aurora"

    await adapter.abandon_recovery(
        plan(SafeChangeProvider.AURORA, "anti-demo-aurora", "adrc-ad-test-003-aurora")
    )

    assert [method for method, _ in double.calls] == [
        "delete_db_instance",
        "delete_db_cluster",
    ]


async def test_aurora_recovery_abandon_still_tries_the_cluster_after_a_wedged_writer() -> None:
    double = recording_aws_double(source_id="anti-demo-aurora")
    double.errors["delete_db_instance"] = client_error(
        "InvalidDBInstanceState", "DeleteDBInstance"
    )
    adapter = AuroraRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-aurora"

    await adapter.abandon_recovery(
        plan(SafeChangeProvider.AURORA, "anti-demo-aurora", "adrc-ad-test-003-aurora")
    )

    assert [method for method, _ in double.calls] == [
        "delete_db_instance",
        "delete_db_cluster",
    ]


async def test_rds_recovery_abandon_issues_exactly_one_delete() -> None:
    double = recording_aws_double(source_id="anti-demo-rds")
    adapter = RdsRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-rds"

    await adapter.abandon_recovery(
        plan(SafeChangeProvider.RDS, "anti-demo-rds", "adrc-ad-test-003-rds")
    )

    assert double.calls == [
        (
            "delete_db_instance",
            {
                "DBInstanceIdentifier": "adrc-ad-test-003-rds",
                "SkipFinalSnapshot": True,
                "DeleteAutomatedBackups": True,
            },
        )
    ]


async def test_rds_recovery_abandon_tolerates_an_absent_restore() -> None:
    double = recording_aws_double(source_id="anti-demo-rds")
    double.errors["delete_db_instance"] = client_error(
        "DBInstanceNotFound", "DeleteDBInstance"
    )
    adapter = RdsRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-rds"

    await adapter.abandon_recovery(
        plan(SafeChangeProvider.RDS, "anti-demo-rds", "adrc-ad-test-003-rds")
    )

    assert [method for method, _ in double.calls] == ["delete_db_instance"]


@pytest.mark.parametrize(
    ("provider", "source", "adapter_class"),
    [
        (SafeChangeProvider.AURORA, "anti-demo-aurora", AuroraRecoveryAdapter),
        (SafeChangeProvider.RDS, "anti-demo-rds", RdsRecoveryAdapter),
    ],
)
async def test_recovery_abandon_refuses_an_identifier_it_did_not_derive(
    provider, source, adapter_class
) -> None:
    """Nothing is described before these deletes, so the deterministic-identifier
    check is the only thing keeping the path off a resource it does not own."""

    double = recording_aws_double(source_id=source)
    adapter = adapter_class(double)  # type: ignore[arg-type]
    adapter.source_id = source

    with pytest.raises(SafeChangeLiveConfigurationError):
        await adapter.abandon_recovery(plan(provider, source, source))

    assert double.calls == []


async def test_lakebase_recovery_abandon_issues_the_delete_under_the_teardown_bound() -> None:
    commands: list[tuple[str, str]] = []
    timeouts: list[float] = []

    async def run(method, path, *, body=None, timeout_seconds: float) -> None:
        assert body is None
        commands.append((method, path))
        timeouts.append(timeout_seconds)

    underlying = SimpleNamespace(
        name="Lakebase",
        source_id="projects/ad-test/branches/production/endpoints/primary",
        _project="projects/ad-test",
        _runner=SimpleNamespace(run=run),
        config=SimpleNamespace(control_timeout_seconds=900.0),
    )
    adapter = LakebaseRecoveryAdapter(underlying)  # type: ignore[arg-type]

    await adapter.abandon_recovery(
        plan(
            SafeChangeProvider.LAKEBASE,
            "projects/ad-test/branches/production/endpoints/primary",
            "recovery-ad-test-003",
        )
    )

    assert commands == [
        (
            "DELETE",
            lakebase_resource_path("projects/ad-test/branches/recovery-ad-test-003"),
        )
    ]
    assert timeouts == [DEFAULT_CANCEL_TEARDOWN_SECONDS]


async def test_lakebase_recovery_abandon_tolerates_a_branch_that_never_existed() -> None:
    async def run(_method, _path, *, body=None, timeout_seconds: float) -> None:
        del body, timeout_seconds
        raise ControlPlaneCommandError("missing", not_found=True)

    underlying = SimpleNamespace(
        name="Lakebase",
        source_id="projects/ad-test/branches/production/endpoints/primary",
        _project="projects/ad-test",
        _runner=SimpleNamespace(run=run),
        config=SimpleNamespace(control_timeout_seconds=900.0),
    )
    adapter = LakebaseRecoveryAdapter(underlying)  # type: ignore[arg-type]

    await adapter.abandon_recovery(
        plan(
            SafeChangeProvider.LAKEBASE,
            "projects/ad-test/branches/production/endpoints/primary",
            "recovery-ad-test-003",
        )
    )


# ---------------------------------------------------------------------------
# A towel thrown mid-restore.
#
# `abandon_recovery` above is only reached when the lane task is cancelled, and
# it deliberately tolerates a refusal. `delete_recovery` is the path that has to
# finish the job, because it is what the cooldown runs and what decides whether
# the ring lease is released. A PITR clone that is still `creating` when the
# towel lands refuses every delete AWS is asked for, and `reap.py` will not
# sweep a resource whose round still holds a lease -- so if `delete_recovery`
# gives up here, an Aurora cluster and its writer bill until somebody notices.
#
# These fakes drive production's own `_wait_for` on a virtual clock, and raise
# the real RDS fault codes.
# ---------------------------------------------------------------------------

#: Wall-clock seconds the fake clone stays in `creating`. The campaign's Round 3
#: towel lands about ninety seconds into the restore, so any value inside the
#: creating window reproduces it; this one is several poll intervals wide.
FAKE_CREATING_SECONDS = 90.0
FAKE_POLL_INTERVAL = 5.0
FAKE_POLL_TIMEOUT = 900.0


class FakeRdsClone:
    """One RDS resource that refuses deletion until it leaves `creating`."""

    def __init__(
        self,
        *,
        fault: str,
        creating_seconds: float = FAKE_CREATING_SECONDS,
    ) -> None:
        self.fault = fault
        self.creating_seconds = creating_seconds
        self.deleted_at: float | None = None
        self.delete_attempts = 0
        self.refusals = 0

    def status(self, now: float) -> str | None:
        if self.deleted_at is not None:
            # RDS reports `deleting` for a while before the resource is gone.
            return "deleting" if now < self.deleted_at + 10.0 else None
        return "creating" if now < self.creating_seconds else "available"

    def delete(self, now: float, operation: str) -> None:
        self.delete_attempts += 1
        if self.status(now) == "creating":
            self.refusals += 1
            raise client_error(self.fault, operation)
        if self.deleted_at is None:
            self.deleted_at = now


def mid_restore_double(*, source_id: str, **resources):
    """An AWS underlying double whose clones are mid-restore, on a fake clock."""

    now = {"t": 0.0}

    async def sleep(seconds: float) -> None:
        now["t"] += seconds

    async def call(_service, method, *, mutation=False, **kwargs):
        del mutation
        if method == "delete_db_cluster":
            resources["cluster"].delete(now["t"], "DeleteDBCluster")
        elif method == "delete_db_instance":
            resources[
                "writer" if "writer" in kwargs["DBInstanceIdentifier"] else "instance"
            ].delete(now["t"], "DeleteDBInstance")
        else:  # pragma: no cover - the double is only asked to delete
            raise AssertionError(f"unexpected mutation {method}")
        return {}

    async def describe_cluster(_identifier, *, missing_ok=False):
        del missing_ok
        status = resources["cluster"].status(now["t"])
        return None if status is None else {"Status": status}

    async def describe_instance(identifier, *, missing_ok=False):
        del missing_ok
        key = "writer" if identifier.endswith("-writer") else "instance"
        resource = resources.get(key)
        if resource is None:
            return None
        status = resource.status(now["t"])
        return None if status is None else {"DBInstanceStatus": status}

    double = SimpleNamespace(
        name="double",
        source_id=source_id,
        config=SimpleNamespace(
            account_id=SCOPE.aws_account_id,
            region=SCOPE.aws_region,
            poll_timeout_seconds=FAKE_POLL_TIMEOUT,
            poll_interval_seconds=FAKE_POLL_INTERVAL,
        ),
        _call=call,
        _describe_cluster=describe_cluster,
        _describe_instance=describe_instance,
        _sleep=sleep,
        _clock=lambda: now["t"],
        now=now,
        resources=resources,
    )
    # Both are bound off production so the retry-and-report behaviour under test
    # is production's, not the double's.
    double._wait_for = _AwsSafeChangeAdapter._wait_for.__get__(double)
    double._delete_when_deletable = _AwsSafeChangeAdapter._delete_when_deletable.__get__(
        double
    )
    return double


async def test_aurora_recovery_delete_waits_out_a_cluster_still_creating() -> None:
    cluster = FakeRdsClone(fault="InvalidDBClusterStateFault")
    double = mid_restore_double(source_id="anti-demo-aurora", cluster=cluster)
    adapter = AuroraRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-aurora"
    recovery_plan = plan(
        SafeChangeProvider.AURORA,
        "anti-demo-aurora",
        "adrc-ad-test-003-aurora",
    )
    adapter.inspect_recovery = AsyncMock(  # type: ignore[method-assign]
        return_value=artifact(recovery_plan, writer_id="adrc-ad-test-003-aurora-writer")
    )

    await adapter.delete_recovery(
        recovery_plan,
        artifact(recovery_plan, writer_id="adrc-ad-test-003-aurora-writer"),
        quiet_recovery_report,
    )

    assert cluster.refusals > 0, "the fake never reproduced the creating window"
    assert cluster.deleted_at is not None
    assert double._clock() >= FAKE_CREATING_SECONDS
    assert cluster.status(double._clock()) is None


async def test_aurora_recovery_delete_waits_out_a_writer_still_creating() -> None:
    """The other Aurora shape: the cluster is up, the writer is mid-create.

    The writer is the Serverless v2 compute, so this is the expensive half.
    """

    writer = FakeRdsClone(fault="InvalidDBInstanceState")
    cluster = FakeRdsClone(fault="InvalidDBClusterStateFault", creating_seconds=0.0)
    double = mid_restore_double(
        source_id="anti-demo-aurora", cluster=cluster, writer=writer
    )
    adapter = AuroraRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-aurora"
    recovery_plan = plan(
        SafeChangeProvider.AURORA,
        "anti-demo-aurora",
        "adrc-ad-test-003-aurora",
    )
    owned = artifact(recovery_plan, writer_id="adrc-ad-test-003-aurora-writer")
    adapter.inspect_recovery = AsyncMock(return_value=owned)  # type: ignore[method-assign]

    await adapter.delete_recovery(recovery_plan, owned, quiet_recovery_report)

    assert writer.refusals > 0, "the fake never reproduced the creating window"
    assert writer.deleted_at is not None
    assert cluster.deleted_at is not None
    assert writer.deleted_at <= cluster.deleted_at


async def test_rds_recovery_delete_waits_out_a_restore_still_creating() -> None:
    instance = FakeRdsClone(fault="InvalidDBInstanceState")
    double = mid_restore_double(source_id="anti-demo-rds", instance=instance)
    adapter = RdsRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-rds"
    recovery_plan = plan(
        SafeChangeProvider.RDS,
        "anti-demo-rds",
        "adrc-ad-test-003-rds",
    )
    owned = artifact(recovery_plan)
    adapter.inspect_recovery = AsyncMock(return_value=owned)  # type: ignore[method-assign]

    await adapter.delete_recovery(recovery_plan, owned, quiet_recovery_report)

    assert instance.refusals > 0, "the fake never reproduced the creating window"
    assert instance.deleted_at is not None
    assert instance.status(double._clock()) is None


@pytest.mark.parametrize(
    ("adapter_class", "provider", "source", "resources", "identifier"),
    [
        (
            AuroraRecoveryAdapter,
            SafeChangeProvider.AURORA,
            "anti-demo-aurora",
            {"cluster": ("InvalidDBClusterStateFault", "cluster")},
            "adrc-ad-test-003-aurora",
        ),
        (
            RdsRecoveryAdapter,
            SafeChangeProvider.RDS,
            "anti-demo-rds",
            {"instance": ("InvalidDBInstanceState", "instance")},
            "adrc-ad-test-003-rds",
        ),
    ],
)
async def test_recovery_delete_that_never_becomes_deletable_stays_loud(
    adapter_class, provider, source, resources, identifier, caplog
) -> None:
    """Giving up must name the resource, not swallow the refusal.

    A clone that never leaves `creating` is a real orphan. The standing
    convention for "this may still be billing" is the `ORPHAN RISK` line, and
    the raise is what keeps the towel `failed` and retryable instead of
    releasing the ring over a resource nobody deleted.
    """

    clones = {
        key: FakeRdsClone(fault=fault, creating_seconds=float("inf"))
        for key, (fault, _) in resources.items()
    }
    double = mid_restore_double(source_id=source, **clones)
    adapter = adapter_class(double)  # type: ignore[arg-type]
    adapter.source_id = source
    recovery_plan = plan(provider, source, identifier)
    owned = artifact(recovery_plan, writer_id=f"{identifier}-writer")
    adapter.inspect_recovery = AsyncMock(return_value=owned)  # type: ignore[method-assign]

    with caplog.at_level("ERROR", logger="server.safe_change_live"):
        with pytest.raises(SafeChangeControlPlaneError):
            await adapter.delete_recovery(recovery_plan, owned, quiet_recovery_report)

    orphan_lines = [
        record.getMessage() for record in caplog.records if "ORPHAN RISK" in record.getMessage()
    ]
    assert orphan_lines, "a resource that may still be billing was not reported"
    assert any(identifier in line for line in orphan_lines)


@pytest.mark.parametrize("method", ["delete_db_instance", "delete_db_cluster"])
async def test_recovery_abandon_delete_arguments_pass_botocore_validation(method: str) -> None:
    double = recording_aws_double(source_id="anti-demo-aurora")
    adapter = AuroraRecoveryAdapter(double)  # type: ignore[arg-type]
    adapter.source_id = "anti-demo-aurora"
    await adapter.abandon_recovery(
        plan(SafeChangeProvider.AURORA, "anti-demo-aurora", "adrc-ad-test-003-aurora")
    )

    arguments = dict(double.calls)[method]

    stub_rds_parameters(method, arguments)
