from datetime import UTC, datetime
from types import SimpleNamespace

from server.connection_spike_journal import JournalEvent, LifecycleState
from server.cost_runtime_identity import (
    RuntimeIdentityStatus,
    extract_round2_runtime_identity,
    extract_round3_runtime_identity,
    extract_round5_runtime_identity,
)
from server.models import CompetitorId, RoundId
from server.recovery import RecoveryArm, RecoveryLaneArm, RecoveryLaneResult, RecoveryRunResult
from server.safe_change import (
    ArtifactInspection,
    SafeChangeArm,
    SafeChangeLaneArm,
    SafeChangeLaneResult,
    SafeChangeLaneState,
    SafeChangeOwnershipScope,
    SafeChangePlan,
    SafeChangeProvider,
    SafeChangeRunResult,
)

NOW = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
SHA = "a" * 64
SCOPE = SafeChangeOwnershipScope(
    run_id="runtime-cost-123",
    owner="anti-demo",
    aws_account_id="123456789012",
    aws_region="us-west-2",
)


def _round2_lane(provider: SafeChangeProvider, artifact_id: str):
    plan = SafeChangePlan(
        lane_id=provider.value,
        name=provider.value,
        provider=provider,
        source_id=f"source-{provider.value}",
        artifact_id=artifact_id,
        scope=SCOPE,
    )
    arm = SafeChangeLaneArm(plan=plan, evidence={})
    result = SafeChangeLaneResult(
        lane_id=provider.value,
        name=provider.value,
        provider=provider,
        state=SafeChangeLaneState.VERIFIED,
        elapsed_ms=1.0,
        first_action_ns=1,
        completed_ns=2,
        artifact_id=artifact_id,
    )
    return plan, arm, result


def test_round2_extracts_exact_lakebase_and_aurora_runtime_identities() -> None:
    lake_plan, lake_arm, lake_result = _round2_lane(
        SafeChangeProvider.LAKEBASE, "safe-change-runtime-cost-123"
    )
    aws_plan, aws_arm, aws_result = _round2_lane(
        SafeChangeProvider.AURORA, "adsc-runtime-cost-123-aurora"
    )
    arm = SafeChangeArm(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        armed_at=NOW,
        armed_at_monotonic_ns=1,
        contract_sha256=SHA,
        scope=SCOPE,
        lanes={"lakebase": lake_arm, "competitor": aws_arm},
    )
    result = SafeChangeRunResult(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        nonce="nonce",
        started_ns=1,
        completed_ns=2,
        launch_skew_ms=0,
        contract_sha256=SHA,
        lanes={"lakebase": lake_result, "competitor": aws_result},
    )
    branch = f"projects/round2/branches/{lake_plan.artifact_id}"
    artifacts = {
        "lakebase": ArtifactInspection(
            artifact_id=lake_plan.artifact_id,
            provider=lake_plan.provider,
            source_id=lake_plan.source_id,
            run_id=SCOPE.run_id,
            owner=SCOPE.owner,
            state="READY/ACTIVE",
            metadata={
                "branch_name": branch,
                "ownership_endpoint": f"{branch}/endpoints/primary",
            },
        ),
        "competitor": ArtifactInspection(
            artifact_id=aws_plan.artifact_id,
            provider=aws_plan.provider,
            source_id=aws_plan.source_id,
            run_id=SCOPE.run_id,
            owner=SCOPE.owner,
            state="AVAILABLE/AVAILABLE",
            aws_account_id=SCOPE.aws_account_id,
            aws_region=SCOPE.aws_region,
            metadata={
                "cluster_arn": (
                    "arn:aws:rds:us-west-2:123456789012:cluster:adsc-runtime-cluster"
                ),
                "cluster_resource_id": "cluster-EXACT",
                "writer_arn": "arn:aws:rds:us-west-2:123456789012:db:adsc-runtime-writer",
                "writer_resource_id": "db-WRITEREXACT",
                "security_group_ids": ["sg-0123abcd"],
                "public_ipv4_addresses": ["198.51.100.24"],
            },
        ),
    }

    capture = extract_round2_runtime_identity(arm, result, artifacts)
    by_type = {item.resource_type: item for item in capture.resources}

    assert capture.status is RuntimeIdentityStatus.READY
    assert capture.round_id is RoundId.MAKE_SCHEMA_CHANGE_SAFELY
    assert by_type["lakebase_runtime_branch"].resource_id == branch
    assert by_type["lakebase_runtime_endpoint"].resource_id == f"{branch}/endpoints/primary"
    assert by_type["aurora_runtime_cluster"].resource_id == "cluster-EXACT"
    assert by_type["aurora_runtime_writer"].resource_id == "db-WRITEREXACT"
    assert by_type["runtime_security_group"].resource_id == "sg-0123abcd"
    assert by_type["runtime_public_ipv4"].resource_id == "198.51.100.24"


def test_current_aurora_inspection_shape_is_quarantined_without_name_inference() -> None:
    plan, lane_arm, lane_result = _round2_lane(
        SafeChangeProvider.AURORA, "adsc-runtime-cost-123-aurora"
    )
    arm = SafeChangeArm(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        armed_at=NOW,
        armed_at_monotonic_ns=1,
        contract_sha256=SHA,
        scope=SCOPE,
        lanes={"competitor": lane_arm},
    )
    result = SafeChangeRunResult(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        nonce="nonce",
        started_ns=1,
        completed_ns=2,
        launch_skew_ms=0,
        contract_sha256=SHA,
        lanes={"competitor": lane_result},
    )
    artifact = ArtifactInspection(
        artifact_id=plan.artifact_id,
        provider=plan.provider,
        source_id=plan.source_id,
        run_id=SCOPE.run_id,
        owner=SCOPE.owner,
        state="AVAILABLE/AVAILABLE",
        aws_account_id=SCOPE.aws_account_id,
        aws_region=SCOPE.aws_region,
        metadata={
            "cluster_arn": "arn:aws:rds:us-west-2:123456789012:cluster:exact",
            "writer_id": "friendly-writer-name",
        },
    )

    capture = extract_round2_runtime_identity(arm, result, {"competitor": artifact})

    assert capture.status is RuntimeIdentityStatus.QUARANTINED
    assert [item.resource_type for item in capture.resources] == ["aurora_runtime_cluster"]
    assert capture.resources[0].resource_id is None
    assert all(item.resource_id != "friendly-writer-name" for item in capture.resources)
    assert {item.resource_type for item in capture.quarantine} == {
        "aurora_runtime_cluster",
        "aurora_runtime_writer",
        "runtime_security_group",
        "runtime_public_ipv4",
    }


def test_round3_extracts_exact_rds_restore_identity() -> None:
    plan = SimpleNamespace(
        lane_id="competitor",
        provider=SafeChangeProvider.RDS,
        source_id="source-rds",
        artifact_id="adrc-runtime-cost-123-rds",
        scope=SCOPE,
    )
    lane_arm = RecoveryLaneArm(plan=plan, evidence={})
    arm = RecoveryArm(
        competitor=CompetitorId.RDS_POSTGRES,
        armed_at=NOW,
        armed_at_monotonic_ns=1,
        contract_sha256=SHA,
        scope=SCOPE,
        lanes={"competitor": lane_arm},
    )
    lane_result = RecoveryLaneResult(
        lane_id="competitor",
        name="RDS",
        provider=SafeChangeProvider.RDS,
        elapsed_ms=1,
        first_action_ns=1,
        completed_ns=2,
        artifact_id=plan.artifact_id,
        ok=True,
    )
    result = RecoveryRunResult(
        competitor=CompetitorId.RDS_POSTGRES,
        started_ns=1,
        completed_ns=2,
        launch_skew_ms=0,
        contract_sha256=SHA,
        lanes={"competitor": lane_result},
    )
    artifact = ArtifactInspection(
        artifact_id=plan.artifact_id,
        provider=plan.provider,
        source_id=plan.source_id,
        run_id=SCOPE.run_id,
        owner=SCOPE.owner,
        state="AVAILABLE",
        aws_account_id=SCOPE.aws_account_id,
        aws_region=SCOPE.aws_region,
        metadata={
            "instance_arn": "arn:aws:rds:us-west-2:123456789012:db:exact-restore",
            "instance_resource_id": "db-RESTOREEXACT",
            "security_group_ids": ["sg-abcd1234"],
            "public_ipv4_addresses": ["198.51.100.25"],
        },
    )

    capture = extract_round3_runtime_identity(arm, result, {"competitor": artifact})

    assert capture.status is RuntimeIdentityStatus.READY
    assert capture.round_id is RoundId.RECOVER_DELETED_ORDER
    rds = next(item for item in capture.resources if item.resource_type == "rds_runtime_instance")
    assert rds.resource_id == "db-RESTOREEXACT"
    assert rds.resource_arn == "arn:aws:rds:us-west-2:123456789012:db:exact-restore"


def _created_event(
    ordinal: int,
    kind: str,
    provider_id: str,
    *,
    bout_id: str = "bout-runtime",
) -> JournalEvent:
    metadata = {
        "tags": {"anti-demo-bout-id": "bout-runtime"},
        "competitor_resource_id": "cluster-TARGETEXACT",
    }
    return JournalEvent(
        bout_id=bout_id,
        fencing_token=7,
        ordinal=ordinal,
        resource_kind=kind,
        deterministic_name=f"runtime-{kind}",
        client_token=None,
        provider_id=provider_id,
        lifecycle_state=LifecycleState.CREATED,
        metadata=metadata,
        runtime_seal_sha256=SHA,
        intent_at=NOW,
        occurred_at=NOW,
        completed_at=NOW,
    )


def test_round5_extracts_only_exact_fenced_journal_identities() -> None:
    result = SimpleNamespace(bout_id="bout-runtime")
    events = (
        _created_event(1, "proxy_security_group", "sg-0123abcd"),
        _created_event(
            7,
            "rds_proxy",
            "arn:aws:rds:us-west-2:123456789012:db-proxy:prx-0123abcd",
        ),
        _created_event(9, "proxy_target", "cluster-TARGETEXACT"),
    )

    capture = extract_round5_runtime_identity(result, events)
    by_type = {item.resource_type: item for item in capture.resources}

    assert capture.status is RuntimeIdentityStatus.READY
    assert by_type["proxy_security_group"].resource_id == "sg-0123abcd"
    assert by_type["rds_proxy"].resource_arn.endswith("db-proxy:prx-0123abcd")
    assert by_type["rds_proxy_target"].resource_id == "cluster-TARGETEXACT"


def test_round5_missing_or_cross_bout_journal_identity_is_quarantined() -> None:
    result = SimpleNamespace(bout_id="bout-runtime")
    foreign = _created_event(
        1,
        "proxy_security_group",
        "sg-0123abcd",
        bout_id="another-bout",
    )

    capture = extract_round5_runtime_identity(result, (foreign,))

    assert capture.status is RuntimeIdentityStatus.QUARANTINED
    assert capture.resources == ()
    assert capture.quarantine[0].resource_type == "round5_journal_scope"
