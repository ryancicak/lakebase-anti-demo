from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from server.cost_identity import AttributionStatus, capture_bout_cost_identity
from server.manifest import DemoManifest
from server.models import RoundId

START = datetime(2026, 8, 20, 12, 0, tzinfo=UTC)
END = START + timedelta(minutes=5)
INSTALLATION_ID = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"


def _lakebase(number: int) -> SimpleNamespace:
    branch = f"projects/install-r{number}/branches/production"
    return SimpleNamespace(
        project_id=f"install-r{number}",
        project_uid=f"project-uid-r{number}",
        branch_name=branch,
        branch_uid=f"branch-uid-r{number}",
        endpoint_name=f"{branch}/endpoints/primary",
        endpoint_uid=f"endpoint-uid-r{number}",
    )


def _aws_environment(number: int) -> SimpleNamespace:
    return SimpleNamespace(
        lakebase=_lakebase(number),
        aurora=SimpleNamespace(
            cluster_id=f"anti-demo-r{number}-aurora",
            cluster_resource_id=f"cluster-resource-r{number}",
            writer_instance_id=f"anti-demo-r{number}-writer",
            secret_arn=(f"arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-r{number}"),
        ),
        rds=SimpleNamespace(
            instance_id=f"anti-demo-r{number}-rds",
            resource_id=f"db-resource-r{number}",
            secret_arn=f"arn:aws:secretsmanager:us-west-2:123456789012:secret:rds-r{number}",
        ),
    )


def _manifest_v7() -> DemoManifest:
    environments = {
        RoundId.WAKE_IDLE_APP: _aws_environment(1),
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY: _aws_environment(2),
        RoundId.RECOVER_DELETED_ORDER: _aws_environment(3),
        RoundId.PUT_MODEL_SCORE_IN_APP: SimpleNamespace(lakebase=_lakebase(4)),
        RoundId.SURVIVE_CONNECTION_SPIKE: _aws_environment(5),
        RoundId.ANALYZE_LIVE_ORDERS: SimpleNamespace(lakebase=_lakebase(6)),
    }
    return DemoManifest.model_construct(
        manifest_version=7,
        installation_id=INSTALLATION_ID,
        aws=SimpleNamespace(account_id="123456789012", region="us-west-2"),
        round_environments=environments,
        round4=SimpleNamespace(pipeline_id="pipeline-r4", warehouse_id="warehouse-r4"),
        round6=SimpleNamespace(
            warehouse_id="warehouse-r6",
            cdf_config_id="cdf-r6",
            cdf_config_name="projects/install-r6/databases/default/cdf-configs/cdf-r6",
            cdf_status_id="live_orders",
            cdf_status_name=(
                "projects/install-r6/databases/default/cdf-configs/cdf-r6/cdf-statuses/live_orders"
            ),
            destination_catalog="main",
            destination_schema="anti_demo_r6",
            destination_schema_id="schema-r6",
            destination_table_full_name="main.anti_demo_r6.live_orders_history",
            destination_table_id="table-r6",
        ),
    )


def _capture(manifest: DemoManifest, round_id: RoundId | int):
    return capture_bout_cost_identity(
        manifest,
        round_id=round_id,
        bout_id="bout-123",
        session_id="session-456",
        window_start=START,
        window_end=END,
    )


def test_v7_captures_exact_lakebase_and_aws_database_identities() -> None:
    capture = _capture(_manifest_v7(), 2)
    by_type = {resource.resource_type: resource for resource in capture.resources}

    assert capture.status == AttributionStatus.READY
    assert capture.installation_id == INSTALLATION_ID
    assert capture.bout_id == "bout-123"
    assert capture.session_id == "session-456"
    assert capture.window.start_inclusive == START
    assert by_type["lakebase_project"].resource_id == "project-uid-r2"
    assert by_type["lakebase_branch"].resource_id == "branch-uid-r2"
    assert by_type["lakebase_endpoint"].resource_id == "endpoint-uid-r2"
    assert by_type["aurora_cluster"].resource_id == "cluster-resource-r2"
    assert by_type["aurora_cluster"].resource_arn == (
        "arn:aws:rds:us-west-2:123456789012:cluster:anti-demo-r2-aurora"
    )
    assert by_type["aurora_writer_instance"].resource_arn == (
        "arn:aws:rds:us-west-2:123456789012:db:anti-demo-r2-writer"
    )
    assert by_type["rds_postgres_instance"].resource_id == "db-resource-r2"


@pytest.mark.parametrize(
    ("round_id", "expected_types"),
    [
        (
            RoundId.PUT_MODEL_SCORE_IN_APP,
            {"database_table_sync_pipeline", "sql_warehouse"},
        ),
        (
            RoundId.ANALYZE_LIVE_ORDERS,
            {
                "sql_warehouse",
                "lakebase_cdf_config",
                "lakebase_cdf_status",
                "unity_catalog_schema",
                "unity_catalog_table",
            },
        ),
    ],
)
def test_native_pipeline_rounds_capture_sealed_components_without_aws_databases(
    round_id: RoundId, expected_types: set[str]
) -> None:
    capture = _capture(_manifest_v7(), round_id)
    resource_types = {resource.resource_type for resource in capture.resources}

    assert capture.status == AttributionStatus.READY
    assert expected_types <= resource_types
    assert {resource.provider for resource in capture.resources} == {"databricks"}


def test_round5_keeps_exact_static_resources_but_quarantines_unsealed_proxy() -> None:
    capture = _capture(_manifest_v7(), RoundId.SURVIVE_CONNECTION_SPIKE)

    assert capture.status == AttributionStatus.QUARANTINED
    assert {resource.resource_type for resource in capture.resources} >= {
        "lakebase_project",
        "aurora_cluster",
        "rds_postgres_instance",
    }
    assert [(item.provider, item.resource_type) for item in capture.quarantine] == [
        ("aws", "rds_proxy")
    ]
    assert "runtime-created" in capture.quarantine[0].reason


def test_v6_is_explicitly_unavailable_instead_of_using_shared_time_window() -> None:
    manifest = DemoManifest.model_construct(manifest_version=6, installation_id=None)

    capture = _capture(manifest, RoundId.WAKE_IDLE_APP)

    assert capture.status == AttributionStatus.ATTRIBUTION_UNAVAILABLE
    assert capture.resources == ()
    assert "shared resources" in capture.quarantine[0].reason


def test_ambiguous_aws_seal_is_quarantined_without_guessed_arns() -> None:
    manifest = _manifest_v7()
    environment = manifest.round_environments[RoundId.WAKE_IDLE_APP]
    environment.aurora.secret_arn = (
        "arn:aws:secretsmanager:us-east-1:123456789012:secret:wrong-region"
    )

    capture = _capture(manifest, RoundId.WAKE_IDLE_APP)

    assert capture.status == AttributionStatus.QUARANTINED
    assert all(resource.provider != "aws" for resource in capture.resources)
    assert capture.quarantine[0].resource_type == "database_environment"


def test_cost_window_is_half_open_utc_and_rejects_invalid_bounds() -> None:
    central = timezone(timedelta(hours=-5))
    capture = capture_bout_cost_identity(
        _manifest_v7(),
        round_id=4,
        bout_id=" bout ",
        session_id=" session ",
        window_start=START.astimezone(central),
        window_end=END.astimezone(central),
    )
    assert capture.window.start_inclusive.tzinfo is UTC
    assert capture.window.end_exclusive.tzinfo is UTC
    assert capture.bout_id == "bout"

    with pytest.raises(ValueError, match="positive duration"):
        capture_bout_cost_identity(
            _manifest_v7(),
            round_id=4,
            bout_id="bout",
            session_id="session",
            window_start=END,
            window_end=START,
        )
