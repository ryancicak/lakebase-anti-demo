import hashlib
import json
import logging
import os
from datetime import UTC, datetime, timedelta

import conftest
import pytest
from pydantic import ValidationError

import app as app_module
from server.lifecycle import _reseal_round5_harness
from server.manifest import (
    _NO_MANIFEST_SELECTED,
    MANIFEST_JSON_ENV,
    AuroraEnvironmentSeal,
    AwsManifest,
    AwsResources,
    DatabricksManifest,
    DemoManifest,
    LakebaseEnvironmentSeal,
    RdsEnvironmentSeal,
    Round3Anchor,
    Round3AnchorLane,
    Round4Resources,
    Round5FrozenConstants,
    Round5OwnershipTags,
    Round5Resources,
    Round6Resources,
    RoundEnvironmentSeal,
    apply_manifest_environment,
    load_manifest,
    manifest_path,
    save_manifest,
)
from server.models import RoundId
from server.round6_contract import round6_contract_sha256


def test_an_unselected_manifest_refuses_instead_of_resolving_a_previous_generation(
    monkeypatch,
) -> None:
    """A bare ./antidemo once resolved .anti-demo/manifest.json, a dead generation."""
    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)

    with pytest.raises(RuntimeError, match="ANTI_DEMO_MANIFEST"):
        manifest_path()


def test_an_unselected_manifest_refuses_to_be_loaded_or_saved(monkeypatch, tmp_path) -> None:
    # Both names, because `load_manifest` reads the JSON one *first*: a suite that
    # neutralised only `ANTI_DEMO_MANIFEST` would leave the higher-precedence
    # source live, and the two delenv calls below are how this test found that out
    # -- they were needed. `tests/conftest.py` now scrubs both for every test, and
    # this asserts the JSON one is in that list, because it was deliberately left
    # out of it once on the argument that only a deployed app ever sets it.
    # Whether an operator exports it is not the question; `load_manifest` reads it
    # either way, and a test in this suite reaches that read.
    assert MANIFEST_JSON_ENV in conftest.AMBIENT_INSTALLATION_NAMES
    monkeypatch.delenv("ANTI_DEMO_MANIFEST", raising=False)
    monkeypatch.delenv(MANIFEST_JSON_ENV, raising=False)

    with pytest.raises(RuntimeError, match="No owned demo manifest is selected"):
        load_manifest()
    with pytest.raises(RuntimeError, match="No owned demo manifest is selected"):
        save_manifest(_round_one_manifest(tmp_path))


def test_the_unselected_manifest_message_names_the_command_that_sets_it() -> None:
    """Refusing to guess is only useful if the refusal says what to run instead.

    The suggested command is the one that can actually change the caller's
    environment: plain ./bootstrap.sh runs in a child process and cannot.
    """

    assert 'eval "$(./bootstrap.sh --print-env)"' in _NO_MANIFEST_SELECTED
    assert "There is no default" in _NO_MANIFEST_SELECTED


def test_an_explicitly_selected_manifest_resolves_exactly_as_before(monkeypatch, tmp_path) -> None:
    """The explicit path is the only path, and it is unchanged."""
    selected = tmp_path / "generation" / "manifest.json"
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(selected))

    assert manifest_path() == selected.resolve()

    save_manifest(_round_one_manifest(tmp_path))
    assert selected.exists()
    assert load_manifest().run_id == "ad-test-selected"


def _round_one_manifest(tmp_path) -> DemoManifest:
    return DemoManifest(
        run_id="ad-test-selected",
        owner="operator@databricks.com",
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        status="ready",
        aws=AwsManifest(
            profile="fe-vm-test",
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state=str(tmp_path / "terraform.tfstate"),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-selected",
            endpoint_name="projects/ad-test-selected/branches/production/endpoints/primary",
            user="operator@databricks.com",
        ),
        schema_sha256="abc123",
    )


def test_manifest_is_secret_free_private_and_can_drive_runtime_environment(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("AWS_EXPECTED_ACCOUNT_ID", "before-test")
    monkeypatch.setenv("LAKEBASE_ENDPOINT_NAME", "before-test")
    monkeypatch.delenv("ANTI_DEMO_LOCAL_OPERATOR", raising=False)
    monkeypatch.delenv("ANTI_DEMO_LOCAL_OPERATOR_EMAIL", raising=False)
    monkeypatch.delenv("ANTI_DEMO_LOCAL_OPERATOR_ID", raising=False)
    path = tmp_path / "manifest.json"
    manifest = DemoManifest(
        run_id="ad-test-001",
        owner="operator@databricks.com",
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=24),
        status="ready",
        aws=AwsManifest(
            profile="sandbox-admin",
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state=str(tmp_path / "terraform.tfstate"),
            resources=AwsResources(
                aurora_cluster_id="anti-demo-aurora",
                aurora_secret_arn=(
                    "arn:aws:secretsmanager:us-west-2:123456789012:secret:anti-demo"
                ),
                rds_instance_id="anti-demo-rds",
            ),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-001",
            endpoint_name="projects/ad-test-001/branches/production/endpoints/primary",
            user="operator@databricks.com",
        ),
        schema_sha256="abc123",
    )

    save_manifest(manifest, path)
    monkeypatch.setenv("ANTI_DEMO_MANIFEST", str(path))
    loaded = load_manifest(path)
    assert load_manifest().run_id == "ad-test-001"
    apply_manifest_environment(loaded)

    contents = path.read_text(encoding="utf-8")
    assert "password" not in contents.lower()
    assert path.stat().st_mode & 0o777 == 0o600
    assert loaded.run_id == "ad-test-001"
    assert os.environ["AWS_EXPECTED_ACCOUNT_ID"] == "123456789012"
    assert os.environ["LAKEBASE_ENDPOINT_NAME"] == (
        "projects/ad-test-001/branches/production/endpoints/primary"
    )
    assert os.environ["LAKEBASE_EXPECTED_REGION"] == "us-west-2"
    assert os.environ["ANTI_DEMO_COORDINATION_ENDPOINT_NAME"] == (
        "projects/ad-test-001/branches/coordination/endpoints/primary"
    )
    assert os.environ["ANTI_DEMO_LOCAL_OPERATOR"] == "Operator"
    assert os.environ["ANTI_DEMO_LOCAL_OPERATOR_EMAIL"] == "operator@databricks.com"

    environment_manifest = manifest.model_copy(update={"run_id": "ad-test-from-environment"})
    monkeypatch.setenv("ANTI_DEMO_MANIFEST_JSON", environment_manifest.model_dump_json())
    assert load_manifest().run_id == "ad-test-from-environment"
    assert load_manifest(path).run_id == "ad-test-001"


def test_environment_auth_manifest_never_serializes_ambient_keys(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "must-not-be-written")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-be-written-either")
    manifest = DemoManifest(
        run_id="ad-test-env",
        owner="operator@databricks.com",
        created_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        status="ready",
        aws=AwsManifest(
            auth_mode="environment",
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state=str(tmp_path / "terraform.tfstate"),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-env",
            endpoint_name="projects/ad-test-env/branches/production/endpoints/primary",
        ),
    )
    path = save_manifest(manifest, tmp_path / "manifest.json")

    contents = path.read_text(encoding="utf-8")
    assert '"auth_mode": "environment"' in contents
    assert "must-not-be-written" not in contents
    assert "AWS_ACCESS_KEY_ID" not in contents


def test_round3_anchor_round_trips_and_legacy_manifest_remains_readable(tmp_path) -> None:
    reset_at = datetime(2026, 8, 18, 15, 0, tzinfo=UTC)
    manifest = DemoManifest(
        run_id="ad-test-anchor",
        owner="operator@databricks.com",
        created_at=reset_at - timedelta(hours=1),
        expires_at=reset_at + timedelta(hours=24),
        status="ready",
        aws=AwsManifest(
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state=str(tmp_path / "terraform.tfstate"),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-anchor",
            endpoint_name=("projects/ad-test-anchor/branches/production/endpoints/primary"),
        ),
        schema_sha256="schema-hash",
        last_reset_at=reset_at,
        round3_anchor=Round3Anchor(
            run_id="ad-test-anchor",
            owner="operator@databricks.com",
            aws_account_id="123456789012",
            aws_region="us-west-2",
            contract_sha256="contract-hash",
            schema_sha256="schema-hash",
            last_reset_at=reset_at,
            lakebase=Round3AnchorLane(
                provider="lakebase",
                source_id="projects/ad-test-anchor/branches/production/endpoints/primary",
                recovery_at=reset_at + timedelta(seconds=2),
            ),
            aurora=Round3AnchorLane(
                provider="aurora",
                source_id="anti-demo-aurora",
                recovery_at=reset_at + timedelta(seconds=3),
            ),
            rds=Round3AnchorLane(
                provider="rds",
                source_id="anti-demo-rds",
                recovery_at=reset_at + timedelta(seconds=4),
            ),
        ),
    )
    path = save_manifest(manifest, tmp_path / "manifest.json")

    loaded = load_manifest(path)
    assert loaded.round3_anchor == manifest.round3_anchor
    legacy = DemoManifest.model_validate(manifest.model_dump(exclude={"round3_anchor"}))
    assert legacy.round3_anchor is None


def test_round4_manifest_v2_round_trips_exact_returned_resource_identities(tmp_path) -> None:
    created_at = datetime(2026, 8, 18, 20, 0, tzinfo=UTC)
    round4 = Round4Resources(
        warehouse_id="0123456789abcdef",
        setup_principal="operator@databricks.com",
        app_service_principal_client_id="11111111-2222-3333-4444-555555555555",
        source_table_full_name=(
            "customer_catalog.anti_demo_ad_20260818_2000_abcd.model_scores_source"
        ),
        storage_catalog="customer_catalog",
        storage_schema="anti_demo_sync_ad_20260818_2000_abcd",
        synced_table_id=("customer_catalog.anti_demo_online_ad_20260818_2000_abcd.model_scores"),
        synced_table_resource_name=(
            "synced_tables/customer_catalog.anti_demo_online_ad_20260818_2000_abcd.model_scores"
        ),
        synced_table_uid="6fb5fb44-954a-49ce-a751-2a9fbc0686c2",
        pipeline_id="f2af6c88-7da3-40cf-881f-7971e50a6b18",
        physical_database="anti_demo",
        physical_schema="anti_demo_online_ad_20260818_2000_abcd",
        physical_table="model_scores",
        project_uid="9db34e86-cb48-4b34-9be2-c93309ff6417",
        branch_uid="77a0203d-5ffd-4f18-9cbd-942ee4d9193e",
        branch="projects/ad-20260818-2000-abcd/branches/production",
        endpoint_name=("projects/ad-20260818-2000-abcd/branches/production/endpoints/primary"),
        contract_sha256="a" * 64,
    )
    manifest = DemoManifest(
        manifest_version=2,
        run_id="ad-20260818-2000-abcd",
        owner="operator@databricks.com",
        created_at=created_at,
        expires_at=created_at + timedelta(hours=24),
        status="ready",
        aws=AwsManifest(
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state=str(tmp_path / "terraform.tfstate"),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-20260818-2000-abcd",
            endpoint_name=round4.endpoint_name,
        ),
        round4=round4,
    )

    loaded = load_manifest(save_manifest(manifest, tmp_path / "manifest-v2.json"))

    assert loaded.manifest_version == 2
    assert loaded.round4 == round4
    assert loaded.round4.pipeline_id == "f2af6c88-7da3-40cf-881f-7971e50a6b18"
    assert loaded.round4.synced_table_uid == "6fb5fb44-954a-49ce-a751-2a9fbc0686c2"
    assert loaded.round4.project_uid == "9db34e86-cb48-4b34-9be2-c93309ff6417"
    assert loaded.round4.branch_uid == "77a0203d-5ffd-4f18-9cbd-942ee4d9193e"


def test_manifest_v2_rejects_missing_round4_seal(monkeypatch, caplog) -> None:
    monkeypatch.setenv(
        MANIFEST_JSON_ENV,
        """{
            "manifest_version": 2,
            "run_id": "ad-test-v2-incomplete",
            "owner": "operator@databricks.com",
            "created_at": "2026-08-18T20:00:00Z",
            "expires_at": "2026-08-19T20:00:00Z",
            "status": "provisioning",
            "aws": {
                "account_id": "123456789012",
                "region": "us-west-2",
                "operator_cidr": "203.0.113.10/32",
                "terraform_state": "/tmp/terraform.tfstate"
            },
            "databricks": {
                "profile": "fe-vm-test",
                "project_id": "ad-test-v2-incomplete",
                "endpoint_name":
                    "projects/ad-test-v2-incomplete/branches/production/endpoints/primary"
            }
        }""",
    )
    with pytest.raises(ValidationError, match="requires sealed Round 4 resources"):
        load_manifest()

    # The refusal is right, and `ValidationError` is a `ValueError`, so for a long
    # while it went straight through `app._owned_manifest_or_none` -- which caught
    # only `RuntimeError` -- and out of the lifespan, taking down a server whose
    # premise is that any one round may be absent without the other five going
    # with it. The premise wins: an unreadable ambient manifest is "no owned
    # manifest", exactly as the function's name says. Loudly, though, because a
    # manifest that will not parse is nobody's normal state, and an installation
    # silently serving zero live rounds is the failure this repository has already
    # had once. The log has to name the variable: in deployed mode it arrives from
    # a Databricks secret and there is no file for anyone to go and look at.
    with caplog.at_level(logging.DEBUG, logger="app"):
        assert app_module._owned_manifest_or_none(None) is None
    reported = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert len(reported) == 1
    assert MANIFEST_JSON_ENV in reported[0].getMessage()
    assert "requires sealed Round 4 resources" in reported[0].getMessage()


def _round5_round4_resources() -> Round4Resources:
    return Round4Resources(
        warehouse_id="warehouse",
        setup_principal="operator@databricks.com",
        app_service_principal_client_id="11111111-2222-3333-4444-555555555555",
        source_table_full_name="catalog.source.model_scores_source",
        storage_catalog="catalog",
        storage_schema="storage",
        synced_table_id="catalog.online.model_scores",
        synced_table_resource_name="synced_tables/catalog.online.model_scores",
        synced_table_uid="synced-table-uid",
        pipeline_id="pipeline-id",
        physical_database="anti_demo",
        physical_schema="online",
        physical_table="model_scores",
        project_uid="project-uid",
        branch_uid="branch-uid",
        branch="projects/ad-test-v3/branches/production",
        endpoint_name="projects/ad-test-v3/branches/production/endpoints/primary",
        contract_sha256="a" * 64,
    )


def _round5_resources() -> Round5Resources:
    values = dict(
        lakebase_direct_host="direct.database.cloud.databricks.com",
        lakebase_pooled_host="pooled.database.cloud.databricks.com",
        aurora_direct_host="anti-demo.cluster-abc123.us-west-2.rds.amazonaws.com",
        aurora_cluster_id="anti-demo-aurora",
        aurora_cluster_resource_id="cluster-ABCDEFGHIJKLMNOPQ",
        aurora_writer_instance_id="anti-demo-aurora-writer",
        aurora_master_secret_arn=(
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-master-AbCdEf"
        ),
        rds_direct_host="anti-demo.abc123.us-west-2.rds.amazonaws.com",
        rds_master_secret_arn=(
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:rds-master-AbCdEf"
        ),
        rds_resource_id="db-ABCDEFGHIJKLMNOPQ",
        vpc_id="vpc-0123456789abcdef0",
        proxy_subnet_ids=("subnet-0123456789abcdef0", "subnet-1123456789abcdef0"),
        control_role_arn="arn:aws:iam::123456789012:role/anti-demo-round5-control",
        control_role_trusted_principal_arn=("arn:aws:iam::123456789012:role/anti-demo-app"),
        proxy_service_role_arn=("arn:aws:iam::123456789012:role/anti-demo-round5-proxy"),
        proxy_service_policy_name="r5-proxy-secrets-abc123",
        aurora_proxy_secret_arn=(
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora-proxy-AbCdEf"
        ),
        rds_proxy_secret_arn=(
            "arn:aws:secretsmanager:us-west-2:123456789012:secret:rds-proxy-AbCdEf"
        ),
        runner_permissions_boundary_arn=(
            "arn:aws:iam::123456789012:policy/anti-demo-round5-runner"
        ),
        runner_instance_id="i-0123456789abcdef0",
        runner_instance_profile_arn=(
            "arn:aws:iam::123456789012:instance-profile/anti-demo-round5-runner"
        ),
        runner_role_arn="arn:aws:iam::123456789012:role/anti-demo-round5-runner",
        runner_subnet_id="subnet-0123456789abcdef0",
        runner_security_group_id="sg-0123456789abcdef0",
        runner_egress_rule_id="sgr-0123456789abcdef0",
        runner_public_key_sha256="e" * 64,
        lakebase_credential_sha256="f" * 64,
        aurora_credential_sha256="8" * 64,
        rds_credential_sha256="9" * 64,
        bout_name_prefix="anti-demo-r5-bout",
        ownership_tags=Round5OwnershipTags(
            anti_demo_run_id="ad-test-v3",
            owner="operator@databricks.com",
            expires_at="2026-08-19T20:00:00Z",
        ),
        trust_bundle_sha256="d" * 64,
        harness_sha256="b" * 64,
        frozen_constants=Round5FrozenConstants(),
        contract_sha256=("f0e9a6960fb22cc052486b62cf01e32dcaabacf70508a2bc087ddc25deafa81c"),
    )
    provisional = Round5Resources.model_construct(
        **values, baseline_sha256="0" * 64, config_sha256="0" * 64
    )
    baseline_sha256 = hashlib.sha256(
        json.dumps(
            provisional.model_dump(
                mode="json",
                exclude={"baseline_sha256", "config_sha256"},
                exclude_none=True,
            ),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    config_sha256 = hashlib.sha256(
        json.dumps(
            {
                "baseline_sha256": baseline_sha256,
                "contract_sha256": values["contract_sha256"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return Round5Resources(**values, baseline_sha256=baseline_sha256, config_sha256=config_sha256)


def _round5_manifest(tmp_path, **updates) -> DemoManifest:
    created_at = datetime(2026, 8, 18, 20, 0, tzinfo=UTC)
    values = {
        "manifest_version": 5,
        "run_id": "ad-test-v3",
        "owner": "operator@databricks.com",
        "created_at": created_at,
        "expires_at": created_at + timedelta(hours=24),
        "status": "ready",
        "aws": AwsManifest(
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state=str(tmp_path / "terraform.tfstate"),
        ),
        "databricks": DatabricksManifest(
            profile="fe-vm-test",
            project_id="ad-test-v3",
            endpoint_name="projects/ad-test-v3/branches/production/endpoints/primary",
        ),
        "round4": _round5_round4_resources(),
        "round5": _round5_resources(),
    }
    values.update(updates)
    return DemoManifest(**values)


def test_manifest_v5_round_trips_complete_static_seal_and_is_factory_ready(tmp_path) -> None:
    manifest = _round5_manifest(tmp_path)

    loaded = load_manifest(save_manifest(manifest, tmp_path / "manifest-v5.json"))

    assert loaded.round5_ready is True
    assert loaded.require_round5_resources() == manifest.round5
    assert loaded.require_round5_resources().lakebase_pooled_host == (
        "pooled.database.cloud.databricks.com"
    )
    assert loaded.require_round5_resources().aurora_cluster_resource_id == (
        "cluster-ABCDEFGHIJKLMNOPQ"
    )
    assert loaded.require_round5_resources().aurora_credential_sha256 == "8" * 64
    assert loaded.require_round5_resources().frozen_constants.scored_attempts_per_lane == 128
    assert loaded.require_round5_resources().runner_harness_sha256 == "b" * 64
    assert loaded.require_round5_resources().runner_trust_policy["Statement"] == [
        {
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ]
    assert loaded.require_round5_resources().control_trust_policy["Statement"] == [
        {
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:role/anti-demo-app"},
            "Action": "sts:AssumeRole",
        }
    ]


def test_round5_ownership_tags_preserve_legacy_and_seal_v7_scope() -> None:
    legacy = Round5OwnershipTags(
        anti_demo_run_id="ad-test-v3",
        owner="operator@databricks.com",
        expires_at="2026-08-19T20:00:00Z",
    )
    assert "anti_demo_installation_slug" not in legacy.model_dump()
    assert "anti-demo-installation-slug" not in legacy.as_aws_tags()

    scoped = legacy.model_copy(
        update={
            "anti_demo_installation_slug": "i0123456789abcdefabcd-r5",
            "anti_demo_round": "r5",
        }
    )
    assert scoped.as_aws_tags()["anti-demo-installation-slug"] == (
        "i0123456789abcdefabcd-r5"
    )
    assert scoped.as_aws_tags()["anti-demo-round"] == "r5"

    with pytest.raises(ValidationError, match="installation ownership scope is incomplete"):
        Round5OwnershipTags(
            anti_demo_run_id="ad-test-v3",
            owner="operator@databricks.com",
            expires_at="2026-08-19T20:00:00Z",
            anti_demo_round="r5",
        )


def test_round5_harness_reseal_updates_only_harness_and_canonical_hashes() -> None:
    sealed = _round5_resources()

    resealed = _reseal_round5_harness(sealed, "a" * 64)

    assert resealed.harness_sha256 == "a" * 64
    assert resealed.baseline_sha256 != sealed.baseline_sha256
    assert resealed.config_sha256 != sealed.config_sha256
    assert resealed.model_dump(
        exclude={"harness_sha256", "baseline_sha256", "config_sha256"}
    ) == sealed.model_dump(exclude={"harness_sha256", "baseline_sha256", "config_sha256"})


def test_manifest_v5_rejects_incomplete_or_changed_contract(tmp_path) -> None:
    with pytest.raises(ValidationError, match="requires the static Round 5 baseline seal"):
        _round5_manifest(tmp_path, round5=None)

    payload = _round5_resources().model_dump()
    payload["contract_sha256"] = "d" * 64
    with pytest.raises(ValidationError, match="contract_sha256"):
        Round5Resources.model_validate(payload)

    constants = Round5FrozenConstants().model_dump()
    constants["scored_attempts_per_lane"] = 127
    with pytest.raises(ValidationError, match="scored_attempts_per_lane"):
        Round5FrozenConstants.model_validate(constants)


def test_manifest_v5_serializes_only_sealed_secret_arns_and_v2_stays_planned(tmp_path) -> None:
    contents = _round5_manifest(tmp_path).model_dump_json(exclude_none=True)
    assert "rds-master-AbCdEf" in contents
    assert "aurora-master-AbCdEf" in contents
    assert "rds-proxy-AbCdEf" in contents
    assert "aurora-proxy-AbCdEf" in contents
    assert "rds_proxy_arn" not in contents
    assert "rds_proxy_endpoint" not in contents
    assert "lakebase_secret_arn" not in contents
    assert "per_bout_role_boundary_arn" not in contents
    assert "secret_name_prefix" not in contents
    assert "must-never-be-serialized" not in contents

    payload = _round5_resources().model_dump()
    payload["password"] = "must-never-be-serialized"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Round5Resources.model_validate(payload)

    v2 = _round5_manifest(tmp_path, manifest_version=2, round5=None)
    assert v2.round5_ready is False
    with pytest.raises(RuntimeError, match="factory-ready manifest v5"):
        v2.require_round5_resources()


def test_manifest_v4_legacy_seal_loads_only_for_cleanup_or_resealing(tmp_path) -> None:
    payload = _round5_resources().model_dump(mode="json")
    for field in (
        "aurora_direct_host",
        "aurora_cluster_id",
        "aurora_cluster_resource_id",
        "aurora_writer_instance_id",
        "aurora_master_secret_arn",
        "aurora_credential_sha256",
        "proxy_service_role_arn",
        "proxy_service_policy_name",
        "aurora_proxy_secret_arn",
        "rds_proxy_secret_arn",
    ):
        payload.pop(field)
    payload["per_bout_role_boundary_arn"] = (
        "arn:aws:iam::123456789012:policy/anti-demo-round5-per-bout"
    )
    payload["secret_name_prefix"] = "anti-demo/r5/secret"
    unhashed = {
        key: value
        for key, value in payload.items()
        if key not in {"baseline_sha256", "config_sha256"} and value is not None
    }
    payload["baseline_sha256"] = hashlib.sha256(
        json.dumps(unhashed, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    payload["config_sha256"] = hashlib.sha256(
        json.dumps(
            {
                "baseline_sha256": payload["baseline_sha256"],
                "contract_sha256": payload["contract_sha256"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()

    manifest = _round5_manifest(
        tmp_path,
        manifest_version=4,
        round5=Round5Resources.model_validate(payload),
    )

    assert manifest.round5_ready is False
    with pytest.raises(RuntimeError, match="factory-ready manifest v5"):
        manifest.require_round5_resources()


def _v7_lakebase(project_id: str, number: int) -> LakebaseEnvironmentSeal:
    branch = f"projects/{project_id}/branches/production"
    return LakebaseEnvironmentSeal(
        project_id=project_id,
        project_uid=f"project-uid-{number}",
        branch_name=branch,
        branch_uid=f"branch-uid-{number}",
        endpoint_name=f"{branch}/endpoints/primary",
        endpoint_uid=f"endpoint-uid-{number}",
        direct_host=f"r{number}.direct.database.cloud.databricks.com",
        pooled_host=f"r{number}.pooled.database.cloud.databricks.com",
    )


def _v7_aurora(number: int) -> AuroraEnvironmentSeal:
    return AuroraEnvironmentSeal(
        cluster_id=f"install-r{number}-aurora",
        cluster_resource_id=f"cluster-RESOURCE{number}",
        writer_instance_id=f"install-r{number}-aurora-writer",
        direct_host=f"r{number}.cluster-abc123.us-west-2.rds.amazonaws.com",
        secret_arn=(
            f"arn:aws:secretsmanager:us-west-2:123456789012:secret:r{number}-aurora-AbCdEf"
        ),
        security_group_id=f"sg-{number:017x}",
        db_subnet_group_name=f"install-r{number}-subnets",
    )


def _v7_rds(number: int) -> RdsEnvironmentSeal:
    return RdsEnvironmentSeal(
        instance_id=f"install-r{number}-rds",
        resource_id=f"db-RESOURCE{number}",
        direct_host=f"r{number}.abc123.us-west-2.rds.amazonaws.com",
        secret_arn=(f"arn:aws:secretsmanager:us-west-2:123456789012:secret:r{number}-rds-AbCdEf"),
        security_group_id=f"sg-{number + 6:017x}",
        db_subnet_group_name=f"install-r{number}-subnets",
    )


def _v7_round6(lakebase: LakebaseEnvironmentSeal) -> Round6Resources:
    created = datetime(2026, 8, 19, 18, 0, tzinfo=UTC)
    database = f"{lakebase.branch_name}/databases/databricks-postgres"
    cdf_config = f"{database}/cdf-configs/anti_demo_r6"
    values = {
        "warehouse_id": "warehouse-id",
        "setup_principal": "operator@databricks.com",
        "app_service_principal_client_id": "app-client-id",
        "branch_name": lakebase.branch_name,
        "branch_id": "production",
        "branch_uid": lakebase.branch_uid,
        "branch_create_time": created,
        "endpoint_name": lakebase.endpoint_name,
        "endpoint_id": "primary",
        "endpoint_uid": lakebase.endpoint_uid,
        "endpoint_create_time": created,
        "database_resource_name": database,
        "database_resource_id": "databricks-postgres",
        "postgres_database": "databricks_postgres",
        "source_schema": "anti_demo_r6",
        "source_table": "live_orders",
        "source_table_oid": 16432,
        "cdf_config_name": cdf_config,
        "cdf_config_id": "anti_demo_r6",
        "cdf_config_create_time": created,
        "cdf_status_name": f"{cdf_config}/cdf-statuses/live_orders",
        "cdf_status_id": "live_orders",
        "cdf_status_create_time": created,
        "destination_catalog": "main",
        "destination_schema": "anti_demo_r6",
        "destination_schema_id": "schema-id",
        "destination_table_full_name": "main.anti_demo_r6.live_orders_history",
        "destination_table_id": "table-id",
        "baseline_order_id": "00000000-0000-4000-8000-000000000006",
        "baseline_proof_nonce": "round6-baseline",
        "baseline_sku": "RED-GLOVE",
        "baseline_store": "CHICAGO",
        "baseline_quantity": 1,
        "baseline_total_cents": 8450,
        "baseline_status": "baseline",
    }
    contract_values = {
        key: value.isoformat() if isinstance(value, datetime) else value
        for key, value in values.items()
        if key not in {"warehouse_id", "setup_principal", "app_service_principal_client_id"}
    }
    return Round6Resources(
        **values,
        contract_sha256=round6_contract_sha256(**contract_values),
    )


def _v7_round5(
    lakebase: LakebaseEnvironmentSeal,
    aurora: AuroraEnvironmentSeal,
    rds: RdsEnvironmentSeal,
) -> Round5Resources:
    values = _round5_resources().model_dump(
        exclude={"baseline_sha256", "config_sha256"}, exclude_none=True
    )
    values.update(
        lakebase_direct_host=lakebase.direct_host,
        lakebase_pooled_host=lakebase.pooled_host,
        aurora_direct_host=aurora.direct_host,
        aurora_cluster_id=aurora.cluster_id,
        aurora_cluster_resource_id=aurora.cluster_resource_id,
        aurora_writer_instance_id=aurora.writer_instance_id,
        aurora_master_secret_arn=aurora.secret_arn,
        rds_direct_host=rds.direct_host,
        rds_master_secret_arn=rds.secret_arn,
        rds_resource_id=rds.resource_id,
    )
    baseline = hashlib.sha256(
        json.dumps(
            values,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    config = hashlib.sha256(
        json.dumps(
            {
                "baseline_sha256": baseline,
                "contract_sha256": values["contract_sha256"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return Round5Resources(**values, baseline_sha256=baseline, config_sha256=config)


def _v7_manifest(tmp_path) -> DemoManifest:
    lakebase = {
        round_id: _v7_lakebase(f"install-r{number}", number)
        for number, round_id in enumerate(RoundId, start=1)
    }
    aws_numbers = {
        RoundId.WAKE_IDLE_APP: 1,
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY: 2,
        RoundId.RECOVER_DELETED_ORDER: 3,
        RoundId.SURVIVE_CONNECTION_SPIKE: 5,
    }
    environments = {
        round_id: RoundEnvironmentSeal(
            lakebase=sealed,
            aurora=_v7_aurora(aws_numbers[round_id]) if round_id in aws_numbers else None,
            rds=_v7_rds(aws_numbers[round_id]) if round_id in aws_numbers else None,
        )
        for round_id, sealed in lakebase.items()
    }
    round1 = environments[RoundId.WAKE_IDLE_APP]
    round4_lakebase = lakebase[RoundId.PUT_MODEL_SCORE_IN_APP]
    round5 = environments[RoundId.SURVIVE_CONNECTION_SPIKE]
    round6_lakebase = lakebase[RoundId.ANALYZE_LIVE_ORDERS]
    assert round1.aurora and round1.rds and round5.aurora and round5.rds
    round4 = _round5_round4_resources().model_copy(
        update={
            "project_uid": round4_lakebase.project_uid,
            "branch_uid": round4_lakebase.branch_uid,
            "branch": round4_lakebase.branch_name,
            "endpoint_name": round4_lakebase.endpoint_name,
        }
    )
    coordination = _v7_lakebase("install-coordination", 7)
    return _round5_manifest(
        tmp_path,
        manifest_version=7,
        installation_id="018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1",
        aws=AwsManifest(
            account_id="123456789012",
            region="us-west-2",
            operator_cidr="203.0.113.10/32",
            terraform_state=str(tmp_path / "terraform.tfstate"),
            resources=AwsResources(
                aurora_cluster_id=round1.aurora.cluster_id,
                aurora_writer_instance_id=round1.aurora.writer_instance_id,
                aurora_secret_arn=round1.aurora.secret_arn,
                rds_instance_id=round1.rds.instance_id,
                rds_secret_arn=round1.rds.secret_arn,
                security_group_id=round1.aurora.security_group_id,
                rds_security_group_id=round1.rds.security_group_id,
                db_subnet_group_name=round1.aurora.db_subnet_group_name,
            ),
        ),
        databricks=DatabricksManifest(
            profile="fe-vm-test",
            project_id=round1.lakebase.project_id,
            endpoint_name=round1.lakebase.endpoint_name,
            coordination_endpoint_name=coordination.endpoint_name,
        ),
        round4=round4,
        round5=_v7_round5(round5.lakebase, round5.aurora, round5.rds),
        round6=_v7_round6(round6_lakebase),
        round_environments=environments,
        coordination_environment=coordination,
    )


def test_manifest_v7_round_trips_immutable_installation_and_exact_environments(
    tmp_path,
) -> None:
    manifest = _v7_manifest(tmp_path)
    loaded = load_manifest(save_manifest(manifest, tmp_path / "manifest-v7.json"))

    assert set(loaded.round_environments or {}) == set(RoundId)
    assert loaded.round_lakebase(1).project_id == "install-r1"
    assert loaded.round_environment(RoundId.SURVIVE_CONNECTION_SPIKE).aurora is not None
    assert loaded.round_environment(4).aurora is None
    assert loaded.round_environment(6).rds is None
    assert loaded.coordination_lakebase == loaded.coordination_environment
    with pytest.raises(ValidationError, match="frozen"):
        loaded.installation_id = "019f6f50-7d3a-7cc1-9d5d-4d9ac8d107a2"

    incomplete = loaded.model_dump(mode="json")
    incomplete["round_environments"].pop("analyze_live_orders_without_slowing_checkout")
    with pytest.raises(ValidationError, match="exactly the six canonical RoundId keys"):
        DemoManifest.model_validate(incomplete)

    invalid_installation = loaded.model_dump(mode="json")
    invalid_installation["installation_id"] = "7d3a"
    with pytest.raises(ValidationError, match="installation_id"):
        DemoManifest.model_validate(invalid_installation)

    coordination_alias = loaded.model_dump(mode="json")
    coordination_alias["coordination_environment"] = coordination_alias["round_environments"][
        "make_schema_change_safely"
    ]["lakebase"]
    coordination_alias["databricks"]["coordination_endpoint_name"] = coordination_alias[
        "coordination_environment"
    ]["endpoint_name"]
    with pytest.raises(ValidationError, match="coordination project aliases"):
        DemoManifest.model_validate(coordination_alias)

    provisional_round1 = loaded.round_environment(1)
    provisional = _round5_manifest(
        tmp_path,
        installation_id=loaded.installation_id,
        round_environments={RoundId.WAKE_IDLE_APP: provisional_round1},
    )
    assert provisional.round_lakebase(1) == provisional_round1.lakebase


@pytest.mark.parametrize(
    ("target", "source", "message"),
    [
        (
            ("round_environments", "make_schema_change_safely", "lakebase", "direct_host"),
            ("round_environments", "wake_idle_app", "lakebase", "direct_host"),
            "cross-round alias of Lakebase direct host",
        ),
        (
            ("round_environments", "make_schema_change_safely", "aurora", "cluster_id"),
            ("round_environments", "wake_idle_app", "aurora", "cluster_id"),
            "cross-round alias of Aurora cluster",
        ),
        (
            (
                "round_environments",
                "make_schema_change_safely",
                "aurora",
                "writer_instance_id",
            ),
            ("round_environments", "wake_idle_app", "aurora", "writer_instance_id"),
            "cross-round alias of Aurora writer",
        ),
        (
            ("round_environments", "make_schema_change_safely", "rds", "instance_id"),
            ("round_environments", "wake_idle_app", "rds", "instance_id"),
            "cross-round alias of RDS instance",
        ),
        (
            ("round4", "endpoint_name"),
            ("round_environments", "wake_idle_app", "lakebase", "endpoint_name"),
            "Round 4 adapter identities",
        ),
        (
            ("round_environments", "survive_connection_spike", "lakebase", "direct_host"),
            "changed.direct.database.cloud.databricks.com",
            "Round 5 adapter identities",
        ),
        (
            (
                "round_environments",
                "analyze_live_orders_without_slowing_checkout",
                "lakebase",
                "endpoint_uid",
            ),
            "changed-endpoint-uid",
            "Round 6 adapter identities",
        ),
        (
            ("round_environments", "put_model_score_in_app", "aurora"),
            ("round_environments", "wake_idle_app", "aurora"),
            "must not seal unused AWS databases",
        ),
        (
            ("databricks", "project_id"),
            "not-the-round1-project",
            "Round 1 mirror",
        ),
    ],
)
def test_manifest_v7_fails_closed_on_aliases_and_adapter_drift(
    tmp_path, target, source, message
) -> None:
    payload = _v7_manifest(tmp_path).model_dump(mode="json")

    def lookup(path):
        value = payload
        for part in path:
            value = value[part]
        return value

    parent = lookup(target[:-1])
    parent[target[-1]] = lookup(source) if isinstance(source, tuple) else source

    with pytest.raises(ValidationError, match=message):
        DemoManifest.model_validate(payload)
