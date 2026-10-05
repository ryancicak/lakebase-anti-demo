"""Round 6's AWS lane as built: its Terraform, its IAM, its Glue script and its manifest seal.

The lane stands for the life of the installation and is parked between bouts, so these pin the
properties that make that safe and fair: nothing of it exists until the credential does, it is
added without touching any other round, the role Unity Catalog reads through can do nothing but
read, the task and job never start themselves, and a lane that stopped half-way reads as an
interrupted provision rather than a finished one.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from server import lifecycle
from server.manifest import DemoManifest, RoundId

REPO = Path(__file__).resolve().parent.parent
AWS = REPO / "infra" / "aws"
TERRAFORM = (AWS / "round6_aws.tf").read_text(encoding="utf-8")


def _block(source: str, header: str) -> str:
    start = source.index(header)
    depth = 0
    for index in range(source.index("{", start), len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"unterminated block {header!r}")


def _code(block: str) -> str:
    return "\n".join(line.split("#", 1)[0] for line in block.splitlines())


def resource(kind: str, name: str) -> str:
    return _code(_block(TERRAFORM, f'resource "{kind}" "{name}"'))


def data(kind: str, name: str) -> str:
    return _code(_block(TERRAFORM, f'data "{kind}" "{name}"'))


class TestTheTerraform:
    def test_an_uninstall_waits_out_dms_deleting_its_endpoints(self):
        # The first full v1.1 uninstall failed on the provider's five-minute default: DMS held
        # the S3 target endpoint in `deleting` just after its task went, then finished alone.
        for kind, name in (
            ("aws_dms_endpoint", "round6_source"),
            ("aws_dms_s3_endpoint", "round6_target"),
        ):
            timeouts = _code(_block(resource(kind, name), "timeouts"))
            assert 'delete = "20m"' in timeouts, f"{kind}.{name} keeps the five-minute default"

    def test_nothing_of_the_lane_exists_until_its_credential_does(self):
        assert (
            "round6_aws_enabled = local.v7_enabled && var.round6_uc_external_id != null"
            in TERRAFORM
        )
        headers = re.findall(r'^(resource|data) "([a-z0-9_]+)" "([a-z0-9_]+)"', TERRAFORM, re.M)
        assert headers, "no resources found"
        for kind, typ, name in headers:
            body = _code(_block(TERRAFORM, f'{kind} "{typ}" "{name}"'))
            assert (
                "count = local.round6_aws_enabled" in body
                or "for_each = local.round6_aws_competitors" in body
            ), f"{typ}.{name} exists without the lane"

    def test_the_lanes_state_addresses_are_the_ones_the_lifecycle_expects(self):
        declared = set()
        for typ, name in re.findall(r'^resource "([a-z0-9_]+)" "([a-z0-9_]+)"', TERRAFORM, re.M):
            body = resource(typ, name)
            if "for_each = local.round6_aws_competitors" in body:
                declared |= {f'{typ}.{name}["{key}"]' for key in ("aurora", "rds")}
            elif "local.round6_aws_enabled ? 2 : 0" in body:
                declared |= {f"{typ}.{name}[0]", f"{typ}.{name}[1]"}
            else:
                declared.add(f"{typ}.{name}[0]")
        assert declared == set(lifecycle._ROUND6_AWS_STATE_ADDRESSES)

    def test_the_replication_instance_is_private_small_and_pinned(self):
        body = resource("aws_dms_replication_instance", "round6")
        assert "publicly_accessible         = false" in body
        assert 'replication_instance_class  = "dms.t3.small"' in body
        assert "multi_az                    = false" in body
        assert "auto_minor_version_upgrade  = false" in body
        # An engine AWS upgrades is not drift for a later plan to try to undo.
        assert 'ignore_changes = [tags["expires-at"], engine_version]' in body

    def test_the_tasks_are_change_capture_only_and_never_start_themselves(self):
        body = resource("aws_dms_replication_task", "round6")
        assert 'migration_type           = "cdc"' in body
        assert "start_replication_task   = false" in body
        assert '"schema-name" = local.round6_source_schema' in body
        assert '"table-name"  = local.round6_source_table' in body

    def test_the_source_endpoint_holds_no_real_password_and_needs_no_ddl_privilege(self):
        body = resource("aws_dms_endpoint", "round6_source")
        assert "password      = local.round6_endpoint_placeholder" in body
        # The same placeholder the installer knows it replaces.
        from server.round6_aws_lifecycle import ROUND6_PLACEHOLDER_PASSWORD

        assert f'round6_endpoint_placeholder = "{ROUND6_PLACEHOLDER_PASSWORD}"' in TERRAFORM
        assert 'ignore_changes = [password, tags["expires-at"]]' in body
        assert 'ssl_mode      = "require"' in body
        assert "capture_ddls = false" in body
        assert "username      = local.round6_capture_database_role" in body

    def test_the_target_writes_parquet_changes_within_a_second(self):
        body = resource("aws_dms_s3_endpoint", "round6_target")
        for fragment in (
            'data_format             = "parquet"',
            'timestamp_column_name   = "dms_commit_ts"',
            "cdc_max_batch_interval  = 1",
            "cdc_min_file_size       = 1",
            'bucket_folder           = "dms/${each.key}"',
        ):
            assert fragment in body

    def test_one_run_at_a_time_and_a_run_nothing_stops_stops_itself(self):
        body = resource("aws_glue_job", "round6_writer")
        assert "max_concurrent_runs = 1" in body
        assert "timeout = 30" in body
        assert 'glue_version      = "5.0"' in body
        # It reads and writes S3 only, so it needs no network connection at all.
        assert "connections" not in body

    def test_only_change_files_expire_never_the_tables_or_checkpoints(self):
        body = resource("aws_s3_bucket_lifecycle_configuration", "round6_aws")
        prefixes = re.findall(r'prefix = "([^"]*)"', body)
        assert prefixes == ["dms/"]
        assert "versioning" not in TERRAFORM

    def test_unity_catalog_may_assume_the_read_role_only_with_the_external_id(self):
        trust = data("aws_iam_policy_document", "round6_uc_assume")
        assert "identifiers = [var.round6_uc_master_role_arn]" in trust
        assert 'variable = "sts:ExternalId"' in trust
        assert "values   = [var.round6_uc_external_id]" in trust
        # The role may assume itself, pinned to its own ARN, without naming a principal that does
        # not exist yet when the role is created.
        assert 'variable = "aws:PrincipalArn"' in trust
        assert "values   = [local.round6_uc_role_arn]" in trust

    def test_the_read_role_can_only_read_the_lanes_tables(self):
        access = data("aws_iam_policy_document", "round6_uc_access")
        actions = set(re.findall(r'"(s3:[A-Za-z]+|sts:[A-Za-z]+)"', access))
        assert actions == {
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:ListBucket",
            "s3:GetBucketLocation",
            "sts:AssumeRole",
        }
        assert "/delta/*" in access

    def test_dms_writes_only_its_own_prefix(self):
        access = data("aws_iam_policy_document", "round6_dms_s3_access")
        assert '"${aws_s3_bucket.round6_aws[0].arn}/dms/*"' in access

    def test_the_account_wide_dms_role_is_read_never_managed(self):
        assert 'data "aws_iam_role" "dms_vpc"' in TERRAFORM
        assert not re.search(r'resource "aws_iam_role" "[a-z_]*" \{[^}]*"dms-vpc-role"', TERRAFORM)

    def test_the_dms_subnets_never_take_round_fours(self):
        assert "local.round4_glue_subnet_netnum - 64 + 1" in TERRAFORM
        # For every Round 4 choice, Round 6's is another /24, whatever the second digest is.
        for round4 in range(64, 255):
            for step in range(190):
                assert 64 + (round4 - 64 + 1 + step) % 191 != round4

    def test_the_r6_databases_admit_dms_only_once_the_lane_exists(self):
        network = (AWS / "network.tf").read_text(encoding="utf-8")
        rules = re.findall(
            r'for_each = each.key == "r6" && local.round6_aws_enabled \? \[true\]', network
        )
        assert len(rules) == 2
        assert network.count("security_groups = [aws_security_group.round6_dms[0].id]") == 2


class TestTheIam:
    def test_the_operator_document_fits_and_is_attached_to_the_runtime_role(self):
        document = (REPO / "docs" / "iam" / "anti-demo-operator-6-round6.json").read_text()
        assert len(json.dumps(json.loads(document), separators=(",", ":"))) <= 6144
        runtime = (AWS / "anti_demo_runtime.tf").read_text(encoding="utf-8")
        assert '"6-round6"    = "anti-demo-operator-6-round6.json"' in runtime
        assert "6-round6" in lifecycle._ANTI_DEMO_RUNTIME_POLICY_KEYS

    def test_the_operator_may_create_but_never_delete_the_account_wide_dms_role(self):
        document = json.loads(
            (REPO / "docs" / "iam" / "anti-demo-operator-6-round6.json").read_text()
        )
        for statement in document["Statement"]:
            resources = statement["Resource"]
            resources = [resources] if isinstance(resources, str) else resources
            actions = statement["Action"]
            actions = [actions] if isinstance(actions, str) else actions
            if any(resource.endswith("role/dms-vpc-role") for resource in resources):
                assert not any("Delete" in action or "Detach" in action for action in actions)

    def test_the_deployed_app_may_start_and_stop_only_round_sixs_tasks(self):
        document = json.loads((REPO / "docs" / "iam" / "anti-demo-app-runtime.json").read_text())
        statement = next(
            item
            for item in document["Statement"]
            if item.get("Sid") == "StartAndParkTheRoundSixDmsTasksOnly"
        )
        assert statement["Condition"]["StringEquals"]["aws:ResourceTag/anti-demo-round"] == "r6"
        glue = next(
            item
            for item in document["Statement"]
            if item.get("Sid") == "StartAndParkTheRoundFourAndSixGlueWritersOnly"
        )
        assert any("-r6-writer-" in resource for resource in glue["Resource"])


def _load_writer():
    spec = importlib.util.spec_from_file_location(
        "round6_writer", REPO / "glue" / "round6_writer.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


writer = _load_writer()


class TestTheGlueScript:
    def test_the_declared_schema_is_dms_two_columns_then_the_source_tables(self):
        from server.round6_aws_lifecycle import SOURCE_COLUMNS

        names = [name for name, _ in writer.SOURCE_COLUMNS]
        assert names[:2] == ["Op", "dms_commit_ts"]
        assert names[2:] == [name for name, _, _ in SOURCE_COLUMNS]
        spark_types = {"text": "string", "integer": "int"}
        assert [kind for _, kind in writer.SOURCE_COLUMNS[2:]] == [
            spark_types[kind] for _, kind, _ in SOURCE_COLUMNS
        ]

    def test_the_schema_is_spark_ddl(self):
        assert writer.schema_ddl((("Op", "string"), ("quantity", "int"))) == (
            "`Op` string, `quantity` int"
        )

    @pytest.mark.parametrize("value", ["0", "-1", "x"])
    def test_a_trigger_that_would_spin_or_stall_is_refused(self, value):
        with pytest.raises(ValueError):
            writer.trigger_interval(value)

    def test_a_trigger_of_one_second(self):
        assert writer.trigger_interval("1") == "1 seconds"

    @pytest.mark.parametrize("value", ["delta/x", "s3://", "s3://bucket", "gs://b/x"])
    def test_a_location_outside_a_bucket_prefix_is_refused(self, value):
        with pytest.raises(ValueError):
            writer.s3_location(value)

    def test_the_job_arguments_are_the_ones_the_script_reads(self):
        body = resource("aws_glue_job", "round6_writer")
        for argument in writer.ARGUMENTS:
            assert f'"--{argument}"' in body


def manifest_with(**fields) -> SimpleNamespace:
    environment = SimpleNamespace(aurora=object(), rds=object())
    base = {
        "round6_aws": None,
        "round6_aws_unsupported": None,
        "installation_id": "11111111-1111-4111-8111-111111111111",
        "round6": object(),
        "round_environments": {RoundId.ANALYZE_LIVE_ORDERS: environment},
    }
    base.update(fields)
    return SimpleNamespace(**base)


class TestPending:
    def pending(self, **fields) -> bool:
        return DemoManifest.round6_aws_pending.fget(manifest_with(**fields))

    def test_an_install_with_r6_databases_and_no_lane_is_interrupted(self):
        assert self.pending()

    def test_a_sealed_lane_is_finished(self):
        assert not self.pending(round6_aws=object())

    def test_an_identity_that_may_not_build_the_lane_is_finished_too(self):
        assert not self.pending(round6_aws_unsupported="may not create a storage credential")

    def test_a_v1_0_install_without_r6_databases_is_not_pending(self):
        empty = SimpleNamespace(aurora=None, rds=None)
        assert not self.pending(round_environments={RoundId.ANALYZE_LIVE_ORDERS: empty})

    def test_bootstrap_reads_the_same_condition(self):
        source = (REPO / "bootstrap.sh").read_text(encoding="utf-8")
        assert "analyze_live_orders_without_slowing_checkout // {}) as $r6" in source
        assert "(.round6_aws != null) or (.round6_aws_unsupported != null)" in source


def plan(*changes) -> dict:
    return {
        "resource_changes": [
            {"address": address, "type": address.split(".")[0], "change": change}
            for address, change in changes
        ]
    }


class TestThePlanGuard:
    @pytest.fixture
    def manifest(self, monkeypatch):
        candidate = SimpleNamespace()
        monkeypatch.setattr(
            lifecycle,
            "_expected_aws_state_addresses",
            lambda _manifest: {
                *lifecycle._ROUND6_AWS_STATE_ADDRESSES,
                'aws_security_group.aurora_by_round["r6"]',
                'aws_db_instance.rds_by_round["r5"]',
            },
        )
        return candidate

    def test_the_lanes_own_resources_may_be_created(self, manifest):
        changes = [
            (address, {"actions": ["create"]}) for address in lifecycle._ROUND6_AWS_STATE_ADDRESSES
        ]
        assert lifecycle._round6_aws_plan_violations(manifest, plan(*changes)) == []

    def test_a_database_group_may_gain_its_dms_rule(self, manifest):
        change = {"actions": ["update"], "before": {"ingress": []}, "after": {"ingress": ["dms"]}}
        assert (
            lifecycle._round6_aws_plan_violations(
                manifest, plan(('aws_security_group.aurora_by_round["r6"]', change))
            )
            == []
        )

    def test_any_other_rounds_drift_is_refused(self, manifest):
        change = {
            "actions": ["update"],
            "before": {"instance_class": "a"},
            "after": {"instance_class": "b"},
        }
        violations = lifecycle._round6_aws_plan_violations(
            manifest, plan(('aws_db_instance.rds_by_round["r5"]', change))
        )
        assert violations == ['aws_db_instance.rds_by_round["r5"]: changes instance_class']


FAKE_RUN_ID = "ad-20" + "990101-0000-" + "test"


def cleanup_manifest() -> SimpleNamespace:
    return SimpleNamespace(
        installation_id="11111111-1111-4111-8111-111111111111",
        run_id=FAKE_RUN_ID,
        databricks=SimpleNamespace(profile="p"),
        aws=SimpleNamespace(region="us-west-2", account_id="123456789012"),
    )


class TestCleanup:
    @pytest.fixture
    def catalog(self, monkeypatch):
        manifest = cleanup_manifest()
        role = lifecycle._round6_uc_role_arn(manifest)
        objects: dict[str, dict | None] = {
            "external-locations": {"credential_name": "anti_demo_r6_aws_ad_20990101_0000_test"},
            "storage-credentials": {"aws_iam_role": {"role_arn": role}},
        }
        deleted: list[str] = []
        monkeypatch.setattr(
            lifecycle,
            "_databricks_api_optional",
            lambda profile, path: objects[path.split("/")[-2]],
        )
        monkeypatch.setattr(
            lifecycle,
            "_databricks_api_delete_no_response",
            lambda profile, path: deleted.append(path),
        )
        return manifest, objects, deleted

    def test_the_location_goes_before_the_credential_it_uses(self, catalog):
        manifest, _, deleted = catalog
        lifecycle._delete_round6_aws_catalog(manifest)
        assert deleted == [
            "/api/2.1/unity-catalog/external-locations/anti_demo_r6_aws_ad_20990101_0000_test"
            "?force=true",
            "/api/2.1/unity-catalog/storage-credentials/anti_demo_r6_aws_ad_20990101_0000_test"
            "?force=true",
        ]

    def test_objects_already_gone_are_skipped(self, catalog):
        manifest, objects, deleted = catalog
        objects["external-locations"] = None
        objects["storage-credentials"] = None
        lifecycle._delete_round6_aws_catalog(manifest)
        assert deleted == []

    def test_a_credential_for_another_role_is_refused_not_deleted(self, catalog):
        manifest, objects, deleted = catalog
        objects["storage-credentials"] = {
            "aws_iam_role": {"role_arn": "arn:aws:iam::123456789012:role/someone-else"}
        }
        with pytest.raises(RuntimeError, match="names an IAM role other"):
            lifecycle._delete_round6_aws_catalog(manifest)
        assert all("storage-credentials" not in path for path in deleted)

    def test_a_location_on_another_credential_is_refused(self, catalog):
        manifest, objects, deleted = catalog
        objects["external-locations"] = {"credential_name": "someone_elses"}
        with pytest.raises(RuntimeError, match="does not use this installation"):
            lifecycle._delete_round6_aws_catalog(manifest)
        assert deleted == []

    def test_a_v1_0_installation_has_no_lane_objects_to_look_up(self, monkeypatch):
        monkeypatch.setattr(
            lifecycle,
            "_databricks_api_optional",
            lambda profile, path: pytest.fail("looked up a lane object that cannot exist"),
        )
        legacy = cleanup_manifest()
        legacy.installation_id = None
        assert lifecycle._round6_aws_catalog_objects(legacy) == []

    def test_the_role_the_credential_names_is_the_one_terraform_makes(self):
        manifest = cleanup_manifest()
        slug = lifecycle._round_installation_slug(manifest, "r6")
        assert lifecycle._round6_uc_role_arn(manifest) == (
            f"arn:aws:iam::123456789012:role/{slug}-uc"
        )
        assert 'round6_uc_role_name = local.v7_enabled ? "${local.v7_round_slugs["r6"]}-uc"' in (
            TERRAFORM
        )

    def test_the_lane_is_parked_before_the_destroy_and_its_catalog_deleted_before_it(self):
        import inspect

        source = inspect.getsource(lifecycle.cleanup)
        park = source.index("_park_round6_aws_lane(manifest, managed_addresses)")
        catalog = source.index("_delete_round6_aws_catalog(manifest)")
        destroy = source.index("_destroy_after_releasing_interfaces(manifest, destroy_plan)")
        assert park < destroy and catalog < destroy

    def test_nothing_about_the_lane_is_parked_when_terraform_never_built_it(self, monkeypatch):
        monkeypatch.setattr(
            lifecycle,
            "_terraform_state_resource_values",
            lambda *args: pytest.fail("read the state for a lane that is not in it"),
        )
        lifecycle._park_round6_aws_lane(
            cleanup_manifest(), {'aws_rds_cluster.aurora_by_round["r6"]'}
        )
