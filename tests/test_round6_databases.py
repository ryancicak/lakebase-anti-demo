"""Round 6's own Aurora cluster and RDS instance, with logical replication on.

v1.1 races Round 6's AWS lane as AWS DMS change capture out of the round's own
databases (docs/design/v1.1-rounds-4-6-aws.md, section 4). DMS reads PostgreSQL
through a logical replication slot, so these databases run on parameter groups
that turn logical replication on, and every other round keeps the engine
defaults. These tests hold the Terraform, the lifecycle's view of the state it
makes, and the seal it writes to one another.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from server import lifecycle
from server.capacity import LAKEBASE_ONLY_ROUNDS, RDS_PROVISIONED_ROUNDS
from server.cost_model import LOGICAL_REPLICATION_ROUNDS, PROVISIONED_NOT_YET_RACED_ROUNDS
from server.models import RoundId

REPO = Path(__file__).resolve().parent.parent
AWS = REPO / "infra" / "aws"


def _source(name: str) -> str:
    return (AWS / name).read_text(encoding="utf-8")


def _keys(name: str) -> tuple[str, ...]:
    match = re.search(rf"{name}\s*=\s*toset\(\[([^\]]*)\]\)", _source("locals.tf"))
    assert match is not None, f"{name} is no longer declared as a toset literal"
    return tuple(re.findall(r'"([^"]+)"', match.group(1)))


def _block(source: str, header: str) -> str:
    """The body of the one block that opens with ``header``, braces matched."""

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
    """The block without its comments, so a remark cannot satisfy an assertion."""

    return "\n".join(line.split("#", 1)[0] for line in block.splitlines())


@pytest.fixture(scope="module")
def groups() -> dict[str, str]:
    source = _source("parameter_groups.tf")
    return {
        "aurora": _code(
            _block(source, 'resource "aws_rds_cluster_parameter_group" "lakeflow_aurora"')
        ),
        "rds": _code(_block(source, 'resource "aws_db_parameter_group" "lakeflow_rds"')),
    }


class TestTheDatabasesStand:
    def test_round_six_stands_an_aurora_cluster_and_an_rds_instance(self):
        assert "r6" in _keys("v7_round_keys")
        assert "r6" in _keys("v7_rds_round_keys")

    def test_the_lifecycle_expects_the_same_rounds_terraform_stands_up(self):
        # `_aws_state_is_complete` demands exact equality with Terraform's state,
        # so a round Terraform stands up but the lifecycle forgets is a refusal.
        assert lifecycle._V7_ROUND_KEYS == _keys("v7_round_keys")
        assert lifecycle._V7_RDS_ROUND_KEYS == _keys("v7_rds_round_keys")
        assert lifecycle._V7_LOGICAL_REPLICATION_ROUND_KEYS == _keys("v7_lakeflow_round_keys")

    def test_only_round_six_replicates(self):
        # Aurora with logical replication never pauses, so no round that races a
        # cold database may join this set.
        assert _keys("v7_lakeflow_round_keys") == ("r6",)
        assert LOGICAL_REPLICATION_ROUNDS == frozenset({RoundId.ANALYZE_LIVE_ORDERS})

    def test_the_parameter_groups_are_in_the_state_the_lifecycle_expects(self):
        addresses = lifecycle._V7_AWS_STATE_ADDRESSES
        assert 'aws_rds_cluster_parameter_group.lakeflow_aurora["r6"]' in addresses
        assert 'aws_db_parameter_group.lakeflow_rds["r6"]' in addresses
        for key in ("r1", "r2", "r3", "r4", "r5"):
            assert f'aws_rds_cluster_parameter_group.lakeflow_aurora["{key}"]' not in addresses
            assert f'aws_db_parameter_group.lakeflow_rds["{key}"]' not in addresses

    def test_round_six_databases_are_in_the_state_the_lifecycle_expects(self):
        addresses = lifecycle._V7_AWS_STATE_ADDRESSES
        for address in (
            'aws_db_subnet_group.by_round["r6"]',
            'aws_rds_cluster.aurora_by_round["r6"]',
            'aws_rds_cluster_instance.aurora_writer_by_round["r6"]',
            'aws_security_group.aurora_by_round["r6"]',
            'aws_db_instance.rds_by_round["r6"]',
            'aws_security_group.rds_by_round["r6"]',
        ):
            assert address in addresses

    def test_the_rds_instance_is_validated_and_raced_where_its_lane_is_sealed(self):
        # Whatever stands is checked for ownership, ingress and capacity.
        assert RoundId.ANALYZE_LIVE_ORDERS in RDS_PROVISIONED_ROUNDS
        # Its lane races it only where the installation sealed that lane, as Round 4's
        # does, so it is optional rather than not yet raced.
        assert RoundId.ANALYZE_LIVE_ORDERS in LAKEBASE_ONLY_ROUNDS
        assert RoundId.ANALYZE_LIVE_ORDERS not in PROVISIONED_NOT_YET_RACED_ROUNDS


class TestTheSeal:
    @staticmethod
    def _outputs() -> dict[str, dict[str, str]]:
        aurora = lifecycle._V7_ROUND_KEYS
        rds = lifecycle._V7_RDS_ROUND_KEYS

        def keyed(keys: tuple[str, ...], stem: str) -> dict[str, str]:
            return {key: f"{stem}-{key}" for key in keys}

        def groups(keys: tuple[str, ...], digit: str) -> dict[str, str]:
            return {key: f"sg-{digit * 7}{key.removeprefix('r')}" for key in keys}

        def secrets(keys: tuple[str, ...], kind: str) -> dict[str, str]:
            return {
                key: f"arn:aws:secretsmanager:us-west-2:123456789012:secret:rds!{kind}-{key}"
                for key in keys
            }

        return {
            "db_subnet_group_names": keyed(aurora, "subnet"),
            "aurora_security_group_ids": groups(aurora, "a"),
            "aurora_cluster_ids": keyed(aurora, "cluster"),
            "aurora_cluster_resource_ids": keyed(aurora, "cluster-resource"),
            "aurora_writer_instance_ids": keyed(aurora, "writer"),
            "aurora_writer_endpoints": keyed(aurora, "writer.example"),
            "aurora_secret_arns": secrets(aurora, "cluster"),
            "rds_security_group_ids": groups(rds, "b"),
            "rds_instance_ids": keyed(rds, "instance"),
            "rds_addresses": keyed(rds, "instance.example"),
            "rds_resource_ids": keyed(rds, "db-resource"),
            "rds_secret_arns": secrets(rds, "db"),
        }

    def test_the_installer_seals_round_six_databases_into_its_round(self):
        sealed = lifecycle._v7_aws_environment_seals(self._outputs())
        aurora, rds = sealed[RoundId.ANALYZE_LIVE_ORDERS]
        assert aurora.cluster_id == "cluster-r6"
        assert aurora.direct_host == "writer.example-r6"
        assert rds is not None
        assert rds.instance_id == "instance-r6"
        assert rds.db_subnet_group_name == aurora.db_subnet_group_name == "subnet-r6"

    def test_every_round_terraform_stands_up_is_sealed(self):
        sealed = lifecycle._v7_aws_environment_seals(self._outputs())
        assert len(sealed) == len(lifecycle._V7_ROUND_KEYS) == 6

    def test_outputs_without_round_six_are_refused_rather_than_sealed_short(self):
        # An apply that stood up five rounds' databases is an incomplete apply.
        outputs = self._outputs()
        del outputs["aurora_cluster_ids"]["r6"]
        with pytest.raises(RuntimeError, match="unexpected round keys"):
            lifecycle._v7_aws_environment_seals(outputs)


class TestLogicalReplication:
    def test_both_groups_are_for_the_replicating_rounds_only(self, groups):
        for body in groups.values():
            assert "for_each = local.v7_lakeflow_rounds" in body

    def test_both_groups_keep_the_names_the_branch_created(self, groups):
        # An installation made from fix/contest-ledger-set-integrity already has
        # these groups; a new name would replace them under a live database.
        assert '"${local.v7_round_resource_names[each.key]}-aurora-lakeflow"' in groups["aurora"]
        assert '"${local.v7_round_resource_names[each.key]}-rds-lakeflow"' in groups["rds"]
        assert 'family      = "aurora-postgresql17"' in groups["aurora"]
        assert 'family      = "postgres17"' in groups["rds"]

    @pytest.mark.parametrize("engine", ["aurora", "rds"])
    def test_logical_replication_is_on_and_set_as_aws_accepts_it(self, groups, engine):
        # A static parameter: AWS rejects "immediate" for it at apply time.
        body = groups[engine]
        parameter = _block(body, "parameter {\n")
        assert 'name         = "rds.logical_replication"' in parameter
        assert 'value        = "1"' in parameter
        assert 'apply_method = "pending-reboot"' in parameter

    @pytest.mark.parametrize("engine", ["aurora", "rds"])
    def test_a_parked_slot_cannot_hold_wal_without_bound(self, groups, engine):
        body = groups[engine]
        assert 'name         = "max_slot_wal_keep_size"' in body
        match = re.search(
            r'name\s+=\s+"max_slot_wal_keep_size"\s+value\s+=\s+"(\d+)"', body, re.MULTILINE
        )
        assert match is not None
        assert 0 < int(match.group(1)) <= 1024

    @pytest.mark.parametrize("engine", ["aurora", "rds"])
    def test_a_lease_move_or_the_branch_description_is_not_a_replacement(self, groups, engine):
        # The description forces a replacement, which the fixed name and
        # create_before_destroy would turn into a collision on an upgrade.
        body = groups[engine]
        assert "create_before_destroy = true" in body
        assert 'ignore_changes        = [tags["expires-at"], description]' in body

    def test_the_groups_are_attached_at_creation_and_only_to_round_six(self):
        aurora = _code(_block(_source("aurora.tf"), 'resource "aws_rds_cluster" "aurora_by_round"'))
        rds = _code(_block(_source("rds.tf"), 'resource "aws_db_instance" "rds_by_round"'))
        assert (
            "db_cluster_parameter_group_name = lookup("
            "local.v7_aurora_cluster_parameter_group_names, each.key, null)"
        ) in aurora
        assert (
            "parameter_group_name = lookup("
            'local.v7_rds_parameter_group_names, each.key, "default.postgres17")'
        ) in rds

    def test_the_operator_may_manage_only_the_installations_own_groups(self):
        policy = json.loads(
            (REPO / "docs" / "iam" / "anti-demo-operator-2-databases.json").read_text()
        )
        statements = {statement["Sid"]: statement for statement in policy["Statement"]}
        prefix = "arn:aws:rds:<AWS_REGION>:<AWS_ACCOUNT_ID>"
        managed = statements["ManageDemoOwnedParameterGroups"]
        assert set(managed["Resource"]) == {
            f"{prefix}:pg:lakebase-ant*",
            f"{prefix}:cluster-pg:lakebase-ant*",
        }
        verbs = {"Create", "Delete", "Modify"}
        for verb in verbs:
            assert f"rds:{verb}DBParameterGroup" in managed["Action"]
            assert f"rds:{verb}DBClusterParameterGroup" in managed["Action"]
        # No statement may delete or modify a group the installation did not name,
        # which `cluster-pg:*` in the database statement would otherwise allow.
        for statement in policy["Statement"]:
            actions = statement["Action"]
            resources = statement["Resource"]
            resources = [resources] if isinstance(resources, str) else resources
            if any(
                f"rds:{verb}DB{kind}ParameterGroup" in actions
                for verb in verbs
                for kind in ("", "Cluster")
            ):
                assert all("lakebase-ant" in resource for resource in resources)
        # An instance may be created on the group, and Terraform can read both kinds.
        assert f"{prefix}:pg:lakebase-ant*" in statements["ManageDemoOwnedDatabases"]["Resource"]
        reads = statements["ReadRdsCatalogAndState"]["Action"]
        for action in (
            "rds:DescribeDBParameterGroups",
            "rds:DescribeDBParameters",
            "rds:DescribeDBClusterParameterGroups",
            "rds:DescribeDBClusterParameters",
        ):
            assert action in reads

    def test_the_preflight_proves_both_creates_before_the_apply(self):
        # RDS has no dry run, so a missing grant would otherwise surface mid-apply.
        source = (REPO / "bootstrap.sh").read_text(encoding="utf-8")
        assert "rds:CreateDBParameterGroup|arn:$SIM_PARTITION:rds:" in source
        assert "rds:CreateDBClusterParameterGroup|arn:$SIM_PARTITION:rds:" in source
        assert ":pg:lakebase-ant-preflight-r6-rds-lakeflow" in source
        assert ":cluster-pg:lakebase-ant-preflight-r6-aurora-lakeflow" in source

    def test_every_other_round_keeps_its_cluster_able_to_pause(self):
        # The lookup falls back to the engine default, and the scaling floor that
        # lets Rounds 1-5 park is unchanged.
        aurora = _code(_block(_source("aurora.tf"), 'resource "aws_rds_cluster" "aurora_by_round"'))
        assert "min_capacity             = 0" in aurora
        assert "seconds_until_auto_pause = 300" in aurora
