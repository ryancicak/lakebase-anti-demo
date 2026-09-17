from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from runner import connection_spike_runner as runner
from server import lifecycle

ROOT = Path(__file__).resolve().parents[1]


def _terraform(name: str) -> str:
    return (ROOT / "infra" / "aws" / name).read_text(encoding="utf-8")


def _block(source: str, start: str, end: str) -> str:
    return source[source.index(start) : source.index(end, source.index(start))]


def test_control_role_cannot_tag_databases_or_reopen_static_runner_egress() -> None:
    control = _terraform("round5_control.tf")
    # The RDS Proxy tag-management statements and their Deny guard were split out
    # of the inline execution policy into the attached `round5_execution_proxy`
    # managed policy to respect IAM's 10,240-character inline-policy aggregate
    # limit. The guarantees below are unchanged; only their storage moved.
    tag_block = _block(
        control,
        "# CreateDBProxy evaluates AddTagsToResource as a dependent action",
        'resource "aws_iam_policy" "round5_execution_proxy"',
    )

    assert '"rds:AddTagsToResource"' in tag_block
    assert ":db-proxy:*" in tag_block
    assert ":target-group:*" in tag_block
    assert ":db:*" not in tag_block
    assert ":cluster:*" not in tag_block
    assert 'variable = "aws:TagKeys"' in tag_block
    assert 'effect  = "Deny"' in tag_block
    assert 'variable = "aws:ResourceTag/${statement.value}"' in tag_block
    assert 'values   = ["$${aws:ResourceTag/${statement.value}}"]' in tag_block
    assert 'resources = ["*"]' not in tag_block

    static_egress_grant = re.compile(
        r'actions\s*=\s*\["ec2:(?:Authorize|Revoke)SecurityGroupEgress"\]'
        r".*?aws_security_group\.round5_(?:competitor_)?runner\.arn",
        re.DOTALL,
    )
    assert static_egress_grant.search(control) is None


def test_terraform_isolates_runner_roles_secrets_and_network_identities() -> None:
    runner_hcl = _terraform("round5_runner.tf")
    network = _terraform("network.tf")

    lakebase_policy = _block(
        runner_hcl,
        'data "aws_iam_policy_document" "round5_lakebase_runner_static_secrets"',
        'data "aws_iam_policy_document" "round5_competitor_runner_static_secrets"',
    )
    assert "round5_runner_control.arn" in lakebase_policy
    assert "round5_competitor_runner_control" not in lakebase_policy
    assert "round5_aurora_proxy_credentials" not in lakebase_policy
    assert "round5_rds_proxy_credentials" not in lakebase_policy
    assert "master_user_secret" not in lakebase_policy

    assert (
        "permissions_boundary = aws_iam_policy.round5_competitor_runner_boundary.arn"
    ) in runner_hcl
    assert (
        "vpc_security_group_ids      = [aws_security_group.round5_competitor_runner.id]"
    ) in runner_hcl
    assert ('resource "aws_secretsmanager_secret" "round5_competitor_runner_control"') in runner_hcl

    assert 'resource "aws_security_group" "round5_competitor_runner"' in network
    assert 'ip_protocol       = "-1"' not in _block(
        network,
        'resource "aws_security_group" "round5_runner"',
        "# Stable least-privilege Proxy network fixtures.",
    )
    assert (
        "referenced_security_group_id = aws_security_group.round5_competitor_runner.id"
    ) in network
    competitor_postgres = _block(
        network,
        'resource "aws_vpc_security_group_egress_rule" "round5_competitor_runner_to_proxy"',
        'resource "aws_vpc_security_group_egress_rule" "round5_proxy_to_database"',
    )
    assert 'cidr_ipv4         = "0.0.0.0/0"' not in competitor_postgres
    assert competitor_postgres.count("referenced_security_group_id") == 2


def test_resident_database_capability_is_lane_and_generation_scoped() -> None:
    source = inspect.getsource(lifecycle._rotate_round5_resident_login)

    assert "lane_id not in" in source
    assert "NOBYPASSRLS" in source
    assert "GRANT SELECT, INSERT ON" in source
    assert "GRANT SELECT, INSERT, UPDATE" not in source
    assert "ENABLE ROW LEVEL SECURITY" in source
    assert "round5_runner_event_authorized_v1" in source
    assert "ROUND5_WARM_SLOT_TABLE" in source
    assert "ROUND5_CONTROL_OUTBOX_TABLE" in source
    assert "(control.payload -> 'binding') - 'runner_process_boot_id'::text" in source

    write_source = inspect.getsource(runner._write_resident_event)
    assert "ON CONFLICT" in write_source
    assert "DO NOTHING" in write_source
    assert "DO UPDATE" not in write_source


def test_legacy_shared_resident_login_is_disabled_during_migration() -> None:
    source = inspect.getsource(lifecycle._retire_round5_shared_resident_login)

    assert "ALTER ROLE {} NOLOGIN" in source
    assert "REVOKE ALL ON {} FROM {}" in source
    assert "REVOKE CONNECT ON DATABASE" in source


@pytest.mark.asyncio
async def test_legacy_shared_login_retirement_executes_fail_closed_acl_changes() -> None:
    statements: list[str] = []

    class Cursor:
        async def execute(self, statement, parameters=None):
            del parameters
            statements.append(str(statement))

        async def fetchone(self):
            return (1,)

    await lifecycle._retire_round5_shared_resident_login(
        Cursor(),
        database="anti_demo",
        role="anti_demo_r5_legacy",
    )

    rendered = " ".join(statements)
    assert "ALTER ROLE" in rendered
    assert "NOLOGIN" in rendered
    assert "REVOKE ALL ON" in rendered
    assert "REVOKE USAGE ON SCHEMA" in rendered
    assert "REVOKE CONNECT ON DATABASE" in rendered
