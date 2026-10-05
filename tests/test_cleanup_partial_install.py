"""A partial install that failed before its Round 5 seal can be uninstalled, and says why it failed.

2026-10-02. rc12's install stopped on "ERROR ╵". Its Terraform working directory had been
deleted mid-install, `terraform output` said "Error: Backend initialization required", and
only the last line of Terraform's diagnostic box, its bottom edge, reached the operator.

The cleanup the failure message recommends then refused: with Round 5 unsealed, the residue
inventory read the tags of every IAM role in the account as the installation's runtime role,
which may read only this installation's, and stopped on ``AccessDenied@ListRoleTags``. The
partial install was left billing.
"""

from __future__ import annotations

import hashlib
import subprocess
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from test_lifecycle import make_manifest

from server.lifecycle import (
    _failure_summary,
    _round5_runtime_tag_inventory,
    _safe_failure,
    _utc_tag,
)

# What `terraform output -json` wrote to stderr on rc12, colour codes and all.
TERRAFORM_BACKEND_ERROR = (
    "\x1b[31m╷\x1b[0m\x1b[0m\n"
    "\x1b[31m│\x1b[0m \x1b[0m\x1b[1m\x1b[31mError: \x1b[0m\x1b[0m\x1b[1mBackend initialization "
    'required, please run "terraform init"\x1b[0m\n'
    "\x1b[31m│\x1b[0m \x1b[0m\n"
    "\x1b[31m│\x1b[0m \x1b[0m\x1b[0mReason: Initial configuration of the requested backend "
    '"local"\n'
    "\x1b[31m│\x1b[0m \x1b[0m\n"
    "\x1b[31m╵\x1b[0m\x1b[0m\n"
)


def test_a_terraform_failure_reports_its_error_line_not_the_boxs_edge() -> None:
    assert _failure_summary(TERRAFORM_BACKEND_ERROR) == (
        'Error: Backend initialization required, please run "terraform init"'
    )


def test_the_raised_error_carries_it() -> None:
    result = subprocess.CompletedProcess(
        ["terraform"], 1, stdout="", stderr=TERRAFORM_BACKEND_ERROR
    )
    assert str(_safe_failure(result)).startswith("Error: Backend initialization required")


@pytest.mark.parametrize(
    ("output", "summary"),
    [
        # The Databricks CLI's single-line errors, which identity classification reads.
        (
            "Error: default auth: cannot configure default credentials\n",
            "Error: default auth: cannot configure default credentials",
        ),
        # Anything without a diagnostic keeps its last line, as before.
        (
            "starting\nAn error occurred (Throttling): Rate exceeded\n",
            "An error occurred (Throttling): Rate exceeded",
        ),
        ("\x1b[31m╵\x1b[0m\n", "command failed"),
        ("", "command failed"),
    ],
)
def test_other_output_keeps_its_old_summary(output: str, summary: str) -> None:
    assert _failure_summary(output) == summary


def test_the_summary_is_still_redacted() -> None:
    result = subprocess.CompletedProcess(
        ["databricks"], 1, stdout="", stderr="Error: login failed client_secret=abc123def\n"
    )
    message = str(_safe_failure(result))
    assert "abc123def" not in message
    assert "[redacted]" in message


def _refused(operation: str) -> ClientError:
    return ClientError({"Error": {"Code": "AccessDenied", "Message": "not authorized"}}, operation)


class _NoSecrets:
    def list_secrets(self, **kwargs):
        return {"SecretList": []}


class _NoGroups:
    def describe_security_groups(self, **kwargs):
        return {"SecurityGroups": []}

    def describe_security_group_rules(self, **kwargs):
        return {"SecurityGroupRules": []}


class _ForeignProxy:
    def describe_db_proxies(self, **kwargs):
        return {
            "DBProxies": [
                {
                    "DBProxyArn": "arn:aws:rds:us-west-2:123456789012:db-proxy:prx-other",
                    "DBProxyName": "other",
                }
            ]
        }

    def list_tags_for_resource(self, **kwargs):
        raise _refused("ListTagsForResource")


class _Roles:
    def __init__(self, roles: list[dict], tags: dict[str, list[dict]]) -> None:
        self.roles = roles
        self.tags = tags

    def list_roles(self, **kwargs):
        return {"Roles": self.roles, "IsTruncated": False}

    def list_role_tags(self, *, RoleName: str):
        if RoleName not in self.tags:
            raise _refused("ListRoleTags")
        return {"Tags": self.tags[RoleName]}


def _unsealed(monkeypatch, iam: _Roles) -> SimpleNamespace:
    """An installation whose install failed before Round 5 was sealed."""

    manifest = make_manifest()
    clients = {
        "secretsmanager": _NoSecrets(),
        "ec2": _NoGroups(),
        "rds": _ForeignProxy(),
        "iam": iam,
    }
    monkeypatch.setattr(
        "server.lifecycle._aws_session",
        lambda _: SimpleNamespace(client=lambda name: clients[name]),
    )
    return SimpleNamespace(
        round5=None,
        round5_ready=False,
        run_id=manifest.run_id,
        owner=manifest.owner,
        expires_at=manifest.expires_at,
        installation_id=manifest.installation_id,
        aws=SimpleNamespace(
            resources=SimpleNamespace(rds_security_group_id="sg-1123456789abcdef0")
        ),
    )


def test_foreign_roles_and_proxies_the_runtime_role_may_not_read_are_not_this_installations(
    monkeypatch,
) -> None:
    iam = _Roles(
        roles=[
            {
                "Arn": "arn:aws:iam::123456789012:role/another-teams-role",
                "RoleName": "another-teams-role",
            },
            {
                "Arn": "arn:aws:iam::123456789012:role/OrganizationAccountAccessRole",
                "RoleName": "OrganizationAccountAccessRole",
            },
        ],
        tags={},
    )
    assert _round5_runtime_tag_inventory(_unsealed(monkeypatch, iam)) == []


def test_a_readable_role_tagged_for_this_run_is_still_found(monkeypatch) -> None:
    candidate = _unsealed(monkeypatch, _Roles([], {}))
    bout = "bout-1"
    tags = [
        {"Key": key, "Value": value}
        for key, value in {
            "anti-demo-run-id": candidate.run_id,
            "owner": candidate.owner,
            "expires-at": _utc_tag(candidate.expires_at),
            "managed-by": "round5-lifecycle",
            "anti-demo-bout-id": bout,
            "anti-demo:bout-token": hashlib.sha256(bout.encode()).hexdigest()[:16],
        }.items()
    ]
    arn = "arn:aws:iam::123456789012:role/legacy-bout-role"
    iam = _Roles(
        roles=[
            {"Arn": arn, "RoleName": "legacy-bout-role"},
            {
                "Arn": "arn:aws:iam::123456789012:role/another-teams-role",
                "RoleName": "another-teams-role",
            },
        ],
        tags={"legacy-bout-role": tags},
    )
    candidate = _unsealed(monkeypatch, iam)
    assert _round5_runtime_tag_inventory(candidate) == [arn]


def test_a_role_this_installation_names_as_its_own_still_has_to_answer(monkeypatch) -> None:
    # Sealed: the control role is this installation's by its ARN, so a refused tag read
    # cannot be skipped -- its ownership tags are what make the teardown safe.
    manifest = make_manifest().model_copy(
        update={"installation_id": "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"}
    )
    prefix = "ib3beef6697cc1d6dce31-r5-"
    control_role_arn = f"arn:aws:iam::123456789012:role/{prefix}exec-static"
    iam = _Roles(roles=[{"Arn": control_role_arn, "RoleName": f"{prefix}exec-static"}], tags={})
    clients = {
        "secretsmanager": _NoSecrets(),
        "ec2": _NoGroups(),
        "rds": _ForeignProxy(),
        "iam": iam,
    }
    monkeypatch.setattr(
        "server.lifecycle._aws_session",
        lambda _: SimpleNamespace(client=lambda name: clients[name]),
    )
    sealed = SimpleNamespace(
        ownership_tags=SimpleNamespace(as_aws_tags=lambda: {}),
        bout_name_prefix=prefix,
        secret_name_prefix=None,
        vpc_id="vpc-0123456789abcdef0",
        runner_security_group_id="sg-0123456789abcdef0",
        control_role_arn=control_role_arn,
        runner_role_arn="arn:aws:iam::123456789012:role/static-runner",
        proxy_service_role_arn="arn:aws:iam::123456789012:role/static-proxy",
    )
    candidate = SimpleNamespace(
        round5_ready=True,
        require_round5_resources=lambda: sealed,
        run_id=manifest.run_id,
        owner=manifest.owner,
        expires_at=manifest.expires_at,
        installation_id=manifest.installation_id,
        aws=SimpleNamespace(
            resources=SimpleNamespace(rds_security_group_id="sg-1123456789abcdef0")
        ),
    )
    with pytest.raises(ClientError, match="ListRoleTags"):
        _round5_runtime_tag_inventory(candidate)
