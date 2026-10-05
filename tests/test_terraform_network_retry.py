"""Terraform asks again when this host loses the network, and says why it failed when it can't.

2026-10-04. rc16's fresh install stopped in Round 6's lane plan on a laptop DNS outage of a
few minutes: Terraform could not look up iam.amazonaws.com. The install printed only
"ERROR command failed", because Terraform's output went straight to the terminal and the
installer kept none of it, and nothing asked again once the network was back.
"""

from __future__ import annotations

import ast
import inspect
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from server import lifecycle

# rc16's two diagnostics, colour codes and all, with the account and policy names replaced.
RC16_LOST_NETWORK = (
    "\x1b[0m\x1b[31m╷\x1b[0m\x1b[0m\n"
    "\x1b[31m│\x1b[0m \x1b[0m\x1b[1m\x1b[31mError: \x1b[0m\x1b[0m\x1b[1mreading IAM Policy "
    "(arn:aws:iam::123456789012:policy/anti-demo-runtime-example-3-identity): operation error "
    "IAM: GetPolicy, https response error StatusCode: 0, RequestID: , request send failed, "
    'Post "https://iam.amazonaws.com/": dial tcp: lookup iam.amazonaws.com: no such host'
    "\x1b[0m\n"
    "\x1b[31m│\x1b[0m \x1b[0m\n"
    '\x1b[31m│\x1b[0m \x1b[0m\x1b[0m  with aws_iam_policy.anti_demo_runtime["3-identity"],\n'
    "\x1b[31m│\x1b[0m \x1b[0m  on anti_demo_runtime.tf line 133, in resource "
    '"aws_iam_policy" "anti_demo_runtime":\n'
    "\x1b[31m│\x1b[0m \x1b[0m\n"
    "\x1b[31m╵\x1b[0m\x1b[0m\n"
    "\x1b[31m╷\x1b[0m\x1b[0m\n"
    "\x1b[31m│\x1b[0m \x1b[0m\x1b[1m\x1b[31mError: \x1b[0m\x1b[0m\x1b[1mreading IAM Role "
    "(dms-vpc-role): operation error IAM: GetRole, https response error StatusCode: 0, "
    'RequestID: , request send failed, Post "https://iam.amazonaws.com/": dial tcp: lookup '
    "iam.amazonaws.com: no such host\x1b[0m\n"
    "\x1b[31m│\x1b[0m \x1b[0m\n"
    "\x1b[31m│\x1b[0m \x1b[0m\x1b[0m  with data.aws_iam_role.dms_vpc[0],\n"
    "\x1b[31m╵\x1b[0m\x1b[0m\n"
)

ACCESS_DENIED = (
    "╷\n"
    "│ Error: creating IAM Role (example-uc): operation error IAM: CreateRole, https response "
    "error StatusCode: 403, RequestID: 00000000-0000-0000-0000-000000000000, api error "
    "AccessDenied: User: arn:aws:iam::123456789012:user/operator is not authorized to perform: "
    "iam:CreateRole\n"
    "│\n"
    "│   with aws_iam_role.round6_uc[0],\n"
    "╵\n"
)

# `terraform init` puts the cause on a detail line, under a summary that names no network.
INIT_LOST_NETWORK = (
    "╷\n"
    "│ Error: Failed to query available provider packages\n"
    "│\n"
    "│ Could not retrieve the list of available versions for provider hashicorp/aws: could "
    "not connect to registry.terraform.io: failed to request discovery document: Get "
    '"https://registry.terraform.io/.well-known/terraform.json": dial tcp: lookup '
    "registry.terraform.io: no such host\n"
    "╵\n"
)


@pytest.mark.parametrize(
    ("output", "lost"),
    [
        (RC16_LOST_NETWORK, True),
        (INIT_LOST_NETWORK, True),
        (
            'Error: Get "https://workspace.example.com/api/2.0/apps": dial tcp: lookup '
            "workspace.example.com: no such host\n",
            True,
        ),
        ("read tcp 10.0.0.1:50000->10.0.0.2:443: read: connection reset by peer\n", True),
        # The provider's own credential check, when DNS is what failed it, is a lost network.
        (
            "Error: configuring Terraform AWS Provider: validating provider credentials: "
            "retrieving caller identity from STS: operation error STS: GetCallerIdentity, "
            "https response error StatusCode: 0, RequestID: , request send failed, Post "
            '"https://sts.us-west-2.amazonaws.com/": dial tcp: lookup '
            "sts.us-west-2.amazonaws.com: no such host\n",
            True,
        ),
        # No credential at all: the dial is to the EC2 metadata address, and nothing answers.
        (
            "Error: No valid credential sources found\n"
            "│ Error: failed to refresh cached credentials, no EC2 IMDS role found, operation "
            'error ec2imds: GetMetadata, request send failed, Get "http://169.254.169.254/'
            'latest/meta-data/iam/security-credentials/": dial tcp 169.254.169.254:80: '
            "connect: host is down\n",
            False,
        ),
        (ACCESS_DENIED, False),
        # One refusal among them is a failure asking again cannot fix.
        (RC16_LOST_NETWORK + ACCESS_DENIED, False),
        ("Error: Saved plan is stale\n", False),
        ("", False),
    ],
)
def test_only_errors_that_are_all_a_lost_network_read_as_one(output: str, lost: bool) -> None:
    assert lifecycle._lost_the_network(output) is lost


def test_rc16s_failure_is_a_network_failure_that_names_its_error_line() -> None:
    failure = lifecycle._safe_failure(
        subprocess.CompletedProcess(["terraform"], 1, stdout=RC16_LOST_NETWORK, stderr="")
    )

    assert isinstance(failure, lifecycle.NetworkFailure)
    assert str(failure).startswith("Error: reading IAM Policy")
    assert str(failure).endswith("lookup iam.amazonaws.com: no such host")


def test_a_refusal_is_no_network_failure() -> None:
    failure = lifecycle._safe_failure(
        subprocess.CompletedProcess(["terraform"], 1, stdout=ACCESS_DENIED, stderr="")
    )

    assert type(failure) is RuntimeError
    assert str(failure).startswith("Error: creating IAM Role (example-uc)")


def _python(code: str) -> list[str]:
    return [sys.executable, "-c", code]


_UTF8 = {**os.environ, "PYTHONIOENCODING": "utf-8"}


def test_a_watched_command_still_shows_its_output_and_fails_with_its_own_error(capsys) -> None:
    script = (
        "import sys\n"
        "print('aws_iam_role.round6_uc[0]: Creating...')\n"
        "print('╷', file=sys.stderr)\n"
        "print('│ Error: creating IAM Role (example-uc): AccessDenied', file=sys.stderr)\n"
        "print('╵', file=sys.stderr)\n"
        "sys.exit(1)\n"
    )

    with pytest.raises(RuntimeError) as failed:
        lifecycle._run(_python(script), env=_UTF8)

    assert str(failed.value) == "Error: creating IAM Role (example-uc): AccessDenied"
    assert not isinstance(failed.value, lifecycle.NetworkFailure)
    shown = capsys.readouterr().out
    assert "aws_iam_role.round6_uc[0]: Creating..." in shown
    assert "│ Error: creating IAM Role (example-uc): AccessDenied" in shown


def test_a_watched_command_that_lost_the_network_raises_a_network_failure() -> None:
    script = (
        "import sys\n"
        "print('│ Error: reading IAM Role (dms-vpc-role): request send failed, Post "
        '"https://iam.amazonaws.com/": dial tcp: lookup iam.amazonaws.com: no such host\', '
        "file=sys.stderr)\n"
        "sys.exit(1)\n"
    )

    with pytest.raises(lifecycle.NetworkFailure, match="no such host"):
        lifecycle._run(_python(script), env=_UTF8)


def test_a_watched_command_that_succeeds_returns_what_it_said(capsys) -> None:
    result = lifecycle._run(_python("print('Apply complete!')"), env=_UTF8)

    assert result.returncode == 0
    assert result.stdout == "Apply complete!\n"
    assert capsys.readouterr().out == "Apply complete!\n"


def test_only_the_end_of_a_long_output_is_kept(capsys) -> None:
    result = lifecycle._run_streamed(
        _python("for n in range(1000): print(f'line {n}')"), timeout=60, env=_UTF8
    )

    kept = result.stdout.splitlines()
    assert len(kept) == lifecycle._KEPT_OUTPUT_LINES
    assert kept[-1] == "line 999"
    assert len(capsys.readouterr().out.splitlines()) == 1000


def test_a_watched_command_past_its_timeout_is_killed() -> None:
    started = time.monotonic()

    with pytest.raises(subprocess.TimeoutExpired):
        lifecycle._run(
            _python("import time; print('waiting', flush=True); time.sleep(60)"),
            timeout=0.5,
            env=_UTF8,
        )

    assert time.monotonic() - started < 30


MANIFEST = SimpleNamespace(run_id="ad-test-001")
LOST = lifecycle.NetworkFailure(
    "Error: reading IAM Role (dms-vpc-role): dial tcp: lookup iam.amazonaws.com: no such host"
)


class _Terraform:
    """Plan and apply, each failing as scripted, in order, and every call recorded."""

    def __init__(self, monkeypatch, *, plan_failures=(), apply_failures=()) -> None:
        self.calls: list[str] = []
        self.plan_failures = list(plan_failures)
        self.apply_failures = list(apply_failures)
        self.plans = 0
        monkeypatch.setattr(lifecycle, "_terraform_plan", self.plan)
        monkeypatch.setattr(lifecycle, "_terraform_apply", self.apply)

    def plan(self, manifest, **options) -> Path:
        assert manifest is MANIFEST
        self.plans += 1
        self.calls.append(f"plan {self.plans}" + (f" {options}" if options else ""))
        failure = self.plan_failures.pop(0) if self.plan_failures else None
        if failure is not None:
            raise failure
        return Path(f"plan-{self.plans}.tfplan")

    def apply(self, manifest, plan: Path) -> None:
        assert manifest is MANIFEST
        self.calls.append(f"apply {plan.name}")
        failure = self.apply_failures.pop(0) if self.apply_failures else None
        if failure is not None:
            raise failure


@pytest.fixture
def pauses(monkeypatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(lifecycle.time, "sleep", slept.append)
    return slept


def test_an_apply_that_lost_the_network_is_planned_checked_and_applied_again(
    monkeypatch, pauses, capsys
) -> None:
    terraform = _Terraform(monkeypatch, apply_failures=[LOST])
    checked: list[str] = []

    plan = lifecycle._plan_and_apply(MANIFEST, check=lambda plan: checked.append(plan.name))

    # A new plan, because the stopped apply may have made part of the old one.
    assert terraform.calls == ["plan 1", "apply plan-1.tfplan", "plan 2", "apply plan-2.tfplan"]
    assert checked == ["plan-1.tfplan", "plan-2.tfplan"]
    assert plan == Path("plan-2.tfplan")
    assert pauses == [30.0]
    assert (
        "WAIT  30s for the network, then Terraform again: Error: reading IAM Role (dms-vpc-role)"
        in capsys.readouterr().out
    )


def test_a_plan_that_lost_the_network_is_asked_again(monkeypatch, pauses) -> None:
    terraform = _Terraform(monkeypatch, plan_failures=[LOST, LOST])

    lifecycle._plan_and_apply(MANIFEST)

    assert terraform.calls == ["plan 1", "plan 2", "plan 3", "apply plan-3.tfplan"]
    assert pauses == [30.0, 60.0]


def test_a_refused_plan_is_neither_applied_nor_asked_again(monkeypatch, pauses) -> None:
    terraform = _Terraform(monkeypatch)

    def refuse(_plan: Path) -> None:
        raise RuntimeError("Round 6's AWS DMS lane apply would change more than the lane")

    with pytest.raises(RuntimeError, match="would change more than the lane"):
        lifecycle._plan_and_apply(MANIFEST, check=refuse)

    assert terraform.calls == ["plan 1"]
    assert pauses == []


def test_any_other_failure_raises_at_once(monkeypatch, pauses) -> None:
    denied = RuntimeError("Error: creating IAM Role (example-uc): AccessDenied")
    terraform = _Terraform(monkeypatch, apply_failures=[denied])

    with pytest.raises(RuntimeError) as failed:
        lifecycle._plan_and_apply(MANIFEST)

    assert failed.value is denied
    assert terraform.calls == ["plan 1", "apply plan-1.tfplan"]
    assert pauses == []


def test_a_network_that_stays_gone_raises_after_the_last_pause(monkeypatch, pauses) -> None:
    attempts = len(lifecycle.TERRAFORM_NETWORK_RETRY_SECONDS) + 1
    terraform = _Terraform(monkeypatch, apply_failures=[LOST] * attempts)

    with pytest.raises(lifecycle.NetworkFailure):
        lifecycle._plan_and_apply(MANIFEST)

    assert pauses == list(lifecycle.TERRAFORM_NETWORK_RETRY_SECONDS)
    assert sum(pauses) == 750.0
    assert len(terraform.calls) == 2 * attempts


def test_every_plan_gets_the_options_its_caller_gave(monkeypatch, pauses) -> None:
    terraform = _Terraform(monkeypatch, apply_failures=[LOST])

    lifecycle._plan_and_apply(MANIFEST, targets=("aws_instance.round5_runner",))

    options = "{'targets': ('aws_instance.round5_runner',)}"
    assert terraform.calls == [
        f"plan 1 {options}",
        "apply plan-1.tfplan",
        f"plan 2 {options}",
        "apply plan-2.tfplan",
    ]


def test_a_destroy_that_lost_the_network_destroys_again_from_a_new_plan(
    monkeypatch, pauses
) -> None:
    monkeypatch.setattr(lifecycle, "_release_interfaces_left_on_security_groups", lambda _: [])
    terraform = _Terraform(monkeypatch, apply_failures=[LOST])

    lifecycle._destroy_after_releasing_interfaces(MANIFEST, Path("aws-destroy.tfplan"))

    assert terraform.calls == [
        "apply aws-destroy.tfplan",
        "plan 1 {'destroy': True}",
        "apply plan-1.tfplan",
    ]
    assert pauses == [30.0]


_INSTALLER = ast.parse(inspect.getsource(lifecycle))


def _callers(name: str) -> dict[str, list[ast.Call]]:
    """Each top-level function of the installer that calls `name`, with those calls."""

    callers: dict[str, list[ast.Call]] = {}
    for top in _INSTALLER.body:
        if not isinstance(top, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for node in ast.walk(top):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == name
            ):
                callers.setdefault(top.name, []).append(node)
    return callers


def test_every_plan_and_apply_asks_again_when_the_network_drops() -> None:
    assert set(_callers("_terraform_apply")) == {
        "_plan_and_apply",
        "_destroy_after_releasing_interfaces",
    }
    assert set(_callers("_terraform_plan")) == {
        "_plan_and_apply",
        "_destroy_after_releasing_interfaces",
        "cleanup",
    }
    # Cleanup's own plan is the destroy's first, asked again the same way.
    retried = {
        id(node)
        for call in _callers("_asking_again_on_network_failure")["cleanup"]
        for node in ast.walk(call)
    }
    assert all(id(call) in retried for call in _callers("_terraform_plan")["cleanup"])
    # And every stage that used to plan and apply by hand now goes through the retry.
    for stage in (
        "_complete_provision",
        "_prepare_and_reseal_round4_aws",
        "_prepare_and_reseal_round6_aws",
        "_follow_operator_address",
        "reconcile_infrastructure",
        "_renew_locked",
    ):
        assert stage in _callers("_plan_and_apply"), stage
