"""Every AWS action a round calls is granted to the principal that will call it.

The invariant this replaces predicted the exact trim that shipped and did not
catch it, for a reason worth keeping in front of whoever reads this next: it
compared one hardcoded action name against another hardcoded action name, and it
scanned only `infra/aws/*.tf`, where nothing governs Round 3. It passed by
checking nothing. A guard that has never been seen to go red is not evidence.

So the required set here is *derived*. `server/aws_permissions.py` reads it out
of the calls the rounds actually make, the way `_coordination_runtime_grants`
reads its target out of the accessor the runtime uses. Nothing in this file
writes down an action name that a policy is then checked against -- and
`test_the_coverage_check_fails_when_a_granted_action_is_trimmed` removes an
action from a real document and asserts the comparison notices, so the red path
is exercised on every run rather than taken on trust.

Two principals, two assertions, because they are genuinely different:

*   `AntiDemoAppRuntime`, attached to the IAM user whose static keys the deployed
    Databricks App authenticates with. Narrow on purpose. It must satisfy
    Rounds 1, 2 and 3 and the startup probe and orphan sweep, plus exactly one
    action of Round 5's -- the `sts:AssumeRole` that hands the rest of that round
    to the control role.
*   The operator/runtime-role set, whichever `docs/iam/` files
    `infra/aws/anti_demo_runtime.tf` attaches. That principal drives the
    installer as well as every round, so it must satisfy all of them.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from server.aws_permissions import (
    AWS_ROUNDS,
    actions,
    app_runtime_calls,
    assert_entry_points_resolve,
    call_sites,
    round_actions,
    round_calls,
    unclassified_aws_modules,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
IAM_DOCS = PROJECT_ROOT / "docs" / "iam"
RUNTIME_TERRAFORM = PROJECT_ROOT / "infra" / "aws" / "anti_demo_runtime.tf"

#: The policy attached to the IAM user the deployed app authenticates as. Not
#: rendered by Terraform -- an account administrator attaches it, exactly as with
#: the operator files -- but checked in so that the principal that runs every
#: bout in front of an audience is governed by something other than memory.
APP_RUNTIME_POLICY = IAM_DOCS / "anti-demo-app-runtime.json"

#: What the deployed app does with its own credentials. Round 5 stops short of
#: this list because all but one of its actions travel on an assumed role; that
#: one is asserted by
#: `test_the_app_principal_can_assume_the_round_five_control_role`.
APP_ROUNDS = (1, 2, 3)

#: IAM's own cap on a customer-managed policy, counted over non-whitespace JSON.
IAM_POLICY_CHARACTER_LIMIT = 6144


def granted_actions(*paths: Path) -> set[str]:
    """The union of what these documents allow.

    Unioned across statements and across files because that is what a principal
    actually holds: these documents are split by service boundary and by the
    6144-character cap, not by capability, so a per-statement or per-file view
    would report a policy as missing what the file next to it grants.
    """

    granted: set[str] = set()
    for path in paths:
        document = json.loads(path.read_text(encoding="utf-8"))
        for statement in document.get("Statement", []):
            if statement.get("Effect") != "Allow":
                continue
            action = statement.get("Action", [])
            granted |= {action} if isinstance(action, str) else set(action)
    return granted


def covers(granted: set[str], required: str) -> bool:
    """Whether a grant set allows one action, honouring the wildcards IAM honours."""

    if required in granted:
        return True
    service = required.split(":", 1)[0]
    return any(
        entry == "*"
        or entry == f"{service}:*"
        or (entry.endswith("*") and required.startswith(entry[:-1]))
        for entry in granted
    )


def attached_operator_documents() -> tuple[Path, ...]:
    """The `docs/iam/` files Terraform attaches to the runtime role.

    Read out of `anti_demo_runtime.tf` rather than listed here, so that
    attaching a fourth document extends this check instead of quietly leaving it
    behind -- and so that `anti-demo-operator-4-state.json`, which is opt-in and
    carries an unrenderable placeholder, stays out of it without being named.
    """

    body = RUNTIME_TERRAFORM.read_text(encoding="utf-8")
    block = re.search(r"anti_demo_runtime_policy_files\s*=.*?\{(.*?)\}", body, flags=re.DOTALL)
    assert block is not None, f"no policy file map found in {RUNTIME_TERRAFORM}"
    names = re.findall(r'"(anti-demo-operator-[^"]+\.json)"', block.group(1))
    assert names, "the policy file map named no documents; the scan has stopped working"
    return tuple(IAM_DOCS / name for name in names)


def missing(granted: set[str], required: set[str]) -> list[str]:
    return sorted(action for action in required if not covers(granted, action))


def app_required_actions() -> set[str]:
    required: set[str] = set()
    for round_number in APP_ROUNDS:
        required |= round_actions(round_number)
    return required | actions(app_runtime_calls())


def app_required_calls():
    calls = list(app_runtime_calls())
    for round_number in APP_ROUNDS:
        calls.extend(round_calls(round_number))
    return calls


def test_the_derivation_still_finds_calls_in_every_round() -> None:
    """Keeps every assertion below from passing because the scan stopped working.

    The failure this exists for is the one the previous guard actually had: an
    empty required set compares clean against any policy at all, so the check
    that matters is that the derivation found something in each round before
    anything is concluded from it.
    """

    assert_entry_points_resolve()
    for round_number in AWS_ROUNDS:
        derived = round_actions(round_number)
        assert derived, f"round {round_number} derived no AWS actions at all"
    assert unclassified_aws_modules() == (), (
        f"{unclassified_aws_modules()} open an AWS client and belong to no runtime "
        "surface; classify them in server/aws_permissions.py so their permissions "
        "are checked rather than discovered in the room"
    )


def test_round_three_needs_exactly_one_read_round_two_does_not() -> None:
    """The dependency this whole area exists for, derived rather than asserted.

    `RdsRecoveryAdapter._restorable_window` falls through to
    `DescribeDBInstanceAutomatedBackups` because `DBInstance` carries no
    `EarliestRestorableTime`. That is the single thing Round 3 asks of RDS that
    Round 2 does not, and stating it as a difference rather than as a name means
    a refactor that moves the call shows up here as a changed difference instead
    of as a passing test.
    """

    difference = round_actions(3) - round_actions(2)
    assert difference == {"rds:DescribeDBInstanceAutomatedBackups"}, (
        f"Round 3 now differs from Round 2 by {sorted(difference)}; if that is "
        "intended, every policy for the recovery lane needs the new action too"
    )


def test_the_app_runtime_policy_grants_everything_the_deployed_app_calls() -> None:
    """The one that would have caught tonight's trim, on the live principal.

    Rounds 1, 2 and 3 plus the startup credential probe and the orphan sweep.
    The sweep is in here rather than treated as incidental because a round that
    measures correctly and then cannot delete what it created leaks billable
    resources, and that failure is invisible from the result screen.
    """

    granted = granted_actions(APP_RUNTIME_POLICY)
    required = app_required_actions()
    gaps = missing(granted, required)
    detail = "; ".join(
        f"{action} (called at {', '.join(call_sites(app_required_calls(), action))})"
        for action in gaps
    )
    assert gaps == [], (
        f"{APP_RUNTIME_POLICY.name} does not grant {detail} — the deployed app "
        "would reach the database and then fail on IAM"
    )


def test_the_operator_policy_set_grants_everything_every_round_calls() -> None:
    """The same question of the principal that runs the installer and all six rounds."""

    documents = attached_operator_documents()
    granted = granted_actions(*documents)
    required: set[str] = set(actions(app_runtime_calls()))
    for round_number in AWS_ROUNDS:
        required |= round_actions(round_number)
    every_call = list(app_runtime_calls())
    for round_number in AWS_ROUNDS:
        every_call.extend(round_calls(round_number))
    gaps = missing(granted, required)
    detail = "; ".join(
        f"{action} (called at {', '.join(call_sites(every_call, action))})" for action in gaps
    )
    assert gaps == [], f"{[path.name for path in documents]} together do not grant {detail}"


def test_the_coverage_check_fails_when_a_granted_action_is_trimmed(
    tmp_path: Path,
) -> None:
    """The red path, run on every suite so it is never taken on trust.

    Every required action is removed in turn from a real document, and the
    comparison must name that action and only that action. Removing one at a
    time rather than one chosen action is what makes this refuse to rot: an
    action that stops being derived stops being tested here too, which is
    visible in the parametrisation count rather than silent.
    """

    document = json.loads(APP_RUNTIME_POLICY.read_text(encoding="utf-8"))
    required = app_required_actions()
    assert required, "nothing to trim; the derivation returned an empty set"

    for victim in sorted(required):
        trimmed = {
            "Version": document["Version"],
            "Statement": [
                {
                    **statement,
                    "Action": (
                        [
                            action
                            for action in (
                                [statement["Action"]]
                                if isinstance(statement["Action"], str)
                                else statement["Action"]
                            )
                            if action != victim
                        ]
                    ),
                }
                for statement in document["Statement"]
            ],
        }
        path = tmp_path / "trimmed.json"
        path.write_text(json.dumps(trimmed), encoding="utf-8")
        assert missing(granted_actions(path), required) == [victim], (
            f"removing {victim} from the app runtime policy did not make the "
            "coverage check fail; the check is not reading what it claims to"
        )


def test_the_documented_app_policy_fits_in_one_iam_policy() -> None:
    """The 6144-character cap that forced the operator set into three files.

    Checked here rather than left to `aws iam create-policy` because the
    consequence of exceeding it is an operator discovering, mid-repair, that the
    document they were told to attach cannot be attached.
    """

    packed = json.dumps(
        json.loads(APP_RUNTIME_POLICY.read_text(encoding="utf-8")), separators=(",", ":")
    )
    assert len(packed) <= IAM_POLICY_CHARACTER_LIMIT, (
        f"{APP_RUNTIME_POLICY.name} packs to {len(packed)} characters; split it "
        "rather than dropping a resource constraint to fit"
    )


def test_the_documented_app_policy_still_uses_placeholders() -> None:
    """No account number and no region, in a file the guards read either way."""

    body = APP_RUNTIME_POLICY.read_text(encoding="utf-8")
    assert "<AWS_ACCOUNT_ID>" in body
    assert "<AWS_REGION>" in body
    assert re.search(r"\b\d{12}\b", body) is None


@pytest.mark.parametrize("round_number", [5])
def test_the_app_principal_can_assume_the_round_five_control_role(
    round_number: int,
) -> None:
    """The one Round 5 action that travels on the app's own credentials.

    The rest of Round 5's actions are the *assumed* role's.
    `connection_spike_live` opens its clients on a session it gets back from
    `sts:AssumeRole`, so almost everything it calls is authorised by the Round 5
    control role rather than by the app's own principal. Source alone cannot say
    which credentials a call travels on, so encoding all of Round 5 against the
    app policy would demand thirty-odd actions the app must never hold -- which is
    why `APP_ROUNDS` still stops at 3.

    The assume itself is not ambiguous, and it is the reason this assertion
    inverted. It used to read `not covers(...)`, recording that the app principal
    lacked this grant and that Round 5 had therefore never run in the deployed
    app. That was a description of a gap, not an invariant worth keeping: the gap
    has since been closed on purpose, on both sides -- the grant below, and a
    control-role trust policy naming the app principal. Trimming either one puts
    Round 5 back to failing at arm, after the catalog has already offered it,
    which is the failure this file exists to prevent.

    The trust policy is the half that cannot be checked from here; it lives in
    AWS, and `_round5_topology_check` in `server/lifecycle.py` compares it against
    the seal on every `antidemo status`.
    """

    assert "sts:AssumeRole" in round_actions(round_number)
    granted = granted_actions(APP_RUNTIME_POLICY)
    assert covers(granted, "sts:AssumeRole"), (
        f"{APP_RUNTIME_POLICY.name} no longer grants sts:AssumeRole; Round 5 "
        "would be offered by the catalog and then fail to assume its control role"
    )
