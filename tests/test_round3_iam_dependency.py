"""Round 3's RDS lane needs a second RDS API, and therefore a second permission.

`RdsRecoveryAdapter._restorable_window` calls `DescribeDBInstanceAutomatedBackups`
for `RestoreWindow` because the instance shape has no lower bound to read.  That
makes `rds:DescribeDBInstanceAutomatedBackups` a real IAM dependency rather than
an incidental extra call, and a least-privilege policy assembled from
"the lane describes instances and restores them" would omit it.  The resulting
failure names the restore window, not permissions, so nothing about the symptom
points at the policy.

`Stubber` rather than a hand-written fake, because a fake returns whatever dict
the test writes: the whole claim here is about what the real RDS service model
does and does not allow, and a forgiving double is how the `RestoreToTime` bug
and the `EarliestRestorableTime` mistake both nearly survived.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ParamValidationError
from botocore.session import get_session as botocore_session
from botocore.stub import Stubber

from server.aws_permissions import round_actions
from server.recovery_live import RdsRecoveryAdapter

# Read out of the rounds' own call sites rather than written down here.  The
# earlier version of this file named both actions as string literals, which is
# the failure mode the whole area exists to prevent: a hand-written expectation
# cannot catch a hand-written grant, because they are the same sentence typed
# twice.  The prose half of this dependency lives in `docs/iam/README.md`, under
# "2 — databases", which is where an operator assembling the policy will
# actually look.
#
# The RDS read the lane's restore window cannot be computed without: the one
# thing Round 3 asks of AWS that Round 2 does not.  `_restorable_window` needs a
# lower bound and `DBInstance` carries no `EarliestRestorableTime`, so it falls
# through to the automated-backup `RestoreWindow` — and that fall-through is the
# entire difference between the two rounds' permission sets.
_ROUND_THREE_ONLY = round_actions(3) - round_actions(2)
assert len(_ROUND_THREE_ONLY) == 1, (
    f"Round 3 now needs {sorted(_ROUND_THREE_ONLY)} beyond Round 2 rather than one "
    "action; this file's premise has changed and its prose needs rewriting"
)
RESTORE_WINDOW_IAM_ACTION = next(iter(_ROUND_THREE_ONLY))

# The RDS surface that belongs to the restore lanes and to nothing else: what
# Round 3 calls, less what merely observing a target calls (Round 1) and less
# the proxy lane (Round 5).  A policy holding all of this except the window read
# is the trim that shipped, stated as a shape instead of as a pair of names, so
# that a lane which grows a seventh call is covered without an edit here.
RDS_LANE_ACTIONS = frozenset(
    action
    for action in round_actions(3) - round_actions(1) - round_actions(5)
    if action.startswith("rds:")
)

REGION = "us-west-2"
ACCOUNT = "123456789012"
INSTANCE = "anti-demo-rds"
CLUSTER = "anti-demo-aurora"
INFRA = Path(__file__).resolve().parents[1] / "infra" / "aws"
IAM_DOCS = Path(__file__).resolve().parents[1] / "docs" / "iam"


@pytest.fixture(autouse=True)
def _no_ambient_aws_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """These tests read the RDS service model, never an account.

    Creating any botocore client resolves a profile first, and another test in
    the suite leaves `AWS_PROFILE` set, so without this the shape assertions
    fail on a missing profile instead of running. Pointing the config files at
    paths that do not exist makes the outcome independent of both the developer's
    `~/.aws` and test ordering.
    """

    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "absent-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "absent-credentials"))


def _stubbed_rds() -> tuple[Any, Stubber]:
    client = botocore_session().create_client(
        "rds",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        aws_session_token="testing",
    )
    stubber = Stubber(client)
    stubber.activate()
    return client, stubber


def test_a_db_instance_has_no_earliest_restorable_time_to_read() -> None:
    """The shape asymmetry that forces the second call.

    `EarliestRestorableTime` is a member of `DBCluster` and not of `DBInstance`.
    So the Aurora lane reads both bounds from one `DescribeDBClusters` response
    and the RDS lane cannot: it has a `LatestRestorableTime` and no lower bound,
    which is why `_restorable_window` falls through to the automated-backup
    `RestoreWindow` instead of just reading a key.
    """

    now = datetime.now(UTC)
    _, stubber = _stubbed_rds()

    stubber.add_response(
        "describe_db_clusters",
        {
            "DBClusters": [
                {
                    "DBClusterIdentifier": CLUSTER,
                    "EarliestRestorableTime": now - timedelta(hours=1),
                    "LatestRestorableTime": now,
                }
            ]
        },
        {"DBClusterIdentifier": CLUSTER},
    )

    with pytest.raises(ParamValidationError, match="EarliestRestorableTime"):
        stubber.add_response(
            "describe_db_instances",
            {
                "DBInstances": [
                    {
                        "DBInstanceIdentifier": INSTANCE,
                        "EarliestRestorableTime": now - timedelta(hours=1),
                        "LatestRestorableTime": now,
                    }
                ]
            },
            {"DBInstanceIdentifier": INSTANCE},
        )


def test_the_restore_window_the_rds_lane_needs_lives_behind_a_second_operation() -> None:
    """`RestoreWindow` is a member of the automated-backup shape and only that one.

    Validated against the real service model, so a future attempt to fold this
    back into `DescribeDBInstances` fails here rather than at a customer demo.
    """

    now = datetime.now(UTC)
    _, stubber = _stubbed_rds()

    stubber.add_response(
        "describe_db_instance_automated_backups",
        {
            "DBInstanceAutomatedBackups": [
                {
                    "DBInstanceIdentifier": INSTANCE,
                    "Status": "active",
                    "Region": REGION,
                    "RestoreWindow": {
                        "EarliestTime": now - timedelta(hours=1),
                        "LatestTime": now,
                    },
                }
            ]
        },
        {"DBInstanceIdentifier": INSTANCE},
    )

    with pytest.raises(ParamValidationError, match="RestoreWindow"):
        stubber.add_response(
            "describe_db_instances",
            {
                "DBInstances": [
                    {
                        "DBInstanceIdentifier": INSTANCE,
                        "RestoreWindow": {
                            "EarliestTime": now - timedelta(hours=1),
                            "LatestTime": now,
                        },
                    }
                ]
            },
            {"DBInstanceIdentifier": INSTANCE},
        )


def test_the_rds_lane_still_calls_the_operation_the_permission_covers() -> None:
    """A refactor that drops the call must fail here, not in the room.

    `tests/test_recovery_live.py` already pins the exact awaited call. This pins
    the weaker but refactor-proof half: the source of the lane's restore window
    names the operation the IAM action authorizes, so the permission and the
    code cannot drift apart silently.
    """

    client, _ = _stubbed_rds()
    operation = RESTORE_WINDOW_IAM_ACTION.split(":", 1)[1]
    # botocore's own mapping, so the IAM action name and the client method the
    # lane calls are tied together by the service model rather than by a
    # hand-rolled case conversion.
    method_names = {
        api_name: method for method, api_name in client.meta.method_to_api_mapping.items()
    }

    assert operation in method_names, f"{operation} is not an RDS API operation"

    source = Path(RdsRecoveryAdapter._restorable_window.__code__.co_filename).read_text(
        encoding="utf-8"
    )
    assert method_names[operation] in source, (
        f"{RESTORE_WINDOW_IAM_ACTION} is declared as a dependency but the lane no "
        "longer calls it; remove one or restore the other"
    )


def _terraform_policy_action_sets() -> list[tuple[Path, set[str]]]:
    """Every `actions = [...]` list in the account's Terraform, by file.

    Deliberately a text scan rather than a Terraform parse: the invariant below
    only needs to know which action strings appear together in one statement,
    and a scan cannot fail to run because Terraform is uninitialized.
    """

    found: list[tuple[Path, set[str]]] = []
    for path in sorted(INFRA.glob("*.tf")):
        body = path.read_text(encoding="utf-8")
        for block in re.findall(r"actions\s*=\s*\[(.*?)\]", body, flags=re.DOTALL):
            found.append((path, set(re.findall(r'"([a-zA-Z0-9]+:[A-Za-z0-9*]+)"', block))))
    return found


def _documented_policy_action_sets() -> list[tuple[Path, set[str]]]:
    """What each policy in `docs/iam/` grants, one entry per attachable document.

    Parsed as JSON rather than scanned, because these files are the policy
    documents themselves rather than a language that generates them, and a
    malformed one is a failure worth seeing here.

    Unioned across statements, unlike the Terraform scan above, and the
    difference is not cosmetic: what a principal may do is the union of every
    statement in the policy attached to it, and these files are split by service
    boundary rather than by capability. `rds:RestoreDBInstanceToPointInTime`
    sits in `ManageDemoOwnedDatabases` and the reads sit in
    `ReadRdsCatalogAndState`, so a per-statement check would call the file an
    offender while it grants both.
    """

    found: list[tuple[Path, set[str]]] = []
    for path in sorted(IAM_DOCS.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        granted: set[str] = set()
        for statement in document.get("Statement", []):
            actions = statement.get("Action", [])
            granted |= {actions} if isinstance(actions, str) else set(actions)
        found.append((path, granted))
    return found


def _policy_action_sets() -> list[tuple[Path, set[str]]]:
    """Both places a policy for this lane can be written down."""

    return [*_terraform_policy_action_sets(), *_documented_policy_action_sets()]


def test_the_policy_scans_find_real_statements() -> None:
    """Keeps the invariant below from passing because a scan stopped working."""

    terraform = _terraform_policy_action_sets()
    documented = _documented_policy_action_sets()

    assert terraform, f"no Terraform action lists found under {INFRA}"
    assert any("rds:DescribeDBInstances" in actions for _path, actions in terraform)
    assert documented, f"no policy documents found under {IAM_DOCS}"
    assert any("rds:DescribeDBInstances" in actions for _path, actions in documented)
    assert RDS_LANE_ACTIONS, "the derived recovery-lane action set is empty"
    assert RESTORE_WINDOW_IAM_ACTION in RDS_LANE_ACTIONS


def test_a_policy_for_the_rds_lane_must_grant_the_restore_window_read() -> None:
    """The trim this is here to catch, in both places a policy is written down.

    `docs/iam/anti-demo-operator-2-databases.json` is the live case. It is the
    least-privilege set an operator attaches to the principal whose keys drive
    the deployed app, it grants `rds:RestoreDBInstanceToPointInTime`, and it
    shipped without the read the restore window comes from -- exactly the trim
    this predicted, in the file it predicted it in. The symptom would have been
    "AWS restorable window is unavailable" raised mid-bout, naming the window
    and never IAM.

    `infra/aws/` is the latent case. Nothing there governs the identity that
    runs Round 3 today -- the only RDS grants belong to the Round 5 control role
    -- so the invariant holds vacuously until someone writes that policy, and
    catches them when they do. That vacuity is why this cannot be the only
    guard: `tests/test_aws_permission_coverage.py` checks the whole derived set
    against the principals that actually hold it.

    Both action sets come from `server.aws_permissions`, so this compares a
    policy against the code rather than against a second copy of the policy.
    """

    rest_of_lane = RDS_LANE_ACTIONS - {RESTORE_WINDOW_IAM_ACTION}
    offenders = [
        str(path.name)
        for path, actions in _policy_action_sets()
        # A policy that can drive the whole lane and cannot read the window.
        # Stated as "holds the rest of the lane" rather than "holds one named
        # restore" so that a partial grant -- a reaper that only deletes, say --
        # is not accused of needing a restore permission it never uses.
        if rest_of_lane <= actions
        and RESTORE_WINDOW_IAM_ACTION not in actions
        and "rds:*" not in actions
        and "rds:Describe*" not in actions
    ]

    assert offenders == [], (
        f"{offenders} grant the Round 3 RDS lane {sorted(rest_of_lane)} without "
        f"{RESTORE_WINDOW_IAM_ACTION}; the lane reads RestoreWindow from "
        "DescribeDBInstanceAutomatedBackups because DBInstance carries no "
        "EarliestRestorableTime, and the failure names the window rather than IAM"
    )
