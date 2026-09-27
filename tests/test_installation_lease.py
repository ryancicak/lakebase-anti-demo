"""The installation lease: moved forward while the demo is used, never proof of ownership.

`server/lease.py` keeps every Terraform-made resource's `expires-at` tag at
`last use + window`. What these tests hold it to:

* Terraform ignores the tag after creation, on every tagged resource, so a
  moved lease is never a plan diff (the operator-address rebind refuses any
  plan that does more than move ingress, so a diff there would break it).
* Nothing proves ownership with the tag any more, except Round 5's per-bout set,
  which still carries the sealed value its IAM conditions require.
* Discovery keeps only this installation's Terraform-made resources: never a
  neighbor's, and never a round's own per-bout artifacts.
* A lease only ever moves forward, and only because a person used the app.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from starlette.requests import Request
from test_lifecycle import make_manifest

import app as app_module
from server import lifecycle
from server.lease import (
    LEASE_CURRENT,
    LEASE_FAILED,
    LEASE_IDLE,
    RENEW_HYSTERESIS,
    LeaseInventory,
    LeaseKeeper,
    LeaseRenewal,
    LeaseTarget,
    discover_lease_targets,
    format_lease,
    lease_window,
    parse_lease,
    renew_lease,
)

INFRA = Path(__file__).resolve().parents[1] / "infra" / "aws"
NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
ACCOUNT = "123456789012"
RUN = "ad-test-001"
OWN = {"anti-demo-run-id": RUN, "managed-by": "terraform"}


def _tags(mapping: dict[str, str]) -> list[dict[str, str]]:
    return [{"Key": key, "Value": value} for key, value in mapping.items()]


def _error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "Operation")


class FakePaginator:
    def __init__(self, session: FakeSession, key: tuple[str, str]) -> None:
        self.session = session
        self.key = key

    def paginate(self, **kwargs):
        self.session.calls.append((*self.key, kwargs))
        pages = self.session.pages.get(self.key, [{}])
        if isinstance(pages, Exception):
            raise pages
        return iter(pages)


class FakeClient:
    def __init__(self, service: str, session: FakeSession) -> None:
        self.service = service
        self.session = session

    def get_paginator(self, operation: str) -> FakePaginator:
        return FakePaginator(self.session, (self.service, operation))

    def __getattr__(self, operation: str):
        def call(**kwargs):
            self.session.calls.append((self.service, operation, kwargs))
            answer = self.session.responses.get((self.service, operation), {})
            if isinstance(answer, Exception):
                raise answer
            return answer(**kwargs) if callable(answer) else answer

        return call


class FakeSession:
    """Never reaches botocore, so the suite's real-AWS guard is never in play."""

    def __init__(self, pages=None, responses=None) -> None:
        self.pages = pages or {}
        self.responses = responses or {}
        self.calls: list[tuple[str, str, dict]] = []

    def client(self, service: str, **_kwargs) -> FakeClient:
        return FakeClient(service, self)

    def tagged(self) -> list[tuple[str, str, dict]]:
        writes = {
            "create_tags",
            "add_tags_to_resource",
            "tag_resource",
            "tag_queue",
            "tag_role",
            "tag_policy",
            "tag_instance_profile",
        }
        return [call for call in self.calls if call[1] in writes]


def _manifest_with_round5():
    manifest = make_manifest()
    manifest.aws.runtime_role_arn = f"arn:aws:iam::{ACCOUNT}:role/anti-demo-runtime-abc"
    manifest.round5 = SimpleNamespace(
        control_role_arn=f"arn:aws:iam::{ACCOUNT}:role/i1-r5-exec-1",
        runner_role_arn=f"arn:aws:iam::{ACCOUNT}:role/i1-r5-runner-1",
        runner_instance_profile_arn=f"arn:aws:iam::{ACCOUNT}:instance-profile/i1-r5-runner-1",
        lakebase_control_queue_url=(
            f"https://sqs.us-west-2.amazonaws.com/{ACCOUNT}/i1-r5-lakebase-control.fifo"
        ),
    )
    return manifest


# --------------------------------------------------------------------------
# Terraform: the tag is written once and then left alone.
# --------------------------------------------------------------------------


def _depth(line: str) -> int:
    change, quoted, escaped = 0, False, False
    for index, char in enumerate(line):
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            quoted = True
        elif char == "#" or line.startswith("//", index):
            break
        elif char == "{":
            change += 1
        elif char == "}":
            change -= 1
    return change


def _resource_blocks(text: str):
    lines = text.splitlines()
    index = 0
    header = re.compile(r'^resource "([\w-]+)" "([\w-]+)" \{\s*$')
    while index < len(lines):
        match = header.match(lines[index])
        if not match:
            index += 1
            continue
        depth, start = _depth(lines[index]), index
        index += 1
        while depth > 0:
            depth += _depth(lines[index])
            index += 1
        yield match[1], match[2], "\n".join(lines[start + 1 : index - 1])


def test_every_tagged_terraform_resource_ignores_later_changes_to_the_lease() -> None:
    """A moved lease must never read as drift.

    Without the ignore, the next plan after a renewal would propose moving every
    tag back, the operator-address rebind -- which refuses a plan doing anything
    but moving ingress -- would refuse, and an apply would undo the lease.
    """
    checked = 0
    missing: list[str] = []
    for path in sorted(INFRA.glob("*.tf")):
        for kind, name, body in _resource_blocks(path.read_text(encoding="utf-8")):
            if not re.search(r"^  tags\s*=", body, re.MULTILINE):
                continue
            checked += 1
            ignore = re.search(r"^    ignore_changes\s*=\s*\[(.*)\]", body, re.MULTILINE)
            listed = ignore.group(1) if ignore else ""
            if 'tags["expires-at"]' not in listed:
                missing.append(f"{path.name}: {kind}.{name}")
            if (
                kind == "aws_instance"
                and re.search(r"^    tags\s*=", body, re.MULTILINE)
                and 'root_block_device[0].tags["expires-at"]' not in listed
            ):
                missing.append(f"{path.name}: {kind}.{name} (root volume)")
    assert checked >= 40, f"the scan found only {checked} tagged resources; it is not reading"
    assert missing == [], "tagged resources that would fight the lease: " + ", ".join(missing)


# --------------------------------------------------------------------------
# Ownership: proven without the lease, except where IAM requires the seal.
# --------------------------------------------------------------------------


def test_ownership_is_proven_without_the_lease() -> None:
    manifest = make_manifest()
    required = lifecycle._required_tags(manifest)
    assert "expires-at" not in required
    assert "expires-at" not in lifecycle._required_tags_for_address(
        manifest, "aws_iam_role.round5_runner"
    )
    # Every ownership check here is a subset comparison, so a resource whose
    # lease the app moved is still recognized as this installation's.
    moved = {**required, "expires-at": "2031-01-01T00:00:00Z"}
    assert all(moved.get(key) == value for key, value in required.items())


def test_round5_per_bout_tags_still_carry_the_sealed_expiry_exactly() -> None:
    """The control role's IAM conditions require exactly the sealed value."""
    manifest = make_manifest()
    output = {
        **lifecycle._required_round_tags(manifest, "r5"),
        "expires-at": lifecycle._utc_tag(manifest.expires_at),
        "managed-by": "round5-lifecycle",
    }

    sealed = lifecycle._round5_ownership_tags(manifest, {"ownership_tags": output})
    assert sealed.expires_at == lifecycle._utc_tag(manifest.expires_at)

    for drifted in (
        {**output, "expires-at": "2031-01-01T00:00:00Z"},
        {key: value for key, value in output.items() if key != "expires-at"},
    ):
        with pytest.raises(RuntimeError, match="not exact"):
            lifecycle._round5_ownership_tags(manifest, {"ownership_tags": drifted})


# --------------------------------------------------------------------------
# Discovery: this installation's Terraform-made resources, and nothing else.
# --------------------------------------------------------------------------


def test_discovery_keeps_only_this_installations_terraform_resources() -> None:
    manifest = _manifest_with_round5()
    lease = "2026-09-28T00:00:00Z"
    own = {**OWN, "expires-at": lease}
    prefix = f"arn:aws:rds:us-west-2:{ACCOUNT}"
    dead_letter = f"https://sqs.us-west-2.amazonaws.com/{ACCOUNT}/i1-r5-lakebase-control-dlq.fifo"
    session = FakeSession(
        pages={
            ("ec2", "describe_instances"): [
                {"Reservations": [{"Instances": [{"InstanceId": "i-own", "Tags": _tags(own)}]}]}
            ],
            ("ec2", "describe_volumes"): [
                {"Volumes": [{"VolumeId": "vol-own", "Tags": _tags(own)}]}
            ],
            ("ec2", "describe_security_groups"): [
                {"SecurityGroups": [{"GroupId": "sg-own", "Tags": _tags(own)}]}
            ],
            ("ec2", "describe_security_group_rules"): [
                {
                    "SecurityGroupRules": [
                        {"SecurityGroupRuleId": "sgr-own", "Tags": _tags(own)},
                        # A Round 5 per-bout rule: same run, not Terraform's.
                        {
                            "SecurityGroupRuleId": "sgr-bout",
                            "Tags": _tags({**own, "managed-by": "round5-lifecycle"}),
                        },
                    ]
                }
            ],
            ("rds", "describe_db_clusters"): [
                {
                    "DBClusters": [
                        {
                            "DBClusterArn": f"{prefix}:cluster:own",
                            "TagList": _tags(own),
                            "DBSubnetGroup": "own-subnets",
                            "DBClusterParameterGroup": "own-cluster-pg",
                        },
                        {
                            "DBClusterArn": f"{prefix}:cluster:neighbor",
                            "TagList": _tags({**own, "anti-demo-run-id": "somebody-else"}),
                            "DBSubnetGroup": "their-subnets",
                        },
                        {
                            "DBClusterArn": f"{prefix}:cluster:adsc-clone",
                            "TagList": _tags({**own, "managed-by": "lakebase-anti-demo-round-2"}),
                        },
                    ]
                }
            ],
            ("rds", "describe_db_instances"): [
                {
                    "DBInstances": [
                        {
                            "DBInstanceArn": f"{prefix}:db:own",
                            "TagList": _tags(own),
                            "DBSubnetGroup": {"DBSubnetGroupName": "own-subnets"},
                            "DBParameterGroups": [
                                {"DBParameterGroupName": "default.postgres17"},
                                {"DBParameterGroupName": "own-pg"},
                            ],
                        }
                    ]
                }
            ],
            ("secretsmanager", "list_secrets"): [
                {
                    "SecretList": [
                        {"ARN": "arn:secret:lakebase-ant-own", "Tags": _tags(own)},
                        {
                            "ARN": "arn:secret:rds!db-own",
                            "Tags": _tags(own),
                            "OwningService": "rds",
                        },
                    ]
                }
            ],
            ("iam", "list_attached_role_policies"): [
                {
                    "AttachedPolicies": [
                        {"PolicyArn": f"arn:aws:iam::{ACCOUNT}:policy/anti-demo-runtime-1-x"},
                        {"PolicyArn": "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"},
                    ]
                }
            ],
            ("iam", "list_instance_profiles_for_role"): [{"InstanceProfiles": []}],
        },
        responses={
            ("rds", "list_tags_for_resource"): {"TagList": _tags(own)},
            ("sqs", "get_queue_attributes"): {
                "Attributes": {
                    "RedrivePolicy": (
                        '{"deadLetterTargetArn": '
                        f'"arn:aws:sqs:us-west-2:{ACCOUNT}:i1-r5-lakebase-control-dlq.fifo"}}'
                    )
                }
            },
            ("sqs", "get_queue_url"): {"QueueUrl": dead_letter},
            ("sqs", "list_queue_tags"): {"Tags": own},
            ("iam", "get_role"): lambda RoleName: {
                "Role": {"RoleName": RoleName, "Tags": _tags(own)}
            },
            ("iam", "list_policy_tags"): {"Tags": _tags(own)},
            ("iam", "get_instance_profile"): {"InstanceProfile": {"Tags": _tags(own)}},
        },
    )

    inventory = discover_lease_targets(session, manifest)

    found = {(target.service, target.identifier) for target in inventory.targets}
    assert found == {
        ("ec2", "i-own"),
        ("ec2", "vol-own"),
        ("ec2", "sg-own"),
        ("ec2", "sgr-own"),
        ("rds", f"{prefix}:cluster:own"),
        ("rds", f"{prefix}:db:own"),
        ("rds", f"{prefix}:subgrp:own-subnets"),
        ("rds", f"{prefix}:pg:own-pg"),
        ("rds", f"{prefix}:cluster-pg:own-cluster-pg"),
        ("secretsmanager", "arn:secret:lakebase-ant-own"),
        ("sqs", manifest.round5.lakebase_control_queue_url),
        ("sqs", dead_letter),
        ("iam-role", "anti-demo-runtime-abc"),
        ("iam-role", "i1-r5-exec-1"),
        ("iam-role", "i1-r5-runner-1"),
        ("iam-policy", f"arn:aws:iam::{ACCOUNT}:policy/anti-demo-runtime-1-x"),
        ("iam-instance-profile", "i1-r5-runner-1"),
    }
    assert inventory.unreadable == ()
    assert inventory.earliest == parse_lease(lease)
    # AWS filters EC2 on both ownership tags, so a shared account's other
    # resources are never listed at all.
    ec2_filters = [call[2]["Filters"] for call in session.calls if call[0] == "ec2"]
    for filters in ec2_filters:
        names = {entry["Name"]: entry["Values"] for entry in filters}
        assert names["tag:anti-demo-run-id"] == [RUN]
        assert names["tag:managed-by"] == ["terraform"]
    # Discovery only reads.
    assert session.tagged() == []


def test_a_service_that_cannot_be_listed_is_named_and_the_rest_still_read() -> None:
    manifest = make_manifest()
    own = {**OWN, "expires-at": "2026-09-28T00:00:00Z"}
    session = FakeSession(
        pages={
            ("rds", "describe_db_clusters"): _error("AccessDenied"),
            ("ec2", "describe_security_groups"): [
                {"SecurityGroups": [{"GroupId": "sg-own", "Tags": _tags(own)}]}
            ],
        }
    )

    inventory = discover_lease_targets(session, manifest)

    assert inventory.unreadable == ("rds: AccessDenied",)
    assert [target.identifier for target in inventory.targets] == ["sg-own"]


# --------------------------------------------------------------------------
# Renewal: forward only, one refusal never holds the rest back.
# --------------------------------------------------------------------------


def test_a_renewal_only_moves_leases_forward_and_past_the_minimum_gain() -> None:
    manifest = make_manifest()
    target = NOW + timedelta(hours=72)
    inventory = LeaseInventory(
        (
            LeaseTarget("rds", "arn:behind", NOW + timedelta(hours=10)),
            LeaseTarget("rds", "arn:ahead", NOW + timedelta(hours=100)),
            LeaseTarget("rds", "arn:close", NOW + timedelta(hours=70)),
            LeaseTarget("secretsmanager", "arn:unreadable", None),
        )
    )
    session = FakeSession()

    renewal = renew_lease(
        session, manifest, target, minimum_gain=RENEW_HYSTERESIS, inventory=inventory
    )

    moved = {
        call[2].get("ResourceName") or call[2].get("SecretId"): call[2]["Tags"]
        for call in session.tagged()
    }
    assert set(moved) == {"arn:behind", "arn:unreadable"}
    expected = [{"Key": "expires-at", "Value": format_lease(target)}]
    assert all(tags == expected for tags in moved.values())
    assert renewal.complete and renewal.renewed == 2 and renewal.resources == 4
    # The resource that was already ahead keeps its later lease: never backwards.
    assert renewal.earliest == NOW + timedelta(hours=70)


def test_a_resource_gone_is_not_a_failure_and_a_refusal_is_one_line() -> None:
    manifest = make_manifest()

    def add_tags(ResourceName, Tags):
        del Tags
        if ResourceName == "arn:gone":
            raise _error("DBClusterNotFoundFault")
        if ResourceName == "arn:denied":
            raise _error("AccessDenied")
        return {}

    session = FakeSession(responses={("rds", "add_tags_to_resource"): add_tags})
    old = NOW + timedelta(hours=1)
    inventory = LeaseInventory(
        (
            LeaseTarget("rds", "arn:gone", old),
            LeaseTarget("rds", "arn:denied", old),
            LeaseTarget("rds", "arn:fine", old),
        )
    )

    renewal = renew_lease(session, manifest, NOW + timedelta(hours=72), inventory=inventory)

    assert renewal.vanished == 1
    assert renewal.renewed == 1
    assert renewal.failed == ("rds AccessDenied",)
    assert not renewal.complete
    # The refused resource still carries its old lease, and that is what counts.
    assert renewal.earliest == old
    assert "1 of 3 resources moved" in renewal.summary()


def test_ec2_is_tagged_in_one_batch_and_retried_one_by_one_when_refused() -> None:
    manifest = make_manifest()

    def create_tags(Resources, Tags):
        del Tags
        if "sg-gone" in Resources:
            raise _error("InvalidGroup.NotFound")
        return {}

    session = FakeSession(responses={("ec2", "create_tags"): create_tags})
    old = NOW + timedelta(hours=1)
    inventory = LeaseInventory(
        (LeaseTarget("ec2", "sg-own", old), LeaseTarget("ec2", "sg-gone", old))
    )

    renewal = renew_lease(session, manifest, NOW + timedelta(hours=72), inventory=inventory)

    batches = [call[2]["Resources"] for call in session.tagged()]
    assert batches[0] == ["sg-own", "sg-gone"]
    assert sorted(batches[1:]) == [["sg-gone"], ["sg-own"]]
    assert renewal.renewed == 1 and renewal.vanished == 1 and renewal.complete


# --------------------------------------------------------------------------
# The keeper: renews on use, not on the clock, and never storms on failure.
# --------------------------------------------------------------------------


def _renewal(target, *, failed=()):
    return LeaseRenewal(
        target=target,
        resources=5,
        renewed=0 if failed else 5,
        vanished=0,
        failed=tuple(failed),
        unreadable=(),
        earliest=None if failed else target,
    )


async def test_the_keeper_renews_on_use_and_not_on_the_clock() -> None:
    clock = [NOW]
    renewals: list[tuple[datetime, timedelta]] = []

    def renew(session, manifest, target, *, minimum_gain):
        renewals.append((target, minimum_gain))
        return _renewal(target)

    keeper = LeaseKeeper(
        make_manifest(),
        lambda manifest: object(),
        window=timedelta(hours=72),
        clock=lambda: clock[0],
        renew=renew,
    )

    # A server left running with nobody on it does not keep itself alive.
    assert await keeper.tick() is None
    assert keeper.snapshot()["lease_state"] == LEASE_IDLE
    assert renewals == []

    keeper.note_activity()
    await keeper.tick()
    assert renewals == [(NOW + timedelta(hours=72), RENEW_HYSTERESIS)]
    snapshot = keeper.snapshot()
    assert snapshot["lease_state"] == LEASE_CURRENT
    assert snapshot["lease_expires_at"] == format_lease(NOW + timedelta(hours=72))

    # Inside the hysteresis a use changes nothing: no retag per request.
    clock[0] = NOW + timedelta(hours=5)
    keeper.note_activity()
    assert await keeper.tick() is None
    assert len(renewals) == 1

    clock[0] = NOW + timedelta(hours=7)
    keeper.note_activity()
    await keeper.tick()
    assert renewals[-1][0] == NOW + timedelta(hours=79)


async def test_a_failed_renewal_waits_out_its_retry_instead_of_retrying_per_request() -> None:
    clock = [NOW]
    attempts = []

    def renew(session, manifest, target, *, minimum_gain):
        attempts.append(target)
        raise RuntimeError("sts is down")

    keeper = LeaseKeeper(
        make_manifest(),
        lambda manifest: object(),
        window=timedelta(hours=72),
        retry_seconds=600,
        clock=lambda: clock[0],
        renew=renew,
    )
    keeper.note_activity()
    await keeper.tick()
    assert len(attempts) == 1
    assert keeper.snapshot()["lease_state"] == LEASE_FAILED
    assert "RuntimeError" in keeper.snapshot()["lease_detail"]

    clock[0] = NOW + timedelta(minutes=1)
    keeper.note_activity()
    await keeper.tick()
    assert len(attempts) == 1

    clock[0] = NOW + timedelta(minutes=11)
    await keeper.tick()
    assert len(attempts) == 2


async def test_a_partial_renewal_is_reported_and_retried() -> None:
    clock = [NOW]
    results = iter(
        [
            _renewal(NOW + timedelta(hours=72), failed=["iam-role AccessDenied"]),
            _renewal(NOW + timedelta(hours=72, minutes=11)),
        ]
    )

    keeper = LeaseKeeper(
        make_manifest(),
        lambda manifest: object(),
        window=timedelta(hours=72),
        retry_seconds=600,
        clock=lambda: clock[0],
        renew=lambda session, manifest, target, *, minimum_gain: next(results),
    )
    keeper.note_activity()
    await keeper.tick()
    assert keeper.snapshot()["lease_state"] == LEASE_FAILED
    assert "iam-role AccessDenied" in keeper.snapshot()["lease_detail"]

    clock[0] = NOW + timedelta(minutes=11)
    keeper.note_activity()
    await keeper.tick()
    assert keeper.snapshot()["lease_state"] == LEASE_CURRENT


def test_the_window_is_the_ttl_the_installation_was_provisioned_with() -> None:
    manifest = make_manifest()
    manifest.created_at = NOW
    manifest.expires_at = NOW + timedelta(hours=72)
    assert lease_window(manifest) == timedelta(hours=72)

    manifest.expires_at = NOW - timedelta(hours=1)
    assert lease_window(manifest) == timedelta(hours=72)

    manifest.expires_at = NOW + timedelta(days=365)
    assert lease_window(manifest) == timedelta(hours=720)


# --------------------------------------------------------------------------
# The app: who counts as a use, and what /readyz says.
# --------------------------------------------------------------------------


def _request(path: str, headers: tuple[tuple[str, str], ...] = ()) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": b"",
            "headers": [(key.encode(), value.encode()) for key, value in headers],
        }
    )


def test_only_a_person_using_the_app_counts_as_a_use(monkeypatch) -> None:
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    signed_in = (("x-forwarded-email", "someone@databricks.com"),)

    # Locally, the operator is the only one who can reach the server.
    assert app_module._is_a_use(_request("/api/catalog"))
    assert not app_module._is_a_use(_request("/readyz"))

    # Deployed, only a request the Apps proxy vouched for with a user counts.
    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")
    assert not app_module._is_a_use(_request("/api/catalog"))
    assert app_module._is_a_use(_request("/api/catalog", signed_in))
    for probe in ("/healthz", "/readyz", "/api/ready"):
        assert not app_module._is_a_use(_request(probe, signed_in))


def test_the_middleware_notes_a_use_and_ignores_a_probe(monkeypatch) -> None:
    monkeypatch.delenv("ANTI_DEMO_ENV", raising=False)
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    noted: list[str] = []
    keeper = SimpleNamespace(note_activity=lambda: noted.append("use"))
    monkeypatch.setattr(app_module.app.state, "lease_keeper", keeper, raising=False)
    client = TestClient(app_module.app)

    client.get("/healthz")
    assert noted == []
    client.get("/api/lease-test-no-such-route")
    assert noted == ["use"]


def test_readyz_reports_the_lease_without_degrading(monkeypatch) -> None:
    payload: dict[str, object] = {"status": "ready"}
    monkeypatch.setattr(app_module.app.state, "lease_keeper", None, raising=False)
    app_module._apply_installation_lease(payload)
    assert payload["lease_state"] == "unkept"
    assert payload["status"] == "ready"

    keeper = LeaseKeeper(make_manifest(), lambda manifest: object())
    monkeypatch.setattr(app_module.app.state, "lease_keeper", keeper, raising=False)
    app_module._apply_installation_lease(payload)
    assert payload["lease_state"] == LEASE_IDLE
    assert payload["status"] == "ready"
    assert "degraded" not in payload


# --------------------------------------------------------------------------
# Setup: a use of the installation, never failed by the lease.
# --------------------------------------------------------------------------


def test_setup_moves_the_lease_and_a_failure_is_one_warning(monkeypatch, capsys) -> None:
    manifest = make_manifest()
    monkeypatch.setattr(lifecycle, "_aws_session", lambda candidate: object())
    captured: dict[str, object] = {}

    def moved(session, candidate, target, *, minimum_gain):
        captured["target"] = target
        captured["gain"] = minimum_gain
        return _renewal(target)

    monkeypatch.setattr("server.lease.renew_lease", moved)
    before = datetime.now(UTC)
    lifecycle._keep_lease_current(manifest)
    assert captured["target"] >= before + lease_window(manifest) - timedelta(seconds=1)
    assert captured["gain"] == RENEW_HYSTERESIS
    assert "LEASE 5 of 5 resources moved" in capsys.readouterr().out

    def refused(session, candidate, target, *, minimum_gain):
        raise _error("ExpiredToken")

    monkeypatch.setattr("server.lease.renew_lease", refused)
    lifecycle._keep_lease_current(manifest)
    warned = capsys.readouterr().out
    assert "WARN  the expires-at lease could not be checked (ClientError)" in warned

    def guarded(session, candidate, target, *, minimum_gain):
        raise AssertionError("the suite's real-AWS guard")

    monkeypatch.setattr("server.lease.renew_lease", guarded)
    with pytest.raises(AssertionError):
        lifecycle._keep_lease_current(manifest)
