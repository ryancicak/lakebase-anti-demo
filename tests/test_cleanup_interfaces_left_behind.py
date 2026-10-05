"""Cleanup deletes the network interfaces AWS left detached on the installation's security groups.

2026-09-30: the live uninstall destroyed everything but four security groups and stopped.
RDS had left a network interface detached on Round 2's RDS security group, a security group
can't be deleted while an interface holds it, and Terraform's provider got 403 deleting the
interface itself. The operator deleted it by hand and ran cleanup again.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from server import lifecycle

RUN_ID = "ad-test-001"
REGION = "us-west-2"


def _interface(
    interface_id: str,
    *groups: str,
    status: str = "available",
    managed: bool = True,
    description: str = "RDSNetworkInterface",
    requester: str = "amazon-rds",
) -> dict:
    interface = {
        "NetworkInterfaceId": interface_id,
        "Status": status,
        "RequesterManaged": managed,
        "RequesterId": requester,
        "Description": description,
        "Groups": [{"GroupId": group} for group in groups],
    }
    if status == "in-use":
        interface["Attachment"] = {"AttachmentId": f"attach-{interface_id}", "Status": "attached"}
    return interface


class _Ec2:
    """Answers the two reads the way EC2 filters them, and records each delete."""

    def __init__(self, *, owned_groups, interfaces, delete_error: str | None = None) -> None:
        self.owned_groups = list(owned_groups)
        self.interfaces = list(interfaces)
        self.delete_error = delete_error
        self.deleted: list[str] = []
        self.interface_reads = 0

    def get_paginator(self, operation: str):
        return SimpleNamespace(paginate=lambda **kwargs: self._pages(operation, **kwargs))

    def _pages(self, operation: str, *, Filters):  # noqa: N803 - boto3's keyword
        named = {item["Name"]: set(item["Values"]) for item in Filters}
        if operation == "describe_security_groups":
            assert named == {"tag:anti-demo-run-id": {RUN_ID}}
            return [{"SecurityGroups": [{"GroupId": group} for group in self.owned_groups]}]
        assert operation == "describe_network_interfaces"
        self.interface_reads += 1
        return [
            {
                "NetworkInterfaces": [
                    interface
                    for interface in self.interfaces
                    if named["group-id"] & {group["GroupId"] for group in interface["Groups"]}
                    and interface["Status"] in named["status"]
                ]
            }
        ]

    def delete_network_interface(self, *, NetworkInterfaceId):  # noqa: N803 - boto3's keyword
        if self.delete_error:
            raise ClientError(
                {"Error": {"Code": self.delete_error, "Message": "refused"}},
                "DeleteNetworkInterface",
            )
        self.deleted.append(NetworkInterfaceId)


MANIFEST = SimpleNamespace(run_id=RUN_ID, aws=SimpleNamespace(region=REGION))


def _release(monkeypatch, ec2: _Ec2) -> list[tuple[str, str, str]]:
    def client(name: str):
        assert name == "ec2"
        return ec2

    monkeypatch.setattr(lifecycle, "_aws_session", lambda _manifest: SimpleNamespace(client=client))
    return lifecycle._release_interfaces_left_on_security_groups(MANIFEST)


def test_only_interfaces_aws_left_detached_on_the_installations_own_groups_are_deleted(
    monkeypatch, capsys
) -> None:
    ec2 = _Ec2(
        owned_groups=["sg-round2-rds", "sg-round2-aurora"],
        interfaces=[
            # The live case: an RDS instance's interface, detached, on Round 2's group.
            _interface("eni-rds-left", "sg-round2-rds"),
            # An Aurora writer signs its interface with an AWS account ID, not amazon-rds.
            _interface("eni-aurora-left", "sg-round2-aurora", requester="111122223333"),
            # A database that is still running.
            _interface("eni-in-use", "sg-round2-rds", status="in-use"),
            # Detached, but it also holds a security group that is not this installation's.
            _interface("eni-shared", "sg-round2-rds", "sg-another-team"),
            # Detached, but somebody made it by hand: no AWS service manages it.
            _interface("eni-by-hand", "sg-round2-rds", managed=False, requester="111111111111"),
            # Another installation's, on its own group.
            _interface("eni-elsewhere", "sg-another-team"),
        ],
    )

    assert _release(monkeypatch, ec2) == []

    assert ec2.deleted == ["eni-rds-left", "eni-aurora-left"]
    printed = capsys.readouterr().out
    assert "DELETED network interface eni-rds-left (RDSNetworkInterface)" in printed
    assert "left detached on sg-round2-aurora" in printed


def test_an_interface_the_service_removed_first_is_already_released(monkeypatch) -> None:
    ec2 = _Ec2(
        owned_groups=["sg-round2-rds"],
        interfaces=[_interface("eni-rds-left", "sg-round2-rds")],
        delete_error="InvalidNetworkInterfaceID.NotFound",
    )

    assert _release(monkeypatch, ec2) == []


def test_one_this_principal_may_not_delete_is_reported_not_raised(monkeypatch, capsys) -> None:
    """Raising here would keep the whole fleet billing; the destroy can still remove the rest."""

    ec2 = _Ec2(
        owned_groups=["sg-round2-rds"],
        interfaces=[_interface("eni-rds-left", "sg-round2-rds")],
        delete_error="UnauthorizedOperation",
    )

    undeletable = _release(monkeypatch, ec2)

    assert undeletable == [("eni-rds-left", "RDSNetworkInterface", "sg-round2-rds")]
    assert "WARN  network interface eni-rds-left" in capsys.readouterr().out


def test_any_other_delete_error_is_raised_as_it_came(monkeypatch) -> None:
    ec2 = _Ec2(
        owned_groups=["sg-round2-rds"],
        interfaces=[_interface("eni-rds-left", "sg-round2-rds")],
        delete_error="RequestLimitExceeded",
    )

    with pytest.raises(ClientError):
        _release(monkeypatch, ec2)


def test_an_installation_with_no_security_groups_reads_no_interfaces(monkeypatch) -> None:
    ec2 = _Ec2(owned_groups=[], interfaces=[_interface("eni-elsewhere", "sg-another-team")])

    _release(monkeypatch, ec2)

    assert ec2.interface_reads == 0
    assert ec2.deleted == []


def _destroy(monkeypatch, *, undeletable, destroy_error: Exception | None) -> list[str]:
    calls: list[str] = []

    def release(_manifest):
        calls.append("release")
        return undeletable

    def apply(_manifest, plan):
        calls.append(f"apply {plan}")
        if destroy_error is not None:
            raise destroy_error

    monkeypatch.setattr(lifecycle, "_release_interfaces_left_on_security_groups", release)
    monkeypatch.setattr(lifecycle, "_terraform_apply", apply)
    lifecycle._destroy_after_releasing_interfaces(MANIFEST, "aws-destroy.tfplan")
    return calls


def test_the_destroy_runs_after_the_release_and_a_clean_one_needs_nothing_more(
    monkeypatch,
) -> None:
    assert _destroy(monkeypatch, undeletable=[], destroy_error=None) == [
        "release",
        "apply aws-destroy.tfplan",
    ]


def test_a_destroy_that_stops_on_an_undeletable_interface_names_it_and_its_command(
    monkeypatch,
) -> None:
    terraform = RuntimeError("Error: deleting ENIs using Security Group: 403")

    with pytest.raises(RuntimeError) as stopped:
        _destroy(
            monkeypatch,
            undeletable=[("eni-rds-left", "RDSNetworkInterface", "sg-round2-rds")],
            destroy_error=terraform,
        )

    message = str(stopped.value)
    assert message.startswith("The destroy removed what it could and stopped")
    assert "RDSNetworkInterface left eni-rds-left detached on sg-round2-rds" in message
    assert "ec2:DeleteNetworkInterface" in message
    assert (
        "aws ec2 delete-network-interface --network-interface-id eni-rds-left --region us-west-2"
        in message
    )
    assert stopped.value.__cause__ is terraform


def test_a_destroy_failure_with_nothing_left_behind_is_raised_as_it_came(monkeypatch) -> None:
    terraform = RuntimeError("Error: something else")

    with pytest.raises(RuntimeError) as stopped:
        _destroy(monkeypatch, undeletable=[], destroy_error=terraform)

    assert stopped.value is terraform


def test_an_undeletable_interface_the_destroy_got_past_is_no_failure(monkeypatch) -> None:
    """The service can remove its own interface between the release and the destroy."""

    calls = _destroy(
        monkeypatch,
        undeletable=[("eni-rds-left", "RDSNetworkInterface", "sg-round2-rds")],
        destroy_error=None,
    )

    assert calls == ["release", "apply aws-destroy.tfplan"]


def test_cleanup_destroys_through_the_release_only_after_the_app_is_gone() -> None:
    """After the app, so no bout can be creating a database whose interface isn't attached."""

    source = inspect.getsource(lifecycle.cleanup)
    assert "_terraform_apply(manifest, destroy_plan)" not in source
    app = source.index("_delete_databricks_app(manifest)")
    destroy = source.index("_destroy_after_releasing_interfaces(manifest, destroy_plan)")
    assert app < destroy
