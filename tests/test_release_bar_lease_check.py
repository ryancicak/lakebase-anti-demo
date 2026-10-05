"""The lease check's Terraform plan fails on drift the app made, and only on that.

2026-10-05, rc19. The plan after the bar proposed two VPC endpoint updates and an output change,
and the step failed. None of it was the app's: the account's own automation had tagged both
endpoints at 04:00Z, so Terraform proposed dropping a tag it never set, and AWS had listed the
same three subnets in another order, so `subnet_ids` changed without anything in it changing.
"""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

BAR = Path(__file__).resolve().parent.parent / "scripts" / "release_bar"


def _load():
    spec = importlib.util.spec_from_file_location("release_bar_lease_check", BAR / "lease_check.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


lease_check = _load()

OURS = {
    "Name": "lakebase-ant-example-r6-dms-s3",
    "anti-demo-run-id": "ad-test-001",
    "expires-at": "2026-10-08T06:38:31Z",
    "managed-by": "terraform",
}
FOREIGN = {"governance-marker": "2026-10-05_04-00-47"}


def endpoint_update(*, before_tags=None, after_tags=None, before=None, after=None, unknown=None):
    """An `aws_vpc_endpoint` update as `terraform show -json` writes it."""

    had = {**OURS, **FOREIGN} if before_tags is None else before_tags
    keeps = dict(OURS) if after_tags is None else after_tags
    base = {"id": "vpce-0123456789abcdef0", "service_name": "com.amazonaws.us-west-2.s3"}
    return {
        "address": "aws_vpc_endpoint.round6_dms_s3[0]",
        "type": "aws_vpc_endpoint",
        "change": {
            "actions": ["update"],
            "before": {**base, **(before or {}), "tags": had, "tags_all": had},
            "after": {**base, **(after or {}), "tags": keeps, "tags_all": keeps},
            "after_unknown": {"dns_entry": [{}], "tags": {}, "tags_all": {}}
            if unknown is None
            else unknown,
        },
    }


def subnets_reordered():
    return {
        "actions": ["update"],
        "before": [
            "subnet-a",
            "subnet-b",
            "subnet-c",
        ],
        "after": [
            "subnet-b",
            "subnet-c",
            "subnet-a",
        ],
        "after_unknown": False,
    }


def rc19_plan():
    second = endpoint_update()
    second["address"] = "aws_vpc_endpoint.round4_glue_s3[0]"
    return {
        "resource_changes": [
            endpoint_update(),
            second,
            {"address": "aws_db_instance.rds", "change": {"actions": ["no-op"]}},
        ],
        "output_changes": {
            "subnet_ids": subnets_reordered(),
            "vpc_id": {"actions": ["no-op"], "before": "vpc-1", "after": "vpc-1"},
        },
    }


def test_rc19s_plan_is_quiet_once_the_changes_nothing_the_app_does_are_set_aside():
    changes, outputs, set_aside = lease_check.plan_drift(rc19_plan())

    assert changes == []
    assert outputs == []
    assert set_aside == 3


@pytest.mark.parametrize(
    ("label", "change"),
    [
        # The app's own retag missing from AWS, or a value of its own moved: real drift.
        (
            "its own lease changed",
            endpoint_update(after_tags={**OURS, "expires-at": "2026-10-09T00:00:00Z"}),
        ),
        ("its own tag missing", endpoint_update(before_tags=dict(FOREIGN), after_tags=dict(OURS))),
        (
            "its own tag dropped",
            endpoint_update(before_tags=dict(OURS), after_tags={"Name": OURS["Name"]}),
        ),
        # Anything but tags.
        ("another attribute", endpoint_update(before={"policy": "a"}, after={"policy": "b"})),
        ("an unknown value", endpoint_update(unknown={"policy": True, "tags": {}})),
        # Nothing dropped at all is not this case either.
        ("no change in tags", endpoint_update(before_tags=dict(OURS), after_tags=dict(OURS))),
    ],
)
def test_any_other_resource_change_is_still_drift(label, change):
    changes, _, set_aside = lease_check.plan_drift({"resource_changes": [change]})

    assert changes == [(change["address"], ["update"])], label
    assert set_aside == 0


@pytest.mark.parametrize("actions", [["create"], ["delete"], ["delete", "create"]])
def test_only_an_update_can_be_foreign_tags(actions):
    change = copy.deepcopy(endpoint_update())
    change["change"]["actions"] = actions

    changes, _, _ = lease_check.plan_drift({"resource_changes": [change]})

    assert changes == [(change["address"], actions)]


def test_an_output_whose_items_changed_is_still_drift():
    moved = subnets_reordered()
    moved["after"] = [*moved["after"][:2], "subnet-d"]
    unknown = subnets_reordered()
    unknown["after_unknown"] = True

    _, outputs, set_aside = lease_check.plan_drift(
        {"output_changes": {"subnet_ids": moved, "vpc_ids": unknown}}
    )

    assert outputs == ["subnet_ids", "vpc_ids"]
    assert set_aside == 0
