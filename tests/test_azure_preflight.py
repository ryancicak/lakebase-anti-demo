"""server/azure_preflight.py against scripted ARM answers, including the exact permissions a
Contributor-only resource group returned in Phase 0 (no role assignments)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from server.azure_arm import Response
from server.azure_preflight import (
    ROLE_ASSIGNMENT_WRITE,
    RUNNER_FAMILY,
    RUNNER_VCPUS,
    action_allowed,
    check_quota,
    check_resource_group_tags,
    check_role_assignments,
)
from server.capacity import AZURE_ROUND5_RUNNER_SIZE
from tests.test_azure_arm import TOKEN, client

CONTRIBUTOR_AS_SEEN = [
    {"actions": ["*"], "notActions": [
        "Microsoft.Authorization/*/Delete", "Microsoft.Authorization/*/Write",
        "Microsoft.Authorization/elevateAccess/Action"]},
    {"actions": ["*/read", "Microsoft.PolicyInsights/policyEvents/queryResults/action"],
     "notActions": []},
]


def test_contributor_as_seen_tonight_cannot_assign_roles() -> None:
    assert not action_allowed(CONTRIBUTOR_AS_SEEN, ROLE_ASSIGNMENT_WRITE)
    assert action_allowed(CONTRIBUTOR_AS_SEEN, "Microsoft.DBforPostgreSQL/flexibleServers/write")


def test_a_second_grant_that_allows_role_assignments_wins_the_union() -> None:
    granted = CONTRIBUTOR_AS_SEEN + [{"actions": ["Microsoft.Authorization/roleAssignments/*"],
                                      "notActions": []}]
    assert action_allowed(granted, ROLE_ASSIGNMENT_WRITE)


def test_the_role_check_names_its_remedy() -> None:
    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/Microsoft.Authorization/permissions?",
         Response(200, {"value": CONTRIBUTOR_AS_SEEN}, {})),
    ])
    finding = check_role_assignments(arm)
    assert not finding.ok
    assert "Role Based Access Control Administrator" in (finding.remedy or "")


def test_group_tags_are_the_subscriptions_policy_and_a_date_tag_must_be_ahead() -> None:
    def group(tags: dict[str, str]):
        return client([("POST", "/oauth2/v2.0/token", TOKEN),
                       ("GET", "/resourceGroups/rg-test?", Response(200, {"tags": tags}, {}))])[0]

    def check(tags: dict[str, str]):
        return check_resource_group_tags(group(tags), required=("Owner",), date_tag="review-by",
                                         today=date(2026, 10, 19))

    assert check({"owner": "o@x", "Review-By": "2027-04-18"}).ok  # names compare without case
    missing = check({"Owner": "o@x"})
    assert not missing.ok and "review-by" in (missing.remedy or "")
    assert not check({"Owner": "o@x", "review-by": "2026-10-01"}).ok
    assert not check({"Owner": "o@x", "review-by": "next week"}).ok


def test_with_no_policy_tags_configured_the_group_is_not_read() -> None:
    arm, transport = client([("POST", "/oauth2/v2.0/token", TOKEN)])
    assert check_resource_group_tags(arm).ok
    assert [url for _, url, _ in transport.calls if "oauth2" not in url] == []


def test_the_runner_quota_is_the_family_of_the_size_terraform_creates() -> None:
    # One size, named once: Terraform's default, the quota checked, and the receipt's line.
    hcl = (Path(__file__).resolve().parents[1] / "infra/azure/variables.tf").read_text(
        encoding="utf-8")
    block = re.search(r'variable "runner_vm_sku" \{(.*?)\n\}', hcl, re.S)
    assert block is not None
    assert re.search(r'default\s*=\s*"([^"]+)"', block.group(1)).group(1) == (  # type: ignore[union-attr]
        AZURE_ROUND5_RUNNER_SIZE)
    shape = re.fullmatch(r"Standard_D(\d+)ls_v(\d+)", AZURE_ROUND5_RUNNER_SIZE)
    assert shape is not None
    assert RUNNER_VCPUS == 2 * int(shape.group(1))
    assert RUNNER_FAMILY == f"StandardDlsv{shape.group(2)}Family"  # as ARM's usages name it


def test_quota_reports_what_is_free_against_what_is_needed() -> None:
    # The name in the case ARM reported it on 2026-10-20; the match ignores case.
    usage = Response(200, {"value": [{"name": {"value": "StandardDlsv7Family"},
                                      "currentValue": 340, "limit": 350}]}, {})
    cores = Response(200, {"value": [{"name": {"value": "cores"}, "currentValue": 49,
                                      "limit": 1536}]}, {})
    storage = Response(200, {"value": [{"name": {"value": "StorageAccounts"},
                                        "currentValue": 234, "limit": 250}]}, {})
    arm, _ = client([("POST", "/oauth2/v2.0/token", TOKEN),
                     ("GET", "/Microsoft.Compute/locations/westus2/usages?", usage),
                     ("GET", "/flexibleServers/usages?", cores),
                     ("GET", "/Microsoft.Storage/locations/westus2/usages?", storage)])
    runners, servers, accounts = check_quota(arm, "westus2")
    assert not runners.ok and "6 more" in (runners.remedy or "")
    assert servers.ok and accounts.ok
