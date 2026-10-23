"""server/azure_lease.py against a fake ARM client: only Terraform-made resources of this run
move, never backwards, with the subscription's date tag beside `expires-at` when one is named."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from server.azure_arm import ArmError, Response
from server.azure_lease import (
    AzureLeaseScope,
    discover_azure_lease_targets,
    renew_azure_lease,
)

RUN = "ad-test-001"
RG = "/subscriptions/s/resourceGroups/rg"
NOW = datetime(2026, 10, 19, 7, 0, tzinfo=UTC)


def resource(name: str, kind: str, **tags: str) -> dict[str, Any]:
    return {"id": f"{RG}/providers/{kind}/{name}", "name": name, "type": kind, "tags": tags}


OURS = {"anti-demo-run-id": RUN, "managed-by": "terraform"}


class FakeArm:
    resource_group_path = RG

    def __init__(self, resources: list[dict[str, Any]], *, refuse: set[str] | None = None,
                 gone: set[str] | None = None) -> None:
        self.resources = resources
        self.refuse = refuse or set()
        self.gone = gone or set()
        self.merged: dict[str, dict[str, str]] = {}

    def request(self, method: str, url: str, body: Any = None) -> Response:
        assert method == "GET" and "/resources?" in url
        return Response(200, {"value": self.resources}, {})

    def merge_tags(self, resource_id: str, tags: dict[str, str]) -> None:
        name = resource_id.rsplit("/", 1)[-1]
        if name in self.gone:
            raise ArmError("gone", status=404, code="ResourceNotFound")
        if name in self.refuse:
            raise ArmError("refused", status=409, code="Conflict")
        self.merged[name] = dict(tags)


def test_only_this_runs_terraform_made_resources_are_lease_targets() -> None:
    arm = FakeArm([
        resource("r4", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS,
                 **{"expires-at": "2026-10-20T00:00:00Z"}),
        # Round 2's copy: this run's, but sealed by its adapter, not Terraform's.
        resource("adsc-ad-test-001-azure", "Microsoft.DBforPostgreSQL/flexibleServers",
                 **{"anti-demo-run-id": RUN, "managed-by": "lakebase-anti-demo-round-2"}),
        resource("someone-elses", "Microsoft.Storage/storageAccounts",
                 **{"anti-demo-run-id": "ad-other-002", "managed-by": "terraform"}),
        resource("policy-attached", "Microsoft.Network/dnsResolverPolicies"),
    ])
    inventory = discover_azure_lease_targets(arm, AzureLeaseScope(RUN))  # type: ignore[arg-type]
    assert [target.identifier.rsplit("/", 1)[-1] for target in inventory.targets] == ["r4"]
    assert inventory.targets[0].lease == datetime(2026, 10, 20, tzinfo=UTC)


def test_renewal_moves_expires_at_and_the_date_tag_forward_only() -> None:
    arm = FakeArm([
        resource("r2", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS,
                 **{"expires-at": "2026-10-20T00:00:00Z", "Review-By": "2026-10-20"}),
        # Its date tag was moved further out by hand; the lease must not pull it back.
        resource("factory", "Microsoft.DataFactory/factories", **OURS,
                 **{"expires-at": "2026-10-20T00:00:00Z", "review-by": "2027-01-01"}),
        # Already past the target: untouched.
        resource("vault", "Microsoft.KeyVault/vaults", **OURS,
                 **{"expires-at": "2026-10-30T00:00:00Z"}),
    ])
    scope = AzureLeaseScope(RUN, lease_date_tag="review-by")
    renewal = renew_azure_lease(arm, scope, NOW + timedelta(hours=72))  # type: ignore[arg-type]
    assert arm.merged == {
        "r2": {"expires-at": "2026-10-22T07:00:00Z", "review-by": "2026-10-22"},
        "factory": {"expires-at": "2026-10-22T07:00:00Z"},
    }
    assert (renewal.renewed, renewal.resources, renewal.complete) == (2, 3, True)
    assert renewal.earliest == datetime(2026, 10, 22, 7, 0, tzinfo=UTC)


def test_a_factorys_rewritten_lease_still_reads_as_its_lease() -> None:
    # Phase 0: a factory handed "2026-10-22T08:05:18Z" back as "10/22/2026 08:05:18".
    arm = FakeArm([
        resource("factory", "Microsoft.DataFactory/factories", **OURS,
                 **{"expires-at": "10/22/2026 08:05:18"}),
        resource("r2", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS,
                 **{"expires-at": "2026-10-22T08:05:18Z"}),
    ])
    inventory = discover_azure_lease_targets(arm, AzureLeaseScope(RUN))  # type: ignore[arg-type]
    assert {target.lease for target in inventory.targets} == {
        datetime(2026, 10, 22, 8, 5, 18, tzinfo=UTC)}
    # So a renewal to the same instant moves nothing, rather than retagging it every pass.
    renewal = renew_azure_lease(arm, AzureLeaseScope(RUN), datetime(2026, 10, 22, 8, 5, 18,  # type: ignore[arg-type]
                                                                  tzinfo=UTC), inventory=inventory)
    assert renewal.renewed == 0 and arm.merged == {}


def test_a_round_two_copy_wearing_its_sources_tags_is_not_a_lease_target() -> None:
    # A restore copies the source's tags, `managed-by = terraform` included, and the copy's own
    # merge shows ~20 min late (adversarial review, 2026-10-19).
    arm = FakeArm([
        resource("r2", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS),
        resource("adsc-ad-test-001-azure", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS),
        resource("adrc-ad-test-001-azure", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS),
    ])
    inventory = discover_azure_lease_targets(arm, AzureLeaseScope(RUN))  # type: ignore[arg-type]
    assert [t.identifier.rsplit("/", 1)[-1] for t in inventory.targets] == ["r2"]


def test_a_date_tag_in_a_form_not_ours_is_left_alone_and_a_us_date_is_read() -> None:
    arm = FakeArm([
        resource("a", "Microsoft.Storage/storageAccounts", **OURS, **{"review-by": "someday"}),
        resource("b", "Microsoft.Storage/storageAccounts", **OURS, **{"review-by": "10/26/2026"}),
        resource("c", "Microsoft.Storage/storageAccounts", **OURS, **{"review-by": "10/20/2026"}),
    ])
    renew_azure_lease(arm, AzureLeaseScope(RUN, lease_date_tag="review-by"),  # type: ignore[arg-type]
                      NOW + timedelta(hours=72))
    assert "review-by" not in arm.merged["a"]  # someone's own value, untouched
    assert "review-by" not in arm.merged["b"]  # 2026-10-26 is past the lease's 10-22
    assert arm.merged["c"]["review-by"] == "2026-10-22"


def test_without_a_date_tag_only_expires_at_is_written() -> None:
    arm = FakeArm([resource("r2", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS)])
    renew_azure_lease(arm, AzureLeaseScope(RUN), NOW)  # type: ignore[arg-type]
    assert arm.merged == {"r2": {"expires-at": "2026-10-19T07:00:00Z"}}


def test_a_refusal_is_collected_and_a_vanished_resource_is_not_a_failure() -> None:
    arm = FakeArm([
        resource("r2", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS),
        resource("r3", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS),
        resource("r4", "Microsoft.DBforPostgreSQL/flexibleServers", **OURS),
    ], refuse={"r3"}, gone={"r4"})
    renewal = renew_azure_lease(arm, AzureLeaseScope(RUN), NOW)  # type: ignore[arg-type]
    assert (renewal.renewed, renewal.vanished) == (1, 1)
    assert renewal.failed == ("Microsoft.DBforPostgreSQL/flexibleServers Conflict",)
    assert not renewal.complete
    assert renewal.earliest is None  # r3 kept no readable lease


def test_a_group_that_cannot_be_listed_is_reported_not_passed_off_as_empty() -> None:
    class Unlistable(FakeArm):
        def request(self, method: str, url: str, body: Any = None) -> Response:
            return Response(403, {"error": {"code": "AuthorizationFailed"}}, {})

    inventory = discover_azure_lease_targets(Unlistable([]), AzureLeaseScope(RUN))  # type: ignore[arg-type]
    assert inventory.targets == () and inventory.unreadable == ("resource group: 403",)
