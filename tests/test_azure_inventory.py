"""server/azure_inventory.py: an installation's leftovers by tag and by copy name; whatever else
the group holds is not the installation's."""

from __future__ import annotations

from server.azure_inventory import all_gone, classify, copy_names

RUN = "ad-test-001"
SERVER = "Microsoft.DBforPostgreSQL/flexibleServers"


def test_copy_names_are_the_round_two_and_three_deterministic_ids() -> None:
    assert copy_names(RUN) == {"adsc-ad-test-001-azure", "adrc-ad-test-001-azure"}


def test_an_untagged_copy_is_found_by_its_name() -> None:
    # A restore gives the copy the source's tags, so a crash before the merge leaves only this.
    standing = classify([{"name": "adsc-ad-test-001-azure", "type": SERVER,
                          "tags": {"Owner": "o@x", "spike": "S-Z2"}}], RUN)
    assert [(item.name, item.reason) for item in standing] == [
        ("adsc-ad-test-001-azure", "copy name")]
    assert not all_gone(standing)


def test_another_runs_resources_and_strangers_are_not_ours() -> None:
    standing = classify([
        {"name": "adsc-ad-other-002-azure", "type": SERVER, "tags": {}},
        {"name": "r2", "type": SERVER, "tags": {"anti-demo-run-id": "ad-other-002"}},
        {"name": "someone-elses-vm", "type": "Microsoft.Compute/virtualMachines", "tags": {}},
    ], RUN)
    assert standing == []


def test_a_copy_still_being_restored_is_found_from_the_provider_list() -> None:
    # The group's generic list omits a server in Provisioning; the provider's list has it.
    from server.azure_arm import Response
    from server.azure_inventory import inventory
    from tests.test_azure_arm import TOKEN, client

    arm, _ = client([
        ("POST", "/oauth2/v2.0/token", TOKEN),
        ("GET", "/resources?", Response(200, {"value": []}, {})),
        ("GET", "/flexibleServers?", Response(200, {"value": [
            {"name": "adsc-ad-test-001-azure", "tags": {"Owner": "o@x"},
             "properties": {"state": "Provisioning"}}]}, {})),
    ])
    standing = inventory(arm, RUN)
    assert [(item.name, item.reason) for item in standing] == [
        ("adsc-ad-test-001-azure", "copy name")]
    assert not all_gone(standing)


def test_tagged_resources_are_found_and_what_a_policy_attached_is_not_ours() -> None:
    # A subscription's policy can attach resources of its own beside the installation's VNet,
    # without the run's tags. They don't hold up ALL GONE, and cleanup leaves them alone.
    standing = classify([
        {"name": "vnet", "type": "Microsoft.Network/virtualNetworks",
         "tags": {"Anti-Demo-Run-Id": RUN}},
        {"name": "subscription-dns-policy", "type": "Microsoft.Network/dnsResolverPolicies",
         "tags": {"Owner": "o@x"}},
        {"name": "subscription-dns-policy/link", "tags": {},
         "type": "Microsoft.Network/dnsResolverPolicies/virtualNetworkLinks"},
    ], RUN)
    assert [(item.name, item.reason) for item in standing] == [("vnet", "run tag")]
    assert not all_gone(standing)
    assert all_gone(classify([
        {"name": "subscription-dns-policy", "type": "Microsoft.Network/dnsResolverPolicies",
         "tags": {}}], RUN))
