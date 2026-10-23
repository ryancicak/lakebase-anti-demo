"""server/azure_manifest.py: the Azure edition's manifest section and what it refuses.

Addresses are documentation space (RFC 5737) or, for the /23 no RFC 5737 block holds, the
benchmarking range (RFC 2544, 198.18.0.0/15); none is globally routable, as the identifier guard
requires. IDs are placeholders.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from server.azure_manifest import AzureManifest
from server.azure_safe_change import config_from_manifest

SUB = "00000000-1111-2222-3333-444444444444"
GROUP = f"/subscriptions/{SUB}/resourceGroups/rg-test/providers/"


def server(name: str, **overrides: Any) -> dict[str, Any]:
    return {"server": name,
            "resource_id": f"{GROUP}Microsoft.DBforPostgreSQL/flexibleServers/{name}",
            "host": f"{name}.postgres.database.azure.com", "admin_login": "antidemo_admin",
            "sku": "B_Standard_B2s", "password_secret": f"{name}-admin-password", **overrides}


def manifest(**overrides: Any) -> dict[str, Any]:
    return {"tenant_id": "aaaaaaaa-0000-0000-0000-000000000000", "subscription_id": SUB,
            "region": "westus2", "resource_group": "rg-test",
            "principal_object_id": "bbbbbbbb-0000-0000-0000-000000000000",
            "operator_cidr": "192.0.2.10/32",
            "databricks_egress_cidrs": ["198.51.100.0/24", "198.18.0.0/23"],
            "databricks_egress_published_at": 1_790_000_000, "storage_account": "stantidemo01",
            "servers": {"r2": server("r2-src"), "r3": server("r3-src")}, **overrides}


def test_a_whole_section_validates_and_builds_round_twos_config() -> None:
    azure = AzureManifest.model_validate(manifest(policy_tags={"cost-center": "demo"},
                                                  lease_date_tag="review-by"))
    config = config_from_manifest(azure, "r2", database="anti_demo")
    assert (config.source_server, config.region, config.admin_login) == (
        "r2-src", "westus2", "antidemo_admin")
    # The operator first, then the egress prefixes, one rule each, as the adapter creates them.
    assert config.firewall_rules == (("operator", "192.0.2.10/32"),
                                     ("databricks-01", "198.51.100.0/24"),
                                     ("databricks-02", "198.18.0.0/23"))
    assert config.policy_tags == (("cost-center", "demo"),) and config.lease_date_tag == "review-by"


@pytest.mark.parametrize("change, refusal", [
    ({"operator_cidr": "192.0.2.0/24"}, "a /32"),
    ({"operator_cidr": "0.0.0.0/32"}, "an address an operator connects from"),
    ({"operator_cidr": "127.0.0.1/32"}, "an address an operator connects from"),
    ({"lease_date_tag": "managed-by"}, "over one of the installation's own tags"),
    ({"databricks_egress_cidrs": ["198.51.100.0/22"]}, "wider than a /23"),
    ({"databricks_egress_published_at": None}, "sealed together"),
    ({"policy_tags": {"Owner": "someone"}}, "redefine the installation's own"),
    ({"policy_tags": {"Service": "demo", "service": "demo"}}, "differ in more than case"),
    ({"lease_date_tag": "review-by", "policy_tags": {"Review-By": "2030-01-01"}},
     "can't also be a fixed policy tag"),
    ({"servers": {"r2": server("r2-src", resource_id=(
        f"/subscriptions/{SUB}/resourceGroups/someone-elses/providers/"
        "Microsoft.DBforPostgreSQL/flexibleServers/r2-src"))}}, "outside the installation"),
    ({"servers": {"r2": server("r2-src", host="r9.postgres.database.azure.com")}}, "not its own"),
    ({"servers": {"r2": server("R2_src")}}, "pattern"),
])
def test_what_the_section_refuses(change: dict[str, Any], refusal: str) -> None:
    with pytest.raises(ValidationError, match=refusal):
        AzureManifest.model_validate(manifest(**change))


def test_the_section_names_secrets_and_holds_none_and_has_no_aws_field() -> None:
    from server.azure_manifest import AzureDataFlowSeal, AzurePostgresEnvironmentSeal

    fields = {name for model in (AzureManifest, AzurePostgresEnvironmentSeal, AzureDataFlowSeal)
              for name in model.model_fields}
    assert {name for name in fields if "password" in name} == {"password_secret"}
    assert not {name for name in fields if name.startswith("aws")}
