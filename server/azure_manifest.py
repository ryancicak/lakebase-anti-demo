"""The Azure edition's manifest section (design section 9), before `DemoManifest` carries it.

Manifest v8 seals ``cloud`` (``aws`` or ``azure``) at an installation's first provision; ``aws`` is
required on AWS and absent on Azure, and ``azure`` the reverse. This is the Azure half: everything
an Azure installation's lanes are built from, and nothing they could build an AWS client with. It
is not wired into `DemoManifest` yet, so the AWS edition's manifest is unchanged. Phase 2 adds
``cloud`` and this section side by side, with v7 manifests reading as ``cloud = aws``.

Rules carried over from the AWS sections, and the two that differ:

* **Nothing secret.** A server's admin password is named by its secret-scope key, never held.
* **The operator is one address** (a /32), as on AWS.
* **The egress floor is a /23, for Azure rows only** (design decision 1). Databricks' feed
  publishes a /23 in every US Azure region, and AWS's /24 floor would refuse the vendor's own list.
* **Every resource is the installation's own:** in its subscription and resource group, named in
  the shape Azure allows (Flexible Server names are lowercase letters, digits and hyphens).
* **The subscription's tag policy is a setting** (`policy_tags`, `lease_date_tag`). Nothing here
  knows any particular subscription's rules.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

#: The narrowest published prefix an Azure installation admits to a server's firewall.
AZURE_EGRESS_MIN_PREFIXLEN = 23
#: Tags that are the installation's own; a policy tag can't redefine one.
OWN_TAGS = frozenset({"anti-demo-run-id", "owner", "managed-by", "expires-at"})

_GUID = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_SERVER = r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$"
_TAG_NAME = r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$"
#: A Unity Catalog name as the installer makes them, `anti_demo_<run>_<purpose>`.
_UC_NAME = r"^[a-z][a-z0-9_]{0,127}$"

RoundKey = Literal["r2", "r3", "r4", "r5", "r6"]


class AzurePostgresEnvironmentSeal(BaseModel):
    """One round's Azure Database for PostgreSQL server, as Terraform made it."""

    server: str = Field(pattern=_SERVER)
    resource_id: str = Field(min_length=1)
    host: str = Field(min_length=1, max_length=253)
    port: int = 5432
    admin_login: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    sku: str = Field(pattern=r"^(B|GP|MO)_Standard_[A-Za-z0-9_]+$")
    #: The secret-scope key that holds the admin password; the password itself is never sealed.
    password_secret: str = Field(min_length=1)

    @model_validator(mode="after")
    def require_the_names_to_agree(self) -> AzurePostgresEnvironmentSeal:
        if not self.resource_id.lower().endswith(
                f"/providers/microsoft.dbforpostgresql/flexibleservers/{self.server}"):
            raise ValueError(f"{self.server}'s resource ID names another server")
        if self.host.lower() != f"{self.server}.postgres.database.azure.com":
            raise ValueError(f"{self.server}'s host is not its own")
        return self


class AzureDataFlowSeal(BaseModel):
    """Round 4's or Round 6's standing ADF pipeline and the private route it runs over."""

    factory: str = Field(pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9-]{1,61}[A-Za-z0-9])$")
    pipeline: str = Field(min_length=1)
    data_flow: str = Field(min_length=1)
    #: The managed-VNet integration runtime. ADF allows letters, digits and hyphens only.
    runtime: str = Field(pattern=r"^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$")
    #: The factory's managed private endpoints: to the round's server, and to the ADLS account.
    server_endpoint: str = Field(min_length=1)
    storage_endpoint: str = Field(min_length=1)
    container: str = Field(pattern=r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$")
    folder: str = Field(min_length=1)


class AzureUnityCatalogSeal(BaseModel):
    """What the installation made in Unity Catalog on its own ADLS, and the App's identity there.

    The storage credential is Unity Catalog's Access Connector, and each container has an external
    location on it. The catalog's managed location is one of them, so every table the installation
    makes lives in its own account, where Data Factory can reach it. The App's service credential
    is on the App's own connector: it is how the App reaches ARM without a secret.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    storage_credential: str = Field(pattern=_UC_NAME)
    #: Each container's external location, by container.
    external_locations: dict[str, str]
    catalog: str = Field(pattern=_UC_NAME)
    app_service_credential: str = Field(pattern=_UC_NAME)


class AzureManifest(BaseModel):
    tenant_id: str = Field(pattern=_GUID)
    subscription_id: str = Field(pattern=_GUID)
    #: Azure's programmatic region name, e.g. ``westus2``.
    region: str = Field(pattern=r"^[a-z0-9]+$")
    resource_group: str = Field(min_length=1, max_length=90)
    #: The installer's principal, sealed at first provision so a later run as anyone else stops.
    principal_object_id: str = Field(pattern=_GUID)
    operator_cidr: str
    databricks_egress_cidrs: tuple[str, ...] | None = None
    databricks_egress_published_at: int | None = None
    storage_account: str = Field(pattern=r"^[a-z0-9]{3,24}$")
    policy_tags: dict[str, str] = Field(default_factory=dict)
    lease_date_tag: str | None = Field(default=None, pattern=_TAG_NAME)
    servers: dict[RoundKey, AzurePostgresEnvironmentSeal]
    round4: AzureDataFlowSeal | None = None
    round6: AzureDataFlowSeal | None = None
    unity_catalog: AzureUnityCatalogSeal | None = None

    @field_validator("operator_cidr")
    @classmethod
    def require_one_operator_address(cls, value: str) -> str:
        network = ipaddress.ip_network(value, strict=True)
        if network.version != 4 or network.prefixlen != 32:
            raise ValueError("operator_cidr must be one IPv4 address, a /32")
        # On Flexible Server a 0.0.0.0 rule is the "allow every Azure service" switch, which
        # admits other customers' resources; and no operator connects from these.
        if (network.is_unspecified or network.is_loopback or network.is_multicast
                or network.is_link_local):
            raise ValueError(f"operator_cidr must be an address an operator connects from; "
                             f"got {value!r}")
        return value

    @field_validator("databricks_egress_cidrs")
    @classmethod
    def require_published_azure_egress(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("databricks_egress_cidrs cannot be empty")
        if len(set(value)) != len(value):
            raise ValueError("databricks_egress_cidrs must be distinct")
        for entry in value:
            network = ipaddress.ip_network(entry, strict=True)
            if network.version != 4:
                raise ValueError(f"databricks_egress_cidrs must be IPv4; got {entry!r}")
            if network.prefixlen < AZURE_EGRESS_MIN_PREFIXLEN:
                raise ValueError(f"databricks_egress_cidrs refuses anything wider than a "
                                 f"/{AZURE_EGRESS_MIN_PREFIXLEN}; got {entry!r}")
            if (network.is_loopback or network.is_multicast or network.is_link_local
                    or network.is_unspecified):
                raise ValueError(f"databricks_egress_cidrs entries must be addresses something "
                                 f"can egress from; got {entry!r}")
        return value

    @field_validator("policy_tags")
    @classmethod
    def require_policy_tags_leave_the_installations_alone(
        cls, value: dict[str, str]
    ) -> dict[str, str]:
        for name in value:
            if re.fullmatch(_TAG_NAME, name) is None:
                raise ValueError(f"policy tag {name!r} is not a plain tag name")
            if name.lower() in OWN_TAGS:
                raise ValueError(f"policy tag {name!r} would redefine the installation's own")
        # Azure reads tag names in any case, and refuses a request that names one twice.
        if len({name.lower() for name in value}) != len(value):
            raise ValueError("policy tag names must differ in more than case")
        return value

    @model_validator(mode="after")
    def require_the_egress_seal_is_whole(self) -> AzureManifest:
        if (self.databricks_egress_cidrs is None) != (self.databricks_egress_published_at is None):
            raise ValueError("databricks_egress_cidrs and databricks_egress_published_at are "
                             "sealed together or not at all")
        return self

    @model_validator(mode="after")
    def require_every_server_is_the_installations(self) -> AzureManifest:
        group = (f"/subscriptions/{self.subscription_id}/resourcegroups/"
                 f"{self.resource_group}/providers/").lower()
        for key, seal in self.servers.items():
            if not seal.resource_id.lower().startswith(group):
                raise ValueError(f"the {key} server is outside the installation's subscription "
                                 "and resource group")
        fixed = {name.lower() for name in self.policy_tags}
        if self.lease_date_tag and self.lease_date_tag.lower() in fixed:
            raise ValueError("lease_date_tag is the lease's; it can't also be a fixed policy tag")
        if self.lease_date_tag and self.lease_date_tag.lower() in OWN_TAGS:
            raise ValueError("lease_date_tag would write a date over one of the installation's "
                             "own tags")
        return self

    def firewall_rules(self) -> tuple[tuple[str, str], ...]:
        """A copy's rules, in the order the adapter creates them: the operator, then egress."""

        egress = self.databricks_egress_cidrs or ()
        return (("operator", self.operator_cidr),
                *((f"databricks-{index:02d}", cidr) for index, cidr in enumerate(egress, 1)))
