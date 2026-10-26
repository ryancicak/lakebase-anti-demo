"""The Databricks side's cloud: what differs between an AWS and an Azure workspace (design § 11.2).

An installation's edition comes from its workspace host, never from a switch (design § 8.1):
an ``azuredatabricks.net`` workspace is the Azure edition. Everything here is a fact about where
Databricks runs, not about the competitor:

* **Databricks' published egress** comes from one feed, filtered by platform. The firewall floor
  is the narrowest prefix the platform publishes: a /24 on AWS (us-west-2's widest), and a /23 on
  Azure, which publishes /23s in every US region (decision 1).
* **The DNS witness:** a host that resolves whenever Databricks' own DNS does. Beside a workspace
  name that doesn't resolve, it says the workspace was removed rather than this machine being
  offline. It's the cloud's account console.

The storage scheme, the App's secret bindings and the Lakebase host pattern join this in Phase 2,
once S-Z1 has read them on an Azure workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit


@dataclass(frozen=True)
class HomeCloud:
    name: Literal["aws", "azure"]
    workspace_host_suffix: str
    egress_feed_platform: str
    egress_min_prefixlen: int
    dns_witness: str


AWS = HomeCloud(name="aws", workspace_host_suffix=".cloud.databricks.com",
                egress_feed_platform="aws", egress_min_prefixlen=24,
                dns_witness="accounts.cloud.databricks.com")
AZURE = HomeCloud(name="azure", workspace_host_suffix=".azuredatabricks.net",
                  egress_feed_platform="azure", egress_min_prefixlen=23,
                  dns_witness="accounts.azuredatabricks.net")
CLOUDS = (AWS, AZURE)

#: A Lakebase endpoint host on either cloud, its region the one group. Azure's is the AWS one's
#: shape under Azure's domain, ``ep-….database.westus2.azuredatabricks.net``. An AWS region
#: never equals an Azure one, so a host is still held to its own region.
LAKEBASE_HOST_PATTERN = (
    r"^[^.]+\.database\.(?P<region>[a-z0-9-]+)\.(?:cloud\.databricks\.com|azuredatabricks\.net)$"
)


def home_cloud_for_host(host: str, *, default: HomeCloud | None = None) -> HomeCloud:
    """The cloud a workspace host belongs to. Refuses a host of neither, unless `default` is given.

    >>> home_cloud_for_host("https://adb-example.0.azuredatabricks.net").name
    'azure'
    """

    hostname = (urlsplit(host if "://" in host else f"https://{host}").hostname or "").lower()
    for cloud in CLOUDS:
        if hostname.endswith(cloud.workspace_host_suffix):
            return cloud
    if default is not None:
        return default
    raise ValueError(f"{hostname or host!r} is not an AWS or Azure Databricks workspace host")
