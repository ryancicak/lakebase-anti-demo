"""A free /24 for an AWS lane's own subnets, chosen from what the VPC actually holds.

The AWS lanes of Rounds 4 and 6 each put their network interfaces in a subnet of their own, in the
installation's VPC, above the default subnets. Terraform used to take that /24 from a digest of the
installation ID, which cannot know what else the VPC holds. On the v1.1 test installation of
2026-09-29 the digest chose 172.31.122.0/24 in a shared account's default VPC that also holds a
foreign 172.31.112.0/20, and the lane's apply failed on the overlap: a collision the digest could
not avoid and would have repeated on every retry.

So the installer chooses before the lane's first apply, from the VPC's own subnets, and records the
choice in the manifest, where every later plan reads it. The digest's /24 is still the first
choice, so an installation whose digest was free gets the /24 it always would have.
"""

from __future__ import annotations

import hashlib
import ipaddress
from collections.abc import Iterable, Sequence
from typing import Any

#: The /24s a lane may take: above the first /18, which the default subnets fill (at most four
#: /20s), and below the last /24. Matches `infra/aws/round4_glue.tf`.
FIRST_LANE_NETNUM = 64
LANE_NETNUMS = 191


class NoFreeLaneSubnet(RuntimeError):
    """Every /24 a lane may take overlaps something already in the VPC."""


def round4_preferred_netnum(installation_id: str) -> int:
    """The /24 Terraform's digest chose for Round 4's Glue lane (`round4_glue.tf`)."""

    digest = hashlib.sha256(f"{installation_id.strip()}:r4-glue".encode()).hexdigest()
    return FIRST_LANE_NETNUM + int(digest[:2], 16) % LANE_NETNUMS


def round6_preferred_netnum(installation_id: str) -> int:
    """The /24 Terraform's digest chose for Round 6's DMS lane (`round6_aws.tf`)."""

    round4 = round4_preferred_netnum(installation_id)
    digest = hashlib.sha256(f"{installation_id.strip()}:r6-dms".encode()).hexdigest()
    step = 1 + int(digest[:2], 16) % (LANE_NETNUMS - 1)
    return FIRST_LANE_NETNUM + (round4 - FIRST_LANE_NETNUM + step) % LANE_NETNUMS


def vpc_subnet_cidrs(ec2: Any, vpc_id: str) -> list[str]:
    """Every subnet CIDR in the VPC, whoever made it."""

    cidrs: list[str] = []
    for page in ec2.get_paginator("describe_subnets").paginate(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    ):
        cidrs.extend(str(subnet["CidrBlock"]) for subnet in page.get("Subnets") or [])
    return cidrs


def free_lane_cidr(
    vpc_cidr: str,
    taken: Iterable[str],
    *,
    preferred_netnum: int,
    avoid: Sequence[str] = (),
) -> str:
    """The first /24 a lane may take that overlaps nothing taken and nothing to avoid.

    Tries ``preferred_netnum`` first, then every other lane /24 in order after it, wrapping, so
    the answer is deterministic for one VPC's contents.
    """

    network = ipaddress.ip_network(vpc_cidr)
    blocked = [ipaddress.ip_network(cidr) for cidr in (*taken, *avoid)]
    if network.prefixlen <= 16:
        # A default-sized VPC: the lane band above the default subnets, as Terraform's digest.
        start = preferred_netnum - FIRST_LANE_NETNUM
        candidates = [
            ipaddress.ip_network(
                (
                    int(network.network_address)
                    + ((FIRST_LANE_NETNUM + (start + offset) % LANE_NETNUMS) << 8),
                    24,
                )
            )
            for offset in range(LANE_NETNUMS)
        ]
    elif network.prefixlen <= 24:
        # A smaller, explicit VPC has no default layout to stay above: any /24 of its own.
        candidates = list(network.subnets(new_prefix=24))
    else:
        candidates = []
    for candidate in candidates:
        if not any(candidate.overlaps(other) for other in blocked):
            return str(candidate)
    raise NoFreeLaneSubnet(
        f"every /24 a lane may take in {vpc_cidr} overlaps a subnet already in the VPC"
    )
