"""Read-only: per-bout AWS resources an installation's bouts left behind.

    leftovers.py CHECKOUT [--wait SECS]

A bout of Round 2 or 3 makes an RDS clone or restore; a Round 5 bout makes an RDS
Proxy with its own security group, security-group rules and secrets. Each is
tagged with the installation's run id and a `managed-by` other than `terraform`,
and each must be gone once its round is READY again. This lists every resource
carrying the run id that Terraform did not make, across all of those types.

With `--wait`, it polls every 30 s for up to SECS while AWS finishes deletions
that were already under way. Prints counts by type, owner and status, never an
identifier. Uses the AWS key in CHECKOUT's `.env.bootstrap`. Exit 0 when nothing
remains, 1 when something does.
"""

from __future__ import annotations

import argparse
import collections
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import installation_run_id, load_checkout_aws  # noqa: E402


def _tags(values: list[dict[str, str]] | None) -> dict[str, str]:
    return {tag["Key"]: tag["Value"] for tag in values or []}


def leftovers(session, run_id: str) -> collections.Counter:
    """(type, managed-by, status) for every non-Terraform resource with the run id."""
    found: collections.Counter = collections.Counter()

    def per_bout(tags: dict[str, str]) -> bool:
        return tags.get("anti-demo-run-id") == run_id and tags.get("managed-by") != "terraform"

    rds = session.client("rds")
    for page in rds.get_paginator("describe_db_clusters").paginate():
        for item in page["DBClusters"]:
            tags = _tags(item.get("TagList"))
            if per_bout(tags):
                found[("rds-cluster", tags.get("managed-by"), item["Status"])] += 1
    for page in rds.get_paginator("describe_db_instances").paginate():
        for item in page["DBInstances"]:
            tags = _tags(item.get("TagList"))
            if per_bout(tags):
                found[("rds-instance", tags.get("managed-by"), item["DBInstanceStatus"])] += 1
    for page in rds.get_paginator("describe_db_proxies").paginate():
        for item in page["DBProxies"]:
            listed = rds.list_tags_for_resource(ResourceName=item["DBProxyArn"])
            tags = _tags(listed.get("TagList"))
            if per_bout(tags):
                found[("rds-proxy", tags.get("managed-by"), item["Status"])] += 1

    ec2 = session.client("ec2")
    tagged = [{"Name": "tag:anti-demo-run-id", "Values": [run_id]}]
    for page in ec2.get_paginator("describe_security_groups").paginate(Filters=tagged):
        for item in page["SecurityGroups"]:
            tags = _tags(item.get("Tags"))
            if per_bout(tags):
                found[("security-group", tags.get("managed-by"), "present")] += 1
    # Round 5 also opens rules on Terraform's own groups, tagged as its own.
    for page in ec2.get_paginator("describe_security_group_rules").paginate(Filters=tagged):
        for item in page["SecurityGroupRules"]:
            tags = _tags(item.get("Tags"))
            if per_bout(tags):
                found[("security-group-rule", tags.get("managed-by"), "present")] += 1

    # A secret scheduled for deletion is already on its way out and is not listed.
    secrets = session.client("secretsmanager")
    for page in secrets.get_paginator("list_secrets").paginate(
        Filters=[
            {"Key": "tag-key", "Values": ["anti-demo-run-id"]},
            {"Key": "tag-value", "Values": [run_id]},
        ]
    ):
        for item in page["SecretList"]:
            tags = _tags(item.get("Tags"))
            if per_bout(tags):
                found[("secret", tags.get("managed-by"), "present")] += 1
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("--wait", type=float, default=0.0, metavar="SECS")
    args = parser.parse_args()
    run_id = installation_run_id(args.checkout)
    if run_id is None:
        raise SystemExit(f"no installation found in {args.checkout}")
    load_checkout_aws(args.checkout)
    import boto3

    session = boto3.Session(region_name=os.environ["AWS_DEFAULT_REGION"])
    deadline = time.monotonic() + args.wait
    while True:
        found = leftovers(session, run_id)
        if not found:
            print("no per-bout resources remain")
            return 0
        if time.monotonic() >= deadline:
            print("per-bout resources remain:")
            for (kind, owner, status), count in sorted(found.items()):
                print(f"  {count} x {kind} managed-by={owner} status={status}")
            return 1
        print(f"waiting on {sum(found.values())} per-bout resources: {dict(found)}", flush=True)
        time.sleep(30)


if __name__ == "__main__":
    sys.exit(main())
