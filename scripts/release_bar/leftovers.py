"""Read-only: per-bout AWS resources an installation's bouts left behind.

    leftovers.py CHECKOUT [--wait SECS]

A bout of Round 2 or 3 makes an RDS clone or restore; a Round 5 bout makes an RDS
Proxy with its own security group, security-group rules and secrets. Each is
tagged with the installation's run id and a `managed-by` other than `terraform`,
and each must be gone once its round is READY again. This lists every resource
carrying the run id that Terraform did not make, across all of those types.
A Round 4 bout starts a run of its competitor's Glue writer instead, which bills
while it lasts, so a run of this installation's writers still active counts too.
A Round 6 bout starts its competitor's DMS task and Glue writer, so a task still
running counts as well as the run.

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
from _common import (  # noqa: E402
    GLUE_LANE_PREFIX,
    counted_when_settled,
    dms_listing,
    installation_run_id,
    load_checkout_aws,
    require_env,
    tags_of,
)

#: A Glue run in any of these has not let go of its workers yet.
_GLUE_ACTIVE_STATES = {"STARTING", "RUNNING", "STOPPING", "WAITING"}
#: A DMS task in any of these is still reading its source.
_DMS_ACTIVE_STATES = {"starting", "running", "stopping", "modifying"}


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
    # Every Proxy in the account is asked in turn, other teams' too, so one deleted
    # between the listing and its tag read is skipped rather than allowed to crash this.
    for page in rds.get_paginator("describe_db_proxies").paginate():
        for item in page["DBProxies"]:
            tags = _tags(
                tags_of(rds.list_tags_for_resource, "TagList", ResourceName=item["DBProxyArn"])
            )
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

    # The writer jobs are Terraform's and stay; their runs are the bouts'. A run a
    # dead app could not stop ends by the job's own 30-minute timeout
    # (infra/aws/round4_glue.tf), which `--wait` gives room for.
    glue = session.client("glue")
    account = session.client("sts").get_caller_identity()["Account"]
    arn = f"arn:aws:glue:{session.region_name}:{account}:job"
    for page in glue.get_paginator("get_jobs").paginate():
        for job in page["Jobs"]:
            name = job["Name"]
            if not name.startswith(GLUE_LANE_PREFIX):
                continue
            tags = glue.get_tags(ResourceArn=f"{arn}/{name}").get("Tags") or {}
            if tags.get("anti-demo-run-id") != run_id:
                continue
            for runs in glue.get_paginator("get_job_runs").paginate(JobName=name):
                for run in runs["JobRuns"]:
                    if run["JobRunState"] in _GLUE_ACTIVE_STATES:
                        found[("glue-job-run", "app", run["JobRunState"])] += 1

    # Round 6's DMS tasks are Terraform's and stay, parked; a task still running is
    # the bout's. DMS lists no tags with its tasks, so each one named like the
    # installation's is asked in turn.
    dms = session.client("dms")
    for task in dms_listing(dms, "describe_replication_tasks", "ReplicationTasks"):
        if not str(task.get("ReplicationTaskIdentifier", "")).startswith(GLUE_LANE_PREFIX):
            continue
        listed = tags_of(
            dms.list_tags_for_resource, "TagList", ResourceArn=task["ReplicationTaskArn"]
        )
        if _tags(listed).get("anti-demo-run-id") != run_id:
            continue
        if task.get("Status") in _DMS_ACTIVE_STATES:
            found[("dms-task", "app", task["Status"])] += 1
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
    region = require_env("AWS_DEFAULT_REGION", f"no installation in {args.checkout} names one")
    import boto3

    session = boto3.Session(region_name=region)
    deadline = time.monotonic() + args.wait
    while True:
        found = counted_when_settled(lambda: leftovers(session, run_id))
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
