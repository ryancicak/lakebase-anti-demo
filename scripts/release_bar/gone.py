"""Read-only: after an uninstall, is anything of the installation left in AWS?

    gone.py CHECKOUT [RUN_ID]

Counts every resource still carrying the installation's `anti-demo-run-id` tag:
EC2 instances that are not terminated, volumes, security groups and their rules;
RDS clusters, instances and proxies, and Round 6's logical-replication parameter
groups; SQS queues; secrets not already scheduled for deletion; IAM roles,
instance profiles and customer-managed policies; Round 4's AWS Glue lane: its
subnet, route table and S3 gateway endpoint, its bucket, and its Glue jobs and
connections; and Round 6's AWS DMS lane: its subnets, route table and endpoint,
its bucket, its Glue jobs, and its DMS replication instance, subnet group,
endpoints and tasks. DMS's account-wide `dms-vpc-role` is nobody's installation's
and is not counted. In Databricks, Round 6's Unity Catalog storage credential and
external location, by the names the installer derives from the run id.
The run id defaults to the one in CHECKOUT's newest manifest, or, once
`./antidemo cleanup` has removed that, its `cleanup-receipt.json`.

Prints counts only, then ALL GONE or SOMETHING REMAINS. Uses the AWS key in
CHECKOUT's `.env.bootstrap`, and the Databricks CLI profile the installation
recorded. Exit 0 when all gone, 1 otherwise.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import (  # noqa: E402
    GLUE_LANE_PREFIX,
    counted_when_settled,
    dms_listing,
    installation_databricks_profile,
    installation_run_id,
    load_checkout_aws,
    require_env,
)
from _common import tags_of as _tags_of  # noqa: E402

RUN_ID_TAG = "anti-demo-run-id"


def _carries(tags: list[dict[str, str]] | None, run_id: str) -> bool:
    return any(tag["Key"] == RUN_ID_TAG and tag["Value"] == run_id for tag in tags or [])


def remaining(session, run_id: str) -> dict[str, int]:
    tagged = [{"Name": f"tag:{RUN_ID_TAG}", "Values": [run_id]}]
    ec2 = session.client("ec2")
    alive = {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
    counts = {
        "ec2_instances": sum(
            len(reservation.get("Instances", []))
            for page in ec2.get_paginator("describe_instances").paginate(Filters=[*tagged, alive])
            for reservation in page["Reservations"]
        ),
        "ec2_volumes": sum(
            len(page["Volumes"])
            for page in ec2.get_paginator("describe_volumes").paginate(Filters=tagged)
        ),
        "security_groups": sum(
            len(page["SecurityGroups"])
            for page in ec2.get_paginator("describe_security_groups").paginate(Filters=tagged)
        ),
        "security_group_rules": sum(
            len(page["SecurityGroupRules"])
            for page in ec2.get_paginator("describe_security_group_rules").paginate(Filters=tagged)
        ),
    }

    rds = session.client("rds")
    counts["rds_clusters"] = sum(
        _carries(item.get("TagList"), run_id)
        for page in rds.get_paginator("describe_db_clusters").paginate()
        for item in page["DBClusters"]
    )
    counts["rds_instances"] = sum(
        _carries(item.get("TagList"), run_id)
        for page in rds.get_paginator("describe_db_instances").paginate()
        for item in page["DBInstances"]
    )
    counts["rds_proxies"] = sum(
        _carries(
            _tags_of(rds.list_tags_for_resource, "TagList", ResourceName=item["DBProxyArn"]),
            run_id,
        )
        for page in rds.get_paginator("describe_db_proxies").paginate()
        for item in page["DBProxies"]
    )
    # Round 6's parameter groups (v1.1). Their describes carry no tags, so each one
    # named like the installation's (`infra/aws/parameter_groups.tf`) is asked in turn.
    counts["rds_parameter_groups"] = sum(
        _carries(
            _tags_of(
                rds.list_tags_for_resource,
                "TagList",
                ResourceName=item["DBParameterGroupArn"],
            ),
            run_id,
        )
        for page in rds.get_paginator("describe_db_parameter_groups").paginate()
        for item in page["DBParameterGroups"]
        if item["DBParameterGroupName"].endswith("-rds-lakeflow")
    )
    counts["rds_cluster_parameter_groups"] = sum(
        _carries(
            _tags_of(
                rds.list_tags_for_resource,
                "TagList",
                ResourceName=item["DBClusterParameterGroupArn"],
            ),
            run_id,
        )
        for page in rds.get_paginator("describe_db_cluster_parameter_groups").paginate()
        for item in page["DBClusterParameterGroups"]
        if item["DBClusterParameterGroupName"].endswith("-aurora-lakeflow")
    )

    sqs = session.client("sqs")
    counts["sqs_queues"] = sum(
        (_tags_of(sqs.list_queue_tags, "Tags", QueueUrl=url) or {}).get(RUN_ID_TAG) == run_id
        for page in sqs.get_paginator("list_queues").paginate()
        for url in page.get("QueueUrls", [])
    )

    secrets = session.client("secretsmanager")
    counts["secrets_live"] = sum(
        len(page["SecretList"])
        for page in secrets.get_paginator("list_secrets").paginate(
            Filters=[
                {"Key": "tag-key", "Values": [RUN_ID_TAG]},
                {"Key": "tag-value", "Values": [run_id]},
            ]
        )
    )

    # IAM lists without tags, so each role, profile and local policy is asked in
    # turn. Slow in a busy account, and run once, after an uninstall.
    iam = session.client("iam")
    counts["iam_roles"] = sum(
        _carries(_tags_of(iam.list_role_tags, "Tags", RoleName=role["RoleName"]), run_id)
        for page in iam.get_paginator("list_roles").paginate()
        for role in page["Roles"]
    )
    counts["iam_instance_profiles"] = sum(
        _carries(
            _tags_of(
                iam.list_instance_profile_tags,
                "Tags",
                InstanceProfileName=profile["InstanceProfileName"],
            ),
            run_id,
        )
        for page in iam.get_paginator("list_instance_profiles").paginate()
        for profile in page["InstanceProfiles"]
    )
    counts["iam_policies"] = sum(
        _carries(_tags_of(iam.list_policy_tags, "Tags", PolicyArn=policy["Arn"]), run_id)
        for page in iam.get_paginator("list_policies").paginate(Scope="Local")
        for policy in page["Policies"]
    )

    # Round 4's Glue lane (v1.1). Its network pieces are EC2 and filter on the tag;
    # an endpoint is gone only once AWS says `deleted`, not while it is deleting.
    counts["subnets"] = sum(
        len(page["Subnets"])
        for page in ec2.get_paginator("describe_subnets").paginate(Filters=tagged)
    )
    counts["route_tables"] = sum(
        len(page["RouteTables"])
        for page in ec2.get_paginator("describe_route_tables").paginate(Filters=tagged)
    )
    counts["vpc_endpoints"] = sum(
        str(endpoint.get("State", "")).lower() != "deleted"
        for page in ec2.get_paginator("describe_vpc_endpoints").paginate(Filters=tagged)
        for endpoint in page["VpcEndpoints"]
    )
    # S3 and Glue list without tags, so each lane-named resource is asked in turn. A
    # bucket whose tags cannot be read raises rather than counting as gone.
    s3 = session.client("s3")
    counts["s3_buckets"] = sum(
        _carries(_tags_of(s3.get_bucket_tagging, "TagSet", Bucket=bucket["Name"]), run_id)
        for bucket in s3.list_buckets().get("Buckets", [])
        if bucket["Name"].startswith(GLUE_LANE_PREFIX)
        and bucket["Name"].endswith(("-r4-glue", "-r6-cdc"))
    )
    glue = session.client("glue")
    account = session.client("sts").get_caller_identity()["Account"]
    arn = f"arn:aws:glue:{session.region_name}:{account}"

    def glue_carries(kind: str, name: str) -> bool:
        tags = _tags_of(glue.get_tags, "Tags", ResourceArn=f"{arn}:{kind}/{name}") or {}
        return tags.get(RUN_ID_TAG) == run_id

    counts["glue_jobs"] = sum(
        glue_carries("job", job["Name"])
        for page in glue.get_paginator("get_jobs").paginate()
        for job in page["Jobs"]
        if job["Name"].startswith(GLUE_LANE_PREFIX)
    )
    counts["glue_connections"] = sum(
        glue_carries("connection", connection["Name"])
        for page in glue.get_paginator("get_connections").paginate()
        for connection in page["ConnectionList"]
        if connection["Name"].startswith(GLUE_LANE_PREFIX)
    )

    # Round 6's DMS lane (v1.1). DMS lists no tags, so each resource named like the
    # installation's is asked in turn. A subnet group carries no ARN in its listing.
    dms = session.client("dms")

    def dms_carries(resource_arn: str) -> bool:
        return _carries(
            _tags_of(dms.list_tags_for_resource, "TagList", ResourceArn=resource_arn), run_id
        )

    for key, operation, listing, name_key, arn_key in (
        (
            "dms_replication_instances",
            "describe_replication_instances",
            "ReplicationInstances",
            "ReplicationInstanceIdentifier",
            "ReplicationInstanceArn",
        ),
        ("dms_endpoints", "describe_endpoints", "Endpoints", "EndpointIdentifier", "EndpointArn"),
        (
            "dms_tasks",
            "describe_replication_tasks",
            "ReplicationTasks",
            "ReplicationTaskIdentifier",
            "ReplicationTaskArn",
        ),
    ):
        counts[key] = sum(
            dms_carries(item[arn_key])
            for item in dms_listing(dms, operation, listing)
            if str(item.get(name_key, "")).startswith(GLUE_LANE_PREFIX)
        )
    counts["dms_subnet_groups"] = sum(
        dms_carries(
            f"arn:aws:dms:{session.region_name}:{account}:subgrp:"
            f"{item['ReplicationSubnetGroupIdentifier']}"
        )
        for item in dms_listing(
            dms, "describe_replication_subnet_groups", "ReplicationSubnetGroups"
        )
        if str(item.get("ReplicationSubnetGroupIdentifier", "")).startswith(GLUE_LANE_PREFIX)
    )
    return counts


def unity_catalog_names(run_id: str) -> dict[str, str]:
    """Round 6's Unity Catalog objects, named as `round6_aws_lifecycle.uc_names` names them."""

    suffix = re.sub(r"[^a-z0-9]+", "_", run_id.casefold()).strip("_")
    name = f"anti_demo_r6_aws_{suffix}"
    return {"storage-credentials": name, "external-locations": name}


def unity_catalog_remaining(profile: str, run_id: str, *, run=subprocess.run) -> dict[str, int]:
    """Whether Round 6's storage credential and external location are still in the metastore.

    They bill nothing, but an uninstall that left them would leave a credential trusting a
    deleted role and a location over a deleted bucket. A read that fails for any other reason
    stops the check rather than counting as gone.
    """

    counts: dict[str, int] = {}
    for kind, name in unity_catalog_names(run_id).items():
        result = run(
            ["databricks", "api", "get", f"/api/2.1/unity-catalog/{kind}/{name}", "-p", profile],
            capture_output=True,
            text=True,
            timeout=120,
        )
        text = f"{result.stdout}\n{result.stderr}"
        key = "uc_" + kind.replace("-", "_")
        if result.returncode == 0:
            counts[key] = 1
        elif "RESOURCE_DOES_NOT_EXIST" in text or "does not exist" in text.casefold():
            counts[key] = 0
        else:
            raise SystemExit(f"could not read the {kind} {name}: {text.strip()[:300]}")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("run_id", nargs="?")
    args = parser.parse_args()
    run_id = args.run_id or installation_run_id(args.checkout)
    if not run_id:
        raise SystemExit(f"no run id given and none recorded in {args.checkout}")
    profile = installation_databricks_profile(args.checkout)
    if not profile:
        raise SystemExit(
            f"no Databricks profile recorded in {args.checkout}, so Round 6's Unity Catalog "
            "objects cannot be checked"
        )
    load_checkout_aws(args.checkout)
    region = require_env("AWS_DEFAULT_REGION", f"no installation in {args.checkout} names one")
    import boto3

    session = boto3.Session(region_name=region)
    counts = counted_when_settled(lambda: remaining(session, run_id))
    counts.update(unity_catalog_remaining(profile, run_id))
    for name, count in counts.items():
        print(f"  {name}: {count}")
    gone = not any(counts.values())
    print("ALL GONE" if gone else "SOMETHING REMAINS")
    return 0 if gone else 1


if __name__ == "__main__":
    sys.exit(main())
