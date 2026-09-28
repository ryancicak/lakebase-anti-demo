"""Read-only: after an uninstall, is anything of the installation left in AWS?

    gone.py CHECKOUT [RUN_ID]

Counts every resource still carrying the installation's `anti-demo-run-id` tag:
EC2 instances that are not terminated, volumes, security groups and their rules;
RDS clusters, instances and proxies; SQS queues; secrets not already scheduled
for deletion; and IAM roles, instance profiles and customer-managed policies.
The run id defaults to the one in CHECKOUT's newest manifest, or, once
`./antidemo cleanup` has removed that, its `cleanup-receipt.json`.

Prints counts only, then ALL GONE or SOMETHING REMAINS. Uses the AWS key in
CHECKOUT's `.env.bootstrap`. Exit 0 when all gone, 1 otherwise.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import installation_run_id, load_checkout_aws  # noqa: E402

RUN_ID_TAG = "anti-demo-run-id"

#: A resource deleted between being listed and having its tags read is gone.
_GONE_CODES = {
    "NoSuchEntity",
    "DBProxyNotFoundFault",
    "AWS.SimpleQueueService.NonExistentQueue",
    "QueueDoesNotExist",
}


def _carries(tags: list[dict[str, str]] | None, run_id: str) -> bool:
    return any(tag["Key"] == RUN_ID_TAG and tag["Value"] == run_id for tag in tags or [])


def _tags_of(read, key: str, **arguments) -> object:
    from botocore.exceptions import ClientError

    try:
        return read(**arguments).get(key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in _GONE_CODES:
            return None
        raise


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
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("checkout", type=Path)
    parser.add_argument("run_id", nargs="?")
    args = parser.parse_args()
    run_id = args.run_id or installation_run_id(args.checkout)
    if not run_id:
        raise SystemExit(f"no run id given and none recorded in {args.checkout}")
    load_checkout_aws(args.checkout)
    import boto3

    counts = remaining(boto3.Session(region_name=os.environ["AWS_DEFAULT_REGION"]), run_id)
    for name, count in counts.items():
        print(f"  {name}: {count}")
    gone = not any(counts.values())
    print("ALL GONE" if gone else "SOMETHING REMAINS")
    return 0 if gone else 1


if __name__ == "__main__":
    sys.exit(main())
