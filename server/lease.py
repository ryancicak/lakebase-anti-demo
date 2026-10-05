"""Keep an installation's `expires-at` tag moving while people use it.

Every AWS resource this demo creates carries an `expires-at` tag, and account
automation that honors the tag may reap a resource once it passes. The tag used
to be written once, at `created_at + ttl_hours`, and the only way to move it was
`antidemo renew`: a Terraform apply, a Round 5 re-seal, a rewritten app secret
and an app restart. So nobody moved it. An installation in daily use went on
declaring a deadline it had passed days before, and the one reminder was a
warning telling its operator to run that renewal.

The tag is now a lease: "reapable this long after anyone last used it". The
process serving the demo keeps it true. `LeaseKeeper` notes when a person uses
the app, and once the lease has fallen `RENEW_HYSTERESIS` behind `last use +
window` it moves the tag forward on every resource Terraform made for this
installation. An installation nobody uses still lapses on schedule, and that is
the point of the tag: an abandoned install is exactly what account cleanup is for.

Three rules keep that safe.

* **Only the lease moves.** Ownership is proven by the run ID, the owner and
  `managed-by`, which never change. Nothing compares `expires-at` for ownership
  any more (`lifecycle._required_tags`), and Terraform ignores the tag after
  creation (`ignore_changes = [tags["expires-at"]]` on every tagged resource),
  so a moved lease is neither drift in a plan nor a reason to refuse cleanup.
* **Only what Terraform made.** Rounds 2, 3 and 5 create short-lived per-bout
  resources tagged with the sealed expiry, and they prove those resources their
  own by exact equality with it. They are selected out by `managed-by` and left
  exactly as they were.
* **Never backwards, and never on the clock alone.** A tag only moves to a later
  value, and only because a person used the installation. A server left running
  with nobody on it does not keep itself alive.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .manifest import DemoManifest

LOGGER = logging.getLogger(__name__)

LEASE_TAG = "expires-at"
RUN_ID_TAG = "anti-demo-run-id"
MANAGED_BY_TAG = "managed-by"
TERRAFORM_MANAGED = "terraform"

#: How far the lease must lag `last use + window` before it is moved. Without a
#: floor, every request of a demo would retag the whole installation. With it, an
#: installation in continuous use is retagged about four times a day and always
#: holds at least `window - RENEW_HYSTERESIS` of lease.
RENEW_HYSTERESIS = timedelta(hours=6)

#: How often the keeper wakes on its own, without a request to prompt it.
LEASE_CHECK_SECONDS = 600.0

#: After a failed renewal, how long to wait before the next attempt. A request
#: arriving meanwhile does not bring the retry forward.
LEASE_RETRY_SECONDS = 600.0

DEFAULT_WINDOW = timedelta(hours=72)
_MIN_WINDOW = timedelta(hours=1)
_MAX_WINDOW = timedelta(hours=720)

#: Every Round 5 seal field that can name an IAM role or instance profile the
#: installation's Terraform created. Read with `getattr`, because the fields
#: differ between seal generations.
_ROUND5_ROLE_FIELDS = (
    "control_role_arn",
    "proxy_service_role_arn",
    "runner_role_arn",
    "competitor_runner_role_arn",
    "rds_proxy_role_arn",
    "execution_role_arn",
)
_ROUND5_PROFILE_FIELDS = (
    "runner_instance_profile_arn",
    "competitor_runner_instance_profile_arn",
)
_ROUND5_QUEUE_FIELDS = (
    "lakebase_control_queue_url",
    "competitor_control_queue_url",
)

#: Short, because a renewal runs on a worker thread that app shutdown waits for,
#: and a pass that cannot reach AWS should give up in seconds, not minutes: the
#: next use retries it anyway.
_BOTO_CONFIG = Config(
    connect_timeout=3,
    read_timeout=15,
    retries={"max_attempts": 3, "mode": "standard"},
)

LEASE_IDLE = "idle"
LEASE_CURRENT = "current"
LEASE_RENEWING = "renewing"
LEASE_FAILED = "failed"


def format_lease(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_lease(value: object) -> datetime | None:
    """The tag's timestamp, or None when it is absent or not one."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def lease_window(manifest: DemoManifest) -> timedelta:
    """How long after its last use the installation stays declared alive.

    The TTL the installation was provisioned with, recovered from the seal:
    `expires_at` is written as `created_at + ttl_hours` and a first provision is
    the only thing that writes both. Clamped, because an old-style renew moved
    `expires_at` alone and can leave a gap no one chose.
    """
    window = manifest.expires_at - manifest.created_at
    if window <= timedelta(0):
        return DEFAULT_WINDOW
    return min(max(window, _MIN_WINDOW), _MAX_WINDOW)


@dataclass(frozen=True)
class LeaseTarget:
    """One Terraform-made resource and the lease it carries now."""

    service: str
    identifier: str
    lease: datetime | None


@dataclass(frozen=True)
class LeaseInventory:
    targets: tuple[LeaseTarget, ...]
    #: Services whose resources could not be listed, each as "service: reason".
    #: A lease read with a gap is reported, never passed off as complete.
    unreadable: tuple[str, ...] = ()

    @property
    def earliest(self) -> datetime | None:
        """The soonest lease, or None if any resource carries none it can parse."""
        leases = [target.lease for target in self.targets]
        if not leases or any(lease is None for lease in leases):
            return None
        return min(lease for lease in leases if lease is not None)


@dataclass(frozen=True)
class LeaseRenewal:
    target: datetime
    resources: int
    renewed: int
    #: Resources that went between listing and tagging: not failures.
    vanished: int
    failed: tuple[str, ...]
    unreadable: tuple[str, ...]
    #: The soonest lease across the installation after this renewal.
    earliest: datetime | None

    @property
    def complete(self) -> bool:
        return not self.failed and not self.unreadable

    def summary(self) -> str:
        parts = [
            f"{self.renewed} of {self.resources} resources moved to {format_lease(self.target)}"
        ]
        if self.vanished:
            parts.append(f"{self.vanished} gone before they could be tagged")
        if self.failed:
            parts.append(f"{len(self.failed)} refused ({', '.join(sorted(set(self.failed)))})")
        if self.unreadable:
            parts.append(f"not listed: {', '.join(self.unreadable)}")
        return "; ".join(parts)


def _tag_map(tags: object) -> dict[str, str]:
    """AWS's two tag shapes, a list of Key/Value pairs or a plain mapping."""
    if isinstance(tags, Mapping):
        return {str(key): str(value) for key, value in tags.items()}
    result: dict[str, str] = {}
    if isinstance(tags, Iterable) and not isinstance(tags, str | bytes):
        for item in tags:
            if isinstance(item, Mapping) and item.get("Key"):
                result[str(item["Key"])] = str(item.get("Value") or "")
    return result


def _owned(manifest: DemoManifest, tags: Mapping[str, str]) -> bool:
    return (
        tags.get(RUN_ID_TAG) == manifest.run_id
        and tags.get(MANAGED_BY_TAG) == TERRAFORM_MANAGED
    )


def _target(service: str, identifier: str, tags: Mapping[str, str]) -> LeaseTarget:
    return LeaseTarget(service, identifier, parse_lease(tags.get(LEASE_TAG)))


def _error_code(error: ClientError) -> str:
    return str((error.response.get("Error") or {}).get("Code") or "ClientError")


def _is_gone(error: ClientError) -> bool:
    """Whether AWS refused because the resource no longer exists."""
    code = _error_code(error)
    return (
        "NotFound" in code
        or code
        in {
            "NoSuchEntity",
            "InvalidID",
            "QueueDoesNotExist",
            "AWS.SimpleQueueService.NonExistentQueue",
            "NoSuchBucket",
        }
    )


def _ec2_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    ec2 = session.client("ec2", region_name=manifest.aws.region, config=_BOTO_CONFIG)
    # Filtered by AWS on both ownership tags, so a shared account's other
    # resources are never even listed.
    owned = [
        {"Name": f"tag:{RUN_ID_TAG}", "Values": [manifest.run_id]},
        {"Name": f"tag:{MANAGED_BY_TAG}", "Values": [TERRAFORM_MANAGED]},
    ]
    found: list[tuple[str, dict[str, str]]] = []
    live_states = {
        "Name": "instance-state-name",
        "Values": ["pending", "running", "stopping", "stopped"],
    }
    for page in ec2.get_paginator("describe_instances").paginate(Filters=[*owned, live_states]):
        for reservation in page.get("Reservations") or []:
            for instance in reservation.get("Instances") or []:
                found.append((str(instance["InstanceId"]), _tag_map(instance.get("Tags"))))
    for page in ec2.get_paginator("describe_volumes").paginate(Filters=owned):
        for volume in page.get("Volumes") or []:
            found.append((str(volume["VolumeId"]), _tag_map(volume.get("Tags"))))
    for page in ec2.get_paginator("describe_security_groups").paginate(Filters=owned):
        for group in page.get("SecurityGroups") or []:
            found.append((str(group["GroupId"]), _tag_map(group.get("Tags"))))
    for page in ec2.get_paginator("describe_security_group_rules").paginate(Filters=owned):
        for rule in page.get("SecurityGroupRules") or []:
            found.append((str(rule["SecurityGroupRuleId"]), _tag_map(rule.get("Tags"))))
    # Round 4's Glue lane: its own subnet, the route table beside it and that table's S3
    # gateway endpoint.
    for page in ec2.get_paginator("describe_subnets").paginate(Filters=owned):
        for subnet in page.get("Subnets") or []:
            found.append((str(subnet["SubnetId"]), _tag_map(subnet.get("Tags"))))
    for page in ec2.get_paginator("describe_route_tables").paginate(Filters=owned):
        for table in page.get("RouteTables") or []:
            found.append((str(table["RouteTableId"]), _tag_map(table.get("Tags"))))
    for page in ec2.get_paginator("describe_vpc_endpoints").paginate(Filters=owned):
        for endpoint in page.get("VpcEndpoints") or []:
            found.append((str(endpoint["VpcEndpointId"]), _tag_map(endpoint.get("Tags"))))
    return [
        _target("ec2", identifier, tags) for identifier, tags in found if _owned(manifest, tags)
    ]


def _rds_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    """Clusters and instances by their inline tags, then the groups they use.

    RDS cannot filter a describe by tag, so the two describes read the region
    and keep what the tags say is ours. Subnet and parameter groups carry no tags
    in their describes, so they are found through the databases that use them,
    and each is proven ours by its own tags before it is counted.
    """
    rds = session.client("rds", region_name=manifest.aws.region, config=_BOTO_CONFIG)
    targets: list[LeaseTarget] = []
    arn_prefix = ""
    groups: dict[str, set[str]] = {"subgrp": set(), "pg": set(), "cluster-pg": set()}
    for page in rds.get_paginator("describe_db_clusters").paginate():
        for cluster in page.get("DBClusters") or []:
            tags = _tag_map(cluster.get("TagList"))
            if not _owned(manifest, tags):
                continue
            arn = str(cluster["DBClusterArn"])
            arn_prefix = arn.split(":cluster:", 1)[0]
            targets.append(_target("rds", arn, tags))
            groups["subgrp"].add(str(cluster.get("DBSubnetGroup") or ""))
            groups["cluster-pg"].add(str(cluster.get("DBClusterParameterGroup") or ""))
    for page in rds.get_paginator("describe_db_instances").paginate():
        for instance in page.get("DBInstances") or []:
            tags = _tag_map(instance.get("TagList"))
            if not _owned(manifest, tags):
                continue
            arn = str(instance["DBInstanceArn"])
            arn_prefix = arn.split(":db:", 1)[0]
            targets.append(_target("rds", arn, tags))
            groups["subgrp"].add(
                str((instance.get("DBSubnetGroup") or {}).get("DBSubnetGroupName") or "")
            )
            for group in instance.get("DBParameterGroups") or []:
                groups["pg"].add(str(group.get("DBParameterGroupName") or ""))
    if not arn_prefix:
        return targets
    for kind, names in groups.items():
        # `default.*` groups belong to AWS, not to this installation.
        for name in sorted(name for name in names if name and not name.startswith("default")):
            arn = f"{arn_prefix}:{kind}:{name}"
            try:
                tags = _tag_map(rds.list_tags_for_resource(ResourceName=arn).get("TagList"))
            except ClientError as error:
                if _is_gone(error):
                    continue
                raise
            if _owned(manifest, tags):
                targets.append(_target("rds", arn, tags))
    return targets


def _secret_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    secrets = session.client(
        "secretsmanager", region_name=manifest.aws.region, config=_BOTO_CONFIG
    )
    targets: list[LeaseTarget] = []
    for page in secrets.get_paginator("list_secrets").paginate(
        Filters=[
            {"Key": "tag-key", "Values": [RUN_ID_TAG]},
            {"Key": "tag-value", "Values": [manifest.run_id]},
        ]
    ):
        for secret in page.get("SecretList") or []:
            # RDS's own master-user secrets inherit the database's tags but belong
            # to RDS, which keeps them for exactly as long as the database.
            if secret.get("OwningService"):
                continue
            tags = _tag_map(secret.get("Tags"))
            if _owned(manifest, tags):
                targets.append(_target("secretsmanager", str(secret["ARN"]), tags))
    return targets


def _queue_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    """Round 5's control queues, from the seal, and the dead-letter queue of each."""
    round5 = manifest.round5
    urls = [
        str(value)
        for value in (getattr(round5, field, None) for field in _ROUND5_QUEUE_FIELDS)
        if value
    ]
    if not urls:
        return []
    sqs = session.client("sqs", region_name=manifest.aws.region, config=_BOTO_CONFIG)
    queues: list[str] = []
    for url in urls:
        try:
            attributes = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["RedrivePolicy"]
            ).get("Attributes") or {}
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        queues.append(url)
        redrive = attributes.get("RedrivePolicy")
        if redrive:
            dead_letter = str(json.loads(redrive).get("deadLetterTargetArn") or "")
            parts = dead_letter.split(":")
            if len(parts) == 6:
                try:
                    queues.append(
                        str(
                            sqs.get_queue_url(
                                QueueName=parts[5], QueueOwnerAWSAccountId=parts[4]
                            )["QueueUrl"]
                        )
                    )
                except ClientError as error:
                    if not _is_gone(error):
                        raise
    targets: list[LeaseTarget] = []
    for url in dict.fromkeys(queues):
        try:
            tags = _tag_map(sqs.list_queue_tags(QueueUrl=url).get("Tags"))
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        if _owned(manifest, tags):
            targets.append(_target("sqs", url, tags))
    return targets


def _iam_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    """The sealed roles, then the policies and instance profiles hanging off them.

    IAM has no tag search, so discovery starts from the role ARNs the seal
    already names. Each resource is still proven ours by its own tags, so a
    policy attached from somewhere else is read and left alone.
    """
    round5 = manifest.round5
    round6_aws = getattr(manifest, "round6_aws", None)
    role_arns = [
        manifest.aws.runtime_role_arn,
        *(getattr(round5, field, None) for field in _ROUND5_ROLE_FIELDS),
        getattr(getattr(manifest, "round4_aws", None), "role_arn", None),
        *(
            getattr(round6_aws, field, None)
            for field in ("glue_role_arn", "dms_s3_role_arn", "uc_role_arn")
        ),
    ]
    roles = list(
        dict.fromkeys(
            str(arn).rsplit("/", 1)[-1] for arn in role_arns if arn and ":role/" in str(arn)
        )
    )
    profiles = {
        str(arn).rsplit("/", 1)[-1]
        for arn in (getattr(round5, field, None) for field in _ROUND5_PROFILE_FIELDS)
        if arn
    }
    if not roles and not profiles:
        return []
    iam = session.client("iam", config=_BOTO_CONFIG)
    own_policy_marker = f":iam::{manifest.aws.account_id}:policy/"
    targets: list[LeaseTarget] = []
    policies: set[str] = set()
    for name in roles:
        try:
            role = iam.get_role(RoleName=name)["Role"]
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        tags = _tag_map(role.get("Tags"))
        if not _owned(manifest, tags):
            continue
        targets.append(_target("iam-role", name, tags))
        boundary = str((role.get("PermissionsBoundary") or {}).get("PermissionsBoundaryArn") or "")
        if own_policy_marker in boundary:
            policies.add(boundary)
        for page in iam.get_paginator("list_attached_role_policies").paginate(RoleName=name):
            for policy in page.get("AttachedPolicies") or []:
                arn = str(policy.get("PolicyArn") or "")
                if own_policy_marker in arn:
                    policies.add(arn)
        for page in iam.get_paginator("list_instance_profiles_for_role").paginate(RoleName=name):
            for profile in page.get("InstanceProfiles") or []:
                profiles.add(str(profile["InstanceProfileName"]))
    for arn in sorted(policies):
        try:
            tags = _tag_map(iam.list_policy_tags(PolicyArn=arn).get("Tags"))
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        if _owned(manifest, tags):
            targets.append(_target("iam-policy", arn, tags))
    for name in sorted(profiles):
        try:
            profile = iam.get_instance_profile(InstanceProfileName=name)["InstanceProfile"]
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        tags = _tag_map(profile.get("Tags"))
        if _owned(manifest, tags):
            targets.append(_target("iam-instance-profile", name, tags))
    return targets


def _glue_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    """The Glue jobs and connections of Rounds 4 and 6, from their seals, each proven ours by
    its tags."""
    arns: list[str] = []
    sealed = getattr(manifest, "round4_aws", None)
    if sealed is not None:
        partition = str(sealed.role_arn).split(":")[1]
        prefix = f"arn:{partition}:glue:{manifest.aws.region}:{manifest.aws.account_id}"
        arns += [
            *(f"{prefix}:job/{lane.job_name}" for lane in (sealed.aurora, sealed.rds)),
            *(
                f"{prefix}:connection/{lane.connection_name}"
                for lane in (sealed.aurora, sealed.rds)
            ),
        ]
    round6 = getattr(manifest, "round6_aws", None)
    if round6 is not None:
        partition = str(round6.glue_role_arn).split(":")[1]
        prefix = f"arn:{partition}:glue:{manifest.aws.region}:{manifest.aws.account_id}"
        arns += [f"{prefix}:job/{lane.job_name}" for lane in (round6.aurora, round6.rds)]
    if not arns:
        return []
    glue = session.client("glue", region_name=manifest.aws.region, config=_BOTO_CONFIG)
    targets: list[LeaseTarget] = []
    for arn in arns:
        try:
            tags = _tag_map(glue.get_tags(ResourceArn=arn).get("Tags"))
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        if _owned(manifest, tags):
            targets.append(_target("glue", arn, tags))
    return targets


def _bucket_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    """The buckets of Round 4's and Round 6's AWS lanes, from their seals: the only buckets this
    installation makes."""
    buckets = [
        sealed.bucket
        for sealed in (getattr(manifest, "round4_aws", None), getattr(manifest, "round6_aws", None))
        if sealed is not None
    ]
    if not buckets:
        return []
    s3 = session.client("s3", region_name=manifest.aws.region, config=_BOTO_CONFIG)
    targets: list[LeaseTarget] = []
    for bucket in buckets:
        try:
            tags = _tag_map(s3.get_bucket_tagging(Bucket=bucket).get("TagSet"))
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        if _owned(manifest, tags):
            targets.append(_target("s3-bucket", bucket, tags))
    return targets


def _dms_targets(session: Any, manifest: DemoManifest) -> list[LeaseTarget]:
    """Round 6's DMS instance, its subnet group, endpoints and tasks, from the seal.

    DMS lists no tags with its resources, so each sealed ARN is asked for its own. The subnet
    group carries no ARN in the seal and is read off the instance that uses it.
    """
    sealed = getattr(manifest, "round6_aws", None)
    if sealed is None:
        return []
    dms = session.client("dms", region_name=manifest.aws.region, config=_BOTO_CONFIG)
    arns = [
        sealed.replication_instance_arn,
        *(
            arn
            for lane in (sealed.aurora, sealed.rds)
            for arn in (lane.source_endpoint_arn, lane.target_endpoint_arn, lane.task_arn)
        ),
    ]
    try:
        instances = dms.describe_replication_instances(
            Filters=[
                {"Name": "replication-instance-arn", "Values": [sealed.replication_instance_arn]}
            ]
        ).get("ReplicationInstances") or []
    except ClientError as error:
        if not _is_gone(error):
            raise
        instances = []
    for instance in instances:
        group = str(
            (instance.get("ReplicationSubnetGroup") or {}).get("ReplicationSubnetGroupIdentifier")
            or ""
        )
        if group:
            arns.append(sealed.replication_instance_arn.split(":rep:", 1)[0] + f":subgrp:{group}")
    targets: list[LeaseTarget] = []
    for arn in arns:
        try:
            tags = _tag_map(dms.list_tags_for_resource(ResourceArn=arn).get("TagList"))
        except ClientError as error:
            if _is_gone(error):
                continue
            raise
        if _owned(manifest, tags):
            targets.append(_target("dms", arn, tags))
    return targets


_DISCOVERY: tuple[tuple[str, Callable[[Any, DemoManifest], list[LeaseTarget]]], ...] = (
    ("ec2", _ec2_targets),
    ("rds", _rds_targets),
    ("secretsmanager", _secret_targets),
    ("sqs", _queue_targets),
    ("iam", _iam_targets),
    ("glue", _glue_targets),
    ("s3", _bucket_targets),
    ("dms", _dms_targets),
)


def discover_lease_targets(session: Any, manifest: DemoManifest) -> LeaseInventory:
    """Every resource Terraform made for this installation, with its lease. Reads only.

    One service failing to list does not stop the others: it is named in
    `unreadable`, and the lease across the rest is still reported.
    """
    targets: list[LeaseTarget] = []
    unreadable: list[str] = []
    for service, discover in _DISCOVERY:
        try:
            targets.extend(discover(session, manifest))
        except ClientError as error:
            unreadable.append(f"{service}: {_error_code(error)}")
        except AssertionError:
            raise
        except Exception as error:  # noqa: BLE001 - one service's surprise is not the rest's
            unreadable.append(f"{service}: {type(error).__name__}")
    return LeaseInventory(tuple(targets), tuple(unreadable))


def _lease_writers(
    session: Any, manifest: DemoManifest, value: str
) -> dict[str, Callable[[str], object]]:
    """One tagging call per service, each writing only the lease key.

    Every client is a named local on purpose: `server/aws_permissions.py` reads
    the IAM actions this module needs from its call sites, and a client picked
    by a runtime string is a call site it cannot see.
    """
    region = manifest.aws.region
    pair = [{"Key": LEASE_TAG, "Value": value}]
    ec2 = session.client("ec2", region_name=region, config=_BOTO_CONFIG)
    rds = session.client("rds", region_name=region, config=_BOTO_CONFIG)
    secrets = session.client("secretsmanager", region_name=region, config=_BOTO_CONFIG)
    sqs = session.client("sqs", region_name=region, config=_BOTO_CONFIG)
    iam = session.client("iam", config=_BOTO_CONFIG)
    glue = session.client("glue", region_name=region, config=_BOTO_CONFIG)
    s3 = session.client("s3", region_name=region, config=_BOTO_CONFIG)
    dms = session.client("dms", region_name=region, config=_BOTO_CONFIG)

    def move_bucket_lease(bucket: str) -> None:
        # S3 replaces a bucket's whole tag set, so the set just read is written back with
        # only the lease changed. A set that cannot be read, or no longer proves this
        # installation owns the bucket, is never overwritten.
        tags = _tag_map(s3.get_bucket_tagging(Bucket=bucket).get("TagSet"))
        if not _owned(manifest, tags):
            raise ValueError(f"{bucket}'s tags no longer prove this installation owns it")
        tags[LEASE_TAG] = value
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": key, "Value": tag} for key, tag in tags.items()]},
        )

    return {
        "ec2": lambda identifier: ec2.create_tags(Resources=[identifier], Tags=pair),
        "rds": lambda identifier: rds.add_tags_to_resource(ResourceName=identifier, Tags=pair),
        "secretsmanager": lambda identifier: secrets.tag_resource(SecretId=identifier, Tags=pair),
        "sqs": lambda identifier: sqs.tag_queue(QueueUrl=identifier, Tags={LEASE_TAG: value}),
        "iam-role": lambda identifier: iam.tag_role(RoleName=identifier, Tags=pair),
        "iam-policy": lambda identifier: iam.tag_policy(PolicyArn=identifier, Tags=pair),
        "iam-instance-profile": lambda identifier: iam.tag_instance_profile(
            InstanceProfileName=identifier, Tags=pair
        ),
        "glue": lambda identifier: glue.tag_resource(
            ResourceArn=identifier, TagsToAdd={LEASE_TAG: value}
        ),
        "s3-bucket": move_bucket_lease,
        "dms": lambda identifier: dms.add_tags_to_resource(ResourceArn=identifier, Tags=pair),
    }


def renew_lease(
    session: Any,
    manifest: DemoManifest,
    target: datetime,
    *,
    minimum_gain: timedelta = timedelta(0),
    inventory: LeaseInventory | None = None,
) -> LeaseRenewal:
    """Move the lease on every Terraform-made resource to `target`, never back.

    A resource is retagged only when `target` is at least `minimum_gain` past
    its current lease, or it carries no lease this can read. Never raises for a
    single resource: each refusal is collected, so one resource AWS will not tag
    cannot keep the other sixty on the old lease.
    """
    target = target.astimezone(UTC).replace(microsecond=0)
    inventory = inventory if inventory is not None else discover_lease_targets(session, manifest)
    value = format_lease(target)
    lagging = [
        item
        for item in inventory.targets
        if item.lease is None or target - item.lease >= max(minimum_gain, timedelta(seconds=1))
    ]
    writers = _lease_writers(session, manifest, value) if lagging else {}
    # EC2 takes many IDs in one call, and a batch is all-or-nothing, so the
    # batch is the fast path and a refused batch is retried one ID at a time.
    ec2_ids = [item.identifier for item in lagging if item.service == "ec2"]
    batched: set[str] = set()
    if ec2_ids:
        ec2 = session.client("ec2", region_name=manifest.aws.region, config=_BOTO_CONFIG)
        for start in range(0, len(ec2_ids), 100):
            chunk = ec2_ids[start : start + 100]
            try:
                ec2.create_tags(Resources=chunk, Tags=[{"Key": LEASE_TAG, "Value": value}])
            except (BotoCoreError, ClientError):
                continue
            batched.update(chunk)
    renewed = 0
    vanished = 0
    failed: list[str] = []
    # Each resource's lease once this is done: the target where it moved, the old
    # value where it did not. None anywhere makes the installation's lease unknown.
    after: list[datetime | None] = [
        item.lease for item in inventory.targets if item not in lagging
    ]
    for item in lagging:
        if item.service == "ec2" and item.identifier in batched:
            renewed += 1
            after.append(target)
            continue
        write = writers.get(item.service)
        try:
            if write is None:
                raise ValueError(f"no tagging call for {item.service}")
            write(item.identifier)
        except ClientError as error:
            if _is_gone(error):
                vanished += 1
                continue
            failed.append(f"{item.service} {_error_code(error)}")
            LOGGER.warning(
                "The lease on %s %s could not be moved: %s",
                item.service,
                item.identifier,
                _error_code(error),
            )
            after.append(item.lease)
            continue
        except (BotoCoreError, ValueError) as error:
            failed.append(f"{item.service} {type(error).__name__}")
            after.append(item.lease)
            continue
        renewed += 1
        after.append(target)
    earliest = (
        None
        if not after or any(lease is None for lease in after)
        else min(lease for lease in after if lease is not None)
    )
    return LeaseRenewal(
        target=target,
        resources=len(inventory.targets),
        renewed=renewed,
        vanished=vanished,
        failed=tuple(failed),
        unreadable=inventory.unreadable,
        earliest=earliest,
    )


def _utcnow() -> datetime:
    return datetime.now(UTC)


class LeaseKeeper:
    """Moves the lease while the app is in use. Started once per serving process.

    `note_activity` is called for every request a person makes and does nothing
    but record the moment, so it can sit on the request path. The renewal itself
    runs on this keeper's own task, in a worker thread, and cannot fail a request
    or a bout: its failures are logged, reported on `/readyz`, and retried.
    """

    def __init__(
        self,
        manifest: DemoManifest,
        session_factory: Callable[[DemoManifest], Any],
        *,
        window: timedelta | None = None,
        interval_seconds: float = LEASE_CHECK_SECONDS,
        retry_seconds: float = LEASE_RETRY_SECONDS,
        hysteresis: timedelta = RENEW_HYSTERESIS,
        clock: Callable[[], datetime] = _utcnow,
        renew: Callable[..., LeaseRenewal] = renew_lease,
    ) -> None:
        self._manifest = manifest
        self._session_factory = session_factory
        self._window = window if window is not None else lease_window(manifest)
        self._interval = interval_seconds
        self._retry = timedelta(seconds=retry_seconds)
        self._hysteresis = hysteresis
        self._clock = clock
        self._renew = renew
        self._wake = asyncio.Event()
        self._activity: datetime | None = None
        self._lease: datetime | None = None
        self._retry_after: datetime | None = None
        self._renewed_at: datetime | None = None
        self._state = LEASE_IDLE
        self._detail = "No one has used the app since it started, so the lease has not moved."

    @property
    def window(self) -> timedelta:
        return self._window

    def note_activity(self) -> None:
        now = self._clock()
        self._activity = now
        if self._due(now):
            self._wake.set()

    def _target(self) -> datetime | None:
        if self._activity is None:
            return None
        return (self._activity + self._window).astimezone(UTC).replace(microsecond=0)

    def _due(self, now: datetime) -> bool:
        target = self._target()
        if target is None:
            return False
        if self._retry_after is not None and now < self._retry_after:
            return False
        return self._lease is None or target - self._lease >= self._hysteresis

    async def run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval)
            except TimeoutError:
                pass
            self._wake.clear()
            await self.tick()

    async def tick(self) -> LeaseRenewal | None:
        now = self._clock()
        if not self._due(now):
            return None
        target = self._target()
        assert target is not None
        self._state = LEASE_RENEWING
        try:
            renewal = await asyncio.to_thread(self._renew_once, target)
        except Exception as error:  # noqa: BLE001 - an observer may never break the app
            self._retry_after = now + self._retry
            self._state = LEASE_FAILED
            self._detail = f"The lease could not be renewed ({type(error).__name__}); retrying."
            LOGGER.warning("The installation lease could not be renewed", exc_info=True)
            return None
        self._lease = renewal.earliest
        if renewal.complete:
            self._retry_after = None
            self._renewed_at = now if renewal.renewed else self._renewed_at
            self._state = LEASE_CURRENT
            self._detail = (
                f"Kept alive while in use: {renewal.summary()}."
                if renewal.renewed
                else f"Kept alive while in use: all {renewal.resources} resources already current."
            )
            if renewal.renewed:
                LOGGER.info("LEASE %s", renewal.summary())
        else:
            self._retry_after = now + self._retry
            self._state = LEASE_FAILED
            self._detail = f"The lease was only partly renewed: {renewal.summary()}."
            LOGGER.warning("LEASE partial renewal: %s", renewal.summary())
        return renewal

    def _renew_once(self, target: datetime) -> LeaseRenewal:
        session = self._session_factory(self._manifest)
        return self._renew(session, self._manifest, target, minimum_gain=self._hysteresis)

    def snapshot(self) -> dict[str, Any]:
        return {
            "lease_state": self._state,
            "lease_expires_at": format_lease(self._lease) if self._lease is not None else None,
            "lease_renewed_at": (
                format_lease(self._renewed_at) if self._renewed_at is not None else None
            ),
            "lease_window_hours": round(self._window.total_seconds() / 3600, 2),
            "lease_detail": self._detail,
        }
