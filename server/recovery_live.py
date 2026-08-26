from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Mapping
from datetime import datetime
from typing import Any

from .manifest import DemoManifest, load_manifest
from .models import CompetitorId
from .recovery import (
    RecoveryAdapter,
    RecoveryEngine,
    RecoveryPlan,
    RecoveryReporter,
    deterministic_recovery_artifact_id,
)
from .safe_change import (
    DEFAULT_CANCEL_TEARDOWN_SECONDS,
    ArtifactInspection,
    SafeChangePlan,
    SafeChangeProvider,
    UnsafeCleanupError,
    deterministic_artifact_id,
)
from .safe_change_live import (
    _ABSENT_CLUSTER_CODES,
    _ABSENT_INSTANCE_CODES,
    _RDS_SERVING_STATES,
    _UNDELETABLE_CLUSTER_CODES,
    _UNDELETABLE_INSTANCE_CODES,
    LAKEBASE_API_ROOT,
    AuroraSafeChangeAdapter,
    ControlPlaneCommandError,
    LakebaseSafeChangeAdapter,
    RdsSafeChangeAdapter,
    SafeChangeControlPlaneError,
    SafeChangeLiveConfigurationError,
    _aws_child_id,
    _lakebase_create_path,
    _lakebase_endpoint_type,
    _lakebase_owner_endpoint_id,
    build_safe_change_engine,
    lakebase_resource_path,
)

LOGGER = logging.getLogger(__name__)

_CONTRACT_TAG = "anti-demo-contract"
_CONTRACT_VALUE = "recovery-v1"


class _RecoveryAdapterBase(RecoveryAdapter):
    async def settle_pending_mutations(self) -> None:
        return None

    def _assert_plan(self, plan: RecoveryPlan) -> None:
        if plan.provider != self.provider or plan.source_id != self.source_id:
            raise SafeChangeLiveConfigurationError("Recovery plan binding changed")
        if plan.artifact_id != deterministic_recovery_artifact_id(
            plan.scope.run_id,
            plan.provider,
        ):
            raise SafeChangeLiveConfigurationError("Recovery artifact ID is not deterministic")

    def _source_plan(self, plan: RecoveryPlan) -> SafeChangePlan:
        self._assert_plan(plan)
        return SafeChangePlan(
            lane_id=plan.lane_id,
            name=plan.name,
            provider=plan.provider,
            source_id=plan.source_id,
            artifact_id=deterministic_artifact_id(plan.scope.run_id, plan.provider),
            scope=plan.scope,
        )


class LakebaseRecoveryAdapter(_RecoveryAdapterBase):
    provider = SafeChangeProvider.LAKEBASE

    def __init__(self, adapter: LakebaseSafeChangeAdapter) -> None:
        self.adapter = adapter
        self.name = adapter.name
        self.source_id = adapter.source_id
        self._pending_mutations: set[asyncio.Task[Any]] = set()

    async def _run_mutation(self, operation: Awaitable[Any]) -> Any:
        task = asyncio.create_task(operation)
        self._pending_mutations.add(task)
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                self._pending_mutations.discard(task)

    async def settle_pending_mutations(self) -> None:
        tracked = list(self._pending_mutations)
        if not tracked:
            return
        try:
            async with asyncio.timeout(self.adapter.config.control_timeout_seconds):
                await asyncio.gather(*(asyncio.shield(task) for task in tracked))
        except TimeoutError as exc:
            raise SafeChangeControlPlaneError(
                "Lakebase mutation is still unresolved; cleanup cannot report success"
            ) from exc
        finally:
            self._pending_mutations.difference_update(
                task for task in tracked if task.done()
            )

    async def connect_source(self, plan: RecoveryPlan):
        return await self.adapter.connect_source(self._source_plan(plan))

    async def inspect_recovery(self, plan: RecoveryPlan) -> ArtifactInspection | None:
        self._assert_plan(plan)
        branch_name = f"{self.adapter._project}/branches/{plan.artifact_id}"
        branch = await self.adapter._get_or_none(branch_name)
        if branch is None:
            return None
        self.adapter._resource_name(branch, branch_name, "recovery branch")
        status = branch.get("status") or {}
        spec = branch.get("spec") or {}
        source_branch = str(
            (status.get("source_branch") if isinstance(status, Mapping) else "")
            or (spec.get("source_branch") if isinstance(spec, Mapping) else "")
            or ""
        )
        endpoint_id = _lakebase_owner_endpoint_id(plan)  # type: ignore[arg-type]
        endpoint_name = f"{branch_name}/endpoints/{endpoint_id}"
        endpoint = await self.adapter._get_or_none(endpoint_name)
        endpoint_state = "ABSENT"
        marker_valid = False
        if endpoint is not None:
            self.adapter._resource_name(endpoint, endpoint_name, "recovery endpoint")
            endpoint_status = endpoint.get("status") or {}
            endpoint_state = str(endpoint_status.get("current_state") or "").upper()
            marker_valid = (
                _lakebase_endpoint_type(endpoint) == "ENDPOINT_TYPE_READ_WRITE"
            )
        branch_state = str(status.get("current_state") or "").upper()
        owned = source_branch == self.adapter._source_branch
        return ArtifactInspection(
            artifact_id=plan.artifact_id,
            provider=plan.provider,
            source_id=plan.source_id if owned else source_branch,
            run_id=plan.scope.run_id if owned else "",
            owner=plan.scope.owner if owned and marker_valid else "",
            state=f"{branch_state or 'UNKNOWN'}/{endpoint_state or 'UNKNOWN'}",
            metadata={
                "branch_name": branch_name,
                "source_branch": source_branch,
                "ownership_endpoint": endpoint_name,
                "ownership_marker_valid": marker_valid,
                "recovery_contract": _CONTRACT_VALUE,
            },
        )

    def _assert_owned(self, plan: RecoveryPlan, artifact: ArtifactInspection) -> None:
        self._assert_plan(plan)
        branch_name = f"{self.adapter._project}/branches/{plan.artifact_id}"
        expected_endpoint = (
            f"{branch_name}/endpoints/{_lakebase_owner_endpoint_id(plan)}"  # type: ignore[arg-type]
        )
        if (
            artifact.artifact_id != plan.artifact_id
            or artifact.provider != plan.provider
            or artifact.source_id != plan.source_id
            or artifact.run_id != plan.scope.run_id
            or artifact.owner != plan.scope.owner
            or artifact.metadata.get("branch_name") != branch_name
            or artifact.metadata.get("source_branch") != self.adapter._source_branch
            or artifact.metadata.get("ownership_endpoint") != expected_endpoint
            or artifact.metadata.get("ownership_marker_valid") is not True
            or artifact.metadata.get("recovery_contract") != _CONTRACT_VALUE
        ):
            raise UnsafeCleanupError("Lakebase recovery branch ownership mismatch")

    async def wait_recovery_point(
        self,
        plan: RecoveryPlan,
        recovery_at: datetime,
        report: RecoveryReporter,
    ) -> Mapping[str, object]:
        await report(
            "Validating the Lakebase source; branch-request acceptance enforces "
            "recovery-point eligibility",
            (
                f"GET {lakebase_resource_path(self.adapter._project)} + "
                f"{lakebase_resource_path(self.adapter._source_branch)} + "
                f"{lakebase_resource_path(self.source_id)} → the branch POST's "
                "acceptance enforces source_branch_time eligibility"
            ),
        )
        await self.adapter.preflight(self._source_plan(plan))
        return {"source_branch_time": recovery_at.isoformat()}

    async def create_recovery(
        self,
        plan: RecoveryPlan,
        recovery_at: datetime,
        report: RecoveryReporter,
    ) -> ArtifactInspection:
        self._assert_plan(plan)
        branch_name = f"{self.adapter._project}/branches/{plan.artifact_id}"
        branch_path = _lakebase_create_path(
            self.adapter._project, "branches", "branch_id", plan.artifact_id
        )
        branch_wire_call = (
            f"POST {branch_path} source_branch_time={recovery_at.isoformat()}"
        )
        await report(
            "Requesting the Lakebase point-in-time recovery branch",
            branch_wire_call,
        )
        await self._run_mutation(
            self.adapter._runner.json(
                "POST",
                branch_path,
                body={
                    "spec": {
                        "source_branch": self.adapter._source_branch,
                        "source_branch_time": recovery_at.isoformat(),
                        "no_expiry": True,
                    }
                },
                timeout_seconds=self.adapter.config.control_timeout_seconds,
            )
        )
        await report(
            "Lakebase point-in-time recovery branch request accepted",
            branch_wire_call,
        )
        endpoint_id = _lakebase_owner_endpoint_id(plan)  # type: ignore[arg-type]
        endpoint_name = f"{branch_name}/endpoints/{endpoint_id}"
        # The branch POST returns as soon as the request is accepted, so its
        # native `primary` endpoint cannot be probed until the branch itself is
        # addressable. Without this wait the probe 404s on a branch mid-creation
        # and the lane tries to create a second read-write endpoint on it.
        await self.adapter._wait_branch_present(branch_name)
        endpoint = await self.adapter._get_or_none(endpoint_name)
        if endpoint is None:
            endpoint_wire_call = (
                "POST "
                + _lakebase_create_path(branch_name, "endpoints", "endpoint_id", endpoint_id)
            )
            await report(
                "Requesting the Lakebase recovery endpoint",
                endpoint_wire_call,
            )
            await self._run_mutation(
                self.adapter._runner.json(
                    "POST",
                    _lakebase_create_path(
                        branch_name, "endpoints", "endpoint_id", endpoint_id
                    ),
                    body={
                        "spec": {
                            "endpoint_type": "ENDPOINT_TYPE_READ_WRITE",
                            "autoscaling_limit_min_cu": 0.5,
                            "autoscaling_limit_max_cu": 2.0,
                        }
                    },
                    timeout_seconds=self.adapter.config.control_timeout_seconds,
                )
            )
            await report(
                "Lakebase recovery endpoint request accepted",
                endpoint_wire_call,
            )
        else:
            self.adapter._resource_name(endpoint, endpoint_name, "recovery endpoint")
            if _lakebase_endpoint_type(endpoint) != "ENDPOINT_TYPE_READ_WRITE":
                raise SafeChangeLiveConfigurationError(
                    "Lakebase recovery primary endpoint is not read-write"
                )
        await report(
            "Waiting for the Lakebase recovery branch and endpoint",
            f"GET {LAKEBASE_API_ROOT}/<branch> + <endpoint>",
        )
        deadline = self.adapter._clock() + self.adapter.config.poll_timeout_seconds
        while True:
            artifact = await self.inspect_recovery(plan)
            if artifact is not None and artifact.state in {"READY/ACTIVE", "READY/IDLE"}:
                self._assert_owned(plan, artifact)
                return artifact
            if self.adapter._clock() >= deadline:
                raise SafeChangeControlPlaneError("Lakebase recovery branch did not become ready")
            await self.adapter._sleep(self.adapter.config.poll_interval_seconds)

    async def connect_recovery(
        self,
        plan: RecoveryPlan,
        artifact: ArtifactInspection,
    ):
        self._assert_owned(plan, artifact)
        return await self.adapter._connect_endpoint(
            str(artifact.metadata["ownership_endpoint"])
        )

    async def delete_recovery(
        self,
        plan: RecoveryPlan,
        artifact: ArtifactInspection,
        report: RecoveryReporter,
    ) -> None:
        self._assert_owned(plan, artifact)
        current = await self.inspect_recovery(plan)
        if current is None:
            return
        self._assert_owned(plan, current)
        branch_name = f"{self.adapter._project}/branches/{plan.artifact_id}"
        await self.adapter._runner.run(
            "DELETE",
            lakebase_resource_path(branch_name),
            timeout_seconds=self.adapter.config.control_timeout_seconds,
        )
        await report(
            "Waiting for the owned Lakebase recovery branch deletion",
            f"GET {LAKEBASE_API_ROOT}/<branch>",
        )
        deadline = self.adapter._clock() + self.adapter.config.poll_timeout_seconds
        while await self.inspect_recovery(plan) is not None:
            if self.adapter._clock() >= deadline:
                raise SafeChangeControlPlaneError("Lakebase recovery branch still exists")
            await self.adapter._sleep(self.adapter.config.poll_interval_seconds)

    async def abandon_recovery(self, plan: RecoveryPlan) -> None:
        """Issue branch deletion for a cancelled lane without waiting for it.

        No waiter to talk out of waiting any more: the DELETE returns its
        operation immediately, because here the request is the point and absence
        is somebody else's job.
        """

        self._assert_plan(plan)
        branch_name = f"{self.adapter._project}/branches/{plan.artifact_id}"
        try:
            await self.adapter._runner.run(
                "DELETE",
                lakebase_resource_path(branch_name),
                timeout_seconds=DEFAULT_CANCEL_TEARDOWN_SECONDS,
            )
        except ControlPlaneCommandError as exc:
            if not exc.not_found:
                raise
            LOGGER.info("Cancelled lane teardown: %s is already absent", branch_name)
            return
        LOGGER.warning(
            "Cancelled lane teardown: issued delete-branch for %s",
            branch_name,
        )


class _AwsRecoveryAdapter(_RecoveryAdapterBase):
    adapter: AuroraSafeChangeAdapter | RdsSafeChangeAdapter

    async def settle_pending_mutations(self) -> None:
        await self.adapter._reconcile_pending_mutations()

    async def _instance_status(self, instance_id: str) -> str | None:
        instance = await self.adapter._describe_instance(instance_id, missing_ok=True)
        if instance is None:
            return None
        return str(instance.get("DBInstanceStatus") or "").lower()

    def _assert_plan(self, plan: RecoveryPlan) -> None:
        super()._assert_plan(plan)
        if (
            plan.scope.aws_account_id != self.adapter.config.account_id
            or plan.scope.aws_region != self.adapter.config.region
        ):
            raise SafeChangeLiveConfigurationError(
                f"{self.name} recovery plan does not match the exact AWS target"
            )

    def _tags(self, plan: RecoveryPlan) -> list[dict[str, str]]:
        tags = self.adapter._tags(self._source_plan(plan))
        tags.append({"Key": _CONTRACT_TAG, "Value": _CONTRACT_VALUE})
        return tags

    async def connect_source(self, plan: RecoveryPlan):
        return await self.adapter.connect_source(self._source_plan(plan))

    async def _restorable_window(
        self,
        plan: RecoveryPlan,
        report: RecoveryReporter | None = None,
    ) -> tuple[datetime, datetime]:
        del report
        self._assert_plan(plan)
        source = await self._source_control()
        earliest = source.get("EarliestRestorableTime")
        latest = source.get("LatestRestorableTime")
        if not isinstance(earliest, datetime) or not isinstance(latest, datetime):
            raise SafeChangeLiveConfigurationError("AWS restorable window is unavailable")
        return earliest, latest

    async def wait_recovery_point(
        self,
        plan: RecoveryPlan,
        recovery_at: datetime,
        report: RecoveryReporter,
    ) -> Mapping[str, object]:
        await report(
            "Waiting for the AWS restore window to include the recovery point",
            self._source_wire_call,
        )
        deadline = self.adapter._clock() + self.adapter.config.poll_timeout_seconds
        while True:
            earliest, latest = await self._restorable_window(plan, report)
            # Both bounds are re-read every poll, and `create_recovery` runs
            # within a progress callback of the loop returning, so the window
            # backing the restore is never more than a poll old. What these two
            # messages have to carry is the diagnosis, because the alternative is
            # a bare `InvalidRestoreFault` from the restore API that names none
            # of the three timestamps that explain it.
            if earliest > recovery_at:
                # `recovery_at` is seconds old when the bell derives it, so with
                # any sane retention this means the source clock and RDS disagree
                # by roughly the whole retention period rather than that the
                # floor crept up on a valid request.
                raise SafeChangeLiveConfigurationError(
                    "AWS restore window no longer includes the recovery point: "
                    f"requested {recovery_at.isoformat()}, window opens at "
                    f"{earliest.isoformat()} and closes at {latest.isoformat()}"
                )
            if latest >= recovery_at:
                return {
                    "earliest_restorable_time": earliest.isoformat(),
                    "latest_restorable_time": latest.isoformat(),
                }
            if self.adapter._clock() >= deadline:
                # The expected reason to be here is the normal several-minute lag
                # of `LatestRestorableTime` behind now, so the remaining gap is
                # the number that says whether this was nearly done or nowhere
                # close.
                raise SafeChangeControlPlaneError(
                    "Timed out waiting for the AWS restore window: requested "
                    f"{recovery_at.isoformat()} is still "
                    f"{(recovery_at - latest).total_seconds():.0f}s beyond the "
                    f"latest restorable time {latest.isoformat()}"
                )
            await self.adapter._sleep(self.adapter.config.poll_interval_seconds)

    async def _source_control(self) -> Mapping[str, Any]:
        raise NotImplementedError


class AuroraRecoveryAdapter(_AwsRecoveryAdapter):
    provider = SafeChangeProvider.AURORA

    def __init__(self, adapter: AuroraSafeChangeAdapter) -> None:
        self.adapter = adapter
        self.name = adapter.name
        self.source_id = adapter.source_id
        self._recovery_source: Mapping[str, Any] | None = None
        self._recovery_writer: Mapping[str, Any] | None = None

    @property
    def _source_wire_call(self) -> str:
        return (
            "RDS.DescribeDBClusters("
            f"DBClusterIdentifier={self.source_id}) → LatestRestorableTime"
        )

    async def _source_control(self) -> Mapping[str, Any]:
        cluster, writer = await self.adapter._source()
        self._recovery_source = cluster
        self._recovery_writer = writer
        return cluster

    async def _cluster_status(self, cluster_id: str) -> str | None:
        cluster = await self.adapter._describe_cluster(cluster_id, missing_ok=True)
        if cluster is None:
            return None
        return str(cluster.get("Status") or "").lower()

    async def inspect_recovery(self, plan: RecoveryPlan) -> ArtifactInspection | None:
        self._assert_plan(plan)
        await self.adapter._assert_identity()
        cluster = await self.adapter._describe_cluster(plan.artifact_id, missing_ok=True)
        if cluster is None:
            return None
        tags = await self.adapter._resource_tags(str(cluster.get("DBClusterArn") or ""))
        writer_id = _aws_child_id(plan.artifact_id, "writer")
        groups = {
            str(group.get("VpcSecurityGroupId") or "")
            for group in cluster.get("VpcSecurityGroups") or []
            if isinstance(group, Mapping)
        }
        expected_tags = self.adapter._tag_mapping(self._tags(plan))
        members = cluster.get("DBClusterMembers") or []
        member_ids = {
            str(member.get("DBInstanceIdentifier") or "")
            for member in members
            if isinstance(member, Mapping)
        }
        member_shape_owned = not members or (
            len(members) == 1
            and isinstance(members[0], Mapping)
            and str(members[0].get("DBInstanceIdentifier") or "") == writer_id
            and members[0].get("IsClusterWriter") is True
        )
        topology_valid = (
            member_ids.issubset({writer_id})
            and member_shape_owned
            and str(cluster.get("Engine") or "").lower() == "aurora-postgresql"
            and str(cluster.get("DBSubnetGroup") or "")
            == self.adapter.config.db_subnet_group_name
            and groups == {self.adapter.config.security_group_id}
        )
        owned = topology_valid and all(
            tags.get(key) == value for key, value in expected_tags.items()
        )
        writer = await self.adapter._describe_instance(writer_id, missing_ok=True)
        if writer is not None:
            writer_tags = await self.adapter._resource_tags(
                str(writer.get("DBInstanceArn") or "")
            )
            writer_valid = (
                all(writer_tags.get(key) == value for key, value in expected_tags.items())
                and str(writer.get("DBClusterIdentifier") or "") == plan.artifact_id
                and str(writer.get("Engine") or "").lower() == "aurora-postgresql"
                and str(writer.get("DBInstanceClass") or "") == "db.serverless"
            )
            topology_valid = topology_valid and writer_valid
            owned = owned and writer_valid
        return ArtifactInspection(
            artifact_id=plan.artifact_id,
            provider=plan.provider,
            source_id=plan.source_id if owned else "",
            run_id=plan.scope.run_id if owned else "",
            owner=plan.scope.owner if owned else "",
            state=(
                f"{str(cluster.get('Status') or '').upper()}/"
                f"{str((writer or {}).get('DBInstanceStatus') or 'ABSENT').upper()}"
            ),
            aws_account_id=plan.scope.aws_account_id,
            aws_region=plan.scope.aws_region,
            metadata={
                "writer_id": writer_id,
                "member_ids": sorted(member_ids),
                "topology_valid": topology_valid,
                "endpoint": str(cluster.get("Endpoint") or ""),
                "port": int(cluster.get("Port") or 5432),
                "recovery_contract": tags.get(_CONTRACT_TAG, ""),
            },
        )

    def _assert_owned(self, plan: RecoveryPlan, artifact: ArtifactInspection) -> None:
        self._assert_plan(plan)
        if (
            artifact.artifact_id != plan.artifact_id
            or artifact.provider != plan.provider
            or artifact.owner != plan.scope.owner
            or artifact.run_id != plan.scope.run_id
            or artifact.source_id != plan.source_id
            or artifact.aws_account_id != plan.scope.aws_account_id
            or artifact.aws_region != plan.scope.aws_region
            or artifact.metadata.get("topology_valid") is not True
            or artifact.metadata.get("recovery_contract") != _CONTRACT_VALUE
        ):
            raise UnsafeCleanupError("Aurora recovery cluster ownership mismatch")

    async def create_recovery(
        self,
        plan: RecoveryPlan,
        recovery_at: datetime,
        report: RecoveryReporter,
    ) -> ArtifactInspection:
        self._assert_plan(plan)
        source = self._recovery_source
        source_writer = self._recovery_writer
        if source is None or source_writer is None:
            raise SafeChangeLiveConfigurationError(
                "Aurora recovery source was not revalidated before the barrier"
            )
        restore_arguments: dict[str, Any] = {
            "SourceDBClusterIdentifier": self.source_id,
            "DBClusterIdentifier": plan.artifact_id,
            "RestoreToTime": recovery_at,
            "DBSubnetGroupName": self.adapter.config.db_subnet_group_name,
            "VpcSecurityGroupIds": [self.adapter.config.security_group_id],
            "Port": int(source.get("Port") or 5432),
            "DeletionProtection": False,
            "CopyTagsToSnapshot": False,
            "Tags": self._tags(plan),
        }
        scaling = source.get("ServerlessV2ScalingConfiguration") or {}
        if isinstance(scaling, Mapping) and scaling:
            restore_arguments["ServerlessV2ScalingConfiguration"] = {
                key: scaling[key]
                for key in ("MinCapacity", "MaxCapacity", "SecondsUntilAutoPause")
                if key in scaling
            }
        network_type = str(source.get("NetworkType") or "")
        if network_type:
            restore_arguments["NetworkType"] = network_type
        restore_wire_call = (
            "RDS.RestoreDBClusterToPointInTime("
            f"SourceDBClusterIdentifier={self.source_id}, "
            f"DBClusterIdentifier={plan.artifact_id}, "
            f"RestoreToTime={recovery_at.isoformat()})"
        )
        await report(
            "Requesting the Aurora full-copy PITR recovery cluster",
            restore_wire_call,
        )
        await self.adapter._call(
            "rds",
            "restore_db_cluster_to_point_in_time",
            mutation=True,
            **restore_arguments,
        )
        await report(
            "Aurora full-copy PITR recovery cluster request accepted",
            restore_wire_call,
        )
        await report(
            "Waiting for the Aurora full-copy PITR recovery cluster",
            "RDS.DescribeDBClusters",
        )

        async def cluster_ready():
            cluster = await self.adapter._describe_cluster(plan.artifact_id)
            return cluster if str(cluster.get("Status") or "").lower() == "available" else None

        await self.adapter._wait_for(cluster_ready, description="Aurora recovery cluster")
        writer_id = _aws_child_id(plan.artifact_id, "writer")
        writer_wire_call = (
            "RDS.CreateDBInstance("
            f"DBInstanceIdentifier={writer_id}, "
            f"DBClusterIdentifier={plan.artifact_id}, "
            "DBInstanceClass=db.serverless)"
        )
        await report(
            "Requesting the Aurora recovery cluster writer",
            writer_wire_call,
        )
        await self.adapter._call(
            "rds",
            "create_db_instance",
            mutation=True,
            DBInstanceIdentifier=writer_id,
            DBClusterIdentifier=plan.artifact_id,
            DBInstanceClass="db.serverless",
            Engine="aurora-postgresql",
            PubliclyAccessible=bool(source_writer.get("PubliclyAccessible", True)),
            AutoMinorVersionUpgrade=False,
            PromotionTier=0,
            Tags=self._tags(plan),
        )
        await report(
            "Aurora recovery cluster writer request accepted",
            writer_wire_call,
        )
        await report(
            "Waiting for the Aurora recovery cluster writer",
            "RDS.DescribeDBInstances",
        )

        async def writer_ready():
            writer = await self.adapter._describe_instance(writer_id)
            return (
                writer
                if str(writer.get("DBInstanceStatus") or "").lower() == "available"
                else None
            )

        await self.adapter._wait_for(writer_ready, description="Aurora recovery writer")
        artifact = await self.inspect_recovery(plan)
        assert artifact is not None
        self._assert_owned(plan, artifact)
        return artifact

    async def connect_recovery(self, plan: RecoveryPlan, artifact: ArtifactInspection):
        self._assert_owned(plan, artifact)
        credentials = await self.adapter._source_credentials()
        return await self.adapter._connect_target(
            host=str(artifact.metadata.get("endpoint") or ""),
            port=int(artifact.metadata.get("port") or 5432),
            credentials=credentials,
        )

    async def delete_recovery(
        self,
        plan: RecoveryPlan,
        artifact: ArtifactInspection,
        report: RecoveryReporter,
    ) -> None:
        self._assert_owned(plan, artifact)
        current = await self.inspect_recovery(plan)
        if current is None:
            return
        self._assert_owned(plan, current)
        writer_id = str(current.metadata.get("writer_id") or "")
        writer = await self.adapter._describe_instance(writer_id, missing_ok=True)
        if writer is not None:
            await self.adapter._delete_when_deletable(
                "delete_db_instance",
                identifier=writer_id,
                observe=lambda: self._instance_status(writer_id),
                absent_codes=_ABSENT_INSTANCE_CODES,
                undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
                description="the owned Aurora recovery writer",
                report=report,
                wire_call="RDS.DescribeDBInstances",
                DBInstanceIdentifier=writer_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
            await report(
                "Waiting for the owned Aurora recovery writer deletion",
                "RDS.DescribeDBInstances",
            )

            async def writer_absent():
                writer = await self.adapter._describe_instance(writer_id, missing_ok=True)
                return True if writer is None else None

            await self.adapter._wait_for(
                writer_absent,
                description="Aurora recovery writer deletion",
            )
        cluster = await self.adapter._describe_cluster(
            plan.artifact_id,
            missing_ok=True,
        )
        if cluster is not None:
            await self.adapter._delete_when_deletable(
                "delete_db_cluster",
                identifier=plan.artifact_id,
                observe=lambda: self._cluster_status(plan.artifact_id),
                absent_codes=_ABSENT_CLUSTER_CODES,
                undeletable_codes=_UNDELETABLE_CLUSTER_CODES,
                description="the owned Aurora recovery cluster",
                report=report,
                wire_call="RDS.DescribeDBClusters",
                DBClusterIdentifier=plan.artifact_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
            await report(
                "Waiting for the owned Aurora recovery cluster deletion",
                "RDS.DescribeDBClusters",
            )

            async def cluster_absent():
                cluster = await self.adapter._describe_cluster(
                    plan.artifact_id,
                    missing_ok=True,
                )
                return True if cluster is None else None

            await self.adapter._wait_for(
                cluster_absent,
                description="Aurora recovery cluster deletion",
            )

    async def abandon_recovery(self, plan: RecoveryPlan) -> None:
        """Issue teardown for a cancelled lane without waiting for it.

        Same reasoning and same ordering as the Round 2 Aurora clone: the writer
        is the compute, so its delete goes out first and unconditionally, and the
        cluster is attempted regardless of how the writer went.
        """

        self._assert_plan(plan)
        writer_id = _aws_child_id(plan.artifact_id, "writer")
        try:
            await self.adapter._issue_delete(
                "delete_db_instance",
                identifier=writer_id,
                absent_codes=_ABSENT_INSTANCE_CODES,
                undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
                DBInstanceIdentifier=writer_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
        finally:
            await self.adapter._issue_delete(
                "delete_db_cluster",
                identifier=plan.artifact_id,
                absent_codes=_ABSENT_CLUSTER_CODES,
                undeletable_codes=_UNDELETABLE_CLUSTER_CODES,
                DBClusterIdentifier=plan.artifact_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )


class RdsRecoveryAdapter(_AwsRecoveryAdapter):
    provider = SafeChangeProvider.RDS

    def __init__(self, adapter: RdsSafeChangeAdapter) -> None:
        self.adapter = adapter
        self.name = adapter.name
        self.source_id = adapter.source_id
        self._recovery_source: Mapping[str, Any] | None = None

    @property
    def _source_wire_call(self) -> str:
        return (
            "RDS.DescribeDBInstances("
            f"DBInstanceIdentifier={self.source_id}) → LatestRestorableTime"
        )

    async def _source_control(self) -> Mapping[str, Any]:
        source = await self.adapter._source()
        self._recovery_source = source
        return source

    async def _restorable_window(
        self,
        plan: RecoveryPlan,
        report: RecoveryReporter | None = None,
    ) -> tuple[datetime, datetime]:
        self._assert_plan(plan)
        source = await self._source_control()
        earliest = source.get("EarliestRestorableTime")
        latest = source.get("LatestRestorableTime")
        if isinstance(earliest, datetime) and isinstance(latest, datetime):
            return earliest, latest

        def aware(value: object) -> bool:
            return (
                isinstance(value, datetime)
                and value.tzinfo is not None
                and value.utcoffset() is not None
            )

        if earliest is not None or not aware(latest):
            raise SafeChangeLiveConfigurationError("AWS restorable window is unavailable")

        if report is not None:
            await report(
                "Using the exact active automated-backup restore-window fallback",
                (
                    "RDS.DescribeDBInstanceAutomatedBackups("
                    f"DBInstanceIdentifier={self.source_id}) → RestoreWindow"
                ),
            )
        response = await self.adapter._call(
            "rds",
            "describe_db_instance_automated_backups",
            DBInstanceIdentifier=self.source_id,
        )
        backups = response.get("DBInstanceAutomatedBackups")
        backup = backups[0] if isinstance(backups, list) and len(backups) == 1 else None
        restore_window = backup.get("RestoreWindow") if isinstance(backup, Mapping) else None
        backup_earliest = (
            restore_window.get("EarliestTime")
            if isinstance(restore_window, Mapping)
            else None
        )
        backup_latest = (
            restore_window.get("LatestTime")
            if isinstance(restore_window, Mapping)
            else None
        )
        source_arn = str(source.get("DBInstanceArn") or "")
        source_resource_id = str(source.get("DbiResourceId") or "")
        if (
            response.get("Marker")
            or not isinstance(backup, Mapping)
            or str(backup.get("Status") or "").lower() != "active"
            or str(backup.get("DBInstanceIdentifier") or "") != self.source_id
            or not source_arn
            or str(backup.get("DBInstanceArn") or "") != source_arn
            or not source_resource_id
            or str(backup.get("DbiResourceId") or "") != source_resource_id
            or str(backup.get("Region") or "") != self.adapter.config.region
            or not aware(backup_earliest)
            or not aware(backup_latest)
            or backup_earliest > backup_latest
            or backup_latest != latest
        ):
            raise SafeChangeLiveConfigurationError("AWS restorable window is unavailable")
        return backup_earliest, backup_latest

    async def inspect_recovery(self, plan: RecoveryPlan) -> ArtifactInspection | None:
        self._assert_plan(plan)
        await self.adapter._assert_identity()
        instance = await self.adapter._describe_instance(plan.artifact_id, missing_ok=True)
        if instance is None:
            return None
        tags = await self.adapter._resource_tags(str(instance.get("DBInstanceArn") or ""))
        subnet = instance.get("DBSubnetGroup") or {}
        groups = {
            str(group.get("VpcSecurityGroupId") or "")
            for group in instance.get("VpcSecurityGroups") or []
            if isinstance(group, Mapping)
        }
        expected_tags = self.adapter._tag_mapping(self._tags(plan))
        topology_valid = (
            str(instance.get("Engine") or "").lower() == "postgres"
            and not str(instance.get("DBClusterIdentifier") or "")
            and str(subnet.get("DBSubnetGroupName") or "")
            == self.adapter.config.db_subnet_group_name
            and groups == {self.adapter.config.security_group_id}
        )
        owned = topology_valid and all(
            tags.get(key) == value for key, value in expected_tags.items()
        )
        endpoint = instance.get("Endpoint") or {}
        return ArtifactInspection(
            artifact_id=plan.artifact_id,
            provider=plan.provider,
            source_id=plan.source_id if owned else "",
            run_id=plan.scope.run_id if owned else "",
            owner=plan.scope.owner if owned else "",
            state=str(instance.get("DBInstanceStatus") or "").upper(),
            aws_account_id=plan.scope.aws_account_id,
            aws_region=plan.scope.aws_region,
            metadata={
                "endpoint": str(endpoint.get("Address") or ""),
                "port": int(endpoint.get("Port") or 5432),
                "topology_valid": topology_valid,
                "recovery_contract": tags.get(_CONTRACT_TAG, ""),
            },
        )

    def _assert_owned(self, plan: RecoveryPlan, artifact: ArtifactInspection) -> None:
        self._assert_plan(plan)
        if (
            artifact.artifact_id != plan.artifact_id
            or artifact.provider != plan.provider
            or artifact.owner != plan.scope.owner
            or artifact.run_id != plan.scope.run_id
            or artifact.source_id != plan.source_id
            or artifact.aws_account_id != plan.scope.aws_account_id
            or artifact.aws_region != plan.scope.aws_region
            or artifact.metadata.get("topology_valid") is not True
            or artifact.metadata.get("recovery_contract") != _CONTRACT_VALUE
        ):
            raise UnsafeCleanupError("RDS recovery restore ownership mismatch")

    async def create_recovery(
        self,
        plan: RecoveryPlan,
        recovery_at: datetime,
        report: RecoveryReporter,
    ) -> ArtifactInspection:
        self._assert_plan(plan)
        source = self._recovery_source
        if source is None:
            raise SafeChangeLiveConfigurationError(
                "RDS recovery source was not revalidated before the barrier"
            )
        restore_arguments: dict[str, Any] = {
            "SourceDBInstanceIdentifier": self.source_id,
            "TargetDBInstanceIdentifier": plan.artifact_id,
            # RestoreDBInstanceToPointInTime names this `RestoreTime`, while the
            # cluster API RestoreDBClusterToPointInTime names the same concept
            # `RestoreToTime`. The asymmetry is real; do not unify them.
            "RestoreTime": recovery_at,
            "DBInstanceClass": str(source.get("DBInstanceClass") or ""),
            "DBSubnetGroupName": self.adapter.config.db_subnet_group_name,
            "VpcSecurityGroupIds": [self.adapter.config.security_group_id],
            "PubliclyAccessible": bool(source.get("PubliclyAccessible", True)),
            "MultiAZ": False,
            "AutoMinorVersionUpgrade": False,
            "DeletionProtection": False,
            "CopyTagsToSnapshot": False,
            "Tags": self._tags(plan),
        }
        network_type = str(source.get("NetworkType") or "")
        if network_type:
            restore_arguments["NetworkType"] = network_type
        restore_wire_call = (
            "RDS.RestoreDBInstanceToPointInTime("
            f"SourceDBInstanceIdentifier={self.source_id}, "
            f"TargetDBInstanceIdentifier={plan.artifact_id}, "
            f"RestoreTime={recovery_at.isoformat()})"
        )
        await report(
            "Requesting the RDS PostgreSQL PITR recovery restore",
            restore_wire_call,
        )
        await self.adapter._call(
            "rds",
            "restore_db_instance_to_point_in_time",
            mutation=True,
            **restore_arguments,
        )
        await report(
            "RDS PostgreSQL PITR recovery restore request accepted",
            restore_wire_call,
        )
        await report(
            "Waiting for the RDS PostgreSQL PITR recovery restore",
            "RDS.DescribeDBInstances",
        )

        async def ready():
            instance = await self.adapter._describe_instance(plan.artifact_id)
            return (
                instance
                if str(instance.get("DBInstanceStatus") or "").lower() in _RDS_SERVING_STATES
                else None
            )

        await self.adapter._wait_for(ready, description="RDS recovery restore")
        artifact = await self.inspect_recovery(plan)
        assert artifact is not None
        self._assert_owned(plan, artifact)
        return artifact

    async def connect_recovery(self, plan: RecoveryPlan, artifact: ArtifactInspection):
        self._assert_owned(plan, artifact)
        credentials = await self.adapter._source_credentials()
        return await self.adapter._connect_target(
            host=str(artifact.metadata.get("endpoint") or ""),
            port=int(artifact.metadata.get("port") or 5432),
            credentials=credentials,
        )

    async def delete_recovery(
        self,
        plan: RecoveryPlan,
        artifact: ArtifactInspection,
        report: RecoveryReporter,
    ) -> None:
        self._assert_owned(plan, artifact)
        current = await self.inspect_recovery(plan)
        if current is None:
            return
        self._assert_owned(plan, current)
        instance = await self.adapter._describe_instance(plan.artifact_id)
        assert instance is not None
        await self.adapter._delete_when_deletable(
            "delete_db_instance",
            identifier=plan.artifact_id,
            observe=lambda: self._instance_status(plan.artifact_id),
            absent_codes=_ABSENT_INSTANCE_CODES,
            undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
            description="the owned RDS PITR recovery restore",
            report=report,
            wire_call="RDS.DescribeDBInstances",
            DBInstanceIdentifier=plan.artifact_id,
            SkipFinalSnapshot=True,
            DeleteAutomatedBackups=True,
        )
        await report(
            "Waiting for the owned RDS PITR recovery restore deletion",
            "RDS.DescribeDBInstances",
        )

        async def absent():
            instance = await self.adapter._describe_instance(
                plan.artifact_id,
                missing_ok=True,
            )
            return True if instance is None else None

        await self.adapter._wait_for(
            absent,
            description="RDS recovery restore deletion",
        )

    async def abandon_recovery(self, plan: RecoveryPlan) -> None:
        """Issue teardown for a cancelled lane without waiting for it."""

        self._assert_plan(plan)
        await self.adapter._issue_delete(
            "delete_db_instance",
            identifier=plan.artifact_id,
            absent_codes=_ABSENT_INSTANCE_CODES,
            undeletable_codes=_UNDELETABLE_INSTANCE_CODES,
            DBInstanceIdentifier=plan.artifact_id,
            SkipFinalSnapshot=True,
            DeleteAutomatedBackups=True,
        )


def build_recovery_engine(
    manifest: DemoManifest | None = None,
    *,
    cleanup_only: bool = False,
) -> RecoveryEngine:
    owned = manifest or load_manifest()
    safe_change = build_safe_change_engine(
        owned, cleanup_only=cleanup_only, round_number=3
    )
    lakebase = LakebaseRecoveryAdapter(safe_change.lakebase)  # type: ignore[arg-type]
    competitors: dict[CompetitorId, RecoveryAdapter] = {
        CompetitorId.AURORA_SERVERLESS_V2: AuroraRecoveryAdapter(
            safe_change.competitors[CompetitorId.AURORA_SERVERLESS_V2]  # type: ignore[arg-type]
        ),
        CompetitorId.RDS_POSTGRES: RdsRecoveryAdapter(
            safe_change.competitors[CompetitorId.RDS_POSTGRES]  # type: ignore[arg-type]
        ),
    }
    return RecoveryEngine(
        scope=safe_change.scope,
        lakebase=lakebase,
        competitors=competitors,
        arm_ttl_seconds=safe_change.arm_ttl_seconds,
        run_timeout_seconds=safe_change.run_timeout_seconds,
        reset_timeout_seconds=safe_change.reset_timeout_seconds,
    )


__all__ = [
    "AuroraRecoveryAdapter",
    "LakebaseRecoveryAdapter",
    "RdsRecoveryAdapter",
    "build_recovery_engine",
]
