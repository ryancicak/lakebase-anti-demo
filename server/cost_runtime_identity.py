"""Exact identities for per-bout resources created while a round is running.

Static v7 identities live in :mod:`server.cost_identity`.  This module covers
the resources that do not exist until Round 2, 3, or 5 starts.  It deliberately
accepts only authoritative adapter inspections and the fenced Round 5 creation
journal.  A deterministic display name or a time window is never promoted to a
provider billing identity.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from .connection_spike_journal import JournalEvent, LifecycleState
from .connection_spike_live import ConnectionSpikeSetupResult
from .models import RoundId
from .recovery import RecoveryArm, RecoveryRunResult, RecoveryStoppedResult
from .safe_change import (
    ArtifactInspection,
    SafeChangeArm,
    SafeChangeProvider,
    SafeChangeRunResult,
)


class RuntimeIdentityStatus(StrEnum):
    READY = "ready"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class RuntimeResourceIdentity:
    """One exact runtime provider identity.

    Some provider records expose an ARN before they expose a billing resource
    ID.  Keeping the two fields separate prevents an ARN, identifier, hostname,
    or deterministic name from being silently relabelled as a resource ID.
    """

    lane_id: str
    provider: str
    resource_type: str
    resource_id: str | None = None
    resource_arn: str | None = None

    def __post_init__(self) -> None:
        if not self.lane_id.strip() or not self.provider.strip() or not self.resource_type.strip():
            raise ValueError("runtime resource identity labels must not be empty")
        if not (self.resource_id or self.resource_arn):
            raise ValueError("runtime resource identity requires an exact ID or ARN")


@dataclass(frozen=True, slots=True)
class RuntimeIdentityQuarantine:
    lane_id: str
    provider: str
    resource_type: str
    missing_fields: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class RuntimeIdentityCapture:
    round_id: RoundId
    status: RuntimeIdentityStatus
    resources: tuple[RuntimeResourceIdentity, ...]
    quarantine: tuple[RuntimeIdentityQuarantine, ...]


_PROXY_ARN = re.compile(
    r"arn:[a-z0-9-]+:rds:[a-z0-9-]+:[0-9]{12}:db-proxy:[A-Za-z0-9-]+"
)
_SECURITY_GROUP_ID = re.compile(r"sg-[0-9a-f]+")


def extract_round2_runtime_identity(
    arm: SafeChangeArm,
    result: SafeChangeRunResult,
    artifacts: Mapping[str, ArtifactInspection],
) -> RuntimeIdentityCapture:
    """Extract Round 2 identities from final authoritative inspections."""

    return _extract_artifact_identities(
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        arm=arm,
        result=result,
        artifacts=artifacts,
    )


def extract_round3_runtime_identity(
    arm: RecoveryArm,
    result: RecoveryRunResult | RecoveryStoppedResult,
    artifacts: Mapping[str, ArtifactInspection],
) -> RuntimeIdentityCapture:
    """Extract Round 3 identities from final authoritative inspections."""

    return _extract_artifact_identities(
        RoundId.RECOVER_DELETED_ORDER,
        arm=arm,
        result=result,
        artifacts=artifacts,
    )


def extract_round5_runtime_identity(
    setup_result: ConnectionSpikeSetupResult,
    journal_events: Sequence[JournalEvent],
) -> RuntimeIdentityCapture:
    """Extract the exact Proxy, target, and network identity from its journal."""

    resources: list[RuntimeResourceIdentity] = []
    quarantine: list[RuntimeIdentityQuarantine] = []
    bout_id = setup_result.bout_id.strip()
    if not bout_id:
        _quarantine(
            quarantine,
            lane_id="competitor",
            provider="aws",
            resource_type="round5_journal_scope",
            fields=("bout_id",),
            reason="the trusted setup result has no bout ID",
        )
        return _capture(RoundId.SURVIVE_CONNECTION_SPIKE, resources, quarantine)

    if any(event.bout_id != bout_id for event in journal_events):
        _quarantine(
            quarantine,
            lane_id="competitor",
            provider="aws",
            resource_type="round5_journal_scope",
            fields=("bout_id",),
            reason="journal events are not all bound to the trusted setup bout",
        )
        return _capture(RoundId.SURVIVE_CONNECTION_SPIKE, resources, quarantine)

    scopes = {(event.fencing_token, event.runtime_seal_sha256) for event in journal_events}
    if len(scopes) != 1:
        _quarantine(
            quarantine,
            lane_id="competitor",
            provider="aws",
            resource_type="round5_journal_scope",
            fields=("fencing_token", "runtime_seal_sha256"),
            reason="journal events do not have one exact fenced runtime scope",
        )
        return _capture(RoundId.SURVIVE_CONNECTION_SPIKE, resources, quarantine)

    created_by_kind: dict[str, list[JournalEvent]] = {}
    for event in journal_events:
        if event.lifecycle_state is LifecycleState.CREATED:
            created_by_kind.setdefault(event.resource_kind, []).append(event)

    required = ("proxy_security_group", "rds_proxy", "proxy_target")
    selected: dict[str, JournalEvent] = {}
    for kind in required:
        candidates = created_by_kind.get(kind, [])
        if len(candidates) != 1 or not candidates[0].provider_id:
            _quarantine(
                quarantine,
                lane_id="competitor",
                provider="aws",
                resource_type=kind,
                fields=("provider_id",),
                reason="the fenced journal does not contain exactly one created provider identity",
            )
            continue
        event = candidates[0]
        tags = event.metadata.get("tags")
        if not isinstance(tags, Mapping) or tags.get("anti-demo-bout-id") != bout_id:
            _quarantine(
                quarantine,
                lane_id="competitor",
                provider="aws",
                resource_type=kind,
                fields=("metadata.tags.anti-demo-bout-id",),
                reason="the created identity is not tagged to the trusted setup bout",
            )
            continue
        selected[kind] = event

    security_group = selected.get("proxy_security_group")
    if security_group is not None:
        group_id = str(security_group.provider_id)
        if _SECURITY_GROUP_ID.fullmatch(group_id) is None:
            _quarantine(
                quarantine,
                lane_id="competitor",
                provider="aws",
                resource_type="proxy_security_group",
                fields=("provider_id",),
                reason="the journaled provider identity is not an EC2 security-group ID",
            )
        else:
            resources.append(
                RuntimeResourceIdentity(
                    lane_id="competitor",
                    provider="aws",
                    resource_type="proxy_security_group",
                    resource_id=group_id,
                )
            )

    proxy = selected.get("rds_proxy")
    if proxy is not None:
        proxy_arn = str(proxy.provider_id)
        if _PROXY_ARN.fullmatch(proxy_arn) is None:
            _quarantine(
                quarantine,
                lane_id="competitor",
                provider="aws",
                resource_type="rds_proxy",
                fields=("resource_arn",),
                reason="the journaled provider identity is not an RDS Proxy ARN",
            )
        else:
            resources.append(
                RuntimeResourceIdentity(
                    lane_id="competitor",
                    provider="aws",
                    resource_type="rds_proxy",
                    resource_arn=proxy_arn,
                )
            )

    target = selected.get("proxy_target")
    if target is not None:
        target_id = str(target.provider_id)
        metadata_id = target.metadata.get("competitor_resource_id")
        if not target_id or not isinstance(metadata_id, str) or target_id != metadata_id:
            _quarantine(
                quarantine,
                lane_id="competitor",
                provider="aws",
                resource_type="rds_proxy_target",
                fields=("provider_id", "metadata.competitor_resource_id"),
                reason="the Proxy target does not match its exact journaled database resource ID",
            )
        else:
            resources.append(
                RuntimeResourceIdentity(
                    lane_id="competitor",
                    provider="aws",
                    resource_type="rds_proxy_target",
                    resource_id=target_id,
                )
            )

    return _capture(RoundId.SURVIVE_CONNECTION_SPIKE, resources, quarantine)


def _extract_artifact_identities(
    round_id: RoundId,
    *,
    arm: object,
    result: object,
    artifacts: Mapping[str, ArtifactInspection],
) -> RuntimeIdentityCapture:
    resources: list[RuntimeResourceIdentity] = []
    quarantine: list[RuntimeIdentityQuarantine] = []
    arm_lanes = getattr(arm, "lanes", {})
    result_lanes = getattr(result, "lanes", {})
    scope = getattr(arm, "scope", None)
    if not isinstance(arm_lanes, Mapping) or not isinstance(result_lanes, Mapping) or scope is None:
        _quarantine(
            quarantine,
            lane_id="all",
            provider="all",
            resource_type="runtime_evidence",
            fields=("arm.lanes", "result.lanes", "arm.scope"),
            reason="trusted arm/result evidence is incomplete",
        )
        return _capture(round_id, resources, quarantine)

    lane_ids = set(arm_lanes) | set(result_lanes)
    for lane_id in sorted(str(value) for value in lane_ids):
        lane_arm = arm_lanes.get(lane_id)
        lane_result = result_lanes.get(lane_id)
        plan = getattr(lane_arm, "plan", None)
        provider_value = getattr(plan, "provider", "unknown")
        provider = getattr(provider_value, "value", str(provider_value))
        if plan is None or lane_result is None:
            _quarantine(
                quarantine,
                lane_id=lane_id,
                provider=provider,
                resource_type="artifact_inspection",
                fields=("arm_lane", "result_lane"),
                reason="arm and result do not contain the same runtime lane",
            )
            continue
        artifact = artifacts.get(lane_id)
        if artifact is None:
            _quarantine(
                quarantine,
                lane_id=lane_id,
                provider=provider,
                resource_type="artifact_inspection",
                fields=("artifact",),
                reason="no final authoritative artifact inspection was supplied",
            )
            continue
        if not _artifact_matches(plan, lane_result, artifact, scope):
            _quarantine(
                quarantine,
                lane_id=lane_id,
                provider=provider,
                resource_type="artifact_inspection",
                fields=("ownership_identity",),
                reason="artifact inspection does not match its trusted arm and result",
            )
            continue

        if plan.provider is SafeChangeProvider.LAKEBASE:
            _extract_lakebase(lane_id, artifact, resources, quarantine)
        elif plan.provider is SafeChangeProvider.AURORA:
            _extract_aurora(lane_id, artifact, resources, quarantine)
        elif plan.provider is SafeChangeProvider.RDS:
            _extract_rds(lane_id, artifact, resources, quarantine)
        else:
            _quarantine(
                quarantine,
                lane_id=lane_id,
                provider=provider,
                resource_type="artifact_inspection",
                fields=("provider",),
                reason="runtime artifact provider is unsupported",
            )

    return _capture(round_id, resources, quarantine)


def _artifact_matches(
    plan: object,
    result: object,
    artifact: ArtifactInspection,
    scope: object,
) -> bool:
    if (
        artifact.artifact_id != getattr(plan, "artifact_id", None)
        or artifact.artifact_id != getattr(result, "artifact_id", None)
        or artifact.provider != getattr(plan, "provider", None)
        or artifact.provider != getattr(result, "provider", None)
        or artifact.source_id != getattr(plan, "source_id", None)
        or artifact.run_id != getattr(scope, "run_id", None)
        or artifact.owner != getattr(scope, "owner", None)
    ):
        return False
    if artifact.provider is SafeChangeProvider.LAKEBASE:
        return True
    return (
        artifact.aws_account_id == getattr(scope, "aws_account_id", None)
        and artifact.aws_region == getattr(scope, "aws_region", None)
    )


def _extract_lakebase(
    lane_id: str,
    artifact: ArtifactInspection,
    resources: list[RuntimeResourceIdentity],
    quarantine: list[RuntimeIdentityQuarantine],
) -> None:
    branch = artifact.metadata.get("branch_name")
    endpoint = artifact.metadata.get("ownership_endpoint")
    valid_branch = isinstance(branch, str) and branch.endswith(f"/branches/{artifact.artifact_id}")
    valid_endpoint = (
        isinstance(endpoint, str)
        and valid_branch
        and endpoint.startswith(f"{branch}/endpoints/")
    )
    if not valid_branch or not valid_endpoint:
        _quarantine(
            quarantine,
            lane_id=lane_id,
            provider="databricks",
            resource_type="lakebase_runtime_branch_endpoint",
            fields=("metadata.branch_name", "metadata.ownership_endpoint"),
            reason="exact Lakebase branch or endpoint resource name is missing or inconsistent",
        )
        return
    resources.extend(
        (
            RuntimeResourceIdentity(
                lane_id=lane_id,
                provider="databricks",
                resource_type="lakebase_runtime_branch",
                resource_id=branch,
            ),
            RuntimeResourceIdentity(
                lane_id=lane_id,
                provider="databricks",
                resource_type="lakebase_runtime_endpoint",
                resource_id=endpoint,
            ),
        )
    )


def _extract_aurora(
    lane_id: str,
    artifact: ArtifactInspection,
    resources: list[RuntimeResourceIdentity],
    quarantine: list[RuntimeIdentityQuarantine],
) -> None:
    metadata = artifact.metadata
    _extract_aws_resource(
        lane_id,
        artifact,
        resource_type="aurora_runtime_cluster",
        arn_key="cluster_arn",
        id_key="cluster_resource_id",
        arn_prefix="cluster:",
        resources=resources,
        quarantine=quarantine,
    )
    _extract_aws_resource(
        lane_id,
        artifact,
        resource_type="aurora_runtime_writer",
        arn_key="writer_arn",
        id_key="writer_resource_id",
        arn_prefix="db:",
        resources=resources,
        quarantine=quarantine,
    )
    _extract_network_identities(lane_id, metadata, resources, quarantine)


def _extract_rds(
    lane_id: str,
    artifact: ArtifactInspection,
    resources: list[RuntimeResourceIdentity],
    quarantine: list[RuntimeIdentityQuarantine],
) -> None:
    _extract_aws_resource(
        lane_id,
        artifact,
        resource_type="rds_runtime_instance",
        arn_key="instance_arn",
        id_key="instance_resource_id",
        arn_prefix="db:",
        resources=resources,
        quarantine=quarantine,
    )
    _extract_network_identities(lane_id, artifact.metadata, resources, quarantine)


def _extract_aws_resource(
    lane_id: str,
    artifact: ArtifactInspection,
    *,
    resource_type: str,
    arn_key: str,
    id_key: str,
    arn_prefix: str,
    resources: list[RuntimeResourceIdentity],
    quarantine: list[RuntimeIdentityQuarantine],
) -> None:
    raw_arn = artifact.metadata.get(arn_key)
    raw_id = artifact.metadata.get(id_key)
    arn = raw_arn.strip() if isinstance(raw_arn, str) else ""
    resource_id = raw_id.strip() if isinstance(raw_id, str) else ""
    expected = (
        f"arn:aws:rds:{artifact.aws_region}:{artifact.aws_account_id}:{arn_prefix}"
        if artifact.aws_region and artifact.aws_account_id
        else ""
    )
    valid_arn = bool(arn and expected and arn.startswith(expected))
    if valid_arn or resource_id:
        resources.append(
            RuntimeResourceIdentity(
                lane_id=lane_id,
                provider="aws",
                resource_type=resource_type,
                resource_id=resource_id or None,
                resource_arn=arn if valid_arn else None,
            )
        )
    missing = tuple(
        field
        for field, present in ((arn_key, valid_arn), (id_key, bool(resource_id)))
        if not present
    )
    if missing:
        _quarantine(
            quarantine,
            lane_id=lane_id,
            provider="aws",
            resource_type=resource_type,
            fields=missing,
            reason="authoritative inspection omitted an exact AWS ARN or billing resource ID",
        )


def _extract_network_identities(
    lane_id: str,
    metadata: Mapping[str, object],
    resources: list[RuntimeResourceIdentity],
    quarantine: list[RuntimeIdentityQuarantine],
) -> None:
    security_groups = _exact_string_sequence(metadata.get("security_group_ids"))
    if security_groups is None:
        _quarantine(
            quarantine,
            lane_id=lane_id,
            provider="aws",
            resource_type="runtime_security_group",
            fields=("security_group_ids",),
            reason="authoritative inspection omitted exact attached security-group IDs",
        )
    else:
        resources.extend(
            RuntimeResourceIdentity(
                lane_id=lane_id,
                provider="aws",
                resource_type="runtime_security_group",
                resource_id=group_id,
            )
            for group_id in security_groups
        )

    public_ipv4 = _exact_string_sequence(metadata.get("public_ipv4_addresses"))
    if public_ipv4 is None:
        _quarantine(
            quarantine,
            lane_id=lane_id,
            provider="aws",
            resource_type="runtime_public_ipv4",
            fields=("public_ipv4_addresses",),
            reason="authoritative inspection omitted exact temporary public IPv4 identities",
        )
    else:
        resources.extend(
            RuntimeResourceIdentity(
                lane_id=lane_id,
                provider="aws",
                resource_type="runtime_public_ipv4",
                resource_id=address,
            )
            for address in public_ipv4
        )


def _exact_string_sequence(value: object) -> tuple[str, ...] | None:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return None
    items = tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
    if not items or len(items) != len(value) or len(items) != len(set(items)):
        return None
    return items


def _quarantine(
    values: list[RuntimeIdentityQuarantine],
    *,
    lane_id: str,
    provider: str,
    resource_type: str,
    fields: tuple[str, ...],
    reason: str,
) -> None:
    values.append(
        RuntimeIdentityQuarantine(
            lane_id=lane_id,
            provider=provider,
            resource_type=resource_type,
            missing_fields=fields,
            reason=reason,
        )
    )


def _capture(
    round_id: RoundId,
    resources: list[RuntimeResourceIdentity],
    quarantine: list[RuntimeIdentityQuarantine],
) -> RuntimeIdentityCapture:
    return RuntimeIdentityCapture(
        round_id=round_id,
        status=(RuntimeIdentityStatus.QUARANTINED if quarantine else RuntimeIdentityStatus.READY),
        resources=tuple(resources),
        quarantine=tuple(quarantine),
    )
