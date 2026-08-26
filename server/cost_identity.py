"""Fail-closed provider identities used to reconcile one bout's cost.

This module deliberately does not query either provider.  It only turns the
immutable identities already sealed in a v7 manifest into a bounded attribution
scope.  Anything that cannot be named exactly is quarantined instead of being
inferred from a timestamp or a friendly name.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from .manifest import DemoManifest
from .models import RoundId


class AttributionStatus(StrEnum):
    READY = "ready"
    QUARANTINED = "quarantined"
    ATTRIBUTION_UNAVAILABLE = "attribution_unavailable"


@dataclass(frozen=True, slots=True)
class CostWindow:
    """A half-open UTC interval: ``[start_inclusive, end_exclusive)``."""

    start_inclusive: datetime
    end_exclusive: datetime

    def __post_init__(self) -> None:
        if self.start_inclusive.tzinfo is None or self.start_inclusive.utcoffset() is None:
            raise ValueError("cost window start must be timezone-aware")
        if self.end_exclusive.tzinfo is None or self.end_exclusive.utcoffset() is None:
            raise ValueError("cost window end must be timezone-aware")
        start = self.start_inclusive.astimezone(UTC)
        end = self.end_exclusive.astimezone(UTC)
        if start >= end:
            raise ValueError("cost window must have a positive duration")
        object.__setattr__(self, "start_inclusive", start)
        object.__setattr__(self, "end_exclusive", end)


@dataclass(frozen=True, slots=True)
class ProviderResourceIdentity:
    """One exact provider resource key; no display-name-only identities."""

    provider: str
    resource_type: str
    resource_id: str
    resource_name: str | None = None
    resource_arn: str | None = None


@dataclass(frozen=True, slots=True)
class QuarantinedIdentity:
    provider: str
    resource_type: str
    reason: str


@dataclass(frozen=True, slots=True)
class BoutCostIdentity:
    status: AttributionStatus
    manifest_version: int
    installation_id: str | None
    bout_id: str
    session_id: str
    round_id: RoundId
    window: CostWindow
    resources: tuple[ProviderResourceIdentity, ...]
    quarantine: tuple[QuarantinedIdentity, ...]


_AWS_ROUNDS = frozenset(
    {
        RoundId.WAKE_IDLE_APP,
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        RoundId.RECOVER_DELETED_ORDER,
        RoundId.SURVIVE_CONNECTION_SPIKE,
    }
)


def capture_bout_cost_identity(
    manifest: DemoManifest,
    *,
    round_id: RoundId | str | int,
    bout_id: str,
    session_id: str,
    window_start: datetime,
    window_end: datetime,
) -> BoutCostIdentity:
    """Capture exact per-bout attribution keys from a manifest.

    V6 and earlier shared resources between rounds, so their historical usage
    cannot be split honestly.  Those manifests return an explicit unavailable
    result.  V7 emits all exact sealed identities it has and quarantines any
    missing identity independently.
    """

    canonical_round = _coerce_round_id(round_id)
    canonical_bout = _require_id("bout_id", bout_id)
    canonical_session = _require_id("session_id", session_id)
    window = CostWindow(window_start, window_end)

    if manifest.manifest_version != 7:
        return BoutCostIdentity(
            status=AttributionStatus.ATTRIBUTION_UNAVAILABLE,
            manifest_version=manifest.manifest_version,
            installation_id=manifest.installation_id,
            bout_id=canonical_bout,
            session_id=canonical_session,
            round_id=canonical_round,
            window=window,
            resources=(),
            quarantine=(
                QuarantinedIdentity(
                    provider="all",
                    resource_type="per_round_attribution",
                    reason=(
                        f"manifest v{manifest.manifest_version} has shared resources; "
                        "per-round attribution is unavailable"
                    ),
                ),
            ),
        )

    resources: list[ProviderResourceIdentity] = []
    quarantine: list[QuarantinedIdentity] = []

    if not manifest.installation_id:
        quarantine.append(
            QuarantinedIdentity(
                provider="all",
                resource_type="installation",
                reason="v7 installation_id is missing",
            )
        )

    try:
        environment = manifest.round_environment(canonical_round)
    except (KeyError, RuntimeError):
        environment = None
        quarantine.append(
            QuarantinedIdentity(
                provider="databricks",
                resource_type="lakebase_environment",
                reason="exact v7 round environment is missing",
            )
        )

    if environment is not None:
        lakebase = environment.lakebase
        resources.extend(
            (
                ProviderResourceIdentity(
                    provider="databricks",
                    resource_type="lakebase_project",
                    resource_id=lakebase.project_uid,
                    resource_name=lakebase.project_id,
                ),
                ProviderResourceIdentity(
                    provider="databricks",
                    resource_type="lakebase_branch",
                    resource_id=lakebase.branch_uid,
                    resource_name=lakebase.branch_name,
                ),
                ProviderResourceIdentity(
                    provider="databricks",
                    resource_type="lakebase_endpoint",
                    resource_id=lakebase.endpoint_uid,
                    resource_name=lakebase.endpoint_name,
                ),
            )
        )

    if canonical_round == RoundId.PUT_MODEL_SCORE_IN_APP:
        _capture_round4(manifest, resources, quarantine)
    elif canonical_round == RoundId.ANALYZE_LIVE_ORDERS:
        _capture_round6(manifest, resources, quarantine)

    if canonical_round in _AWS_ROUNDS:
        if environment is None:
            quarantine.append(
                QuarantinedIdentity(
                    provider="aws",
                    resource_type="database_environment",
                    reason="exact v7 AWS round environment is missing",
                )
            )
        else:
            _capture_aws_databases(manifest, environment, resources, quarantine)
        if canonical_round == RoundId.SURVIVE_CONNECTION_SPIKE:
            quarantine.append(
                QuarantinedIdentity(
                    provider="aws",
                    resource_type="rds_proxy",
                    reason=(
                        "per-bout RDS Proxy ARN/resource ID is runtime-created and is not "
                        "sealed in the static manifest"
                    ),
                )
            )

    return BoutCostIdentity(
        status=(AttributionStatus.QUARANTINED if quarantine else AttributionStatus.READY),
        manifest_version=manifest.manifest_version,
        installation_id=manifest.installation_id,
        bout_id=canonical_bout,
        session_id=canonical_session,
        round_id=canonical_round,
        window=window,
        resources=tuple(resources),
        quarantine=tuple(quarantine),
    )


def _capture_round4(
    manifest: DemoManifest,
    resources: list[ProviderResourceIdentity],
    quarantine: list[QuarantinedIdentity],
) -> None:
    seal = manifest.round4
    if seal is None:
        quarantine.append(
            QuarantinedIdentity(
                provider="databricks",
                resource_type="round4_pipeline_and_warehouse",
                reason="Round 4 pipeline and warehouse seals are missing",
            )
        )
        return
    resources.extend(
        (
            ProviderResourceIdentity(
                provider="databricks",
                resource_type="database_table_sync_pipeline",
                resource_id=seal.pipeline_id,
            ),
            ProviderResourceIdentity(
                provider="databricks",
                resource_type="sql_warehouse",
                resource_id=seal.warehouse_id,
            ),
        )
    )


def _capture_round6(
    manifest: DemoManifest,
    resources: list[ProviderResourceIdentity],
    quarantine: list[QuarantinedIdentity],
) -> None:
    seal = manifest.round6
    if seal is None:
        quarantine.append(
            QuarantinedIdentity(
                provider="databricks",
                resource_type="round6_cdf_and_destination",
                reason="Round 6 warehouse, CDF, and destination seals are missing",
            )
        )
        return
    resources.extend(
        (
            ProviderResourceIdentity(
                provider="databricks",
                resource_type="sql_warehouse",
                resource_id=seal.warehouse_id,
            ),
            ProviderResourceIdentity(
                provider="databricks",
                resource_type="lakebase_cdf_config",
                resource_id=seal.cdf_config_id,
                resource_name=seal.cdf_config_name,
            ),
            ProviderResourceIdentity(
                provider="databricks",
                resource_type="lakebase_cdf_status",
                resource_id=seal.cdf_status_id,
                resource_name=seal.cdf_status_name,
            ),
            ProviderResourceIdentity(
                provider="databricks",
                resource_type="unity_catalog_schema",
                resource_id=seal.destination_schema_id,
                resource_name=f"{seal.destination_catalog}.{seal.destination_schema}",
            ),
            ProviderResourceIdentity(
                provider="databricks",
                resource_type="unity_catalog_table",
                resource_id=seal.destination_table_id,
                resource_name=seal.destination_table_full_name,
            ),
        )
    )


def _capture_aws_databases(
    manifest: DemoManifest,
    environment: object,
    resources: list[ProviderResourceIdentity],
    quarantine: list[QuarantinedIdentity],
) -> None:
    aurora = getattr(environment, "aurora", None)
    rds = getattr(environment, "rds", None)
    if aurora is None or rds is None:
        quarantine.append(
            QuarantinedIdentity(
                provider="aws",
                resource_type="database_environment",
                reason="dedicated Aurora and RDS seals are incomplete",
            )
        )
        return

    partition = _verified_aws_partition(
        aurora.secret_arn,
        region=manifest.aws.region,
        account_id=manifest.aws.account_id,
    )
    rds_partition = _verified_aws_partition(
        rds.secret_arn,
        region=manifest.aws.region,
        account_id=manifest.aws.account_id,
    )
    if partition is None or rds_partition is None or partition != rds_partition:
        quarantine.append(
            QuarantinedIdentity(
                provider="aws",
                resource_type="database_environment",
                reason="AWS seal partition/account/region is ambiguous or inconsistent",
            )
        )
        return

    arn_root = f"arn:{partition}:rds:{manifest.aws.region}:{manifest.aws.account_id}"
    resources.extend(
        (
            ProviderResourceIdentity(
                provider="aws",
                resource_type="aurora_cluster",
                resource_id=aurora.cluster_resource_id,
                resource_name=aurora.cluster_id,
                resource_arn=f"{arn_root}:cluster:{aurora.cluster_id}",
            ),
            ProviderResourceIdentity(
                provider="aws",
                resource_type="aurora_writer_instance",
                resource_id=aurora.writer_instance_id,
                resource_name=aurora.writer_instance_id,
                resource_arn=f"{arn_root}:db:{aurora.writer_instance_id}",
            ),
            ProviderResourceIdentity(
                provider="aws",
                resource_type="rds_postgres_instance",
                resource_id=rds.resource_id,
                resource_name=rds.instance_id,
                resource_arn=f"{arn_root}:db:{rds.instance_id}",
            ),
        )
    )


def _verified_aws_partition(secret_arn: str, *, region: str, account_id: str) -> str | None:
    parts = secret_arn.split(":", 5)
    if len(parts) != 6:
        return None
    arn, partition, service, arn_region, arn_account, _ = parts
    if (
        arn != "arn"
        or service != "secretsmanager"
        or arn_region != region
        or arn_account != account_id
    ):
        return None
    return partition or None


def _coerce_round_id(round_id: RoundId | str | int) -> RoundId:
    if isinstance(round_id, int):
        rounds = tuple(RoundId)
        if round_id < 1 or round_id > len(rounds):
            raise ValueError(f"unknown round number: {round_id}")
        try:
            return rounds[round_id - 1]
        except (IndexError, TypeError):
            raise ValueError(f"unknown round number: {round_id}") from None
    try:
        return RoundId(round_id)
    except ValueError:
        raise ValueError(f"unknown round id: {round_id}") from None


def _require_id(label: str, value: str) -> str:
    canonical = value.strip()
    if not canonical:
        raise ValueError(f"{label} must not be empty")
    return canonical
