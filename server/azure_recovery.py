"""Round 3's Azure lane: restore the competitor's server to the recovery point, then read the order.

Like-for-like with RDS (design section 6.3): restore to `recovery_at`, add the copy's firewall rules
(Azure does not copy them), read the deleted order from the copy and prove it absent on the
source. It is Round 2's copy at a fixed point instead of "now", so it is built on
`AzurePostgresSafeChangeAdapter`, with the recovery contract tag the AWS recovery adapters add.

**Eligibility is Azure's, inside the restore** (Phase 0, design 0a #11). RDS's adapter waits for
`LatestRestorableTime` to pass the recovery point before asking. Azure exposes no such time, and
`pg_stat_archiver` is empty on a new server. So the spike asked directly instead: a restore to a
point 8 s after the order's insert, requested the instant after its delete, while the insert's WAL
segment was not yet archived. Azure **accepted it at once and the order was in the copy**. That
restore took 538 s against 451 s for "now"; the difference is Azure waiting for its own WAL, on
Azure's clock, as RDS's wait is on RDS's. So `wait_recovery_point` waits for nothing and says so.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from .azure_safe_change import AzurePostgresSafeChangeAdapter
from .recovery import RecoveryPlan
from .safe_change import ArtifactInspection, SafeChangeSqlConnection

#: The tag the AWS recovery adapters add (server/recovery_live.py), so one ownership rule holds.
_CONTRACT_TAG = "anti-demo-contract"
_CONTRACT_VALUE = "recovery-v1"


def _point(recovery_at: datetime) -> str:
    if recovery_at.tzinfo is None:
        raise ValueError("recovery_at must be timezone-aware")
    return recovery_at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class AzurePostgresRecoveryAdapter(AzurePostgresSafeChangeAdapter):
    """The `RecoveryAdapter` for Azure Database for PostgreSQL Flexible Server."""

    extra_tags = {_CONTRACT_TAG: _CONTRACT_VALUE}

    async def inspect_recovery(self, plan: RecoveryPlan) -> ArtifactInspection | None:
        return await self.inspect_artifact(plan)  # type: ignore[arg-type]

    async def wait_recovery_point(
        self, plan: RecoveryPlan, recovery_at: datetime, report
    ) -> Mapping[str, object]:
        """Nothing to wait for before asking: Azure's restore holds its own WAL wait."""

        self._assert_plan(plan)  # type: ignore[arg-type]
        return {"eligibility": "azure_restore_waits_for_its_wal",
                "recovery_at": _point(recovery_at)}

    async def create_recovery(
        self, plan: RecoveryPlan, recovery_at: datetime, report
    ) -> ArtifactInspection:
        return await self._restore_copy(plan, report, point=_point(recovery_at))  # type: ignore[arg-type]

    async def connect_recovery(
        self, plan: RecoveryPlan, artifact: ArtifactInspection
    ) -> SafeChangeSqlConnection:
        return await self.connect_isolated(plan, artifact)  # type: ignore[arg-type]

    async def delete_recovery(
        self, plan: RecoveryPlan, artifact: ArtifactInspection, report
    ) -> None:
        await self.delete_isolated(plan, artifact, report)  # type: ignore[arg-type]

    async def abandon_recovery(self, plan: RecoveryPlan) -> None:
        await self.abandon_isolated(plan)  # type: ignore[arg-type]

    async def settle_pending_mutations(self) -> None:
        """No mutation journal: every write is found again by deterministic name and tags."""

        return None
