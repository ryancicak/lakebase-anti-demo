"""Round 3's Azure lane (server/azure_recovery.py) against Round 2's fake ARM client.

Pins the restore to the exact recovery point, the recovery contract tag on every copy (written
and required), and that eligibility waits for nothing, because Azure's restore holds its own
WAL wait (Phase 0: accepted at once, the order in the copy).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from server.azure_recovery import AzurePostgresRecoveryAdapter
from server.recovery import RecoveryPlan
from server.safe_change import SafeChangeProvider
from tests.test_azure_safe_change import CONFIG, SCOPE, FakeArm, connector, server

PLAN = RecoveryPlan(lane_id="azure_postgres", name="Azure Database for PostgreSQL",
                    provider=SafeChangeProvider.AZURE_POSTGRES, source_id="r2-src",
                    artifact_id="adrc-ad-test-001-azure", scope=SCOPE)


class PointRecordingArm(FakeArm):
    def begin_restore(self, **kwargs: Any):  # type: ignore[override]
        self.point = kwargs["point_in_time_utc"]
        return super().begin_restore(**kwargs)


def adapter(arm: FakeArm) -> AzurePostgresRecoveryAdapter:
    return AzurePostgresRecoveryAdapter(CONFIG, arm=arm, password=lambda: "pw",  # type: ignore[arg-type]
                                        connector=connector)


async def _noop(*_: Any) -> None:
    return None


async def test_the_copy_is_restored_to_the_exact_recovery_point_with_the_contract_tag() -> None:
    arm = PointRecordingArm()
    recovery_at = datetime(2026, 10, 19, 4, 0, 6, 982448, tzinfo=UTC)
    artifact = await adapter(arm).create_recovery(PLAN, recovery_at, _noop)
    assert arm.point == "2026-10-19T04:00:06.982Z"
    assert arm.servers[PLAN.artifact_id]["tags"]["anti-demo-contract"] == "recovery-v1"
    assert artifact.owner == "operator@databricks.com"


async def test_a_recovery_point_in_another_zone_is_sent_in_utc() -> None:
    arm = PointRecordingArm()
    central = timezone(timedelta(hours=-5))
    await adapter(arm).create_recovery(PLAN, datetime(2026, 10, 18, 23, 0, 6, tzinfo=central),
                                       _noop)
    assert arm.point == "2026-10-19T04:00:06.000Z"


async def test_a_copy_whose_contract_tag_names_another_contract_is_not_round_threes() -> None:
    # Round 2 and Round 3 copies differ by name (adsc- / adrc-); within the name, a contract tag
    # that names something else is a contradiction, while a missing one (Azure's tag lag) is not.
    arm = FakeArm()
    lane = adapter(arm)
    arm.servers[PLAN.artifact_id] = server(
        PLAN.artifact_id, tags={**lane._tags(PLAN), "anti-demo-contract": "something-else"})
    assert (await lane.inspect_recovery(PLAN)).owner == ""
    arm.servers[PLAN.artifact_id] = server(PLAN.artifact_id, tags={})
    assert (await lane.inspect_recovery(PLAN)).owner == "operator@databricks.com"


async def test_eligibility_waits_for_nothing_and_says_why() -> None:
    arm = FakeArm()
    evidence = await adapter(arm).wait_recovery_point(
        PLAN, datetime(2026, 10, 19, 4, 0, 6, tzinfo=UTC), _noop)
    assert evidence == {"eligibility": "azure_restore_waits_for_its_wal",
                        "recovery_at": "2026-10-19T04:00:06.000Z"}
    assert arm.calls == []


async def test_a_naive_recovery_point_is_refused() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        await adapter(FakeArm()).create_recovery(PLAN, datetime(2026, 10, 19, 4, 0, 6), _noop)
