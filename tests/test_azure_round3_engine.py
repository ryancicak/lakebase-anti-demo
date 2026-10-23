"""Round 3's Azure adapter through the real RecoveryEngine: arm, run, reset.

Phase 0 ran the adapter live on its own (the order in the copy, gone from the source). This
checks it fits the engine the app runs, with Azure's own eligibility answer ("the restore waits
for its WAL", no pre-wait), the copy's tags and rules, and the cleanup. ARM is the Round 2 tests'
fake, and the data plane is the Round 3 tests' fake, so the SQL is the engine's own.
"""

from __future__ import annotations

from server.azure_recovery import AzurePostgresRecoveryAdapter
from server.recovery import RecoveryEngine
from server.safe_change import SafeChangeOwnershipScope, SafeChangeProvider
from tests.test_azure_safe_change import CONFIG, RULES, SUB, FakeArm, connector
from tests.test_recovery import FakeRecoveryAdapter, FakeRecoveryConnection

AZURE = "azure_postgres"
COPY = "adrc-ad-test-003-azure"


def build_engine():
    timeline: list[tuple[str, str]] = []
    scope = SafeChangeOwnershipScope(run_id="ad-test-003", owner="operator@databricks.com",
                                     expires_at="2030-01-01T00:00:00Z", cloud="azure",
                                     azure_subscription_id=SUB, azure_region="westus2")
    lakebase = FakeRecoveryAdapter(SafeChangeProvider.LAKEBASE, "Lakebase", timeline)
    # The Azure lane's data plane: the source server's rows, as the Round 3 fakes keep them.
    data = FakeRecoveryAdapter(SafeChangeProvider.AZURE_POSTGRES, "Azure Database for PostgreSQL",
                               timeline)
    arm = FakeArm()
    azure = AzurePostgresRecoveryAdapter(CONFIG, arm=arm, password=lambda: "pw",  # type: ignore[arg-type]
                                         connector=connector)

    async def connect_source(plan):
        return await data.connect_source(plan)

    async def connect_recovery(plan, artifact):
        # The restored copy holds the order as it was at recovery_at: before the delete.
        data.recovery_row = data.historical_row
        data.timeline.append((data.provider.value, "connect_recovery"))
        return FakeRecoveryConnection(data, recovered=True)

    azure.connect_source = connect_source  # type: ignore[method-assign]
    azure.connect_recovery = connect_recovery  # type: ignore[method-assign]
    tick = 0

    def clock_ns() -> int:
        nonlocal tick
        tick += 1_000_000
        return tick

    engine = RecoveryEngine(scope=scope, lakebase=lakebase,
                            competitors={AZURE: azure},  # type: ignore[dict-item]
                            clock_ns=clock_ns)
    return engine, arm, data


async def test_the_azure_recovery_lane_runs_and_proves_through_the_real_engine() -> None:
    engine, arm, data = build_engine()
    armed = await engine.arm(AZURE)  # type: ignore[arg-type]
    assert armed.lanes["competitor"].plan.artifact_id == COPY

    result = await engine.run(armed)

    assert result.all_verified is True
    competitor = result.lanes["competitor"]
    assert competitor.recovery_at is not None and competitor.recovery_at.microsecond == 0
    # Azure answers eligibility at once: its restore waits for its own WAL.
    assert [c for c in arm.calls if c.startswith(("begin", "finish", "tags", "rules"))] == [
        f"begin:{COPY}", "finish", f"tags:{COPY}", f"rules:{COPY}"]
    assert arm.rules == list(RULES)
    assert arm.servers[COPY]["tags"]["anti-demo-contract"] == "recovery-v1"
    assert data.recovery_row == engine.contract.row and data.source_row is None


async def test_reset_deletes_the_owned_recovery_copy() -> None:
    engine, arm, _ = build_engine()
    await engine.run(await engine.arm(AZURE))  # type: ignore[arg-type]
    assert COPY in arm.servers

    await engine.reset(AZURE)  # type: ignore[arg-type]
    assert COPY not in arm.servers and f"delete:{COPY}" in arm.calls
