"""Round 2's Azure adapter through the real SafeChangeEngine: arm, run, reset.

Phase 0 ran the adapter live on its own. This checks it fits the engine the app runs: the
engine's preflight and stale-artifact sweep, its ownership check on the copy against an Azure
scope, the barrier, the identical SQL proof and the cleanup. ARM and both databases are fakes.

The competitor key is the Azure edition's ID as a string, because `CompetitorId` gains the member
in Phase 2 (design section 12); a StrEnum member will find the same entry.
"""

from __future__ import annotations

import copy

from server.azure_safe_change import AzurePostgresSafeChangeAdapter
from server.safe_change import (
    SafeChangeContract,
    SafeChangeEngine,
    SafeChangeLaneState,
    SafeChangeOwnershipScope,
    SafeChangeProvider,
)
from tests.test_azure_safe_change import CONFIG, RULES, SUB, FakeArm, connector
from tests.test_safe_change import FakeAdapter, FakeConnection, FakeDatabase

AZURE = "azure_postgres"
COPY = "adsc-ad-test-001-azure"


def make_engine() -> tuple[SafeChangeEngine, FakeArm, FakeDatabase, dict[str, FakeDatabase]]:
    scope = SafeChangeOwnershipScope(run_id="ad-test-001", owner="operator@databricks.com",
                                     expires_at="2030-01-01T00:00:00Z", cloud="azure",
                                     azure_subscription_id=SUB, azure_region="westus2")
    contract = SafeChangeContract()
    lakebase = FakeAdapter(provider=SafeChangeProvider.LAKEBASE, name="Lakebase",
                           source_id="projects/ad-test-001/branches/production", scope=scope,
                           contract=contract)
    arm = FakeArm()
    azure = AzurePostgresSafeChangeAdapter(CONFIG, arm=arm, password=lambda: "pw",  # type: ignore[arg-type]
                                           connector=connector)
    source = FakeDatabase()
    copies: dict[str, FakeDatabase] = {}

    async def connect_source(plan):  # the data plane is faked; the control plane is the adapter's
        return FakeConnection(source, contract)

    async def connect_isolated(plan, artifact):
        # A restore copies the source as it was; nothing writes the source in between.
        database = copies.setdefault(artifact.artifact_id, copy.deepcopy(source))
        return FakeConnection(database, contract)

    azure.connect_source = connect_source  # type: ignore[method-assign]
    azure.connect_isolated = connect_isolated  # type: ignore[method-assign]
    engine = SafeChangeEngine(scope=scope, lakebase=lakebase,
                              competitors={AZURE: azure},  # type: ignore[dict-item]
                              contract=contract,
                              nonce_factory=lambda: "11111111-1111-1111-1111-111111111111")
    return engine, arm, source, copies


async def test_the_azure_lane_arms_runs_and_proves_through_the_real_engine() -> None:
    engine, arm, source, copies = make_engine()
    armed = await engine.arm(AZURE)  # type: ignore[arg-type]
    assert armed.lanes["competitor"].plan.artifact_id == COPY

    result = await engine.run(armed)

    assert result.all_verified is True
    assert result.lanes["competitor"].state == SafeChangeLaneState.VERIFIED
    # The copy is Azure's real workflow: restored, tagged as this run's, then its firewall opened.
    assert [c for c in arm.calls if c.startswith(("begin", "finish", "tags", "rules"))] == [
        f"begin:{COPY}", "finish", f"tags:{COPY}", f"rules:{COPY}"]
    assert arm.rules == list(RULES)
    # The migration and the order went into the copy, and the source never saw either.
    assert copies[COPY].has_delivery_column is True and result.nonce in copies[COPY].rows
    assert source.has_delivery_column is False and result.nonce not in source.rows


async def test_reset_deletes_the_owned_copy_and_is_idempotent() -> None:
    engine, arm, _, _ = make_engine()
    await engine.run(await engine.arm(AZURE))  # type: ignore[arg-type]
    assert COPY in arm.servers

    await engine.reset(AZURE)  # type: ignore[arg-type]
    assert COPY not in arm.servers and f"delete:{COPY}" in arm.calls
    await engine.reset(AZURE)  # type: ignore[arg-type]
    assert arm.calls.count(f"delete:{COPY}") == 1


async def test_a_copy_azure_is_still_deleting_is_waited_out_before_the_next_bell() -> None:
    # The release bar's finish-linger wave, right after rapid-rearm-towel (2026-10-20). A towel
    # issues its copy's delete and returns; Azure's reads say 404 while it still holds the name;
    # and the next bout's restore to that name was refused ServerNameAlreadyExists.
    engine, arm, _, _ = make_engine()
    azure = engine.competitors[AZURE]
    slept: list[float] = []

    async def counted(seconds: float) -> None:
        slept.append(seconds)

    azure._sleep = counted  # type: ignore[attr-defined]
    # The previous bout's copy: gone from reads, its name still taken for five more checks.
    arm.reserved[COPY] = 5
    armed = await engine.arm(AZURE)  # type: ignore[arg-type]
    # Prepare found it, cleared it (a delete of nothing), and waited until the name was free,
    # all before the bell, so none of the wait is on Azure's clock.
    assert f"delete:{COPY}" in arm.calls and arm.reserved[COPY] == 0
    assert len(slept) == 2
    result = await engine.run(armed)
    assert result.all_verified is True
    assert arm.calls.index(f"begin:{COPY}") > arm.calls.index(f"delete:{COPY}")


async def test_an_adapter_claiming_the_wrong_provider_is_refused_at_construction() -> None:
    engine, *_ = make_engine()
    import pytest

    with pytest.raises(ValueError, match="no safe-change provider is known for gcp_postgres"):
        SafeChangeEngine(scope=engine.scope, lakebase=engine.lakebase,
                         competitors={"gcp_postgres": engine.competitors[AZURE]})  # type: ignore[dict-item]
