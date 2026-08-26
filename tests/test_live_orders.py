from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from server.live_orders import (
    LiveOrder,
    LiveOrderHistory,
    LiveOrdersContract,
    LiveOrdersEngine,
    LiveOrdersLiveAdapter,
    LiveOrdersLiveConfig,
    LiveOrdersNotArmedError,
    LiveOrdersPhase,
    LiveOrdersProgress,
    LiveOrdersTimeoutError,
    LiveOrdersVerificationError,
    NativeCdfStatus,
)
from server.manager import RunManager
from server.models import CompetitorId, Corner, RoundId, SessionCreate, SessionState


class FakeAdapter:
    def __init__(self, contract: LiveOrdersContract) -> None:
        self.contract = contract
        self.source = {contract.baseline.order_id: contract.baseline}
        self.history = {
            contract.baseline.proof_nonce: LiveOrderHistory(
                contract.baseline, "insert", 1
            )
        }
        self.publish_insert = True

    async def inspect_feed(self) -> NativeCdfStatus:
        return NativeCdfStatus(
            state="CDF_STATE_STREAMING",
            committed_lsn="0/16B6C50",
            last_sync_time=datetime.now(UTC),
            source_table=self.contract.source_table,
            history_table=self.contract.history_table,
        )

    async def read_checkout(self, order_id: str) -> LiveOrder | None:
        return self.source.get(order_id)

    async def insert_checkout(self, order: LiveOrder) -> None:
        self.source[order.order_id] = order
        if self.publish_insert:
            self.history[order.proof_nonce] = LiveOrderHistory(order, "insert", 2)

    async def delete_checkout_exact(self, order: LiveOrder) -> None:
        if self.source.get(order.order_id) == order:
            self.source.pop(order.order_id)
        if self.source.get(order.order_id) == order:
            raise AssertionError("exact checkout row was not deleted")

    async def read_history(self, order: LiveOrder) -> LiveOrderHistory | None:
        return self.history.get(order.proof_nonce)


class ConcurrentGuardrailAdapter(FakeAdapter):
    def __init__(self, expected: LiveOrdersContract) -> None:
        super().__init__(expected)
        self.history_poll_started = asyncio.Event()
        self.guardrail_started = asyncio.Event()
        self.inserted: list[LiveOrder] = []
        self.feed_checks = 0

    async def inspect_feed(self) -> NativeCdfStatus:
        self.feed_checks += 1
        if self.feed_checks > 1:
            await asyncio.sleep(0.03)
        return await super().inspect_feed()

    async def insert_checkout(self, order: LiveOrder) -> None:
        self.inserted.append(order)
        if order.proof_nonce.startswith("round6-checkout"):
            self.guardrail_started.set()
            await self.history_poll_started.wait()
            await asyncio.sleep(0.03)
        await super().insert_checkout(order)

    async def read_history(self, order: LiveOrder) -> LiveOrderHistory | None:
        if order.proof_nonce != self.contract.baseline.proof_nonce:
            self.history_poll_started.set()
            await self.guardrail_started.wait()
        return await super().read_history(order)


class DelayedGuardrailAdapter(FakeAdapter):
    def __init__(self, expected: LiveOrdersContract) -> None:
        super().__init__(expected)
        self.guardrail_started = asyncio.Event()
        self.release_guardrail = asyncio.Event()
        self.deleted: list[LiveOrder] = []

    async def insert_checkout(self, order: LiveOrder) -> None:
        if order.proof_nonce.startswith("round6-checkout"):
            self.guardrail_started.set()
            await self.release_guardrail.wait()
        await super().insert_checkout(order)

    async def delete_checkout_exact(self, order: LiveOrder) -> None:
        self.deleted.append(order)
        await super().delete_checkout_exact(order)


WAKE_NS = 2_410_000_000
WARM_ROUND_TRIP_NS = 1_000_000


class ColdEndpointAdapter(FakeAdapter):
    """A fake whose endpoint suspends during the arm-to-bell gap.

    Every operation that opens a Postgres connection pays ``WAKE_NS`` once while
    the endpoint is suspended, the way a scaled-to-zero Lakebase endpoint does.
    ``read_history`` never does, because it reads Delta through a warehouse and
    never touches the endpoint -- which is exactly why arming cannot be relied on
    to keep the endpoint warm.
    """

    def __init__(self, expected: LiveOrdersContract) -> None:
        super().__init__(expected)
        self.suspended = False
        self.now_ns = 0
        self.connects: list[str] = []
        self.fail_next_connect = False

    def clock_ns(self) -> int:
        return self.now_ns

    def _connect(self, label: str) -> None:
        self.connects.append(label)
        if self.fail_next_connect:
            self.fail_next_connect = False
            raise OSError("simulated endpoint connection failure")
        if self.suspended:
            self.now_ns += WAKE_NS
            self.suspended = False
        self.now_ns += WARM_ROUND_TRIP_NS

    async def read_checkout(self, order_id: str) -> LiveOrder | None:
        self._connect(f"read:{order_id}")
        return await super().read_checkout(order_id)

    async def insert_checkout(self, order: LiveOrder) -> None:
        self._connect(f"insert:{order.order_id}")
        await super().insert_checkout(order)

    async def delete_checkout_exact(self, order: LiveOrder) -> None:
        self._connect(f"delete:{order.order_id}")
        await super().delete_checkout_exact(order)


def cold_engine(
    expected: LiveOrdersContract, adapter: ColdEndpointAdapter
) -> LiveOrdersEngine:
    return LiveOrdersEngine(
        adapter,
        contract=expected,
        poll_interval_seconds=0,
        clock_ns=adapter.clock_ns,
    )


def contract() -> LiveOrdersContract:
    return LiveOrdersContract(
        database_resource_name=(
            "projects/demo/branches/production/databases/databricks-postgres"
        ),
        postgres_database="databricks_postgres",
        source_schema="anti_demo_round6",
        source_table="live_orders",
        cdf_config_name=(
            "projects/demo/branches/production/databases/databricks-postgres/"
            "cdf-configs/anti_demo_round6"
        ),
        cdf_status_name=(
            "projects/demo/branches/production/databases/databricks-postgres/"
            "cdf-configs/anti_demo_round6/cdf-statuses/live_orders"
        ),
        history_table="catalog.anti_demo_round6.lb_live_orders_history",
    )


def live_order(order_id: str, nonce: str) -> LiveOrder:
    return LiveOrder(
        order_id=order_id,
        sku="RED-GLOVE",
        store="CHICAGO",
        quantity=1,
        total_cents=8450,
        status="paid",
        proof_nonce=nonce,
    )


@pytest.mark.asyncio
async def test_endpoint_wake_cannot_land_inside_the_checkout_commit_clock() -> None:
    """A suspended endpoint at bell time must not be billed to checkout_commit_ms.

    Without the pre-clock warm-up the bout's own insert is the first connection
    after arming, so the whole wake lands inside the window that reports how long
    checkout took -- in the round whose claim is that checkout was not slowed.
    """

    expected = contract()
    adapter = ColdEndpointAdapter(expected)
    engine = cold_engine(expected, adapter)
    arm = await engine.arm()
    # The operator rings the bell late in the armed window, by which point the
    # 60s suspend timer -- which started at arming's last Postgres touch -- has
    # already come due.
    adapter.suspended = True

    result = await engine.run(
        arm,
        live_order("order-cold-primary", "round6-cold-primary"),
        live_order("order-cold-guardrail", "round6-checkout-cold"),
    )

    wake_ms = WAKE_NS / 1_000_000
    assert result.checkout_commit_ms < wake_ms
    assert result.checkout_guardrail_commit_ms < wake_ms
    assert result.total_elapsed_ms < wake_ms


@pytest.mark.asyncio
async def test_warm_up_is_a_baseline_read_that_leaves_no_residue() -> None:
    expected = contract()
    adapter = ColdEndpointAdapter(expected)
    engine = cold_engine(expected, adapter)
    arm = await engine.arm()
    adapter.connects.clear()
    before = dict(adapter.source)
    order = live_order("order-residue-primary", "round6-residue-primary")
    guardrail = live_order("order-residue-guardrail", "round6-checkout-residue")

    await engine.run(arm, order, guardrail)

    # The first thing the bell does is read the sealed baseline: no write, no
    # sequence advanced, and nothing the exact cleanup could mistake for a proof.
    assert adapter.connects[0] == f"read:{expected.baseline.order_id}"
    assert adapter.source[expected.baseline.order_id] == before[expected.baseline.order_id]
    assert not any(
        connect.startswith(("insert:", "delete:"))
        for connect in adapter.connects[:1]
    )


@pytest.mark.asyncio
async def test_warm_up_never_shows_the_audience_a_checkout_that_did_not_happen() -> None:
    expected = contract()
    adapter = ColdEndpointAdapter(expected)
    engine = cold_engine(expected, adapter)
    arm = await engine.arm()
    events: list[LiveOrdersProgress] = []

    async def on_progress(progress: LiveOrdersProgress) -> None:
        events.append(progress)

    await engine.run(
        arm,
        live_order("order-phase-primary", "round6-phase-primary"),
        live_order("order-phase-guardrail", "round6-checkout-phase"),
        on_progress,
    )

    first = events[0]
    assert first.phase is LiveOrdersPhase.PREFLIGHT
    # PREFLIGHT maps to a sealed lane, and a null elapsed leaves the on-screen
    # clock alone, so the warm-up cannot read as the bout's own checkout.
    assert first.elapsed_ms is None
    assert first.phase not in {
        LiveOrdersPhase.CHECKOUT,
        LiveOrdersPhase.WAITING_CDF,
        LiveOrdersPhase.READING_CHECKOUT,
    }


@pytest.mark.asyncio
async def test_failed_warm_up_does_not_fail_the_bout() -> None:
    expected = contract()
    adapter = ColdEndpointAdapter(expected)
    engine = cold_engine(expected, adapter)
    arm = await engine.arm()
    adapter.fail_next_connect = True

    result = await engine.run(
        arm,
        live_order("order-warmfail-primary", "round6-warmfail-primary"),
        live_order("order-warmfail-guardrail", "round6-checkout-warmfail"),
    )

    assert result.checkout_verified is True
    assert result.matching_orders == 1


@pytest.mark.asyncio
async def test_analytics_window_still_starts_at_the_completed_commit() -> None:
    """The primary metric must stay immune even when the commit pays a wake."""

    expected = contract()
    adapter = ColdEndpointAdapter(expected)
    engine = cold_engine(expected, adapter)
    arm = await engine.arm()
    # Deny the warm-up its connection so the wake is forced into the commit,
    # which is precisely the corruption the warm-up exists to prevent.
    adapter.fail_next_connect = True
    adapter.suspended = True

    result = await engine.run(
        arm,
        live_order("order-analytics-primary", "round6-analytics-primary"),
        live_order("order-analytics-guardrail", "round6-checkout-analytics"),
    )

    wake_ms = WAKE_NS / 1_000_000
    assert result.checkout_commit_ms >= wake_ms
    assert result.analytics_available_ms < wake_ms


@pytest.mark.asyncio
async def test_one_order_reaches_delta_and_checkout_stays_exact() -> None:
    expected = contract()
    adapter = FakeAdapter(expected)
    engine = LiveOrdersEngine(
        adapter,
        contract=expected,
        poll_interval_seconds=0,
    )
    arm = await engine.arm()
    order = live_order(
        "00000000-0000-4000-8000-000000000106",
        "round6-live-proof",
    )
    guardrail = live_order(
        "00000000-0000-4000-8000-000000000107",
        "round6-checkout-proof",
    )

    result = await engine.run(arm, order, guardrail)

    assert result.order == order
    assert result.matching_orders == 1
    assert result.history_lsn == 2
    assert result.checkout_verified is True
    assert result.checkout_guardrail_order == guardrail


@pytest.mark.asyncio
async def test_separate_checkout_runs_concurrently_without_extending_answer_timer() -> None:
    expected = contract()
    adapter = ConcurrentGuardrailAdapter(expected)
    engine = LiveOrdersEngine(adapter, contract=expected, poll_interval_seconds=0)
    arm = await engine.arm()
    primary = live_order(
        "order-primary-text-id",
        "round6-primary-concurrent",
    )
    guardrail = live_order(
        "order-guardrail-text-id",
        "round6-checkout-concurrent",
    )

    result = await asyncio.wait_for(engine.run(arm, primary, guardrail), timeout=1)

    assert adapter.inserted == [primary, guardrail]
    assert result.checkout_guardrail_order == guardrail
    assert result.total_elapsed_ms > result.analytics_available_ms + 15


@pytest.mark.asyncio
async def test_arm_requires_the_exact_baseline_in_source_and_history() -> None:
    expected = contract()
    adapter = FakeAdapter(expected)
    adapter.source.clear()

    with pytest.raises(LiveOrdersNotArmedError, match="checkout baseline"):
        await LiveOrdersEngine(adapter, contract=expected).arm()


@pytest.mark.asyncio
async def test_run_fails_closed_when_exact_order_never_reaches_delta() -> None:
    expected = contract()
    adapter = FakeAdapter(expected)
    adapter.publish_insert = False
    engine = LiveOrdersEngine(
        adapter,
        contract=expected,
        max_poll_attempts=1,
        poll_interval_seconds=0,
    )
    arm = await engine.arm()

    with pytest.raises(LiveOrdersTimeoutError, match="exact order"):
        await engine.run(
            arm,
            live_order(
                "00000000-0000-4000-8000-000000000206",
                "round6-missing-proof",
            ),
            live_order(
                "00000000-0000-4000-8000-000000000207",
                "round6-missing-checkout",
            ),
        )


@pytest.mark.asyncio
async def test_an_arm_cannot_be_reused() -> None:
    expected = contract()
    adapter = FakeAdapter(expected)
    engine = LiveOrdersEngine(adapter, contract=expected, poll_interval_seconds=0)
    arm = await engine.arm()
    first = live_order(
        "00000000-0000-4000-8000-000000000306",
        "round6-first-proof",
    )
    first_guardrail = live_order(
        "00000000-0000-4000-8000-000000000307",
        "round6-first-checkout",
    )
    await engine.run(arm, first, first_guardrail)

    with pytest.raises(LiveOrdersVerificationError, match="already used"):
        await engine.run(
            arm,
            live_order(
                "00000000-0000-4000-8000-000000000406",
                "round6-second-proof",
            ),
            live_order(
                "00000000-0000-4000-8000-000000000407",
                "round6-second-checkout",
            ),
        )


@pytest.mark.asyncio
async def test_towel_settlement_waits_for_cancelled_commit_then_deletes_exact_rows() -> None:
    expected = contract()
    adapter = DelayedGuardrailAdapter(expected)
    engine = LiveOrdersEngine(adapter, contract=expected, poll_interval_seconds=0)
    arm = await engine.arm()
    primary = live_order("order-primary-towel", "round6-primary-towel")
    guardrail = live_order("order-guardrail-towel", "round6-checkout-towel")
    unrelated = live_order("order-not-owned", "round6-not-owned")
    adapter.source[unrelated.order_id] = unrelated

    run = asyncio.create_task(engine.run(arm, primary, guardrail))
    await asyncio.wait_for(adapter.guardrail_started.wait(), timeout=1)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    settlement = asyncio.create_task(
        engine.settle_and_cleanup_owned(primary, guardrail)
    )
    await asyncio.sleep(0)
    assert adapter.deleted == []

    adapter.release_guardrail.set()
    await asyncio.wait_for(settlement, timeout=1)

    assert adapter.deleted == [primary, guardrail]
    assert primary.order_id not in adapter.source
    assert guardrail.order_id not in adapter.source
    assert adapter.source[unrelated.order_id] == unrelated

    await engine.settle_and_cleanup_owned(primary, guardrail)
    assert adapter.deleted == [primary, guardrail, primary, guardrail]


@pytest.mark.asyncio
async def test_live_cleanup_uses_full_payload_and_verifies_absence() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []
            self.results = [[("owned-order",)], []]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def execute(self, statement: str, parameters: tuple[object, ...]) -> None:
            self.calls.append((statement, parameters))

        async def fetchall(self):
            return self.results.pop(0)

    class Connection:
        def __init__(self) -> None:
            self.value = Cursor()
            self.commits = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def cursor(self) -> Cursor:
            return self.value

        async def commit(self) -> None:
            self.commits += 1

    config = LiveOrdersLiveConfig(
        profile="test",
        warehouse_id="warehouse",
        setup_principal="setup",
        app_service_principal_client_id="app",
        endpoint_name="endpoint",
        database_resource_name="database",
        postgres_database="postgres",
        source_schema="round6",
        source_table="live_orders",
        cdf_config_name="config",
        cdf_status_name="status",
        destination_table_full_name="catalog.schema.history",
    )
    adapter = object.__new__(LiveOrdersLiveAdapter)
    adapter.config = config
    connection = Connection()

    async def connect():
        return connection

    adapter._connect = connect
    order = live_order("owned-order", "owned-nonce")

    await adapter.delete_checkout_exact(order)

    assert connection.commits == 1
    assert len(connection.value.calls) == 2
    delete, verify = connection.value.calls
    assert delete[0].startswith('DELETE FROM "round6"."live_orders"')
    assert verify[0].startswith('SELECT 1 FROM "round6"."live_orders"')
    assert all(
        f"{column} = %s" in delete[0]
        for column in (
            "order_id",
            "proof_nonce",
            "sku",
            "store",
            "quantity",
            "total_cents",
            "status",
        )
    )
    assert delete[1] == verify[1] == (
        order.order_id,
        order.proof_nonce,
        order.sku,
        order.store,
        order.quantity,
        order.total_cents,
        order.status,
    )


@pytest.mark.asyncio
async def test_manager_runs_round6_as_one_native_cdf_lane() -> None:
    expected = contract()
    adapter = FakeAdapter(expected)
    manager = RunManager(
        live_orders_factory=lambda: LiveOrdersEngine(
            adapter, contract=expected, poll_interval_seconds=0
        )
    )
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="data_analyst",
            secondary_personas=[],
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.ANALYZE_LIVE_ORDERS,
        )
    )
    await manager.start_arm(created.id)
    for _ in range(100):
        armed = await manager.get(created.id)
        if armed.state == SessionState.ARMED:
            break
        await asyncio.sleep(0)
    assert armed.state == SessionState.ARMED

    await manager.start_run(created.id)
    for _ in range(100):
        finished = await manager.get(created.id)
        if finished.state in {SessionState.VERIFIED, SessionState.FAILED}:
            break
        await asyncio.sleep(0)

    assert finished.state == SessionState.VERIFIED
    assert finished.lanes["competitor"].state.value == "not_supported"
    assert finished.lanes["lakebase"].elapsed_ms == finished.metrics[0].value
    assert finished.lanes["lakebase"].evidence["total_display"] == "$84.50"
    assert finished.metrics[2].display_value == "SEPARATE CHECKOUT COMMITTED ✓"
    assert finished.metrics[1].value == 1
