"""server/azure_round5_setup.py: AWS's Round 5 setup sequencing, with Azure's PgBouncer timed.

The sequencing (fence, journal intent before the bell, shared gate and launch barrier, cleanup
handoffs, restart reconcile) is AWS's own, inherited. These pin that Azure's PgBouncer rides it: the
switch is the bell's first timed mutation, journaled before and after; the competitor's runner is
staged while PgBouncer comes up and released once 6432 commits; cleanup is the baseline reset; and a
restart's reconcile finds a PgBouncer left on and resets it. ARM and the pooled port are a fake that
behaves as Phase 0 measured (server/azure_pgbouncer.py's tests).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

import psycopg
import pytest

from server.azure_arm import ArmError
from server.azure_pgbouncer import DEFAULT_MAX_CLIENT_CONN, ROUND5_MAX_CLIENT_CONN, AzurePgBouncer
from server.azure_round5_setup import (
    GATE_ID,
    TIMED_KIND,
    AzureConnectionSpikeSetupOrchestrator,
    AzureRound5SetupConfig,
)
from server.connection_spike_journal import CreationScope, JournalMutationError, LifecycleState
from server.connection_spike_live import (
    ConnectionSpikeCleanupError,
    ConnectionSpikeLiveConfigurationError,
    ConnectionSpikeLiveOperationError,
    ConnectionSpikeLiveSourceUnavailableError,
    ConnectionSpikeLiveTransientError,
)

SERVER = "psql-test-r5"
SERVER_ID = ("/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-test/providers/"
             "Microsoft.DBforPostgreSQL/flexibleServers/psql-test-r5")
HOST = "psql-test-r5.postgres.database.azure.com"
BOUT = "bout-azure-1"
SEAL = "a" * 64


class MemoryJournal:
    """The creation journal, in memory, with the real store's notion of unresolved."""

    def __init__(self) -> None:
        self.rows: list[Any] = []

    async def commit(self, event: Any, *, authority_scope: Any = None) -> None:
        self.rows.append(event)

    async def events(self, scope: CreationScope) -> list[Any]:
        return [row for row in self.rows
                if row.bout_id == scope.bout_id and row.fencing_token == scope.fencing_token]

    async def scopes(self, bout_id: str) -> tuple[CreationScope, ...]:
        return tuple({CreationScope(row.bout_id, row.fencing_token, row.runtime_seal_sha256)
                      for row in self.rows if row.bout_id == bout_id})

    async def unresolved_bout_ids(self) -> tuple[str, ...]:
        newest: dict[tuple[str, int, int], Any] = {}
        for row in self.rows:
            newest[(row.bout_id, row.fencing_token, row.ordinal)] = row
        return tuple(sorted({key[0] for key, row in newest.items()
                             if row.lifecycle_state is not LifecycleState.DELETED}))

    def states(self) -> list[str]:
        return [str(row.lifecycle_state) for row in self.rows]


class Fence:
    def __init__(self) -> None:
        self.current = True

    async def assert_current(self, _scope: Any) -> None:
        if not self.current:
            raise ConnectionSpikeLiveOperationError("the bout's fence is no longer current")


class FakeAzure:
    """ARM and the pooled port, as Phase 0 measured them."""

    def __init__(self) -> None:
        self.enabled = False
        self.limit = DEFAULT_MAX_CLIENT_CONN
        self.writes: list[tuple[str, str]] = []
        self.probes_since_switch = 0
        self.runners = {"vm-r5-0": True, "vm-r5-1": True}
        self.server_state = "Ready"
        #: How long each parameter write takes, in its worker thread: ARM's take seconds.
        self.write_seconds = 0.0

    def get_server(self, name: str) -> dict[str, Any] | None:
        assert name == SERVER
        return {"id": SERVER_ID, "properties": {"fullyQualifiedDomainName": HOST,
                                                "state": self.server_state}}

    def vm_ready(self, name: str) -> bool | None:
        return self.runners.get(name)

    def get_configuration(self, server: str, name: str) -> str | None:
        if name == "pgbouncer.enabled":
            return "true" if self.enabled else "false"
        return self.limit if self.enabled else None

    def set_configuration(self, server: str, name: str, value: str) -> None:
        if name == "pgbouncer.max_client_conn" and not self.enabled:
            raise ArmError("blocked", code="ServerParameterToCMSBlockedUpdateForDisabledPgBouncer")
        time.sleep(self.write_seconds)
        self.writes.append((name, value))
        if name == "pgbouncer.enabled":
            self.enabled, self.probes_since_switch = value == "true", 0
        else:
            self.limit = value

    async def connect(self, **kwargs: Any) -> Any:
        assert kwargs["port"] == 6432 and kwargs["host"] == HOST
        self.probes_since_switch += 1
        if not self.enabled or self.probes_since_switch <= 2:
            raise psycopg.OperationalError("connection refused")
        return Connection()


class Connection:
    @contextlib.asynccontextmanager
    async def transaction(self):  # type: ignore[no-untyped-def]
        yield

    async def execute(self, statement: str) -> None:
        return None

    async def close(self) -> None:
        return None


def build(azure: FakeAzure, journal: MemoryJournal | None = None,
          fence: Fence | None = None) -> AzureConnectionSpikeSetupOrchestrator:
    async def no_sleep(_: float) -> None:
        await asyncio.sleep(0)

    async def fresh_host() -> str:
        return "lakebase.test"

    config = AzureRound5SetupConfig(
        baseline_sha256=SEAL, deterministic_name_prefix="anti-demo-test-r5",
        lakebase_pooled_host="lakebase.test", lakebase_credential_sha256="b" * 64,
        competitor_credential_sha256="c" * 64,
        proxy_secret_arn="arn:aws:secretsmanager:westus2:000000000000:secret:round5-rds",
        competitor_target_id=SERVER, competitor_resource_id=SERVER_ID, competitor_host=HOST,
        runner_vm_names={"lakebase": "vm-r5-0", "competitor": "vm-r5-1"})
    pgbouncer = AzurePgBouncer(azure, server=SERVER, host=HOST, database="postgres",  # type: ignore[arg-type]
                               user="admin", password=lambda: "pw", ca_file="/tmp/roots.pem",
                               connector=azure.connect, sleep=no_sleep)
    return AzureConnectionSpikeSetupOrchestrator(
        config, arm=azure, pgbouncer=pgbouncer, journal=journal or MemoryJournal(),  # type: ignore[arg-type]
        fence=fence or Fence(), fresh_lakebase_host=fresh_host, sleep=no_sleep)


async def bell(orchestrator: AzureConnectionSpikeSetupOrchestrator, events: list[str]) -> Any:
    await orchestrator.warm(1)
    await orchestrator.prepare(BOUT, 7)
    await orchestrator.precommit_launch_intent(BOUT, 7)

    async def staged(stop: Any) -> None:
        events.append(f"stage:{stop.lane_id}:{stop.endpoint_host}")

    async def ready(stop: Any) -> None:
        events.append(f"ready:{stop.lane_id}:{stop.endpoint_host}")

    return await orchestrator.setup(BOUT, 7, on_lane_ready=ready, on_lane_stage=staged,
                                    t0_ns=orchestrator._monotonic_ns())


async def test_the_bell_switches_pgbouncer_on_stages_the_runner_then_releases_it_on_6432() -> None:
    azure, journal, events = FakeAzure(), MemoryJournal(), []
    orchestrator = build(azure, journal)
    result = await bell(orchestrator, events)
    # The intent was journaled before the bell, the switch after it.
    assert journal.states() == ["create_intent", "created"]
    assert journal.rows[0].resource_kind == TIMED_KIND
    # The competitor is staged on the server's own host while PgBouncer comes up, and released
    # only once it answers with the round's limit.
    assert events.index(f"stage:competitor:{HOST}") < events.index(f"ready:competitor:{HOST}")
    assert "ready:lakebase:lakebase.test" in events
    assert azure.writes == [("pgbouncer.enabled", "true"),
                            ("pgbouncer.max_client_conn", ROUND5_MAX_CLIENT_CONN)]
    competitor = result.competitor
    assert competitor.create_db_proxy_requested_ns is not None
    assert competitor.create_db_proxy_requested_ns >= result.t0_ns
    assert competitor.endpoint_host == HOST
    observation = result.observations[1]
    assert observation.stop_gate_evidence.gate_id == GATE_ID
    facts = {fact.key: fact.value for fact in observation.stop_gate_evidence.observed}
    assert facts == {"pgbouncer_enabled": True, "max_client_conn": 10100, "pooled_port": 6432,
                     "pooled_transaction_committed": True}
    assert observation.requires_create_db_proxy_stamp


async def test_a_slow_staging_never_holds_the_competitors_setup_clock() -> None:
    # The resident's staging runs beside PgBouncer's gate: the setup clock stops at the gate,
    # and the release still waits for the stage to finish.
    azure, journal = FakeAzure(), MemoryJournal()
    orchestrator = build(azure, journal)
    await orchestrator.warm(1)
    await orchestrator.prepare(BOUT, 7)
    await orchestrator.precommit_launch_intent(BOUT, 7)
    staged = asyncio.Event()
    gate_seen: list[bool] = []

    async def slow_stage(stop: Any) -> None:
        # Staging that outlasts the gate: it finishes only once the gate has opened, however fast
        # the machine (counting loop turns raced the gate's threads on a slow CI runner). Had the
        # staging held the gate up, the gate could never open here, and this times out.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10.0
        while not (azure.enabled and azure.probes_since_switch > 2):
            assert loop.time() < deadline, "the gate never opened beside the staging"
            await asyncio.sleep(0.01)
        gate_seen.append(True)
        staged.set()

    ready: list[str] = []

    async def on_ready(stop: Any) -> None:
        if stop.lane_id == "competitor":  # Lakebase's lane has nothing to stage
            assert staged.is_set(), "released before the resident was staged"
        ready.append(stop.lane_id)

    result = await orchestrator.setup(BOUT, 7, on_lane_ready=on_ready, on_lane_stage=slow_stage,
                                      t0_ns=orchestrator._monotonic_ns())
    assert gate_seen == [True]  # the gate had opened while staging was still running
    assert "competitor" in ready
    assert result.competitor.stopped_ns <= orchestrator._monotonic_ns()


async def test_cleanup_is_the_baseline_reset_and_crosses_the_delete_handoff() -> None:
    azure, journal = FakeAzure(), MemoryJournal()
    orchestrator = build(azure, journal)
    await bell(orchestrator, [])
    azure.writes.clear()
    await orchestrator.begin_cleanup(BOUT)
    await orchestrator.wait_for_proxy_delete_accepted(BOUT)
    await orchestrator.wait_for_cleanup_complete(BOUT)
    assert orchestrator.proxy_delete_accepted(BOUT)
    # The limit back to Azure's default first, then off: nothing pre-staged survives.
    assert azure.writes == [("pgbouncer.max_client_conn", DEFAULT_MAX_CLIENT_CONN),
                            ("pgbouncer.enabled", "false")]
    assert journal.states()[-2:] == ["delete_intent", "deleted"]
    assert await journal.unresolved_bout_ids() == ()
    await orchestrator.prove_bout_absent(BOUT)


async def test_a_second_cleanup_pass_waits_for_the_reset_in_flight_and_writes_nothing() -> None:
    # After a bout the warm owner runs a reconstructive pass over the bout's own cleanup
    # (`reconcile_post_bell_claim`). AWS's DeleteDBProxy is idempotent; two PgBouncer resets race
    # their ARM writes. The first live bout reset it twice at once (2026-10-20).
    azure, journal = FakeAzure(), MemoryJournal()
    orchestrator = build(azure, journal)
    await bell(orchestrator, [])
    azure.writes.clear()
    azure.write_seconds = 0.05
    await asyncio.gather(orchestrator.cleanup(BOUT),
                         orchestrator.reconcile_failed_cleanup(BOUT, 7))
    assert azure.writes == [("pgbouncer.max_client_conn", DEFAULT_MAX_CLIENT_CONN),
                            ("pgbouncer.enabled", "false")]
    assert not azure.enabled and await journal.unresolved_bout_ids() == ()


async def test_warm_resets_a_pgbouncer_no_cleanup_owns_and_waits_on_one_that_does() -> None:
    azure, journal = FakeAzure(), MemoryJournal()
    azure.enabled, azure.limit = True, "10100"
    await build(azure, journal).warm(1)
    assert not azure.enabled and azure.limit == DEFAULT_MAX_CLIENT_CONN
    # With a bout's cleanup outstanding, that cleanup owns it: warm waits, as AWS's warm does on
    # a Proxy still deleting.
    orchestrator = build(azure, journal)
    await bell(orchestrator, [])
    with pytest.raises(ConnectionSpikeLiveTransientError, match="cleanup still owns it"):
        await build(azure, journal).warm(2)


async def test_warm_retries_what_passes_and_refuses_a_server_that_is_not_the_sealed_one() -> None:
    # AWS's taxonomy: a runner briefly not ready and a busy source are retried; drift blocks.
    azure = FakeAzure()
    azure.runners["vm-r5-1"] = False
    with pytest.raises(ConnectionSpikeLiveTransientError, match="competitor runner VM"):
        await build(azure).warm(1)
    azure.runners["vm-r5-1"], azure.server_state = True, "Stopped"
    with pytest.raises(ConnectionSpikeLiveSourceUnavailableError, match="not serving"):
        await build(azure).warm(1)
    # A parameter write leaves the server Updating, and every bout's own reset is one.
    azure.server_state = "Updating"
    await build(azure).warm(1)
    azure.server_state = "Ready"
    orchestrator = build(azure)
    orchestrator.config = type(orchestrator.config)(  # type: ignore[misc]
        **{**orchestrator.config.__dict__, "competitor_host": "elsewhere.test"})
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="sealed"):
        await orchestrator.warm(1)


async def test_a_stale_owner_never_switches_pgbouncer_on() -> None:
    # The bell capability is checked with no await before the switch, as before CreateDBProxy.
    azure, journal = FakeAzure(), MemoryJournal()
    orchestrator = build(azure, journal)
    await orchestrator.warm(1)
    await orchestrator.prepare(BOUT, 7)
    await orchestrator.precommit_launch_intent(BOUT, 7)
    orchestrator.arm_bell_capability(BOUT, 7, ttl_seconds=-1)
    with pytest.raises(JournalMutationError) as raised:
        await orchestrator.setup(BOUT, 7)
    assert isinstance(raised.value.__cause__, ConnectionSpikeLiveOperationError)
    assert "bell capability expired" in str(raised.value.__cause__)
    assert ("pgbouncer.enabled", "true") not in azure.writes
    assert "create_failed" in journal.states()


async def test_the_absence_proof_needs_no_debt_and_pgbouncer_off() -> None:
    azure, journal = FakeAzure(), MemoryJournal()
    orchestrator = build(azure, journal)
    await orchestrator.prove_bout_absent("bout-never-ran")
    await bell(orchestrator, [])
    with pytest.raises(ConnectionSpikeCleanupError, match="journal debt"):
        await orchestrator.prove_bout_absent(BOUT)
    with pytest.raises(ConnectionSpikeCleanupError, match="still on"):
        await orchestrator.prove_bout_absent("bout-other")


async def test_a_restarted_app_resets_a_pgbouncer_its_predecessor_left_on() -> None:
    # The inherited `reconcile_failed_cleanup`, the path a restart's warm owner takes.
    azure, journal = FakeAzure(), MemoryJournal()
    await bell(build(azure, journal), [])
    assert azure.enabled and await journal.unresolved_bout_ids() == (BOUT,)
    restarted = build(azure, journal)
    await restarted.reconcile_failed_cleanup(BOUT, 8)
    assert not azure.enabled and azure.limit == DEFAULT_MAX_CLIENT_CONN
    assert await journal.unresolved_bout_ids() == ()


async def test_a_lost_fence_refuses_the_reset() -> None:
    azure, journal, fence = FakeAzure(), MemoryJournal(), Fence()
    orchestrator = build(azure, journal, fence)
    await bell(orchestrator, [])
    fence.current = False
    await orchestrator.begin_cleanup(BOUT)
    with pytest.raises(ConnectionSpikeLiveOperationError, match="no longer current"):
        await orchestrator.wait_for_cleanup_complete(BOUT)
    assert azure.enabled  # the next owner's, not a stale one's, to reset


def test_every_override_still_replaces_a_method_of_awss_orchestrator() -> None:
    # A rename on AWS's side would leave an Azure override dead and AWS's method live on Azure.
    from server.connection_spike_live import LiveConnectionSpikeSetupOrchestrator

    overrides = {name for name, value in vars(AzureConnectionSpikeSetupOrchestrator).items()
                 if callable(value) and not name.startswith("__")}
    azure_only = {"_require_sealed_server", "_observed", "_switch_on", "_inspect_pgbouncer",
                  "_reset_pgbouncer", "_validate"}
    missing = sorted(name for name in overrides - azure_only
                     if not hasattr(LiveConnectionSpikeSetupOrchestrator, name))
    assert missing == []
    # And the sequencing Azure relies on is still AWS's, not overridden here.
    for inherited in ("setup", "prepare", "precommit_launch_intent", "begin_cleanup",
                      "reconcile_failed_cleanup", "_setup_lakebase", "_cleanup_exactly"):
        assert inherited not in vars(AzureConnectionSpikeSetupOrchestrator), inherited


def test_the_config_needs_one_distinct_runner_per_lane() -> None:
    with pytest.raises(ConnectionSpikeLiveConfigurationError, match="distinct"):
        AzureRound5SetupConfig(
            baseline_sha256=SEAL, deterministic_name_prefix="p", lakebase_pooled_host="l",
            lakebase_credential_sha256="b" * 64, competitor_credential_sha256="c" * 64,
            proxy_secret_arn="arn:x", competitor_target_id=SERVER,
            competitor_resource_id=SERVER_ID, competitor_host=HOST,
            runner_vm_names={"lakebase": "vm", "competitor": "vm"})
