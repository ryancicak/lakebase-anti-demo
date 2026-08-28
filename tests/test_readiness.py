import asyncio
from contextlib import suppress
from datetime import timedelta
from types import SimpleNamespace

import psycopg
import pytest

import server.readiness as readiness_module
from server.capacity import LAKEBASE_SUSPEND_SECONDS
from server.coordination import (
    ROUND5_RING_KEY,
    CoordinationObjectsMissingError,
    InMemoryBoutLeaseStore,
    is_retryable_startup_error,
    round_ring_key,
)
from server.manager import InvalidStateError, RunManager
from server.models import BoutOperator, RoundId, SessionState
from server.readiness import (
    MAINTENANCE_COPY,
    ROUND5_IDLE_POLL_SECONDS,
    ShowtimeReadinessGate,
    StartupReadinessStore,
)


class DurableFakeLeaseStore(InMemoryBoutLeaseStore):
    mode = "lakebase"

    async def _run(self, _operation):
        raise AssertionError("the injected readiness state store owns test persistence")


class FakeReadinessStore:
    def __init__(self, leases: DurableFakeLeaseStore) -> None:
        self.leases = leases
        self.value = None

    async def initialize(self) -> None:
        return None

    async def read(self):
        return self.value

    async def ring_generation(self) -> int:
        return self.leases._generation

    async def write(self, lease, *, manifest_seal, state, detail) -> None:
        current = await self.leases.current()
        assert current is not None
        assert current.lease_id == lease.lease_id
        assert current.fencing_token == lease.fencing_token
        assert current.phase == "startup_cleanup"
        self.value = SimpleNamespace(
            manifest_seal=manifest_seal,
            state=state,
            detail=detail,
            fencing_token=lease.fencing_token,
        )


class CountingLeaseStore(DurableFakeLeaseStore):
    """Counts the ring reads that would each open their own connection."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.reads = 0

    async def current(self):
        self.reads += 1
        return await super().current()


class CountingReadinessStore(FakeReadinessStore):
    def __init__(self, leases) -> None:
        super().__init__(leases)
        self.reads = 0

    async def read(self):
        self.reads += 1
        return await super().read()

    async def ring_generation(self) -> int:
        self.reads += 1
        return await super().ring_generation()


class CountingJournal:
    """A journal whose unresolved set can change after the gate has settled."""

    def __init__(self, unresolved: tuple[str, ...] = ()) -> None:
        self.unresolved = list(unresolved)
        self.reads = 0

    async def unresolved_bout_ids(self):
        self.reads += 1
        return tuple(self.unresolved)

    async def scopes(self, bout_id):
        return (SimpleNamespace(bout_id=bout_id),)

    async def events(self, _scope):
        return (SimpleNamespace(metadata={"competitor_id": "rds_postgres"}),)


class CleanupEngine:
    def __init__(self, calls: list[str], name: str, *, fail: bool = False) -> None:
        self.calls = calls
        self.name = name
        self.fail = fail

    async def reset_all(self) -> None:
        self.calls.append(self.name)
        if self.fail:
            raise RuntimeError("ownership mismatch")


def manifest(*, round5_ready: bool = False):
    return SimpleNamespace(
        run_id="ad-readiness-001",
        round5_ready=round5_ready,
        round4=SimpleNamespace(app_service_principal_client_id="app-client-id"),
        model_dump_json=lambda **_kwargs: '{"run_id":"ad-readiness-001"}',
    )


async def test_durable_ready_generation_makes_replica_cleanup_a_singleton() -> None:
    leases = DurableFakeLeaseStore()
    state = FakeReadinessStore(leases)
    calls: list[str] = []

    def gate() -> ShowtimeReadinessGate:
        return ShowtimeReadinessGate(
            manifest(),
            leases,
            safe_change_factory=lambda _manifest: CleanupEngine(calls, "round2"),
            recovery_factory=lambda _manifest: CleanupEngine(calls, "round3"),
            round5_factory=None,
            state_store=state,
            heartbeat_seconds=0.01,
            lease_seconds=1,
        )

    leader = gate()
    follower = gate()
    await leader.run()
    await follower.run()

    assert calls == ["round2", "round3"]
    assert leader.status.ring_ready is True
    assert follower.status.ring_ready is True
    assert await leases.current() is None


async def test_cleanup_ownership_failure_is_durably_fail_closed() -> None:
    leases = DurableFakeLeaseStore()
    state = FakeReadinessStore(leases)
    calls: list[str] = []
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: CleanupEngine(calls, "round2", fail=True),
        recovery_factory=lambda _manifest: CleanupEngine(calls, "round3"),
        round5_factory=None,
        state_store=state,
    )

    await gate.run()

    assert gate.status.maintenance_state == "blocked"
    assert state.value.state == "blocked"
    with pytest.raises(InvalidStateError, match="OWNERSHIP OR SEAL"):
        gate.require_ready()

    retry = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: CleanupEngine(calls, "round2-retry"),
        recovery_factory=lambda _manifest: CleanupEngine(calls, "round3-retry"),
        round5_factory=None,
        state_store=state,
    )
    await retry.run()

    assert retry.status.ring_ready is True
    assert state.value.state == "ready"


async def test_busy_round5_lease_does_not_block_main_readiness() -> None:
    main_leases = DurableFakeLeaseStore()
    round5_leases = DurableFakeLeaseStore(ring_key=ROUND5_RING_KEY)
    main_state = FakeReadinessStore(main_leases)
    round5_state = FakeReadinessStore(round5_leases)
    calls: list[str] = []
    await round5_leases.claim(
        session_id="active-round5",
        operator=BoutOperator(display_name="Round 5 owner", subject="round5-owner"),
        phase="cleanup_retry",
        session_state=SessionState.FAILED,
        round_id="survive_connection_spike",
        round_title="Get spike-ready",
        competitor_id="rds_postgres",
        competitor_name="RDS PostgreSQL",
        ttl=timedelta(minutes=1),
    )
    gate = ShowtimeReadinessGate(
        manifest(round5_ready=True),
        main_leases,
        round5_lease_store=round5_leases,
        safe_change_factory=lambda _manifest: CleanupEngine(calls, "round2"),
        recovery_factory=lambda _manifest: CleanupEngine(calls, "round3"),
        round5_factory=None,
        state_store=main_state,
        round5_state_store=round5_state,
        poll_seconds=0.01,
    )

    task = asyncio.create_task(gate.run())
    try:
        for _ in range(20):
            if gate.status.ring_ready:
                break
            await asyncio.sleep(0.01)

        assert gate.status.ring_ready is True
        assert gate.round5_status.ring_ready is False
        assert gate.round5_status.maintenance_state == "maintenance"
        assert gate.round5_status.reason_code == "cleanup_in_progress"
        gate.require_ready()
        with pytest.raises(InvalidStateError, match="BACKSTAGE CLEANUP"):
            gate.require_round5_ready()
        assert calls == ["round2", "round3"]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_round5_monitor_reconciles_old_bout_after_active_lease_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_leases = DurableFakeLeaseStore()
    round5_leases = DurableFakeLeaseStore(ring_key=ROUND5_RING_KEY)
    active = await round5_leases.claim(
        session_id="current-bout",
        operator=BoutOperator(display_name="Round 5 owner", subject="round5-owner"),
        phase="cleanup_retry",
        session_state=SessionState.FAILED,
        round_id="survive_connection_spike",
        round_title="Get spike-ready",
        competitor_id="rds_postgres",
        competitor_name="RDS PostgreSQL",
        ttl=timedelta(minutes=1),
    )

    class FakeJournal:
        unresolved = ["old-bout"]

        async def unresolved_bout_ids(self):
            return tuple(self.unresolved)

        async def scopes(self, bout_id):
            return (SimpleNamespace(bout_id=bout_id),)

        async def events(self, _scope):
            return (SimpleNamespace(metadata={"competitor_id": "rds_postgres"}),)

    journal = FakeJournal()
    monkeypatch.setattr(
        readiness_module,
        "LakebaseCreationJournalStore",
        lambda *_args, **_kwargs: journal,
    )
    reconciled: list[tuple[str, int]] = []

    class Round5Cleanup:
        async def reconcile_failed_cleanup(self, bout_id, fencing_token) -> None:
            authority = await round5_leases.current()
            assert authority is not None
            assert authority.session_id == bout_id
            assert authority.fencing_token == fencing_token
            reconciled.append((bout_id, fencing_token))
            journal.unresolved.remove(bout_id)

    gate = ShowtimeReadinessGate(
        manifest(round5_ready=True),
        main_leases,
        round5_lease_store=round5_leases,
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=lambda *_args: Round5Cleanup(),
        state_store=FakeReadinessStore(main_leases),
        round5_state_store=FakeReadinessStore(round5_leases),
        poll_seconds=0.01,
    )

    task = asyncio.create_task(gate.run())
    try:
        for _ in range(20):
            if gate.status.ring_ready:
                break
            await asyncio.sleep(0.01)
        assert gate.status.ring_ready is True
        assert gate.round5_status.ring_ready is False
        assert reconciled == []

        assert await round5_leases.release(active) is True
        for _ in range(30):
            if reconciled and gate.round5_status.ring_ready:
                break
            await asyncio.sleep(0.01)

        assert reconciled[0][0] == "old-bout"
        assert reconciled[0][1] > active.fencing_token
        assert gate.status.ring_ready is True
        assert gate.round5_status.ring_ready is True
        assert gate.round5_status.reason_code is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_round5_prearm_guard_rejects_other_unresolved_bout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_leases = DurableFakeLeaseStore()
    round5_leases = DurableFakeLeaseStore(ring_key=ROUND5_RING_KEY)
    current = await round5_leases.claim(
        session_id="current-bout",
        operator=BoutOperator(display_name="Round 5 owner", subject="round5-owner"),
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id="survive_connection_spike",
        round_title="Get spike-ready",
        competitor_id="rds_postgres",
        competitor_name="RDS PostgreSQL",
        ttl=timedelta(minutes=1),
    )

    class FakeJournal:
        unresolved = ("old-bout", "current-bout")

        async def unresolved_bout_ids(self):
            return self.unresolved

    journal = FakeJournal()
    monkeypatch.setattr(
        readiness_module,
        "LakebaseCreationJournalStore",
        lambda *_args, **_kwargs: journal,
    )
    gate = ShowtimeReadinessGate(
        manifest(round5_ready=True),
        main_leases,
        round5_lease_store=round5_leases,
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=None,
        state_store=FakeReadinessStore(main_leases),
        round5_state_store=FakeReadinessStore(round5_leases),
    )

    with pytest.raises(InvalidStateError, match="OTHER ROUNDS ARE READY"):
        await gate.round5_prearm_guard(current.session_id, current.fencing_token)

    journal.unresolved = ("current-bout",)
    await gate.round5_prearm_guard(current.session_id, current.fencing_token)


async def test_v7_round5_prearm_and_journal_use_scoped_cleanup_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_leases = DurableFakeLeaseStore()
    scoped_key = round_ring_key(
        "install-a",
        RoundId.SURVIVE_CONNECTION_SPIKE.value,
        cleanup=True,
    )
    round5_leases = DurableFakeLeaseStore(ring_key=scoped_key)
    current = await round5_leases.claim(
        session_id="current-bout",
        operator=BoutOperator(display_name="Round 5 owner", subject="round5-owner"),
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE.value,
        round_title="Get spike-ready",
        competitor_id="rds_postgres",
        competitor_name="RDS PostgreSQL",
        ttl=timedelta(minutes=1),
    )
    authority_keys: list[str] = []

    class FakeJournal:
        async def unresolved_bout_ids(self):
            return (current.session_id,)

    def fake_journal(_run, *, authority_ring_key: str):
        authority_keys.append(authority_ring_key)
        return FakeJournal()

    monkeypatch.setattr(readiness_module, "LakebaseCreationJournalStore", fake_journal)
    gate = ShowtimeReadinessGate(
        manifest(round5_ready=True),
        main_leases,
        round5_lease_store=round5_leases,
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=None,
        state_store=FakeReadinessStore(main_leases),
        round5_state_store=FakeReadinessStore(round5_leases),
    )

    await gate.round5_prearm_guard(current.session_id, current.fencing_token)

    assert authority_keys == [scoped_key]


def test_round5_idle_recheck_interval_clears_the_endpoint_suspend_window() -> None:
    # Every re-read opens its own coordination connection, so an interval inside
    # the suspend window holds the endpoint awake forever. The steady-state
    # interval has to clear the window by enough that the endpoint actually
    # sleeps for most of each cycle.
    assert ROUND5_IDLE_POLL_SECONDS > LAKEBASE_SUSPEND_SECONDS

    gate = ShowtimeReadinessGate(
        manifest(),
        DurableFakeLeaseStore(),
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=None,
    )

    assert gate._idle_poll_seconds > LAKEBASE_SUSPEND_SECONDS
    assert gate._poll_seconds < LAKEBASE_SUSPEND_SECONDS


async def test_round5_reconciler_stops_reading_once_the_state_is_steady(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_leases = DurableFakeLeaseStore()
    round5_leases = CountingLeaseStore(ring_key=ROUND5_RING_KEY)
    round5_state = CountingReadinessStore(round5_leases)
    journal = CountingJournal()
    monkeypatch.setattr(
        readiness_module,
        "LakebaseCreationJournalStore",
        lambda *_args, **_kwargs: journal,
    )
    gate = ShowtimeReadinessGate(
        manifest(round5_ready=True),
        main_leases,
        round5_lease_store=round5_leases,
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=None,
        state_store=FakeReadinessStore(main_leases),
        round5_state_store=round5_state,
        poll_seconds=0.01,
    )

    task = asyncio.create_task(gate.run())
    try:
        for _ in range(200):
            if gate.round5_status.ring_ready:
                break
            await asyncio.sleep(0.01)
        assert gate.round5_status.ring_ready is True

        # Let the loop reach its settled wait, then prove it has genuinely
        # stopped rather than merely slowed: no ring read, no journal read and
        # no durable read for many multiples of the fast cadence.
        await asyncio.sleep(0.05)
        settled = (round5_leases.reads, journal.reads, round5_state.reads)
        await asyncio.sleep(0.5)

        assert (round5_leases.reads, journal.reads, round5_state.reads) == settled
        assert gate.round5_status.ring_ready is True
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_settled_round5_reconciler_wakes_on_demand_to_clean_a_late_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_leases = DurableFakeLeaseStore()
    round5_leases = CountingLeaseStore(ring_key=ROUND5_RING_KEY)
    round5_state = CountingReadinessStore(round5_leases)
    journal = CountingJournal()
    monkeypatch.setattr(
        readiness_module,
        "LakebaseCreationJournalStore",
        lambda *_args, **_kwargs: journal,
    )
    reconciled: list[str] = []

    class Round5Cleanup:
        async def reconcile_failed_cleanup(self, bout_id, fencing_token) -> None:
            authority = await round5_leases.current()
            assert authority is not None
            assert authority.session_id == bout_id
            assert authority.fencing_token == fencing_token
            reconciled.append(bout_id)
            journal.unresolved.remove(bout_id)

    gate = ShowtimeReadinessGate(
        manifest(round5_ready=True),
        main_leases,
        round5_lease_store=round5_leases,
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=lambda *_args: Round5Cleanup(),
        state_store=FakeReadinessStore(main_leases),
        round5_state_store=round5_state,
        poll_seconds=0.01,
    )

    task = asyncio.create_task(gate.run())
    try:
        for _ in range(200):
            if gate.round5_status.ring_ready:
                break
            await asyncio.sleep(0.01)
        assert gate.round5_status.ring_ready is True
        await asyncio.sleep(0.05)

        # A later bout arms and fails, stranding artifacts under its own id.
        current = await round5_leases.claim(
            session_id="current-bout",
            operator=BoutOperator(display_name="Round 5 owner", subject="round5-owner"),
            phase="checking",
            session_state=SessionState.CHECKING,
            round_id="survive_connection_spike",
            round_title="Get spike-ready",
            competitor_id="rds_postgres",
            competitor_name="RDS PostgreSQL",
            ttl=timedelta(minutes=1),
        )
        journal.unresolved = ["old-bout", "current-bout"]
        settled = (round5_leases.reads, journal.reads, round5_state.reads)
        await asyncio.sleep(0.2)

        # Nothing has asked for a fresh answer, so the reconciler is still idle.
        assert (round5_leases.reads, journal.reads, round5_state.reads) == settled
        assert reconciled == []

        # The arm-time guard is the authority, reads fresh, and hands the work over.
        with pytest.raises(InvalidStateError, match="OTHER ROUNDS ARE READY"):
            await gate.round5_prearm_guard(current.session_id, current.fencing_token)
        assert gate.round5_status.ring_ready is False

        journal.unresolved = ["old-bout"]
        assert await round5_leases.release(current) is True
        for _ in range(200):
            if reconciled and gate.round5_status.ring_ready:
                break
            await asyncio.sleep(0.01)

        assert reconciled == ["old-bout"]
        assert gate.round5_status.ring_ready is True
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_bout_status_combines_durable_ring_with_cached_gate() -> None:
    status = SimpleNamespace(
        ring_ready=False,
        maintenance_state="maintenance",
        maintenance_detail=MAINTENANCE_COPY,
    )
    class CountedViewerDatabaseReads(DurableFakeLeaseStore):
        reads = 0

        async def current(self):
            self.reads += 1
            return None

    lease_store = CountedViewerDatabaseReads()
    manager = RunManager(
        lease_store=lease_store,
        readiness_status=lambda: status,
    )

    bout = await manager.bout_status()

    assert bout.ring_ready is False
    assert bout.maintenance_state == "maintenance"
    assert bout.maintenance_detail == MAINTENANCE_COPY
    assert lease_store.reads == 1


# ---------------------------------------------------------------------------
# The readiness row's table, on a principal that consumes the schema
#
# This one does not crash the app -- it runs on the gate's task -- which makes
# it the more dangerous of the two: the app would serve, and refuse every
# control action behind a banner blaming ownership, for a cause that is
# actually a missing GRANT.
# ---------------------------------------------------------------------------


class _ReadinessCatalogCursor:
    def __init__(self, *, table_present: bool) -> None:
        self._table_present = table_present
        self.statements: list[str] = []
        self._pending: list[tuple] = []

    async def execute(self, sql: str, _params: tuple = ()) -> None:
        statement = " ".join(sql.split())
        self.statements.append(statement)
        if statement.upper().startswith(("CREATE", "ALTER", "DROP")):
            raise psycopg.errors.InsufficientPrivilege("permission denied for database anti_demo")
        if "pg_catalog.pg_namespace WHERE nspname" in statement:
            self._pending = [("anti_demo_coordination",)]
        elif "pg_catalog.pg_class" in statement:
            self._pending = [("startup_readiness",)] if self._table_present else []
        else:  # pragma: no cover - initialize issues nothing else
            raise AssertionError(f"unexpected statement: {statement}")

    async def fetchone(self) -> tuple | None:
        return self._pending[0] if self._pending else None

    async def fetchall(self) -> list[tuple]:
        return list(self._pending)


def _readiness_store(cursor: _ReadinessCatalogCursor) -> StartupReadinessStore:
    async def run(operation):
        return await operation(cursor)

    return StartupReadinessStore(run)


def _planned_grant(table: str) -> str:
    """The GRANT setup's own plan says ``table`` needs, never a copy of it.

    Derived for the reason the plan exists: a list of table names and privileges
    typed out in a test agrees with the list typed out beside it, which is how
    `startup_readiness` shipped ungranted for a release with every test green.
    A `KeyError` here is a real failure -- it means the relation the readiness
    gate depends on has left the plan that provisions it.
    """

    from server.lifecycle import _coordination_runtime_grants

    grant = {plan.table: plan for plan in _coordination_runtime_grants()}[table]
    return f"GRANT {', '.join(grant.privileges)} ON {grant.table}"


def _blocked_gate(state_store) -> ShowtimeReadinessGate:
    return ShowtimeReadinessGate(
        manifest(),
        DurableFakeLeaseStore(),
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=None,
        state_store=state_store,
    )


async def test_readiness_starts_on_a_table_it_can_see_but_may_not_create() -> None:
    cursor = _ReadinessCatalogCursor(table_present=True)
    await _readiness_store(cursor).initialize()
    assert [s for s in cursor.statements if s.upper().startswith("CREATE")] == []


async def test_an_absent_readiness_table_blocks_the_gate_by_name() -> None:
    cursor = _ReadinessCatalogCursor(table_present=False)
    with pytest.raises(CoordinationObjectsMissingError) as missing:
        await _readiness_store(cursor).initialize()

    assert "anti_demo_coordination.startup_readiness" in str(missing.value)
    assert "docs/DEPLOY.md" in str(missing.value)
    # Non-retryable, so `_run_main_until_settled` gives up and says so rather
    # than backing off forever against a denial that waiting cannot clear.
    assert is_retryable_startup_error(missing.value) is False

    # That sentence reaches the log and nothing else. What an operator reads is
    # `maintenance_detail`, on `/readyz` and on the stage, and it said the seal
    # and ownership could not be verified -- for an unprovisioned table.
    class Unprovisioned(FakeReadinessStore):
        async def initialize(self) -> None:
            raise missing.value

    gate = _blocked_gate(Unprovisioned(DurableFakeLeaseStore()))
    await gate.run()

    detail = gate.status.maintenance_detail or ""
    assert gate.status.maintenance_state == "blocked"
    assert gate.recovery.state == "given_up"
    # A GRANT cannot name a table that does not exist, so this one is told to
    # provision rather than to grant.
    assert "antidemo setup" in detail
    assert "OWNERSHIP OR SEAL" not in detail


async def test_a_refused_readiness_select_names_the_grant_not_the_seal() -> None:
    """Tonight's failure, as an operator saw it: a `/readyz` 503 blaming the seal.

    The app role held no ACL entry on `startup_readiness`. `initialize()`
    returned early because the table was *present*, so its own
    `InsufficientPrivilege` handler -- the one place written to explain a
    privilege problem -- was never reached, and the refusal surfaced at the
    `SELECT` in `read()` instead. The banner then said OWNERSHIP OR SEAL COULD
    NOT BE VERIFIED, which names the manifest seal and the AWS ownership tags,
    and the first hours went into IAM. The cause was four lines of SQL.
    """

    class Ungranted(FakeReadinessStore):
        async def read(self):
            # Verbatim what Postgres raises for a relation a role may see in
            # `pg_catalog` but not read: SQLSTATE 42501, naming the relation in
            # the message text and nowhere else -- `diag.table_name` is empty
            # for an ACL failure.
            raise psycopg.errors.InsufficientPrivilege(
                "permission denied for table startup_readiness"
            )

    gate = _blocked_gate(Ungranted(DurableFakeLeaseStore()))
    await gate.run()

    detail = gate.status.maintenance_detail or ""
    assert gate.status.maintenance_state == "blocked"
    # Unchanged on purpose: no retry clears a missing grant, so the schedule
    # still stops. What changes is what it says while stopped.
    assert gate.recovery.state == "given_up"
    assert gate.recovery.error == "InsufficientPrivilege"
    # The remedy, and the relation, taken from the plan setup issues.
    assert _planned_grant(readiness_module.READINESS_TABLE) in detail
    assert "docs/DEPLOY.md" in detail
    # The wording is the whole defect: this is a Lakebase ACL, and the banner
    # that sent the reader to IAM and to the seal cost the night.
    assert "NOT AWS IAM" in detail
    assert "OWNERSHIP OR SEAL" not in detail


async def test_a_blocked_gate_reports_what_the_control_plane_actually_said() -> None:
    """The banner that turned minutes into hours, with the answer put back in it.

    Round 2's Lakebase lane reached the control plane through a `databricks`
    subprocess, and the runner discarded its stderr for secret safety. So the
    lane reported only "Databricks control-plane command failed", that is in no
    named class, and every round on the card was refused behind
    "OWNERSHIP OR SEAL COULD NOT BE VERIFIED" -- a sentence about the manifest
    seal and AWS ownership tags, for a container that had the wrong CLI.

    Two things have to hold for that to be closed, and the second is the one a
    previous defect got wrong:

    * a `DatabricksError`'s own words survive, because they are the workspace
      answering a question about this app's authorization, while a psycopg or
      botocore message contributes its type name alone;
    * and the chain is walked through the *fan-in*. `SafeChangeResetError`
      summarises independent lanes, so its own `__cause__` is `None` and the real
      failures hang off `underlying_causes()`. Reporting the head of that chain
      reports the word "SafeChangeResetError" and nothing else.
    """

    from databricks.sdk.errors import PermissionDenied

    from server.safe_change import (
        SafeChangeProvider,
        SafeChangeResetError,
        SafeChangeResetLaneResult,
        SafeChangeResetResult,
    )

    refusal = PermissionDenied(
        "assign the user 'Can Use' or 'Can Manage' for Database project"
    )
    lane_failure = RuntimeError("Databricks control-plane request was refused")
    lane_failure.__cause__ = refusal
    # Not `OperationalError`: that one is transient, so a mixed card would stay
    # retryable and this test would never reach a blocked banner. The point of
    # this lane is only that a non-Databricks, non-`server` message is reduced to
    # its type name however it got here.
    secret_bearing = psycopg.ProgrammingError(
        "role cannot connect: password=hunter2"
    )

    class Refused(CleanupEngine):
        async def reset_all(self) -> None:
            raise SafeChangeResetError(
                SafeChangeResetResult(
                    competitor=None,
                    lanes={
                        "lakebase": SafeChangeResetLaneResult(
                            lane_id="lakebase",
                            name="Lakebase",
                            provider=SafeChangeProvider.LAKEBASE,
                            artifact_id="safe-change-ad-readiness-001",
                            ok=False,
                            error="Databricks control-plane request was refused",
                            cause=lane_failure,
                        ),
                        "aurora": SafeChangeResetLaneResult(
                            lane_id="aurora",
                            name="Aurora",
                            provider=SafeChangeProvider.AURORA,
                            artifact_id="safe-change-ad-readiness-001",
                            ok=False,
                            error="the isolated cluster could not be reached",
                            cause=secret_bearing,
                        ),
                    },
                )
            )

    leases = DurableFakeLeaseStore()
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: Refused([], "round2"),
        recovery_factory=lambda _manifest: CleanupEngine([], "round3"),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
    )
    await gate.run()

    detail = gate.status.maintenance_detail or ""
    assert gate.status.maintenance_state == "blocked"
    assert gate.recovery.state == "given_up"
    # What the workspace said, reached through the fan-in and past two wrappers.
    assert "Can Use" in detail
    assert "Database project" in detail
    assert "PermissionDenied" in detail
    # The other lane is named, but only by type: its message carries a password.
    assert "ProgrammingError" in detail
    assert "hunter2" not in detail
    assert "password=" not in detail


async def test_a_retrying_gate_says_why_and_not_only_what_class_failed() -> None:
    """The state an operator actually stands in front of, because it never ends.

    `given_up` stops after one attempt and is the loud failure. Retrying is the
    quiet one: a deployed app sat at `BACKSTAGE CLEANUP RETRYING · ATTEMPT 5
    FAILED (RECOVERYRESETERROR)` with a lane underneath being refused by name and
    the banner carrying nothing but the class name of the summary object. The
    same redaction boundary applies here as on the blocked banner -- this asserts
    both halves of it, so the sentence cannot start leaking to earn its detail.
    """

    from databricks.sdk.errors import TemporarilyUnavailable

    from server.recovery import RecoveryResetError
    from server.safe_change import (
        SafeChangeProvider,
        SafeChangeResetLaneResult,
        SafeChangeResetResult,
    )

    # Transient on purpose: a permanent cause gives up instead of retrying, and
    # the retrying banner is the thing under test.
    refusal = TemporarilyUnavailable("the Lakebase control plane is unavailable")
    lane_failure = RuntimeError("Databricks control-plane request was refused")
    lane_failure.__cause__ = refusal
    secret_bearing = psycopg.OperationalError("could not connect: password=hunter2")

    attempts = 0

    class Flaky(CleanupEngine):
        async def reset_all(self) -> None:
            nonlocal attempts
            attempts += 1
            raise RecoveryResetError(
                SafeChangeResetResult(
                    competitor=None,
                    lanes={
                        "lakebase": SafeChangeResetLaneResult(
                            lane_id="lakebase",
                            name="Lakebase",
                            provider=SafeChangeProvider.LAKEBASE,
                            artifact_id="recovery-ad-readiness-001",
                            ok=False,
                            error="Databricks control-plane request was refused",
                            cause=lane_failure,
                        ),
                        "aurora": SafeChangeResetLaneResult(
                            lane_id="aurora",
                            name="Aurora",
                            provider=SafeChangeProvider.AURORA,
                            artifact_id="recovery-ad-readiness-001",
                            ok=False,
                            error="the recovery cluster could not be reached",
                            cause=secret_bearing,
                        ),
                    },
                )
            )

    leases = DurableFakeLeaseStore()
    gate = ShowtimeReadinessGate(
        manifest(),
        leases,
        safe_change_factory=lambda _manifest: CleanupEngine([], "round2"),
        recovery_factory=lambda _manifest: Flaky([], "round3"),
        round5_factory=None,
        state_store=FakeReadinessStore(leases),
    )
    task = asyncio.create_task(gate.run())
    try:
        # Bounded, in the cadence the rest of this file waits on the gate with:
        # the state is set after `reset_all` raises, inside the gate's own loop.
        for _ in range(200):
            if gate.recovery.state in {"retrying", "escalated"}:
                break
            await asyncio.sleep(0.01)
        detail = gate.recovery.detail or ""
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    # The sentence the control API and the operator already match on survives.
    assert "RETRYING" in detail
    assert "RECOVERYRESETERROR" in detail
    # ... and now it also says what the control plane said. The banner is
    # upper-cased for the ring, so the comparison is too.
    assert "TEMPORARILYUNAVAILABLE" in detail
    assert "THE LAKEBASE CONTROL PLANE IS UNAVAILABLE" in detail
    # The other lane by type only. A banner that never stops being shown is the
    # last place a password should be able to appear.
    assert "OPERATIONALERROR" in detail
    assert "hunter2" not in detail
    assert "password=" not in detail
