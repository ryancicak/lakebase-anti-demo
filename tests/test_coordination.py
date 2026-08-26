from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import psycopg
import pytest
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    UnauthorizedSSOTokenError,
)
from databricks.sdk.errors import (
    BadRequest,
    NotFound,
    PermissionDenied,
    TemporarilyUnavailable,
    TooManyRequests,
    Unauthenticated,
)

from server import coordination
from server.coordination import (
    ALLOW_INMEMORY_COORDINATION_ENV,
    COORDINATION_ENDPOINT_ENV,
    CREDENTIAL_FAULT_SUBJECT,
    INMEMORY_COORDINATION_LOSSES,
    RING_KEY,
    ROUND5_RING_KEY,
    SERVICE_FAULT_SUBJECT,
    BoutLease,
    InMemoryBoutLeaseStore,
    LakebaseBoutLeaseStore,
    LeaseHeldError,
    LeaseLostError,
    build_lease_store,
    diagnose_held_lease,
    environment_fault_subject,
    is_retryable_startup_error,
    is_transient_coordination_error,
    lease_heartbeat_seconds,
    round_ring_key,
)
from server.manager import InvalidStateError
from server.models import BoutOperator, SessionState
from server.readiness import RingFenceLostError, ShowtimeReadinessGate
from server.safe_change import (
    SafeChangeProvider,
    SafeChangeResetError,
    SafeChangeResetLaneResult,
    SafeChangeResetResult,
)


@dataclass
class MutableClock:
    now: datetime

    def __call__(self) -> datetime:
        return self.now


def operator(email: str, subject: str) -> BoutOperator:
    return BoutOperator(display_name=email.split("@", 1)[0], email=email, subject=subject)


async def _ready(value: object) -> object:
    return value


def _lease_at(*, updated_at: datetime, expires_at: datetime) -> BoutLease:
    owner = operator("operator@example.com", "user-a")
    return BoutLease(
        lease_id="00000000-0000-0000-0000-000000000001",
        fencing_token=1,
        session_id="session-a",
        operator=owner,
        owner_subject="user-a",
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id="wake_idle_app",
        round_title="Wake this idle app",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        started_at=updated_at,
        updated_at=updated_at,
        expires_at=expires_at,
    )


async def claim(
    store: InMemoryBoutLeaseStore,
    session_id: str,
    owner: BoutOperator,
    *,
    ttl: timedelta = timedelta(minutes=1),
    expected_previous_token: int | None = None,
):
    return await store.claim(
        session_id=session_id,
        operator=owner,
        phase="checking",
        session_state=SessionState.CHECKING,
        round_id="wake_idle_app",
        round_title="Wake this idle app",
        competitor_id="aurora_serverless_v2",
        competitor_name="Aurora Serverless v2",
        ttl=ttl,
        expected_previous_token=expected_previous_token,
    )


async def test_fenced_lease_has_one_owner_and_atomic_run_commit() -> None:
    store = InMemoryBoutLeaseStore()
    owner_operator = operator("operator@example.com", "user-123")
    another = operator("another@databricks.com", "user-456")
    lease = await claim(store, "session-a", owner_operator)

    with pytest.raises(LeaseHeldError) as held:
        await claim(store, "session-b", another)
    assert held.value.lease.operator.email == "operator@example.com"

    armed = await store.transition(
        lease,
        operator=owner_operator,
        expected_phase="checking",
        phase="armed",
        session_state=SessionState.ARMED,
        ttl=timedelta(seconds=60),
    )
    with pytest.raises(LeaseLostError):
        await store.transition(
            armed,
            operator=another,
            expected_phase="armed",
            phase="run_committed",
            session_state=SessionState.RUNNING,
            ttl=timedelta(minutes=30),
        )

    committed = await store.transition(
        armed,
        operator=owner_operator,
        expected_phase="armed",
        phase="run_committed",
        session_state=SessionState.RUNNING,
        ttl=timedelta(minutes=30),
    )
    assert committed.phase == "run_committed"
    assert committed.fencing_token == lease.fencing_token


async def test_expiry_increments_fence_and_stale_owner_cannot_release_successor() -> None:
    clock = MutableClock(datetime(2026, 8, 17, 20, 0, tzinfo=UTC))
    store = InMemoryBoutLeaseStore(clock=clock)
    first = await claim(
        store,
        "session-a",
        operator("first@databricks.com", "user-a"),
        ttl=timedelta(seconds=5),
    )
    clock.now += timedelta(seconds=6)
    second = await claim(store, "session-b", operator("second@databricks.com", "user-b"))

    assert second.fencing_token == first.fencing_token + 1
    assert await store.release(first) is False
    assert (await store.current()) == second


async def test_stale_continuity_claim_does_not_consume_a_fencing_token() -> None:
    store = InMemoryBoutLeaseStore()
    owner_operator = operator("operator@example.com", "user-a")
    first = await claim(store, "session-a", owner_operator)
    assert await store.release(first) is True
    second = await claim(store, "session-b", owner_operator)
    assert await store.release(second) is True

    with pytest.raises(LeaseLostError, match="FENCE CHANGED"):
        await claim(
            store,
            "session-a",
            owner_operator,
            expected_previous_token=first.fencing_token,
        )

    rightful = await claim(
        store,
        "session-b",
        owner_operator,
        expected_previous_token=second.fencing_token,
    )
    assert rightful.fencing_token == second.fencing_token + 1


async def test_main_and_round5_leases_are_independent() -> None:
    main = InMemoryBoutLeaseStore(ring_key=RING_KEY)
    round5 = InMemoryBoutLeaseStore(ring_key=ROUND5_RING_KEY)
    owner = operator("operator@example.com", "user-a")

    main_first = await claim(main, "main-a", owner)
    round5_first = await claim(round5, "round5-a", owner)

    assert main_first.fencing_token == 1
    assert round5_first.fencing_token == 1
    assert await main.release(main_first) is True
    main_second = await claim(main, "main-b", owner)

    assert main_second.fencing_token == 2
    assert (await round5.current()) == round5_first
    with pytest.raises(LeaseHeldError):
        await claim(round5, "round5-b", owner)


def test_lease_store_rejects_unknown_ring_key() -> None:
    with pytest.raises(ValueError, match="ring_key must be one of"):
        InMemoryBoutLeaseStore(ring_key="Round5")


async def test_scoped_round_keys_share_a_coordinator_but_not_a_lease() -> None:
    base = InMemoryBoutLeaseStore()
    round_one = base.for_ring_key(round_ring_key("install-a", "wake_idle_app"))
    same_round = base.for_ring_key(round_ring_key("install-a", "wake_idle_app"))
    round_four = base.for_ring_key(round_ring_key("install-a", "put_model_score_in_app"))
    another_install = base.for_ring_key(round_ring_key("install-b", "wake_idle_app"))
    owner = operator("operator@example.com", "user-a")

    first = await claim(round_one, "round-one", owner)
    with pytest.raises(LeaseHeldError):
        await claim(same_round, "same-round", owner)

    assert await claim(round_four, "round-four", owner)
    assert await claim(another_install, "another-install", owner)
    assert (await same_round.current()) == first


def test_round_cleanup_key_is_installation_scoped() -> None:
    assert round_ring_key("demo-123", "survive_connection_spike", cleanup=True) == (
        "installation:demo-123:round:survive_connection_spike:cleanup"
    )
    with pytest.raises(ValueError, match="reserved for Round 5"):
        round_ring_key("demo-123", "wake_idle_app", cleanup=True)


def test_databricks_app_fails_closed_without_lakebase_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")
    monkeypatch.delenv("ANTI_DEMO_COORDINATION_ENDPOINT_NAME", raising=False)

    with pytest.raises(RuntimeError, match="process-local locking is not allowed"):
        build_lease_store()


def test_a_deployed_app_refuses_in_memory_even_with_the_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The opt-in is a developer's choice; replicas do not get to make it.

    Two replicas each holding their own in-memory ring is exactly the failure the
    ring exists to prevent, so this refusal is checked before the opt-in is read.
    """
    monkeypatch.setenv("DATABRICKS_APP_NAME", "lakebase-anti-demo")
    monkeypatch.delenv(COORDINATION_ENDPOINT_ENV, raising=False)
    monkeypatch.setenv(ALLOW_INMEMORY_COORDINATION_ENV, "1")

    with pytest.raises(RuntimeError) as refusal:
        build_lease_store()

    message = str(refusal.value)
    assert "process-local locking is not allowed" in message
    assert f"{ALLOW_INMEMORY_COORDINATION_ENV} does not apply to a deployed app" in message


def test_a_local_process_refuses_the_in_memory_ring_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The silent fallback is gone: no endpoint and no opt-in is now a refusal.

    This is the bug being closed. A launcher that bypassed `antidemo serve` never set
    the endpoint name, so every process built a process-local ring and said
    nothing about it.
    """
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv(COORDINATION_ENDPOINT_ENV, raising=False)
    monkeypatch.delenv(ALLOW_INMEMORY_COORDINATION_ENV, raising=False)

    with pytest.raises(RuntimeError) as refusal:
        build_lease_store()

    message = str(refusal.value)
    # An operator reading this has to learn the variable, the supported launch
    # path, and the deliberate escape hatch without opening the source.
    assert COORDINATION_ENDPOINT_ENV in message
    assert "antidemo serve" in message
    assert f"{ALLOW_INMEMORY_COORDINATION_ENV}=1" in message


def test_the_opt_in_permits_the_in_memory_ring_and_names_every_loss(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv(COORDINATION_ENDPOINT_ENV, raising=False)
    monkeypatch.setenv(ALLOW_INMEMORY_COORDINATION_ENV, "1")
    monkeypatch.setattr(coordination, "_inmemory_warning_emitted", False)

    with caplog.at_level("WARNING", logger="server.coordination"):
        store = build_lease_store()
        second = build_lease_store(ring_key=ROUND5_RING_KEY)

    assert isinstance(store, InMemoryBoutLeaseStore)
    assert store.mode == "memory"
    assert isinstance(second, InMemoryBoutLeaseStore)
    warnings = [
        record for record in caplog.records if "DEGRADED COORDINATION" in record.getMessage()
    ]
    # One line per process, not one per ring: the second store must not repeat it.
    assert len(warnings) == 1
    warned = warnings[0].getMessage()
    for loss in INMEMORY_COORDINATION_LOSSES:
        assert loss in warned
    # The enumeration itself is the point, so assert what it has to name rather
    # than only that whatever it says was echoed into the log.
    for capability in (
        "cross-process fencing",
        "durable readiness gate",
        "ring isolation",
        "cost recording",
        "orphan deletion",
        "Round 5",
    ):
        assert capability in warned


async def test_a_live_bout_refusal_names_the_owner_and_the_exact_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", raising=False)
    clock = MutableClock(datetime(2026, 8, 20, 19, 0, tzinfo=UTC))
    store = InMemoryBoutLeaseStore(clock=clock)
    await claim(
        store,
        "session-a",
        operator("operator@example.com", "user-a"),
        ttl=timedelta(seconds=90),
    )
    # One heartbeat interval has not elapsed, so the owner is provably still alive.
    clock.now += timedelta(seconds=4)

    with pytest.raises(LeaseHeldError) as held:
        await claim(store, "session-b", operator("other@databricks.com", "user-b"))

    assert str(held.value) == (
        "BOUT IN PROGRESS · WAKE THIS IDLE APP · CHECKING · OPERATOR · "
        "FENCE 1 · HEARTBEAT 4S AGO · RING UNLOCKS IN 86S"
    )
    assert held.value.diagnosis.likely_stale is False
    assert held.value.diagnosis.remaining_seconds == 86


async def test_a_crashed_owner_is_reported_as_likely_stale_with_its_countdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", raising=False)
    clock = MutableClock(datetime(2026, 8, 20, 19, 0, tzinfo=UTC))
    store = InMemoryBoutLeaseStore(clock=clock)
    await claim(
        store,
        "session-a",
        operator("operator@example.com", "user-a"),
        ttl=timedelta(seconds=90),
    )
    # A restarted server never renews, so the row survives with a quiet heartbeat.
    clock.now += timedelta(seconds=41)

    with pytest.raises(LeaseHeldError) as held:
        await claim(store, "session-b", operator("operator@example.com", "user-a"))

    assert str(held.value) == (
        "BOUT IN PROGRESS · LEASE LIKELY STALE · WAKE THIS IDLE APP · CHECKING · "
        "OPERATOR · FENCE 1 · NO HEARTBEAT FOR 41S · RING UNLOCKS IN 49S"
    )
    assert held.value.diagnosis.likely_stale is True
    assert held.value.diagnosis.heartbeat_age_seconds == 41
    assert held.value.diagnosis.remaining_seconds == 49
    # The threshold it was judged against travels with the diagnosis.
    assert held.value.diagnosis.heartbeat_interval_seconds == 15


async def test_an_armed_lease_is_never_called_stale_for_not_heartbeating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Silence proves nothing about an armed lease, because it never renews.

    `RunManager._mark_bout_armed` cancels the heartbeat and pins the row to the
    arm's expiry on purpose, so every armed bout is silent by construction. The
    heartbeat rule read that silence as a probably-dead owner and printed LEASE
    LIKELY STALE on the ring banner for a bout that was simply waiting for the
    bell -- which is a wrong reading of a healthy ring, on stage, and it invites
    the operator to wait out a countdown they were told not to trust.
    """

    monkeypatch.delenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", raising=False)
    clock = MutableClock(datetime(2026, 8, 20, 19, 0, tzinfo=UTC))
    store = InMemoryBoutLeaseStore(clock=clock)
    lease = await claim(
        store,
        "session-a",
        operator("operator@example.com", "user-a"),
        ttl=timedelta(seconds=90),
    )
    await store.transition(
        lease,
        operator=operator("operator@example.com", "user-a"),
        expected_phase="checking",
        phase="armed",
        session_state=SessionState.ARMED,
        ttl=timedelta(seconds=180),
    )
    # Ordinary presentation pacing: arm, address the room, then ring the bell.
    clock.now += timedelta(seconds=41)

    with pytest.raises(LeaseHeldError) as held:
        await claim(store, "session-b", operator("other@databricks.com", "user-b"))

    assert held.value.diagnosis.likely_stale is False
    assert "LEASE LIKELY STALE" not in str(held.value)
    assert "NO HEARTBEAT" not in str(held.value)
    assert str(held.value) == (
        "BOUT IN PROGRESS · WAKE THIS IDLE APP · ARMED · OPERATOR · "
        "FENCE 1 · ARMED 41S AGO · RING UNLOCKS IN 139S"
    )


async def test_a_heartbeat_clears_the_stale_report_without_touching_the_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", raising=False)
    clock = MutableClock(datetime(2026, 8, 20, 19, 0, tzinfo=UTC))
    store = InMemoryBoutLeaseStore(clock=clock)
    owner = operator("operator@example.com", "user-a")
    lease = await claim(store, "session-a", owner, ttl=timedelta(seconds=90))
    clock.now += timedelta(seconds=41)
    renewed = await store.renew(lease, ttl=timedelta(seconds=90))
    clock.now += timedelta(seconds=4)

    with pytest.raises(LeaseHeldError) as held:
        await claim(store, "session-b", operator("other@databricks.com", "user-b"))

    assert held.value.diagnosis.likely_stale is False
    assert "LEASE LIKELY STALE" not in str(held.value)
    assert "RING UNLOCKS IN 86S" in str(held.value)
    assert renewed.fencing_token == lease.fencing_token


async def test_a_stale_lease_still_clears_only_by_expiring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The diagnosis is advisory: nothing may claim early, and expiry still fences."""
    monkeypatch.delenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", raising=False)
    clock = MutableClock(datetime(2026, 8, 20, 19, 0, tzinfo=UTC))
    store = InMemoryBoutLeaseStore(clock=clock)
    first = await claim(
        store,
        "session-a",
        operator("operator@example.com", "user-a"),
        ttl=timedelta(seconds=90),
    )

    clock.now += timedelta(seconds=89)
    with pytest.raises(LeaseHeldError) as held:
        await claim(store, "session-b", operator("other@databricks.com", "user-b"))
    assert held.value.diagnosis.likely_stale is True
    assert held.value.diagnosis.remaining_seconds == 1

    clock.now += timedelta(seconds=1)
    successor = await claim(store, "session-b", operator("other@databricks.com", "user-b"))
    assert successor.fencing_token == first.fencing_token + 1
    assert await store.release(first) is False


class _FakeCursor:
    """Answers the two statements LakebaseBoutLeaseStore.claim issues, in order."""

    def __init__(self, rows: list[tuple | None]) -> None:
        self._rows = rows
        self.statements: list[str] = []
        self._pending: tuple | None = None

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def execute(self, sql: str, _params: tuple = ()) -> None:
        self.statements.append(" ".join(sql.split()))
        self._pending = self._rows.pop(0)

    async def fetchone(self) -> tuple | None:
        return self._pending


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    async def __aenter__(self) -> _FakeConnection:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


async def test_the_lakebase_refusal_reads_the_countdown_from_the_database_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The durable path must diagnose against Postgres time, not the replica's clock."""
    monkeypatch.delenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", raising=False)
    row_time = datetime(2026, 8, 20, 19, 0, tzinfo=UTC)
    held_row = (
        "00000000-0000-0000-0000-000000000001",
        7,
        "session-a",
        "user-a",
        "Demo Operator",
        "operator@example.com",
        "run_committed",
        SessionState.RUNNING.value,
        "make_schema_change_safely",
        "Make a schema change safely",
        "aurora_serverless_v2",
        "Aurora Serverless v2",
        row_time,
        row_time,
        row_time + timedelta(seconds=90),
        # clock_timestamp(): 41s after the last heartbeat, 49s before expiry.
        row_time + timedelta(seconds=41),
    )
    cursor = _FakeCursor([None, held_row])
    store = LakebaseBoutLeaseStore(
        endpoint_name="coordination-endpoint",
        database="anti_demo",
        host="coordination.example",
        user="service-principal",
        connector=lambda **_kwargs: _ready(_FakeConnection(cursor)),
        workspace_client=SimpleNamespace(
            postgres=SimpleNamespace(
                generate_database_credential=lambda _name: SimpleNamespace(
                    token="coordination-token"
                )
            )
        ),
    )

    with pytest.raises(LeaseHeldError) as held:
        await claim(store, "session-b", operator("other@databricks.com", "user-b"))

    assert str(held.value) == (
        "BOUT IN PROGRESS · LEASE LIKELY STALE · MAKE A SCHEMA CHANGE SAFELY · "
        "RUN COMMITTED · DEMO OPERATOR · FENCE 7 · NO HEARTBEAT FOR 41S · "
        "RING UNLOCKS IN 49S"
    )
    assert held.value.lease.fencing_token == 7
    assert held.value.lease.expires_at == row_time + timedelta(seconds=90)
    # The diagnosis is a second, read-only statement. The compare-and-set that
    # decides ownership is still exactly one atomic INSERT ... ON CONFLICT.
    claim_statement, diagnosis_statement = cursor.statements
    assert claim_statement.startswith("INSERT INTO anti_demo_coordination.ring_lease")
    assert "ON CONFLICT (ring_key) DO UPDATE SET" in claim_statement
    assert "expires_at <= clock_timestamp()" in claim_statement
    assert diagnosis_statement.startswith("SELECT")
    assert "clock_timestamp() FROM anti_demo_coordination.ring_lease" in diagnosis_statement


def test_no_lease_store_exposes_a_force_or_steal_path() -> None:
    for store in (InMemoryBoutLeaseStore(), InMemoryBoutLeaseStore(ring_key=ROUND5_RING_KEY)):
        forced = [
            name
            for name in dir(store)
            if any(word in name.lower() for word in ("force", "steal", "break", "evict"))
        ]
        assert forced == []


def test_the_stale_threshold_follows_the_configured_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", "30")
    assert lease_heartbeat_seconds() == 30.0
    monkeypatch.setenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", "not-a-number")
    assert lease_heartbeat_seconds() == 15.0
    monkeypatch.setenv("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", "0")
    assert lease_heartbeat_seconds() == 15.0


def test_a_naive_row_timestamp_is_read_as_utc_not_as_local_time() -> None:
    """Lakebase returns timestamptz, but a fake store must not shift the countdown."""
    naive = datetime(2026, 8, 20, 19, 0)
    lease = _lease_at(updated_at=naive, expires_at=naive + timedelta(seconds=90))

    diagnosis = diagnose_held_lease(
        lease,
        now=naive + timedelta(seconds=20),
        heartbeat_seconds=15.0,
    )

    assert diagnosis.remaining_seconds == 70
    assert diagnosis.heartbeat_age_seconds == 20
    assert diagnosis.likely_stale is True


def test_private_subject_is_not_serialized_to_the_browser() -> None:
    identity = operator("operator@example.com", "stable-sso-id")

    assert identity.model_dump() == {
        "display_name": "operator",
        "email": "operator@example.com",
    }


def test_transient_and_permanent_coordination_faults_are_told_apart() -> None:
    """The classifier is the whole basis of "retry" versus "tell an operator"."""

    # Transport, provider and contention faults: the same call can succeed later.
    for transient in (
        psycopg.OperationalError("server closed the connection unexpectedly"),
        ConnectionResetError("reset by peer"),
        TimeoutError("coordination connect timed out"),
        LeaseLostError("RING FENCE CHANGED"),
        RingFenceLostError("Startup cleanup lost its durable ring fence"),
    ):
        assert is_transient_coordination_error(transient) is True

    # Wrapped by a caller that raises its own type: classifying only the
    # outermost exception would call every one of these permanent.
    wrapped = InvalidStateError("preflight failed")
    wrapped.__cause__ = psycopg.OperationalError("connection refused")
    assert is_transient_coordination_error(wrapped) is True

    # Failures every retry reproduces exactly. `build_lease_store` refusing an
    # unconfigured endpoint is the important one: it is a misconfiguration, so
    # retrying it forever would hide the one thing an operator has to fix.
    for permanent in (
        InvalidStateError("Demo setup is not ready: sealed manifest v2 is required"),
        ValueError("ring_key must be one of ..."),
        RuntimeError(coordination._NO_COORDINATION_ENDPOINT),
        AssertionError("Round 5 durable lease store is unavailable"),
    ):
        assert is_transient_coordination_error(permanent) is False

    # `DatabricksError` is a subclass of `OSError`, and `OSError` is on the
    # transient list for sockets. So every control-plane refusal the SDK raises
    # arrives already looking like a transport blip -- a `PermissionDenied` that
    # no amount of waiting clears would be retried at the ceiling interval
    # forever, and the gate would report ordinary maintenance while an operator
    # went unpaged. The SDK's own taxonomy is the line: the four already named on
    # the transient list stay transient, and the refusals do not.
    for refusal in (
        PermissionDenied("assign the user 'Can Use' for Database project"),
        Unauthenticated("default auth: cannot configure default credentials"),
        NotFound("branch id not found"),
        BadRequest("unknown command postgres"),
    ):
        assert is_transient_coordination_error(refusal) is False
        assert is_transient_coordination_error(_reset_error(refusal, refusal)) is False
    for outage in (
        TemporarilyUnavailable("upstream connect error"),
        TooManyRequests("throttled"),
    ):
        assert is_transient_coordination_error(outage) is True


class _DurableRing(InMemoryBoutLeaseStore):
    """An in-memory ring that presents itself as the durable one.

    The gate refuses to run without durable coordination, and that refusal is
    the point of the check, so tests satisfy it rather than bypass it.
    """

    mode = "lakebase"

    async def _run(self, _operation: object) -> object:
        raise AssertionError("the injected readiness row owns test persistence")


class _ReadinessRow:
    """The durable readiness row, in memory, with its fence check intact."""

    def __init__(self, ring: _DurableRing) -> None:
        self.ring = ring
        self.value: object | None = None
        self.writes: list[tuple[str, int]] = []

    async def initialize(self) -> None:
        return None

    async def read(self) -> object | None:
        return self.value

    async def ring_generation(self) -> int:
        return self.ring._generation

    async def write(self, lease, *, manifest_seal: str, state: str, detail: str | None) -> None:
        held = await self.ring.current()
        assert held is not None
        assert held.lease_id == lease.lease_id
        assert held.fencing_token == lease.fencing_token
        assert held.phase == "startup_cleanup"
        self.writes.append((state, lease.fencing_token))
        self.value = SimpleNamespace(
            manifest_seal=manifest_seal,
            state=state,
            detail=detail,
            fencing_token=lease.fencing_token,
        )


class _CleanupEngine:
    """A cleanup pass that fails a set number of times, then works."""

    def __init__(
        self,
        calls: list[str],
        name: str,
        *,
        error: Callable[[], BaseException] | None = None,
        failures: int | None = None,
        observe: Callable[[], None] | None = None,
    ) -> None:
        self.calls = calls
        self.name = name
        self.error = error
        self.failures = failures
        self.observe = observe

    async def reset_all(self) -> None:
        self.calls.append(self.name)
        if self.observe is not None:
            self.observe()
        if self.error is None:
            return
        if self.failures is None or len(self.calls) <= self.failures:
            raise self.error()


def _gate_manifest():
    return SimpleNamespace(
        run_id="ad-recovery-001",
        round5_ready=False,
        round4=SimpleNamespace(app_service_principal_client_id="app-client-id"),
        model_dump_json=lambda **_kwargs: '{"run_id":"ad-recovery-001"}',
    )


def _gate(ring: _DurableRing, row: _ReadinessRow, *, safe_change, **overrides):
    settings: dict[str, object] = {
        "poll_seconds": 0.01,
        "retry_seconds": 0.01,
        "retry_ceiling_seconds": 0.05,
        "escalate_after_seconds": 1000.0,
        "heartbeat_seconds": 0.5,
        "lease_seconds": 60,
    }
    settings.update(overrides)
    return ShowtimeReadinessGate(
        _gate_manifest(),
        ring,
        safe_change_factory=safe_change,
        recovery_factory=lambda _manifest: _CleanupEngine([], "round3"),
        round5_factory=None,
        state_store=row,
        **settings,
    )


async def test_a_transient_fault_recovers_in_process_without_a_restart() -> None:
    """One blip used to cost every control action until somebody restarted.

    `_run_main` swallowed the exception, `run()` saw a not-ready ring and the
    task exited, and nothing in this repository restarts the process. The gate
    now retries in place, and each retry claims a *fresh* fence.
    """

    ring = _DurableRing()
    row = _ReadinessRow(ring)
    calls: list[str] = []
    seen: list[tuple[str, int]] = []
    concurrent = 0
    overlaps: list[int] = []

    def observe() -> None:
        seen.append((gate.recovery.state, gate.recovery.attempts))

    class OneAtATime(_CleanupEngine):
        async def reset_all(self) -> None:
            nonlocal concurrent
            concurrent += 1
            overlaps.append(concurrent)
            try:
                await super().reset_all()
            finally:
                concurrent -= 1

    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: OneAtATime(
            calls,
            "round2",
            error=lambda: psycopg.OperationalError("server closed the connection"),
            failures=2,
            observe=observe,
        ),
    )

    await asyncio.wait_for(gate.run(), timeout=10)

    assert calls == ["round2", "round2", "round2"]
    assert gate.status.ring_ready is True
    assert gate.status.maintenance_state == "ready"
    assert gate.recovery.state == "settled"
    # The two failed attempts were reported as retries, not as a dead end, and
    # the second attempt happened with the first one's failure already recorded.
    assert seen[0] == ("settled", 0)
    assert seen[1] == ("retrying", 1)
    assert seen[2] == ("retrying", 2)
    # No attempt overlapped another: two concurrent attempts would be two
    # holders of one ring, which is exactly what the fence exists to prevent.
    assert max(overlaps) == 1
    # Every attempt claimed a new fence rather than reusing a stale one: the
    # fence only ever moves forward, each failed attempt recorded its failure
    # under a fence of its own, and READY is written under a newer one still.
    tokens = [token for _state, token in row.writes]
    failed_under = [token for state, token in row.writes if state == "blocked"]
    assert tokens == sorted(tokens)
    assert len(failed_under) == 2
    assert len(set(failed_under)) == 2
    assert row.writes[-1] == ("ready", max(tokens))
    assert max(tokens) > max(failed_under)


async def test_a_permanent_failure_stays_visible_and_is_not_retried() -> None:
    """A hopeless condition must not be papered over by an endless retry."""

    ring = _DurableRing()
    row = _ReadinessRow(ring)
    calls: list[str] = []
    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: _CleanupEngine(
            calls,
            "round2",
            error=lambda: RuntimeError("ownership mismatch"),
        ),
    )

    await asyncio.wait_for(gate.run(), timeout=10)

    # Exactly one attempt: retrying a misconfiguration only delays the fix.
    assert calls == ["round2"]
    assert gate.status.ring_ready is False
    assert gate.status.maintenance_state == "blocked"
    detail = gate.status.maintenance_detail or ""
    # The refusal operators and the control API already match on is preserved,
    # and only the fact that nothing further will be tried is added to it.
    assert "OWNERSHIP OR SEAL COULD NOT BE VERIFIED" in detail
    assert "NOT RETRYING · OPERATOR ACTION REQUIRED" in detail
    assert gate.recovery.state == "given_up"
    assert gate.recovery.attempts == 1
    assert gate.recovery.error == "RuntimeError"
    assert row.value is not None and row.value.state == "blocked"
    with pytest.raises(InvalidStateError, match="OWNERSHIP OR SEAL"):
        gate.require_ready()


async def test_a_long_transient_outage_escalates_but_keeps_retrying() -> None:
    """Escalation is the alarm; giving up on a transient fault is not.

    A network partition that heals in an hour still has to recover unattended,
    so the retry schedule continues at its ceiling. What changes at the budget
    is what the state *says*, which is what a monitor pages on.
    """

    ring = _DurableRing()
    row = _ReadinessRow(ring)
    calls: list[str] = []
    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: _CleanupEngine(
            calls,
            "round2",
            error=lambda: psycopg.OperationalError("coordination endpoint unreachable"),
        ),
        escalate_after_seconds=0.0,
    )

    task = asyncio.create_task(gate.run())
    try:
        for _ in range(500):
            if gate.recovery.state == "escalated":
                break
            await asyncio.sleep(0.01)
        assert gate.recovery.state == "escalated"
        assert gate.status.maintenance_state == "blocked"
        detail = gate.status.maintenance_detail or ""
        assert "STILL FAILING" in detail
        assert "STILL RETRYING" in detail

        # Still trying, so the outage clearing needs no human.
        settled = gate.recovery.attempts
        for _ in range(500):
            if gate.recovery.attempts > settled:
                break
            await asyncio.sleep(0.01)
        assert gate.recovery.attempts > settled
        assert task.done() is False
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_a_short_transient_outage_reads_as_maintenance_not_as_blocked() -> None:
    ring = _DurableRing()
    row = _ReadinessRow(ring)
    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: _CleanupEngine(
            [],
            "round2",
            error=lambda: psycopg.OperationalError("coordination endpoint unreachable"),
        ),
        retry_seconds=0.05,
        retry_ceiling_seconds=0.05,
    )

    task = asyncio.create_task(gate.run())
    try:
        for _ in range(500):
            if gate.recovery.state == "retrying":
                break
            await asyncio.sleep(0.01)
        assert gate.recovery.state == "retrying"
        assert gate.status.maintenance_state == "maintenance"
        assert "RETRYING" in (gate.status.maintenance_detail or "")
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_a_refused_control_action_cuts_a_long_backoff_short() -> None:
    """The wait is an event with a timeout backstop, the Round 5 idiom.

    Whoever discovers that the state may have moved asks for the next look, so a
    recovered endpoint does not have to wait out a ceiling-length sleep. The
    floor inside `_await_retry` is what stops refused clicks becoming a storm.
    """

    ring = _DurableRing()
    row = _ReadinessRow(ring)
    calls: list[str] = []
    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: _CleanupEngine(
            calls,
            "round2",
            error=lambda: psycopg.OperationalError("server closed the connection"),
            failures=1,
        ),
        # Long enough that finishing inside the timeout below proves the wake-up
        # worked rather than that the sleep expired.
        retry_seconds=120.0,
        retry_ceiling_seconds=120.0,
    )

    task = asyncio.create_task(gate.run())
    try:
        for _ in range(500):
            if gate.recovery.state == "retrying":
                break
            await asyncio.sleep(0.01)
        assert gate.recovery.state == "retrying"

        # A control action arrives; `require_ready` refuses it and asks for a look.
        with pytest.raises(InvalidStateError):
            gate.require_ready()

        await asyncio.wait_for(task, timeout=5)
        assert gate.status.ring_ready is True
        assert gate.recovery.state == "settled"
        assert calls == ["round2", "round2"]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_a_recheck_asked_for_mid_attempt_is_not_slept_through() -> None:
    """A request that lands before the wait starts still has to be honoured.

    `require_ready` refuses control actions whenever they arrive, including while
    an attempt is still in flight. Clearing that request unread and then sleeping
    out the whole ceiling would reproduce the wedge this fix is about, on a
    smaller scale.
    """

    ring = _DurableRing()
    gate = _gate(
        ring,
        _ReadinessRow(ring),
        safe_change=lambda _manifest: _CleanupEngine([], "round2"),
        poll_seconds=0.05,
    )

    gate.request_recheck()
    started = time.monotonic()
    await gate._await_retry(120.0)
    elapsed = time.monotonic() - started

    # Honoured promptly, but not before the floor: the floor is what keeps a
    # burst of refused clicks from turning into a retry storm.
    assert 0.05 <= elapsed < 5.0


# --------------------------------------------------------------------------- #
# An environmental fault is not a dead end
# --------------------------------------------------------------------------- #


def _reset_error(*causes: BaseException) -> SafeChangeResetError:
    """A Round 2 reset failure carrying whatever actually failed each lane.

    The shape the live server produced: `SafeChangeResetError: Round 2 reset
    failed for: Aurora Serverless v2, RDS PostgreSQL`. It is raised outside any
    `except` block, from a gather over independent lanes, so it has no
    `__cause__` of its own -- the causes hang off the lane results.
    """

    lanes = {
        f"competitor-{index}": SafeChangeResetLaneResult(
            lane_id=f"competitor-{index}",
            name=("Aurora Serverless v2", "RDS PostgreSQL")[index % 2],
            provider=SafeChangeProvider.AURORA,
            artifact_id=f"artifact-{index}",
            ok=False,
            error=str(cause),
            cause=cause,
        )
        for index, cause in enumerate(causes)
    }
    return SafeChangeResetError(SafeChangeResetResult(competitor=None, lanes=lanes))


def _expired_sso() -> UnauthorizedSSOTokenError:
    return UnauthorizedSSOTokenError()


def _denied() -> ClientError:
    return ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "not authorized"}},
        "DeleteDBInstance",
    )


def test_an_unavailable_environment_is_told_apart_from_a_wrong_one() -> None:
    """Two things a single "is this transient" question used to conflate.

    Temporarily unavailable: the environment will heal, possibly with nobody
    touching this process, so the only correct response is to keep trying.
    Genuinely wrong: a denial, a seal that does not match, an ownership
    violation. Retrying those hides the one thing an operator has to fix.
    """

    for unavailable, subject in (
        (_expired_sso(), CREDENTIAL_FAULT_SUBJECT),
        (NoCredentialsError(), CREDENTIAL_FAULT_SUBJECT),
        (
            ClientError({"Error": {"Code": "ExpiredToken"}}, "DescribeDBInstances"),
            CREDENTIAL_FAULT_SUBJECT,
        ),
        (
            ClientError({"Error": {"Code": "ThrottlingException"}}, "DescribeDBInstances"),
            SERVICE_FAULT_SUBJECT,
        ),
    ):
        assert environment_fault_subject(unavailable) == subject
        assert is_retryable_startup_error(unavailable) is True

    for wrong in (
        _denied(),
        RuntimeError("ownership mismatch"),
        ValueError("the isolated environment is not sealed to this account"),
    ):
        assert environment_fault_subject(wrong) is None
        assert is_retryable_startup_error(wrong) is False

    # The wrapper is the load-bearing case. `SafeChangeResetError` is a fan-in
    # summary of several lanes, so the *same type* carries either kind of fault
    # and classifying on the type is guaranteed to be wrong half the time.
    assert is_retryable_startup_error(_reset_error(_expired_sso(), _expired_sso())) is True
    assert is_retryable_startup_error(_reset_error(_denied(), _denied())) is False
    # Mixed: one lane could not authenticate and the other was refused. The
    # unauthenticated lane may yet succeed, so this has to stay retryable --
    # and the denial is still in the log either way.
    assert is_retryable_startup_error(_reset_error(_expired_sso(), _denied())) is True


async def test_expired_credentials_do_not_end_startup_cleanup_for_the_process() -> None:
    """The live wedge: one AWS fault ended the gate for the life of the process.

    An expired SSO session made startup cleanup raise `SafeChangeResetError`.
    That is not a *coordination* fault, so the gate gave up after one attempt --
    `recovery_state: given_up`, `recovering: false` -- and every round, including
    the two that need no AWS at all, reported `can_start: false` until somebody
    restarted the server. For an unattended install that is a hang.
    """

    ring = _DurableRing()
    row = _ReadinessRow(ring)
    calls: list[str] = []
    seen: list[tuple[str, str | None]] = []

    def observe() -> None:
        seen.append((gate.recovery.state, gate.recovery.waiting_on))

    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: _CleanupEngine(
            calls,
            "round2",
            error=lambda: _reset_error(_expired_sso(), _expired_sso()),
            failures=3,
            observe=observe,
        ),
    )

    await asyncio.wait_for(gate.run(), timeout=10)

    # It kept trying, and the environment healing was enough on its own.
    assert calls == ["round2"] * 4
    assert gate.status.ring_ready is True
    assert gate.recovery.state == "settled"
    # Every failed attempt reported itself as waiting on something nameable,
    # rather than as a server that had broken.
    assert seen[0] == ("settled", None)
    assert [state for state, _subject in seen[1:]] == ["retrying"] * 3
    assert {subject for _state, subject in seen[1:]} == {CREDENTIAL_FAULT_SUBJECT}


async def test_a_denied_reset_still_stops_loudly_rather_than_retrying_forever() -> None:
    """The other half of the widening: it must not have swallowed everything.

    A denial is a statement about this principal, so no amount of waiting changes
    it. Retrying it forever would bury the only thing an operator can act on.
    """

    ring = _DurableRing()
    row = _ReadinessRow(ring)
    calls: list[str] = []
    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: _CleanupEngine(
            calls,
            "round2",
            error=lambda: _reset_error(_denied()),
        ),
    )

    await asyncio.wait_for(gate.run(), timeout=10)

    assert calls == ["round2"]
    assert gate.recovery.state == "given_up"
    assert gate.recovery.waiting_on is None
    assert "NOT RETRYING · OPERATOR ACTION REQUIRED" in (
        gate.status.maintenance_detail or ""
    )
    # The give-up path released its lease rather than leaking it. That was
    # verified live and must not regress.
    assert await ring.current() is None


async def test_credentials_returning_makes_the_gate_ready_with_no_restart() -> None:
    """The promise this whole fix exists for, end to end.

    The credential sentry notices when credentials come back -- it already
    re-probes on an interval and caches the answer. Before, nothing connected
    that to the readiness gate, so `aws sso login` succeeding changed nothing:
    the sentry went green, the gate stayed blocked, and only a restart cleared
    it. Here the sentry's bad-to-good transition is the only thing that can
    finish the run, because the retry interval is far longer than the timeout.
    """

    from server.aws_credential_probe import CredentialSentry, ProbeExpectations

    ring = _DurableRing()
    row = _ReadinessRow(ring)
    calls: list[str] = []
    credentials_work = False

    class _AwsShapedCleanup:
        """Fails exactly as long as the credentials are unusable."""

        async def reset_all(self) -> None:
            calls.append("round2")
            if not credentials_work:
                raise _reset_error(_expired_sso(), _expired_sso())

    gate = _gate(
        ring,
        row,
        safe_change=lambda _manifest: _AwsShapedCleanup(),
        # Long enough that finishing inside the timeout below proves the sentry
        # woke the gate, rather than that a backoff happened to expire.
        retry_seconds=600.0,
        retry_ceiling_seconds=600.0,
        poll_seconds=0.01,
    )

    def session_factory(**_kwargs):
        if not credentials_work:
            raise _expired_sso()

        class _Session:
            @staticmethod
            def client(name: str, **_inner):
                return SimpleNamespace(
                    get_caller_identity=lambda **_a: {
                        "Account": "123456789012",
                        "Arn": "arn:aws:iam::123456789012:user/operator",
                    },
                    describe_db_instances=lambda **_a: {"DBInstances": []},
                )

        return _Session()

    sentry = CredentialSentry(
        ProbeExpectations(region="us-west-2", account_id="123456789012"),
        session_factory=session_factory,
        interval_seconds=0.01,
        environ={
            "AWS_ACCESS_KEY_ID": "AKIAEXAMPLEEXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "example-secret-access-key",
        },
        on_recovered=gate.notify_credentials_recovered,
    )

    gate_task = asyncio.create_task(gate.run())
    sentry_task = asyncio.create_task(sentry.run())
    try:
        for _ in range(1000):
            if gate.recovery.state == "retrying":
                break
            await asyncio.sleep(0.01)
        # Waiting, saying so, and saying what for -- not given up, and no
        # restart has happened or is needed.
        assert gate.recovery.state == "retrying"
        assert gate.recovery.waiting_on == CREDENTIAL_FAULT_SUBJECT
        assert gate.status.ring_ready is False
        assert sentry.verdict().healthy is False

        # `aws sso login`, in another terminal, with this process untouched.
        credentials_work = True

        await asyncio.wait_for(gate_task, timeout=10)
        assert gate.status.ring_ready is True
        assert gate.recovery.state == "settled"
        assert sentry.verdict().healthy is True
        gate.require_ready()
    finally:
        for task in (gate_task, sentry_task):
            task.cancel()
        await asyncio.gather(gate_task, sentry_task, return_exceptions=True)
