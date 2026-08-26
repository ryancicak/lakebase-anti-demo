from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

import psycopg

from .capacity import LAKEBASE_SUSPEND_SECONDS
from .connection_spike_live import LakebaseCreationJournalStore
from .coordination import (
    COORDINATION_SCHEMA,
    RING_KEY,
    BoutLease,
    BoutLeaseStore,
    CoordinationObjectsMissingError,
    LeaseHeldError,
    LeaseLostError,
    environment_fault_subject,
    is_retryable_startup_error,
    privilege_refusal,
    read_coordination_objects,
    validate_ring_key,
)
from .manager import InvalidStateError, operator_diagnosis
from .manifest import DemoManifest
from .models import BoutOperator, SessionState

LOGGER = logging.getLogger(__name__)

READINESS_TABLE = "anti_demo_coordination.startup_readiness"
MAINTENANCE_COPY = "BACKSTAGE CLEANUP IN PROGRESS · SHOWTIME WILL UNLOCK AUTOMATICALLY"

#: Recovery schedule for the startup gate. One transient coordination fault used
#: to end this gate for the life of the process: `_run_main` swallowed the
#: exception, `run()` observed a not-ready ring and the task exited, and every
#: control action stayed refused until a human restarted the server. Nothing in
#: this repository restarts it, so "until a restart" meant "indefinitely".
#:
#: The first retry is fast because the overwhelmingly common fault is a Lakebase
#: endpoint waking up or a single dropped connection. The ceiling exists because
#: a longer outage must not turn into a connection storm from every replica, and
#: because each attempt opens its own coordination connection and so holds the
#: endpoint awake.
GATE_RETRY_SECONDS = 2.0
GATE_RETRY_CEILING_SECONDS = 60.0

#: How long a transient-looking failure may keep failing before this stops being
#: reported as ordinary maintenance. Retrying continues at the ceiling -- a
#: network partition that heals in an hour must still recover unattended -- but a
#: monitor has to be able to page somebody long before that hour is up.
GATE_ESCALATE_AFTER_SECONDS = 300.0

# How long the Round 5 reconciler waits between re-reads once it has observed a
# steady ready state, absent an on-demand wake. Every read opens its own
# coordination connection, so an interval inside the endpoint's suspend window
# holds that endpoint awake for the life of the process to re-learn a constant.
# This is deliberately a large multiple of the window: the endpoint stays awake
# for the window's length after each re-check and sleeps for the remainder, and
# anything that actually needs a fresh answer wakes the reconciler directly
# rather than waiting for this backstop.
ROUND5_IDLE_POLL_SECONDS = 15 * float(LAKEBASE_SUSPEND_SECONDS)


@dataclass(frozen=True)
class ReadinessStatus:
    ring_ready: bool
    maintenance_state: Literal["ready", "maintenance", "blocked"]
    maintenance_detail: str | None


@dataclass(frozen=True)
class RecoveryState:
    """What this process is doing about its own last failure.

    ``maintenance_state`` says whether the ring is usable. It cannot say whether
    anybody is still working on it, and that is the difference between a blip
    that will clear itself and an outage that needs a human. The four states:

    * ``settled`` -- nothing has failed, or the last failure has been recovered.
    * ``retrying`` -- a transient fault, inside the escalation budget. Expect it
      to clear on its own; do not page.
    * ``escalated`` -- still failing after the budget. Retries continue at the
      ceiling interval, so an outage that heals still recovers unattended, but
      this is the state a monitor alerts on.
    * ``given_up`` -- classified permanent, so the recovery schedule has stopped.
      Only an operator changing something (or an explicit re-check) moves it.

    ``waiting_on`` names the thing outside this process that has to come back
    before a retry can succeed -- "AWS credentials", typically. Without it,
    ``retrying`` on a health page says the server is not idle and nothing more,
    so an expired SSO session and a genuine thrash look identical.
    """

    state: Literal["settled", "retrying", "escalated", "given_up"]
    attempts: int = 0
    detail: str | None = None
    next_attempt_seconds: float | None = None
    error: str | None = None
    waiting_on: str | None = None


SETTLED = RecoveryState("settled")


class RingFenceLostError(LeaseLostError):
    """The fence this attempt was holding moved. A fresh claim is the fix.

    Still a ``RuntimeError`` through ``LeaseLostError``, so every existing caller
    and message match holds. Named, so that losing a fence is classified as the
    contention it is rather than as a permanent fault.
    """


@dataclass(frozen=True)
class _DurableStatus:
    manifest_seal: str
    state: str
    detail: str | None
    fencing_token: int


class StartupReadinessStore:
    """Small durable state row colocated with the fenced ring lease."""

    def __init__(
        self,
        run: Callable[[Callable[[Any], Awaitable[Any]]], Awaitable[Any]],
        *,
        ring_key: str = RING_KEY,
    ):
        self._run = run
        self.ring_key = validate_ring_key(ring_key)

    async def initialize(self) -> None:
        """Confirm the readiness row's table exists, and create it only if not.

        Same consumer/owner split as the cost ledger, and the same reason: on the
        deployed path this runs as a principal with no DDL on
        ``anti_demo_coordination``, so an unconditional ``CREATE TABLE IF NOT
        EXISTS`` was refused for a statement that would have done nothing. That
        refusal is not fatal the way the cost ledger's was -- it happens on the
        readiness task, so the app serves -- which makes it worse, not better:
        the gate would give up and every control action would stay refused
        behind a banner blaming ownership.

        If the table is genuinely absent and this identity cannot create it, the
        gate still gives up, and now it gives up naming the real cause.
        """

        async def ensure(cursor: Any) -> None:
            objects = await read_coordination_objects(cursor, (READINESS_TABLE,))
            if objects.complete:
                return
            try:
                if not objects.schema_present:
                    await cursor.execute(
                        f"CREATE SCHEMA IF NOT EXISTS {COORDINATION_SCHEMA}"
                    )
                await cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {READINESS_TABLE} (
                        ring_key text PRIMARY KEY,
                        manifest_seal text NOT NULL,
                        state text NOT NULL,
                        detail text,
                        fencing_token bigint NOT NULL,
                        updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
                    )
                    """
                )
            except psycopg.errors.InsufficientPrivilege as exc:
                raise CoordinationObjectsMissingError(
                    f"Startup readiness is missing {objects.describe_missing()}, and this "
                    "identity may not create it. Provision the coordination schema with an "
                    "identity that owns it (`antidemo setup`), then grant this one the runtime "
                    "privileges in docs/DEPLOY.md."
                ) from exc

        await self._run(ensure)

    async def read(self) -> _DurableStatus | None:
        async def select(cursor: Any) -> _DurableStatus | None:
            await cursor.execute(
                f"""
                SELECT manifest_seal, state, detail, fencing_token
                FROM {READINESS_TABLE}
                WHERE ring_key = %s
                """,
                (self.ring_key,),
            )
            row = await cursor.fetchone()
            return (
                _DurableStatus(str(row[0]), str(row[1]), row[2], int(row[3]))
                if row is not None
                else None
            )

        return await self._run(select)

    async def ring_generation(self) -> int:
        async def select(cursor: Any) -> int:
            await cursor.execute(
                "SELECT fencing_token FROM anti_demo_coordination.ring_lease "
                "WHERE ring_key = %s",
                (self.ring_key,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Durable ring generation is unavailable")
            return int(row[0])

        return await self._run(select)

    async def write(
        self,
        lease: BoutLease,
        *,
        manifest_seal: str,
        state: str,
        detail: str | None,
    ) -> None:
        """Write only while the exact, unexpired lease fence remains current."""

        async def upsert(cursor: Any) -> None:
            await cursor.execute(
                f"""
                INSERT INTO {READINESS_TABLE} (
                    ring_key, manifest_seal, state, detail, fencing_token, updated_at
                )
                SELECT %s, %s, %s, %s, %s, clock_timestamp()
                FROM anti_demo_coordination.ring_lease
                WHERE ring_key = %s
                  AND lease_id = %s::uuid
                  AND session_id = %s
                  AND fencing_token = %s
                  AND owner_subject = %s
                  AND phase = 'startup_cleanup'
                  AND expires_at > clock_timestamp()
                ON CONFLICT (ring_key) DO UPDATE SET
                    manifest_seal = EXCLUDED.manifest_seal,
                    state = EXCLUDED.state,
                    detail = EXCLUDED.detail,
                    fencing_token = EXCLUDED.fencing_token,
                    updated_at = EXCLUDED.updated_at
                RETURNING fencing_token
                """,
                (
                    self.ring_key,
                    manifest_seal,
                    state,
                    detail,
                    lease.fencing_token,
                    self.ring_key,
                    lease.lease_id,
                    lease.session_id,
                    lease.fencing_token,
                    lease.owner_subject,
                ),
            )
            if await cursor.fetchone() is None:
                raise RingFenceLostError("Startup cleanup lost its durable ring fence")

        await self._run(upsert)


def _manifest_seal(manifest: DemoManifest) -> str:
    payload = manifest.model_dump_json(exclude_none=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


#: What a gate failure says when its cause is not one this module can name. The
#: default rather than the only answer, so adding the two named causes below
#: changed no other banner.
UNVERIFIED_COPY = "OWNERSHIP OR SEAL COULD NOT BE VERIFIED"


def _coordination_grant_remedy(refusal: BaseException) -> str:
    """The ``GRANT`` that would clear ``refusal``, read out of the setup plan.

    Not written out here. ``_coordination_runtime_grants()`` in
    ``server/lifecycle.py`` is the single declarative statement of what the
    deployed app may hold, and ``docs/DEPLOY.md`` is the authoritative SQL behind
    it. A third copy in this module is precisely the drift this area keeps
    suffering from: the last one left ``startup_readiness`` with no ACL entry for
    a release, because the hand-written list of grants agreed with itself.

    Postgres does not populate ``diag.table_name`` for an ACL failure -- the
    relation appears only in the message text ("permission denied for table
    startup_readiness") -- so the message is used as a lookup *key* into the plan
    and never as the remedy. A relation the plan does not know falls back to the
    whole set instead of guessing at a statement.
    """

    message = str(refusal)
    try:
        # Imported here rather than at module scope: `server/lifecycle.py` reads
        # `READINESS_TABLE` out of this module for the same plan, and this runs
        # only on a failure path. Guarded because a diagnostic that can raise
        # while explaining a failure destroys the failure it was explaining --
        # the shape that let a seal-binding crash hide a credential fault.
        from .lifecycle import _coordination_runtime_grants

        plan = _coordination_runtime_grants()
    except Exception:  # noqa: BLE001 - the refusal matters more than the remedy
        LOGGER.warning("Could not read the coordination grant plan", exc_info=True)
        plan = ()
    for grant in plan:
        # Sequences first: their names contain their table's, and INSERT on the
        # table is not what a refused `nextval` is asking for.
        for sequence in grant.sequences:
            if sequence in message:
                return f"GRANT USAGE, SELECT ON SEQUENCE {grant.schema}.{sequence}"
        if grant.name in message:
            return f"GRANT {', '.join(grant.privileges)} ON {grant.table}"
    return "APPLY THE COMPLETE COORDINATION RUNTIME GRANT SET"


def _blocked_detail(
    prefix: str,
    error: BaseException,
    *,
    unverified: str = UNVERIFIED_COPY,
) -> str:
    """The banner for a gate failure, named after the subsystem that refused it.

    Every failure here used to read "OWNERSHIP OR SEAL COULD NOT BE VERIFIED",
    which names the manifest seal and the AWS ownership tags. A missing Postgres
    grant is neither, and that sentence sends its reader to IAM -- the wrong
    subsystem, on the one class of failure whose remedy is four lines of SQL. It
    cost hours. So the two causes this gate can actually distinguish say what
    refused, what to do about it, and that AWS is not involved.

    Both stay non-retryable: an ACL and an absent table fail identically on
    every attempt, so ``given_up`` is correct and unchanged. Only the words move.

    The unnamed case is no longer a fixed string either, and that is the half
    that cost the most. A control-plane refusal reached this banner as
    "OWNERSHIP OR SEAL COULD NOT BE VERIFIED" and nothing else, because the
    runner that produced it discarded the answer for secret safety and the
    banner had nothing left to say. `manager.operator_diagnosis` is the boundary
    this codebase already settled for arm refusals -- the whole cause chain, with
    a `DatabricksError`'s or this codebase's own words kept and everything else
    reduced to a type name, capped so one runaway provider message cannot push
    the sentence explaining it off the panel.
    """

    if isinstance(error, CoordinationObjectsMissingError):
        # Refused a `CREATE` for an object that is genuinely absent. Telling an
        # operator to GRANT here would send them at a relation that does not
        # exist; only the identity that owns the schema can help.
        return (
            f"{prefix} BLOCKED · THE COORDINATION SCHEMA IS INCOMPLETE AND THIS APP "
            "MAY NOT CREATE IT · PROVISION IT WITH `antidemo setup` AS THE SCHEMA "
            "OWNER (docs/DEPLOY.md) · A LAKEBASE OBJECT, NOT AWS IAM AND NOT THE "
            "MANIFEST SEAL"
        )
    refusal = privilege_refusal(error)
    if refusal is None:
        return f"{prefix} BLOCKED · {unverified} · {operator_diagnosis(error)}"
    return (
        f"{prefix} BLOCKED · POSTGRES REFUSED THIS APP'S ACCESS TO THE COORDINATION "
        f"DATABASE · {_coordination_grant_remedy(refusal)} TO THE APP'S CLIENT ID "
        "(docs/DEPLOY.md) · A LAKEBASE GRANT, NOT AWS IAM AND NOT THE MANIFEST SEAL"
    )


class ShowtimeReadinessGate:
    """Replica-safe startup cleanup gate; browser requests are read-only observers."""

    def __init__(
        self,
        manifest: DemoManifest,
        lease_store: BoutLeaseStore,
        *,
        safe_change_factory: Callable[[DemoManifest], Any],
        recovery_factory: Callable[[DemoManifest], Any],
        round5_factory: Callable[[str, LakebaseCreationJournalStore, Any], Any] | None,
        round5_lease_store: BoutLeaseStore | None = None,
        manifest_check: Callable[[], None] | None = None,
        poll_seconds: float = 2.0,
        idle_poll_seconds: float = ROUND5_IDLE_POLL_SECONDS,
        heartbeat_seconds: float = 15.0,
        lease_seconds: float = 90.0,
        state_store: StartupReadinessStore | None = None,
        round5_state_store: StartupReadinessStore | None = None,
        retry_seconds: float = GATE_RETRY_SECONDS,
        retry_ceiling_seconds: float = GATE_RETRY_CEILING_SECONDS,
        escalate_after_seconds: float = GATE_ESCALATE_AFTER_SECONDS,
    ) -> None:
        run = getattr(lease_store, "_run", None)
        if getattr(lease_store, "mode", None) != "lakebase" or not callable(run):
            raise RuntimeError("Deployed startup cleanup requires durable Lakebase coordination")
        round5_run = getattr(round5_lease_store, "_run", None)
        if self._manifest_has_round5(manifest) and (
            getattr(round5_lease_store, "mode", None) != "lakebase"
            or not callable(round5_run)
        ):
            raise RuntimeError("Round 5 startup cleanup requires durable Lakebase coordination")
        self._manifest = manifest
        self._lease_store = lease_store
        self._round5_lease_store = round5_lease_store
        self._store = state_store or StartupReadinessStore(run, ring_key=RING_KEY)
        self._round5_store = (
            round5_state_store
            or (
                StartupReadinessStore(
                    round5_run,
                    ring_key=round5_lease_store.ring_key,
                )
                if callable(round5_run)
                else None
            )
        )
        self._safe_change_factory = safe_change_factory
        self._recovery_factory = recovery_factory
        self._round5_factory = round5_factory
        self._manifest_check = manifest_check or (lambda: None)
        self._poll_seconds = poll_seconds
        self._idle_poll_seconds = idle_poll_seconds
        self._recheck = asyncio.Event()
        self._round5_recheck = asyncio.Event()
        self._retry_seconds = retry_seconds
        self._retry_ceiling_seconds = retry_ceiling_seconds
        self._escalate_after_seconds = escalate_after_seconds
        # One attempt at a time. A retry claims a *fresh* fenced lease, so two
        # overlapping attempts would be two owners of one ring -- exactly what
        # the fence exists to prevent.
        self._attempt_lock = asyncio.Lock()
        self._recovery = SETTLED
        self._round5_recovery = SETTLED
        self._heartbeat_seconds = heartbeat_seconds
        self._ttl = timedelta(seconds=lease_seconds)
        self._seal = _manifest_seal(manifest)
        self._status = ReadinessStatus(False, "maintenance", MAINTENANCE_COPY)
        self._round5_status = ReadinessStatus(
            not self._manifest_has_round5(manifest),
            "maintenance" if self._manifest_has_round5(manifest) else "ready",
            MAINTENANCE_COPY if self._manifest_has_round5(manifest) else None,
        )

    @staticmethod
    def _manifest_has_round5(manifest: DemoManifest) -> bool:
        return bool(getattr(manifest, "round5_ready", False))

    @property
    def status(self) -> ReadinessStatus:
        return self._status

    @property
    def round5_status(self) -> ReadinessStatus:
        return self._round5_status

    @property
    def recovery(self) -> RecoveryState:
        return self._recovery

    @property
    def round5_recovery(self) -> RecoveryState:
        return self._round5_recovery

    def require_ready(self) -> None:
        self._manifest_check()
        if not self._status.ring_ready:
            # Somebody is trying to use the ring, which is the moment a pending
            # retry is worth most. Waking it only shortens the wait -- the floor
            # in `_await_retry` is what stops a busy stage from turning refused
            # clicks into a retry storm.
            self.request_recheck()
            raise InvalidStateError(self._status.maintenance_detail or MAINTENANCE_COPY)

    def require_round5_ready(self) -> None:
        self._manifest_check()
        if not self._round5_status.ring_ready:
            raise InvalidStateError(
                self._round5_status.maintenance_detail or MAINTENANCE_COPY
            )

    def _round5_journal(self) -> LakebaseCreationJournalStore:
        lease_store = self._round5_lease_store
        if lease_store is None:
            raise RuntimeError("Round 5 durable lease store is unavailable")
        return LakebaseCreationJournalStore(
            lease_store._run,  # type: ignore[attr-defined]
            authority_ring_key=lease_store.ring_key,
        )

    async def round5_prearm_guard(self, session_id: str, fencing_token: int) -> None:
        """Close the cache-to-claim race before any Round 5 setup mutation."""
        self._manifest_check()
        lease_store = self._round5_lease_store
        if lease_store is None:
            raise InvalidStateError("Round 5 durable coordination is unavailable")
        active = await lease_store.current()
        if (
            active is None
            or active.session_id != session_id
            or active.fencing_token != fencing_token
        ):
            raise InvalidStateError("Round 5 artifact authority is no longer current")
        try:
            unresolved = await self._unresolved_round5_bouts(self._round5_journal())
        except Exception as exc:
            self._round5_blocked(
                _blocked_detail(
                    "ROUND 5 BACKSTAGE CLEANUP",
                    exc,
                    unverified="DURABLE JOURNAL COULD NOT BE VERIFIED",
                )
            )
            self.request_round5_recheck()
            raise InvalidStateError(
                "Round 5 durable cleanup state could not be verified"
            ) from exc
        if any(bout_id != session_id for bout_id in unresolved):
            self._round5_maintenance()
            # This read is the authority for refusing the arm, and it has just
            # found reconciliation work that a settled reconciler is not looking
            # for. Hand it over rather than leaving it for the idle backstop.
            self.request_round5_recheck()
            raise InvalidStateError(
                "ROUND 5 BACKSTAGE CLEANUP IS STILL FINISHING · OTHER ROUNDS ARE READY"
            )

    def _maintenance(self, detail: str = MAINTENANCE_COPY) -> None:
        self._status = ReadinessStatus(False, "maintenance", detail)

    def _ready(self) -> None:
        self._status = ReadinessStatus(True, "ready", None)

    def _blocked(self, detail: str) -> None:
        self._status = ReadinessStatus(False, "blocked", detail)

    def _round5_maintenance(self, detail: str = MAINTENANCE_COPY) -> None:
        self._round5_status = ReadinessStatus(False, "maintenance", detail)

    def _round5_ready(self) -> None:
        self._round5_status = ReadinessStatus(True, "ready", None)

    def _round5_blocked(self, detail: str) -> None:
        self._round5_status = ReadinessStatus(False, "blocked", detail)

    async def run(self) -> None:
        await self._run_main_until_settled()
        if self._status.ring_ready and self._manifest_has_round5(self._manifest):
            await self._run_round5()

    def _retry_delay(self, attempts: int) -> float:
        """Exponential from `retry_seconds`, flat once it reaches the ceiling."""
        exponent = max(0, attempts - 1)
        return min(
            self._retry_seconds * (2.0**exponent),
            self._retry_ceiling_seconds,
        )

    def request_recheck(self) -> None:
        """Wake a waiting retry. Callers ask for a look; they do not set the rate."""

        self._recheck.set()

    def notify_credentials_recovered(self) -> None:
        """The credential sentry saw its verdict go from bad to good. Look now.

        Without this the gate has no way to learn that the fault it is backing
        off from has cleared, so it would sit out the rest of its ceiling-length
        sleep -- and the sentry, which asks AWS every few minutes and already
        knows, would have nowhere to say so. Both retry loops are woken: an
        expired session stops Round 5's reconciler for exactly the same reason it
        stops the ring.

        Only a wake-up, never a state change. The gate re-observes and decides;
        an observer that could declare the ring ready would be able to unlock a
        stage on the strength of a credential probe.
        """

        LOGGER.info(
            "AWS credentials are usable again; asking the readiness gate to look now"
        )
        self.request_recheck()
        self.request_round5_recheck()

    async def _await_retry(self, delay: float) -> None:
        """Wait out the backoff, but let an on-demand re-check cut it short.

        The same shape `_await_round5_recheck` uses -- an event with a timeout
        backstop -- with one addition: a floor. Every refused control action asks
        for a re-check, so without a floor a busy audience would drive one
        coordination attempt per click.
        """

        started = time.monotonic()
        deadline = started + delay
        floor = started + min(delay, self._poll_seconds)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            if self._recheck.is_set():
                # Checked before waiting, so a request that arrived while the
                # attempt was still running is honoured instead of being cleared
                # unread and slept through.
                self._recheck.clear()
                pause = min(floor - time.monotonic(), remaining)
                if pause > 0:
                    await asyncio.sleep(pause)
                return
            try:
                async with asyncio.timeout(remaining):
                    await self._recheck.wait()
            except TimeoutError:
                return

    async def _run_main_until_settled(self) -> None:
        """Keep observing until the ring settles, or until it is proven hopeless.

        `_run_main` used to be the whole of `run`: it swallowed every exception,
        `run` saw a not-ready ring and the task exited. A single blip therefore
        cost every control action until a human restarted the process, and
        nothing here restarts it. Retrying is safe because every step of
        `_run_main` observes -- it reads the durable row and the live ring
        generation, and the only writes it makes are to its own readiness row
        under a lease it holds at that moment.
        """

        attempts = 0
        first_failure_at: float | None = None
        while True:
            attempts += 1
            failure: BaseException | None = None
            try:
                async with self._attempt_lock:
                    await self._run_main()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - classified immediately below
                failure = exc
            if failure is None and self._status.ring_ready:
                self._recovery = SETTLED
                return
            if failure is not None and not is_retryable_startup_error(failure):
                self._give_up(failure, attempts=attempts)
                return
            if first_failure_at is None:
                first_failure_at = time.monotonic()
            delay = self._retry_delay(attempts)
            escalated = (
                time.monotonic() - first_failure_at
            ) >= self._escalate_after_seconds
            self._retrying(
                failure,
                attempts=attempts,
                delay=delay,
                escalated=escalated,
                waiting_on=(
                    environment_fault_subject(failure) if failure is not None else None
                ),
            )
            await self._await_retry(delay)

    @staticmethod
    def _retry_state(
        error: BaseException | None,
        *,
        prefix: str,
        attempts: int,
        delay: float,
        escalated: bool,
        waiting_on: str | None = None,
    ) -> tuple[str, RecoveryState]:
        """The banner copy and the machine-readable state for one pending retry."""

        name = type(error).__name__ if error is not None else "UnsettledRing"
        every = max(1, round(delay))
        detail = (
            f"{prefix} STILL FAILING AFTER {attempts} ATTEMPTS ({name.upper()}) · "
            f"STILL RETRYING EVERY {every}S · AN OPERATOR SHOULD LOOK"
            if escalated
            else (
                f"{prefix} RETRYING · ATTEMPT {attempts} FAILED ({name.upper()}) · "
                f"NEXT ATTEMPT IN {every}S"
            )
        )
        if waiting_on:
            # Appended rather than substituted: the existing sentence is what
            # operators and the control API already match on, and the subject is
            # the one fact that turns "failing" into an action.
            detail = f"{detail} · WAITING ON {waiting_on.upper()}"
        if error is not None:
            # The same appended-not-substituted bargain, for the same reason, and
            # the reason it is here at all: a retrying gate showed the class name
            # alone -- "ATTEMPT 5 FAILED (RECOVERYRESETERROR)" -- while the lane
            # underneath it was being refused by name. `given_up` had already
            # been taught to say why, and the retrying state is where an operator
            # is standing for longer, because it never stops.
            diagnosis = operator_diagnosis(error)
            if diagnosis and diagnosis != name:
                detail = f"{detail} · {diagnosis.upper()}"
        return detail, RecoveryState(
            "escalated" if escalated else "retrying",
            attempts=attempts,
            detail=detail,
            next_attempt_seconds=delay,
            error=name,
            waiting_on=waiting_on,
        )

    @staticmethod
    def _given_up_state(
        error: BaseException,
        *,
        current_detail: str | None,
        attempts: int,
    ) -> tuple[str, RecoveryState]:
        """Stop the schedule for a fault every retry would fail identically at.

        The refusal text the ring already carries is kept, because operators and
        the control API match on it; only the fact that nothing further will be
        attempted is added.
        """

        detail = (
            f"{current_detail or MAINTENANCE_COPY} · NOT RETRYING · "
            "OPERATOR ACTION REQUIRED"
        )
        return detail, RecoveryState(
            "given_up",
            attempts=attempts,
            detail=detail,
            error=type(error).__name__,
        )

    def _retrying(
        self,
        error: BaseException | None,
        *,
        attempts: int,
        delay: float,
        escalated: bool,
        waiting_on: str | None = None,
    ) -> None:
        detail, recovery = self._retry_state(
            error,
            prefix="BACKSTAGE CLEANUP",
            attempts=attempts,
            delay=delay,
            escalated=escalated,
            waiting_on=waiting_on,
        )
        # Escalation is reported as blocked rather than maintenance: the copy is
        # the only thing an operator watching the stage sees, and "will unlock
        # automatically" stops being true enough to keep saying.
        if escalated:
            self._blocked(detail)
        else:
            self._maintenance(detail)
        self._recovery = recovery
        LOGGER.warning(
            "Startup readiness attempt %d failed with %s; retrying in %.1fs%s",
            attempts,
            recovery.error,
            delay,
            " (escalated)" if escalated else "",
            exc_info=error is not None,
        )

    def _give_up(self, error: BaseException, *, attempts: int) -> None:
        detail, recovery = self._given_up_state(
            error,
            current_detail=self._status.maintenance_detail,
            attempts=attempts,
        )
        self._blocked(detail)
        self._recovery = recovery
        LOGGER.error(
            "Startup readiness is blocked by a %s that a retry cannot clear; "
            "this process will not try again",
            recovery.error,
            exc_info=error,
        )

    async def _run_main(self) -> None:
        try:
            await self._store.initialize()
            while True:
                durable = await self._store.read()
                generation = await self._store.ring_generation()
                if (
                    durable is not None
                    and durable.manifest_seal == self._seal
                    and durable.fencing_token == generation
                ):
                    if durable.state == "ready":
                        self._ready()
                        return
                    if durable.state == "blocked":
                        # A blocked result is durable evidence that the prior
                        # attempt failed, not a permanent operator lockout. Fall
                        # through and retry under a fresh fenced lease --
                        # in-process now, not only after a restart. Unchanged
                        # ownership still fails closed again.
                        self._maintenance("BACKSTAGE CLEANUP RETRY IN PROGRESS")

                active = await self._lease_store.current()
                if active is not None:
                    self._maintenance()
                    await asyncio.sleep(self._poll_seconds)
                    continue

                try:
                    await self._lead_cleanup()
                    return
                except LeaseHeldError:
                    self._maintenance()
                    await asyncio.sleep(self._poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Name the ring's state here, where the failure is, and then let the
            # caller decide whether it is worth another attempt. Swallowing this
            # is what used to end the gate for the life of the process.
            self._blocked(_blocked_detail("BACKSTAGE CLEANUP", exc))
            raise

    def request_round5_recheck(self) -> None:
        """Wake a settled reconciler because something needs a fresh answer.

        The reconciler stops re-reading once it has seen a steady ready state,
        so whatever *discovers* that the state may have moved is responsible for
        asking for the next look. Setting an event rather than reading here keeps
        the observation on the single owner of the cleanup lease.
        """

        self._round5_recheck.set()

    async def _await_round5_recheck(self) -> None:
        """Idle until something asks for a re-check, or the backstop expires."""

        try:
            async with asyncio.timeout(self._idle_poll_seconds):
                await self._round5_recheck.wait()
        except TimeoutError:
            pass
        finally:
            self._round5_recheck.clear()

    def _round5_retrying(
        self,
        error: BaseException,
        *,
        attempts: int,
        delay: float,
        escalated: bool,
        waiting_on: str | None = None,
    ) -> None:
        detail, recovery = self._retry_state(
            error,
            prefix="ROUND 5 BACKSTAGE CLEANUP",
            attempts=attempts,
            delay=delay,
            escalated=escalated,
            waiting_on=waiting_on,
        )
        if escalated:
            self._round5_blocked(detail)
        else:
            self._round5_maintenance(detail)
        self._round5_recovery = recovery
        LOGGER.warning(
            "Round 5 readiness attempt %d failed with %s; retrying in %.1fs%s",
            attempts,
            recovery.error,
            delay,
            " (escalated)" if escalated else "",
            exc_info=error,
        )

    def _round5_give_up(self, error: BaseException, *, attempts: int) -> None:
        detail, recovery = self._given_up_state(
            error,
            current_detail=self._round5_status.maintenance_detail,
            attempts=attempts,
        )
        self._round5_blocked(detail)
        self._round5_recovery = recovery
        LOGGER.error(
            "Round 5 readiness is blocked by a %s that a retry cannot clear; "
            "only an operator or an explicit re-check will move it",
            recovery.error,
            exc_info=error,
        )

    async def _round5_iteration(
        self,
        store: StartupReadinessStore,
        lease_store: BoutLeaseStore,
        journal: LakebaseCreationJournalStore,
    ) -> bool:
        """One reconciliation pass. True only once nothing is left to do.

        That return value is the steady signal: it is produced only by the path
        that has proven, against the durable row and the live ring generation,
        that the work is finished. Every other path returns False and keeps the
        fast cadence, so active leases and unresolved bouts are still chased at
        `poll_seconds`.
        """

        active = await lease_store.current()
        if active is not None:
            self._round5_maintenance()
            return False

        bouts = await self._unresolved_round5_bouts(journal)
        if bouts:
            self._round5_maintenance()
            await self._lead_round5_cleanup(journal=journal, bouts=bouts)
            return False

        durable = await store.read()
        generation = await store.ring_generation()
        if (
            durable is not None
            and durable.manifest_seal == self._seal
            and durable.fencing_token == generation
            and durable.state == "ready"
        ):
            self._round5_ready()
            return True
        await self._lead_round5_cleanup(journal=journal, bouts=())
        return False

    async def _run_round5(self) -> None:
        store = self._round5_store
        lease_store = self._round5_lease_store
        assert store is not None
        assert lease_store is not None
        initialized = False
        journal: LakebaseCreationJournalStore | None = None
        failures = 0
        first_failure_at: float | None = None
        while True:
            steady = False
            # Set when a failure is classified as one no retry can clear. The
            # loop then idles on the same on-demand event a settled reconciler
            # waits on, instead of re-running a hopeless attempt every
            # `poll_seconds` and holding the coordination endpoint awake for it.
            surrendered = False
            answered = False
            delay = self._poll_seconds
            try:
                if not initialized:
                    await store.initialize()
                    journal = self._round5_journal()
                    initialized = True
                assert journal is not None
                steady = await self._round5_iteration(store, lease_store, journal)
                answered = True
            except asyncio.CancelledError:
                raise
            except LeaseHeldError:
                # Contention, not a fault: coordination answered, and the answer
                # was that somebody else owns the ring right now.
                self._round5_maintenance()
                answered = True
            except Exception as exc:  # noqa: BLE001 - classified immediately below
                failures += 1
                self._round5_blocked(
                    _blocked_detail("ROUND 5 BACKSTAGE CLEANUP", exc)
                )
                if is_retryable_startup_error(exc):
                    if first_failure_at is None:
                        first_failure_at = time.monotonic()
                    delay = self._retry_delay(failures)
                    self._round5_retrying(
                        exc,
                        attempts=failures,
                        delay=delay,
                        escalated=(
                            time.monotonic() - first_failure_at
                            >= self._escalate_after_seconds
                        ),
                        waiting_on=environment_fault_subject(exc),
                    )
                else:
                    self._round5_give_up(exc, attempts=failures)
                    surrendered = True
            if answered:
                failures = 0
                first_failure_at = None
                if steady:
                    self._round5_recovery = SETTLED
            if steady or surrendered:
                await self._await_round5_recheck()
            else:
                await asyncio.sleep(delay)

    async def _unresolved_round5_bouts(
        self, journal: LakebaseCreationJournalStore
    ) -> tuple[str, ...]:
        if not self._manifest.round5_ready:
            return ()
        return tuple(await journal.unresolved_bout_ids())

    def _operator(self) -> BoutOperator:
        round4 = self._manifest.round4
        assert round4 is not None
        return BoutOperator(
            display_name="Backstage maintenance",
            subject=f"maintenance:{round4.app_service_principal_client_id.casefold()}",
        )

    async def _claim(
        self,
        session_id: str,
        *,
        lease_store: BoutLeaseStore | None = None,
    ) -> BoutLease:
        store = lease_store or self._lease_store
        return await store.claim(
            session_id=session_id,
            operator=self._operator(),
            phase="startup_cleanup",
            session_state=SessionState.RUNNING,
            round_id="startup_cleanup",
            round_title="Backstage startup cleanup",
            competitor_id="all",
            competitor_name="Manifest-owned runtime artifacts",
            ttl=self._ttl,
        )

    async def _with_heartbeat(
        self,
        lease: BoutLease,
        operation: Callable[[BoutLease], Awaitable[None]],
        *,
        lease_store: BoutLeaseStore | None = None,
    ) -> BoutLease:
        store = lease_store or self._lease_store
        holder = [lease]

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(self._heartbeat_seconds)
                holder[0] = await store.renew(holder[0], ttl=self._ttl)

        heartbeat_task = asyncio.create_task(heartbeat(), name="startup-cleanup-heartbeat")
        operation_task = asyncio.create_task(operation(lease), name="startup-cleanup-operation")
        try:
            done, _ = await asyncio.wait(
                {heartbeat_task, operation_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeat_task in done:
                operation_task.cancel()
                await asyncio.gather(operation_task, return_exceptions=True)
                raise RingFenceLostError(
                    "Startup cleanup lost its durable ring fence"
                ) from heartbeat_task.exception()
            operation_task.result()
            return holder[0]
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def _lead_cleanup(self) -> None:
        lease = await self._claim(f"startup-cleanup-{self._manifest.run_id}")
        try:
            await self._store.write(
                lease, manifest_seal=self._seal, state="maintenance", detail=MAINTENANCE_COPY
            )

            async def cleanup(_authority: BoutLease) -> None:
                await self._safe_change_factory(self._manifest).reset_all()
                await self._recovery_factory(self._manifest).reset_all()

            lease = await self._with_heartbeat(lease, cleanup)
            await self._lease_store.release(lease)
            lease = await self._claim(f"startup-cleanup-{self._manifest.run_id}")
            await self._store.write(
                lease, manifest_seal=self._seal, state="ready", detail=None
            )
            self._ready()
        except Exception as exc:
            detail = _blocked_detail("BACKSTAGE CLEANUP", exc)
            self._blocked(detail)
            try:
                await self._store.write(
                    lease, manifest_seal=self._seal, state="blocked", detail=detail
                )
            except Exception:
                pass
            raise exc
        finally:
            await self._lease_store.release(lease)

    async def _lead_round5_cleanup(
        self,
        *,
        journal: LakebaseCreationJournalStore | None = None,
        bouts: tuple[str, ...] | None = None,
    ) -> None:
        lease_store = self._round5_lease_store
        store = self._round5_store
        assert lease_store is not None
        assert store is not None
        journal = journal or self._round5_journal()
        bouts = bouts if bouts is not None else await self._unresolved_round5_bouts(journal)
        first_session = bouts[0] if bouts else f"startup-cleanup-{self._manifest.run_id}"
        lease = await self._claim(first_session, lease_store=lease_store)
        try:
            await store.write(
                lease, manifest_seal=self._seal, state="maintenance", detail=MAINTENANCE_COPY
            )

            if bouts:
                lease = await self._with_heartbeat(
                    lease,
                    lambda authority: self._reconcile_round5(bouts[0], authority, journal),
                    lease_store=lease_store,
                )
            await lease_store.release(lease)

            for bout_id in bouts[1:]:
                lease = await self._claim(bout_id, lease_store=lease_store)
                try:
                    lease = await self._with_heartbeat(
                        lease,
                        lambda authority, bout_id=bout_id: self._reconcile_round5(
                            bout_id, authority, journal
                        ),
                        lease_store=lease_store,
                    )
                finally:
                    await lease_store.release(lease)

            # A final short lease makes READY itself fenced and records the exact
            # generation that all replicas compare before unlocking.
            lease = await self._claim(
                f"startup-cleanup-{self._manifest.run_id}", lease_store=lease_store
            )
            await store.write(
                lease, manifest_seal=self._seal, state="ready", detail=None
            )
            self._round5_ready()
        except Exception as exc:
            detail = _blocked_detail("ROUND 5 BACKSTAGE CLEANUP", exc)
            self._round5_blocked(detail)
            try:
                await store.write(
                    lease, manifest_seal=self._seal, state="blocked", detail=detail
                )
            except Exception:
                pass
            raise exc
        finally:
            await lease_store.release(lease)

    async def _reconcile_round5(
        self,
        bout_id: str,
        authority: BoutLease,
        journal: LakebaseCreationJournalStore,
    ) -> None:
        if self._round5_factory is None:
            raise RuntimeError("Round 5 cleanup adapter is unavailable for a durable journal")

        scopes = tuple(await journal.scopes(bout_id))
        competitor_ids: set[str] = set()
        if not scopes:
            raise RuntimeError("Round 5 cleanup journal scope is unavailable")
        for scope in scopes:
            events = tuple(await journal.events(scope))
            if not events:
                raise RuntimeError("Round 5 cleanup journal scope is empty")
            for event in events:
                competitor_id = event.metadata.get("competitor_id")
                if not isinstance(competitor_id, str) or competitor_id not in {
                    "rds_postgres",
                    "aurora_serverless_v2",
                }:
                    raise RuntimeError("Round 5 cleanup journal competitor is not sealed")
                competitor_ids.add(competitor_id)
        if len(competitor_ids) != 1:
            raise RuntimeError("Round 5 cleanup journal mixes competitor ownership")

        class ActiveFence:
            async def assert_current(inner_self, scope: Any) -> None:
                assert self._round5_lease_store is not None
                active = await self._round5_lease_store.current()
                if (
                    active is None
                    or active.session_id != scope.bout_id
                    or active.fencing_token != scope.fencing_token
                    or active.phase != "startup_cleanup"
                ):
                    raise RuntimeError("Round 5 startup cleanup fence is no longer current")

        engine = self._round5_factory(competitor_ids.pop(), journal, ActiveFence())
        await engine.reconcile_failed_cleanup(bout_id, authority.fencing_token)


__all__ = [
    "GATE_ESCALATE_AFTER_SECONDS",
    "GATE_RETRY_CEILING_SECONDS",
    "GATE_RETRY_SECONDS",
    "MAINTENANCE_COPY",
    "ROUND5_IDLE_POLL_SECONDS",
    "SETTLED",
    "UNVERIFIED_COPY",
    "ReadinessStatus",
    "RecoveryState",
    "RingFenceLostError",
    "ShowtimeReadinessGate",
    "StartupReadinessStore",
]
