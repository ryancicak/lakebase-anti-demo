from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from math import ceil, floor
from typing import Any, Protocol, TypeVar
from uuid import uuid4

import psycopg
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    CredentialRetrievalError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
    ProfileNotFound,
    ReadTimeoutError,
    RefreshWithMFAUnsupportedError,
    SSOError,
    TokenRetrievalError,
    UnknownCredentialError,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    Aborted,
    DatabricksError,
    DeadlineExceeded,
    InternalError,
    RequestLimitExceeded,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
)

from .models import BoutOperator, SessionState

LOGGER = logging.getLogger(__name__)

RING_KEY = "main"
ROUND5_RING_KEY = "round5"
_VALID_RING_KEYS = frozenset({RING_KEY, ROUND5_RING_KEY})
_ROUND_RING_KEY = re.compile(
    r"installation:[A-Za-z0-9][A-Za-z0-9._:-]{0,127}:"
    r"round:[a-z0-9][a-z0-9_]{0,127}(?::cleanup)?"
)
COORDINATION_SCHEMA = "anti_demo_coordination"
COORDINATION_TABLE = f"{COORDINATION_SCHEMA}.ring_lease"

LEASE_HEARTBEAT_SECONDS_ENV = "ANTI_DEMO_LEASE_HEARTBEAT_SECONDS"
DEFAULT_LEASE_HEARTBEAT_SECONDS = 15.0

COORDINATION_ENDPOINT_ENV = "ANTI_DEMO_COORDINATION_ENDPOINT_NAME"
ALLOW_INMEMORY_COORDINATION_ENV = "ANTI_DEMO_ALLOW_INMEMORY_COORDINATION"

# Everything a process gives up by coordinating in its own memory. This is not a
# list of theoretical risks: a launch that bypassed `antidemo serve` took every one of
# these losses at once, silently, in front of an audience, because the fallback
# looked identical to the durable path from the outside.
INMEMORY_COORDINATION_LOSSES = (
    "cross-process fencing -- a second server process can hold the same ring and "
    "run a bout at the same time",
    "the durable readiness gate -- it is replaced by a stub that reports ready "
    "without checking anything",
    "per-round ring isolation -- rounds share one ring and the round_id query "
    "parameter is discarded",
    "per-round cost recording -- no cost ledger rows are written for any bout",
    "orphan deletion at startup -- leaked clones are reported and refused, never "
    "deleted",
    "Round 5 (survive_connection_spike) -- it is withheld and the catalog reports "
    "its availability as planned",
)

# There is deliberately no silent fallback. `build_lease_store` used to return a
# process-local store whenever the endpoint name was absent, which made a
# developer-mode server indistinguishable from a fenced one: same UI, same
# /readyz, same green catalog. Refusing is what makes the missing endpoint
# visible at startup instead of during a bout.
_NO_COORDINATION_ENDPOINT = (
    f"No coordination endpoint is configured: {COORDINATION_ENDPOINT_ENV} is unset, "
    "so this process cannot fence a bout against any other process. Start the server "
    "through 'antidemo serve', which loads the owned manifest and sets it for you; running "
    "uvicorn against app:app directly skips that step and is how this happens. To "
    f"accept process-local coordination on purpose, set {ALLOW_INMEMORY_COORDINATION_ENV}=1, "
    "which permits the in-memory store and logs exactly what it gives up. There is no "
    "silent default: a degraded ring looks identical to a durable one from the outside."
)

# Checked before the opt-in, and separately from it. Replicas of a deployed app
# cannot see each other's memory, so a process-local ring there is not a
# developer convenience -- it is two replicas owning one ring at once.
_DEPLOYED_REQUIRES_LAKEBASE = (
    f"Databricks Apps require {COORDINATION_ENDPOINT_ENV}; "
    "process-local locking is not allowed. "
    f"{ALLOW_INMEMORY_COORDINATION_ENV} does not apply to a deployed app: replicas do "
    "not share memory, so an in-memory ring would let two of them run one bout at once."
)

_inmemory_warning_emitted = False


def validate_ring_key(ring_key: str) -> str:
    if (
        not isinstance(ring_key, str)
        or (
            ring_key not in _VALID_RING_KEYS
            and _ROUND_RING_KEY.fullmatch(ring_key) is None
        )
    ):
        raise ValueError(
            f"ring_key must be one of {sorted(_VALID_RING_KEYS)!r} or a valid scoped round key"
        )
    return ring_key


def round_ring_key(
    installation_id: str,
    round_id: str,
    *,
    cleanup: bool = False,
) -> str:
    """Return the durable v7 row key for one installed app and one round."""
    if cleanup and round_id != "survive_connection_spike":
        raise ValueError("a cleanup ring key is reserved for Round 5")
    suffix = ":cleanup" if cleanup else ""
    return validate_ring_key(
        f"installation:{installation_id}:round:{round_id}{suffix}"
    )


def lease_heartbeat_seconds() -> float:
    """The cadence a live lease owner is expected to keep, in seconds."""
    raw = os.environ.get(LEASE_HEARTBEAT_SECONDS_ENV, "").strip()
    try:
        seconds = float(raw) if raw else DEFAULT_LEASE_HEARTBEAT_SECONDS
    except ValueError:
        seconds = DEFAULT_LEASE_HEARTBEAT_SECONDS
    return seconds if seconds > 0 else DEFAULT_LEASE_HEARTBEAT_SECONDS


def _as_utc(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


#: Phases whose row is pinned when it is written and deliberately never renewed.
#: ``RunManager._mark_bout_armed`` cancels the heartbeat and sets the expiry to
#: the arm's own deadline, so an armed row is silent for the whole of its life by
#: design. ``cooldown_failed`` is also silent by design: it is a terminal,
#: finite-TTL safety fence whose whole purpose is to expire rather than renew.
#: Every other phase heartbeats, which is what makes silence the only evidence a
#: crashed owner leaves -- and what makes reading silence off a phase that never
#: speaks a false accusation rather than a finding.
PINNED_LEASE_PHASES = frozenset({"armed", "cooldown_failed"})


@dataclass(frozen=True)
class HeldLeaseDiagnosis:
    """Exactly what one observation of a held ring row proves, and nothing more."""

    remaining_seconds: int
    heartbeat_age_seconds: int
    heartbeat_interval_seconds: int
    likely_stale: bool
    pinned: bool = False


def diagnose_held_lease(
    lease: BoutLease,
    *,
    now: datetime,
    heartbeat_seconds: float | None = None,
) -> HeldLeaseDiagnosis:
    """Measure a held row against the clock that observed it.

    A crashed replica leaves an unexpired row behind, so an unexpired row alone
    cannot tell an operator whether anyone is still working. The heartbeat can:
    a live owner renews every interval, so silence longer than one interval is
    evidence of a dead owner. It is evidence, not proof, which is why the caller
    reports it as *likely* stale and still waits out the full TTL.

    Unless the phase is one that never renews. An armed bout stops its heartbeat
    on purpose, so its row is silent from the moment it is armed and the rule
    above turns every ordinary pause between arming and ringing the bell into a
    LEASE LIKELY STALE on the ring banner. Ordinary presentation pacing is tens
    of seconds against a 15 s interval, so that reading was not an edge case; it
    was the common case, and it told an operator the ring was probably dead at
    the one moment it was certainly alive.
    """
    interval = lease_heartbeat_seconds() if heartbeat_seconds is None else heartbeat_seconds
    observed = _as_utc(now)
    remaining = max(0, ceil((_as_utc(lease.expires_at) - observed).total_seconds()))
    heartbeat_age = max(0, floor((observed - _as_utc(lease.updated_at)).total_seconds()))
    pinned = lease.phase.strip().casefold() in PINNED_LEASE_PHASES
    return HeldLeaseDiagnosis(
        remaining_seconds=remaining,
        heartbeat_age_seconds=heartbeat_age,
        heartbeat_interval_seconds=max(1, ceil(interval)),
        likely_stale=heartbeat_age > interval and not pinned,
        pinned=pinned,
    )


def describe_held_lease(lease: BoutLease, diagnosis: HeldLeaseDiagnosis) -> str:
    """Render the refusal an operator reads on stage.

    Keeps the BOUT IN PROGRESS prefix that every existing caller and the ring
    banner match on, then adds only facts read from the row itself.
    """
    owner = (lease.operator.display_name or lease.owner_subject or "unknown operator").strip()
    title = (lease.round_title or lease.round_id or "live proof").strip()
    phase = (lease.phase or "unknown phase").strip().replace("_", " ")
    segments = ["BOUT IN PROGRESS"]
    if diagnosis.likely_stale:
        segments.append("LEASE LIKELY STALE")
    segments.extend((title.upper(), phase.upper(), owner.upper()))
    segments.append(f"FENCE {lease.fencing_token}")
    # A pinned phase gets its age reported against the phase itself rather than
    # against a heartbeat it was never going to send. Saying "HEARTBEAT 41S AGO"
    # of an armed row would claim a renewal that never happened.
    if diagnosis.pinned:
        segments.append(f"{phase.upper()} {diagnosis.heartbeat_age_seconds}S AGO")
    else:
        segments.append(
            f"NO HEARTBEAT FOR {diagnosis.heartbeat_age_seconds}S"
            if diagnosis.likely_stale
            else f"HEARTBEAT {diagnosis.heartbeat_age_seconds}S AGO"
        )
    segments.append(f"RING UNLOCKS IN {diagnosis.remaining_seconds}S")
    return " · ".join(segments)


class LeaseHeldError(RuntimeError):
    """The row is held by someone. The message says who, and for how much longer."""

    def __init__(
        self,
        lease: BoutLease,
        *,
        observed_at: datetime | None = None,
        heartbeat_seconds: float | None = None,
    ) -> None:
        observed = _as_utc(observed_at) if observed_at is not None else datetime.now(UTC)
        diagnosis = diagnose_held_lease(
            lease,
            now=observed,
            heartbeat_seconds=heartbeat_seconds,
        )
        super().__init__(describe_held_lease(lease, diagnosis))
        self.lease = lease
        self.observed_at = observed
        self.diagnosis = diagnosis


class LeaseLostError(RuntimeError):
    pass


class CoordinationObjectsMissingError(RuntimeError):
    """A coordination object this process needs is absent, and it cannot create it.

    Deliberately *not* the same thing as ``InsufficientPrivilege`` on its own.
    A deployed replica is a consumer of ``anti_demo_coordination``, not its
    owner: it holds no DDL on the database and is not meant to. Being refused a
    ``CREATE`` when the objects are already there is the designed state and must
    not stop a start. Being refused one when they are *not* there is the other
    case entirely, and it is fatal -- serving on a ledger nothing has ever
    confirmed exists is the "reported health it never checked" failure this
    project keeps rediscovering.

    A plain ``RuntimeError``, so it lands on the non-transient side of
    :func:`is_transient_coordination_error` and the readiness gate gives up
    loudly on it instead of retrying a denial that no amount of waiting clears.
    """


@dataclass(frozen=True)
class CoordinationObjects:
    """What is actually present in the coordination schema, as opposed to usable.

    Existence and permission are separate questions and Postgres answers them in
    the unhelpful order: ``CREATE SCHEMA IF NOT EXISTS`` checks the ACL *before*
    the ``IF NOT EXISTS``, so a consumer is refused for a statement that would
    have done nothing. Reading ``pg_catalog`` asks only the first question, and
    every role may.

    ``schema_present`` is tracked separately from the tables because it decides
    which privilege a repair would need: creating the schema needs ``CREATE`` on
    the *database*, creating a table inside an existing one needs ``CREATE`` on
    the *schema*.
    """

    schema: str
    schema_present: bool
    missing_tables: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return self.schema_present and not self.missing_tables

    def describe_missing(self) -> str:
        if self.schema_present:
            return ", ".join(self.missing_tables) or "nothing"
        return f"schema {self.schema} (and therefore all of its tables)"


async def read_coordination_objects(
    cursor: Any,
    tables: Sequence[str],
    *,
    schema: str = COORDINATION_SCHEMA,
) -> CoordinationObjects:
    """Look up which coordination relations exist, without asking to use them.

    ``tables`` are schema-qualified names; the qualifier is dropped for the
    catalog lookup and restored in :attr:`CoordinationObjects.missing_tables` so
    callers report the name an operator would grant on.
    """

    await cursor.execute(
        "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s",
        (schema,),
    )
    if await cursor.fetchone() is None:
        return CoordinationObjects(
            schema=schema,
            schema_present=False,
            missing_tables=tuple(tables),
        )
    wanted = {name.rsplit(".", 1)[-1]: name for name in tables}
    await cursor.execute(
        """
        SELECT c.relname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = ANY(%s) AND c.relkind IN ('r', 'p')
        """,
        (schema, list(wanted)),
    )
    present = {str(row[0]) for row in await cursor.fetchall()}
    return CoordinationObjects(
        schema=schema,
        schema_present=True,
        missing_tables=tuple(
            qualified for short, qualified in wanted.items() if short not in present
        ),
    )


# The line between "try again" and "tell an operator". Everything here is a
# transport, provider, or contention fault: the same call, made again later,
# can succeed without anybody changing anything. Everything *not* here --
# InvalidStateError, ValueError, a seal or ownership mismatch, a missing
# environment binding, `build_lease_store` refusing an unconfigured endpoint --
# fails identically on every attempt, so retrying it only hides it.
#
# `LeaseHeldError` and `LeaseLostError` are on the transient side deliberately:
# both mean another owner moved the fence, which is exactly the condition a
# fresh claim resolves.
_TRANSIENT_COORDINATION_ERRORS: tuple[type[BaseException], ...] = (
    Aborted,
    ConnectionClosedError,
    ConnectionError,
    ConnectTimeoutError,
    DeadlineExceeded,
    EndpointConnectionError,
    InternalError,
    LeaseHeldError,
    LeaseLostError,
    OSError,
    ReadTimeoutError,
    ResourceExhausted,
    TemporarilyUnavailable,
    TimeoutError,
    psycopg.OperationalError,
)

# `databricks.sdk.errors.DatabricksError` is a subclass of `OSError`, so the bare
# `OSError` above -- which is there for sockets -- silently swept in every answer
# the workspace control plane ever gives. A `PermissionDenied` naming a project
# ACL, an `Unauthenticated` naming an expired principal, a `BadRequest` naming a
# command the deployed CLI does not have: all of them read as transport blips, so
# the gate would retry them at the ceiling interval forever and keep reporting
# ordinary maintenance while nothing paged anybody.
#
# The SDK's own taxonomy is the line, and it is the same line this tuple already
# drew by hand: `Aborted`, `DeadlineExceeded`, `InternalError`,
# `ResourceExhausted` and `TemporarilyUnavailable` are named above because they
# *are* worth retrying. The two added here complete that set rather than widening
# it. Everything else the control plane raises is an answer, not an outage, and
# an answer does not change because you asked again.
_TRANSIENT_DATABRICKS_ERRORS: tuple[type[BaseException], ...] = (
    Aborted,
    DeadlineExceeded,
    InternalError,
    RequestLimitExceeded,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
)


def _link_is_transient(link: BaseException) -> bool:
    if isinstance(link, DatabricksError):
        return isinstance(link, _TRANSIENT_DATABRICKS_ERRORS)
    return isinstance(link, _TRANSIENT_COORDINATION_ERRORS)


# Botocore's shapes for "this process cannot obtain usable credentials right
# now". Every one of them is a statement about the *environment*, not about this
# installation: an expired SSO session, an unreadable token cache, a profile that
# has not been logged into. `aws sso login` in another terminal, or a rotated key
# file landing on disk, clears them with nothing inside this process changing --
# which is exactly why they must be retried rather than given up on. A serving
# process that stops on one stays wedged after the credentials come back, and
# only a restart clears it.
#
# `NoCredentialsError` is here for the same reason: absent credentials are the
# state a machine is in *between* two valid sets of them.
_ENVIRONMENT_CREDENTIAL_ERRORS: tuple[type[BaseException], ...] = (
    CredentialRetrievalError,
    NoCredentialsError,
    PartialCredentialsError,
    ProfileNotFound,
    RefreshWithMFAUnsupportedError,
    SSOError,
    TokenRetrievalError,
    UnknownCredentialError,
)

# AWS error codes that describe the service or the session, never this
# installation's configuration. Expiry is deliberately on this side even though
# `server/aws_credential_probe.py` reports it as `rejected`: the probe is
# answering "should an operator be told", and this is answering "can waiting
# help". For a profile-mode install it demonstrably can, because credentials are
# re-resolved from disk on every call.
#
# `AccessDenied` and friends are deliberately *not* here. A denial is a policy
# statement about this principal, so it is the "genuinely wrong" case that has to
# stop loudly rather than be retried into the background.
_ENVIRONMENT_ERROR_CODES = frozenset(
    {
        "ExpiredToken",
        "ExpiredTokenException",
        "InternalError",
        "InternalFailure",
        "RequestExpired",
        "RequestLimitExceeded",
        "RequestThrottled",
        "RequestThrottledException",
        "ServiceUnavailable",
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "TokenRefreshRequired",
    }
)

#: What an operator would have to change outside this process, phrased for the
#: `/readyz` line an operator actually reads. Keyed by nothing clever on purpose:
#: there are two environmental subjects and conflating them would hide the one
#: that has a one-command fix.
CREDENTIAL_FAULT_SUBJECT = "AWS credentials"
SERVICE_FAULT_SUBJECT = "AWS service availability"


def _cause_chain(error: BaseException) -> tuple[BaseException, ...]:
    """Every exception implicated in ``error``, wrappers and lane causes alike.

    Two kinds of indirection have to be followed. The ordinary one is
    ``__cause__``/``__context__``: a preflight or a manifest load re-raises the
    transport error underneath its own type, and classifying only the outermost
    one would call every one of them permanent.

    The other is a *fan-in* wrapper. ``SafeChangeResetError`` and
    ``RecoveryResetError`` summarise several independent lanes, so they have no
    single cause and their own ``__cause__`` is ``None``. The real failures hang
    off the lane results, which is why those errors publish
    ``underlying_causes()``. Duck-typed rather than imported so this module keeps
    knowing nothing about the round engines.
    """

    found: list[BaseException] = []
    seen: set[int] = set()
    pending: list[BaseException] = [error]
    while pending:
        current = pending.pop()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            found.append(current)
            causes = getattr(current, "underlying_causes", None)
            if callable(causes):
                try:
                    pending.extend(cause for cause in causes() if cause is not None)
                except Exception:  # noqa: BLE001 - a classifier may never raise
                    pass
            current = current.__cause__ or current.__context__
    return tuple(found)


def is_transient_coordination_error(error: BaseException) -> bool:
    """Whether waiting and retrying this failure can plausibly clear it."""

    return any(_link_is_transient(link) for link in _cause_chain(error))


def privilege_refusal(error: BaseException) -> psycopg.errors.InsufficientPrivilege | None:
    """The Postgres ACL refusal implicated in ``error``, if there is one.

    ``InsufficientPrivilege`` (SQLSTATE 42501) is the only class worth matching.
    Its ancestors are ``ProgrammingError`` and ``DatabaseError``, which a syntax
    error and a constraint violation also are, so widening to either would tell
    an operator to fix a grant for a bug in a statement. The class-28 pair --
    ``InvalidAuthorizationSpecification`` and ``InvalidPassword`` -- is a failure
    to *log in*, not a refusal to touch a relation, and its remedy is a
    credential rather than a ``GRANT``.

    Chained rather than a bare ``isinstance`` because the refusal reaches its
    reader wrapped: ``StartupReadinessStore.initialize`` re-raises it as
    ``CoordinationObjectsMissingError``, and the cleanup lanes fan several
    independent failures into one summary error whose own ``__cause__`` is
    ``None``. Both are shapes that have already hidden a real cause here once.
    """

    for link in _cause_chain(error):
        if isinstance(link, psycopg.errors.InsufficientPrivilege):
            return link
    return None


def environment_fault_subject(error: BaseException) -> str | None:
    """What the environment is failing to supply, or ``None`` if it is not at fault.

    The return value is the whole point rather than a byproduct: a gate that is
    waiting has to be able to say *what* it is waiting for, or "still retrying"
    is indistinguishable from "broken and thrashing" on a health page.
    """

    subject: str | None = None
    for link in _cause_chain(error):
        if isinstance(link, _ENVIRONMENT_CREDENTIAL_ERRORS):
            # Credentials outrank a service fault: they are the one an operator
            # can fix with a single command, so they must not be masked by a
            # throttle that happened to be in the same chain.
            return CREDENTIAL_FAULT_SUBJECT
        if isinstance(link, ClientError):
            code = str((link.response or {}).get("Error", {}).get("Code") or "")
            if code not in _ENVIRONMENT_ERROR_CODES:
                continue
            if code in {
                "ExpiredToken",
                "ExpiredTokenException",
                "RequestExpired",
                "TokenRefreshRequired",
            }:
                return CREDENTIAL_FAULT_SUBJECT
            subject = subject or SERVICE_FAULT_SUBJECT
    return subject


def is_recoverable_environment_error(error: BaseException) -> bool:
    """Whether the fault is the environment's rather than this installation's."""

    return environment_fault_subject(error) is not None


def is_retryable_startup_error(error: BaseException) -> bool:
    """The readiness gate's line between "wait" and "stop and tell somebody".

    Deliberately a composition rather than a wider
    :func:`is_transient_coordination_error`. That function's contract is about
    *coordination* -- transport, provider and contention -- and two other callers
    depend on it meaning only that: ``_open_runtime``'s three-attempt store-open
    loop, and the startup retry that must still fail fast on a misconfigured
    endpoint. An expired SSO session is not a coordination fault, so widening
    that predicate would have changed both of those as a side effect. This is the
    predicate for the one question the gate asks.
    """

    return is_transient_coordination_error(error) or is_recoverable_environment_error(error)


def owner_subject(operator: BoutOperator) -> str:
    """Return the stable identity used for authorization, never a display label."""
    value = operator.subject or operator.email
    if value:
        return value.strip().casefold()
    # This path exists only for the explicitly local in-memory mode. Databricks
    # Apps requests are rejected before reaching the manager without SSO identity.
    return f"local:{operator.display_name.strip().casefold()}"


@dataclass(frozen=True)
class BoutLease:
    lease_id: str
    fencing_token: int
    session_id: str
    operator: BoutOperator
    owner_subject: str
    phase: str
    session_state: SessionState
    round_id: str
    round_title: str
    competitor_id: str
    competitor_name: str
    started_at: datetime
    updated_at: datetime
    expires_at: datetime


class BoutLeaseStore(Protocol):
    mode: str
    ring_key: str

    async def initialize(self) -> None: ...

    async def close(self) -> None: ...

    def for_ring_key(self, ring_key: str) -> BoutLeaseStore: ...

    async def claim(
        self,
        *,
        session_id: str,
        operator: BoutOperator,
        phase: str,
        session_state: SessionState,
        round_id: str,
        round_title: str,
        competitor_id: str,
        competitor_name: str,
        ttl: timedelta,
        expected_previous_token: int | None = None,
    ) -> BoutLease: ...

    async def transition(
        self,
        lease: BoutLease,
        *,
        operator: BoutOperator,
        expected_phase: str,
        phase: str,
        session_state: SessionState,
        ttl: timedelta,
    ) -> BoutLease: ...

    async def renew(self, lease: BoutLease, *, ttl: timedelta) -> BoutLease: ...

    async def release(self, lease: BoutLease) -> bool: ...

    async def current(self) -> BoutLease | None: ...


class InMemoryBoutLeaseStore:
    """Single-process fallback for local development and deterministic tests."""

    mode = "memory"

    def __init__(
        self,
        *,
        ring_key: str = RING_KEY,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.ring_key = validate_ring_key(ring_key)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock = asyncio.Lock()
        self._lease: BoutLease | None = None
        self._generation = 0
        self._scoped_stores: dict[str, InMemoryBoutLeaseStore] = {ring_key: self}

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def for_ring_key(self, ring_key: str) -> InMemoryBoutLeaseStore:
        ring_key = validate_ring_key(ring_key)
        store = self._scoped_stores.get(ring_key)
        if store is None:
            store = InMemoryBoutLeaseStore(ring_key=ring_key, clock=self._clock)
            store._scoped_stores = self._scoped_stores
            self._scoped_stores[ring_key] = store
        return store

    def _active(self, now: datetime) -> BoutLease | None:
        lease = self._lease
        if lease is not None and lease.expires_at <= now:
            self._lease = None
            return None
        return lease

    async def claim(
        self,
        *,
        session_id: str,
        operator: BoutOperator,
        phase: str,
        session_state: SessionState,
        round_id: str,
        round_title: str,
        competitor_id: str,
        competitor_name: str,
        ttl: timedelta,
        expected_previous_token: int | None = None,
    ) -> BoutLease:
        async with self._lock:
            now = self._clock()
            active = self._active(now)
            if active is not None:
                raise LeaseHeldError(active, observed_at=now)
            if (
                expected_previous_token is not None
                and expected_previous_token != self._generation
            ):
                raise LeaseLostError("RING FENCE CHANGED · CLEANUP REFUSED")
            self._generation += 1
            lease = BoutLease(
                lease_id=str(uuid4()),
                fencing_token=self._generation,
                session_id=session_id,
                operator=operator,
                owner_subject=owner_subject(operator),
                phase=phase,
                session_state=session_state,
                round_id=round_id,
                round_title=round_title,
                competitor_id=competitor_id,
                competitor_name=competitor_name,
                started_at=now,
                updated_at=now,
                expires_at=now + ttl,
            )
            self._lease = lease
            return lease

    async def transition(
        self,
        lease: BoutLease,
        *,
        operator: BoutOperator,
        expected_phase: str,
        phase: str,
        session_state: SessionState,
        ttl: timedelta,
    ) -> BoutLease:
        async with self._lock:
            now = self._clock()
            active = self._active(now)
            if (
                active is None
                or active.lease_id != lease.lease_id
                or active.fencing_token != lease.fencing_token
                or active.session_id != lease.session_id
                or active.owner_subject != owner_subject(operator)
                or active.phase != expected_phase
            ):
                raise LeaseLostError("RING LEASE EXPIRED · PREPARE THE FIGHT CARD AGAIN")
            transitioned = replace(
                active,
                phase=phase,
                session_state=session_state,
                updated_at=now,
                expires_at=now + ttl,
            )
            self._lease = transitioned
            return transitioned

    async def renew(self, lease: BoutLease, *, ttl: timedelta) -> BoutLease:
        async with self._lock:
            now = self._clock()
            active = self._active(now)
            if (
                active is None
                or active.lease_id != lease.lease_id
                or active.fencing_token != lease.fencing_token
                or active.session_id != lease.session_id
                or active.owner_subject != lease.owner_subject
                or active.phase != lease.phase
            ):
                raise LeaseLostError("RING LEASE LOST DURING ACTIVE WORK")
            renewed = replace(active, updated_at=now, expires_at=now + ttl)
            self._lease = renewed
            return renewed

    async def release(self, lease: BoutLease) -> bool:
        async with self._lock:
            active = self._active(self._clock())
            if (
                active is None
                or active.lease_id != lease.lease_id
                or active.fencing_token != lease.fencing_token
                or active.session_id != lease.session_id
                or active.owner_subject != lease.owner_subject
            ):
                return False
            self._lease = None
            return True

    async def current(self) -> BoutLease | None:
        async with self._lock:
            return self._active(self._clock())


T = TypeVar("T")


class LakebaseBoutLeaseStore:
    """A single-row, fenced lease shared by every Databricks App replica."""

    mode = "lakebase"

    def __init__(
        self,
        *,
        endpoint_name: str,
        database: str,
        profile: str = "",
        host: str = "",
        user: str = "",
        port: int = 5432,
        ring_key: str = RING_KEY,
        connector: Callable[..., Awaitable[Any]] = psycopg.AsyncConnection.connect,
        workspace_client: WorkspaceClient | None = None,
    ) -> None:
        if not endpoint_name:
            raise ValueError("A dedicated Lakebase coordination endpoint is required")
        self.endpoint_name = endpoint_name
        self.database = database
        self.profile = profile
        self.host = host
        self.user = user
        self.port = port
        self.ring_key = validate_ring_key(ring_key)
        self._connector = connector
        self._workspace = workspace_client or (
            WorkspaceClient(profile=profile) if profile else WorkspaceClient()
        )
        self._material_lock = asyncio.Lock()
        self._cached_token = ""
        self._token_refresh_at = 0.0

    async def _connection_material(self, *, force_refresh: bool = False) -> tuple[str, str, str]:
        async with self._material_lock:
            if not self.host:
                endpoint = await asyncio.to_thread(
                    self._workspace.postgres.get_endpoint,
                    self.endpoint_name,
                )
                self.host = str(endpoint.status.hosts.host if endpoint.status else "")
            if not self.user:
                self.user = (
                    os.environ.get("PGUSER", "").strip()
                    or str(
                        (await asyncio.to_thread(self._workspace.current_user.me)).user_name
                        or ""
                    )
                )
            now = time.monotonic()
            if force_refresh or not self._cached_token or now >= self._token_refresh_at:
                credential = await asyncio.to_thread(
                    self._workspace.postgres.generate_database_credential,
                    self.endpoint_name,
                )
                self._cached_token = str(credential.token or "")
                # Lakebase OAuth credentials are one hour. Recycle at 45 minutes.
                self._token_refresh_at = now + 2700
            if not self.host or not self.user or not self._cached_token:
                raise RuntimeError("Lakebase coordination host, user, or credential is missing")
            return self.host, self.user, self._cached_token

    async def _run(self, operation: Callable[[Any], Awaitable[T]]) -> T:
        last_error: BaseException | None = None
        for attempt in range(2):
            try:
                host, user, token = await self._connection_material(force_refresh=attempt > 0)
                connection = await self._connector(
                    host=host,
                    port=self.port,
                    dbname=self.database,
                    user=user,
                    password=token,
                    sslmode="require",
                    application_name="lakebase-anti-demo-coordination",
                    connect_timeout=15,
                    autocommit=True,
                )
                async with connection:
                    async with connection.cursor() as cursor:
                        return await operation(cursor)
            except psycopg.OperationalError as exc:
                last_error = exc
                self._cached_token = ""
                if attempt == 0:
                    await asyncio.sleep(0.25)
        assert last_error is not None
        raise last_error

    async def initialize(self) -> None:
        async def create_schema(cursor: Any) -> None:
            try:
                await cursor.execute(
                    f"SELECT fencing_token FROM {COORDINATION_TABLE} WHERE ring_key = %s",
                    (self.ring_key,),
                )
            except (psycopg.errors.InvalidSchemaName, psycopg.errors.UndefinedTable):
                await cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {COORDINATION_SCHEMA}")
                await cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {COORDINATION_TABLE} (
                    ring_key text PRIMARY KEY,
                    fencing_token bigint NOT NULL,
                    lease_id uuid,
                    session_id text,
                    owner_subject text,
                    owner_display_name text,
                    owner_email text,
                    phase text,
                    session_state text,
                    round_id text,
                    round_title text,
                    competitor_id text,
                    competitor_name text,
                    started_at timestamptz,
                    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                    expires_at timestamptz
                )
                    """
                )
            await cursor.execute(
                f"""
                INSERT INTO {COORDINATION_TABLE} (ring_key, fencing_token)
                VALUES (%s, 0)
                ON CONFLICT (ring_key) DO NOTHING
                """,
                (self.ring_key,),
            )

        await self._run(create_schema)

    async def close(self) -> None:
        self._cached_token = ""
        self._token_refresh_at = 0.0

    def for_ring_key(self, ring_key: str) -> LakebaseBoutLeaseStore:
        return LakebaseBoutLeaseStore(
            endpoint_name=self.endpoint_name,
            database=self.database,
            profile=self.profile,
            host=self.host,
            user=self.user,
            port=self.port,
            ring_key=validate_ring_key(ring_key),
            connector=self._connector,
            workspace_client=self._workspace,
        )

    @staticmethod
    def _row_to_lease(row: Any) -> BoutLease:
        return BoutLease(
            lease_id=str(row[0]),
            fencing_token=int(row[1]),
            session_id=str(row[2]),
            operator=BoutOperator(
                display_name=str(row[4]),
                email=str(row[5]) if row[5] else None,
                subject=str(row[3]),
            ),
            owner_subject=str(row[3]),
            phase=str(row[6]),
            session_state=SessionState(str(row[7])),
            round_id=str(row[8]),
            round_title=str(row[9]),
            competitor_id=str(row[10]),
            competitor_name=str(row[11]),
            started_at=row[12],
            updated_at=row[13],
            expires_at=row[14],
        )

    @staticmethod
    def _returning_columns() -> str:
        return (
            "lease_id, fencing_token, session_id, owner_subject, "
            "owner_display_name, owner_email, phase, session_state, round_id, "
            "round_title, competitor_id, competitor_name, started_at, updated_at, expires_at"
        )

    async def claim(
        self,
        *,
        session_id: str,
        operator: BoutOperator,
        phase: str,
        session_state: SessionState,
        round_id: str,
        round_title: str,
        competitor_id: str,
        competitor_name: str,
        ttl: timedelta,
        expected_previous_token: int | None = None,
    ) -> BoutLease:
        lease_id = str(uuid4())
        subject = owner_subject(operator)

        async def claim_row(cursor: Any) -> BoutLease:
            await cursor.execute(
                f"""
                INSERT INTO {COORDINATION_TABLE} (
                    ring_key, fencing_token, lease_id, session_id, owner_subject,
                    owner_display_name, owner_email, phase, session_state, round_id,
                    round_title, competitor_id, competitor_name, started_at, updated_at,
                    expires_at
                )
                VALUES (
                    %s, 1, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    clock_timestamp(), clock_timestamp(), clock_timestamp() + %s
                )
                ON CONFLICT (ring_key) DO UPDATE SET
                    fencing_token = {COORDINATION_TABLE}.fencing_token + 1,
                    lease_id = EXCLUDED.lease_id,
                    session_id = EXCLUDED.session_id,
                    owner_subject = EXCLUDED.owner_subject,
                    owner_display_name = EXCLUDED.owner_display_name,
                    owner_email = EXCLUDED.owner_email,
                    phase = EXCLUDED.phase,
                    session_state = EXCLUDED.session_state,
                    round_id = EXCLUDED.round_id,
                    round_title = EXCLUDED.round_title,
                    competitor_id = EXCLUDED.competitor_id,
                    competitor_name = EXCLUDED.competitor_name,
                    started_at = clock_timestamp(),
                    updated_at = clock_timestamp(),
                    expires_at = clock_timestamp() + %s
                WHERE (
                    {COORDINATION_TABLE}.lease_id IS NULL
                    OR {COORDINATION_TABLE}.expires_at <= clock_timestamp()
                )
                  AND (%s::bigint IS NULL OR {COORDINATION_TABLE}.fencing_token = %s)
                RETURNING {self._returning_columns()}
                """,
                (
                    self.ring_key,
                    lease_id,
                    session_id,
                    subject,
                    operator.display_name,
                    operator.email,
                    phase,
                    session_state.value,
                    round_id,
                    round_title,
                    competitor_id,
                    competitor_name,
                    ttl,
                    ttl,
                    expected_previous_token,
                    expected_previous_token,
                ),
            )
            row = await cursor.fetchone()
            if row is not None:
                return self._row_to_lease(row)
            # Read-only diagnosis of the row that just won the compare-and-set.
            # clock_timestamp() comes last so the lease column order is untouched,
            # and it is the same clock the WHERE clauses above compare against.
            await cursor.execute(
                f"""
                SELECT {self._returning_columns()}, clock_timestamp()
                FROM {COORDINATION_TABLE}
                WHERE ring_key = %s
                  AND lease_id IS NOT NULL
                  AND expires_at > clock_timestamp()
                """,
                (self.ring_key,),
            )
            active = await cursor.fetchone()
            if active is None:
                raise LeaseLostError("RING LEASE CHANGED WHILE CLAIMING; TRY AGAIN")
            raise LeaseHeldError(self._row_to_lease(active), observed_at=active[15])

        return await self._run(claim_row)

    async def transition(
        self,
        lease: BoutLease,
        *,
        operator: BoutOperator,
        expected_phase: str,
        phase: str,
        session_state: SessionState,
        ttl: timedelta,
    ) -> BoutLease:
        async def transition_row(cursor: Any) -> BoutLease:
            await cursor.execute(
                f"""
                UPDATE {COORDINATION_TABLE}
                SET phase = %s,
                    session_state = %s,
                    updated_at = clock_timestamp(),
                    expires_at = clock_timestamp() + %s
                WHERE ring_key = %s
                  AND lease_id = %s::uuid
                  AND fencing_token = %s
                  AND session_id = %s
                  AND owner_subject = %s
                  AND phase = %s
                  AND expires_at > clock_timestamp()
                RETURNING {self._returning_columns()}
                """,
                (
                    phase,
                    session_state.value,
                    ttl,
                    self.ring_key,
                    lease.lease_id,
                    lease.fencing_token,
                    lease.session_id,
                    owner_subject(operator),
                    expected_phase,
                ),
            )
            row = await cursor.fetchone()
            if row is None:
                raise LeaseLostError("RING LEASE EXPIRED · PREPARE THE FIGHT CARD AGAIN")
            return self._row_to_lease(row)

        return await self._run(transition_row)

    async def renew(self, lease: BoutLease, *, ttl: timedelta) -> BoutLease:
        async def renew_row(cursor: Any) -> BoutLease:
            await cursor.execute(
                f"""
                UPDATE {COORDINATION_TABLE}
                SET updated_at = clock_timestamp(),
                    expires_at = clock_timestamp() + %s
                WHERE ring_key = %s
                  AND lease_id = %s::uuid
                  AND fencing_token = %s
                  AND session_id = %s
                  AND owner_subject = %s
                  AND phase = %s
                  AND expires_at > clock_timestamp()
                RETURNING {self._returning_columns()}
                """,
                (
                    ttl,
                    self.ring_key,
                    lease.lease_id,
                    lease.fencing_token,
                    lease.session_id,
                    lease.owner_subject,
                    lease.phase,
                ),
            )
            row = await cursor.fetchone()
            if row is None:
                raise LeaseLostError("RING LEASE LOST DURING ACTIVE WORK")
            return self._row_to_lease(row)

        return await self._run(renew_row)

    async def release(self, lease: BoutLease) -> bool:
        async def release_row(cursor: Any) -> bool:
            await cursor.execute(
                f"""
                UPDATE {COORDINATION_TABLE}
                SET lease_id = NULL,
                    session_id = NULL,
                    owner_subject = NULL,
                    owner_display_name = NULL,
                    owner_email = NULL,
                    phase = NULL,
                    session_state = NULL,
                    round_id = NULL,
                    round_title = NULL,
                    competitor_id = NULL,
                    competitor_name = NULL,
                    started_at = NULL,
                    updated_at = clock_timestamp(),
                    expires_at = NULL
                WHERE ring_key = %s
                  AND lease_id = %s::uuid
                  AND fencing_token = %s
                  AND session_id = %s
                  AND owner_subject = %s
                RETURNING fencing_token
                """,
                (
                    self.ring_key,
                    lease.lease_id,
                    lease.fencing_token,
                    lease.session_id,
                    lease.owner_subject,
                ),
            )
            return await cursor.fetchone() is not None

        return await self._run(release_row)

    async def current(self) -> BoutLease | None:
        async def current_row(cursor: Any) -> BoutLease | None:
            await cursor.execute(
                f"""
                SELECT {self._returning_columns()}
                FROM {COORDINATION_TABLE}
                WHERE ring_key = %s
                  AND lease_id IS NOT NULL
                  AND expires_at > clock_timestamp()
                """,
                (self.ring_key,),
            )
            row = await cursor.fetchone()
            return self._row_to_lease(row) if row is not None else None

        return await self._run(current_row)


def inmemory_coordination_allowed() -> bool:
    """Whether the operator has explicitly accepted process-local coordination."""
    return os.environ.get(ALLOW_INMEMORY_COORDINATION_ENV, "").strip().casefold() in {
        "1",
        "true",
        "yes",
    }


def _warn_inmemory_coordination_once() -> None:
    """Say once, at the top of the log, exactly what this process cannot do."""
    global _inmemory_warning_emitted
    if _inmemory_warning_emitted:
        return
    _inmemory_warning_emitted = True
    LOGGER.warning(
        "DEGRADED COORDINATION: %s is unset and %s permits the process-local "
        "in-memory ring. This process gives up:\n%s",
        COORDINATION_ENDPOINT_ENV,
        ALLOW_INMEMORY_COORDINATION_ENV,
        "\n".join(f"  - {loss}" for loss in INMEMORY_COORDINATION_LOSSES),
    )


def build_lease_store(*, ring_key: str = RING_KEY) -> BoutLeaseStore:
    ring_key = validate_ring_key(ring_key)
    endpoint_name = os.environ.get(COORDINATION_ENDPOINT_ENV, "").strip()
    if not endpoint_name:
        # Deployed first, and unconditionally: the opt-in below must never reach a
        # multi-replica runtime, so this refusal is not allowed to depend on it.
        if os.environ.get("DATABRICKS_APP_NAME"):
            raise RuntimeError(_DEPLOYED_REQUIRES_LAKEBASE)
        if not inmemory_coordination_allowed():
            raise RuntimeError(_NO_COORDINATION_ENDPOINT)
        _warn_inmemory_coordination_once()
        return InMemoryBoutLeaseStore(ring_key=ring_key)

    measured_endpoint = os.environ.get("LAKEBASE_ENDPOINT_NAME", "").strip()
    if measured_endpoint and endpoint_name == measured_endpoint:
        raise RuntimeError(
            "The coordination endpoint must be separate from the measured Lakebase endpoint"
        )
    return LakebaseBoutLeaseStore(
        endpoint_name=endpoint_name,
        database=os.environ.get("ANTI_DEMO_COORDINATION_DATABASE", "anti_demo"),
        profile=os.environ.get("DATABRICKS_PROFILE", ""),
        host=os.environ.get("ANTI_DEMO_COORDINATION_HOST", ""),
        user=os.environ.get("ANTI_DEMO_COORDINATION_USER", ""),
        port=int(os.environ.get("ANTI_DEMO_COORDINATION_PORT", "5432")),
        ring_key=ring_key,
    )
