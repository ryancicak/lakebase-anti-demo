from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.errors.platform import PermissionDenied, Unauthenticated

from .bout_cost import build_bout_cost_disclosure
from .capacity import (
    ObservedCapacity,
    build_capacity_disclosure,
    observed_rds_instance_class,
)
from .catalog import (
    build_presenter_pack,
    competitor_by_id,
    persona_by_id,
    recommend_round,
    round_by_id,
)
from .coordination import (
    ROUND5_RING_KEY,
    BoutLease,
    BoutLeaseStore,
    InMemoryBoutLeaseStore,
    LeaseHeldError,
    LeaseLostError,
    describe_held_lease,
    diagnose_held_lease,
    privilege_refusal,
    round_ring_key,
)
from .cost_identity import ProviderResourceIdentity, capture_bout_cost_identity
from .cost_ledger import CalibrationKey, CostEstimate, CostLedgerStore
from .descent_cost import build_descent_cost_disclosure
from .live_orders import (
    LiveOrder,
    LiveOrdersArm,
    LiveOrdersEngine,
    LiveOrdersError,
    LiveOrdersPhase,
    LiveOrdersProgress,
    LiveOrdersResult,
)
from .manifest import DemoManifest
from .model_score import (
    ModelScoreArm,
    ModelScoreEngine,
    ModelScoreError,
    ModelScorePhase,
    ModelScoreProgress,
    ModelScoreProofResult,
    ModelScoreRunResult,
    ModelScoreUpdate,
)
from .models import (
    Availability,
    BoutOperator,
    BoutStatus,
    CapacityDisclosure,
    ComparisonKind,
    ComparisonSnapshot,
    CompetitorId,
    CooldownLaneSnapshot,
    CooldownLaneState,
    CooldownSnapshot,
    CooldownState,
    FairnessSnapshot,
    LaneActivity,
    LaneSnapshot,
    LaneState,
    MetricValue,
    RedoSnapshot,
    RedoState,
    ResetMode,
    RoundFiveSetupEvidenceSnapshot,
    RoundFiveSetupGateSnapshot,
    RoundFiveSetupLaneSnapshot,
    RoundFiveSetupSnapshot,
    RoundFiveSetupState,
    RoundId,
    RunEvent,
    SessionCreate,
    SessionSnapshot,
    SessionState,
    StandingCostDisclosure,
    TowelSnapshot,
    TowelState,
)
from .posted_usage import PostedDatabricksUsage
from .pricing import build_cost_receipt, update_terminal_cost_receipt
from .receipts import record_sealed_bout
from .reconcile import ReconciliationReport
from .recovery import (
    RecoveryArm,
    RecoveryEngine,
    RecoveryNotArmedError,
    RecoveryPhase,
    RecoveryProgress,
    RecoveryResetError,
    RecoveryRunResult,
    RecoveryStopControl,
    RecoveryStoppedResult,
)
from .round5_cleanup_owed import (
    Round5CleanupOwed,
    clear_round5_cleanup_owed,
    record_round5_cleanup_owed,
)
from .round_availability import GRANT_REFUSAL_HEADLINE, grant_refusal
from .safe_change import (
    SafeChangeArm,
    SafeChangeEngine,
    SafeChangeLaneState,
    SafeChangeNotArmedError,
    SafeChangePhase,
    SafeChangeProgress,
    SafeChangeResetError,
    SafeChangeRunResult,
    abandon_on_cancel,
)
from .standing_cost import build_standing_cost_disclosure
from .targets import (
    LiveTarget,
    TargetConfigurationError,
    TargetNotArmedError,
    TargetResolver,
)
from .towel import adjudicate_round_five_towel, adjudicate_towel
from .verifier import NeutralVerifier, VerificationResult, VerifierStopped

logger = logging.getLogger(__name__)


#: How much of a chain's own words reach a screen an audience may be looking at.
#: Both refusals that motivated this fit with room to spare: the longer renders
#: to 241 characters and names a principal, a permission and a database project.
#: The cap exists so one runaway provider message cannot push the sentence
#: explaining it off the panel, not to ration the diagnosis.
_DIAGNOSIS_LIMIT = 480


def _walk_chain(
    error: BaseException,
    render: Callable[[BaseException], str],
) -> str:
    """Render an exception and everything that caused it, on one line.

    THE CHAIN AND NOT THE HEAD. Round 5's disappearance surfaced as an
    `InvalidStateError` whose real cause was three frames further down, and the
    outermost link was the least informative one in it. A gate that validates a
    precondition has to report every failure it can see rather than the first,
    and for an exception the failures it can see *are* the chain.

    AND THROUGH THE FAN-IN. `SafeChangeResetError` and `RecoveryResetError`
    summarise several independent lanes, so their own `__cause__` is `None` and
    every real failure hangs off `underlying_causes()`. Following only
    `__cause__` through one of those renders the single word
    "SafeChangeResetError" -- which is precisely what the startup gate's banner
    said for every round on the card while Round 2's Lakebase lane was being
    refused by name. `coordination.is_transient_coordination_error` already walks
    the fan-in for the retry decision; this is the same shape for the sentence a
    reader gets, kept separate only because that one answers a yes/no about
    membership and this one has to produce something legible in lane order.

    Identity of what has already been rendered terminates the walk rather than
    depth alone, because `__context__` can point back at something already
    visited and a diagnostic must never become the failure.
    """

    values: list[str] = []
    seen: set[int] = set()
    pending: list[BaseException] = [error]
    while pending and len(values) < 6:
        current: BaseException | None = pending.pop(0)
        fanned: list[BaseException] = []
        while current is not None and id(current) not in seen and len(values) < 6:
            seen.add(id(current))
            values.append(render(current))
            causes = getattr(current, "underlying_causes", None)
            if callable(causes):
                try:
                    fanned.extend(cause for cause in causes() if cause is not None)
                except Exception:  # noqa: BLE001 - a diagnostic may never raise
                    pass
            current = current.__cause__ or current.__context__
        # Lane order, and after this link's own `__cause__` chain: a summary
        # reads "what failed, then why each lane failed".
        pending = fanned + pending
    return " <- ".join(values)


def _redacted_link(error: BaseException) -> str:
    """One link as a type name, plus the codes on it: AWS error, operation, SQLSTATE.

    The SQLSTATE is here for the same reason the AWS error code is. Without it
    every psycopg failure renders as one bare class name, so a privilege refusal
    and a connection fault are the same word on the screen -- and those two have
    opposite remedies, one a ``GRANT`` and one a wait. A five-character code is
    not an identifier: ``DataPlaneConnectionRefusedError`` already keeps the
    SQLSTATE and drops the DSN on exactly this reasoning, because the SQLSTATE is
    the actionable half and the message that carries it is not ours to quote.
    """

    value = type(error).__name__
    response = getattr(error, "response", None)
    if isinstance(response, Mapping):
        details = response.get("Error")
        if isinstance(details, Mapping):
            code = str(details.get("Code") or "")
            if code and code.replace("_", "").replace("-", "").isalnum():
                value = f"{value}[{code}]"
    sqlstate = getattr(error, "sqlstate", None)
    if isinstance(sqlstate, str) and sqlstate.isalnum():
        value = f"{value}[SQLSTATE {sqlstate}]"
    operation = str(getattr(error, "operation_name", "") or "")
    if operation and operation.replace("_", "").isalnum():
        value = f"{value}@{operation}"
    return value


def _message_is_ours_to_quote(error: BaseException) -> bool:
    """Whether this exception's own words may be shown to an operator.

    Two kinds may, and the boundary is who wrote the sentence.

    A `DatabricksError` may, because its message is the workspace answering a
    question this app asked about its own authorization, and the answer *is* the
    remedy: it names the table, the principal and the permission a workspace
    admin has to grant. `server.round_construction.exception_diagnostic` already
    took this position for messages this codebase raised about its own manifest;
    this is the same bargain extended exactly as far as the evidence requires.

    Anything else contributes its type name alone. That is not squeamishness:
    Rounds 1-3 and 5 reach Postgres and AWS on this same path, a psycopg or
    botocore message can carry a DSN or a secret ARN, and
    `_redacted_exception_chain` exists because one of them once nearly did.
    """

    kind = type(error)
    if issubclass(kind, DatabricksError):
        return True
    return (kind.__module__ or "").split(".")[0] == "server"


def _quotable_link(error: BaseException) -> str:
    rendered = _redacted_link(error)
    message = " ".join(str(error).split())
    if message and _message_is_ours_to_quote(error):
        return f"{rendered}: {message}"
    return rendered


def _redacted_exception_chain(error: BaseException) -> str:
    """Return useful failure codes without provider messages or resource values."""

    return _walk_chain(error, _redacted_link)


def operator_diagnosis(error: BaseException) -> str:
    """The whole cause chain as one readable line, keeping the words we may keep.

    A traceback is not the answer -- this text reaches a screen an audience may
    see -- but neither is a fixed string, which is what was there before and
    what turned two refusals naming an exact table and an exact principal into
    "could not be verified".
    """

    diagnosis = _walk_chain(error, _quotable_link)
    if len(diagnosis) <= _DIAGNOSIS_LIMIT:
        return diagnosis
    return diagnosis[: _DIAGNOSIS_LIMIT - 1].rstrip() + "…"


def authorization_refusal(error: BaseException) -> BaseException | None:
    """The Databricks authorization refusal anywhere in this cause chain.

    Both members matter and they are different failures wearing the same
    consequence. `PermissionDenied` is "we know who you are and you may not";
    `Unauthenticated` is "we do not know who you are". Neither is a fault to
    wait out, and neither is fixable from inside this app, which is why the
    catalog treats them as one class and says so in one sentence.

    The whole chain and not just the head, for the reason `_walk_chain`
    documents: an adapter that wraps its own call in a `RuntimeError` would
    otherwise hide the refusal that caused it.
    """

    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, PermissionDenied | Unauthenticated):
            return current
        current = current.__cause__ or current.__context__
    return None


def arm_failure_message(summary: str, error: BaseException) -> str:
    """What an operator reads when an arm is refused, with the reason attached.

    `summary` stays first and unchanged: it is the round's own words for what it
    was doing, and it is what makes the rest legible. What follows is why.
    """

    diagnosis = operator_diagnosis(error)
    if authorization_refusal(error) is not None:
        return f"{summary} {GRANT_REFUSAL_HEADLINE} Databricks said: {diagnosis}"
    return f"{summary} Diagnosis: {diagnosis}"


#: The Postgres counterpart of `GRANT_REFUSAL_HEADLINE`, for the one Postgres
#: class whose remedy is a `GRANT` rather than a wait. `server.readiness` already
#: says this on the startup banner for the coordination tables, and for the same
#: reason: an ACL refusal and an outage are indistinguishable from their effect
#: and could not be less alike in what an operator has to do about them.
COST_LEDGER_GRANT_HEADLINE = (
    "POSTGRES REFUSED THIS APP'S WRITE TO THE COST LEDGER, AND THIS IS NOT A "
    "FAULT TO WAIT OUT. This app's client ID has not been granted what the "
    "ledger write needs (docs/DEPLOY.md) · A LAKEBASE GRANT, NOT AWS IAM."
)


def cost_window_refusal(summary: str, error: BaseException) -> str:
    """What an operator reads when a bout's cost window could not be opened.

    One hardcoded sentence used to stand for every way this can fail, and the
    thing it named -- sealing the v7 cost identity -- was the half that usually
    had not failed. A Postgres privilege refusal, a connection fault and a
    genuine sealing bug all reached the 409 as the same words, so the screen an
    audience is watching said "cost identity" while the actual cause was a
    missing `GRANT` on the ledger. The reproduced case was exactly that: an
    `InsufficientPrivilege` on `cost_ledger` displayed as a sealing failure.

    So the summary claims only what is true of every case, and the cause chain
    follows it. The two refusal classes with a named remedy say the remedy,
    which is the same bargain `arm_failure_message` already makes for Databricks.
    The chain rather than the head, because `_walk_chain` reports every failure
    it can see and the outermost link here is routinely the least informative.
    """

    diagnosis = operator_diagnosis(error)
    if privilege_refusal(error) is not None:
        return f"{summary} {COST_LEDGER_GRANT_HEADLINE} Diagnosis: {diagnosis}"
    if authorization_refusal(error) is not None:
        return f"{summary} {GRANT_REFUSAL_HEADLINE} Databricks said: {diagnosis}"
    return f"{summary} Diagnosis: {diagnosis}"


class SessionNotFoundError(KeyError):
    pass


class InvalidStateError(RuntimeError):
    pass


class AmbiguousRingQueryError(ValueError):
    """A ring question that has no single answer on this installation.

    Distinct from InvalidStateError: nothing is wrong with the installation, the
    question is wrong. It maps to 400, not 409 or 503, because the caller can
    fix it by naming what it meant.
    """


_ROUND_ONE_TRANSACTION_WIRE_CALL = "PostgreSQL TLS connect → INSERT → COMMIT → SELECT"

_ROUND_FOUR_UNSUPPORTED_REASON = (
    "No AWS-native equivalent lane was configured or timed in this scoped proof."
)
_ROUND_FOUR_COMPARISON_DETAIL = (
    "Lakebase verified the scoped native Synced Tables capability; no AWS-native "
    "equivalent lane was timed. This is not a speed comparison."
)
_ROUND_FOUR_REMEMBERED_RESULT = "LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A"
_ROUND_SIX_REMEMBERED_RESULT = "LAKEBASE NATIVE CDF WIN · AWS PIPELINE NOT BUILT · MARGIN N/A"

_ROUND_FIVE_SCHEDULED_CLIENTS = 128
_ROUND_FIVE_WARMUP_CONNECTIONS = 4
_ROUND_FIVE_CONCURRENCY = 64
_ROUND_FIVE_WITNESS_CLIENTS = 64
_ROUND_FIVE_RUNNER = "Python 3.12 + psycopg 3.3.4"
_ROUND_FIVE_CLEANUP_PENDING = "Round 5 cleanup is settling automatically · Ring remains protected"

#: The two honest accounts of a control action aimed at a fight card this process
#: does not have. Both lead with what did *not* happen, because the operator's
#: first question at the bell is whether a bout is running somewhere and the old
#: answer -- a bare "Session not found" -- did not address it.
_FIGHT_CARD_GONE = (
    "This server process does not hold that fight card, and no ring lease names "
    "it, so nothing ran and no result was recorded. A fight card lives in the "
    "memory of the process that created it and does not survive a restart. "
    "Prepare a new fight card."
)

_FIGHT_CARD_ORPHANED = (
    "The ring still holds this fight card but this server process does not, so "
    "nothing ran and no result was recorded. An armed fight card lives only in "
    "the memory of the process that armed it, so a restart, a crash, or a second "
    "app instance leaves the ring holding it with no process able to ring the "
    "bell. Wait out the countdown below, then prepare the fight card again. {ring}"
)


def _observed_capacity(evidence: dict[str, dict]) -> ObservedCapacity:
    """Read configured capacity out of the arming evidence.

    Each control plane reports its own shape: Lakebase returns autoscaling CU
    limits, Aurora returns a serverless ACU range, RDS returns a fixed instance
    class. Anything a plane did not report stays None so the disclosure can say
    "unreported" rather than fall back to a constant.
    """

    lakebase = evidence.get("lakebase") or {}
    competitor = evidence.get("competitor") or {}

    def _optional_float(source: dict, key: str) -> float | None:
        value = source.get(key)
        return None if value is None else float(value)

    def _optional_str(source: dict, key: str) -> str | None:
        value = source.get(key)
        return str(value) if value else None

    auto_pause = competitor.get("auto_pause_seconds")
    return ObservedCapacity(
        lakebase_min_cu=_optional_float(lakebase, "autoscaling_limit_min_cu"),
        lakebase_max_cu=_optional_float(lakebase, "autoscaling_limit_max_cu"),
        aurora_min_acu=_optional_float(competitor, "min_capacity_acu"),
        aurora_max_acu=_optional_float(competitor, "max_capacity_acu"),
        aurora_auto_pause_seconds=None if auto_pause is None else int(auto_pause),
        aurora_engine_version=_optional_str(competitor, "engine_version"),
        rds_instance_class=_optional_str(competitor, "instance_class"),
        rds_engine_version=_optional_str(competitor, "engine_version"),
    )


def _round_one_cooldown_wire_call(
    lane_id: str,
    competitor_id: CompetitorId,
) -> str:
    if lane_id == "lakebase":
        return "databricks postgres get-endpoint"
    if competitor_id == CompetitorId.AURORA_SERVERLESS_V2:
        return (
            "RDS DescribeDBClusters + DescribeDBInstances + DescribeEvents → "
            "CloudWatch GetMetricStatistics fallback"
        )
    return "RDS DescribeDBInstances"


def reset_mode_for_round(round_id: RoundId) -> ResetMode:
    """Return the explicit, round-specific contract for a re-do."""
    if round_id == RoundId.WAKE_IDLE_APP:
        return ResetMode.RETURN_TO_IDLE
    if round_id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
        return ResetMode.DELETE_ISOLATED_ENVIRONMENT
    if round_id == RoundId.RECOVER_DELETED_ORDER:
        return ResetMode.DELETE_RECOVERY_ENVIRONMENT
    raise InvalidStateError(f"Re-do behavior is not defined for round {round_id.value}")


def _event_log_limit() -> int:
    """How many events one session keeps in memory. 0 or less means unbounded.

    A session's log is append-only and every snapshot-bearing event carries a
    fully serialised session, which runs 13-60 KB apiece on a real round, so the
    number here is worth roughly its own weight in tens of megabytes.

    500 is sized off the longest legitimate life one session can have rather than
    off a typical one. A bout publishes a few dozen events; an arm that polls to
    its 900s timeout at 5s intervals adds up to 180, and a cooldown that watches
    to its own deadline another 180, so even a session that waits out both ends
    well inside the cap and never evicts anything. What the cap bounds is the
    session that loops: a card re-armed after every failure keeps the same record,
    and its log would otherwise grow for as long as the process lives.
    """

    try:
        return int(os.environ.get("ANTI_DEMO_EVENT_LOG_MAX_EVENTS", "500"))
    except ValueError:
        return 500


@dataclass
class EventLog:
    events: list[RunEvent] = field(default_factory=list)
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    limit: int = field(default_factory=_event_log_limit)
    # How many events have been dropped off the front. This is the whole reason a
    # cap is safe: sequence numbers are assigned from it rather than from the
    # length of the list, so they stay monotonic and stable for the life of the
    # log even after early events are gone. Truncating a list whose sequences are
    # derived from len() would silently renumber every later event and hand a
    # resuming consumer the wrong ones -- a worse failure than the memory it saves.
    evicted: int = 0

    @property
    def next_sequence(self) -> int:
        """The sequence the next published event will carry."""
        return self.evicted + len(self.events) + 1

    @property
    def first_retained_sequence(self) -> int:
        """The oldest sequence still servable, or ``next_sequence`` when empty."""
        return self.evicted + 1

    async def publish(self, event: str, payload: dict[str, object]) -> RunEvent:
        async with self.condition:
            item = RunEvent.now(self.next_sequence, event, payload)
            self.events.append(item)
            if self.limit > 0 and len(self.events) > self.limit:
                overflow = len(self.events) - self.limit
                del self.events[:overflow]
                self.evicted += overflow
            self.condition.notify_all()
        # Every terminal transition in every round path funnels through here with
        # the serialised snapshot already in the payload, so one hook covers all of
        # them without any round needing to know receipts exist. Deliberately
        # outside the condition: no disk I/O while subscribers are blocked.
        record_sealed_bout(event, payload)
        return item

    async def stream(self, after_sequence: int = 0) -> AsyncIterator[RunEvent]:
        """Every event after ``after_sequence``, waiting for ones not published yet.

        A cursor that points into evicted history cannot be honoured, and the one
        thing this must never do is honour it approximately: the events are
        indexed by ``sequence - evicted``, never by the cursor itself, so a
        consumer can never be handed a shifted window that looks contiguous. When
        the requested cursor is older than the floor, the stream resumes at the
        oldest event it still has and says so -- ``gap_before`` on the first
        delivered event counts exactly how many events that consumer will never
        see. Silence there would be the corruption; a resumed play-by-play that
        skipped a beat without admitting it is indistinguishable from one that
        never had that beat at all.
        """

        cursor = max(0, after_sequence)
        while True:
            async with self.condition:
                while self.next_sequence <= cursor + 1:
                    await self.condition.wait()
                start = cursor - self.evicted
                gap = 0
                if start < 0:
                    gap = -start
                    start = 0
                pending = self.events[start:]
            if gap:
                logger.warning(
                    "A stream asked to resume at sequence %d but the log now starts "
                    "at %d; %d event(s) are gone and the gap is reported to the "
                    "consumer rather than skipped over silently.",
                    cursor + 1,
                    self.first_retained_sequence,
                    gap,
                )
            for item in pending:
                cursor = item.sequence
                # The stored event is shared with every other subscriber, so the
                # gap travels on a copy. Recomputed each pass: a consumer slow
                # enough to be outrun twice is told twice.
                yield item.model_copy(update={"gap_before": gap}) if gap else item
                gap = 0


@dataclass
class SessionRecord:
    snapshot: SessionSnapshot
    event_log: EventLog = field(default_factory=EventLog)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    lease_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    task: asyncio.Task[None] | None = None
    cooldown_task: asyncio.Task[None] | None = None
    live_targets: tuple[LiveTarget, LiveTarget] | None = None
    safe_change_engine: SafeChangeEngine | None = None
    safe_change_arm: SafeChangeArm | None = None
    recovery_engine: RecoveryEngine | None = None
    recovery_arm: RecoveryArm | None = None
    recovery_stop_control: RecoveryStopControl | None = None
    recovery_outcome: RecoveryRunResult | RecoveryStoppedResult | None = None
    model_score_engine: ModelScoreEngine | None = None
    model_score_arm: ModelScoreArm | None = None
    model_score_result: ModelScoreRunResult | None = None
    model_score_pending_update: ModelScoreUpdate | None = None
    model_score_terminal_published: bool = False
    model_score_terminal_task: asyncio.Task[None] | None = None
    model_score_terminal_lease: BoutLease | None = None
    connection_spike_engine: object | None = None
    connection_spike_arm: object | None = None
    connection_spike_setup_result: object | None = None
    connection_spike_cleanup_task: asyncio.Task[None] | None = None
    live_orders_engine: LiveOrdersEngine | None = None
    live_orders_arm: LiveOrdersArm | None = None
    live_orders_result: LiveOrdersResult | None = None
    live_orders_pending_order: LiveOrder | None = None
    live_orders_guardrail_order: LiveOrder | None = None
    settlement_task: asyncio.Task[None] | None = None
    run_started_monotonic_ns: int | None = None
    # Measurement-clock reading taken when a Round 5 setup lane last advanced
    # its published `setup_elapsed_ms`, keyed by lane id. Pairing the two lets a
    # towel carry that lane's elapsed time forward across a phase that reports
    # nothing, without the manager ever having to know the orchestrator's
    # private T0. Kept off the snapshot deliberately: it is a raw monotonic
    # reading, and no monotonic value may reach a public event.
    round5_progress_observed_ns: dict[str, int] = field(default_factory=dict)
    # Consecutive Lakebase IDLE observations are timed in the process monotonic
    # domain. The public snapshot carries only their count and wall-clock upper
    # bound; a raw monotonic value must never cross the API boundary.
    cooldown_idle_observations: dict[str, tuple[int, int]] = field(default_factory=dict)
    towel_generation: int = 0
    towel_stop_event: asyncio.Event | None = None
    armed_at_monotonic: float | None = None
    armed_expiry_task: asyncio.Task[None] | None = None
    lease_heartbeat_task: asyncio.Task[None] | None = None
    lease_heartbeat_lease: BoutLease | None = None
    lease: BoutLease | None = None
    round5_lease_heartbeat_task: asyncio.Task[None] | None = None
    round5_lease_heartbeat_lease: BoutLease | None = None
    round5_lease: BoutLease | None = None
    round2_fencing_token: int | None = None
    operator: BoutOperator | None = None
    cost_bout_id: str | None = None
    cost_bout_started_at: datetime | None = None
    cost_bout_kind: str | None = None
    # Monotonic reading of the last time anything asked for this record. Retention
    # is decided from it rather than from ``snapshot.updated_at``, because a wall
    # clock that steps backwards would make a settled record look freshly touched
    # and keep it forever.
    #
    # Deliberately taken from time.monotonic_ns() and not from the manager's
    # injected ``clock_ns``. That clock is the measurement clock -- what the rounds
    # time bouts with -- and housekeeping must not appear in it. Rounds assert on
    # what the measurement clock reads, which is exactly the guarantee that would
    # be lost by letting record bookkeeping move it.
    last_active_ns: int = 0


class RunManager:
    def __init__(
        self,
        resolver: TargetResolver | None = None,
        verifier: NeutralVerifier | None = None,
        safe_change_factory: Callable[[], SafeChangeEngine] | None = None,
        recovery_factory: Callable[[], RecoveryEngine] | None = None,
        model_score_factory: Callable[[], ModelScoreEngine] | None = None,
        connection_spike_factory: Callable[[CompetitorId], object] | None = None,
        live_orders_factory: Callable[[], LiveOrdersEngine] | None = None,
        lease_store: BoutLeaseStore | None = None,
        round5_lease_store: BoutLeaseStore | None = None,
        readiness_check: Callable[[], None] | None = None,
        readiness_status: Callable[[], object] | None = None,
        round5_readiness_check: Callable[[], None] | None = None,
        round5_readiness_status: Callable[[], object] | None = None,
        round5_prearm_guard: Callable[[str, int], Awaitable[None]] | None = None,
        round_isolation: bool = False,
        installation_id: str = "",
        cost_ledger_store: CostLedgerStore | None = None,
        cost_manifest: DemoManifest | None = None,
        posted_usage: Callable[[], PostedDatabricksUsage | None] | None = None,
        drift_report: Callable[[], ReconciliationReport | None] | None = None,
        clock_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._records: dict[str, SessionRecord] = {}
        self._records_lock = asyncio.Lock()
        self._lease_store = lease_store or InMemoryBoutLeaseStore()
        self._round5_lease_store = round5_lease_store or InMemoryBoutLeaseStore(
            ring_key=ROUND5_RING_KEY
        )
        self._round_isolation = round_isolation
        self._installation_id = installation_id.strip()
        if self._round_isolation and not self._installation_id:
            raise ValueError("round isolation requires a sealed installation_id")
        if (cost_ledger_store is None) != (cost_manifest is None):
            raise ValueError("cost ledger store and cost manifest must be configured together")
        if cost_manifest is not None:
            if cost_manifest.manifest_version != 7:
                raise ValueError("durable per-round cost capture requires manifest v7")
            if cost_manifest.installation_id != self._installation_id:
                raise ValueError("cost manifest installation identity does not match the ring")
        self._cost_ledger_store = cost_ledger_store
        self._cost_manifest = cost_manifest
        # Both are cache reads, never provider calls. A disclosure render that
        # reached the billing API or the AWS account would make the panel's
        # latency depend on two services it exists to describe, and reconciling
        # inline would put a describe-* sweep behind every poll of a session.
        self._posted_usage = posted_usage
        self._drift_report = drift_report
        # Keyed by durable ring key, and bounded by construction rather than by a
        # policy: every key comes from round_ring_key() over the six-member RoundId
        # enum, plus the one Round 5 cleanup ring, so this holds at most seven
        # entries no matter how many bouts the installation runs.
        self._scoped_lease_stores: dict[str, BoutLeaseStore] = {}
        self._resolver = resolver or TargetResolver()
        self._verifier = verifier or NeutralVerifier()
        self._safe_change_factory = safe_change_factory or self._build_safe_change_engine
        self._recovery_factory = recovery_factory or self._build_recovery_engine
        self._model_score_factory = model_score_factory
        self._connection_spike_factory = connection_spike_factory
        self._live_orders_factory = live_orders_factory
        self._readiness_check = readiness_check or (lambda: None)
        self._readiness_status = readiness_status
        self._round5_readiness_check = round5_readiness_check or (lambda: None)
        self._round5_readiness_status = round5_readiness_status
        self._round5_prearm_guard = round5_prearm_guard
        # Rounds Databricks has refused on authorization in this process. Kept
        # here rather than on the session because the point of keeping it is to
        # stop the *next* fight card being offered, and the session that was
        # refused is released long before that happens.
        self._grant_refusals: dict[RoundId, str] = {}
        self._clock_ns = clock_ns
        self._close_lock = asyncio.Lock()
        self._closed = False
        self._arm_timeout = float(os.environ.get("ANTI_DEMO_ARM_TIMEOUT_SECONDS", "900"))
        self._round5_prearm_timeout = float(
            os.environ.get("ANTI_DEMO_ROUND5_PREARM_TIMEOUT_SECONDS", "120")
        )
        self._arm_poll = float(os.environ.get("ANTI_DEMO_ARM_POLL_SECONDS", "5"))
        # How long an armed fight card may wait for the bell. This is a
        # ring-release deadline, not a warmth guarantee, and the distinction is
        # what sets the number.
        #
        # It cannot be a warmth guarantee: every round that has a decayable
        # precondition re-establishes it *after* the bell rather than trusting
        # the arm. Round 1 re-runs assert_armed in _run, Round 4 calls
        # _warm_application_endpoint immediately before the measured commit, and
        # Round 6 calls _warm_checkout_endpoint before its clock starts. Those
        # two warm-ups exist precisely because the endpoint's idle suspend comes
        # due *inside* this window -- read their docstrings -- so an earlier 60 s
        # bounded nothing that mattered to measurement integrity.
        #
        # What it does bound is how long an abandoned arm holds the durable ring
        # and any state the round set up, because _mark_bout_armed pins the lease
        # to armed_expires_at and deliberately stops the heartbeat. Against that
        # purpose 60 s was far tighter than anything else in the system: the same
        # ring tolerates a heartbeaten CHECKING hold for up to _arm_timeout, 900
        # seconds. Ordinary presentation pacing -- arm, address the room, ring --
        # was measured at a 37 s gap and came within 10 ms of expiring the round
        # in front of an audience.
        self._armed_ttl = float(os.environ.get("ANTI_DEMO_ARM_TTL_SECONDS", "180"))
        self._cooldown_timeout = float(os.environ.get("ANTI_DEMO_COOLDOWN_TIMEOUT_SECONDS", "900"))
        self._cooldown_poll_timeout = float(
            os.environ.get("ANTI_DEMO_COOLDOWN_POLL_TIMEOUT_SECONDS", "30")
        )
        self._lakebase_idle_dwell = max(
            0.001,
            float(os.environ.get("ANTI_DEMO_LAKEBASE_IDLE_DWELL_SECONDS", "0.001")),
        )
        self._lease_heartbeat = float(os.environ.get("ANTI_DEMO_LEASE_HEARTBEAT_SECONDS", "15"))
        self._active_lease_ttl = max(
            self._lease_heartbeat * 3,
            float(os.environ.get("ANTI_DEMO_ACTIVE_LEASE_TTL_SECONDS", "90")),
        )
        self._running_lease_ttl = max(
            self._active_lease_ttl,
            float(
                os.environ.get(
                    "ANTI_DEMO_RUNNING_LEASE_TTL_SECONDS",
                    str(self._active_lease_ttl),
                )
            ),
        )
        self._terminal_release_call_timeout = float(
            os.environ.get("ANTI_DEMO_TERMINAL_RELEASE_CALL_TIMEOUT_SECONDS", "5")
        )
        self._terminal_release_backoff_cap = float(
            os.environ.get("ANTI_DEMO_TERMINAL_RELEASE_BACKOFF_CAP_SECONDS", "1")
        )
        # How long a cancelled Round 4 waits for its terminal publication before
        # abandoning it. Matches the sibling rounds' teardown bound and stays
        # well inside the shutdown settle timeout below, so a cancelled record
        # still settles within the window close() gives it.
        self._terminal_publish_timeout = float(
            os.environ.get("ANTI_DEMO_TERMINAL_PUBLISH_TIMEOUT_SECONDS", "5")
        )
        # The overall ceiling on that publication's own lease retry. Bounding
        # only the waiter would leave this loop running for the life of the
        # process, because it swallows every store failure by design.
        self._terminal_settle_deadline = float(
            os.environ.get("ANTI_DEMO_TERMINAL_SETTLE_DEADLINE_SECONDS", "30")
        )
        self._shutdown_settle_timeout = float(
            os.environ.get("ANTI_DEMO_SHUTDOWN_SETTLE_TIMEOUT_SECONDS", "15")
        )
        self._shutdown_cleanup_timeout = float(
            os.environ.get("ANTI_DEMO_SHUTDOWN_CLEANUP_TIMEOUT_SECONDS", "30")
        )
        self._cleanup_retry_initial = float(
            os.environ.get("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "1")
        )
        self._cleanup_retry_max = float(os.environ.get("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "30"))
        # The automatic Round 5 cleanup retry has to terminate. It used to loop
        # `while not self._closed`, which is correct for a transient AWS fault
        # and wrong for a durable one: the towel stays `cleaning` forever, which
        # is the one cleanup state that offers the operator neither a retry
        # button nor an exit. So it is bounded, and then it hands over to a
        # human by failing loudly.
        #
        # 120 attempts is roughly 58 minutes at the 1s..30s backoff, plus each
        # attempt's own AWS round trip. It was 20 attempts -- roughly eight
        # minutes -- and eight minutes is shorter than AWS. A measured bout took
        # 31.5 minutes from an accepted `DeleteDBProxy` to the proxy actually
        # disappearing, so cleanup gave up about five minutes early and wrote
        # `cleanup_failure` onto the receipt of a bout that was entirely
        # healthy. Worse, it abandoned while still waiting on the proxy, so the
        # per-bout security group -- deletable only after the proxy that
        # references it is gone -- was never attempted even once, and leaked.
        #
        # The budget is not the ring-held time: abandoning does not release the
        # lease either, it only converts automatic recovery into a manual one,
        # and a human pressing "Retry cleanup" at minute eight cannot make AWS
        # finish sooner. What the budget really buys is the delay before the
        # operator is told to intervene, so it is sized at roughly double the
        # one slow deletion observed rather than trimmed to just clear it.
        self._cleanup_retry_attempts = max(
            1, int(os.environ.get("ANTI_DEMO_CLEANUP_RETRY_ATTEMPTS", "120"))
        )
        # Swappable so a test can total up the schedule the loop actually asks
        # for. The attempt count on its own says nothing about the budget --
        # only the sum of the delays does -- and the alternative to this seam is
        # a test that either waits an hour or re-derives the backoff and then
        # drifts away from it.
        self._cleanup_retry_sleep = asyncio.sleep
        # How long a towelled run task gets to notice the cooperative stop
        # before it is cancelled outright. Rounds 1 and 3 have no explicit
        # cancel -- they stop by observing a control -- so a task parked in a
        # provider call that never looks at that control used to hold the ring
        # for as long as the process lived.
        self._towel_stop_grace = float(
            os.environ.get("ANTI_DEMO_TOWEL_STOP_GRACE_SECONDS", "60")
        )
        # And how long the cancellation itself gets to land. Past this the
        # settlement is reported as unverified rather than waited on, because a
        # task that ignores both the stop control and its own cancellation is
        # not going to be waited out.
        self._towel_cancel_timeout = float(
            os.environ.get("ANTI_DEMO_TOWEL_CANCEL_TIMEOUT_SECONDS", "30")
        )
        self._settlement_attempts = max(
            1, int(os.environ.get("ANTI_DEMO_SETTLEMENT_ATTEMPTS", "4"))
        )
        # Session retention. A session is created per bout attempt, abandoned ones
        # included, and each one holds a snapshot, up to five engines and an event
        # log -- about a megabyte. Nothing used to release them, so an installation
        # meant to run unattended grew by that much per attempt forever.
        #
        # Two bounds, because neither alone is honest. The cap is the hard
        # guarantee: no burst of attempts can grow the store past it. The idle
        # window is what makes an installation return to its floor overnight
        # instead of resting at the cap. The floor keeps the newest few records
        # whatever their age, because releasing one is not free -- a browser still
        # holding that session 404s on its next reconcile and loses the screen the
        # operator is presenting from, which is why age alone must not evict.
        self._session_retention_max = max(
            1, int(os.environ.get("ANTI_DEMO_SESSION_RETENTION_MAX", "32"))
        )
        self._session_retention_seconds = max(
            0.0, float(os.environ.get("ANTI_DEMO_SESSION_RETENTION_SECONDS", "900"))
        )
        self._session_retention_floor = min(
            self._session_retention_max,
            max(1, int(os.environ.get("ANTI_DEMO_SESSION_RETENTION_FLOOR", "4"))),
        )

    def _lease_store_for_round(self, round_id: RoundId) -> BoutLeaseStore:
        if not self._round_isolation:
            return self._lease_store
        key = round_ring_key(self._installation_id, round_id.value)
        store = self._scoped_lease_stores.get(key)
        if store is None:
            store = self._lease_store.for_ring_key(key)
            self._scoped_lease_stores[key] = store
        return store

    def _lease_store_for_record(self, record: SessionRecord) -> BoutLeaseStore:
        return self._lease_store_for_round(record.snapshot.round.id)

    def _round5_cleanup_store(self) -> BoutLeaseStore:
        if not self._round_isolation:
            return self._round5_lease_store
        key = round_ring_key(
            self._installation_id,
            RoundId.SURVIVE_CONNECTION_SPIKE.value,
            cleanup=True,
        )
        if self._round5_lease_store.ring_key == key:
            return self._round5_lease_store
        store = self._scoped_lease_stores.get(key)
        if store is None:
            store = self._lease_store.for_ring_key(key)
            self._scoped_lease_stores[key] = store
        return store

    @property
    def model_score_available(self) -> bool:
        return self._model_score_factory is not None

    @property
    def connection_spike_available(self) -> bool:
        return self._connection_spike_factory is not None

    @property
    def live_orders_available(self) -> bool:
        return self._live_orders_factory is not None

    @property
    def grant_refusals(self) -> Mapping[RoundId, str]:
        """Rounds Databricks has refused on authorization, and what it said.

        A copy, because the round-select screen must not be able to edit the
        record it is reading. Free to call: the catalog reads it on every render
        and nothing here opens a socket, which is the rule
        `server.round_availability` is built on.
        """

        return dict(self._grant_refusals)

    def _standing_cost_disclosure(
        self,
        capacity: CapacityDisclosure | None = None,
    ) -> StandingCostDisclosure:
        """What the installation is billed for right now, rebuilt on every read.

        WHY PER READ AND NOT ONCE AT CREATION. ``as_of`` and the accrued figure
        beside it are the whole subject of this panel. Building the disclosure
        when the session is created would freeze both, so a fight card left open
        for an hour would show an hour-old ``as_of`` and an accrued total that
        stopped accruing -- on a panel whose entire claim is elapsed spend.

        The builder does no I/O, which is what makes rebuilding it per read
        cheap: the AWS half is arithmetic over a sealed shape and a rate card,
        and both of the inputs that *do* involve I/O arrive already cached. A
        ``None`` manifest correctly yields the unreadable disclosure, which
        carries no dollar figure anywhere, so it needs no guard of its own.

        WHERE IT IS REBUILT. ``create`` and ``get``. Snapshots published onto
        the event stream keep whatever the last of those built, which is right:
        an event is a record of a moment and rewriting the disclosure inside one
        would backdate history. The panel is on the ringside overlay, which is
        fed by ``api.getSession`` rather than by the stream.

        ``capacity`` is the session's own capacity disclosure, and the RDS
        instance class is read out of it so the cost lane can say whether the
        class it prices is the class AWS is running. This adds no I/O: the
        reading, when there is one, was taken by the arming gate's
        ``describe-db-instances`` and is already on the snapshot. Before arming
        there is none, and the cost lane labels its figure configured-only
        rather than assuming the two agree.
        """

        return build_standing_cost_disclosure(
            self._cost_manifest,
            now=datetime.now(UTC),
            report=self._drift_report() if self._drift_report is not None else None,
            posted=self._posted_usage() if self._posted_usage is not None else None,
            observed_rds_instance_class=observed_rds_instance_class(capacity),
        )

    @staticmethod
    def _cost_resource_for_line(
        line: object,
        resources: tuple[ProviderResourceIdentity, ...],
        competitor_id: CompetitorId,
    ) -> ProviderResourceIdentity | None:
        component = str(getattr(line, "component", ""))
        wanted: str | None = None
        if component == "Lakebase compute":
            wanted = "lakebase_endpoint"
        elif component == "Lakebase database, PITR, and snapshot storage":
            wanted = "lakebase_project"
        elif component == "Lakeflow Connect managed sync compute":
            wanted = "database_table_sync_pipeline"
        elif component == "Databricks SQL warehouse query compute":
            wanted = "sql_warehouse"
        elif component == "Lakebase native CDF change-feed processing":
            wanted = "lakebase_cdf_config"
        elif component == "Delta live-orders table storage":
            wanted = "unity_catalog_table"
        elif component.startswith("Aurora "):
            wanted = "aurora_cluster"
        elif component.startswith("RDS PostgreSQL "):
            wanted = "rds_postgres_instance"
        elif component == "Database public IPv4":
            wanted = (
                "aurora_cluster"
                if competitor_id == CompetitorId.AURORA_SERVERLESS_V2
                else "rds_postgres_instance"
            )
        if wanted is None:
            return None
        matches = tuple(resource for resource in resources if resource.resource_type == wanted)
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def _cost_decimal(value: object) -> Decimal | None:
        if value is None:
            return None
        return Decimal(str(value))

    async def _open_cost_bout(
        self,
        record: SessionRecord,
        *,
        kind: str,
        started_at: datetime,
    ) -> str | None:
        store = self._cost_ledger_store
        manifest = self._cost_manifest
        if store is None or manifest is None:
            return None
        if record.cost_bout_id is not None:
            raise InvalidStateError("The current cost bout window is already open")
        bout_id = uuid4().hex
        identity = capture_bout_cost_identity(
            manifest,
            round_id=record.snapshot.round.id,
            bout_id=bout_id,
            session_id=record.snapshot.id,
            window_start=started_at,
            # Resource identity capture is independent of the eventual close. This
            # minimal positive interval validates the UTC start without guessing an end.
            window_end=started_at + timedelta(microseconds=1),
        )
        if identity.installation_id != self._installation_id:
            raise InvalidStateError("The sealed cost identity does not match this installation")
        receipt = record.snapshot.cost_receipt
        if receipt is None:
            raise InvalidStateError("The original cost receipt is unavailable")

        estimates: list[CostEstimate] = []
        ambiguous = bool(identity.quarantine)
        for ordinal, line in enumerate(receipt.lines):
            resource = self._cost_resource_for_line(
                line,
                identity.resources,
                record.snapshot.competitor.id,
            )
            if resource is None:
                if line.reconciliation_status != "selection_required":
                    line.reconciliation_status = "attribution_ambiguous"
                    ambiguous = True
                continue
            fingerprint_material = "\0".join(
                (
                    resource.provider,
                    resource.resource_type,
                    line.component,
                    line.unit,
                    str(line.unit_rate_usd),
                    str(line.reference_list_unit_rate_usd),
                    line.rate_basis,
                )
            )
            fingerprint = hashlib.sha256(fingerprint_material.encode()).hexdigest()
            ledger_material = "\0".join(
                (
                    self._installation_id,
                    bout_id,
                    str(ordinal),
                    resource.provider,
                    resource.resource_type,
                    resource.resource_id,
                    line.component,
                )
            )
            estimates.append(
                CostEstimate(
                    ledger_id=hashlib.sha256(ledger_material.encode()).hexdigest(),
                    installation_id=self._installation_id,
                    bout_id=bout_id,
                    session_id=record.snapshot.id,
                    round_id=record.snapshot.round.id.value,
                    lane_id=line.lane_id,
                    resource_id=resource.resource_id,
                    resource_type=resource.resource_type,
                    resource_name=resource.resource_name,
                    resource_arn=resource.resource_arn,
                    scope=line.scope,
                    key=CalibrationKey(
                        provider=resource.provider,
                        region=receipt.region,
                        component=line.component,
                        attribution_method="exact_sealed_resource_interval",
                        configuration_fingerprint=fingerprint,
                        unit=line.unit,
                    ),
                    original_quantity=self._cost_decimal(line.quantity),
                    original_unit_rate_usd=self._cost_decimal(line.unit_rate_usd),
                    original_cost_usd=self._cost_decimal(
                        line.original_estimate_usd
                        if line.original_estimate_usd is not None
                        else line.subtotal_usd
                    ),
                    window_start=started_at,
                    created_at=started_at,
                )
            )
        if not estimates:
            raise InvalidStateError("No exact sealed cost resource can be attributed to this bout")
        if ambiguous:
            receipt.reconciliation_status = "attribution_ambiguous"
        await store.record_estimates(tuple(estimates))
        record.cost_bout_id = bout_id
        record.cost_bout_started_at = started_at
        record.cost_bout_kind = kind
        return bout_id

    async def _close_cost_bout(
        self,
        record: SessionRecord,
        *,
        bout_id: str,
        ended_at: datetime,
        outcome: str,
    ) -> None:
        store = self._cost_ledger_store
        if store is None or record.cost_bout_id != bout_id:
            return
        await store.close_bout(
            installation_id=self._installation_id,
            bout_id=bout_id,
            window_end=ended_at,
            terminal_outcome=outcome,
        )
        record.cost_bout_id = None
        record.cost_bout_started_at = None
        record.cost_bout_kind = None

    async def _run_cost_bout(
        self,
        record: SessionRecord,
        bout_id: str | None,
        operation: Awaitable[None],
    ) -> None:
        try:
            await operation
        finally:
            if bout_id is not None and record.cost_bout_id == bout_id:
                terminal_states = {
                    SessionState.VERIFIED,
                    SessionState.FAILED,
                    SessionState.TOWELLED,
                }
                state = record.snapshot.state
                outcome = state.value if state in terminal_states else "cancelled"
                if record.cost_bout_kind == "redo" and record.snapshot.redo is not None:
                    outcome = f"redo_{record.snapshot.redo.state.value}"
                ended_at = record.snapshot.updated_at
                if ended_at <= (record.cost_bout_started_at or ended_at):
                    ended_at = datetime.now(UTC)
                try:
                    await self._close_cost_bout(
                        record,
                        bout_id=bout_id,
                        ended_at=ended_at,
                        outcome=outcome,
                    )
                except Exception as exc:
                    # The bout's own outcome outranks its tidy-up, and this is a
                    # `finally`: an exception raised here does not accompany the
                    # round's own failure, it REPLACES it. This repo already has a
                    # bout that reported "verified" while its settlement failed
                    # four times on a missing grant, and a ledger row is not worth
                    # either losing the reason a round failed or the leak below.
                    logger.error(
                        "Bout cost window could not be closed session=%s outcome=%s diagnostic=%s",
                        record.snapshot.id,
                        outcome,
                        _redacted_exception_chain(exc),
                    )
                    if record.snapshot.towel is None:
                        # Only the towel path has a retry: `_settle_towel_cost_window`
                        # runs on every cleanup attempt and clears the pointer itself
                        # if it finally gives up. With no towel this was the last
                        # close, so holding the pointer would pin the record forever
                        # against `_releasable`'s open-cost-bout check -- a memory
                        # leak reached through a won bout. The row goes to
                        # reconciliation instead, which is where that function
                        # already sends one for the same reason.
                        record.cost_bout_id = None
                        record.cost_bout_started_at = None
                        record.cost_bout_kind = None

    def _require_open(self) -> None:
        if self._closed:
            raise InvalidStateError("The demo server is restarting; refresh in a moment")

    async def close(self) -> None:
        """Settle active work before the coordinator connection is closed.

        Only the record holding the exact current fence may clean or release the
        ring. If task settlement or cleanup cannot be proved, the durable lease
        is deliberately left to expire so startup reconciliation runs before the
        next bout.

        All eight tasks in :meth:`_unfinished_operations` are accounted for here,
        in three different ways, and the difference is deliberate rather than an
        oversight. ``armed_expiry_task`` goes first, on its own, because it is a
        timer and cancelling it settles nothing. The four in the wait set below are
        cancelled and waited on, because whether they finished decides whether this
        record may clean and release its ring. The two heartbeats are cancelled
        further down, after the release attempts and *before* the scoped stores are
        closed -- they must outlive the release they are keeping alive, and must not
        outlive the store they beat against. ``model_score_terminal_task`` is
        cancelled by none of them: it is shielded in
        :meth:`_await_model_score_terminal` because it holds the ring lease and the
        run-owned proof row, and the cancellation of the parent turns into a bounded
        wait and then a logged abandonment. It never releases, so a closing store
        cannot surprise it.
        """

        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            async with self._records_lock:
                records = tuple(self._records.values())

            settled: dict[str, bool] = {}
            for record in records:
                self._cancel_armed_expiry(record)
                operations = {
                    operation
                    for operation in (
                        record.task,
                        record.cooldown_task,
                        record.connection_spike_cleanup_task,
                        record.settlement_task,
                    )
                    if operation is not None
                    and operation is not asyncio.current_task()
                    and not operation.done()
                }
                for operation in operations:
                    operation.cancel()
                if not operations:
                    settled[record.snapshot.id] = True
                    continue
                done, pending = await asyncio.wait(
                    operations,
                    timeout=max(0.001, self._shutdown_settle_timeout),
                )
                for operation in done:
                    try:
                        operation.result()
                    except (asyncio.CancelledError, Exception):
                        pass
                settled[record.snapshot.id] = not pending

            cleanup_results: dict[str, bool] = {}
            for record in records:
                local_lease = record.lease
                if local_lease is None:
                    continue
                try:
                    current = await self._lease_store_for_record(record).current()
                except Exception as exc:
                    logger.error(
                        "Shutdown could not verify the durable ring lease: %s",
                        type(exc).__name__,
                    )
                    continue
                if current is None or not self._same_exact_lease(local_lease, current):
                    continue
                if not settled.get(record.snapshot.id, False):
                    continue
                cleanup_ok = await self._cleanup_for_shutdown(record)
                cleanup_results[record.snapshot.id] = cleanup_ok
                if cleanup_ok and not await self._release_bout(record):
                    logger.error("Shutdown could not confirm release of the exact ring lease")

            try:
                current_round5 = await self._round5_cleanup_store().current()
            except Exception as exc:
                logger.error(
                    "Shutdown could not verify the durable Round 5 lease: %s",
                    type(exc).__name__,
                )
                current_round5 = None
            round5_record = next(
                (
                    record
                    for record in records
                    if record.round5_lease is not None
                    and current_round5 is not None
                    and self._same_exact_lease(record.round5_lease, current_round5)
                ),
                None,
            )
            if round5_record is not None and settled.get(round5_record.snapshot.id, False):
                cleanup_ok = cleanup_results.get(
                    round5_record.snapshot.id,
                    False,
                )
                if round5_record.snapshot.id not in cleanup_results:
                    cleanup_ok = await self._cleanup_for_shutdown(round5_record)
                if cleanup_ok:
                    await self._release_bout(round5_record)
                    if not await self._release_round5_lease(round5_record):
                        logger.error(
                            "Shutdown could not confirm release of the exact Round 5 lease"
                        )

            # The ninth task, and the one that is owned by no record at all.
            # A settled Round 4 bout leaves a delayed pipeline release keyed by
            # pipeline in `model_score_live._ACTIVATIONS`, so none of the
            # per-record accounting above can see it -- and cancelling it
            # silently, which is what happened before this line, threw away a
            # stop on a pipeline that bills $14.57/day while it is up, with
            # nothing anywhere recording that one was owed.
            await self._release_pipelines_for_shutdown()

            for record in records:
                # A failed cleanup stays fenced only until its durable TTL. No
                # process-local heartbeat may survive manager shutdown.
                self._cancel_lease_heartbeat(record)
                self._cancel_round5_lease_heartbeat(record)

            for store in self._scoped_lease_stores.values():
                try:
                    await store.close()
                except Exception as exc:
                    logger.error(
                        "Shutdown could not close a scoped coordinator: %s",
                        type(exc).__name__,
                    )

    @staticmethod
    async def _release_pipelines_for_shutdown() -> None:
        """Perform any Round 4 pipeline stop this process still owes.

        Imported here rather than at module scope for the reason every other
        live engine is: `model_score_live` pulls in the Databricks SDK and
        `psycopg`, and `RunManager` is constructed in tests and on checkouts
        that have neither. A process with no live Round 4 has nothing to
        discharge, so an import that cannot succeed is the same no-op as an
        empty registry.
        """

        try:
            from .model_score_live import aclose_activations
        except Exception:
            return
        await aclose_activations()

    async def _cleanup_for_shutdown(self, record: SessionRecord) -> bool:
        async def cleanup() -> bool:
            round_id = record.snapshot.round.id
            if round_id == RoundId.SURVIVE_CONNECTION_SPIKE:
                if record.connection_spike_engine is None:
                    return False
                return await self._cleanup_connection_spike(record)
            if round_id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
                engine = record.safe_change_engine
                if engine is None:
                    return False
                await engine.reset(record.snapshot.competitor.id)
            elif round_id == RoundId.RECOVER_DELETED_ORDER:
                engine = record.recovery_engine
                if engine is None:
                    return False
                await engine.settle_pending_mutations(record.snapshot.competitor.id)
                await engine.reset(record.snapshot.competitor.id)
            return True

        try:
            async with asyncio.timeout(max(0.001, self._shutdown_cleanup_timeout)):
                return await cleanup()
        except (asyncio.CancelledError, Exception) as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise
            logger.error(
                "Shutdown cleanup was not verified for round=%s: %s",
                record.snapshot.round.id,
                type(exc).__name__,
            )
            return False

    @staticmethod
    def _build_safe_change_engine() -> SafeChangeEngine:
        from .safe_change_live import build_safe_change_engine

        return build_safe_change_engine()

    @staticmethod
    def _build_recovery_engine() -> RecoveryEngine:
        from .recovery_live import build_recovery_engine

        return build_recovery_engine()

    async def create(self, request: SessionCreate) -> SessionSnapshot:
        self._require_open()
        self._readiness_check()
        primary = persona_by_id(request.primary_persona)
        secondary = [persona_by_id(persona_id) for persona_id in request.secondary_personas]
        if request.round_id is None:
            selected_round, reason = recommend_round(
                request.competitor,
                primary,
                model_score_available=self.model_score_available,
                connection_spike_available=self.connection_spike_available,
                live_orders_available=self.live_orders_available,
            )
        else:
            selected_round = round_by_id(
                request.round_id,
                model_score_available=self.model_score_available,
                connection_spike_available=self.connection_spike_available,
                live_orders_available=self.live_orders_available,
            )
            if request.competitor not in selected_round.competitors:
                raise ValueError("The selected round does not support this competitor")
            reason = "Explicitly selected by the operator before arming."

        competitor = competitor_by_id(request.competitor)
        now = datetime.now(UTC)
        session_id = uuid4().hex
        lanes = {
            "lakebase": LaneSnapshot(id="lakebase", name="Lakebase"),
            "competitor": LaneSnapshot(
                id="competitor",
                name=(
                    f"{competitor.short_name} + RDS Proxy"
                    if selected_round.id == RoundId.SURVIVE_CONNECTION_SPIKE
                    else competitor.short_name
                ),
            ),
        }
        round5_setup = (
            RoundFiveSetupSnapshot(
                lanes={
                    lane_id: RoundFiveSetupLaneSnapshot(id=lane_id, name=lane.name)
                    for lane_id, lane in lanes.items()
                }
            )
            if selected_round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            else None
        )
        capacity = build_capacity_disclosure(selected_round.id, request.competitor)
        snapshot = SessionSnapshot(
            id=session_id,
            state=SessionState.DRAFT,
            created_at=now,
            updated_at=now,
            competitor=competitor,
            primary_persona=primary,
            secondary_personas=secondary,
            corners=request.corners,
            round=selected_round,
            recommendation_reason=reason,
            presenter_pack=build_presenter_pack(
                primary,
                secondary,
                selected_round,
                request.corners,
                request.competitor,
            ),
            lanes=lanes,
            capacity=capacity,
            descent_cost=build_descent_cost_disclosure(
                selected_round.id, request.competitor
            ),
            # Round-agnostic too, and rebuilt on every read rather than pinned
            # here: see _standing_cost_disclosure. Nothing has been read back
            # from AWS at session-create, so the RDS cost lane will label its
            # figure configured-only rather than claim a match.
            standing_cost=self._standing_cost_disclosure(capacity),
            # Round-agnostic on purpose: the claim it carries is a comparison
            # between rounds, so every round shows the whole measured table.
            bout_cost=build_bout_cost_disclosure(),
            cost_receipt=build_cost_receipt(selected_round.id, request.competitor),
            round5_setup=round5_setup,
            metric_specs=[item.model_copy(deep=True) for item in selected_round.metric_specs],
        )
        record = SessionRecord(snapshot=snapshot, last_active_ns=time.monotonic_ns())
        async with self._records_lock:
            self._records[session_id] = record
            self._release_settled_records(record.last_active_ns, keep=session_id)
        await record.event_log.publish(
            "session_created", {"session": snapshot.model_dump(mode="json")}
        )
        return snapshot.model_copy(deep=True)

    def _round_five_elapsed_floors(
        self,
        record: SessionRecord,
        *,
        as_of_ns: int,
    ) -> dict[str, float]:
        """Project each setup latch to one lane-owned monotonic observation.

        ``setup_elapsed_ms`` is deliberately a callback latch. It is exact when
        the setup orchestrator reports, but AWS can then stay silent for minutes
        while a Proxy target becomes available. The manager cannot recover the
        orchestrator's private T0, so the safe floor is the published elapsed
        plus the interval on *this same measurement clock* since the manager
        observed it.

        This is the only arithmetic used both for public running snapshots and
        the towel cutoff. A browser therefore restarts from the same floor that
        would become authoritative if the operator stopped the bout at that
        instant, without receiving either process-local monotonic values or
        wall-clock anchors.
        """

        setup = record.snapshot.round5_setup
        if setup is None:
            return {}
        floors: dict[str, float] = {}
        for lane_id, lane in setup.lanes.items():
            published_ms = lane.setup_elapsed_ms
            if published_ms is None:
                continue
            floor_ms = published_ms
            observed_ns = record.round5_progress_observed_ns.get(lane_id)
            if (
                record.snapshot.state == SessionState.RUNNING
                and lane.state == RoundFiveSetupState.RUNNING
                and observed_ns is not None
            ):
                floor_ms += max(0.0, (as_of_ns - observed_ns) / 1_000_000)
            floors[lane_id] = floor_ms
        return floors

    def _public_snapshot_locked(
        self,
        record: SessionRecord,
        *,
        as_of_ns: int | None = None,
    ) -> SessionSnapshot:
        """Copy a snapshot and stamp server-derived active timer floors.

        Normal timed rounds share the same monotonic origin used by towel
        adjudication. Round 5 has lane-owned setup clocks, so it keeps its
        callback-latch projection below. Neither path exposes process-local
        timestamps to the browser.
        """

        snapshot = record.snapshot.model_copy(deep=True)
        active_lane_ids = [
            lane_id
            for lane_id, lane in record.snapshot.lanes.items()
            if lane.state in {LaneState.CONNECTING, LaneState.VERIFYING}
        ]
        started_ns = record.run_started_monotonic_ns
        if (
            record.recovery_stop_control is not None
            and record.recovery_stop_control.started_ns is not None
        ):
            started_ns = record.recovery_stop_control.started_ns
        needs_lane_projection = (
            record.snapshot.state == SessionState.RUNNING
            and started_ns is not None
            and bool(active_lane_ids)
        )
        setup = snapshot.round5_setup
        needs_round_five_projection = (
            setup is not None
            and record.snapshot.state == SessionState.RUNNING
            and any(
            lane.state == RoundFiveSetupState.RUNNING
            and lane.setup_elapsed_ms is not None
            and lane_id in record.round5_progress_observed_ns
            for lane_id, lane in record.snapshot.round5_setup.lanes.items()
            )
        )
        measured_at_ns = (
            self._clock_ns()
            if as_of_ns is None and (needs_lane_projection or needs_round_five_projection)
            else as_of_ns if as_of_ns is not None else 0
        )

        if needs_lane_projection:
            assert started_ns is not None
            bout_floor_ms = max(0.0, (measured_at_ns - started_ns) / 1_000_000)
            for lane_id in active_lane_ids:
                lane = snapshot.lanes[lane_id]
                lane.elapsed_at_snapshot_ms = max(
                    lane.elapsed_ms if lane.elapsed_ms is not None else 0.0,
                    bout_floor_ms,
                )

        if setup is not None:
            floors = self._round_five_elapsed_floors(record, as_of_ns=measured_at_ns)
            for lane_id, lane in setup.lanes.items():
                lane.elapsed_at_snapshot_ms = floors.get(lane_id)
        return snapshot

    async def get(self, session_id: str) -> SessionSnapshot:
        record = await self._record(session_id)
        async with record.lock:
            record.snapshot.standing_cost = self._standing_cost_disclosure(
                record.snapshot.capacity
            )
            return self._public_snapshot_locked(record)

    async def cancel_arm(
        self,
        session_id: str,
        operator: BoutOperator,
    ) -> SessionSnapshot:
        """Cancel a Round 1 start-state check before any timed work begins."""
        self._require_open()
        record = await self._record(session_id)
        async with record.lock:
            if (
                record.snapshot.state == SessionState.FAILED
                and record.snapshot.failure is not None
                and record.snapshot.failure.startswith("Fight-card check cancelled")
            ):
                self._assert_operator(record.operator, operator)
                return record.snapshot.model_copy(deep=True)
            if record.snapshot.round.id != RoundId.WAKE_IDLE_APP:
                raise InvalidStateError("Only a Round 1 start-state check can be cancelled")
            if record.snapshot.state != SessionState.CHECKING:
                raise InvalidStateError("Only a fight card that is still checking can be cancelled")
            if record.snapshot.run_started_at is not None:
                raise InvalidStateError("A bout cannot be cancelled after the run has started")
            async with record.lease_lock:
                expected_lease = record.lease
                if (
                    expected_lease is None
                    or expected_lease.session_id != record.snapshot.id
                    or expected_lease.phase != "checking"
                ):
                    raise InvalidStateError("The checking ring lease is no longer available")
                self._assert_operator(expected_lease.operator, operator)
            arm_task = record.task
            if arm_task is not None and not arm_task.done():
                arm_task.cancel()

        if arm_task is not None:
            try:
                await arm_task
            except asyncio.CancelledError:
                pass

        message = (
            "Fight-card check cancelled by the ring owner. "
            "No run started and no result was recorded."
        )
        async with record.lock:
            if record.snapshot.state != SessionState.CHECKING:
                raise InvalidStateError("The fight card finished checking before cancellation")
            async with record.lease_lock:
                current_lease = record.lease
                if current_lease is None or not self._same_exact_lease(
                    current_lease, expected_lease
                ):
                    raise InvalidStateError("The checking ring lease changed before cancellation")
            if not await self._release_bout(record):
                raise InvalidStateError(
                    "Ring release could not be confirmed; cancellation is still pending"
                )
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            record.snapshot.remembered_result = None
            record.snapshot.metrics = []
            record.snapshot.comparison = None
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            if record.task is arm_task:
                record.task = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_cancelled",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )
        return snapshot

    async def start_arm(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        self._require_open()
        self._readiness_check()
        record = await self._record(session_id)
        async with record.lock:
            bout_operator = operator or BoutOperator(display_name="Local operator")
            if record.snapshot.state == SessionState.ARMED:
                self._assert_operator(record.operator, bout_operator)
                return record.snapshot.model_copy(deep=True)
            if (
                record.snapshot.state == SessionState.CHECKING
                and record.task is not None
                and not record.task.done()
                and record.task.get_name() == f"arm-{session_id}"
            ):
                self._assert_operator(record.operator, bout_operator)
                return record.snapshot.model_copy(deep=True)
            if record.task and not record.task.done():
                raise InvalidStateError("A session operation is already running")
            is_model_score = record.snapshot.round.id == RoundId.PUT_MODEL_SCORE_IN_APP
            is_connection_spike = record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            is_live_orders = record.snapshot.round.id == RoundId.ANALYZE_LIVE_ORDERS
            if is_connection_spike:
                self._round5_readiness_check()
            if is_model_score and self._model_score_factory is None:
                raise InvalidStateError("Round 4 live adapter is not configured.")
            if is_connection_spike and self._connection_spike_factory is None:
                raise InvalidStateError("Round 5 live adapter is not configured.")
            if is_live_orders and self._live_orders_factory is None:
                raise InvalidStateError("Round 6 native CDF adapter is not configured.")
            if record.snapshot.round.availability != Availability.READY:
                raise InvalidStateError(
                    "This round is not executable in the current vertical slice"
                )
            if record.snapshot.state not in {SessionState.DRAFT, SessionState.FAILED}:
                raise InvalidStateError(f"Cannot arm a session in state {record.snapshot.state}")
            if is_connection_spike and record.snapshot.state == SessionState.FAILED:
                raise InvalidStateError("Round 5 requires a clean baseline and a new bout")
            await self._claim_bout(record, bout_operator)
            if is_connection_spike and self._round5_prearm_guard is not None:
                async with record.lease_lock:
                    authority = record.round5_lease
                try:
                    if authority is None or authority.phase != "checking":
                        raise InvalidStateError("The Round 5 artifact lease is unavailable")
                    async with asyncio.timeout(max(0.001, self._round5_prearm_timeout)):
                        await self._round5_prearm_guard(
                            record.snapshot.id,
                            authority.fencing_token,
                        )
                except Exception as exc:
                    await self._release_round5_lease(record)
                    await self._release_bout(record)
                    if isinstance(exc, InvalidStateError):
                        raise
                    raise InvalidStateError(
                        "ROUND 5 BACKSTAGE CLEANUP IS STILL FINISHING · OTHER ROUNDS ARE READY"
                    ) from exc
            record.operator = bout_operator
            self._reset_outcome(record.snapshot)
            record.armed_at_monotonic = None
            record.safe_change_arm = None
            record.recovery_arm = None
            record.recovery_stop_control = None
            record.recovery_outcome = None
            record.model_score_engine = None
            record.model_score_arm = None
            record.model_score_result = None
            record.model_score_pending_update = None
            record.model_score_terminal_published = False
            record.model_score_terminal_task = None
            record.model_score_terminal_lease = None
            record.connection_spike_engine = None
            record.connection_spike_arm = None
            record.connection_spike_setup_result = None
            record.connection_spike_cleanup_task = None
            record.live_orders_engine = None
            record.live_orders_arm = None
            record.live_orders_result = None
            record.live_orders_pending_order = None
            record.live_orders_guardrail_order = None
            record.run_started_monotonic_ns = None
            record.towel_stop_event = None
            if is_model_score or is_live_orders:
                competitor = record.snapshot.lanes["competitor"]
                competitor.state = LaneState.NOT_SUPPORTED
                competitor.status = (
                    "AWS lane not timed for this native CDF proof"
                    if is_live_orders
                    else "AWS lane not timed for this Managed Sync proof"
                )
                competitor.activity = LaneActivity(phase="not_supported")
                competitor.evidence = {
                    "unsupported_reason": (
                        "Aurora/RDS require a separately configured CDC pipeline into Delta."
                        if is_live_orders
                        else _ROUND_FOUR_UNSUPPORTED_REASON
                    )
                }
            record.snapshot.state = SessionState.CHECKING
            record.snapshot.updated_at = datetime.now(UTC)
            try:
                if record.snapshot.round.id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
                    record.live_targets = None
                    record.safe_change_engine = None
                    operation = self._arm_safe_change(record)
                elif record.snapshot.round.id == RoundId.RECOVER_DELETED_ORDER:
                    record.live_targets = None
                    record.recovery_engine = None
                    operation = self._arm_recovery(record)
                elif is_model_score:
                    record.live_targets = None
                    operation = self._arm_model_score(record)
                elif is_connection_spike:
                    record.live_targets = None
                    operation = self._arm_connection_spike(record)
                elif is_live_orders:
                    record.live_targets = None
                    operation = self._arm_live_orders(record)
                else:
                    record.live_targets = self._resolver.resolve(record.snapshot.competitor.id)
                    record.safe_change_engine = None
                    operation = self._arm(record)
            except Exception:
                if is_connection_spike:
                    await self._release_round5_lease(record)
                await self._release_bout(record)
                raise
            record.task = asyncio.create_task(operation, name=f"arm-{session_id}")
            result = record.snapshot.model_copy(deep=True)
        return result

    async def start_run(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        self._require_open()
        self._readiness_check()
        record = await self._record(session_id)
        async with record.lock:
            effective_operator = operator or record.operator
            if (
                record.task is not None
                and not record.task.done()
                and record.task.get_name() == f"run-{session_id}"
            ):
                self._assert_operator(record.operator, effective_operator)
                return record.snapshot.model_copy(deep=True)
            if record.snapshot.run_started_at is not None and record.snapshot.state in {
                SessionState.RUNNING,
                SessionState.VERIFIED,
                SessionState.FAILED,
                SessionState.TOWELLED,
            }:
                self._assert_operator(record.operator, effective_operator)
                return record.snapshot.model_copy(deep=True)
            if record.task and not record.task.done():
                raise InvalidStateError("A session operation is already running")
            if record.snapshot.state != SessionState.ARMED:
                raise InvalidStateError("The session must be armed before the bell can ring")
            is_safe_change = record.snapshot.round.id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY
            is_recovery = record.snapshot.round.id == RoundId.RECOVER_DELETED_ORDER
            is_model_score = record.snapshot.round.id == RoundId.PUT_MODEL_SCORE_IN_APP
            is_connection_spike = record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            is_live_orders = record.snapshot.round.id == RoundId.ANALYZE_LIVE_ORDERS
            if is_safe_change and (
                record.safe_change_engine is None or record.safe_change_arm is None
            ):
                raise InvalidStateError("The isolated-change proof must be armed again")
            if is_recovery and (record.recovery_engine is None or record.recovery_arm is None):
                raise InvalidStateError("The point-in-time recovery proof must be armed again")
            if is_model_score and (
                record.model_score_engine is None or record.model_score_arm is None
            ):
                raise InvalidStateError("The Managed Sync proof must be armed again")
            if is_connection_spike and (
                record.connection_spike_engine is None
                or (
                    record.connection_spike_arm is None
                    and not self._round_five_has_timed_setup(record.connection_spike_engine)
                )
            ):
                raise InvalidStateError("The connection-spike proof must be armed again")
            if is_live_orders and (
                record.live_orders_engine is None or record.live_orders_arm is None
            ):
                raise InvalidStateError("The native CDF proof must be armed again")
            if (
                not is_safe_change
                and not is_recovery
                and not is_model_score
                and not is_connection_spike
                and not is_live_orders
                and record.live_targets is None
            ):
                raise InvalidStateError("The live targets are unavailable")
            await self._commit_bout(record, operator or record.operator)
            cost_started_at = datetime.now(UTC)
            try:
                cost_bout_id = await self._open_cost_bout(
                    record,
                    kind="run",
                    started_at=cost_started_at,
                )
            except Exception as exc:
                if is_connection_spike:
                    await self._release_round5_lease(record)
                await self._release_bout(record)
                raise InvalidStateError(
                    cost_window_refusal(
                        "The bout cost window could not be opened before the bell.",
                        exc,
                    )
                ) from exc
            self._cancel_armed_expiry(record)
            record.towel_stop_event = asyncio.Event()
            if is_recovery:
                record.recovery_stop_control = RecoveryStopControl()
                record.recovery_outcome = None
            if is_model_score:
                contract = record.model_score_engine.contract
                record.model_score_terminal_published = False
                record.model_score_pending_update = ModelScoreUpdate(
                    entity_id=contract.entity_id,
                    score=0.81,
                    model_version="risk-v1",
                    proof_nonce=f"round4-v1-{uuid4().hex}",
                )
            if is_live_orders:
                record.live_orders_pending_order = LiveOrder(
                    order_id=str(uuid4()),
                    sku="RED-GLOVE",
                    store="CHICAGO",
                    quantity=1,
                    total_cents=8450,
                    status="paid",
                    proof_nonce=f"round6-{uuid4().hex}",
                )
            operation = (
                self._run_safe_change(record)
                if is_safe_change
                else self._run_recovery(record, record.recovery_stop_control)
                if is_recovery
                else self._run_model_score(record)
                if is_model_score
                else self._run_connection_spike(record)
                if is_connection_spike
                else self._run_live_orders(record)
                if is_live_orders
                else self._run(record)
            )
            record.task = asyncio.create_task(
                self._run_cost_bout(record, cost_bout_id, operation),
                name=f"run-{session_id}",
            )
            result = record.snapshot.model_copy(deep=True)
        return result

    async def start_redo(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        self._require_open()
        record = await self._record(session_id)
        async with record.lock:
            if record.snapshot.round.id != RoundId.PUT_MODEL_SCORE_IN_APP:
                raise InvalidStateError("The score-change re-do is available only in Round 4")
            effective_operator = operator or record.operator
            if effective_operator is None:
                raise InvalidStateError("ONLY THE RING OWNER CAN CONTROL THIS BOUT")
            self._assert_operator(record.operator, effective_operator)
            redo = record.snapshot.redo
            if redo is not None and redo.state in {
                RedoState.RUNNING,
                RedoState.VERIFIED,
                RedoState.FAILED,
            }:
                return record.snapshot.model_copy(deep=True)
            if self._model_score_factory is None:
                raise InvalidStateError("Round 4 live adapter is not configured.")
            self._readiness_check()
            if record.snapshot.state != SessionState.VERIFIED or redo is None:
                raise InvalidStateError("Round 4 must verify before changing the score again")
            if redo.state != RedoState.READY:
                raise InvalidStateError("The Round 4 re-do is not ready")
            if record.task and not record.task.done():
                raise InvalidStateError("A session operation is already running")
            engine = record.model_score_engine
            arm = record.model_score_arm
            result = record.model_score_result
            if engine is None or arm is None or result is None:
                raise InvalidStateError("The exact issued Managed Sync proof is unavailable")

            await self._claim_bout(
                record,
                effective_operator,
                phase="redo_committed",
            )
            started_at = datetime.now(UTC)
            try:
                cost_bout_id = await self._open_cost_bout(
                    record,
                    kind="redo",
                    started_at=started_at,
                )
            except Exception as exc:
                await self._release_bout(record)
                raise InvalidStateError(
                    cost_window_refusal(
                        "The re-do cost window could not be opened.",
                        exc,
                    )
                ) from exc
            update = ModelScoreUpdate(
                entity_id=engine.contract.entity_id,
                score=0.33,
                model_version="risk-v2",
                proof_nonce=f"round4-v2-{uuid4().hex}",
            )
            record.model_score_pending_update = update
            record.model_score_terminal_published = False
            record.snapshot.state = SessionState.RUNNING
            record.snapshot.updated_at = started_at
            redo.state = RedoState.RUNNING
            redo.started_at = started_at
            redo.completed_at = None
            redo.failure = None
            lane = redo.lanes["lakebase"]
            lane.state = LaneState.CONNECTING
            lane.status = "Committing the distinct v2 model score update"
            lane.activity = LaneActivity(phase=ModelScorePhase.COMMITTING_SOURCE)
            snapshot = record.snapshot.model_copy(deep=True)
            await record.event_log.publish(
                "redo_started",
                {"session": snapshot.model_dump(mode="json")},
            )
            record.task = asyncio.create_task(
                self._run_cost_bout(
                    record,
                    cost_bout_id,
                    self._redo_model_score(record, engine, arm, result, update),
                ),
                name=f"redo-{session_id}",
            )
            return snapshot

    async def retry_connection_spike_cleanup(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        self._require_open()
        record = await self._record(session_id)
        async with record.lock:
            if record.snapshot.round.id != RoundId.SURVIVE_CONNECTION_SPIKE:
                raise InvalidStateError("Retry Cleanup is available only in Round 5")
            effective_operator = operator or record.operator
            if effective_operator is None:
                raise InvalidStateError("ONLY THE RING OWNER CAN CONTROL THIS BOUT")
            self._assert_operator(record.operator, effective_operator)
            setup = record.snapshot.round5_setup
            if setup is None:
                raise InvalidStateError("Round 5 setup state is unavailable")
            if not setup.cleanup_retryable:
                if record.snapshot.state == SessionState.FAILED:
                    return record.snapshot.model_copy(deep=True)
                raise InvalidStateError("Round 5 cleanup does not require a retry")
            if (
                record.connection_spike_cleanup_task is not None
                and not record.connection_spike_cleanup_task.done()
            ):
                return record.snapshot.model_copy(deep=True)
            if record.task and not record.task.done():
                return record.snapshot.model_copy(deep=True)
            if self._connection_spike_factory is None:
                raise InvalidStateError("Round 5 live adapter is not configured.")
            await self._retain_connection_spike_cleanup_lease(
                record,
                effective_operator,
            )
            engine = record.connection_spike_engine
            if engine is None or getattr(engine, "reconcile_failed_cleanup", None) is None:
                engine = self._connection_spike_factory(record.snapshot.competitor.id)
                record.connection_spike_engine = engine
            record.task = asyncio.create_task(
                self._retry_connection_spike_cleanup(record, engine),
                name=f"retry-cleanup-{session_id}",
            )
            return record.snapshot.model_copy(deep=True)

    async def start_towel(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        self._require_open()
        # Deliberately not gated on `_readiness_check`. Arming and running ask
        # for new resource consumption and must wait for a green ring; the towel
        # is the emergency stop, and the bout it stops is burning money whether
        # backstage cleanup happens to be mid-flight or not. Refusing the stop
        # because the ring is busy recovering is the wrong way round.
        record = await self._record(session_id)
        async with record.lock:
            effective_operator = operator or record.operator
            self._assert_operator(record.operator, effective_operator)
            if effective_operator is None:
                raise InvalidStateError("ONLY THE RING OWNER CAN CONTROL THIS BOUT")

            towel = record.snapshot.towel
            if towel is not None:
                # `STOPPING` belongs here as much as `FAILED` does. A towel that
                # never got its cleanup task -- because something between the
                # durable state change and `create_task` raised -- sits at
                # `STOPPING` holding a heartbeating lease, and a guard that only
                # re-entered on `FAILED` answered the operator's retry click
                # with an unchanged 200 and no explanation.
                if towel.state in {TowelState.FAILED, TowelState.STOPPING} and (
                    record.cooldown_task is None or record.cooldown_task.done()
                ):
                    self._assert_towel_cleanup_lease(record, effective_operator)
                    towel.state = TowelState.CLEANING
                    towel.cleanup_failure = None
                    record.snapshot.updated_at = datetime.now(UTC)
                    snapshot = record.snapshot.model_copy(deep=True)
                    await record.event_log.publish(
                        "towel_update",
                        {"session": snapshot.model_dump(mode="json")},
                    )
                    record.cooldown_task = asyncio.create_task(
                        self._complete_towel(record, wait_for_run=False),
                        name=f"towel-cleanup-{session_id}",
                    )
                    return snapshot
                return record.snapshot.model_copy(deep=True)

            if record.snapshot.state != SessionState.RUNNING:
                raise InvalidStateError("The bout must be running before throwing in the towel")
            lease = record.lease
            if (
                lease is None
                or lease.session_id != record.snapshot.id
                or lease.phase != "run_committed"
            ):
                raise InvalidStateError("THE COMMITTED RING LEASE IS REQUIRED FOR A TOWEL")
            self._assert_operator(lease.operator, effective_operator)

            is_round_five = (
                record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            )
            round_five_setup = record.snapshot.round5_setup if is_round_five else None
            if is_round_five and round_five_setup is None:
                raise InvalidStateError("Round 5 setup state is unavailable")

            stop_control = record.recovery_stop_control
            cutoff_ns: int | None = None
            round_five_elapsed_at_cutoff_ms: dict[str, float] = {}
            if is_round_five:
                round_five_cutoff_ns = self._clock_ns()
                round_five_elapsed_at_cutoff_ms = self._round_five_elapsed_floors(
                    record,
                    as_of_ns=round_five_cutoff_ns,
                )
            else:
                started_ns = record.run_started_monotonic_ns
                if stop_control is not None and stop_control.started_ns is not None:
                    started_ns = stop_control.started_ns
                if started_ns is None:
                    raise InvalidStateError("The live bout clock has not started")
                cutoff_ns = self._clock_ns()
                cutoff_ms = max(0.0, (cutoff_ns - started_ns) / 1_000_000)

            # Fence every callback before changing durable lease phase. Any callback
            # already waiting on this lock will observe the new generation and no-op.
            record.towel_generation += 1

            # The durable lease phase moves first, and the bout is only killed
            # once it has. This order is the whole point: the transition can be
            # refused (an expired lease, a fence that moved), and killing the
            # bout before asking meant a refusal returned 409 "towel refused"
            # over an already-dead bout -- session still `RUNNING`, no towel
            # snapshot, and a latched stop event that only `start_arm` or
            # `start_run` can clear, neither of which is reachable from
            # `RUNNING`. Nothing above this line has touched the run.
            #
            # Nothing is lost by waiting: this whole body holds `record.lock`,
            # which is what actually serializes the towel against a natural
            # finish, and every finisher bails on a non-null towel snapshot.
            await self._transition_bout_to_towel(record, effective_operator)

            stop_event = record.towel_stop_event
            if stop_event is None:
                stop_event = asyncio.Event()
                record.towel_stop_event = stop_event
            stop_event.set()
            if stop_control is not None and cutoff_ns is not None:
                stop_control.request(cutoff_ns, active_lane="competitor")

            if is_round_five:
                assert round_five_setup is not None
                adjudication = adjudicate_round_five_towel(
                    lanes=record.snapshot.lanes,
                    setup_lanes=round_five_setup.lanes,
                    elapsed_at_cutoff_ms=round_five_elapsed_at_cutoff_ms,
                )
            else:
                normal_comparison = record.snapshot.comparison
                if normal_comparison is None and all(
                    lane.state == LaneState.VERIFIED
                    for lane in record.snapshot.lanes.values()
                ):
                    left = record.snapshot.lanes["lakebase"].elapsed_ms
                    right = record.snapshot.lanes["competitor"].elapsed_ms
                    if left is not None and right is not None:
                        delta = abs(left - right)
                        if delta < 5:
                            normal_comparison = ComparisonSnapshot(kind=ComparisonKind.TIE)
                        else:
                            metric_id = (
                                record.snapshot.metric_specs[0].id
                                if record.snapshot.metric_specs
                                else "elapsed_ms"
                            )
                            normal_comparison = ComparisonSnapshot(
                                kind=ComparisonKind.MEASURED,
                                winner_lane_id=(
                                    "lakebase" if left < right else "competitor"
                                ),
                                margin=MetricValue(
                                    spec_id=metric_id,
                                    value=delta,
                                    display_value=f"{delta:.2f} ms",
                                ),
                                detail=(
                                    "Both exact lane proofs verified before the towel cutoff."
                                ),
                            )
                adjudication = adjudicate_towel(
                    round_id=record.snapshot.round.id,
                    lanes=record.snapshot.lanes,
                    cutoff_ms=cutoff_ms,
                    normal_comparison=normal_comparison,
                )
            requested_at = datetime.now(UTC)
            restore_started = False
            competitor = record.snapshot.lanes["competitor"]
            if competitor.activity is not None:
                restore_started = competitor.activity.phase in {
                    RecoveryPhase.RESTORING,
                    RecoveryPhase.CONNECTING,
                    RecoveryPhase.VERIFYING_RECOVERED_ORDER,
                    RecoveryPhase.VERIFYING_SOURCE,
                }
            lakebase_verified_ms = (
                adjudication.lanes["lakebase"].elapsed_ms
                if adjudication.lanes["lakebase"].state == LaneState.VERIFIED
                else None
            )
            requested_towel = TowelSnapshot(
                state=TowelState.STOPPING,
                requested_at=requested_at,
                cutoff_ms=adjudication.cutoff_ms,
                censored_lower_bounds_ms=adjudication.censored_lower_bounds_ms,
                public_result=adjudication.public_result,
                active_lane=(
                    next(iter(adjudication.censored_lower_bounds_ms))
                    if len(adjudication.censored_lower_bounds_ms) == 1
                    else None
                ),
                lower_bound_ms=adjudication.cutoff_ms,
                lakebase_verified_ms=lakebase_verified_ms,
                restore_started=restore_started,
            )
            record.snapshot.towel = requested_towel
            record.snapshot.lanes = adjudication.lanes
            record.snapshot.comparison = adjudication.comparison
            record.snapshot.state = SessionState.TOWELLED
            record.snapshot.failure = None
            record.snapshot.remembered_result = adjudication.public_result
            if round_five_setup is not None:
                for lane in round_five_setup.lanes.values():
                    if lane.state != RoundFiveSetupState.VERIFIED:
                        lane.state = RoundFiveSetupState.TOWELLED
                        lane.verified = False
                        lane.status = "Toweled before the exact setup stop"
                        lane.error = None
                        # The receipt reads its Round 5 figures from here, not
                        # from the towel's censored bounds, so the corrected
                        # floor has to land here too. Leaving the progress latch
                        # in place is what put ">7.29s" on the screen for a lane
                        # that had been running 230 s.
                        bound_ms = adjudication.censored_lower_bounds_ms.get(lane.id)
                        if bound_ms is not None:
                            lane.setup_elapsed_ms = bound_ms
                round_five_setup.state = RoundFiveSetupState.TOWELLED
                round_five_setup.downstream_validated = False
                round_five_setup.failure = None
                round_five_setup.cleanup_retryable = False
            record.snapshot.updated_at = requested_at
            cost_bout_id = record.cost_bout_id
            if cost_bout_id is not None:
                # Never allowed to raise past here. This is a network write to
                # the coordination endpoint, and the condition that makes an
                # operator reach for the towel -- Lakebase slow or unreachable
                # -- is the same condition that makes it fail. Unguarded, it
                # skipped `create_task` below: AWS artifacts were never deleted,
                # the ring lease stayed held and heartbeating with no natural
                # expiry, and the open cost window kept the record unreleasable.
                # The ledger row is worth a lot less than any of those.
                try:
                    await self._close_cost_bout(
                        record,
                        bout_id=cost_bout_id,
                        ended_at=requested_at,
                        outcome=SessionState.TOWELLED.value,
                    )
                except Exception as exc:
                    logger.error(
                        "Towel could not close the cost window session=%s "
                        "diagnostic=%s; deferring to cleanup",
                        record.snapshot.id,
                        _redacted_exception_chain(exc),
                    )
                    requested_towel.cost_close_failure = (
                        str(exc) or "The bout cost window could not be closed"
                    )
            snapshot = record.snapshot.model_copy(deep=True)
            await record.event_log.publish(
                "towel_started",
                {"session": snapshot.model_dump(mode="json")},
            )
            record.cooldown_task = asyncio.create_task(
                self._complete_towel(record, wait_for_run=True),
                name=f"towel-cleanup-{session_id}",
            )
            return snapshot

    async def start_cooldown(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        self._require_open()
        self._readiness_check()
        record = await self._record(session_id)
        async with record.lock:
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                raise InvalidStateError("Round 5 has no cooldown or re-do")
            reset_mode = reset_mode_for_round(record.snapshot.round.id)
            is_safe_change = reset_mode == ResetMode.DELETE_ISOLATED_ENVIRONMENT
            is_recovery = reset_mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT
            owns_artifacts = is_safe_change or is_recovery
            allowed_states = (
                {SessionState.VERIFIED, SessionState.FAILED}
                if owns_artifacts
                else {SessionState.VERIFIED}
            )
            if record.snapshot.state not in allowed_states:
                raise InvalidStateError(
                    "A completed round is required before starting the re-do clock"
                )
            if is_safe_change:
                if record.safe_change_engine is None:
                    raise InvalidStateError("The isolated environments are unavailable for reset")
            elif is_recovery:
                if record.recovery_engine is None:
                    raise InvalidStateError("The recovery environments are unavailable for reset")
            elif record.live_targets is None:
                raise InvalidStateError("The live targets are unavailable for re-arming")
            effective_operator = (
                operator or record.operator or BoutOperator(display_name="Local operator")
            )
            self._assert_operator(record.operator, effective_operator)
            if record.cooldown_task is not None and not record.cooldown_task.done():
                return record.snapshot.model_copy(deep=True)
            if (
                record.snapshot.cooldown is None
                or record.snapshot.cooldown.state == CooldownState.FAILED
            ):
                await self._claim_bout(
                    record,
                    effective_operator,
                    phase="cooldown",
                    expected_previous_token=(
                        record.round2_fencing_token if owns_artifacts else None
                    ),
                )
                record.snapshot.cooldown = self._new_cooldown(record, reset_mode)
                started_at = record.snapshot.cooldown.started_at
                record.snapshot.updated_at = started_at
                reset_operation = (
                    self._reset_safe_change(record)
                    if is_safe_change
                    else self._reset_recovery(record)
                    if is_recovery
                    else self._monitor_cooldown(record)
                )
                record.cooldown_task = asyncio.create_task(
                    self._hold_bout_during_reset(record, reset_operation),
                    name=f"cooldown-{record.snapshot.id}",
                )
            return record.snapshot.model_copy(deep=True)

    @staticmethod
    def _new_cooldown(
        record: SessionRecord,
        reset_mode: ResetMode,
    ) -> CooldownSnapshot:
        started_at = datetime.now(UTC)
        record.cooldown_idle_observations.clear()
        owns_artifacts = reset_mode in {
            ResetMode.DELETE_ISOLATED_ENVIRONMENT,
            ResetMode.DELETE_RECOVERY_ENVIRONMENT,
        }
        is_recovery = reset_mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT
        return CooldownSnapshot(
            mode=reset_mode,
            started_at=started_at,
            lanes={
                lane_id: CooldownLaneSnapshot(
                    id=lane_id,
                    name=lane.name,
                    started_at=(
                        min(lane.connection_closed_at, started_at)
                        if (
                            reset_mode == ResetMode.RETURN_TO_IDLE
                            and lane.state == LaneState.VERIFIED
                            and lane.connection_closed_at is not None
                        )
                        else started_at
                    ),
                    status=(
                        "Deleting owned recovery environment"
                        if is_recovery
                        else "Deleting owned isolated environment"
                        if owns_artifacts
                        else "Watching for confirmed zero"
                    ),
                    activity=LaneActivity(
                        phase=("resetting" if owns_artifacts else "watching"),
                        wire_call=(
                            None
                            if owns_artifacts
                            else _round_one_cooldown_wire_call(
                                lane_id,
                                record.snapshot.competitor.id,
                            )
                        ),
                    ),
                )
                for lane_id, lane in record.snapshot.lanes.items()
            },
        )

    async def events(self, session_id: str, after_sequence: int = 0) -> AsyncIterator[RunEvent]:
        record = await self._record(session_id)
        async for event in record.event_log.stream(after_sequence):
            yield event

    @property
    def round_isolation(self) -> bool:
        """Whether each round owns its own ring.

        Exposed because it changes what an unscoped question means: with
        isolation on there is no single bout to report, so callers have to name
        a round or be told to.
        """
        return self._round_isolation

    async def bout_status(self, round_id: RoundId | None = None) -> BoutStatus:
        if self._round_isolation and round_id is None:
            # v7 gives every round its own ring and leaves the installation-wide
            # ring unused, so answering this from that ring reports idle no
            # matter what is running. Somebody polling it for readiness gets a
            # green light on a busy or broken installation, which is worse than
            # no answer at all -- so refuse, and say what to ask instead.
            raise AmbiguousRingQueryError(
                "ROUND REQUIRED: this installation gives every round its own ring, "
                "so there is no installation-wide bout to report. Ask per round -- "
                "/api/bout?round_id=<round> -- with one of: "
                f"{', '.join(round.value for round in RoundId)}. "
                "For liveness alone, use /api/health."
            )
        readiness = self._readiness_status() if self._readiness_status else None
        if (
            self._round_isolation
            and round_id == RoundId.SURVIVE_CONNECTION_SPIKE
            and (readiness is None or readiness.ring_ready)
            and self._round5_readiness_status is not None
        ):
            readiness = self._round5_readiness_status()
        readiness_fields = (
            {
                "ring_ready": bool(readiness.ring_ready),
                "maintenance_state": str(readiness.maintenance_state),
                "maintenance_detail": readiness.maintenance_detail,
            }
            if readiness is not None
            else {}
        )
        # During startup maintenance the gate's replica-local cache mirrors the
        # durable readiness row. Viewer polling must not fan out into one
        # coordination query per browser; only the bounded gate poller reads it.
        scoped_round = round_id if self._round_isolation else None
        status_scope = {
            "scope": "round" if scoped_round is not None else "global",
            "round_id": scoped_round,
        }
        store = (
            self._lease_store_for_round(scoped_round)
            if scoped_round is not None
            else self._lease_store
        )
        lease = await store.current()
        if scoped_round == RoundId.SURVIVE_CONNECTION_SPIKE and lease is None:
            lease = await self._round5_cleanup_store().current()
        if lease is not None:
            # A real lease outranks a replica-local readiness cache. This matters
            # most for Round 5: its reconciler deliberately marks the artifact
            # ring unready while a bout owns it, but the fight card must still say
            # BOUT IN PROGRESS rather than hiding another viewer's active bout
            # behind generic maintenance.
            return BoutStatus(
                **status_scope,
                **readiness_fields,
                active=True,
                can_start=False,
                operator=lease.operator,
                started_at=lease.started_at,
                updated_at=lease.updated_at,
                expires_at=lease.expires_at,
                phase=lease.phase,
                state=lease.session_state,
                round_title=lease.round_title,
                competitor=lease.competitor_name,
            )
        return BoutStatus(
            active=False,
            can_start=bool(readiness is None or readiness.ring_ready),
            **status_scope,
            **readiness_fields,
        )

    async def all_bout_statuses(self) -> dict[RoundId, BoutStatus]:
        """Observe all six round rings through one bounded manager call.

        The bound is structural: six enum members, with Round 5 allowed one
        additional cleanup-ring read inside ``bout_status``. Reads run
        concurrently so this endpoint does not turn six independent network
        latencies into one long serial wait.
        """

        statuses = await asyncio.gather(
            *(self.bout_status(round_id=round_id) for round_id in RoundId)
        )
        return dict(zip(RoundId, statuses, strict=True))

    def _every_ring_store(self) -> tuple[BoutLeaseStore, ...]:
        """Every ring this installation can hold a bout on.

        Bounded by construction rather than by a policy: the six-member
        ``RoundId`` enum plus Round 5's cleanup ring, so at most seven, whatever
        the installation has been doing.
        """

        if not self._round_isolation:
            return (self._lease_store, self._round5_lease_store)
        stores = [self._lease_store_for_round(round_id) for round_id in RoundId]
        stores.append(self._round5_cleanup_store())
        return tuple(stores)

    async def missing_session_detail(self, session_id: str) -> str:
        """What to tell an operator whose fight card this process does not hold.

        THE INCIDENT, 2026-08-23. The deployed app was driven end to end for the
        first time. An arm succeeded, the run that followed reached a process
        with no record of the session, and the operator got ``404 Session not
        found`` while the durable ring went on holding the armed lease for the
        rest of its window. On stage that is the round dying at the bell for no
        stated reason and then refusing to restart for no stated reason either --
        which is worse than a round that refuses honestly up front, and is the
        failure this project exists to avoid.

        This cannot make the armed fight card survive. An arm is a live object
        graph -- engines, tasks, an event log, an SSE subscriber list -- and the
        ring lease is the only part of it that was ever durable. What this can do
        is convert a bare 404 into the two facts the operator needs: that nothing
        ran, and how long the ring is theirs to wait out.

        Reads only, and every failure is swallowed. The worst outcome here is to
        make the reply *less* useful than the 404 it replaces, so a coordinator
        that cannot answer leaves the generic account standing.
        """

        held: BoutLease | None = None
        for store in self._every_ring_store():
            try:
                current = await store.current()
            except Exception:  # noqa: BLE001 - an explanation may never raise
                logger.warning(
                    "Could not read a ring while explaining a missing fight card",
                    exc_info=True,
                )
                continue
            if current is not None and current.session_id == session_id:
                held = current
                break
        if held is None:
            return _FIGHT_CARD_GONE
        return _FIGHT_CARD_ORPHANED.format(
            ring=describe_held_lease(
                held,
                diagnose_held_lease(held, now=datetime.now(UTC)),
            )
        )

    async def event_floor(self, session_id: str) -> int:
        """The oldest sequence this session's log can still serve.

        **Nothing calls this.** It is an unused seam, kept deliberately, and it is
        not a guard: no resume is refused anywhere on the strength of it. The SSE
        endpoint passes the client's cursor straight through and relies on
        ``EventLog.stream`` reporting ``gap_before`` when it has to resume across a
        hole.

        The obvious caller -- a 409 on the browser's resume -- was considered and
        rejected. A native ``EventSource`` cannot be told to forget its
        ``Last-Event-ID``, so a refusal would loop: reconnect, refuse, reconnect,
        until the component remounted. Reporting the hole is the weaker answer and
        the only one the browser can act on. This stays for a transport that *can*
        be told a cursor; do not wire it to the one that cannot.
        """
        record = await self._record(session_id)
        return record.event_log.first_retained_sequence

    @staticmethod
    def _unfinished_operations(record: SessionRecord) -> tuple[asyncio.Task[None], ...]:
        """Every background task this record still owns.

        The settlement task is the one that makes this necessary rather than
        merely tidy: it runs *after* the bout is terminal and the receipt is
        published, and it holds the record it is settling. Releasing the record
        underneath it would leave the task orphaned -- still running, still
        holding its own reference, and no longer reachable from ``close()``,
        which is the only thing that cancels it at shutdown.
        """
        return tuple(
            operation
            for operation in (
                record.task,
                record.cooldown_task,
                record.connection_spike_cleanup_task,
                record.settlement_task,
                record.model_score_terminal_task,
                record.armed_expiry_task,
                record.lease_heartbeat_task,
                record.round5_lease_heartbeat_task,
            )
            if operation is not None and not operation.done()
        )

    @classmethod
    def _releasable(cls, record: SessionRecord) -> bool:
        """Whether forgetting this record can be proved to cost nothing.

        Deliberately not "is the session terminal". Terminal is neither necessary
        nor sufficient here. Not sufficient, because a verified bout can still be
        settling its proof rows, renewing a lease it failed to release, or holding
        an open cost window the ledger has not closed. Not necessary, because the
        records that actually accumulate are the ones that never reach a terminal
        state at all: a fight card drafted and abandoned sits in ``draft`` with no
        task, no lease and nothing pending, and would otherwise be immortal.

        So the question asked is ownership, not outcome: does anything still point
        at this record from outside the store? A lease still held is authority over
        real cloud resources; an open cost bout is a ledger row waiting to be
        closed; an unfinished task is work that will touch the record again.
        """
        if cls._unfinished_operations(record):
            return False
        if (
            record.lease is not None
            or record.round5_lease is not None
            or record.lease_heartbeat_lease is not None
            or record.round5_lease_heartbeat_lease is not None
            or record.model_score_terminal_lease is not None
        ):
            return False
        return record.cost_bout_id is None

    def _release_settled_records(self, now_ns: int, keep: str | None = None) -> None:
        """Bring the session store back inside its bounds.

        Runs under ``_records_lock`` and deliberately awaits nothing: it only drops
        references to records that have been proved to own no task, no lease and no
        open cost window, so there is nothing here to cancel or close and no
        opportunity for a second caller to interleave. The receipt for every sealed
        bout is already on disk before this can reach it, which is what makes the
        release invisible to the recap.
        """

        # Oldest first by last touch; ties keep insertion order, so a burst of
        # untouched records is still released in the order it arrived.
        ordered = sorted(self._records.items(), key=lambda item: item[1].last_active_ns)
        # max(0, ...) and not a bare negative index: a store smaller than the floor
        # must be covered entirely, and ordered[-2:] on a two-record store would
        # protect one of them instead of both.
        newest = max(0, len(ordered) - self._session_retention_floor)
        floor = {session_id for session_id, _ in ordered[newest:]}
        idle_ns = int(self._session_retention_seconds * 1_000_000_000)
        released = 0
        for session_id, record in ordered:
            if session_id == keep or not self._releasable(record):
                continue
            # The floor binds both reasons for releasing, not just the idle one.
            # It used to bind only `aged_out`, and the gap mattered exactly when
            # the store was in trouble: over the cap with every older record
            # pinned by a task, a lease or an open cost window, the only
            # releasable records left are among the newest -- and those are the
            # ones a browser is reconciling against right now. Dropping them is
            # the 404 mid-presentation this floor exists to prevent. Staying over
            # the cap is the better failure, and the warning below reports it.
            over_capacity = (
                len(self._records) > self._session_retention_max and session_id not in floor
            )
            aged_out = (
                idle_ns > 0
                and session_id not in floor
                and now_ns - record.last_active_ns >= idle_ns
            )
            if not (over_capacity or aged_out):
                continue
            del self._records[session_id]
            released += 1
        if released:
            logger.info(
                "Released %d settled session record(s); %d retained.",
                released,
                len(self._records),
            )
        if len(self._records) > self._session_retention_max:
            # Not a leak yet, but the only shape one could take now: every record
            # over the cap still owns a task, a lease or an open cost window.
            logger.warning(
                "The session store holds %d records against a cap of %d; none of "
                "the excess could be proved settled.",
                len(self._records),
                self._session_retention_max,
            )

    async def _record(self, session_id: str) -> SessionRecord:
        async with self._records_lock:
            now_ns = time.monotonic_ns()
            record = self._records.get(session_id)
            if record is not None:
                record.last_active_ns = now_ns
            # Swept on read as well as on create so an installation that goes quiet
            # after a busy evening returns to its floor on the next request rather
            # than resting at the cap until somebody starts another bout.
            self._release_settled_records(now_ns, keep=session_id if record else None)
        if record is None:
            raise SessionNotFoundError(session_id)
        return record

    async def _claim_bout(
        self,
        record: SessionRecord,
        operator: BoutOperator,
        *,
        phase: str = "checking",
        expected_previous_token: int | None = None,
    ) -> None:
        ttl = timedelta(
            seconds=(
                self._running_lease_ttl if phase == "redo_committed" else self._active_lease_ttl
            )
        )

        async def claim(store: BoutLeaseStore) -> BoutLease:
            return await store.claim(
                session_id=record.snapshot.id,
                operator=operator,
                phase=phase,
                session_state=(
                    SessionState.VERIFIED
                    if phase == "cooldown"
                    else SessionState.FAILED
                    if phase == "cleanup_retry"
                    else SessionState.RUNNING
                    if phase == "redo_committed"
                    else SessionState.CHECKING
                ),
                round_id=record.snapshot.round.id.value,
                round_title=record.snapshot.round.title,
                competitor_id=record.snapshot.competitor.id.value,
                competitor_name=record.snapshot.competitor.short_name,
                ttl=ttl,
                expected_previous_token=expected_previous_token,
            )

        try:
            lease = await claim(self._lease_store_for_record(record))
        except LeaseHeldError as exc:
            detail = self._describe_ring_holder(exc)
            logger.warning("Ring claim refused: %s", detail)
            raise InvalidStateError(detail) from exc
        except LeaseLostError as exc:
            raise InvalidStateError(str(exc)) from exc
        record.lease = lease
        if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
            try:
                record.round5_lease = await claim(self._round5_cleanup_store())
            except LeaseHeldError as exc:
                await self._release_bout(record)
                raise InvalidStateError(
                    "ROUND 5 BACKSTAGE CLEANUP IS STILL FINISHING · OTHER ROUNDS ARE READY"
                ) from exc
            except LeaseLostError as exc:
                await self._release_bout(record)
                raise InvalidStateError(str(exc)) from exc
            self._start_round5_lease_heartbeat(record, ttl)
        if record.snapshot.round.id in {
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            RoundId.RECOVER_DELETED_ORDER,
        }:
            record.round2_fencing_token = lease.fencing_token
        self._start_lease_heartbeat(record, ttl)

    def _describe_ring_holder(self, exc: LeaseHeldError) -> str:
        """Say why the ring is held, not just that it is.

        A settlement failure keeps the lease on purpose -- authority over
        resources you failed to clean is not something to hand back -- but the
        refusal an operator then reads names only the round and the phase. That
        is enough to see that a towel is wedged and nothing about what wedged
        it, which left the server log as the only place to find out. The towel
        already carries the diagnosis; this puts it in the refusal.
        """

        message = str(exc)
        lease = exc.lease
        if lease.phase != "towel_cleanup":
            return message
        # A plain dict read with no await, so no lock is needed and none is
        # taken: a holder in another process simply is not here, and then the
        # unenriched refusal is the honest answer.
        holder = self._records.get(lease.session_id)
        towel = holder.snapshot.towel if holder is not None else None
        if towel is None:
            return message
        if towel.state == TowelState.FAILED and towel.cleanup_failure:
            return f"{message} · TOWEL CLEANUP FAILED · {towel.cleanup_failure}"
        return f"{message} · TOWEL CLEANUP {towel.state.value.upper()}"

    @staticmethod
    def _same_operator(expected: BoutOperator, actual: BoutOperator) -> bool:
        if expected.subject and actual.subject:
            return expected.subject.casefold() == actual.subject.casefold()
        if expected.email and actual.email:
            return expected.email.casefold() == actual.email.casefold()
        return expected.display_name.casefold() == actual.display_name.casefold()

    @classmethod
    def _assert_operator(
        cls,
        expected: BoutOperator | None,
        actual: BoutOperator | None,
    ) -> None:
        if actual is not None and expected is not None and not cls._same_operator(expected, actual):
            raise InvalidStateError("ONLY THE RING OWNER CAN CONTROL THIS BOUT")

    async def _commit_bout(
        self,
        record: SessionRecord,
        operator: BoutOperator | None,
    ) -> None:
        transition_error: LeaseLostError | None = None
        async with record.lease_lock:
            lease = record.lease
            if lease is None or lease.session_id != record.snapshot.id:
                raise InvalidStateError("RING LEASE EXPIRED · PREPARE THE FIGHT CARD AGAIN")
            actual = operator or lease.operator
            self._assert_operator(lease.operator, actual)
            round5_lease = record.round5_lease
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                if round5_lease is None or round5_lease.session_id != record.snapshot.id:
                    raise InvalidStateError(
                        "ROUND 5 CLEANUP AUTHORITY EXPIRED · PREPARE THE FIGHT CARD AGAIN"
                    )
                self._assert_operator(round5_lease.operator, actual)
            try:
                if round5_lease is not None:
                    record.round5_lease = await self._round5_cleanup_store().transition(
                        round5_lease,
                        operator=actual,
                        expected_phase="armed",
                        phase="run_committed",
                        session_state=SessionState.RUNNING,
                        ttl=timedelta(seconds=self._running_lease_ttl),
                    )
                # This is the atomic, fenced ARMED -> RUN_COMMITTED handoff. No other
                # app replica can claim the ring between the bell and live preparation.
                record.lease = await self._lease_store_for_record(record).transition(
                    lease,
                    operator=actual,
                    expected_phase="armed",
                    phase="run_committed",
                    session_state=SessionState.RUNNING,
                    ttl=timedelta(seconds=self._running_lease_ttl),
                )
                if record.snapshot.round.id in {
                    RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
                    RoundId.RECOVER_DELETED_ORDER,
                }:
                    record.round2_fencing_token = record.lease.fencing_token
            except LeaseLostError as exc:
                transition_error = exc
        if transition_error is not None:
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                await self._release_round5_lease(record)
                await self._release_bout(record)
            raise InvalidStateError(
                "FIGHT CARD EXPIRED · PREPARE IT AGAIN BEFORE RINGING THE BELL"
            ) from transition_error
        self._start_lease_heartbeat(
            record,
            timedelta(seconds=self._running_lease_ttl),
        )
        if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
            self._start_round5_lease_heartbeat(
                record,
                timedelta(seconds=self._running_lease_ttl),
            )

    async def _retain_connection_spike_cleanup_lease(
        self,
        record: SessionRecord,
        operator: BoutOperator,
        *,
        session_state: SessionState = SessionState.FAILED,
        allow_claim: bool = True,
    ) -> None:
        async with record.lease_lock:
            lease = record.round5_lease
            if lease is not None:
                self._assert_operator(lease.operator, operator)
                if lease.phase != "round5_cleanup":
                    try:
                        record.round5_lease = await self._round5_cleanup_store().transition(
                            lease,
                            operator=operator,
                            expected_phase=lease.phase,
                            phase="round5_cleanup",
                            session_state=session_state,
                            ttl=timedelta(seconds=self._running_lease_ttl),
                        )
                    except LeaseLostError:
                        record.round5_lease = None
                else:
                    record.round5_lease = lease
        if record.round5_lease is None and not allow_claim:
            raise InvalidStateError("ROUND 5 CLEANUP AUTHORITY CHANGED")
        if record.round5_lease is None:
            try:
                record.round5_lease = await self._round5_cleanup_store().claim(
                    session_id=record.snapshot.id,
                    operator=operator,
                    phase="round5_cleanup",
                    session_state=session_state,
                    round_id=record.snapshot.round.id.value,
                    round_title=record.snapshot.round.title,
                    competitor_id=record.snapshot.competitor.id.value,
                    competitor_name=record.snapshot.competitor.short_name,
                    ttl=timedelta(seconds=self._running_lease_ttl),
                )
            except (LeaseHeldError, LeaseLostError) as exc:
                raise InvalidStateError(
                    "ROUND 5 BACKSTAGE CLEANUP AUTHORITY IS HELD BY ANOTHER WORKER"
                ) from exc
        self._start_round5_lease_heartbeat(
            record,
            timedelta(seconds=self._running_lease_ttl),
        )

    async def _transition_bout_to_towel(
        self,
        record: SessionRecord,
        operator: BoutOperator,
    ) -> None:
        async with record.lease_lock:
            lease = record.lease
            if lease is None or lease.session_id != record.snapshot.id:
                raise InvalidStateError("RING LEASE EXPIRED · TOWEL CLEANUP REFUSED")
            self._assert_operator(lease.operator, operator)
            try:
                record.lease = await self._lease_store_for_record(record).transition(
                    lease,
                    operator=operator,
                    expected_phase="run_committed",
                    phase="towel_cleanup",
                    session_state=SessionState.RUNNING,
                    ttl=timedelta(seconds=self._running_lease_ttl),
                )
            except LeaseLostError as exc:
                raise InvalidStateError("RING LEASE CHANGED · TOWEL CLEANUP REFUSED") from exc
        self._start_lease_heartbeat(
            record,
            timedelta(seconds=self._running_lease_ttl),
        )

    def _assert_towel_cleanup_lease(
        self,
        record: SessionRecord,
        operator: BoutOperator,
    ) -> None:
        lease = record.lease
        if (
            lease is None
            or lease.session_id != record.snapshot.id
            or lease.phase != "towel_cleanup"
        ):
            raise InvalidStateError("TOWEL CLEANUP LEASE WAS LOST · RETRY REFUSED")
        self._assert_operator(lease.operator, operator)

    async def _mark_bout_armed(
        self,
        record: SessionRecord,
        expires_at: datetime,
    ) -> None:
        self._cancel_lease_heartbeat(record)
        self._cancel_round5_lease_heartbeat(record)
        transition_error: LeaseLostError | None = None
        async with record.lease_lock:
            lease = record.lease
            if lease is None or lease.session_id != record.snapshot.id:
                raise InvalidStateError("RING LEASE EXPIRED · PREPARE THE FIGHT CARD AGAIN")
            ttl = max(timedelta(milliseconds=1), expires_at - datetime.now(UTC))
            try:
                round5_lease = record.round5_lease
                if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                    if round5_lease is None:
                        raise LeaseLostError("ROUND 5 CLEANUP AUTHORITY EXPIRED")
                    record.round5_lease = await self._round5_cleanup_store().transition(
                        round5_lease,
                        operator=record.operator or round5_lease.operator,
                        expected_phase="checking",
                        phase="armed",
                        session_state=SessionState.ARMED,
                        ttl=ttl,
                    )
                record.lease = await self._lease_store_for_record(record).transition(
                    lease,
                    operator=record.operator or lease.operator,
                    expected_phase="checking",
                    phase="armed",
                    session_state=SessionState.ARMED,
                    ttl=ttl,
                )
            except LeaseLostError as exc:
                transition_error = exc
        if transition_error is not None:
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                await self._release_round5_lease(record)
                await self._release_bout(record)
            raise InvalidStateError(str(transition_error)) from transition_error
        # The one success exit, and the last line of it deliberately. An arm that
        # got here made the round's calls and Databricks did not refuse them,
        # which is the only evidence more recent and more authoritative than a
        # recorded refusal -- so the record lifts and the round is offered again.
        #
        # Here rather than in each of the six arm handlers, because every one of
        # them reaches this line on success and a per-handler clear is a list
        # that can be added to incompletely. After the raises rather than before,
        # because a lost lease is an arm that did not happen: clearing on the way
        # in would let a lease failure retire a refusal that still stands, which
        # is a false green, and false green is the direction that costs an
        # evening.
        self._grant_refusals.pop(record.snapshot.round.id, None)

    def _cancel_lease_heartbeat(
        self,
        record: SessionRecord,
        expected_lease: BoutLease | None = None,
    ) -> None:
        heartbeat_lease = record.lease_heartbeat_lease
        if expected_lease is not None and (
            heartbeat_lease is None or not self._same_exact_lease(heartbeat_lease, expected_lease)
        ):
            return
        task = record.lease_heartbeat_task
        if task and task is not asyncio.current_task() and not task.done():
            task.cancel()
        record.lease_heartbeat_task = None
        record.lease_heartbeat_lease = None

    def _start_lease_heartbeat(self, record: SessionRecord, ttl: timedelta) -> None:
        self._cancel_lease_heartbeat(record)
        record.lease_heartbeat_task = asyncio.create_task(
            self._heartbeat_lease(record, ttl),
            name=f"lease-heartbeat-{record.snapshot.id}",
        )
        record.lease_heartbeat_lease = record.lease

    def _cancel_round5_lease_heartbeat(
        self,
        record: SessionRecord,
        expected_lease: BoutLease | None = None,
    ) -> None:
        heartbeat_lease = record.round5_lease_heartbeat_lease
        if expected_lease is not None and (
            heartbeat_lease is None or not self._same_exact_lease(heartbeat_lease, expected_lease)
        ):
            return
        task = record.round5_lease_heartbeat_task
        if task and task is not asyncio.current_task() and not task.done():
            task.cancel()
        record.round5_lease_heartbeat_task = None
        record.round5_lease_heartbeat_lease = None

    def _start_round5_lease_heartbeat(
        self,
        record: SessionRecord,
        ttl: timedelta,
    ) -> None:
        self._cancel_round5_lease_heartbeat(record)
        record.round5_lease_heartbeat_task = asyncio.create_task(
            self._heartbeat_round5_lease(record, ttl),
            name=f"round5-lease-heartbeat-{record.snapshot.id}",
        )
        record.round5_lease_heartbeat_lease = record.round5_lease

    async def _heartbeat_round5_lease(
        self,
        record: SessionRecord,
        ttl: timedelta,
    ) -> None:
        interval = min(self._lease_heartbeat, max(0.25, ttl.total_seconds() / 3))
        while True:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return
            lost: BoutLease | None = None
            async with record.lease_lock:
                lease = record.round5_lease
                if lease is None or lease.phase == "armed":
                    return
                try:
                    renewed = await self._round5_cleanup_store().renew(lease, ttl=ttl)
                except LeaseLostError:
                    lost = lease
                except Exception:
                    if datetime.now(UTC) >= lease.expires_at:
                        lost = lease
                else:
                    if record.round5_lease == lease:
                        record.round5_lease = renewed
            if lost is None:
                continue
            async with record.lock:
                async with record.lease_lock:
                    if record.round5_lease != lost:
                        return
                    record.round5_lease = None
                    record.round5_lease_heartbeat_task = None
                    record.round5_lease_heartbeat_lease = None
                if record.snapshot.state not in {
                    SessionState.VERIFIED,
                    SessionState.TOWELLED,
                    SessionState.FAILED,
                }:
                    message = "Round 5 artifact authority was lost; no comparison was declared."
                    record.snapshot.state = SessionState.FAILED
                    record.snapshot.failure = message
                    record.snapshot.remembered_result = None
                    record.snapshot.metrics = []
                    record.snapshot.comparison = None
                    record.snapshot.updated_at = datetime.now(UTC)
                    snapshot = record.snapshot.model_copy(deep=True)
                else:
                    snapshot = None
            if snapshot is not None:
                await record.event_log.publish(
                    "session_failed",
                    {
                        "state": SessionState.FAILED,
                        "message": snapshot.failure,
                        "session": snapshot.model_dump(mode="json"),
                    },
                )
            await self._release_bout(record)
            return

    async def _heartbeat_lease(self, record: SessionRecord, ttl: timedelta) -> None:
        interval = min(self._lease_heartbeat, max(0.25, ttl.total_seconds() / 3))
        while True:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return
            lost_lease: BoutLease | None = None
            async with record.lease_lock:
                lease = record.lease
                if lease is None or lease.phase == "armed":
                    return
                try:
                    renewed = await self._lease_store_for_record(record).renew(lease, ttl=ttl)
                except LeaseLostError:
                    lost_lease = lease
                except Exception:
                    # A transient coordinator outage must not create two owners. The
                    # existing database expiry remains authoritative; fail only once
                    # this replica can no longer prove its lease is still live.
                    if datetime.now(UTC) >= lease.expires_at:
                        lost_lease = lease
                else:
                    if record.lease == lease:
                        record.lease = renewed
            if lost_lease is not None:
                async with record.lease_lock:
                    terminal_lease = record.model_score_terminal_lease
                    if terminal_lease is not None and self._same_exact_lease(
                        terminal_lease,
                        lost_lease,
                    ):
                        # A release/current probe for this exact fence is already
                        # authoritative. It may have removed the coordinator row
                        # before its response reached us.
                        return
                await self._handle_lost_lease(record, lost_lease)
                return

    async def _handle_lost_lease(self, record: SessionRecord, lease: BoutLease) -> None:
        message = "Ring lease lost; active work was stopped before another bout can begin."
        async with record.lock:
            async with record.lease_lock:
                terminal_lease = record.model_score_terminal_lease
                if terminal_lease is not None and self._same_exact_lease(
                    terminal_lease,
                    lease,
                ):
                    return
                if record.lease != lease:
                    return
                record.lease = None
                record.lease_heartbeat_task = None
                record.lease_heartbeat_lease = None
            is_model_score = record.snapshot.round.id == RoundId.PUT_MODEL_SCORE_IN_APP
            is_connection_spike = record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            if is_connection_spike and record.snapshot.state in {
                SessionState.VERIFIED,
                SessionState.TOWELLED,
            }:
                # The verdict is immutable. The separate Round 5 lease and
                # journal continue to own backstage cleanup.
                return
            if record.snapshot.towel is not None:
                message = "Towel cleanup authority was lost; the frozen result is preserved."
                record.snapshot.towel.state = TowelState.FAILED
                record.snapshot.towel.cleanup_failure = message
                record.snapshot.updated_at = datetime.now(UTC)
                for operation in (record.task, record.cooldown_task):
                    if (
                        operation is not None
                        and operation is not asyncio.current_task()
                        and not operation.done()
                    ):
                        operation.cancel()
                snapshot = record.snapshot.model_copy(deep=True)
                await record.event_log.publish(
                    "towel_update",
                    {"session": snapshot.model_dump(mode="json")},
                )
                return
            if is_model_score and record.model_score_terminal_published:
                return
            redo_lost = (
                lease.phase == "redo_committed"
                and is_model_score
                and record.snapshot.redo is not None
                and record.snapshot.redo.state == RedoState.RUNNING
            )
            if redo_lost:
                message = "Managed Sync re-do lease was lost"
                redo = record.snapshot.redo
                assert redo is not None
                redo.state = RedoState.FAILED
                redo.failure = message
                redo.completed_at = datetime.now(UTC)
                lane = redo.lanes["lakebase"]
                lane.state = LaneState.FAILED
                lane.error = message
                lane.status = "Managed Sync re-do lease was lost"
                lane.activity = LaneActivity(phase=ModelScorePhase.FAILED)
                record.snapshot.state = SessionState.VERIFIED
                record.snapshot.failure = None
                record.model_score_pending_update = None
                record.model_score_terminal_published = True
            elif is_model_score and lease.phase == "run_committed":
                message = "Managed Sync proof lease was lost before terminal verification."
                lane = record.snapshot.lanes["lakebase"]
                lane.state = LaneState.FAILED
                lane.error = message
                lane.status = message
                lane.activity = LaneActivity(phase=ModelScorePhase.FAILED)
                record.model_score_result = None
                record.model_score_pending_update = None
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = message
                record.snapshot.remembered_result = None
                record.model_score_terminal_published = True
            elif is_connection_spike:
                message = (
                    "Round 5 ring lease was lost; cleanup was required and no "
                    "comparison was declared."
                )
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = message
                record.snapshot.remembered_result = None
                record.snapshot.metrics = []
                record.snapshot.comparison = None
                if record.snapshot.round5_setup is not None:
                    record.snapshot.round5_setup.state = RoundFiveSetupState.FAILED
                    record.snapshot.round5_setup.downstream_validated = False
                    record.snapshot.round5_setup.failure = (
                        "Setup or downstream verification did not complete"
                    )
                for lane in record.snapshot.lanes.values():
                    lane.state = LaneState.FAILED
                    lane.status = message
                    lane.error = message
                    lane.activity = LaneActivity(phase="failed")
            else:
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = message
                record.snapshot.remembered_result = None
            record.snapshot.updated_at = datetime.now(UTC)
            task = record.task
            cooldown_task = record.cooldown_task
            snapshot = record.snapshot.model_copy(deep=True)
        cleanup_ok = True
        if is_connection_spike:
            if task and task is not asyncio.current_task() and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    # The lease-loss state remains authoritative. The explicit
                    # cleanup probe below must still settle any issued command.
                    pass
            cleanup_ok = await self._cleanup_connection_spike(record)
            if cleanup_ok:
                await self._release_round5_lease(record)
            if not cleanup_ok:
                if record.operator is not None:
                    try:
                        await self._retain_connection_spike_cleanup_lease(
                            record,
                            record.operator,
                        )
                    except InvalidStateError:
                        pass
                async with record.lock:
                    record.snapshot.failure = _ROUND_FIVE_CLEANUP_PENDING
                    setup = record.snapshot.round5_setup
                    if setup is not None:
                        setup.state = RoundFiveSetupState.CLEANUP_FAILED
                        setup.cleanup_retryable = True
                        setup.downstream_validated = False
                        setup.failure = "Automatic cleanup verification is in progress"
                    record.snapshot.updated_at = datetime.now(UTC)
                    snapshot = record.snapshot.model_copy(deep=True)
        for operation in (task, cooldown_task):
            if is_connection_spike and operation is task:
                continue
            if operation and operation is not asyncio.current_task() and not operation.done():
                operation.cancel()
        event = "redo_failed" if redo_lost else "session_failed"
        await record.event_log.publish(
            event,
            {
                "state": snapshot.state,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )
        if is_connection_spike and not cleanup_ok and record.connection_spike_engine is not None:
            self._schedule_connection_spike_cleanup_retry(
                record,
                record.connection_spike_engine,
            )

    async def _release_bout(self, record: SessionRecord) -> bool:
        self._cancel_lease_heartbeat(record)
        store = self._lease_store_for_record(record)
        async with record.lease_lock:
            lease = record.lease
            if lease is None:
                return True
            try:
                released = await store.release(lease)
            except Exception:
                released = False
            if not released:
                try:
                    released = await store.current() is None
                except Exception:
                    released = False
            if released:
                current = record.lease
                if current is not None and (
                    current.lease_id == lease.lease_id
                    and current.fencing_token == lease.fencing_token
                ):
                    record.lease = None
                return True
        current = record.lease
        if current is not None and (
            current.lease_id == lease.lease_id and current.fencing_token == lease.fencing_token
        ):
            ttl_seconds = (
                self._running_lease_ttl
                if lease.phase in {
                    "run_committed",
                    "redo_committed",
                    "towel_cleanup",
                    "cooldown",
                }
                or lease.phase == "cleanup_retry"
                else self._active_lease_ttl
            )
            if (
                lease.phase == "cooldown"
                and record.snapshot.round.id == RoundId.WAKE_IDLE_APP
            ):
                ttl_seconds = max(
                    ttl_seconds,
                    self._cooldown_timeout + self._active_lease_ttl,
                )
            self._start_lease_heartbeat(record, timedelta(seconds=ttl_seconds))
        return False

    async def _release_round5_lease(self, record: SessionRecord) -> bool:
        self._cancel_round5_lease_heartbeat(record)
        store = self._round5_cleanup_store()
        async with record.lease_lock:
            lease = record.round5_lease
            if lease is None:
                return True
            try:
                released = await store.release(lease)
            except Exception:
                released = False
            if not released:
                try:
                    released = await store.current() is None
                except Exception:
                    released = False
            if released:
                current = record.round5_lease
                if current is not None and self._same_exact_lease(current, lease):
                    record.round5_lease = None
                return True
        current = record.round5_lease
        if current is not None and self._same_exact_lease(current, lease):
            self._start_round5_lease_heartbeat(
                record,
                timedelta(seconds=self._running_lease_ttl),
            )
        return False

    @staticmethod
    def _same_exact_lease(left: BoutLease, right: BoutLease) -> bool:
        return (
            left.lease_id == right.lease_id
            and left.fencing_token == right.fencing_token
            and left.session_id == right.session_id
            and left.owner_subject == right.owner_subject
        )

    async def _settle_model_score_terminal_lease(
        self,
        record: SessionRecord,
    ) -> str:
        """Return once the Round 4 lease is released, provably lost, or out of time.

        Every store failure below is swallowed on purpose: a coordinator that is
        briefly unreachable must not turn a good proof into a reported lease
        loss. That tolerance has no natural end, though, so it gets an overall
        deadline. Past it the answer is ``"unknown"`` -- the lease is neither
        released nor declared lost, the heartbeat stops so the durable TTL can
        expire it, and startup reconciliation settles it before the next bout.
        That is the same outcome ``close()`` documents for a settlement that
        cannot be proved, reached sooner rather than avoided.
        """
        store = self._lease_store_for_record(record)
        async with record.lease_lock:
            expected = record.model_score_terminal_lease
            local = record.lease
        if expected is None:
            return "lost"
        deadline = time.monotonic() + max(0.0, self._terminal_settle_deadline)
        backoff = 0.01
        while True:
            outcome: str | None = None
            stop_expected_heartbeat = False
            async with record.lease_lock:
                local = record.lease
            if local is None:
                outcome = "lost"
                stop_expected_heartbeat = True
            elif not self._same_exact_lease(local, expected):
                # This operation owns only the captured fence. A later local claim
                # must remain untouched and continue heartbeating.
                outcome = "lost"
            else:
                try:
                    released = await asyncio.wait_for(
                        store.release(expected),
                        timeout=max(0.001, self._terminal_release_call_timeout),
                    )
                except Exception:
                    released = False
                async with record.lease_lock:
                    local = record.lease
                    if local is None:
                        outcome = "lost"
                        stop_expected_heartbeat = True
                    elif not self._same_exact_lease(local, expected):
                        outcome = "lost"
                    elif released:
                        record.lease = None
                        outcome = "released"
                        stop_expected_heartbeat = True
                if outcome is None and not released:
                    try:
                        current = await asyncio.wait_for(
                            store.current(),
                            timeout=max(0.001, self._terminal_release_call_timeout),
                        )
                    except Exception:
                        current_known = False
                        current = None
                    else:
                        current_known = True
                    async with record.lease_lock:
                        local = record.lease
                        if local is None:
                            outcome = "lost"
                            stop_expected_heartbeat = True
                        elif not self._same_exact_lease(local, expected):
                            outcome = "lost"
                        elif current_known and (
                            current is None or not self._same_exact_lease(current, expected)
                        ):
                            record.lease = None
                            outcome = "lost"
                            stop_expected_heartbeat = True
            if outcome is not None:
                if stop_expected_heartbeat:
                    self._cancel_lease_heartbeat(record, expected)
                return outcome
            if time.monotonic() >= deadline:
                # Stop renewing, or the TTL this is being handed to never expires.
                self._cancel_lease_heartbeat(record, expected)
                logger.error(
                    "Round 4 terminal lease settlement for %s could neither release "
                    "nor disprove the ring lease within %.1fs. The durable lease is "
                    "left to expire so startup reconciliation settles it before the "
                    "next bout.",
                    record.snapshot.id,
                    self._terminal_settle_deadline,
                )
                return "unknown"
            await asyncio.sleep(backoff)
            backoff = min(
                backoff * 2,
                max(0.01, self._terminal_release_backoff_cap),
            )

    def _terminal_orphan_identifier(self, record: SessionRecord) -> str:
        """Name everything an abandoned Round 4 publication may leave behind.

        Round 4 provisions nothing, so there is no environment to hunt for. What
        survives is the ring lease, held until its durable TTL, and the
        run-owned proof row sitting in the sealed source and its synced table in
        place of the baseline.

        The row is named in full for diagnosis, not for repair. It is the update
        minted before the bell, which is one of ``OWNED_PROOF_SHAPES`` exactly,
        and both paths that reclaim owned residue identify it with
        ``is_owned_prior_proof`` -- a pure shape check. So the next arm re-seeds
        the sealed baseline on its own, as does the `round4_managed_sync` gate.
        Settlement is the one place that consults process memory, and it is not
        what runs next.
        """

        lease = record.lease
        if lease is None:
            ring = "no ring lease was held"
        else:
            ring = (
                f"ring lease {lease.lease_id} (fence {lease.fencing_token}) on "
                f"{self._lease_store_for_record(record).ring_key}, held until its TTL"
            )
        update = record.model_score_pending_update
        contract = getattr(record.model_score_engine, "contract", None)
        if update is None:
            row = "no run-owned proof row was pending"
        else:
            row = (
                f"proof row entity {update.entity_id} nonce {update.proof_nonce} "
                f"(score {update.score}, model {update.model_version}) in "
                f"{getattr(contract, 'source_table', 'the Round 4 Delta source')} and "
                f"{getattr(contract, 'synced_table', 'its synced table')}, "
                "recognisable as demo-owned residue by its shape alone"
            )
        return f"Round 4 terminal publication for {record.snapshot.id}: {ring}; {row}"

    async def _await_model_score_terminal(
        self,
        record: SessionRecord,
        terminal: Awaitable[None],
    ) -> None:
        async with record.lease_lock:
            expected_lease = record.lease
            record.model_score_terminal_lease = expected_lease
        identifier = self._terminal_orphan_identifier(record)
        child = asyncio.create_task(
            terminal,
            name=f"model-score-terminal-{record.snapshot.id}",
        )
        record.model_score_terminal_task = child

        def _clear_slot(done: asyncio.Future[None]) -> None:
            if record.model_score_terminal_task is done:
                record.model_score_terminal_task = None

        try:
            try:
                await asyncio.shield(child)
            except asyncio.CancelledError:
                # The publication holds the ring lease and the run-owned proof
                # row, so it is shielded rather than cancelled. The wait for it
                # is bounded all the same: underneath it is a retry against a
                # coordination endpoint that may simply be unreachable, and a
                # cancellation that is never delivered is worse than one that
                # leaves something findable behind.
                await abandon_on_cancel(
                    lambda: asyncio.shield(child),
                    identifier=identifier,
                    timeout_seconds=self._terminal_publish_timeout,
                    # Nothing here is a resource whose absence anybody should go
                    # and confirm: the lease is meant to expire, and the next arm
                    # re-seeds the baseline off the row's shape.
                    remedy=(
                        "the ring lease is left to expire so startup "
                        "reconciliation settles it, and the next Round 4 arm "
                        "re-seeds the sealed baseline over the proof row; "
                        "neither is an operator repair"
                    ),
                )
                if not child.done():
                    # Abandoned. Nothing is waiting on it any more, so the slot
                    # it occupies has to be cleared by the task itself.
                    child.add_done_callback(_clear_slot)
                raise
        finally:
            if record.model_score_terminal_task is child and child.done():
                record.model_score_terminal_task = None
            async with record.lease_lock:
                terminal_lease = record.model_score_terminal_lease
                if (
                    expected_lease is not None
                    and terminal_lease is not None
                    and self._same_exact_lease(terminal_lease, expected_lease)
                ):
                    record.model_score_terminal_lease = None

    async def _confirm_terminal_release(self, record: SessionRecord) -> bool:
        if await self._release_bout(record):
            return True
        record.snapshot.failure = "Ring release could not be confirmed; final state is pending."
        record.snapshot.remembered_result = None
        record.snapshot.updated_at = datetime.now(UTC)
        return False

    async def _hold_bout_during_reset(
        self,
        record: SessionRecord,
        operation: Awaitable[None],
    ) -> None:
        try:
            await operation
        finally:
            async with record.lock:
                cooldown = record.snapshot.cooldown
                mode = cooldown.mode if cooldown is not None else None
                cleanup_ready = bool(cooldown is not None and cooldown.state == CooldownState.READY)
            owns_artifacts = mode in {
                ResetMode.DELETE_ISOLATED_ENVIRONMENT,
                ResetMode.DELETE_RECOVERY_ENVIRONMENT,
            }
            if owns_artifacts:
                released = cleanup_ready and await self._release_bout(record)
                if not released:
                    await self._auto_cleanup_owned_artifacts(
                        record,
                        recovery=mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT,
                    )
            elif mode != ResetMode.RETURN_TO_IDLE or cleanup_ready:
                await self._release_bout(record)

    async def _transition_bout_to_cleanup(
        self,
        record: SessionRecord,
        session_state: SessionState,
    ) -> None:
        cleanup_ttl_seconds = self._running_lease_ttl
        if record.snapshot.round.id == RoundId.WAKE_IDLE_APP:
            # Aurora's configured auto-pause floor can outlive the ordinary
            # 90-second running lease. A replica restart stops the in-process
            # watcher, so the durable cleanup fence must itself span the whole
            # observation window; otherwise the board turns green early and the
            # next arm rediscovers the hidden wait.
            cleanup_ttl_seconds = max(
                cleanup_ttl_seconds,
                self._cooldown_timeout + self._active_lease_ttl,
            )
        async with record.lease_lock:
            lease = record.lease
            if lease is None or lease.session_id != record.snapshot.id:
                raise InvalidStateError("RING LEASE EXPIRED · CLEANUP REMAINS FENCED")
            try:
                record.lease = await self._lease_store_for_record(record).transition(
                    lease,
                    operator=record.operator or lease.operator,
                    expected_phase="run_committed",
                    phase="cooldown",
                    session_state=session_state,
                    ttl=timedelta(seconds=cleanup_ttl_seconds),
                )
            except LeaseLostError as exc:
                raise InvalidStateError("RING LEASE CHANGED · CLEANUP REMAINS FENCED") from exc
        self._start_lease_heartbeat(
            record,
            timedelta(seconds=cleanup_ttl_seconds),
        )

    def _schedule_round_settlement(
        self,
        record: SessionRecord,
        settle: Callable[[], Awaitable[None]] | None,
        *,
        label: str,
    ) -> None:
        """Remove a round's own residue in the background, retrying until it sticks.

        Rounds 4 and 6 write a proof row into a real source table. Until this
        existed only the towel path took it back out, so an ordinary win left
        its row behind for every later bout to inherit, and a failure left one
        too while discarding the identity needed to find it.

        Settlement runs off the terminal path because the receipt the operator
        is reading has already been published and must not wait on cleanup, and
        it retries because one transient Statement Execution error is not a
        reason to keep residue forever.

        An engine that cannot settle is skipped rather than raised on. Residue
        is a cost problem; a terminal path that throws is a broken bout, and
        trading the first for the second would be a poor bargain.
        """

        if settle is None or self._closed:
            return
        current = record.settlement_task
        if current is not None and not current.done():
            return
        record.settlement_task = asyncio.create_task(
            self._settle_with_retry(record, settle, label=label),
            name=f"round-settlement-{record.snapshot.id}",
        )

    async def _settle_with_retry(
        self,
        record: SessionRecord,
        settle: Callable[[], Awaitable[None]],
        *,
        label: str,
    ) -> None:
        task = asyncio.current_task()
        delay = max(0.1, self._cleanup_retry_initial)
        maximum_delay = max(delay, self._cleanup_retry_max)
        try:
            for attempt in range(1, self._settlement_attempts + 1):
                if self._closed:
                    return
                try:
                    await settle()
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # A failed settlement is residue, not a broken bout. The
                    # receipt is already published and the ring is already
                    # released, so the only honest thing left is to say so
                    # loudly enough that reconciliation has something to match.
                    logger.error(
                        "%s settlement attempt %d/%d failed session=%s: %s: %s",
                        label,
                        attempt,
                        self._settlement_attempts,
                        record.snapshot.id,
                        type(exc).__name__,
                        exc,
                    )
                    if attempt == self._settlement_attempts:
                        return
                await asyncio.sleep(delay)
                delay = min(delay * 2, maximum_delay)
        finally:
            if record.settlement_task is task:
                record.settlement_task = None

    def _schedule_owned_artifact_cleanup(
        self,
        record: SessionRecord,
        *,
        recovery: bool,
    ) -> None:
        if self._closed:
            return
        current = record.cooldown_task
        if current is not None and not current.done():
            return
        record.cooldown_task = asyncio.create_task(
            self._auto_cleanup_owned_artifacts(record, recovery=recovery),
            name=f"auto-artifact-cleanup-{record.snapshot.id}",
        )

    async def _auto_cleanup_owned_artifacts(
        self,
        record: SessionRecord,
        *,
        recovery: bool,
    ) -> None:
        task = asyncio.current_task()
        delay = max(0.1, self._cleanup_retry_initial)
        maximum_delay = max(delay, self._cleanup_retry_max)
        cleanup_complete = False
        try:
            # Let the verified/failed receipt event reach the room first.
            await asyncio.sleep(0)
            while not self._closed:
                if not cleanup_complete:
                    if recovery:
                        engine = record.recovery_engine
                        if engine is None:
                            return
                        settle = getattr(engine, "settle_pending_mutations", None)
                        try:
                            if settle is not None:
                                await settle(record.snapshot.competitor.id)
                        except Exception as exc:
                            logger.error(
                                "Recovery cleanup settlement is pending session=%s: %s",
                                record.snapshot.id,
                                type(exc).__name__,
                            )
                    operation = (
                        self._reset_recovery(record)
                        if recovery
                        else self._reset_safe_change(record)
                    )
                    await operation
                    async with record.lock:
                        cooldown = record.snapshot.cooldown
                        cleanup_complete = bool(
                            cooldown is not None and cooldown.state == CooldownState.READY
                        )
                if cleanup_complete and await self._release_bout(record):
                    async with record.lock:
                        cooldown = record.snapshot.cooldown
                        if cooldown is not None:
                            cooldown.state = CooldownState.READY
                            cooldown.failure = None
                            record.snapshot.updated_at = datetime.now(UTC)
                            snapshot = cooldown.model_copy(deep=True)
                        else:
                            snapshot = None
                    if snapshot is not None:
                        await record.event_log.publish(
                            "cooldown_ready",
                            {"cooldown": snapshot.model_dump(mode="json")},
                        )
                    return

                async with record.lock:
                    cooldown = record.snapshot.cooldown
                    if cooldown is None:
                        return
                    if cleanup_complete:
                        cooldown.state = CooldownState.WATCHING
                        cooldown.failure = None
                        for lane in cooldown.lanes.values():
                            lane.status = "Cleanup verified · Releasing the ring"
                    else:
                        replacement = self._new_cooldown(record, cooldown.mode)
                        for lane in replacement.lanes.values():
                            lane.status = "Automatic cleanup retry scheduled"
                        record.snapshot.cooldown = replacement
                        cooldown = replacement
                    record.snapshot.updated_at = datetime.now(UTC)
                    snapshot = cooldown.model_copy(deep=True)
                await record.event_log.publish(
                    "cooldown_update",
                    {"cooldown": snapshot.model_dump(mode="json")},
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, maximum_delay)
        except asyncio.CancelledError:
            raise
        finally:
            if record.cooldown_task is task:
                record.cooldown_task = None

    def _schedule_round_one_cooldown(self, record: SessionRecord) -> None:
        if self._closed or record.live_targets is None:
            return
        current = record.cooldown_task
        if current is not None and not current.done():
            return
        record.cooldown_task = asyncio.create_task(
            self._run_automatic_round_one_cooldown(record),
            name=f"auto-idle-watch-{record.snapshot.id}",
        )

    async def _run_automatic_round_one_cooldown(self, record: SessionRecord) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(0)
            await self._monitor_cooldown(record)
            async with record.lock:
                cooldown = record.snapshot.cooldown
                ready = bool(cooldown is not None and cooldown.state == CooldownState.READY)
            if ready and not await self._release_bout(record):
                async with record.lock:
                    cooldown = record.snapshot.cooldown
                    if cooldown is not None:
                        cooldown.state = CooldownState.FAILED
                        cooldown.failure = (
                            "Return to idle was verified, but ring release could not be confirmed."
                        )
                        record.snapshot.updated_at = datetime.now(UTC)
        except asyncio.CancelledError:
            raise
        finally:
            if record.cooldown_task is task:
                record.cooldown_task = None

    def _cancel_armed_expiry(self, record: SessionRecord) -> None:
        task = record.armed_expiry_task
        if task and task is not asyncio.current_task() and not task.done():
            task.cancel()
        record.armed_expiry_task = None

    def _schedule_armed_expiry(
        self,
        record: SessionRecord,
        armed_at_monotonic: float,
    ) -> None:
        self._cancel_armed_expiry(record)
        record.armed_expiry_task = asyncio.create_task(
            self._expire_abandoned_arm(record, armed_at_monotonic),
            name=f"armed-expiry-{record.snapshot.id}",
        )

    async def _expire_abandoned_arm(
        self,
        record: SessionRecord,
        armed_at_monotonic: float,
    ) -> None:
        try:
            await asyncio.sleep(self._armed_ttl)
        except asyncio.CancelledError:
            return
        message = (
            "Fight card expired before the bell. The ring was released automatically; "
            "prepare it again."
        )
        async with record.lock:
            if (
                record.snapshot.state != SessionState.ARMED
                or record.armed_at_monotonic != armed_at_monotonic
            ):
                return
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                cleanup_ok = await self._cleanup_connection_spike(record)
                record.snapshot.metrics = []
                record.snapshot.comparison = None
                if record.snapshot.round5_setup is not None:
                    record.snapshot.round5_setup.state = RoundFiveSetupState.FAILED
                    record.snapshot.round5_setup.failure = "The clean per-bout baseline expired"
                if not cleanup_ok:
                    if record.operator is not None:
                        try:
                            await self._retain_connection_spike_cleanup_lease(
                                record,
                                record.operator,
                            )
                        except InvalidStateError:
                            pass
                    message = _ROUND_FIVE_CLEANUP_PENDING
                    setup = record.snapshot.round5_setup
                    if setup is not None:
                        setup.state = RoundFiveSetupState.CLEANUP_FAILED
                        setup.cleanup_retryable = True
                        setup.downstream_validated = False
                        setup.failure = "Automatic cleanup verification is in progress"
                    record.snapshot.state = SessionState.FAILED
                    record.snapshot.failure = message
                    record.snapshot.remembered_result = None
                    record.snapshot.updated_at = datetime.now(UTC)
                    record.armed_at_monotonic = None
                    record.armed_expiry_task = None
                    for lane in record.snapshot.lanes.values():
                        lane.state = LaneState.FAILED
                        lane.status = message
                        lane.error = message
                        lane.activity = LaneActivity(phase="cleanup_failed")
                    snapshot = record.snapshot.model_copy(deep=True)
                    await record.event_log.publish(
                        "session_failed",
                        {
                            "state": SessionState.FAILED,
                            "message": message,
                            "session": snapshot.model_dump(mode="json"),
                        },
                    )
                    if record.connection_spike_engine is not None:
                        self._schedule_connection_spike_cleanup_retry(
                            record,
                            record.connection_spike_engine,
                        )
                    return
            if not await self._confirm_terminal_release(record):
                record.armed_expiry_task = None
                return
            if (
                record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
                and not await self._release_round5_lease(record)
            ):
                record.armed_expiry_task = None
                return
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            record.snapshot.remembered_result = None
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                record.snapshot.metrics = []
                record.snapshot.comparison = None
                if record.snapshot.round5_setup is not None:
                    record.snapshot.round5_setup.state = RoundFiveSetupState.FAILED
                    record.snapshot.round5_setup.downstream_validated = False
                    record.snapshot.round5_setup.failure = (
                        "Setup or downstream verification did not complete"
                    )
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            record.armed_expiry_task = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )

    def _arm_refusal(
        self,
        record: SessionRecord,
        summary: str,
        error: BaseException,
        *,
        round_number: int,
    ) -> str:
        """Turn a caught arm exception into a message, and record what it means.

        THE DEFECT THIS CLOSES. Every one of the six arm handlers below ended in
        a bare `except Exception` that dropped the exception and substituted a
        fixed sentence. On 2026-08-23 the deployed app refused Round 4 with
        `PermissionDenied: User does not have SELECT on Table '<catalog>.<schema>
        .model_scores'` and Round 6 with a `PermissionDenied` naming the exact
        principal, the exact permission and the exact database project. The API
        said "The Managed Sync baseline could not be verified." and "The native
        CDF start state could not be verified." Recovering either real message
        meant pulling the app's container logs over a WebSocket.

        Three things happen here, and it is one function so that no arm handler
        can do a curated subset of them. That is the whole lesson of the fix
        before this one: a curated subset is exactly what lets a second gate
        stay hidden.

        * The chain reaches the operator, readably, on the failure banner.
        * The chain reaches the log, with a traceback, for the case where the
          readable form was cut short.
        * An authorization refusal is remembered, so `/api/catalog` stops
          offering a round Databricks has already turned down. The round-select
          screen is what a room full of customers looks at, and a round shown
          green that dies when the bell rings is the worst failure this project
          has.
        """

        message = arm_failure_message(summary, error)
        logger.error(
            "Round %d arm failed session=%s round=%s diagnosis=%s",
            round_number,
            record.snapshot.id,
            record.snapshot.round.id.value,
            operator_diagnosis(error),
            exc_info=True,
        )
        if authorization_refusal(error) is not None:
            self._grant_refusals[record.snapshot.round.id] = grant_refusal(
                operator_diagnosis(error)
            )
        return message

    async def _fail_arm(
        self,
        record: SessionRecord,
        summary: str,
        error: BaseException,
        *,
        round_number: int,
    ) -> None:
        """Fail an arm without discarding what refused it."""

        await self._fail(
            record,
            self._arm_refusal(record, summary, error, round_number=round_number),
        )

    def _log_bout_refusal(
        self,
        record: SessionRecord,
        error: BaseException,
        *,
        round_number: int,
    ) -> None:
        """Put a mid-bout failure in the log. Change nothing else.

        THE DEFECT THIS CLOSES. `_arm_refusal` fixed the six *arm* handlers. The
        six *run* handlers below it kept the shape it removed: a bare
        `except Exception` that drops the exception and substitutes a fixed
        sentence. So a refusal before the bell named itself and the identical
        refusal after the bell did not, and the second one is the one a room is
        watching. A transient "Databricks control-plane request was refused"
        during a live campaign left no server-log trace at all.

        Deliberately unlike `_arm_refusal` in two ways, both of which are the
        point rather than an omission.

        * It returns nothing. `_arm_refusal` returns a message that its callers
          put on the failure banner; this one is a log call and the fixed
          sentence on the line below every call site is left exactly as it was.
          A mid-bout lane outcome is what an audience is looking at and what the
          bout record keeps, and this is not the change that gets to move it.
        * It does not remember an authorization refusal. `_arm_refusal` writes
          `_grant_refusals` so `/api/catalog` stops offering a round Databricks
          already turned down. Doing that here would let a log call take a round
          off the select screen, which is a behaviour change wearing a
          diagnostic's clothes.

        `operator_diagnosis` and not `_redacted_exception_chain`, because the
        whole complaint is that the reason was unrecoverable and the reason
        lives in the message: `ControlPlaneCommandError` alone does not say
        refused, timed out, or returned nonsense. That is the same boundary
        `_arm_refusal` already accepted for the same class of text, and it is
        the boundary that keeps a `psycopg` DSN or a `botocore` secret ARN down
        to a type name. `exc_info=True` is added because a traceback is
        acceptable in a log and is not acceptable on a screen, and this call
        only ever reaches the log.
        """

        logger.error(
            "Round %d bout failed session=%s round=%s diagnosis=%s",
            round_number,
            record.snapshot.id,
            record.snapshot.round.id.value,
            operator_diagnosis(error),
            exc_info=True,
        )

    def _log_lane_refusals(self, record: SessionRecord, *, round_number: int) -> None:
        """Name every settled lane refusal in the log, one line per lane.

        WHY THIS IS NOT COVERED BY `_log_bout_refusal`. For Rounds 1, 2 and 3 a
        refused lane does not raise out of `engine.run` at all -- the engine
        catches it and reports it as a lane result, which is exactly the design
        that lets one lane fail without destroying the other's timing. So the
        run-level handler never fires, and the refusal's own words existed in
        precisely two places: the SSE stream, and the bout record. Both are
        gone by the time an operator asks why a lane failed; the screen has
        moved on and the record holds a sentence with no clock and no lane
        beside it.

        WHY THIS IS A SETTLEMENT CALL AND NOT A PROGRESS CALL. The obvious
        place to catch a refusal is `on_progress`, where `progress.error`
        already flows past. That would be wrong. Progress fires on every poll
        and every retry, so a refusal that was transient and immediately
        recovered would log once per attempt and read, in the log, exactly like
        one that ended the bout. Settlement fires once, after the retries are
        spent, and a lane that recovered is `VERIFIED` here and says nothing.
        Silence for the recovered case is not an accident of where the call
        sits; it is the reason the call sits here.

        `lane.error` is logged as it stands rather than re-filtered. It is
        already the operator-visible sentence -- this same string is on the
        panel and in the bout record before this runs -- so quoting it into a
        log widens nothing, and running it through a second policy here would
        be inventing the second policy `_message_is_ours_to_quote` exists to
        prevent. What produced it upstream is where that filter belongs.
        """

        refused = [
            (lane_id, lane)
            for lane_id, lane in record.snapshot.lanes.items()
            if lane.state == LaneState.FAILED
        ]
        for lane_id, lane in refused:
            logger.error(
                "Round %d lane refused session=%s round=%s lane=%s status=%s reason=%s",
                round_number,
                record.snapshot.id,
                record.snapshot.round.id.value,
                lane_id,
                lane.status,
                lane.error or "no reason was reported",
            )
        if refused or record.snapshot.state != SessionState.FAILED:
            # The state guard is not belt-and-braces. A Round 4 re-do failure
            # settles its own `redo.lanes` and deliberately leaves the bout
            # `VERIFIED`, because the initial proof still stands; without this,
            # the fallback below would announce a failed bout that succeeded.
            return
        # A bout can settle as failed with no lane individually refused: Round 5
        # fails on a setup or downstream gate that belongs to the bout rather
        # than to either lane. Logging nothing there would reproduce the defect
        # this closes one layer up, so the session's own sentence is said
        # instead. It is a worse sentence than a lane reason, and it is the only
        # one there is.
        logger.error(
            "Round %d bout failed with no lane naming a reason session=%s round=%s failure=%s",
            round_number,
            record.snapshot.id,
            record.snapshot.round.id.value,
            record.snapshot.failure or "no failure was recorded",
        )

    async def _arm(self, record: SessionRecord) -> None:
        assert record.live_targets is not None
        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._arm_timeout
        last_status = "Waiting for both systems to reach their sealed start state."
        try:
            while loop.time() < deadline:
                checks = await asyncio.gather(
                    *(target.assert_armed() for target in record.live_targets),
                    return_exceptions=True,
                )
                configuration_error = next(
                    (item for item in checks if isinstance(item, TargetConfigurationError)), None
                )
                if configuration_error:
                    raise configuration_error
                unexpected_error = next(
                    (
                        item
                        for item in checks
                        if isinstance(item, BaseException)
                        and not isinstance(item, TargetNotArmedError)
                    ),
                    None,
                )
                if unexpected_error:
                    raise unexpected_error
                if all(isinstance(item, dict) for item in checks):
                    async with record.lock:
                        armed_at = datetime.now(UTC)
                        record.snapshot.state = SessionState.ARMED
                        record.snapshot.armed_at = armed_at
                        record.snapshot.armed_expires_at = armed_at + timedelta(
                            seconds=self._armed_ttl
                        )
                        record.snapshot.updated_at = armed_at
                        record.armed_at_monotonic = loop.time()
                        for target, check in zip(record.live_targets, checks, strict=True):
                            assert isinstance(check, dict)
                            lane = record.snapshot.lanes[target.id]
                            if check.get("eligible", True) is False:
                                lane.state = LaneState.NOT_SUPPORTED
                                lane.status = (
                                    "No automatic scale-to-zero or connection-triggered wake"
                                )
                                lane.activity = LaneActivity(
                                    phase="not_supported",
                                    wire_call=None,
                                )
                            else:
                                lane.state = LaneState.SEALED
                                lane.status = "Scale zero verified"
                        # Arming is the one moment both control planes are read,
                        # so replace the configured disclosure with what they
                        # actually reported.
                        record.snapshot.capacity = build_capacity_disclosure(
                            record.snapshot.round.id,
                            record.snapshot.competitor.id,
                            observed=_observed_capacity(
                                {
                                    target.id: check
                                    for target, check in zip(
                                        record.live_targets, checks, strict=True
                                    )
                                    if isinstance(check, dict)
                                }
                            ),
                        )
                        snapshot = record.snapshot.model_copy(deep=True)
                        armed_at_monotonic = record.armed_at_monotonic
                    assert armed_at_monotonic is not None
                    armed_expires_at = snapshot.armed_expires_at
                    assert armed_expires_at is not None
                    await self._mark_bout_armed(record, armed_expires_at)
                    self._schedule_armed_expiry(record, armed_at_monotonic)
                    await record.event_log.publish(
                        "armed",
                        {
                            "state": SessionState.ARMED,
                            "evidence": {
                                record.live_targets[index].id: check
                                for index, check in enumerate(checks)
                            },
                            "session": snapshot.model_dump(mode="json"),
                        },
                    )
                    return

                waiting = [str(item) for item in checks if isinstance(item, TargetNotArmedError)]
                last_status = " · ".join(waiting) or last_status
                await record.event_log.publish(
                    "arm_waiting",
                    {"state": SessionState.CHECKING, "status": last_status},
                )
                await asyncio.sleep(self._arm_poll)
            raise TargetNotArmedError(last_status)
        except (TargetConfigurationError, TargetNotArmedError) as exc:
            await self._fail(record, str(exc))
        except Exception as exc:
            await self._fail_arm(
                record,
                "The start state could not be verified.",
                exc,
                round_number=1,
            )

    async def _arm_safe_change(self, record: SessionRecord) -> None:
        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        try:
            engine = self._safe_change_factory()
            record.safe_change_engine = engine

            async def on_progress(progress: SafeChangeProgress) -> None:
                async with record.lock:
                    lane = record.snapshot.lanes[progress.lane_id]
                    lane.status = progress.status
                    record.snapshot.updated_at = datetime.now(UTC)
                await record.event_log.publish(
                    "arm_waiting",
                    {
                        "state": SessionState.CHECKING,
                        "status": f"{progress.lane_name} · {progress.status}",
                    },
                )

            arm = await engine.arm(record.snapshot.competitor.id, on_progress)
            loop = asyncio.get_running_loop()
            async with record.lock:
                armed_at = datetime.now(UTC)
                record.safe_change_arm = arm
                record.snapshot.state = SessionState.ARMED
                record.snapshot.armed_at = armed_at
                record.snapshot.armed_expires_at = armed_at + timedelta(seconds=self._armed_ttl)
                record.snapshot.updated_at = armed_at
                record.armed_at_monotonic = loop.time()
                for lane in record.snapshot.lanes.values():
                    lane.state = LaneState.SEALED
                    lane.status = "Source clean · No isolated environment exists"
                snapshot = record.snapshot.model_copy(deep=True)
                armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            armed_expires_at = snapshot.armed_expires_at
            assert armed_expires_at is not None
            await self._mark_bout_armed(record, armed_expires_at)
            self._schedule_armed_expiry(record, armed_at_monotonic)
            evidence = {lane_id: dict(lane.evidence) for lane_id, lane in arm.lanes.items()}
            await record.event_log.publish(
                "armed",
                {
                    "state": SessionState.ARMED,
                    "evidence": evidence,
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except SafeChangeNotArmedError as exc:
            await self._fail(record, str(exc))
        except Exception as exc:
            await self._fail_arm(
                record,
                "The isolated-change start state could not be verified.",
                exc,
                round_number=2,
            )

    async def _arm_recovery(self, record: SessionRecord) -> None:
        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        try:
            engine = self._recovery_factory()
            record.recovery_engine = engine

            async def on_progress(progress: RecoveryProgress) -> None:
                async with record.lock:
                    lane = record.snapshot.lanes[progress.lane_id]
                    lane.status = progress.status
                    lane.activity = LaneActivity(
                        phase=progress.phase,
                        wire_call=progress.wire_call,
                        recovery_at=progress.recovery_at,
                    )
                    record.snapshot.updated_at = datetime.now(UTC)
                await record.event_log.publish(
                    "arm_waiting",
                    {
                        "state": SessionState.CHECKING,
                        "status": f"{progress.lane_name} · {progress.status}",
                    },
                )

            arm = await engine.arm(record.snapshot.competitor.id, on_progress)
            loop = asyncio.get_running_loop()
            async with record.lock:
                armed_at = datetime.now(UTC)
                record.recovery_arm = arm
                record.snapshot.state = SessionState.ARMED
                record.snapshot.armed_at = armed_at
                record.snapshot.armed_expires_at = armed_at + timedelta(seconds=self._armed_ttl)
                record.snapshot.updated_at = armed_at
                record.armed_at_monotonic = loop.time()
                for lane in record.snapshot.lanes.values():
                    lane.state = LaneState.SEALED
                    lane.status = (
                        "Exact incident committed · No recovery artifact exists · "
                        "Recovery eligibility not pre-waited"
                    )
                    lane.activity = LaneActivity(
                        phase=RecoveryPhase.PREPARING_INCIDENT,
                        wire_call=None,
                    )
                snapshot = record.snapshot.model_copy(deep=True)
                armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            assert snapshot.armed_expires_at is not None
            await self._mark_bout_armed(record, snapshot.armed_expires_at)
            self._schedule_armed_expiry(record, armed_at_monotonic)
            await record.event_log.publish(
                "armed",
                {
                    "state": SessionState.ARMED,
                    "evidence": {
                        lane_id: dict(lane.evidence) for lane_id, lane in arm.lanes.items()
                    },
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except RecoveryNotArmedError as exc:
            await self._fail(record, str(exc))
        except Exception as exc:
            await self._fail_arm(
                record,
                "The recovery start state could not be verified.",
                exc,
                round_number=3,
            )

    async def _arm_model_score(self, record: SessionRecord) -> None:
        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        factory = self._model_score_factory
        if factory is None:
            await self._fail(record, "Round 4 live adapter is not configured.")
            return
        try:
            engine = factory()
            record.model_score_engine = engine

            async def on_progress(progress: ModelScoreProgress) -> None:
                async with record.lock:
                    lane = record.snapshot.lanes["lakebase"]
                    lane.status = progress.status
                    lane.attempts = progress.attempt or lane.attempts
                    lane.activity = LaneActivity(phase=progress.phase)
                    record.snapshot.updated_at = datetime.now(UTC)
                await record.event_log.publish(
                    "arm_waiting",
                    {
                        "state": SessionState.CHECKING,
                        "status": progress.status,
                    },
                )

            arm = await engine.arm(on_progress)
            loop = asyncio.get_running_loop()
            async with record.lock:
                armed_at = datetime.now(UTC)
                record.model_score_arm = arm
                record.snapshot.state = SessionState.ARMED
                record.snapshot.armed_at = armed_at
                record.snapshot.armed_expires_at = armed_at + timedelta(seconds=self._armed_ttl)
                record.snapshot.updated_at = armed_at
                record.armed_at_monotonic = loop.time()
                lakebase = record.snapshot.lanes["lakebase"]
                lakebase.state = LaneState.SEALED
                lakebase.status = "Managed Sync baseline verified"
                lakebase.error = None
                lakebase.activity = LaneActivity(phase=ModelScorePhase.ARMED)
                lakebase.evidence = {
                    "primary_key": arm.baseline.entity_id,
                    "score": arm.baseline.score,
                    "model_version": arm.baseline.model_version,
                    "proof_nonce": arm.baseline.proof_nonce,
                    "delta_version": arm.source_version,
                }
                competitor = record.snapshot.lanes["competitor"]
                competitor.state = LaneState.NOT_SUPPORTED
                competitor.status = "AWS lane not timed for this Managed Sync proof"
                competitor.error = None
                competitor.activity = LaneActivity(phase="not_supported")
                competitor.evidence = {"unsupported_reason": _ROUND_FOUR_UNSUPPORTED_REASON}
                snapshot = record.snapshot.model_copy(deep=True)
                armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            assert snapshot.armed_expires_at is not None
            await self._mark_bout_armed(record, snapshot.armed_expires_at)
            self._schedule_armed_expiry(record, armed_at_monotonic)
            await record.event_log.publish(
                "armed",
                {
                    "state": SessionState.ARMED,
                    "evidence": {
                        lane_id: lane.evidence for lane_id, lane in snapshot.lanes.items()
                    },
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except ModelScoreError as exc:
            await self._fail(record, str(exc))
        except Exception as exc:
            await self._fail_arm(
                record,
                "The Managed Sync baseline could not be verified.",
                exc,
                round_number=4,
            )

    async def _arm_connection_spike(self, record: SessionRecord) -> None:
        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        factory = self._connection_spike_factory
        if factory is None:
            await self._fail(record, "Round 5 live adapter is not configured.")
            return
        try:
            engine = factory(record.snapshot.competitor.id)
            record.connection_spike_engine = engine
            has_timed_setup = self._round_five_has_timed_setup(engine)
            arm = None
            if not has_timed_setup:
                check = engine.check  # type: ignore[attr-defined]
                arm = await check()
            loop = asyncio.get_running_loop()
            async with record.lock:
                armed_at = datetime.now(UTC)
                record.connection_spike_arm = arm
                record.snapshot.state = SessionState.ARMED
                record.snapshot.armed_at = armed_at
                record.snapshot.armed_expires_at = armed_at + timedelta(seconds=self._armed_ttl)
                record.snapshot.updated_at = armed_at
                record.armed_at_monotonic = loop.time()
                record.snapshot.fairness = FairnessSnapshot(
                    launch_skew_ms=None,
                    warmup_connections=_ROUND_FIVE_WARMUP_CONNECTIONS,
                    concurrency=_ROUND_FIVE_CONCURRENCY,
                    runner=_ROUND_FIVE_RUNNER,
                    tls="verify-full",
                    timeout="10 seconds",
                )
                for lane in record.snapshot.lanes.values():
                    lane.state = LaneState.SEALED
                    lane.status = (
                        "Round 5 bout sealed · Timed setup starts at the bell"
                        if has_timed_setup
                        else "Round 5 preflight verified · Waiting for the bell"
                    )
                    lane.error = None
                    lane.activity = LaneActivity(phase="armed")
                snapshot = record.snapshot.model_copy(deep=True)
                armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            assert snapshot.armed_expires_at is not None
            await self._mark_bout_armed(record, snapshot.armed_expires_at)
            self._schedule_armed_expiry(record, armed_at_monotonic)
            await record.event_log.publish(
                "armed",
                {
                    "state": SessionState.ARMED,
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except Exception as exc:
            # Taken once and used on both exits. Round 5 is the one arm handler
            # with two terminal paths, and building the message separately in
            # each is how the two would drift into saying different things about
            # the same refusal.
            message = self._arm_refusal(
                record,
                "The Round 5 start state could not be verified.",
                exc,
                round_number=5,
            )
            cleanup_ok = await self._cleanup_connection_spike(record)
            if cleanup_ok:
                await self._fail(record, message)
            else:
                await self._finish_connection_spike_failure(
                    record,
                    message,
                    cleanup_verified=False,
                )

    async def _arm_live_orders(self, record: SessionRecord) -> None:
        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        factory = self._live_orders_factory
        if factory is None:
            await self._fail(record, "Round 6 native CDF adapter is not configured.")
            return
        try:
            engine = factory()
            record.live_orders_engine = engine

            async def on_progress(progress: LiveOrdersProgress) -> None:
                async with record.lock:
                    lane = record.snapshot.lanes["lakebase"]
                    lane.status = progress.status
                    lane.attempts = progress.attempt or lane.attempts
                    lane.activity = LaneActivity(phase=progress.phase)
                    record.snapshot.updated_at = datetime.now(UTC)
                await record.event_log.publish(
                    "arm_waiting",
                    {"state": SessionState.CHECKING, "status": progress.status},
                )

            arm = await engine.arm(on_progress)
            loop = asyncio.get_running_loop()
            async with record.lock:
                armed_at = datetime.now(UTC)
                record.live_orders_arm = arm
                record.snapshot.state = SessionState.ARMED
                record.snapshot.armed_at = armed_at
                record.snapshot.armed_expires_at = armed_at + timedelta(seconds=self._armed_ttl)
                record.snapshot.updated_at = armed_at
                record.armed_at_monotonic = loop.time()
                lakebase = record.snapshot.lanes["lakebase"]
                lakebase.state = LaneState.SEALED
                lakebase.status = "Native CDF is streaming"
                lakebase.error = None
                lakebase.activity = LaneActivity(phase=LiveOrdersPhase.ARMED)
                lakebase.evidence = {"cdf_committed_lsn": arm.committed_lsn}
                competitor = record.snapshot.lanes["competitor"]
                competitor.state = LaneState.NOT_SUPPORTED
                competitor.status = "AWS CDC pipeline not built or timed"
                competitor.error = None
                competitor.activity = LaneActivity(phase="not_supported")
                competitor.evidence = {
                    "unsupported_reason": (
                        "Aurora/RDS require a separately configured CDC pipeline into Delta."
                    )
                }
                snapshot = record.snapshot.model_copy(deep=True)
                armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            assert snapshot.armed_expires_at is not None
            await self._mark_bout_armed(record, snapshot.armed_expires_at)
            self._schedule_armed_expiry(record, armed_at_monotonic)
            await record.event_log.publish(
                "armed",
                {"state": SessionState.ARMED, "session": snapshot.model_dump(mode="json")},
            )
        except LiveOrdersError as exc:
            await self._fail(record, str(exc))
        except Exception as exc:
            await self._fail_arm(
                record,
                "The native CDF start state could not be verified.",
                exc,
                round_number=6,
            )

    async def _run_connection_spike(self, record: SessionRecord) -> None:
        engine = record.connection_spike_engine
        arm = record.connection_spike_arm
        has_timed_setup = self._round_five_has_timed_setup(engine)
        setup_operation = getattr(engine, "setup", None) if has_timed_setup else None
        if engine is None or (arm is None and not has_timed_setup):
            await self._fail(record, "The connection-spike proof must be armed again.")
            return
        async with record.lock:
            started_at = datetime.now(UTC)
            record.snapshot.state = SessionState.RUNNING
            record.snapshot.run_started_at = started_at
            record.run_started_monotonic_ns = self._clock_ns()
            record.snapshot.updated_at = started_at
            record.snapshot.metrics = []
            record.snapshot.comparison = None
            record.snapshot.round5_setup = self._new_round_five_setup(record.snapshot)
            record.snapshot.round5_setup.state = RoundFiveSetupState.RUNNING
            record.round5_progress_observed_ns.clear()
            for lane in record.snapshot.lanes.values():
                lane.state = LaneState.CONNECTING
                lane.status = "Preparing a clean per-bout pooling setup"
                lane.attempts = 0
                lane.successes = 0
                lane.errors = 0
                lane.p99_ms = None
                lane.error = None
                lane.activity = LaneActivity(phase="setup")
            running_snapshot = self._public_snapshot_locked(record)
        await record.event_log.publish(
            "run_started",
            {
                "state": SessionState.RUNNING,
                "lanes": ["lakebase", "competitor"],
                "session": running_snapshot.model_dump(mode="json"),
            },
        )

        async def publish_lane_snapshots(
            snapshot: SessionSnapshot,
            lane_ids: tuple[str, ...],
        ) -> None:
            serialized = snapshot.model_dump(mode="json")
            for lane_id in lane_ids:
                lane = snapshot.lanes[lane_id]
                await record.event_log.publish(
                    "lane_update",
                    {
                        "lane_id": lane_id,
                        "state": lane.state,
                        "attempts": lane.attempts,
                        "elapsed_ms": lane.elapsed_ms,
                        "status": lane.status,
                        "error": lane.error,
                        "activity": (
                            lane.activity.model_dump(mode="json")
                            if lane.activity is not None
                            else None
                        ),
                        # Retained for clients that understand the richer Round 5
                        # setup snapshot; the standard lane fields above keep the
                        # SSE contract valid for every existing client.
                        "session": serialized,
                    },
                )

        async def on_setup_progress(progress: object) -> None:
            lane_id = self._round_five_value(progress, "lane_id")
            phase = self._round_five_public_phase(
                self._round_five_value(progress, "phase", "setup")
            )
            progress_status = str(
                self._round_five_value(progress, "status", "running")
            ).lower()
            progress_elapsed_ms = self._round_five_number(
                self._round_five_value(progress, "setup_elapsed_ms")
            )
            reached_stop = (
                phase == "setup_stop"
                and progress_status == "verified"
                and progress_elapsed_ms is not None
            )
            status = self._round_five_setup_status(lane_id, phase)
            async with record.lock:
                if record.snapshot.towel is not None:
                    return
                setup = record.snapshot.round5_setup
                if setup is None:
                    return
                lanes = (
                    [setup.lanes[lane_id]] if lane_id in setup.lanes else list(setup.lanes.values())
                )
                affected_lane_ids = (
                    (str(lane_id),)
                    if lane_id in record.snapshot.lanes
                    else ("lakebase", "competitor")
                )
                absorbed_lane_ids = {
                    lane.id
                    for lane in lanes
                    if lane.state == RoundFiveSetupState.VERIFIED
                }
                observed_ns: int | None = None
                for lane in lanes:
                    if lane.id in absorbed_lane_ids:
                        continue
                    lane.state = (
                        RoundFiveSetupState.VERIFIED
                        if reached_stop
                        else RoundFiveSetupState.RUNNING
                    )
                    if progress_elapsed_ms is not None and (
                        lane.setup_elapsed_ms is None
                        or progress_elapsed_ms >= lane.setup_elapsed_ms
                    ):
                        lane.setup_elapsed_ms = progress_elapsed_ms
                        # Anchor the published value to a measurement-clock
                        # reading so a later towel can say how long this lane
                        # has run since, rather than repeating a figure the
                        # lane stopped updating minutes ago.
                        if observed_ns is None:
                            observed_ns = self._clock_ns()
                        record.round5_progress_observed_ns[lane.id] = observed_ns
                    lane.status = status
                for affected_lane_id in affected_lane_ids:
                    if affected_lane_id in absorbed_lane_ids:
                        continue
                    lane = record.snapshot.lanes[affected_lane_id]
                    lane.status = status
                    lane.activity = LaneActivity(phase=phase)
                record.snapshot.updated_at = datetime.now(UTC)
                snapshot = self._public_snapshot_locked(record)
            await publish_lane_snapshots(snapshot, affected_lane_ids)

        if setup_operation is not None:
            try:
                lease = record.round5_lease
                if lease is None:
                    raise InvalidStateError("The Round 5 artifact lease is unavailable")
                setup_result = await setup_operation(
                    record.snapshot.id,
                    lease.fencing_token,
                    on_setup_progress,
                )
                record.connection_spike_setup_result = setup_result
                preliminary = self._round_five_finalize_setup(setup_result, {})
                async with record.lock:
                    if record.snapshot.towel is not None:
                        return
                    record.snapshot.round5_setup = self._round_five_setup_snapshot(
                        record.snapshot,
                        preliminary,
                        terminal=False,
                    )
                    setup_snapshot = self._public_snapshot_locked(record)
                await publish_lane_snapshots(
                    setup_snapshot,
                    ("lakebase", "competitor"),
                )
                arm = await engine.check()  # type: ignore[attr-defined]
                record.connection_spike_arm = arm
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Setup owns the same operator-safe quoting boundary as every
                # other bout failure. The old bespoke log used only exception
                # types, discarding our runner's bounded refusal token (for
                # example ``baseline_auth_hash_invalid``) even though the
                # runner had returned it explicitly. Third-party messages still
                # remain redacted by ``operator_diagnosis``.
                self._log_bout_refusal(record, exc, round_number=5)
                message = "The Round 5 setup phase failed; no comparison was declared."
                await self._finish_connection_spike_failure(
                    record,
                    message,
                    cleanup_verified=False,
                )
                return

        async with record.lock:
            if record.snapshot.towel is not None:
                return
            for lane in record.snapshot.lanes.values():
                lane.status = "Executing the frozen 128-client secondary burst"
                lane.activity = LaneActivity(phase="burst")
            burst_snapshot = self._public_snapshot_locked(record)
        await publish_lane_snapshots(
            burst_snapshot,
            ("lakebase", "competitor"),
        )

        async def on_progress(progress: object) -> None:
            lane_id = self._round_five_value(progress, "lane_id")
            phase = self._round_five_public_phase(
                self._round_five_value(progress, "phase", "burst")
            )
            async with record.lock:
                if record.snapshot.towel is not None:
                    return
                lanes = (
                    [record.snapshot.lanes[lane_id]]
                    if lane_id in record.snapshot.lanes
                    else list(record.snapshot.lanes.values())
                )
                for lane in lanes:
                    lane.status = "Executing and validating the secondary burst"
                    lane.activity = LaneActivity(phase=phase)
                affected_lane_ids = (
                    (str(lane_id),)
                    if lane_id in record.snapshot.lanes
                    else ("lakebase", "competitor")
                )
                record.snapshot.updated_at = datetime.now(UTC)
                snapshot = record.snapshot.model_copy(deep=True)
            await publish_lane_snapshots(snapshot, affected_lane_ids)

        try:
            if arm is None:
                raise InvalidStateError("The Round 5 burst arm is unavailable")
            result = await engine.run(arm, on_progress)  # type: ignore[attr-defined]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Round 5 burst failed session=%s competitor=%s diagnostic=%s",
                record.snapshot.id,
                record.snapshot.competitor.id,
                _redacted_exception_chain(exc),
            )
            message = "The live Round 5 proof failed unexpectedly."
            await self._finish_connection_spike_failure(
                record,
                message,
                cleanup_verified=False,
            )
            return
        await self._finish_connection_spike(record, result)

    async def _run_live_orders(self, record: SessionRecord) -> None:
        engine = record.live_orders_engine
        arm = record.live_orders_arm
        order = record.live_orders_pending_order
        if engine is None or arm is None or order is None:
            await self._fail(record, "The native CDF proof must be armed again.")
            return
        async with record.lock:
            started_at = datetime.now(UTC)
            record.snapshot.state = SessionState.RUNNING
            record.snapshot.run_started_at = started_at
            record.run_started_monotonic_ns = self._clock_ns()
            record.snapshot.updated_at = started_at
            lane = record.snapshot.lanes["lakebase"]
            lane.state = LaneState.CONNECTING
            lane.status = "Committing one checkout order"
            lane.activity = LaneActivity(phase=LiveOrdersPhase.CHECKOUT)
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_started",
            {
                "state": SessionState.RUNNING,
                "lanes": ["lakebase"],
                "session": snapshot.model_dump(mode="json"),
            },
        )

        phase_states = {
            LiveOrdersPhase.PREFLIGHT: LaneState.SEALED,
            LiveOrdersPhase.ARMED: LaneState.SEALED,
            LiveOrdersPhase.CHECKOUT: LaneState.CONNECTING,
            LiveOrdersPhase.WAITING_CDF: LaneState.VERIFYING,
            LiveOrdersPhase.READING_CHECKOUT: LaneState.VERIFYING,
            LiveOrdersPhase.VERIFIED: LaneState.VERIFIED,
            LiveOrdersPhase.FAILED: LaneState.FAILED,
        }

        async def on_progress(progress: LiveOrdersProgress) -> None:
            async with record.lock:
                if record.snapshot.towel is not None:
                    return
                lane = record.snapshot.lanes["lakebase"]
                lane.state = phase_states[progress.phase]
                lane.status = progress.status
                lane.attempts = progress.attempt or lane.attempts
                if progress.elapsed_ms is not None:
                    lane.elapsed_ms = progress.elapsed_ms
                lane.activity = LaneActivity(phase=progress.phase)
                record.snapshot.updated_at = datetime.now(UTC)
                activity = lane.activity.model_dump(mode="json")
            await record.event_log.publish(
                "lane_update",
                {
                    "lane_id": "lakebase",
                    "state": phase_states[progress.phase],
                    "attempts": progress.attempt or lane.attempts,
                    "elapsed_ms": lane.elapsed_ms,
                    "status": progress.status,
                    "activity": activity,
                },
            )

        try:
            checkout_guardrail_order = LiveOrder(
                order_id=str(uuid4()),
                sku="RED-GLOVE",
                store="CHICAGO",
                quantity=1,
                total_cents=8450,
                status="paid",
                proof_nonce=f"round6-checkout-{uuid4().hex}",
            )
            async with record.lock:
                record.live_orders_guardrail_order = checkout_guardrail_order
            result = await engine.run(
                arm,
                order,
                checkout_guardrail_order,
                on_progress,
            )
        except asyncio.CancelledError:
            raise
        except LiveOrdersError as exc:
            await self._finish_live_orders_failure(record, str(exc))
            return
        except Exception as exc:
            self._log_bout_refusal(record, exc, round_number=6)
            await self._finish_live_orders_failure(
                record, "The live native CDF proof failed unexpectedly."
            )
            return
        await self._finish_live_orders(record, result)

    async def _finish_live_orders(
        self,
        record: SessionRecord,
        result: LiveOrdersResult,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            if not await self._confirm_terminal_release(record):
                return
            lane = record.snapshot.lanes["lakebase"]
            lane.state = LaneState.VERIFIED
            lane.elapsed_ms = result.analytics_available_ms
            lane.attempts = result.poll_attempts
            lane.successes = 1
            lane.errors = 0
            lane.status = "Exact Delta answer · Separate checkout committed"
            lane.error = None
            lane.verified_at = datetime.now(UTC)
            lane.activity = LaneActivity(phase=LiveOrdersPhase.VERIFIED)
            lane.evidence = {
                "order_id": result.order.order_id,
                "sku": result.order.sku,
                "store": result.order.store,
                "quantity": result.order.quantity,
                "total_cents": result.order.total_cents,
                "total_display": "$84.50",
                "status": result.order.status,
                "proof_nonce": result.order.proof_nonce,
                "history_lsn": result.history_lsn,
                "checkout_commit_ms": result.checkout_commit_ms,
                "checkout_guardrail_order_id": result.checkout_guardrail_order.order_id,
                "checkout_guardrail_proof_nonce": (result.checkout_guardrail_order.proof_nonce),
                "checkout_guardrail_commit_ms": result.checkout_guardrail_commit_ms,
                "checkout_guardrail_read_ms": result.checkout_guardrail_read_ms,
            }
            record.snapshot.metrics = [
                MetricValue(
                    spec_id="analytics_available_ms",
                    lane_id="lakebase",
                    value=result.analytics_available_ms,
                    display_value=f"{result.analytics_available_ms:.2f} ms",
                ),
                MetricValue(
                    spec_id="matching_live_orders",
                    lane_id="lakebase",
                    value=result.matching_orders,
                    display_value="1 exact order",
                ),
                MetricValue(
                    spec_id="checkout_verified",
                    lane_id="lakebase",
                    value=result.checkout_verified,
                    display_value="SEPARATE CHECKOUT COMMITTED ✓",
                ),
            ]
            record.snapshot.comparison = ComparisonSnapshot(
                kind=ComparisonKind.CAPABILITY_GAP,
                winner_lane_id="lakebase",
                detail=(
                    "Lakebase native CDF produced the exact Delta answer; the selected AWS "
                    "database requires a separately configured CDC pipeline and was not timed."
                ),
            )
            record.snapshot.state = SessionState.VERIFIED
            record.snapshot.failure = None
            record.snapshot.remembered_result = _ROUND_SIX_REMEMBERED_RESULT
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            record.live_orders_result = result
            record.live_orders_pending_order = None
            engine = record.live_orders_engine
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_finished",
            {"state": SessionState.VERIFIED, "session": snapshot.model_dump(mode="json")},
        )
        settle = getattr(engine, "settle_and_cleanup_owned", None)
        self._schedule_round_settlement(
            record,
            None
            if settle is None
            else lambda: settle(result.order, result.checkout_guardrail_order),
            label="Round 6",
        )

    async def _finish_live_orders_failure(
        self,
        record: SessionRecord,
        message: str,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            if not await self._confirm_terminal_release(record):
                return
            lane = record.snapshot.lanes["lakebase"]
            lane.state = LaneState.FAILED
            lane.status = "Native CDF proof could not be verified"
            lane.error = message
            lane.activity = LaneActivity(phase=LiveOrdersPhase.FAILED)
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            # Here rather than at each `except` above, because this is the one
            # funnel every Round 6 failure passes through: the typed
            # `LiveOrdersError` path carries the real reason in `message` and had
            # no log of its own either.
            self._log_lane_refusals(record, round_number=6)
            record.snapshot.remembered_result = None
            record.snapshot.metrics = []
            record.snapshot.comparison = None
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            # Captured before the identity is cleared: a failed run can have
            # committed its checkout row before failing verification, and
            # dropping the identity here is what used to make that row
            # permanently unattributable.
            engine = record.live_orders_engine
            settling_order = record.live_orders_pending_order
            guardrail_order = record.live_orders_guardrail_order
            record.live_orders_pending_order = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )
        settle = getattr(engine, "settle_and_cleanup_owned", None)
        self._schedule_round_settlement(
            record,
            None
            if settle is None or settling_order is None
            else lambda: settle(settling_order, guardrail_order),
            label="Round 6",
        )

    async def _begin_connection_spike_cleanup_handoff(
        self,
        record: SessionRecord,
    ) -> None:
        """Move Round 5 artifacts backstage without delaying the verdict."""

        engine = record.connection_spike_engine
        operator = record.operator
        if engine is None or operator is None:
            await self._mark_connection_spike_cleanup_pending(record)
            return
        try:
            await self._retain_connection_spike_cleanup_lease(
                record,
                operator,
                session_state=record.snapshot.state,
                allow_claim=False,
            )
            stop_run = getattr(engine, "stop_and_begin_cleanup", None)
            stop_setup = getattr(engine, "stop_setup_and_begin_cleanup", None)
            if stop_run is not None and record.connection_spike_arm is not None:
                await stop_run(record.connection_spike_arm)
            elif stop_setup is not None:
                await stop_setup(record.snapshot.id)
        except Exception as exc:
            logger.error(
                "Round 5 cleanup handoff failed session=%s diagnostic=%s",
                record.snapshot.id,
                _redacted_exception_chain(exc),
            )
            await self._mark_connection_spike_cleanup_pending(record)
            if record.snapshot.towel is None:
                await self._release_bout(record)
            self._schedule_connection_spike_cleanup_retry(record, engine)
            return

        current = record.connection_spike_cleanup_task
        if current is not None and not current.done():
            return
        record.connection_spike_cleanup_task = asyncio.create_task(
            self._complete_connection_spike_cleanup_handoff(record, engine),
            name=f"round5-backstage-cleanup-{record.snapshot.id}",
        )

    async def _round5_proof_authority_is_current(
        self,
        record: SessionRecord,
    ) -> bool:
        async with record.lease_lock:
            main = record.lease
            round5 = record.round5_lease
        if main is None or round5 is None:
            return False
        if main.phase != "run_committed" or round5.phase != "run_committed":
            return False
        try:
            current_main, current_round5 = await asyncio.gather(
                self._lease_store_for_record(record).current(),
                self._round5_cleanup_store().current(),
            )
        except Exception:
            return False
        return bool(
            current_main is not None
            and current_round5 is not None
            and self._same_exact_lease(main, current_main)
            and self._same_exact_lease(round5, current_round5)
        )

    async def _complete_connection_spike_cleanup_handoff(
        self,
        record: SessionRecord,
        engine: object,
    ) -> None:
        task = asyncio.current_task()
        retry = False
        try:
            wait_accepted = getattr(engine, "wait_for_proxy_delete_accepted", None)
            wait_complete = getattr(engine, "wait_for_cleanup_complete", None)
            if wait_accepted is not None and wait_complete is not None:
                try:
                    await wait_accepted()
                except Exception:
                    accepted = getattr(engine, "proxy_delete_accepted", None)
                    if accepted is None or not bool(accepted()):
                        raise
                if not await self._release_bout(record):
                    raise InvalidStateError("Main ring release is still pending")
                await wait_complete()
            else:
                if not await self._cleanup_connection_spike(record):
                    raise InvalidStateError("Round 5 cleanup could not be verified")
                if not await self._release_bout(record):
                    raise InvalidStateError("Main ring release is still pending")
            if not await self._release_round5_lease(record):
                raise InvalidStateError("Round 5 cleanup lease release is still pending")
            await self._mark_connection_spike_cleanup_complete(record)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Round 5 backstage cleanup is not settled session=%s diagnostic=%s",
                record.snapshot.id,
                _redacted_exception_chain(exc),
            )
            await self._mark_connection_spike_cleanup_pending(record)
            retry = not self._closed
        finally:
            if record.connection_spike_cleanup_task is task:
                record.connection_spike_cleanup_task = None
        if retry:
            self._schedule_connection_spike_cleanup_retry(record, engine)

    async def _mark_connection_spike_cleanup_pending(
        self,
        record: SessionRecord,
    ) -> None:
        async with record.lock:
            setup = record.snapshot.round5_setup
            if setup is not None:
                setup.cleanup_retryable = True
                if record.snapshot.state == SessionState.FAILED:
                    setup.state = RoundFiveSetupState.CLEANUP_FAILED
                    setup.failure = "Automatic backstage cleanup is still settling"
            if record.snapshot.towel is not None:
                record.snapshot.towel.state = TowelState.CLEANING
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = record.snapshot.model_copy(deep=True)
        self._note_round5_proxy_at_risk(record)
        await record.event_log.publish(
            "cleanup_update",
            {"session": snapshot.model_dump(mode="json")},
        )

    def _note_round5_proxy_at_risk(
        self,
        record: SessionRecord,
        *,
        attempts: int | None = None,
        still_retrying: bool = True,
    ) -> Round5CleanupOwed | None:
        """Put a Proxy that cleanup has not deleted where ``/readyz`` can see it.

        **Gated on whether AWS accepted the delete, not on whether cleanup has
        finished**, and that distinction is the whole reason this can be called
        on every failed attempt without becoming noise. A healthy Round 5
        cleanup crosses its delete handoff in seconds and then waits -- once for
        a measured 31.5 minutes -- for AWS to make the Proxy actually disappear.
        Reporting *that* as a leak would fire on ordinary bouts and be learned
        away by the second week. A delete that was never accepted is a different
        animal: nothing has asked AWS to remove anything, so nothing will.

        Abandonment overrides the gate. Once the retries are spent, "the delete
        was accepted" is no longer reassuring -- it was accepted and the resource
        was still never proved gone -- and the operator is the only remaining
        way that gets resolved.
        """

        engine = record.connection_spike_engine
        if engine is None:
            return None
        accepted = getattr(engine, "proxy_delete_accepted", None)
        if still_retrying and accepted is not None:
            try:
                if bool(accepted()):
                    return None
            except Exception:
                # Unanswerable is not the same as reassuring, so it falls
                # through and reports rather than returning quietly.
                pass
        name = getattr(engine, "proxy_name_for_bout", None)
        resource = ""
        if name is not None:
            try:
                resource = str(name(record.snapshot.id) or "")
            except Exception:
                resource = ""
        return record_round5_cleanup_owed(
            record.snapshot.id,
            resource=resource,
            attempts=attempts,
            still_retrying=still_retrying,
        )

    async def _mark_connection_spike_cleanup_complete(
        self,
        record: SessionRecord,
    ) -> None:
        async with record.lock:
            setup = record.snapshot.round5_setup
            if setup is not None:
                setup.cleanup_retryable = False
                setup.cleanup_failure = None
                if record.snapshot.state == SessionState.FAILED:
                    setup.state = RoundFiveSetupState.FAILED
                    setup.failure = "Cleanup verified"
            if record.snapshot.towel is not None:
                record.snapshot.towel.state = TowelState.READY
                record.snapshot.towel.cleanup_failure = None
            record.connection_spike_setup_result = None
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = record.snapshot.model_copy(deep=True)
        # The one place a Proxy is proved gone. Every surface that was warning
        # about it stops here, together, so `/readyz` cannot keep naming a
        # resource the snapshot has already stopped naming.
        clear_round5_cleanup_owed(record.snapshot.id)
        await record.event_log.publish(
            "cleanup_update",
            {"session": snapshot.model_dump(mode="json")},
        )

    async def _finish_connection_spike(
        self,
        record: SessionRecord,
        result: object,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
        if not await self._round5_proof_authority_is_current(record):
            await self._finish_connection_spike_failure(
                record,
                "Round 5 proof authority changed; no comparison was declared.",
                cleanup_verified=False,
            )
            return
        async with record.lock:
            lanes = self._round_five_lanes(result)
            valid = set(lanes) == {"lakebase", "competitor"}
            for lane_id in ("lakebase", "competitor"):
                raw = lanes.get(lane_id)
                lane = record.snapshot.lanes[lane_id]
                if raw is None:
                    lane.state = LaneState.FAILED
                    lane.status = "Round 5 result omitted this lane"
                    lane.error = lane.status
                    valid = False
                    continue
                evidence = self._round_five_evidence(raw)
                lane.attempts = int(evidence["scheduled_clients"])
                lane.successes = int(evidence["successful_clients"])
                lane.errors = int(evidence["error_clients"])
                lane.p99_ms = self._round_five_number(
                    self._round_five_value(raw, "application_p99_ms")
                )
                # Setup is the primary clock. Burst p99 remains an independent
                # secondary metric and is never folded into generic elapsed time.
                lane.elapsed_ms = None
                lane.evidence = evidence
                lane_valid = self._round_five_lane_valid(raw, evidence)
                valid = valid and lane_valid
                lane.state = LaneState.VERIFIED if lane_valid else LaneState.FAILED
                lane.status = (
                    "Load, PID witness, and cleanup gates verified"
                    if lane_valid
                    else "Round 5 contract gate failed"
                )
                lane.error = None if lane_valid else lane.status
                lane.verified_at = datetime.now(UTC) if lane_valid else None
                lane.activity = LaneActivity(phase="verified" if lane_valid else "failed")

            launch_skews = [
                self._round_five_number(self._round_five_value(raw, "launch_skew_ms"))
                for raw in lanes.values()
            ]
            skew = max((value for value in launch_skews if value is not None), default=None)
            valid = (
                valid
                and len(launch_skews) == 2
                and all(value is not None and 0 <= value <= 10 for value in launch_skews)
            )
            record.snapshot.fairness = FairnessSnapshot(
                launch_skew_ms=skew,
                warmup_connections=_ROUND_FIVE_WARMUP_CONNECTIONS,
                concurrency=_ROUND_FIVE_CONCURRENCY,
                runner=_ROUND_FIVE_RUNNER,
                tls="verify-full",
                timeout="10 seconds",
            )
            raw_downstream = self._round_five_value(result, "lanes", {})
            setup_result = self._round_five_finalize_setup(
                record.connection_spike_setup_result,
                raw_downstream if isinstance(raw_downstream, Mapping) else {},
            )
            setup_snapshot = self._round_five_setup_snapshot(
                record.snapshot,
                setup_result,
                terminal=True,
            )
            record.snapshot.round5_setup = setup_snapshot
            valid = (
                valid
                and setup_result is not None
                and setup_snapshot.setup_validated
                and setup_snapshot.downstream_validated
            )
            comparison = (
                self._round_five_setup_comparison(
                    setup_result,
                    record.snapshot.competitor.short_name,
                )
                if valid
                else None
            )
            if comparison is None:
                valid = False
            record.snapshot.comparison = comparison if valid else None
            record.snapshot.metrics = (
                self._round_five_metrics(record.snapshot, setup_snapshot) if valid else []
            )
            if valid:
                record.snapshot.state = SessionState.VERIFIED
                record.snapshot.failure = None
                record.snapshot.remembered_result = comparison.detail
                setup_snapshot.state = RoundFiveSetupState.VERIFIED
            else:
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = (
                    "Round 5 contract gate failed; no comparison was declared."
                )
                record.snapshot.remembered_result = None
                setup_snapshot.state = RoundFiveSetupState.FAILED
                setup_snapshot.failure = "Setup or downstream verification did not complete"
                # Before the overwrite below, not after. That loop replaces every
                # lane's own reason with one bout-level sentence, so a log call
                # placed after it would report the same generic string twice and
                # lose which lane's contract gate actually refused.
                self._log_lane_refusals(record, round_number=5)
                for lane in record.snapshot.lanes.values():
                    lane.state = LaneState.FAILED
                    lane.status = record.snapshot.failure
                    lane.error = record.snapshot.failure
                    lane.activity = LaneActivity(phase="failed")
            terminal_at = datetime.now(UTC)
            record.snapshot.updated_at = terminal_at
            competitor_setup = setup_snapshot.lanes.get("competitor")
            if record.snapshot.cost_receipt is not None:
                record.snapshot.cost_receipt = update_terminal_cost_receipt(
                    record.snapshot.cost_receipt,
                    record.snapshot.competitor.id,
                    terminal_at=terminal_at,
                    run_started_at=record.snapshot.run_started_at,
                    rds_proxy_created=bool(
                        competitor_setup is not None and competitor_setup.verified
                    ),
                )
            record.armed_at_monotonic = None
            record.connection_spike_setup_result = None
            try:
                # Publish the verdict only after the durable main-ring phase says
                # cleanup. The Round 5 readiness reconciler may need one poll to
                # observe its separate artifact lease; this fence makes every
                # intermediate all-round snapshot conservatively CLEANUP rather
                # than BOUT or generic UNAVAILABLE.
                await self._transition_bout_to_cleanup(record, record.snapshot.state)
            except InvalidStateError:
                logger.error(
                    "Round 5 result reached terminal state before its cleanup phase "
                    "could be durably published session=%s",
                    record.snapshot.id,
                    exc_info=True,
                )
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_finished",
            {"state": snapshot.state, "session": snapshot.model_dump(mode="json")},
        )
        await self._begin_connection_spike_cleanup_handoff(record)

    async def _finish_connection_spike_failure(
        self,
        record: SessionRecord,
        message: str,
        *,
        cleanup_verified: bool,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            record.snapshot.remembered_result = None
            record.snapshot.metrics = []
            record.snapshot.comparison = None
            setup = record.snapshot.round5_setup
            if setup is not None:
                setup.state = (
                    RoundFiveSetupState.FAILED
                    if cleanup_verified
                    else RoundFiveSetupState.CLEANUP_FAILED
                )
                setup.downstream_validated = False
                setup.failure = (
                    "Setup or downstream verification did not complete"
                    if cleanup_verified
                    else "Automatic cleanup verification is in progress"
                )
                setup.cleanup_retryable = not cleanup_verified
            terminal_at = datetime.now(UTC)
            record.snapshot.updated_at = terminal_at
            competitor_setup = setup.lanes.get("competitor") if setup is not None else None
            if record.snapshot.cost_receipt is not None:
                record.snapshot.cost_receipt = update_terminal_cost_receipt(
                    record.snapshot.cost_receipt,
                    record.snapshot.competitor.id,
                    terminal_at=terminal_at,
                    run_started_at=record.snapshot.run_started_at,
                    rds_proxy_created=bool(
                        competitor_setup is not None and competitor_setup.verified
                    ),
                )
            for lane in record.snapshot.lanes.values():
                lane.state = LaneState.FAILED
                lane.status = record.snapshot.failure
                lane.error = record.snapshot.failure
                lane.activity = LaneActivity(
                    phase="cleanup_failed" if not cleanup_verified else "failed"
                )
            try:
                await self._transition_bout_to_cleanup(record, record.snapshot.state)
            except InvalidStateError:
                logger.error(
                    "Round 5 failure reached terminal state before its cleanup phase "
                    "could be durably published session=%s",
                    record.snapshot.id,
                    exc_info=True,
                )
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )
        if cleanup_verified:
            await self._release_bout(record)
            await self._release_round5_lease(record)
        else:
            await self._begin_connection_spike_cleanup_handoff(record)

    async def _retry_connection_spike_cleanup(
        self,
        record: SessionRecord,
        engine: object,
        *,
        emit_failure: bool = True,
    ) -> bool:
        reconcile = getattr(engine, "reconcile_failed_cleanup", None)
        async with record.lease_lock:
            lease = record.round5_lease
        if reconcile is None or lease is None or lease.phase != "round5_cleanup":
            return False
        try:
            await reconcile(record.snapshot.id, lease.fencing_token)
        except Exception as exc:
            logger.error(
                "Round 5 cleanup is not settled session=%s diagnostic=%s",
                record.snapshot.id,
                _redacted_exception_chain(exc),
            )
            async with record.lock:
                setup = record.snapshot.round5_setup
                if setup is not None:
                    setup.cleanup_retryable = True
                    if record.snapshot.state == SessionState.FAILED:
                        setup.state = RoundFiveSetupState.CLEANUP_FAILED
                        setup.failure = "Automatic cleanup verification is in progress"
                record.snapshot.updated_at = datetime.now(UTC)
                snapshot = record.snapshot.model_copy(deep=True)
            if emit_failure:
                await record.event_log.publish(
                    "session_failed",
                    {
                        "state": SessionState.FAILED,
                        "message": snapshot.failure,
                        "session": snapshot.model_dump(mode="json"),
                    },
                )
            return False

        main_released = await self._release_bout(record)
        round5_released = await self._release_round5_lease(record)
        if main_released and round5_released:
            await self._mark_connection_spike_cleanup_complete(record)
        else:
            await self._mark_connection_spike_cleanup_pending(record)
        snapshot = await self.get(record.snapshot.id)
        await record.event_log.publish(
            "cleanup_update",
            {
                "session": snapshot.model_dump(mode="json"),
            },
        )
        return not (snapshot.round5_setup is not None and snapshot.round5_setup.cleanup_retryable)

    def _schedule_connection_spike_cleanup_retry(
        self,
        record: SessionRecord,
        engine: object,
    ) -> None:
        if self._closed:
            return
        current = record.connection_spike_cleanup_task
        if current is not None and not current.done():
            return
        record.connection_spike_cleanup_task = asyncio.create_task(
            self._auto_retry_connection_spike_cleanup(record, engine),
            name=f"auto-cleanup-{record.snapshot.id}",
        )

    async def _auto_retry_connection_spike_cleanup(
        self,
        record: SessionRecord,
        engine: object,
    ) -> None:
        task = asyncio.current_task()
        delay = max(0.1, self._cleanup_retry_initial)
        maximum_delay = max(delay, self._cleanup_retry_max)
        attempts = 0
        try:
            while not self._closed:
                if attempts >= self._cleanup_retry_attempts:
                    # Terminating matters more than the last attempt. This loop
                    # used to be unbounded, and on a durable failure -- expired
                    # credentials, a permanent AWS error -- it ground on while
                    # the towel sat at `cleaning`: the one cleanup state with
                    # neither a retry button nor an exit, on the only round that
                    # can reach it. Landing on `failed` is what hands control
                    # back, to the UI and to `start_towel`'s retry branch alike.
                    await self._abandon_connection_spike_cleanup_retry(record, attempts)
                    return
                attempts += 1
                if await self._retry_connection_spike_cleanup(
                    record,
                    engine,
                    emit_failure=False,
                ):
                    return
                async with record.lock:
                    setup = record.snapshot.round5_setup
                    async with record.lease_lock:
                        lease = record.round5_lease
                    if (
                        setup is None
                        or not setup.cleanup_retryable
                        or lease is None
                        or lease.phase != "round5_cleanup"
                    ):
                        return
                    if record.snapshot.state == SessionState.FAILED:
                        setup.state = RoundFiveSetupState.CLEANUP_FAILED
                        setup.failure = "Automatic cleanup is still settling"
                    record.snapshot.updated_at = datetime.now(UTC)
                    snapshot = record.snapshot.model_copy(deep=True)
                serialized = snapshot.model_dump(mode="json")
                for lane_id, lane in snapshot.lanes.items():
                    await record.event_log.publish(
                        "lane_update",
                        {
                            "lane_id": lane_id,
                            "state": lane.state,
                            "attempts": lane.attempts,
                            "elapsed_ms": lane.elapsed_ms,
                            "status": lane.status,
                            "error": lane.error,
                            "activity": (
                                lane.activity.model_dump(mode="json")
                                if lane.activity is not None
                                else None
                            ),
                            "session": serialized,
                        },
                    )
                await self._cleanup_retry_sleep(delay)
                delay = min(delay * 2, maximum_delay)
        except asyncio.CancelledError:
            raise
        finally:
            if record.connection_spike_cleanup_task is task:
                record.connection_spike_cleanup_task = None

    async def _abandon_connection_spike_cleanup_retry(
        self,
        record: SessionRecord,
        attempts: int,
    ) -> None:
        """Stop retrying Round 5 cleanup, and say so where the operator looks.

        The ring lease stays held: the artifacts were not proved gone, and
        handing back authority over resources that may still exist is worse than
        keeping the lockout. What changes is that the lockout is now legible and
        has a way out -- a `failed` towel is what both the "Retry cleanup"
        button and `start_towel`'s server-side retry branch key on.

        The diagnostic is not written here. It comes from
        `round5_cleanup_owed.leaked_proxy_sentence`, which `/readyz` reads too,
        because the sentence written here used to say only that cleanup "did not
        converge" -- true, and silent about the one consequence that costs money.
        Naming the resource in two places independently is how this project
        already produced two warnings that disagreed.
        """

        owed = self._note_round5_proxy_at_risk(
            record,
            attempts=attempts,
            still_retrying=False,
        )
        diagnostic = (
            owed.detail
            if owed is not None
            else (
                f"Round 5 backstage cleanup did not converge after {attempts} "
                "automatic attempts. The ring stays held until cleanup is "
                "confirmed; retry cleanup."
            )
        )
        logger.error(
            "Round 5 automatic cleanup abandoned session=%s attempts=%d",
            record.snapshot.id,
            attempts,
        )
        async with record.lock:
            setup = record.snapshot.round5_setup
            if setup is not None:
                setup.cleanup_retryable = True
                # Recorded outside the FAILED guard below, deliberately. A Round
                # 5 bout that verified keeps its win -- a tidy-up failure is not
                # evidence the setup did not verify -- but the proxy it built may
                # still be billing, and the guard used to leave that fact
                # nowhere: not on the snapshot, so not in the receipt and not on
                # the operator's screen either. The only trace was the log line
                # above.
                setup.cleanup_failure = diagnostic
                if record.snapshot.state == SessionState.FAILED:
                    setup.state = RoundFiveSetupState.CLEANUP_FAILED
                    setup.failure = diagnostic
            towel = record.snapshot.towel
            if towel is not None:
                towel.state = TowelState.FAILED
                towel.cleanup_failure = diagnostic
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "towel_update" if snapshot.towel is not None else "cleanup_update",
            {"session": snapshot.model_dump(mode="json")},
        )

    async def _cleanup_connection_spike(self, record: SessionRecord) -> bool:
        engine = record.connection_spike_engine
        arm = record.connection_spike_arm
        cleanup = getattr(engine, "cancel_and_cleanup", None) if engine is not None else None
        setup_cleanup = (
            getattr(engine, "cancel_setup_and_settle", None) if engine is not None else None
        )
        ok = True
        try:
            if cleanup is not None and arm is not None:
                task = asyncio.create_task(cleanup(arm))
                await asyncio.shield(task)
        except Exception:
            ok = False
        try:
            if setup_cleanup is not None:
                task = asyncio.create_task(setup_cleanup(record.snapshot.id))
                await asyncio.shield(task)
        except Exception:
            ok = False
        if ok:
            record.connection_spike_setup_result = None
        return ok

    @staticmethod
    def _round_five_value(source: object, name: str, default: object = None) -> object:
        if isinstance(source, Mapping):
            return source.get(name, default)
        return getattr(source, name, default)

    @classmethod
    def _round_five_lanes(cls, result: object) -> dict[str, object]:
        raw = cls._round_five_value(result, "lanes", {})
        if isinstance(raw, Mapping):
            return {str(key): value for key, value in raw.items()}
        if isinstance(raw, (list, tuple)):
            return {
                str(cls._round_five_value(item, "lane_id")): item
                for item in raw
                if cls._round_five_value(item, "lane_id") is not None
            }
        return {}

    @classmethod
    def _round_five_evidence(cls, lane: object) -> dict[str, object]:
        latencies = cls._round_five_value(lane, "successful_latency_ms", ())
        if not isinstance(latencies, (list, tuple)):
            latencies = ()
        return {
            "scheduled_clients": cls._round_five_count(
                cls._round_five_value(lane, "scheduled_clients")
            ),
            "terminal_clients": cls._round_five_count(
                cls._round_five_value(lane, "terminal_clients")
            ),
            "successful_clients": cls._round_five_count(
                cls._round_five_value(lane, "successful_clients")
            ),
            "error_clients": cls._round_five_count(cls._round_five_value(lane, "error_clients")),
            "successful_latency_ms": list(latencies),
            "witness_verified_clients": cls._round_five_count(
                cls._round_five_value(lane, "witness_verified_clients")
            ),
            "unique_backend_pids": cls._round_five_count(
                cls._round_five_value(lane, "unique_backend_pids")
            ),
            "peak_backend_sessions": cls._round_five_count(
                cls._round_five_value(lane, "peak_backend_sessions")
            ),
        }

    @staticmethod
    def _round_five_count(value: object) -> int:
        return (
            value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else -1
        )

    @staticmethod
    def _round_five_number(value: object) -> float | None:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            return number if math.isfinite(number) and number >= 0 else None
        return None

    @classmethod
    def _round_five_lane_valid(
        cls,
        raw: object,
        evidence: Mapping[str, object],
    ) -> bool:
        latencies = evidence["successful_latency_ms"]
        gates = cls._round_five_value(raw, "gates")
        gate_passed = cls._round_five_value(gates, "passed")
        if gate_passed is None:
            failures = cls._round_five_value(gates, "failures", ())
            gate_passed = (
                bool(gates)
                and all(
                    bool(cls._round_five_value(gates, name))
                    for name in ("cleanup", "fairness", "contracts")
                )
                and not failures
            )
        return bool(cls._round_five_value(raw, "verified", gate_passed)) and (
            evidence["scheduled_clients"] == _ROUND_FIVE_SCHEDULED_CLIENTS
            and evidence["terminal_clients"] == _ROUND_FIVE_SCHEDULED_CLIENTS
            and evidence["successful_clients"] + evidence["error_clients"]
            == _ROUND_FIVE_SCHEDULED_CLIENTS
            and isinstance(latencies, list)
            and len(latencies) == evidence["successful_clients"]
            and evidence["witness_verified_clients"] == _ROUND_FIVE_WITNESS_CLIENTS
            and 1 <= evidence["unique_backend_pids"] < _ROUND_FIVE_WITNESS_CLIENTS
            and 1 <= evidence["peak_backend_sessions"] < _ROUND_FIVE_WITNESS_CLIENTS
        )

    @staticmethod
    def _round_five_has_timed_setup(engine: object | None) -> bool:
        if engine is None:
            return False
        marker = getattr(engine, "has_timed_setup", None)
        if marker is not None:
            return bool(marker)
        return callable(getattr(engine, "setup", None))

    @staticmethod
    def _new_round_five_setup(snapshot: SessionSnapshot) -> RoundFiveSetupSnapshot:
        return RoundFiveSetupSnapshot(
            lanes={
                lane_id: RoundFiveSetupLaneSnapshot(id=lane_id, name=lane.name)
                for lane_id, lane in snapshot.lanes.items()
            }
        )

    @staticmethod
    def _round_five_public_phase(value: object) -> str:
        phase = str(getattr(value, "value", value)).lower()
        if "burst" in phase or "witness" in phase or "cleanup" in phase:
            return "burst"
        if phase in {
            "validating_host",
            "creating_proxy_network",
            "freezing_proxy_egress",
            "authorizing_proxy_ingress",
            "authorizing_proxy_egress",
            "authorizing_runner_egress",
            "authorizing_rds_ingress",
            "creating_proxy",
            "freezing_proxy_settings",
            "registering_proxy_target",
            "waiting_for_proxy_target",
            "resuming_database",
            "verifying_topology",
            "verifying_transaction",
            "setup_stop",
        }:
            return phase
        return "setup"

    @staticmethod
    def _round_five_setup_status(lane_id: object, phase: str) -> str:
        generic = "Preparing and verifying the per-bout pooling path"
        if lane_id == "lakebase":
            return {
                "validating_host": "Checking the built-in Lakebase pool",
                "verifying_transaction": "Verifying the built-in Lakebase pool",
                "setup_stop": "Built-in Lakebase pool verified",
            }.get(phase, generic)
        if lane_id == "competitor":
            network_phases = {
                "creating_proxy_network",
                "freezing_proxy_egress",
                "authorizing_proxy_ingress",
                "authorizing_proxy_egress",
                "authorizing_runner_egress",
                "authorizing_rds_ingress",
            }
            if phase in network_phases:
                return "Building the per-bout Proxy network"
            return {
                "creating_proxy": "AWS is creating a new RDS Proxy",
                "freezing_proxy_settings": "Configuring the new RDS Proxy",
                "registering_proxy_target": "Registering the database with the Proxy",
                "waiting_for_proxy_target": (
                    "Waiting for Proxy and database target to become available"
                ),
                "resuming_database": "Waking Aurora for the new Proxy",
                "verifying_topology": "Checking the exact Proxy and database bindings",
                "verifying_transaction": "Verifying an exact transaction through the Proxy",
                "setup_stop": "Per-bout Proxy path verified",
            }.get(phase, generic)
        return generic

    @staticmethod
    def _round_five_public_evidence_fact(
        value: object,
    ) -> RoundFiveSetupEvidenceSnapshot | None:
        key = str(getattr(value, "key", "")).strip()
        fact = getattr(value, "value", None)
        lowered = key.lower()
        forbidden = (
            not key
            or len(key) > 80
            or key == "id"
            or key.endswith("_id")
            or any(
                token in lowered
                for token in (
                    "credential",
                    "password",
                    "secret",
                    "token",
                    "fenc",
                    "journal",
                    "nonce",
                    "command",
                    "arn",
                    "endpoint",
                    "host",
                )
            )
        )
        if forbidden or (fact is not None and not isinstance(fact, (str, int, float, bool))):
            return None
        if isinstance(fact, str):
            unsafe = fact.lower()
            if len(fact) > 160 or any(
                token in unsafe for token in ("arn:", "password", "secret", "token", "-----begin")
            ):
                return None
        return RoundFiveSetupEvidenceSnapshot(key=key, value=fact)

    @classmethod
    def _round_five_public_gate(
        cls,
        value: object,
    ) -> RoundFiveSetupGateSnapshot | None:
        if value is None:
            return None
        gate_id = str(cls._round_five_value(value, "gate_id", "")).strip()
        if (
            not gate_id
            or len(gate_id) > 80
            or not all(character.isalnum() or character in "_-" for character in gate_id)
        ):
            return None
        expected_raw = cls._round_five_value(value, "expected", ())
        observed_raw = cls._round_five_value(value, "observed", ())
        if not isinstance(expected_raw, (list, tuple)) or not isinstance(
            observed_raw, (list, tuple)
        ):
            return None
        expected = [cls._round_five_public_evidence_fact(item) for item in expected_raw]
        observed = [cls._round_five_public_evidence_fact(item) for item in observed_raw]
        if (
            not expected
            or any(item is None for item in expected)
            or any(item is None for item in observed)
        ):
            return None
        return RoundFiveSetupGateSnapshot(
            gate_id=gate_id,
            expected=[item for item in expected if item is not None],
            observed=[item for item in observed if item is not None],
            exact=bool(cls._round_five_value(value, "exact", False)),
        )

    @classmethod
    def _round_five_finalize_setup(
        cls,
        source: object,
        downstream_lanes: Mapping[str, object],
    ) -> object | None:
        if source is None:
            return None
        from .connection_spike import (
            SetupPhaseResult,
            compare_setup_lanes,
            finalize_setup_phase,
        )

        arm = cls._round_five_value(source, "arm")
        observations = cls._round_five_value(source, "observations")
        if arm is not None and isinstance(observations, (list, tuple)):
            return finalize_setup_phase(arm, observations, downstream_lanes)
        if not isinstance(source, SetupPhaseResult):
            return None
        validated = set(downstream_lanes) == set(source.lanes) and all(
            bool(cls._round_five_value(lane, "verified", False))
            for lane in downstream_lanes.values()
        )
        lane_ids = tuple(source.lanes)
        if len(lane_ids) != 2:
            return None
        comparison = compare_setup_lanes(
            source.lanes[lane_ids[0]],
            source.lanes[lane_ids[1]],
            downstream_validated=validated,
        )
        return SetupPhaseResult(
            contract_sha256=source.contract_sha256,
            t0_ns=source.t0_ns,
            deadline_ns=source.deadline_ns,
            workflow_launch_skew_ms=source.workflow_launch_skew_ms,
            lanes=source.lanes,
            downstream_validated=validated,
            comparison=comparison,
        )

    @classmethod
    def _round_five_setup_snapshot(
        cls,
        snapshot: SessionSnapshot,
        result: object | None,
        *,
        terminal: bool,
    ) -> RoundFiveSetupSnapshot:
        public = cls._new_round_five_setup(snapshot)
        public.state = RoundFiveSetupState.FAILED if terminal else RoundFiveSetupState.RUNNING
        if result is None:
            return public
        raw_lanes = cls._round_five_value(result, "lanes", {})
        if not isinstance(raw_lanes, Mapping) or set(raw_lanes) != set(public.lanes):
            return public
        safe_validated = True
        for lane_id, raw in raw_lanes.items():
            lane = public.lanes[str(lane_id)]
            status_value = cls._round_five_value(raw, "status", "failed")
            status = str(getattr(status_value, "value", status_value))
            gate = cls._round_five_public_gate(cls._round_five_value(raw, "stop_gate_evidence"))
            verified = bool(cls._round_five_value(raw, "verified", False)) and gate is not None
            lane.state = (
                RoundFiveSetupState.VERIFIED
                if verified
                else RoundFiveSetupState.TOWELLED
                if status == "towelled"
                else RoundFiveSetupState.FAILED
            )
            lane.setup_elapsed_ms = cls._round_five_number(
                cls._round_five_value(raw, "setup_elapsed_ms")
            )
            lane.stop_gate_evidence = gate
            lane.verified = verified
            lane.status = (
                "Setup stop gate verified" if verified else "Setup stop gate did not verify"
            )
            lane.error = None if verified else "Setup verification failed"
            safe_validated = safe_validated and verified
        public.workflow_launch_skew_ms = cls._round_five_number(
            cls._round_five_value(result, "workflow_launch_skew_ms")
        )
        public.setup_validated = (
            bool(cls._round_five_value(result, "setup_validated", False)) and safe_validated
        )
        public.downstream_validated = bool(
            cls._round_five_value(result, "downstream_validated", False)
        )
        return public

    @classmethod
    def _round_five_setup_comparison(
        cls,
        result: object,
        competitor_name: str,
    ) -> ComparisonSnapshot | None:
        if not bool(cls._round_five_value(result, "setup_validated", False)) or not bool(
            cls._round_five_value(result, "downstream_validated", False)
        ):
            return None
        raw = cls._round_five_value(result, "comparison")
        outcome_value = cls._round_five_value(raw, "outcome", "")
        outcome = str(getattr(outcome_value, "value", outcome_value))
        winner = cls._round_five_value(raw, "winner_lane_id")
        if outcome == "tie":
            return ComparisonSnapshot(
                kind=ComparisonKind.TIE,
                detail=(
                    "Both setup lanes tied on raw elapsed time; both secondary bursts, "
                    "witnesses, and cleanup gates verified."
                ),
            )
        margin_ms = cls._round_five_number(cls._round_five_value(raw, "margin_ms"))
        if winner not in {"lakebase", "competitor"} or margin_ms is None or margin_ms <= 0:
            return None
        return ComparisonSnapshot(
            kind=ComparisonKind.MEASURED,
            winner_lane_id=str(winner),
            margin=MetricValue(
                spec_id="setup_elapsed_ms",
                lane_id=str(winner),
                value=margin_ms,
                display_value=f"{margin_ms:.2f} ms",
            ),
            detail=(
                f"{'Lakebase' if winner == 'lakebase' else competitor_name} wins "
                "primary setup by "
                f"{margin_ms:.2f} ms; both secondary bursts, witnesses, and cleanup "
                "gates verified."
            ),
        )

    @staticmethod
    def _round_five_metrics(
        snapshot: SessionSnapshot,
        setup: RoundFiveSetupSnapshot,
    ) -> list[MetricValue]:
        metrics: list[MetricValue] = []
        for lane_id, lane in snapshot.lanes.items():
            setup_elapsed_ms = setup.lanes[lane_id].setup_elapsed_ms
            assert setup_elapsed_ms is not None
            metrics.extend(
                [
                    MetricValue(
                        spec_id="setup_elapsed_ms",
                        lane_id=lane_id,
                        value=setup_elapsed_ms,
                        display_value=f"{setup_elapsed_ms:.2f} ms",
                    ),
                    MetricValue(
                        spec_id="successful_clients",
                        lane_id=lane_id,
                        value=lane.successes,
                        display_value=str(lane.successes),
                    ),
                    MetricValue(
                        spec_id="application_p99_ms",
                        lane_id=lane_id,
                        value=lane.p99_ms if lane.p99_ms is not None else "N/A",
                        display_value=(
                            f"{lane.p99_ms:.2f} ms" if lane.p99_ms is not None else "N/A"
                        ),
                    ),
                    MetricValue(
                        spec_id="error_clients",
                        lane_id=lane_id,
                        value=lane.errors,
                        display_value=str(lane.errors),
                    ),
                ]
            )
        return metrics

    async def _run_model_score(self, record: SessionRecord) -> None:
        engine = record.model_score_engine
        arm = record.model_score_arm
        update = record.model_score_pending_update
        if engine is None or arm is None or update is None:
            await self._fail(record, "The Managed Sync proof must be armed again.")
            return

        async with record.lock:
            started_at = datetime.now(UTC)
            record.snapshot.state = SessionState.RUNNING
            record.snapshot.run_started_at = started_at
            record.run_started_monotonic_ns = self._clock_ns()
            record.snapshot.updated_at = started_at
            lakebase = record.snapshot.lanes["lakebase"]
            lakebase.state = LaneState.CONNECTING
            lakebase.status = "Committing the run-owned model score update"
            lakebase.attempts = 0
            lakebase.activity = LaneActivity(phase=ModelScorePhase.COMMITTING_SOURCE)
            running_snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_started",
            {
                "state": SessionState.RUNNING,
                "lanes": ["lakebase"],
                "session": running_snapshot.model_dump(mode="json"),
            },
        )

        async def on_progress(progress: ModelScoreProgress) -> None:
            await self._apply_model_score_progress(record, progress, redo=False)

        try:
            result = await engine.run(arm, update, on_progress)
        except asyncio.CancelledError:
            raise
        except ModelScoreError as exc:
            await self._await_model_score_terminal(
                record,
                self._finish_model_score_failure(record, str(exc)),
            )
            return
        except Exception as exc:
            self._log_bout_refusal(record, exc, round_number=4)
            await self._await_model_score_terminal(
                record,
                self._finish_model_score_failure(
                    record,
                    "The live Managed Sync proof failed unexpectedly.",
                ),
            )
            return
        await self._await_model_score_terminal(
            record,
            self._finish_model_score(record, result),
        )

    async def _apply_model_score_progress(
        self,
        record: SessionRecord,
        progress: ModelScoreProgress,
        *,
        redo: bool,
    ) -> None:
        phase_states = {
            ModelScorePhase.PREFLIGHT: LaneState.SEALED,
            ModelScorePhase.ARMED: LaneState.SEALED,
            ModelScorePhase.COMMITTING_SOURCE: LaneState.CONNECTING,
            ModelScorePhase.WAITING_SYNC: LaneState.VERIFYING,
            ModelScorePhase.READING_APPLICATION: LaneState.VERIFYING,
            ModelScorePhase.VERIFIED: LaneState.VERIFIED,
            ModelScorePhase.FAILED: LaneState.FAILED,
        }
        async with record.lock:
            if not redo and record.snapshot.towel is not None:
                return
            target = record.snapshot.redo if redo else record.snapshot
            if redo:
                if not isinstance(target, RedoSnapshot):
                    return
                lane = target.lanes["lakebase"]
            else:
                lane = record.snapshot.lanes["lakebase"]
            lane.state = phase_states[progress.phase]
            lane.status = progress.status
            lane.attempts = progress.attempt or lane.attempts
            lane.activity = LaneActivity(phase=progress.phase)
            if progress.elapsed_ms is not None:
                lane.elapsed_ms = (
                    progress.elapsed_ms
                    if progress.phase == ModelScorePhase.VERIFIED
                    else max(lane.elapsed_ms or 0.0, progress.elapsed_ms)
                )
            if lane.state == LaneState.VERIFIED and lane.verified_at is None:
                lane.verified_at = datetime.now(UTC)
            record.snapshot.updated_at = datetime.now(UTC)
            activity = lane.activity.model_dump(mode="json")
        await record.event_log.publish(
            "redo_lane_update" if redo else "lane_update",
            {
                "lane_id": "lakebase",
                "state": phase_states[progress.phase],
                "attempts": progress.attempt or lane.attempts,
                "elapsed_ms": lane.elapsed_ms,
                "status": progress.status,
                "activity": activity,
            },
        )

    async def _finish_model_score(
        self,
        record: SessionRecord,
        result: ModelScoreRunResult,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
        lease_outcome = await self._settle_model_score_terminal_lease(record)
        if lease_outcome == "lost":
            await self._finish_model_score_lease_loss(record, redo=False)
            return
        async with record.lock:
            if record.model_score_terminal_published or record.snapshot.towel is not None:
                return
            record.model_score_result = result
            record.model_score_pending_update = None
            self._apply_model_score_result(record.snapshot, result.initial)
            record.snapshot.state = SessionState.VERIFIED
            record.snapshot.failure = None
            record.snapshot.remembered_result = _ROUND_FOUR_REMEMBERED_RESULT
            record.snapshot.redo = RedoSnapshot(
                state=RedoState.READY,
                lanes=self._new_model_score_lanes(record.snapshot),
                metric_specs=[item.model_copy(deep=True) for item in record.snapshot.metric_specs],
            )
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            record.model_score_terminal_published = True
            engine = record.model_score_engine
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_finished",
            {"state": SessionState.VERIFIED, "session": snapshot.model_dump(mode="json")},
        )
        self._schedule_round_settlement(
            record,
            getattr(engine, "settle_and_restore_baseline", None),
            label="Round 4",
        )

    async def _finish_model_score_failure(
        self,
        record: SessionRecord,
        message: str,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
        lease_outcome = await self._settle_model_score_terminal_lease(record)
        if lease_outcome == "lost":
            await self._finish_model_score_lease_loss(record, redo=False)
            return
        async with record.lock:
            if record.model_score_terminal_published or record.snapshot.towel is not None:
                return
            failed_at = datetime.now(UTC)
            lane = record.snapshot.lanes["lakebase"]
            lane.state = LaneState.FAILED
            lane.error = message
            lane.status = "Managed Sync proof could not be verified"
            lane.activity = LaneActivity(phase=ModelScorePhase.FAILED)
            record.model_score_pending_update = None
            record.model_score_result = None
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            # The one funnel every Round 4 initial-proof failure passes through,
            # including the typed `ModelScoreError` path that carries the real
            # reason and had no log of its own.
            self._log_lane_refusals(record, round_number=4)
            record.snapshot.remembered_result = None
            record.snapshot.updated_at = failed_at
            record.armed_at_monotonic = None
            record.model_score_terminal_published = True
            engine = record.model_score_engine
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )
        self._schedule_round_settlement(
            record,
            getattr(engine, "settle_and_restore_baseline", None),
            label="Round 4",
        )

    async def _redo_model_score(
        self,
        record: SessionRecord,
        engine: ModelScoreEngine,
        arm: ModelScoreArm,
        initial: ModelScoreRunResult,
        update: ModelScoreUpdate,
    ) -> None:
        async def on_progress(progress: ModelScoreProgress) -> None:
            await self._apply_model_score_progress(record, progress, redo=True)

        try:
            result = await engine.redo(arm, initial, update, on_progress)
        except asyncio.CancelledError:
            raise
        except ModelScoreError as exc:
            await self._await_model_score_terminal(
                record,
                self._fail_model_score_redo(record, str(exc)),
            )
            return
        except Exception as exc:
            self._log_bout_refusal(record, exc, round_number=4)
            await self._await_model_score_terminal(
                record,
                self._fail_model_score_redo(
                    record,
                    "The live Managed Sync re-do failed unexpectedly.",
                ),
            )
            return
        await self._await_model_score_terminal(
            record,
            self._finish_model_score_redo(record, result),
        )

    async def _finish_model_score_redo(
        self,
        record: SessionRecord,
        result: ModelScoreRunResult,
    ) -> None:
        proof = result.redo
        if proof is None:
            await self._fail_model_score_redo(
                record,
                "The Managed Sync engine returned no re-do proof.",
            )
            return
        lease_outcome = await self._settle_model_score_terminal_lease(record)
        if lease_outcome == "lost":
            await self._finish_model_score_lease_loss(record, redo=True)
            return
        async with record.lock:
            redo = record.snapshot.redo
            if (
                record.model_score_terminal_published
                or redo is None
                or redo.state != RedoState.RUNNING
            ):
                return
            self._apply_model_score_result(redo, proof)
            redo.state = RedoState.VERIFIED
            redo.failure = None
            redo.completed_at = datetime.now(UTC)
            record.model_score_result = result
            record.model_score_pending_update = None
            record.snapshot.state = SessionState.VERIFIED
            record.snapshot.failure = None
            record.snapshot.updated_at = redo.completed_at
            record.model_score_terminal_published = True
            engine = record.model_score_engine
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "redo_finished",
            {"session": snapshot.model_dump(mode="json")},
        )
        self._schedule_round_settlement(
            record,
            getattr(engine, "settle_and_restore_baseline", None),
            label="Round 4",
        )

    async def _fail_model_score_redo(
        self,
        record: SessionRecord,
        message: str,
    ) -> None:
        lease_outcome = await self._settle_model_score_terminal_lease(record)
        if lease_outcome == "lost":
            await self._finish_model_score_lease_loss(record, redo=True)
            return
        async with record.lock:
            redo = record.snapshot.redo
            if (
                record.model_score_terminal_published
                or redo is None
                or redo.state != RedoState.RUNNING
            ):
                return
            failed_at = datetime.now(UTC)
            redo.state = RedoState.FAILED
            redo.failure = message
            redo.completed_at = failed_at
            lane = redo.lanes["lakebase"]
            lane.state = LaneState.FAILED
            lane.error = message
            lane.status = "Managed Sync re-do could not be verified"
            lane.activity = LaneActivity(phase=ModelScorePhase.FAILED)
            record.model_score_pending_update = None
            record.snapshot.state = SessionState.VERIFIED
            record.snapshot.failure = None
            record.snapshot.updated_at = failed_at
            record.model_score_terminal_published = True
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "redo_failed",
            {"message": message, "session": snapshot.model_dump(mode="json")},
        )

    async def _finish_model_score_lease_loss(
        self,
        record: SessionRecord,
        *,
        redo: bool,
    ) -> None:
        async with record.lock:
            if record.model_score_terminal_published or (
                not redo and record.snapshot.towel is not None
            ):
                return
            failed_at = datetime.now(UTC)
            if redo:
                redo_snapshot = record.snapshot.redo
                if redo_snapshot is None or redo_snapshot.state != RedoState.RUNNING:
                    return
                message = "Managed Sync re-do lease was lost"
                redo_snapshot.state = RedoState.FAILED
                redo_snapshot.failure = message
                redo_snapshot.completed_at = failed_at
                lane = redo_snapshot.lanes["lakebase"]
                lane.state = LaneState.FAILED
                lane.error = message
                lane.status = message
                lane.activity = LaneActivity(phase=ModelScorePhase.FAILED)
                record.snapshot.state = SessionState.VERIFIED
                record.snapshot.failure = None
                event = "redo_failed"
            else:
                message = "Managed Sync proof lease was lost before terminal verification."
                lane = record.snapshot.lanes["lakebase"]
                lane.state = LaneState.FAILED
                lane.error = message
                lane.status = message
                lane.activity = LaneActivity(phase=ModelScorePhase.FAILED)
                record.model_score_result = None
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = message
                record.snapshot.remembered_result = None
                event = "session_failed"
            record.model_score_pending_update = None
            record.snapshot.updated_at = failed_at
            record.armed_at_monotonic = None
            record.model_score_terminal_published = True
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            event,
            {
                "state": snapshot.state,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )

    @staticmethod
    def _new_model_score_lanes(snapshot: SessionSnapshot) -> dict[str, LaneSnapshot]:
        competitor = snapshot.lanes["competitor"]
        return {
            "lakebase": LaneSnapshot(id="lakebase", name="Lakebase"),
            "competitor": LaneSnapshot(
                id="competitor",
                name=competitor.name,
                state=LaneState.NOT_SUPPORTED,
                status=competitor.status,
                activity=LaneActivity(phase="not_supported"),
                evidence=dict(competitor.evidence),
            ),
        }

    @staticmethod
    def _apply_model_score_result(
        snapshot: SessionSnapshot | RedoSnapshot,
        proof: ModelScoreProofResult,
    ) -> None:
        lane = snapshot.lanes["lakebase"]
        lane.state = LaneState.VERIFIED
        lane.elapsed_ms = proof.application_read_elapsed_ms
        lane.attempts = proof.poll_attempts
        lane.status = "Exact committed version and fresh Postgres row verified"
        lane.error = None
        lane.verified_at = datetime.now(UTC)
        lane.activity = LaneActivity(phase=ModelScorePhase.VERIFIED)
        lane.evidence = {
            "primary_key": proof.update.entity_id,
            "score": proof.update.score,
            "model_version": proof.update.model_version,
            "proof_nonce": proof.update.proof_nonce,
            "delta_version": proof.source_version,
            "status_delta_commit_time": proof.delta_commit_time,
            "sync_end_time": proof.sync_end_time,
            "verified_row": {
                "primary_key": proof.verified_row.entity_id,
                "score": proof.verified_row.score,
                "model_version": proof.verified_row.model_version,
                "proof_nonce": proof.verified_row.proof_nonce,
            },
        }
        snapshot.metrics = [
            MetricValue(
                spec_id="managed_availability_ms",
                lane_id="lakebase",
                value=proof.managed_availability_ms,
                display_value=f"{proof.managed_availability_ms:.2f} ms",
            ),
            MetricValue(
                spec_id="application_proof_elapsed_ms",
                lane_id="lakebase",
                value=proof.application_read_elapsed_ms,
                display_value=f"{proof.application_read_elapsed_ms:.2f} ms",
            ),
            MetricValue(
                spec_id="delta_commit_version",
                lane_id="lakebase",
                value=proof.source_version,
                display_value=str(proof.source_version),
            ),
            MetricValue(
                spec_id="exact_row_verified",
                lane_id="lakebase",
                value=True,
                display_value="Verified",
            ),
        ]
        snapshot.comparison = ComparisonSnapshot(
            kind=ComparisonKind.CAPABILITY_GAP,
            winner_lane_id="lakebase",
            margin=None,
            detail=_ROUND_FOUR_COMPARISON_DETAIL,
        )

    async def _run(self, record: SessionRecord) -> None:
        assert record.live_targets is not None
        await record.event_log.publish("run_preparing", {"state": SessionState.ARMED})
        loop = asyncio.get_running_loop()
        if (
            record.armed_at_monotonic is None
            or loop.time() - record.armed_at_monotonic > self._armed_ttl
        ):
            await self._fail(record, "Armed proof expired. Verify scale zero and arm again.")
            return
        eligible_targets = tuple(
            target
            for target in record.live_targets
            if record.snapshot.lanes[target.id].state != LaneState.NOT_SUPPORTED
        )
        try:
            prepared = await asyncio.gather(*(target.prepare() for target in eligible_targets))
        except (TargetConfigurationError, OSError) as exc:
            self._log_bout_refusal(record, exc, round_number=1)
            await self._fail(record, "Live database credentials could not be prepared.")
            return
        except Exception as exc:
            self._log_bout_refusal(record, exc, round_number=1)
            await self._fail(record, "The live targets could not be prepared.")
            return

        # Credential generation and secret retrieval are control-plane-only. Recheck
        # both systems after that work so the last meaningful action before the
        # barrier is fresh proof that each eligible lane is still at scale zero.
        checks = await asyncio.gather(
            *(target.assert_armed() for target in record.live_targets),
            return_exceptions=True,
        )
        revalidation_error = next(
            (item for item in checks if isinstance(item, TargetConfigurationError)), None
        )
        if revalidation_error is None:
            revalidation_error = next(
                (item for item in checks if isinstance(item, TargetNotArmedError)), None
            )
        if revalidation_error is not None:
            await self._fail(
                record,
                f"Start state changed before the bell: {revalidation_error}",
            )
            return
        if not all(isinstance(item, dict) for item in checks):
            await self._fail(record, "The live start state could not be revalidated.")
            return

        async with record.lock:
            started_at = datetime.now(UTC)
            record.snapshot.state = SessionState.RUNNING
            record.snapshot.run_started_at = started_at
            record.run_started_monotonic_ns = self._clock_ns()
            record.snapshot.updated_at = started_at
            for target in eligible_targets:
                lane = record.snapshot.lanes[target.id]
                lane.state = LaneState.CONNECTING
                lane.status = "Waiting for bell"
                lane.activity = LaneActivity(
                    phase="connecting",
                    wire_call=_ROUND_ONE_TRANSACTION_WIRE_CALL,
                )
            running_snapshot = record.snapshot.model_copy(deep=True)
        nonce = str(uuid4())
        expected_value = secrets.token_hex(24)
        await record.event_log.publish(
            "run_started",
            {
                "state": SessionState.RUNNING,
                "lanes": [target.id for target in eligible_targets],
                "session": running_snapshot.model_dump(mode="json"),
            },
        )

        async def on_lane_event(lane_id: str, state: str, payload: dict[str, object]) -> None:
            async with record.lock:
                if record.snapshot.towel is not None:
                    return
                lane = record.snapshot.lanes[lane_id]
                lane.state = LaneState(state)
                lane.activity = LaneActivity(
                    phase=state,
                    wire_call=_ROUND_ONE_TRANSACTION_WIRE_CALL,
                )
                lane.attempts = int(payload.get("attempts", lane.attempts))
                lane.elapsed_ms = payload.get("elapsed_ms")  # type: ignore[assignment]
                lane.status = str(payload.get("status", lane.status))
                if lane.state == LaneState.VERIFIED and lane.verified_at is None:
                    lane.verified_at = datetime.now(UTC)
            await record.event_log.publish(
                "lane_update",
                {
                    "lane_id": lane_id,
                    "state": state,
                    **payload,
                    "activity": lane.activity.model_dump(mode="json"),
                },
            )

        try:
            result = await self._verifier.run(
                targets=tuple(prepared),
                nonce=nonce,
                expected_value=expected_value,
                on_event=on_lane_event,
                stop_event=record.towel_stop_event,
            )
        except VerifierStopped:
            if record.snapshot.towel is not None:
                return
            await self._fail(record, "The live verification run stopped unexpectedly.")
            return
        except Exception as exc:
            if record.snapshot.towel is not None:
                return
            self._log_bout_refusal(record, exc, round_number=1)
            await self._fail(record, "The live verification run failed unexpectedly.")
            return
        await self._finish(record, result)

    async def _run_safe_change(self, record: SessionRecord) -> None:
        engine = record.safe_change_engine
        arm = record.safe_change_arm
        if engine is None or arm is None:
            await self._fail(record, "The isolated-change proof must be armed again.")
            return
        await record.event_log.publish("run_preparing", {"state": SessionState.ARMED})

        phase_states = {
            SafeChangePhase.PREFLIGHT: LaneState.SEALED,
            SafeChangePhase.ARMED: LaneState.SEALED,
            SafeChangePhase.CREATING: LaneState.CONNECTING,
            SafeChangePhase.MIGRATING: LaneState.VERIFYING,
            SafeChangePhase.VERIFYING_APPLICATION: LaneState.VERIFYING,
            SafeChangePhase.VERIFYING_SOURCE: LaneState.VERIFYING,
            SafeChangePhase.VERIFIED: LaneState.VERIFIED,
            SafeChangePhase.FAILED: LaneState.FAILED,
        }

        async def on_progress(progress: SafeChangeProgress) -> None:
            started_snapshot: SessionSnapshot | None = None
            async with record.lock:
                if record.snapshot.towel is not None:
                    return
                if (
                    progress.phase not in {SafeChangePhase.PREFLIGHT, SafeChangePhase.ARMED}
                    and record.snapshot.state != SessionState.RUNNING
                ):
                    started_at = datetime.now(UTC)
                    record.snapshot.state = SessionState.RUNNING
                    record.snapshot.run_started_at = started_at
                    record.run_started_monotonic_ns = self._clock_ns()
                    record.snapshot.updated_at = started_at
                    for candidate in record.snapshot.lanes.values():
                        candidate.state = LaneState.CONNECTING
                        candidate.status = "Creating isolated environment"
                        candidate.activity = LaneActivity(
                            phase=SafeChangePhase.CREATING,
                            wire_call=None,
                        )
                    started_snapshot = record.snapshot.model_copy(deep=True)
                lane = record.snapshot.lanes[progress.lane_id]
                lane.state = phase_states[progress.phase]
                lane.status = progress.status
                lane.attempts = max(1, lane.attempts)
                if progress.elapsed_ms is not None:
                    lane.elapsed_ms = progress.elapsed_ms
                lane.error = progress.error
                lane.activity = LaneActivity(
                    phase=progress.phase,
                    wire_call=progress.wire_call,
                )
                if lane.state == LaneState.VERIFIED and lane.verified_at is None:
                    lane.verified_at = datetime.now(UTC)
                record.snapshot.updated_at = datetime.now(UTC)
            if started_snapshot is not None:
                await record.event_log.publish(
                    "run_started",
                    {
                        "state": SessionState.RUNNING,
                        "lanes": ["lakebase", "competitor"],
                        "session": started_snapshot.model_dump(mode="json"),
                    },
                )
            await record.event_log.publish(
                "lane_update",
                {
                    "lane_id": progress.lane_id,
                    "state": phase_states[progress.phase],
                    "attempts": 1,
                    "elapsed_ms": progress.elapsed_ms,
                    "status": progress.status,
                    "error": progress.error,
                    "activity": lane.activity.model_dump(mode="json"),
                },
            )

        try:
            result = await engine.run(arm, on_progress)
        except SafeChangeNotArmedError as exc:
            if record.snapshot.towel is not None:
                return
            await self._fail(record, str(exc))
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if record.snapshot.towel is not None:
                return
            self._log_bout_refusal(record, exc, round_number=2)
            await self._fail(record, "The live isolated-change proof failed unexpectedly.")
            return
        await self._finish_safe_change(record, result)

    async def _run_recovery(
        self,
        record: SessionRecord,
        stop_control: RecoveryStopControl | None,
    ) -> None:
        engine = record.recovery_engine
        arm = record.recovery_arm
        if engine is None or arm is None:
            await self._fail(record, "The point-in-time recovery proof must be armed again.")
            return
        await record.event_log.publish("run_preparing", {"state": SessionState.ARMED})
        phase_states = {
            RecoveryPhase.PREPARING_INCIDENT: LaneState.SEALED,
            RecoveryPhase.DELETING_INCIDENT: LaneState.CONNECTING,
            RecoveryPhase.WAITING_RECOVERY_POINT: LaneState.CONNECTING,
            RecoveryPhase.RESTORING: LaneState.CONNECTING,
            RecoveryPhase.CONNECTING: LaneState.CONNECTING,
            RecoveryPhase.VERIFYING_RECOVERED_ORDER: LaneState.VERIFYING,
            RecoveryPhase.VERIFYING_SOURCE: LaneState.VERIFYING,
            RecoveryPhase.VERIFIED: LaneState.VERIFIED,
            RecoveryPhase.FAILED: LaneState.FAILED,
            RecoveryPhase.RESETTING: LaneState.VERIFYING,
            RecoveryPhase.RESET: LaneState.VERIFIED,
        }

        async def on_started() -> None:
            async with record.lock:
                started_at = datetime.now(UTC)
                record.snapshot.state = SessionState.RUNNING
                record.snapshot.run_started_at = started_at
                record.snapshot.updated_at = started_at
                record.run_started_monotonic_ns = (
                    stop_control.started_ns
                    if stop_control is not None and stop_control.started_ns is not None
                    else self._clock_ns()
                )
                for lane in record.snapshot.lanes.values():
                    lane.state = LaneState.CONNECTING
                    lane.elapsed_ms = 0.0
                    lane.status = "Deleting the exact incident at the timed boundary"
                    lane.activity = LaneActivity(
                        phase=RecoveryPhase.DELETING_INCIDENT,
                        wire_call="PostgreSQL DELETE + clock_timestamp() → COMMIT",
                    )
                started_snapshot = record.snapshot.model_copy(deep=True)
            await record.event_log.publish(
                "run_started",
                {
                    "state": SessionState.RUNNING,
                    "lanes": ["lakebase", "competitor"],
                    "session": started_snapshot.model_dump(mode="json"),
                },
            )

        async def on_progress(progress: RecoveryProgress) -> None:
            async with record.lock:
                if stop_control is not None and stop_control.event.is_set():
                    return
                lane = record.snapshot.lanes[progress.lane_id]
                lane.state = phase_states[progress.phase]
                lane.status = progress.status
                lane.attempts = max(1, lane.attempts)
                if progress.elapsed_ms is not None:
                    lane.elapsed_ms = progress.elapsed_ms
                lane.error = progress.error
                lane.activity = LaneActivity(
                    phase=progress.phase,
                    wire_call=progress.wire_call,
                    recovery_at=progress.recovery_at,
                )
                if lane.state == LaneState.VERIFIED and lane.verified_at is None:
                    lane.verified_at = datetime.now(UTC)
                record.snapshot.updated_at = datetime.now(UTC)
            await record.event_log.publish(
                "lane_update",
                {
                    "lane_id": progress.lane_id,
                    "state": phase_states[progress.phase],
                    "attempts": 1,
                    "elapsed_ms": progress.elapsed_ms,
                    "status": progress.status,
                    "error": progress.error,
                    "activity": lane.activity.model_dump(mode="json"),
                },
            )

        try:
            result = await engine.run(
                arm,
                on_progress=on_progress,
                on_started=on_started,
                stop_control=stop_control,
            )
        except RecoveryNotArmedError as exc:
            if stop_control is not None and stop_control.event.is_set():
                return
            await self._fail(record, str(exc))
            return
        except Exception as exc:
            if stop_control is not None and stop_control.event.is_set():
                return
            self._log_bout_refusal(record, exc, round_number=3)
            await self._fail(record, "The live point-in-time recovery proof failed unexpectedly.")
            return
        async with record.lock:
            record.recovery_outcome = result
            if isinstance(result, RecoveryStoppedResult):
                record.snapshot.fairness = FairnessSnapshot(launch_skew_ms=result.launch_skew_ms)
                if record.snapshot.towel is not None:
                    record.snapshot.towel.restore_started = (
                        record.snapshot.towel.restore_started or result.restore_started
                    )
            towel_requested = record.snapshot.towel is not None
        if towel_requested:
            return
        if isinstance(result, RecoveryStoppedResult):
            await self._fail(record, "The live recovery proof stopped without a towel request.")
            return
        await self._finish_recovery(record, result)

    async def _finish_recovery(
        self,
        record: SessionRecord,
        result: RecoveryRunResult,
    ) -> None:
        cleanup_owned = False
        async with record.lock:
            record.snapshot.fairness = FairnessSnapshot(launch_skew_ms=result.launch_skew_ms)
            for lane_id, lane_result in result.lanes.items():
                lane = record.snapshot.lanes[lane_id]
                lane.state = LaneState.VERIFIED if lane_result.ok else LaneState.FAILED
                lane.elapsed_ms = lane_result.elapsed_ms
                lane.attempts = 1
                lane.error = lane_result.error
                lane.status = (
                    "Exact recovered order verified · Source deletion preserved"
                    if lane_result.ok
                    else "Could not verify the recovered order"
                )
                lane.activity = LaneActivity(
                    phase=(RecoveryPhase.VERIFIED if lane_result.ok else RecoveryPhase.FAILED),
                    wire_call=None,
                    recovery_at=lane_result.recovery_at,
                )
                if lane_result.ok:
                    lane.verified_at = datetime.now(UTC)
            if result.all_verified:
                record.snapshot.state = SessionState.VERIFIED
                record.snapshot.remembered_result = self._remembered_result(record.snapshot)
                record.snapshot.failure = None
            else:
                record.snapshot.state = SessionState.FAILED
                record.snapshot.remembered_result = None
                record.snapshot.failure = "One or more recovered orders could not be verified."
                self._log_lane_refusals(record, round_number=3)
            try:
                await self._transition_bout_to_cleanup(record, record.snapshot.state)
            except InvalidStateError as exc:
                logger.error(
                    "Round 3 result verified but cleanup fence was lost session=%s: %s",
                    record.snapshot.id,
                    type(exc).__name__,
                )
                cooldown = self._new_cooldown(
                    record,
                    ResetMode.DELETE_RECOVERY_ENVIRONMENT,
                )
                cooldown.state = CooldownState.FAILED
                cooldown.failure = "Automatic recovery cleanup lost its ring fence"
                record.snapshot.cooldown = cooldown
            else:
                record.snapshot.cooldown = self._new_cooldown(
                    record,
                    ResetMode.DELETE_RECOVERY_ENVIRONMENT,
                )
                cleanup_owned = True
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_finished",
            {"state": snapshot.state, "session": snapshot.model_dump(mode="json")},
        )
        if cleanup_owned:
            self._schedule_owned_artifact_cleanup(record, recovery=True)

    async def _finish_safe_change(
        self,
        record: SessionRecord,
        result: SafeChangeRunResult,
    ) -> None:
        cleanup_owned = False
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            record.snapshot.fairness = FairnessSnapshot(launch_skew_ms=result.launch_skew_ms)
            for lane_id, lane_result in result.lanes.items():
                lane = record.snapshot.lanes[lane_id]
                lane.state = (
                    LaneState.VERIFIED
                    if lane_result.state == SafeChangeLaneState.VERIFIED
                    else LaneState.FAILED
                )
                lane.elapsed_ms = lane_result.elapsed_ms
                lane.attempts = 1
                lane.error = lane_result.error
                lane.status = (
                    "Isolated migration and application transaction verified"
                    if lane.state == LaneState.VERIFIED
                    else "Could not verify isolated schema change"
                )
                lane.activity = LaneActivity(
                    phase=("verified" if lane.state == LaneState.VERIFIED else "failed"),
                    wire_call=None,
                )
                if lane.state == LaneState.VERIFIED:
                    lane.verified_at = datetime.now(UTC)
            if result.all_verified:
                record.snapshot.state = SessionState.VERIFIED
                record.snapshot.remembered_result = self._remembered_result(record.snapshot)
                record.snapshot.failure = None
            else:
                record.snapshot.state = SessionState.FAILED
                record.snapshot.remembered_result = None
                record.snapshot.failure = (
                    "One or more isolated schema changes could not be verified."
                )
                self._log_lane_refusals(record, round_number=2)
            try:
                await self._transition_bout_to_cleanup(record, record.snapshot.state)
            except InvalidStateError as exc:
                logger.error(
                    "Round 2 result verified but cleanup fence was lost session=%s: %s",
                    record.snapshot.id,
                    type(exc).__name__,
                )
                cooldown = self._new_cooldown(
                    record,
                    ResetMode.DELETE_ISOLATED_ENVIRONMENT,
                )
                cooldown.state = CooldownState.FAILED
                cooldown.failure = "Automatic isolated-environment cleanup lost its ring fence"
                record.snapshot.cooldown = cooldown
            else:
                record.snapshot.cooldown = self._new_cooldown(
                    record,
                    ResetMode.DELETE_ISOLATED_ENVIRONMENT,
                )
                cleanup_owned = True
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_finished",
            {"state": snapshot.state, "session": snapshot.model_dump(mode="json")},
        )
        if cleanup_owned:
            self._schedule_owned_artifact_cleanup(record, recovery=False)

    async def _finish(self, record: SessionRecord, result: VerificationResult) -> None:
        cleanup_owned = False
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            record.snapshot.fairness = FairnessSnapshot(launch_skew_ms=result.launch_skew_ms)
            for lane_id, lane_result in result.lanes.items():
                lane = record.snapshot.lanes[lane_id]
                lane.state = lane_result.state
                lane.elapsed_ms = lane_result.elapsed_ms
                lane.attempts = lane_result.attempts
                lane.error = lane_result.error
                lane.connection_closed_at = lane_result.connection_closed_at
                lane.verified_at = (
                    lane_result.connection_closed_at
                    if lane_result.state == LaneState.VERIFIED
                    else None
                )
                lane.status = (
                    "Transaction verified"
                    if lane_result.state == LaneState.VERIFIED
                    else "Could not verify"
                )
                lane.activity = LaneActivity(
                    phase=lane_result.state,
                    wire_call=_ROUND_ONE_TRANSACTION_WIRE_CALL,
                )
            eligible_lanes = [
                lane
                for lane in record.snapshot.lanes.values()
                if lane.state != LaneState.NOT_SUPPORTED
            ]
            all_verified = bool(eligible_lanes) and all(
                lane.state == LaneState.VERIFIED for lane in eligible_lanes
            )
            if all_verified:
                record.snapshot.state = SessionState.VERIFIED
                record.snapshot.remembered_result = self._remembered_result(record.snapshot)
                record.snapshot.failure = None
            else:
                record.snapshot.state = SessionState.FAILED
                record.snapshot.remembered_result = None
                if record.snapshot.lanes["competitor"].state == LaneState.NOT_SUPPORTED:
                    record.snapshot.failure = (
                        "Lakebase could not verify the application transaction. "
                        "No winner was declared."
                    )
                else:
                    record.snapshot.failure = "One or more lanes could not be verified."
                self._log_lane_refusals(record, round_number=1)
            try:
                # The verdict ends the timed bout, but not the round's ownership.
                # Keep the same durable fence in a cleanup phase until both
                # control planes prove zero. Releasing here was the hidden-wait
                # defect: every viewer saw READY while the next arm was forced to
                # rediscover Aurora's auto-pause floor.
                await self._transition_bout_to_cleanup(record, record.snapshot.state)
            except InvalidStateError as exc:
                logger.error(
                    "Round 1 result verified but cleanup fence was lost session=%s: %s",
                    record.snapshot.id,
                    type(exc).__name__,
                )
                cooldown = self._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
                cooldown.state = CooldownState.FAILED
                cooldown.failure = "Automatic return-to-idle cleanup lost its ring fence"
                record.snapshot.cooldown = cooldown
            else:
                record.snapshot.cooldown = self._new_cooldown(
                    record,
                    ResetMode.RETURN_TO_IDLE,
                )
                cleanup_owned = True
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_finished",
            {"state": snapshot.state, "session": snapshot.model_dump(mode="json")},
        )
        if cleanup_owned:
            self._schedule_round_one_cooldown(record)

    @staticmethod
    def _new_towel_cooldown(record: SessionRecord) -> CooldownSnapshot:
        round_id = record.snapshot.round.id
        if round_id == RoundId.WAKE_IDLE_APP:
            return RunManager._new_cooldown(record, ResetMode.RETURN_TO_IDLE)
        mode = (
            ResetMode.DELETE_ISOLATED_ENVIRONMENT
            if round_id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY
            else ResetMode.DELETE_RECOVERY_ENVIRONMENT
        )
        cooldown = RunManager._new_cooldown(record, mode)
        towel = record.snapshot.towel
        if (
            round_id == RoundId.RECOVER_DELETED_ORDER
            and towel is not None
            and towel.restore_started
        ):
            cooldown.lanes[
                "competitor"
            ].status = "AWS RESTORE ALREADY IN MOTION · SAFE CLEANUP MAY TAKE MINUTES"
        return cooldown

    async def _await_towelled_run_task(self, run_task: asyncio.Task[None]) -> None:
        """Wait out the towelled run, but never unboundedly.

        Rounds 2, 4 and 6 are cancelled outright before this. Rounds 1 and 3 are
        not: they stop by observing a control, which is the right design and only
        works if the task looks. A Round 3 task parked inside a provider call
        that never looks used to make this `gather` never return -- ring held,
        no diagnostic, nothing to retry. So the cooperative stop gets a generous
        window and then the cancellation it should not have needed.
        """

        try:
            async with asyncio.timeout(self._towel_stop_grace):
                await asyncio.gather(run_task, return_exceptions=True)
            return
        except TimeoutError:
            pass
        logger.warning(
            "Towelled run task %s did not observe the stop within %.0fs; cancelling",
            run_task.get_name(),
            self._towel_stop_grace,
        )
        run_task.cancel()
        try:
            async with asyncio.timeout(self._towel_cancel_timeout):
                await asyncio.gather(run_task, return_exceptions=True)
        except TimeoutError as exc:
            # Settling around a task that is still running would be worse than
            # refusing to: it may still be mutating the very artifacts about to
            # be deleted. Reported as a settlement failure, which keeps the ring
            # held on purpose and leaves the towel `failed` and retryable -- and
            # the retry does not wait for the run task at all.
            raise InvalidStateError(
                "The live bout did not stop when the towel was thrown; "
                "cleanup was not attempted. Retry cleanup."
            ) from exc

    async def _mark_towel_cleanup_failed(self, record: SessionRecord, failure: str) -> None:
        async with record.lock:
            towel = record.snapshot.towel
            if towel is None:
                return
            towel.state = TowelState.FAILED
            towel.cleanup_failure = failure
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "towel_update",
            {"session": snapshot.model_dump(mode="json")},
        )

    async def _settle_towel_cost_window(self, record: SessionRecord) -> None:
        """Close a cost window the towel could not close inline.

        ``start_towel`` refuses to let the ledger stop it, so a close failure
        there is recorded and deferred to here, and retried on every cleanup
        attempt -- including an operator's retry click. This runs only on the
        path that is about to release the ring, and if the close still fails at
        that point there is nothing left to retry against: the record would
        otherwise be pinned forever by ``_releasable``'s open-cost-bout check,
        which is a memory leak reached through the towel. So the local pointer
        is dropped and the ledger row is left to reconciliation, said out loud.
        """

        async with record.lock:
            bout_id = record.cost_bout_id
            towel = record.snapshot.towel
            if bout_id is None or towel is None:
                return
            try:
                await self._close_cost_bout(
                    record,
                    bout_id=bout_id,
                    ended_at=datetime.now(UTC),
                    outcome=SessionState.TOWELLED.value,
                )
            except Exception as exc:
                logger.error(
                    "Towel cleanup could not close the cost window session=%s "
                    "diagnostic=%s; abandoning it to reconciliation",
                    record.snapshot.id,
                    _redacted_exception_chain(exc),
                )
                record.cost_bout_id = None
                record.cost_bout_started_at = None
                record.cost_bout_kind = None
                towel.cost_close_failure = (
                    f"{str(exc) or 'The bout cost window could not be closed'} · "
                    "the open ledger window was left for reconciliation"
                )
                return
            towel.cost_close_failure = None

    async def _complete_towel(
        self,
        record: SessionRecord,
        *,
        wait_for_run: bool,
    ) -> None:
        run_task = record.task
        async with record.lock:
            towel = record.snapshot.towel
            if towel is None:
                return
            towel.state = TowelState.CLEANING
            towel.cleanup_failure = None
            round_id = record.snapshot.round.id
            if round_id in {
                RoundId.WAKE_IDLE_APP,
                RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
                RoundId.RECOVER_DELETED_ORDER,
            }:
                record.snapshot.cooldown = self._new_towel_cooldown(record)
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "towel_update",
            {"session": snapshot.model_dump(mode="json")},
        )

        if round_id == RoundId.SURVIVE_CONNECTION_SPIKE:
            await self._begin_connection_spike_cleanup_handoff(record)
            return

        settlement_error: str | None = None
        try:
            if round_id in {
                RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
                RoundId.PUT_MODEL_SCORE_IN_APP,
                RoundId.ANALYZE_LIVE_ORDERS,
            } and (
                run_task is not None
                and run_task is not asyncio.current_task()
                and not run_task.done()
            ):
                run_task.cancel()
            if wait_for_run and run_task is not None and run_task is not asyncio.current_task():
                await self._await_towelled_run_task(run_task)

            if round_id == RoundId.WAKE_IDLE_APP:
                # There is no owned artifact to settle. The verifier contexts
                # above are the safety boundary; provider scale-to-zero is a
                # lease-free backstage observation and must not block the ring.
                pass
            elif round_id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
                engine = record.safe_change_engine
                if engine is None:
                    raise InvalidStateError("The isolated-change cleanup engine is unavailable")
                await engine.settle_pending_mutations(record.snapshot.competitor.id)
                await self._reset_safe_change(record)
            elif round_id == RoundId.RECOVER_DELETED_ORDER:
                engine = record.recovery_engine
                if engine is None:
                    raise InvalidStateError("The recovery cleanup engine is unavailable")
                outcome = record.recovery_outcome
                async with record.lock:
                    current = record.snapshot.towel
                    if current is not None and isinstance(outcome, RecoveryStoppedResult):
                        current.restore_started = current.restore_started or outcome.restore_started
                settle = getattr(engine, "settle_pending_mutations", None)
                if settle is not None:
                    await settle(record.snapshot.competitor.id)
                await self._reset_recovery(record)
            elif round_id == RoundId.PUT_MODEL_SCORE_IN_APP:
                engine = record.model_score_engine
                if engine is None:
                    raise InvalidStateError("The Managed Sync cleanup engine is unavailable")
                await engine.settle_and_restore_baseline()
            elif round_id == RoundId.ANALYZE_LIVE_ORDERS:
                engine = record.live_orders_engine
                order = record.live_orders_pending_order
                if engine is None or order is None:
                    raise InvalidStateError("The live-order cleanup identity is unavailable")
                await engine.settle_and_cleanup_owned(
                    order,
                    record.live_orders_guardrail_order,
                )

            async with record.lock:
                cooldown = record.snapshot.cooldown
                if (
                    round_id != RoundId.WAKE_IDLE_APP
                    and cooldown is not None
                    and cooldown.state != CooldownState.READY
                ):
                    raise InvalidStateError(
                        cooldown.failure or "Automatic towel cleanup could not be verified"
                    )
        except asyncio.CancelledError:
            # `except Exception` does not catch this, and without an arm here a
            # cancelled cooldown task left the towel at `cleaning` with the ring
            # lease held and heartbeating -- the same dead end as T2, reachable
            # on any round. Shielded so the state change survives the
            # cancellation that provoked it.
            await asyncio.shield(
                asyncio.create_task(
                    self._mark_towel_cleanup_failed(
                        record,
                        "Towel cleanup was cancelled before the ring could be "
                        "released; retry cleanup.",
                    ),
                    name=f"towel-cancelled-{record.snapshot.id}",
                )
            )
            raise
        except Exception as exc:
            settlement_error = str(exc) or "Automatic towel cleanup could not be verified"

        if settlement_error is None:
            await self._settle_towel_cost_window(record)
        released = settlement_error is None and await self._release_bout(record)
        if settlement_error is None and not released:
            settlement_error = "Ring release could not be confirmed; retry towel cleanup."

        async with record.lock:
            towel = record.snapshot.towel
            if towel is None:
                return
            if settlement_error is None:
                towel.state = TowelState.READY
                towel.cleanup_failure = None
                event = "towel_finished"
            else:
                towel.state = TowelState.FAILED
                towel.cleanup_failure = settlement_error
                event = "towel_update"
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            event,
            {"session": snapshot.model_dump(mode="json")},
        )
        if settlement_error is None and round_id == RoundId.WAKE_IDLE_APP:
            if record.cooldown_task is asyncio.current_task():
                record.cooldown_task = None
            self._schedule_round_one_cooldown(record)

    async def _reset_safe_change(self, record: SessionRecord) -> None:
        engine = record.safe_change_engine
        cooldown = record.snapshot.cooldown
        if engine is None or cooldown is None:
            return
        await record.event_log.publish(
            "cooldown_started",
            {"cooldown": cooldown.model_dump(mode="json")},
        )

        async def on_progress(progress: SafeChangeProgress) -> None:
            now = datetime.now(UTC)
            async with record.lock:
                current = record.snapshot.cooldown
                if current is None:
                    return
                lane = current.lanes[progress.lane_id]
                lane.status = progress.status
                lane.activity = LaneActivity(
                    phase=progress.phase,
                    wire_call=progress.wire_call,
                )
                if progress.phase == SafeChangePhase.RESET:
                    lane.state = CooldownLaneState.CONFIRMED_DELETED
                    lane.confirmed_at = now
                    lane.elapsed_ms = max(
                        0.0,
                        (now - lane.started_at).total_seconds() * 1000,
                    )
                elif progress.phase == SafeChangePhase.FAILED:
                    lane.state = CooldownLaneState.FAILED
                snapshot = current.model_copy(deep=True)
            await record.event_log.publish(
                "cooldown_update",
                {"cooldown": snapshot.model_dump(mode="json")},
            )

        try:
            result = await engine.reset(record.snapshot.competitor.id, on_progress)
        except SafeChangeResetError as exc:
            result = exc.result
        except Exception:
            async with record.lock:
                current = record.snapshot.cooldown
                if current is None:
                    return
                current.state = CooldownState.FAILED
                current.failure = "Isolated environments could not be safely removed."
                for lane in current.lanes.values():
                    if lane.state == CooldownLaneState.WATCHING:
                        lane.state = CooldownLaneState.FAILED
                        lane.status = "Owned isolated environment was not removed"
                        lane.activity = LaneActivity(phase="failed", wire_call=None)
                snapshot = current.model_copy(deep=True)
            await record.event_log.publish(
                "cooldown_update",
                {"cooldown": snapshot.model_dump(mode="json")},
            )
            return

        now = datetime.now(UTC)
        async with record.lock:
            current = record.snapshot.cooldown
            if current is None:
                return
            for lane_id, lane_result in result.lanes.items():
                lane = current.lanes[lane_id]
                if lane_result.ok:
                    if lane.state != CooldownLaneState.CONFIRMED_DELETED:
                        lane.state = CooldownLaneState.CONFIRMED_DELETED
                        lane.confirmed_at = now
                        lane.elapsed_ms = max(
                            0.0,
                            (now - lane.started_at).total_seconds() * 1000,
                        )
                    lane.status = "Owned isolated environment confirmed deleted"
                    lane.activity = LaneActivity(phase="reset", wire_call=None)
                else:
                    lane.state = CooldownLaneState.FAILED
                    lane.status = lane_result.error or "Isolated environment was not removed"
                    lane.activity = LaneActivity(phase="failed", wire_call=None)
            if all(
                lane.state == CooldownLaneState.CONFIRMED_DELETED for lane in current.lanes.values()
            ):
                current.state = CooldownState.READY
                current.failure = None
            else:
                current.state = CooldownState.FAILED
                current.failure = "Isolated environments could not be safely removed."
            record.snapshot.updated_at = now
            snapshot = current.model_copy(deep=True)
        await record.event_log.publish(
            "cooldown_ready" if snapshot.state == CooldownState.READY else "cooldown_update",
            {"cooldown": snapshot.model_dump(mode="json")},
        )

    async def _reset_recovery(self, record: SessionRecord) -> None:
        engine = record.recovery_engine
        cooldown = record.snapshot.cooldown
        if engine is None or cooldown is None:
            return
        await record.event_log.publish(
            "cooldown_started",
            {"cooldown": cooldown.model_dump(mode="json")},
        )

        async def on_progress(progress: RecoveryProgress) -> None:
            now = datetime.now(UTC)
            async with record.lock:
                current = record.snapshot.cooldown
                if current is None:
                    return
                lane = current.lanes[progress.lane_id]
                lane.status = progress.status
                lane.activity = LaneActivity(
                    phase=progress.phase,
                    wire_call=progress.wire_call,
                )
                if progress.phase == RecoveryPhase.RESET:
                    lane.state = CooldownLaneState.CONFIRMED_DELETED
                    lane.confirmed_at = now
                    lane.elapsed_ms = max(
                        0.0,
                        (now - lane.started_at).total_seconds() * 1000,
                    )
                elif progress.phase == RecoveryPhase.FAILED:
                    lane.state = CooldownLaneState.FAILED
                snapshot = current.model_copy(deep=True)
            await record.event_log.publish(
                "cooldown_update",
                {"cooldown": snapshot.model_dump(mode="json")},
            )

        try:
            result = await engine.reset(record.snapshot.competitor.id, on_progress)
        except RecoveryResetError as exc:
            result = exc.result
        except Exception:
            async with record.lock:
                current = record.snapshot.cooldown
                if current is None:
                    return
                current.state = CooldownState.FAILED
                current.failure = "Recovery environments could not be safely removed."
                for lane in current.lanes.values():
                    if lane.state == CooldownLaneState.WATCHING:
                        lane.state = CooldownLaneState.FAILED
                        lane.status = "Owned recovery environment was not removed"
                        lane.activity = LaneActivity(phase="failed", wire_call=None)
                snapshot = current.model_copy(deep=True)
            await record.event_log.publish(
                "cooldown_update",
                {"cooldown": snapshot.model_dump(mode="json")},
            )
            return

        now = datetime.now(UTC)
        async with record.lock:
            current = record.snapshot.cooldown
            if current is None:
                return
            for lane_id, lane_result in result.lanes.items():
                lane = current.lanes[lane_id]
                if lane_result.ok:
                    if lane.state != CooldownLaneState.CONFIRMED_DELETED:
                        lane.state = CooldownLaneState.CONFIRMED_DELETED
                        lane.confirmed_at = now
                        lane.elapsed_ms = max(
                            0.0,
                            (now - lane.started_at).total_seconds() * 1000,
                        )
                    lane.status = "Owned recovery environment confirmed deleted"
                    lane.activity = LaneActivity(phase="reset", wire_call=None)
                else:
                    lane.state = CooldownLaneState.FAILED
                    lane.status = lane_result.error or "Recovery environment was not removed"
                    lane.activity = LaneActivity(phase="failed", wire_call=None)
            if all(
                lane.state == CooldownLaneState.CONFIRMED_DELETED for lane in current.lanes.values()
            ):
                current.state = CooldownState.READY
                current.failure = None
            else:
                current.state = CooldownState.FAILED
                current.failure = "Recovery environments could not be safely removed."
            record.snapshot.updated_at = now
            snapshot = current.model_copy(deep=True)
        await record.event_log.publish(
            "cooldown_ready" if snapshot.state == CooldownState.READY else "cooldown_update",
            {"cooldown": snapshot.model_dump(mode="json")},
        )

    async def _monitor_cooldown(self, record: SessionRecord) -> None:
        if record.live_targets is None or record.snapshot.cooldown is None:
            return
        await record.event_log.publish(
            "cooldown_started",
            {"cooldown": record.snapshot.cooldown.model_dump(mode="json")},
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._cooldown_timeout
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            poll_timeout = max(
                0.001,
                min(self._cooldown_poll_timeout, remaining),
            )
            tasks = [
                asyncio.create_task(
                    self._observe_cooldown_target(
                        target,
                        record.snapshot.cooldown.lanes[target.id].started_at,
                        poll_timeout,
                    )
                )
                for target in record.live_targets
                # A confirmed lane owns an immutable stop result. Continuing to
                # poll it used to publish a later `confirmed_at`/`elapsed_ms`
                # every cycle while the other lane was still active.
                if (
                    record.snapshot.cooldown.lanes[target.id].state
                    != CooldownLaneState.CONFIRMED_ZERO
                )
            ]
            for task in asyncio.as_completed(tasks):
                target, check, completed_at = await task
                changed, snapshot = await self._apply_cooldown_observation(
                    record,
                    target.id,
                    check,
                    completed_at,
                )
                if changed:
                    await record.event_log.publish(
                        "cooldown_update",
                        {"cooldown": snapshot.model_dump(mode="json")},
                    )

            async with record.lock:
                cooldown = record.snapshot.cooldown
                if cooldown is None:
                    return
                terminal = {
                    CooldownLaneState.CONFIRMED_ZERO,
                    CooldownLaneState.NOT_SUPPORTED,
                }
                if all(lane.state in terminal for lane in cooldown.lanes.values()):
                    cooldown.state = CooldownState.READY
                    cooldown.failure = None
                    record.snapshot.updated_at = datetime.now(UTC)
                snapshot = cooldown.model_copy(deep=True)

            if snapshot.state == CooldownState.READY:
                await record.event_log.publish(
                    "cooldown_ready",
                    {"cooldown": snapshot.model_dump(mode="json")},
                )
                return
            await asyncio.sleep(min(self._arm_poll, max(0.0, deadline - loop.time())))

        async with record.lock:
            cooldown = record.snapshot.cooldown
            if cooldown is None:
                return
            cooldown.state = CooldownState.FAILED
            cooldown.failure = "Timed out waiting for return to confirmed zero."
            for lane in cooldown.lanes.values():
                if lane.state == CooldownLaneState.WATCHING:
                    lane.state = CooldownLaneState.FAILED
                    lane.status = "Timed out waiting for control-plane proof of zero"
                    lane.activity = LaneActivity(
                        phase="failed",
                        wire_call=_round_one_cooldown_wire_call(
                            lane.id,
                            record.snapshot.competitor.id,
                        ),
                    )
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = cooldown.model_copy(deep=True)
        await record.event_log.publish(
            "cooldown_update",
            {"cooldown": snapshot.model_dump(mode="json")},
        )
        await self._pin_round_one_cooldown_failure(record)

    async def _pin_round_one_cooldown_failure(self, record: SessionRecord) -> None:
        """Publish a finite durable failure fence and stop renewing it.

        A terminal observation timeout has no owned artifact to clean, but it
        must not flash READY immediately either. Transitioning the existing row
        to `cooldown_failed` keeps this round unavailable across replicas for one
        ordinary active-lease TTL. The heartbeat is cancelled whether the
        transition succeeds or not, so a coordinator fault degrades to the
        existing row's finite expiry instead of an immortal cooldown lease.
        """

        expected: BoutLease | None
        async with record.lease_lock:
            expected = record.lease
            if (
                expected is not None
                and expected.session_id == record.snapshot.id
                and expected.phase == "cooldown"
            ):
                try:
                    record.lease = await self._lease_store_for_record(record).transition(
                        expected,
                        operator=record.operator or expected.operator,
                        expected_phase="cooldown",
                        phase="cooldown_failed",
                        session_state=record.snapshot.state,
                        ttl=timedelta(seconds=self._active_lease_ttl),
                    )
                except Exception as exc:
                    logger.error(
                        "Round 1 cooldown failure fence could not be persisted "
                        "session=%s diagnostic=%s",
                        record.snapshot.id,
                        _redacted_exception_chain(exc),
                    )
        if expected is None:
            self._cancel_lease_heartbeat(record)
        else:
            self._cancel_lease_heartbeat(record, expected)

    @staticmethod
    async def _observe_cooldown_target(
        target: LiveTarget,
        not_before: datetime,
        timeout_seconds: float,
    ) -> tuple[LiveTarget, dict[str, object] | Exception, datetime]:
        try:
            async with asyncio.timeout(timeout_seconds):
                check = await target.assert_armed(not_before=not_before)
            return target, check, datetime.now(UTC)
        except Exception as exc:
            return target, exc, datetime.now(UTC)

    async def _apply_cooldown_observation(
        self,
        record: SessionRecord,
        target_id: str,
        check: dict[str, object] | Exception,
        completed_at: datetime,
    ) -> tuple[bool, CooldownSnapshot]:
        changed = False
        async with record.lock:
            cooldown = record.snapshot.cooldown
            if cooldown is None:
                raise InvalidStateError("The Round 1 re-do clock is no longer active")
            lane = cooldown.lanes[target_id]
            if lane.state == CooldownLaneState.CONFIRMED_ZERO:
                # A task may have been launched just before another observation
                # confirmed this lane. Never let that in-flight or duplicated
                # result move the terminal evidence boundary.
                return False, cooldown.model_copy(deep=True)

            def assign(field_name: str, value: object) -> None:
                nonlocal changed
                if getattr(lane, field_name) != value:
                    setattr(lane, field_name, value)
                    changed = True

            wire_call = _round_one_cooldown_wire_call(
                target_id,
                record.snapshot.competitor.id,
            )
            assign("checked_at", completed_at)
            if isinstance(check, dict):
                observed_state = str(check.get("state") or "").upper() or None
                assign("observed_state", observed_state)
                if check.get("eligible", True) is False:
                    assign("state", CooldownLaneState.NOT_SUPPORTED)
                    assign("confirmed_at", completed_at)
                    assign("elapsed_ms", None)
                    assign("observation_count", 1)
                    assign("confirmation_basis", "engine_capability")
                    status = "No automatic scale-to-zero"
                    activity = LaneActivity(
                        phase="not_supported",
                        wire_call=wire_call,
                    )
                elif (
                    target_id == "lakebase"
                    and cooldown.mode == ResetMode.RETURN_TO_IDLE
                ):
                    source_lane = record.snapshot.lanes[target_id]
                    activity_proved = bool(
                        source_lane.state == LaneState.VERIFIED
                        and source_lane.connection_closed_at is not None
                    )
                    provider_updated_at = self._cooldown_timestamp(
                        check.get("provider_updated_at")
                    )
                    assign("provider_updated_at", provider_updated_at)
                    now_ns = time.monotonic_ns()
                    previous = record.cooldown_idle_observations.get(target_id)
                    if previous is None:
                        observation_count, first_observed_ns = 1, now_ns
                    else:
                        prior_count, first_observed_ns = previous
                        observation_count = prior_count + 1
                    record.cooldown_idle_observations[target_id] = (
                        observation_count,
                        first_observed_ns,
                    )
                    assign("observation_count", observation_count)

                    provider_corrobates_post_close = bool(
                        provider_updated_at is not None
                        and lane.started_at <= provider_updated_at <= completed_at
                    )
                    dwell_seconds = max(0.0, (now_ns - first_observed_ns) / 1e9)
                    dwell_confirmed = bool(
                        observation_count >= 2
                        and now_ns > first_observed_ns
                        and dwell_seconds >= self._lakebase_idle_dwell
                    )
                    if activity_proved and (
                        provider_corrobates_post_close or dwell_confirmed
                    ):
                        # This is an observed-by upper bound. Even when the
                        # provider resource was updated after the connection
                        # closed, its `update_time` remains advisory metadata,
                        # never a claimed IDLE transition time.
                        assign("state", CooldownLaneState.CONFIRMED_ZERO)
                        assign("confirmed_at", completed_at)
                        assign(
                            "elapsed_ms",
                            max(
                                0.0,
                                (completed_at - lane.started_at).total_seconds() * 1000,
                            ),
                        )
                        basis = (
                            "provider_update_corroboration"
                            if provider_corrobates_post_close
                            else "observed_idle_dwell"
                        )
                        assign("confirmation_basis", basis)
                        status = (
                            "IDLE confirmed by current control-plane state; endpoint "
                            "update metadata corroborates a post-close observation"
                            if provider_corrobates_post_close
                            else "IDLE confirmed by repeated independent control-plane "
                            "observations after the lane connection closed"
                        )
                        activity = LaneActivity(
                            phase="confirmed_zero",
                            wire_call=wire_call,
                        )
                    else:
                        assign("state", CooldownLaneState.WATCHING)
                        assign("confirmed_at", None)
                        assign("elapsed_ms", None)
                        assign("confirmation_basis", None)
                        if not activity_proved:
                            status = (
                                "IDLE observed, but no verified lane transaction and "
                                "final connection close prove post-bout activity"
                            )
                        else:
                            status = (
                                "IDLE observed after the lane connection closed; "
                                "confirming monotonic dwell with another independent poll"
                            )
                        activity = LaneActivity(
                            phase="watching",
                            wire_call=wire_call,
                        )
                else:
                    confirmed_at = self._cooldown_evidence_time(
                        check,
                        lane.started_at,
                        completed_at,
                    )
                    assign("state", CooldownLaneState.CONFIRMED_ZERO)
                    assign("confirmed_at", confirmed_at)
                    assign(
                        "elapsed_ms",
                        max(
                            0.0,
                            (confirmed_at - lane.started_at).total_seconds() * 1000,
                        ),
                    )
                    assign("observation_count", 1)
                    assign(
                        "confirmation_basis",
                        (
                            "provider_transition"
                            if check.get("evidence") == "RDS_EVENT_SUCCESSFULLY_PAUSED"
                            else "observed_samples"
                        ),
                    )
                    status = "Control plane confirmed zero"
                    activity = LaneActivity(
                        phase="confirmed_zero",
                        wire_call=wire_call,
                    )
                assign("activity", activity)
                assign("status", status)
            elif isinstance(check, TargetNotArmedError):
                record.cooldown_idle_observations.pop(target_id, None)
                assign("observation_count", 0)
                assign("confirmation_basis", None)
                assign("provider_updated_at", None)
                assign("observed_state", None)
                status = str(check)
                activity = LaneActivity(phase="watching", wire_call=wire_call)
                assign("activity", activity)
                if lane.state == CooldownLaneState.CONFIRMED_ZERO:
                    assign("state", CooldownLaneState.WATCHING)
                    assign("confirmed_at", None)
                    assign("elapsed_ms", None)
                assign("status", status)
            else:
                status = "Control-plane check delayed; retrying"
                activity = LaneActivity(phase="watching", wire_call=wire_call)
                assign("activity", activity)
                assign("status", status)
            if changed:
                record.snapshot.updated_at = completed_at
            return changed, cooldown.model_copy(deep=True)

    @staticmethod
    def _cooldown_timestamp(raw: object) -> datetime | None:
        if not isinstance(raw, str):
            return None
        try:
            observed_at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if observed_at.tzinfo is None:
            return None
        return observed_at.astimezone(UTC)

    @staticmethod
    def _cooldown_evidence_time(
        check: dict[str, object],
        started_at: datetime,
        completed_at: datetime,
    ) -> datetime:
        observed_at = RunManager._cooldown_timestamp(check.get("observed_at"))
        if observed_at is None:
            return completed_at
        # A provider can return the timestamp of its control-plane evidence. Clamp
        # it to this re-do window so skew or malformed evidence cannot forge a time.
        return min(max(observed_at.astimezone(UTC), started_at), completed_at)

    async def _fail(self, record: SessionRecord, message: str) -> None:
        cleanup_mode: ResetMode | None = None
        schedule_round_one = False
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            self._cancel_armed_expiry(record)
            async with record.lease_lock:
                lease_phase = record.lease.phase if record.lease is not None else None
            if lease_phase == "run_committed":
                if record.snapshot.round.id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
                    cleanup_mode = ResetMode.DELETE_ISOLATED_ENVIRONMENT
                elif record.snapshot.round.id == RoundId.RECOVER_DELETED_ORDER:
                    cleanup_mode = ResetMode.DELETE_RECOVERY_ENVIRONMENT
            if cleanup_mode is None and not await self._confirm_terminal_release(record):
                return
            if (
                cleanup_mode is None
                and record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
                and not await self._release_round5_lease(record)
            ):
                record.snapshot.failure = (
                    "Round 5 cleanup authority release could not be confirmed."
                )
                record.snapshot.updated_at = datetime.now(UTC)
                return
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            record.snapshot.remembered_result = None
            if cleanup_mode is not None:
                try:
                    await self._transition_bout_to_cleanup(record, SessionState.FAILED)
                except InvalidStateError:
                    cooldown = self._new_cooldown(record, cleanup_mode)
                    cooldown.state = CooldownState.FAILED
                    cooldown.failure = "Automatic cleanup lost its ring fence"
                    record.snapshot.cooldown = cooldown
                    cleanup_mode = None
                else:
                    record.snapshot.cooldown = self._new_cooldown(record, cleanup_mode)
            elif (
                record.snapshot.round.id == RoundId.WAKE_IDLE_APP
                and record.snapshot.run_started_at is not None
                and record.live_targets is not None
            ):
                record.snapshot.cooldown = self._new_cooldown(
                    record,
                    ResetMode.RETURN_TO_IDLE,
                )
                schedule_round_one = True
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
        )
        if cleanup_mode == ResetMode.DELETE_ISOLATED_ENVIRONMENT:
            self._schedule_owned_artifact_cleanup(record, recovery=False)
        elif cleanup_mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT:
            self._schedule_owned_artifact_cleanup(record, recovery=True)
        elif schedule_round_one:
            self._schedule_round_one_cooldown(record)

    @staticmethod
    def _reset_outcome(snapshot: SessionSnapshot) -> None:
        snapshot.failure = None
        snapshot.remembered_result = None
        snapshot.armed_at = None
        snapshot.armed_expires_at = None
        snapshot.run_started_at = None
        snapshot.fairness = FairnessSnapshot()
        snapshot.cooldown = None
        snapshot.towel = None
        snapshot.metrics = []
        snapshot.comparison = None
        snapshot.redo = None
        snapshot.round5_setup = (
            RunManager._new_round_five_setup(snapshot)
            if snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            else None
        )
        for lane in snapshot.lanes.values():
            lane.state = LaneState.SEALED
            lane.elapsed_ms = None
            lane.attempts = 0
            lane.successes = 0
            lane.errors = 0
            lane.p99_ms = None
            lane.status = "Sealed"
            lane.error = None
            lane.verified_at = None
            lane.connection_closed_at = None
            lane.activity = None
            lane.evidence = {}

    @staticmethod
    def _remembered_result(snapshot: SessionSnapshot) -> str:
        if snapshot.round.id == RoundId.PUT_MODEL_SCORE_IN_APP:
            return _ROUND_FOUR_REMEMBERED_RESULT
        if snapshot.lanes["competitor"].state == LaneState.NOT_SUPPORTED:
            assert snapshot.lanes["lakebase"].state == LaneState.VERIFIED
            return "LAKEBASE WINS — RDS CANNOT ENTER THE ROUND"

        lakebase_ms = snapshot.lanes["lakebase"].elapsed_ms
        competitor_ms = snapshot.lanes["competitor"].elapsed_ms
        assert lakebase_ms is not None and competitor_ms is not None
        delta_ms = abs(lakebase_ms - competitor_ms)
        if delta_ms < 5:
            return "NECK AND NECK — BOTH VERIFIED"
        winner = (
            "LAKEBASE" if lakebase_ms < competitor_ms else snapshot.competitor.short_name.upper()
        )
        if delta_ms < 1000:
            return f"{winner} — {delta_ms:.0f} MILLISECONDS SOONER"
        return f"{winner} — {delta_ms / 1000:.2f} SECONDS SOONER"
