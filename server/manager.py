from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from typing import Any
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
    ROUND5_BELL_PROTOCOL,
    ROUND5_FANIN_PROTOCOL,
    ROUND5_PROTOCOLS,
    build_presenter_pack,
    competitor_by_id,
    persona_by_id,
    recommend_round,
    round_by_id,
)
from .connection_fanin import FANIN_SCHEMA_VERSION, SAFETY_EVIDENCE_VERSION
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
    LiveOrdersError,
)
from .manifest import DemoManifest
from .model_score import (
    ModelScoreError,
    ModelScoreUpdate,
)
from .model_score_live import (
    DeltaStorageAccessDeniedError,
    WorkspaceStatementExecutionError,
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
    ResetMode,
    RoundFiveClockProjectionSnapshot,
    RoundFiveRuntimeLaneSnapshot,
    RoundFiveRuntimeSnapshot,
    RoundFiveSetupEvidenceSnapshot,
    RoundFiveSetupGateSnapshot,
    RoundFiveSetupLaneSnapshot,
    RoundFiveSetupSnapshot,
    RoundFiveSetupState,
    RoundFiveStartStatus,
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
from .round4_race import (
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    LANE_TIMEOUT_SECONDS,
    Round4Arm,
    Round4Phase,
    Round4RaceEngine,
    Round4RaceError,
    Round4RaceResult,
    Verdict,
)
from .round5_cleanup_owed import (
    Round5CleanupOwed,
    clear_round5_cleanup_owed,
    record_round5_cleanup_owed,
)
from .round5_warm import (
    BellContext,
    Round5LaunchCapsule,
    Round5Variant,
    Round5WarmCoordinator,
    Round5WarmSlot,
    Round5WarmState,
    WarmBellMarginError,
    WarmClaimUnavailableError,
    WarmStoreConflictError,
)
from .round6_race import LANE_TIMEOUT_SECONDS as ROUND6_LANE_TIMEOUT_SECONDS
from .round6_race import (
    Round6Arm,
    Round6Phase,
    Round6RaceEngine,
    Round6RaceError,
    Round6RaceResult,
    new_bout_orders,
)
from .round_availability import (
    GRANT_REFUSAL_HEADLINE,
    grant_refusal,
    missing_delta_refusal,
    storage_refusal,
)
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
from .towel import (
    adjudicate_round_five_bell_towel,
    adjudicate_round_five_towel,
    adjudicate_towel,
)
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
    # Attribute names are not provenance. Provider exceptions can carry fields
    # named stage/reason_code/lane whose values are credentials or identifiers;
    # even alphanumeric values are not safe merely because they fit a regex.
    # Import lazily to keep manager's module graph independent of the live AWS
    # implementation during startup while still requiring the trusted type.
    from .connection_spike_live import ConnectionSpikeCleanupError

    if isinstance(error, ConnectionSpikeCleanupError):
        cleanup_fields = (
            ("stage", error.stage),
            ("code", error.reason_code),
            ("lane", error.lane),
        )
        safe_cleanup_fields = [
            f"{label}={field}"
            for label, field in cleanup_fields
            if isinstance(field, str)
            and field
            and field.replace("_", "").replace("-", "").isalnum()
        ]
        if safe_cleanup_fields:
            value = f"{value}[{','.join(safe_cleanup_fields)}]"
    return value


def _message_is_ours_to_quote(error: BaseException) -> bool:
    """Whether this exception's words may enter operator-only diagnostics.

    Two kinds may, and the boundary is who wrote the sentence.

    A `DatabricksError` may, because its message names the table, principal and
    permission an operator must repair. The audience-facing arm and cost
    messages never call this renderer directly; they use fixed wording and send
    this detail only to the operator log and cached operator refusal.

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
    """The whole cause chain for operator-only logs and refusal detail.

    A traceback alone is not an actionable answer, but neither is a fixed
    string. Audience-facing failure banners are deliberately built elsewhere
    and never interpolate this value.
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


def storage_access_refusal(error: BaseException) -> DeltaStorageAccessDeniedError | None:
    """The typed shared-Delta storage denial anywhere in a wrapped cause chain."""

    failure = statement_execution_failure(error)
    return failure if isinstance(failure, DeltaStorageAccessDeniedError) else None


def statement_execution_failure(
    error: BaseException,
) -> WorkspaceStatementExecutionError | None:
    """The typed Statement Execution failure anywhere in a wrapped cause chain."""

    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, WorkspaceStatementExecutionError):
            return current
        current = current.__cause__ or current.__context__
    return None


def arm_failure_message(summary: str, error: BaseException) -> str:
    """What an operator reads when an arm is refused, with the reason attached.

    `summary` stays first and unchanged: it is the round's own words for what it
    was doing, and it is what makes the rest legible. What follows is why.
    """

    if storage_access_refusal(error) is not None:
        return (
            f"{summary} Shared lakehouse storage is unavailable backstage. "
            "Rounds 4 and 6 are off the card; no run started."
        )
    if statement_execution_failure(error) is not None:
        return (
            f"{summary} A backstage data check failed; no run started. "
            "The operator log has the provider diagnosis."
        )
    if authorization_refusal(error) is not None:
        return (
            f"{summary} {GRANT_REFUSAL_HEADLINE} No run started. "
            "The operator log has the exact grant diagnosis."
        )
    return (
        f"{summary} A backstage check failed; no run started. "
        "The operator log has the diagnosis."
    )


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

    if privilege_refusal(error) is not None:
        return (
            f"{summary} {COST_LEDGER_GRANT_HEADLINE} No run started. "
            "The operator log has the exact grant diagnosis."
        )
    if authorization_refusal(error) is not None:
        return (
            f"{summary} {GRANT_REFUSAL_HEADLINE} No run started. "
            "The operator log has the exact grant diagnosis."
        )
    return (
        f"{summary} A backstage cost record could not be started; no run started. "
        "The operator log has the diagnosis."
    )


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


class RoundStorageReadinessError(RuntimeError):
    """A non-repairable missing Delta path attributed to one exact round."""

    def __init__(self, round_id: RoundId, message: str) -> None:
        if round_id not in {
            RoundId.PUT_MODEL_SCORE_IN_APP,
            RoundId.ANALYZE_LIVE_ORDERS,
        }:
            raise ValueError("Delta storage readiness applies only to Rounds 4 and 6")
        super().__init__(message)
        self.round_id = round_id


_ROUND_ONE_TRANSACTION_WIRE_CALL = "PostgreSQL TLS connect → INSERT → COMMIT → SELECT"

#: How long the next Round 4 Prepare waits for the previous bout to finish
#: settling (restore the row, let both lanes carry it, park both) before
#: refusing. Settling carries a row through both running lanes and then parks
#: a Glue run, which waits out its slot release, so it takes a minute or two.
_ROUND4_SETTLEMENT_WAIT_SECONDS = 480.0

_ROUND_FOUR_UNSUPPORTED_REASON = (
    "Round 4's AWS lane is not installed on this installation, so only Lakebase ran. "
    "It needs Round 4's Delta table to be readable from the installation's AWS account."
)
_ROUND_FOUR_CONDITION = "both integrations cold start at the bell"

#: The least time between two fight-card wakes of one matchup's destinations. Aurora pauses
#: after five idle minutes, so a fight card left open and asking every minute keeps it awake.
ROUND4_WAKE_INTERVAL_SECONDS = 60.0
#: One unrenewed fence's expiry check's read; below the 15 s close() waits for record work.
FENCE_EXPIRY_READ_SECONDS = 10.0


def _margin_floor_clause(winner_elapsed_ms: float | None, bound_seconds: float) -> str:
    """The loser was still running at its bound from the bell and the winner delivered at its
    own time, so only the bound less that time is proven."""

    if winner_elapsed_ms is None:
        return "so the margin is a lower bound and is not a measurement"
    # Rounded down: printing a floor rounded up would claim more than is proven. In
    # decimal, so binary float noise cannot take a cent off what is proven.
    proven = Decimal(str(bound_seconds)) - Decimal(str(winner_elapsed_ms)) / 1000
    floor_seconds = max(Decimal(0), proven).quantize(Decimal("0.01"), rounding=ROUND_FLOOR)
    return f"so the margin is at least {floor_seconds} s and is not a measurement"


def _round_four_comparison(
    result: Round4RaceResult,
    competitor_name: str,
    *,
    has_competitor: bool,
) -> tuple[ComparisonSnapshot, bool]:
    """The card's verdict, taken from the engine's resolution and never recomputed.

    Returns the comparison and whether the bout measured anything. A lane that
    errored measures nothing, so its bout is not a result.
    """

    resolution = result.resolution
    names = {LAKEBASE_LANE: "Lakebase", COMPETITOR_LANE: competitor_name}
    lakebase = result.outcomes[LAKEBASE_LANE]
    if not has_competitor:
        if not lakebase.verified:
            return (
                ComparisonSnapshot(
                    kind=ComparisonKind.NOT_COMPARABLE,
                    detail=lakebase.failure or "Lakebase did not deliver the row.",
                ),
                False,
            )
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.CAPABILITY_GAP,
                winner_lane_id=LAKEBASE_LANE,
                detail=_ROUND_FOUR_UNSUPPORTED_REASON,
            ),
            True,
        )
    if resolution.verdict is Verdict.WIN and resolution.winner and resolution.margin_ms:
        winner = resolution.winner
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.MEASURED,
                winner_lane_id=winner,
                margin=MetricValue(
                    spec_id="bell_to_exact_read_ms",
                    lane_id=winner,
                    value=resolution.margin_ms,
                    display_value=f"{resolution.margin_ms / 1000:.2f} s",
                ),
                detail=(
                    f"{names[winner]} put the exact row in the application "
                    f"{resolution.margin_ms / 1000:.2f} s sooner; {_ROUND_FOUR_CONDITION}."
                ),
            ),
            True,
        )
    if resolution.verdict is Verdict.WITHIN_RESOLUTION:
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.TIE,
                detail=(
                    "Both rows arrived within the verifiers' measurement resolution; "
                    f"{_ROUND_FOUR_CONDITION}."
                ),
            ),
            True,
        )
    if resolution.verdict is Verdict.LOWER_BOUND and resolution.winner:
        winner = resolution.winner
        loser = COMPETITOR_LANE if winner == LAKEBASE_LANE else LAKEBASE_LANE
        floor = _margin_floor_clause(result.outcomes[winner].elapsed_ms, LANE_TIMEOUT_SECONDS)
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.ADJUDICATED_STOPPAGE,
                winner_lane_id=winner,
                detail=(
                    f"{names[winner]} put the exact row in the application; {names[loser]} "
                    f"did not within {LANE_TIMEOUT_SECONDS:.0f} s of the bell, {floor}; "
                    f"{_ROUND_FOUR_CONDITION}."
                ),
            ),
            True,
        )
    return (
        ComparisonSnapshot(
            kind=ComparisonKind.NOT_COMPARABLE,
            detail=f"No verdict: {resolution.detail}.",
        ),
        False,
    )


def _round_four_remembered_result(snapshot: SessionSnapshot) -> str:
    """The one line the room remembers: who won and by how much, or why nobody did.

    Each lane's own clock is on screen above it, so as in Rounds 1 to 3 the line is
    the verdict alone. With both clocks and the round's name in it, it wrapped onto
    three lines of the ring (2026-09-29).
    """

    lakebase = snapshot.lanes[LAKEBASE_LANE]
    competitor = snapshot.lanes[COMPETITOR_LANE]
    comparison = snapshot.comparison

    if competitor.state == LaneState.NOT_SUPPORTED:
        elapsed = lakebase.elapsed_ms
        clock = f"{elapsed / 1000:.1f}s" if elapsed is not None else "NO ROW"
        return f"LAKEBASE {clock} · AWS LANE NOT INSTALLED"
    winner = None
    if comparison is not None and comparison.winner_lane_id is not None:
        winner = "LAKEBASE" if comparison.winner_lane_id == LAKEBASE_LANE else "AWS"
    if comparison is not None and comparison.kind == ComparisonKind.MEASURED and winner:
        margin = comparison.margin.value if comparison.margin is not None else 0.0
        return f"{winner} WINS · MARGIN {float(margin) / 1000:.1f}s"
    if comparison is not None and comparison.kind == ComparisonKind.TIE:
        return "TIE · WITHIN MEASUREMENT RESOLUTION"
    if winner:
        return f"{winner} WINS · MARGIN IS A LOWER BOUND"
    return "NO DECLARED WINNER · MARGIN N/A"


_ROUND_SIX_UNSUPPORTED_REASON = (
    "Round 6's AWS lane is not installed on this installation, so only Lakebase ran. It needs "
    "this installation's identity to create a Unity Catalog storage credential and external "
    "location."
)
#: Round 6's cold rule, as Ryan chose it (the v1.1 design, section 2): AWS starts cold at the bell,
#: and Lakebase's feed is part of the database and never stops.
_ROUND_SIX_CONDITION = (
    "AWS DMS and Glue start cold at the bell; Lakebase's change feed is built in and always on"
)

#: How long the next Round 6 Prepare waits for the previous bout to finish settling (remove its
#: orders, park DMS and Glue, and wait out Glue's slot release) before refusing.
_ROUND6_SETTLEMENT_WAIT_SECONDS = 480.0


def _round_six_comparison(
    result: Round6RaceResult,
    competitor_name: str,
    *,
    has_competitor: bool,
) -> tuple[ComparisonSnapshot, bool]:
    """The card's verdict, taken from the engine's resolution and never recomputed.

    Returns the comparison and whether the bout measured anything. A lane that errored measures
    nothing, so its bout is not a result.
    """

    resolution = result.resolution
    names = {LAKEBASE_LANE: "Lakebase", COMPETITOR_LANE: competitor_name}
    lakebase = result.outcomes[LAKEBASE_LANE]
    if not has_competitor:
        if not lakebase.verified:
            return (
                ComparisonSnapshot(
                    kind=ComparisonKind.NOT_COMPARABLE,
                    detail=lakebase.failure or "Lakebase did not deliver the order.",
                ),
                False,
            )
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.CAPABILITY_GAP,
                winner_lane_id=LAKEBASE_LANE,
                detail=_ROUND_SIX_UNSUPPORTED_REASON,
            ),
            True,
        )
    if resolution.verdict is Verdict.WIN and resolution.winner and resolution.margin_ms:
        winner = resolution.winner
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.MEASURED,
                winner_lane_id=winner,
                margin=MetricValue(
                    spec_id="bell_to_exact_history_ms",
                    lane_id=winner,
                    value=resolution.margin_ms,
                    display_value=f"{resolution.margin_ms / 1000:.2f} s",
                ),
                detail=(
                    f"{names[winner]} put the exact order in the lakehouse "
                    f"{resolution.margin_ms / 1000:.2f} s sooner; {_ROUND_SIX_CONDITION}."
                ),
            ),
            True,
        )
    if resolution.verdict is Verdict.WITHIN_RESOLUTION:
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.TIE,
                detail=(
                    "Both orders arrived within the verifiers' measurement resolution; "
                    f"{_ROUND_SIX_CONDITION}."
                ),
            ),
            True,
        )
    if resolution.verdict is Verdict.LOWER_BOUND and resolution.winner:
        winner = resolution.winner
        loser = COMPETITOR_LANE if winner == LAKEBASE_LANE else LAKEBASE_LANE
        floor = _margin_floor_clause(
            result.outcomes[winner].elapsed_ms,
            ROUND6_LANE_TIMEOUT_SECONDS,
        )
        return (
            ComparisonSnapshot(
                kind=ComparisonKind.ADJUDICATED_STOPPAGE,
                winner_lane_id=winner,
                detail=(
                    f"{names[winner]} put the exact order in the lakehouse; {names[loser]} did "
                    f"not within {ROUND6_LANE_TIMEOUT_SECONDS:.0f} s of the bell, {floor}; "
                    f"{_ROUND_SIX_CONDITION}."
                ),
            ),
            True,
        )
    return (
        ComparisonSnapshot(
            kind=ComparisonKind.NOT_COMPARABLE,
            detail=f"No verdict: {resolution.detail}.",
        ),
        False,
    )


def _round_six_remembered_result(snapshot: SessionSnapshot) -> str:
    """The one line the room remembers, in Round 4's shape: each lane's own clock is on screen
    above it, so the line is the verdict alone."""

    lakebase = snapshot.lanes[LAKEBASE_LANE]
    competitor = snapshot.lanes[COMPETITOR_LANE]
    comparison = snapshot.comparison
    if competitor.state == LaneState.NOT_SUPPORTED:
        elapsed = lakebase.elapsed_ms
        clock = f"{elapsed / 1000:.1f}s" if elapsed is not None else "NO ORDER"
        return f"LAKEBASE {clock} · AWS LANE NOT INSTALLED"
    winner = None
    if comparison is not None and comparison.winner_lane_id is not None:
        winner = "LAKEBASE" if comparison.winner_lane_id == LAKEBASE_LANE else "AWS"
    if comparison is not None and comparison.kind == ComparisonKind.MEASURED and winner:
        margin = comparison.margin.value if comparison.margin is not None else 0.0
        return f"{winner} WINS · MARGIN {float(margin) / 1000:.1f}s"
    if comparison is not None and comparison.kind == ComparisonKind.TIE:
        return "TIE · WITHIN MEASUREMENT RESOLUTION"
    if winner:
        return f"{winner} WINS · MARGIN IS A LOWER BOUND"
    return "NO DECLARED WINNER · MARGIN N/A"


def _finished_over_a_bound(
    finished: Mapping[str, bool],
    timed_out: Mapping[str, bool],
) -> tuple[str, str] | None:
    """Rounds 2 and 3, decided as Rounds 4 and 6 are: ``(winner, stopped)`` when exactly one
    lane finished and the other was still running at its bound from the bell.

    A lane that errored measured nothing, so nobody wins over it, and a lane still running
    is never handed a win.
    """

    if set(finished) != {LAKEBASE_LANE, COMPETITOR_LANE}:
        return None
    winners = [lane_id for lane_id, ok in finished.items() if ok]
    if len(winners) != 1:
        return None
    (winner,) = winners
    stopped = COMPETITOR_LANE if winner == LAKEBASE_LANE else LAKEBASE_LANE
    return (winner, stopped) if timed_out.get(stopped) else None


def _floor_evidence(floor_ms: float) -> dict[str, Any]:
    """A lane stopped at its bound, with its clock as a floor, in the shape a towel gives a
    lane it stops: the lane's clock, the receipt and the recap all read it as a lower bound.

    Ryan (2026-10-03): without the floor, a lane AWS could not finish "looks like the app
    crapped out and that's not the case".
    """

    return {
        "censored": True,
        "lower_bound_ms": floor_ms,
        "display_value": f">{floor_ms / 1000:.2f}s",
    }


def _adjudicate_bound(
    snapshot: SessionSnapshot,
    winner: str,
    stopped: str,
    *,
    finished_task: str,
) -> None:
    """Put a Round 2 or 3 bout decided at a lane's bound on the card.

    The stopped lane keeps the time it ran as its floor.
    """

    lane = snapshot.lanes[stopped]
    floor_ms = lane.elapsed_ms or 0.0
    lane.status = "Still running at its bound from the bell · stopped there"
    lane.error = None
    lane.evidence = _floor_evidence(floor_ms)
    display = lane.evidence["display_value"]
    snapshot.comparison = ComparisonSnapshot(
        kind=ComparisonKind.ADJUDICATED_STOPPAGE,
        winner_lane_id=winner,
        detail=(
            f"{snapshot.lanes[winner].name} {finished_task}; {lane.name} was still running "
            f"{display.removeprefix('>')} after the bell, so the margin is a lower bound "
            "and not a measurement."
        ),
    )


#: The fan-in target, read from the contract so this file cannot disagree with the runner
#: about what 10,000 means.
from .connection_fanin import MAX_RETRIES as ROUND5_MAX_RETRIES  # noqa: E402
from .connection_fanin import TARGET_CLIENTS_PER_LANE  # noqa: E402

_ROUND_FIVE_SCHEDULED_CLIENTS = 128
_ROUND_FIVE_WARMUP_CONNECTIONS = 0
_ROUND_FIVE_CONCURRENCY = 10_000
_ROUND_FIVE_WITNESS_CLIENTS = 64
_ROUND_FIVE_RUNNER = "Python 3.12 event-driven TLS/native-password"
_ROUND_FIVE_CLEANUP_PENDING = "Round 5 cleanup is settling automatically · Ring remains protected"
_ARMED_CARD_CANCELLED = (
    "Fight card released by the ring owner before the bell. "
    "No run started and no result was recorded."
)
_LOST_RING_STILL_STOPPING = (
    "BOUT IN PROGRESS · RING LEASE LOST · ACTIVE WORK IS STILL STOPPING · "
    "TRY AGAIN IN A MOMENT · IF IT PERSISTS, RESTART THE APP"
)
_LAST_BOUT_STILL_SETTLING = "THIS CARD'S LAST BOUT IS STILL SETTLING · TRY AGAIN IN A MOMENT"
_BELL_ACCEPTANCE_UNCONFIRMED = (
    "Bell acceptance could not be confirmed during a coordinator outage; "
    "the ring is left to its durable expiry."
)

#: Rounds whose towel cleanup retries automatically before it reports FAILED. Rounds 2 and 3
#: delete isolated AWS environments and so can race an in-flight create. Rounds 4 and 6 park
#: a Databricks pipeline and an AWS lane, and either can refuse once: rc23's Round 4 towel lost
#: its pipeline stop to "Please try again later" (2026-10-05), and with no retry the round
#: stayed held until a person pressed Retry cleanup.
_TOWEL_CLEANUP_AUTO_RETRY_ROUNDS = frozenset(
    {
        RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        RoundId.RECOVER_DELETED_ORDER,
        RoundId.PUT_MODEL_SCORE_IN_APP,
        RoundId.ANALYZE_LIVE_ORDERS,
    }
)

#: Rounds whose towel cleanup reports through a cooldown, which each retry starts afresh. The
#: others settle through their engine alone, and a cooldown nobody completes would fail every
#: retry as unverified.
_TOWEL_COOLDOWN_ROUNDS = frozenset(
    {RoundId.WAKE_IDLE_APP, RoundId.MAKE_SCHEMA_CHANGE_SAFELY, RoundId.RECOVER_DELETED_ORDER}
)

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

    async def publish(
        self,
        event: str,
        payload: dict[str, object],
        *,
        receipt_bout_id: str | None = None,
    ) -> RunEvent:
        """Append an event and wake its subscribers, then seal a receipt if it ends one.

        ``receipt_bout_id`` is the published snapshot's own attempt identity, which
        its JSON excludes. Every sealing publisher passes it from the snapshot it
        built, so a publish that lands after a re-arm still keys its own attempt.
        It goes to the receipt hook only, never into the event.
        """

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
        record_sealed_bout(event, payload, receipt_bout_id=receipt_bout_id)
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
class CooldownWatch:
    """One watch of a Round 1 cooldown, bound to what it was started for.

    ``cooldown`` is the exact snapshot it observes and ``owned`` the lease it began under.
    A card's next bout installs a new cooldown, and can hold a new lease, while an earlier
    watch is still waiting on a control plane, so a watch acts only while the record still
    shows that very cooldown and holds no lease but ``owned``.
    """

    cooldown: CooldownSnapshot
    owned: BoutLease | None
    # Consecutive Lakebase IDLE observations, timed in the process monotonic domain and
    # counted for this watch alone. The public snapshot carries only their count and
    # wall-clock upper bound; a raw monotonic value must never cross the API boundary.
    idle_observations: dict[str, tuple[int, int]] = field(default_factory=dict)


@dataclass
class SessionRecord:
    snapshot: SessionSnapshot
    event_log: EventLog = field(default_factory=EventLog)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    lease_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    task: asyncio.Task[None] | None = None
    arm_task_bout_id: str | None = None
    # One shielded CHECKING-cancel finalizer per immutable arm attempt. A prior
    # attempt may still be publishing its receipt after this card is re-armed.
    checking_cancel_tasks: dict[str, asyncio.Task[SessionSnapshot]] = field(
        default_factory=dict
    )
    cooldown_task: asyncio.Task[None] | None = None
    # Round 1's automatic idle watch while it runs in ``cooldown_task``: the one task
    # in that slot that ``RunManager._ring_work`` does not count.
    idle_watch_task: asyncio.Task[None] | None = None
    live_targets: tuple[LiveTarget, LiveTarget] | None = None
    safe_change_engine: SafeChangeEngine | None = None
    safe_change_arm: SafeChangeArm | None = None
    recovery_engine: RecoveryEngine | None = None
    recovery_arm: RecoveryArm | None = None
    recovery_stop_control: RecoveryStopControl | None = None
    recovery_outcome: RecoveryRunResult | RecoveryStoppedResult | None = None
    model_score_engine: Round4RaceEngine | None = None
    model_score_arm: Round4Arm | None = None
    model_score_result: Round4RaceResult | None = None
    model_score_pending_update: ModelScoreUpdate | None = None
    model_score_terminal_published: bool = False
    model_score_terminal_task: asyncio.Task[None] | None = None
    model_score_terminal_lease: BoutLease | None = None
    # One (registration token, fence) entry per `_settle_terminal_release` still
    # running, so concurrent settles of one fence each hold it and each removes
    # only its own. Like Round 4's terminal lease, a loss of exactly such a fence
    # is those settles' to decide, and the last to exit stops its heartbeat.
    terminal_release_settles: list[tuple[object, BoutLease]] = field(default_factory=list)
    # The non-renewing release retry left behind when a settle ran out of time.
    terminal_release_retry_task: asyncio.Task[None] | None = None
    terminal_release_retry_lease: BoutLease | None = None
    # The same retry for a finished bout's Round 5 cleanup lease.
    round5_release_retry_task: asyncio.Task[None] | None = None
    round5_release_retry_lease: BoutLease | None = None
    # Forgets an unrenewed fence once its row expires: a Round 1 cooldown failure's, or
    # the armed fence of a bell whose acceptance could not be confirmed.
    fence_expiry_task: asyncio.Task[None] | None = None
    # The lease row a failed Round 1 cooldown fences the ring with, from the
    # moment it fails until the durable row is gone.
    cooldown_failure_fence: BoutLease | None = None
    # An armed card's no-bell release is being settled outside `record.lock`;
    # until it finishes, the bell may not ring.
    armed_release_pending: bool = False
    # The owner's cancel that started such a release, by the arm attempt it releases:
    # pressing cancel again joins it rather than being refused.
    armed_owner_release: tuple[str | None, asyncio.Task[bool]] | None = None
    connection_spike_engine: object | None = None
    connection_spike_arm: object | None = None
    connection_spike_setup_result: object | None = None
    connection_spike_cleanup_task: asyncio.Task[None] | None = None
    #: Durable, automatic cleanup-convergence loop. When a backstage cleanup /
    #: rewarm handoff fails (e.g. a warm-slot ``WarmStoreConflictError`` CAS race,
    #: or a delete that has not yet been re-verified absent), this loop keeps
    #: re-running the reconcile+rewarm with bounded backoff until it converges,
    #: instead of surfacing a human "Retry Cleanup". No end-user action ever
    #: depends on it.
    connection_spike_cleanup_retry_task: asyncio.Task[None] | None = None
    live_orders_engine: Round6RaceEngine | None = None
    live_orders_arm: Round6Arm | None = None
    live_orders_result: Round6RaceResult | None = None
    live_orders_pending_order: LiveOrder | None = None
    live_orders_guardrail_order: LiveOrder | None = None
    settlement_task: asyncio.Task[None] | None = None
    # The HTTP/request task never owns the accepted-bell handoff. One manager
    # task carries an immutable arm identity from the durable lease transition
    # through RUNNING projection and runner publication; duplicate bells for
    # that identity join it and caller cancellation cannot tear it down.
    bell_handoff_task: asyncio.Task[SessionSnapshot] | None = None
    bell_handoff_bout_id: str | None = None
    # Set under record.lock immediately before the durable bell CAS. This
    # distinguishes a handoff that owns acceptance ambiguity from a task merely
    # registered before it got the lock; cancel wins the latter race.
    bell_commit_attempted_bout_id: str | None = None
    # When that commit's request was sent: a run_committed row adopted after its
    # answer was lost is provably live from here, not from when it was read.
    bell_commit_sent_at: float | None = None
    # The same for the CHECKING -> ARMED commit, whose row is adopted the same way.
    arm_commit_sent_at: float | None = None
    # A cancelled handoff can need coordinator/receipt I/O to classify an
    # ambiguous bell. Keep that child manager-owned as well: shutdown may cancel
    # the handoff that created it and must still drain this task before stores
    # close.
    bell_convergence_task: asyncio.Task[None] | None = None
    # Immutable process-local authority that Rounds 1-3 crossed the accepted-bell
    # boundary. Their runner publishes its timed start asynchronously, so the
    # snapshot can lag behind the durable ``run_committed`` lease. Cancellation
    # and late callbacks must consult this fact, not infer a no-bell arm from an
    # ARMED projection. Cleared only when a new arm identity is allocated.
    bell_committed_at: datetime | None = None
    run_started_monotonic_ns: int | None = None
    # Measurement-clock reading taken when a Round 5 setup lane last advanced
    # its published `setup_elapsed_ms`, keyed by lane id. Pairing the two lets a
    # towel carry that lane's elapsed time forward across a phase that reports
    # nothing, without the manager ever having to know the orchestrator's
    # private T0. Kept off the snapshot deliberately: it is a raw monotonic
    # reading, and no monotonic value may reach a public event.
    round5_progress_observed_ns: dict[str, int] = field(default_factory=dict)
    # Canonical wire identity for every accepted physical-runner revision.  SSM
    # stdout and the durable job registry are two transports for the same
    # progress record; an equal sequence is idempotent only when its payload is
    # identical.  Kept off the public snapshot because it is an ingestion
    # guard, not bout evidence.
    round5_progress_payload_digests: dict[tuple[str, int], str] = field(
        default_factory=dict
    )
    round5_terminal_lane_evidence: dict[str, dict[str, object]] = field(
        default_factory=dict
    )
    round5_projection_revision: int = 0
    round5_clock_projection_revision: int = 0
    # Set by the terminal mutation while holding ``lock``. Every Round 5
    # callback checks it before touching or publishing state, so a late SSM
    # progress line cannot resurrect RUNNING after FAILED/TOWELLED/VERIFIED.
    round5_ingestion_closed: bool = False
    round5_callback_epoch: int = 0
    round5_terminal_published: bool = False
    towel_generation: int = 0
    towel_stop_event: asyncio.Event | None = None
    armed_at_monotonic: float | None = None
    armed_expiry_task: asyncio.Task[None] | None = None
    lease_heartbeat_task: asyncio.Task[None] | None = None
    lease_heartbeat_lease: BoutLease | None = None
    lease: BoutLease | None = None
    # The confirmed write a lease came from, and the monotonic time until which that
    # write provably keeps it live (``RunManager._proven_until``).
    lease_proof: tuple[BoutLease, float] | None = None
    # Renews abandoned on sockets that never answered, by the fence each renewed. No
    # renew of a fence starts beside its own unanswered one, so a black hole holds at
    # most one per fence, and a newer fence is never held up by an older one's.
    abandoned_renewals: dict[tuple[str, int], asyncio.Future[Any]] = field(default_factory=dict)
    # The renew the heartbeat is waiting on, by fence. It runs as its own task, so
    # stopping the heartbeat abandons it too, or it could still be sent after the stop.
    renewal_in_flight: tuple[tuple[str, int], asyncio.Future[Any]] | None = None
    # Set when the ring lease is lost, lapses or is taken instead of released; cleared
    # once this record claims the ring again. The row can lapse before
    # ``RunManager._ring_work`` has finished, so until then this process does not
    # claim that ring.
    ring_lease_lost: bool = False
    round5_lease_heartbeat_task: asyncio.Task[None] | None = None
    round5_lease_heartbeat_lease: BoutLease | None = None
    round5_lease: BoutLease | None = None
    round5_lease_proof: tuple[BoutLease, float] | None = None
    round5_abandoned_renewals: dict[tuple[str, int], asyncio.Future[Any]] = field(
        default_factory=dict
    )
    round5_renewal_in_flight: tuple[tuple[str, int], asyncio.Future[Any]] | None = None
    round5_warm_slot: Round5WarmSlot | None = None
    round5_launch_capsule: Round5LaunchCapsule | None = None
    round5_bell_context: BellContext | None = None
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
        model_score_factory: Callable[[CompetitorId], Round4RaceEngine] | None = None,
        connection_spike_factory: Callable[[CompetitorId], object] | None = None,
        live_orders_factory: Callable[[CompetitorId], Round6RaceEngine] | None = None,
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
        delta_storage_probe: Callable[[], Awaitable[None]] | None = None,
        delta_storage_probe_interval_seconds: float = 60.0,
        delta_storage_probe_watched: Callable[[], bool] | None = None,
        round5_protocol: str = ROUND5_BELL_PROTOCOL,
        round5_warm_coordinator: Round5WarmCoordinator | None = None,
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
        # Calls given up on at a deadline: heartbeat renews and towel transitions, which
        # are cancelled, and a towel's cost-window close, which is left to land. Each
        # may still hold a coordinator connection, so close() waits for them, within
        # its settle window.
        self._abandoned_calls: set[asyncio.Future[Any]] = set()
        # Every unfinished heartbeat. One that lost its lease leaves its record's slot
        # while it still stops the bout, and close() stops it from here.
        self._heartbeat_tasks: set[asyncio.Task[None]] = set()
        self._resolver = resolver or TargetResolver()
        self._verifier = verifier or NeutralVerifier()
        self._safe_change_factory = safe_change_factory or self._build_safe_change_engine
        self._recovery_factory = recovery_factory or self._build_recovery_engine
        self._model_score_factory = model_score_factory
        self._connection_spike_factory = connection_spike_factory
        if round5_protocol not in ROUND5_PROTOCOLS:
            raise ValueError(f"Unknown Round 5 protocol: {round5_protocol}")
        self._round5_protocol = round5_protocol
        self._round5_warm_coordinator = round5_warm_coordinator
        self._live_orders_factory = live_orders_factory
        self._readiness_check = readiness_check or (lambda: None)
        self._readiness_status = readiness_status
        self._round5_readiness_check = round5_readiness_check or (lambda: None)
        self._round5_readiness_status = round5_readiness_status
        self._round5_prearm_guard = round5_prearm_guard
        # Refreshed by the same durable cleanup read used by bout status,
        # catalog admission, and ARM. A positive observation remains a
        # synchronous subtraction from ring_ready until a later durable read
        # proves the cleanup fence gone.
        self._round5_durable_cleanup_blocked = False
        # Rounds Databricks has refused on authorization in this process. Kept
        # here rather than on the session because the point of keeping it is to
        # stop the *next* fight card being offered, and the session that was
        # refused is released long before that happens.
        self._grant_refusals: dict[RoundId, str] = {}
        self._storage_refusals: dict[RoundId, str] = {}
        self._delta_storage_probe = delta_storage_probe
        if delta_storage_probe_interval_seconds < 0:
            raise ValueError("Delta storage probe interval cannot be negative")
        self._delta_storage_probe_interval_ns = int(
            delta_storage_probe_interval_seconds * 1_000_000_000
        )
        self._delta_storage_probe_finished_ns: int | None = None
        self._delta_storage_probe_task: asyncio.Task[None] | None = None
        # Whether a person is on the app. None counts as always, for a caller with no
        # such signal; the app passes one so an open tab cannot keep a warehouse up.
        self._delta_storage_probe_watched = delta_storage_probe_watched
        # Round 4's fight-card wake, one per competitor: the task, and when it last started.
        self._round4_wake_tasks: dict[CompetitorId, asyncio.Task[None]] = {}
        self._round4_woken_at: dict[CompetitorId, float] = {}
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
        # A towel closes its cost window holding record.lock, which every read of the
        # card and the lease's loss handling also take; it waits this long for the
        # ledger, then lets the close land without it.
        self._cost_close_wait = float(os.environ.get("ANTI_DEMO_COST_CLOSE_WAIT_SECONDS", "2"))
        # The bell handoff is shielded from its HTTP waiter because an accepted
        # durable bell must finish independently of a disconnected client. That
        # shield must not make a coordinator or cost-ledger outage immortal:
        # the handoff owns record.lock while it establishes the run, so every
        # control for the card would otherwise queue behind it forever.
        self._bell_handoff_timeout = float(
            os.environ.get("ANTI_DEMO_BELL_HANDOFF_TIMEOUT_SECONDS", "30")
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
        # The overall ceiling on that publication's own lease retry, and on every
        # other finished bout's terminal release retry. Bounding only the waiter
        # would leave such a loop running for the life of the process, because
        # it swallows every store failure by design.
        self._terminal_settle_deadline = float(
            os.environ.get("ANTI_DEMO_TERMINAL_SETTLE_DEADLINE_SECONDS", "30")
        )
        self._shutdown_settle_timeout = float(
            os.environ.get("ANTI_DEMO_SHUTDOWN_SETTLE_TIMEOUT_SECONDS", "15")
        )
        self._shutdown_cleanup_timeout = float(
            os.environ.get("ANTI_DEMO_SHUTDOWN_CLEANUP_TIMEOUT_SECONDS", "30")
        )
        # Bounded, fail-safe budget for cancelling local Round 5 lane bursts/run
        # tasks BEFORE begin_cleanup makes the slot CLEANING visible. If local
        # cancellation stalls past this, we still enter durable CLEANING so the
        # coordinator fence/authority is preserved.
        self._round5_local_cancel_timeout = float(
            os.environ.get("ANTI_DEMO_ROUND5_LOCAL_CANCEL_TIMEOUT_SECONDS", "5")
        )
        self._cleanup_retry_initial = float(
            os.environ.get("ANTI_DEMO_CLEANUP_RETRY_INITIAL_SECONDS", "1")
        )
        self._cleanup_retry_max = float(os.environ.get("ANTI_DEMO_CLEANUP_RETRY_MAX_SECONDS", "30"))
        # Rounds 2 and 3 delete isolated AWS environments on a towel, and a towel
        # that lands mid-create races the lane's own teardown: the first reset can
        # find a clone whose ownership is not provable yet and refuse it. That is a
        # cleanup still settling, not a failure, so it retries on its own (with the
        # backoff above) inside this window before the towel reports FAILED and
        # asks for Retry. Live 2026-09-26: a 78 s Round 2 towel sat in cleanup for
        # ~20 minutes until its lease expired, although one retry would have passed.
        self._towel_cleanup_retry_window = max(
            0.0,
            float(os.environ.get("ANTI_DEMO_TOWEL_CLEANUP_RETRY_WINDOW_SECONDS", "600")),
        )
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
        # The newest Round 4 background settlement. The ring is released before
        # it runs, so the next arm must wait for it rather than race it.
        self._round4_settlement: asyncio.Task[None] | None = None
        self._round6_settlement: asyncio.Task[None] | None = None
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

    def wake_round4(self, competitor: CompetitorId) -> bool:
        """Wake Round 4's destinations for this matchup ahead of Prepare. True if it began one.

        The fight card asks when it opens and again while it stays open, so a paused Aurora
        is awake before anyone presses Prepare. At most one wake per competitor per
        `ROUND4_WAKE_INTERVAL_SECONDS`, well inside Aurora's five idle minutes. Nothing is
        started: the integrations stay parked, so the race is as cold as ever.
        """

        factory = self._model_score_factory
        if factory is None or self._closed:
            return False
        now = time.monotonic()
        last = self._round4_woken_at.get(competitor)
        running = self._round4_wake_tasks.get(competitor)
        if (running is not None and not running.done()) or (
            last is not None and now - last < ROUND4_WAKE_INTERVAL_SECONDS
        ):
            return False
        self._round4_woken_at[competitor] = now

        async def wake() -> None:
            try:
                # Off the loop, as at arm: the live engine builds a workspace client and an AWS
                # session.
                engine = await asyncio.to_thread(factory, competitor)
                await engine.wake()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a wake is only a head start for Prepare
                logger.info("Round 4 fight-card wake for %s failed", competitor, exc_info=True)

        self._round4_wake_tasks[competitor] = asyncio.create_task(
            wake(), name=f"round4-wake-{competitor.value}"
        )
        return True

    @property
    def connection_spike_available(self) -> bool:
        return self._connection_spike_factory is not None


    @property
    def round5_protocol(self) -> str:
        return self._round5_protocol

    @property
    def round5_ring_ready(self) -> bool:
        # A local artifact lease is a third, synchronous gate. It closes the
        # health/catalog race where the warm cache still said READY for one beat
        # after cleanup authority moved to ``round5_cleanup``.
        #
        # Only an unexpired lease can close it. A lease past its expiry is no
        # longer authority anywhere -- the durable ring would admit the next
        # atomic claim -- so a record that kept one after a refused Prepare must
        # not hold the fight card at UNAVAILABLE until the process restarts
        # (observed live for 11 minutes on 2026-09-25). The durable claim still
        # decides who may arm; this gate only stops reporting a stale veto.
        now = datetime.now(UTC)
        if self._round5_durable_cleanup_blocked or any(
            record.round5_lease is not None and record.round5_lease.expires_at > now
            for record in self._records.values()
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
        ):
            return False
        return bool(
            self._round5_warm_coordinator is not None
            and self._round5_warm_coordinator.ring_ready
        )

    @property
    def round5_cleanup_owed(self) -> bool:
        """Whether this process holds or is retrying Round 5 cleanup work."""

        return any(
            record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            and (
                (
                    record.snapshot.round5_setup is not None
                    and record.snapshot.round5_setup.cleanup_retryable
                )
                or (
                    record.round5_lease is not None
                    and record.round5_lease.phase == "round5_cleanup"
                )
            )
            for record in self._records.values()
        )

    @property
    def round5_warm_status(self) -> Mapping[str, object] | None:
        coordinator = self._round5_warm_coordinator
        if coordinator is None:
            return None
        status = dict(coordinator.public_status_cached())
        cleanup_records = tuple(
            record
            for record in self._records.values()
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            and (
                (
                    record.round5_lease is not None
                    and record.round5_lease.phase == "round5_cleanup"
                )
                or (
                    record.snapshot.round5_setup is not None
                    and record.snapshot.round5_setup.cleanup_retryable
                )
            )
        )
        if cleanup_records:
            recovery_scheduled = any(
                (
                    record.connection_spike_cleanup_task is not None
                    and not record.connection_spike_cleanup_task.done()
                )
                or (
                    record.connection_spike_cleanup_retry_task is not None
                    and not record.connection_spike_cleanup_retry_task.done()
                )
                for record in cleanup_records
            )
            prebell_resident_cleanup = any(
                self._round5_prebell_cleanup_required(record)
                for record in cleanup_records
            )
            status.update(
                {
                    "round5_warm_state": "cleaning",
                    "round5_start_stage": "cleaning",
                    "round5_ring_ready": False,
                    "round5_cleanup_owed": True,
                    "round5_cleanup_recovery_scheduled": recovery_scheduled,
                    "round5_cleanup_scope": (
                        "prebell_resident"
                        if prebell_resident_cleanup
                        else "postbell_resources"
                    ),
                }
            )
        return status

    @staticmethod
    def _round5_prebell_cleanup_required(record: SessionRecord) -> bool:
        """Whether the warm claim never crossed the bell transaction.

        Derived from durable bell/session truth, NOT solely a cached warm slot: a
        durable bell acceptance records ``record.round5_bell_context`` (and the
        RUNNING session state), which is authoritative even if the cached
        ``round5_warm_slot`` still reads CLAIMED/null-bell because the local record
        was not yet refreshed from the RUNNING slot. Misreading a post-bell abort
        as pre-bell would skip the mandatory post-setup teardown
        (cancel_setup_and_settle / journal / provider absence) for a bout that had
        already created a per-bout Proxy intent and setup resources.
        """

        if getattr(record, "round5_bell_context", None) is not None:
            return False
        slot = record.round5_warm_slot
        if slot is not None and getattr(slot, "bell_id", None) is not None:
            return False
        return bool(
            slot is not None
            and slot.claim is not None
            and hasattr(slot, "bell_id")
            and hasattr(slot, "bell_at_utc")
            and slot.bell_id is None
            and slot.bell_at_utc is None
        )

    async def round5_durable_cleanup_overlay(
        self,
        lease: BoutLease | None = None,
    ) -> Mapping[str, object] | None:
        """Read the durable cleanup fence and warm claim for health surfaces."""

        if lease is None:
            try:
                lease = await self._round5_cleanup_store().current()
            except Exception:
                # A SINGLE failed read of the cleanup-fence lease (a transient store
                # blip) must NOT latch ``_round5_durable_cleanup_blocked`` and paint the
                # idle fight card "Temporarily Unavailable" for ~30s across board polls
                # while the warm slot is still READY. The warm coordinator's own
                # ``cleanup_owed``/``claim`` fields are the authority for whether a
                # cleanup is actually owed; a read blip here is UNKNOWN, not blocked.
                # Return no overlay (do not withdraw READY) and leave the latch as-is so
                # a genuine cleanup lease (read successfully below) still surfaces.
                return None
        if lease is None or lease.phase != "round5_cleanup":
            self._round5_durable_cleanup_blocked = False
            return None
        self._round5_durable_cleanup_blocked = True

        slot = None
        coordinator = self._round5_warm_coordinator
        store = getattr(coordinator, "store", None)
        read = getattr(store, "read", None)
        installation_id = getattr(coordinator, "installation_id", None)
        if callable(read) and isinstance(installation_id, str):
            try:
                slot = await read(installation_id)
            except Exception:
                slot = None
        prebell = bool(
            slot is not None
            and slot.claim is not None
            and slot.claim.session_id == lease.session_id
            and slot.bell_id is None
            and slot.bell_at_utc is None
        )
        local = self._records.get(lease.session_id)
        recovery_scheduled = bool(
            local is not None
            and (
                (
                    local.connection_spike_cleanup_task is not None
                    and not local.connection_spike_cleanup_task.done()
                )
                or (
                    local.connection_spike_cleanup_retry_task is not None
                    and not local.connection_spike_cleanup_retry_task.done()
                )
            )
        )
        generation = getattr(slot, "generation", None)
        revision = getattr(slot, "revision", None)
        last_error_code = getattr(slot, "last_error_code", None)
        generation_detail = (
            f"GENERATION {generation}"
            if isinstance(generation, int)
            else "GENERATION PENDING"
        )
        scope_detail = (
            "PRE-BELL RESIDENT CLEANUP"
            if prebell
            else "ROUND 5 RESOURCE CLEANUP"
        )
        worker_detail = (
            "AUTO-CONVERGE SCHEDULED"
            if recovery_scheduled
            else "RECOVERY WORKER NOT CONFIRMED"
        )
        return {
            "round5_ring_ready": False,
            "round5_cleanup_owed": True,
            "round5_warm_state": "cleaning",
            "round5_start_stage": "cleaning",
            "round5_warm_generation": generation,
            "round5_warm_revision": revision,
            "round5_warm_last_error_code": last_error_code,
            "round5_cleanup_scope": (
                "prebell_resident" if prebell else "postbell_resources"
            ),
            "round5_cleanup_recovery_scheduled": recovery_scheduled,
            "round5_cleanup_detail": (
                f"ROUND 5 NOT STARTABLE · STAGE CLEANING · {generation_detail} · "
                f"CAN_START FALSE"
                + (
                    f" · ERROR {last_error_code}"
                    if isinstance(last_error_code, str) and last_error_code
                    else ""
                )
                + f" · {scope_detail} · {worker_detail} · "
                "WAIT FOR READY / RING_READY TRUE"
            ),
        }

    @staticmethod
    def _round5_structured_start(
        status: Mapping[str, object] | None,
    ) -> RoundFiveStartStatus | None:
        if status is None:
            return None
        allowed_stages = {
            "ready",
            "cleaning",
            "claim-drain",
            "terminal-blocked",
            "identity-refresh",
            "rewarming",
        }
        stage = str(status.get("round5_start_stage") or "rewarming").lower()
        if stage not in allowed_stages:
            stage = "rewarming"
        generation = status.get("round5_warm_generation")
        recovery = status.get("round5_cleanup_recovery_scheduled")
        scope = status.get("round5_cleanup_scope")
        revision = status.get("round5_warm_revision")
        last_error_code = status.get("round5_warm_last_error_code")
        return RoundFiveStartStatus(
            stage=stage,
            generation=(
                generation
                if isinstance(generation, int) and not isinstance(generation, bool)
                else None
            ),
            recovery_scheduled=recovery if isinstance(recovery, bool) else None,
            cleanup_scope=(
                scope
                if scope in {"prebell_resident", "postbell_resources", "unknown"}
                else None
            ),
            revision=(
                revision
                if isinstance(revision, int) and not isinstance(revision, bool)
                else None
            ),
            last_error_code=str(last_error_code) if last_error_code else None,
        )

    def _round5_start_refusal(self) -> str:
        status = self.round5_warm_status or {}
        stage = str(status.get("round5_start_stage") or "rewarming").upper()
        generation = status.get("round5_warm_generation")
        generation_detail = (
            f"GENERATION {generation}"
            if isinstance(generation, int)
            else "GENERATION PENDING"
        )
        error = status.get("round5_warm_last_error_code")
        error_detail = f" · ERROR {error}" if isinstance(error, str) and error else ""
        recovery_detail = (
            " · AUTO-CONVERGE SCHEDULED"
            if stage == "CLEANING"
            and status.get("round5_cleanup_recovery_scheduled") is True
            else " · RECOVERY WORKER NOT CONFIRMED"
            if stage == "CLEANING"
            else ""
        )
        scope_detail = (
            " · PRE-BELL RESIDENT CLEANUP"
            if status.get("round5_cleanup_scope") == "prebell_resident"
            else ""
        )
        return (
            f"ROUND 5 NOT STARTABLE · STAGE {stage} · {generation_detail} · "
            f"CAN_START FALSE{error_detail}{scope_detail}{recovery_detail} · "
            "WAIT FOR READY / RING_READY TRUE"
        )

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

    @property
    def storage_refusals(self) -> Mapping[RoundId, str]:
        """Rounds whose latest readiness probe or arm proved storage inaccessible."""

        return dict(self._storage_refusals)

    async def refresh_delta_storage_readiness(self, *, force: bool = False) -> None:
        """Refresh the cached shared-storage verdict once, with request coalescing.

        Catalog and all-round status reads call this before advertising Round 4
        or Round 6. The probe is read-only and cached so the polling fight card
        cannot turn into a SQL warehouse keepalive. Concurrent browser polls join
        the same task rather than issuing duplicate Statement Execution calls.

        The cache alone did not do that: it lasts a minute, the warehouse stops
        only after ten quiet minutes, and an open tab polls the catalog every
        thirty seconds, so a tab left open kept the warehouse up all night
        (2026-10-02). Once a verdict exists, it is re-read only while a person is
        on the app (``delta_storage_probe_watched``).
        """

        if self._closed or self._delta_storage_probe is None:
            return
        running = self._delta_storage_probe_task
        if running is not None and not running.done():
            await asyncio.shield(running)
            return
        now = time.monotonic_ns()
        last = self._delta_storage_probe_finished_ns
        if not force and last is not None:
            fresh = now - last < self._delta_storage_probe_interval_ns
            watched = self._delta_storage_probe_watched
            if fresh or (watched is not None and not watched()):
                return
        task = asyncio.create_task(
            self._probe_delta_storage(),
            name="round4-round6-storage-readiness",
        )
        self._delta_storage_probe_task = task
        try:
            await asyncio.shield(task)
        finally:
            if task.done() and self._delta_storage_probe_task is task:
                self._delta_storage_probe_task = None

    async def _probe_delta_storage(self) -> None:
        try:
            probe = self._delta_storage_probe
            if probe is None:
                return
            await probe()
        except asyncio.CancelledError:
            raise
        except DeltaStorageAccessDeniedError as exc:
            diagnosis = operator_diagnosis(exc)
            refusal = storage_refusal(diagnosis)
            self._storage_refusals[RoundId.PUT_MODEL_SCORE_IN_APP] = refusal
            self._storage_refusals[RoundId.ANALYZE_LIVE_ORDERS] = refusal
            logger.error(
                "Shared Delta storage readiness was refused; Rounds 4 and 6 are "
                "disabled before Prepare. diagnosis=%s provider_category=%s "
                "error_code=%s sqlstate=%s",
                diagnosis,
                exc.provider_category,
                exc.error_code,
                exc.sql_state or "unreported",
                exc_info=True,
            )
        except RoundStorageReadinessError as exc:
            diagnosis = operator_diagnosis(exc)
            self._storage_refusals[exc.round_id] = missing_delta_refusal(diagnosis)
            logger.error(
                "Round %d Delta storage path is missing and has no safe runtime repair. "
                "diagnosis=%s",
                4 if exc.round_id == RoundId.PUT_MODEL_SCORE_IN_APP else 6,
                diagnosis,
                exc_info=True,
            )
        except Exception as exc:
            # An inconclusive readiness read must not clear a prior denial or
            # manufacture a new one. The exact arm preflight still fails closed.
            logger.warning(
                "Shared Delta storage readiness could not be refreshed: %s",
                operator_diagnosis(exc),
                exc_info=True,
            )
        else:
            self._storage_refusals.pop(RoundId.PUT_MODEL_SCORE_IN_APP, None)
            self._storage_refusals.pop(RoundId.ANALYZE_LIVE_ORDERS, None)
        finally:
            self._delta_storage_probe_finished_ns = time.monotonic_ns()

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
        bout_id: str | None = None,
    ) -> str | None:
        store = self._cost_ledger_store
        manifest = self._cost_manifest
        if store is None or manifest is None:
            return None
        if record.cost_bout_id is not None:
            raise InvalidStateError("The current cost bout window is already open")
        bout_id = bout_id or uuid4().hex
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
        # A towel's close can land after the card has moved on to a newer bout.
        if record.cost_bout_id == bout_id:
            record.cost_bout_id = None
            record.cost_bout_started_at = None
            record.cost_bout_kind = None

    async def _close_towel_cost_window(
        self,
        record: SessionRecord,
        towel: TowelSnapshot,
        *,
        bout_id: str,
    ) -> bool:
        """Close the cost window a towel ended; False if the ledger has not answered.

        Callers hold ``record.lock``, so the ledger gets ``_cost_close_wait`` and no
        more. An unanswered close lands on its own and reports on the towel. Every
        attempt ends the window at the towel, so attempts never disagree.
        """

        closing = asyncio.ensure_future(
            self._close_cost_bout(
                record,
                bout_id=bout_id,
                ended_at=towel.requested_at,
                outcome=SessionState.TOWELLED.value,
            )
        )
        try:
            await asyncio.wait({closing}, timeout=max(0.001, self._cost_close_wait))
        finally:
            if not closing.done():
                self._let_towel_cost_close_land(record, towel, closing, bout_id=bout_id)
        if not closing.done():
            return False
        closing.result()
        return True

    def _let_towel_cost_close_land(
        self,
        record: SessionRecord,
        towel: TowelSnapshot,
        closing: asyncio.Future[None],
        *,
        bout_id: str,
    ) -> None:
        logger.warning(
            "Towel cost window close unanswered after %.1fs session=%s; it lands on its own",
            self._cost_close_wait,
            record.snapshot.id,
        )
        self._abandoned_calls.add(closing)

        def landed(done: asyncio.Future[None]) -> None:
            self._abandoned_calls.discard(done)
            if done.cancelled():
                return
            failure = done.exception()
            if failure is not None:
                logger.error(
                    "Towel could not close the cost window session=%s diagnostic=%s",
                    record.snapshot.id,
                    _redacted_exception_chain(failure),
                )
            if record.snapshot.towel is not towel:
                return
            if failure is None:
                towel.cost_close_failure = None
            elif record.cost_bout_id == bout_id:
                # Only while the window is still this card's to close: a retry may
                # already have closed it, or handed it to reconciliation.
                towel.cost_close_failure = (
                    str(failure) or "The bout cost window could not be closed"
                )

        closing.add_done_callback(landed)

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
                towel = record.snapshot.towel
                # Every close of a towelled window ends it at the towel, as the towel's
                # own close does; towel cleanup moves ``updated_at`` on before this runs.
                ended_at = towel.requested_at if towel is not None else record.snapshot.updated_at
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

    async def _run_round5_cost_bout_after_bell(
        self,
        record: SessionRecord,
        *,
        started_at: datetime,
        bell_id: str,
        operation: asyncio.Task[None],
    ) -> None:
        """Persist Round 5 cost evidence without holding the bell request open."""

        cost_bout_id: str | None = None
        try:
            try:
                cost_bout_id = await self._open_cost_bout(
                    record,
                    kind="run",
                    started_at=started_at,
                    bout_id=hashlib.sha256(
                        (
                            f"round5-cost\0{self._installation_id}\0"
                            f"{record.snapshot.id}\0{bell_id}"
                        ).encode()
                    ).hexdigest(),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Bell acceptance and both lane tasks already exist.  Cost
                # bookkeeping is reconciliation debt, never authority to undo
                # the single durable bell or strand its cleanup leases.
                logger.error(
                    "Round 5 cost window open failed after bell session=%s "
                    "diagnostic=%s",
                    record.snapshot.id,
                    _redacted_exception_chain(exc),
                )
            await self._run_cost_bout(record, cost_bout_id, operation)
        finally:
            if not operation.done():
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)

    def _require_open(self) -> None:
        if self._closed:
            raise InvalidStateError("The server is restarting; refresh in a moment")

    async def _drain_armed_release_for_shutdown(self, record: SessionRecord) -> bool:
        """Drain a no-bell finalizer; cancel only an idle expiry timer."""

        operation = record.armed_expiry_task
        if not record.armed_release_pending:
            self._cancel_armed_expiry(record, force=True)
            return True
        if (
            operation is None
            or operation is asyncio.current_task()
            or operation.done()
        ):
            return operation is None or operation.done()

        done, _ = await asyncio.wait(
            {operation},
            timeout=max(0.001, self._shutdown_cleanup_timeout),
        )
        if operation not in done:
            operation.cancel()
            done, _ = await asyncio.wait(
                {operation},
                timeout=max(0.001, self._shutdown_settle_timeout),
            )
        if operation in done:
            try:
                operation.result()
            except (asyncio.CancelledError, Exception):
                pass

        # Round 5 converts cancellation into a separately tracked durable
        # no-bell finalizer. It can be installed only after the parent catches
        # CancelledError, so discover it after draining the original task.
        replacement = record.armed_expiry_task
        if (
            replacement is not None
            and replacement is not operation
            and replacement is not asyncio.current_task()
            and not replacement.done()
        ):
            replacement_done, _ = await asyncio.wait(
                {replacement},
                timeout=max(0.001, self._shutdown_cleanup_timeout),
            )
            if replacement not in replacement_done:
                replacement.cancel()
                await asyncio.gather(replacement, return_exceptions=True)
        current = record.armed_expiry_task
        return current is None or current.done()

    async def close(self) -> None:
        """Settle active work before the coordinator connection is closed.

        Only the record holding the exact current fence may clean or release the
        ring. If task settlement or cleanup cannot be proved, the durable lease
        is deliberately left to expire so startup reconciliation runs before the
        next bout.

        All tasks in :meth:`_unfinished_operations` are accounted for here, in
        different ways deliberately. An idle ``armed_expiry_task`` is only a timer
        and is cancelled, but once it owns ``armed_release_pending`` it is the
        no-bell finalizer and is drained before shutdown proceeds (including a
        Round 5 replacement child). The operations in the wait set below are
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
            storage_probe = self._delta_storage_probe_task
            if (
                storage_probe is not None
                and storage_probe is not asyncio.current_task()
                and not storage_probe.done()
            ):
                storage_probe.cancel()
                await asyncio.gather(storage_probe, return_exceptions=True)
            self._delta_storage_probe_task = None
            wakes = [task for task in self._round4_wake_tasks.values() if not task.done()]
            for wake in wakes:
                wake.cancel()
            await asyncio.gather(*wakes, return_exceptions=True)
            self._round4_wake_tasks.clear()
            async with self._records_lock:
                records = tuple(self._records.values())

            settled: dict[str, bool] = {}
            for record in records:
                armed_release_settled = await self._drain_armed_release_for_shutdown(
                    record
                )
                # A checking cancel owns its no-bell receipt just as an armed
                # cancel does. Give already-running finalizers a bounded grace
                # period before the ordinary cancellation sweep.
                checking_finalizers = {
                    task
                    for task in record.checking_cancel_tasks.values()
                    if task is not asyncio.current_task() and not task.done()
                }
                if checking_finalizers:
                    await asyncio.wait(
                        checking_finalizers,
                        timeout=max(0.001, self._shutdown_cleanup_timeout),
                    )
                # A bell handoff owns the ambiguity-sensitive durable transition.
                # Let it publish RUNNING and install the runner before shutdown
                # starts cancelling ordinary background work. Cancelling it first
                # could land between the lease-store commit and local projection,
                # which is exactly the stranded ARMED state this task prevents.
                handoff = record.bell_handoff_task
                if (
                    handoff is not None
                    and handoff is not asyncio.current_task()
                    and not handoff.done()
                ):
                    done, _ = await asyncio.wait(
                        {handoff},
                        timeout=max(0.001, self._shutdown_settle_timeout),
                    )
                    for operation in done:
                        try:
                            operation.result()
                        except (asyncio.CancelledError, Exception):
                            pass
                # Cancelling record.task alone does NOT stop the lane bursts
                # that run() popped into its launched map -- an orphaned burst could
                # keep dispatching. Explicitly cancel the engine's local run tasks
                # (both _lane_bursts and _run_burst_tasks) before the coordinator is
                # closed, so no burst survives manager shutdown. Idempotent + local-only.
                engine = record.connection_spike_engine
                cancel_local = (
                    getattr(engine, "cancel_local_round5_run_tasks", None)
                    if engine is not None
                    else None
                )
                if callable(cancel_local):
                    try:
                        await cancel_local()
                    except Exception:
                        logger.error(
                            "Round 5 local run-task cancellation failed during "
                            "shutdown session=%s",
                            record.snapshot.id,
                            exc_info=True,
                        )
                operations = {
                    operation
                    for operation in (
                        record.task,
                        record.bell_handoff_task,
                        record.bell_convergence_task,
                        *record.checking_cancel_tasks.values(),
                        record.cooldown_task,
                        record.connection_spike_cleanup_task,
                        record.connection_spike_cleanup_retry_task,
                        record.settlement_task,
                        record.terminal_release_retry_task,
                        record.round5_release_retry_task,
                        record.fence_expiry_task,
                    )
                    if operation is not None
                    and operation is not asyncio.current_task()
                    and not operation.done()
                }
                for operation in operations:
                    operation.cancel()
                if operations:
                    done, pending = await asyncio.wait(
                        operations,
                        timeout=max(0.001, self._shutdown_settle_timeout),
                    )
                else:
                    done, pending = set(), set()
                for operation in done:
                    try:
                        operation.result()
                    except (asyncio.CancelledError, Exception):
                        pass
                # Cancellation of the handoff can install its convergence child
                # after the operation snapshot above. Discover that handoff just
                # as the armed-release drain discovers a Round 5 replacement.
                convergence = record.bell_convergence_task
                convergence_settled = True
                if (
                    convergence is not None
                    and convergence is not asyncio.current_task()
                    and not convergence.done()
                ):
                    done_convergence, _ = await asyncio.wait(
                        {convergence},
                        timeout=max(0.001, self._shutdown_cleanup_timeout),
                    )
                    convergence_settled = convergence in done_convergence
                    if not convergence_settled:
                        convergence.cancel()
                        done_convergence, _ = await asyncio.wait(
                            {convergence},
                            timeout=max(0.001, self._shutdown_settle_timeout),
                        )
                        convergence_settled = convergence in done_convergence
                # Round 4 installs this shielded terminal publisher from inside
                # record.task. Cancelling the parent can therefore create the
                # child after the operation snapshot, and the child is the work
                # that seals run_finished. Drain it while the receipt and lease
                # stores are still installed; if it exceeds its publication
                # budget, cancel and settle it so no write survives store close.
                model_terminal = record.model_score_terminal_task
                model_terminal_settled = True
                if (
                    model_terminal is not None
                    and model_terminal is not asyncio.current_task()
                    and not model_terminal.done()
                ):
                    done_terminal, _ = await asyncio.wait(
                        {model_terminal},
                        timeout=max(0.001, self._terminal_publish_timeout),
                    )
                    model_terminal_settled = model_terminal in done_terminal
                    if not model_terminal_settled:
                        model_terminal.cancel()
                        done_terminal, _ = await asyncio.wait(
                            {model_terminal},
                            timeout=max(0.001, self._shutdown_settle_timeout),
                        )
                        model_terminal_settled = model_terminal in done_terminal
                settled[record.snapshot.id] = (
                    armed_release_settled
                    and all(operation.done() for operation in pending)
                    and convergence_settled
                    and model_terminal_settled
                )

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
                fence = record.cooldown_failure_fence
                if local_lease.phase == "cooldown_failed" or (
                    fence is not None and self._same_lease_authority(local_lease, fence)
                ):
                    # Unconfirmed-idle targets stay fenced until the durable TTL.
                    logger.warning(
                        "Shutdown left the Round 1 cooldown failure fence to its durable TTL "
                        "session=%s lease_id=%s phase=%s expires_at=%s",
                        record.snapshot.id,
                        local_lease.lease_id,
                        local_lease.phase,
                        local_lease.expires_at.isoformat(),
                    )
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

            # The twelfth task, and the one that is owned by no record at all.
            # A settled Round 4 bout leaves a delayed pipeline release keyed by
            # pipeline in `model_score_live._ACTIVATIONS`, so none of the
            # per-record accounting above can see it -- and cancelling it
            # silently, which is what happened before this line, threw away a
            # stop on a pipeline that bills $14.57/day while it is up, with
            # nothing anywhere recording that one was owed.
            await self._release_pipelines_for_shutdown()

            heartbeats = {
                task
                for task in self._heartbeat_tasks
                if task is not asyncio.current_task() and not task.done()
            }
            for record in records:
                # A failed cleanup stays fenced only until its durable TTL. No
                # process-local heartbeat may survive manager shutdown.
                self._cancel_lease_heartbeat(record)
                self._cancel_round5_lease_heartbeat(record)
            for heartbeat in heartbeats:
                heartbeat.cancel()
            # An abandoned call may still hold a coordinator connection: one settle
            # window for the heartbeats to stop and for the calls given up on.
            settle_until = time.monotonic() + max(0.001, self._shutdown_settle_timeout)
            if heartbeats:
                await asyncio.wait(heartbeats, timeout=max(0.001, self._shutdown_settle_timeout))
            abandoned = {call for call in self._abandoned_calls if not call.done()}
            if abandoned:
                await asyncio.wait(
                    abandoned,
                    timeout=max(0.001, settle_until - time.monotonic()),
                )

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
                if self._round5_warm_coordinator is not None:
                    # With a coordinator the manager's own cleanup is a local no-op
                    # and is NOT external cleanup proof. Shutdown must not release
                    # the ring / Round 5 leases on the strength of it while a shared
                    # converge may still be mutating -- the coordinator.close drains
                    # those tasks separately and safely. Report "not verified" so
                    # close() retains durable CLEANING + leases/fencing for startup/
                    # takeover reconciliation instead of releasing from a no-op.
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

        task = asyncio.current_task()
        cancels_on_entry = task.cancelling() if task is not None else 0
        try:
            async with asyncio.timeout(max(0.001, self._shutdown_cleanup_timeout)):
                return await cleanup()
        except (asyncio.CancelledError, Exception) as exc:
            self._reraise_own_cancel(exc, cancels_on_entry)
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
                round5_protocol=self.round5_protocol,
            )
        else:
            selected_round = round_by_id(
                request.round_id,
                model_score_available=self.model_score_available,
                connection_spike_available=self.connection_spike_available,
                live_orders_available=self.live_orders_available,
                round5_protocol=self.round5_protocol,
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
            and record.snapshot.round5_runtime is None
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
            if as_of_ns is None
            and (
                needs_lane_projection
                or needs_round_five_projection
                or (
                    record.snapshot.round5_runtime is not None
                    and record.snapshot.state == SessionState.RUNNING
                )
            )
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

        if setup is not None and snapshot.round5_runtime is None:
            floors = self._round_five_elapsed_floors(record, as_of_ns=measured_at_ns)
            for lane_id, lane in setup.lanes.items():
                lane.elapsed_at_snapshot_ms = floors.get(lane_id)
        runtime = snapshot.round5_runtime
        if (
            runtime is not None
            and record.snapshot.state == SessionState.RUNNING
            and record.run_started_monotonic_ns is not None
        ):
            if not measured_at_ns:
                measured_at_ns = self._clock_ns()
            bell_floor_ms = max(
                0.0,
                (measured_at_ns - record.run_started_monotonic_ns) / 1_000_000,
            )
            record.round5_clock_projection_revision += 1
            snapshot.round5_clock_projection = RoundFiveClockProjectionSnapshot(
                bell_id=runtime.bell_id,
                projection_revision=record.round5_clock_projection_revision,
                elapsed_ms={
                    lane_id: (
                    lane.bell_to_10000_observed_ms
                    if lane.bell_to_10000_observed_ms is not None
                    else max(lane.elapsed_at_snapshot_ms, bell_floor_ms)
                    )
                    for lane_id, lane in runtime.lanes.items()
                },
            )
        return self._revalidated_snapshot(snapshot)

    @staticmethod
    def _revalidated_snapshot(snapshot: SessionSnapshot) -> SessionSnapshot:
        revalidated = SessionSnapshot.model_validate(
            snapshot.model_dump(mode="python")
        )
        # Excluded from every dump, so the round trip would otherwise drop it.
        revalidated.receipt_bout_id = snapshot.receipt_bout_id
        return revalidated

    @staticmethod
    def _advance_round5_revision_locked(record: SessionRecord) -> int:
        """Allocate exactly one revision for a Round 5 state mutation.

        Callers hold ``record.lock``. Projection/GET paths never call this;
        revisions describe durable logical mutations, not how often a browser
        happened to poll.
        """

        if not record.lock.locked():
            raise RuntimeError("Round 5 revision allocation requires the record lock")
        runtime = record.snapshot.round5_runtime
        if runtime is None:
            return 0
        record.round5_projection_revision = max(
            record.round5_projection_revision,
            runtime.revision,
        ) + 1
        runtime.revision = record.round5_projection_revision
        return runtime.revision

    @staticmethod
    def _new_round_five_runtime(context: BellContext) -> RoundFiveRuntimeSnapshot:
        return RoundFiveRuntimeSnapshot(
            warm_generation=context.warm_generation,
            bell_id=context.bell_id,
            revision=1,
            state="running",
            bell_at_utc=context.bell_at_utc,
            lanes={
                "lakebase": RoundFiveRuntimeLaneSnapshot(
                    id="lakebase",
                    phase="dispatching",
                    elapsed_at_snapshot_ms=0.0,
                    status="Dispatching the retained first Lakebase client",
                ),
                "competitor": RoundFiveRuntimeLaneSnapshot(
                    id="competitor",
                    phase="provisioning_proxy",
                    elapsed_at_snapshot_ms=0.0,
                    status="AWS is creating the per-bout RDS Proxy",
                ),
            },
        )

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
        """Cancel a fight card before its bell.

        Round 1 cancels a start-state check that is still running. Every round cancels an
        ARMED card that has not rung, through the same no-bell release the armed-window
        expiry uses, so changing the matchup costs one release rather than the rest of the
        window. Round 5's goes through its own warm-pool cleanup.
        """
        self._require_open()
        record = await self._record(session_id)
        if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
            return await self._cancel_armed_round5(record, operator)
        # Only RUNNING is read off the card's lock, since it can only be this bout's.
        # Each path below checks the accepted bell under the lock: an owner re-arming
        # the card holds it while the ring is claimed, and until then the card still
        # carries the last bout's bell.
        if record.snapshot.state == SessionState.RUNNING:
            raise InvalidStateError(
                "Only an armed fight card that has not rung can be cancelled"
            )
        if record.snapshot.state == SessionState.ARMED or (
            record.snapshot.state == SessionState.FAILED
            and record.snapshot.failure == _ARMED_CARD_CANCELLED
        ):
            return await self._cancel_armed_card(record, operator)
        return await self._cancel_checking_card(record, operator)

    async def _cancel_checking_card(
        self,
        record: SessionRecord,
        operator: BoutOperator,
    ) -> SessionSnapshot:
        """Finish a Round 1 checking cancellation independently of its HTTP caller."""

        message = (
            "Fight-card check cancelled by the ring owner. "
            "No run started and no result was recorded."
        )
        async with record.lock:
            if (
                record.snapshot.state == SessionState.FAILED
                and record.snapshot.failure is not None
                and record.snapshot.failure.startswith("Fight-card check cancelled")
            ):
                self._assert_operator(record.operator, operator)
                return record.snapshot.model_copy(deep=True)
            bout_id = record.snapshot.receipt_bout_id
            if bout_id is None:
                raise InvalidStateError("The checking attempt identity is unavailable")
            finalizer = record.checking_cancel_tasks.get(bout_id)
            if finalizer is not None and not finalizer.done():
                self._assert_operator(record.operator, operator)
            else:
                if record.snapshot.round.id != RoundId.WAKE_IDLE_APP:
                    raise InvalidStateError("Only a Round 1 start-state check can be cancelled")
                if record.snapshot.state != SessionState.CHECKING:
                    raise InvalidStateError(
                        "Only a fight card that is still checking can be cancelled"
                    )
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
                finalizer = asyncio.create_task(
                    self._finalize_checking_cancel(
                        record,
                        arm_task,
                        expected_lease,
                        message,
                        bout_id,
                    ),
                    name=f"checking-cancel-{record.snapshot.id}-{bout_id}",
                )
                record.checking_cancel_tasks[bout_id] = finalizer

                def clear_unstarted(done: asyncio.Task[SessionSnapshot]) -> None:
                    if record.checking_cancel_tasks.get(bout_id) is done:
                        record.checking_cancel_tasks.pop(bout_id, None)

                finalizer.add_done_callback(clear_unstarted)
        return await asyncio.shield(finalizer)

    async def _finalize_checking_cancel(
        self,
        record: SessionRecord,
        arm_task: asyncio.Task[None] | None,
        expected_lease: BoutLease,
        message: str,
        bout_id: str,
    ) -> SessionSnapshot:
        finalizer = asyncio.current_task()
        try:
            if arm_task is not None:
                try:
                    await arm_task
                except asyncio.CancelledError:
                    pass

            async with record.lock:
                if record.snapshot.state != SessionState.CHECKING:
                    if (
                        record.snapshot.state == SessionState.FAILED
                        and record.snapshot.failure == message
                    ):
                        return self._revalidated_snapshot(record.snapshot)
                    raise InvalidStateError(
                        "The fight card finished checking before cancellation"
                    )
                async with record.lease_lock:
                    current_lease = record.lease
                    if current_lease is not None and not self._same_exact_lease(
                        current_lease, expected_lease
                    ):
                        raise InvalidStateError(
                            "The checking ring lease changed before cancellation"
                        )
            # A cancelled check's own release may already have cleared the lease;
            # otherwise this bounded settle is the finalizer's, never its caller's.
            if (
                current_lease is not None
                and await self._settle_terminal_release(record, current_lease) == "lost"
            ):
                raise InvalidStateError(
                    "The checking ring lease changed before cancellation"
                )
            async with record.lock:
                if record.snapshot.state != SessionState.CHECKING:
                    if (
                        record.snapshot.state == SessionState.FAILED
                        and record.snapshot.failure == message
                    ):
                        return self._revalidated_snapshot(record.snapshot)
                    raise InvalidStateError(
                        "The fight card finished checking before cancellation"
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
                snapshot = self._revalidated_snapshot(record.snapshot)
            await record.event_log.publish(
                "session_cancelled",
                {
                    "state": SessionState.FAILED,
                    "message": message,
                    "session": snapshot.model_dump(mode="json"),
                },
                receipt_bout_id=snapshot.receipt_bout_id,
            )
            return snapshot
        finally:
            if record.checking_cancel_tasks.get(bout_id) is finalizer:
                record.checking_cancel_tasks.pop(bout_id, None)

    async def _cancel_armed_card(
        self,
        record: SessionRecord,
        operator: BoutOperator,
    ) -> SessionSnapshot:
        """Release an armed card that has not rung, for any round but Round 5.

        The armed-window expiry's release, worded as the owner's choice and published as
        a cancellation, as Round 1's pre-arm cancel is: the browser goes back to the card
        without reporting an error, and the record keeps an abandoned attempt that never
        reaches the scoreboard. A second press, or a browser's retry of a press whose
        answer it lost, waits for the release still settling and returns the released
        card.
        """

        async with record.lock:
            self._assert_operator(record.operator, operator)
            if (
                record.snapshot.state == SessionState.FAILED
                and record.snapshot.run_started_at is None
                and record.snapshot.failure == _ARMED_CARD_CANCELLED
            ):
                return self._revalidated_snapshot(record.snapshot)
            if (
                record.snapshot.state != SessionState.ARMED
                or record.snapshot.run_started_at is not None
                or record.armed_at_monotonic is None
            ):
                raise InvalidStateError(
                    "Only an armed fight card that has not rung can be cancelled"
                )
            if record.bell_commit_attempted_bout_id == record.snapshot.receipt_bout_id:
                raise InvalidStateError(
                    "Only an armed fight card that has not rung can be cancelled"
                )
            async with record.lease_lock:
                armed_lease = record.lease
                if (
                    record.bell_committed_at is not None
                    or (
                        armed_lease is not None
                        and (
                            armed_lease.session_id != record.snapshot.id
                            or armed_lease.phase not in {"checking", "armed"}
                        )
                    )
                ):
                    raise InvalidStateError(
                        "Only an armed fight card that has not rung can be cancelled"
                    )
            if record.armed_release_pending:
                release_task = self._owner_release_in_flight_locked(record)
                if release_task is None:
                    raise InvalidStateError(
                        "The fight card changed before it could be cancelled"
                    )
            else:
                armed_at = record.armed_at_monotonic
                # Reserve the card before dropping its lock, so the bell cannot cross
                # between this decision and `_abandon_armed_bout` taking the lock.
                # The release replaces the timer in its slot and is shielded below:
                # a disconnected caller must not leave an ARMED card with no expiry.
                self._cancel_armed_expiry(record, force=True)
                record.armed_release_pending = True
                arm_task = self._current_arm_task_locked(record)
                if arm_task is not None:
                    arm_task.cancel()
                release_task = asyncio.create_task(
                    self._abandon_after_arm_stops(
                        record,
                        arm_task,
                        armed_at,
                        message=_ARMED_CARD_CANCELLED,
                        event="session_cancelled",
                        release_reserved=True,
                    ),
                    name=f"armed-owner-release-{record.snapshot.id}",
                )
                record.armed_expiry_task = release_task
                self._register_owner_release(record, release_task)
        if not await asyncio.shield(release_task):
            raise InvalidStateError("The fight card changed before it could be cancelled")
        async with record.lock:
            return self._revalidated_snapshot(record.snapshot)

    async def _cancel_armed_round5(
        self,
        record: SessionRecord,
        operator: BoutOperator,
    ) -> SessionSnapshot:
        async with record.lock:
            self._assert_operator(record.operator, operator)
            if (
                record.snapshot.state == SessionState.FAILED
                and record.snapshot.run_started_at is None
            ):
                # Already released -- a second click, or the window expired first.
                return self._revalidated_snapshot(record.snapshot)
            if (
                record.snapshot.state != SessionState.ARMED
                or record.snapshot.run_started_at is not None
                or record.armed_at_monotonic is None
            ):
                raise InvalidStateError(
                    "Only an armed Round 5 fight card that has not rung can be cancelled"
                )
            if record.armed_release_pending:
                release_task = self._owner_release_in_flight_locked(record)
                if release_task is None:
                    raise InvalidStateError(
                        "The Round 5 fight card changed before it could be cancelled"
                    )
            else:
                armed_at = record.armed_at_monotonic
                self._cancel_armed_expiry(record, force=True)
                record.armed_release_pending = True
                arm_task = self._current_arm_task_locked(record)
                if arm_task is not None:
                    arm_task.cancel()
                release_task = asyncio.create_task(
                    self._abandon_after_arm_stops(
                        record,
                        arm_task,
                        armed_at,
                        release_reserved=True,
                    ),
                    name=f"round5-armed-owner-release-{record.snapshot.id}",
                )
                record.armed_expiry_task = release_task
                self._register_owner_release(record, release_task)
        if not await asyncio.shield(release_task):
            raise InvalidStateError(
                "The Round 5 fight card changed before it could be cancelled"
            )
        async with record.lock:
            return self._revalidated_snapshot(record.snapshot)

    @staticmethod
    def _register_owner_release(record: SessionRecord, task: asyncio.Task[bool]) -> None:
        """Note ``task`` as the owner's release of this arm; called under ``record.lock``."""

        record.armed_owner_release = (record.snapshot.receipt_bout_id, task)

        def forget(done: asyncio.Task[bool]) -> None:
            if record.armed_owner_release is not None and record.armed_owner_release[1] is done:
                record.armed_owner_release = None

        task.add_done_callback(forget)

    @staticmethod
    def _owner_release_in_flight_locked(record: SessionRecord) -> asyncio.Task[bool] | None:
        """The owner's release of this arm, while it still holds the no-bell reservation.

        Called under ``record.lock``. A reservation held by anything else, the armed
        window's expiry or a bell's convergence, means the card changed under the cancel.
        """

        if record.armed_owner_release is None:
            return None
        bout_id, task = record.armed_owner_release
        if (
            not record.armed_release_pending
            or bout_id != record.snapshot.receipt_bout_id
            or task is not record.armed_expiry_task
            or task.done()
        ):
            return None
        return task

    @staticmethod
    def _current_arm_task_locked(
        record: SessionRecord,
    ) -> asyncio.Task[None] | None:
        task = record.task
        if (
            task is None
            or task.done()
            or task is asyncio.current_task()
            or record.arm_task_bout_id != record.snapshot.receipt_bout_id
            or task.get_name() != f"arm-{record.snapshot.id}"
        ):
            return None
        return task

    @staticmethod
    def _committed_run_active_locked(record: SessionRecord) -> bool:
        """Whether a non-Round-5 callback still belongs to its accepted bell.

        ``bell_committed_at`` is immutable for the arm. An ARMED state with that
        commitment is only a lagging publication and is repaired to RUNNING; a
        terminal state is authoritative and is never revived by a late callback.
        """

        committed_at = record.bell_committed_at
        if committed_at is None:
            return False
        if record.snapshot.state == SessionState.ARMED:
            record.snapshot.state = SessionState.RUNNING
        if record.snapshot.state != SessionState.RUNNING:
            return False
        if record.snapshot.run_started_at is None:
            record.snapshot.run_started_at = committed_at
        return record.snapshot.run_started_at == committed_at

    async def _abandon_after_arm_stops(
        self,
        record: SessionRecord,
        arm_task: asyncio.Task[None] | None,
        armed_at_monotonic: float,
        *,
        message: str = "Fight card expired before the bell.",
        event: str = "session_failed",
        release_reserved: bool,
    ) -> bool:
        if arm_task is not None:
            await asyncio.gather(arm_task, return_exceptions=True)
        return await self._abandon_armed_bout(
            record,
            armed_at_monotonic,
            message=message,
            event=event,
            release_reserved=release_reserved,
        )

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
                durable_cleanup = await self.round5_durable_cleanup_overlay()
                if durable_cleanup is not None:
                    raise InvalidStateError(
                        str(
                            durable_cleanup.get("round5_cleanup_detail")
                            or "ROUND 5 NOT STARTABLE · DURABLE CLEANUP IS STILL ACTIVE"
                        )
                    )
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
            if record.snapshot.state == SessionState.FAILED:
                # A DRAFT is anyone's to arm, but a retained FAILED card still has
                # its owner. Refused before the claim, so neither the ring nor
                # `record.operator` ever passes to someone else.
                self._assert_operator(record.operator, bout_operator)
            if (
                record.snapshot.round.id
                in {RoundId.PUT_MODEL_SCORE_IN_APP, RoundId.ANALYZE_LIVE_ORDERS}
                and record.settlement_task is not None
                and not record.settlement_task.done()
            ):
                raise InvalidStateError(
                    _LOST_RING_STILL_STOPPING
                    if record.ring_lease_lost
                    else _LAST_BOUT_STILL_SETTLING
                )
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
                    # No engine or resident mutation exists yet. Return the
                    # coherent READY claim and both ring rows together; routing
                    # this through CLEANING creates the false cleanup -> TU flap
                    # the operator saw after a refused Prepare.
                    if await self._rollback_unstarted_round5_arm(record):
                        if isinstance(exc, InvalidStateError):
                            raise
                        raise InvalidStateError(
                            "The Round 5 start state could not be verified."
                        ) from exc
                    if (
                        self._round5_warm_coordinator is not None
                        and record.round5_warm_slot is not None
                        and record.round5_warm_slot.claim is not None
                    ):
                        claim_id = record.round5_warm_slot.claim.claim_id
                        try:
                            await self._round5_warm_coordinator.begin_cleanup(claim_id)
                            await self._round5_warm_coordinator.converge_cleanup(
                                claim_id
                            )
                        except Exception:
                            logger.error(
                                "Round 5 failed claim could not return to warming",
                                exc_info=True,
                            )
                    await self._release_round5_lease(record)
                    await self._release_bout(record)
                    if isinstance(exc, InvalidStateError):
                        raise
                    raise InvalidStateError(
                        "ROUND 5 BACKSTAGE CLEANUP IS STILL FINISHING · OTHER ROUNDS ARE READY"
                    ) from exc
            if (
                is_connection_spike
                and self._round5_warm_coordinator is not None
                and record.round5_warm_slot is None
            ):
                authority = record.round5_lease
                if authority is None:
                    await self._release_bout(record)
                    raise InvalidStateError(
                        "Round 5 cleanup authority is unavailable"
                    )
                try:
                    warm_slot, capsule = await self._round5_warm_coordinator.claim(
                        session_id=record.snapshot.id,
                        bout_id=record.snapshot.id,
                        selected_variant=(
                            Round5Variant.AURORA
                            if record.snapshot.competitor.id
                            == CompetitorId.AURORA_SERVERLESS_V2
                            else Round5Variant.RDS
                        ),
                        bout_fence=authority.fencing_token,
                    )
                except (WarmClaimUnavailableError, WarmStoreConflictError) as exc:
                    await self._release_round5_lease(record)
                    await self._release_bout(record)
                    raise InvalidStateError(self._round5_start_refusal()) from exc
                except Exception:
                    await self._release_round5_lease(record)
                    await self._release_bout(record)
                    raise
                record.round5_warm_slot = warm_slot
                record.round5_launch_capsule = capsule
                record.round5_bell_context = None
            record.operator = bout_operator
            self._reset_outcome(record.snapshot)
            self._cancel_idle_watch(record)
            record.snapshot.receipt_bout_id = uuid4().hex
            arm_bout_id = record.snapshot.receipt_bout_id
            record.armed_at_monotonic = None
            record.armed_release_pending = False
            record.bell_committed_at = None
            record.bell_commit_attempted_bout_id = None
            record.bell_commit_sent_at = None
            record.arm_commit_sent_at = None
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
            if is_connection_spike:
                try:
                    await self._verify_round5_start_state(record)
                except Exception as exc:
                    await self._rollback_unstarted_round5_arm(record)
                    if isinstance(exc, InvalidStateError):
                        raise
                    raise InvalidStateError(
                        self._arm_refusal(
                            record,
                            "The Round 5 start state could not be verified.",
                            exc,
                            round_number=5,
                        )
                    ) from exc
            # Rounds 4 and 6 leave their competitor lane to their own Prepare, which
            # knows whether this installation's AWS lane is sealed. A lane marked not
            # supported here reads "not installed" through Prepare, and the browser
            # held it past the bell.
            record.snapshot.state = SessionState.CHECKING
            record.snapshot.updated_at = datetime.now(UTC)
            try:
                if record.snapshot.round.id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
                    record.live_targets = None
                    record.safe_change_engine = None
                    operation = self._arm_safe_change(record, arm_bout_id)
                elif record.snapshot.round.id == RoundId.RECOVER_DELETED_ORDER:
                    record.live_targets = None
                    record.recovery_engine = None
                    operation = self._arm_recovery(record, arm_bout_id)
                elif is_model_score:
                    record.live_targets = None
                    operation = self._arm_model_score(record, arm_bout_id)
                elif is_connection_spike:
                    record.live_targets = None
                    operation = self._arm_connection_spike(record, arm_bout_id)
                elif is_live_orders:
                    record.live_targets = None
                    operation = self._arm_live_orders(record, arm_bout_id)
                else:
                    record.live_targets = self._resolver.resolve(record.snapshot.competitor.id)
                    record.safe_change_engine = None
                    operation = self._arm(record, arm_bout_id)
            except Exception:
                if is_connection_spike:
                    await self._release_round5_lease(record)
                await self._release_bout(record)
                raise
            record.task = asyncio.create_task(operation, name=f"arm-{session_id}")
            record.arm_task_bout_id = arm_bout_id
            result = (
                self._public_snapshot_locked(record)
                if is_connection_spike
                else record.snapshot.model_copy(deep=True)
            )
        return result

    async def start_run(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        self._require_open()
        self._readiness_check()
        record = await self._record(session_id)
        # The handoff itself holds record.lock across its durability-sensitive
        # awaits. Read its immutable registration first so a duplicate request
        # joins the same task instead of waiting for the lock and starting a
        # second (even if ultimately idempotent) handoff after it completes.
        effective_operator = operator or record.operator
        self._assert_operator(record.operator, effective_operator)
        bout_id = record.snapshot.receipt_bout_id
        handoff = record.bell_handoff_task
        if handoff is not None and not handoff.done():
            if record.bell_handoff_bout_id != bout_id:
                raise InvalidStateError("A different bell handoff is still running")
            return await asyncio.shield(handoff)
        async with record.lock:
            effective_operator = operator or record.operator
            self._assert_operator(record.operator, effective_operator)
            bout_id = record.snapshot.receipt_bout_id
            handoff = record.bell_handoff_task
            if handoff is not None and not handoff.done():
                if record.bell_handoff_bout_id != bout_id:
                    raise InvalidStateError("A different bell handoff is still running")
            else:
                handoff = asyncio.create_task(
                    self._complete_bell_handoff(
                        record,
                        bout_id,
                        effective_operator,
                    ),
                    name=f"bell-handoff-{session_id}-{bout_id}",
                )
                record.bell_handoff_task = handoff
                record.bell_handoff_bout_id = bout_id
                handoff.add_done_callback(
                    lambda completed, owned_record=record: self._bell_handoff_done(
                        owned_record,
                        completed,
                    )
                )
        return await asyncio.shield(handoff)

    @staticmethod
    def _bell_handoff_done(
        record: SessionRecord,
        completed: asyncio.Task[SessionSnapshot],
    ) -> None:
        if record.bell_handoff_task is completed:
            record.bell_handoff_task = None
            record.bell_handoff_bout_id = None
        if not completed.cancelled():
            # Retrieve an exception even when the only request waiter was
            # cancelled. A retry still receives it by awaiting the same task.
            completed.exception()

    async def _complete_bell_handoff(
        self,
        record: SessionRecord,
        bout_id: str,
        operator: BoutOperator | None,
    ) -> SessionSnapshot:
        started = time.monotonic()
        commit_attempted = asyncio.Event()
        try:
            handoff = self._perform_bell_handoff(
                record,
                bout_id,
                operator,
                commit_attempted=commit_attempted,
            )
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                # Round 5 owns a separate, durable cleanup protocol. Generic
                # handoff cancellation cannot safely substitute for it midway
                # through atomic bell acceptance.
                result = await handoff
            else:
                result = await asyncio.wait_for(
                    handoff,
                    timeout=max(0.001, self._bell_handoff_timeout),
                )
        except TimeoutError:
            elapsed = time.monotonic() - started
            logger.error(
                "Bell handoff timed out session=%s bout_id=%s outcome=runner_handoff_incomplete "
                "elapsed_s=%.1f",
                record.snapshot.id,
                bout_id,
                elapsed,
            )
            await self._run_bell_handoff_convergence(
                record,
                bout_id,
                bell_acceptance_unknown=commit_attempted.is_set(),
            )
            raise InvalidStateError(
                _BELL_ACCEPTANCE_UNCONFIRMED
                if record.snapshot.receipt_bout_id == bout_id
                and record.snapshot.failure == _BELL_ACCEPTANCE_UNCONFIRMED
                else "The accepted bell could not complete its runner handoff."
            ) from None
        except asyncio.CancelledError:
            elapsed = time.monotonic() - started
            logger.error(
                "Bell handoff cancelled session=%s bout_id=%s "
                "outcome=runner_handoff_incomplete elapsed_s=%.1f",
                record.snapshot.id,
                bout_id,
                elapsed,
            )
            convergence = self._start_bell_handoff_convergence(
                record,
                bout_id,
                bell_acceptance_unknown=commit_attempted.is_set(),
            )
            await abandon_on_cancel(
                lambda: asyncio.shield(convergence),
                identifier=f"bell handoff convergence for session {record.snapshot.id}",
                timeout_seconds=self._terminal_settle_deadline,
                remedy=(
                    "verify the durable ring lease and bout receipt before another bout"
                ),
            )
            raise
        except Exception as exc:
            elapsed = time.monotonic() - started
            # The Round 5 bell already logged its margin refusal, once, as a warning.
            if not isinstance(exc.__cause__, WarmBellMarginError):
                logger.error(
                    "Bell handoff failed session=%s bout_id=%s "
                    "outcome=runner_handoff_incomplete elapsed_s=%.1f diagnosis=%s",
                    record.snapshot.id,
                    bout_id,
                    elapsed,
                    operator_diagnosis(exc),
                    exc_info=True,
                )
            await self._run_bell_handoff_convergence(
                record,
                bout_id,
                bell_acceptance_unknown=commit_attempted.is_set(),
                failure_message=(
                    str(exc) if isinstance(exc, InvalidStateError) else None
                ),
            )
            if (
                not isinstance(exc, InvalidStateError)
                and record.snapshot.receipt_bout_id == bout_id
                and record.snapshot.failure == _BELL_ACCEPTANCE_UNCONFIRMED
            ):
                raise InvalidStateError(_BELL_ACCEPTANCE_UNCONFIRMED) from exc
            raise
        logger.info(
            "Bell handoff completed session=%s bout_id=%s outcome=running elapsed_s=%.1f",
            record.snapshot.id,
            bout_id,
            time.monotonic() - started,
        )
        return result

    def _start_bell_handoff_convergence(
        self,
        record: SessionRecord,
        bout_id: str,
        *,
        bell_acceptance_unknown: bool,
        failure_message: str | None = None,
    ) -> asyncio.Task[None]:
        existing = record.bell_convergence_task
        if existing is not None and not existing.done():
            return existing
        task = asyncio.create_task(
            self._converge_cancelled_bell_handoff(
                record,
                bout_id,
                bell_acceptance_unknown=bell_acceptance_unknown,
                failure_message=failure_message,
            ),
            name=f"bell-convergence-{record.snapshot.id}-{bout_id}",
        )
        record.bell_convergence_task = task

        def completed(done: asyncio.Task[None]) -> None:
            if record.bell_convergence_task is done:
                record.bell_convergence_task = None
            if not done.cancelled():
                done.exception()

        task.add_done_callback(completed)
        return task

    async def _run_bell_handoff_convergence(
        self,
        record: SessionRecord,
        bout_id: str,
        *,
        bell_acceptance_unknown: bool,
        failure_message: str | None = None,
    ) -> None:
        task = self._start_bell_handoff_convergence(
            record,
            bout_id,
            bell_acceptance_unknown=bell_acceptance_unknown,
            failure_message=failure_message,
        )
        await asyncio.shield(task)

    async def _converge_cancelled_bell_handoff(
        self,
        record: SessionRecord,
        bout_id: str,
        *,
        bell_acceptance_unknown: bool = False,
        failure_message: str | None = None,
    ) -> None:
        """Turn an uncertain accepted non-Round-5 bell into an ordinary failed run."""

        if bell_acceptance_unknown:
            async with record.lease_lock:
                local = record.lease
            durable_known = False
            durable: BoutLease | None = None
            if local is not None and local.phase == "armed":
                resolve_deadline = time.monotonic() + max(
                    0.001, self._terminal_release_call_timeout
                )
                while not durable_known:
                    remaining = resolve_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        durable = await self._bounded_lease_call(
                            self._lease_store_for_record(record).current(),
                            max(0.001, remaining),
                        )
                    except Exception:
                        remaining = resolve_deadline - time.monotonic()
                        if remaining > 0:
                            await asyncio.sleep(min(0.05, remaining))
                    else:
                        durable_known = True
                if (
                    durable is not None
                    and durable.phase == "run_committed"
                    and self._same_lease_authority(durable, local)
                ):
                    adopted = False
                    async with record.lease_lock:
                        current_local = record.lease
                        if (
                            current_local is not None
                            and self._same_exact_lease(current_local, local)
                        ):
                            self._adopt_lease(record, durable, record.bell_commit_sent_at)
                            adopted = True
                    if adopted:
                        self._start_lease_heartbeat(
                            record,
                            timedelta(seconds=self._running_lease_ttl),
                        )
                elif durable_known and (
                    durable is None
                    or (
                        durable.phase == "armed"
                        and self._same_lease_authority(durable, local)
                    )
                ):
                    if durable is None:
                        async with record.lease_lock:
                            current_local = record.lease
                            if (
                                current_local is not None
                                and self._same_exact_lease(current_local, local)
                            ):
                                self._lose_ring_lease(record)
                    armed_at = record.armed_at_monotonic
                    if armed_at is not None:
                        await self._abandon_armed_bout(
                            record,
                            armed_at,
                            message=(
                                "The bell handoff timed out before durable acceptance; "
                                "the ring was released."
                            ),
                        )
                    return
                elif durable_known:
                    await self._handle_lost_lease(record, local)
                    return
            if (
                not durable_known
                and local is not None
                and local.phase == "armed"
                and record.snapshot.round.id != RoundId.SURVIVE_CONNECTION_SPIKE
            ):
                message = _BELL_ACCEPTANCE_UNCONFIRMED
                async with record.lock:
                    if (
                        record.snapshot.receipt_bout_id != bout_id
                        or record.snapshot.state != SessionState.ARMED
                    ):
                        return
                    self._cancel_armed_expiry(record)
                    record.snapshot.state = SessionState.FAILED
                    record.snapshot.failure = message
                    record.snapshot.remembered_result = None
                    record.snapshot.updated_at = datetime.now(UTC)
                    record.armed_at_monotonic = None
                    snapshot = record.snapshot.model_copy(deep=True)
                    self._forget_fence_at_expiry(record, local, "Unconfirmed bell's armed fence")
                await record.event_log.publish(
                    "session_failed",
                    {
                        "state": SessionState.FAILED,
                        "message": message,
                        "session": snapshot.model_dump(mode="json"),
                    },
                    receipt_bout_id=snapshot.receipt_bout_id,
                )
                return

        should_fail = False
        async with record.lock:
            lease = record.lease
            if (
                record.snapshot.receipt_bout_id == bout_id
                and record.snapshot.round.id != RoundId.SURVIVE_CONNECTION_SPIKE
                and record.snapshot.state == SessionState.ARMED
                and lease is not None
                and lease.session_id == record.snapshot.id
                and lease.phase == "run_committed"
            ):
                committed_at = record.bell_committed_at or datetime.now(UTC)
                record.bell_committed_at = committed_at
                record.snapshot.state = SessionState.RUNNING
                record.snapshot.run_started_at = committed_at
                record.snapshot.updated_at = committed_at
                should_fail = True
        if should_fail:
            await self._fail(
                record,
                failure_message
                or "The accepted bell could not complete its runner handoff.",
            )

    async def _perform_bell_handoff(
        self,
        record: SessionRecord,
        bout_id: str,
        operator: BoutOperator | None,
        *,
        commit_attempted: asyncio.Event,
    ) -> SessionSnapshot:
        session_id = record.snapshot.id
        async with record.lock:
            if record.snapshot.receipt_bout_id != bout_id:
                raise InvalidStateError("The fight card changed before the bell could ring")
            effective_operator = operator or record.operator
            self._assert_operator(record.operator, effective_operator)
            if (
                record.task is not None
                and not record.task.done()
                and record.task.get_name() == f"run-{session_id}"
            ):
                self._assert_operator(record.operator, effective_operator)
                return (
                    self._public_snapshot_locked(record)
                    if record.snapshot.round.id
                    == RoundId.SURVIVE_CONNECTION_SPIKE
                    else record.snapshot.model_copy(deep=True)
                )
            if record.snapshot.run_started_at is not None and record.snapshot.state in {
                SessionState.RUNNING,
                SessionState.VERIFIED,
                SessionState.FAILED,
                SessionState.TOWELLED,
            }:
                self._assert_operator(record.operator, effective_operator)
                return (
                    self._public_snapshot_locked(record)
                    if record.snapshot.round.id
                    == RoundId.SURVIVE_CONNECTION_SPIKE
                    else record.snapshot.model_copy(deep=True)
                )
            if record.task and not record.task.done():
                raise InvalidStateError("A session operation is already running")
            if record.snapshot.state != SessionState.ARMED:
                raise InvalidStateError("The session must be armed before the bell can ring")
            if record.armed_release_pending:
                raise InvalidStateError("The fight card is being released and cannot ring")
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
                raise InvalidStateError("The Round 5 pooled-path proof must be armed again")
            if is_live_orders and (
                record.live_orders_engine is None or record.live_orders_arm is None
            ):
                raise InvalidStateError("Round 6 must be prepared again")
            if (
                not is_safe_change
                and not is_recovery
                and not is_model_score
                and not is_connection_spike
                and not is_live_orders
                and record.live_targets is None
            ):
                raise InvalidStateError("The live targets are unavailable")
            round5_atomic_bell = bool(
                is_connection_spike
                and self._round5_warm_coordinator is not None
                and callable(
                    getattr(
                        self._round5_warm_coordinator.store,
                        "accept_bell_with_leases",
                        None,
                    )
                )
            )
            if not round5_atomic_bell:
                record.bell_commit_attempted_bout_id = bout_id
                commit_attempted.set()
                await self._commit_bout(record, operator or record.operator)
            cost_started_at = datetime.now(UTC)
            cost_bout_id: str | None = None
            if not is_connection_spike:
                try:
                    cost_bout_id = await self._open_cost_bout(
                        record,
                        kind="run",
                        started_at=cost_started_at,
                    )
                except Exception as exc:
                    logger.error(
                        "Bout cost window open failed session=%s round=%s diagnosis=%s",
                        record.snapshot.id,
                        record.snapshot.round.id.value,
                        operator_diagnosis(exc),
                        exc_info=True,
                    )
                    raise InvalidStateError(
                        cost_window_refusal(
                            "The bout cost window could not be opened before the bell.",
                            exc,
                        )
                    ) from exc
            if is_connection_spike and self._round5_warm_coordinator is not None:
                warm_slot = record.round5_warm_slot
                claim = warm_slot.claim if warm_slot is not None else None
                if claim is None:
                    await self._release_round5_lease(record)
                    await self._release_bout(record)
                    raise InvalidStateError(
                        "Round 5 warm claim is unavailable"
                    )
                # Durably commit the timed CreateDBProxy CREATE_INTENT BEFORE the
                # authoritative bell transaction captures T0 and before Lakebase is
                # released, so the competitor's create record always exists first.
                # This is the only coordination write ordered ahead of the bell for
                # the timed lane; after T0 the competitor path awaits no journal I/O
                # before boto3 CreateDBProxy.
                #
                # ``precommit_launch_intent`` is a REQUIRED bell-seam method on the
                # Round 5 engine, invoked directly -- not an optional getattr hook
                # whose absence would silently no-op the durability guarantee.  It
                # self-no-ops only when the engine has no timed setup.
                #
                # The precommit AND bell acceptance share one compensation scope: a
                # failure of either (refused precommit, or a bell CAS that loses the
                # claim/broker epoch) rings nothing and hands the claim to the
                # coordinator's durable CLEANING below.  This is safe because
                # Lakebase is only actually released later, by ``bind_bell`` ->
                # ``allow_release``, which runs OUTSIDE this block after the durable
                # commit: on every path handled here, no lane has launched.  A bell
                # refused after its precommit leaves a CREATE_INTENT under a NULL
                # bell, which the no-bell cleanup reconstructs from the journal.
                # Idempotent for a duplicate /run.
                #
                # The dispatch margin is checked before the precommit, so the usual
                # refusal leaves no CREATE_INTENT, and again once the bell commits,
                # because the commit can take longer than the margin the bell had left.
                precommit_launch = record.connection_spike_engine.precommit_launch_intent
                launch_lease = record.round5_lease
                bell_stage = "before_precommit"
                commit_sent = 0.0
                try:
                    self._round5_warm_coordinator.require_dispatchable(claim)
                    if launch_lease is not None:
                        await precommit_launch(
                            record.snapshot.id, launch_lease.fencing_token
                        )
                    bell_stage = "bell"
                    commit_sent = time.monotonic()
                    if round5_atomic_bell:
                        main_lease = record.lease
                        cleanup_lease = record.round5_lease
                        if main_lease is None or cleanup_lease is None:
                            raise InvalidStateError(
                                "Round 5 bell leases are unavailable"
                            )
                        sent = time.monotonic()
                        (
                            context,
                            committed_main,
                            committed_cleanup,
                        ) = await self._round5_warm_coordinator.accept_bell_with_leases(
                            claim.claim_id,
                            main_store=self._lease_store_for_record(record),
                            main_lease=main_lease,
                            cleanup_store=self._round5_cleanup_store(),
                            cleanup_lease=cleanup_lease,
                            ttl=timedelta(seconds=self._running_lease_ttl),
                            release_event_factory=getattr(
                                record.connection_spike_engine,
                                "lakebase_release_event",
                                None,
                            ),
                        )
                        bell_stage = "after_commit"
                        self._set_lease(record, committed_main, sent)
                        self._set_lease(record, committed_cleanup, sent, round5=True)
                        if self._bell_proof_lapsed(record):
                            # The committed-then-failed path below: nothing launched.
                            raise InvalidStateError(
                                "FIGHT CARD EXPIRED · PREPARE IT AGAIN BEFORE RINGING THE BELL"
                            )
                        self._round5_warm_coordinator.require_dispatchable(claim)
                        self._start_lease_heartbeat(
                            record,
                            timedelta(seconds=self._running_lease_ttl),
                        )
                        self._start_round5_lease_heartbeat(
                            record,
                            timedelta(seconds=self._running_lease_ttl),
                        )
                    else:
                        context = await self._round5_warm_coordinator.accept_bell(
                            claim.claim_id
                        )
                        bell_stage = "after_commit"
                        self._round5_warm_coordinator.require_dispatchable(claim)
                except Exception as exc:
                    if isinstance(exc, WarmBellMarginError):
                        logger.warning(
                            "round5_bell_refused_dispatch_margin session=%s bout_id=%s "
                            "stage=%s remaining_s=%.1f armed_wait_s=%.1f commit_s=%s",
                            record.snapshot.id,
                            bout_id,
                            bell_stage,
                            exc.remaining_seconds,
                            exc.armed_wait_seconds,
                            # How long the bell's own commit took to answer, which is
                            # what an after_commit refusal lost its margin to.
                            (
                                f"{time.monotonic() - commit_sent:.1f}"
                                if bell_stage == "after_commit"
                                else "none"
                            ),
                        )
                    else:
                        logger.error(
                            "Round 5 bell failed before RUNNING session=%s diagnosis=%s",
                            record.snapshot.id,
                            operator_diagnosis(exc),
                            exc_info=True,
                        )
                    # A refused bell ALWAYS enters durable CLEANING and converges
                    # through the coordinator's single owner (provider reconcile on
                    # the adopted engine). The manager performs no external cleanup
                    # mutation of its own.
                    settlement_deferred = True
                    if self._round5_warm_coordinator is not None:
                        await self._begin_round5_warm_cleanup(record)
                    else:
                        settle_abandoned = getattr(
                            record.connection_spike_engine, "settle_abandoned_arm", None
                        )
                        settlement_deferred = settle_abandoned is None
                        if settle_abandoned is not None:
                            try:
                                settlement_deferred = bool(await settle_abandoned())
                            except Exception:
                                settlement_deferred = True
                                logger.error(
                                    "Round 5 refused-bell resident settle failed "
                                    "session=%s; restart reconciliation will settle it",
                                    record.snapshot.id,
                                    exc_info=True,
                                )
                    if settlement_deferred:
                        self._cancel_armed_expiry(record)
                        record.snapshot.state = SessionState.FAILED
                        record.snapshot.failure = _ROUND_FIVE_CLEANUP_PENDING
                        record.snapshot.remembered_result = None
                        record.snapshot.updated_at = datetime.now(UTC)
                        record.armed_at_monotonic = None
                        for lane in record.snapshot.lanes.values():
                            lane.state = LaneState.FAILED
                            lane.status = _ROUND_FIVE_CLEANUP_PENDING
                            lane.error = _ROUND_FIVE_CLEANUP_PENDING
                            lane.activity = LaneActivity(phase="cleanup_failed")
                        if effective_operator is not None:
                            try:
                                await self._retain_connection_spike_cleanup_lease(
                                    record,
                                    effective_operator,
                                    session_state=SessionState.FAILED,
                                    allow_claim=False,
                                )
                            except Exception:
                                logger.error(
                                    "Round 5 refused-bell cleanup fence could not "
                                    "be transitioned session=%s",
                                    record.snapshot.id,
                                    exc_info=True,
                                )
                        owed = self._note_round5_proxy_at_risk(
                            record,
                            still_retrying=True,
                            force=True,
                        )
                        setup = record.snapshot.round5_setup
                        if setup is not None:
                            setup.state = RoundFiveSetupState.CLEANUP_FAILED
                            setup.cleanup_retryable = True
                            setup.cleanup_failure = (
                                owed.detail
                                if owed is not None
                                else "Automatic backstage cleanup is retrying."
                            )
                            setup.failure = setup.cleanup_failure
                        self._schedule_connection_spike_cleanup_convergence(record)
                    else:
                        await self._release_round5_lease(record)
                        await self._release_bout(record)
                    if isinstance(exc, InvalidStateError):
                        raise
                    if isinstance(exc, WarmBellMarginError):
                        raise InvalidStateError(str(exc)) from exc
                    raise InvalidStateError(
                        "Round 5 bell could not start; nothing was launched."
                    ) from exc
                # The accept_bell transaction has COMMITTED, so record the
                # durable post-bell truth (bell context + authoritative RUNNING slot)
                # BEFORE bind_bell. If bind_bell -- or any later step -- raises, a
                # subsequent cleanup still classifies as post-bell and runs the
                # mandatory post-setup teardown instead of misreading a stale CLAIMED
                # slot as pre-bell. (Cleanup also has a durable-store fallback, see
                # _refresh_round5_slot_from_durable.)
                record.round5_bell_context = context
                running_slot = getattr(
                    self._round5_warm_coordinator, "last_slot", None
                )
                if (
                    running_slot is not None
                    and getattr(running_slot, "claim", None) is not None
                    and running_slot.claim.claim_id == claim.claim_id
                    and getattr(running_slot, "bell_id", None) is not None
                ):
                    record.round5_warm_slot = running_slot
                bind_bell = getattr(
                    record.connection_spike_engine,
                    "bind_bell",
                    None,
                )
                if callable(bind_bell):
                    bind_bell(context)
                record.snapshot.state = SessionState.RUNNING
                record.snapshot.run_started_at = context.bell_at_utc
                record.run_started_monotonic_ns = context.t0_monotonic_ns
                record.snapshot.updated_at = context.bell_at_utc
                record.snapshot.metrics = []
                record.snapshot.comparison = None
                record.snapshot.round5_setup = self._new_round_five_setup(
                    record.snapshot
                )
                record.snapshot.round5_setup.state = RoundFiveSetupState.RUNNING
                record.snapshot.round5_runtime = self._new_round_five_runtime(
                    context
                )
                record.round5_projection_revision = 1
                record.round5_clock_projection_revision = 0
                record.round5_ingestion_closed = False
                record.round5_callback_epoch += 1
                record.round5_terminal_published = False
                for lane_id, lane in record.snapshot.lanes.items():
                    lane.state = LaneState.CONNECTING
                    lane.status = record.snapshot.round5_runtime.lanes[
                        lane_id
                    ].status
                    lane.attempts = 0
                    lane.successes = 0
                    lane.errors = 0
                    lane.p99_ms = None
                    lane.error = None
                    lane.activity = LaneActivity(
                        phase=record.snapshot.round5_runtime.lanes[lane_id].phase
                    )
            elif not is_connection_spike:
                # The durable lease has crossed ARMED -> RUN_COMMITTED and the
                # cost window accepted. Publish that accepted bell synchronously,
                # before the runner gets its first scheduling turn. The physical
                # lane clock remains ``run_started_monotonic_ns`` and is captured
                # by each runner at its existing exact boundary. Rounds 4 and 6
                # replace this provisional wall-clock timestamp when their
                # physical engine bell rings, but they must not return ARMED
                # after durable acceptance.
                committed_at = datetime.now(UTC)
                record.bell_committed_at = committed_at
                record.snapshot.state = SessionState.RUNNING
                record.snapshot.run_started_at = committed_at
                record.snapshot.updated_at = committed_at
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
                # One checkout for every source and a separate guardrail, each with a bout
                # nonce, so residue a crash leaves is recognizably this demo's.
                order, guardrail = new_bout_orders(record.live_orders_engine.baseline)
                record.live_orders_pending_order = order
                record.live_orders_guardrail_order = guardrail
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
            if is_connection_spike:
                lane_operation = asyncio.create_task(
                    operation,
                    name=f"round5-lanes-{session_id}",
                )
                context = record.round5_bell_context
                round5_cost_started_at = (
                    context.bell_at_utc if context is not None else cost_started_at
                )
                round5_bell_id = (
                    context.bell_id if context is not None else f"legacy-{session_id}"
                )
                record.task = asyncio.create_task(
                    self._run_round5_cost_bout_after_bell(
                        record,
                        started_at=round5_cost_started_at,
                        bell_id=round5_bell_id,
                        operation=lane_operation,
                    ),
                    name=f"run-{session_id}",
                )
            else:
                record.task = asyncio.create_task(
                    self._run_cost_bout(record, cost_bout_id, operation),
                    name=f"run-{session_id}",
                )
            result = (
                self._public_snapshot_locked(record)
                if is_connection_spike
                else record.snapshot.model_copy(deep=True)
            )
        return result

    async def start_redo(
        self,
        session_id: str,
        operator: BoutOperator | None = None,
    ) -> SessionSnapshot:
        """Refuse: v1.1 has no re-do in any round that offered one here.

        Round 4 dropped its re-do (Ryan, 2026-09-28). After a bout both of its
        integrations are running, so a re-do could not start cold, and the
        both-cold rule allows no other kind. Racing again is a new bout, which
        meets both lanes parked. The endpoint stays so an older client gets a
        reason instead of a 404.
        """

        self._require_open()
        record = await self._record(session_id)
        async with record.lock:
            effective_operator = operator or record.operator
            if effective_operator is None:
                raise InvalidStateError("ONLY THE RING OWNER CAN CONTROL THIS BOUT")
            self._assert_operator(record.operator, effective_operator)
        raise InvalidStateError(
            "Round 4 has no re-do: both lanes start cold at the bell, and after a bout they "
            "are running. Prepare a new bout to race again."
        )

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
            setup.cleanup_retryable = True
            setup.cleanup_failure = None
            self._note_round5_proxy_at_risk(record, force=True)
            self._schedule_connection_spike_cleanup_convergence(record)
            if record.snapshot.towel is not None:
                record.snapshot.towel.state = TowelState.CLEANING
                record.snapshot.towel.cleanup_failure = None
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = self._revalidated_snapshot(record.snapshot)
            await record.event_log.publish(
                "cleanup_update",
                {"session": snapshot.model_dump(mode="json")},
                receipt_bout_id=snapshot.receipt_bout_id,
            )
            # Only WAKE the durable auto-convergence worker; never start a second
            # concurrent reconcile racing it. The worker keeps retrying the
            # journal-based reconcile + rewarm CAS (with backoff) until the exact
            # per-bout Proxy is confirmed absent and the ring rewarms, so a manual
            # Retry Cleanup is now just an operator nudge, idempotent by design.
            self._schedule_connection_spike_cleanup_convergence(record)
            return snapshot

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
            is_round_five_v3 = (
                record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
                and record.snapshot.round5_runtime is not None
            )
            if is_round_five_v3 and (
                towel is not None
                or record.snapshot.state
                in {
                    SessionState.VERIFIED,
                    SessionState.FAILED,
                    SessionState.TOWELLED,
                }
            ):
                # Terminal V3 state is absorbing. Cleanup has its own retry
                # endpoint; a late or repeated towel cannot rewrite the verdict
                # or consume another ownership fence.
                return self._public_snapshot_locked(record)
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
                        receipt_bout_id=snapshot.receipt_bout_id,
                    )
                    record.cooldown_task = asyncio.create_task(
                        self._complete_towel(record, wait_for_run=False),
                        name=f"towel-cleanup-{session_id}",
                    )
                    return snapshot
                return record.snapshot.model_copy(deep=True)

            if record.snapshot.state != SessionState.RUNNING:
                # A runner failure and an operator click can cross in flight:
                # the browser still has the last RUNNING revision while this
                # process has already moved Round 5 to FAILED and begun the
                # exact same stop-and-cleanup handoff. Emergency stop is
                # idempotent at that edge. Returning the newer terminal
                # snapshot both acknowledges the click and lets the UI replace
                # its stale button; every other round/state combination keeps
                # the strict refusal below.
                if (
                    record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
                    and record.snapshot.state == SessionState.FAILED
                ):
                    if record.towel_stop_event is not None:
                        record.towel_stop_event.set()
                    return record.snapshot.model_copy(deep=True)
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
                cutoff_ns = self._clock_ns()
                if started_ns is not None:
                    cutoff_ms = max(0.0, (cutoff_ns - started_ns) / 1_000_000)
                elif record.snapshot.round.id in {
                    RoundId.PUT_MODEL_SCORE_IN_APP,
                    RoundId.ANALYZE_LIVE_ORDERS,
                }:
                    # Round 4 or 6 before its bell: the engine was still reading each
                    # source's baseline, so every lane is sealed, nothing was timed and
                    # nothing is censored. The towel stops the bout all the same.
                    cutoff_ms = 0.0
                elif record.bell_committed_at is not None:
                    # Rounds 1-3 publish RUNNING as soon as the durable bell is
                    # accepted. A towel can therefore beat the runner to its
                    # physical measurement boundary; that is a zero-duration
                    # committed run, not an unstarted arm and not a refusal.
                    cutoff_ms = 0.0
                else:
                    raise InvalidStateError("The live bout clock has not started")

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
                record.round5_ingestion_closed = True
                record.round5_callback_epoch += 1
                runtime = record.snapshot.round5_runtime
                if runtime is not None:
                    runtime.state = "towelled"
                    for runtime_lane in runtime.lanes.values():
                        if runtime_lane.phase != "verified":
                            runtime_lane.phase = "cancelled"
                    self._advance_round5_revision_locked(record)
                if runtime is not None and runtime.protocol == "round5-bell-to-10k-v4":
                    # V4 durable truth comes from the bell runtime, never the
                    # ~0.01s pooled-path setup stop. This keeps the setup clock
                    # out of the primary `snapshot.lanes` verification so
                    # `derive_receipt`, summary, and finale all read the exact
                    # bell_to_10000 observation and its reached/verified
                    # distinction instead of a promoted setup stop.
                    # Both clocks run from the bell's T0, and the towel freezes each
                    # at the elapsed the room watched (the live projection's
                    # arithmetic), not at its last published milestone.
                    bell_ns = record.run_started_monotonic_ns
                    adjudication = adjudicate_round_five_bell_towel(
                        lanes=record.snapshot.lanes,
                        runtime_lanes=runtime.lanes,
                        cutoff_ms=(
                            max(0.0, (round_five_cutoff_ns - bell_ns) / 1_000_000)
                            if bell_ns is not None
                            else None
                        ),
                    )
                else:
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
            # `lakebase_verified_ms` is a transitional Round 3 compatibility field.
            # It must never carry a V4 value: the V4 receipt reads the canonical
            # bell runtime, and populating this legacy field from a towel-promoted
            # setup stop is exactly what put "0.01s · EXACT VERIFIED" on the share
            # card. Leave it unset for the V4 bell protocol.
            is_round_five_bell = (
                is_round_five
                and record.snapshot.round5_runtime is not None
                and record.snapshot.round5_runtime.protocol == "round5-bell-to-10k-v4"
            )
            lakebase_verified_ms = (
                None
                if is_round_five_bell
                else adjudication.lanes["lakebase"].elapsed_ms
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
            record.snapshot.comparison = (
                None
                if is_round_five and record.snapshot.round5_runtime is not None
                else adjudication.comparison
            )
            record.snapshot.state = SessionState.TOWELLED
            record.snapshot.failure = None
            record.snapshot.remembered_result = adjudication.public_result
            if is_round_five and record.snapshot.round5_runtime is not None:
                record.snapshot.metrics = []
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
                    await self._close_towel_cost_window(
                        record, requested_towel, bout_id=cost_bout_id
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
            if is_round_five:
                record.round5_terminal_published = True
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
                cooldown = record.snapshot.cooldown = self._new_cooldown(record, reset_mode)
                started_at = cooldown.started_at
                record.snapshot.updated_at = started_at
                reset_operation = (
                    self._reset_safe_change(record)
                    if is_safe_change
                    else self._reset_recovery(record)
                    if is_recovery
                    else self._monitor_cooldown(record, CooldownWatch(cooldown, record.lease))
                )
                record.cooldown_task = asyncio.create_task(
                    self._hold_bout_during_reset(record, reset_operation, record.lease),
                    name=f"cooldown-{record.snapshot.id}",
                )
            return record.snapshot.model_copy(deep=True)

    @staticmethod
    def _new_cooldown(
        record: SessionRecord,
        reset_mode: ResetMode,
    ) -> CooldownSnapshot:
        started_at = datetime.now(UTC)
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
        warm_status = (
            self.round5_warm_status
            if round_id == RoundId.SURVIVE_CONNECTION_SPIKE
            and self._round5_warm_coordinator is not None
            else None
        )
        round5_start = self._round5_structured_start(warm_status)
        if (
            self._round_isolation
            and round_id == RoundId.SURVIVE_CONNECTION_SPIKE
            and self._round5_readiness_status is not None
        ):
            # Round 5 bout_status answers from the round5-specific readiness/warm
            # signal ALONE -- never gated on the installation-wide ``readiness``.
            # Requiring the global ring to be ready first coupled Round 5
            # availability to unrelated installation/other-round startup state, so a
            # global blip could report Round 5 unavailable even though its own
            # per-round ring/warm coordinator was healthy.
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
        if (
            self._round_isolation
            and round_id == RoundId.SURVIVE_CONNECTION_SPIKE
            and self._round5_warm_coordinator is not None
            and not self.round5_ring_ready
        ):
            warm_status = warm_status or {}
            readiness_fields = {
                "ring_ready": False,
                "maintenance_state": (
                    "blocked"
                    if warm_status.get("round5_warm_blocked_terminal")
                    else "maintenance"
                ),
                "maintenance_detail": self._round5_start_refusal(),
            }
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
            if lease is None:
                # The durable cleanup ring was just read and is empty, which is
                # the same fact that lets ``round5_durable_cleanup_overlay`` lift
                # its latch. Lift it here too: otherwise only /api/catalog or a
                # restart ever cleared it, and a fight card polled through
                # /api/bout/all alone could keep reporting Round 5 closed behind
                # a cleanup that had already finished.
                self._round5_durable_cleanup_blocked = False
        if (
            scoped_round == RoundId.SURVIVE_CONNECTION_SPIKE
            and lease is not None
            and lease.phase == "round5_cleanup"
        ):
            overlay = await self.round5_durable_cleanup_overlay(lease)
            if overlay is not None:
                readiness_fields = {
                    "ring_ready": False,
                    "maintenance_state": "maintenance",
                    "maintenance_detail": overlay["round5_cleanup_detail"],
                }
                round5_start = self._round5_structured_start(overlay)
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
                round5_start=round5_start,
            )
        can_start = bool(readiness_fields.get("ring_ready", True))
        if can_start and self._lost_ring_still_stopping(store.ring_key):
            # The lost row is gone, but a claim of this ring is refused here until
            # the work that lost it has stopped.
            readiness_fields = {
                **readiness_fields,
                "maintenance_state": "maintenance",
                "maintenance_detail": _LOST_RING_STILL_STOPPING,
            }
            can_start = False
        return BoutStatus(
            active=False,
            can_start=can_start,
            **status_scope,
            **readiness_fields,
            round5_start=round5_start,
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
    def _unfinished_operations(record: SessionRecord) -> tuple[asyncio.Task[Any], ...]:
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
                record.bell_handoff_task,
                record.bell_convergence_task,
                *record.checking_cancel_tasks.values(),
                record.cooldown_task,
                record.connection_spike_cleanup_task,
                record.settlement_task,
                record.model_score_terminal_task,
                record.armed_expiry_task,
                record.lease_heartbeat_task,
                record.round5_lease_heartbeat_task,
                record.terminal_release_retry_task,
                record.round5_release_retry_task,
                record.fence_expiry_task,
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
        setup = record.snapshot.round5_setup
        if (
            record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            and setup is not None
            and (
                setup.cleanup_retryable
                or setup.cleanup_failure is not None
                or (
                    record.snapshot.towel is not None
                    and record.snapshot.towel.state
                    in {TowelState.CLEANING, TowelState.FAILED}
                )
            )
        ):
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
        stopping = self._unstopped_ring_work(record, claimed=False)
        if stopping:
            raise self._unstopped_ring_work_refusal(record, stopping)
        ttl = timedelta(
            seconds=(
                self._running_lease_ttl if phase == "redo_committed" else self._active_lease_ttl
            )
        )
        if (
            record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            and self._round5_warm_coordinator is not None
            and callable(
                getattr(
                    self._round5_warm_coordinator.store,
                    "claim_ready_with_leases",
                    None,
                )
            )
            and phase == "checking"
        ):
            sent = time.monotonic()
            try:
                (
                    warm_slot,
                    capsule,
                    main_lease,
                    cleanup_lease,
                ) = await self._round5_warm_coordinator.claim_with_leases(
                    session_id=record.snapshot.id,
                    bout_id=record.snapshot.id,
                    selected_variant=(
                        Round5Variant.AURORA
                        if record.snapshot.competitor.id
                        == CompetitorId.AURORA_SERVERLESS_V2
                        else Round5Variant.RDS
                    ),
                    main_store=self._lease_store_for_record(record),
                    cleanup_store=self._round5_cleanup_store(),
                    operator=operator,
                    round_id=record.snapshot.round.id.value,
                    round_title=record.snapshot.round.title,
                    competitor_id=record.snapshot.competitor.id.value,
                    competitor_name=record.snapshot.competitor.short_name,
                )
            except Exception as exc:
                logger.warning(
                    "Round 5 atomic claim refused diagnosis=%s",
                    operator_diagnosis(exc),
                )
                if isinstance(
                    exc,
                    (
                        LeaseHeldError,
                        WarmClaimUnavailableError,
                        WarmStoreConflictError,
                    ),
                ):
                    raise InvalidStateError(self._round5_start_refusal()) from exc
                raise
            self._set_lease(record, main_lease, sent)
            self._set_lease(record, cleanup_lease, sent, round5=True)
            record.round5_warm_slot = warm_slot
            record.round5_launch_capsule = capsule
            # Checked again now both rows are won: the claim may have waited out a loss.
            stopping = self._unstopped_ring_work(record, claimed=True)
            if stopping:
                await self._give_back_round5_claim(record)
                raise self._unstopped_ring_work_refusal(record, stopping)
            record.ring_lease_lost = False
            self._start_lease_heartbeat(record, ttl)
            self._start_round5_lease_heartbeat(record, ttl)
            return

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

        sent = time.monotonic()
        try:
            lease = await claim(self._lease_store_for_record(record))
        except LeaseHeldError as exc:
            detail = self._describe_ring_holder(exc)
            logger.warning("Ring claim refused: %s", detail)
            raise InvalidStateError(detail) from exc
        except LeaseLostError as exc:
            raise InvalidStateError(str(exc)) from exc
        self._set_lease(record, lease, sent)
        cleanup_lease: BoutLease | None = None
        if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
            sent = time.monotonic()
            try:
                cleanup_lease = await claim(self._round5_cleanup_store())
            except LeaseHeldError as exc:
                await self._release_bout(record)
                raise InvalidStateError(self._round5_start_refusal()) from exc
            except LeaseLostError as exc:
                await self._release_bout(record)
                raise InvalidStateError(str(exc)) from exc
            self._set_lease(record, cleanup_lease, sent, round5=True)
        # Checked again now the rows are won: a claim may have waited out a loss.
        stopping = self._unstopped_ring_work(record, claimed=True)
        if stopping:
            await self._give_back_claim(record, lease)
            if cleanup_lease is not None:
                await self._give_back_claim(record, cleanup_lease, round5=True)
            raise self._unstopped_ring_work_refusal(record, stopping)
        record.ring_lease_lost = False
        if cleanup_lease is not None:
            self._start_round5_lease_heartbeat(record, ttl)
        if record.snapshot.round.id in {
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            RoundId.RECOVER_DELETED_ORDER,
        }:
            record.round2_fencing_token = lease.fencing_token
        self._start_lease_heartbeat(record, ttl)

    @staticmethod
    def _ring_work(record: SessionRecord) -> list[asyncio.Task[Any]]:
        """``record``'s unfinished tasks that can act on what its ring lease fences.

        A loss cancels some of them and lets others, such as Round 5's backstage
        cleanup, run to the end; either way the ring stays closed here until they
        finish. Any task that touches the round's sources or artifacts belongs here.
        Round 1's idle watch does not: it only reads both control planes, and the only
        lease it acts on is the one it began under, by exact fence, which a lost ring
        no longer holds. It is known by the record's own reference to it, so every
        other task in its slot, a towel or artifact cleanup, still counts.
        """

        return [
            task
            for task in (
                record.task,
                record.cooldown_task,
                record.bell_handoff_task,
                record.bell_convergence_task,
                record.model_score_terminal_task,
                record.settlement_task,
                record.connection_spike_cleanup_task,
                record.connection_spike_cleanup_retry_task,
            )
            if task is not None and not task.done() and task is not record.idle_watch_task
        ]

    def _unstopped_ring_work(
        self,
        record: SessionRecord,
        *,
        claimed: bool,
    ) -> list[asyncio.Task[Any]]:
        """The work on ``record``'s ring that still runs here without its authority.

        Always the ring work of every record that lost the ring, the claimer's own
        included: the heartbeat gives a lease up before it expires, but the row then
        expires on its own clock whether or not that work has stopped. Once a claim
        has won the row, also any other local holder of the ring that still has work
        running; it lost the ring too and has not noticed yet. Across replicas the
        early loss is the only fence; here the ring waits for the work. There is no
        timed escape: a claim made beside work that may still write would be the
        second owner this exists to prevent, and a restart's shutdown stops the work
        instead.
        """

        ring_key = self._lease_store_for_record(record).ring_key
        current = asyncio.current_task()
        unstopped: list[asyncio.Task[Any]] = []
        for other in tuple(self._records.values()):
            if self._lease_store_for_record(other).ring_key != ring_key:
                continue
            if other.ring_lease_lost or (
                claimed and other is not record and other.lease is not None
            ):
                unstopped.extend(task for task in self._ring_work(other) if task is not current)
        return unstopped

    def _lost_ring_still_stopping(self, ring_key: str) -> bool:
        """Whether work that lost ``ring_key`` still runs here, so a claim of it is refused."""

        return any(
            other.ring_lease_lost
            and self._lease_store_for_record(other).ring_key == ring_key
            and self._ring_work(other)
            for other in tuple(self._records.values())
        )

    @staticmethod
    def _lose_ring_lease(record: SessionRecord) -> None:
        """Forget a ring lease that was lost, lapsed or taken; held under ``lease_lock``.

        Whatever ring work still runs lost its fence with it, so the ring stays closed
        here until that work has finished. A confirmed release is not this: the work
        holding the fence makes it once done with it, and what outlives it either
        carries its own fence, as Round 5's run and cleanup do, or is settlement,
        which the next arm of its round waits for.
        """

        record.lease = None
        record.ring_lease_lost = True

    def _lose_absent_ring_lease(self, record: SessionRecord, lease: BoutLease) -> None:
        """Drop ``lease`` as lost: its row is gone and no release of it was confirmed.

        Held under ``lease_lock``. The row may have lapsed under ring work that still
        runs, and another replica can claim the ring at once, so the alarm checks now.
        """

        self._log_lease_lost(
            record, lease, "row_absent", self._lease_proven_until(lease, record.lease_proof)
        )
        self._lose_ring_lease(record)
        self._alarm_if_ring_work_outlives(record, time.monotonic())

    def _expired_on_its_own(self, record: SessionRecord, lease: BoutLease) -> bool:
        """Whether ``lease``'s row, found gone, simply reached its own end.

        Nothing renews an armed card's row or a failed Round 1 cooldown's fence, and no
        ring work runs under either: each ends at its TTL. Once this process can no
        longer prove the row live, its absence is that expiry and not a loss.
        """

        return lease.phase in {"armed", "cooldown_failed"} and time.monotonic() >= (
            self._lease_proven_until(lease, record.lease_proof)
        )

    @staticmethod
    def _unstopped_ring_work_refusal(
        record: SessionRecord,
        stopping: list[asyncio.Task[Any]],
    ) -> InvalidStateError:
        logger.warning(
            "Ring claim refused while lost-lease work is still stopping session=%s tasks=%s",
            record.snapshot.id,
            ",".join(task.get_name() for task in stopping),
        )
        return InvalidStateError(_LOST_RING_STILL_STOPPING)

    async def _give_back_claim(
        self,
        record: SessionRecord,
        lease: BoutLease,
        *,
        round5: bool = False,
    ) -> None:
        """Release a claim that won its row but must not be used, never renewing it."""

        store = self._round5_cleanup_store() if round5 else self._lease_store_for_record(record)
        until = time.monotonic() + max(0.001, self._terminal_release_call_timeout)
        released = await self._release_exact_fence_once(
            record, store, lease, until, round5=round5
        )
        if released != "released":
            self._retry_terminal_release_in_background(record, lease, round5=round5)

    async def _give_back_round5_claim(self, record: SessionRecord) -> None:
        """Give back a won Round 5 claim: its warm slot and both rings, renewing none."""

        main, cleanup = record.lease, record.round5_lease
        coordinator = self._round5_warm_coordinator
        warm_slot = record.round5_warm_slot
        claim = warm_slot.claim if warm_slot is not None else None
        if coordinator is not None and claim is not None:
            # Stopped first, so the claim lapses on its own if it cannot be returned.
            stop_renewing = getattr(coordinator, "release_claim_active", None)
            if callable(stop_renewing):
                stop_renewing(claim.claim_id)
            returned = getattr(coordinator, "return_unstarted_claim", None)
            if callable(returned):
                call = asyncio.ensure_future(
                    returned(
                        claim.claim_id,
                        main_store=self._lease_store_for_record(record),
                        main_lease=main,
                        cleanup_store=self._round5_cleanup_store(),
                        cleanup_lease=cleanup,
                    )
                )
                try:
                    await asyncio.wait(
                        {call}, timeout=max(0.001, self._terminal_release_call_timeout)
                    )
                finally:
                    answered = call.done()
                    if not answered:
                        self._abandon_lease_call(call)
                if answered and not call.cancelled() and call.exception() is None:
                    record.round5_warm_slot = call.result()
                    if await self._forget_rings_the_warm_store_returned(record, main, cleanup):
                        return
                else:
                    logger.warning(
                        "Round 5 claim given back without returning its warm slot "
                        "session=%s; the claim lapses on its own",
                        record.snapshot.id,
                    )
        if main is not None:
            await self._give_back_claim(record, main)
        if cleanup is not None:
            await self._give_back_claim(record, cleanup, round5=True)

    async def _forget_rings_the_warm_store_returned(
        self,
        record: SessionRecord,
        main: BoutLease | None,
        cleanup: BoutLease | None,
    ) -> bool:
        """Drop ``main`` and ``cleanup`` once returning their unstarted claim released both.

        The production warm store returns the claim and clears both ring rows, by their
        exact fences, in one transaction, so the rows are gone because of it and not
        lost. That transaction does not check expiry, so it proves no unbroken hold:
        only a record with no ring work running takes it as a plain clear. True if so.
        """

        store = getattr(self._round5_warm_coordinator, "store", None)
        if not callable(getattr(store, "return_unstarted_with_leases", None)):
            return False
        current = asyncio.current_task()
        if any(task is not current for task in self._ring_work(record)):
            return False
        async with record.lease_lock:
            local = record.lease
            if main is not None and local is not None and self._same_exact_lease(local, main):
                record.lease = None
            local = record.round5_lease
            if cleanup is not None and local is not None and self._same_exact_lease(local, cleanup):
                record.round5_lease = None
        if main is not None:
            self._cancel_lease_heartbeat(record, main)
        if cleanup is not None:
            self._cancel_round5_lease_heartbeat(record, cleanup)
        return True

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
                    sent = time.monotonic()
                    committed_round5 = await self._round5_cleanup_store().transition(
                        round5_lease,
                        operator=actual,
                        expected_phase="armed",
                        phase="run_committed",
                        session_state=SessionState.RUNNING,
                        ttl=timedelta(seconds=self._running_lease_ttl),
                    )
                    self._set_lease(record, committed_round5, sent, round5=True)
                # This is the atomic, fenced ARMED -> RUN_COMMITTED handoff. No other
                # app replica can claim the ring between the bell and live preparation.
                sent = record.bell_commit_sent_at = time.monotonic()
                committed = await self._lease_store_for_record(record).transition(
                    lease,
                    operator=actual,
                    expected_phase="armed",
                    phase="run_committed",
                    session_state=SessionState.RUNNING,
                    ttl=timedelta(seconds=self._running_lease_ttl),
                )
                self._set_lease(record, committed, sent)
                if record.snapshot.round.id in {
                    RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
                    RoundId.RECOVER_DELETED_ORDER,
                }:
                    record.round2_fencing_token = record.lease.fencing_token
            except LeaseLostError as exc:
                transition_error = exc
        # A lapsed commit is refused too. Convergence fails that card; Round 5, which
        # it skips, gives its rows back below.
        if transition_error is not None or self._bell_proof_lapsed(record):
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

    def _bell_proof_lapsed(self, record: SessionRecord) -> bool:
        """Whether the bell's commit answered too late to prove its leases still fence.

        A run must never start under such a lease; its heartbeat would declare it lost
        at once, after the run had already begun.
        """

        _, margin = self._heartbeat_cadence(timedelta(seconds=self._running_lease_ttl))
        now = time.monotonic()
        lapsed = any(
            held is not None and self._lease_proven_until(held, proof) - margin <= now
            for held, proof in (
                (record.lease, record.lease_proof),
                (record.round5_lease, record.round5_lease_proof),
            )
        )
        if lapsed:
            logger.warning(
                "Bell refused: its commit was answered after its lease could be proven "
                "live session=%s",
                record.snapshot.id,
            )
        return lapsed

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
                        sent = time.monotonic()
                        retained = await self._round5_cleanup_store().transition(
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
                        self._set_lease(record, retained, sent, round5=True)
                else:
                    record.round5_lease = lease
        if record.round5_lease is None and not allow_claim:
            raise InvalidStateError("ROUND 5 CLEANUP AUTHORITY CHANGED")
        if record.round5_lease is None:
            try:
                sent = time.monotonic()
                claimed = await self._round5_cleanup_store().claim(
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
            self._set_lease(record, claimed, sent, round5=True)
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
            # Awaited under both locks the heartbeat needs to declare this lease lost,
            # so never past the moment it must: a towel stalled on the coordinator
            # cannot keep the run going beyond its lease.
            _, margin = self._heartbeat_cadence(self._lease_ttl(record, lease.phase))
            sent = time.monotonic()
            left = self._lease_proven_until(lease, record.lease_proof) - margin - sent
            if left <= 0:
                raise InvalidStateError("RING LEASE UNCONFIRMED · TOWEL CLEANUP REFUSED")
            transition = asyncio.ensure_future(
                self._lease_store_for_record(record).transition(
                    lease,
                    operator=operator,
                    expected_phase="run_committed",
                    phase="towel_cleanup",
                    session_state=SessionState.RUNNING,
                    ttl=timedelta(seconds=self._running_lease_ttl),
                )
            )
            try:
                await asyncio.wait({transition}, timeout=left)
            finally:
                answered = transition.done()
                if not answered:
                    self._abandon_lease_call(transition)
            if not answered:
                logger.warning(
                    "Towel transition abandoned reason=timeout session=%s phase=%s",
                    record.snapshot.id,
                    lease.phase,
                )
                raise InvalidStateError("RING LEASE UNCONFIRMED · TOWEL CLEANUP REFUSED")
            try:
                towel = transition.result()
            except LeaseLostError as exc:
                raise InvalidStateError("RING LEASE CHANGED · TOWEL CLEANUP REFUSED") from exc
            self._set_lease(record, towel, sent)
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
                    sent = time.monotonic()
                    armed_round5 = await self._round5_cleanup_store().transition(
                        round5_lease,
                        operator=record.operator or round5_lease.operator,
                        expected_phase="checking",
                        phase="armed",
                        session_state=SessionState.ARMED,
                        ttl=ttl,
                    )
                    self._set_lease(record, armed_round5, sent, round5=True)
                sent = record.arm_commit_sent_at = time.monotonic()
                armed = await self._lease_store_for_record(record).transition(
                    lease,
                    operator=record.operator or lease.operator,
                    expected_phase="checking",
                    phase="armed",
                    session_state=SessionState.ARMED,
                    ttl=ttl,
                )
                self._set_lease(record, armed, sent)
            except LeaseLostError as exc:
                transition_error = exc
        if transition_error is not None:
            if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
                await self._release_round5_lease(record)
                await self._release_bout(record)
            raise InvalidStateError(str(transition_error)) from transition_error
        # Register the AUTHORITATIVE armed-lease deadline with the warm
        # coordinator so it renews the pre-bell claim through exactly this deadline
        # (honoring a long configured arm TTL, e.g. 1800s) and stops after -- not the
        # coarse leak-fallback horizon, which now applies ONLY when no armed deadline
        # was ever registered (a leaked active-id that never learned its deadline).
        if record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
            coordinator = self._round5_warm_coordinator
            warm_slot = record.round5_warm_slot
            claim = warm_slot.claim if warm_slot is not None else None
            setter = (
                getattr(coordinator, "set_active_claim_deadline", None)
                if coordinator is not None
                else None
            )
            if callable(setter) and claim is not None:
                setter(claim.claim_id, expires_at)
            # Hand the claimed engine (now holding the ARM-staged resident
            # bindings) to the coordinator/provider so the provider -- the sole
            # cleanup janitor -- can cancel those residents itself. The manager does
            # no external cleanup mutation of its own when a coordinator is present.
            adopt = (
                getattr(coordinator, "adopt_claimed_engine", None)
                if coordinator is not None
                else None
            )
            engine = record.connection_spike_engine
            if callable(adopt) and claim is not None and engine is not None:
                adopt(claim.claim_id, engine)
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
        if record.snapshot.round.id in {
            RoundId.PUT_MODEL_SCORE_IN_APP,
            RoundId.ANALYZE_LIVE_ORDERS,
        }:
            self._storage_refusals.pop(RoundId.PUT_MODEL_SCORE_IN_APP, None)
            self._storage_refusals.pop(RoundId.ANALYZE_LIVE_ORDERS, None)

    async def _publish_armed_if_current(
        self,
        record: SessionRecord,
        *,
        arm_bout_id: str,
        armed_at_monotonic: float,
        snapshot: SessionSnapshot,
        payload: dict[str, object],
    ) -> bool:
        """Commit and publish one arm only while its card is still current."""

        armed_expires_at = snapshot.armed_expires_at
        assert armed_expires_at is not None
        await self._mark_bout_armed(record, armed_expires_at)
        async with record.lock:
            if (
                record.snapshot.state != SessionState.ARMED
                or record.snapshot.receipt_bout_id != arm_bout_id
                or record.armed_at_monotonic != armed_at_monotonic
                or record.armed_release_pending
            ):
                return False
            # Scheduling and publication are serialized with no-bell cancellation.
            # Thus cancellation either suppresses both, or follows an already
            # visible armed event; it can never be followed by stale arm output.
            self._schedule_armed_expiry(record, armed_at_monotonic)
            await record.event_log.publish("armed", payload)
            return True

    async def _arm_attempt_was_cancelled(
        self,
        record: SessionRecord,
        arm_bout_id: str,
    ) -> bool:
        async with record.lock:
            if record.snapshot.receipt_bout_id != arm_bout_id:
                return True
            return record.armed_release_pending or (
                record.snapshot.state == SessionState.FAILED
                and record.snapshot.run_started_at is None
                and record.snapshot.failure is not None
                and (
                    record.snapshot.failure == _ARMED_CARD_CANCELLED
                    or record.snapshot.failure.startswith("Fight-card check cancelled")
                )
            )

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
            self._abandon_renewal_in_flight(record)
        record.lease_heartbeat_task = None
        record.lease_heartbeat_lease = None

    def _abandon_renewal_in_flight(self, record: SessionRecord, *, round5: bool = False) -> None:
        prefix = "round5_" if round5 else ""
        in_flight = getattr(record, f"{prefix}renewal_in_flight")
        setattr(record, f"{prefix}renewal_in_flight", None)
        if in_flight is not None and not in_flight[1].done():
            self._abandon_renewal(record, *in_flight, round5=round5)

    def _abandon_renewal(
        self,
        record: SessionRecord,
        fence: tuple[str, int],
        renewal: asyncio.Future[Any],
        *,
        round5: bool = False,
    ) -> None:
        abandoned = record.round5_abandoned_renewals if round5 else record.abandoned_renewals
        if abandoned.get(fence) is renewal:
            return
        abandoned[fence] = renewal
        self._abandon_lease_call(renewal)

        def forget(done: asyncio.Future[Any]) -> None:
            if abandoned.get(fence) is done:
                del abandoned[fence]

        renewal.add_done_callback(forget)

    def _start_lease_heartbeat(self, record: SessionRecord, ttl: timedelta) -> None:
        self._cancel_lease_heartbeat(record)
        record.lease_heartbeat_task = self._track_heartbeat(
            asyncio.create_task(
                self._heartbeat_lease(record, ttl),
                name=f"lease-heartbeat-{record.snapshot.id}",
            )
        )
        record.lease_heartbeat_lease = record.lease

    def _track_heartbeat(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        self._heartbeat_tasks.add(task)
        task.add_done_callback(self._heartbeat_tasks.discard)
        return task

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
            self._abandon_renewal_in_flight(record, round5=True)
        record.round5_lease_heartbeat_task = None
        record.round5_lease_heartbeat_lease = None

    def _start_round5_lease_heartbeat(
        self,
        record: SessionRecord,
        ttl: timedelta,
    ) -> None:
        self._cancel_round5_lease_heartbeat(record)
        record.round5_lease_heartbeat_task = self._track_heartbeat(
            asyncio.create_task(
                self._heartbeat_round5_lease(record, ttl),
                name=f"round5-lease-heartbeat-{record.snapshot.id}",
            )
        )
        record.round5_lease_heartbeat_lease = record.round5_lease

    def _heartbeat_cadence(self, ttl: timedelta) -> tuple[float, float]:
        """How often a lease of ``ttl`` renews, and how long before expiry it is lost.

        Loss is declared that long before the lease expires, so the run is stopped
        while its row still fences the ring. Capped so the first renew still fits.
        """

        interval = min(self._lease_heartbeat, max(0.25, ttl.total_seconds() / 3))
        return interval, min(interval, max(0.0, ttl.total_seconds() - interval) / 2)

    @staticmethod
    def _proven_until(lease: BoutLease, sent: float) -> float:
        """The monotonic time before which the write that returned ``lease`` cannot lapse.

        ``sent`` is when that write's request left. The coordinator applied the TTL on
        its own clock after that, and the TTL it applied is the difference of two of
        its own stamps, so the row lives at least that long past ``sent`` however far
        the coordinator's clock runs ahead of this one.
        """

        return sent + (lease.expires_at - lease.updated_at).total_seconds()

    def _set_lease(
        self,
        record: SessionRecord,
        lease: BoutLease,
        sent: float,
        *,
        round5: bool = False,
    ) -> None:
        proof = (lease, self._proven_until(lease, sent))
        if round5:
            record.round5_lease, record.round5_lease_proof = lease, proof
        else:
            record.lease, record.lease_proof = lease, proof

    def _adopt_lease(self, record: SessionRecord, holder: BoutLease, sent: float | None) -> None:
        """Install a row adopted after its write's answer was lost, proven from that write."""

        if sent is None:
            record.lease = holder
        else:
            self._set_lease(record, holder, sent)

    @staticmethod
    def _lease_proven_until(
        lease: BoutLease,
        proof: tuple[BoutLease, float] | None,
    ) -> float:
        if proof is not None and proof[0] == lease:
            return proof[1]
        # No confirmed write here vouches for this lease, as for one installed by
        # hand. Its stamped expiry read against this clock is the only bound left.
        return time.monotonic() + (lease.expires_at - datetime.now(UTC)).total_seconds()

    async def _renew_within(
        self,
        record: SessionRecord,
        store: BoutLeaseStore,
        lease: BoutLease,
        ttl: timedelta,
        proven_until: float,
        margin: float,
        *,
        round5: bool = False,
    ) -> tuple[tuple[BoutLease, float] | None, str | None]:
        """Renew ``lease`` once, never waiting past ``proven_until - margin``.

        Returns the renewed lease with its proof, or why the lease is lost
        (``"lease_lost"``, ``"timeout"`` or ``"deadline"``), or neither while it is
        too early to tell.
        """

        abandoned = record.round5_abandoned_renewals if round5 else record.abandoned_renewals
        in_flight = "round5_renewal_in_flight" if round5 else "renewal_in_flight"
        fence = (lease.lease_id, lease.fencing_token)
        sent = time.monotonic()
        left = proven_until - margin - sent
        if left <= 0:
            return None, "deadline"
        pending = abandoned.get(fence)
        if pending is not None and not pending.done():
            return None, None
        renewal = asyncio.ensure_future(store.renew(lease, ttl=ttl))
        setattr(record, in_flight, (fence, renewal))
        try:
            # Only the deadline bounds it: a slow coordinator that answers in time
            # proves the lease live, and a renew cut short may still have landed.
            await asyncio.wait({renewal}, timeout=left)
        finally:
            current = getattr(record, in_flight)
            if current is not None and current[1] is renewal:
                setattr(record, in_flight, None)
            answered = renewal.done()
            if not answered:
                self._abandon_renewal(record, fence, renewal, round5=round5)
        if not answered:
            logger.warning(
                "Ring lease renew abandoned lease=%s reason=timeout session=%s phase=%s "
                "remaining_seconds=%.1f",
                "round5" if round5 else "main",
                record.snapshot.id,
                lease.phase,
                max(0.0, proven_until - time.monotonic()),
            )
            return None, "timeout"
        try:
            renewed = renewal.result()
        except LeaseLostError:
            return None, "lease_lost"
        except Exception:
            # A transient coordinator outage must not create two owners. The
            # existing database expiry remains authoritative; fail only once
            # this replica can no longer prove its lease is still live.
            if time.monotonic() >= proven_until - margin:
                return None, "deadline"
            return None, None
        return (renewed, self._proven_until(renewed, sent)), None

    def _abandon_lease_call(self, call: asyncio.Future[Any]) -> None:
        # Abandoned rather than awaited: psycopg meets a cancellation by waiting on
        # the same, possibly black-holed, socket again.
        if call in self._abandoned_calls:
            return
        call.cancel()
        self._abandoned_calls.add(call)

        def forget(done: asyncio.Future[Any]) -> None:
            self._abandoned_calls.discard(done)
            if not done.cancelled():
                done.exception()

        call.add_done_callback(forget)

    async def _bounded_lease_call(
        self, operation: Awaitable[Any], timeout_seconds: float
    ) -> Any:
        """``operation``'s answer, or TimeoutError once ``timeout_seconds`` pass without one.

        Unlike ``asyncio.wait_for``, a call that has not answered by then is abandoned
        (see ``_abandon_lease_call``), not awaited while it unwinds, so it cannot hold its
        caller past the deadline that set ``timeout_seconds``.
        """

        call = asyncio.ensure_future(operation)
        try:
            await asyncio.wait({call}, timeout=timeout_seconds)
        finally:
            answered = call.done()
            if not answered:
                self._abandon_lease_call(call)
        if not answered:
            raise TimeoutError
        return call.result()

    @staticmethod
    def _log_lease_lost(
        record: SessionRecord,
        lease: BoutLease,
        reason: str,
        proven_until: float,
        *,
        round5: bool = False,
    ) -> None:
        logger.warning(
            "Ring lease declared lost lease=%s reason=%s session=%s phase=%s "
            "lease_id=%s fence=%s remaining_seconds=%.1f",
            "round5" if round5 else "main",
            reason,
            record.snapshot.id,
            lease.phase,
            lease.lease_id,
            lease.fencing_token,
            max(0.0, proven_until - time.monotonic()),
        )

    async def _heartbeat_round5_lease(
        self,
        record: SessionRecord,
        ttl: timedelta,
    ) -> None:
        interval, margin = self._heartbeat_cadence(ttl)
        store = self._round5_cleanup_store()
        pause = interval
        while True:
            try:
                await asyncio.sleep(pause)
            except asyncio.CancelledError:
                return
            async with record.lease_lock:
                lease = record.round5_lease
                proof = record.round5_lease_proof
            if lease is None or lease.phase == "armed":
                return
            proven_until = self._lease_proven_until(lease, proof)
            renewed, lost = await self._renew_within(
                record, store, lease, ttl, proven_until, margin, round5=True
            )
            pause = (
                interval
                if renewed is not None
                else min(interval, max(0.0, proven_until - margin - time.monotonic()))
            )
            async with record.lease_lock:
                current = record.round5_lease
                if current is None:
                    return
                if not self._same_exact_lease(current, lease):
                    continue
                if renewed is not None:
                    record.round5_lease, record.round5_lease_proof = renewed[0], renewed
                if lost is None:
                    continue
            self._log_lease_lost(record, current, lost, proven_until, round5=True)
            failure_message: str | None = None
            async with record.lock:
                async with record.lease_lock:
                    if record.round5_lease != current:
                        return
                    record.round5_lease = None
                    record.round5_lease_heartbeat_task = None
                    record.round5_lease_heartbeat_lease = None
                if record.snapshot.state not in {
                    SessionState.VERIFIED,
                    SessionState.TOWELLED,
                    SessionState.FAILED,
                } and not record.round5_terminal_published:
                    failure_message = (
                        "Round 5 artifact authority was lost; no comparison was declared."
                    )
                    if not record.round5_ingestion_closed:
                        record.round5_ingestion_closed = True
                        record.round5_callback_epoch += 1
            if failure_message is not None:
                task = record.task
                if (
                    task is not None
                    and task is not asyncio.current_task()
                    and not task.done()
                ):
                    task.cancel()
                    await self._wait_stopped(task)
                await self._finish_connection_spike_failure(
                    record,
                    failure_message,
                )
            elif any(task is not asyncio.current_task() for task in self._ring_work(record)):
                # Backstage cleanup holds the main ring until it ends, then releases it
                # itself, claiming a fresh cleanup lease first if it still needs one.
                logger.warning(
                    "Round 5 cleanup lease lost while backstage cleanup still runs "
                    "session=%s; the main ring stays held until that cleanup ends",
                    record.snapshot.id,
                )
            else:
                await self._release_bout(record)
            return

    async def _heartbeat_lease(self, record: SessionRecord, ttl: timedelta) -> None:
        interval, margin = self._heartbeat_cadence(ttl)
        store = self._lease_store_for_record(record)
        pause = interval
        while True:
            try:
                await asyncio.sleep(pause)
            except asyncio.CancelledError:
                return
            # Renewed outside lease_lock: a renew can hang, and the towel and a read
            # of the card both wait on that lock.
            async with record.lease_lock:
                lease = record.lease
                proof = record.lease_proof
            if lease is None or lease.phase == "armed":
                return
            proven_until = self._lease_proven_until(lease, proof)
            renewed, lost = await self._renew_within(
                record, store, lease, ttl, proven_until, margin
            )
            pause = (
                interval
                if renewed is not None
                else min(interval, max(0.0, proven_until - margin - time.monotonic()))
            )
            async with record.lease_lock:
                current = record.lease
                if current is None:
                    return
                if not self._same_exact_lease(current, lease):
                    # A towel or handoff moved the fence during the renew; whatever
                    # the renew answered, a refusal included, was about the old one.
                    continue
                if renewed is not None:
                    record.lease, record.lease_proof = renewed[0], renewed
                if lost is None:
                    continue
                if self._terminal_release_owns(record, current):
                    # A release/current probe for this exact fence is already
                    # authoritative. It may have removed the coordinator row
                    # before its response reached us.
                    return
            self._log_lease_lost(record, current, lost, proven_until)
            self._alarm_if_ring_work_outlives(record, proven_until)
            await self._handle_lost_lease(record, current)
            return

    def _alarm_if_ring_work_outlives(self, record: SessionRecord, proven_until: float) -> None:
        """Log an error if work that lost the ring still runs once the lost row can lapse.

        From then another replica may claim the ring, and nothing fences that work off
        from its new owner. The task raising the alarm is not counted: it is the one
        handling the loss, and when it is ring work, it is the work giving the ring up.
        Raised by Round 4's terminal publication, neither is its run, which only awaits
        that publication.
        """

        raised_by = asyncio.current_task()
        handling = {raised_by}
        if raised_by is not None and raised_by is record.model_score_terminal_task:
            handling.add(record.task)

        def check() -> None:
            if not record.ring_lease_lost or record.lease is not None:
                return
            running = [task for task in self._ring_work(record) if task not in handling]
            if running:
                logger.error(
                    "Ring lease may have lapsed while work that lost it still runs "
                    "session=%s tasks=%s; another replica can claim the ring meanwhile",
                    record.snapshot.id,
                    ",".join(task.get_name() for task in running),
                )

        asyncio.get_running_loop().call_later(max(0.0, proven_until - time.monotonic()), check)

    @staticmethod
    async def _wait_stopped(task: asyncio.Task[Any]) -> None:
        """Wait for a cancelled ``task`` to end; cancelling the waiter ends only the wait."""

        await asyncio.wait({task})
        if not task.cancelled():
            task.exception()

    async def _handle_lost_lease(self, record: SessionRecord, lease: BoutLease) -> None:
        message = "Ring lease lost; active work was stopped before another bout can begin."
        round5_terminal_event_published = False
        round5_failure_pending = False
        async with record.lock:
            async with record.lease_lock:
                if self._terminal_release_owns(record, lease):
                    return
                if record.lease != lease:
                    return
                self._lose_ring_lease(record)
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
                    receipt_bout_id=snapshot.receipt_bout_id,
                )
                return
            if is_model_score and record.model_score_terminal_published:
                return
            if is_model_score and lease.phase == "run_committed":
                message = "Round 4's ring lease was lost before the result could be published."
                for lane in record.snapshot.lanes.values():
                    if lane.state == LaneState.NOT_SUPPORTED:
                        continue
                    lane.state = LaneState.FAILED
                    lane.error = message
                    lane.status = message
                    lane.activity = LaneActivity(phase=Round4Phase.FAILED)
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
                if record.round5_terminal_published:
                    return
                if not record.round5_ingestion_closed:
                    record.round5_ingestion_closed = True
                    record.round5_callback_epoch += 1
                round5_failure_pending = True
            else:
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = message
                record.snapshot.remembered_result = None
            record.snapshot.updated_at = datetime.now(UTC)
            # Scheduled under the lock that set FAILED, so no re-arm can slip in
            # between the card failing and its settlement existing.
            if record.snapshot.round.id == RoundId.PUT_MODEL_SCORE_IN_APP:
                self._schedule_round_settlement(
                    record,
                    getattr(record.model_score_engine, "settle_and_restore_baseline", None),
                    label="Round 4",
                )
            elif record.snapshot.round.id == RoundId.ANALYZE_LIVE_ORDERS:
                self._schedule_round_settlement(
                    record,
                    getattr(record.live_orders_engine, "settle_and_cleanup_owned", None),
                    label="Round 6",
                )
            task = record.task
            cooldown_task = record.cooldown_task
            snapshot = record.snapshot.model_copy(deep=True)
            if (
                is_connection_spike
                and not round5_terminal_event_published
                and not round5_failure_pending
            ):
                await record.event_log.publish(
                    "session_failed",
                    {
                        "state": SessionState.FAILED,
                        "message": message,
                        "session": snapshot.model_dump(mode="json"),
                    },
                    receipt_bout_id=snapshot.receipt_bout_id,
                )
                round5_terminal_event_published = True
        if round5_failure_pending:
            if task and task is not asyncio.current_task() and not task.done():
                task.cancel()
                await self._wait_stopped(task)
            await self._finish_connection_spike_failure(
                record,
                message,
            )
            return
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
                    self._note_round5_proxy_at_risk(record, force=True)
                    self._schedule_connection_spike_cleanup_convergence(record)
                    self._advance_round5_revision_locked(record)
                    record.snapshot.updated_at = datetime.now(UTC)
                    snapshot = record.snapshot.model_copy(deep=True)
                    await record.event_log.publish(
                        "cleanup_update",
                        {"session": snapshot.model_dump(mode="json")},
                        receipt_bout_id=snapshot.receipt_bout_id,
                    )
        for operation in (task, cooldown_task):
            if is_connection_spike and operation is task:
                continue
            if operation and operation is not asyncio.current_task() and not operation.done():
                operation.cancel()
        event = "session_failed"
        if not round5_terminal_event_published:
            await record.event_log.publish(
                event,
                {
                    "state": snapshot.state,
                    "message": message,
                    "session": snapshot.model_dump(mode="json"),
                },
                receipt_bout_id=snapshot.receipt_bout_id,
            )

    def _lease_moved_on(self, record: SessionRecord, owned: BoutLease | None) -> bool:
        """Whether the record now holds a fence claimed after ``owned``."""

        local = record.lease
        return (
            owned is not None
            and local is not None
            and not self._same_lease_authority(local, owned)
        )

    async def _release_bout(
        self,
        record: SessionRecord,
        owned: BoutLease | None = None,
    ) -> bool:
        """Release the record's lease; with ``owned``, only a fence of that authority.

        Work that outlives its bout passes the fence it ran under. A newer claim's
        fence is then left alone, heartbeat included, and the caller's is reported
        gone, which it is. A lease the coordinator no longer holds without having
        confirmed this release is reported released too, but dropped as lost (see
        ``_lose_absent_ring_lease``) unless its row ran out on its own (see
        ``_expired_on_its_own``).
        """

        if owned is None:
            self._cancel_lease_heartbeat(record)
        store = self._lease_store_for_record(record)
        async with record.lease_lock:
            lease = record.lease
            if lease is None or self._lease_moved_on(record, owned):
                return True
            if owned is not None:
                self._cancel_lease_heartbeat(record)
            try:
                released = await store.release(lease)
            except Exception:
                released = False
            lost = False
            if not released:
                try:
                    holder = await store.current()
                except Exception:
                    holder = lease
                if holder is None:
                    released = True
                    lost = not self._expired_on_its_own(record, lease)
                elif (
                    lease.phase == "checking"
                    and holder.phase == "armed"
                    and self._same_lease_authority(holder, lease)
                    and record.snapshot.state == SessionState.CHECKING
                ):
                    # The CHECKING -> ARMED CAS committed but its response was
                    # lost. This is still our pre-bell fence, not another
                    # holder. Adopt its exact phase and release that row; never
                    # generalize this to RUN_COMMITTED, which is bell authority.
                    self._adopt_lease(record, holder, record.arm_commit_sent_at)
                    lease = holder
                    try:
                        released = await store.release(holder)
                    except Exception:
                        released = False
                else:
                    released = False
            if released:
                current = record.lease
                if current is not None and (
                    current.lease_id == lease.lease_id
                    and current.fencing_token == lease.fencing_token
                ):
                    if lost:
                        self._lose_absent_ring_lease(record, current)
                    else:
                        record.lease = None
                return True
        current = record.lease
        if current is not None and (
            current.lease_id == lease.lease_id and current.fencing_token == lease.fencing_token
        ):
            self._start_lease_heartbeat(record, self._lease_ttl(record, lease.phase))
        return False

    def _lease_ttl(self, record: SessionRecord, phase: str) -> timedelta:
        ttl_seconds = (
            self._running_lease_ttl
            if phase in {
                "run_committed",
                "redo_committed",
                "towel_cleanup",
                "cooldown",
            }
            or phase == "cleanup_retry"
            else self._active_lease_ttl
        )
        if phase == "cooldown" and record.snapshot.round.id == RoundId.WAKE_IDLE_APP:
            ttl_seconds = max(
                ttl_seconds,
                self._cooldown_timeout + self._active_lease_ttl,
            )
        return timedelta(seconds=ttl_seconds)

    # _abandon_round5_warm_claim (direct pre-bell return-to-READY) was removed:
    # every no-bell abandon now enters CLEANING via _begin_round5_warm_cleanup and
    # converges through exact durable SETTLED + provider absence + rewarm N+1.

    async def _begin_round5_warm_cleanup(self, record: SessionRecord) -> bool:
        """Move the held warm claim to CLEANING and stop claim renewal."""

        coordinator = self._round5_warm_coordinator
        warm_slot = record.round5_warm_slot
        claim = warm_slot.claim if warm_slot is not None else None
        if coordinator is None or claim is None:
            return True
        coordinator.release_claim_active(claim.claim_id)
        try:
            record.round5_warm_slot = await coordinator.begin_cleanup(
                claim.claim_id
            )
        except Exception as exc:
            logger.error(
                "Round 5 abandoned claim could not enter durable cleanup "
                "session=%s diagnostic=%s",
                record.snapshot.id,
                _redacted_exception_chain(exc),
            )
            return False
        return True

    async def _release_round5_lease(self, record: SessionRecord) -> bool:
        self._cancel_round5_lease_heartbeat(record)
        # Stop backstage renewal of the warm claim so an abandoned arm's claim can
        # expire and return the warm generation to READY/WARMING.  The coordinator
        # already clears the claim at the bell and during cleanup; this covers the
        # remaining arm-abandon paths (arm failure, refused bell) that release the
        # Round 5 lease without a cleanup transition.
        coordinator = self._round5_warm_coordinator
        warm_slot = record.round5_warm_slot
        if (
            coordinator is not None
            and warm_slot is not None
            and warm_slot.claim is not None
        ):
            coordinator.release_claim_active(warm_slot.claim.claim_id)
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
                    # Gone is released, never lost, on this ring: it is only released once
                    # no Round 5 work runs under it, so none can have outlived its row.
                    released = await store.current() is None
                except Exception:
                    released = False
            if released:
                current = record.round5_lease
                if current is not None and self._same_exact_lease(current, lease):
                    record.round5_lease = None
                self._round5_durable_cleanup_blocked = False
                return True
        current = record.round5_lease
        if current is not None and self._same_exact_lease(current, lease):
            self._start_round5_lease_heartbeat(
                record,
                timedelta(seconds=self._running_lease_ttl),
            )
        return False

    @staticmethod
    def _same_lease_authority(left: BoutLease, right: BoutLease) -> bool:
        return (
            left.lease_id == right.lease_id
            and left.fencing_token == right.fencing_token
            and left.session_id == right.session_id
            and left.owner_subject == right.owner_subject
        )

    @classmethod
    def _same_exact_lease(cls, left: BoutLease, right: BoutLease) -> bool:
        return (
            cls._same_lease_authority(left, right)
            and left.phase == right.phase
        )

    @classmethod
    def _terminal_release_owns(cls, record: SessionRecord, lease: BoutLease) -> bool:
        terminal = record.model_score_terminal_lease
        if terminal is not None and cls._same_exact_lease(terminal, lease):
            return True
        return cls._terminal_settle_active(record, lease)

    @classmethod
    def _terminal_settle_active(cls, record: SessionRecord, lease: BoutLease) -> bool:
        return any(
            cls._same_exact_lease(held, lease) for _, held in record.terminal_release_settles
        )

    async def _settle_model_score_terminal_lease(
        self,
        record: SessionRecord,
    ) -> str:
        """Return once the Round 4 lease is released, provably lost, or out of time.

        Every store failure below is swallowed on purpose: a coordinator that is
        briefly unreachable must not turn a good proof into a reported lease
        loss. That tolerance has no natural end, though, so it gets an overall
        deadline, which no coordinator call outlives. Past it the answer is
        ``"unknown"`` -- the lease is neither released nor declared lost, the
        heartbeat stops so the durable TTL can expire it, while a non-renewing
        background retry releases it sooner if the coordinator recovers. That is
        the same bounded ownership handoff as the other terminal paths.
        """
        store = self._lease_store_for_record(record)
        async with record.lease_lock:
            expected = record.model_score_terminal_lease
            local = record.lease
        if expected is None:
            return "lost"
        deadline = time.monotonic() + max(0.0, self._terminal_settle_deadline)
        call_timeout = max(0.001, self._terminal_release_call_timeout)

        def call_window() -> float:
            return max(0.001, min(call_timeout, deadline - time.monotonic()))

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
                    released = await self._bounded_lease_call(
                        store.release(expected), call_window()
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
                        current = await self._bounded_lease_call(store.current(), call_window())
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
                        elif current_known and current is None:
                            self._lose_absent_ring_lease(record, local)
                            outcome = "lost"
                            stop_expected_heartbeat = True
                        elif current_known and not self._same_exact_lease(current, expected):
                            self._lose_ring_lease(record)
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
                    "retried without renewal until it is released or expires.",
                    record.snapshot.id,
                    self._terminal_settle_deadline,
                )
                self._retry_terminal_release_in_background(record, expected)
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

    async def _settle_terminal_release(
        self,
        record: SessionRecord,
        expected: BoutLease | None,
    ) -> str:
        """Release the fence a bout finished under: "released", "lost" or "unknown".

        For the terminal paths that nothing retries afterwards: Round 6's two
        finishes, ``_fail`` (including Round 5's failure after verified cleanup),
        Round 1's automatic return to idle and Round 1's owner cancel of a check.
        ``_release_bout`` keeps an unconfirmed lease heartbeating so that the
        towel's retry still holds the ring; on these paths no retry was coming,
        so that heartbeat renewed a finished bout's ring for the life of the
        process.

        Here ``expected`` is retried, bounded by ``_terminal_settle_deadline``,
        which is absolute: no wait outlives it and no call starts after it. The
        heartbeat lives exactly as long as some settle of ``expected`` runs (see
        ``_ensure_terminal_heartbeat``) and leaves a loss of this exact fence to
        those settles, as Round 4's terminal settle does: a release may have
        removed the row before its answer was lost. "released" means the
        coordinator holds no lease for that fence any more; unless it confirmed
        this release, the bout still finishes, but the ring stays closed here
        while the bout's ring work runs. "lost" means the record no longer holds
        that fence, or the coordinator names another holder, which is then
        reported as a lost lease; either way the bout is not this caller's to
        finish.
        "unknown" means the deadline passed first: the heartbeat stops and the
        caller finishes while a non-renewing background retry keeps releasing
        the fence until it is released, moves on, or expires at its durable TTL.
        Cancelled, the last settle of the fence stops the heartbeat and leaves
        the lease to that TTL. Never called holding ``record.lock``, so a towel
        or a read of the card does not wait out the retry.
        """

        if expected is None:
            return "released"
        store = self._lease_store_for_record(record)
        deadline = time.monotonic() + max(0.0, self._terminal_settle_deadline)
        backoff = 0.01
        outcome: str | None = None
        registration = (object(), expected)
        record.terminal_release_settles.append(registration)
        working = expected
        try:
            async with record.lease_lock:
                self._ensure_terminal_heartbeat(record, expected)
            while outcome is None:
                async with record.lease_lock:
                    local = record.lease
                if (
                    local is not None
                    and working.phase == "checking"
                    and local.phase == "armed"
                    and self._same_lease_authority(local, working)
                    and record.bell_committed_at is None
                ):
                    # `_release_exact_fence_once` may have adopted the pre-bell
                    # successor after a lost CHECKING→ARMED answer. Retry that
                    # exact armed row; the original checking copy would look
                    # like a lost fence and `_fail` would abandon the card.
                    working = local
                    async with record.lease_lock:
                        self._ensure_terminal_heartbeat(record, working)
                outcome = await self._release_exact_fence_once(
                    record, store, working, deadline
                )
                if outcome is None:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        outcome = "unknown"
                    else:
                        await asyncio.sleep(min(backoff, left))
                        backoff = min(
                            backoff * 2,
                            max(0.01, self._terminal_release_backoff_cap),
                        )
        finally:
            record.terminal_release_settles = [
                entry for entry in record.terminal_release_settles if entry is not registration
            ]
            last = not self._terminal_settle_active(record, expected)
            # The last settle of this fence to exit, however it exits, ends the
            # heartbeat it ran under. Only that exact fence's: a newer lease's
            # heartbeat is not this settle's to stop. After a lost CHECKING→ARMED
            # answer the settle may have adopted the successor; stop that
            # heartbeat too so it cannot outlive the bout.
            if last:
                self._cancel_lease_heartbeat(record, expected)
                if working is not expected:
                    self._cancel_lease_heartbeat(record, working)
        async with record.lease_lock:
            local = record.lease
        if (
            local is not None
            and working.phase == "checking"
            and local.phase == "armed"
            and self._same_lease_authority(local, working)
            and record.bell_committed_at is None
        ):
            # Adoption can finish inside `_release_exact_fence_once` on the
            # same pass that then returns taken/unknown. `working` still names
            # checking until this refresh; the stale copy would skip lost-lease
            # handling and `_fail` would abandon an ARMED card.
            working = local
        if outcome == "unknown":
            logger.error(
                "Terminal ring release for %s could not be confirmed within %.1fs. "
                "Its release is retried in the background, never renewing, until "
                "it is confirmed or the lease expires at its durable TTL.",
                record.snapshot.id,
                self._terminal_settle_deadline,
            )
            # Whatever a sibling settle still running goes on to do, including
            # being cancelled, this release is still owed. Retry the fence the
            # settle actually held: after a lost CHECKING→ARMED answer that is
            # the adopted armed row, not the stale checking copy.
            self._retry_terminal_release_in_background(record, working)
            return "unknown"
        if not last:
            # A sibling settle of this fence is still running; it owns the lost
            # lease report.
            return "lost" if outcome == "taken" else outcome
        if outcome == "taken":
            if local is not None and self._same_exact_lease(local, working):
                await self._handle_lost_lease(record, local)
            return "lost"
        return outcome

    async def _settle_round5_release(
        self,
        record: SessionRecord,
        expected: BoutLease | None,
    ) -> str:
        """Release a finished bout's Round 5 cleanup lease: "released", "lost" or "unknown".

        For a bout whose cleanup already verified, so the lease no longer fences any
        backstage work and is not renewed while its release is retried. Bounded by
        the same absolute ``_terminal_settle_deadline``, with every coordinator call
        outside ``lease_lock`` and the record checked for exactly ``expected``
        around them. "unknown" leaves a non-renewing background retry, as the main
        ring's settle does. Never called holding ``record.lock``.
        """

        # As in `_release_round5_lease`: stop backstage renewal of the warm claim
        # so it can expire and return the generation to READY/WARMING.
        coordinator = self._round5_warm_coordinator
        warm_slot = record.round5_warm_slot
        if coordinator is not None and warm_slot is not None and warm_slot.claim is not None:
            coordinator.release_claim_active(warm_slot.claim.claim_id)
        if expected is None:
            return "released"
        self._cancel_round5_lease_heartbeat(record, expected)
        store = self._round5_cleanup_store()
        deadline = time.monotonic() + max(0.0, self._terminal_settle_deadline)
        backoff = 0.01
        outcome: str | None = None
        while outcome is None:
            outcome = await self._release_exact_fence_once(
                record, store, expected, deadline, round5=True
            )
            if outcome is None:
                left = deadline - time.monotonic()
                if left <= 0:
                    outcome = "unknown"
                else:
                    await asyncio.sleep(min(backoff, left))
                    backoff = min(backoff * 2, max(0.01, self._terminal_release_backoff_cap))
        if outcome == "released":
            self._round5_durable_cleanup_blocked = False
        elif outcome == "taken":
            async with record.lease_lock:
                local = record.round5_lease
                if local is not None and self._same_exact_lease(local, expected):
                    record.round5_lease = None
            outcome = "lost"
        elif outcome == "unknown":
            logger.error(
                "Round 5 cleanup lease release for %s could not be confirmed within %.1fs. "
                "Its release is retried in the background, never renewing, until it "
                "is confirmed or the lease expires at its durable TTL.",
                record.snapshot.id,
                self._terminal_settle_deadline,
            )
            self._retry_terminal_release_in_background(record, expected, round5=True)
        return outcome

    def _ensure_terminal_heartbeat(self, record: SessionRecord, expected: BoutLease) -> None:
        """Renew ``expected`` for as long as a settle of it runs; call under ``lease_lock``.

        A settle normally inherits the bout's heartbeat. One that starts after a
        cancelled settle stopped it restarts it, so the fence it still retries is
        not left to lapse mid-retry. A live heartbeat is never replaced: the
        record's lease, and so its heartbeat, may already be a newer fence's. The
        last settle of ``expected`` to exit stops it.
        """

        local = record.lease
        if local is None or not self._same_exact_lease(local, expected):
            return
        heartbeat = record.lease_heartbeat_task
        if heartbeat is not None and not heartbeat.done():
            return
        self._start_lease_heartbeat(record, self._lease_ttl(record, local.phase))

    async def _release_exact_fence_once(
        self,
        record: SessionRecord,
        store: BoutLeaseStore,
        expected: BoutLease,
        until: float,
        *,
        round5: bool = False,
    ) -> str | None:
        """One release of ``expected`` whose every wait ends by monotonic ``until``.

        "released" (the coordinator holds no lease for it any more), "taken" (the
        coordinator names another holder), "lost" (the record no longer holds this
        fence), "unknown" (no time was left for the next wait), or None to try
        again. The coordinator calls run outside ``lease_lock``, so a towel moving
        this fence is never queued behind them: a release matches the phase as well
        as the fence, so a towel's row cannot be released by this stale copy, and
        the record is checked for exactly ``expected`` before and after.

        Only a release the coordinator confirmed is known to be this one. A main
        ring row found gone without that may have lapsed under ring work that still
        runs, so its fence is dropped as lost (see ``_lose_absent_ring_lease``),
        unless it is a fence nothing renews that has run out on its own (see
        ``_expired_on_its_own``).
        """

        call_timeout = max(0.001, self._terminal_release_call_timeout)
        held = await self._check_exact_lease(record, expected, until, round5=round5)
        if held != "held":
            return held
        left = until - time.monotonic()
        if left <= 0:
            return "unknown"
        try:
            released = await self._bounded_lease_call(
                store.release(expected), min(call_timeout, left)
            )
        except Exception:
            released = False
        if not released:
            left = until - time.monotonic()
            if left <= 0:
                return "unknown"
            try:
                holder = await self._bounded_lease_call(
                    store.current(), min(call_timeout, left)
                )
            except Exception:
                return None
            if holder is not None and self._same_exact_lease(holder, expected):
                return None
            if (
                not round5
                and holder is not None
                and expected.phase == "checking"
                and holder.phase == "armed"
                and self._same_lease_authority(holder, expected)
                and record.bell_committed_at is None
            ):
                # The arm CAS can commit and lose only its response. Adopt that
                # exact pre-bell successor while local authority still matches,
                # then release it with the phase-fenced operation. A later
                # RUN_COMMITTED phase is deliberately excluded.
                adopted = False
                async with record.lease_lock:
                    local = record.lease
                    if (
                        local is not None
                        and self._same_exact_lease(local, expected)
                        and record.bell_committed_at is None
                    ):
                        self._adopt_lease(record, holder, record.arm_commit_sent_at)
                        adopted = True
                if adopted:
                    return await self._release_exact_fence_once(
                        record,
                        store,
                        holder,
                        until,
                    )
            if holder is not None:
                held = await self._check_exact_lease(record, expected, until, round5=round5)
                return "taken" if held == "held" else held
        held = await self._check_exact_lease(
            record, expected, until, clear=True, lost=not released, round5=round5
        )
        return "released" if held == "held" else held

    async def _check_exact_lease(
        self,
        record: SessionRecord,
        expected: BoutLease,
        until: float,
        *,
        clear: bool = False,
        lost: bool = False,
        round5: bool = False,
    ) -> str:
        """"held", "lost", or "unknown" when ``lease_lock`` was not free by ``until``.

        With ``clear`` a held lease is dropped in the same critical section, and with
        ``lost`` too a main ring lease is dropped as lost (see ``_lose_absent_ring_lease``)
        unless its row ran out on its own (see ``_expired_on_its_own``).
        ``round5`` checks the Round 5 cleanup lease instead of the main ring's.
        """

        left = until - time.monotonic()
        if left <= 0:
            return "unknown"
        try:
            await asyncio.wait_for(record.lease_lock.acquire(), timeout=left)
        except TimeoutError:
            return "unknown"
        try:
            local = record.round5_lease if round5 else record.lease
            if local is None or not self._same_exact_lease(local, expected):
                return "lost"
            if clear:
                if round5:
                    record.round5_lease = None
                elif lost and not self._expired_on_its_own(record, local):
                    self._lose_absent_ring_lease(record, local)
                else:
                    record.lease = None
            return "held"
        finally:
            record.lease_lock.release()

    def _retry_terminal_release_in_background(
        self,
        record: SessionRecord,
        expected: BoutLease,
        *,
        round5: bool = False,
    ) -> None:
        if self._closed:
            return
        if (
            not round5
            and expected.phase == "checking"
            and record.bell_committed_at is None
        ):
            local = record.lease
            if (
                local is not None
                and local.phase == "armed"
                and self._same_lease_authority(local, expected)
            ):
                expected = local
        slot = "round5_release_retry" if round5 else "terminal_release_retry"
        previous: asyncio.Task[None] | None = getattr(record, f"{slot}_task")
        if previous is not None and not previous.done():
            running: BoutLease | None = getattr(record, f"{slot}_lease")
            if running is not None and self._same_exact_lease(running, expected):
                return
            # A retry of an older fence would only find it gone; this one is owed.
            previous.cancel()
        setattr(record, f"{slot}_lease", expected)

        async def retry_and_forget() -> None:
            try:
                await self._retry_terminal_release(record, expected, round5=round5)
            finally:
                current = asyncio.current_task()
                if getattr(record, f"{slot}_task") is current:
                    setattr(record, f"{slot}_task", None)
                    setattr(record, f"{slot}_lease", None)

        task = asyncio.create_task(
            retry_and_forget(),
            name=f"{slot.replace('_', '-')}-{record.snapshot.id}",
        )
        setattr(record, f"{slot}_task", task)

        def forget_unstarted(done: asyncio.Task[None]) -> None:
            # A task cancelled before its coroutine is first entered cannot run
            # the `finally` above; keep that shutdown-only edge from retaining it.
            if getattr(record, f"{slot}_task") is done:
                setattr(record, f"{slot}_task", None)
                setattr(record, f"{slot}_lease", None)

        task.add_done_callback(forget_unstarted)

    async def _retry_terminal_release(
        self,
        record: SessionRecord,
        expected: BoutLease,
        *,
        round5: bool = False,
    ) -> None:
        """Keep releasing a finished bout's fence after its settle ran out of time.

        The bout has already finished and published; this only shortens how long
        its ring stays held when the coordinator recovers before the TTL. It never
        renews, so it ends by the lease's own ``expires_at`` at the latest, and it
        stops as soon as the record no longer holds exactly ``expected``. With
        ``round5`` the fence is the Round 5 cleanup lease, on its own ring.
        """

        store = self._round5_cleanup_store() if round5 else self._lease_store_for_record(record)
        working = expected
        async with record.lease_lock:
            local = record.round5_lease if round5 else record.lease
            if (
                not round5
                and local is not None
                and working.phase == "checking"
                and local.phase == "armed"
                and self._same_lease_authority(local, working)
                and record.bell_committed_at is None
            ):
                working = local
        if local is None or not self._same_exact_lease(local, working):
            return
        context = (
            "round5" if round5 else "main",
            record.snapshot.id,
            local.phase,
            local.lease_id,
            local.fencing_token,
            local.expires_at.isoformat(),
        )
        until = time.monotonic() + max(
            0.0, (local.expires_at - datetime.now(UTC)).total_seconds()
        )
        logger.warning(
            "Retrying terminal ring release in the background "
            "ring=%s session=%s phase=%s lease_id=%s fence=%s expires_at=%s",
            *context,
        )
        backoff = 0.01
        outcome: str | None = None
        while outcome is None:
            async with record.lease_lock:
                local = record.round5_lease if round5 else record.lease
            if (
                not round5
                and local is not None
                and working.phase == "checking"
                and local.phase == "armed"
                and self._same_lease_authority(local, working)
                and record.bell_committed_at is None
            ):
                working = local
            outcome = await self._release_exact_fence_once(
                record, store, working, until, round5=round5
            )
            if outcome is None:
                left = until - time.monotonic()
                if left <= 0:
                    outcome = "unknown"
                else:
                    await asyncio.sleep(min(backoff, left))
                    backoff = min(
                        backoff * 2,
                        max(0.01, self._terminal_release_backoff_cap),
                    )
        if round5 and outcome == "released":
            self._round5_durable_cleanup_blocked = False
        if outcome in {"taken", "unknown"}:
            # Expired, or named as another's: either way no longer this record's.
            async with record.lease_lock:
                if round5:
                    local = record.round5_lease
                    if local is not None and self._same_exact_lease(local, working):
                        record.round5_lease = None
                else:
                    local = record.lease
                    if (
                        local is not None
                        and self._same_exact_lease(local, working)
                        and not self._terminal_release_owns(record, local)
                    ):
                        self._lose_ring_lease(record)
        log = logger.info if outcome in {"released", "lost"} else logger.warning
        log(
            "Background terminal ring release ended outcome=%s "
            "ring=%s session=%s phase=%s lease_id=%s fence=%s expires_at=%s",
            outcome,
            *context,
        )

    async def _hold_bout_during_reset(
        self,
        record: SessionRecord,
        operation: Awaitable[None],
        held: BoutLease | None,
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
                released = cleanup_ready and await self._release_bout(record, held)
                if not released:
                    await self._auto_cleanup_owned_artifacts(
                        record,
                        recovery=mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT,
                        owned=held,
                    )
            elif mode != ResetMode.RETURN_TO_IDLE or cleanup_ready:
                await self._release_bout(record, held)

    async def _transition_bout_to_cleanup(
        self,
        record: SessionRecord,
        session_state: SessionState,
        *,
        expected_phase: str = "run_committed",
        expected_lease: BoutLease | None = None,
        expected_round5_lease: BoutLease | None = None,
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
            if expected_lease is not None and not self._same_exact_lease(
                lease,
                expected_lease,
            ):
                raise InvalidStateError("RING LEASE CHANGED · CLEANUP REMAINS FENCED")
            if expected_round5_lease is not None:
                round5_lease = record.round5_lease
                if round5_lease is None or not self._same_exact_lease(
                    round5_lease,
                    expected_round5_lease,
                ):
                    raise InvalidStateError(
                        "ROUND 5 ARTIFACT LEASE CHANGED · CLEANUP REMAINS FENCED"
                    )
            try:
                sent = time.monotonic()
                cooldown = await self._lease_store_for_record(record).transition(
                    lease,
                    operator=record.operator or lease.operator,
                    expected_phase=expected_phase,
                    phase="cooldown",
                    session_state=session_state,
                    ttl=timedelta(seconds=cleanup_ttl_seconds),
                )
            except LeaseLostError as exc:
                raise InvalidStateError("RING LEASE CHANGED · CLEANUP REMAINS FENCED") from exc
            self._set_lease(record, cooldown, sent)
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
        if record.snapshot.round.id == RoundId.PUT_MODEL_SCORE_IN_APP:
            self._round4_settlement = record.settlement_task
        elif record.snapshot.round.id == RoundId.ANALYZE_LIVE_ORDERS:
            self._round6_settlement = record.settlement_task

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
                        "%s settlement attempt %d/%d failed session=%s diagnosis=%s",
                        label,
                        attempt,
                        self._settlement_attempts,
                        record.snapshot.id,
                        operator_diagnosis(exc),
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
            self._auto_cleanup_owned_artifacts(record, recovery=recovery, owned=record.lease),
            name=f"auto-artifact-cleanup-{record.snapshot.id}",
        )

    async def _auto_cleanup_owned_artifacts(
        self,
        record: SessionRecord,
        *,
        recovery: bool,
        owned: BoutLease | None = None,
    ) -> None:
        task = asyncio.current_task()
        delay = max(0.1, self._cleanup_retry_initial)
        maximum_delay = max(delay, self._cleanup_retry_max)
        cleanup_complete = False
        try:
            # Let the verified/failed receipt event reach the room first.
            await asyncio.sleep(0)
            while not self._closed:
                if self._lease_moved_on(record, owned):
                    return
                if not cleanup_complete:
                    if recovery:
                        if record.recovery_engine is None:
                            return
                    operation = (
                        self._reset_recovery(record)
                        if recovery
                        else self._reset_safe_change(record)
                    )
                    await operation
                    if self._lease_moved_on(record, owned):
                        return
                    async with record.lock:
                        cooldown = record.snapshot.cooldown
                        cleanup_complete = bool(
                            cooldown is not None and cooldown.state == CooldownState.READY
                        )
                if cleanup_complete and await self._release_bout(record, owned):
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
        cooldown = record.snapshot.cooldown
        if self._closed or record.live_targets is None or cooldown is None:
            return
        current = record.cooldown_task
        if current is not None and not current.done():
            if current is not record.idle_watch_task:
                return
            # Each caller has just ended a bout under a fresh cooldown, so a watch still
            # running is an earlier cooldown's, which it can no longer act on.
            current.cancel()
        record.cooldown_task = record.idle_watch_task = asyncio.create_task(
            self._run_automatic_round_one_cooldown(record, CooldownWatch(cooldown, record.lease)),
            name=f"auto-idle-watch-{record.snapshot.id}",
        )

    async def _run_automatic_round_one_cooldown(
        self,
        record: SessionRecord,
        watch: CooldownWatch,
    ) -> None:
        """Watch Round 1 return to idle, then release the lease the watch began under.

        The watch acts on no other cooldown and no other lease. Once the record shows
        another cooldown or holds another lease, the card may have been armed again, and
        what it shows and holds is the newer bout's.
        """

        task = asyncio.current_task()
        try:
            await asyncio.sleep(0)
            await self._monitor_cooldown(record, watch)
            async with record.lock:
                async with record.lease_lock:
                    held = record.lease
                ready = bool(
                    self._watch_is_current(record, watch)
                    and watch.cooldown.state == CooldownState.READY
                )
            if ready and await self._settle_terminal_release(record, held) != "released":
                async with record.lock:
                    if self._watch_is_current(record, watch):
                        watch.cooldown.state = CooldownState.FAILED
                        watch.cooldown.failure = (
                            "Return to idle was verified, but ring release could not be confirmed."
                        )
                        record.snapshot.updated_at = datetime.now(UTC)
        except asyncio.CancelledError:
            raise
        finally:
            if record.cooldown_task is task:
                record.cooldown_task = None
            if record.idle_watch_task is task:
                record.idle_watch_task = None

    def _watch_is_current(self, record: SessionRecord, watch: CooldownWatch) -> bool:
        """Whether ``watch`` may still act on ``record``; called under ``record.lock``."""

        if record.snapshot.cooldown is not watch.cooldown:
            return False
        lease = record.lease
        return lease is None or (
            watch.owned is not None and self._same_exact_lease(lease, watch.owned)
        )

    @staticmethod
    def _cancel_idle_watch(record: SessionRecord) -> None:
        """Stop the record's idle watch, its cooldown being replaced; held under ``record.lock``.

        Not awaited, so the lock is not held across its unwinding. A watch whose cooldown
        is gone acts on nothing in any case; this only stops it polling sooner.
        """

        watch = record.idle_watch_task
        if watch is not None and not watch.done():
            watch.cancel()

    def _cancel_armed_expiry(self, record: SessionRecord, *, force: bool = False) -> None:
        task = record.armed_expiry_task
        if record.armed_release_pending and not force:
            # The window already expired and its release is being settled; only
            # the timer was this call's to cancel.
            return
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
        await self._abandon_armed_bout(record, armed_at_monotonic)

    async def _abandon_armed_bout(
        self,
        record: SessionRecord,
        armed_at_monotonic: float,
        *,
        message: str | None = None,
        event: str = "session_failed",
        release_reserved: bool = False,
    ) -> bool:
        """Release an armed fight card that never rang.

        Reached when the armed window expires, and when its owner cancels the card, so
        that "prepare, then change the matchup" converges through the same no-bell
        cleanup the expiry already uses instead of holding the ring for the rest of the
        window. An owner's cancel of any round but Round 5 carries its own ``message``
        and publishes ``session_cancelled``, which the browser takes back to the card
        without an error. Returns False,
        changing nothing, when the session already moved on (rang, cancelled, or
        re-armed).
        """
        message = message or (
            "Fight card expired before the bell. The ring was released automatically; "
            "prepare it again."
        )
        is_round_five = False
        round_five_operator: BoutOperator | None = None
        owns_reservation = False
        arm_bout_id: str | None = None
        async with record.lock:
            if (
                record.snapshot.state != SessionState.ARMED
                or record.armed_at_monotonic != armed_at_monotonic
                or record.bell_committed_at is not None
            ):
                if (
                    release_reserved
                    and record.armed_expiry_task is asyncio.current_task()
                ):
                    record.armed_release_pending = False
                    record.armed_expiry_task = None
                return False
            handoff = record.bell_handoff_task
            if (
                handoff is not None
                and not handoff.done()
                and record.bell_commit_attempted_bout_id
                == record.snapshot.receipt_bout_id
                and asyncio.current_task() is not record.bell_convergence_task
            ):
                # A bell for this attempt owns the ARMED/run_committed
                # ambiguity. Expiry or owner-cancel queued behind it must not
                # race a stale ARMED release and erase the local authority that
                # convergence needs to adopt the durable committed phase.
                if (
                    release_reserved
                    and record.armed_expiry_task is asyncio.current_task()
                ):
                    # Owner-cancel reserved the no-bell slot before discovering
                    # the in-flight bell. Hand ownership back to convergence;
                    # otherwise its later durable-ARMED path sees a permanently
                    # reserved slot and neither path can settle the card.
                    record.armed_release_pending = False
                    record.armed_expiry_task = None
                return False
            if release_reserved:
                if not record.armed_release_pending:
                    return False
            else:
                if record.armed_release_pending:
                    if asyncio.current_task() is not record.bell_convergence_task:
                        return False
                    # Convergence can arrive before the owner-cancel task has
                    # observed the in-flight handoff and relinquished its
                    # reservation. Transfer the slot atomically; that older
                    # task will then fail its ownership check and return.
                    record.armed_expiry_task = asyncio.current_task()
                    owns_reservation = True
                    arm_bout_id = record.snapshot.receipt_bout_id
                else:
                    # Atomically reserves every no-bell path before any coordinator or
                    # provider await. `start_run` checks this under the same card lock.
                    record.armed_release_pending = True
                    owns_reservation = True
                    arm_bout_id = record.snapshot.receipt_bout_id
            is_round_five = (
                record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE
            )
            if is_round_five:
                round_five_operator = record.operator
            else:
                async with record.lease_lock:
                    armed_lease = record.lease
                    if (
                        armed_lease is not None
                        and (
                            armed_lease.session_id != record.snapshot.id
                            or armed_lease.phase not in {"checking", "armed"}
                        )
                    ):
                        record.armed_release_pending = False
                        return False

        if is_round_five:
            async def bounded_step(label: str, operation):
                try:
                    async with asyncio.timeout(
                        max(0.001, self._terminal_release_call_timeout)
                    ):
                        return await operation
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(
                        "Round 5 no-bell %s did not complete session=%s diagnostic=%s",
                        label,
                        record.snapshot.id,
                        _redacted_exception_chain(exc),
                    )
                    return None

            try:
                # A no-bell abandon ALWAYS enters CLEANING. All external work is
                # outside `record.lock`, so reads/SSE remain prompt while it stalls.
                if self._round5_warm_coordinator is None:
                    await bounded_step(
                        "provider cleanup",
                        self._cleanup_connection_spike(record),
                    )
                if round_five_operator is not None:
                    await bounded_step(
                        "cleanup-fence retention",
                        self._retain_connection_spike_cleanup_lease(
                            record,
                            round_five_operator,
                        ),
                    )
                await bounded_step(
                    "durable cleanup transition",
                    self._begin_round5_warm_cleanup(record),
                )
            except asyncio.CancelledError:
                # Cancellation means the I/O answer is unknown, never that cleanup
                # is no longer owed. A tracked child records the debt before this
                # task propagates cancellation.
                finalizer = asyncio.create_task(
                    self._finalize_round5_no_bell_abandon(
                        record,
                        armed_at_monotonic,
                    ),
                    name=f"round5-cancelled-no-bell-{record.snapshot.id}",
                )
                record.armed_expiry_task = finalizer
                try:
                    await asyncio.shield(finalizer)
                finally:
                    raise
            return await self._finalize_round5_no_bell_abandon(
                record,
                armed_at_monotonic,
            )

        try:
            if await self._settle_terminal_release(record, armed_lease) == "lost":
                return False
            async with record.lock:
                if (
                    record.snapshot.state != SessionState.ARMED
                    or record.armed_at_monotonic != armed_at_monotonic
                ):
                    return False
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = message
                record.snapshot.remembered_result = None
                record.snapshot.updated_at = datetime.now(UTC)
                record.armed_at_monotonic = None
                snapshot = record.snapshot.model_copy(deep=True)
        finally:
            async with record.lock:
                owns_slot = record.armed_expiry_task is asyncio.current_task()
                owns_same_bout = (
                    owns_reservation
                    and record.snapshot.receipt_bout_id == arm_bout_id
                )
                if owns_slot or owns_same_bout:
                    expiry_task = record.armed_expiry_task
                    if (
                        expiry_task is not None
                        and expiry_task is not asyncio.current_task()
                        and not expiry_task.done()
                    ):
                        expiry_task.cancel()
                    record.armed_release_pending = False
                    record.armed_expiry_task = None
        await record.event_log.publish(
            event,
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        return True

    async def _finalize_round5_no_bell_abandon(
        self,
        record: SessionRecord,
        armed_at_monotonic: float,
    ) -> bool:
        """Record one terminal Round 5 cleanup debt after no-bell I/O stops."""

        task = asyncio.current_task()
        message = _ROUND_FIVE_CLEANUP_PENDING
        try:
            async with record.lock:
                if (
                    record.snapshot.state != SessionState.ARMED
                    or record.armed_at_monotonic != armed_at_monotonic
                ):
                    return False
                record.snapshot.metrics = []
                record.snapshot.comparison = None
                setup = record.snapshot.round5_setup
                if setup is not None:
                    setup.state = RoundFiveSetupState.CLEANUP_FAILED
                    setup.cleanup_retryable = True
                    setup.downstream_validated = False
                    setup.failure = "Automatic cleanup verification is in progress"
                self._note_round5_proxy_at_risk(record, force=True)
                self._schedule_connection_spike_cleanup_convergence(record)
                record.snapshot.state = SessionState.FAILED
                record.snapshot.failure = message
                record.snapshot.remembered_result = None
                record.snapshot.updated_at = datetime.now(UTC)
                record.armed_at_monotonic = None
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
                receipt_bout_id=snapshot.receipt_bout_id,
            )
            # Retryable cleanup always has both an owed notice and a live
            # convergence worker, even when the initiating task was cancelled.
            await self._mark_connection_spike_cleanup_pending(record)
            return True
        finally:
            record.armed_release_pending = False
            if record.armed_expiry_task is task:
                record.armed_expiry_task = None

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

        * The chain reaches the operator log and cached operator refusal.
        * The failure banner gets fixed audience-safe wording.
        * The chain reaches the log, with a traceback, for the case where the
          readable form was cut short.
        * An authorization refusal is remembered, so `/api/catalog` stops
          offering a round Databricks has already turned down. The round-select
          screen is what a room full of customers looks at, and a round shown
          green that dies when the bell rings is the worst failure this project
          has.
        """

        message = arm_failure_message(summary, error)
        statement_failure = statement_execution_failure(error)
        logger.error(
            "Round %d arm failed session=%s round=%s diagnosis=%s "
            "provider_category=%s error_code=%s sqlstate=%s",
            round_number,
            record.snapshot.id,
            record.snapshot.round.id.value,
            operator_diagnosis(error),
            statement_failure.provider_category if statement_failure is not None else "",
            statement_failure.error_code if statement_failure is not None else "",
            statement_failure.sql_state if statement_failure is not None else "",
            exc_info=True,
        )
        if authorization_refusal(error) is not None:
            self._grant_refusals[record.snapshot.round.id] = grant_refusal(
                operator_diagnosis(error)
            )
        if storage_access_refusal(error) is not None:
            refusal = storage_refusal(operator_diagnosis(error))
            # Both live Delta rounds share this installation's managed-storage
            # bucket. A provider-level 403 against either one is evidence that
            # the other must not be offered until a fresh arm proves access.
            self._storage_refusals[RoundId.PUT_MODEL_SCORE_IN_APP] = refusal
            self._storage_refusals[RoundId.ANALYZE_LIVE_ORDERS] = refusal
        return message

    async def _fail_arm(
        self,
        record: SessionRecord,
        summary: str,
        error: BaseException,
        *,
        round_number: int,
        arm_bout_id: str,
    ) -> None:
        """Fail an arm without discarding what refused it."""

        if await self._arm_attempt_was_cancelled(record, arm_bout_id):
            return
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

        statement_failure = statement_execution_failure(error)
        logger.error(
            "Round %d bout failed session=%s round=%s diagnosis=%s "
            "provider_category=%s error_code=%s sqlstate=%s",
            round_number,
            record.snapshot.id,
            record.snapshot.round.id.value,
            operator_diagnosis(error),
            statement_failure.provider_category if statement_failure is not None else "",
            statement_failure.error_code if statement_failure is not None else "",
            statement_failure.sql_state if statement_failure is not None else "",
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

    @staticmethod
    def _log_bound_stoppage(
        record: SessionRecord, winner: str, stopped: str, *, round_number: int
    ) -> None:
        """A bout decided at a lane's bound is a result, but the operator needs to see why."""

        lane = record.snapshot.lanes[stopped]
        logger.warning(
            "Round %d lane was still running at its bound session=%s round=%s lane=%s "
            "ran=%.0fs winner=%s",
            round_number,
            record.snapshot.id,
            record.snapshot.round.id.value,
            stopped,
            (lane.elapsed_ms or 0.0) / 1000,
            winner,
        )

    async def _arm(self, record: SessionRecord, arm_bout_id: str) -> None:
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
                    await self._publish_armed_if_current(
                        record,
                        arm_bout_id=arm_bout_id,
                        armed_at_monotonic=armed_at_monotonic,
                        snapshot=snapshot,
                        payload={
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
            await self._fail_arm(
                record,
                "The start state could not be verified.",
                exc,
                round_number=1,
                arm_bout_id=arm_bout_id,
            )
        except Exception as exc:
            await self._fail_arm(
                record,
                "The start state could not be verified.",
                exc,
                round_number=1,
                arm_bout_id=arm_bout_id,
            )

    async def _arm_safe_change(
        self,
        record: SessionRecord,
        arm_bout_id: str,
    ) -> None:
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
            evidence = {lane_id: dict(lane.evidence) for lane_id, lane in arm.lanes.items()}
            await self._publish_armed_if_current(
                record,
                arm_bout_id=arm_bout_id,
                armed_at_monotonic=armed_at_monotonic,
                snapshot=snapshot,
                payload={
                    "state": SessionState.ARMED,
                    "evidence": evidence,
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except SafeChangeNotArmedError as exc:
            await self._fail_arm(
                record,
                "The isolated-change start state could not be verified.",
                exc,
                round_number=2,
                arm_bout_id=arm_bout_id,
            )
        except Exception as exc:
            await self._fail_arm(
                record,
                "The isolated-change start state could not be verified.",
                exc,
                round_number=2,
                arm_bout_id=arm_bout_id,
            )

    async def _arm_recovery(
        self,
        record: SessionRecord,
        arm_bout_id: str,
    ) -> None:
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
            await self._publish_armed_if_current(
                record,
                arm_bout_id=arm_bout_id,
                armed_at_monotonic=armed_at_monotonic,
                snapshot=snapshot,
                payload={
                    "state": SessionState.ARMED,
                    "evidence": {
                        lane_id: dict(lane.evidence) for lane_id, lane in arm.lanes.items()
                    },
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except RecoveryNotArmedError as exc:
            await self._fail_arm(
                record,
                "The recovery start state could not be verified.",
                exc,
                round_number=3,
                arm_bout_id=arm_bout_id,
            )
        except Exception as exc:
            await self._fail_arm(
                record,
                "The recovery start state could not be verified.",
                exc,
                round_number=3,
                arm_bout_id=arm_bout_id,
            )

    async def _arm_model_score(
        self,
        record: SessionRecord,
        arm_bout_id: str,
    ) -> None:
        """Round 4's Prepare: both lanes parked and every destination at its baseline.

        Starts nothing. Both integrations stay parked until the bell, because
        each lane's clock contains its own start from parked (the v1.1 design,
        section 2). A park still in progress is waited out, visibly.
        """

        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        factory = self._model_score_factory
        if factory is None:
            await self._fail(record, "Round 4 live adapter is not configured.")
            return
        try:
            # Off the loop: the live engine builds a workspace client and an AWS session.
            engine = await asyncio.to_thread(factory, record.snapshot.competitor.id)
            record.model_score_engine = engine

            async def notify(status: str) -> None:
                async with record.lock:
                    for lane in record.snapshot.lanes.values():
                        if lane.state != LaneState.NOT_SUPPORTED:
                            lane.status = status
                            lane.activity = LaneActivity(phase=Round4Phase.PREPARING)
                    record.snapshot.updated_at = datetime.now(UTC)
                await record.event_log.publish(
                    "arm_waiting",
                    {
                        "state": SessionState.CHECKING,
                        "status": status,
                    },
                )

            await self._await_prior_round4_settlement(record, notify)
            arm = await engine.prepare(notify)
            loop = asyncio.get_running_loop()
            async with record.lock:
                armed_at = datetime.now(UTC)
                record.model_score_arm = arm
                record.snapshot.state = SessionState.ARMED
                record.snapshot.armed_at = armed_at
                record.snapshot.armed_expires_at = armed_at + timedelta(seconds=self._armed_ttl)
                record.snapshot.updated_at = armed_at
                record.armed_at_monotonic = loop.time()
                baseline_evidence = {
                    "primary_key": arm.baseline.entity_id,
                    "score": arm.baseline.score,
                    "model_version": arm.baseline.model_version,
                    "proof_nonce": arm.baseline.proof_nonce,
                    "delta_version": arm.source_version,
                }
                for lane_id, lane in record.snapshot.lanes.items():
                    if lane_id in arm.lanes:
                        lane.state = LaneState.SEALED
                        lane.status = "Parked, with the baseline verified in its destination"
                        lane.error = None
                        lane.activity = LaneActivity(phase=Round4Phase.ARMED)
                        lane.evidence = dict(baseline_evidence)
                    else:
                        lane.state = LaneState.NOT_SUPPORTED
                        lane.status = "AWS lane not installed on this installation"
                        lane.error = None
                        lane.activity = LaneActivity(phase="not_supported")
                        lane.evidence = {"unsupported_reason": _ROUND_FOUR_UNSUPPORTED_REASON}
                # The receipt built at create could not know whether this installation
                # races Round 4's AWS lane; the engine just said.
                record.snapshot.cost_receipt = build_cost_receipt(
                    record.snapshot.round.id,
                    record.snapshot.competitor.id,
                    round4_aws_lane=COMPETITOR_LANE in arm.lanes,
                )
                snapshot = record.snapshot.model_copy(deep=True)
                armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            await self._publish_armed_if_current(
                record,
                arm_bout_id=arm_bout_id,
                armed_at_monotonic=armed_at_monotonic,
                snapshot=snapshot,
                payload={
                    "state": SessionState.ARMED,
                    "evidence": {
                        lane_id: lane.evidence for lane_id, lane in snapshot.lanes.items()
                    },
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except (ModelScoreError, Round4RaceError) as exc:
            await self._fail_arm(
                record,
                "Round 4 could not confirm both lanes parked at their baseline.",
                exc,
                round_number=4,
                arm_bout_id=arm_bout_id,
            )
        except Exception as exc:
            await self._fail_arm(
                record,
                "Round 4 could not confirm both lanes parked at their baseline.",
                exc,
                round_number=4,
                arm_bout_id=arm_bout_id,
            )

    async def _await_prior_round4_settlement(
        self,
        record: SessionRecord,
        notify: Callable[[str], Awaitable[None]],
    ) -> None:
        """Let the previous Round 4 bout finish settling before this Prepare looks.

        A finished bout releases the ring and then, in the background, restores
        the source row, waits for both lanes to carry it, and parks both. A
        Prepare pressed inside that window (a quick Ring Again) waits for it here
        rather than meeting a lane half-parked.
        """

        prior = self._round4_settlement
        if prior is None or prior.done() or prior is record.settlement_task:
            return
        await notify("Putting the previous bout's row back and parking both lanes first")
        try:
            async with asyncio.timeout(_ROUND4_SETTLEMENT_WAIT_SECONDS):
                await asyncio.shield(prior)
        except TimeoutError as exc:
            raise InvalidStateError(
                "The previous Round 4 bout was still putting its row back. Prepare again "
                "in a moment."
            ) from exc

    @staticmethod
    def _round5_arm_has_started_mutation(engine: object | None) -> bool:
        if engine is None:
            return False
        bindings = getattr(engine, "_resident_bindings", None)
        return bool(isinstance(bindings, dict) and bindings)

    async def _verify_round5_start_state(self, record: SessionRecord) -> None:
        """Refuse Round 5 CHECKING until the claimed fence/capsule still match."""

        factory = self._connection_spike_factory
        capsule = record.round5_launch_capsule
        warm_slot = record.round5_warm_slot
        if factory is None and capsule is None:
            raise InvalidStateError("Round 5 live adapter is not configured.")
        if capsule is not None and warm_slot is not None:
            variant = (
                Round5Variant.AURORA
                if record.snapshot.competitor.id == CompetitorId.AURORA_SERVERLESS_V2
                else Round5Variant.RDS
            )
            engine = capsule.variant_contexts[variant]
        else:
            assert factory is not None
            engine = factory(record.snapshot.competitor.id)
        record.connection_spike_engine = engine
        lease = record.round5_lease
        if lease is None:
            raise InvalidStateError("Round 5 automatic warm context is unavailable")
        claim = warm_slot.claim if warm_slot is not None else None
        bind_claim = getattr(engine, "bind_claim", None)
        if claim is not None and callable(bind_claim):
            bind_claim(claim)
        verify = getattr(engine, "verify_start_state", None)
        if callable(verify):
            await verify(record.snapshot.id, lease.fencing_token)
            return
        # Compatibility engines expose only prepare(). Keep them under the same
        # no-ghost contract instead of leaving a second caller family that can
        # publish CHECKING before discovering its start fence is stale.
        if self._round_five_has_timed_setup(engine):
            prepare = getattr(engine, "prepare", None)
            if callable(prepare):
                await prepare(record.snapshot.id, lease.fencing_token)

    async def _rollback_unstarted_round5_arm(self, record: SessionRecord) -> bool:
        """Drop a failed prepare that never started AWS or resident work.

        Returns True when the claim was returned to READY and both rings were
        released. Returns False when cleanup still owns the bout.
        """

        engine = record.connection_spike_engine
        if self._round5_arm_has_started_mutation(engine):
            return False
        coordinator = self._round5_warm_coordinator
        warm_slot = record.round5_warm_slot
        claim = warm_slot.claim if warm_slot is not None else None
        if coordinator is not None and claim is not None:
            returned = getattr(coordinator, "return_unstarted_claim", None)
            if not callable(returned):
                return False
            main, cleanup = record.lease, record.round5_lease
            try:
                record.round5_warm_slot = await returned(
                    claim.claim_id,
                    main_store=self._lease_store_for_record(record),
                    main_lease=main,
                    cleanup_store=self._round5_cleanup_store(),
                    cleanup_lease=cleanup,
                )
            except Exception:
                logger.error(
                    "Round 5 unstarted claim could not return to READY",
                    exc_info=True,
                )
                return False
            await self._forget_rings_the_warm_store_returned(record, main, cleanup)
        await self._release_round5_lease(record)
        await self._release_bout(record)
        return True

    async def _arm_connection_spike(
        self,
        record: SessionRecord,
        arm_bout_id: str,
    ) -> None:
        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        factory = self._connection_spike_factory
        capsule = record.round5_launch_capsule
        warm_slot = record.round5_warm_slot
        if factory is None and capsule is None:
            await self._fail(record, "Round 5 live adapter is not configured.")
            return
        try:
            if record.connection_spike_engine is not None:
                engine = record.connection_spike_engine
            elif capsule is not None and warm_slot is not None:
                variant = (
                    Round5Variant.AURORA
                    if record.snapshot.competitor.id
                    == CompetitorId.AURORA_SERVERLESS_V2
                    else Round5Variant.RDS
                )
                engine = capsule.variant_contexts[variant]
            else:
                assert factory is not None
                engine = factory(record.snapshot.competitor.id)
            record.connection_spike_engine = engine
            has_timed_setup = self._round_five_has_timed_setup(engine)
            arm = None
            if not has_timed_setup:
                check = engine.check  # type: ignore[attr-defined]
                arm = await check()
            else:
                # Compatibility path until the installation-scoped warm slot is
                # injected. Both calls are mandatory: the bell has no provider,
                # discovery, credential, or capacity fallback.
                lease = record.round5_lease
                prepare = getattr(engine, "prepare", None)
                if lease is None:
                    raise InvalidStateError(
                        "Round 5 automatic warm context is unavailable"
                    )
                if capsule is not None:
                    if prepare is None:
                        raise InvalidStateError(
                            "Round 5 automatic warm context is unavailable"
                        )
                    claim = warm_slot.claim
                    bind_claim = getattr(engine, "bind_claim", None)
                    if claim is None or bind_claim is None:
                        raise InvalidStateError(
                            "Round 5 deterministic runner job claim is unavailable"
                        )
                    bind_claim(claim)
                    await prepare(record.snapshot.id, lease.fencing_token)
                    arm = getattr(engine, "_armed", None)
                    if arm is None:
                        raise InvalidStateError(
                            "Round 5 warm capacity receipt is unavailable"
                        )
                else:
                    if prepare is not None:
                        await prepare(record.snapshot.id, lease.fencing_token)
                    check = getattr(engine, "check", None) or getattr(
                        engine,
                        "preflight",
                        None,
                    )
                    if check is None:
                        raise InvalidStateError(
                            "Round 5 warm capacity receipt is unavailable"
                        )
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
                    timeout="20s connect · 600s run",
                    protocol=ROUND5_FANIN_PROTOCOL,
                    target_clients_per_lane=TARGET_CLIENTS_PER_LANE,
                    hold_seconds=30,
                    sampled_queries_per_lane=64,
                    max_retries=ROUND5_MAX_RETRIES,
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
            await self._publish_armed_if_current(
                record,
                arm_bout_id=arm_bout_id,
                armed_at_monotonic=armed_at_monotonic,
                snapshot=snapshot,
                payload={
                    "state": SessionState.ARMED,
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except Exception as exc:
            if await self._arm_attempt_was_cancelled(record, arm_bout_id):
                return
            message = self._arm_refusal(
                record,
                "The Round 5 start state could not be verified.",
                exc,
                round_number=5,
            )
            coordinator = self._round5_warm_coordinator
            warm_slot = record.round5_warm_slot
            claim = warm_slot.claim if warm_slot is not None else None
            engine = record.connection_spike_engine
            if coordinator is not None and claim is not None:
                # Arm failed AFTER prepare staged the Lakebase resident but before
                # (or during) _mark_bout_armed. The manager's coordinator-path
                # cleanup is a no-op that must NOT be treated as external cleanup
                # proof: doing so would release the leases and re-warm over the
                # orphaned staged resident (warm_baseline_unexpected). Instead adopt
                # the EXACT engine holding the staged resident (so the provider --
                # the sole janitor -- cancels that resident), then drive durable
                # CLEANING + convergence through the failure handoff. Leases are
                # retained until convergence proves cleanup; nothing is released
                # from the no-op here.
                adopt = getattr(coordinator, "adopt_claimed_engine", None)
                if callable(adopt) and engine is not None:
                    adopt(claim.claim_id, engine)
                await self._finish_connection_spike_failure(
                    record,
                    message,
                )
            else:
                cleanup_ok = await self._cleanup_connection_spike(record)
                if cleanup_ok:
                    await self._fail(record, message)
                else:
                    await self._finish_connection_spike_failure(
                        record,
                        message,
                    )

    async def _arm_live_orders(
        self,
        record: SessionRecord,
        arm_bout_id: str,
    ) -> None:
        """Round 6's Prepare: the AWS pipeline parked and every source at its baseline.

        Starts nothing. DMS and Glue stay parked until the bell, because the AWS lane's
        clock contains its own start from parked; Lakebase's feed is built in and always
        on (the v1.1 design, section 2). A park still in progress is waited out, visibly.
        """

        await record.event_log.publish("arm_started", {"state": SessionState.CHECKING})
        factory = self._live_orders_factory
        if factory is None:
            await self._fail(record, "Round 6 live adapter is not configured.")
            return
        try:
            # Off the loop: the live engine builds a workspace client and an AWS session.
            engine = await asyncio.to_thread(factory, record.snapshot.competitor.id)
            record.live_orders_engine = engine

            async def notify(status: str) -> None:
                async with record.lock:
                    for lane in record.snapshot.lanes.values():
                        if lane.state != LaneState.NOT_SUPPORTED:
                            lane.status = status
                            lane.activity = LaneActivity(phase=Round6Phase.PREPARING)
                    record.snapshot.updated_at = datetime.now(UTC)
                await record.event_log.publish(
                    "arm_waiting",
                    {"state": SessionState.CHECKING, "status": status},
                )

            await self._await_prior_round6_settlement(record, notify)
            arm = await engine.prepare(notify)
            loop = asyncio.get_running_loop()
            async with record.lock:
                armed_at = datetime.now(UTC)
                record.live_orders_arm = arm
                record.snapshot.state = SessionState.ARMED
                record.snapshot.armed_at = armed_at
                record.snapshot.armed_expires_at = armed_at + timedelta(seconds=self._armed_ttl)
                record.snapshot.updated_at = armed_at
                record.armed_at_monotonic = loop.time()
                baseline = {
                    "order_id": arm.baseline.order_id,
                    "total_cents": arm.baseline.total_cents,
                    "proof_nonce": arm.baseline.proof_nonce,
                }
                for lane_id, lane in record.snapshot.lanes.items():
                    if lane_id in arm.lanes:
                        lane.state = LaneState.SEALED
                        lane.status = (
                            "Change feed streaming, with the baseline in its source"
                            if lane_id == LAKEBASE_LANE
                            else "Parked, with the baseline in its source"
                        )
                        lane.error = None
                        lane.activity = LaneActivity(phase=Round6Phase.ARMED)
                        lane.evidence = dict(baseline)
                    else:
                        lane.state = LaneState.NOT_SUPPORTED
                        lane.status = "AWS lane not installed on this installation"
                        lane.error = None
                        lane.activity = LaneActivity(phase="not_supported")
                        lane.evidence = {"unsupported_reason": _ROUND_SIX_UNSUPPORTED_REASON}
                # As in Round 4: the receipt built at create could not know whether this
                # installation races Round 6's AWS lane; the engine just said.
                record.snapshot.cost_receipt = build_cost_receipt(
                    record.snapshot.round.id,
                    record.snapshot.competitor.id,
                    round6_aws_lane=COMPETITOR_LANE in arm.lanes,
                )
                snapshot = record.snapshot.model_copy(deep=True)
                armed_at_monotonic = record.armed_at_monotonic
            assert armed_at_monotonic is not None
            await self._publish_armed_if_current(
                record,
                arm_bout_id=arm_bout_id,
                armed_at_monotonic=armed_at_monotonic,
                snapshot=snapshot,
                payload={
                    "state": SessionState.ARMED,
                    "session": snapshot.model_dump(mode="json"),
                },
            )
        except Exception as exc:
            await self._fail_arm(
                record,
                "Round 6 could not confirm its pipelines at rest and its sources at baseline.",
                exc,
                round_number=6,
                arm_bout_id=arm_bout_id,
            )

    async def _await_prior_round6_settlement(
        self,
        record: SessionRecord,
        notify: Callable[[str], Awaitable[None]],
    ) -> None:
        """Let the previous Round 6 bout finish settling before this Prepare looks.

        A finished bout releases the ring and then, in the background, removes its orders and
        parks DMS and Glue. A Prepare pressed inside that window waits for it here rather than
        meeting a pipeline half-parked.
        """

        prior = self._round6_settlement
        if prior is None or prior.done() or prior is record.settlement_task:
            return
        await notify("Removing the previous bout's orders and parking its pipeline first")
        try:
            async with asyncio.timeout(_ROUND6_SETTLEMENT_WAIT_SECONDS):
                await asyncio.shield(prior)
        except TimeoutError as exc:
            raise InvalidStateError(
                "The previous Round 6 bout was still settling. Prepare again in a moment."
            ) from exc

    async def _run_connection_spike(self, record: SessionRecord) -> None:
        engine = record.connection_spike_engine
        arm = record.connection_spike_arm
        has_timed_setup = self._round_five_has_timed_setup(engine)
        setup_operation = getattr(engine, "setup", None) if has_timed_setup else None
        if engine is None or (arm is None and not has_timed_setup):
            await self._fail(record, "The Round 5 pooled-path proof must be armed again.")
            return
        async with record.lock:
            if record.round5_bell_context is None:
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
                record.round5_progress_payload_digests.clear()
                record.round5_terminal_lane_evidence.clear()
                record.round5_ingestion_closed = False
                record.round5_callback_epoch += 1
                record.round5_terminal_published = False
                for lane in record.snapshot.lanes.values():
                    lane.state = LaneState.CONNECTING
                    lane.status = "Preparing a clean per-bout pooling setup"
                    lane.attempts = 0
                    lane.successes = 0
                    lane.errors = 0
                    lane.p99_ms = None
                    lane.error = None
                    lane.activity = LaneActivity(phase="setup")
            callback_runtime = record.snapshot.round5_runtime
            callback_bell_id = (
                callback_runtime.bell_id if callback_runtime is not None else None
            )
            callback_generation = (
                callback_runtime.warm_generation
                if callback_runtime is not None
                else None
            )
            callback_epoch = record.round5_callback_epoch
            running_snapshot = self._public_snapshot_locked(record)
        run_started_payload = {
            "state": SessionState.RUNNING,
            "lanes": ["lakebase", "competitor"],
            "session": running_snapshot.model_dump(mode="json"),
        }
        publication_ready = asyncio.Event()

        def ingestion_is_open() -> bool:
            runtime = record.snapshot.round5_runtime
            return (
                not record.round5_ingestion_closed
                and record.snapshot.state == SessionState.RUNNING
                and record.round5_callback_epoch == callback_epoch
                and (
                    runtime is None
                    or (
                        runtime.bell_id == callback_bell_id
                        and runtime.warm_generation == callback_generation
                        and runtime.state == "running"
                    )
                )
            )

        async def publish_lane_snapshots(
            lane_ids: tuple[str, ...],
        ) -> None:
            if not record.lock.locked():
                raise RuntimeError("Round 5 publication requires the record lock")
            if not ingestion_is_open():
                return
            snapshot = self._public_snapshot_locked(record)
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
                        "session": serialized,
                    },
                )

        async def on_setup_progress(progress: object) -> None:
            await publication_ready.wait()
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
                if not ingestion_is_open():
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
                    runtime = record.snapshot.round5_runtime
                    if runtime is not None:
                        runtime_lane = runtime.lanes[affected_lane_id]
                        runtime_lane.phase = (
                            "dispatching"
                            if reached_stop
                            else "provisioning_proxy"
                            if affected_lane_id == "competitor"
                            and phase
                            in {
                                "creating_proxy",
                                "freezing_proxy_settings",
                                "registering_proxy_target",
                                "waiting_for_proxy_target",
                                "resuming_database",
                            }
                            else "verifying_proxy"
                            if affected_lane_id == "competitor"
                            else "dispatching"
                        )
                        runtime_lane.status = status
                self._advance_round5_revision_locked(record)
                record.snapshot.updated_at = datetime.now(UTC)
                await publish_lane_snapshots(affected_lane_ids)

        async def on_lane_progress(progress: object) -> None:
            await publication_ready.wait()
            lane_id = str(self._round_five_value(progress, "lane_id", ""))
            if lane_id not in {"lakebase", "competitor"}:
                return
            phase_value = str(
                self._round_five_value(progress, "phase", "ramping")
            ).lower()
            phase = "holding" if phase_value == "holding" else "ramping"
            initiated = max(
                0,
                self._round_five_count(
                    self._round_five_value(
                        progress,
                        "initiated_clients",
                        self._round_five_value(
                            progress,
                            "authenticated_clients",
                            0,
                        ),
                    )
                ),
            )
            authenticated = max(
                0,
                self._round_five_count(
                    self._round_five_value(
                        progress,
                        "authenticated_clients",
                        0,
                    )
                ),
            )
            held = max(
                0,
                self._round_five_count(
                    self._round_five_value(progress, "held_clients", 0)
                ),
            )
            peak_held = max(
                held,
                self._round_five_count(
                    self._round_five_value(
                        progress,
                        "peak_held_clients",
                        held,
                    )
                ),
            )
            samples = max(
                0,
                self._round_five_count(
                    self._round_five_value(
                        progress,
                        "sampled_queries_succeeded",
                        0,
                    )
                ),
            )
            runner_elapsed = self._round_five_number(
                self._round_five_value(progress, "elapsed_ms")
            )
            target_elapsed = self._round_five_number(
                self._round_five_value(progress, "time_to_target_ms")
            )
            milestone = str(
                self._round_five_value(progress, "milestone", "") or ""
            )
            first_socket_ms = self._round_five_number(
                self._round_five_value(
                    progress,
                    "first_socket_initiated_ms",
                )
            )
            first_authentication_ms = self._round_five_number(
                self._round_five_value(
                    progress,
                    "first_client_authenticated_ms",
                )
            )
            exact_target = (
                initiated == TARGET_CLIENTS_PER_LANE
                and authenticated == TARGET_CLIENTS_PER_LANE
                and held == TARGET_CLIENTS_PER_LANE
                and target_elapsed is not None
            )
            if not exact_target:
                phase = "ramping"
                target_elapsed = None
            raw_progress_revision = self._round_five_value(progress, "sequence")
            progress_revision = (
                int(raw_progress_revision)
                if isinstance(raw_progress_revision, int)
                and not isinstance(raw_progress_revision, bool)
                and raw_progress_revision > 0
                else None
            )
            observed_ns = self._clock_ns()
            progress_digest = (
                hashlib.sha256(
                    json.dumps(
                        {
                            "lane_id": lane_id,
                            "phase": phase,
                            "initiated_clients": initiated,
                            "authenticated_clients": authenticated,
                            "held_clients": held,
                            "peak_held_clients": peak_held,
                            "sampled_queries_succeeded": samples,
                            "elapsed_ms": runner_elapsed,
                            "time_to_target_ms": target_elapsed,
                            "milestone": milestone,
                            "first_socket_initiated_ms": first_socket_ms,
                            "first_client_authenticated_ms": (
                                first_authentication_ms
                            ),
                            "sequence": progress_revision,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                if progress_revision is not None
                else None
            )
            async with record.lock:
                if not ingestion_is_open():
                    return
                runtime = record.snapshot.round5_runtime
                started_ns = record.run_started_monotonic_ns
                if runtime is None or started_ns is None:
                    return
                lane_runtime = runtime.lanes[lane_id]
                # Worker progress is a current-state projection, not a set of
                # monotonic counters. Multiprocess output can be replayed by an
                # SSM poll, so only a newer per-lane revision may replace it.
                # Synthetic final-result updates have no runner sequence and
                # are ordered locally after every observed progress callback.
                if progress_revision is not None:
                    progress_key = (lane_id, progress_revision)
                    if progress_revision < lane_runtime.progress_revision:
                        return
                    if progress_revision == lane_runtime.progress_revision:
                        if (
                            record.round5_progress_payload_digests.get(progress_key)
                            != progress_digest
                        ):
                            raise InvalidStateError(
                                "Round 5 equal progress revision changed payload"
                            )
                        return
                    assert progress_digest is not None
                    record.round5_progress_payload_digests[progress_key] = (
                        progress_digest
                    )
                lane_runtime.progress_revision = (
                    progress_revision
                    if progress_revision is not None
                    else lane_runtime.progress_revision + 1
                )
                observed_ms = max(0.0, (observed_ns - started_ns) / 1_000_000)
                previous_ns = record.round5_progress_observed_ns.get(lane_id)
                uncertainty_ms = (
                    max(0.0, (observed_ns - previous_ns) / 1_000_000)
                    if previous_ns is not None
                    else 0.0
                )
                record.round5_progress_observed_ns[lane_id] = observed_ns
                lane_runtime.phase = phase
                lane_runtime.elapsed_at_snapshot_ms = (
                    lane_runtime.bell_to_10000_observed_ms
                    or observed_ms
                )
                lane_runtime.clients_initiated = initiated
                lane_runtime.clients_authenticated = authenticated
                lane_runtime.held_clients = held
                lane_runtime.peak_clients_authenticated = max(
                    lane_runtime.peak_clients_authenticated,
                    authenticated,
                )
                lane_runtime.peak_held_clients = max(
                    lane_runtime.peak_held_clients,
                    peak_held,
                )
                lane_runtime.sampled_queries_succeeded = samples
                if (
                    lane_runtime.ramp_started_observed_ms is None
                    and first_socket_ms is not None
                ):
                    lane_runtime.ramp_started_observed_ms = first_socket_ms
                if milestone == "runner_observed_release":
                    lane_runtime.release_published_observed_ms = (
                        lane_runtime.release_published_observed_ms or observed_ms
                    )
                    lane_runtime.runner_release_observed_ms = (
                        lane_runtime.runner_release_observed_ms or observed_ms
                    )
                if first_socket_ms is not None:
                    release_floor = lane_runtime.runner_release_observed_ms
                    if release_floor is not None:
                        lane_runtime.first_socket_initiated_observed_ms = (
                            lane_runtime.first_socket_initiated_observed_ms
                            or release_floor + first_socket_ms
                        )
                if first_authentication_ms is not None:
                    release_floor = lane_runtime.runner_release_observed_ms
                    if release_floor is not None:
                        authentication_at = (
                            release_floor + first_authentication_ms
                        )
                        lane_runtime.first_client_authenticated_observed_ms = (
                            lane_runtime.first_client_authenticated_observed_ms
                            or authentication_at
                        )
                        lane_runtime.pooled_path_ready_observed_ms = (
                            lane_runtime.pooled_path_ready_observed_ms
                            or authentication_at
                        )
                if runner_elapsed is not None:
                    lane_runtime.ramp_time_to_10000_ms = target_elapsed
                if (
                    exact_target
                    and lane_runtime.bell_to_10000_observed_ms is None
                ):
                    lane_runtime.bell_to_10000_observed_ms = observed_ms
                    lane_runtime.elapsed_at_snapshot_ms = observed_ms
                    lane_runtime.observation_uncertainty_ms = uncertainty_ms
                lane_runtime.status = (
                    f"{held:,} / {TARGET_CLIENTS_PER_LANE:,} clients held"
                    if phase != "holding"
                    else (
                        f"{held:,} held · {samples} of 64 verification samples completed"
                    )
                )
                self._advance_round5_revision_locked(record)
                lane = record.snapshot.lanes[lane_id]
                lane.state = (
                    LaneState.VERIFYING
                    if phase == "holding"
                    else LaneState.CONNECTING
                )
                lane.attempts = lane_runtime.held_clients
                lane.successes = lane_runtime.clients_authenticated
                lane.status = lane_runtime.status
                lane.activity = LaneActivity(phase=phase)
                lane.evidence = {
                    **lane.evidence,
                    "initiated_clients": lane_runtime.clients_initiated,
                    "authenticated_clients": lane_runtime.clients_authenticated,
                    "held_clients": lane_runtime.held_clients,
                    "peak_authenticated_clients": (
                        lane_runtime.peak_clients_authenticated
                    ),
                    "peak_held_clients": lane_runtime.peak_held_clients,
                    "progress_revision": lane_runtime.progress_revision,
                    "sampled_queries_succeeded": (
                        lane_runtime.sampled_queries_succeeded
                    ),
                }
                record.snapshot.updated_at = datetime.now(UTC)
                await publish_lane_snapshots((lane_id,))

        async def on_lane_result(result: object) -> None:
            await publication_ready.wait()
            lane_id = str(self._round_five_value(result, "lane_id", ""))
            if lane_id not in {"lakebase", "competitor"}:
                raise InvalidStateError("Round 5 result named an unknown lane")
            evidence = self._round_five_evidence(result)
            lane_valid = self._round_five_lane_valid(result, evidence)
            if lane_valid:
                async with record.lock:
                    runtime = record.snapshot.round5_runtime
                    lane_valid = (
                        runtime is None
                        or runtime.lanes[lane_id].bell_to_10000_observed_ms
                        is not None
                    )
            if not lane_valid:
                async with record.lock:
                    if not ingestion_is_open():
                        return
                    record.round5_ingestion_closed = True
                    record.round5_callback_epoch += 1
                    record.round5_terminal_lane_evidence[lane_id] = evidence
                raise InvalidStateError(
                    f"Round 5 {lane_id} result failed its exact gate"
                )
            async with record.lock:
                if not ingestion_is_open():
                    return
                lane = record.snapshot.lanes[lane_id]
                lane.attempts = int(evidence.get("initiated_clients") or 0)
                lane.successes = int(evidence.get("authenticated_clients") or 0)
                lane.errors = int(evidence.get("terminal_failures") or 0)
                lane.p99_ms = self._round_five_number(
                    evidence.get("connect_latency_p99_ms")
                )
                lane.evidence = evidence
                # This is a lane-scoped proof latch, not the bout verdict.  It
                # must survive a sibling failure so the terminal snapshot does
                # not rewrite valid evidence as though this lane also failed.
                lane.state = LaneState.VERIFIED
                lane.status = "Exact lane result verified; awaiting sibling lane"
                lane.error = None
                lane.verified_at = datetime.now(UTC)
                lane.activity = LaneActivity(phase="verified")
                runtime = record.snapshot.round5_runtime
                if runtime is not None:
                    runtime_lane = runtime.lanes[lane_id]
                    runtime_lane.phase = "verified"
                    runtime_lane.status = lane.status
                self._advance_round5_revision_locked(record)
                record.snapshot.updated_at = datetime.now(UTC)
                await publish_lane_snapshots((lane_id,))

        if setup_operation is not None:
            try:
                lease = record.round5_lease
                if lease is None:
                    raise InvalidStateError("The Round 5 artifact lease is unavailable")
                setup_started = time.monotonic()
                setup_awaitable = (
                    setup_operation(
                        record.snapshot.id,
                        lease.fencing_token,
                        on_setup_progress,
                        on_lane_progress,
                        on_lane_result,
                    )
                    if record.snapshot.round5_runtime is not None
                    else setup_operation(
                        record.snapshot.id,
                        lease.fencing_token,
                        on_setup_progress,
                    )
                )
                setup_task = asyncio.create_task(
                    setup_awaitable,
                    name=f"round5-lane-pipelines-{record.snapshot.id}",
                )
                # The setup coroutine's first turn constructs both lane tasks
                # and releases their shared bell gate. Event publication is
                # deliberately scheduled only after that turn and is never on
                # either eligibility edge.
                await asyncio.sleep(0)
                try:
                    async with record.lock:
                        if ingestion_is_open():
                            await record.event_log.publish(
                                "run_started",
                                run_started_payload,
                            )
                finally:
                    publication_ready.set()
                setup_result = await setup_task
                logger.warning(
                    "Round 5 bell timing session=%s step=setup_phase elapsed_ms=%.0f",
                    record.snapshot.id,
                    (time.monotonic() - setup_started) * 1000,
                )
                record.connection_spike_setup_result = setup_result
                preliminary = self._round_five_finalize_setup(setup_result, {})
                async with record.lock:
                    if not ingestion_is_open():
                        return
                    record.snapshot.round5_setup = self._round_five_setup_snapshot(
                        record.snapshot,
                        preliminary,
                        terminal=False,
                    )
                    self._advance_round5_revision_locked(record)
                    await publish_lane_snapshots(("lakebase", "competitor"))
                if arm is None:
                    raise InvalidStateError(
                        "Round 5 warm capacity receipt is unavailable"
                    )
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
                )
                return
        else:
            await record.event_log.publish("run_started", run_started_payload)
            publication_ready.set()

        async with record.lock:
            if not ingestion_is_open():
                return
            if record.snapshot.round5_runtime is None:
                for lane in record.snapshot.lanes.values():
                    lane.status = "Opening 10,000 client connections"
                    lane.activity = LaneActivity(phase="burst")
            self._advance_round5_revision_locked(record)
            await publish_lane_snapshots(("lakebase", "competitor"))

        async def on_progress(progress: object) -> None:
            await publication_ready.wait()
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
                    lane.status = "Holding 10,000 clients and verifying multiplexing"
                    lane.activity = LaneActivity(phase=phase)
                affected_lane_ids = (
                    (str(lane_id),)
                    if lane_id in record.snapshot.lanes
                    else ("lakebase", "competitor")
                )
                record.snapshot.updated_at = datetime.now(UTC)
                self._advance_round5_revision_locked(record)
                await publish_lane_snapshots(affected_lane_ids)

        try:
            if arm is None:
                raise InvalidStateError("The Round 5 burst arm is unavailable")
            result = await engine.run(arm, on_progress)  # type: ignore[attr-defined]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Round 5 burst failed session=%s competitor=%s diagnosis=%s",
                record.snapshot.id,
                record.snapshot.competitor.id,
                operator_diagnosis(exc),
                exc_info=True,
            )
            message = "The live Round 5 proof failed unexpectedly."
            await self._finish_connection_spike_failure(
                record,
                message,
            )
            return
        await self._finish_connection_spike(record, result)

    async def _run_live_orders(self, record: SessionRecord) -> None:
        """Round 6's bell: the checkout commits everywhere at once, and AWS's pipeline starts.

        Every lane's clock runs from this one bell to its own first exact history read, on the
        controller's clock (``server/round6_race.py``). Nothing here waits on a status API; the
        verdict comes from the lanes' reads alone.
        """

        engine = record.live_orders_engine
        arm = record.live_orders_arm
        order = record.live_orders_pending_order
        guardrail = record.live_orders_guardrail_order
        if engine is None or arm is None or order is None or guardrail is None:
            await self._fail(record, "Round 6 must be prepared again.")
            return
        async with record.lock:
            if not self._committed_run_active_locked(record):
                return
            started_at = datetime.now(UTC)
            record.snapshot.state = SessionState.RUNNING
            record.snapshot.run_started_at = started_at
            # As in Round 4, the clock starts at the engine's bell, which it rings only once each
            # source has answered a pre-bell read; its first report stamps the origin.
            record.run_started_monotonic_ns = None
            record.snapshot.updated_at = started_at
            for lane_id in arm.lanes:
                lane = record.snapshot.lanes[lane_id]
                lane.state = LaneState.SEALED
                lane.status = "Reading the source's baseline before the bell"
                lane.attempts = 0
                lane.elapsed_ms = None
                lane.activity = LaneActivity(phase=Round6Phase.ARMED)
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_started",
            {
                "state": SessionState.RUNNING,
                "lanes": list(arm.lanes),
                "session": snapshot.model_dump(mode="json"),
            },
        )

        async def on_progress(
            lane_id: str,
            phase: Round6Phase,
            status: str,
            elapsed_ms: float | None,
        ) -> None:
            await self._apply_live_orders_progress(record, lane_id, phase, status, elapsed_ms)

        try:
            result = await engine.run(arm, order, guardrail, on_progress)
        except asyncio.CancelledError:
            raise
        except (LiveOrdersError, Round6RaceError) as exc:
            await self._finish_live_orders_failure(record, str(exc))
            return
        except Exception as exc:
            self._log_bout_refusal(record, exc, round_number=6)
            await self._finish_live_orders_failure(record, "Round 6 failed unexpectedly.")
            return
        await self._finish_live_orders(record, result)

    async def _apply_live_orders_progress(
        self,
        record: SessionRecord,
        lane_id: str,
        phase: Round6Phase,
        status: str,
        elapsed_ms: float | None,
    ) -> None:
        phase_states = {
            Round6Phase.STARTING: LaneState.CONNECTING,
            Round6Phase.WAITING: LaneState.VERIFYING,
            Round6Phase.VERIFIED: LaneState.VERIFIED,
            Round6Phase.FAILED: LaneState.FAILED,
        }
        async with record.lock:
            if record.snapshot.towel is not None or lane_id not in record.snapshot.lanes:
                return
            if phase == Round6Phase.STARTING and record.run_started_monotonic_ns is None:
                # The bell: the engine reports every lane STARTING at elapsed zero.
                record.run_started_monotonic_ns = self._clock_ns()
            lane = record.snapshot.lanes[lane_id]
            lane.state = phase_states.get(phase, lane.state)
            lane.status = status
            lane.activity = LaneActivity(phase=phase)
            if elapsed_ms is not None:
                lane.elapsed_ms = (
                    elapsed_ms
                    if phase == Round6Phase.VERIFIED
                    else max(lane.elapsed_ms or 0.0, elapsed_ms)
                )
            if lane.state == LaneState.VERIFIED and lane.verified_at is None:
                lane.verified_at = datetime.now(UTC)
            record.snapshot.updated_at = datetime.now(UTC)
            activity = lane.activity.model_dump(mode="json")
            state = lane.state
            elapsed = lane.elapsed_ms
        await record.event_log.publish(
            "lane_update",
            {
                "lane_id": lane_id,
                "state": state,
                "elapsed_ms": elapsed,
                "status": status,
                "activity": activity,
            },
        )

    async def _finish_live_orders(
        self,
        record: SessionRecord,
        result: Round6RaceResult,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None or record.round5_terminal_published:
                return
            async with record.lease_lock:
                terminal_lease = record.lease
        if await self._settle_terminal_release(record, terminal_lease) == "lost":
            return
        async with record.lock:
            if record.snapshot.towel is not None or record.round5_terminal_published:
                return
            measured = self._apply_live_orders_result(record.snapshot, result)
            record.snapshot.state = SessionState.VERIFIED if measured else SessionState.FAILED
            record.snapshot.failure = None if measured else record.snapshot.comparison.detail
            record.snapshot.remembered_result = (
                _round_six_remembered_result(record.snapshot) if measured else None
            )
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            record.live_orders_result = result
            record.live_orders_pending_order = None
            record.live_orders_guardrail_order = None
            engine = record.live_orders_engine
            snapshot = record.snapshot.model_copy(deep=True)
        if measured:
            await record.event_log.publish(
                "run_finished",
                {"state": SessionState.VERIFIED, "session": snapshot.model_dump(mode="json")},
                receipt_bout_id=snapshot.receipt_bout_id,
            )
        else:
            self._log_lane_refusals(record, round_number=6)
            await record.event_log.publish(
                "session_failed",
                {
                    "state": SessionState.FAILED,
                    "message": snapshot.failure,
                    "session": snapshot.model_dump(mode="json"),
                },
                receipt_bout_id=snapshot.receipt_bout_id,
            )
        self._schedule_round_settlement(
            record,
            getattr(engine, "settle_and_cleanup_owned", None),
            label="Round 6",
        )

    @staticmethod
    def _apply_live_orders_result(snapshot: SessionSnapshot, result: Round6RaceResult) -> bool:
        """Put a bout's lanes, metrics and verdict on the card. True when it measured something.

        The verdict is the engine's, from the lanes' own reads (``round4_race.resolve``);
        nothing here recomputes it.
        """

        order = result.order
        metrics: list[MetricValue] = []
        for lane_id, outcome in result.outcomes.items():
            lane = snapshot.lanes[lane_id]
            guardrail = result.guardrails.get(lane_id)
            lane.evidence = {
                "order_id": order.order_id,
                "total_cents": order.total_cents,
                "total_display": f"${order.total_cents / 100:.2f}",
                "proof_nonce": order.proof_nonce,
                "commit_ack_ms": result.commit_ack_ms.get(lane_id),
                "commit_skew_ms": result.commit_skew_ms,
                "checkout_guardrail_order_id": result.guardrail_order.order_id,
                "checkout_guardrail_commit_ms": guardrail.commit_ms if guardrail else None,
                "checkout_guardrail_read_ms": guardrail.read_ms if guardrail else None,
                "reads": outcome.reads,
                "max_read_gap_ms": outcome.max_read_gap_ms,
                "last_negative_ms": outcome.last_negative_ms,
                "bell_at": result.rung_at,
                "protocol": result.protocol,
                **dict(outcome.evidence),
            }
            lane.attempts = outcome.reads
            if outcome.verified and outcome.elapsed_ms is not None:
                lane.state = LaneState.VERIFIED
                lane.elapsed_ms = outcome.elapsed_ms
                lane.status = "The exact order is in the lakehouse · separate checkout committed"
                lane.error = None
                lane.verified_at = lane.verified_at or datetime.now(UTC)
                lane.activity = LaneActivity(phase=Round6Phase.VERIFIED)
                metrics.append(
                    MetricValue(
                        spec_id="bell_to_exact_history_ms",
                        lane_id=lane_id,
                        value=outcome.elapsed_ms,
                        display_value=f"{outcome.elapsed_ms / 1000:.2f} s",
                    )
                )
            else:
                lane.state = LaneState.FAILED
                lane.elapsed_ms = None
                lane.status = (
                    "Did not deliver the order within its bound"
                    if outcome.timed_out
                    else "Could not be measured"
                )
                lane.error = outcome.failure
                if outcome.timed_out and outcome.bound_ms is not None:
                    # Still running at its bound, which is a result, not an error.
                    lane.error = None
                    lane.evidence.update(_floor_evidence(outcome.bound_ms))
                lane.activity = LaneActivity(phase=Round6Phase.FAILED)
            metrics.append(
                MetricValue(
                    spec_id="exact_order_verified",
                    lane_id=lane_id,
                    value=bool(outcome.verified),
                    display_value="Verified" if outcome.verified else "Not verified",
                )
            )
            if guardrail is not None:
                metrics.append(
                    MetricValue(
                        spec_id="checkout_verified",
                        lane_id=lane_id,
                        value=True,
                        display_value="SEPARATE CHECKOUT COMMITTED ✓",
                    )
                )
        if result.commit_skew_ms is not None:
            metrics.append(
                MetricValue(
                    spec_id="commit_skew_ms",
                    value=result.commit_skew_ms,
                    display_value=f"{result.commit_skew_ms:.0f} ms",
                )
            )
        snapshot.metrics = metrics
        snapshot.comparison, measured = _round_six_comparison(
            result,
            snapshot.lanes[COMPETITOR_LANE].name,
            has_competitor=COMPETITOR_LANE in result.outcomes,
        )
        return measured

    async def _finish_live_orders_failure(
        self,
        record: SessionRecord,
        message: str,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            async with record.lease_lock:
                terminal_lease = record.lease
        if await self._settle_terminal_release(record, terminal_lease) == "lost":
            return
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            for lane in record.snapshot.lanes.values():
                if lane.state in {LaneState.NOT_SUPPORTED, LaneState.VERIFIED}:
                    continue
                lane.state = LaneState.FAILED
                lane.error = message
                lane.status = "Round 6 could not be verified"
                lane.activity = LaneActivity(phase=Round6Phase.FAILED)
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            # The one funnel every Round 6 bout failure passes through, including the typed
            # errors that carry the real reason and have no log of their own.
            self._log_lane_refusals(record, round_number=6)
            record.snapshot.remembered_result = None
            record.snapshot.metrics = []
            record.snapshot.comparison = None
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            # The engine keeps every order it rang until settling has removed it, so a failure
            # after a commit still owes and finds its rows.
            engine = record.live_orders_engine
            record.live_orders_pending_order = None
            record.live_orders_guardrail_order = None
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        self._schedule_round_settlement(
            record,
            getattr(engine, "settle_and_cleanup_owned", None),
            label="Round 6",
        )

    async def _begin_connection_spike_cleanup_handoff(
        self,
        record: SessionRecord,
    ) -> None:
        """Move Round 5 artifacts backstage without delaying the verdict."""

        await self._mark_connection_spike_cleanup_in_progress(record)
        engine = record.connection_spike_engine
        operator = record.operator
        if engine is None or operator is None:
            await self._mark_connection_spike_cleanup_pending(record)
            return
        try:
            coordinator = self._round5_warm_coordinator
            claim_id = (
                record.round5_warm_slot.claim.claim_id
                if record.round5_warm_slot is not None
                and record.round5_warm_slot.claim is not None
                else None
            )
            if coordinator is not None and claim_id is not None:
                # Cancel ALL local in-process work (setup task, the
                # lane bursts still parked in _lane_bursts AND the ones run() popped
                # into its launched map, plus the in-flight run task itself) BEFORE
                # begin_cleanup makes the slot CLEANING visible and wakes the loop --
                # otherwise a burst could dispatch AFTER CLEANING (or the loop's
                # converge could race a live burst). Bounded and fail-safe: if local
                # cancellation stalls or raises we STILL enter durable CLEANING so the
                # coordinator fence/authority is preserved (a stalled local task must
                # never block the durable transition).
                cancel_local = getattr(engine, "cancel_local_round5_run_tasks", None)
                if callable(cancel_local):
                    try:
                        async with asyncio.timeout(
                            max(0.001, self._round5_local_cancel_timeout)
                        ):
                            await cancel_local()
                    except TimeoutError:
                        logger.error(
                            "Round 5 local run-task cancellation did not complete "
                            "before CLEANING session=%s; entering durable cleanup "
                            "anyway to preserve authority",
                            record.snapshot.id,
                        )
                    except Exception:
                        logger.error(
                            "Round 5 local run-task cancellation failed session=%s; "
                            "entering durable cleanup anyway",
                            record.snapshot.id,
                            exc_info=True,
                        )
                # Retain the cleanup lease BEST EFFORT before begin_cleanup: on
                # success the round5_cleanup phase is retained so a begin_cleanup CAS
                # conflict can be retried under that authority by the convergence
                # worker. A retain FAILURE (e.g. _mark_bout_armed already
                # lost/released the lease on a LeaseLostError) must NOT skip
                # begin_cleanup -- otherwise the slot stays CLAIMED with an orphaned
                # staged resident. So swallow retain failures and fence CLEANING
                # regardless; the warm coordinator owns the warm slot independently of
                # the ring/cleanup lease.
                try:
                    await self._retain_connection_spike_cleanup_lease(
                        record,
                        operator,
                        session_state=record.snapshot.state,
                        allow_claim=False,
                    )
                except Exception:
                    logger.error(
                        "Round 5 cleanup-lease retain failed before CLEANING "
                        "session=%s; fencing CLEANING anyway (convergence/takeover "
                        "will proceed)",
                        record.snapshot.id,
                        exc_info=True,
                    )
                record.round5_warm_slot = await coordinator.begin_cleanup(claim_id)
            else:
                await self._retain_connection_spike_cleanup_lease(
                    record,
                    operator,
                    session_state=record.snapshot.state,
                    allow_claim=False,
                )
                stop_run = getattr(engine, "stop_and_begin_cleanup", None)
                stop_setup = getattr(engine, "stop_setup_and_begin_cleanup", None)
                if (
                    stop_run is not None
                    and record.connection_spike_arm is not None
                    and record.snapshot.round5_setup is not None
                    and record.snapshot.round5_setup.setup_validated
                ):
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
            return

        current = record.connection_spike_cleanup_task
        if current is not None and not current.done():
            return
        record.connection_spike_cleanup_task = asyncio.create_task(
            self._complete_connection_spike_cleanup_handoff(record, engine),
            name=f"round5-backstage-cleanup-{record.snapshot.id}",
        )

    async def _mark_connection_spike_cleanup_in_progress(
        self,
        record: SessionRecord,
    ) -> None:
        async with record.lock:
            setup = record.snapshot.round5_setup
            if setup is not None:
                setup.cleanup_retryable = False
                setup.cleanup_failure = None
            if record.snapshot.towel is not None:
                record.snapshot.towel.state = TowelState.CLEANING
                record.snapshot.towel.cleanup_failure = None
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = self._revalidated_snapshot(record.snapshot)
            await record.event_log.publish(
                "cleanup_update",
                {"session": snapshot.model_dump(mode="json")},
                receipt_bout_id=snapshot.receipt_bout_id,
            )

    async def _round5_proof_authority_is_current(
        self,
        record: SessionRecord,
    ) -> tuple[BoutLease, BoutLease] | None:
        async with record.lease_lock:
            main = record.lease
            round5 = record.round5_lease
        if main is None or round5 is None:
            return None
        if main.phase != "run_committed" or round5.phase != "run_committed":
            return None
        try:
            current_main, current_round5 = await asyncio.gather(
                self._lease_store_for_record(record).current(),
                self._round5_cleanup_store().current(),
            )
        except Exception:
            return None
        if not (
            current_main is not None
            and current_round5 is not None
            and self._same_exact_lease(main, current_main)
            and self._same_exact_lease(round5, current_round5)
            and current_main.phase == "run_committed"
            and current_round5.phase == "run_committed"
        ):
            return None
        return main, round5

    async def _commit_round5_proof_authority_to_cleanup(
        self,
        record: SessionRecord,
        authority: tuple[BoutLease, BoutLease],
        session_state: SessionState,
    ) -> bool:
        """CAS the exact observed proof fence and read it back before publication."""

        main, round5 = authority
        try:
            current_main, current_round5 = await asyncio.gather(
                self._lease_store_for_record(record).current(),
                self._round5_cleanup_store().current(),
            )
        except Exception:
            return False
        if not (
            current_main is not None
            and current_round5 is not None
            and self._same_exact_lease(main, current_main)
            and self._same_exact_lease(round5, current_round5)
            and current_main.phase == "run_committed"
            and current_round5.phase == "run_committed"
        ):
            return False
        try:
            await self._transition_bout_to_cleanup(
                record,
                session_state,
                expected_lease=main,
                expected_round5_lease=round5,
            )
        except InvalidStateError:
            return False
        async with record.lease_lock:
            transitioned = record.lease
            local_round5 = record.round5_lease
        if (
            transitioned is None
            or local_round5 is None
            or transitioned.phase != "cooldown"
            or not self._same_exact_lease(local_round5, round5)
        ):
            return False
        try:
            readback_main, readback_round5 = await asyncio.gather(
                self._lease_store_for_record(record).current(),
                self._round5_cleanup_store().current(),
            )
        except Exception:
            return False
        return bool(
            readback_main is not None
            and readback_round5 is not None
            and self._same_exact_lease(transitioned, readback_main)
            and self._same_exact_lease(round5, readback_round5)
            and readback_main.phase == "cooldown"
            and readback_round5.phase == "run_committed"
        )

    async def _complete_connection_spike_cleanup_handoff(
        self,
        record: SessionRecord,
        engine: object,
    ) -> None:
        task = asyncio.current_task()
        try:
            if self._round5_warm_coordinator is not None:
                if not await self._retry_connection_spike_cleanup(record, engine):
                    raise InvalidStateError("Round 5 cleanup could not be verified")
                return
            wait_accepted = getattr(engine, "wait_for_proxy_delete_accepted", None)
            wait_complete = getattr(engine, "wait_for_cleanup_complete", None)
            if wait_accepted is not None and wait_complete is not None:
                try:
                    await wait_accepted()
                except Exception:
                    accepted = getattr(engine, "proxy_delete_accepted", None)
                    if accepted is None or not bool(accepted()):
                        raise
                await wait_complete()
            else:
                if not await self._cleanup_connection_spike(record):
                    raise InvalidStateError("Round 5 cleanup could not be verified")
            if (
                self._round5_warm_coordinator is not None
                and record.round5_warm_slot is not None
                and record.round5_warm_slot.claim is not None
            ):
                record.round5_warm_slot = (
                    await self._round5_warm_coordinator.finish_cleanup_and_rewarm(
                        record.round5_warm_slot.claim.claim_id
                    )
                )
                record.round5_launch_capsule = None
            self._require_round5_rewarm_transition(record)
            if not await self._release_bout(record):
                raise InvalidStateError("Main ring release is still pending")
            if not await self._release_round5_lease(record):
                raise InvalidStateError("Round 5 cleanup lease release is still pending")
            await self._mark_connection_spike_cleanup_complete(record)
        except asyncio.CancelledError:
            # Cancellation is a lost handoff, not a reason to leave a CLEANING
            # generation and a renewing lease with no worker. Publish retryable
            # debt and install the convergence backstop before propagating it.
            await asyncio.shield(
                self._mark_connection_spike_cleanup_pending(record)
            )
            raise
        except Exception as exc:
            logger.error(
                "Round 5 backstage cleanup is not settled session=%s diagnostic=%s",
                record.snapshot.id,
                _redacted_exception_chain(exc),
            )
            await self._mark_connection_spike_cleanup_pending(record)
        finally:
            if record.connection_spike_cleanup_task is task:
                record.connection_spike_cleanup_task = None

    def _require_round5_rewarm_transition(
        self,
        record: SessionRecord,
    ) -> None:
        if self._round5_warm_coordinator is None:
            return
        slot = record.round5_warm_slot
        if (
            slot is None
            or slot.claim is not None
            or slot.state
            in {
                Round5WarmState.CLAIMED,
                Round5WarmState.RUNNING,
                Round5WarmState.CLEANING,
            }
        ):
            raise InvalidStateError(
                "Round 5 durable rewarm transition is not confirmed"
            )

    def _schedule_connection_spike_cleanup_convergence(
        self,
        record: SessionRecord,
    ) -> None:
        """Drive a stalled Round 5 backstage cleanup to convergence automatically.

        The observed live failure was a warm-slot ``WarmStoreConflictError`` CAS
        race at the cleanup->rewarm handoff: the handoff gave up, latched a
        process-local "cleanup owed" notice, and stopped -- leaving a stale
        billing banner even though AWS had already deleted (or never had) the
        Proxy. This loop removes the human "Retry Cleanup": it keeps re-running
        the journal-based reconcile with bounded backoff until the exact per-bout
        Proxy is confirmed absent (NotFound) and the rewarm CAS wins. It never
        clears the owed notice on an empty-orchestrator no-op -- only
        ``_retry_connection_spike_cleanup`` (which calls the journal
        ``reconcile_failed_cleanup`` and clears owed solely via
        ``_mark_connection_spike_cleanup_complete`` after reconcile+rewarm
        succeed) does. No end-user action ever waits on this loop.
        """

        # Defensive getattr: some unit tests exercise cleanup helpers on a
        # manager built via object.__new__ (no __init__), so these attributes may
        # be absent. In that case there is nothing to schedule against.
        if getattr(self, "_closed", False):
            return
        round_obj = getattr(getattr(record, "snapshot", None), "round", None)
        if getattr(round_obj, "id", None) != RoundId.SURVIVE_CONNECTION_SPIKE:
            return
        if getattr(self, "_connection_spike_factory", None) is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        del loop
        existing = record.connection_spike_cleanup_retry_task
        if existing is not None and not existing.done():
            return
        record.connection_spike_cleanup_retry_task = asyncio.create_task(
            self._auto_converge_connection_spike_cleanup(record),
            name=f"round5-auto-cleanup-{record.snapshot.id}",
        )

    async def _auto_converge_connection_spike_cleanup(
        self,
        record: SessionRecord,
    ) -> None:
        task = asyncio.current_task()
        delay = max(0.05, self._cleanup_retry_initial)
        maximum_delay = max(delay, self._cleanup_retry_max)
        attempts = 0
        try:
            # Let the sealed receipt event reach the room before any backstage
            # reconcile churn.
            await asyncio.sleep(delay)
            while not self._closed:
                # A live bout task or an in-flight backstage cleanup owns the
                # ring; never race them, just wait and re-check.
                if record.task is not None and not record.task.done():
                    await asyncio.sleep(delay)
                    continue
                if (
                    record.connection_spike_cleanup_task is not None
                    and not record.connection_spike_cleanup_task.done()
                ):
                    await asyncio.sleep(delay)
                    continue
                async with record.lock:
                    setup = record.snapshot.round5_setup
                    still_owed = bool(setup is not None and setup.cleanup_retryable)
                if not still_owed:
                    return
                engine = record.connection_spike_engine
                if engine is None or getattr(engine, "reconcile_failed_cleanup", None) is None:
                    engine = self._connection_spike_factory(record.snapshot.competitor.id)
                    record.connection_spike_engine = engine
                attempts += 1
                converged = False
                try:
                    converged = await self._retry_connection_spike_cleanup(record, engine)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(
                        "Round 5 automatic cleanup convergence attempt %d "
                        "session=%s diagnostic=%s",
                        attempts,
                        record.snapshot.id,
                        _redacted_exception_chain(exc),
                    )
                if converged:
                    logger.warning(
                        "Round 5 automatic cleanup converged after %d attempt(s) "
                        "session=%s",
                        attempts,
                        record.snapshot.id,
                    )
                    return
                # Bounded exponential backoff (capped): retry the CAS/reconcile
                # persistently without storming. AWS delete/absence is proven by
                # reconcile_failed_cleanup before the rewarm CAS is attempted.
                await asyncio.sleep(delay)
                delay = min(delay * 2, maximum_delay)
        except asyncio.CancelledError:
            raise
        finally:
            if record.connection_spike_cleanup_retry_task is task:
                record.connection_spike_cleanup_retry_task = None

    async def _mark_connection_spike_cleanup_pending(
        self,
        record: SessionRecord,
    ) -> None:
        owed = self._note_round5_proxy_at_risk(
            record,
            # Automatic convergence keeps retrying (below), so the operator
            # notice reflects "still retrying", never "gave up, ask a human".
            still_retrying=True,
            # ``cleanup_retryable`` is itself the admission that deletion and
            # durable rewarm are not confirmed. It must always have a matching
            # owed record, even if AWS accepted an earlier delete request.
            force=True,
        )
        # Inside the owed notice's grace window a cleanup that is still retrying is
        # an ordinary Proxy delete in progress, not a failure. Surfacing the
        # leaked-Proxy sentence and a failed towel with a Retry control there put
        # an alarm on the result screen ~30 s into ordinary towel cleanups, then
        # took it back seconds later. The session now waits for the same due time
        # /readyz already honours; a cleanup still unconfirmed after it escalates
        # on the next failed attempt exactly as before.
        due = owed is None or datetime.now(UTC) >= owed.due_at
        cleanup_failure = (
            (owed.detail if owed is not None else "Automatic backstage cleanup is retrying.")
            if due
            else None
        )
        try:
            async with record.lock:
                setup = record.snapshot.round5_setup
                if setup is not None:
                    setup.cleanup_retryable = True
                    setup.cleanup_failure = cleanup_failure
                    if record.snapshot.towel is not None:
                        setup.state = RoundFiveSetupState.TOWELLED
                    elif record.snapshot.state == SessionState.FAILED:
                        setup.state = RoundFiveSetupState.CLEANUP_FAILED
                        setup.failure = (
                            setup.cleanup_failure
                            or "Automatic cleanup verification is in progress"
                        )
                if record.snapshot.towel is not None and due:
                    record.snapshot.towel.state = TowelState.FAILED
                    record.snapshot.towel.cleanup_failure = cleanup_failure
                record.snapshot.updated_at = datetime.now(UTC)
                snapshot = self._revalidated_snapshot(record.snapshot)
                await record.event_log.publish(
                    "cleanup_update",
                    {"session": snapshot.model_dump(mode="json")},
                    receipt_bout_id=snapshot.receipt_bout_id,
                )
        finally:
            # Never leave a stalled cleanup waiting on a human, even if publishing
            # the operator snapshot itself failed. This is a no-op when a
            # convergence loop is already running.
            self._schedule_connection_spike_cleanup_convergence(record)

    def _note_round5_proxy_at_risk(
        self,
        record: SessionRecord,
        *,
        attempts: int | None = None,
        still_retrying: bool = True,
        force: bool = False,
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

        Handing control back to the operator overrides the gate. Once automatic
        cleanup has failed, "the delete was accepted" is not proof that the
        resource disappeared; the retained fence and Retry Cleanup are the only
        safe continuation.
        """

        engine = record.connection_spike_engine
        if engine is None:
            return None
        accepted = getattr(engine, "proxy_delete_accepted", None)
        if not force and still_retrying and accepted is not None:
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
        self._require_round5_rewarm_transition(record)
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
            snapshot = self._revalidated_snapshot(record.snapshot)
            await record.event_log.publish(
                "cleanup_update",
                {
                    "session": snapshot.model_dump(mode="json"),
                    # The public event name remains stable for existing clients,
                    # while the receipt hook can distinguish confirmed recovery
                    # from an in-progress cleanup snapshot with the same shape.
                    "cleanup_settled": True,
                },
                receipt_bout_id=snapshot.receipt_bout_id,
            )
        # The one place a Proxy is proved gone. Every surface that was warning
        # about it stops here, together, so `/readyz` cannot keep naming a
        # resource the snapshot has already stopped naming.
        clear_round5_cleanup_owed(record.snapshot.id)

    async def _finish_connection_spike(
        self,
        record: SessionRecord,
        result: object,
    ) -> None:
        async with record.lock:
            if (
                record.snapshot.towel is not None
                or record.round5_terminal_published
            ):
                return
            record.round5_ingestion_closed = True
            record.round5_callback_epoch += 1
        proof_authority = await self._round5_proof_authority_is_current(record)
        if proof_authority is None:
            await self._finish_connection_spike_failure(
                record,
                "Round 5 proof authority changed; no comparison was declared.",
            )
            return
        async with record.lock:
            if (
                record.snapshot.towel is not None
                or record.round5_terminal_published
            ):
                return
            pre_verdict_snapshot = record.snapshot.model_copy(deep=True)
            pre_verdict_armed_at = record.armed_at_monotonic
            pre_verdict_setup_result = record.connection_spike_setup_result
            record.round5_ingestion_closed = True
            lanes = self._round_five_lanes(result)
            runtime = record.snapshot.round5_runtime
            valid = set(lanes) == {"lakebase", "competitor"}
            launch_skews: list[float | None] = []
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
                lane.attempts = int(
                    evidence.get(
                        "scheduled_clients",
                        evidence.get("initiated_clients", 0),
                    )
                    or 0
                )
                lane.successes = int(
                    evidence.get(
                        "successful_clients",
                        evidence.get("authenticated_clients", 0),
                    )
                    or 0
                )
                lane.errors = int(
                    evidence.get(
                        "error_clients",
                        evidence.get("terminal_failures", 0),
                    )
                    or 0
                )
                lane.p99_ms = self._round_five_number(
                    self._round_five_value(raw, "application_p99_ms")
                )
                lane.evidence = evidence
                lane_valid = self._round_five_lane_valid(raw, evidence)
                launch_skew = self._round_five_number(
                    self._round_five_value(raw, "launch_skew_ms")
                )
                launch_skews.append(launch_skew)
                # Launch skew is recorded fairness evidence, not a millisecond
                # SLO that can void an otherwise verified exact-10K proof.
                # Missing, non-finite, and negative evidence still fail closed.
                lane_valid = (
                    lane_valid
                    and launch_skew is not None
                    and launch_skew >= 0
                )
                if runtime is not None:
                    runtime_lane = runtime.lanes[lane_id]
                    # Result transfer happens after ramp, hold, sampling and
                    # socket cleanup.  It is not the parent barrier edge and
                    # must never be substituted for a missing bell-to-10K
                    # progress observation.
                    barrier_observed = (
                        runtime_lane.bell_to_10000_observed_ms is not None
                    )
                    lane_valid = lane_valid and barrier_observed
                    observed_ms = max(
                        runtime_lane.elapsed_at_snapshot_ms,
                        (
                            self._clock_ns() - record.run_started_monotonic_ns
                        )
                        / 1_000_000
                        if record.run_started_monotonic_ns is not None
                        else 0.0,
                    )
                    runtime_lane.clients_initiated = int(
                        evidence.get("initiated_clients") or 0
                    )
                    runtime_lane.clients_authenticated = int(
                        evidence.get("authenticated_clients") or 0
                    )
                    runtime_lane.held_clients = int(
                        evidence.get("held_clients_at_gate") or 0
                    )
                    runtime_lane.sampled_queries_succeeded = int(
                        evidence.get("sampled_queries_succeeded") or 0
                    )
                    runtime_lane.ramp_time_to_10000_ms = self._round_five_number(
                        evidence.get("time_to_target_ms")
                    )
                    if lane_valid:
                        runtime_lane.elapsed_at_snapshot_ms = (
                            runtime_lane.bell_to_10000_observed_ms
                        )
                        runtime_lane.phase = "verified"
                    else:
                        runtime_lane.phase = "failed"
                        runtime_lane.elapsed_at_snapshot_ms = observed_ms
                    runtime_lane.status = (
                        "Exact 10,000-client retained gate verified"
                        if lane_valid
                        else "Round 5 exact lane gate or barrier evidence failed"
                    )
                    lane.elapsed_ms = runtime_lane.bell_to_10000_observed_ms
                else:
                    # Legacy setup-clock scorecards retain their historical
                    # rendering without being relabelled as V4 runtime evidence.
                    lane.elapsed_ms = None
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

            skew = max((value for value in launch_skews if value is not None), default=None)
            record.snapshot.fairness = FairnessSnapshot(
                launch_skew_ms=skew,
                warmup_connections=_ROUND_FIVE_WARMUP_CONNECTIONS,
                concurrency=_ROUND_FIVE_CONCURRENCY,
                runner=_ROUND_FIVE_RUNNER,
                tls="verify-full",
                timeout="20s connect · 600s run",
                protocol=ROUND5_FANIN_PROTOCOL,
                target_clients_per_lane=TARGET_CLIENTS_PER_LANE,
                hold_seconds=30,
                sampled_queries_per_lane=64,
                max_retries=ROUND5_MAX_RETRIES,
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
            comparison = None
            if valid:
                comparison = (
                    self._round_five_v4_comparison(
                        runtime,
                        record.snapshot.competitor.short_name,
                    )
                    if runtime is not None
                    else self._round_five_setup_comparison(
                        setup_result,
                        record.snapshot.competitor.short_name,
                    )
                )
            if comparison is None:
                valid = False
            record.snapshot.comparison = comparison if valid else None
            record.snapshot.metrics = (
                self._round_five_metrics(record.snapshot, setup_snapshot)
                if valid and runtime is None
                else [
                    MetricValue(
                        spec_id="bell_to_10000_observed_ms",
                        lane_id=lane_id,
                        value=runtime_lane.bell_to_10000_observed_ms or 0.0,
                        display_value=(
                            f"{runtime_lane.bell_to_10000_observed_ms:.2f} ms"
                            if runtime_lane.bell_to_10000_observed_ms is not None
                            else "N/A"
                        ),
                    )
                    for lane_id, runtime_lane in runtime.lanes.items()
                ]
                if valid and runtime is not None
                else []
            )
            if valid:
                record.snapshot.state = SessionState.VERIFIED
                record.snapshot.failure = None
                if comparison.kind == ComparisonKind.TIE:
                    record.snapshot.remembered_result = (
                        "BOTH POOLED PATHS VERIFIED TOGETHER"
                    )
                else:
                    assert comparison.winner_lane_id is not None
                    assert comparison.margin is not None
                    winner = (
                        "LAKEBASE"
                        if comparison.winner_lane_id == "lakebase"
                        else record.snapshot.lanes["competitor"].name.upper()
                    )
                    record.snapshot.remembered_result = (
                        f"{winner} VERIFIED A POOLED PATH · "
                        f"{comparison.margin.value / 1000:.2f}s SOONER"
                    )
                setup_snapshot.state = RoundFiveSetupState.VERIFIED
                if runtime is not None:
                    runtime.state = "verified"
            else:
                record.snapshot.state = SessionState.FAILED
                # Name the actual gate in the bout-level sentence so a future
                # paid failure is self-describing (e.g. "... [competitor:
                # create_db_proxy_window]") instead of the generic contract-gate
                # line that forced a log dig on 2026-09-17.
                setup_diagnostics = [
                    f"{lane_id}:{lane.setup_diagnostic}"
                    for lane_id, lane in setup_snapshot.lanes.items()
                    if not lane.verified and lane.setup_diagnostic
                ]
                diagnostic_suffix = (
                    f" [{'; '.join(setup_diagnostics)}]" if setup_diagnostics else ""
                )
                record.snapshot.failure = (
                    "Round 5 contract gate failed; no comparison was declared."
                    + diagnostic_suffix
                )
                record.snapshot.remembered_result = None
                setup_snapshot.state = RoundFiveSetupState.FAILED
                setup_snapshot.failure = "Setup or downstream verification did not complete"
                if runtime is not None:
                    runtime.state = "failed"
                # Before the overwrite below, not after. That loop replaces every
                # lane's own reason with one bout-level sentence, so a log call
                # placed after it would report the same generic string twice and
                # lose which lane's contract gate actually refused.
                self._log_lane_refusals(record, round_number=5)
                for lane in record.snapshot.lanes.values():
                    if lane.state == LaneState.VERIFIED:
                        continue
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
            # Publish the verdict only after the exact main/artifact observations
            # used to authorize it have won the cleanup CAS and that transition
            # has been read back.  A lost CAS restores the pre-verdict state;
            # otherwise a local VERIFIED mutation could leak through GET or a
            # sealed receipt even though durable authority had already moved.
            authority_committed = await self._commit_round5_proof_authority_to_cleanup(
                record,
                proof_authority,
                record.snapshot.state,
            )
            if not authority_committed:
                logger.error(
                    "Round 5 result lost its exact proof fence before terminal "
                    "publication session=%s",
                    record.snapshot.id,
                )
                record.snapshot = pre_verdict_snapshot
                record.armed_at_monotonic = pre_verdict_armed_at
                record.connection_spike_setup_result = pre_verdict_setup_result
                record.round5_ingestion_closed = True
            else:
                self._advance_round5_revision_locked(record)
                snapshot = self._revalidated_snapshot(record.snapshot)
                record.round5_terminal_published = True
                await record.event_log.publish(
                    "run_finished",
                    {"state": snapshot.state, "session": snapshot.model_dump(mode="json")},
                    receipt_bout_id=snapshot.receipt_bout_id,
                )
        if not authority_committed:
            await self._finish_connection_spike_failure(
                record,
                "Round 5 proof authority changed; no comparison was declared.",
            )
            return
        await self._begin_connection_spike_cleanup_handoff(record)

    async def _publish_connection_spike_failure_locked(
        self,
        record: SessionRecord,
        message: str,
    ) -> bool:
        """Publish the one absorbing Round 5 failure snapshot under the record gate."""

        if not record.lock.locked():
            raise RuntimeError("Round 5 terminal publication requires the record lock")
        if record.snapshot.towel is not None or record.round5_terminal_published:
            return False
        if not record.round5_ingestion_closed:
            record.round5_ingestion_closed = True
            record.round5_callback_epoch += 1
        setup_elapsed_floors = self._round_five_elapsed_floors(
            record,
            as_of_ns=self._clock_ns(),
        )
        record.snapshot.state = SessionState.FAILED
        record.snapshot.failure = message
        record.snapshot.remembered_result = None
        record.snapshot.metrics = []
        record.snapshot.comparison = None
        setup = record.snapshot.round5_setup
        runtime = record.snapshot.round5_runtime
        for lane_id, evidence in record.round5_terminal_lane_evidence.items():
            lane = record.snapshot.lanes[lane_id]
            if lane.state == LaneState.VERIFIED:
                continue
            lane.attempts = int(evidence.get("initiated_clients") or 0)
            lane.successes = int(evidence.get("authenticated_clients") or 0)
            lane.errors = int(evidence.get("terminal_failures") or 0)
            lane.evidence = evidence
            if runtime is not None:
                runtime_lane = runtime.lanes[lane_id]
                runtime_lane.clients_initiated = lane.attempts
                runtime_lane.clients_authenticated = lane.successes
                runtime_lane.held_clients = int(
                    evidence.get("held_clients_at_gate") or 0
                )
                runtime_lane.sampled_queries_succeeded = int(
                    evidence.get("sampled_queries_succeeded") or 0
                )
                # Preserve a legitimate exact-10k observed stop. This field is set
                # only by the authoritative progress observation of exactly
                # TARGET_CLIENTS_PER_LANE held clients, so a non-null value records
                # a real moment the lane reached 10,000 -- keep it even when later
                # telemetry leaves the lane unscored or failed. The failed verdict
                # is carried by lane.state / runtime_lane.phase below, not by
                # discarding this measurement. Only clear it when the terminal
                # evidence contradicts the observation (the lane never actually
                # held the exact target).
                if int(evidence.get("held_clients_at_gate") or 0) < TARGET_CLIENTS_PER_LANE:
                    runtime_lane.bell_to_10000_observed_ms = None
        if runtime is not None:
            runtime.state = "failed"
            for runtime_lane in runtime.lanes.values():
                if runtime_lane.phase != "verified":
                    runtime_lane.phase = "failed"
                    runtime_lane.status = (
                        f"{runtime_lane.held_clients:,} / "
                        f"{TARGET_CLIENTS_PER_LANE:,} clients held · "
                        "Bout stopped · cleanup underway"
                    )
        if setup is not None:
            for lane_id, lane in setup.lanes.items():
                if lane.state == RoundFiveSetupState.VERIFIED:
                    continue
                frozen_elapsed_ms = setup_elapsed_floors.get(lane_id)
                if frozen_elapsed_ms is not None:
                    lane.setup_elapsed_ms = frozen_elapsed_ms
                    lane.elapsed_at_snapshot_ms = frozen_elapsed_ms
                lane.state = RoundFiveSetupState.FAILED
                lane.verified = False
                lane.status = "Stopped after the Round 5 proof failed"
                lane.error = message
            setup.state = RoundFiveSetupState.FAILED
            setup.downstream_validated = False
            setup.failure = "Setup or downstream verification did not complete"
            setup.cleanup_retryable = False
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
            if lane.state == LaneState.VERIFIED:
                continue
            lane.state = LaneState.FAILED
            lane.status = message
            lane.error = message
            lane.activity = LaneActivity(phase="failed")
        self._advance_round5_revision_locked(record)
        try:
            await self._transition_bout_to_cleanup(record, record.snapshot.state)
        except InvalidStateError:
            logger.error(
                "Round 5 failure reached terminal state before its cleanup phase "
                "could be durably published session=%s",
                record.snapshot.id,
                exc_info=True,
            )
        snapshot = self._revalidated_snapshot(record.snapshot)
        record.round5_terminal_published = True
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        return True

    async def _finish_connection_spike_failure(
        self,
        record: SessionRecord,
        message: str,
    ) -> None:
        async with record.lock:
            published = await self._publish_connection_spike_failure_locked(
                record,
                message,
            )
        if published:
            await self._begin_connection_spike_cleanup_handoff(record)

    async def _retry_connection_spike_cleanup(
        self,
        record: SessionRecord,
        engine: object,
    ) -> bool:
        await self._refresh_round5_slot_from_durable(record)
        # If the durable head proves THIS claim's cleanup already
        # completed and the generation advanced (finished by another owner or the
        # supervised loop), the stale local claim/cleanup lease must be retired --
        # rather than begin_cleanup-ing a claim that no longer exists (WarmFenceLost)
        # and heartbeating a dead round5_cleanup lease that keeps the overlay
        # unstartable. Never retire on unrelated/blocking states: only when our claim
        # is gone from the head AND the generation advanced past it.
        coordinator = self._round5_warm_coordinator
        if coordinator is not None:
            durable = await self._read_round5_durable_slot()
            if durable is not None and self._round5_cleanup_completed_and_advanced(
                record, durable
            ):
                return await self._retire_completed_round5_cleanup(record, durable)
        async with record.lease_lock:
            lease = record.round5_lease
        if lease is None or lease.phase != "round5_cleanup":
            operator = record.operator
            if operator is not None:
                try:
                    await self._retain_connection_spike_cleanup_lease(
                        record,
                        operator,
                        session_state=record.snapshot.state,
                        allow_claim=True,
                    )
                except Exception:
                    pass
            async with record.lease_lock:
                lease = record.round5_lease
            if lease is None or lease.phase != "round5_cleanup":
                await self._mark_connection_spike_cleanup_pending(record)
                return False
        claim_id = (
            record.round5_warm_slot.claim.claim_id
            if record.round5_warm_slot is not None
            and record.round5_warm_slot.claim is not None
            else None
        )
        coordinator = self._round5_warm_coordinator
        converge = (
            getattr(coordinator, "converge_cleanup", None)
            if coordinator is not None
            else None
        )
        if coordinator is not None and claim_id is not None and callable(converge):
            # SINGLE-OWNER path (a warm coordinator exists): the manager requests +
            # awaits the coordinator's coalesced convergence and NEVER
            # settles/cancels/reconciles external resources itself. begin_cleanup is
            # a durable store CAS (ownership fence), not an external mutation.
            try:
                record.round5_warm_slot = await coordinator.begin_cleanup(claim_id)
            except Exception as exc:
                logger.error(
                    "Round 5 durable cleanup start is not settled session=%s "
                    "diagnostic=%s",
                    record.snapshot.id,
                    _redacted_exception_chain(exc),
                )
                await self._mark_connection_spike_cleanup_pending(record)
                return False
            # THE single external-mutation owner: the coordinator's coalesced
            # convergence performs the exact durable SETTLED + provider/journal
            # absence under the lease heartbeat, then finish_cleanup_and_rewarm to
            # generation N+1. Concurrent triggers (this worker, the supervised loop,
            # a takeover) collapse to one reconcile sequence; a lost fence aborts
            # without mutating.
            try:
                converged = await converge(claim_id)
            except Exception as exc:
                logger.error(
                    "Round 5 durable cleanup convergence is not settled session=%s "
                    "diagnostic=%s",
                    record.snapshot.id,
                    _redacted_exception_chain(exc),
                )
                await self._mark_connection_spike_cleanup_pending(record)
                return False
            if converged.state != Round5WarmState.WARMING:
                # Not yet proven SETTLED + absent; the slot stays CLEANING (visible
                # as cleanup owed) and the worker retries -- never a silent strand.
                record.round5_warm_slot = converged
                await self._mark_connection_spike_cleanup_pending(record)
                return False
            record.round5_warm_slot = converged
            record.round5_launch_capsule = None
        else:
            # LEGACY path: NO warm coordinator is configured, so there is nothing to
            # delegate to and the manager is the SOLE cleanup owner (still exactly
            # one owner -- not dual ownership). Drive the engine reconcile directly.
            reconcile = getattr(engine, "reconcile_failed_cleanup", None)
            if reconcile is None:
                await self._mark_connection_spike_cleanup_pending(record)
                return False
            if self._round5_prebell_cleanup_required(record):
                reconcile_abandoned = getattr(engine, "reconcile_abandoned_claim", None)
                claim = (
                    record.round5_warm_slot.claim
                    if record.round5_warm_slot is not None
                    else None
                )
                if not callable(reconcile_abandoned) or claim is None:
                    await self._mark_connection_spike_cleanup_pending(record)
                    return False
                try:
                    await reconcile_abandoned(claim)
                except Exception as exc:
                    logger.error(
                        "Round 5 pre-bell resident cleanup is not settled "
                        "session=%s diagnostic=%s",
                        record.snapshot.id,
                        _redacted_exception_chain(exc),
                    )
                    await self._mark_connection_spike_cleanup_pending(record)
                    return False
            else:
                try:
                    await reconcile(record.snapshot.id, lease.fencing_token)
                except Exception as exc:
                    logger.error(
                        "Round 5 cleanup is not settled session=%s diagnostic=%s",
                        record.snapshot.id,
                        _redacted_exception_chain(exc),
                    )
                    await self._mark_connection_spike_cleanup_pending(record)
                    return False
        self._require_round5_rewarm_transition(record)
        main_released = await self._release_bout(record)
        round5_released = (
            await self._release_round5_lease(record) if main_released else False
        )
        if main_released and round5_released:
            await self._mark_connection_spike_cleanup_complete(record)
        else:
            await self._mark_connection_spike_cleanup_pending(record)
        snapshot = await self.get(record.snapshot.id)
        return not (snapshot.round5_setup is not None and snapshot.round5_setup.cleanup_retryable)

    async def _read_round5_durable_slot(self):
        """Best-effort read of the authoritative durable warm slot (or None)."""

        coordinator = self._round5_warm_coordinator
        if coordinator is None:
            return None
        try:
            return await coordinator.store.read(coordinator.installation_id)
        except Exception:
            return None

    @staticmethod
    def _round5_cleanup_completed_and_advanced(record: SessionRecord, durable) -> bool:
        """Whether THIS claim's cleanup already completed and advanced the generation.

        True only when our claim is no longer the durable head AND the durable
        generation has advanced past the one the claim was created against -- i.e.
        finish_cleanup_and_rewarm ran (here or under another owner). A head still
        bearing our claim (cleanup in progress), or at/below our generation (a stall
        or a block on our own generation), is NOT complete, so this never retires on
        an unrelated or blocking state.
        """

        slot = record.round5_warm_slot
        claim = getattr(slot, "claim", None) if slot is not None else None
        if claim is None or durable is None:
            return False
        durable_claim = getattr(durable, "claim", None)
        durable_claim_id = (
            durable_claim.claim_id if durable_claim is not None else None
        )
        return (
            durable_claim_id != claim.claim_id
            and durable.generation > claim.capsule_generation
        )

    async def _retire_completed_round5_cleanup(
        self, record: SessionRecord, durable
    ) -> bool:
        """Retire a stale local claim whose cleanup completed+advanced elsewhere.

        Adopts the durable head, drops the stale launch capsule, stops the cleanup
        heartbeat and releases the cleanup + ring leases so the overlay is startable
        again. Marks the local cleanup complete; never issues external mutation.
        """

        record.round5_warm_slot = durable
        record.round5_launch_capsule = None
        main_released = await self._release_bout(record)
        round5_released = (
            await self._release_round5_lease(record) if main_released else False
        )
        if main_released and round5_released:
            await self._mark_connection_spike_cleanup_complete(record)
            return True
        await self._mark_connection_spike_cleanup_pending(record)
        return False

    async def _refresh_round5_slot_from_durable(self, record: SessionRecord) -> None:
        """Refresh the cached warm slot from durable truth for cleanup classification.

        Cleanup's pre/post-bell classification must be derivable from the
        COMMITTED store keyed by this claim, not only a local cache that a crash
        between accept_bell and the local update could have left stale. Best-effort
        and same-claim only (never clobbers across generations).
        """

        coordinator = self._round5_warm_coordinator
        if coordinator is None:
            return
        try:
            durable = await coordinator.store.read(coordinator.installation_id)
        except Exception:
            return
        if durable is None:
            return
        durable_claim = getattr(durable, "claim", None)
        if durable_claim is None:
            return
        cached = record.round5_warm_slot
        cached_claim = getattr(cached, "claim", None) if cached is not None else None
        if cached is None or (
            cached_claim is not None and cached_claim.claim_id == durable_claim.claim_id
        ):
            record.round5_warm_slot = durable

    async def _cleanup_connection_spike(self, record: SessionRecord) -> bool:
        # Adopt durable post-bell truth before classifying pre/post-bell cleanup.
        await self._refresh_round5_slot_from_durable(record)
        # When a warm coordinator is configured it (via converge_cleanup ->
        # provider.reconcile on the ADOPTED claimed engine) is the SOLE external
        # cleanup janitor -- staged-resident cancel, per-bout Proxy delete, journal
        # absence. The manager therefore performs NO external mutation here: it only
        # drops its local, non-mutating setup cache. This eliminates the dual-mutation
        # where the manager ran the full janitor while the coordinator reconciled a
        # fresh engine. (The legacy branch below is the degraded/no-coordinator SOLE
        # owner; see _round5_cleanup_uses_coordinator.)
        if self._round5_warm_coordinator is not None:
            record.connection_spike_setup_result = None
            return True
        engine = record.connection_spike_engine
        arm = record.connection_spike_arm
        settle_abandoned = (
            getattr(engine, "settle_abandoned_arm", None) if engine is not None else None
        )
        cleanup = getattr(engine, "cancel_and_cleanup", None) if engine is not None else None
        setup_cleanup = (
            getattr(engine, "cancel_setup_and_settle", None) if engine is not None else None
        )
        ok = True
        # Unstage the Lakebase resident staged at ARM before any other teardown.
        # ARM prepares that resident ahead of the bell, so on a pre-bell abandon
        # (armed-TTL expiry, refused bell, arm failure) it is not represented in
        # the dispatched-lane cleanup below and would otherwise stay resident,
        # colliding with the next warm generation's wait_agent_ready ("resident
        # readiness binding changed") and latching the slot BLOCKED
        # (warm_baseline_unexpected). Idempotent and a no-op once settled, so it
        # is safe on every cleanup path including a fully-run bout.
        try:
            if settle_abandoned is not None:
                task = asyncio.create_task(settle_abandoned())
                deferred = await asyncio.shield(task)
                if deferred:
                    ok = False
                    logger.error(
                        "Round 5 abandoned-arm resident settlement remains deferred "
                        "session=%s jobs=%d; cleanup fence retained",
                        record.snapshot.id,
                        len(deferred),
                    )
        except Exception as exc:
            ok = False
            logger.error(
                "Round 5 abandoned-arm resident settle failed session=%s diagnosis=%s; "
                "the warm slot is held closed until it settles",
                record.snapshot.id,
                operator_diagnosis(exc),
                exc_info=True,
            )
        try:
            if cleanup is not None and arm is not None:
                task = asyncio.create_task(cleanup(arm))
                await asyncio.shield(task)
        except Exception:
            ok = False
        # A pre-bell abandon (the claim never crossed the bell transaction) created
        # no per-bout Proxy and no timed-setup resources, so it must NOT invoke the
        # post-setup teardown seam. ``cancel_setup_and_settle`` tears down timed
        # setup a pre-bell claim never ran and, on the real engine, raises "setup
        # cleanup bout is stale" (no ``_setup_bout_id`` was recorded), turning a
        # clean unstage into a spurious cleanup failure. The staged ARM residents
        # are already settled above by ``settle_abandoned_arm``; the durable
        # resident reconcile (``reconcile_abandoned_claim``) proves provider
        # absence on the retry worker.
        prebell = self._round5_prebell_cleanup_required(record)
        try:
            if setup_cleanup is not None and not prebell:
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
        held = cls._round_five_value(lane, "held_clients_at_gate")
        if held is not None:
            gates = cls._round_five_value(lane, "gates")
            fanin_fields = (
                "initiated_clients",
                "authenticated_clients",
                "cancelled_clients",
                "held_clients_at_gate",
                "terminal_failures",
                "failure_codes",
                "connection_diagnostics",
                "retries",
                "disconnected_during_hold",
                "time_to_target_ns",
                "time_to_target_ms",
                "hold_elapsed_ms",
                "sampled_queries_attempted",
                "sampled_queries_succeeded",
                "sampled_queries_failed",
                "preexisting_client_role_sessions",
                "observer_role",
                "client_role",
                "observer_direct",
                "current_backend_sessions",
                "peak_backend_sessions",
                "unique_backend_pids",
                "distinct_socket_fds",
                "distinct_local_endpoints",
                "socket_identity_sha256",
                "connect_latency_p50_ms",
                "connect_latency_p95_ms",
                "connect_latency_p99_ms",
                "endpoint_host_sha256",
                "credential_sha256",
                "observer_credential_sha256",
                "tls_mode",
                "auth_method",
                "config_sha256",
                "generator_sha256",
                "capacity_model_sha256",
                "telemetry_samples",
                "telemetry_physical_memory_bytes",
                "telemetry_min_available_memory_bytes",
                "telemetry_peak_rss_bytes",
                "telemetry_fd_soft_limit",
                "telemetry_peak_open_fds",
                "telemetry_ephemeral_port_count",
                "telemetry_peak_ephemeral_ports_in_use",
                "telemetry_min_ephemeral_port_reserve",
                "telemetry_peak_event_loop_p99_ms",
                "telemetry_peak_raw_event_loop_p99_ms",
                "telemetry_peak_external_event_loop_p99_ms",
                "telemetry_peak_cpu_capacity_fraction",
                "telemetry_failures",
                "telemetry_advisories",
                "safety_evidence_version",
                "hard_safety_verified",
                "port_accounting_verified",
                "admission_controller_min_concurrency",
                "admission_controller_reductions",
                "admission_controller_recoveries",
                "admission_controller_throttled_ms",
                "admission_controller_recovery_hysteresis_intervals",
                "admission_controller_pressure_hysteresis_intervals",
                "launch_skew_ms",
                "achieved_elapsed_ms",
            )
            evidence = {
                name: cls._round_five_value(lane, name)
                for name in fanin_fields
            }
            evidence.update(
                {
                    "schema_version": FANIN_SCHEMA_VERSION,
                    "protocol": ROUND5_FANIN_PROTOCOL,
                    "held_clients": held,
                    "telemetry_verified": bool(
                        cls._round_five_value(gates, "telemetry", False)
                    ),
                }
            )
            return evidence
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
        latencies = evidence.get("successful_latency_ms", ())
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
        verified = bool(cls._round_five_value(raw, "verified", gate_passed))
        held = cls._round_five_value(raw, "held_clients_at_gate")
        if held is not None:
            required_gates = (
                "exact_count",
                "zero_failures",
                "hold",
                "sampled_queries",
                "multiplexing",
                "identity",
                "observer_separation",
                "clean_start",
                "fairness",
                "telemetry",
                "cleanup",
            )
            failure_codes = evidence.get("failure_codes")
            telemetry_failures = evidence.get("telemetry_failures")
            ephemeral_count = cls._round_five_count(
                evidence.get("telemetry_ephemeral_port_count")
            )
            ephemeral_used = cls._round_five_count(
                evidence.get("telemetry_peak_ephemeral_ports_in_use")
            )
            ephemeral_remaining = cls._round_five_count(
                evidence.get("telemetry_min_ephemeral_port_reserve")
            )
            physical_memory = cls._round_five_count(
                evidence.get("telemetry_physical_memory_bytes")
            )
            peak_rss = cls._round_five_count(
                evidence.get("telemetry_peak_rss_bytes")
            )
            fd_soft = cls._round_five_count(
                evidence.get("telemetry_fd_soft_limit")
            )
            peak_fds = cls._round_five_count(
                evidence.get("telemetry_peak_open_fds")
            )
            return bool(
                verified
                and gate_passed is True
                and all(
                    cls._round_five_value(gates, name) is True
                    for name in required_gates
                )
                and not cls._round_five_value(gates, "failures", ())
                and evidence.get("schema_version") == FANIN_SCHEMA_VERSION
                and evidence.get("protocol") == ROUND5_FANIN_PROTOCOL
                and cls._round_five_count(evidence.get("initiated_clients"))
                == TARGET_CLIENTS_PER_LANE
                and cls._round_five_count(evidence.get("authenticated_clients"))
                == TARGET_CLIENTS_PER_LANE
                and cls._round_five_count(held) == TARGET_CLIENTS_PER_LANE
                and cls._round_five_count(evidence.get("cancelled_clients")) == 0
                and cls._round_five_count(evidence.get("terminal_failures")) == 0
                and isinstance(failure_codes, Mapping)
                and not any(failure_codes.values())
                and 0 <= cls._round_five_count(evidence.get("retries")) <= ROUND5_MAX_RETRIES
                and cls._round_five_count(evidence.get("disconnected_during_hold"))
                == 0
                and cls._round_five_number(evidence.get("time_to_target_ms"))
                is not None
                and cls._round_five_number(evidence.get("hold_elapsed_ms")) is not None
                and float(evidence["hold_elapsed_ms"]) >= 30_000
                and cls._round_five_count(
                    evidence.get("sampled_queries_attempted")
                )
                == 64
                and cls._round_five_count(
                    evidence.get("sampled_queries_succeeded")
                )
                == 64
                and cls._round_five_count(evidence.get("sampled_queries_failed")) == 0
                and cls._round_five_count(evidence.get("distinct_socket_fds"))
                == TARGET_CLIENTS_PER_LANE
                and cls._round_five_count(evidence.get("distinct_local_endpoints"))
                == TARGET_CLIENTS_PER_LANE
                and evidence.get("safety_evidence_version")
                == SAFETY_EVIDENCE_VERSION
                and evidence.get("hard_safety_verified") is True
                and evidence.get("port_accounting_verified") is True
                and evidence.get(
                    "admission_controller_recovery_hysteresis_intervals"
                )
                == 3
                and evidence.get(
                    "admission_controller_pressure_hysteresis_intervals"
                )
                == 2
                and isinstance(telemetry_failures, (list, tuple))
                and not telemetry_failures
                and cls._round_five_count(evidence.get("telemetry_samples")) > 0
                and cls._round_five_count(
                    evidence.get("telemetry_min_available_memory_bytes")
                )
                >= 768 * 1024 * 1024
                and physical_memory >= peak_rss + 768 * 1024 * 1024
                and fd_soft > 0
                and 0 <= peak_fds <= math.floor(fd_soft * 0.80)
                and ephemeral_used >= TARGET_CLIENTS_PER_LANE
                and ephemeral_remaining >= 2_000
                and ephemeral_count == ephemeral_used + ephemeral_remaining
            )
        return verified and (
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
        # Mapping-aware: a stop-gate fact survives a JSON/dict round trip (e.g.
        # a persisted terminal receipt reloaded as plain dicts) as well as the
        # in-process ``PublicSetupEvidence`` dataclass.  The strict sanitizer
        # below is unchanged; only where the key/value are read from differs.
        if isinstance(value, Mapping):
            key = str(value.get("key", "")).strip()
            fact = value.get("value", None)
        else:
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

    #: Fixed, secret-free FATAL evidence/anti-cheat subcodes that void the bout.
    #: Extended only with new fixed labels; never with runtime/provider strings.
    _ROUND_FIVE_SETUP_LANE_FAILURES = (
        # Retired labels; still accepted so a legacy sealed receipt stays legible.
        "workflow_launch_window",
        # Fatal evidence/provenance/clock-domain faults.
        "workflow_launch_ordering",
        "create_db_proxy_pre_bell",
        "create_db_proxy_missing",
        "stop_gate_evidence",
        "stop_gate_before_workflow_launch",
        "setup_deadline",
        "setup_error",
        "setup_failed",
        "setup_towelled",
    )
    #: Fixed, secret-free NON-FATAL scheduling-conformance advisories. These are
    #: surfaced for play-by-play but never void an exact, honestly-obtained bout.
    _ROUND_FIVE_SETUP_LANE_ADVISORIES = (
        "create_db_proxy_window",
        "workflow_launch_skew",
    )
    _ROUND_FIVE_PUBLIC_FACT_REJECTED = "public_fact_key_rejected"

    @classmethod
    def _round_five_setup_lane_diagnostic(
        cls,
        raw: object,
        *,
        core_verified: bool,
        gate_dropped: bool,
    ) -> str | None:
        """Reduce a lane result to one durable, secret-free finalizer subcode.

        A public-projection rejection (core proof verified but a fact key was
        redacted) is reported distinctly from a genuine stop-gate failure, so a
        reader can tell a sanitizer collision from a real setup miss. Only fixed
        labels are ever emitted.
        """

        subcodes: list[str] = []
        if gate_dropped:
            subcodes.append(cls._ROUND_FIVE_PUBLIC_FACT_REJECTED)
        raw_failures = cls._round_five_value(raw, "failures", ())
        if isinstance(raw_failures, (list, tuple)):
            for failure in raw_failures:
                label = str(getattr(failure, "value", failure))
                if label in cls._ROUND_FIVE_SETUP_LANE_FAILURES and label not in subcodes:
                    subcodes.append(label)
        if not subcodes and not core_verified:
            subcodes.append("setup_unverified")
        return ";".join(subcodes) if subcodes else None

    @classmethod
    def _round_five_setup_lane_advisories(cls, raw: object) -> str | None:
        """Reduce a lane's NON-FATAL scheduling advisories to a fixed-label string.

        Only known advisory labels are emitted, so an advisory can be surfaced in
        the play-by-play without ever being confused for a fatal evidence fault.
        """

        codes: list[str] = []
        raw_advisories = cls._round_five_value(raw, "scheduling_advisories", ())
        if isinstance(raw_advisories, (list, tuple)):
            for advisory in raw_advisories:
                label = str(getattr(advisory, "value", advisory))
                if label in cls._ROUND_FIVE_SETUP_LANE_ADVISORIES and label not in codes:
                    codes.append(label)
        return ";".join(codes) if codes else None

    @classmethod
    def _round_five_setup_snapshot(
        cls,
        snapshot: SessionSnapshot,
        result: object | None,
        *,
        terminal: bool,
    ) -> RoundFiveSetupSnapshot:
        from .connection_spike import MAX_SETUP_REQUEST_LAUNCH_DELAY_MS

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
            core_verified = bool(cls._round_five_value(raw, "verified", False))
            gate = cls._round_five_public_gate(cls._round_five_value(raw, "stop_gate_evidence"))
            verified = core_verified and gate is not None
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
            lane.workflow_launch_delay_ms = cls._round_five_number(
                cls._round_five_value(raw, "workflow_launch_delay_ms")
            )
            lane.create_db_proxy_request_delta_ms = cls._round_five_number(
                cls._round_five_value(raw, "create_db_proxy_request_delta_ms")
            )
            lane.stop_gate_evidence = gate
            lane.verified = verified
            # Durable, secret-free finalizer subcode. Fixed labels only: the core
            # gate failures come from ``finalize_setup_lane`` and a projection
            # rejection is flagged explicitly, so the exact reason survives the
            # terminal receipt and auto-cleanup instead of collapsing to generic
            # text. Never fabricated from the burst result.
            lane.setup_diagnostic = cls._round_five_setup_lane_diagnostic(
                raw,
                core_verified=core_verified,
                gate_dropped=core_verified and gate is None,
            )
            # Non-fatal scheduling advisories are recorded separately from the
            # fatal diagnostic so a verified lane can still carry, e.g., a slow
            # CreateDBProxy note for the play-by-play without being marked failed.
            lane.scheduling_advisory = cls._round_five_setup_lane_advisories(raw)
            lane.status = (
                "Setup stop gate verified" if verified else "Setup stop gate did not verify"
            )
            lane.error = None if verified else "Setup verification failed"
            # Always publish the competitor's observed CreateDBProxy request delta
            # (pass or fail) so "12 ms vs an advisory 163 ms" is visible in the
            # log. None prints for a missing stamp, which IS a fatal fault.
            if lane.create_db_proxy_request_delta_ms is not None or str(lane_id) == "competitor":
                logger.warning(
                    "Round 5 CreateDBProxy timing session=%s lane=%s "
                    "create_db_proxy_request_delta_ms=%s advisory_ms=%.0f verified=%s advisory=%s",
                    snapshot.id,
                    str(lane_id),
                    (
                        f"{lane.create_db_proxy_request_delta_ms:.3f}"
                        if lane.create_db_proxy_request_delta_ms is not None
                        else "None"
                    ),
                    MAX_SETUP_REQUEST_LAUNCH_DELAY_MS,
                    verified,
                    lane.scheduling_advisory or "none",
                )
            if not verified and lane.setup_diagnostic:
                logger.warning(
                    "Round 5 setup lane finalizer session=%s lane=%s subcode=%s",
                    snapshot.id,
                    str(lane_id),
                    lane.setup_diagnostic,
                )
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
    def _round_five_v4_comparison(
        runtime: RoundFiveRuntimeSnapshot,
        competitor_name: str,
    ) -> ComparisonSnapshot | None:
        left = runtime.lanes["lakebase"].bell_to_10000_observed_ms
        right = runtime.lanes["competitor"].bell_to_10000_observed_ms
        if left is None or right is None:
            return None
        if left == right:
            return ComparisonSnapshot(
                kind=ComparisonKind.TIE,
                detail="Both lanes reached the exact 10,000-held gate together.",
            )
        winner = "lakebase" if left < right else "competitor"
        margin_ms = abs(left - right)
        return ComparisonSnapshot(
            kind=ComparisonKind.MEASURED,
            winner_lane_id=winner,
            margin=MetricValue(
                spec_id="bell_to_10000_observed_ms",
                lane_id=winner,
                value=margin_ms,
                display_value=f"{margin_ms:.2f} ms",
            ),
            detail=(
                f"{'Lakebase' if winner == 'lakebase' else competitor_name} "
                f"reached exactly 10,000 held clients {margin_ms:.2f} ms sooner "
                "on the server-authoritative bell clock."
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
        """Round 4's bell: both integrations start from parked and one change is committed.

        Every lane's clock runs from this one bell to its own first exact read, on
        the controller's clock (``server/round4_race.py``). Nothing here waits on
        a status API; the verdict comes from the lanes' reads alone.
        """

        engine = record.model_score_engine
        arm = record.model_score_arm
        update = record.model_score_pending_update
        if engine is None or arm is None or update is None:
            await self._fail(record, "Round 4 must be prepared again.")
            return

        async with record.lock:
            if not self._committed_run_active_locked(record):
                return
            started_at = datetime.now(UTC)
            record.snapshot.state = SessionState.RUNNING
            record.snapshot.run_started_at = started_at
            # Every clock starts at the bell, which the engine rings only once the verifier's
            # connections are open and awake (3 s live, more for a paused Aurora). Until then the
            # lanes stay sealed and nothing is timed; the bell's own report stamps the origin
            # the snapshot floors and the towel read from.
            record.run_started_monotonic_ns = None
            record.snapshot.updated_at = started_at
            for lane_id in arm.lanes:
                lane = record.snapshot.lanes[lane_id]
                lane.state = LaneState.SEALED
                lane.status = "Opening the verifier's connection before the bell"
                lane.attempts = 0
                lane.elapsed_ms = None
                lane.activity = LaneActivity(phase=Round4Phase.ARMED)
            running_snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "run_started",
            {
                "state": SessionState.RUNNING,
                "lanes": list(arm.lanes),
                "session": running_snapshot.model_dump(mode="json"),
            },
        )

        async def on_progress(
            lane_id: str,
            phase: Round4Phase,
            status: str,
            elapsed_ms: float | None,
        ) -> None:
            await self._apply_model_score_progress(record, lane_id, phase, status, elapsed_ms)

        try:
            result = await engine.run(arm, update, on_progress)
        except asyncio.CancelledError:
            raise
        except (ModelScoreError, Round4RaceError) as exc:
            statement_failure = statement_execution_failure(exc)
            if statement_failure is not None:
                self._log_bout_refusal(record, exc, round_number=4)
            await self._await_model_score_terminal(
                record,
                self._finish_model_score_failure(
                    record,
                    (
                        "Round 4 failed a backstage data check. The operator log has the "
                        "provider diagnosis."
                        if statement_failure is not None
                        else str(exc)
                    ),
                ),
            )
            return
        except Exception as exc:
            self._log_bout_refusal(record, exc, round_number=4)
            await self._await_model_score_terminal(
                record,
                self._finish_model_score_failure(
                    record,
                    "Round 4 failed unexpectedly.",
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
        lane_id: str,
        phase: Round4Phase,
        status: str,
        elapsed_ms: float | None,
    ) -> None:
        phase_states = {
            Round4Phase.STARTING: LaneState.CONNECTING,
            Round4Phase.WAITING: LaneState.VERIFYING,
            Round4Phase.VERIFIED: LaneState.VERIFIED,
            Round4Phase.FAILED: LaneState.FAILED,
        }
        async with record.lock:
            if record.snapshot.towel is not None or lane_id not in record.snapshot.lanes:
                return
            if phase == Round4Phase.STARTING and record.run_started_monotonic_ns is None:
                # The bell: the engine reports every lane STARTING at elapsed zero.
                record.run_started_monotonic_ns = self._clock_ns()
            lane = record.snapshot.lanes[lane_id]
            lane.state = phase_states.get(phase, lane.state)
            lane.status = status
            lane.activity = LaneActivity(phase=phase)
            if elapsed_ms is not None:
                lane.elapsed_ms = (
                    elapsed_ms
                    if phase == Round4Phase.VERIFIED
                    else max(lane.elapsed_ms or 0.0, elapsed_ms)
                )
            if lane.state == LaneState.VERIFIED and lane.verified_at is None:
                lane.verified_at = datetime.now(UTC)
            record.snapshot.updated_at = datetime.now(UTC)
            activity = lane.activity.model_dump(mode="json")
            state = lane.state
            elapsed = lane.elapsed_ms
        await record.event_log.publish(
            "lane_update",
            {
                "lane_id": lane_id,
                "state": state,
                "elapsed_ms": elapsed,
                "status": status,
                "activity": activity,
            },
        )

    async def _finish_model_score(
        self,
        record: SessionRecord,
        result: Round4RaceResult,
    ) -> None:
        async with record.lock:
            if record.snapshot.towel is not None:
                return
        lease_outcome = await self._settle_model_score_terminal_lease(record)
        if lease_outcome == "lost":
            await self._finish_model_score_lease_loss(record)
            return
        async with record.lock:
            if record.model_score_terminal_published or record.snapshot.towel is not None:
                return
            record.model_score_result = result
            record.model_score_pending_update = None
            measured = self._apply_model_score_result(record.snapshot, result)
            record.snapshot.state = SessionState.VERIFIED if measured else SessionState.FAILED
            record.snapshot.failure = None if measured else record.snapshot.comparison.detail
            record.snapshot.remembered_result = (
                _round_four_remembered_result(record.snapshot) if measured else None
            )
            # Round 4 has no re-do in v1.1: a re-do could not start cold. Racing again
            # is a new bout, which meets both lanes parked.
            record.snapshot.redo = None
            record.snapshot.updated_at = datetime.now(UTC)
            record.armed_at_monotonic = None
            record.model_score_terminal_published = True
            engine = record.model_score_engine
            snapshot = record.snapshot.model_copy(deep=True)
            self._schedule_round_settlement(
                record,
                getattr(engine, "settle_and_restore_baseline", None),
                label="Round 4",
            )
        if measured:
            await record.event_log.publish(
                "run_finished",
                {"state": SessionState.VERIFIED, "session": snapshot.model_dump(mode="json")},
                receipt_bout_id=snapshot.receipt_bout_id,
            )
            return
        self._log_lane_refusals(record, round_number=4)
        await record.event_log.publish(
            "session_failed",
            {
                "state": SessionState.FAILED,
                "message": snapshot.failure,
                "session": snapshot.model_dump(mode="json"),
            },
            receipt_bout_id=snapshot.receipt_bout_id,
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
            await self._finish_model_score_lease_loss(record)
            return
        async with record.lock:
            if record.model_score_terminal_published or record.snapshot.towel is not None:
                return
            failed_at = datetime.now(UTC)
            for lane in record.snapshot.lanes.values():
                if lane.state in {LaneState.NOT_SUPPORTED, LaneState.VERIFIED}:
                    continue
                lane.state = LaneState.FAILED
                lane.error = message
                lane.status = "Round 4 could not be verified"
                lane.activity = LaneActivity(phase=Round4Phase.FAILED)
            record.model_score_pending_update = None
            record.model_score_result = None
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            # The one funnel every Round 4 bout failure passes through, including the
            # typed errors that carry the real reason and have no log of their own.
            self._log_lane_refusals(record, round_number=4)
            record.snapshot.remembered_result = None
            record.snapshot.redo = None
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
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        self._schedule_round_settlement(
            record,
            getattr(engine, "settle_and_restore_baseline", None),
            label="Round 4",
        )

    async def _finish_model_score_lease_loss(self, record: SessionRecord) -> None:
        async with record.lock:
            if record.model_score_terminal_published or record.snapshot.towel is not None:
                return
            failed_at = datetime.now(UTC)
            message = "Round 4's ring lease was lost before the result could be published."
            for lane in record.snapshot.lanes.values():
                if lane.state == LaneState.NOT_SUPPORTED:
                    continue
                lane.state = LaneState.FAILED
                lane.error = message
                lane.status = message
                lane.activity = LaneActivity(phase=Round4Phase.FAILED)
            record.model_score_result = None
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            record.snapshot.remembered_result = None
            record.model_score_pending_update = None
            record.snapshot.updated_at = failed_at
            record.armed_at_monotonic = None
            record.model_score_terminal_published = True
            engine = record.model_score_engine
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "session_failed",
            {
                "state": snapshot.state,
                "message": message,
                "session": snapshot.model_dump(mode="json"),
            },
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        # The bout's row and running lanes are still owed their settlement.
        self._schedule_round_settlement(
            record,
            getattr(engine, "settle_and_restore_baseline", None),
            label="Round 4",
        )

    @staticmethod
    def _apply_model_score_result(
        snapshot: SessionSnapshot,
        result: Round4RaceResult,
    ) -> bool:
        """Put a bout's lanes, metrics and verdict on the card. True when it measured something.

        The verdict is the engine's, from the lanes' own reads (``round4_race.resolve``);
        nothing here recomputes it.
        """

        update = result.update
        metrics: list[MetricValue] = []
        for lane_id, outcome in result.outcomes.items():
            lane = snapshot.lanes[lane_id]
            evidence: dict[str, Any] = {
                "primary_key": update.entity_id,
                "score": update.score,
                "model_version": update.model_version,
                "proof_nonce": update.proof_nonce,
                "delta_version": result.commit.version,
                "delta_commit_time": result.commit.committed_at,
                "reads": outcome.reads,
                "max_read_gap_ms": outcome.max_read_gap_ms,
                "last_negative_ms": outcome.last_negative_ms,
                "bell_at": result.bell.rung_at,
                "protocol": result.protocol,
                **dict(outcome.evidence),
            }
            lane.evidence = evidence
            lane.attempts = outcome.reads
            if outcome.verified and outcome.elapsed_ms is not None:
                lane.state = LaneState.VERIFIED
                lane.elapsed_ms = outcome.elapsed_ms
                lane.status = "The exact row is in the application"
                lane.error = None
                lane.verified_at = lane.verified_at or datetime.now(UTC)
                lane.activity = LaneActivity(phase=Round4Phase.VERIFIED)
                metrics.append(
                    MetricValue(
                        spec_id="bell_to_exact_read_ms",
                        lane_id=lane_id,
                        value=outcome.elapsed_ms,
                        display_value=f"{outcome.elapsed_ms / 1000:.2f} s",
                    )
                )
            else:
                lane.state = LaneState.FAILED
                lane.elapsed_ms = None
                lane.status = (
                    "Did not deliver the row within its bound"
                    if outcome.timed_out
                    else "Could not be measured"
                )
                lane.error = outcome.failure
                if outcome.timed_out and outcome.bound_ms is not None:
                    # Still running at its bound, which is a result, not an error.
                    lane.error = None
                    lane.evidence.update(_floor_evidence(outcome.bound_ms))
                lane.activity = LaneActivity(phase=Round4Phase.FAILED)
            metrics.append(
                MetricValue(
                    spec_id="exact_row_verified",
                    lane_id=lane_id,
                    value=bool(outcome.verified),
                    display_value="Verified" if outcome.verified else "Not verified",
                )
            )
        sync_ms = result.outcomes[LAKEBASE_LANE].evidence.get("managed_availability_ms")
        if isinstance(sync_ms, int | float):
            metrics.append(
                MetricValue(
                    spec_id="managed_availability_ms",
                    lane_id=LAKEBASE_LANE,
                    value=float(sync_ms),
                    display_value=f"{float(sync_ms) / 1000:.2f} s",
                )
            )
        metrics.append(
            MetricValue(
                spec_id="delta_commit_version",
                value=result.commit.version,
                display_value=str(result.commit.version),
            )
        )
        snapshot.metrics = metrics
        snapshot.comparison, measured = _round_four_comparison(
            result,
            snapshot.lanes[COMPETITOR_LANE].name,
            has_competitor=COMPETITOR_LANE in result.outcomes,
        )
        return measured

    async def _run(self, record: SessionRecord) -> None:
        assert record.live_targets is not None
        await record.event_log.publish(
            "run_preparing",
            {"state": record.snapshot.state},
        )
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
            self._log_bout_refusal(record, revalidation_error, round_number=1)
            await self._fail(
                record,
                "Start state changed before the bell; no timed transaction began. "
                "The operator log has the diagnosis.",
            )
            return
        if not all(isinstance(item, dict) for item in checks):
            await self._fail(record, "The live start state could not be revalidated.")
            return

        async with record.lock:
            if not self._committed_run_active_locked(record):
                return
            record.run_started_monotonic_ns = self._clock_ns()
            record.snapshot.updated_at = datetime.now(UTC)
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
                if (
                    record.snapshot.towel is not None
                    or not self._committed_run_active_locked(record)
                ):
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
                connection_closed_at = payload.get("connection_closed_at")
                if isinstance(connection_closed_at, datetime):
                    lane.connection_closed_at = connection_closed_at
                if lane.state == LaneState.VERIFIED and lane.verified_at is None:
                    lane.verified_at = lane.connection_closed_at or datetime.now(UTC)
                record.snapshot.updated_at = datetime.now(UTC)
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
        await record.event_log.publish(
            "run_preparing",
            {"state": record.snapshot.state},
        )

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
                if (
                    record.snapshot.towel is not None
                    or not self._committed_run_active_locked(record)
                ):
                    return
                if (
                    progress.phase not in {SafeChangePhase.PREFLIGHT, SafeChangePhase.ARMED}
                    and record.run_started_monotonic_ns is None
                ):
                    record.run_started_monotonic_ns = self._clock_ns()
                    record.snapshot.updated_at = datetime.now(UTC)
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
            self._log_bout_refusal(record, exc, round_number=2)
            await self._fail(
                record,
                "The isolated-change start state changed before the proof. "
                "The operator log has the diagnosis.",
            )
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
        await record.event_log.publish(
            "run_preparing",
            {"state": record.snapshot.state},
        )
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
                if not self._committed_run_active_locked(record):
                    return
                record.snapshot.updated_at = datetime.now(UTC)
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
                if (
                    (stop_control is not None and stop_control.event.is_set())
                    or not self._committed_run_active_locked(record)
                ):
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
            self._log_bout_refusal(record, exc, round_number=3)
            await self._fail(
                record,
                "The recovery start state changed before the proof. "
                "The operator log has the diagnosis.",
            )
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
        stoppage = _finished_over_a_bound(
            {lane_id: lane.ok for lane_id, lane in result.lanes.items()},
            {lane_id: lane.timed_out for lane_id, lane in result.lanes.items()},
        )
        async with record.lock:
            if not self._committed_run_active_locked(record):
                return
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
            if stoppage is not None:
                _adjudicate_bound(
                    record.snapshot, *stoppage, finished_task="recovered the exact order"
                )
                self._log_bound_stoppage(record, *stoppage, round_number=3)
            if result.all_verified or stoppage is not None:
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
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        if cleanup_owned:
            self._schedule_owned_artifact_cleanup(record, recovery=True)

    async def _finish_safe_change(
        self,
        record: SessionRecord,
        result: SafeChangeRunResult,
    ) -> None:
        cleanup_owned = False
        stoppage = _finished_over_a_bound(
            {
                lane_id: lane.state == SafeChangeLaneState.VERIFIED
                for lane_id, lane in result.lanes.items()
            },
            {lane_id: lane.timed_out for lane_id, lane in result.lanes.items()},
        )
        async with record.lock:
            if (
                record.snapshot.towel is not None
                or not self._committed_run_active_locked(record)
            ):
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
            if stoppage is not None:
                _adjudicate_bound(
                    record.snapshot, *stoppage, finished_task="verified the isolated schema change"
                )
                self._log_bound_stoppage(record, *stoppage, round_number=2)
            if result.all_verified or stoppage is not None:
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
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        if cleanup_owned:
            self._schedule_owned_artifact_cleanup(record, recovery=False)

    async def _finish(self, record: SessionRecord, result: VerificationResult) -> None:
        cleanup_owned = False
        async with record.lock:
            if (
                record.snapshot.towel is not None
                or not self._committed_run_active_locked(record)
            ):
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
            self._cancel_idle_watch(record)
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
            receipt_bout_id=snapshot.receipt_bout_id,
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
            receipt_bout_id=snapshot.receipt_bout_id,
        )

    async def _settle_towel_cost_window(self, record: SessionRecord) -> None:
        """Close a cost window the towel could not close inline.

        ``start_towel`` refuses to let the ledger stop it, so a close failure
        there is recorded and deferred to here, and retried on every cleanup
        attempt -- including an operator's retry click. This runs only on the
        path that is about to release the ring, and if the close still fails or
        goes unanswered at that point there is nothing left to retry against:
        the record would otherwise be pinned forever by ``_releasable``'s
        open-cost-bout check, which is a memory leak reached through the towel.
        So the local pointer is dropped and the ledger row is left to
        reconciliation, said out loud.
        """

        async with record.lock:
            bout_id = record.cost_bout_id
            towel = record.snapshot.towel
            if bout_id is None or towel is None:
                return
            try:
                closed = await self._close_towel_cost_window(record, towel, bout_id=bout_id)
            except Exception as exc:
                failure = str(exc) or "The bout cost window could not be closed"
                diagnostic = _redacted_exception_chain(exc)
            else:
                if closed:
                    towel.cost_close_failure = None
                    return
                failure = "The bout cost window close went unanswered"
                diagnostic = f"unanswered after {self._cost_close_wait:.1f}s"
            logger.error(
                "Towel cleanup could not close the cost window session=%s "
                "diagnostic=%s; abandoning it to reconciliation",
                record.snapshot.id,
                diagnostic,
            )
            record.cost_bout_id = None
            record.cost_bout_started_at = None
            record.cost_bout_kind = None
            towel.cost_close_failure = (
                f"{failure} · the open ledger window was left for reconciliation"
            )

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
            if round_id in _TOWEL_COOLDOWN_ROUNDS:
                self._cancel_idle_watch(record)
                record.snapshot.cooldown = self._new_towel_cooldown(record)
            record.snapshot.updated_at = datetime.now(UTC)
            snapshot = record.snapshot.model_copy(deep=True)
        await record.event_log.publish(
            "towel_update",
            {"session": snapshot.model_dump(mode="json")},
            receipt_bout_id=snapshot.receipt_bout_id,
        )

        if round_id == RoundId.SURVIVE_CONNECTION_SPIKE:
            await self._begin_connection_spike_cleanup_handoff(record)
            return

        settlement_error: str | None = None
        retry_deadline = time.monotonic() + self._towel_cleanup_retry_window
        retry_delay = max(0.05, self._cleanup_retry_initial)
        attempt = 0
        while True:
            attempt += 1
            settlement_error = await self._settle_towel_once(
                record, round_id, run_task, wait_for_run=wait_for_run
            )
            if (
                settlement_error is None
                or round_id not in _TOWEL_CLEANUP_AUTO_RETRY_ROUNDS
                or time.monotonic() + retry_delay > retry_deadline
            ):
                break
            logger.warning(
                "Towel cleanup attempt %d has not converged session=%s diagnostic=%s; "
                "retrying automatically in %.1fs",
                attempt,
                record.snapshot.id,
                settlement_error,
                retry_delay,
            )
            try:
                await asyncio.sleep(retry_delay)
            except asyncio.CancelledError:
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
            retry_delay = min(retry_delay * 2, max(retry_delay, self._cleanup_retry_max))
            async with record.lock:
                if record.snapshot.towel is None:
                    return
                if round_id in _TOWEL_COOLDOWN_ROUNDS:
                    record.snapshot.cooldown = self._new_towel_cooldown(record)
                record.snapshot.updated_at = datetime.now(UTC)
                snapshot = record.snapshot.model_copy(deep=True)
            await record.event_log.publish(
                "towel_update",
                {"session": snapshot.model_dump(mode="json")},
                receipt_bout_id=snapshot.receipt_bout_id,
            )

        if settlement_error is None:
            await self._settle_towel_cost_window(record)
        round_one_cooldown_held = (
            settlement_error is None and round_id == RoundId.WAKE_IDLE_APP
        )
        released = round_one_cooldown_held or (
            settlement_error is None and await self._release_bout(record)
        )
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
            receipt_bout_id=snapshot.receipt_bout_id,
        )
        if settlement_error is None and round_id == RoundId.WAKE_IDLE_APP:
            if record.cooldown_task is asyncio.current_task():
                record.cooldown_task = None
            self._schedule_round_one_cooldown(record)

    async def _settle_towel_once(
        self,
        record: SessionRecord,
        round_id: RoundId,
        run_task: asyncio.Task[None] | None,
        *,
        wait_for_run: bool,
    ) -> str | None:
        """Run one towel settlement for a non-Round-5 round; return why it failed, or None."""

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
                # The timed work is stopped, but the engines have not yet proved
                # idle. Keep the same durable lease in the ordinary Round 1
                # cooldown phase so the fight card cannot advertise READY while
                # either endpoint is still ACTIVE.
                await self._transition_bout_to_cleanup(
                    record,
                    SessionState.TOWELLED,
                    expected_phase="towel_cleanup",
                )
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
                await self._reset_recovery(record)
            elif round_id == RoundId.PUT_MODEL_SCORE_IN_APP:
                engine = record.model_score_engine
                if engine is None:
                    raise InvalidStateError("The Managed Sync cleanup engine is unavailable")
                await engine.settle_and_restore_baseline()
            elif round_id == RoundId.ANALYZE_LIVE_ORDERS:
                engine = record.live_orders_engine
                if engine is None:
                    raise InvalidStateError("The live-order cleanup engine is unavailable")
                # The engine keeps every order it rang until settling removes it from every
                # source, and parks the AWS pipeline whether or not the bell was rung.
                await engine.settle_and_cleanup_owned()

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
            return str(exc) or "Automatic towel cleanup could not be verified"
        return None

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

    @staticmethod
    def _reraise_own_cancel(exc: BaseException, cancels_on_entry: int) -> None:
        """Re-raise ``exc`` when this task was cancelled since entry, not a child's cancel.

        Counted from entry because cleanup also runs from a cancelled task's
        ``finally``, whose count is already raised.
        """

        task = asyncio.current_task()
        if (
            isinstance(exc, asyncio.CancelledError)
            and task is not None
            and task.cancelling() > cancels_on_entry
        ):
            raise exc

    async def _fail_recovery_cooldown(
        self,
        record: SessionRecord,
        exc: BaseException,
        failure: str,
    ) -> None:
        logger.error(
            "Recovery cleanup did not finish session=%s failure=%s diagnosis=%s",
            record.snapshot.id,
            failure,
            operator_diagnosis(exc),
        )
        async with record.lock:
            current = record.snapshot.cooldown
            if current is None:
                return
            current.state = CooldownState.FAILED
            current.failure = failure
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

        task = asyncio.current_task()
        cancels_on_entry = task.cancelling() if task is not None else 0
        # A cancelled lane's shielded create can still land after reset
        # inspects, so absence is only provable once nothing is in flight.
        try:
            await engine.settle_pending_mutations(record.snapshot.competitor.id)
        except (Exception, asyncio.CancelledError) as exc:
            self._reraise_own_cancel(exc, cancels_on_entry)
            await self._fail_recovery_cooldown(
                record,
                exc,
                "A recovery mutation is still unresolved; nothing was reset.",
            )
            return
        try:
            result = await engine.reset(record.snapshot.competitor.id, on_progress)
        except RecoveryResetError as exc:
            result = exc.result
        except (Exception, asyncio.CancelledError) as exc:
            self._reraise_own_cancel(exc, cancels_on_entry)
            await self._fail_recovery_cooldown(
                record,
                exc,
                "Recovery environments could not be safely removed.",
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

    async def _monitor_cooldown(self, record: SessionRecord, watch: CooldownWatch) -> None:
        cooldown = watch.cooldown
        async with record.lock:
            if record.live_targets is None or not self._watch_is_current(record, watch):
                return
            snapshot = cooldown.model_copy(deep=True)
        await record.event_log.publish(
            "cooldown_started",
            {"cooldown": snapshot.model_dump(mode="json")},
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._cooldown_timeout
        while loop.time() < deadline:
            remaining = deadline - loop.time()
            poll_timeout = max(
                0.001,
                min(self._cooldown_poll_timeout, remaining),
            )
            async with record.lock:
                targets = record.live_targets
                if targets is None or not self._watch_is_current(record, watch):
                    return
                tasks = [
                    asyncio.create_task(
                        self._observe_cooldown_target(
                            target,
                            cooldown.lanes[target.id].started_at,
                            poll_timeout,
                        )
                    )
                    for target in targets
                    # A confirmed lane owns an immutable stop result. Continuing to
                    # poll it used to publish a later `confirmed_at`/`elapsed_ms`
                    # every cycle while the other lane was still active.
                    if cooldown.lanes[target.id].state != CooldownLaneState.CONFIRMED_ZERO
                ]
            try:
                for task in asyncio.as_completed(tasks):
                    target, check, completed_at = await task
                    applied = await self._apply_cooldown_observation(
                        record,
                        watch,
                        target.id,
                        check,
                        completed_at,
                    )
                    if applied is None:
                        return
                    changed, snapshot = applied
                    if changed:
                        await record.event_log.publish(
                            "cooldown_update",
                            {"cooldown": snapshot.model_dump(mode="json")},
                        )
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            async with record.lock:
                if not self._watch_is_current(record, watch):
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
            if not self._watch_is_current(record, watch):
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
            registered = None
            owned = watch.owned
            if owned is not None and owned.phase == "cooldown":
                # `_watch_is_current` already read this lease under `record.lock`.
                # Do not await another lock after FAILED and before registering
                # the fence: shutdown can cancel the watch at any suspension.
                registered = record.lease
                if registered is not None:
                    # Registered with FAILED: shutdown may cancel anything after
                    # this assignment and must still keep this row.
                    record.cooldown_failure_fence = registered
        await record.event_log.publish(
            "cooldown_update",
            {"cooldown": snapshot.model_dump(mode="json")},
        )
        await self._pin_round_one_cooldown_failure(record, watch, registered)

    async def _pin_round_one_cooldown_failure(
        self,
        record: SessionRecord,
        watch: CooldownWatch,
        registered: BoutLease | None,
    ) -> None:
        """Publish a finite durable failure fence on the watch's lease and stop renewing it.

        A terminal observation timeout has no owned artifact to clean, but it
        must not flash READY immediately either. Transitioning the watch's own
        cooldown row to `cooldown_failed` keeps this round unavailable across
        replicas for one ordinary active-lease TTL. Its heartbeat is stopped
        first, whether the transition then succeeds or not, so a coordinator fault
        degrades to the existing row's finite expiry instead of an immortal
        cooldown lease, and no renew answered after the row moved reads as a lost
        lease. The transition itself runs off ``lease_lock``, as a renew does.
        Only the cooldown lease the watch began under is pinned, and only while
        the watch may still act: by then the card may show a newer bout's
        cooldown or hold its lease, which are not this watch's to touch, so
        ``registered``, the fence set when the cooldown failed, is then let go.
        """

        owned = watch.owned
        if owned is None or owned.phase != "cooldown":
            return
        async with record.lock:
            if not self._watch_is_current(record, watch):
                if registered is not None and record.cooldown_failure_fence is registered:
                    record.cooldown_failure_fence = None
                return
            async with record.lease_lock:
                local = record.lease
        self._cancel_lease_heartbeat(record, owned)
        if local is None:
            if registered is not None and record.cooldown_failure_fence is registered:
                record.cooldown_failure_fence = None
            return
        fence = local
        record.cooldown_failure_fence = fence
        try:
            sent = time.monotonic()
            pinned = await self._lease_store_for_record(record).transition(
                local,
                operator=record.operator or local.operator,
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
        else:
            async with record.lease_lock:
                current = record.lease
                if current is not None and self._same_exact_lease(current, local):
                    self._set_lease(record, pinned, sent)
                    fence = pinned
        self._forget_cooldown_fence_at_expiry(record, fence)

    def _forget_cooldown_fence_at_expiry(self, record: SessionRecord, fence: BoutLease) -> None:
        """Drop the record's pointer to a pinned cooldown failure once its row is gone."""

        record.cooldown_failure_fence = fence

        def retire() -> None:
            if record.cooldown_failure_fence is fence:
                record.cooldown_failure_fence = None

        self._forget_fence_at_expiry(record, fence, "Round 1 cooldown fence", retire)

    def _forget_fence_at_expiry(
        self,
        record: SessionRecord,
        fence: BoutLease,
        what: str,
        retire: Callable[[], None] = lambda: None,
    ) -> None:
        """Drop the record's pointer to ``fence`` once the store no longer holds it.

        For a fence nothing renews or releases: a pinned cooldown failure, or the armed
        fence of a bell whose acceptance could not be confirmed. Without this the
        pointer outlives its row and the record can never be released. The store
        is asked rather than the local clock: its ``expires_at`` is on the store's
        clock, and a transition whose answer was lost leaves a local copy whose
        expiry is not the row's. Only the exact unrenewed fence is forgotten: a
        newer bout's lease, or one a retry is renewing again, is left alone.
        ``retire`` runs once the record no longer points at ``fence``.
        """

        if self._closed:
            # Work that close() drains can still get here, after it collected the
            # tasks it cancels. Shutdown leaves the row to its durable TTL.
            return
        previous = record.fence_expiry_task
        if previous is not None and not previous.done():
            previous.cancel()
        store = self._lease_store_for_record(record)
        poll = max(0.01, self._active_lease_ttl)

        def still_unrenewed() -> bool:
            local = record.lease
            return (
                local is not None
                and self._same_exact_lease(local, fence)
                and record.lease_heartbeat_lease is None
            )

        async def forget() -> None:
            # The local estimate only times the first look; after that the store is
            # asked once per lease TTL, so skew or an outage cannot become a hot loop.
            left = (fence.expires_at - datetime.now(UTC)).total_seconds()
            delay = max(0.01, min(poll, left))
            try:
                while True:
                    await asyncio.sleep(delay)
                    delay = poll
                    async with record.lease_lock:
                        if not still_unrenewed():
                            retire()
                            return
                    try:
                        async with asyncio.timeout(FENCE_EXPIRY_READ_SECONDS):
                            held = await store.current()
                    except Exception as exc:
                        logger.warning(
                            "%s check could not read the ring session=%s diagnosis=%s",
                            what,
                            record.snapshot.id,
                            operator_diagnosis(exc),
                        )
                        continue
                    if held is not None and self._same_lease_authority(held, fence):
                        continue
                    async with record.lease_lock:
                        if still_unrenewed():
                            record.lease = None
                        retire()
                    return
            finally:
                if record.fence_expiry_task is asyncio.current_task():
                    record.fence_expiry_task = None

        record.fence_expiry_task = asyncio.create_task(
            forget(),
            name=f"fence-expiry-{record.snapshot.id}",
        )

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
        watch: CooldownWatch,
        target_id: str,
        check: dict[str, object] | Exception,
        completed_at: datetime,
    ) -> tuple[bool, CooldownSnapshot] | None:
        """Apply one control-plane answer to the watch's cooldown; None once it may not act."""

        changed = False
        async with record.lock:
            if not self._watch_is_current(record, watch):
                return None
            cooldown = watch.cooldown
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
                    # A pre-verify towel has no verified lane whose connection-close
                    # evidence can be reconciled. Requiring that impossible evidence
                    # made an accepted early towel time out even after the control
                    # plane repeatedly proved IDLE. This exception is deliberately
                    # bout-wide and towel-specific: if either lane verified, its real
                    # cleanup evidence remains required, and an ordinary unverified
                    # failure still cannot pass vacuously.
                    preverify_towel = bool(
                        record.snapshot.state == SessionState.TOWELLED
                        and record.snapshot.towel is not None
                        and not any(
                            candidate.state == LaneState.VERIFIED
                            for candidate in record.snapshot.lanes.values()
                        )
                    )
                    settled_towel_without_close = bool(
                        record.snapshot.state == SessionState.TOWELLED
                        and record.snapshot.towel is not None
                        and record.snapshot.towel.state == TowelState.READY
                        and source_lane.connection_closed_at is None
                    )
                    activity_reconciled = (
                        activity_proved
                        or preverify_towel
                        or settled_towel_without_close
                    )
                    provider_updated_at = self._cooldown_timestamp(
                        check.get("provider_updated_at")
                    )
                    assign("provider_updated_at", provider_updated_at)
                    now_ns = time.monotonic_ns()
                    previous = watch.idle_observations.get(target_id)
                    if previous is None:
                        observation_count, first_observed_ns = 1, now_ns
                    else:
                        prior_count, first_observed_ns = previous
                        observation_count = prior_count + 1
                    watch.idle_observations[target_id] = (
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
                    if activity_reconciled and (
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
                        if preverify_towel:
                            basis = (
                                "preverify_towel_provider_idle"
                                if provider_corrobates_post_close
                                else "preverify_towel_observed_idle_dwell"
                            )
                        elif settled_towel_without_close:
                            basis = (
                                "settled_towel_provider_idle"
                                if provider_corrobates_post_close
                                else "settled_towel_observed_idle_dwell"
                            )
                        else:
                            basis = (
                                "provider_update_corroboration"
                                if provider_corrobates_post_close
                                else "observed_idle_dwell"
                            )
                        assign("confirmation_basis", basis)
                        if preverify_towel:
                            status = (
                                "IDLE confirmed after a pre-verify towel; no verified "
                                "lane activity exists to reconcile"
                            )
                        elif settled_towel_without_close:
                            status = (
                                "IDLE confirmed after the towel settled the run whose "
                                "connection-close timestamp did not arrive"
                            )
                        else:
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
                watch.idle_observations.pop(target_id, None)
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
                terminal_lease = record.lease
                round5_lease = record.round5_lease
            lease_phase = terminal_lease.phase if terminal_lease is not None else None
            if lease_phase == "run_committed":
                if record.snapshot.round.id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
                    cleanup_mode = ResetMode.DELETE_ISOLATED_ENVIRONMENT
                elif record.snapshot.round.id == RoundId.RECOVER_DELETED_ORDER:
                    cleanup_mode = ResetMode.DELETE_RECOVERY_ENVIRONMENT
        if (
            cleanup_mode is None
            and await self._settle_terminal_release(record, terminal_lease) == "lost"
        ):
            return
        if cleanup_mode is None and record.snapshot.round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
            # Its cleanup verified before `_fail`, so the cleanup lease guards nothing
            # left; whether its release is confirmed or still retried, the bout fails.
            await self._settle_round5_release(record, round5_lease)
        async with record.lock:
            if record.snapshot.towel is not None:
                return
            record.snapshot.state = SessionState.FAILED
            record.snapshot.failure = message
            record.snapshot.remembered_result = None
            self._cancel_idle_watch(record)
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
                and record.run_started_monotonic_ns is not None
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
            receipt_bout_id=snapshot.receipt_bout_id,
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
            if snapshot.lanes[LAKEBASE_LANE].elapsed_ms is None and (
                snapshot.lanes[COMPETITOR_LANE].elapsed_ms is None
            ):
                raise RuntimeError("Round 4 verified without either lane's elapsed time")
            return _round_four_remembered_result(snapshot)
        if snapshot.lanes["competitor"].state == LaneState.NOT_SUPPORTED:
            assert snapshot.lanes["lakebase"].state == LaneState.VERIFIED
            return "LAKEBASE WINS — RDS CANNOT ENTER THE ROUND"
        stoppage = snapshot.comparison
        if (
            stoppage is not None
            and stoppage.kind == ComparisonKind.ADJUDICATED_STOPPAGE
            and stoppage.winner_lane_id is not None
        ):
            # A lane stopped at its bound (Rounds 2 and 3): its clock is a floor, so the
            # difference between the two clocks is not a margin. Rounds 4 and 6's words.
            winner = (
                "LAKEBASE"
                if stoppage.winner_lane_id == LAKEBASE_LANE
                else snapshot.competitor.short_name.upper()
            )
            return f"{winner} WINS · MARGIN IS A LOWER BOUND"

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
