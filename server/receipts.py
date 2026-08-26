"""Durable receipts for sealed bouts.

Sessions live in memory and are released, so until now the only trace a bout left
behind was an access log line. That is why diagnosing a failure has meant going to
CloudTrail: the evidence was already gone.

There are two places a receipt can live, and which one a process uses is decided
by what that process actually has:

* **A directory of JSON files**, one per sealed bout, beside the manifest of the
  generation that produced it. No index, no migrations, no queries -- a souvenir
  that can be read with `cat`. This is what an operator's laptop uses, and it is
  the reason a receipt is inspectable without a database client.
* **An append-only table on the coordination database**, beside the ring lease,
  the readiness row and the cost ledger. This exists because a deployed
  Databricks App has neither of the things the directory needs: no manifest state
  directory to locate one, and no filesystem that survives a restart. Both of
  those were found the same night, by driving the deployed app for the first
  time: it logged "there is nowhere to keep a bout receipt" twice and
  `/api/receipts` answered `{"receipts": []}` -- on the one surface an audience
  is meant to look at.

A derived view sits on top of both so the recap does not have to re-implement the
epistemics every time it renders.

Three rules govern everything here:

1. Writing a souvenir must never fail a bout. Every entry point swallows its own
   exceptions and logs at warning level, and the durable write is scheduled
   rather than awaited so a coordination round trip can never sit inside a
   terminal transition.
2. A derived field must never claim more than the snapshot supports. In particular
   a margin is only recorded when both lanes actually verified; subtracting an
   unverified lower bound from a verified time would manufacture a result the
   orchestrator explicitly declined to declare.
3. Nowhere to write is a fact worth logging, but nowhere to *read* is just an
   empty history. A GET must not raise because an installation has no store.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Literal

import psycopg
from pydantic import BaseModel, Field

from .coordination import (
    COORDINATION_SCHEMA,
    CoordinationObjectsMissingError,
    read_coordination_objects,
)
from .manifest import load_manifest
from .models import LaneState, RoundId, SessionSnapshot, SessionState, TowelState
from .process_registry import state_dir_from_environ

logger = logging.getLogger(__name__)

#: Overrides the manifest-derived location. The tests set it to a tmp_path, and
#: an operator can point it somewhere else deliberately. It is an override and
#: not the primary source: relying on it was how tonight's receipts came to
#: depend on a variable being exported by hand.
ARTIFACT_ROOT_ENV = "ANTI_DEMO_ARTIFACT_ROOT"

#: What a caller is told when there is nowhere to write. Deliberately the same
#: shape of refusal as `manifest.manifest_path`: there is no default, because a
#: default is what let a bare command operate on a dead generation while the
#: live one ran beside it.
_NO_ARTIFACT_ROOT = (
    "No artifact root is selected, so there is nowhere to keep a bout receipt. "
    "Set ANTI_DEMO_MANIFEST to the manifest.json of the generation you are "
    "operating on -- receipts live beside it -- or set "
    f"{ARTIFACT_ROOT_ENV} to an explicit directory."
)

# Events after which a bout has reached its end state. Every one of these carries
# the fully serialised snapshot in its payload, which is what makes a single hook
# in EventLog.publish sufficient for all six round paths.
#
# The redo pair belongs here because a Round 4 re-do is a terminal transition in its
# own right: both sites set a terminal session state and mark the terminal event as
# published, so a re-do that verified would otherwise leave no receipt at all.
SEALING_EVENTS = frozenset(
    {
        "run_finished",
        "towel_finished",
        "session_failed",
        "redo_finished",
        "redo_failed",
        # An arm cancelled before the bell. Nothing was measured, so it never
        # reaches the scoreboard, but the record of an abandoned attempt is exactly
        # the kind of evidence that used to vanish with the session.
        "session_cancelled",
    }
)
# Deliberately excluded: "towel_started" seals the session state but the towel is
# still settling, and "towel_finished" follows with the settled truth. The one bout
# that still leaves nothing is one killed between those two events.

# The events a towel settles on when it does *not* settle on "towel_finished",
# which is most of the ways a towel can end.
#
# "towel_finished" is published from exactly one site, and only when cleanup
# succeeded and the round was not Round 5. Everything else about a towel -- a
# cleanup that failed, a cleanup that was cancelled, a lost cleanup lease, and
# every Round 5 towel including the ones that worked -- lands on one of these
# two instead. Round 5's towel finishes inside the backstage cleanup handoff and
# publishes "cleanup_update"; the failure branches publish "towel_update". So
# the two bouts a campaign most needs evidence from -- an abandoned Round 5, and
# an abandonment whose tidy-up did not complete -- were the two that left no
# receipt at all.
#
# These names cannot simply join SEALING_EVENTS: both are also published while
# the towel is still moving. That matters more than it sounds, because
# DurableReceiptStore is keyed on (session, round, sealing_event) and inserts
# ON CONFLICT DO NOTHING -- so sealing on the first "towel_update" would durably
# record a towel mid-cleanup and then *discard* the settled truth that follows.
# The towel's own state is the discriminator, and it is already in the payload.
TOWEL_SETTLING_EVENTS = frozenset({"towel_update", "cleanup_update"})

# The towel states that are an end state. `stopping` and `cleaning` are both
# still in motion and are retried, automatically or by the operator.
SETTLED_TOWEL_STATES = frozenset({TowelState.READY, TowelState.FAILED})


def seals_bout(event: str, snapshot: SessionSnapshot) -> bool:
    """Whether this published event is the one that ends the bout's record.

    The non-towel branch is Round 5's, and it seals a *second* time for a bout
    that already has a receipt. That is deliberate and it is what the store was
    built for: cleanup is abandoned long after the verdict was published, so the
    only alternative would be amending a sealed row, and this app holds no UPDATE
    anywhere near its own history. The later seal supersedes rather than
    accompanies -- ``load`` is ``DISTINCT ON (session_id, round_id)`` and the
    filesystem keeps one file per bout -- which is safe only because the later
    snapshot is a strict superset: abandonment touches the setup and towel
    sub-objects and never a lane, so every measurement survives and the tidy-up
    failure is added. A bout that verified therefore keeps ``outcome ==
    "declared"`` and gains the admission that its proxy may still be billing.
    """

    if event in SEALING_EVENTS:
        return True
    if event not in TOWEL_SETTLING_EVENTS:
        return False
    towel = snapshot.towel
    if towel is not None:
        return towel.state in SETTLED_TOWEL_STATES
    # No towel: nobody abandoned the race, so the only thing left to settle is
    # Round 5's backstage cleanup. `cleanup_retryable` is not the test -- it is
    # true while retries are still running as well as after they were given up
    # on, and sealing mid-flight would durably record the wrong answer and then
    # discard the right one on the conflict.
    setup = snapshot.round5_setup
    return setup is not None and bool(setup.cleanup_failure)


BoutOutcome = Literal["declared", "stopped_short", "pending"]
LaneOutcome = Literal["verified", "failed", "not_supported", "incomplete"]


def artifact_root() -> Path | None:
    """Where this installation keeps its artifacts, or None when none is selected.

    The state directory of the selected manifest, which is the same source the
    server log path and the restart record already resolve from
    (``process_registry.state_dir_from_environ``). That is the point: a receipt
    belongs to the generation that produced it, so it has to be located by the
    same thing that decides which generation is being operated on.

    It used to fall back to a hardcoded ``.anti-demo-v7`` whenever
    ``ANTI_DEMO_ARTIFACT_ROOT`` was unset, which outlived the generation that
    named it: v8's bouts wrote their souvenirs into v7's directory unless the
    operator remembered to export the variable, and ``/receipts`` then read them
    back from there and filtered every one of them out as a foreign
    installation. There is no fallback now for exactly the reason
    ``manifest_path`` has no default -- guessing is what makes a second
    generation invisible.
    """
    configured = os.environ.get(ARTIFACT_ROOT_ENV, "")
    if configured:
        return Path(configured).expanduser().resolve()
    return state_dir_from_environ()


def receipts_root() -> Path | None:
    root = artifact_root()
    return None if root is None else root / "receipts"


def receipt_id(session_id: str) -> str:
    """The short id the share cards print. Matches receiptId() in the frontend."""
    return session_id[:8].upper()


class LaneReceipt(BaseModel):
    ms: float | None = None
    state: LaneOutcome
    # True when `ms` is where the clock stood when the bout stopped, not a verified
    # time. A lower bound supports "had not finished by" and nothing stronger, so it
    # can never take part in a margin.
    lower_bound: bool = False
    # Carried verbatim so a reader can see why there is no number, rather than
    # having to infer it from the absence of one.
    reason: str | None = None


class BoutReceipt(BaseModel):
    receipt: str
    session_id: str
    round_id: RoundId
    round_title: str
    opponent: str
    opponent_id: str
    # Which installation produced this bout. Optional because every receipt written
    # before this field existed has none, and those files are sealed history that
    # must not be rewritten to add one. `belongs_to_installation` is what decides
    # what an absent value means; nothing else may assume it.
    #
    # Deliberately an identity and nothing more. Widening this model with round
    # metrics would feed stored numbers to commentary that enumerates exactly which
    # components a Round 4 total is made of, and pre-warm-up receipts carry a tail
    # that enumeration cannot account for.
    run_id: str | None = None

    outcome: BoutOutcome
    sealing_event: str
    # False when no lane produced a timing at all -- an attempt that failed before
    # it could measure anything. Kept because the evidence is still worth storing,
    # flagged because a scoreboard should not show it as a result.
    has_measurements: bool

    metric: Literal["bout_elapsed_ms", "setup_elapsed_ms"]
    lakebase: LaneReceipt
    opponent_lane: LaneReceipt

    margin_ms: float | None = None
    # None means there was no simultaneous start to measure, which happens when the
    # opponent lane was never launched. That is different from a skew of zero.
    start_skew_ms: float | None = None

    sealed_at: datetime
    remembered_result: str | None = None
    failure: str | None = None
    # Why the tidy-up after an abandoned bout did not complete, or None when
    # there was nothing to tidy or the tidying worked.
    #
    # `failure` cannot carry this and must not be made to. It is the *bout's*
    # failure, and a towelled bout has none: the operator stopped a race that was
    # running fine. What went wrong afterwards is a separate fact, and conflating
    # them would either invent a bout failure that did not happen or lose the
    # cleanup failure that did.
    #
    # Optional with a None default because every receipt sealed before this field
    # existed has none, and those files are history that must not be rewritten.
    # A reader distinguishes "abandoned, cleanup clean" from "abandoned, cleanup
    # did not complete" on this field alone -- not on `sealing_event`, which
    # names a transport detail, and not on the raw snapshot, which a recap should
    # not have to re-derive.
    cleanup_failure: str | None = None


def _cleanup_failure(snapshot: SessionSnapshot) -> str | None:
    """What the tidy-up left behind, stated rather than implied.

    Two sources, one answer. A towelled bout keeps its diagnostic on the towel;
    a bout that ended on its own and then failed to clean up keeps it on the
    Round 5 setup snapshot. A reader asking "did the tidy-up finish?" should not
    have to know which way the bout ended, so both arrive in one field -- and the
    stored snapshot still distinguishes them for anyone who does care.

    The towel fallback is not defensive padding. This project already has a
    recorded defect where a round reported success while its cleanup failed four
    times and the only trace was a log line, and a receipt that says nothing is
    exactly the shape that let that happen. A towel parked at `failed` with no
    diagnostic recorded is still a cleanup that did not complete, and the receipt
    says so even when whoever set the state forgot to say why.
    """

    towel = snapshot.towel
    if towel is not None:
        if towel.cleanup_failure:
            return towel.cleanup_failure
        if towel.state == TowelState.FAILED:
            return "Towel cleanup did not complete; no diagnostic was recorded."
        return None
    setup = snapshot.round5_setup
    return setup.cleanup_failure if setup is not None else None


def _lane_state(state: LaneState) -> LaneOutcome:
    if state == LaneState.VERIFIED:
        return "verified"
    if state == LaneState.FAILED:
        return "failed"
    if state == LaneState.NOT_SUPPORTED:
        return "not_supported"
    return "incomplete"


def derive_receipt(
    snapshot: SessionSnapshot,
    sealing_event: str,
    run_id: str | None = None,
) -> BoutReceipt:
    """Reduce a session snapshot to the facts a recap needs."""
    # Round 5 is judged on setup time, not bout elapsed. Reading its setup value as
    # a bout time would understate the round by two orders of magnitude, so the
    # metric travels with the numbers.
    setup = snapshot.round5_setup
    if setup is not None:
        metric = "setup_elapsed_ms"
        lb_ms = setup.lanes["lakebase"].setup_elapsed_ms if "lakebase" in setup.lanes else None
        opp_ms = setup.lanes["competitor"].setup_elapsed_ms if "competitor" in setup.lanes else None
    else:
        metric = "bout_elapsed_ms"
        lb_ms = snapshot.lanes["lakebase"].elapsed_ms if "lakebase" in snapshot.lanes else None
        opp_ms = snapshot.lanes["competitor"].elapsed_ms if "competitor" in snapshot.lanes else None

    # A thrown towel censors both lanes: the elapsed values move into the towel
    # snapshot as explicit lower bounds. Reading only lane.elapsed_ms would make a
    # towelled bout look like it measured nothing, and drop it off the scoreboard.
    censored = snapshot.towel.censored_lower_bounds_ms if snapshot.towel else {}

    def lane_receipt(lane_id: str, elapsed: float | None) -> LaneReceipt:
        lane = snapshot.lanes.get(lane_id)
        state = _lane_state(lane.state) if lane else "incomplete"
        bound = censored.get(lane_id)
        if elapsed is None and bound is not None:
            return LaneReceipt(
                ms=bound,
                state=state,
                lower_bound=True,
                reason=(lane.error or lane.status) if lane else None,
            )
        return LaneReceipt(
            ms=elapsed,
            # An unverified lane that still carries a number stopped the clock
            # somewhere short of verifying, which is a floor and not a time.
            lower_bound=elapsed is not None and state != "verified",
            state=state,
            reason=(lane.error or lane.status) if lane else None,
        )

    lakebase = lane_receipt("lakebase", lb_ms)
    opponent = lane_receipt("competitor", opp_ms)

    if snapshot.state == SessionState.VERIFIED:
        outcome: BoutOutcome = "declared"
    elif snapshot.state in (SessionState.FAILED, SessionState.TOWELLED):
        outcome = "stopped_short"
    else:
        outcome = "pending"

    # Prefer the orchestrator's own margin. Only fall back to arithmetic when both
    # lanes verified, so an unverified lower bound can never become a margin.
    margin_ms: float | None = None
    comparison = snapshot.comparison
    if comparison is not None and comparison.margin is not None:
        value = comparison.margin.value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            margin_ms = float(value)
    if (
        margin_ms is None
        and lakebase.state == "verified"
        and opponent.state == "verified"
        and lb_ms is not None
        and opp_ms is not None
    ):
        margin_ms = opp_ms - lb_ms

    return BoutReceipt(
        receipt=receipt_id(snapshot.id),
        session_id=snapshot.id,
        round_id=snapshot.round.id,
        round_title=snapshot.round.title,
        opponent=snapshot.competitor.short_name,
        opponent_id=snapshot.competitor.id,
        run_id=run_id,
        outcome=outcome,
        sealing_event=sealing_event,
        has_measurements=lakebase.ms is not None or opponent.ms is not None,
        metric=metric,
        lakebase=lakebase,
        opponent_lane=opponent,
        margin_ms=margin_ms,
        start_skew_ms=snapshot.fairness.launch_skew_ms,
        sealed_at=snapshot.updated_at,
        remembered_result=snapshot.remembered_result,
        failure=snapshot.failure,
        cleanup_failure=_cleanup_failure(snapshot),
    )


class Installation(BaseModel):
    """Which installation a reader is entitled to see receipts from.

    An installation owns real cloud resources. When it is torn down and replaced,
    the new one gets a new ``run_id``, and the old one's bouts describe endpoints,
    instances and clusters that no longer exist. Presenting those beside today's as
    though they were the same demo is the failure this type exists to prevent.
    """

    run_id: str
    # When this installation came into being. Everything sealed before it was
    # necessarily produced by a previous one.
    created_at: datetime


def _as_utc(moment: datetime) -> datetime:
    """A naive timestamp on disk is read as UTC, which is how every writer wrote it."""
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


def current_installation() -> Installation | None:
    """Best-effort identity of the installation this process serves.

    None when no manifest can be resolved. Callers treat that as "do not filter":
    a manifest-less dev server or a unit test has no installation to compare
    against, and answering nothing would be a worse failure than answering
    everything. A deployed server always has one.
    """
    try:
        manifest = load_manifest()
        return Installation(run_id=manifest.run_id, created_at=manifest.created_at)
    except Exception:
        logger.debug("No installation identity available; receipts will not be filtered")
        return None


def belongs_to_installation(receipt: BoutReceipt, installation: Installation) -> bool:
    """Whether this receipt was produced by the installation now running.

    Two tiers, because the field that would settle it did not always exist:

    1. A receipt that carries a ``run_id`` is matched exactly. Nothing else is
       consulted -- an explicit identity is never overridden by a guess.
    2. A receipt with no ``run_id`` predates the stamping and can only be placed by
       when it was sealed. It is admitted when it was sealed at or after this
       installation was created, because a receipt is only ever written by a
       running server, and the previous installation's server could not still have
       been writing after this one was stood up.

    Tier 2 admits rather than excludes deliberately. Excluding an unstamped receipt
    is the safer-sounding rule, but every receipt written before this change has no
    ``run_id``, so that rule would empty the summary of the very rounds the operator
    just ran. The timestamp is weaker evidence than an id and is the reason tier 1
    exists at all; it earns its place only until the unstamped files age out.
    """
    if receipt.run_id:
        return receipt.run_id == installation.run_id
    return _as_utc(receipt.sealed_at) >= _as_utc(installation.created_at)


# ---------------------------------------------------------------------------
# The durable half
#
# Everything below this line exists because of one property of the surface an
# audience actually looks at: a Databricks App container has no manifest state
# directory, and its filesystem does not survive a restart. So the option that
# looked cheapest -- point ANTI_DEMO_ARTIFACT_ROOT at some directory in the
# image -- buys a history that is empty again after every deploy, restart or
# replica reschedule. For an audience-facing record of bouts that is close to
# useless, and for the record every performance claim in this project rests on
# it is worse than useless.
#
# So receipts go where the ring lease, the readiness row and the cost ledger
# already go. Nothing here is a new idea; it is the fourth store in the same
# schema, and it deliberately borrows their shapes: the same
# `read_coordination_objects` existence probe before any DDL, the same
# consumer/owner split, and a connection runner handed in rather than built,
# exactly as `readiness.StartupReadinessStore` takes one.
# ---------------------------------------------------------------------------

#: The append-only receipt history on the coordination database. A module-level
#: constant because ``lifecycle._coordination_runtime_grants`` imports it: a
#: grant that names a coordination table by hand can outlive the table, and the
#: symptom is a 503 on a fresh install weeks later.
BOUT_RECEIPT_TABLE = f"{COORDINATION_SCHEMA}.bout_receipt"

#: The store this process persists receipts to, or None when it has none.
#:
#: Process-global, which needs justifying. The write hook is
#: ``manager.EventLog.publish`` -- one call covering all six round paths and
#: every terminal transition in them -- and it reaches receipts through a plain
#: function call with no application state in hand. Threading a store through
#: every round engine to reach that one line would be a far larger change to
#: code that has nothing to do with souvenirs. So the runtime installs it once
#: at startup and removes it at shutdown, the way it already owns the lease
#: stores.
_durable_store: DurableReceiptStore | None = None

#: Strong references to in-flight durable writes. Without these the tasks below
#: are only referenced by the event loop and may be garbage collected mid-write.
_pending_writes: set[asyncio.Task[None]] = set()


def install_receipt_store(store: DurableReceiptStore | None) -> None:
    """Point every subsequent sealed bout at ``store``, or at nothing."""
    global _durable_store
    _durable_store = store


def installed_receipt_store() -> DurableReceiptStore | None:
    """The durable store this process is using, for tests and health reporting."""
    return _durable_store


async def drain_receipt_writes(grace_seconds: float = 5.0) -> None:
    """Let scheduled receipt writes finish before the runtime closes its stores.

    A fire-and-forget write is the price of a synchronous hook, and shutdown is
    the one moment that price is collectable: the tasks would be cancelled with
    the event loop and the last bout of the night -- the one somebody just
    watched -- would be the receipt that never landed. Bounded, because a
    shutdown may not hang on a coordination endpoint either.
    """

    # Filtered by loop, because the runtime can be opened more than once in one
    # process -- the mutation-wait path re-enters `_open_runtime` every few
    # seconds -- and `asyncio.wait` on a task belonging to a loop that is gone
    # raises rather than returning.
    loop = asyncio.get_running_loop()
    pending = tuple(task for task in _pending_writes if task.get_loop() is loop)
    if not pending:
        return
    _finished, still_running = await asyncio.wait(pending, timeout=grace_seconds)
    if still_running:
        logger.warning(
            "%d bout receipt write(s) did not finish before shutdown", len(still_running)
        )


def _receipt_document(receipt: BoutReceipt, snapshot: SessionSnapshot) -> dict[str, Any]:
    """What both stores keep: the derived view, plus the snapshot it came from.

    The raw snapshot is kept so nothing is lost if the derivation above turns out
    to be wrong. It is the reason a receipt is tens of KB rather than one.
    """

    return {
        "receipt": receipt.model_dump(mode="json"),
        "snapshot": snapshot.model_dump(mode="json"),
    }


def _parse_document(document: Any) -> BoutReceipt:
    """Read a stored document back, whatever the driver handed us for a jsonb."""
    if isinstance(document, (str, bytes, bytearray)):
        document = json.loads(document)
    return BoutReceipt.model_validate(document["receipt"])


def _since_floor(since: date | None) -> datetime | None:
    """The instant a ``since`` date begins, in the zone the writers wrote in.

    Computed here rather than left to Postgres to cast: a bare `date` compared
    against a `timestamptz` is resolved in the *session's* timezone, so the same
    query would select different rows on two replicas. The filesystem reader
    buckets by UTC day, so this does too.
    """

    return None if since is None else datetime.combine(since, time.min, tzinfo=UTC)


class DurableReceiptStore:
    """Sealed bouts on the coordination database, one row per terminal event.

    Append-only, and that is a design choice rather than a simplification. The
    filesystem writer has a rule it needs mutable state to enforce: a bout that
    publishes a second terminal event after it has already been declared -- a
    cleanup that will not verify, a lost lease -- must not overwrite the proven
    result. On disk that means reading the file back and refusing the write. Here
    every terminal event simply gets its own row, and :meth:`load` picks the
    declared one when there is one. The later failure is then *kept* rather than
    merely not-winning, which is strictly more evidence, and the app needs no
    UPDATE and no DELETE anywhere near its own history.

    The runner is handed in for the same reason ``StartupReadinessStore`` takes
    one: this module has no business resolving a Lakebase host, a user and an
    OAuth token when the process already has a store that does.
    """

    def __init__(
        self,
        run: Callable[[Callable[[Any], Awaitable[Any]]], Awaitable[Any]],
    ) -> None:
        self._run = run

    async def initialize(self) -> None:
        """Confirm the receipt table exists, and create it only if it does not.

        Same consumer/owner split as the cost ledger and the readiness row, and
        for the same reason: on the deployed path this runs as a principal with
        no DDL on ``anti_demo_coordination``, and ``CREATE TABLE IF NOT EXISTS``
        checks the ACL *before* the ``IF NOT EXISTS``, so an unconditional create
        is refused for a statement that would have done nothing.

        Raising when the table is genuinely absent is deliberate, and so is the
        fact that the caller absorbs it. A store that has never confirmed its own
        table would accept writes that silently fail one per bout; saying so once
        at startup and serving without a durable history is the honest
        degradation, and it is the runtime's decision to make, not this module's.
        """

        async def ensure(cursor: Any) -> None:
            objects = await read_coordination_objects(cursor, (BOUT_RECEIPT_TABLE,))
            if objects.complete:
                return
            try:
                if not objects.schema_present:
                    await cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {COORDINATION_SCHEMA}")
                await cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {BOUT_RECEIPT_TABLE} (
                        session_id text NOT NULL,
                        round_id text NOT NULL,
                        sealing_event text NOT NULL,
                        receipt text NOT NULL,
                        run_id text,
                        outcome text NOT NULL,
                        sealed_at timestamptz NOT NULL,
                        document jsonb NOT NULL,
                        written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                        PRIMARY KEY (session_id, round_id, sealing_event)
                    )
                    """
                )
            except psycopg.errors.InsufficientPrivilege as exc:
                raise CoordinationObjectsMissingError(
                    f"The bout receipt history is missing {objects.describe_missing()}, "
                    "and this identity may not create it. Provision the coordination "
                    "schema with an identity that owns it (`antidemo setup`), then grant "
                    "this one the runtime privileges in docs/DEPLOY.md."
                ) from exc

        await self._run(ensure)

    async def append(self, receipt: BoutReceipt, snapshot: SessionSnapshot) -> None:
        """Record one sealed bout. Idempotent per (bout, round, terminal event)."""

        document = json.dumps(_receipt_document(receipt, snapshot), sort_keys=True)

        async def insert(cursor: Any) -> None:
            await cursor.execute(
                f"""
                INSERT INTO {BOUT_RECEIPT_TABLE} (
                    session_id, round_id, sealing_event, receipt, run_id,
                    outcome, sealed_at, document
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (session_id, round_id, sealing_event) DO NOTHING
                """,
                (
                    receipt.session_id,
                    receipt.round_id.value,
                    receipt.sealing_event,
                    receipt.receipt,
                    receipt.run_id,
                    receipt.outcome,
                    _as_utc(receipt.sealed_at),
                    document,
                ),
            )

        await self._run(insert)

    async def load(
        self,
        since: date | None = None,
        installation: Installation | None = None,
    ) -> list[BoutReceipt]:
        """Every stored bout on or after ``since``, one per bout, newest last.

        The ``DISTINCT ON`` is what replaces the filesystem's read-modify-write:
        a declared row wins its bout outright, and otherwise the latest terminal
        event does. Unreadable documents are skipped rather than failing the
        read, exactly as one corrupt file must not hide the rest of the record.
        """

        floor = _since_floor(since)

        async def select(cursor: Any) -> list[Any]:
            await cursor.execute(
                f"""
                SELECT DISTINCT ON (session_id, round_id) document
                FROM {BOUT_RECEIPT_TABLE}
                WHERE %s::timestamptz IS NULL OR sealed_at >= %s
                ORDER BY session_id, round_id,
                         (outcome = 'declared') DESC, sealed_at DESC
                """,
                (floor, floor),
            )
            return list(await cursor.fetchall())

        found: list[BoutReceipt] = []
        for row in await self._run(select):
            try:
                receipt = _parse_document(row[0])
            except Exception:
                logger.warning("Skipping an unreadable stored receipt", exc_info=True)
                continue
            if installation is not None and not belongs_to_installation(receipt, installation):
                continue
            found.append(receipt)
        found.sort(key=lambda item: item.sealed_at)
        return found


def _schedule_durable_receipt(receipt: BoutReceipt, snapshot: SessionSnapshot) -> bool:
    """Start the durable write, without ever waiting for it. Never raises.

    Scheduled rather than awaited because the only hook a receipt gets is a
    synchronous call inside ``EventLog.publish``, and the file it lives in is not
    ours to change. That is not purely a constraint: a coordination round trip
    awaited there would put a Lakebase latency spike between a bout finishing and
    the play-by-play saying so, on the one code path where every round path is
    already funnelled through one line.

    Returns whether a write is now in flight, which is the only thing the caller
    needs -- it decides whether an unavailable artifact directory is a fact worth
    a warning or a normal deployed runtime.
    """

    store = _durable_store
    if store is None:
        return False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No loop: a CLI or a test calling the hook directly. The filesystem
        # writer below is the whole answer there.
        return False

    async def write() -> None:
        try:
            await store.append(receipt, snapshot)
        except Exception:
            logger.warning(
                "Could not record bout receipt %s in the coordination database",
                receipt.receipt,
                exc_info=True,
            )

    task = loop.create_task(write(), name=f"bout-receipt-{receipt.receipt}")
    _pending_writes.add(task)
    task.add_done_callback(_pending_writes.discard)
    return True


def _merge_receipts(
    preferred: list[BoutReceipt],
    fallback: list[BoutReceipt],
) -> list[BoutReceipt]:
    """One history from two stores, without showing a bout twice.

    An operator's laptop can legitimately have both: a manifest directory full of
    files from before this change, and the durable table it writes to now. Reading
    only one of them would either hide tonight's bouts or hide last week's, so
    both are read and a bout present in both is reported once. The durable copy
    wins, because it is the one the deployed app and every replica agree on.
    """

    merged = {(item.session_id, item.round_id): item for item in fallback}
    merged.update({(item.session_id, item.round_id): item for item in preferred})
    return sorted(merged.values(), key=lambda item: item.sealed_at)


def receipt_path(receipt: BoutReceipt, root: Path | None = None) -> Path:
    day = receipt.sealed_at.astimezone(UTC).date().isoformat()
    base = root if root is not None else receipts_root()
    if base is None:
        raise RuntimeError(_NO_ARTIFACT_ROOT)
    return base / day / f"{receipt.receipt}-{receipt.round_id.value}.json"


def _already_declared(target: Path) -> bool:
    try:
        document = json.loads(target.read_text(encoding="utf-8"))
        return bool(document["receipt"]["outcome"] == "declared")
    except Exception:
        return False


def write_receipt(
    receipt: BoutReceipt,
    snapshot: SessionSnapshot,
    root: Path | None = None,
) -> Path:
    """Write one receipt atomically. Raises; callers that must not fail use record_*."""
    target = receipt_path(receipt, root)

    # One bout, one file, last terminal event wins -- with one exception. A bout can
    # publish a second terminal event after it has already been declared, when the
    # environment fails behind it (a cleanup that will not verify, a lost lease).
    # That is a fact about the environment, not about whether the transaction
    # verified, and letting it overwrite the file would erase a proven result. The
    # later failure still reaches the operator live and the server log.
    if receipt.outcome != "declared" and target.exists() and _already_declared(target):
        logger.warning(
            "Keeping the declared receipt %s; %s reported %s afterwards",
            target.name,
            receipt.sealing_event,
            receipt.failure or "a later failure",
        )
        return target

    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    document = _receipt_document(receipt, snapshot)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, target)
    return target


def record_sealed_bout(
    event: str,
    payload: dict[str, object],
    root: Path | None = None,
) -> Path | None:
    """Best-effort receipt write from a published event. Never raises.

    Returns the file path written, or None when the event was not terminal, no
    artifact directory is selected, or the file write failed. A None return does
    *not* mean nothing was recorded: on a deployed runtime the durable store is
    the only place a receipt goes and there is no path to return. A bout must
    never fail because its souvenir did, and it must not fail because only one of
    its two stores was available either.
    """

    try:
        # Two filters rather than one, and the cheap one stays first on purpose.
        # A towel is sealed on its settled *state* rather than on its event name,
        # which needs the snapshot parsed -- and "lane_update" carries a whole
        # serialised session too, several times a second on Round 5. Validating
        # one on every published event to answer a question only two event names
        # can ask would put that cost on the hot path.
        if event not in SEALING_EVENTS and event not in TOWEL_SETTLING_EVENTS:
            return None
        session = payload.get("session")
        if not isinstance(session, dict):
            return None
        snapshot = SessionSnapshot.model_validate(session)
        if not seals_bout(event, snapshot):
            return None
        installation = current_installation()
        receipt = derive_receipt(
            snapshot,
            event,
            run_id=installation.run_id if installation else None,
        )
    except Exception:
        logger.warning("Could not derive a bout receipt for %s", event, exc_info=True)
        return None

    durable = _schedule_durable_receipt(receipt, snapshot)
    try:
        return write_receipt(receipt, snapshot, root)
    except Exception:
        if durable:
            # A container has no artifact directory and is not supposed to. The
            # receipt is on its way to the coordination database, so this is the
            # ordinary deployed shape rather than a lost souvenir.
            logger.debug(
                "No artifact directory for receipt %s; the coordination database has it",
                receipt.receipt,
                exc_info=True,
            )
            return None
        logger.warning("Could not write a bout receipt for %s", event, exc_info=True)
        return None


def load_receipts(
    since: date | None = None,
    root: Path | None = None,
    installation: Installation | None = None,
) -> list[BoutReceipt]:
    """Read every receipt on or after `since`, newest last.

    Unreadable files are skipped rather than failing the read: one corrupt souvenir
    must not hide the rest of the record.

    When `installation` is given, receipts from any other installation are dropped.
    Passing None reads the directory as it stands, which is what a test or a
    manifest-less dev server wants; a reader that is going to *present* these to an
    operator should always pass one.
    """
    base = root if root is not None else receipts_root()
    # No selected generation means no receipts to read, which is the same answer
    # a reader gets from an empty directory. A read must not raise: `/receipts`
    # is a plain GET and an installation with nowhere to look has nothing to
    # show, whereas a *write* with nowhere to go is a fact worth logging.
    if base is None or not base.is_dir():
        return []
    found: list[BoutReceipt] = []
    for day_dir in sorted(base.iterdir()):
        if not day_dir.is_dir():
            continue
        try:
            day = date.fromisoformat(day_dir.name)
        except ValueError:
            continue
        if since is not None and day < since:
            continue
        for path in sorted(day_dir.glob("*.json")):
            try:
                document: Any = json.loads(path.read_text(encoding="utf-8"))
                receipt = BoutReceipt.model_validate(document["receipt"])
            except Exception:
                logger.warning("Skipping unreadable receipt at %s", path, exc_info=True)
                continue
            if installation is not None and not belongs_to_installation(receipt, installation):
                continue
            found.append(receipt)
    found.sort(key=lambda item: item.sealed_at)
    return found


async def load_receipts_async(
    since: date | None = None,
    installation: Installation | None = None,
) -> list[BoutReceipt]:
    """The complete history this process can see, from both stores, newest last.

    The reader `/api/receipts` uses. Async because the durable half is a
    coordination round trip and a route may not block the event loop on one; the
    synchronous :func:`load_receipts` stays exactly as it was for the CLI, the
    retention sweep and every test that reads a directory.

    A durable read that fails degrades to whatever is on disk rather than
    failing the request. An empty history renders as an empty history; a 500
    would render as a broken app, and one of those is true.
    """

    on_disk = load_receipts(since, None, installation)
    store = _durable_store
    if store is None:
        return on_disk
    try:
        durable = await store.load(since, installation)
    except Exception:
        logger.warning(
            "Could not read the durable bout receipt history; showing only what is on disk",
            exc_info=True,
        )
        return on_disk
    return _merge_receipts(durable, on_disk)


class ReceiptsResponse(BaseModel):
    receipts: list[BoutReceipt] = Field(default_factory=list)
