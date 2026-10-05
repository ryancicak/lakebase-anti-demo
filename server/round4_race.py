"""Round 4 in v1.1: two lanes, both cold at the bell, one change, one verifier.

The design of record is docs/design/v1.1-rounds-4-6-aws.md. This module is its Round 4 protocol
and nothing else: it knows how to prepare, ring, time, resolve and settle a bout, and it reaches
the outside world only through two lanes and one shared source.

- **Prepare starts nothing.** Both integrations must be parked, a park still in progress is
  waited out, and the source and every destination must read the sealed baseline. If a crash
  left a destination off it, Prepare first settles (restore, carry, park), visibly.
- **The bell** starts both integrations and commits one Delta change, all at once, and every
  lane's verifier starts polling at the same instant. Neither lane waits for the other.
- **Each clock** runs from the bell to the completion of that lane's first read that returns the
  exact expected row. Every read is the same query shape, on the same client, every
  ``POLL_SECONDS``, over a connection opened before the bell. Nothing else waits on a
  verifier's path: each lane's status watch and every progress report run on their own tasks,
  so a slow status API or observer can never stretch one lane's read gaps and time the two
  lanes at different resolutions.
- **Resolution** comes from evidence. A lane's arrival lies between the later of the commit's
  acknowledgment and the start of its last negative read (L) and the completion of its first
  exact read (U). One lane wins only if its U is no later than the other's L; otherwise the bout
  is within measurement resolution. A lane that never verifies gives the other a lower bound, not
  a margin.
- **Settle** restores the source row while the lanes are still running, waits until every
  destination reads the baseline again, and parks both lanes.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

from .bout_limit import BOUT_TIME_LIMIT_SECONDS
from .model_score import (
    DeltaCommit,
    ModelScoreContract,
    ModelScoreRow,
    ModelScoreUpdate,
    is_owned_prior_proof,
)

LOGGER = logging.getLogger(__name__)

#: The protocol this module implements, recorded on every result.
PROTOCOL = "round4-two-lane-v1"

#: Frozen before the first scored bout (design section 1). The poll period is v1's 250 ms fresh
#: read. One timeout for both lanes: the spike's slowest cold lane took 120 s. The timeout was
#: 420 s until 2026-10-03, when Ryan set one maximum for every round (`server/bout_limit.py`).
POLL_SECONDS = 0.25
LANE_TIMEOUT_SECONDS = BOUT_TIME_LIMIT_SECONDS
#: How often a lane's integration is asked whether it has failed, on its own task: a status call
#: can take over a second (Lakebase's pipeline status is two API reads), and a verifier that
#: waited on it would read that lane less often than the other.
WATCH_SECONDS = 3.0
#: How long settling may wait for a restored row to reach every destination.
SETTLE_CARRY_TIMEOUT_SECONDS = 420.0
SETTLE_POLL_SECONDS = 1.0
#: How long a lane that was already running may take to carry the restored row before it is
#: parked and started once more. A running integration carries it in seconds; measured once
#: (2026-09-29, gate bout rds-8), Lakebase's continuous sync ran on without applying it for seven
#: minutes and applied it only when it was stopped, while a fresh start applies everything pending
#: in its first batch.
SETTLE_CARRY_KICK_SECONDS = 60.0
#: How long a bell's commit may take before the bout fails.
COMMIT_TIMEOUT_SECONDS = 120.0

LAKEBASE_LANE = "lakebase"
COMPETITOR_LANE = "competitor"
#: How long one progress callback may take. Progress is observation: an observer that is slow
#: or gone (a disconnected event stream) never holds up or fails a bout.
PROGRESS_TIMEOUT_SECONDS = 2.0

Notify = Callable[[str], Awaitable[None]]


def _observed(callback: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """``callback`` as an observer: bounded, and never able to fail what it watches."""

    async def call(*arguments: Any) -> None:
        try:
            await asyncio.wait_for(callback(*arguments), timeout=PROGRESS_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - an observer never changes a bout
            LOGGER.debug("A Round 4 progress observer failed", exc_info=True)

    return call


class Round4RaceError(RuntimeError):
    """A Round 4 bout could not be prepared, run, or settled as the protocol requires."""


class Round4NotArmedError(Round4RaceError):
    """The lanes or the baseline are not in the state a bout may start from."""


class Round4SettleError(Round4RaceError):
    """The source row could not be restored, carried and parked."""


class Round4Phase(StrEnum):
    PREPARING = "preparing"
    ARMED = "armed"
    STARTING = "starting"
    WAITING = "waiting"
    VERIFIED = "verified"
    FAILED = "failed"


Progress = Callable[[str, Round4Phase, str, float | None], Awaitable[None]]


class _ProgressRelay:
    """Deliver one bout's progress in order on its own task, so no verifier waits on it."""

    def __init__(self, callback: Progress) -> None:
        self._callback = _observed(callback)
        self._queue: asyncio.Queue[tuple[Any, ...] | None] = asyncio.Queue()
        self._task = asyncio.create_task(self._deliver(), name="round4-progress")

    def emit(self, lane_id: str, phase: Round4Phase, status: str, elapsed_ms: float | None) -> None:
        self._queue.put_nowait((lane_id, phase, status, elapsed_ms))

    async def _deliver(self) -> None:
        while (event := await self._queue.get()) is not None:
            await self._callback(*event)

    async def close(self) -> None:
        """Deliver everything emitted so far. Each delivery is bounded by ``_observed``."""

        self._queue.put_nowait(None)
        await self._task

    def abort(self) -> None:
        self._task.cancel()


class Reader(Protocol):
    """One destination's application read, over a connection opened before the bell."""

    async def read(self, entity_id: str) -> ModelScoreRow | None: ...

    async def aclose(self) -> None: ...


class Lane(Protocol):
    """One integration and its destination."""

    lane_id: str
    label: str

    async def confirm_parked(self, notify: Notify) -> None:
        """Return once the integration is parked, parking it if it is running."""

    async def open_reader(self) -> Reader:
        """Open the destination connection the verifier will poll on."""

    async def start(self, bell: Bell) -> None:
        """Ask the integration to start from parked. Returns once the request is accepted."""

    async def ensure_running(self, bell: Bell) -> bool:
        """Start the integration unless it is already starting or running (settling only).

        Returns whether this call started it.
        """

    async def failure(self) -> str | None:
        """The integration's own terminal failure since its start, if it has failed."""

    async def park(self, notify: Notify) -> None:
        """Stop the integration and wait until it is parked."""

    async def evidence(self, commit: DeltaCommit, bell: Bell) -> Mapping[str, Any]:
        """Diagnostics read after the race, never on its clock."""


class Source(Protocol):
    """Round 4's shared Delta source."""

    async def check_storage(self, entity_id: str) -> ModelScoreRow | None: ...

    async def read(self, entity_id: str) -> ModelScoreRow | None: ...

    async def head_version(self) -> int: ...

    async def table_id(self) -> str: ...

    async def commit(
        self,
        update: ModelScoreUpdate,
        *,
        after_version: int,
        on_acknowledged: Callable[[], None] | None = None,
    ) -> DeltaCommit:
        """Commit ``update``; ``on_acknowledged`` fires once the write itself has returned."""


@dataclass(frozen=True)
class Bell:
    """The one instant a bout starts from, and what every lane is told about it."""

    bout_id: str
    rung_at: datetime
    source_table_id: str
    #: The source version every destination was verified to hold at Prepare, so a lane resumes
    #: just after it, as a checkpointed integration would. None when settling, where nothing is
    #: known about the destinations and a lane starts from the snapshot.
    source_version: int | None = None


@dataclass(frozen=True)
class Round4Arm:
    arm_id: str
    armed_at: datetime
    source_version: int
    source_table_id: str
    baseline: ModelScoreRow
    lanes: tuple[str, ...]


@dataclass(frozen=True)
class LaneOutcome:
    """What one lane's verifier saw, on the controller's clock, in milliseconds from the bell."""

    lane_id: str
    verified: bool
    #: U: when the first exact read completed.
    elapsed_ms: float | None
    #: L: the later of the commit's acknowledgment and the start of the last negative read.
    last_negative_ms: float | None
    reads: int
    max_read_gap_ms: float
    failure: str | None = None
    #: True when the lane ran its whole frozen bound without delivering the row: a censored
    #: measurement. False for every other failure, which is no measurement at all.
    timed_out: bool = False
    evidence: Mapping[str, Any] = field(default_factory=dict)
    #: When ``timed_out``: the bound from the bell the lane ran out. Its clock stops at its first
    #: exact read, which would complete after this, so this is a floor on it.
    bound_ms: float | None = None


class Verdict(StrEnum):
    WIN = "win"
    WITHIN_RESOLUTION = "within_resolution"
    #: One lane delivered and the other ran out its whole bound: the delivered lane's time is
    #: a lower bound on the margin, never the margin.
    LOWER_BOUND = "lower_bound"
    #: Nothing to compare: a lane errored, neither delivered, or only Lakebase ran.
    NO_RESULT = "no_result"


@dataclass(frozen=True)
class Resolution:
    verdict: Verdict
    winner: str | None = None
    margin_ms: float | None = None
    detail: str = ""


@dataclass(frozen=True)
class Round4RaceResult:
    protocol: str
    arm_id: str
    update: ModelScoreUpdate
    bell: Bell
    commit: DeltaCommit
    commit_ack_ms: float
    outcomes: Mapping[str, LaneOutcome]
    resolution: Resolution


def resolve(outcomes: Mapping[str, LaneOutcome]) -> Resolution:
    """Decide a bout from its lanes' evidence alone. Pure, so it is tested exhaustively.

    Only a lane whose interval ends before the other's begins wins: its first exact read
    completed no later than the other lane's last negative read began. Anything closer is
    within the resolution the two verifiers can show.
    """

    lanes = list(outcomes.values())
    verified = [lane for lane in lanes if lane.verified and lane.elapsed_ms is not None]
    if len(lanes) < 2:
        if verified:
            return Resolution(Verdict.NO_RESULT, detail="one lane ran; nothing to compare")
        return Resolution(Verdict.NO_RESULT, detail="no lane verified")
    if not verified:
        return Resolution(Verdict.NO_RESULT, detail="neither lane verified")
    if len(verified) == 1:
        (only,) = verified
        (other,) = [lane for lane in lanes if lane is not only]
        if not other.timed_out:
            # A lane that errored was not measured, so it neither loses nor bounds anything.
            return Resolution(
                Verdict.NO_RESULT,
                detail=f"the other lane failed rather than finished: {other.failure}",
            )
        return Resolution(
            Verdict.LOWER_BOUND,
            winner=only.lane_id,
            detail="the other lane did not deliver within its bound, so the margin is a lower "
            "bound, not a measurement",
        )
    first, second = sorted(verified, key=lambda lane: lane.elapsed_ms or 0.0)
    assert first.elapsed_ms is not None and second.elapsed_ms is not None
    second_began = second.last_negative_ms
    if second_began is not None and first.elapsed_ms <= second_began:
        return Resolution(
            Verdict.WIN,
            winner=first.lane_id,
            margin_ms=second.elapsed_ms - first.elapsed_ms,
        )
    return Resolution(
        Verdict.WITHIN_RESOLUTION,
        detail="the two arrivals overlap within the verifiers' 250 ms reads",
    )


@dataclass
class _Clock:
    now_ns: Callable[[], int]
    origin_ns: int

    def elapsed_ms(self, at_ns: int | None = None) -> float:
        return ((self.now_ns() if at_ns is None else at_ns) - self.origin_ns) / 1_000_000


class Round4RaceEngine:
    """One Round 4 matchup: Lakebase and, when its AWS lane is sealed, one competitor."""

    def __init__(
        self,
        source: Source,
        lanes: Sequence[Lane],
        *,
        contract: ModelScoreContract,
        poll_seconds: float = POLL_SECONDS,
        lane_timeout_seconds: float = LANE_TIMEOUT_SECONDS,
        watch_seconds: float = WATCH_SECONDS,
        settle_timeout_seconds: float = SETTLE_CARRY_TIMEOUT_SECONDS,
        settle_poll_seconds: float = SETTLE_POLL_SECONDS,
        carry_kick_seconds: float = SETTLE_CARRY_KICK_SECONDS,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not lanes or lanes[0].lane_id != LAKEBASE_LANE:
            raise ValueError("Round 4's first lane is always Lakebase")
        if len({lane.lane_id for lane in lanes}) != len(lanes):
            raise ValueError("Round 4's lanes must be distinct")
        self.source = source
        self.lanes = tuple(lanes)
        #: The sealed Round 4 contract: its key, its baseline and the tables it names. The
        #: manager reads the key and the tables from here, as it did from the v1 engine.
        self.contract = contract
        self.baseline = contract.baseline
        self.poll_seconds = poll_seconds
        self.lane_timeout_seconds = lane_timeout_seconds
        self.watch_seconds = watch_seconds
        self.settle_timeout_seconds = settle_timeout_seconds
        self.settle_poll_seconds = settle_poll_seconds
        self.carry_kick_seconds = carry_kick_seconds
        self._clock_ns = clock_ns
        self._now = now
        self._sleep = sleep
        self._used_arms: set[str] = set()
        self._owned_rows: dict[str, ModelScoreRow] = {}
        # Work the bell issued that cancellation cannot prove did not happen: a start request or
        # a MERGE already on the wire. Settling waits for it before restoring anything.
        self._in_flight: set[asyncio.Future[Any]] = set()

    @property
    def lane_ids(self) -> tuple[str, ...]:
        return tuple(lane.lane_id for lane in self.lanes)

    async def settle_and_restore_baseline(self) -> None:
        """What the manager's settlement and towel paths call: ``settle`` with no progress."""

        await self.settle()

    # -- Prepare ---------------------------------------------------------------------------

    async def prepare(self, notify: Notify) -> Round4Arm:
        """Confirm both lanes parked and every destination at the baseline. Starts nothing.

        What does not depend on the lanes being parked starts at once. Each destination's
        connection opens beside the source check, and opening it is what wakes a paused
        database: Aurora pauses after five idle minutes, and waking it was 12 s of a 23 s
        Prepare (2026-09-29). The table's version and ID are read beside the park
        confirmation, because nothing but this engine moves them. Nothing is read from a
        destination until both lanes are confirmed parked.
        """

        notify = _observed(notify)
        entity = self.baseline.entity_id
        early = [asyncio.ensure_future(lane.open_reader()) for lane in self.lanes]
        try:
            await notify("Checking Round 4's Delta source")
            source_row = await self.source.check_storage(entity)
            if source_row != self.baseline and not is_owned_prior_proof(source_row):
                raise Round4NotArmedError(
                    "Round 4's source row is neither the sealed baseline nor a row this demo wrote"
                )
            # After the storage check, which may have rebuilt the table and restarted its versions.
            identity = asyncio.ensure_future(
                asyncio.gather(self.source.head_version(), self.source.table_id())
            )
            try:
                await notify("Confirming every integration is parked")
                await asyncio.gather(*(lane.confirm_parked(notify) for lane in self.lanes))
                rows = await asyncio.gather(
                    *(
                        self._read_through(opening, lane, entity)
                        for opening, lane in zip(early, self.lanes, strict=True)
                    )
                )
                version, table_id = await identity
            finally:
                await _discard(identity)
        finally:
            await asyncio.gather(*(_discard_reader(opening) for opening in early))
        if source_row != self.baseline or any(row != self.baseline for row in rows):
            await notify(
                "A destination is not at its baseline yet. Putting Round 4 back first "
                "(this starts, carries and parks the lanes, and can take a few minutes)"
            )
            await self.settle(notify)
            if not await self._everything_at_baseline():
                raise Round4NotArmedError(
                    "Round 4 could not be put back to its baseline on every destination"
                )
            version = await self.source.head_version()
        arm = Round4Arm(
            arm_id=uuid4().hex,
            armed_at=self._now(),
            source_version=version,
            source_table_id=table_id,
            baseline=self.baseline,
            lanes=self.lane_ids,
        )
        await notify("Both integrations are parked and every destination reads the baseline")
        return arm

    async def _everything_at_baseline(self) -> bool:
        entity = self.baseline.entity_id
        source = await self.source.read(entity)
        rows = await asyncio.gather(*(self._read_once(lane, entity) for lane in self.lanes))
        return source == self.baseline and all(row == self.baseline for row in rows)

    @staticmethod
    async def _read_once(lane: Lane, entity: str) -> ModelScoreRow | None:
        reader = await lane.open_reader()
        try:
            return await reader.read(entity)
        finally:
            await reader.aclose()

    @classmethod
    async def _read_through(
        cls, opening: asyncio.Future[Reader], lane: Lane, entity: str
    ) -> ModelScoreRow | None:
        """Read once through Prepare's early connection, or a new one if that did not open.

        Prepare closes the early connection itself, whatever happened here.
        """

        try:
            reader = await opening
        except Exception:  # noqa: BLE001 - the early connection was only a head start
            LOGGER.info("Round 4 %s early connection did not open; opening it now", lane.lane_id)
            return await cls._read_once(lane, entity)
        return await reader.read(entity)

    async def wake(self) -> None:
        """Open and close each destination's connection, so a paused one is awake by Prepare.

        Reads nothing and starts nothing: the integrations stay parked, so the race is as
        cold as ever. Called when Round 4's fight card opens, ahead of Prepare.
        """

        results = await asyncio.gather(
            *(self._touch(lane) for lane in self.lanes), return_exceptions=True
        )
        for lane, result in zip(self.lanes, results, strict=True):
            if isinstance(result, BaseException):
                LOGGER.info("Round 4 %s destination did not wake: %r", lane.lane_id, result)

    @staticmethod
    async def _touch(lane: Lane) -> None:
        reader = await lane.open_reader()
        await reader.aclose()

    # -- The bell ---------------------------------------------------------------------------

    async def run(
        self,
        arm: Round4Arm,
        update: ModelScoreUpdate,
        on_progress: Progress,
    ) -> Round4RaceResult:
        """Ring: start every lane, commit the change, and time each lane to its exact read."""

        self._check_arm(arm, update)
        self._used_arms.add(arm.arm_id)
        self._owned_rows[update.proof_nonce] = update.row
        entity = update.entity_id

        # Before the bell, not on either clock: open each destination connection and wake it
        # with a verified read, so no clock contains a Lakebase or Aurora resume.
        readers: dict[str, Reader] = {}
        try:
            for lane in self.lanes:
                readers[lane.lane_id] = await lane.open_reader()
            woken = await asyncio.gather(*(readers[lane_id].read(entity) for lane_id in readers))
            if any(row != self.baseline for row in woken):
                raise Round4NotArmedError("A destination left its baseline after Prepare")
        except BaseException:
            await self._close_readers(readers)
            raise

        progress = _ProgressRelay(on_progress)
        clock = _Clock(self._clock_ns, self._clock_ns())
        bell = Bell(
            bout_id=arm.arm_id,
            rung_at=self._now(),
            source_table_id=arm.source_table_id,
            source_version=arm.source_version,
        )
        starts = {lane.lane_id: self._track(lane.start(bell)) for lane in self.lanes}
        commit_ack: dict[str, float] = {}

        def acknowledged() -> None:
            commit_ack.setdefault("ms", clock.elapsed_ms())

        commit = self._track(
            self.source.commit(
                update,
                after_version=arm.source_version,
                on_acknowledged=acknowledged,
            )
        )
        # A source that cannot say when its MERGE returned is acknowledged when it finishes.
        commit.add_done_callback(
            lambda future: (
                acknowledged() if not future.cancelled() and future.exception() is None else None
            )
        )
        for lane in self.lanes:
            progress.emit(lane.lane_id, Round4Phase.STARTING, f"Starting {lane.label}", 0.0)
        pollers = {
            lane.lane_id: asyncio.create_task(
                self._poll(
                    lane,
                    readers[lane.lane_id],
                    update.row,
                    clock,
                    starts[lane.lane_id],
                    commit,
                    commit_ack,
                    progress.emit,
                ),
                name=f"round4-verify-{lane.lane_id}",
            )
            for lane in self.lanes
        }
        try:
            outcomes_list = await asyncio.gather(*pollers.values())
            committed = await self._await_commit(commit)
            # Every lane's last word reaches the observer before its result does.
            await progress.close()
        except BaseException:
            for poller in pollers.values():
                poller.cancel()
            await asyncio.gather(*pollers.values(), return_exceptions=True)
            progress.abort()
            raise
        finally:
            await self._close_readers(readers)
        outcomes = {outcome.lane_id: outcome for outcome in outcomes_list}
        # Guardrails and diagnostics, after the race and off its clock.
        enriched: dict[str, LaneOutcome] = {}
        for lane in self.lanes:
            outcome = outcomes[lane.lane_id]
            try:
                evidence = dict(await lane.evidence(committed, bell))
            except Exception as exc:  # noqa: BLE001 - diagnostics never change a verdict
                LOGGER.warning("Round 4 %s evidence could not be read", lane.lane_id, exc_info=True)
                evidence = {"evidence_error": type(exc).__name__}
            enriched[lane.lane_id] = replace(outcome, evidence={**outcome.evidence, **evidence})
        return Round4RaceResult(
            protocol=PROTOCOL,
            arm_id=arm.arm_id,
            update=update,
            bell=bell,
            commit=committed,
            commit_ack_ms=commit_ack.get("ms", clock.elapsed_ms()),
            outcomes=enriched,
            resolution=resolve(enriched),
        )

    def _check_arm(self, arm: Round4Arm, update: ModelScoreUpdate) -> None:
        if arm.arm_id in self._used_arms:
            raise Round4RaceError("This Prepare has already been rung")
        if arm.lanes != self.lane_ids or arm.baseline != self.baseline:
            raise Round4RaceError("The Round 4 matchup changed after Prepare")
        if update.entity_id != self.baseline.entity_id or update.row == self.baseline:
            raise Round4RaceError("The bout's change must be a new row for the contracted key")
        if update.proof_nonce in self._owned_rows:
            raise Round4RaceError("The bout's nonce has already been used")

    def _track(self, operation: Awaitable[Any]) -> asyncio.Future[Any]:
        """Run ``operation`` so that cancelling a waiter never cancels it."""

        future = asyncio.ensure_future(operation)
        self._in_flight.add(future)

        def settled(done: asyncio.Future[Any]) -> None:
            self._in_flight.discard(done)
            if not done.cancelled():
                done.exception()

        future.add_done_callback(settled)
        return future

    async def _await_commit(self, commit: asyncio.Future[DeltaCommit]) -> DeltaCommit:
        try:
            return await asyncio.wait_for(asyncio.shield(commit), timeout=COMMIT_TIMEOUT_SECONDS)
        except TimeoutError as exc:
            raise Round4RaceError(
                f"The bout's Delta commit did not complete within {COMMIT_TIMEOUT_SECONDS:.0f}s"
            ) from exc

    async def _poll(
        self,
        lane: Lane,
        reader: Reader,
        expected: ModelScoreRow,
        clock: _Clock,
        start: asyncio.Future[Any],
        commit: asyncio.Future[Any],
        commit_ack: Mapping[str, float],
        report: Callable[[str, Round4Phase, str, float | None], None],
    ) -> LaneOutcome:
        reads = 0
        last_negative_start_ns: int | None = None
        previous_start_ns: int | None = None
        max_gap_ms = 0.0
        deadline_ns = clock.origin_ns + int(self.lane_timeout_seconds * 1_000_000_000)
        watch: asyncio.Task[str] | None = None
        failing: str | None = None

        def outcome(
            verified: bool,
            failure: str | None,
            at_ns: int | None,
            *,
            timed_out: bool = False,
        ) -> LaneOutcome:
            ack = commit_ack.get("ms")
            negative = (
                clock.elapsed_ms(last_negative_start_ns)
                if last_negative_start_ns is not None
                else None
            )
            last_negative_ms = (
                max(value for value in (ack, negative) if value is not None)
                if ack is not None or negative is not None
                else None
            )
            return LaneOutcome(
                lane_id=lane.lane_id,
                verified=verified,
                elapsed_ms=clock.elapsed_ms(at_ns) if verified and at_ns is not None else None,
                last_negative_ms=last_negative_ms,
                reads=reads,
                max_read_gap_ms=round(max_gap_ms, 3),
                failure=failure,
                timed_out=timed_out,
                bound_ms=self.lane_timeout_seconds * 1000 if timed_out else None,
            )

        try:
            while True:
                if commit.done() and not commit.cancelled() and commit.exception() is not None:
                    return outcome(False, "the bout's Delta commit failed", None)
                if start.done() and not start.cancelled() and start.exception() is not None:
                    return outcome(
                        False, f"{lane.label} could not be started: {start.exception()}", None
                    )
                began_ns = clock.now_ns()
                if previous_start_ns is not None:
                    max_gap_ms = max(max_gap_ms, (began_ns - previous_start_ns) / 1_000_000)
                previous_start_ns = began_ns
                try:
                    row = await reader.read(expected.entity_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - a failed read is a negative read
                    LOGGER.warning("Round 4 %s verifier read failed: %s", lane.lane_id, exc)
                    row = None
                finished_ns = clock.now_ns()
                reads += 1
                if row == expected:
                    report(
                        lane.lane_id,
                        Round4Phase.VERIFIED,
                        "The exact row is in the application",
                        clock.elapsed_ms(finished_ns),
                    )
                    return outcome(True, None, finished_ns)
                last_negative_start_ns = began_ns
                if finished_ns >= deadline_ns:
                    return outcome(
                        False,
                        f"{lane.label} did not deliver the row within "
                        f"{self.lane_timeout_seconds:.0f}s of the bell",
                        None,
                        timed_out=True,
                    )
                if failing is not None:
                    return outcome(False, failing, None)
                if start.done() and watch is None:
                    report(
                        lane.lane_id,
                        Round4Phase.WAITING,
                        f"{lane.label} started; waiting for the row",
                        clock.elapsed_ms(finished_ns),
                    )
                    watch = asyncio.create_task(
                        self._watch(lane), name=f"round4-watch-{lane.lane_id}"
                    )
                if watch is not None and watch.done():
                    # The integration says it failed. The next read decides: a row that landed
                    # before the failure was still delivered.
                    failing = watch.result()
                await self._sleep(max(0.0, self.poll_seconds - (clock.now_ns() - began_ns) / 1e9))
        finally:
            if watch is not None and not watch.done():
                watch.cancel()
                await asyncio.gather(watch, return_exceptions=True)

    async def _watch(self, lane: Lane) -> str:
        """Ask a started lane whether it has failed, every ``watch_seconds``, until it has."""

        while True:
            try:
                failed = await lane.failure()
            except Exception:  # noqa: BLE001 - an unreadable status is not a failure
                failed = None
            if failed:
                return failed
            await self._sleep(self.watch_seconds)

    @staticmethod
    async def _close_readers(readers: Mapping[str, Reader]) -> None:
        for reader in readers.values():
            try:
                await reader.aclose()
            except Exception:  # noqa: BLE001 - closing is best effort once a bout is over
                LOGGER.debug("Round 4 verifier connection did not close cleanly", exc_info=True)

    # -- Settle -----------------------------------------------------------------------------

    async def settle(self, notify: Notify | None = None) -> None:
        """Restore the source row, let the lanes carry it, and park every lane.

        The one way back to the state a bout starts from, used after every bout, after a towel
        or a failure, by Prepare when a crash left a destination off its baseline, and by the
        startup sweep. Waits first for whatever the bell issued, because a start request or a
        MERGE already on the wire may still land.
        """

        say = _observed(notify or _quiet)
        pending = tuple(self._in_flight)
        if pending:
            await asyncio.gather(
                *(asyncio.shield(item) for item in pending), return_exceptions=True
            )
        entity = self.baseline.entity_id
        source = await self.source.read(entity)
        if source != self.baseline:
            if source is None or not (
                self._owned_rows.get(source.proof_nonce) == source or is_owned_prior_proof(source)
            ):
                raise Round4SettleError(
                    "Round 4's source holds a row this demo did not write, so it is left alone"
                )
            await say("Putting Round 4's source row back to its baseline")
            head = await self.source.head_version()
            await self.source.commit(
                ModelScoreUpdate(
                    entity_id=entity,
                    score=self.baseline.score,
                    model_version=self.baseline.model_version,
                    proof_nonce=self.baseline.proof_nonce,
                ),
                after_version=head,
            )
        table_id = await self.source.table_id()
        bell = Bell(
            bout_id=f"settle-{uuid4().hex[:12]}", rung_at=self._now(), source_table_id=table_id
        )
        try:
            await asyncio.gather(*(self._carry(lane, bell, say) for lane in self.lanes))
        finally:
            await say("Parking both integrations")
            results = await asyncio.gather(
                *(lane.park(say) for lane in self.lanes), return_exceptions=True
            )
            failures = [
                f"{lane.label}: {result}"
                for lane, result in zip(self.lanes, results, strict=True)
                if isinstance(result, BaseException)
            ]
            if failures:
                raise Round4SettleError("Round 4 could not park " + "; ".join(failures))

    async def _carry(self, lane: Lane, bell: Bell, say: Notify) -> None:
        """Wait until this lane's destination reads the baseline, starting it if it must.

        A lane that was already running and has not carried the row within
        ``carry_kick_seconds`` is parked and started once more, because a fresh start
        applies everything pending in its first batch (``SETTLE_CARRY_KICK_SECONDS``). A
        lane this started from parked is on its cold start, which is the expected wait,
        so it keeps the whole bound instead.
        """

        entity = self.baseline.entity_id
        reader = await lane.open_reader()
        try:
            if await reader.read(entity) == self.baseline:
                return
            await say(f"{lane.label} is carrying the restored row")
            started = await lane.ensure_running(bell)
            now = self._clock_ns()
            deadline = now + int(self.settle_timeout_seconds * 1_000_000_000)
            kick_at = None if started else now + int(self.carry_kick_seconds * 1_000_000_000)
            while self._clock_ns() < deadline:
                await self._sleep(self.settle_poll_seconds)
                try:
                    if await reader.read(entity) == self.baseline:
                        return
                except Exception:  # noqa: BLE001 - keep trying within the bound
                    LOGGER.debug("Round 4 settle read failed", exc_info=True)
                if kick_at is not None and self._clock_ns() >= kick_at:
                    kick_at = None
                    await say(
                        f"{lane.label} had not carried the restored row after "
                        f"{self.carry_kick_seconds:.0f}s, so it is being started again"
                    )
                    await lane.park(say)
                    await lane.ensure_running(bell)
            raise Round4SettleError(
                f"{lane.label} did not carry the restored row within "
                f"{self.settle_timeout_seconds:.0f}s"
            )
        finally:
            await reader.aclose()


async def _quiet(_message: str) -> None:
    return None


async def _discard(work: asyncio.Future[Any]) -> None:
    """Cancel work nobody will read and wait for it, without taking a cancellation of ours."""

    if not work.done():
        work.cancel()
        await asyncio.wait({work})
    if not work.cancelled():
        work.exception()


async def _discard_reader(opening: asyncio.Future[Reader]) -> None:
    """Close Prepare's early connection whatever became of it: its one close."""

    await _discard(opening)
    if opening.cancelled() or opening.exception() is not None:
        return
    try:
        await opening.result().aclose()
    except Exception:  # noqa: BLE001 - a connection that will not close is not Prepare's failure
        LOGGER.info("Round 4 could not close an early connection", exc_info=True)


def contract_sha256(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()
