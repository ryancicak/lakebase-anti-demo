"""Round 6 in v1.1: two lanes, one checkout, each carried into the lakehouse by its own pipeline.

The design of record is docs/design/v1.1-rounds-4-6-aws.md. This module is its Round 6 protocol
and nothing else: it knows how to prepare, ring, time, resolve and settle a bout, and it reaches
the outside world only through its lanes.

- **Each lane is a source and a history.** Lakebase's lane commits the checkout to Lakebase, and
  its built-in change feed carries it into a Delta history table. The AWS lane commits the same
  checkout to Aurora or RDS, and AWS DMS and an AWS Glue job carry it into the lane's own Delta
  table. Both histories are read on the same SQL warehouse, with the same question.
- **Prepare starts nothing.** The AWS lane must be parked, a park still in progress is waited out,
  and every source must read its sealed baseline. Lakebase's feed is built in and always on, which
  the card and the receipt disclose (section 2); Prepare only confirms it is streaming.
- **The bell** commits the checkout to every source at once and, on the AWS lane, starts DMS and
  Glue from parked, and every lane's verifier starts reading its history at the same instant. The
  commits' skew is recorded. Neither lane waits for the other.
- **Each clock** runs from the bell to the completion of that lane's first history read that
  returns the exact order as one insert. Every read is the same query, every ``POLL_SECONDS``,
  and nothing else waits on a verifier's path: the AWS lane's status watch, the checkout
  guardrails and every progress report run on their own tasks.
- **Resolution** comes from evidence, by Round 4's rule (`server.round4_race.resolve`): a lane
  wins only if its first exact read completed no later than the other's last negative read began.
- **The guardrail** is v1's: a separate checkout commits and reads back on each lane's source
  while the histories are awaited, so the round shows the lakehouse path did not slow checkout.
- **Settle** deletes the bout's exact rows from every source and parks the AWS lane. The histories
  are append-only change logs, so nothing needs carrying back; the deletes are captured at the
  next bell, and a per-bout nonce means no earlier row can ever match.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

from .bout_limit import BOUT_TIME_LIMIT_SECONDS
from .live_orders import LiveOrder
from .round4_race import (
    COMPETITOR_LANE,
    LAKEBASE_LANE,
    LaneOutcome,
    Resolution,
    Verdict,
    resolve,
)

LOGGER = logging.getLogger(__name__)

#: The protocol this module implements, recorded on every result.
PROTOCOL = "round6-two-lane-v1"

#: Frozen before the first scored bout (design section 1). v1's one-second history poll, on both
#: lanes. One timeout for both: the spike's slowest cold AWS lane took 93 s. The timeout was
#: 420 s until 2026-10-03, when Ryan set one maximum for every round (`server/bout_limit.py`).
POLL_SECONDS = 1.0
LANE_TIMEOUT_SECONDS = BOUT_TIME_LIMIT_SECONDS
#: How often the AWS lane is asked whether DMS or Glue has failed, on its own task.
WATCH_SECONDS = 3.0
#: How long one checkout commit may take before the bout fails.
COMMIT_TIMEOUT_SECONDS = 60.0
#: The design's bound on the two commits' skew. Recorded on every result, and a bout that
#: exceeds it says so rather than claiming the checkouts were simultaneous.
COMMIT_SKEW_BOUND_MS = 100.0
#: How long one progress callback may take. Progress is observation and never holds up a bout.
PROGRESS_TIMEOUT_SECONDS = 2.0
#: Every order a bout writes carries a nonce with this prefix, so residue a crash left behind is
#: recognizably this demo's and nothing else ever is.
BOUT_NONCE_PREFIX = "r6-bout-"

Notify = Callable[[str], Awaitable[None]]


def _observed(callback: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """``callback`` as an observer: bounded, and never able to fail what it watches."""

    async def call(*arguments: Any) -> None:
        try:
            await asyncio.wait_for(callback(*arguments), timeout=PROGRESS_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - an observer never changes a bout
            LOGGER.debug("A Round 6 progress observer failed", exc_info=True)

    return call


class Round6RaceError(RuntimeError):
    """A Round 6 bout could not be prepared, run, or settled as the protocol requires."""


class Round6NotArmedError(Round6RaceError):
    """The lanes or the baselines are not in the state a bout may start from."""


class Round6SettleError(Round6RaceError):
    """The bout's rows could not be removed, or a lane could not be parked."""


class Round6Phase(StrEnum):
    PREPARING = "preparing"
    ARMED = "armed"
    STARTING = "starting"
    WAITING = "waiting"
    VERIFIED = "verified"
    FAILED = "failed"


Progress = Callable[[str, Round6Phase, str, float | None], Awaitable[None]]


class _ProgressRelay:
    """Deliver one bout's progress in order on its own task, so no verifier waits on it."""

    def __init__(self, callback: Progress) -> None:
        self._callback = _observed(callback)
        self._queue: asyncio.Queue[tuple[Any, ...] | None] = asyncio.Queue()
        self._task = asyncio.create_task(self._deliver(), name="round6-progress")

    def emit(self, lane_id: str, phase: Round6Phase, status: str, elapsed_ms: float | None) -> None:
        self._queue.put_nowait((lane_id, phase, status, elapsed_ms))

    async def _deliver(self) -> None:
        while (event := await self._queue.get()) is not None:
            await self._callback(*event)

    async def close(self) -> None:
        self._queue.put_nowait(None)
        await self._task

    def abort(self) -> None:
        self._task.cancel()


class Lane(Protocol):
    """One source and the pipeline that carries it into the lakehouse."""

    lane_id: str
    label: str

    async def confirm_parked(self, notify: Notify) -> None:
        """Return once the pipeline is at rest (parking it if it is running), or refuse."""

    async def read_source(self, order_id: str) -> LiveOrder | None:
        """The checkout's own read of one order on this lane's source."""

    async def commit(self, order: LiveOrder) -> None:
        """Commit one checkout order on this lane's source."""

    async def read_history(self, order: LiveOrder) -> bool:
        """Whether this lane's history holds ``order`` as exactly one insert."""

    async def start(self) -> None:
        """Start the pipeline from parked. Returns once the request is accepted."""

    async def failure(self) -> str | None:
        """The pipeline's own terminal failure since its start, if it has failed."""

    async def park(self, notify: Notify) -> None:
        """Stop the pipeline and wait until it is parked."""

    async def delete(self, order: LiveOrder) -> None:
        """Remove exactly ``order`` from this lane's source."""

    async def residue(self) -> list[LiveOrder]:
        """Orders a bout wrote that are still in this lane's source."""

    async def evidence(self, order: LiveOrder) -> Mapping[str, Any]:
        """Diagnostics read after the race, never on its clock."""


@dataclass(frozen=True)
class Round6Arm:
    arm_id: str
    armed_at: datetime
    baseline: LiveOrder
    lanes: tuple[str, ...]


@dataclass(frozen=True)
class Guardrail:
    """The separate checkout on one lane's source: its commit and its read-back."""

    commit_ms: float
    read_ms: float


@dataclass(frozen=True)
class Round6RaceResult:
    protocol: str
    arm_id: str
    order: LiveOrder
    guardrail_order: LiveOrder
    rung_at: datetime
    #: Each lane's checkout acknowledged, in milliseconds from the bell.
    commit_ack_ms: Mapping[str, float]
    #: How far apart the checkouts were acknowledged; None with one lane.
    commit_skew_ms: float | None
    outcomes: Mapping[str, LaneOutcome]
    guardrails: Mapping[str, Guardrail]
    resolution: Resolution

    @property
    def skew_within_bound(self) -> bool:
        return self.commit_skew_ms is None or self.commit_skew_ms <= COMMIT_SKEW_BOUND_MS


def new_bout_orders(baseline: LiveOrder) -> tuple[LiveOrder, LiveOrder]:
    """The bout's checkout and its guardrail: two new orders, each with a bout nonce."""

    def order(status: str) -> LiveOrder:
        return LiveOrder(
            order_id=str(uuid4()),
            sku=baseline.sku,
            store=baseline.store,
            quantity=baseline.quantity,
            total_cents=baseline.total_cents,
            status=status,
            proof_nonce=f"{BOUT_NONCE_PREFIX}{uuid4().hex[:16]}",
        )

    return order("checkout"), order("checkout-guardrail")


@dataclass
class _Clock:
    now_ns: Callable[[], int]
    origin_ns: int

    def elapsed_ms(self, at_ns: int | None = None) -> float:
        return ((self.now_ns() if at_ns is None else at_ns) - self.origin_ns) / 1_000_000


class Round6RaceEngine:
    """One Round 6 matchup: Lakebase and, when its AWS lane is sealed, one competitor."""

    def __init__(
        self,
        lanes: Sequence[Lane],
        *,
        baseline: LiveOrder,
        poll_seconds: float = POLL_SECONDS,
        lane_timeout_seconds: float = LANE_TIMEOUT_SECONDS,
        watch_seconds: float = WATCH_SECONDS,
        commit_timeout_seconds: float = COMMIT_TIMEOUT_SECONDS,
        clock_ns: Callable[[], int] = time.monotonic_ns,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not lanes or lanes[0].lane_id != LAKEBASE_LANE:
            raise ValueError("Round 6's first lane is always Lakebase")
        if len({lane.lane_id for lane in lanes}) != len(lanes):
            raise ValueError("Round 6's lanes must be distinct")
        self.lanes = tuple(lanes)
        self.baseline = baseline
        self.poll_seconds = poll_seconds
        self.lane_timeout_seconds = lane_timeout_seconds
        self.watch_seconds = watch_seconds
        self.commit_timeout_seconds = commit_timeout_seconds
        self._clock_ns = clock_ns
        self._now = now
        self._sleep = sleep
        self._used_arms: set[str] = set()
        #: Every order this engine rang, until settling has removed it from every source.
        self._owned: dict[str, LiveOrder] = {}
        # Work the bell issued that cancellation cannot prove did not happen: a commit or a
        # start already on the wire. Settling waits for it before deleting anything.
        self._in_flight: set[asyncio.Future[Any]] = set()

    @property
    def lane_ids(self) -> tuple[str, ...]:
        return tuple(lane.lane_id for lane in self.lanes)

    # -- Prepare ---------------------------------------------------------------------------

    async def prepare(self, notify: Notify) -> Round6Arm:
        """Every pipeline at rest and every source at its baseline, with no residue. Starts
        nothing.

        Residue a crash left behind (an order carrying a bout nonce) is removed first, visibly,
        which is settling's own work; nothing else in a source is ever touched.
        """

        notify = _observed(notify)
        await notify("Confirming every pipeline is at rest")
        await asyncio.gather(*(lane.confirm_parked(notify) for lane in self.lanes))
        leftovers = await asyncio.gather(*(lane.residue() for lane in self.lanes))
        if any(leftovers):
            await notify("Removing the orders an earlier bout left behind")
            for lane, orders in zip(self.lanes, leftovers, strict=True):
                for order in orders:
                    if not order.proof_nonce.startswith(BOUT_NONCE_PREFIX):
                        raise Round6NotArmedError(
                            f"{lane.label} reported residue this demo did not write"
                        )
                    await lane.delete(order)
        sources = await asyncio.gather(
            *(lane.read_source(self.baseline.order_id) for lane in self.lanes)
        )
        for lane, source in zip(self.lanes, sources, strict=True):
            if source != self.baseline:
                raise Round6NotArmedError(f"{lane.label}'s source is not at its sealed baseline")
        arm = Round6Arm(
            arm_id=uuid4().hex,
            armed_at=self._now(),
            baseline=self.baseline,
            lanes=self.lane_ids,
        )
        await notify("Every pipeline is at rest and every source reads its baseline")
        return arm

    # -- The bell ---------------------------------------------------------------------------

    async def run(
        self,
        arm: Round6Arm,
        order: LiveOrder,
        guardrail_order: LiveOrder,
        on_progress: Progress,
    ) -> Round6RaceResult:
        """Ring: commit the checkout everywhere at once, start the AWS pipeline, time each lane."""

        self._check_arm(arm, order, guardrail_order)
        self._used_arms.add(arm.arm_id)
        self._owned[order.proof_nonce] = order
        self._owned[guardrail_order.proof_nonce] = guardrail_order

        # Before the bell, not on any clock: read each source's baseline, which wakes a Lakebase
        # endpoint that has scaled to zero since Prepare (v1's warm-up).
        woken = await asyncio.gather(
            *(lane.read_source(self.baseline.order_id) for lane in self.lanes)
        )
        if any(row != self.baseline for row in woken):
            raise Round6NotArmedError("A source left its baseline after Prepare")

        progress = _ProgressRelay(on_progress)
        clock = _Clock(self._clock_ns, self._clock_ns())
        rung_at = self._now()
        starts = {lane.lane_id: self._track(lane.start()) for lane in self.lanes}
        commit_ack: dict[str, float] = {}
        commits = {lane.lane_id: self._track(lane.commit(order)) for lane in self.lanes}
        for lane_id, commit in commits.items():
            commit.add_done_callback(
                lambda future, lane_id=lane_id: (
                    commit_ack.setdefault(lane_id, clock.elapsed_ms())
                    if not future.cancelled() and future.exception() is None
                    else None
                )
            )
        for lane in self.lanes:
            progress.emit(lane.lane_id, Round6Phase.STARTING, "Committing the checkout", 0.0)
        pollers = {
            lane.lane_id: asyncio.create_task(
                self._poll(
                    lane,
                    order,
                    clock,
                    starts[lane.lane_id],
                    commits[lane.lane_id],
                    commit_ack,
                    progress.emit,
                ),
                name=f"round6-verify-{lane.lane_id}",
            )
            for lane in self.lanes
        }
        guards = {
            lane.lane_id: asyncio.create_task(
                self._guardrail(lane, guardrail_order), name=f"round6-guardrail-{lane.lane_id}"
            )
            for lane in self.lanes
        }
        try:
            outcomes_list = await asyncio.gather(*pollers.values())
            guardrails = dict(zip(guards, await asyncio.gather(*guards.values()), strict=True))
            await self._await_commits(commits)
            await progress.close()
        except BaseException:
            for task in (*pollers.values(), *guards.values()):
                task.cancel()
            await asyncio.gather(*pollers.values(), *guards.values(), return_exceptions=True)
            progress.abort()
            raise
        outcomes = {outcome.lane_id: outcome for outcome in outcomes_list}
        enriched: dict[str, LaneOutcome] = {}
        for lane in self.lanes:
            outcome = outcomes[lane.lane_id]
            try:
                evidence = dict(await lane.evidence(order))
            except Exception as exc:  # noqa: BLE001 - diagnostics never change a verdict
                LOGGER.warning("Round 6 %s evidence could not be read", lane.lane_id, exc_info=True)
                evidence = {"evidence_error": type(exc).__name__}
            enriched[lane.lane_id] = replace(outcome, evidence={**outcome.evidence, **evidence})
        acks = [commit_ack[lane_id] for lane_id in self.lane_ids if lane_id in commit_ack]
        skew = max(acks) - min(acks) if len(acks) == len(self.lanes) > 1 else None
        return Round6RaceResult(
            protocol=PROTOCOL,
            arm_id=arm.arm_id,
            order=order,
            guardrail_order=guardrail_order,
            rung_at=rung_at,
            commit_ack_ms=dict(commit_ack),
            commit_skew_ms=None if skew is None else round(skew, 3),
            outcomes=enriched,
            guardrails=guardrails,
            resolution=resolve(enriched),
        )

    def _check_arm(self, arm: Round6Arm, order: LiveOrder, guardrail: LiveOrder) -> None:
        if arm.arm_id in self._used_arms:
            raise Round6RaceError("This Prepare has already been rung")
        if arm.lanes != self.lane_ids or arm.baseline != self.baseline:
            raise Round6RaceError("The Round 6 matchup changed after Prepare")
        for candidate in (order, guardrail):
            if candidate.order_id == self.baseline.order_id:
                raise Round6RaceError("A bout's order must not be the baseline")
            if not candidate.proof_nonce.startswith(BOUT_NONCE_PREFIX):
                raise Round6RaceError("A bout's order must carry a bout nonce")
            if candidate.proof_nonce in self._owned:
                raise Round6RaceError("A bout's nonce has already been used")
        if order.order_id == guardrail.order_id or order.proof_nonce == guardrail.proof_nonce:
            raise Round6RaceError("The checkout guardrail must be a separate order")

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

    async def _await_commits(self, commits: Mapping[str, asyncio.Future[Any]]) -> None:
        try:
            await asyncio.wait_for(
                asyncio.gather(*(asyncio.shield(commit) for commit in commits.values())),
                timeout=self.commit_timeout_seconds,
            )
        except TimeoutError as exc:
            raise Round6RaceError(
                f"A checkout did not commit within {self.commit_timeout_seconds:.0f}s"
            ) from exc

    async def _guardrail(self, lane: Lane, order: LiveOrder) -> Guardrail:
        """v1's guardrail on this lane's source: a separate checkout, committed and read back."""

        began = self._clock_ns()
        await asyncio.wait_for(
            asyncio.shield(self._track(lane.commit(order))), timeout=self.commit_timeout_seconds
        )
        committed = self._clock_ns()
        read = await asyncio.wait_for(
            lane.read_source(order.order_id), timeout=self.commit_timeout_seconds
        )
        finished = self._clock_ns()
        if read != order:
            raise Round6RaceError(f"{lane.label}'s checkout guardrail did not read back exactly")
        return Guardrail(
            commit_ms=round((committed - began) / 1_000_000, 3),
            read_ms=round((finished - committed) / 1_000_000, 3),
        )

    async def _poll(
        self,
        lane: Lane,
        order: LiveOrder,
        clock: _Clock,
        start: asyncio.Future[Any],
        commit: asyncio.Future[Any],
        commit_ack: Mapping[str, float],
        report: Callable[[str, Round6Phase, str, float | None], None],
    ) -> LaneOutcome:
        reads = 0
        last_negative_start_ns: int | None = None
        previous_start_ns: int | None = None
        max_gap_ms = 0.0
        deadline_ns = clock.origin_ns + int(self.lane_timeout_seconds * 1_000_000_000)
        watch: asyncio.Task[str] | None = None
        failing: str | None = None

        def outcome(
            verified: bool, failure: str | None, at_ns: int | None, *, timed_out: bool = False
        ) -> LaneOutcome:
            ack = commit_ack.get(lane.lane_id)
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
                    return outcome(False, f"{lane.label}'s checkout did not commit", None)
                if start.done() and not start.cancelled() and start.exception() is not None:
                    return outcome(
                        False, f"{lane.label} could not be started: {start.exception()}", None
                    )
                began_ns = clock.now_ns()
                if previous_start_ns is not None:
                    max_gap_ms = max(max_gap_ms, (began_ns - previous_start_ns) / 1_000_000)
                previous_start_ns = began_ns
                try:
                    found = await lane.read_history(order)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - a failed read is a negative read
                    LOGGER.warning("Round 6 %s verifier read failed: %s", lane.lane_id, exc)
                    found = False
                finished_ns = clock.now_ns()
                reads += 1
                # A row read before its own commit was acknowledged would be a read of another
                # bout's order; the nonce makes that impossible, and the ack bounds it anyway.
                if found and lane.lane_id in commit_ack:
                    report(
                        lane.lane_id,
                        Round6Phase.VERIFIED,
                        "The exact order is in the lakehouse",
                        clock.elapsed_ms(finished_ns),
                    )
                    return outcome(True, None, finished_ns)
                last_negative_start_ns = began_ns
                if finished_ns >= deadline_ns:
                    return outcome(
                        False,
                        f"{lane.label} did not deliver the order within "
                        f"{self.lane_timeout_seconds:.0f}s of the bell",
                        None,
                        timed_out=True,
                    )
                if failing is not None:
                    return outcome(False, failing, None)
                if start.done() and watch is None:
                    report(
                        lane.lane_id,
                        Round6Phase.WAITING,
                        f"{lane.label} is carrying the order",
                        clock.elapsed_ms(finished_ns),
                    )
                    watch = asyncio.create_task(
                        self._watch(lane), name=f"round6-watch-{lane.lane_id}"
                    )
                if watch is not None and watch.done():
                    # The pipeline says it failed. The next read decides: an order that landed
                    # before the failure was still delivered.
                    failing = watch.result()
                await self._sleep(max(0.0, self.poll_seconds - (clock.now_ns() - began_ns) / 1e9))
        finally:
            if watch is not None and not watch.done():
                watch.cancel()
                await asyncio.gather(watch, return_exceptions=True)

    async def _watch(self, lane: Lane) -> str:
        while True:
            try:
                failed = await lane.failure()
            except Exception:  # noqa: BLE001 - an unreadable status is not a failure
                failed = None
            if failed:
                return failed
            await self._sleep(self.watch_seconds)

    # -- Settle -----------------------------------------------------------------------------

    async def settle(self, notify: Notify | None = None) -> None:
        """Remove every order this engine rang from every source, and park every lane.

        Waits first for whatever the bell issued, because a commit or a start already on the wire
        may still land. Parks every lane even when a delete failed, so a settle that cannot finish
        its cleanup never leaves a pipeline billing.
        """

        say = _observed(notify or _quiet)
        pending = tuple(self._in_flight)
        if pending:
            await asyncio.gather(
                *(asyncio.shield(item) for item in pending), return_exceptions=True
            )
        failures: list[str] = []
        try:
            owned = tuple(self._owned.values())
            if owned:
                await say("Removing the bout's orders from every source")
            for order in owned:
                results = await asyncio.gather(
                    *(lane.delete(order) for lane in self.lanes), return_exceptions=True
                )
                bad = [
                    f"{lane.label}: {result}"
                    for lane, result in zip(self.lanes, results, strict=True)
                    if isinstance(result, BaseException)
                ]
                if bad:
                    failures.extend(bad)
                else:
                    self._owned.pop(order.proof_nonce, None)
        finally:
            await say("Parking every pipeline")
            parked = await asyncio.gather(
                *(lane.park(say) for lane in self.lanes), return_exceptions=True
            )
            failures.extend(
                f"{lane.label}: {result}"
                for lane, result in zip(self.lanes, parked, strict=True)
                if isinstance(result, BaseException)
            )
        if failures:
            raise Round6SettleError("Round 6 could not settle: " + "; ".join(failures))

    async def settle_and_cleanup_owned(self) -> None:
        """What the manager's settlement and towel paths call: ``settle`` with no progress."""

        await self.settle()


async def _quiet(_message: str) -> None:
    return None


__all__ = [
    "BOUT_NONCE_PREFIX",
    "COMMIT_SKEW_BOUND_MS",
    "COMPETITOR_LANE",
    "LAKEBASE_LANE",
    "LANE_TIMEOUT_SECONDS",
    "PROTOCOL",
    "Guardrail",
    "Lane",
    "LaneOutcome",
    "Resolution",
    "Round6Arm",
    "Round6NotArmedError",
    "Round6Phase",
    "Round6RaceEngine",
    "Round6RaceError",
    "Round6RaceResult",
    "Round6SettleError",
    "Verdict",
    "new_bout_orders",
]
