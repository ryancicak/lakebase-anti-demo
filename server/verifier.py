from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from .models import LaneState


class FatalProbeError(RuntimeError):
    """A correctness failure that must never be hidden by retries."""


class VerifierStopped(RuntimeError):
    """The verifier settled its lane tasks after an explicit stop request."""


class PreparedTarget(Protocol):
    id: str
    name: str

    async def attempt(self, nonce: str, expected_value: str, timeout_seconds: float) -> None:
        """Complete one connection, transaction, commit, and read-back attempt."""


EventCallback = Callable[[str, str, dict[str, object]], Awaitable[None]]


@dataclass(frozen=True)
class RetryPolicy:
    # Aurora documents resumes around 15 seconds and sometimes 30+ seconds after
    # a long pause. One realistic connection attempt must be allowed to survive
    # that wake instead of being cancelled every four seconds.
    overall_timeout_seconds: float = 90.0
    attempt_timeout_seconds: float = 35.0
    initial_delay_seconds: float = 0.25
    maximum_delay_seconds: float = 1.0


@dataclass(frozen=True)
class LaneResult:
    id: str
    name: str
    state: LaneState
    elapsed_ms: float | None
    attempts: int
    first_attempt_ns: int
    completed_ns: int | None
    error: str | None


@dataclass(frozen=True)
class VerificationResult:
    started_ns: int
    completed_ns: int
    # None means there was no simultaneous start to measure. Rounds 4 and 6 never
    # build the AWS lane and Round 1 against RDS never connects it, so exactly one
    # lane launches. Reporting 0.0 there would advertise a perfect shared start
    # that was never measured.
    launch_skew_ms: float | None
    lanes: dict[str, LaneResult]


def compute_launch_skew_ms(launch_points: Sequence[int]) -> float | None:
    """The spread between the earliest and latest lane start, in milliseconds.

    A skew is only meaningful as a difference between two real starts, so fewer
    than two launch points yields None rather than zero.
    """
    if len(launch_points) < 2:
        return None
    return (max(launch_points) - min(launch_points)) / 1_000_000


class NeutralVerifier:
    def __init__(self, retry_policy: RetryPolicy | None = None) -> None:
        self.retry_policy = retry_policy or RetryPolicy()

    async def run(
        self,
        targets: Sequence[PreparedTarget],
        nonce: str,
        expected_value: str,
        on_event: EventCallback,
        stop_event: asyncio.Event | None = None,
    ) -> VerificationResult:
        if not targets:
            raise ValueError("At least one eligible target is required")
        if stop_event is not None and stop_event.is_set():
            raise VerifierStopped("The live verification run was stopped.")
        barrier = asyncio.Event()
        start_holder: dict[str, int] = {}

        tasks = [
            asyncio.create_task(
                self._run_lane(
                    target,
                    barrier,
                    start_holder,
                    nonce,
                    expected_value,
                    on_event,
                    stop_event,
                ),
                name=f"anti-demo-{target.id}",
            )
            for target in targets
        ]
        stop_waiter = (
            asyncio.create_task(stop_event.wait(), name="anti-demo-verifier-stop")
            if stop_event is not None
            else None
        )

        try:
            await asyncio.sleep(0)
            started_ns = time.monotonic_ns()
            start_holder["started_ns"] = started_ns
            barrier.set()

            if stop_waiter is None:
                results = await asyncio.gather(*tasks)
            else:
                pending: set[asyncio.Task[LaneResult]] = set(tasks)
                while pending:
                    done, _ = await asyncio.wait(
                        (*pending, stop_waiter),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if stop_waiter in done or stop_event.is_set():
                        raise VerifierStopped("The live verification run was stopped.")
                    pending.difference_update(done.intersection(pending))
                results = [task.result() for task in tasks]

            if stop_event is not None and stop_event.is_set():
                raise VerifierStopped("The live verification run was stopped.")
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            if stop_waiter is not None:
                stop_waiter.cancel()
                await asyncio.gather(stop_waiter, return_exceptions=True)

        completed_ns = time.monotonic_ns()
        launch_points = [result.first_attempt_ns for result in results]
        return VerificationResult(
            started_ns=started_ns,
            completed_ns=completed_ns,
            launch_skew_ms=compute_launch_skew_ms(launch_points),
            lanes={result.id: result for result in results},
        )

    async def _run_lane(
        self,
        target: PreparedTarget,
        barrier: asyncio.Event,
        start_holder: dict[str, int],
        nonce: str,
        expected_value: str,
        on_event: EventCallback,
        stop_event: asyncio.Event | None,
    ) -> LaneResult:
        await barrier.wait()
        self._raise_if_stopped(stop_event)
        started_ns = start_holder["started_ns"]
        deadline_ns = started_ns + int(self.retry_policy.overall_timeout_seconds * 1e9)
        first_attempt_ns = time.monotonic_ns()
        attempts = 0
        last_error = "The application transaction could not be verified."

        while time.monotonic_ns() < deadline_ns:
            self._raise_if_stopped(stop_event)
            attempts += 1
            elapsed_ms = (time.monotonic_ns() - started_ns) / 1_000_000
            await self._emit_unless_stopped(
                stop_event,
                on_event,
                target.id,
                LaneState.CONNECTING,
                {"attempts": attempts, "elapsed_ms": elapsed_ms, "status": "Connecting"},
            )
            remaining_seconds = max(0.001, (deadline_ns - time.monotonic_ns()) / 1e9)
            attempt_timeout = min(self.retry_policy.attempt_timeout_seconds, remaining_seconds)

            try:
                async with asyncio.timeout(attempt_timeout):
                    await target.attempt(nonce, expected_value, attempt_timeout)
            except FatalProbeError as exc:
                last_error = str(exc)
                break
            except Exception:
                last_error = "The application transaction could not be verified before timeout."
            else:
                self._raise_if_stopped(stop_event)
                completed_ns = time.monotonic_ns()
                verified_ms = (completed_ns - started_ns) / 1_000_000
                await self._emit_unless_stopped(
                    stop_event,
                    on_event,
                    target.id,
                    LaneState.VERIFIED,
                    {
                        "attempts": attempts,
                        "elapsed_ms": verified_ms,
                        "status": "Transaction verified",
                    },
                )
                return LaneResult(
                    id=target.id,
                    name=target.name,
                    state=LaneState.VERIFIED,
                    elapsed_ms=verified_ms,
                    attempts=attempts,
                    first_attempt_ns=first_attempt_ns,
                    completed_ns=completed_ns,
                    error=None,
                )

            delay = min(
                self.retry_policy.initial_delay_seconds * (2 ** max(0, attempts - 1)),
                self.retry_policy.maximum_delay_seconds,
            )
            sleep_seconds = min(delay, max(0, (deadline_ns - time.monotonic_ns()) / 1e9))
            if stop_event is None:
                await asyncio.sleep(sleep_seconds)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=sleep_seconds)
                except TimeoutError:
                    pass
                self._raise_if_stopped(stop_event)

        await self._emit_unless_stopped(
            stop_event,
            on_event,
            target.id,
            LaneState.FAILED,
            {"attempts": attempts, "elapsed_ms": None, "status": "Could not verify"},
        )
        return LaneResult(
            id=target.id,
            name=target.name,
            state=LaneState.FAILED,
            elapsed_ms=None,
            attempts=attempts,
            first_attempt_ns=first_attempt_ns,
            completed_ns=None,
            error=last_error,
        )

    @staticmethod
    def _raise_if_stopped(stop_event: asyncio.Event | None) -> None:
        if stop_event is not None and stop_event.is_set():
            raise VerifierStopped("The live verification run was stopped.")

    async def _emit_unless_stopped(
        self,
        stop_event: asyncio.Event | None,
        on_event: EventCallback,
        lane_id: str,
        state: str,
        payload: dict[str, object],
    ) -> None:
        self._raise_if_stopped(stop_event)
        await on_event(lane_id, state, payload)
