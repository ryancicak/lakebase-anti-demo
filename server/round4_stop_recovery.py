"""Repay a Round 4 pipeline stop inherited from a dead serving process.

The durable ``stop_owed`` event is a promise made by a settled bout. This
module is the replacement process that keeps it, but only after the promise's
exact due time and only while holding the same scoped ring a real Round 4 bout
must own. The claim and the post-claim power-history read are the safety case:
the first prevents a new arm from racing the stop, and the second lets a newer
start, stop, or redo window supersede the stale event that woke this process.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from . import pipeline_power
from .coordination import (
    BoutLeaseStore,
    LeaseHeldError,
    is_transient_coordination_error,
    round_ring_key,
)
from .manifest import DemoManifest
from .model_score import ManagedSyncState
from .model_score_live import (
    PipelineSignals,
    classify_managed_sync_state,
    pipeline_signals_are_healthy,
    pipeline_signals_prove_deliberate_stop,
    read_pipeline_signals,
)
from .models import BoutOperator, RoundId, SessionState
from .readiness import SETTLED, RecoveryState

LOGGER = logging.getLogger(__name__)

RECOVERY_PHASE = "round4_stop_recovery"
RECOVERY_LEASE_TTL = timedelta(minutes=10)
RECOVERY_RETRY_BASE_SECONDS = 0.5
RECOVERY_RETRY_MAX_SECONDS = 30.0

_OPERATOR_STOP_ACTOR = "operator stop confirmed by startup recovery"


class Round4StopRecoveryError(RuntimeError):
    """Inherited stop debt could not be settled safely."""


class Round4StopStillSettlingError(Round4StopRecoveryError):
    """Live signals are moving toward a state worth re-reading."""


class Round4StopRecoveryRefusedError(Round4StopRecoveryError):
    """Live signals cannot prove that recording a deliberate stop is safe."""


RecoveryOutcome = Literal["settled", "deferred"]


class InheritedRound4StopRecovery:
    """One restart-safe reconciler for the sealed Round 4 pipeline."""

    def __init__(
        self,
        manifest: DemoManifest,
        ring_store: BoutLeaseStore,
        power_store: pipeline_power.DurablePipelinePowerStore,
        inherited: Mapping[str, Any],
        *,
        read_signals: Callable[[], Awaitable[PipelineSignals]],
        stop_pipeline: Callable[[], Awaitable[dict[str, Any]]],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        retry_base_seconds: float = RECOVERY_RETRY_BASE_SECONDS,
        retry_max_seconds: float = RECOVERY_RETRY_MAX_SECONDS,
        lease_ttl: timedelta = RECOVERY_LEASE_TTL,
    ) -> None:
        installation_id = str(getattr(manifest, "installation_id", "") or "")
        if manifest.manifest_version != 7 or not installation_id:
            raise Round4StopRecoveryRefusedError(
                "Inherited Round 4 stop recovery requires a scoped v7 installation ring"
            )
        expected_ring = round_ring_key(
            installation_id,
            RoundId.PUT_MODEL_SCORE_IN_APP.value,
        )
        if ring_store.ring_key != expected_ring:
            raise Round4StopRecoveryRefusedError(
                "Inherited Round 4 stop recovery was not given the exact Round 4 ring"
            )
        self._manifest = manifest
        self._ring_store = ring_store
        self._power_store = power_store
        self._record = dict(inherited)
        self._read_signals = read_signals
        self._stop_pipeline = stop_pipeline
        self._now = now
        self._sleep = sleep
        self._retry_base_seconds = max(0.0, retry_base_seconds)
        self._retry_max_seconds = max(self._retry_base_seconds, retry_max_seconds)
        self._lease_ttl = lease_ttl
        self._status = SETTLED
        self._lease_held = False

    @property
    def status(self) -> RecoveryState:
        return self._status

    @property
    def lease_held(self) -> bool:
        """Whether this replica currently owns the fenced recovery phase."""

        return self._lease_held

    async def run(self) -> None:
        """Retry transient failures for this process's lifetime; never stop serving."""

        failures = 0
        while True:
            due_delay = self._seconds_until_due(self._record)
            if due_delay > 0:
                self._status = RecoveryState(
                    "retrying",
                    detail=(
                        "ROUND 4 PIPELINE STOP RECOVERY IS WAITING FOR THE REDO WINDOW "
                        "TO END · THE PIPELINE WILL NOT BE STOPPED EARLY"
                    ),
                    next_attempt_seconds=due_delay,
                )
                await self._sleep(due_delay)

            try:
                outcome = await self._attempt_once()
            except asyncio.CancelledError:
                raise
            except LeaseHeldError:
                failures = 0
                delay = self._retry_base_seconds
                self._status = RecoveryState(
                    "retrying",
                    detail=(
                        "ROUND 4 PIPELINE STOP RECOVERY DEFERRED · A ROUND 4 BOUT "
                        "OWNS THE RING · THE LATEST POWER INTENT WILL BE RE-READ "
                        "AFTER IT RELEASES"
                    ),
                    next_attempt_seconds=delay,
                )
                await self._sleep(delay)
                continue
            except Exception as exc:  # noqa: BLE001 - classified immediately
                if isinstance(exc, Round4StopStillSettlingError) or (
                    is_transient_coordination_error(exc)
                ):
                    failures += 1
                    delay = self._retry_delay(failures)
                    self._status = RecoveryState(
                        "retrying",
                        attempts=failures,
                        detail=(
                            "ROUND 4 PIPELINE STOP RECOVERY RETRYING · "
                            f"ATTEMPT {failures} FAILED ({type(exc).__name__.upper()}) · "
                            f"NEXT ATTEMPT IN {delay:g}S"
                        ),
                        next_attempt_seconds=delay,
                        error=type(exc).__name__,
                    )
                    LOGGER.warning(
                        "Transient Round 4 inherited stop recovery failure; retrying "
                        "in %.1fs (attempt %d)",
                        delay,
                        failures,
                        exc_info=True,
                    )
                    await self._sleep(delay)
                    continue
                self._status = RecoveryState(
                    "given_up",
                    attempts=max(1, failures + 1),
                    detail=(
                        "ROUND 4 PIPELINE STOP RECOVERY REFUSED · NOT RETRYING · "
                        f"{exc}"
                    ),
                    error=type(exc).__name__,
                )
                LOGGER.error(
                    "Round 4 inherited pipeline stop debt remains visible because "
                    "automatic recovery was permanently refused: %s",
                    exc,
                    exc_info=True,
                )
                return

            failures = 0
            if outcome == "settled":
                self._status = RecoveryState(
                    "settled",
                    detail="ROUND 4 INHERITED PIPELINE STOP DEBT SETTLED",
                )
                return
            # A newer owed event carries its own redo window. `_attempt_once`
            # installed it as both the next record and the health snapshot.

    async def _attempt_once(self) -> RecoveryOutcome:
        """Claim, re-read, act if still owed, and always release the exact lease."""

        lease = await self._ring_store.claim(
            session_id=f"round4-stop-recovery-{self._manifest.run_id}",
            operator=self._operator(),
            phase=RECOVERY_PHASE,
            session_state=SessionState.RUNNING,
            round_id=RoundId.PUT_MODEL_SCORE_IN_APP.value,
            round_title="Round 4 pipeline stop recovery",
            competitor_id="sealed_pipeline",
            competitor_name="Sealed Managed Sync pipeline",
            ttl=self._lease_ttl,
        )
        self._lease_held = True
        holder = [lease]
        heartbeat = asyncio.create_task(
            self._heartbeat(holder),
            name="round4-stop-recovery-heartbeat",
        )
        operation = asyncio.create_task(
            self._act_under_fence(),
            name="round4-stop-recovery-operation",
        )
        try:
            done, _ = await asyncio.wait(
                {heartbeat, operation},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if operation in done:
                return operation.result()
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            heartbeat.result()
            raise Round4StopRecoveryError(
                "Round 4 stop recovery heartbeat ended without an error"
            )
        except asyncio.CancelledError:
            # In particular, wait for `stop_exact_pipeline` to finish its
            # uncancellable SDK worker before releasing the fence. Otherwise a
            # shutdown could close the stores while a stop mutation remained.
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            raise
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            try:
                released = await self._ring_store.release(holder[0])
                if not released:
                    LOGGER.error(
                        "Round 4 stop recovery could not confirm release of its exact "
                        "fenced lease; its finite TTL remains the safety bound"
                    )
            except Exception:
                LOGGER.error(
                    "Round 4 stop recovery could not release its fenced lease; its "
                    "finite TTL remains the safety bound",
                    exc_info=True,
                )
            finally:
                self._lease_held = False

    async def _heartbeat(self, holder: list[Any]) -> None:
        """Keep ownership current for a slow control-plane or coordination call."""

        interval = max(0.1, min(30.0, self._lease_ttl.total_seconds() / 3))
        while True:
            await asyncio.sleep(interval)
            holder[0] = await self._ring_store.renew(
                holder[0],
                ttl=self._lease_ttl,
            )

    async def _act_under_fence(self) -> RecoveryOutcome:
        """Re-read the authority after claiming; no stale snapshot may reach a verb."""

        pipeline_id = pipeline_power._sealed_pipeline_id(self._manifest)
        outcome = await self._latest_debt_outcome(pipeline_id)
        if outcome is not None:
            return outcome

        signals = await self._read_signals()
        # Signal reads are control-plane round trips. Re-read the append-only
        # authority once more after them, immediately before any mutation, so
        # an in-flight newer start, stop, or post-bout redo window cannot land
        # during those reads and still be overwritten by stale recovery.
        outcome = await self._latest_debt_outcome(pipeline_id)
        if outcome is not None:
            return outcome

        if pipeline_signals_are_healthy(signals):
            stopped = await self._stop_pipeline()
        elif pipeline_signals_prove_deliberate_stop(signals):
            # The laptop command cannot write the app's coordination row. The
            # three live signals prove its effect, so settle the durable debt
            # without issuing a second mutation.
            stopped = pipeline_power.stopped_record(
                self._manifest,
                now=self._now,
                stopped_by=_OPERATOR_STOP_ACTOR,
            )
        else:
            state = classify_managed_sync_state(
                {signals.synced_table_state},
                signals.pipeline_state,
                signals.update_state,
            )
            if state is ManagedSyncState.STARTING or (
                signals.pipeline_state.strip().upper() == "STOPPING"
                or signals.update_state.strip().upper() == "STOPPING"
            ):
                raise Round4StopStillSettlingError(
                    f"pipeline signals are still settling ({signals.describe()})"
                )
            raise Round4StopRecoveryRefusedError(
                "pipeline inactivity is not a proven deliberate stop "
                f"({signals.describe()}); debt remains visible"
            )

        self._validate_stopped_record(stopped, pipeline_id)
        # Await the append. A fire-and-forget write would let `/readyz` clear
        # before the authority the next replica reads contains the repayment.
        await self._power_store.append(stopped)
        pipeline_power.install_owed_stop_snapshot(None)
        LOGGER.info(
            "Settled inherited Round 4 pipeline stop debt under the scoped recovery fence"
        )
        return "settled"

    async def _latest_debt_outcome(
        self,
        pipeline_id: str,
    ) -> RecoveryOutcome | None:
        """Refresh the authority; return an outcome when no stop may run now."""

        newest = await self._power_store.latest(pipeline_id)
        if newest is None:
            raise Round4StopRecoveryRefusedError(
                "the durable power history returned no newest event"
            )
        if str(newest.get("pipeline_id") or "") != pipeline_id:
            raise Round4StopRecoveryRefusedError(
                "the newest durable power event does not name the sealed pipeline"
            )
        if pipeline_power._intent(newest) != pipeline_power._INTENT_STOP_OWED:
            # A newer start or stop superseded the inherited event while this
            # process was waiting to claim. It is the authoritative answer.
            pipeline_power.install_owed_stop_snapshot(None)
            return "settled"

        self._record = dict(newest)
        pipeline_power.install_owed_stop_snapshot(newest)
        if self._seconds_until_due(newest) > 0:
            return "deferred"
        return None

    def _operator(self) -> BoutOperator:
        round4 = self._manifest.round4
        assert round4 is not None
        principal = str(round4.app_service_principal_client_id or "").casefold()
        return BoutOperator(
            display_name="Round 4 pipeline recovery",
            subject=f"maintenance:{principal}",
        )

    def _seconds_until_due(self, record: Mapping[str, Any]) -> float:
        due = pipeline_power._parse_stamp(record.get("owed_at"))
        if due is None:
            # Same fail-noisy direction as `read_stop_marker`: an unreadable
            # owed time is due, never an excuse to preserve a possibly running
            # pipeline forever.
            return 0.0
        now = self._now().astimezone(UTC)
        return max(0.0, (due.astimezone(UTC) - now).total_seconds())

    def _retry_delay(self, failures: int) -> float:
        exponent = min(max(0, failures - 1), 16)
        return min(
            self._retry_max_seconds,
            self._retry_base_seconds * (2**exponent),
        )

    @staticmethod
    def _validate_stopped_record(record: Mapping[str, Any], pipeline_id: str) -> None:
        if (
            pipeline_power._intent(record) != pipeline_power._INTENT_STOPPED
            or str(record.get("pipeline_id") or "") != pipeline_id
            or not record.get("stopped_at")
        ):
            raise Round4StopRecoveryRefusedError(
                "the recovered stop did not form an exact durable stopped event"
            )


def build_inherited_round4_stop_recovery(
    manifest: DemoManifest,
    lease_store: BoutLeaseStore,
    power_store: pipeline_power.DurablePipelinePowerStore,
    inherited: Mapping[str, Any],
    *,
    workspace: Any,
) -> InheritedRound4StopRecovery:
    """Bind the production control-plane reads and exact guarded stop."""

    installation_id = str(getattr(manifest, "installation_id", "") or "")
    ring_store = lease_store.for_ring_key(
        round_ring_key(
            installation_id,
            RoundId.PUT_MODEL_SCORE_IN_APP.value,
        )
    )
    api = pipeline_power.workspace_api(workspace)
    pipeline_id = pipeline_power._sealed_pipeline_id(manifest)

    async def signals() -> PipelineSignals:
        return await read_pipeline_signals(
            manifest,
            api,
            pipeline_id=pipeline_id,
        )

    async def stop_exact_pipeline() -> dict[str, Any]:
        records: list[dict[str, Any]] = []

        def issue() -> None:
            pipeline_power.stop(
                manifest,
                api,
                on_record=records.append,
            )

        worker = asyncio.create_task(
            asyncio.to_thread(issue),
            name="round4-pipeline-stop-control-plane",
        )
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError:
            # A thread cannot be cancelled after the SDK call starts. Keep the
            # recovery lease until it finishes so shutdown cannot release the
            # ring and close the stores while a mutation is still in flight.
            await asyncio.shield(worker)
            raise
        if len(records) != 1:
            raise Round4StopRecoveryError(
                "the guarded pipeline stop did not produce one exact power event"
            )
        return records[0]

    return InheritedRound4StopRecovery(
        manifest,
        ring_store,
        power_store,
        inherited,
        read_signals=signals,
        stop_pipeline=stop_exact_pipeline,
    )
