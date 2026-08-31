import ast
import asyncio
import logging
import pathlib
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import psycopg
import pytest
from botocore.exceptions import ClientError
from databricks.sdk.errors.platform import PermissionDenied

from server import model_score_live
from server.connection_spike import (
    ConnectionSpikeGates,
    ConnectionSpikeLaneResult,
    ConnectionSpikeRunResult,
    PublicSetupEvidence,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupStopGateEvidence,
    arm_setup_phase,
)
from server.connection_spike_live import ConnectionSpikeLiveOperationError
from server.coordination import InMemoryBoutLeaseStore, LeaseLostError, round_ring_key
from server.manager import (
    COST_LEDGER_GRANT_HEADLINE,
    AmbiguousRingQueryError,
    InvalidStateError,
    RunManager,
    SessionRecord,
    _redacted_exception_chain,
    arm_failure_message,
    authorization_refusal,
    cost_window_refusal,
    operator_diagnosis,
)
from server.manifest import DemoManifest
from server.model_score import (
    ModelScoreArm,
    ModelScoreContract,
    ModelScoreEngine,
    ModelScorePhase,
    ModelScoreProgress,
    ModelScoreProofKind,
    ModelScoreProofResult,
    ModelScoreRunResult,
    is_owned_prior_proof,
)
from server.models import (
    BoutOperator,
    ComparisonKind,
    CompetitorId,
    CooldownLaneState,
    CooldownState,
    Corner,
    LaneState,
    RedoState,
    ResetMode,
    RoundId,
    SessionCreate,
    SessionState,
    TowelState,
)
from server.pricing import build_cost_receipt, calculate_rds_proxy_cost
from server.receipts import derive_receipt, load_receipts
from server.recovery import (
    RecoveryLaneResult,
    RecoveryPhase,
    RecoveryProgress,
    RecoveryResetError,
    RecoveryRunResult,
    RecoveryStoppedResult,
)
from server.round_availability import GRANT_REFUSAL_HEADLINE
from server.safe_change import (
    SafeChangeArm,
    SafeChangeLaneArm,
    SafeChangeLaneResult,
    SafeChangeLaneState,
    SafeChangeOwnershipScope,
    SafeChangePhase,
    SafeChangePlan,
    SafeChangeProgress,
    SafeChangeProvider,
    SafeChangeResetError,
    SafeChangeResetLaneResult,
    SafeChangeResetResult,
    SafeChangeRunResult,
)
from server.targets import TargetNotArmedError
from server.verifier import FatalProbeError, NeutralVerifier, RetryPolicy


@pytest.mark.parametrize(
    ("competitor_id", "billable_seconds", "expected_quantity", "expected_cost"),
    [
        (CompetitorId.AURORA_SERVERLESS_V2, 600, 8 / 6, 0.020),
        (CompetitorId.RDS_POSTGRES, 599, 2 / 6, 0.005),
        (CompetitorId.AURORA_SERVERLESS_V2, 1200, 8 / 3, 0.040),
        (CompetitorId.AURORA_SERVERLESS_V2, 1296, 2.88, 0.0432),
        (CompetitorId.RDS_POSTGRES, 1200, 2 / 3, 0.010),
    ],
)
def test_rds_proxy_cost_uses_capacity_lifetime_and_ten_minute_minimum(
    competitor_id: CompetitorId,
    billable_seconds: float,
    expected_quantity: float,
    expected_cost: float,
) -> None:
    quantity, cost = calculate_rds_proxy_cost(competitor_id, billable_seconds)

    assert quantity == pytest.approx(expected_quantity)
    assert cost == pytest.approx(expected_cost)
    if billable_seconds == 1296:
        assert cost == 0.0432


@pytest.mark.parametrize(
    ("round_id", "native_component", "external_component"),
    [
        (
            RoundId.PUT_MODEL_SCORE_IN_APP,
            "Lakeflow Connect managed sync compute",
            "Required external reverse-ETL product",
        ),
        (
            RoundId.ANALYZE_LIVE_ORDERS,
            "Lakebase native CDF change-feed processing",
            "Required external CDC-to-Delta stack",
        ),
    ],
)
def test_native_pipeline_round_receipts_exclude_aws_database_and_keep_unknowns_pending(
    round_id: RoundId,
    native_component: str,
    external_component: str,
) -> None:
    receipt = build_cost_receipt(round_id, CompetitorId.AURORA_SERVERLESS_V2)
    components = {line.component: line for line in receipt.lines}

    assert not any(
        line.component.startswith(("Aurora ", "RDS PostgreSQL", "Database public"))
        for line in receipt.lines
    )
    assert components[native_component].status == "usage_pending"
    assert components["Databricks SQL warehouse query compute"].quantity is None
    assert any(line.component.startswith("Delta ") for line in receipt.lines)
    assert components[external_component].status == "selection_required"
    assert receipt.known_bout_estimate_usd is None


@pytest.mark.parametrize(
    ("round_id", "artifact_label"),
    [
        (RoundId.MAKE_SCHEMA_CHANGE_SAFELY, "isolated"),
        (RoundId.RECOVER_DELETED_ORDER, "recovery"),
    ],
)
def test_ephemeral_artifact_receipts_do_not_turn_missing_usage_into_zero(
    round_id: RoundId,
    artifact_label: str,
) -> None:
    receipt = build_cost_receipt(round_id, CompetitorId.RDS_POSTGRES)
    artifacts = [
        line
        for line in receipt.lines
        if "temporary" in line.component.lower() or artifact_label in line.component.lower()
    ]

    assert {line.lane_id for line in artifacts} == {"lakebase", "competitor"}
    assert all(line.scope == "bout_estimate" for line in artifacts)
    assert all(line.quantity is None and line.subtotal_usd is None for line in artifacts)
    assert receipt.known_bout_estimate_usd is None
    assert receipt.known_monthly_carrying_cost_usd == pytest.approx(2.70)


@dataclass
class FakePreparedTarget:
    id: str
    name: str
    delay: float
    fatal: bool = False

    async def attempt(self, nonce: str, expected_value: str, timeout_seconds: float) -> None:
        await asyncio.sleep(self.delay)
        if self.fatal:
            raise FatalProbeError("The Lakebase application transaction did not verify.")


@dataclass
class FakeLiveTarget:
    id: str
    name: str
    delay: float
    eligible: bool = True
    fatal: bool = False
    prepare_calls: int = 0
    arm_calls: int = 0

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.arm_calls += 1
        if not self.eligible:
            return {
                "eligible": False,
                "state": "NO_SCALE_TO_ZERO",
                "reason": "No automatic connection-triggered wake.",
            }
        evidence: dict[str, object] = {
            "state": "IDLE" if self.id == "lakebase" else "ZERO"
        }
        if self.id == "lakebase" and not_before is not None:
            evidence["provider_updated_at"] = not_before.isoformat()
        return evidence

    async def prepare(self) -> FakePreparedTarget:
        self.prepare_calls += 1
        if not self.eligible:
            raise AssertionError("An unsupported RDS lane must never be prepared")
        return FakePreparedTarget(self.id, self.name, self.delay, self.fatal)


class FakeResolver:
    def resolve(self, competitor: CompetitorId):
        return (
            FakeLiveTarget("lakebase", "Lakebase", 0.005),
            FakeLiveTarget("competitor", "Aurora Serverless v2", 0.025),
        )


class BlockingConnectionSpikeEngine:
    def __init__(self) -> None:
        self.run_entered = asyncio.Event()
        self.run_cancelled_and_settled = asyncio.Event()
        self.cleanup_finished = asyncio.Event()

    async def check(self):
        return object()

    async def run(self, _arm, _on_progress):
        self.run_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # Model a command appearing while dispatch is pending: cancellation
            # must be awaited until the adapter's exact-command settlement ends.
            await asyncio.sleep(0.01)
            self.run_cancelled_and_settled.set()
            raise

    async def cancel_and_cleanup(self, _arm) -> None:
        if not self.run_cancelled_and_settled.is_set():
            raise AssertionError("cleanup probed before pending dispatch settled")
        self.cleanup_finished.set()


class SuccessfulTwoPhaseConnectionSpikeEngine:
    def __init__(
        self,
        *,
        burst_valid: bool,
        cleanup_failure: bool = False,
        cleanup_abandoned: bool = False,
    ) -> None:
        self.burst_valid = burst_valid
        self.cleanup_failure = cleanup_failure
        # Cleanup that fails and keeps failing, without touching the run. The
        # separate knob is the point: `cleanup_failure` fails the run too, so it
        # cannot express the case that matters most here -- a bout that verified
        # and then could not clean up after itself.
        self.cleanup_abandoned = cleanup_abandoned
        self.reconcile_calls: list[tuple[str, int]] = []

    async def check(self):
        return object()

    async def setup(self, bout_id, fencing_token, on_progress):
        assert bout_id and fencing_token > 0
        await on_progress(SimpleNamespace(lane_id="lakebase", phase="setup"))
        arm = arm_setup_phase(
            ("lakebase", "competitor"),
            t0_ns=1_000_000_000,
        )

        def observation(lane_id: str, launched_ns: int, stopped_ns: int):
            facts = (
                PublicSetupEvidence("transaction_verified", True),
                PublicSetupEvidence("tls_verified", True),
            )
            return SetupLaneObservation(
                lane_id=lane_id,
                workflow_launched_ns=launched_ns,
                status=SetupLaneStatus.SUCCEEDED,
                stop_gate_evidence=SetupStopGateEvidence(
                    gate_id="application_transaction",
                    expected=facts,
                    observed=facts,
                    verified_at_ns=stopped_ns,
                ),
            )

        return SimpleNamespace(
            bout_id=bout_id,
            arm=arm,
            observations=(
                observation("lakebase", 1_001_000_000, 2_000_000_000),
                observation("competitor", 1_002_000_000, 2_250_000_000),
            ),
            credential_sha256="must-not-reach-browser",
            secret_arn="arn:must-not-reach-browser",
            fencing_token=fencing_token,
        )

    async def run(self, _arm, on_progress):
        await on_progress(SimpleNamespace(phase="burst"))
        if self.cleanup_failure:
            raise RuntimeError("cleanup failed")
        gates = ConnectionSpikeGates(
            cleanup=self.burst_valid,
            fairness=True,
            contracts=True,
            failures=() if self.burst_valid else ("cleanup",),
        )

        def lane(lane_id: str, p99: float):
            latencies = tuple([1.0] * 127 + [p99])
            return ConnectionSpikeLaneResult(
                lane_id=lane_id,
                scheduled_clients=128,
                terminal_clients=128,
                successful_clients=128,
                error_clients=0,
                successful_latency_ms=latencies,
                application_p99_ms=1.0,
                witness_verified_clients=64,
                unique_backend_pids=8,
                peak_backend_sessions=12,
                launch_skew_ms=2.0,
                gates=gates,
            )

        return ConnectionSpikeRunResult(
            contract_sha256="burst-contract",
            lanes={
                "lakebase": lane("lakebase", 7.0),
                "competitor": lane("competitor", 9.0),
            },
            comparison=None,
        )

    async def cancel_and_cleanup(self, _arm) -> None:
        return None

    async def cancel_setup_and_settle(self, _bout_id) -> None:
        if self.cleanup_failure or self.cleanup_abandoned:
            raise RuntimeError("cleanup remains incomplete")

    async def reconcile_failed_cleanup(self, bout_id, current_fencing_token) -> None:
        self.reconcile_calls.append((bout_id, current_fencing_token))
        if self.cleanup_abandoned:
            raise RuntimeError("backstage cleanup will not converge")
        self.cleanup_failure = False


class BackgroundCleanupConnectionSpikeEngine(SuccessfulTwoPhaseConnectionSpikeEngine):
    def __init__(self) -> None:
        super().__init__(burst_valid=True)
        self.cleanup_started = asyncio.Event()
        self.delete_accepted = asyncio.Event()
        self.cleanup_complete = asyncio.Event()

    async def stop_and_begin_cleanup(self, _arm) -> None:
        self.cleanup_started.set()

    async def wait_for_proxy_delete_accepted(self) -> None:
        await self.delete_accepted.wait()

    async def wait_for_cleanup_complete(self) -> None:
        await self.cleanup_complete.wait()

    def proxy_delete_accepted(self) -> bool:
        return self.delete_accepted.is_set()


class BlockingTowelConnectionSpikeEngine(BackgroundCleanupConnectionSpikeEngine):
    def __init__(self) -> None:
        super().__init__()
        self.run_entered = asyncio.Event()

    async def run(self, _arm, _on_progress):
        self.run_entered.set()
        await self.cleanup_started.wait()


class UnconvergingTowelConnectionSpikeEngine(BlockingTowelConnectionSpikeEngine):
    """A Round 5 engine whose backstage cleanup refuses to settle, then relents."""

    def __init__(self) -> None:
        super().__init__()
        self.unconverging = True
        self.reconcile_attempts = 0

    async def wait_for_proxy_delete_accepted(self) -> None:
        if self.unconverging:
            raise RuntimeError("the proxy delete was never accepted")
        await self.delete_accepted.wait()

    def proxy_delete_accepted(self) -> bool:
        return not self.unconverging and self.delete_accepted.is_set()

    async def reconcile_failed_cleanup(self, bout_id, current_fencing_token) -> None:
        self.reconcile_attempts += 1
        if self.unconverging:
            raise RuntimeError("backstage cleanup will not converge")
        await super().reconcile_failed_cleanup(bout_id, current_fencing_token)

    def relent(self) -> None:
        self.unconverging = False
        self.delete_accepted.set()
        self.cleanup_complete.set()


class FakeRdsResolver:
    def __init__(self, lakebase_fatal: bool = False) -> None:
        self.lakebase = FakeLiveTarget("lakebase", "Lakebase", 0.005, fatal=lakebase_fatal)
        self.competitor = FakeLiveTarget("competitor", "RDS PostgreSQL", 0, eligible=False)

    def resolve(self, competitor: CompetitorId):
        assert competitor == CompetitorId.RDS_POSTGRES
        return self.lakebase, self.competitor


@dataclass
class FailingArmTarget:
    id: str
    name: str
    error: Exception
    arm_calls: int = 0

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.arm_calls += 1
        raise self.error

    async def prepare(self):
        raise AssertionError("A failed arm must never prepare credentials")


class UnexpectedArmResolver:
    def __init__(self) -> None:
        self.broken = FailingArmTarget("lakebase", "Lakebase", RuntimeError("boom"))
        self.waiting = FailingArmTarget(
            "competitor",
            "Aurora Serverless v2",
            TargetNotArmedError("not zero"),
        )

    def resolve(self, competitor: CompetitorId):
        return self.broken, self.waiting


class HangingArmTarget(FakeLiveTarget):
    def __init__(self, id: str, name: str) -> None:
        super().__init__(id, name, 0)
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class HangingArmResolver:
    def __init__(self) -> None:
        self.lakebase = HangingArmTarget("lakebase", "Lakebase")
        self.competitor = HangingArmTarget("competitor", "Aurora Serverless v2")

    def resolve(self, competitor: CompetitorId):
        return self.lakebase, self.competitor


class ChangingStartTarget(FakeLiveTarget):
    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.arm_calls += 1
        if self.arm_calls > 1:
            raise TargetNotArmedError("Lakebase woke before the bell")
        return {"state": "ZERO"}


class ChangingStartResolver:
    def __init__(self) -> None:
        self.lakebase = ChangingStartTarget("lakebase", "Lakebase", 0.005)
        self.competitor = FakeLiveTarget("competitor", "Aurora Serverless v2", 0.005)

    def resolve(self, competitor: CompetitorId):
        return self.lakebase, self.competitor


class SequencedCooldownTarget(FakeLiveTarget):
    def __init__(self, id: str, name: str, cooldown_states: list[bool]) -> None:
        super().__init__(id, name, 0.001)
        self.cooldown_states = list(cooldown_states)
        self.cooldown_cutoffs: list[datetime | None] = []

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.arm_calls += 1
        if self.arm_calls <= 2:
            return {"state": "ZERO"}
        self.cooldown_cutoffs.append(not_before)
        state = self.cooldown_states.pop(0) if self.cooldown_states else True
        if not state:
            raise TargetNotArmedError("not zero after the re-do bell")
        return {"state": "ZERO"}


class SequencedCooldownResolver:
    def __init__(self) -> None:
        self.lakebase = SequencedCooldownTarget(
            "lakebase",
            "Lakebase",
            [True, False, True],
        )
        self.competitor = SequencedCooldownTarget(
            "competitor",
            "Aurora Serverless v2",
            [False, False, True],
        )

    def resolve(self, competitor: CompetitorId):
        return self.lakebase, self.competitor


class EvidenceTimedCooldownTarget(FakeLiveTarget):
    def __init__(
        self,
        id: str,
        name: str,
        *,
        check_delay: float,
        evidence_offset_ms: float,
        transient_once: bool = False,
    ) -> None:
        super().__init__(id, name, 0.001)
        self.check_delay = check_delay
        self.evidence_offset_ms = evidence_offset_ms
        self.transient_once = transient_once

    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.arm_calls += 1
        if self.arm_calls <= 2:
            return {"state": "ZERO"}
        if self.transient_once:
            self.transient_once = False
            raise RuntimeError("temporary control-plane failure")
        assert not_before is not None
        await asyncio.sleep(self.check_delay)
        return {
            "state": "ZERO",
            "observed_at": (
                not_before + timedelta(milliseconds=self.evidence_offset_ms)
            ).isoformat(),
        }


class EvidenceTimedCooldownResolver:
    def __init__(self, *, transient_once: bool = False) -> None:
        self.lakebase = EvidenceTimedCooldownTarget(
            "lakebase",
            "Lakebase",
            check_delay=0.01,
            evidence_offset_ms=2,
            transient_once=transient_once,
        )
        self.competitor = EvidenceTimedCooldownTarget(
            "competitor",
            "Aurora Serverless v2",
            check_delay=0.05,
            evidence_offset_ms=10,
        )

    def resolve(self, competitor: CompetitorId):
        return self.lakebase, self.competitor


class HangingCooldownTarget(FakeLiveTarget):
    async def assert_armed(self, *, not_before=None) -> dict[str, object]:
        self.arm_calls += 1
        if self.arm_calls <= 2:
            return {"state": "ZERO"}
        await asyncio.sleep(60)
        return {"state": "ZERO"}


class HangingCooldownResolver:
    def __init__(self) -> None:
        self.lakebase = HangingCooldownTarget("lakebase", "Lakebase", 0.001)
        self.competitor = EvidenceTimedCooldownTarget(
            "competitor",
            "Aurora Serverless v2",
            check_delay=0.001,
            evidence_offset_ms=0.5,
        )

    def resolve(self, competitor: CompetitorId):
        return self.lakebase, self.competitor


async def round_one_cooldown_record(
    *,
    verified_activity: bool = True,
) -> tuple[RunManager, SessionRecord]:
    manager = RunManager()
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )
    record = manager._records[created.id]
    lane = record.snapshot.lanes["lakebase"]
    lane.state = LaneState.VERIFIED if verified_activity else LaneState.SEALED
    lane.connection_closed_at = (
        datetime.now(UTC) - timedelta(seconds=5)
        if verified_activity
        else None
    )
    record.snapshot.cooldown = manager._new_cooldown(
        record,
        ResetMode.RETURN_TO_IDLE,
    )
    return manager, record


class BlockingReleaseStore(InMemoryBoutLeaseStore):
    def __init__(self) -> None:
        super().__init__()
        self.release_started = asyncio.Event()
        self.allow_release = asyncio.Event()
        self.release_completed = asyncio.Event()

    async def release(self, lease):
        self.release_started.set()
        await self.allow_release.wait()
        released = await super().release(lease)
        self.release_completed.set()
        return released


class BlockingRedoReleaseStore(InMemoryBoutLeaseStore):
    def __init__(self) -> None:
        super().__init__()
        self.release_started = asyncio.Event()
        self.allow_release = asyncio.Event()

    async def release(self, lease):
        if lease.phase == "redo_committed":
            self.release_started.set()
            await self.allow_release.wait()
        return await super().release(lease)


class RejectRedoClaimStore(InMemoryBoutLeaseStore):
    async def claim(self, **kwargs):
        if kwargs["phase"] == "redo_committed":
            raise LeaseLostError("redo lease unavailable")
        return await super().claim(**kwargs)


class RetryTerminalReleaseStore(InMemoryBoutLeaseStore):
    def __init__(self, *, phase: str, first_outcome: str = "false") -> None:
        super().__init__()
        self.phase = phase
        self.first_outcome = first_outcome
        self.release_calls = 0
        self.first_attempt = asyncio.Event()
        self.retry_started = asyncio.Event()
        self.allow_success = asyncio.Event()

    async def release(self, lease):
        if lease.phase != self.phase:
            return await super().release(lease)
        self.release_calls += 1
        if self.release_calls == 1:
            self.first_attempt.set()
            if self.first_outcome == "exception":
                raise RuntimeError("coordinator temporarily unavailable")
            return False
        self.retry_started.set()
        await self.allow_success.wait()
        return await super().release(lease)


class UnreachableTerminalReleaseStore(InMemoryBoutLeaseStore):
    """A coordinator that answers the claim and then stops answering at all.

    Round 4's terminal settlement swallows every store exception on purpose, so
    a stand-in that merely returned ``False`` would still let the loop reach a
    decision through ``current()``.  Only a store that refuses both calls
    reproduces the unreachable endpoint that keeps it retrying, which is the
    condition the caller's bound has to survive.
    """

    def __init__(self, *, phase: str | None = None) -> None:
        super().__init__()
        self.phase = phase
        self.release_attempts = 0
        self.current_attempts = 0
        self.attempted = asyncio.Event()

    async def release(self, lease):
        if self.phase is not None and lease.phase != self.phase:
            return await super().release(lease)
        self.release_attempts += 1
        self.attempted.set()
        raise RuntimeError("the coordination endpoint is unreachable")

    async def current(self):
        # Reachable until the settlement starts, so arming and the run itself
        # behave exactly as they do against a healthy coordinator.
        if not self.attempted.is_set():
            return await super().current()
        self.current_attempts += 1
        raise RuntimeError("the coordination endpoint is unreachable")


class LoseTerminalLeaseStore(InMemoryBoutLeaseStore):
    def __init__(self, *, phase: str, current_state: str) -> None:
        super().__init__()
        self.phase = phase
        self.current_state = current_state
        self.loss_triggered = asyncio.Event()

    async def release(self, lease):
        if lease.phase != self.phase:
            return await super().release(lease)
        if self.current_state == "different":
            self._lease = replace(lease, fencing_token=lease.fencing_token + 1)
        else:
            self._lease = None
        self.loss_triggered.set()
        return False


class SlowRenewableTerminalStore(InMemoryBoutLeaseStore):
    def __init__(self, *, phase: str, delay: float = 0.08) -> None:
        super().__init__()
        self.phase = phase
        self.delay = delay
        self.release_calls = 0
        self.release_started = asyncio.Event()
        self.current_started = asyncio.Event()

    async def release(self, lease):
        if lease.phase != self.phase:
            return await super().release(lease)
        self.release_calls += 1
        self.release_started.set()
        await asyncio.sleep(self.delay)
        if self.release_calls == 1:
            return False
        return await super().release(lease)

    async def current(self):
        if self.release_calls == 1:
            self.current_started.set()
            await asyncio.sleep(self.delay)
        return await super().current()


class ClearBeforeReleaseReturnsStore(InMemoryBoutLeaseStore):
    def __init__(self, *, phase: str, delay: float = 0.08) -> None:
        super().__init__()
        self.phase = phase
        self.delay = delay
        self.row_cleared = asyncio.Event()

    async def release(self, lease):
        if lease.phase != self.phase:
            return await super().release(lease)
        async with self._lock:
            active = self._active(self._clock())
            if active is None or active.fencing_token != lease.fencing_token:
                return False
            self._lease = None
        self.row_cleared.set()
        await asyncio.sleep(self.delay)
        return True


class DelayedOldFenceReleaseStore(InMemoryBoutLeaseStore):
    def __init__(self, *, phase: str) -> None:
        super().__init__()
        self.phase = phase
        self.release_fences: list[int] = []
        self.release_started = asyncio.Event()
        self.allow_response = asyncio.Event()

    async def release(self, lease):
        if lease.phase != self.phase:
            return await super().release(lease)
        self.release_fences.append(lease.fencing_token)
        self.release_started.set()
        await self.allow_response.wait()
        return False


class FailOnceReleaseStore(InMemoryBoutLeaseStore):
    def __init__(self) -> None:
        super().__init__()
        self.release_calls = 0

    async def release(self, lease):
        self.release_calls += 1
        if self.release_calls == 1:
            return False
        return await super().release(lease)


class RefusingTowelTransitionStore(InMemoryBoutLeaseStore):
    """A ring whose fence has moved under the towel, until it has not."""

    def __init__(self) -> None:
        super().__init__()
        self.refuse = True

    async def transition(self, lease, **kwargs):
        if self.refuse and kwargs["phase"] == "towel_cleanup":
            raise LeaseLostError("the ring fence moved")
        return await super().transition(lease, **kwargs)


class FailingCostLedgerStore:
    """Enough of the cost ledger to fail a bout window on demand.

    Both halves are refusable because both halves reach Postgres and both have
    hurt: `record_estimates` is the write that a missing `GRANT` refuses at the
    bell, and `close_bout` is the settlement whose refusal used to replace the
    bout's own outcome. `open_error` defaults to `None` so every existing caller
    keeps the close-only behaviour it was written against.
    """

    def __init__(self, *, failures: int = 1, open_error: BaseException | None = None) -> None:
        self.failures = failures
        self.open_error = open_error
        self.close_calls = 0
        self.open_calls = 0

    async def record_estimates(self, estimates) -> None:
        self.open_calls += 1
        if self.open_error is not None:
            raise self.open_error
        return None

    async def close_bout(
        self,
        *,
        installation_id: str,
        bout_id: str,
        window_end,
        terminal_outcome: str,
    ) -> None:
        self.close_calls += 1
        if self.close_calls <= self.failures:
            raise RuntimeError("the cost ledger endpoint is unreachable")


class BlockingTowelTransitionStore(InMemoryBoutLeaseStore):
    def __init__(self) -> None:
        super().__init__()
        self.transition_started = asyncio.Event()
        self.allow_transition = asyncio.Event()
        self.phases: list[str] = []

    async def transition(self, lease, **kwargs):
        phase = kwargs["phase"]
        self.phases.append(phase)
        if phase == "towel_cleanup":
            self.transition_started.set()
            await self.allow_transition.wait()
        return await super().transition(lease, **kwargs)


class FakeSafeChangeEngine:
    def __init__(self) -> None:
        self.fail_run = False
        self.reset_failures = 0
        self.scope = SafeChangeOwnershipScope(
            run_id="ad-test-001",
            owner="operator@databricks.com",
            aws_account_id="123456789012",
            aws_region="us-west-2",
        )
        self.plans = {
            "lakebase": SafeChangePlan(
                lane_id="lakebase",
                name="Lakebase",
                provider=SafeChangeProvider.LAKEBASE,
                source_id="projects/ad-test-001/branches/production",
                artifact_id="safe-change-ad-test-001",
                scope=self.scope,
            ),
            "competitor": SafeChangePlan(
                lane_id="competitor",
                name="Aurora Serverless v2",
                provider=SafeChangeProvider.AURORA,
                source_id="anti-demo-aurora",
                artifact_id="adsc-ad-test-001-aurora",
                scope=self.scope,
            ),
        }

    async def arm(self, competitor, on_progress):
        for plan in self.plans.values():
            await on_progress(
                SafeChangeProgress(
                    lane_id=plan.lane_id,
                    lane_name=plan.name,
                    phase=SafeChangePhase.PREFLIGHT,
                    status="Source clean",
                    occurred_at=datetime.now(UTC),
                )
            )
        return SafeChangeArm(
            competitor=competitor,
            armed_at=datetime.now(UTC),
            armed_at_monotonic_ns=1,
            contract_sha256="contract",
            scope=self.scope,
            lanes={
                lane_id: SafeChangeLaneArm(plan=plan, evidence={"ready": True})
                for lane_id, plan in self.plans.items()
            },
        )

    async def run(self, arm, on_progress):
        for plan in self.plans.values():
            await on_progress(
                SafeChangeProgress(
                    lane_id=plan.lane_id,
                    lane_name=plan.name,
                    phase=SafeChangePhase.CREATING,
                    status="Creating isolated environment",
                    occurred_at=datetime.now(UTC),
                    elapsed_ms=1.0,
                )
            )
        lanes = {
            lane_id: SafeChangeLaneResult(
                lane_id=lane_id,
                name=plan.name,
                provider=plan.provider,
                state=SafeChangeLaneState.VERIFIED,
                elapsed_ms=25.0 if lane_id == "lakebase" else 75.0,
                first_action_ns=1 if lane_id == "lakebase" else 2,
                completed_ns=26 if lane_id == "lakebase" else 76,
                artifact_id=plan.artifact_id,
            )
            for lane_id, plan in self.plans.items()
        }
        if self.fail_run:
            failed_plan = self.plans["lakebase"]
            await on_progress(
                SafeChangeProgress(
                    lane_id="lakebase",
                    lane_name=failed_plan.name,
                    phase=SafeChangePhase.FAILED,
                    status="The isolated schema change could not be verified",
                    occurred_at=datetime.now(UTC),
                    elapsed_ms=6.0,
                    error="isolated endpoint contract rejected",
                )
            )
            lanes["lakebase"] = SafeChangeLaneResult(
                lane_id="lakebase",
                name=failed_plan.name,
                provider=failed_plan.provider,
                state=SafeChangeLaneState.FAILED,
                elapsed_ms=6.0,
                first_action_ns=1,
                completed_ns=7,
                artifact_id=failed_plan.artifact_id,
                error="isolated endpoint contract rejected",
            )
        return SafeChangeRunResult(
            competitor=arm.competitor,
            nonce="11111111-1111-1111-1111-111111111111",
            started_ns=1,
            completed_ns=76,
            launch_skew_ms=0.001,
            contract_sha256="contract",
            lanes=lanes,
        )

    async def reset(self, competitor, on_progress):
        lanes = {}
        for lane_id, plan in self.plans.items():
            await on_progress(
                SafeChangeProgress(
                    lane_id=lane_id,
                    lane_name=plan.name,
                    phase=SafeChangePhase.RESET,
                    status="Owned isolated environment deleted",
                    occurred_at=datetime.now(UTC),
                )
            )
            lanes[lane_id] = SafeChangeResetLaneResult(
                lane_id=lane_id,
                name=plan.name,
                provider=plan.provider,
                artifact_id=plan.artifact_id,
                ok=True,
            )
        result = SafeChangeResetResult(competitor=competitor, lanes=lanes)
        if self.reset_failures:
            self.reset_failures -= 1
            failed = lanes["lakebase"]
            lanes["lakebase"] = SafeChangeResetLaneResult(
                lane_id=failed.lane_id,
                name=failed.name,
                provider=failed.provider,
                artifact_id=failed.artifact_id,
                ok=False,
                error="temporary cleanup failure",
            )
            raise SafeChangeResetError(SafeChangeResetResult(competitor=competitor, lanes=lanes))
        return result


class BlockingTowelSafeChangeEngine(FakeSafeChangeEngine):
    def __init__(self) -> None:
        super().__init__()
        self.run_entered = asyncio.Event()
        self.settled = asyncio.Event()

    async def run(self, arm, on_progress):
        plan = self.plans["lakebase"]
        await on_progress(
            SafeChangeProgress(
                lane_id=plan.lane_id,
                lane_name=plan.name,
                phase=SafeChangePhase.CREATING,
                status="Creating isolated environment",
                occurred_at=datetime.now(UTC),
                elapsed_ms=1.0,
            )
        )
        self.run_entered.set()
        await asyncio.Event().wait()

    async def settle_pending_mutations(self, competitor):
        self.settled.set()


class FailingRecoveryEngine:
    async def arm(self, competitor, on_progress):
        return SimpleNamespace(
            competitor=competitor,
            lanes={
                lane_id: SimpleNamespace(evidence={"exact_incident_committed": True})
                for lane_id in ("lakebase", "competitor")
            },
        )

    async def run(self, arm, on_progress, on_started, stop_control=None):
        await on_started()
        raise RuntimeError("recovery provider failed")

    async def reset(self, competitor, on_progress):
        lanes = {
            lane_id: SafeChangeResetLaneResult(
                lane_id=lane_id,
                name=name,
                provider=provider,
                artifact_id=f"recovery-{lane_id}",
                ok=True,
            )
            for lane_id, name, provider in (
                ("lakebase", "Lakebase", SafeChangeProvider.LAKEBASE),
                ("competitor", "RDS PostgreSQL", SafeChangeProvider.RDS),
            )
        }
        return SafeChangeResetResult(competitor=competitor, lanes=lanes)


class TowelRecoveryEngine:
    def __init__(
        self,
        *,
        competitor_phase: RecoveryPhase = RecoveryPhase.WAITING_RECOVERY_POINT,
        reset_failures: int = 0,
    ) -> None:
        self.competitor_phase = competitor_phase
        self.reset_failures = reset_failures
        self.allow_lakebase = asyncio.Event()
        self.allow_lakebase.set()
        self.towel_ready = asyncio.Event()
        self.reset_started = asyncio.Event()
        self.allow_reset = asyncio.Event()
        self.allow_reset.set()
        self.timeline: list[str] = []

    async def arm(self, competitor, on_progress):
        return SimpleNamespace(
            competitor=competitor,
            lanes={
                lane_id: SimpleNamespace(evidence={"exact_incident_committed": True})
                for lane_id in ("lakebase", "competitor")
            },
        )

    async def run(self, arm, on_progress, on_started, stop_control=None):
        assert stop_control is not None
        stop_control.started_ns = 10_000_000_000
        await on_started()
        await self.allow_lakebase.wait()
        lakebase = RecoveryLaneResult(
            lane_id="lakebase",
            name="Lakebase",
            provider=SafeChangeProvider.LAKEBASE,
            elapsed_ms=14_380.0,
            first_action_ns=10_001_000_000,
            completed_ns=24_380_000_000,
            artifact_id="recovery-lakebase",
            ok=True,
        )
        stop_control.first_action_ns["lakebase"] = lakebase.first_action_ns
        stop_control.first_action_ns["competitor"] = 10_002_000_000
        stop_control.completed_lanes["lakebase"] = lakebase
        await on_progress(
            RecoveryProgress(
                lane_id="lakebase",
                lane_name="Lakebase",
                phase=RecoveryPhase.VERIFIED,
                status="Exact recovered order verified; source deletion preserved",
                occurred_at=datetime.now(UTC),
                elapsed_ms=lakebase.elapsed_ms,
            )
        )
        if self.competitor_phase == RecoveryPhase.RESTORING:
            stop_control.restore_started_lanes.add("competitor")
        await on_progress(
            RecoveryProgress(
                lane_id="competitor",
                lane_name="RDS PostgreSQL",
                phase=self.competitor_phase,
                status="Opponent still recovering",
                occurred_at=datetime.now(UTC),
                elapsed_ms=89_500.0,
            )
        )
        self.towel_ready.set()
        await stop_control.event.wait()
        await on_progress(
            RecoveryProgress(
                lane_id="competitor",
                lane_name="RDS PostgreSQL",
                phase=RecoveryPhase.VERIFIED,
                status="Late verification must be ignored",
                occurred_at=datetime.now(UTC),
                elapsed_ms=90_100.0,
            )
        )
        return RecoveryStoppedResult(
            competitor=arm.competitor,
            started_ns=stop_control.started_ns,
            cutoff_ns=stop_control.cutoff_ns,
            launch_skew_ms=0.001,
            contract_sha256="contract",
            lanes={"lakebase": lakebase},
            active_lane="competitor",
            restore_started=self.competitor_phase == RecoveryPhase.RESTORING,
        )

    async def settle_pending_mutations(self, competitor):
        self.timeline.append("settle")

    async def reset(self, competitor, on_progress):
        self.timeline.append("reset")
        self.reset_started.set()
        await self.allow_reset.wait()
        lanes = {
            lane_id: SafeChangeResetLaneResult(
                lane_id=lane_id,
                name=name,
                provider=provider,
                artifact_id=f"recovery-{lane_id}",
                ok=True,
            )
            for lane_id, name, provider in (
                ("lakebase", "Lakebase", SafeChangeProvider.LAKEBASE),
                ("competitor", "RDS PostgreSQL", SafeChangeProvider.RDS),
            )
        }
        if self.reset_failures:
            self.reset_failures -= 1
            failed = lanes["competitor"]
            lanes["competitor"] = SafeChangeResetLaneResult(
                lane_id=failed.lane_id,
                name=failed.name,
                provider=failed.provider,
                artifact_id=failed.artifact_id,
                ok=False,
                error="temporary cleanup failure",
            )
            raise RecoveryResetError(SafeChangeResetResult(competitor=competitor, lanes=lanes))
        for lane_id, lane in lanes.items():
            await on_progress(
                RecoveryProgress(
                    lane_id=lane_id,
                    lane_name=lane.name,
                    phase=RecoveryPhase.RESET,
                    status="Owned recovery environment and synthetic order cleared",
                    occurred_at=datetime.now(UTC),
                )
            )
        return SafeChangeResetResult(competitor=competitor, lanes=lanes)


class DeafRecoveryEngine(TowelRecoveryEngine):
    """A Round 3 run that never looks at the stop control it was handed.

    Round 3 has no explicit ``run_task.cancel()`` on the towel path, so this is
    what a task parked inside a provider call looks like from the manager's side.
    """

    def __init__(self) -> None:
        super().__init__(competitor_phase=RecoveryPhase.RESTORING)
        self.cancelled = asyncio.Event()

    async def run(self, arm, on_progress, on_started, stop_control=None):
        assert stop_control is not None
        stop_control.started_ns = 10_000_000_000
        await on_started()
        stop_control.first_action_ns["lakebase"] = 10_001_000_000
        stop_control.first_action_ns["competitor"] = 10_002_000_000
        stop_control.restore_started_lanes.add("competitor")
        await on_progress(
            RecoveryProgress(
                lane_id="competitor",
                lane_name="RDS PostgreSQL",
                phase=RecoveryPhase.RESTORING,
                status="Restoring the opponent from a recovery point",
                occurred_at=datetime.now(UTC),
                elapsed_ms=89_500.0,
            )
        )
        self.towel_ready.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class TransitionRaceRecoveryEngine(TowelRecoveryEngine):
    def __init__(self, *, competitor_ok: bool = True) -> None:
        super().__init__(competitor_phase=RecoveryPhase.VERIFYING_SOURCE)
        self.competitor_ok = competitor_ok
        self.complete_competitor = asyncio.Event()
        self.competitor_completed = asyncio.Event()

    async def run(self, arm, on_progress, on_started, stop_control=None):
        assert stop_control is not None
        stop_control.started_ns = 10_000_000_000
        await on_started()
        lakebase = RecoveryLaneResult(
            lane_id="lakebase",
            name="Lakebase",
            provider=SafeChangeProvider.LAKEBASE,
            elapsed_ms=14_380.0,
            first_action_ns=10_001_000_000,
            completed_ns=24_380_000_000,
            artifact_id="recovery-lakebase",
            ok=True,
        )
        stop_control.first_action_ns["lakebase"] = lakebase.first_action_ns
        stop_control.first_action_ns["competitor"] = 10_002_000_000
        stop_control.completed_lanes["lakebase"] = lakebase
        stop_control.terminal_lanes.add("lakebase")
        await on_progress(
            RecoveryProgress(
                lane_id="lakebase",
                lane_name="Lakebase",
                phase=RecoveryPhase.VERIFIED,
                status="Exact recovered order verified; source deletion preserved",
                occurred_at=datetime.now(UTC),
                elapsed_ms=lakebase.elapsed_ms,
            )
        )
        await on_progress(
            RecoveryProgress(
                lane_id="competitor",
                lane_name="RDS PostgreSQL",
                phase=RecoveryPhase.VERIFYING_SOURCE,
                status="Proving the source still reflects the deletion",
                occurred_at=datetime.now(UTC),
                elapsed_ms=89_500.0,
            )
        )
        self.towel_ready.set()
        await self.complete_competitor.wait()
        competitor = RecoveryLaneResult(
            lane_id="competitor",
            name="RDS PostgreSQL",
            provider=SafeChangeProvider.RDS,
            elapsed_ms=89_600.0,
            first_action_ns=10_002_000_000,
            completed_ns=99_600_000_000,
            artifact_id="recovery-competitor",
            ok=self.competitor_ok,
            error=None if self.competitor_ok else "recovery verification failed",
        )
        if self.competitor_ok:
            stop_control.completed_lanes["competitor"] = competitor
        stop_control.terminal_lanes.add("competitor")
        self.competitor_completed.set()
        await on_progress(
            RecoveryProgress(
                lane_id="competitor",
                lane_name="RDS PostgreSQL",
                phase=(RecoveryPhase.VERIFIED if self.competitor_ok else RecoveryPhase.FAILED),
                status=(
                    "Exact recovered order verified; source deletion preserved"
                    if self.competitor_ok
                    else "The recovered order could not be verified"
                ),
                occurred_at=datetime.now(UTC),
                elapsed_ms=competitor.elapsed_ms,
                error=competitor.error,
            )
        )
        return RecoveryRunResult(
            competitor=arm.competitor,
            started_ns=stop_control.started_ns,
            completed_ns=competitor.completed_ns,
            launch_skew_ms=0.001,
            contract_sha256="contract",
            lanes={"lakebase": lakebase, "competitor": competitor},
        )


class FakeModelScoreEngine:
    def __init__(self) -> None:
        self.contract = ModelScoreContract(
            pipeline_id="round4-model-score-sync",
            source_table="main.anti_demo.model_scores",
            synced_table="public.model_scores",
        )
        self.arm_calls = 0
        self.run_calls = 0
        self.redo_calls = 0
        self.emit_progress = True
        #: What `arm` raises instead of returning, if anything. An exception
        #: rather than a boolean so a test can hand over the real refusal it is
        #: about -- a flag could only ever produce a stand-in, and a stand-in is
        #: what a test of "does the cause survive" must not accept.
        self.arm_error: BaseException | None = None
        self.fail_run = False
        self.fail_redo = False
        self.redo_entered = asyncio.Event()
        self.allow_redo = asyncio.Event()
        self.allow_redo.set()
        self.arm_result = ModelScoreArm(
            arm_id="round4-arm",
            armed_at=datetime(2026, 8, 18, 12, 0, tzinfo=UTC),
            contract_sha256=self.contract.sha256,
            source_version=10,
            baseline=self.contract.baseline,
        )
        self.initial_result: ModelScoreRunResult | None = None
        self.redo_received_result: ModelScoreRunResult | None = None

    async def _emit(self, callback, phase: ModelScorePhase, status: str, attempt=None):
        if not self.emit_progress or callback is None:
            return
        try:
            await callback(
                ModelScoreProgress(
                    phase=phase,
                    status=status,
                    occurred_at=datetime.now(UTC),
                    attempt=attempt,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def arm(self, on_progress=None):
        self.arm_calls += 1
        await self._emit(
            on_progress,
            ModelScorePhase.PREFLIGHT,
            "Inspecting Managed Sync",
        )
        if self.arm_error is not None:
            raise self.arm_error
        return self.arm_result

    @staticmethod
    def _proof(kind, update, version: int) -> ModelScoreProofResult:
        commit_time = datetime(2026, 8, 18, 12, 0, version, tzinfo=UTC)
        return ModelScoreProofResult(
            kind=kind,
            update=update,
            source_version=version,
            delta_commit_time=commit_time,
            sync_end_time=commit_time + timedelta(milliseconds=125),
            managed_availability_ms=125.0,
            application_read_elapsed_ms=250.0,
            poll_attempts=2,
            verified_row=update.row,
        )

    async def run(self, arm, update, on_progress=None):
        self.run_calls += 1
        assert arm is self.arm_result
        await self._emit(
            on_progress,
            ModelScorePhase.COMMITTING_SOURCE,
            "Committing source update",
        )
        await self._emit(
            on_progress,
            ModelScorePhase.WAITING_SYNC,
            "Waiting for Managed Sync",
            attempt=2,
        )
        if self.fail_run:
            raise RuntimeError("initial proof failed")
        result = ModelScoreRunResult(
            arm_id=arm.arm_id,
            contract_sha256=self.contract.sha256,
            initial=self._proof(ModelScoreProofKind.INITIAL, update, 11),
        )
        self.initial_result = result
        return result

    async def redo(self, arm, result, update, on_progress=None):
        self.redo_calls += 1
        self.redo_received_result = result
        assert arm is self.arm_result
        assert result is self.initial_result
        self.redo_entered.set()
        await self.allow_redo.wait()
        await self._emit(
            on_progress,
            ModelScorePhase.COMMITTING_SOURCE,
            "Committing v2 source update",
        )
        if self.fail_redo:
            raise RuntimeError("redo failed")
        return replace(
            result,
            redo=self._proof(ModelScoreProofKind.REDO, update, 12),
        )


class BlockingTowelModelScoreEngine(FakeModelScoreEngine):
    """A Round 4 engine that blocks in ``run`` and settles for real at the pipeline.

    ``settle_and_restore_baseline`` used to do nothing but set its own flag, and
    the Round 4 row of the towel test asserted only that the flag was set. That
    passed throughout the entire period in which a towelled bout left its
    Managed Sync pipeline running and billing, because a flag a fake sets cannot
    see a pipeline. It read as covering the towel's cleanup and covered nothing
    of the kind.

    So the release decision is delegated to the production one rather than
    re-stated here. ``ModelScoreEngine._release_pipeline`` is the method that
    chooses between the delayed release and the immediate one out of
    ``_issued_results``, and calling it unbound against this fake runs that exact
    branch instead of a test-local imitation of it -- which is how the original
    guard came to be vacuous in the first place.

    The activation underneath is the real one, wired to a fake pipeline API that
    goes down only when a ``/stop`` actually reaches it. That is the thing which
    would notice a stop not happening.
    """

    def __init__(self) -> None:
        super().__init__()
        self.run_entered = asyncio.Event()
        self.baseline_restored = asyncio.Event()
        # The state `_release_pipeline` decides on. Empty is what a towel looks
        # like from inside the engine: `run` was cancelled mid-flight, no result
        # was issued, and no redo was ever published for a window to protect.
        self._issued_results: dict = {}
        self.pipeline = FakeRound4PipelineApi(running=False)
        self._activation = model_score_live.Round4PipelineActivation(
            round4_activation_manifest(),
            self.pipeline,
            pipeline_id="pipeline-1",
            poll_seconds=0,
        )

    async def arm(self, on_progress=None):
        # The real preflight, so the pipeline is up for the same reason it is up
        # in production: this bout's own arm started it.
        await self._activation.ensure_running(lambda _status: asyncio.sleep(0))
        return await super().arm(on_progress)

    async def run(self, arm, update, on_progress=None):
        await self._emit(
            on_progress,
            ModelScorePhase.COMMITTING_SOURCE,
            "Committing source update",
        )
        self.run_entered.set()
        await asyncio.Event().wait()

    async def settle_and_restore_baseline(self):
        self.baseline_restored.set()
        await ModelScoreEngine._release_pipeline(self)


class FakeRound4PipelineApi:
    """The Round 4 control plane, in the shapes the account actually returns.

    Deliberately a wire-level fake rather than a stub activation: ``running``
    changes only when a ``/stop`` or ``/updates`` POST reaches it, so a test that
    asserts against it is asserting that the effect arrived, not that a call was
    made. Mirrors ``FakePipelineApi`` in ``tests/test_model_score_live.py``,
    where the measured shapes are documented.
    """

    def __init__(self, running: bool = True) -> None:
        self.running = running
        self.calls: list[tuple[str, str]] = []

    def __call__(self, profile, method, path, *, body=None, timeout=600):
        self.calls.append((method, path))
        if method == "post" and path.endswith("/stop"):
            self.running = False
            return {}
        if method == "post" and path.endswith("/updates"):
            self.running = True
            return {"update_id": "update-1"}
        if "/pipelines/" in path:
            return {
                "state": "RUNNING" if self.running else "IDLE",
                "latest_updates": [
                    {"state": "RUNNING" if self.running else "CANCELED", "update_id": "u1"}
                ],
            }
        status = {
            "detailed_state": (
                "SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE"
                if self.running
                else "SYNCED_TABLE_ONLINE_PIPELINE_FAILED"
            )
        }
        if self.running:
            status["continuous_update_status"] = {"last_processed_commit_version": 5}
        return {"data_synchronization_status": status}

    @property
    def stop_verbs(self) -> list[str]:
        return [path for method, path in self.calls if method == "post" and path.endswith("/stop")]


def round4_activation_manifest() -> SimpleNamespace:
    """A manifest naming no live resource. Identifiers here are placeholders."""

    return SimpleNamespace(
        run_id="run-1",
        round4=SimpleNamespace(
            pipeline_id="pipeline-1",
            synced_table_id="storage.round4.model_scores",
        ),
        databricks=SimpleNamespace(profile="demo", user="setup-user"),
    )


class BlockingTowelLiveOrdersEngine:
    def __init__(self) -> None:
        self.run_entered = asyncio.Event()
        self.cleaned = asyncio.Event()
        self.orders = None

    async def arm(self, on_progress=None):
        return SimpleNamespace(committed_lsn=1)

    async def run(self, arm, order, checkout_guardrail_order, on_progress=None):
        self.orders = (order, checkout_guardrail_order)
        self.run_entered.set()
        await asyncio.Event().wait()

    async def settle_and_cleanup_owned(self, order, checkout_guardrail_order):
        assert self.orders == (order, checkout_guardrail_order)
        self.cleaned.set()


def make_verifier() -> NeutralVerifier:
    return NeutralVerifier(
        RetryPolicy(
            overall_timeout_seconds=1,
            attempt_timeout_seconds=0.5,
            initial_delay_seconds=0.001,
            maximum_delay_seconds=0.001,
        )
    )


async def wait_for_state(manager: RunManager, session_id: str, state: SessionState):
    for _ in range(100):
        snapshot = await manager.get(session_id)
        if snapshot.state == state:
            return snapshot
        await asyncio.sleep(0.005)
    raise AssertionError(f"Session never reached {state}")


async def drain_record_operations(
    manager: RunManager,
    record,
    patience: float = 5.0,
) -> None:
    """Wait until the record owns no unfinished background task.

    A terminal snapshot is not the same thing as a quiet record. The manager
    deliberately keeps work running past the verdict -- the cooldown watcher,
    the settlement task, the lease heartbeats -- and `_unfinished_operations`
    is the manager's own list of exactly that set, used here rather than a
    copy of it so this cannot drift as tasks are added.

    Waited on rather than cancelled, and waited on with a bound rather than
    forever: a test that cancels is no longer observing what the manager does,
    and a test that waits forever turns a hung task into a hung suite. The
    bound expiring is a real failure and is reported as one, naming the tasks
    still outstanding.
    """
    deadline = time.monotonic() + patience
    while True:
        pending = manager._unfinished_operations(record)
        if not pending:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(
                f"record still owns {[task.get_name() for task in pending]} "
                f"after {patience}s"
            )
        await asyncio.wait(pending, timeout=remaining)


async def armed_round_one(
    manager: RunManager,
    session_id_holder: list[str] | None = None,
) -> str:
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    if session_id_holder is not None:
        session_id_holder.append(created.id)
    return created.id


async def test_double_rung_bell_opens_exactly_one_run() -> None:
    """A double-rung bell must not open a second run while the first is in flight.

    Covers the in-flight idempotency branch in RunManager.start_run: the second
    caller sees a live run-<session> task and gets the same snapshot back.
    """
    lakebase = FakeLiveTarget("lakebase", "Lakebase", 60)
    competitor = FakeLiveTarget("competitor", "Aurora Serverless v2", 60)
    resolver = SimpleNamespace(resolve=lambda _competitor: (lakebase, competitor))
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    session_id = await armed_round_one(manager)

    first, second = await asyncio.gather(
        manager.start_run(session_id),
        manager.start_run(session_id),
    )

    # Returning at all proves the in-flight branch answered the second bell. Without
    # it the second caller falls through to "A session operation is already running".
    assert first.id == second.id == session_id
    # One bell, one barrier: both callers describe the same single run.
    assert first.state == second.state
    assert first.run_started_at == second.run_started_at

    record = manager._records[session_id]
    assert record.task is not None and not record.task.done()
    assert record.task.get_name() == f"run-{session_id}"

    running = await wait_for_state(manager, session_id, SessionState.RUNNING)
    assert running.run_started_at is not None
    events = record.event_log.events
    assert [event.event for event in events].count("run_started") == 1

    # Settle the in-flight run so the test leaves no dangling task or lease.
    await manager.start_towel(session_id)
    settled = await wait_for_towel(manager, session_id, "ready")
    assert settled.state == SessionState.TOWELLED
    assert await manager._lease_store.current() is None


async def test_bell_rung_again_after_verification_replays_the_same_result() -> None:
    """Ringing again after a run finished must replay the receipt, not re-run.

    Covers the terminal idempotency branch in RunManager.start_run: run_started_at
    is set and the state is terminal, so the stored snapshot is returned unchanged.
    """
    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    session_id = await armed_round_one(manager)

    await manager.start_run(session_id)
    verified = await wait_for_state(manager, session_id, SessionState.VERIFIED)

    events = manager._records[session_id].event_log.events
    event_count = len(events)
    record = manager._records[session_id]
    assert record.task is not None and record.task.done()

    replay = await manager.start_run(session_id)

    assert replay.state == SessionState.VERIFIED
    assert replay.run_started_at == verified.run_started_at
    assert replay.remembered_result == verified.remembered_result
    # No second run, no second barrier, no extra events.
    assert len(events) == event_count
    assert [event.event for event in events].count("run_started") == 1


def test_round_five_diagnostic_keeps_aws_code_but_redacts_provider_message() -> None:
    denied = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "denied secret:should-not-be-logged",
            }
        },
        "DescribeSecret",
    )
    try:
        raise RuntimeError("provider_create_failed") from denied
    except RuntimeError as error:
        diagnostic = _redacted_exception_chain(error)
    assert diagnostic == ("RuntimeError <- ClientError[AccessDeniedException]@DescribeSecret")
    assert "should-not-be-logged" not in diagnostic


def test_the_operator_diagnosis_keeps_databricks_words_and_still_drops_aws_ones() -> None:
    """Both directions of the one boundary, because either alone is a defect.

    Drop everything and you get the 2026-08-23 incident: two `PermissionDenied`
    refusals naming an exact table, an exact principal and an exact permission,
    reduced to "could not be verified" on the only surface an operator could
    reach without a WebSocket. Keep everything and a botocore message carries a
    secret ARN onto a screen an audience is looking at.

    So the boundary is who wrote the sentence, and it is asserted here in one
    test rather than two because a test that only pins the keeping half is what
    would let the dropping half quietly widen.

    The chain is deliberately three deep with the least informative link
    outermost -- an adapter wrapping its own call in a `RuntimeError` is how
    Round 5's real cause once ended up three frames down and unreported. A
    diagnosis that reported only the exception it caught would pass a test built
    on a bare refusal and still lose the refusal in production.
    """

    verbatim = (
        "User does not have SELECT on Table "
        "'example_catalog.anti_demo_online_ad_20260101_0000_0000.model_scores'."
    )
    aws_denied = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "secret:should-not-be-logged"}},
        "DescribeSecret",
    )
    try:
        try:
            try:
                raise aws_denied
            except ClientError as inner:
                raise PermissionDenied(verbatim) from inner
        except PermissionDenied as middle:
            raise RuntimeError("arm_precondition_failed") from middle
    except RuntimeError as error:
        diagnosis = operator_diagnosis(error)
        refusal = authorization_refusal(error)
        message = arm_failure_message("The Managed Sync baseline could not be verified.", error)

    # Databricks' own words survive: they name the table and the permission,
    # which is the entire remedy an operator needs.
    assert verbatim in diagnosis
    # AWS's do not, and the type and error code that were already safe still do.
    assert "should-not-be-logged" not in diagnosis
    assert "ClientError[AccessDeniedException]@DescribeSecret" in diagnosis
    # The uninformative outermost link is reported, not substituted for the rest.
    assert diagnosis.startswith("RuntimeError <- PermissionDenied: ")
    # A builtin's message is nobody's to quote, so the type stands alone.
    assert "arm_precondition_failed" not in diagnosis

    # And the refusal is found through the wrapper rather than only at the head.
    assert refusal is not None and str(refusal) == verbatim
    assert message.startswith("The Managed Sync baseline could not be verified. ")
    assert GRANT_REFUSAL_HEADLINE in message
    assert verbatim in message
    # Readable, not a traceback: one line, no frames, and bounded.
    assert "\n" not in message
    assert "Traceback" not in message
    assert len(diagnosis) <= 480


def test_a_diagnosis_that_runs_long_is_cut_rather_than_left_to_run_over() -> None:
    """The cap is a cap, and it may not eat the sentence that explains it.

    A provider is free to answer with a page of prose. This text reaches a
    screen an audience may be looking at, so the length has to be bounded --
    but bounding it by dropping the message is the defect this whole change is
    about, so it is bounded by truncation and the operator is shown that it was
    truncated.
    """

    error = PermissionDenied("x" * 5_000)
    diagnosis = operator_diagnosis(error)

    assert len(diagnosis) == 480
    assert diagnosis.endswith("…")
    assert diagnosis.startswith("PermissionDenied: xxx")
    # The bounded diagnosis is the tail of the message; the explanation is intact.
    message = arm_failure_message("The native CDF start state could not be verified.", error)
    assert message.startswith("The native CDF start state could not be verified. ")
    assert GRANT_REFUSAL_HEADLINE in message


async def test_round_five_lease_loss_verifies_cleanup_and_forbids_comparison() -> None:
    engine = BlockingConnectionSpikeEngine()
    selected_opponents: list[CompetitorId] = []

    def factory(competitor: CompetitorId):
        selected_opponents.append(competitor)
        return engine

    manager = RunManager(connection_spike_factory=factory)
    operator = BoutOperator(display_name="Round Five Owner", subject="round-five-owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    assert selected_opponents == [CompetitorId.RDS_POSTGRES]
    assert (await manager.get(created.id)).lanes["competitor"].name == (
        "RDS PostgreSQL + RDS Proxy"
    )
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)
    record = manager._records[created.id]
    assert record.lease is not None

    await manager._handle_lost_lease(record, record.lease)

    await asyncio.wait_for(engine.cleanup_finished.wait(), timeout=1)
    assert engine.run_cancelled_and_settled.is_set()
    failed = await manager.get(created.id)
    assert failed.state == SessionState.FAILED
    assert failed.comparison is None
    assert failed.metrics == []
    assert failed.failure is not None and "no comparison" in failed.failure.lower()
    assert failed.round5_setup is not None
    assert failed.round5_setup.downstream_validated is False
    with pytest.raises(InvalidStateError, match="clean baseline and a new bout"):
        await manager.start_arm(created.id, operator)


async def test_round_five_verdict_releases_main_before_full_backstage_cleanup() -> None:
    engine = BackgroundCleanupConnectionSpikeEngine()
    main_store = InMemoryBoutLeaseStore()
    round5_store = InMemoryBoutLeaseStore(ring_key="round5")
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        live_orders_factory=lambda: object(),
        lease_store=main_store,
        round5_lease_store=round5_store,
    )
    operator = BoutOperator(display_name="Round Five Owner", subject="round-five-owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )

    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    assert verified.comparison is not None
    terminal_main_lease = await main_store.current()
    assert terminal_main_lease is not None
    assert terminal_main_lease.phase == "cooldown"
    board_sample = await manager.bout_status(RoundId.SURVIVE_CONNECTION_SPIKE)
    assert board_sample.active is True
    assert board_sample.phase == "cooldown"
    await asyncio.wait_for(engine.cleanup_started.wait(), timeout=1)
    assert await main_store.current() is not None
    cleanup_lease = await round5_store.current()
    assert cleanup_lease is not None and cleanup_lease.phase == "round5_cleanup"

    # A transient backstage failure must remain model-valid without retracting
    # the already-sealed public verdict or its evidence.
    record = manager._records[created.id]
    await manager._mark_connection_spike_cleanup_pending(record)
    pending = await manager.get(created.id)
    revalidated = type(pending).model_validate(pending.model_dump(mode="json"))
    assert revalidated.state == SessionState.VERIFIED
    assert revalidated.round5_setup is not None
    assert revalidated.round5_setup.state.value == "verified"
    assert revalidated.round5_setup.cleanup_retryable is True
    assert revalidated.comparison == verified.comparison
    assert revalidated.metrics == verified.metrics

    engine.delete_accepted.set()
    for _ in range(100):
        if await main_store.current() is None:
            break
        await asyncio.sleep(0.005)
    else:
        raise AssertionError("Main ring was not released after Proxy delete acceptance")
    assert await round5_store.current() is not None

    round6 = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.ANALYZE_LIVE_ORDERS,
        )
    )
    round6_record = manager._records[round6.id]
    await manager._claim_bout(round6_record, operator)
    assert await main_store.current() is not None
    await manager._release_bout(round6_record)

    second_round5 = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    with pytest.raises(InvalidStateError, match="BACKSTAGE CLEANUP"):
        await manager.start_arm(second_round5.id, operator)
    assert await main_store.current() is None

    engine.cleanup_complete.set()
    cleanup_task = manager._records[created.id].connection_spike_cleanup_task
    assert cleanup_task is not None
    await asyncio.wait_for(cleanup_task, timeout=1)
    assert await round5_store.current() is None
    final = await manager.get(created.id)
    assert final.state == SessionState.VERIFIED
    assert final.comparison is not None


async def test_round_five_prearm_guard_refuses_before_engine_or_setup_mutation() -> None:
    main_store = InMemoryBoutLeaseStore()
    round5_store = InMemoryBoutLeaseStore(ring_key="round5")
    factory_calls = 0

    async def guard(session_id: str, fencing_token: int) -> None:
        authority = await round5_store.current()
        assert authority is not None
        assert authority.session_id == session_id
        assert authority.fencing_token == fencing_token
        raise RuntimeError("unresolved journal scope")

    def factory(_competitor: CompetitorId):
        nonlocal factory_calls
        factory_calls += 1
        return BackgroundCleanupConnectionSpikeEngine()

    manager = RunManager(
        connection_spike_factory=factory,
        lease_store=main_store,
        round5_lease_store=round5_store,
        round5_prearm_guard=guard,
    )
    operator = BoutOperator(display_name="Round Five Owner", subject="round-five-owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )

    with pytest.raises(InvalidStateError, match="BACKSTAGE CLEANUP"):
        await manager.start_arm(created.id, operator)
    unchanged = await manager.get(created.id)
    assert unchanged.state == SessionState.DRAFT
    assert unchanged.comparison is None
    assert factory_calls == 0
    assert await main_store.current() is None
    assert await round5_store.current() is None


async def test_v7_round_five_prearm_uses_injected_scoped_cleanup_store() -> None:
    installation_id = "install-a"
    base = InMemoryBoutLeaseStore()
    scoped = InMemoryBoutLeaseStore(
        ring_key=round_ring_key(
            installation_id,
            RoundId.SURVIVE_CONNECTION_SPIKE.value,
            cleanup=True,
        )
    )
    guard_calls: list[tuple[str, int]] = []

    async def guard(session_id: str, fencing_token: int) -> None:
        authority = await scoped.current()
        assert authority is not None
        assert authority.session_id == session_id
        assert authority.fencing_token == fencing_token
        guard_calls.append((session_id, fencing_token))

    manager = RunManager(
        connection_spike_factory=lambda _competitor: (
            SuccessfulTwoPhaseConnectionSpikeEngine(burst_valid=True)
        ),
        lease_store=base,
        round5_lease_store=scoped,
        round5_prearm_guard=guard,
        round_isolation=True,
        installation_id=installation_id,
    )
    owner = BoutOperator(display_name="Owner", subject="owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )

    checking = await manager.start_arm(created.id, owner)
    assert checking.state == SessionState.CHECKING
    await wait_for_state(manager, created.id, SessionState.ARMED)

    authority = await scoped.current()
    assert authority is not None
    assert authority.phase == "armed"
    assert guard_calls == [(created.id, authority.fencing_token)]

    await manager.close()
    assert await scoped.current() is None


async def test_manager_shutdown_settles_round_five_before_releasing_ring() -> None:
    engine = BlockingConnectionSpikeEngine()
    manager = RunManager(connection_spike_factory=lambda _competitor: engine)
    manager._shutdown_settle_timeout = 1
    operator = BoutOperator(display_name="Round Five Owner", subject="round-five-owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )

    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)

    await manager.close()

    assert engine.run_cancelled_and_settled.is_set()
    assert engine.cleanup_finished.is_set()
    assert await manager._lease_store.current() is None
    with pytest.raises(InvalidStateError, match="restarting"):
        await manager.create(
            SessionCreate(
                competitor=CompetitorId.RDS_POSTGRES,
                primary_persona="sre",
                corners=[Corner.PERFORMANCE],
            )
        )


async def test_round_five_publishes_exact_lakebase_stop_while_competitor_continues() -> None:
    stale_progress_published = asyncio.Event()
    allow_lakebase_stop = asyncio.Event()
    lakebase_stopped = asyncio.Event()

    class HoldingSetupEngine:
        has_timed_setup = True

        async def setup(self, bout_id, fencing_token, on_progress):
            assert bout_id and fencing_token > 0
            await on_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="verifying_transaction",
                    status="running",
                    setup_elapsed_ms=250.0,
                    t0_ns=9_876_543_210_987_654_321,
                    workflow_id="workflow-private-123",
                    credential_sha256="credential-private-123",
                )
            )
            # A stale observation must not move the public clock backwards.
            await on_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="verifying_transaction",
                    status="running",
                    setup_elapsed_ms=125.0,
                )
            )
            stale_progress_published.set()
            await allow_lakebase_stop.wait()
            await on_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="setup_stop",
                    status="verified",
                    setup_elapsed_ms=375.25,
                    secret_arn=(
                        "arn:aws:secretsmanager:us-west-2:123456789012:"
                        "secret:private-round-five"
                    ),
                    password="private-password",
                )
            )
            await on_progress(
                SimpleNamespace(
                    lane_id="competitor",
                    phase="creating_proxy",
                    status="running",
                    setup_elapsed_ms=500.0,
                )
            )
            # Once a lane reaches its exact stop, later progress cannot reopen
            # its clock or replace the stopped elapsed value.
            await on_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="verifying_transaction",
                    status="running",
                    setup_elapsed_ms=900.0,
                )
            )
            lakebase_stopped.set()
            await asyncio.Event().wait()

        async def cancel_setup_and_settle(self, _bout_id):
            return None

    # A driveable measurement clock. The bout's own progress decides when it
    # advances, so the gap below is a real silent interval and not an artefact
    # of counting clock reads.
    clock = {"ns": 10_000_000_000}

    def clock_ns() -> int:
        return clock["ns"]

    # The observed shape: the RDS lane reports `waiting_for_proxy_target` and
    # then says nothing for nearly four minutes while the proxy provisions.
    silent_gap_ns = 222_770_000_000

    manager = RunManager(
        connection_spike_factory=lambda _competitor: HoldingSetupEngine(),
        clock_ns=clock_ns,
    )
    operator = BoutOperator(display_name="Round Five Owner", subject="round-five-owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )

    try:
        await manager.start_arm(created.id, operator)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id, operator)
        await asyncio.wait_for(stale_progress_published.wait(), timeout=1)

        progressing = await manager.get(created.id)
        assert progressing.round5_setup is not None
        assert progressing.round5_setup.lanes["lakebase"].state.value == "running"
        assert progressing.round5_setup.lanes["lakebase"].setup_elapsed_ms == 250.0

        allow_lakebase_stop.set()
        await asyncio.wait_for(lakebase_stopped.wait(), timeout=1)

        running = await manager.get(created.id)
        assert running.state == SessionState.RUNNING
        assert running.round5_setup is not None
        assert running.round5_setup.state.value == "running"
        assert running.round5_setup.lanes["lakebase"].state.value == "verified"
        assert running.round5_setup.lanes["lakebase"].setup_elapsed_ms == 375.25
        assert running.round5_setup.lanes["lakebase"].status == (
            "Built-in Lakebase pool verified"
        )
        assert running.round5_setup.lanes["competitor"].state.value == "running"
        assert running.round5_setup.lanes["competitor"].setup_elapsed_ms == 500.0
        assert running.round5_setup.lanes["competitor"].status == (
            "AWS is creating a new RDS Proxy"
        )

        public_events = str(
            [
                event.model_dump(mode="json")
                for event in manager._records[created.id].event_log.events
            ]
        ).lower()
        for forbidden in (
            "t0_ns",
            "9876543210987654321",
            "workflow-private-123",
            "credential-private-123",
            "arn:aws:secretsmanager",
            "private-password",
        ):
            assert forbidden not in public_events

        # The silent phase. No progress callback fires across it, so anything
        # sourced from the last published progress value is now stale by the
        # whole gap.
        clock["ns"] += silent_gap_ns
        silent_floor_ms = 500.0 + silent_gap_ns / 1_000_000

        # What a refreshed browser reads. The progress latch deliberately stays
        # at the last callback, while the public snapshot floor advances from
        # that lane-owned observation anchor. A UI that restarts from the latch
        # would visibly rewind from 223.27s to 0.50s here.
        refreshed = await manager.get(created.id)
        assert refreshed.round5_setup is not None
        refreshed_lakebase = refreshed.round5_setup.lanes["lakebase"]
        refreshed_competitor = refreshed.round5_setup.lanes["competitor"]
        assert refreshed_lakebase.setup_elapsed_ms == 375.25
        assert refreshed_lakebase.elapsed_at_snapshot_ms == 375.25
        assert refreshed_competitor.setup_elapsed_ms == 500.0
        assert refreshed_competitor.elapsed_at_snapshot_ms == silent_floor_ms

        toweled = await manager.start_towel(created.id, operator)
        assert toweled.state == SessionState.TOWELLED
        assert toweled.towel is not None
        assert toweled.towel.cutoff_ms is None
        # The censored bound is the lane's elapsed time when the towel landed,
        # measured forward from its own last published progress, not the frozen
        # progress value itself.
        assert toweled.towel.censored_lower_bounds_ms == {"competitor": silent_floor_ms}
        assert toweled.lanes["lakebase"].state == LaneState.VERIFIED
        assert toweled.lanes["lakebase"].elapsed_ms == 375.25
        assert toweled.lanes["competitor"].state == LaneState.TOWELLED
        assert toweled.lanes["competitor"].elapsed_ms is None
        assert toweled.lanes["competitor"].evidence["lower_bound_ms"] == silent_floor_ms
        assert toweled.lanes["competitor"].evidence["display_value"] == ">223.27s"
        assert toweled.comparison is not None
        assert toweled.comparison.kind == ComparisonKind.NOT_COMPARABLE
        assert toweled.comparison.winner_lane_id is None
        assert toweled.comparison.margin is None
        assert toweled.round5_setup is not None
        assert toweled.round5_setup.lanes["lakebase"].state.value == "verified"
        assert toweled.round5_setup.lanes["lakebase"].setup_elapsed_ms == 375.25
        assert toweled.round5_setup.lanes["competitor"].state.value == "towelled"
        # The setup lane is the second reader of this number: the receipt takes
        # its Round 5 figures from here, so a stale value survives here even if
        # the censored bound above is right.
        assert toweled.round5_setup.lanes["competitor"].setup_elapsed_ms == silent_floor_ms
        assert "No winner · Margin N/A" in (toweled.remembered_result or "")

        # What the result screen reads.
        receipt = derive_receipt(toweled, "towel_finished")
        assert receipt.metric == "setup_elapsed_ms"
        assert receipt.lakebase.ms == 375.25
        assert receipt.lakebase.lower_bound is False
        assert receipt.opponent_lane.ms == silent_floor_ms
        assert receipt.opponent_lane.lower_bound is True
    finally:
        await manager.close()


async def test_the_round_five_towel_floor_never_measures_from_the_run_origin() -> None:
    """Defence in depth on the one direction a Round 5 towel floor can overstate.

    The floor is built from the lane's own last published `setup_elapsed_ms` plus
    the monotonic interval since that value was observed, which is correct. But
    `server/towel.py` accepts *any* `elapsed_at_cutoff_ms` at or above the latch:
    its `max()` guarantees the floor cannot move backwards, and guarantees nothing
    at all about the clock origin the caller measured from. So a future caller that
    reached for `record.run_started_monotonic_ns` -- the obvious attribute, and the
    one the non-Round-5 branch does use -- would produce a floor that is too *high*
    by however long arm and preflight took, and would print it beside the
    opponent's name. Overstating an opponent's time is the failure class this
    project polices hardest, and nothing in `towel.py` can catch it, because by the
    time the number arrives there it is indistinguishable from a correct one.

    HOW THIS AVOIDS BEING VACUOUS, which matters because the shape it would fall
    into is one this repository has shipped: a test asserting against a value its
    own fake sets. Two independent quantities are needed to tell the two candidate
    floors apart, and here they are independent by construction --

    * `silent_gap_ns` is advanced by the *test*, not by the clock being read, so
      time passing and the clock being read are separate events. An earlier
      generation of assertions in this area used a fake whose value was a function
      of its read count, which made "time passed while nobody read the clock" a
      state the fixture could not express, so the assertions could not fail for
      the reason they were written.
    * `preflight_lead_ns` is the distance between the run origin and the lane's
      own origin, planted directly onto the record. Nothing in the Round 5 path
      reads it today, which is exactly the property under test: the floor must be
      the same number whether that origin is honest or absurd.

    The two candidate floors are asserted to differ before the real assertion
    runs, so a future edit that collapsed them (`preflight_lead_ns = 0`, say)
    fails here rather than passing by making the discriminator disappear.
    """

    competitor_published = asyncio.Event()

    class HoldingSetupEngine:
        has_timed_setup = True

        async def setup(self, _bout_id, _fencing_token, on_progress):
            await on_progress(
                SimpleNamespace(
                    lane_id="competitor",
                    phase="creating_proxy",
                    status="running",
                    setup_elapsed_ms=500.0,
                )
            )
            competitor_published.set()
            await asyncio.Event().wait()

        async def cancel_setup_and_settle(self, _bout_id):
            return None

    # Driven by the test, never by the number of times it is read.
    clock = {"ns": 1_000_000_000_000}

    manager = RunManager(
        connection_spike_factory=lambda _competitor: HoldingSetupEngine(),
        clock_ns=lambda: clock["ns"],
    )
    operator = BoutOperator(display_name="Round Five Owner", subject="round-five-owner")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )

    try:
        await manager.start_arm(created.id, operator)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id, operator)
        await asyncio.wait_for(competitor_published.wait(), timeout=1)

        # The poison. A preflight-inclusive origin sits *earlier* than the lane's
        # own T0 by whatever arm and preflight cost, so a floor measured from it
        # overstates by that much. Ten minutes is not plausible; it is legible,
        # which is the point of a planted value.
        preflight_lead_ns = 600_000_000_000
        record = manager._records[created.id]
        assert record.run_started_monotonic_ns is not None, (
            "the run origin is unset, so poisoning it proves nothing -- this test "
            "would pass against a Round 5 path that read it"
        )
        record.run_started_monotonic_ns = clock["ns"] - preflight_lead_ns

        # The silent phase: the RDS lane says nothing while its proxy provisions.
        # No callback fires across it, so nothing re-reads the clock.
        silent_gap_ns = 30_000_000_000
        clock["ns"] += silent_gap_ns

        lane_anchored_ms = 500.0 + silent_gap_ns / 1_000_000
        run_anchored_ms = (preflight_lead_ns + silent_gap_ns) / 1_000_000
        assert run_anchored_ms > lane_anchored_ms, (
            "the two candidate floors are equal, so this test can no longer tell "
            "them apart -- restore a non-zero preflight lead"
        )

        toweled = await manager.start_towel(created.id, operator)

        assert toweled.towel is not None
        assert toweled.towel.censored_lower_bounds_ms == {"competitor": lane_anchored_ms}
        assert toweled.lanes["competitor"].evidence["lower_bound_ms"] == lane_anchored_ms
        # The receipt is the second reader of this number and the one an audience
        # sees, so the overstatement is pinned where it would be displayed too.
        assert derive_receipt(toweled, "towel_finished").opponent_lane.ms == lane_anchored_ms
    finally:
        await manager.close()


def test_no_round_five_towel_branch_names_the_preflight_inclusive_origin() -> None:
    """The same pin, stated structurally, because behaviour cannot see every read.

    The behavioural test above catches a Round 5 path that *uses* the run origin
    for the floor, including via a `max()` that prefers it. It cannot catch a read
    whose effect happens to be invisible on that one fixture. This one asserts the
    stronger and simpler property directly: inside `RunManager.start_towel`, every
    mention of `run_started_monotonic_ns` is in a non-Round-5 branch.

    The positive control is the second assertion, not a comment: the `else` branch
    must still contain such a read. Without it, deleting the attribute outright --
    or mis-locating the branch -- would leave this test green while proving
    nothing.
    """

    tree = ast.parse(pathlib.Path("server/manager.py").read_text(encoding="utf-8"))
    manager = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "RunManager"
    )
    start_towel = next(
        node
        for node in manager.body
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef)
        and node.name == "start_towel"
    )

    def origin_lines(nodes: list[ast.stmt]) -> list[int]:
        return sorted(
            child.lineno
            for node in nodes
            for child in ast.walk(node)
            if isinstance(child, ast.Attribute)
            and child.attr == "run_started_monotonic_ns"
        )

    round_five_branches = [
        node
        for node in ast.walk(start_towel)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "is_round_five"
        and node.orelse
    ]
    assert round_five_branches, (
        "no `if is_round_five: ... else: ...` remains in start_towel, so this "
        "guard can no longer find the branch it is written about. Re-derive it "
        "against the current shape rather than deleting it."
    )

    in_round_five = sorted(
        line for node in round_five_branches for line in origin_lines(node.body)
    )
    assert not in_round_five, (
        f"server/manager.py{in_round_five} reads run_started_monotonic_ns inside "
        f"the Round 5 towel branch. That origin is preflight-inclusive and earlier "
        f"than the setup orchestrator's T0, so a floor derived from it OVERSTATES "
        f"the towelled lane's elapsed time -- and server/towel.py cannot refuse it, "
        f"because its max() only guarantees monotonicity, never a correct origin. "
        f"Round 5 must carry each lane's last published setup_elapsed_ms forward by "
        f"the monotonic interval since it was observed."
    )

    in_other_rounds = sorted(
        line for node in round_five_branches for line in origin_lines(node.orelse)
    )
    assert in_other_rounds, (
        "no branch of start_towel reads run_started_monotonic_ns at all, so the "
        "assertion above passes by finding nothing. Either the non-Round-5 cutoff "
        "stopped measuring from the run origin -- in which case retarget this "
        "guard -- or the branch located above is the wrong one."
    )


async def test_round_five_setup_failure_retains_absorbed_exact_stop(caplog) -> None:
    class FailingSetupEngine:
        has_timed_setup = True

        async def setup(self, _bout_id, _fencing_token, on_progress):
            await on_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="setup_stop",
                    status="verified",
                    setup_elapsed_ms=420.5,
                )
            )
            await on_progress(
                SimpleNamespace(
                    lane_id="lakebase",
                    phase="verifying_transaction",
                    status="running",
                    setup_elapsed_ms=9_999.0,
                )
            )
            await on_progress(
                SimpleNamespace(
                    lane_id="competitor",
                    phase="creating_proxy",
                    status="running",
                    setup_elapsed_ms=750.0,
                )
            )
            raise ConnectionSpikeLiveOperationError(
                "Round 5 setup runner did not return exact sanitized evidence: "
                "the runner refused with baseline_auth_hash_invalid"
            )

        async def cancel_setup_and_settle(self, _bout_id):
            return None

    manager = RunManager(connection_spike_factory=lambda _competitor: FailingSetupEngine())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )

    try:
        await manager.start_arm(created.id)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id)
        failed = await wait_for_state(manager, created.id, SessionState.FAILED)

        assert failed.round5_setup is not None
        lakebase = failed.round5_setup.lanes["lakebase"]
        assert lakebase.state.value == "verified"
        assert lakebase.setup_elapsed_ms == 420.5
        assert lakebase.status == "Built-in Lakebase pool verified"
        assert failed.round5_setup.lanes["competitor"].setup_elapsed_ms == 750.0
        assert failed.comparison is None
        assert failed.metrics == []
        setup_failures = [
            record
            for record in caplog.records
            if record.name == "server.manager" and "bout failed" in record.getMessage()
        ]
        assert setup_failures
        line = setup_failures[-1].getMessage()
        assert "baseline_auth_hash_invalid" in line
        assert created.id in line
        assert setup_failures[-1].exc_info is not None
    finally:
        await manager.close()


@pytest.mark.parametrize(
    ("burst_valid", "cleanup_failure", "cleanup_abandoned"),
    [
        (True, False, False),
        (False, False, False),
        (False, True, False),
        # A bout that verified and then could not clean up after itself. Nothing
        # about this case was recorded anywhere before: the verdict was published
        # first, and the abandonment that followed wrote only a log line.
        (True, False, True),
    ],
)
async def test_round_five_setup_is_primary_and_burst_is_a_secondary_gate(
    burst_valid: bool,
    cleanup_failure: bool,
    cleanup_abandoned: bool,
) -> None:
    engine = SuccessfulTwoPhaseConnectionSpikeEngine(
        burst_valid=burst_valid,
        cleanup_failure=cleanup_failure,
        cleanup_abandoned=cleanup_abandoned,
    )
    selected_opponents: list[CompetitorId] = []

    def factory(competitor: CompetitorId):
        selected_opponents.append(competitor)
        return engine

    manager = RunManager(connection_spike_factory=factory)
    if cleanup_abandoned:
        manager._cleanup_retry_initial = 0.001
        manager._cleanup_retry_max = 0.001
        manager._cleanup_retry_attempts = 2
    operator = BoutOperator(display_name="Round Five Owner", subject="round-five-owner")
    selected_competitor = (
        CompetitorId.AURORA_SERVERLESS_V2
        if burst_valid and not cleanup_failure
        else CompetitorId.RDS_POSTGRES
    )
    created = await manager.create(
        SessionCreate(
            competitor=selected_competitor,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    assert created.cost_receipt is not None
    lakebase_compute = next(
        line for line in created.cost_receipt.lines if line.component == "Lakebase compute"
    )
    assert lakebase_compute.unit_rate_usd == pytest.approx(0.26)
    assert lakebase_compute.reference_list_unit_rate_usd == pytest.approx(0.52)
    assert lakebase_compute.rate_basis == "current_promotion"
    lakebase_storage = next(
        line for line in created.cost_receipt.lines if "storage" in line.component.lower()
    )
    assert lakebase_storage.unit_rate_usd == pytest.approx(0.023)
    assert lakebase_storage.reference_list_unit_rate_usd is None
    assert lakebase_storage.rate_basis == "standard_list"
    proxy_line = next(
        line for line in created.cost_receipt.lines if line.component.startswith("RDS Proxy ·")
    )
    assert proxy_line.quantity is None
    assert proxy_line.subtotal_usd is None
    assert proxy_line.status == "usage_pending"
    assert proxy_line.unit_rate_usd == pytest.approx(0.015)
    assert "published 10-minute minimum applies; provider lifetime pending" in proxy_line.component
    secrets_requests = next(
        line
        for line in created.cost_receipt.lines
        if line.component == "Secrets Manager API requests"
    )
    assert secrets_requests.unit_rate_usd == pytest.approx(0.05)
    assert secrets_requests.unit == "10,000 requests"
    assert secrets_requests.quantity is None
    assert secrets_requests.status == "usage_pending"
    database_ipv4 = next(
        line for line in created.cost_receipt.lines if line.component == "Database public IPv4"
    )
    assert database_ipv4.lane_id == "competitor"
    assert database_ipv4.unit_rate_usd == pytest.approx(0.005)
    assert database_ipv4.unit == "address-hour"
    assert database_ipv4.quantity is None
    assert database_ipv4.status == "usage_pending"
    assert database_ipv4.source_as_of == datetime(2026, 7, 24, 15, 42, 25, tzinfo=UTC)
    runner_ipv4 = next(
        line
        for line in created.cost_receipt.lines
        if line.component == "Neutral runner public IPv4"
    )
    assert runner_ipv4.source_as_of == datetime(2026, 7, 24, 15, 42, 25, tzinfo=UTC)
    assert "AmazonVPC" in runner_ipv4.source
    cross_az = next(
        line
        for line in created.cost_receipt.lines
        if line.component == "Cross-AZ runner ↔ database transfer"
    )
    assert cross_az.unit_rate_usd == pytest.approx(0.01)
    assert cross_az.unit == "GB"
    assert cross_az.quantity is None
    assert cross_az.status == "usage_pending"
    assert "EC2 bytes sent + received counted once; no RDS-side duplicate" in cross_az.source

    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    terminal = await wait_for_state(
        manager,
        created.id,
        SessionState.VERIFIED if burst_valid else SessionState.FAILED,
    )

    assert terminal.round5_setup is not None
    assert terminal.competitor.id == selected_competitor
    assert terminal.lanes["competitor"].name == (f"{terminal.competitor.short_name} + RDS Proxy")
    assert terminal.round5_setup.lanes["lakebase"].setup_elapsed_ms == 1000.0
    assert terminal.round5_setup.lanes["competitor"].setup_elapsed_ms == 1250.0
    assert terminal.round5_setup.workflow_launch_skew_ms == 1.0
    assert terminal.lanes["lakebase"].p99_ms == (None if cleanup_failure else 1.0)
    assert terminal.lanes["lakebase"].elapsed_ms is None
    assert terminal.cost_receipt is not None
    terminal_proxy = next(
        line for line in terminal.cost_receipt.lines if line.component.startswith("RDS Proxy ·")
    )
    expected_proxy_cost = (
        0.020 if selected_competitor == CompetitorId.AURORA_SERVERLESS_V2 else 0.005
    )
    assert terminal_proxy.subtotal_usd == pytest.approx(expected_proxy_cost)
    assert terminal_proxy.quantity_method == "result_evidence"
    assert terminal_proxy.observed_from == terminal.run_started_at
    assert terminal_proxy.observed_through is not None
    assert terminal_proxy.observed_through <= terminal.updated_at
    assert terminal.cost_receipt.known_bout_estimate_usd == pytest.approx(expected_proxy_cost)
    if burst_valid and not cleanup_failure:
        assert terminal.round5_setup.downstream_validated is True
        assert terminal.comparison is not None
        assert terminal.comparison.winner_lane_id == "lakebase"
        assert terminal.comparison.margin is not None
        assert terminal.comparison.margin.spec_id == "setup_elapsed_ms"
        assert terminal.comparison.margin.value == 250.0
        setup_metrics = {
            item.lane_id: item.value
            for item in terminal.metrics
            if item.spec_id == "setup_elapsed_ms"
        }
        assert setup_metrics == {"lakebase": 1000.0, "competitor": 1250.0}
    else:
        assert terminal.round5_setup.downstream_validated is False
        assert terminal.comparison is None
        assert terminal.metrics == []

    if cleanup_failure:
        for _ in range(100):
            retried = await manager.get(created.id)
            if retried.round5_setup and not retried.round5_setup.cleanup_retryable:
                break
            await asyncio.sleep(0.005)
        else:
            raise AssertionError("Cleanup retry did not settle")
        assert retried.round5_setup is not None
        assert retried.round5_setup.state.value == "failed"
        assert all(
            lane.status == "The live Round 5 proof failed unexpectedly."
            and lane.error == lane.status
            and lane.activity is not None
            and lane.activity.phase == "cleanup_failed"
            for lane in retried.lanes.values()
        )
        assert retried.comparison is None
        assert (await manager.bout_status()).active is False
        assert len(engine.reconcile_calls) == 1
        assert engine.reconcile_calls[0][1] == 1
        repeated = await manager.retry_connection_spike_cleanup(created.id, operator)
        assert repeated.round5_setup is not None
        assert repeated.round5_setup.cleanup_retryable is False
        assert len(engine.reconcile_calls) == 1

    if cleanup_abandoned:
        # The bout keeps its win and still admits the mess. Both facts belong on
        # the receipt: the measurement stands, and an RDS Proxy may be billing
        # right now. Until the setup snapshot carried the diagnostic there was
        # nothing to seal -- the verdict's receipt was written before the
        # abandonment, and the abandonment wrote only a `logger.error` line and a
        # message-free `cleanup_update` to the browser.
        sealed = await sealed_receipt(
            created.id,
            until=lambda item: item.cleanup_failure is not None,
        )
        assert sealed is not None, "an abandoned Round 5 cleanup sealed nothing"
        assert sealed.outcome == "declared"
        assert sealed.cleanup_failure is not None
        assert "did not converge" in sealed.cleanup_failure

        abandoned = await manager.get(created.id)
        assert abandoned.state == SessionState.VERIFIED
        assert abandoned.round5_setup is not None
        assert abandoned.round5_setup.cleanup_failure == sealed.cleanup_failure
        # A tidy-up failure is not evidence the setup did not verify, so it must
        # not be written over the round's own state or its failure line.
        assert abandoned.round5_setup.state.value == "verified"
        assert abandoned.round5_setup.failure is None
        # And the ring stays held, which is the whole reason this state has to be
        # legible: the artifacts were never proved gone.
        assert abandoned.round5_setup.cleanup_retryable is True
        assert (await manager.bout_status()).active is True

    public = str(terminal.model_dump(mode="json")).lower()
    for forbidden in (
        "credential_sha256",
        "secret_arn",
        "fencing_token",
        "t0_ns",
        "deadline_ns",
        "workflow_launched_ns",
    ):
        assert forbidden not in public

    lane_events = [
        event.payload
        for event in manager._records[created.id].event_log.events
        if event.event == "lane_update"
    ]
    assert lane_events
    assert all(
        {"lane_id", "state", "status", "activity"} <= payload.keys() for payload in lane_events
    )

    if burst_valid and not cleanup_failure:
        for mutation in (
            lambda value: value.update(lanes={}),
            lambda value: value["round5_setup"].update(lanes={}),
            lambda value: value["comparison"].update(kind="capability_gap"),
            lambda value: value["comparison"].update(kind="not_comparable"),
        ):
            invalid = terminal.model_copy(deep=True).model_dump(mode="json")
            mutation(invalid)
            with pytest.raises(ValueError):
                type(terminal).model_validate(invalid)

    assert selected_opponents
    assert set(selected_opponents) == {selected_competitor}


async def wait_for_cooldown(manager: RunManager, session_id: str, state: CooldownState):
    for _ in range(100):
        snapshot = await manager.get(session_id)
        if snapshot.cooldown and snapshot.cooldown.state == state:
            return snapshot.cooldown
        await asyncio.sleep(0.005)
    raise AssertionError(f"Cooldown never reached {state}")


async def wait_for_towel(manager: RunManager, session_id: str, state: str):
    for _ in range(100):
        snapshot = await manager.get(session_id)
        if snapshot.towel and snapshot.towel.state == state:
            return snapshot
        await asyncio.sleep(0.005)
    raise AssertionError(f"Towel never reached {state}")


async def sealed_receipt(session_id: str, until=None):
    """The durable receipt this bout left behind, read the way `/receipts` reads it.

    Polled rather than read once, because a receipt is written inside
    `EventLog.publish` and every `wait_for_*` above watches the snapshot, which
    the publishing task made visible one statement earlier. Returns the last
    thing it saw when `until` never came true, so a failing assertion reports
    the receipt that is actually there rather than a timeout.
    """

    found = None
    for _ in range(200):
        found = next(
            (item for item in load_receipts() if item.session_id == session_id),
            None,
        )
        if found is not None and (until is None or until(found)):
            return found
        await asyncio.sleep(0.005)
    return found


async def wait_for_redo(manager: RunManager, session_id: str, state: RedoState):
    for _ in range(100):
        snapshot = await manager.get(session_id)
        if snapshot.redo and snapshot.redo.state == state:
            return snapshot
        await asyncio.sleep(0.005)
    raise AssertionError(f"Re-do never reached {state}")


async def test_round_one_towel_stops_verifier_before_zero_state_settlement() -> None:
    resolver = SimpleNamespace(
        resolve=lambda _competitor: (
            FakeLiveTarget("lakebase", "Lakebase", 60),
            FakeLiveTarget("competitor", "Aurora Serverless v2", 60),
        )
    )
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await wait_for_state(manager, created.id, SessionState.RUNNING)

    frozen = await manager.start_towel(created.id)
    assert frozen.towel is not None
    assert set(frozen.towel.censored_lower_bounds_ms) == {"lakebase", "competitor"}
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.state == SessionState.TOWELLED
    assert await manager._lease_store.current() is None


async def test_round_two_public_timer_floor_matches_authoritative_towel_cutoff() -> None:
    clock = {"ns": 10_000_000_000}
    engine = BlockingTowelSafeChangeEngine()
    manager = RunManager(
        safe_change_factory=lambda: engine,
        clock_ns=lambda: clock["ns"],
    )
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)

    # The provider stays silent after its first 1ms callback while AWS continues
    # polling. A public refresh must serialize the bout clock, not that stale latch.
    clock["ns"] += 21_250_000_000
    refreshed = await manager.get(created.id)
    assert refreshed.state == SessionState.RUNNING
    assert refreshed.lanes["lakebase"].elapsed_ms == 1.0
    assert refreshed.lanes["lakebase"].elapsed_at_snapshot_ms == 21_250.0
    assert refreshed.lanes["competitor"].elapsed_ms is None
    assert refreshed.lanes["competitor"].elapsed_at_snapshot_ms == 21_250.0

    stopped = await manager.start_towel(created.id)
    assert stopped.towel is not None
    assert stopped.towel.censored_lower_bounds_ms == {
        "lakebase": 21_250.0,
        "competitor": 21_250.0,
    }
    await wait_for_towel(manager, created.id, "ready")


async def test_a_towel_thrown_mid_run_settles_each_rounds_own_engine() -> None:
    """One shape per round, and the shape is the contract.

    Every one of these rounds owns something a towel has to put back -- Round 2
    an exact environment, Round 4 the managed-sync baseline, Round 6 an order
    identity -- and every one of them is being towelled from inside a blocked
    ``run``, which is the case the cleanup exists for. The engine flag named per
    row is the specific restoration; ``wait_for_towel(..., "ready")`` reaching
    ready at all is the shared part.

    Rounds 1 and 5 are towelled by their own tests: Round 1 has no engine to
    block in, and Round 5's towel is a two-ring handshake rather than a single
    settle.

    **Round 4's row asserts a second, more expensive restoration.** Its engine
    owns a Managed Sync pipeline that bills $14.57/day while up, and a towelled
    bout leaves no redo to protect, so the towel owes an immediate stop. That
    was asserted for a long time only as "the fake set ``baseline_restored``",
    which a fake can set without a pipeline existing anywhere -- so the row
    passed throughout the period the pipeline was being left running. It now
    asserts against the pipeline wire, which goes down only when a ``/stop``
    actually reaches it.
    """

    cases: tuple[tuple[str, str, object, RoundId, str, Corner, str, bool], ...] = (
        (
            "round two settles then deletes exact environments",
            "safe_change_factory",
            BlockingTowelSafeChangeEngine,
            RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
            "software_engineer",
            Corner.SIMPLICITY,
            "settled",
            True,
        ),
        (
            "round four restores the managed sync baseline",
            "model_score_factory",
            BlockingTowelModelScoreEngine,
            RoundId.PUT_MODEL_SCORE_IN_APP,
            "software_engineer",
            Corner.SIMPLICITY,
            "baseline_restored",
            False,
        ),
        (
            "round six settles both owned order identities",
            "live_orders_factory",
            BlockingTowelLiveOrdersEngine,
            RoundId.ANALYZE_LIVE_ORDERS,
            "data_analyst",
            Corner.PERFORMANCE,
            "cleaned",
            False,
        ),
    )

    for name, factory_kwarg, engine_cls, round_id, persona, corner, flag, cooldown in cases:
        engine = engine_cls()
        manager = RunManager(**{factory_kwarg: lambda engine=engine: engine})
        created = await manager.create(
            SessionCreate(
                competitor=CompetitorId.AURORA_SERVERLESS_V2,
                primary_persona=persona,
                corners=[corner],
                round_id=round_id,
            )
        )
        await manager.start_arm(created.id)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id)
        await asyncio.wait_for(engine.run_entered.wait(), timeout=1)

        await manager.start_towel(created.id)
        settled = await wait_for_towel(manager, created.id, "ready")
        assert getattr(engine, flag).is_set(), f"{name}: {flag} never set"
        if cooldown:
            assert settled.cooldown is not None, name
            assert settled.cooldown.state == CooldownState.READY, name
        pipeline = getattr(engine, "pipeline", None)
        if pipeline is not None:
            # The restoration the flag above cannot see. This bout's own arm
            # started the pipeline and the towel published no redo, so the
            # window that twenty minutes of billing buys protects nothing and
            # the stop is owed immediately.
            assert pipeline.stop_verbs, (
                f"{name}: the towel settled its engine but never stopped the Managed "
                f"Sync pipeline its own arm started, which bills until somebody notices"
            )
            assert pipeline.running is False, name


async def test_shutdown_performs_the_round_four_pipeline_stop_no_record_owns() -> None:
    """The clean-finish path's release is the ninth task, and nothing flushed it.

    ``close()`` accounts for eight tasks per session record. The delayed pipeline
    release is not one of them: it belongs to the module-global activation
    registry, keyed by pipeline, because a fresh engine is built for every arm
    while the pipeline outlives all of them. So a server stopped inside the
    twenty-minute window cancelled the loop, the task went with it, no stop verb
    was issued and nothing recorded that one had been owed -- and the next
    ``doctor`` read a pipeline billing $14.57/day as one nobody had ever
    intended to stop.

    The operator behaviour that triggers this is entirely ordinary: finish the
    demo, quit the server.

    Driven through ``RunManager.close()`` rather than through the activation
    directly, because the missing wiring *was* the defect, and asserted against
    a pipeline wire that goes down only when a ``/stop`` reaches it.
    """

    api = FakeRound4PipelineApi(running=False)
    activation = model_score_live.Round4PipelineActivation(
        round4_activation_manifest(),
        api,
        pipeline_id="pipeline-1",
        poll_seconds=0,
    )
    await activation.ensure_running(lambda _status: asyncio.sleep(0))
    assert api.running is True, "arm should have started the stopped pipeline"

    # Exactly what the tail of a clean-finished Round 4 settlement leaves behind.
    activation.release_when_idle()
    assert activation._release is not None

    model_score_live._ACTIVATIONS["pipeline-1"] = activation
    try:
        await RunManager().close()
    finally:
        model_score_live._ACTIVATIONS.pop("pipeline-1", None)
        activation._cancel_release()

    assert api.stop_verbs, (
        "shutdown cancelled the pending Round 4 pipeline release instead of "
        "performing it, so the stop was silently lost and the pipeline bills on"
    )
    assert api.running is False


async def test_round_five_towel_holds_main_until_proxy_delete_acceptance() -> None:
    engine = BlockingTowelConnectionSpikeEngine()
    main_store = InMemoryBoutLeaseStore()
    round5_store = InMemoryBoutLeaseStore(ring_key="round5")
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        lease_store=main_store,
        round5_lease_store=round5_store,
    )
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)

    await manager.start_towel(created.id)
    await asyncio.wait_for(engine.cleanup_started.wait(), timeout=1)
    assert await main_store.current() is not None
    engine.delete_accepted.set()
    for _ in range(100):
        if await main_store.current() is None:
            break
        await asyncio.sleep(0)
    assert await main_store.current() is None
    assert await round5_store.current() is not None
    engine.cleanup_complete.set()
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.state == SessionState.TOWELLED
    assert await round5_store.current() is None

    # And it leaves a receipt. Round 5's towel finishes inside the backstage
    # cleanup handoff rather than at the one site that publishes
    # `towel_finished`, so this bout used to cost ten minutes and a real RDS
    # Proxy and produce nothing durable at all -- no receipt, no scorecard row,
    # only server log lines. Claims here rest on the receipt, not the screen.
    receipt = await sealed_receipt(created.id)
    assert receipt is not None, "a towelled Round 5 sealed no receipt"
    assert receipt.round_id == RoundId.SURVIVE_CONNECTION_SPIKE
    assert receipt.outcome == "stopped_short"
    # Round 5 is judged on the setup stop gate, not a run clock. Its towel
    # freezes already-published setup evidence rather than censoring on a
    # cutoff, and the receipt has to carry that distinction or it understates
    # the round by two orders of magnitude.
    assert receipt.metric == "setup_elapsed_ms"
    # The tidy-up worked, and the receipt says so by having nothing to say.
    assert receipt.cleanup_failure is None


def round_four_request() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="data_scientist_ml",
        corners=[Corner.PERFORMANCE],
        round_id=RoundId.PUT_MODEL_SCORE_IN_APP,
    )


async def verified_round_four(
    manager: RunManager,
    operator: BoutOperator,
):
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    # `wait_for_state` watches the snapshot, and the snapshot is not the whole
    # of "the run is over". The Round 4 run sets `state = VERIFIED` and then
    # keeps going inside the same task -- it still has the terminal event to
    # publish and the round settlement to schedule. Every one of the fourteen
    # callers of this helper goes straight on to `start_redo`, and `start_redo`
    # refuses with "A session operation is already running" while `record.task`
    # is unfinished. So returning on the snapshot alone hands the caller a bout
    # that is ready only if the 5ms poll above happened to land after the task
    # finished rather than during its tail.
    #
    # It almost always does, which is what made this look like a phantom: the
    # tail is sub-millisecond, and instrumenting the window showed it shut on
    # every one of forty randomised orders of this file alone, and on 200
    # consecutive runs of the sequence in a quiet loop. It opens under the
    # loaded event loop of a full-suite run -- one of twelve whole-suite
    # randomised orders caught it, and that order also failed
    # `test_lost_redo_lease_preserves_initial_verified_proof` with the refusal
    # above. A file-scoped stress run would never have found it.
    #
    # Awaited rather than slept on, because the task is precisely the thing
    # being waited for. Bounded because nothing in this suite bounds a hung
    # test, and shielded so that a timeout reports itself rather than cancelling
    # the bout and cascading into a different failure.
    task = manager._records[created.id].task
    if task is not None and not task.done():
        await asyncio.wait_for(asyncio.shield(task), timeout=5)
    return created, verified


async def test_legacy_round_serialization_excludes_round_four_additions() -> None:
    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )

    payload = created.model_dump(mode="json")

    assert "metric_specs" not in payload
    assert "metrics" not in payload
    assert "comparison" not in payload
    assert "redo" not in payload
    assert "evidence" not in payload["lanes"]["lakebase"]
    assert "metric_specs" not in payload["round"]
    assert "comparison_kind" not in payload["round"]
    assert "non_claims" not in payload["round"]
    assert payload["round"]["redo"] == {
        "policy": "show",
        "badge": "★ SHOW",
        "label": "RE-DO ROUND",
        "description": ("Repeat the wake proof to show the same automatic product behavior."),
    }


async def test_round_four_factory_is_lazy_and_missing_factory_arm_precedes_lease() -> None:
    lease_store = InMemoryBoutLeaseStore()
    manager = RunManager(lease_store=lease_store)
    created = await manager.create(round_four_request())
    assert created.round.availability.value == "planned"

    with pytest.raises(InvalidStateError, match="live adapter is not configured"):
        await manager.start_arm(created.id)
    assert await lease_store.current() is None

    engine = FakeModelScoreEngine()
    factory_calls = 0

    def factory():
        nonlocal factory_calls
        factory_calls += 1
        return engine

    available = RunManager(model_score_factory=factory)
    ready = await available.create(round_four_request())
    assert ready.round.availability.value == "ready"
    assert factory_calls == 0
    await available.get(ready.id)
    assert factory_calls == 0
    await available.start_arm(ready.id)
    await wait_for_state(available, ready.id, SessionState.ARMED)
    assert factory_calls == 1
    assert available._records[ready.id].model_score_engine is engine


async def test_round_four_initial_proof_has_exact_evidence_metrics_and_gap() -> None:
    engine = FakeModelScoreEngine()
    manager = RunManager(model_score_factory=lambda: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-4")

    created, verified = await verified_round_four(manager, operator)
    record = manager._records[created.id]

    assert engine.arm_calls == 1
    assert engine.run_calls == 1
    assert record.model_score_engine is engine
    assert record.model_score_arm is engine.arm_result
    assert record.model_score_result is engine.initial_result
    assert verified.remembered_result == ("LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A")
    assert verified.lanes["competitor"].state == LaneState.NOT_SUPPORTED
    assert set(verified.lanes["competitor"].evidence) == {"unsupported_reason"}
    assert verified.lanes["competitor"].evidence["unsupported_reason"] == (
        "No AWS-native equivalent lane was configured or timed in this scoped proof."
    )
    evidence = verified.lanes["lakebase"].evidence
    assert evidence["primary_key"] == engine.contract.entity_id
    assert evidence["score"] == 0.81
    assert evidence["model_version"] == "risk-v1"
    assert evidence["proof_nonce"].startswith("round4-v1-")
    assert evidence["delta_version"] == 11
    assert evidence["status_delta_commit_time"] == datetime(2026, 8, 18, 12, 0, 11, tzinfo=UTC)
    assert evidence["sync_end_time"] == datetime(2026, 8, 18, 12, 0, 11, 125000, tzinfo=UTC)
    assert evidence["verified_row"]["proof_nonce"] == evidence["proof_nonce"]
    assert verified.lanes["lakebase"].elapsed_ms == 250.0
    metrics = {item.spec_id: item for item in verified.metrics}
    assert metrics["managed_availability_ms"].value == 125.0
    assert metrics["application_proof_elapsed_ms"].value == 250.0
    assert metrics["delta_commit_version"].value == 11
    assert metrics["exact_row_verified"].value is True
    assert verified.comparison is not None
    assert verified.comparison.kind == ComparisonKind.CAPABILITY_GAP
    assert verified.comparison.winner_lane_id == "lakebase"
    assert verified.comparison.margin is None
    assert verified.comparison.detail == (
        "Lakebase verified the scoped native Synced Tables capability; no AWS-native "
        "equivalent lane was timed. This is not a speed comparison."
    )
    assert verified.redo is not None and verified.redo.state == RedoState.READY


@pytest.mark.parametrize("emit_progress", [False, True])
async def test_round_four_progress_is_observational(emit_progress: bool) -> None:
    engine = FakeModelScoreEngine()
    engine.emit_progress = emit_progress
    manager = RunManager(model_score_factory=lambda: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-progress")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    if emit_progress:
        record = manager._records[created.id]
        publish = record.event_log.publish

        async def fail_lane_updates(event, payload):
            if event == "lane_update":
                raise RuntimeError("SSE observer disconnected")
            return await publish(event, payload)

        record.event_log.publish = fail_lane_updates
    await manager.start_run(created.id, operator)

    verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    assert verified.lanes["lakebase"].state == LaneState.VERIFIED


async def test_round_four_initial_failure_waits_for_confirmed_release(caplog) -> None:
    """And the exception it substitutes a sentence for reaches the log.

    `_arm_refusal` closed this shape for the six arm handlers and left it in the
    six run handlers, so a refusal before the bell named itself and the identical
    refusal after the bell did not. The added assertions pin both halves of the
    bargain: the diagnosis is recoverable from the log, and the quoting boundary
    is genuinely applied to it rather than assumed.
    """

    lease_store = BlockingReleaseStore()
    engine = FakeModelScoreEngine()
    engine.fail_run = True
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-failure")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)

    await asyncio.wait_for(lease_store.release_started.wait(), timeout=1)
    pending = manager._records[created.id].snapshot
    assert pending.state == SessionState.RUNNING
    assert all(
        event.event != "session_failed" for event in manager._records[created.id].event_log.events
    )
    lease_store.allow_release.set()
    with caplog.at_level(logging.ERROR, logger="server.manager"):
        failed = await wait_for_state(manager, created.id, SessionState.FAILED)
    assert failed.failure == "The live Managed Sync proof failed unexpectedly."
    assert failed.comparison is None
    assert failed.remembered_result is None
    assert failed.lanes["competitor"].evidence["unsupported_reason"] == (
        "No AWS-native equivalent lane was configured or timed in this scoped proof."
    )

    bout = [
        record
        for record in caplog.records
        if record.name == "server.manager" and "bout failed" in record.getMessage()
    ]
    assert bout, (
        "the run handler dropped the exception and substituted a fixed sentence, "
        "so the only copy of the reason was destroyed"
    )
    line = bout[0].getMessage()
    assert "diagnosis=RuntimeError" in line
    assert created.id in line
    # A traceback belongs in the log and not on the panel. This is the half of
    # the split that `operator_diagnosis` deliberately does not carry.
    assert bout[0].exc_info is not None
    assert bout[0].levelno == logging.ERROR
    # THE BOUNDARY, ASSERTED RATHER THAN ASSUMED. `RuntimeError` is a builtin, so
    # `_message_is_ours_to_quote` withholds its words and contributes the type
    # name alone. This is the assertion that would fail if someone reached for
    # `str(exc)` here, and it is what keeps a `psycopg` DSN or a `botocore`
    # secret ARN out of a file that outlives the bout.
    assert "initial proof failed" not in line
    assert not any("initial proof failed" in record.getMessage() for record in caplog.records)


async def test_a_databricks_refusal_survives_the_arm_and_lifts_on_the_next_success() -> None:
    """The refusal reaches the operator, reaches the catalog, and then lifts.

    Verbatim from the deployed app on 2026-08-23, which is the point: Round 4's
    first Lakebase call was refused for a missing `SELECT` on the synced table
    and the API said "The Managed Sync baseline could not be verified." The exact
    table, the exact permission and the exact principal existed only in a
    container log reachable over a WebSocket.

    A test that asserted the exception *type* would have passed against the
    defect, because the defect was never about the type -- it was about throwing
    the message away. So what is asserted is that the words survive.

    The tail is the other half of the bargain. Remembering a refusal so the
    round-select screen stops offering the round is only honest if the memory
    can be corrected, and the one correction that counts is the round arming
    successfully: the same call, against the same principal, no longer refused.
    Requiring a restart to clear it would trade a false green for a false red
    that outlives its cause.
    """

    verbatim = (
        "User does not have SELECT on Table "
        "'example_catalog.anti_demo_online_ad_20260101_0000_0000.model_scores'."
    )
    engine = FakeModelScoreEngine()
    engine.arm_error = PermissionDenied(verbatim)
    manager = RunManager(model_score_factory=lambda: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-denied")

    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    failed = await wait_for_state(manager, created.id, SessionState.FAILED)

    assert failed.failure is not None
    # The round's own words for what it was doing, kept and kept first.
    assert failed.failure.startswith("The Managed Sync baseline could not be verified. ")
    # Then who refused, that waiting will not help, and who can fix it.
    assert GRANT_REFUSAL_HEADLINE in failed.failure
    # Then Databricks' own sentence, intact. This is the assertion the defect
    # would have failed and an exception-type assertion would not.
    assert verbatim in failed.failure
    # Readable on a screen an audience may see: one line, no frames.
    assert "\n" not in failed.failure
    assert "Traceback" not in failed.failure

    # And the round-select screen is told, so the round stops being offered.
    assert set(manager.grant_refusals) == {RoundId.PUT_MODEL_SCORE_IN_APP}
    assert verbatim in manager.grant_refusals[RoundId.PUT_MODEL_SCORE_IN_APP]

    # The grant is added; the round arms; the memory of the refusal goes with it.
    engine.arm_error = None
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)

    assert manager.grant_refusals == {}


@pytest.mark.parametrize("first_outcome", ["false", "exception"])
async def test_initial_terminal_release_retries_same_fence_until_confirmed(
    first_outcome: str,
) -> None:
    lease_store = RetryTerminalReleaseStore(
        phase="run_committed",
        first_outcome=first_outcome,
    )
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    manager._terminal_release_backoff_cap = 0.01
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-retry-initial")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)

    await asyncio.wait_for(lease_store.retry_started.wait(), timeout=1)
    record = manager._records[created.id]
    assert record.snapshot.state == SessionState.RUNNING
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()
    assert all(event.event != "run_finished" for event in record.event_log.events)

    lease_store.allow_success.set()
    finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    assert finished.remembered_result == ("LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A")
    assert lease_store.release_calls == 2
    assert record.lease_heartbeat_task is None
    assert [event.event for event in record.event_log.events].count("run_finished") == 1


@pytest.mark.parametrize("current_state", ["different", "none"])
async def test_initial_terminal_release_maps_confirmed_loss_without_storing_result(
    current_state: str,
) -> None:
    lease_store = LoseTerminalLeaseStore(
        phase="run_committed",
        current_state=current_state,
    )
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-loss-initial")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)

    failed = await wait_for_state(manager, created.id, SessionState.FAILED)
    record = manager._records[created.id]
    assert failed.failure == ("Managed Sync proof lease was lost before terminal verification.")
    assert record.model_score_result is None
    assert record.lease_heartbeat_task is None
    assert [event.event for event in record.event_log.events].count("session_failed") == 1


@pytest.mark.parametrize("phase", ["run_committed", "redo_committed"])
async def test_slow_terminal_coordinator_io_keeps_lease_renewable_and_lock_free(
    phase: str,
) -> None:
    lease_store = SlowRenewableTerminalStore(phase=phase)
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    manager._lease_heartbeat = 0.01
    manager._active_lease_ttl = 0.03
    manager._running_lease_ttl = 0.03
    manager._terminal_release_call_timeout = 0.5
    manager._terminal_release_backoff_cap = 0.01
    operator = BoutOperator(display_name="Round Four Owner", subject=f"owner-slow-{phase}")

    if phase == "run_committed":
        created = await manager.create(round_four_request())
        await manager.start_arm(created.id, operator)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id, operator)
    else:
        created, _ = await verified_round_four(manager, operator)
        await manager.start_redo(created.id, operator)
    record = manager._records[created.id]

    async def take_lease_lock() -> None:
        async with record.lease_lock:
            return None

    await asyncio.wait_for(lease_store.release_started.wait(), timeout=1)
    await asyncio.wait_for(take_lease_lock(), timeout=0.05)
    await asyncio.wait_for(lease_store.current_started.wait(), timeout=1)
    await asyncio.wait_for(take_lease_lock(), timeout=0.05)

    if phase == "run_committed":
        finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)
        assert finished.failure is None
        terminal_event = "run_finished"
    else:
        finished = await wait_for_redo(manager, created.id, RedoState.VERIFIED)
        assert finished.state == SessionState.VERIFIED
        terminal_event = "redo_finished"
    assert lease_store.release_calls == 2
    assert record.lease_heartbeat_task is None
    assert [event.event for event in record.event_log.events].count(terminal_event) == 1


@pytest.mark.parametrize("phase", ["run_committed", "redo_committed"])
async def test_heartbeat_defers_loss_when_release_clears_row_before_true_response(
    phase: str,
) -> None:
    lease_store = ClearBeforeReleaseReturnsStore(phase=phase)
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    manager._lease_heartbeat = 0.01
    manager._active_lease_ttl = 0.03
    manager._running_lease_ttl = 0.03
    manager._terminal_release_call_timeout = 0.5
    operator = BoutOperator(display_name="Round Four Owner", subject=f"owner-clear-{phase}")
    if phase == "run_committed":
        created = await manager.create(round_four_request())
        await manager.start_arm(created.id, operator)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id, operator)
    else:
        created, _ = await verified_round_four(manager, operator)
        await manager.start_redo(created.id, operator)
    record = manager._records[created.id]
    await asyncio.wait_for(lease_store.row_cleared.wait(), timeout=1)

    if phase == "run_committed":
        finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)
        terminal_event = "run_finished"
        forbidden_event = "session_failed"
    else:
        finished = await wait_for_redo(manager, created.id, RedoState.VERIFIED)
        assert finished.state == SessionState.VERIFIED
        terminal_event = "redo_finished"
        forbidden_event = "redo_failed"
    for _ in range(100):
        if record.model_score_terminal_task is None:
            break
        await asyncio.sleep(0.005)
    events = [event.event for event in record.event_log.events]
    assert events.count(terminal_event) == 1
    assert forbidden_event not in events
    assert record.lease_heartbeat_task is None
    assert record.lease_heartbeat_lease is None
    assert record.model_score_terminal_task is None
    assert record.model_score_terminal_lease is None


@pytest.mark.parametrize("phase", ["run_committed", "redo_committed"])
async def test_terminalizer_never_adopts_or_releases_a_newer_local_fence(
    phase: str,
) -> None:
    lease_store = DelayedOldFenceReleaseStore(phase=phase)
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    operator = BoutOperator(display_name="Round Four Owner", subject=f"owner-fence-{phase}")
    if phase == "run_committed":
        created = await manager.create(round_four_request())
        await manager.start_arm(created.id, operator)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id, operator)
        original_lane = None
    else:
        created, verified = await verified_round_four(manager, operator)
        original_lane = verified.lanes["lakebase"].model_dump(mode="json")
        await manager.start_redo(created.id, operator)
    record = manager._records[created.id]
    await asyncio.wait_for(lease_store.release_started.wait(), timeout=1)
    async with record.lease_lock:
        old_lease = record.lease
        assert old_lease is not None
        new_lease = replace(
            old_lease,
            lease_id=f"{old_lease.lease_id}-new",
            fencing_token=old_lease.fencing_token + 1,
            updated_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(seconds=1),
        )
        record.lease = new_lease
        lease_store._lease = new_lease
    manager._start_lease_heartbeat(record, timedelta(seconds=1))
    lease_store.allow_response.set()

    if phase == "run_committed":
        failed = await wait_for_state(manager, created.id, SessionState.FAILED)
        assert failed.failure == ("Managed Sync proof lease was lost before terminal verification.")
        terminal_event = "session_failed"
    else:
        failed = await wait_for_redo(manager, created.id, RedoState.FAILED)
        assert failed.state == SessionState.VERIFIED
        assert failed.redo is not None
        assert failed.redo.failure == "Managed Sync re-do lease was lost"
        assert failed.lanes["lakebase"].model_dump(mode="json") == original_lane
        terminal_event = "redo_failed"
    for _ in range(100):
        if record.model_score_terminal_task is None:
            break
        await asyncio.sleep(0.005)
    assert lease_store.release_fences == [old_lease.fencing_token]
    assert record.lease is new_lease
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()
    assert record.lease_heartbeat_lease is new_lease
    assert record.model_score_terminal_task is None
    assert record.model_score_terminal_lease is None
    assert [event.event for event in record.event_log.events].count(terminal_event) == 1
    manager._cancel_lease_heartbeat(record)
    lease_store._lease = None


async def test_initial_terminal_cancellation_waits_for_release_and_publication() -> None:
    lease_store = RetryTerminalReleaseStore(phase="run_committed")
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    manager._terminal_release_backoff_cap = 0.01
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-cancel-initial")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(lease_store.retry_started.wait(), timeout=1)
    record = manager._records[created.id]
    outer = record.task
    child = record.model_score_terminal_task
    assert outer is not None and child is not None

    outer.cancel()
    await asyncio.sleep(0.01)
    assert not outer.done()
    assert not child.done()
    assert record.snapshot.state == SessionState.RUNNING
    lease_store.allow_success.set()

    finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    for _ in range(100):
        if outer.done():
            break
        await asyncio.sleep(0.005)
    assert finished.failure is None
    assert outer.cancelled()
    assert record.model_score_terminal_task is None
    assert record.lease_heartbeat_task is None
    assert [event.event for event in record.event_log.events].count("run_finished") == 1


# Round 4's terminal publication used to answer a cancellation by calling
# uncancel() and re-shielding, so an unreachable coordinator held the run open
# forever.  These four hang against that version: the wait is the thing under
# test, so each one wedges the coordinator the way an unreachable endpoint does
# rather than merely making it slow.
TERMINAL_BOUND = 0.1
# Generous next to TERMINAL_BOUND, unreachable if the wait is unbounded.
TERMINAL_PATIENCE = 3.0


async def _unreachable_round_four(
    lease_store: UnreachableTerminalReleaseStore,
    subject: str,
) -> tuple[RunManager, object]:
    """A Round 4 bout parked in the terminal settlement of an unreachable ring."""

    manager = RunManager(
        model_score_factory=lambda: FakeModelScoreEngine(),
        lease_store=lease_store,
    )
    # The heartbeat would otherwise decide the lease's fate before the wait does.
    manager._lease_heartbeat = 10
    manager._terminal_release_backoff_cap = 0.01
    manager._terminal_publish_timeout = TERMINAL_BOUND
    # Comfortably past the caller's bound, so the bound is what ends the wait.
    manager._terminal_settle_deadline = 1.0
    operator = BoutOperator(display_name="Round Four Owner", subject=subject)
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, operator)
    await asyncio.wait_for(lease_store.attempted.wait(), timeout=1)
    return manager, created


async def test_an_unreachable_coordinator_cannot_swallow_the_round_four_cancellation() -> None:
    """Hangs without the bound: the wait uncancels itself and re-shields forever."""

    lease_store = UnreachableTerminalReleaseStore(phase="run_committed")
    manager, created = await _unreachable_round_four(lease_store, "owner-unreachable-initial")
    record = manager._records[created.id]
    outer = record.task
    child = record.model_score_terminal_task
    assert outer is not None and child is not None

    started = time.monotonic()
    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(outer), timeout=TERMINAL_PATIENCE)
    elapsed = time.monotonic() - started

    assert outer.cancelled()
    assert elapsed < TERMINAL_PATIENCE
    # Abandoned, not cancelled: the publication owns the lease and the proof row.
    assert not child.cancelled()
    # And it is not left running for the life of the process either.
    await asyncio.wait_for(asyncio.shield(child), timeout=TERMINAL_PATIENCE)
    assert record.model_score_terminal_task is None
    # An unprovable release is not a lease loss, so the proof still publishes.
    assert (await manager.get(created.id)).state == SessionState.VERIFIED


async def test_the_abandoned_terminal_publication_names_what_may_have_survived(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The whole reason not to wait longer: someone has to be able to find these."""

    lease_store = UnreachableTerminalReleaseStore(phase="run_committed")
    caplog.set_level(logging.ERROR, logger="server.safe_change")
    manager, created = await _unreachable_round_four(lease_store, "owner-unreachable-orphan")
    record = manager._records[created.id]
    outer = record.task
    child = record.model_score_terminal_task
    assert outer is not None and child is not None
    lease = record.lease
    update = record.model_score_pending_update
    assert lease is not None and update is not None

    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(outer), timeout=TERMINAL_PATIENCE)
    await asyncio.wait_for(asyncio.shield(child), timeout=TERMINAL_PATIENCE)

    orphan = next(
        record_.getMessage() for record_ in caplog.records if "ORPHAN RISK" in record_.getMessage()
    )
    assert created.id in orphan
    # The ring lease, held to its durable TTL because release was never proved.
    assert lease.lease_id in orphan
    assert str(lease.fencing_token) in orphan
    # The non-baseline owned row, in both the sealed source and the synced table.
    assert update.entity_id in orphan
    assert update.proof_nonce in orphan
    assert "main.anti_demo.model_scores" in orphan
    assert "public.model_scores" in orphan
    # The bound is what is being reported, not a generic failure.
    assert f"{TERMINAL_BOUND:.1f}s" in orphan


async def test_the_abandoned_proof_row_is_a_shape_the_baseline_re_seed_recognises() -> None:
    """Characterization, and the crux of what the orphan log may claim.

    An abandoned publication cannot leave behind a shape the re-seed does not
    know. The only row it can ever commit is the update minted before the bell,
    and that is minted as one of ``OWNED_PROOF_SHAPES`` exactly -- so both the
    next arm and the `round4_managed_sync` gate recognise it without consulting
    any process state.
    """

    lease_store = UnreachableTerminalReleaseStore(phase="run_committed")
    manager, created = await _unreachable_round_four(lease_store, "owner-abandoned-shape")
    record = manager._records[created.id]
    outer = record.task
    child = record.model_score_terminal_task
    assert outer is not None and child is not None
    update = record.model_score_pending_update
    assert update is not None

    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(outer), timeout=TERMINAL_PATIENCE)
    await asyncio.wait_for(asyncio.shield(child), timeout=TERMINAL_PATIENCE)

    assert is_owned_prior_proof(update.row)
    # And it is not the baseline, so the re-seed has something to do rather than
    # short-circuiting on an already-exact row.
    assert update.row != record.model_score_engine.contract.baseline


async def test_the_abandoned_terminal_publication_does_not_dispatch_a_hand_repair(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Naming the row is diagnosis; instructing hand repair would be wrong.

    The row is demo-owned residue by shape, so the next arm re-seeds the sealed
    baseline on its own. Sending an operator to edit a live demo table for that
    invites surgery nobody needed and teaches them to distrust the automation.
    """

    lease_store = UnreachableTerminalReleaseStore(phase="run_committed")
    caplog.set_level(logging.ERROR, logger="server.safe_change")
    manager, created = await _unreachable_round_four(lease_store, "owner-no-hand-repair")
    record = manager._records[created.id]
    outer = record.task
    child = record.model_score_terminal_task
    assert outer is not None and child is not None
    update = record.model_score_pending_update
    assert update is not None

    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(outer), timeout=TERMINAL_PATIENCE)
    await asyncio.wait_for(asyncio.shield(child), timeout=TERMINAL_PATIENCE)

    orphan = next(
        record_.getMessage() for record_ in caplog.records if "ORPHAN RISK" in record_.getMessage()
    )
    # Still fully findable.
    assert update.proof_nonce in orphan
    assert "main.anti_demo.model_scores" in orphan
    # But no longer a work order.
    assert "by hand" not in orphan
    assert "manual" not in orphan.casefold()
    # Round 4 abandons a lease and a Delta row; neither is a resource to hunt.
    assert "verify the resource is gone" not in orphan
    assert "re-seeds the sealed baseline" in orphan


async def test_the_terminal_lease_settlement_gives_up_rather_than_retrying_forever() -> None:
    """Hangs without its own deadline: bounding the caller does not stop this loop."""

    lease_store = UnreachableTerminalReleaseStore(phase="no-such-phase")
    manager = RunManager(
        model_score_factory=lambda: FakeModelScoreEngine(),
        lease_store=lease_store,
    )
    manager._lease_heartbeat = 10
    manager._terminal_release_backoff_cap = 0.01
    manager._terminal_settle_deadline = TERMINAL_BOUND
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-settle-deadline")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]
    expected = record.lease
    assert expected is not None
    async with record.lease_lock:
        record.model_score_terminal_lease = expected
    lease_store.phase = expected.phase

    started = time.monotonic()
    outcome = await asyncio.wait_for(
        manager._settle_model_score_terminal_lease(record),
        timeout=TERMINAL_PATIENCE,
    )
    elapsed = time.monotonic() - started

    assert outcome == "unknown"
    assert elapsed < TERMINAL_PATIENCE
    # It really did keep retrying a swallowed failure before giving up.
    assert lease_store.release_attempts > 1
    assert lease_store.current_attempts > 1


async def test_the_abandoned_terminal_settlement_leaves_the_durable_lease_to_its_ttl() -> None:
    """The designed outcome, deliberately preserved: not released, left to expire.

    An unknown settlement must not be talked into a release.  ``close()`` states
    that a lease whose settlement cannot be *proved* is left for the TTL so that
    startup reconciliation runs before the next bout, and the deadline added
    here is a way of reaching that state sooner, not a way around it.
    """

    lease_store = UnreachableTerminalReleaseStore(phase="no-such-phase")
    manager = RunManager(
        model_score_factory=lambda: FakeModelScoreEngine(),
        lease_store=lease_store,
    )
    manager._lease_heartbeat = 10
    manager._terminal_release_backoff_cap = 0.01
    manager._terminal_settle_deadline = TERMINAL_BOUND
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-settle-ttl")
    created = await manager.create(round_four_request())
    await manager.start_arm(created.id, operator)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    record = manager._records[created.id]
    expected = record.lease
    assert expected is not None
    async with record.lease_lock:
        record.model_score_terminal_lease = expected
    lease_store.phase = expected.phase

    outcome = await asyncio.wait_for(
        manager._settle_model_score_terminal_lease(record),
        timeout=TERMINAL_PATIENCE,
    )

    assert outcome == "unknown"
    # Not released, and not declared lost either.
    assert record.lease is expected
    assert lease_store._lease is not None
    assert manager._same_exact_lease(lease_store._lease, expected)
    # Stopped renewing, which is the only way the durable TTL can settle it.
    assert record.lease_heartbeat_task is None


async def test_round_four_redo_retains_identity_and_preserves_initial_evidence() -> None:
    engine = FakeModelScoreEngine()
    manager = RunManager(model_score_factory=lambda: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-redo")
    created, verified = await verified_round_four(manager, operator)
    initial_evidence = dict(verified.lanes["lakebase"].evidence)
    initial_metrics = [item.model_dump() for item in verified.metrics]

    started = await manager.start_redo(created.id, operator)
    assert started.state == SessionState.RUNNING
    assert started.redo is not None and started.redo.state == RedoState.RUNNING
    finished = await wait_for_redo(manager, created.id, RedoState.VERIFIED)

    assert engine.redo_calls == 1
    assert engine.redo_received_result is engine.initial_result
    assert finished.state == SessionState.VERIFIED
    assert finished.lanes["lakebase"].evidence == initial_evidence
    assert [item.model_dump() for item in finished.metrics] == initial_metrics
    assert finished.redo is not None
    redo_evidence = finished.redo.lanes["lakebase"].evidence
    assert redo_evidence["primary_key"] == initial_evidence["primary_key"]
    assert redo_evidence["score"] == 0.33
    assert redo_evidence["model_version"] == "risk-v2"
    assert redo_evidence["proof_nonce"].startswith("round4-v2-")
    assert redo_evidence["proof_nonce"] != initial_evidence["proof_nonce"]
    assert redo_evidence["delta_version"] == 12
    assert finished.redo.comparison is not None
    assert finished.redo.comparison.kind == ComparisonKind.CAPABILITY_GAP
    assert finished.redo.comparison.winner_lane_id == "lakebase"
    assert finished.redo.comparison.margin is None
    assert finished.redo.comparison.detail == (
        "Lakebase verified the scoped native Synced Tables capability; no AWS-native "
        "equivalent lane was timed. This is not a speed comparison."
    )


async def test_concurrent_and_terminal_redo_posts_are_idempotent() -> None:
    engine = FakeModelScoreEngine()
    engine.allow_redo.clear()
    manager = RunManager(model_score_factory=lambda: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-concurrent")
    created, _ = await verified_round_four(manager, operator)

    first = await manager.start_redo(created.id, operator)
    second = await manager.start_redo(created.id, operator)
    await asyncio.wait_for(engine.redo_entered.wait(), timeout=1)

    assert first.redo is not None and first.redo.state == RedoState.RUNNING
    assert second.redo is not None and second.redo.state == RedoState.RUNNING
    assert engine.redo_calls == 1
    events = manager._records[created.id].event_log.events
    assert [event.event for event in events].count("redo_started") == 1

    engine.allow_redo.set()
    await wait_for_redo(manager, created.id, RedoState.VERIFIED)
    event_count = len(events)
    replay = await manager.start_redo(created.id, operator)
    assert replay.redo is not None and replay.redo.state == RedoState.VERIFIED
    assert engine.redo_calls == 1
    assert len(events) == event_count


async def test_redo_idempotent_refresh_bypasses_disappeared_readiness() -> None:
    engine = FakeModelScoreEngine()
    engine.allow_redo.clear()

    def factory():
        return engine

    manager = RunManager(model_score_factory=factory)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-readiness")
    created, _ = await verified_round_four(manager, operator)

    manager._model_score_factory = None
    with pytest.raises(InvalidStateError, match="live adapter is not configured"):
        await manager.start_redo(created.id, operator)
    ready = await manager.get(created.id)
    assert ready.redo is not None and ready.redo.state == RedoState.READY

    manager._model_score_factory = factory
    await manager.start_redo(created.id, operator)
    await asyncio.wait_for(engine.redo_entered.wait(), timeout=1)

    def no_longer_ready() -> None:
        raise InvalidStateError("dynamic readiness disappeared")

    manager._model_score_factory = None
    manager._readiness_check = no_longer_ready
    running = await manager.start_redo(created.id, operator)
    assert running.redo is not None and running.redo.state == RedoState.RUNNING
    engine.allow_redo.set()
    await wait_for_redo(manager, created.id, RedoState.VERIFIED)
    verified = await manager.start_redo(created.id, operator)
    assert verified.redo is not None and verified.redo.state == RedoState.VERIFIED
    assert engine.redo_calls == 1


async def test_redo_lease_claim_failure_leaves_ready_without_dispatch() -> None:
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=RejectRedoClaimStore(),
    )
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-claim")
    created, _ = await verified_round_four(manager, operator)

    with pytest.raises(InvalidStateError, match="redo lease unavailable"):
        await manager.start_redo(created.id, operator)

    snapshot = await manager.get(created.id)
    assert snapshot.state == SessionState.VERIFIED
    assert snapshot.redo is not None and snapshot.redo.state == RedoState.READY
    assert engine.redo_calls == 0
    assert all(
        event.event != "redo_started" for event in manager._records[created.id].event_log.events
    )


async def test_redo_failure_is_terminal_and_preserves_initial_proof() -> None:
    engine = FakeModelScoreEngine()
    engine.fail_redo = True
    manager = RunManager(model_score_factory=lambda: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-redo-fail")
    created, verified = await verified_round_four(manager, operator)
    initial_evidence = dict(verified.lanes["lakebase"].evidence)
    initial_result = manager._records[created.id].model_score_result
    await manager.start_redo(created.id, operator)

    failed = await wait_for_redo(manager, created.id, RedoState.FAILED)

    assert failed.state == SessionState.VERIFIED
    assert failed.lanes["lakebase"].evidence == initial_evidence
    assert failed.remembered_result == ("LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A")
    assert failed.comparison is not None
    assert failed.comparison.winner_lane_id == "lakebase"
    assert manager._records[created.id].model_score_result is initial_result
    assert engine.redo_calls == 1
    event_count = len(manager._records[created.id].event_log.events)
    manager._model_score_factory = None

    def no_longer_ready() -> None:
        raise InvalidStateError("dynamic readiness disappeared")

    manager._readiness_check = no_longer_ready
    replay = await manager.start_redo(created.id, operator)
    assert replay.redo is not None and replay.redo.state == RedoState.FAILED
    assert engine.redo_calls == 1
    assert len(manager._records[created.id].event_log.events) == event_count


async def test_redo_terminal_state_and_event_wait_for_release() -> None:
    lease_store = BlockingRedoReleaseStore()
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-release")
    created, _ = await verified_round_four(manager, operator)
    await manager.start_redo(created.id, operator)

    await asyncio.wait_for(lease_store.release_started.wait(), timeout=1)
    pending = manager._records[created.id].snapshot
    assert pending.state == SessionState.RUNNING
    assert pending.redo is not None and pending.redo.state == RedoState.RUNNING
    assert all(
        event.event != "redo_finished" for event in manager._records[created.id].event_log.events
    )

    lease_store.allow_release.set()
    finished = await wait_for_redo(manager, created.id, RedoState.VERIFIED)
    assert finished.state == SessionState.VERIFIED
    assert any(
        event.event == "redo_finished" for event in manager._records[created.id].event_log.events
    )


async def test_redo_terminal_release_retries_same_fence_until_confirmed() -> None:
    lease_store = RetryTerminalReleaseStore(phase="redo_committed")
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    manager._terminal_release_backoff_cap = 0.01
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-retry-redo")
    created, _ = await verified_round_four(manager, operator)
    await manager.start_redo(created.id, operator)

    await asyncio.wait_for(lease_store.retry_started.wait(), timeout=1)
    record = manager._records[created.id]
    assert record.snapshot.state == SessionState.RUNNING
    assert record.snapshot.redo is not None
    assert record.snapshot.redo.state == RedoState.RUNNING
    assert record.lease_heartbeat_task is not None
    assert not record.lease_heartbeat_task.done()
    assert all(event.event != "redo_finished" for event in record.event_log.events)

    lease_store.allow_success.set()
    finished = await wait_for_redo(manager, created.id, RedoState.VERIFIED)
    assert finished.state == SessionState.VERIFIED
    assert lease_store.release_calls == 2
    assert record.lease_heartbeat_task is None
    assert [event.event for event in record.event_log.events].count("redo_finished") == 1


@pytest.mark.parametrize("current_state", ["different", "none"])
async def test_redo_terminal_release_loss_preserves_byte_equivalent_v1_proof(
    current_state: str,
) -> None:
    lease_store = LoseTerminalLeaseStore(
        phase="redo_committed",
        current_state=current_state,
    )
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-loss-redo")
    created, verified = await verified_round_four(manager, operator)
    original_evidence = verified.lanes["lakebase"].model_dump(mode="json")
    original_metrics = [item.model_dump(mode="json") for item in verified.metrics]
    original_remembered = verified.remembered_result
    await manager.start_redo(created.id, operator)

    failed = await wait_for_redo(manager, created.id, RedoState.FAILED)
    record = manager._records[created.id]
    assert failed.state == SessionState.VERIFIED
    assert failed.failure is None
    assert failed.lanes["lakebase"].model_dump(mode="json") == original_evidence
    assert [item.model_dump(mode="json") for item in failed.metrics] == original_metrics
    assert failed.remembered_result == original_remembered
    assert failed.redo is not None
    assert failed.redo.failure == "Managed Sync re-do lease was lost"
    assert record.model_score_pending_update is None
    assert record.lease_heartbeat_task is None
    assert [event.event for event in record.event_log.events].count("redo_failed") == 1


async def test_redo_terminal_cancellation_waits_for_confirmed_loss() -> None:
    lease_store = RetryTerminalReleaseStore(phase="redo_committed")
    engine = FakeModelScoreEngine()
    manager = RunManager(
        model_score_factory=lambda: engine,
        lease_store=lease_store,
    )
    manager._lease_heartbeat = 10
    manager._terminal_release_backoff_cap = 0.01
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-cancel-redo")
    created, verified = await verified_round_four(manager, operator)
    original_lane = verified.lanes["lakebase"].model_dump(mode="json")
    original_metrics = [item.model_dump(mode="json") for item in verified.metrics]
    await manager.start_redo(created.id, operator)
    await asyncio.wait_for(lease_store.retry_started.wait(), timeout=1)
    record = manager._records[created.id]
    outer = record.task
    child = record.model_score_terminal_task
    assert outer is not None and child is not None

    outer.cancel()
    await asyncio.sleep(0.01)
    assert not outer.done()
    assert not child.done()
    lease_store._lease = None
    lease_store.allow_success.set()

    failed = await wait_for_redo(manager, created.id, RedoState.FAILED)
    for _ in range(100):
        if outer.done():
            break
        await asyncio.sleep(0.005)
    assert failed.state == SessionState.VERIFIED
    assert failed.lanes["lakebase"].model_dump(mode="json") == original_lane
    assert [item.model_dump(mode="json") for item in failed.metrics] == original_metrics
    assert failed.redo is not None
    assert failed.redo.failure == "Managed Sync re-do lease was lost"
    assert outer.cancelled()
    assert record.model_score_terminal_task is None
    assert record.lease_heartbeat_task is None
    assert [event.event for event in record.event_log.events].count("redo_failed") == 1
    refresh = await manager.start_redo(created.id, operator)
    assert refresh.redo is not None and refresh.redo.state == RedoState.FAILED


async def test_lost_redo_lease_preserves_initial_verified_proof() -> None:
    engine = FakeModelScoreEngine()
    engine.allow_redo.clear()
    manager = RunManager(model_score_factory=lambda: engine)
    operator = BoutOperator(display_name="Round Four Owner", subject="owner-lost")
    created, verified = await verified_round_four(manager, operator)
    initial_evidence = dict(verified.lanes["lakebase"].evidence)
    await manager.start_redo(created.id, operator)
    await asyncio.wait_for(engine.redo_entered.wait(), timeout=1)
    record = manager._records[created.id]
    assert record.lease is not None

    await manager._handle_lost_lease(record, record.lease)

    failed_redo = await manager.get(created.id)
    assert failed_redo.state == SessionState.VERIFIED
    assert failed_redo.remembered_result == ("LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A")
    assert failed_redo.lanes["lakebase"].evidence == initial_evidence
    assert failed_redo.redo is not None
    assert failed_redo.redo.state == RedoState.FAILED
    assert failed_redo.redo.failure == "Managed Sync re-do lease was lost"
    assert any(event.event == "redo_failed" for event in record.event_log.events)


async def test_round_four_redo_rejects_wrong_owner_state_round_and_cooldown() -> None:
    engine = FakeModelScoreEngine()
    manager = RunManager(model_score_factory=lambda: engine)
    owner = BoutOperator(display_name="Owner", subject="owner-auth")
    intruder = BoutOperator(display_name="Intruder", subject="other-auth")
    created = await manager.create(round_four_request())

    with pytest.raises(InvalidStateError, match="must verify"):
        await manager.start_redo(created.id, owner)
    await manager.start_arm(created.id, owner)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id, owner)
    await wait_for_state(manager, created.id, SessionState.VERIFIED)

    with pytest.raises(InvalidStateError, match="ONLY THE RING OWNER"):
        await manager.start_redo(created.id, intruder)
    with pytest.raises(InvalidStateError, match="Re-do behavior is not defined"):
        await manager.start_cooldown(created.id, owner)

    round_one = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    with pytest.raises(InvalidStateError, match="only in Round 4"):
        await manager.start_redo(round_one.id, owner)


async def test_manifest_readiness_gate_applies_to_create_arm_and_run() -> None:
    setup_status = "seeding"

    def require_ready() -> None:
        if setup_status != "ready":
            raise InvalidStateError(f"Demo setup is currently {setup_status.upper()}, not READY")

    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        readiness_check=require_ready,
    )
    request = SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="sre",
        corners=[Corner.PERFORMANCE],
    )

    with pytest.raises(InvalidStateError, match="SEEDING"):
        await manager.create(request)

    setup_status = "ready"
    created = await manager.create(request)
    setup_status = "waiting_for_zero"
    with pytest.raises(InvalidStateError, match="WAITING_FOR_ZERO"):
        await manager.start_arm(created.id)

    setup_status = "ready"
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    setup_status = "seeding"
    with pytest.raises(InvalidStateError, match="SEEDING"):
        await manager.start_run(created.id)

    setup_status = "ready"
    await manager.start_run(created.id)
    await wait_for_state(manager, created.id, SessionState.VERIFIED)
    setup_status = "waiting_for_zero"
    with pytest.raises(InvalidStateError, match="WAITING_FOR_ZERO"):
        await manager.start_cooldown(created.id)


async def test_manager_runs_the_full_honest_state_machine() -> None:
    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    request = SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="sre",
        secondary_personas=["executive"],
        corners=[Corner.COST, Corner.PERFORMANCE],
    )

    created = await manager.create(request)
    assert created.state == SessionState.DRAFT
    assert created.round.id == RoundId.WAKE_IDLE_APP
    assert created.presenter_pack.secondary[0].persona_id == "executive"
    assert created.corners == [Corner.COST, Corner.PERFORMANCE]
    assert created.presenter_pack.remembered_metric == (
        "Cost inputs and elapsed workflow time to the same verified outcome"
    )

    checking = await manager.start_arm(created.id)
    assert checking.state == SessionState.CHECKING
    armed = await wait_for_state(manager, created.id, SessionState.ARMED)
    assert all(lane.status == "Scale zero verified" for lane in armed.lanes.values())

    await manager.start_run(created.id)
    finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)

    assert finished.remembered_result is not None
    assert finished.remembered_result.startswith("LAKEBASE")
    assert finished.fairness.launch_skew_ms is not None
    assert finished.lanes["lakebase"].elapsed_ms < finished.lanes["competitor"].elapsed_ms
    assert all(
        lane.activity is not None
        and lane.activity.wire_call == "PostgreSQL TLS connect → INSERT → COMMIT → SELECT"
        for lane in finished.lanes.values()
    )
    assert finished.armed_at is not None
    assert finished.cooldown is not None
    assert finished.cooldown.mode == ResetMode.RETURN_TO_IDLE
    started = await manager.start_cooldown(created.id)
    assert started.cooldown is not None
    assert started.cooldown.mode == ResetMode.RETURN_TO_IDLE
    assert (
        started.cooldown.lanes["lakebase"].activity.wire_call == "databricks postgres get-endpoint"
    )
    assert (
        started.cooldown.lanes["competitor"].activity.wire_call
        == "RDS DescribeDBClusters + DescribeDBInstances + DescribeEvents → "
        "CloudWatch GetMetricStatistics fallback"
    )
    assert started.cooldown.lanes["lakebase"].started_at == (
        finished.lanes["lakebase"].connection_closed_at
    )
    assert started.cooldown.lanes["competitor"].started_at == (
        finished.lanes["competitor"].connection_closed_at
    )
    assert (
        started.cooldown.lanes["lakebase"].started_at
        < started.cooldown.lanes["competitor"].started_at
        <= started.cooldown.started_at
    )
    cooldown = await wait_for_cooldown(manager, created.id, CooldownState.READY)
    assert cooldown.lanes["lakebase"].elapsed_ms is not None
    assert cooldown.lanes["competitor"].elapsed_ms is not None
    assert all(
        lane.activity is not None and lane.activity.wire_call for lane in cooldown.lanes.values()
    )


async def test_round_one_owner_can_cancel_a_pending_arm_without_a_result() -> None:
    resolver = HangingArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    owner = BoutOperator(display_name="Owner", subject="owner")
    intruder = BoutOperator(display_name="Intruder", subject="intruder")
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )

    checking = await manager.start_arm(created.id, owner)
    assert checking.state == SessionState.CHECKING
    await asyncio.wait_for(resolver.lakebase.entered.wait(), timeout=1)
    await asyncio.wait_for(resolver.competitor.entered.wait(), timeout=1)

    with pytest.raises(InvalidStateError, match="ONLY THE RING OWNER"):
        await manager.cancel_arm(created.id, intruder)
    assert (await manager.bout_status()).active is True

    cancelled = await manager.cancel_arm(created.id, owner)
    assert cancelled.state == SessionState.FAILED
    assert cancelled.run_started_at is None
    assert cancelled.remembered_result is None
    assert cancelled.metrics == []
    assert cancelled.comparison is None
    assert cancelled.failure == (
        "Fight-card check cancelled by the ring owner. No run started and no result was recorded."
    )
    assert resolver.lakebase.cancelled.is_set()
    assert resolver.competitor.cancelled.is_set()
    assert manager._records[created.id].task is None
    assert (await manager.bout_status()).active is False
    assert any(
        event.event == "session_cancelled"
        for event in manager._records[created.id].event_log.events
    )

    repeated = await manager.cancel_arm(created.id, owner)
    assert repeated == cancelled


async def test_round_one_automatically_rechecks_idle_while_holding_cleanup_fence() -> None:
    resolver = SequencedCooldownResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    manager._arm_poll = 0.001
    first = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.COST, Corner.SIMPLICITY, Corner.PERFORMANCE],
        )
    )
    await manager.start_arm(
        first.id,
        BoutOperator(display_name="Demo Operator", email="operator@example.com"),
    )
    await wait_for_state(manager, first.id, SessionState.ARMED)
    await manager.start_run(first.id)
    await wait_for_state(manager, first.id, SessionState.VERIFIED)
    started = await manager.start_cooldown(first.id)
    assert started.cooldown is not None

    active = await manager.bout_status()
    assert active.active is True
    assert active.phase == "cooldown"
    assert active.can_start is False

    cooldown = await wait_for_cooldown(manager, first.id, CooldownState.READY)
    lakebase_origin = cooldown.lanes["lakebase"].started_at
    competitor_origin = cooldown.lanes["competitor"].started_at
    assert len(resolver.lakebase.cooldown_cutoffs) >= 4
    assert len(resolver.competitor.cooldown_cutoffs) >= 3
    assert set(resolver.lakebase.cooldown_cutoffs) == {lakebase_origin}
    assert set(resolver.competitor.cooldown_cutoffs) == {competitor_origin}
    # Restoring the old global-not-before predicate makes this fail: neither
    # target receives the later shared cooldown bookkeeping timestamp.
    assert lakebase_origin != cooldown.started_at
    assert competitor_origin != cooldown.started_at

    events = manager._records[first.id].event_log.events
    cooldown_states = [
        event.payload["cooldown"]["lanes"] for event in events if event.event == "cooldown_update"
    ]
    assert any(
        lanes["lakebase"]["state"] == "watching"
        and lanes["lakebase"]["observation_count"] == 1
        for lanes in cooldown_states
    )

    await asyncio.sleep(0)
    assert (await manager.bout_status()).active is False


async def test_lakebase_post_close_update_can_precede_shared_cooldown_origin() -> None:
    manager, record = await round_one_cooldown_record()
    cooldown = record.snapshot.cooldown
    assert cooldown is not None
    lane = cooldown.lanes["lakebase"]
    provider_updated_at = cooldown.started_at - timedelta(seconds=1.81)
    assert lane.started_at < provider_updated_at < cooldown.started_at
    completed_at = cooldown.started_at + timedelta(seconds=1)

    changed, snapshot = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {
            "state": "IDLE",
            "provider_updated_at": provider_updated_at.isoformat(),
        },
        completed_at,
    )

    observed = snapshot.lanes["lakebase"]
    assert changed is True
    assert observed.state == CooldownLaneState.CONFIRMED_ZERO
    assert observed.confirmed_at == completed_at
    assert observed.confirmation_basis == "provider_update_corroboration"
    assert observed.provider_updated_at == provider_updated_at
    assert observed.elapsed_ms == pytest.approx(
        (completed_at - lane.started_at).total_seconds() * 1000
    )


async def test_confirmed_idle_lane_latches_first_terminal_evidence() -> None:
    manager, record = await round_one_cooldown_record()
    cooldown = record.snapshot.cooldown
    assert cooldown is not None
    lane = cooldown.lanes["lakebase"]
    provider_updated_at = lane.started_at + timedelta(seconds=60)
    first_completed_at = lane.started_at + timedelta(seconds=75)

    changed, first = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {
            "state": "IDLE",
            "provider_updated_at": provider_updated_at.isoformat(),
        },
        first_completed_at,
    )
    assert changed is True
    frozen = first.lanes["lakebase"]
    assert frozen.state == CooldownLaneState.CONFIRMED_ZERO
    assert frozen.confirmed_at == first_completed_at
    assert frozen.elapsed_ms == pytest.approx(75_000)
    assert frozen.checked_at == first_completed_at

    changed, duplicate = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {
            "state": "IDLE",
            "provider_updated_at": provider_updated_at.isoformat(),
        },
        first_completed_at + timedelta(minutes=2),
    )
    assert changed is False
    assert duplicate.lanes["lakebase"] == frozen

    changed, stale_nonterminal = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        TargetNotArmedError("stale ACTIVE observation"),
        first_completed_at + timedelta(minutes=3),
    )
    assert changed is False
    assert stale_nonterminal.lanes["lakebase"] == frozen


async def test_lakebase_stale_update_requires_repeated_idle_dwell() -> None:
    manager, record = await round_one_cooldown_record()
    manager._lakebase_idle_dwell = 0.001
    cooldown = record.snapshot.cooldown
    assert cooldown is not None
    lane = cooldown.lanes["lakebase"]
    stale = lane.started_at - timedelta(minutes=10)

    _, first = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {"state": "IDLE", "provider_updated_at": stale.isoformat()},
        cooldown.started_at,
    )
    first_lane = first.lanes["lakebase"]
    assert first_lane.state == CooldownLaneState.WATCHING
    assert first_lane.observed_state == "IDLE"
    assert first_lane.observation_count == 1

    await asyncio.sleep(0.002)
    completed_at = cooldown.started_at + timedelta(seconds=2)
    _, second = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {"state": "IDLE", "provider_updated_at": stale.isoformat()},
        completed_at,
    )
    second_lane = second.lanes["lakebase"]
    assert second_lane.state == CooldownLaneState.CONFIRMED_ZERO
    assert second_lane.confirmation_basis == "observed_idle_dwell"
    assert second_lane.confirmed_at == completed_at


async def test_lakebase_missing_update_requires_repeated_idle_dwell() -> None:
    manager, record = await round_one_cooldown_record()
    manager._lakebase_idle_dwell = 0.001
    cooldown = record.snapshot.cooldown
    assert cooldown is not None

    _, first = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {"state": "IDLE"},
        cooldown.started_at,
    )
    assert first.lanes["lakebase"].state == CooldownLaneState.WATCHING

    await asyncio.sleep(0.002)
    _, second = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {"state": "IDLE"},
        cooldown.started_at + timedelta(seconds=2),
    )
    assert second.lanes["lakebase"].state == CooldownLaneState.CONFIRMED_ZERO
    assert second.lanes["lakebase"].provider_updated_at is None
    assert second.lanes["lakebase"].confirmation_basis == "observed_idle_dwell"


async def test_lakebase_idle_cannot_vacuously_pass_without_verified_lane_activity() -> None:
    manager, record = await round_one_cooldown_record(verified_activity=False)
    manager._lakebase_idle_dwell = 0.001
    cooldown = record.snapshot.cooldown
    assert cooldown is not None

    for offset in (0, 2):
        if offset:
            await asyncio.sleep(0.002)
        _, snapshot = await manager._apply_cooldown_observation(
            record,
            "lakebase",
            {"state": "IDLE"},
            cooldown.started_at + timedelta(seconds=offset),
        )

    lane = snapshot.lanes["lakebase"]
    assert lane.state == CooldownLaneState.WATCHING
    assert lane.confirmed_at is None
    assert "no verified lane transaction" in lane.status


async def test_lakebase_active_state_remains_waiting_and_resets_idle_dwell() -> None:
    manager, record = await round_one_cooldown_record()
    cooldown = record.snapshot.cooldown
    assert cooldown is not None

    _, first = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        {"state": "IDLE"},
        cooldown.started_at,
    )
    assert first.lanes["lakebase"].observation_count == 1

    _, active = await manager._apply_cooldown_observation(
        record,
        "lakebase",
        TargetNotArmedError("Lakebase endpoint is ACTIVE, not IDLE"),
        cooldown.started_at + timedelta(seconds=1),
    )
    lane = active.lanes["lakebase"]
    assert lane.state == CooldownLaneState.WATCHING
    assert lane.observation_count == 0
    assert lane.confirmed_at is None
    assert lane.status == "Lakebase endpoint is ACTIVE, not IDLE"


@pytest.mark.parametrize("transient_once", [False, True])
async def test_round_one_redo_uses_each_lane_evidence_time_and_retries_transients(
    transient_once: bool,
) -> None:
    resolver = EvidenceTimedCooldownResolver(transient_once=transient_once)
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    manager._arm_poll = 0.001
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await wait_for_state(manager, created.id, SessionState.VERIFIED)
    await manager.start_cooldown(created.id)
    cooldown = await wait_for_cooldown(manager, created.id, CooldownState.READY)

    assert cooldown.lanes["lakebase"].elapsed_ms is not None
    assert cooldown.lanes["lakebase"].elapsed_ms > 0
    assert cooldown.lanes["lakebase"].confirmation_basis == "observed_idle_dwell"
    assert cooldown.lanes["lakebase"].observation_count == 2
    assert cooldown.lanes["competitor"].elapsed_ms == pytest.approx(10)
    assert resolver.lakebase.arm_calls == (5 if transient_once else 4)


async def test_round_one_redo_bounds_a_hung_poll_and_fails_only_at_deadline() -> None:
    resolver = HangingCooldownResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    manager._arm_poll = 0.001
    manager._cooldown_poll_timeout = 0.005
    manager._cooldown_timeout = 0.025
    manager._active_lease_ttl = 0.02
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await wait_for_state(manager, created.id, SessionState.VERIFIED)
    await manager.start_cooldown(created.id)
    cooldown = await wait_for_cooldown(manager, created.id, CooldownState.FAILED)

    assert cooldown.lanes["lakebase"].state == CooldownLaneState.FAILED
    assert cooldown.lanes["competitor"].state == CooldownLaneState.CONFIRMED_ZERO
    assert cooldown.lanes["lakebase"].activity.wire_call == "databricks postgres get-endpoint"
    assert cooldown.failure == "Timed out waiting for return to confirmed zero."
    record = manager._records[created.id]
    assert record.lease is not None
    assert record.lease.phase == "cooldown_failed"
    assert record.lease_heartbeat_task is None
    assert record.lease_heartbeat_lease is None
    status = await manager.bout_status()
    assert status.active is True
    assert status.phase == "cooldown_failed"
    await asyncio.sleep(0.03)
    assert await manager._lease_store.current() is None


async def test_round_two_uses_isolated_change_engine_and_deletes_copies_on_redo() -> None:
    engine = FakeSafeChangeEngine()
    manager = RunManager(safe_change_factory=lambda: engine)
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        )
    )

    await manager.start_arm(created.id)
    armed = await wait_for_state(manager, created.id, SessionState.ARMED)
    assert all("Source clean" in lane.status for lane in armed.lanes.values())

    await manager.start_run(created.id)
    finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    assert finished.lanes["lakebase"].elapsed_ms == 25.0
    assert finished.lanes["competitor"].elapsed_ms == 75.0
    assert finished.remembered_result is not None

    resetting = await manager.start_cooldown(created.id)
    assert resetting.cooldown is not None
    assert resetting.cooldown.mode == ResetMode.DELETE_ISOLATED_ENVIRONMENT
    reset = await wait_for_cooldown(manager, created.id, CooldownState.READY)
    assert all(lane.state.value == "confirmed_deleted" for lane in reset.lanes.values())


async def test_failed_round_two_surfaces_lane_error_and_cleanup_can_be_retried(
    caplog,
) -> None:
    """A refused lane reaches the operator, the log, and nowhere new.

    Round 2 is the round that proves why the run-level handler is not enough. A
    refused lane does not raise out of `engine.run` -- the engine reports it as a
    lane result so one lane can fail without destroying the other's timing --
    so `except Exception` never fires and the refusal's own words existed in
    exactly two places that are both gone by the time anyone asks: the SSE
    stream, and the bout record.
    """

    engine = FakeSafeChangeEngine()
    engine.fail_run = True
    engine.reset_failures = 1
    manager = RunManager(safe_change_factory=lambda: engine)
    manager._cleanup_retry_initial = 0.001
    manager._cleanup_retry_max = 0.002
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    with caplog.at_level(logging.ERROR, logger="server.manager"):
        await manager.start_run(created.id)
        failed = await wait_for_state(manager, created.id, SessionState.FAILED)

    # Unchanged, and asserted before the log is: this is a logging addition, so
    # a passing log assertion beside a moved lane outcome would be a regression
    # wearing a green tick.
    assert failed.lanes["lakebase"].error == "isolated endpoint contract rejected"
    assert failed.failure == "One or more isolated schema changes could not be verified."
    assert failed.lanes["competitor"].state == LaneState.VERIFIED

    refusals = [
        record
        for record in caplog.records
        if record.name == "server.manager" and "lane refused" in record.getMessage()
    ]
    assert refusals, (
        "a mid-bout lane refusal produced no log record at all, which is the "
        "whole defect: the reason lived only on a screen that has moved on"
    )
    # Exactly one. The engine also emits a FAILED `on_progress` event carrying
    # the identical string, and logging there would double every refusal and
    # make a transient-and-recovered one indistinguishable from a fatal one.
    assert len(refusals) == 1, [record.getMessage() for record in refusals]
    line = refusals[0].getMessage()
    assert "lane=lakebase" in line
    assert "reason=isolated endpoint contract rejected" in line
    assert "round=make_schema_change_safely" in line
    assert created.id in line
    # The verified lane says nothing. A log that named both lanes on a
    # single-lane failure would tell an operator to go and look at a lane that
    # was fine.
    assert "lane=competitor" not in line

    retry = await wait_for_cooldown(manager, created.id, CooldownState.READY)
    assert all(lane.state == CooldownLaneState.CONFIRMED_DELETED for lane in retry.lanes.values())


#: The settlement funnel each round's lane refusal passes through, and the round
#: number the log line has to carry. Written down because the lesson of the fix
#: before this one is that a curated subset is exactly what lets a second gate
#: stay hidden: Round 2's refusal is proved end-to-end on a real descriptor in
#: `tests/test_operator_log_visibility.py`, and the other five are the same
#: one-line call in the analogous position with nothing behavioural pinning them.
_LANE_SETTLEMENT_FUNNELS = {
    "_finish": 1,
    "_finish_safe_change": 2,
    "_finish_recovery": 3,
    "_finish_model_score_failure": 4,
    "_finish_connection_spike": 5,
    "_finish_live_orders_failure": 6,
}


def test_every_round_settles_a_refused_lane_into_the_log() -> None:
    """Each round's settlement funnel keeps its refusal log, with its own number.

    WHAT THIS CATCHES that a behavioural test here does not. The fakes in this
    module drive Rounds 1 and 3 into failure by *raising*, which lands on the
    run-level handler; neither reaches the settlement funnel with a lane result
    that says `FAILED`. So `_finish` and `_finish_recovery` could lose their call
    entirely and every other test in this file would still pass -- and Round 3 is
    the round that starts an AWS restore, which is the one an operator most needs
    to reconstruct afterwards.

    WHAT THIS DOES NOT CATCH, said plainly rather than implied: a seventh round
    added with a settlement funnel nobody adds to the dict above. This asserts
    the six that exist keep their call and carry the right round number; it
    cannot assert a name it has never been told.
    """

    tree = ast.parse(pathlib.Path("server/manager.py").read_text(encoding="utf-8"))
    manager = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "RunManager"
    )
    methods = {
        node.name: node
        for node in manager.body
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef)
    }

    missing = sorted(set(_LANE_SETTLEMENT_FUNNELS) - set(methods))
    assert not missing, (
        f"a settlement funnel was renamed or removed: {missing}. Rename it here too, "
        "or the guard silently stops covering that round."
    )

    for name, round_number in sorted(_LANE_SETTLEMENT_FUNNELS.items()):
        rounds = [
            keyword.value.value
            for call in ast.walk(methods[name])
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "_log_lane_refusals"
            for keyword in call.keywords
            if keyword.arg == "round_number" and isinstance(keyword.value, ast.Constant)
        ]
        assert rounds == [round_number], (
            f"`{name}` settles a refused lane for Round {round_number} but logs "
            f"{rounds or 'nothing'}. A refusal that reaches only the SSE stream and "
            "the bout record is gone by the time anyone asks why the lane failed."
        )


async def test_a_verified_round_two_bout_says_nothing_to_the_log(caplog) -> None:
    """Nothing that is quiet today becomes loud.

    The guard against retry noise is structural rather than a level choice: the
    refusal is logged at settlement, and a lane that was refused once and then
    recovered is `VERIFIED` at settlement and has nothing to say. If the call
    ever migrates into `on_progress`, where `progress.error` also flows past,
    this fails -- which is the point, because that is where a 5-second retry
    loop would turn one transient into a screenful.
    """

    manager = RunManager(safe_change_factory=FakeSafeChangeEngine)
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    with caplog.at_level(logging.WARNING, logger="server.manager"):
        await manager.start_run(created.id)
        verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)

    assert verified.failure is None
    noise = [record.getMessage() for record in caplog.records if record.name == "server.manager"]
    assert noise == [], f"a bout that verified cleanly wrote to the log anyway: {noise}"


async def test_round_two_receipt_precedes_fenced_automatic_cleanup_release() -> None:
    lease_store = BlockingReleaseStore()
    manager = RunManager(
        safe_change_factory=FakeSafeChangeEngine,
        lease_store=lease_store,
    )
    request = SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
    )
    first = await manager.create(request)
    challenger = await manager.create(request)

    await manager.start_arm(first.id)
    await wait_for_state(manager, first.id, SessionState.ARMED)
    await manager.start_run(first.id)
    await asyncio.wait_for(lease_store.release_started.wait(), timeout=1)

    record = manager._records[first.id]
    run_task = record.task
    assert run_task is not None
    terminal = await manager.get(first.id)
    resetting = await manager.start_cooldown(first.id)

    assert terminal.state == SessionState.VERIFIED
    assert resetting.cooldown is not None
    assert any(event.event == "run_finished" for event in record.event_log.events)
    assert lease_store.release_completed.is_set() is False
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager.start_arm(challenger.id)

    lease_store.allow_release.set()
    await run_task
    cleanup_task = record.cooldown_task
    if cleanup_task is not None:
        await cleanup_task
    assert lease_store.release_completed.is_set()
    ready = await wait_for_cooldown(manager, first.id, CooldownState.READY)
    assert ready.state == CooldownState.READY


async def test_failed_round_three_receipt_precedes_fenced_automatic_cleanup() -> None:
    lease_store = BlockingReleaseStore()
    manager = RunManager(
        recovery_factory=FailingRecoveryEngine,
        lease_store=lease_store,
    )
    request = SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.RECOVER_DELETED_ORDER,
    )
    first = await manager.create(request)
    challenger = await manager.create(request)

    await manager.start_arm(first.id)
    await wait_for_state(manager, first.id, SessionState.ARMED)
    await manager.start_run(first.id)
    await asyncio.wait_for(lease_store.release_started.wait(), timeout=1)

    record = manager._records[first.id]
    run_task = record.task
    assert run_task is not None
    terminal = await manager.get(first.id)
    resetting = await manager.start_cooldown(first.id)

    assert terminal.state == SessionState.FAILED
    assert resetting.cooldown is not None
    assert resetting.cooldown.mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT
    assert any(event.event == "session_failed" for event in record.event_log.events)
    assert lease_store.release_completed.is_set() is False
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager.start_arm(challenger.id)

    lease_store.allow_release.set()
    await run_task
    cleanup_task = record.cooldown_task
    if cleanup_task is not None:
        await cleanup_task
    assert lease_store.release_completed.is_set()
    ready = await wait_for_cooldown(manager, first.id, CooldownState.READY)
    assert ready.state == CooldownState.READY


async def test_round_three_towel_freezes_cutoff_and_keeps_one_fenced_lease() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine(competitor_phase=RecoveryPhase.RESTORING)
    engine.allow_lakebase.clear()
    engine.allow_reset.clear()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    request = SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.RECOVER_DELETED_ORDER,
    )
    first = await manager.create(request)
    challenger = await manager.create(request)
    owner = BoutOperator(
        display_name="Demo Operator",
        email="operator@example.com",
        subject="workspace-user-123",
    )

    await manager.start_arm(first.id, owner)
    await wait_for_state(manager, first.id, SessionState.ARMED)
    await manager.start_run(first.id, owner)
    await wait_for_state(manager, first.id, SessionState.RUNNING)
    lease_before = lease_store._lease
    assert lease_before is not None and lease_before.phase == "run_committed"
    with pytest.raises(InvalidStateError, match="ONLY THE RING OWNER"):
        await manager.start_towel(
            first.id,
            BoutOperator(
                display_name="Another User",
                email="another@databricks.com",
                subject="workspace-user-456",
            ),
        )

    engine.allow_lakebase.set()
    await asyncio.wait_for(engine.towel_ready.wait(), timeout=1)
    accepted = await manager.start_towel(first.id, owner)
    assert accepted.state == SessionState.TOWELLED
    assert accepted.towel is not None
    assert accepted.towel.cutoff_ms == 90_005.678901
    assert accepted.towel.censored_lower_bounds_ms == {"competitor": 90_005.678901}
    assert accepted.lanes["competitor"].state == LaneState.TOWELLED
    assert accepted.lanes["competitor"].elapsed_ms is None
    lease_after = lease_store._lease
    assert lease_after is not None
    assert lease_after.lease_id == lease_before.lease_id
    assert lease_after.fencing_token == lease_before.fencing_token
    assert lease_after.phase == "towel_cleanup"
    assert lease_store._generation == 1

    await asyncio.wait_for(engine.reset_started.wait(), timeout=1)
    duplicate = await manager.start_towel(first.id, owner)
    assert duplicate.towel is not None
    assert duplicate.towel.requested_at == accepted.towel.requested_at
    assert duplicate.towel.lower_bound_ms == accepted.towel.lower_bound_ms
    assert duplicate.cooldown is not None
    assert (
        duplicate.cooldown.lanes["competitor"].status
        == "AWS RESTORE ALREADY IN MOTION · SAFE CLEANUP MAY TAKE MINUTES"
    )
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager.start_arm(challenger.id)

    engine.allow_reset.set()
    finished = await wait_for_towel(manager, first.id, "ready")
    assert finished.remembered_result is not None
    assert "lakebase exact proof preserved" in finished.remembered_result
    assert "Margin N/A" in finished.remembered_result
    assert finished.lanes["lakebase"].state == LaneState.VERIFIED
    assert finished.lanes["lakebase"].elapsed_ms == 14_380.0
    assert finished.lanes["competitor"].state == LaneState.TOWELLED
    assert finished.lanes["competitor"].status != "Late verification must be ignored"
    assert await lease_store.current() is None


async def test_towel_terminal_state_requires_confirmed_durable_release() -> None:
    lease_store = FailOnceReleaseStore()
    engine = TowelRecoveryEngine()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.RECOVER_DELETED_ORDER,
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.towel_ready.wait(), timeout=1)
    await manager.start_towel(created.id)
    deferred = await wait_for_towel(manager, created.id, "failed")

    record = manager._records[created.id]
    assert deferred.state == SessionState.TOWELLED
    assert deferred.towel is not None
    assert "release could not be confirmed" in deferred.towel.cleanup_failure
    assert record.lease is not None and record.lease.phase == "towel_cleanup"
    assert record.lease_heartbeat_task is not None
    assert all(event.event != "towel_finished" for event in record.event_log.events)
    # No `towel_finished` is correct -- the towel has not finished -- but it must
    # not cost the bout its receipt. The failure branch publishes `towel_update`,
    # and sealing only on event names meant every towel that ended badly left
    # nothing durable behind, on any round.
    deferred_receipt = await sealed_receipt(created.id)
    assert deferred_receipt is not None, "a towel with failed cleanup sealed no receipt"
    assert deferred_receipt.outcome == "stopped_short"
    assert deferred_receipt.cleanup_failure is not None
    assert "release could not be confirmed" in deferred_receipt.cleanup_failure

    await manager.start_towel(created.id)
    finished = await wait_for_towel(manager, created.id, "ready")
    assert finished.towel is not None and finished.towel.cleanup_failure is None
    assert lease_store.release_calls == 2
    assert await lease_store.current() is None
    settled_receipt = await sealed_receipt(
        created.id,
        until=lambda item: item.cleanup_failure is None,
    )
    assert settled_receipt is not None and settled_receipt.cleanup_failure is None


async def test_towel_uses_frozen_public_lane_state_not_unpublished_engine_state() -> None:
    engine = TowelRecoveryEngine(competitor_phase=RecoveryPhase.VERIFYING_SOURCE)
    manager = RunManager(
        recovery_factory=lambda: engine,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.RECOVER_DELETED_ORDER,
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.towel_ready.wait(), timeout=1)
    record = manager._records[created.id]
    stop = record.recovery_stop_control
    assert stop is not None
    stop.completed_lanes["competitor"] = RecoveryLaneResult(
        lane_id="competitor",
        name="RDS PostgreSQL",
        provider=SafeChangeProvider.RDS,
        elapsed_ms=89_500.0,
        first_action_ns=10_002_000_000,
        completed_ns=99_500_000_000,
        artifact_id="recovery-competitor",
        ok=True,
    )
    stop.terminal_lanes.add("competitor")

    snapshot = await manager.start_towel(created.id)
    assert snapshot.towel is not None
    assert snapshot.lanes["competitor"].state == LaneState.TOWELLED
    assert snapshot.lanes["competitor"].elapsed_ms is None
    assert record.task is not None
    await asyncio.gather(record.task, return_exceptions=True)


async def test_towel_wins_the_race_when_the_competitor_completes_during_the_transition() -> None:
    """The towel beats a natural finish that lands while the lease is in flight.

    Nothing is reverted here, and nothing should be: the transition succeeds.
    What is asserted is that a competitor lane completing mid-transition does not
    get to write its result over the towel -- the finisher sees a towel snapshot
    and bails, so the lane stays `towelled`.
    """

    lease_store = BlockingTowelTransitionStore()
    engine = TransitionRaceRecoveryEngine()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.RECOVER_DELETED_ORDER,
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.towel_ready.wait(), timeout=1)
    towel = asyncio.create_task(manager.start_towel(created.id))
    await asyncio.wait_for(lease_store.transition_started.wait(), timeout=1)

    engine.complete_competitor.set()
    await asyncio.wait_for(engine.competitor_completed.wait(), timeout=1)
    lease_store.allow_transition.set()

    accepted = await towel

    record = manager._records[created.id]
    assert record.recovery_stop_control is not None
    assert record.recovery_stop_control.event.is_set() is True
    assert lease_store.phases[-1] == "towel_cleanup"
    assert accepted.towel is not None
    assert accepted.lanes["competitor"].state == LaneState.TOWELLED
    finished = await wait_for_towel(manager, created.id, "ready")
    assert finished.lanes["competitor"].state == LaneState.TOWELLED
    cleanup_task = record.cooldown_task
    if cleanup_task is not None:
        await cleanup_task
    assert await lease_store.current() is None


async def test_towel_wins_the_race_when_the_competitor_fails_during_the_transition() -> None:
    """The same race, with the competitor lane failing instead of verifying.

    Again no revert: a lane *failure* arriving mid-transition must not be written
    over the towel either, so the public lane state stays `towelled` and the
    bout is still recorded as abandoned rather than lost.
    """

    lease_store = BlockingTowelTransitionStore()
    engine = TransitionRaceRecoveryEngine(competitor_ok=False)
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.RECOVER_DELETED_ORDER,
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.towel_ready.wait(), timeout=1)
    towel = asyncio.create_task(manager.start_towel(created.id))
    await asyncio.wait_for(lease_store.transition_started.wait(), timeout=1)

    engine.complete_competitor.set()
    await asyncio.wait_for(engine.competitor_completed.wait(), timeout=1)
    lease_store.allow_transition.set()

    accepted = await towel

    record = manager._records[created.id]
    assert record.recovery_stop_control is not None
    assert record.recovery_stop_control.event.is_set() is True
    assert lease_store.phases[-1] == "towel_cleanup"
    assert accepted.towel is not None
    finished = await wait_for_towel(manager, created.id, "ready")
    assert finished.lanes["competitor"].state == LaneState.TOWELLED
    assert any(event.event == "towel_started" for event in record.event_log.events)
    cleanup_task = record.cooldown_task
    if cleanup_task is not None:
        await cleanup_task
    assert await lease_store.current() is None


async def test_round_three_towel_cleanup_failure_retries_under_retained_lease() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine(reset_failures=1)
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    request = SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.RECOVER_DELETED_ORDER,
    )
    first = await manager.create(request)
    challenger = await manager.create(request)

    await manager.start_arm(first.id)
    await wait_for_state(manager, first.id, SessionState.ARMED)
    await manager.start_run(first.id)
    await asyncio.wait_for(engine.towel_ready.wait(), timeout=1)
    await manager.start_towel(first.id)
    failed = await wait_for_towel(manager, first.id, "failed")
    cleanup_task = manager._records[first.id].cooldown_task
    assert cleanup_task is not None
    await cleanup_task

    assert failed.state == SessionState.TOWELLED
    assert failed.towel is not None and failed.towel.cleanup_failure is not None
    retained = lease_store._lease
    assert retained is not None and retained.phase == "towel_cleanup"
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager.start_arm(challenger.id)

    await manager.start_towel(first.id)
    finished = await wait_for_towel(manager, first.id, "ready")
    assert finished.towel is not None and finished.towel.cleanup_failure is None
    assert engine.timeline == ["settle", "reset", "settle", "reset"]
    assert lease_store._generation == 1
    assert await lease_store.current() is None


async def test_completed_round_two_cleanup_is_idempotent_and_consumes_no_new_fence() -> None:
    lease_store = InMemoryBoutLeaseStore()
    manager = RunManager(
        safe_change_factory=FakeSafeChangeEngine,
        lease_store=lease_store,
    )
    request = SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
    )
    first = await manager.create(request)
    second = await manager.create(request)

    for created in (first, second):
        await manager.start_arm(created.id)
        await wait_for_state(manager, created.id, SessionState.ARMED)
        await manager.start_run(created.id)
        await wait_for_state(manager, created.id, SessionState.VERIFIED)
        await wait_for_cooldown(manager, created.id, CooldownState.READY)

    assert lease_store._generation == 2
    first_again = await manager.start_cooldown(first.id)
    assert first_again.cooldown is not None
    assert first_again.cooldown.state == CooldownState.READY
    assert lease_store._generation == 2

    second_again = await manager.start_cooldown(second.id)
    assert second_again.cooldown is not None
    assert second_again.cooldown.state == CooldownState.READY
    assert lease_store._generation == 2


async def test_only_one_session_can_own_the_ring_at_a_time() -> None:
    manager = RunManager(resolver=SequencedCooldownResolver(), verifier=make_verifier())
    manager._arm_poll = 0.001
    request = SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="sre",
        corners=[Corner.PERFORMANCE],
    )
    first = await manager.create(request)
    second = await manager.create(request)

    await manager.start_arm(
        first.id,
        BoutOperator(display_name="Demo Operator", email="operator@example.com"),
    )
    await wait_for_state(manager, first.id, SessionState.ARMED)
    active = await manager.bout_status()
    assert active.active is True
    assert active.operator is not None
    assert active.operator.display_name == "Demo Operator"
    assert active.round_title == "Wake this idle app"
    assert active.competitor == "Aurora Serverless v2"
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager.start_arm(
            second.id,
            BoutOperator(display_name="Waiting operator"),
        )

    await manager.start_run(first.id)
    await wait_for_state(manager, first.id, SessionState.VERIFIED)
    cleanup = await manager.bout_status()
    assert cleanup.active is True
    assert cleanup.phase == "cooldown"
    await drain_record_operations(manager, manager._records[first.id])
    assert (await manager.bout_status()).active is False
    checking = await manager.start_arm(second.id)

    assert checking.state == SessionState.CHECKING


async def test_v7_round_leases_serialize_only_the_same_installation_and_round() -> None:
    base = InMemoryBoutLeaseStore()
    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=base,
        round_isolation=True,
        installation_id="install-a",
    )
    owner = BoutOperator(display_name="Owner", subject="owner")

    async def draft(round_id: RoundId):
        created = await manager.create(
            SessionCreate(
                competitor=CompetitorId.AURORA_SERVERLESS_V2,
                primary_persona="sre",
                corners=[Corner.PERFORMANCE],
                round_id=round_id,
            )
        )
        return manager._records[created.id]

    round_one = await draft(RoundId.WAKE_IDLE_APP)
    same_round = await draft(RoundId.WAKE_IDLE_APP)
    round_three = await draft(RoundId.RECOVER_DELETED_ORDER)

    await manager._claim_bout(round_one, owner)
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await manager._claim_bout(same_round, owner)
    await manager._claim_bout(round_three, owner)

    assert (await manager.bout_status(RoundId.WAKE_IDLE_APP)).active is True
    assert (await manager.bout_status(RoundId.RECOVER_DELETED_ORDER)).active is True
    assert (await manager.bout_status(RoundId.PUT_MODEL_SCORE_IN_APP)).active is False
    # Asking without a round used to read the unused installation-wide ring and
    # answer "idle" with two rounds mid-bout. There is no honest answer to give.
    with pytest.raises(AmbiguousRingQueryError, match="ROUND REQUIRED"):
        await manager.bout_status()

    another_install = RunManager(
        lease_store=base,
        round_isolation=True,
        installation_id="install-b",
    )
    other = await another_install.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )
    await another_install._claim_bout(another_install._records[other.id], owner)

    await manager._release_bout(round_one)
    await manager._release_bout(round_three)
    await another_install._release_bout(another_install._records[other.id])


async def test_the_unscoped_refusal_costs_no_coordination_query() -> None:
    """Why this refuses rather than aggregating across rounds.

    Aggregating would mean one coordination read per round on every call, on an
    endpoint the browser polls, and the aggregate could not answer the question
    anyone actually has -- can *this* round start. Refusing costs nothing and
    names the parameter that gets a real answer.
    """
    manager = RunManager(round_isolation=True, installation_id="install-a")

    async def forbidden() -> None:
        raise AssertionError("the refusal must not read the ring")

    manager._lease_store.current = forbidden  # type: ignore[method-assign]

    with pytest.raises(AmbiguousRingQueryError) as refusal:
        await manager.bout_status()

    message = str(refusal.value)
    assert "round_id" in message
    assert "/api/health" in message
    for round_id in RoundId:
        assert round_id.value in message


async def test_a_pre_v7_installation_still_answers_the_unscoped_question() -> None:
    """One ring, one bout: the unscoped answer is the true one, so it stands."""
    manager = RunManager(round_isolation=False)

    status = await manager.bout_status()

    assert status.scope == "global"
    assert status.active is False
    assert status.can_start is True


async def test_v7_round_five_status_includes_its_lingering_cleanup_only() -> None:
    manager = RunManager(round_isolation=True, installation_id="install-a")
    owner = BoutOperator(display_name="Owner", subject="owner")
    cleanup_store = manager._round5_cleanup_store()
    cleanup = await cleanup_store.claim(
        session_id="cleanup-session",
        operator=owner,
        phase="round5_cleanup",
        session_state=SessionState.VERIFIED,
        round_id=RoundId.SURVIVE_CONNECTION_SPIKE.value,
        round_title="Survive the connection spike",
        competitor_id=CompetitorId.AURORA_SERVERLESS_V2.value,
        competitor_name="Aurora Serverless v2",
        ttl=timedelta(minutes=1),
    )

    round_five = await manager.bout_status(RoundId.SURVIVE_CONNECTION_SPIKE)
    round_one = await manager.bout_status(RoundId.WAKE_IDLE_APP)

    assert round_five.active is True
    assert round_five.can_start is False
    assert round_five.phase == "round5_cleanup"
    assert round_one.active is False
    assert round_one.can_start is True
    assert await cleanup_store.release(cleanup) is True


async def test_separate_app_replicas_share_one_visible_ring_lease() -> None:
    lease_store = InMemoryBoutLeaseStore()
    first_replica = RunManager(
        resolver=SequencedCooldownResolver(),
        verifier=make_verifier(),
        lease_store=lease_store,
    )
    first_replica._arm_poll = 0.001
    second_replica = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=lease_store,
    )
    request = SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="sre",
        corners=[Corner.PERFORMANCE],
    )
    first = await first_replica.create(request)
    second = await second_replica.create(request)
    owner = BoutOperator(
        display_name="Demo Operator",
        email="operator@example.com",
        subject="workspace-user-123",
    )

    await first_replica.start_arm(first.id, owner)
    await wait_for_state(first_replica, first.id, SessionState.ARMED)
    with pytest.raises(InvalidStateError, match="BOUT IN PROGRESS"):
        await second_replica.start_arm(
            second.id,
            BoutOperator(
                display_name="Another User",
                email="another@databricks.com",
                subject="workspace-user-456",
            ),
        )

    visible_from_other_replica = await second_replica.bout_status()
    assert visible_from_other_replica.active is True
    assert visible_from_other_replica.operator is not None
    assert visible_from_other_replica.operator.email == "operator@example.com"
    assert visible_from_other_replica.state == SessionState.ARMED
    assert visible_from_other_replica.phase == "armed"
    public_payload = visible_from_other_replica.model_dump(mode="json")
    assert "lease_id" not in str(public_payload)
    assert "fencing_token" not in str(public_payload)
    assert "subject" not in str(public_payload)

    await first_replica.start_run(first.id, owner)
    await wait_for_state(first_replica, first.id, SessionState.VERIFIED)
    cleanup_from_other_replica = await second_replica.bout_status()
    assert cleanup_from_other_replica.active is True
    assert cleanup_from_other_replica.phase == "cooldown"
    await drain_record_operations(first_replica, first_replica._records[first.id])
    assert (await second_replica.bout_status()).active is False


async def test_round_three_executes_and_resets_owned_recovery_environments() -> None:
    class FakeRecoveryEngine:
        async def arm(self, competitor, on_progress):
            lanes = {}
            for lane_id, name in (("lakebase", "Lakebase"), ("competitor", "RDS PostgreSQL")):
                await on_progress(
                    RecoveryProgress(
                        lane_id=lane_id,
                        lane_name=name,
                        phase=RecoveryPhase.PREPARING_INCIDENT,
                        status="Committing and aging the exact incident row",
                        occurred_at=datetime.now(UTC),
                    )
                )
                lanes[lane_id] = SimpleNamespace(
                    evidence={"exact_incident_committed": True},
                )
            return SimpleNamespace(competitor=competitor, lanes=lanes)

        async def run(self, arm, on_progress, on_started, stop_control=None):
            await on_started()
            lanes = {}
            for lane_id, name, elapsed in (
                ("lakebase", "Lakebase", 40.0),
                ("competitor", "RDS PostgreSQL", 80.0),
            ):
                recovery_at = datetime.now(UTC).replace(microsecond=0)
                await on_progress(
                    RecoveryProgress(
                        lane_id=lane_id,
                        lane_name=name,
                        phase=RecoveryPhase.WAITING_RECOVERY_POINT,
                        status="Waiting for the recovery point to become eligible",
                        occurred_at=datetime.now(UTC),
                        elapsed_ms=1.0,
                        recovery_at=recovery_at,
                    )
                )
                await on_progress(
                    RecoveryProgress(
                        lane_id=lane_id,
                        lane_name=name,
                        phase=RecoveryPhase.VERIFIED,
                        status="Exact recovered order verified; source deletion preserved",
                        occurred_at=datetime.now(UTC),
                        elapsed_ms=elapsed,
                        recovery_at=recovery_at,
                    )
                )
                lanes[lane_id] = SimpleNamespace(
                    ok=True,
                    elapsed_ms=elapsed,
                    recovery_at=recovery_at,
                    error=None,
                )
            return SimpleNamespace(launch_skew_ms=0.01, lanes=lanes, all_verified=True)

        async def reset(self, competitor, on_progress):
            lanes = {}
            for lane_id, name, provider in (
                ("lakebase", "Lakebase", SafeChangeProvider.LAKEBASE),
                ("competitor", "RDS PostgreSQL", SafeChangeProvider.RDS),
            ):
                await on_progress(
                    RecoveryProgress(
                        lane_id=lane_id,
                        lane_name=name,
                        phase=RecoveryPhase.RESET,
                        status="Owned recovery environment and synthetic order cleared",
                        occurred_at=datetime.now(UTC),
                    )
                )
                lanes[lane_id] = SafeChangeResetLaneResult(
                    lane_id=lane_id,
                    name=name,
                    provider=provider,
                    artifact_id=f"recovery-{lane_id}",
                    ok=True,
                )
            return SafeChangeResetResult(competitor=competitor, lanes=lanes)

    manager = RunManager(recovery_factory=FakeRecoveryEngine)
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="software_engineer",
            secondary_personas=[],
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.RECOVER_DELETED_ORDER,
        )
    )

    assert created.round.id == RoundId.RECOVER_DELETED_ORDER
    await manager.start_arm(created.id)
    armed = await wait_for_state(manager, created.id, SessionState.ARMED)
    armed_status = (
        "Exact incident committed · No recovery artifact exists · "
        "Recovery eligibility not pre-waited"
    )
    assert all(lane.status == armed_status for lane in armed.lanes.values())
    await manager.start_run(created.id)
    finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    assert finished.lanes["lakebase"].elapsed_ms == 40.0
    assert finished.lanes["competitor"].elapsed_ms == 80.0
    assert finished.run_started_at is not None
    with pytest.raises(InvalidStateError, match="must be running"):
        await manager.start_towel(created.id)
    events = manager._records[created.id].event_log.events
    run_started = next(index for index, event in enumerate(events) if event.event == "run_started")
    started_session = events[run_started].payload["session"]
    assert all(
        lane["activity"]["wire_call"] == "PostgreSQL DELETE + clock_timestamp() → COMMIT"
        for lane in started_session["lanes"].values()
    )
    eligibility = next(
        index
        for index, event in enumerate(events)
        if event.event == "lane_update"
        and event.payload["activity"]["phase"] == "waiting_recovery_point"
    )
    assert run_started < eligibility

    resetting = await manager.start_cooldown(created.id)
    assert resetting.cooldown is not None
    assert resetting.cooldown.mode == ResetMode.DELETE_RECOVERY_ENVIRONMENT
    reset = await wait_for_cooldown(manager, created.id, CooldownState.READY)
    assert all(lane.state == CooldownLaneState.CONFIRMED_DELETED for lane in reset.lanes.values())


async def test_rds_round_is_won_only_after_lakebase_verifies() -> None:
    resolver = FakeRdsResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="software_engineer",
            corners=[Corner.SIMPLICITY],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )

    assert created.round.id == RoundId.WAKE_IDLE_APP
    await manager.start_arm(created.id)
    armed = await wait_for_state(manager, created.id, SessionState.ARMED)

    assert armed.lanes["lakebase"].state == LaneState.SEALED
    assert armed.lanes["competitor"].state == LaneState.NOT_SUPPORTED
    assert armed.lanes["competitor"].elapsed_ms is None
    assert "No automatic scale-to-zero" in armed.lanes["competitor"].status

    await manager.start_run(created.id)
    finished = await wait_for_state(manager, created.id, SessionState.VERIFIED)

    assert finished.remembered_result == "LAKEBASE WINS — RDS CANNOT ENTER THE ROUND"
    assert finished.lanes["lakebase"].state == LaneState.VERIFIED
    assert finished.lanes["lakebase"].elapsed_ms is not None
    assert finished.lanes["competitor"].state == LaneState.NOT_SUPPORTED
    assert finished.lanes["competitor"].elapsed_ms is None
    assert finished.lanes["competitor"].attempts == 0
    assert resolver.competitor.prepare_calls == 0

    resetting = await manager.start_cooldown(created.id)
    assert resetting.cooldown is not None
    assert resetting.cooldown.mode == ResetMode.RETURN_TO_IDLE
    assert resetting.cooldown.lanes["competitor"].activity.wire_call == "RDS DescribeDBInstances"
    cooldown = await wait_for_cooldown(manager, created.id, CooldownState.READY)
    assert cooldown.lanes["lakebase"].state.value == "confirmed_zero"
    assert cooldown.lanes["competitor"].state.value == "not_supported"
    assert cooldown.lanes["competitor"].activity.wire_call == "RDS DescribeDBInstances"


async def test_rds_round_declares_no_winner_when_lakebase_does_not_verify() -> None:
    resolver = FakeRdsResolver(lakebase_fatal=True)
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    finished = await wait_for_state(manager, created.id, SessionState.FAILED)

    assert finished.remembered_result is None
    assert finished.failure is not None
    assert "No winner was declared" in finished.failure
    assert finished.lanes["lakebase"].state == LaneState.FAILED
    assert finished.lanes["competitor"].state == LaneState.NOT_SUPPORTED
    assert finished.lanes["competitor"].elapsed_ms is None
    assert resolver.competitor.prepare_calls == 0


async def test_unexpected_arm_exception_fails_immediately_instead_of_polling() -> None:
    """Fail fast, and say what failed rather than only that something did.

    The second half is the part that used to be missing everywhere. This handler
    ended in a bare `except Exception` that dropped the exception and returned a
    fixed sentence, which is the shape that cost an evening on Rounds 4 and 6 --
    so the sentence is still here, and now it is followed by what caused it.

    `RuntimeError("boom")` is chosen for what it proves about the *other* side
    of that: a builtin's message is nobody's to quote, so the type survives and
    the wording does not. Nothing on this path is a Databricks refusal, so the
    grant record stays empty too -- a round refused for an ordinary fault must
    not be struck off the round-select screen.
    """

    resolver = UnexpectedArmResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )

    await manager.start_arm(created.id)
    failed = await wait_for_state(manager, created.id, SessionState.FAILED)

    assert failed.failure == "The start state could not be verified. Diagnosis: RuntimeError"
    assert manager.grant_refusals == {}
    assert resolver.broken.arm_calls == 1
    assert resolver.waiting.arm_calls == 1


async def test_start_state_is_revalidated_immediately_before_timing() -> None:
    resolver = ChangingStartResolver()
    manager = RunManager(resolver=resolver, verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )

    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    failed = await wait_for_state(manager, created.id, SessionState.FAILED)

    assert failed.failure is not None
    assert "Start state changed before the bell" in failed.failure
    assert resolver.lakebase.arm_calls == 2
    assert failed.lanes["lakebase"].elapsed_ms is None


async def test_expired_armed_evidence_requires_a_fresh_arm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTI_DEMO_ARM_TTL_SECONDS", "0.05")
    lease_store = BlockingReleaseStore()
    manager = RunManager(
        resolver=FakeResolver(),
        verifier=make_verifier(),
        lease_store=lease_store,
    )
    request = SessionCreate(
        competitor=CompetitorId.AURORA_SERVERLESS_V2,
        primary_persona="sre",
        corners=[Corner.PERFORMANCE],
    )
    created = await manager.create(request)

    await manager.start_arm(created.id)
    armed = await wait_for_state(manager, created.id, SessionState.ARMED)
    assert armed.armed_expires_at is not None
    # The window the operator actually gets between the arm and the bell is the
    # whole TTL, measured from the moment arming *finished*. Arming's own cost
    # must not come out of it: Round 4's arm takes ~29 s inspecting a live
    # pipeline, and if that were billed to the window it would leave half the
    # pacing budget of a round that arms in nine. This is the one assertion that
    # pins that, so the "arm consumes the window" reading cannot come back.
    assert armed.armed_at is not None
    assert (armed.armed_expires_at - armed.armed_at).total_seconds() == pytest.approx(0.05)
    record = manager._records[created.id]
    expiry_task = record.armed_expiry_task
    assert expiry_task is not None
    await asyncio.wait_for(lease_store.release_started.wait(), timeout=1)

    terminal_read = asyncio.create_task(manager.get(created.id))
    await asyncio.sleep(0)
    assert not terminal_read.done()
    assert all(event.event != "session_failed" for event in record.event_log.events)

    lease_store.allow_release.set()
    failed = await terminal_read
    await expiry_task

    assert failed.failure == (
        "Fight card expired before the bell. The ring was released automatically; prepare it again."
    )
    assert lease_store.release_completed.is_set()
    assert any(event.event == "session_failed" for event in record.event_log.events)
    assert (await manager.bout_status()).active is False


async def test_only_the_sso_owner_can_ring_an_armed_bout() -> None:
    manager = RunManager(resolver=FakeResolver(), verifier=make_verifier())
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
        )
    )
    owner = BoutOperator(
        display_name="Demo Operator",
        email="operator@example.com",
    )
    await manager.start_arm(created.id, owner)
    await wait_for_state(manager, created.id, SessionState.ARMED)

    with pytest.raises(InvalidStateError, match="ONLY THE RING OWNER"):
        await manager.start_run(
            created.id,
            BoutOperator(display_name="Another User", email="another@databricks.com"),
        )

    active = await manager.bout_status()
    assert active.active is True
    assert active.phase == "armed"
    assert active.expires_at is not None

    await manager.start_run(created.id, owner)
    await wait_for_state(manager, created.id, SessionState.VERIFIED)


def round_three_request() -> SessionCreate:
    return SessionCreate(
        competitor=CompetitorId.RDS_POSTGRES,
        primary_persona="software_engineer",
        corners=[Corner.SIMPLICITY],
        round_id=RoundId.RECOVER_DELETED_ORDER,
    )


async def running_round_three(manager: RunManager, engine: TowelRecoveryEngine):
    created = await manager.create(round_three_request())
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.towel_ready.wait(), timeout=1)
    return created


# T1 · a cost-ledger write failure must not be able to stop the towel.
async def test_towel_still_schedules_cleanup_when_the_cost_window_will_not_close() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine()
    ledger = FailingCostLedgerStore(failures=1)
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await running_round_three(manager, engine)
    record = manager._records[created.id]
    manager._cost_ledger_store = ledger
    record.cost_bout_id = "bout-under-test"
    record.cost_bout_started_at = datetime.now(UTC)
    record.cost_bout_kind = "run"

    accepted = await manager.start_towel(created.id)

    assert accepted.state == SessionState.TOWELLED
    assert accepted.towel is not None and accepted.towel.cost_close_failure is not None
    assert record.cooldown_task is not None
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert engine.timeline == ["settle", "reset"]
    # The ring was released and the record can be forgotten: an open cost window
    # pinned by a failed close is the memory leak this reaches through the towel.
    assert await lease_store.current() is None
    assert record.cost_bout_id is None
    assert ledger.close_calls >= 2
    assert manager._releasable(record) is True


# T1 · a towel stuck at `stopping` must be recoverable by throwing it again.
async def test_towel_retry_re_enters_cleanup_for_a_towel_stranded_at_stopping() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await running_round_three(manager, engine)

    await manager.start_towel(created.id)
    record = manager._records[created.id]
    # Strand the towel exactly where a failure between the durable state change
    # and `create_task` leaves it: `stopping`, no cleanup task, lease held.
    stranded = record.cooldown_task
    assert stranded is not None and not stranded.done()
    stranded.cancel()
    await asyncio.gather(stranded, return_exceptions=True)
    record.cooldown_task = None
    async with record.lock:
        assert record.snapshot.towel is not None
        record.snapshot.towel.state = TowelState.STOPPING
        record.snapshot.towel.cleanup_failure = None
    assert await lease_store.current() is not None

    retried = await manager.start_towel(created.id)

    assert retried.towel is not None and retried.towel.state == TowelState.CLEANING
    assert record.cooldown_task is not None
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert await lease_store.current() is None


# T10 · the emergency stop is not gated on the readiness gate.
async def test_readiness_gate_refuses_the_arm_but_never_the_towel() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await running_round_three(manager, engine)
    challenger = await manager.create(round_three_request())

    def under_maintenance() -> None:
        raise InvalidStateError("BACKSTAGE CLEANUP IN PROGRESS · SHOWTIME WILL UNLOCK")

    manager._readiness_check = under_maintenance

    accepted = await manager.start_towel(created.id)

    assert accepted.state == SessionState.TOWELLED
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert await lease_store.current() is None
    # Requesting resources is still gated; only the stop is exempt.
    with pytest.raises(InvalidStateError, match="BACKSTAGE CLEANUP IN PROGRESS"):
        await manager.start_arm(challenger.id)


# T2 · Round 5 cleanup that will not converge must stop retrying and say so.
async def test_round_five_towel_cleanup_that_never_converges_ends_at_failed() -> None:
    engine = UnconvergingTowelConnectionSpikeEngine()
    main_store = InMemoryBoutLeaseStore()
    round5_store = InMemoryBoutLeaseStore(ring_key="round5")
    manager = RunManager(
        connection_spike_factory=lambda _competitor: engine,
        lease_store=main_store,
        round5_lease_store=round5_store,
    )
    # The backoff runs at its shipped defaults here, and only the waiting is
    # skipped. Shrinking the attempt count instead -- which this test used to do
    # -- tests that the loop terminates while saying nothing about *when*, and
    # that blind spot is how the default sat at roughly eight minutes while a
    # measured RDS Proxy deletion took 31.5. Cleanup abandoned five minutes
    # early, wrote `cleanup_failure` onto a healthy bout's receipt, and never
    # reached the security group behind the proxy at all.
    slept: list[float] = []

    async def record_sleep(delay: float) -> None:
        slept.append(delay)
        await asyncio.sleep(0)

    manager._cleanup_retry_sleep = record_sleep
    created = await manager.create(
        SessionCreate(
            competitor=CompetitorId.RDS_POSTGRES,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.SURVIVE_CONNECTION_SPIKE,
        )
    )
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    await asyncio.wait_for(engine.run_entered.wait(), timeout=1)

    await manager.start_towel(created.id)
    failed = await wait_for_towel(manager, created.id, "failed")

    assert engine.reconcile_attempts == manager._cleanup_retry_attempts
    # The budget, not the attempt count: a count is meaningless without the
    # backoff, and this is the sum the loop itself asked to wait for. The floor
    # is the one slow deletion actually measured against the deployed app --
    # 31.5 minutes from an accepted `DeleteDBProxy` to the proxy disappearing.
    # A budget under it puts a false cleanup failure on a healthy receipt.
    assert sum(slept) >= 31.5 * 60, (
        f"automatic cleanup gives up after {sum(slept) / 60:.1f} minutes, which is "
        "shorter than a measured RDS Proxy deletion"
    )
    assert failed.towel is not None
    assert "did not converge" in failed.towel.cleanup_failure
    record = manager._records[created.id]
    retry_task = record.connection_spike_cleanup_task
    assert retry_task is None or retry_task.done()
    # Keeping the ring is deliberate; being unable to leave `cleaning` was not.
    assert await main_store.current() is not None
    assert await round5_store.current() is not None

    # The worst bout to lose is this one. A towel whose cleanup failed is the
    # exact failure a verification campaign exists to hunt, and it used to seal
    # nothing -- the abandonment published `towel_update`, which was not a
    # sealing event, so the only trace was a log line and a held ring.
    abandoned = await sealed_receipt(created.id)
    assert abandoned is not None, "a towel with failed cleanup sealed no receipt"
    assert abandoned.outcome == "stopped_short"
    # And it must not read clean. This project has a recorded defect class where
    # a round reported success while its cleanup silently failed four times; a
    # receipt that omits the tidy-up is how a reader repeats that mistake.
    assert abandoned.cleanup_failure is not None
    assert "did not converge" in abandoned.cleanup_failure

    # And the state the operator is left in is one they can act on.
    engine.relent()
    await manager.start_towel(created.id)
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert await main_store.current() is None
    assert await round5_store.current() is None

    # The successful retry supersedes the record: cleanup did complete in the
    # end, so the receipt stops claiming otherwise.
    cleared = await sealed_receipt(created.id, until=lambda item: item.cleanup_failure is None)
    assert cleared is not None and cleared.cleanup_failure is None


# T6 · a refused lease transition must not have already killed the bout.
async def test_refused_towel_transition_leaves_the_bout_running_and_stoppable() -> None:
    lease_store = RefusingTowelTransitionStore()
    engine = TowelRecoveryEngine()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await running_round_three(manager, engine)
    record = manager._records[created.id]

    with pytest.raises(InvalidStateError, match="TOWEL CLEANUP REFUSED"):
        await manager.start_towel(created.id)

    # A refusal has to mean nothing happened. The bout is still alive, still
    # owns its lease, and the stop has not been latched behind the operator's
    # back -- nothing but `start_arm` or `start_run` ever clears it, and neither
    # is reachable from `running`.
    assert record.snapshot.state == SessionState.RUNNING
    assert record.snapshot.towel is None
    assert record.recovery_stop_control is not None
    assert record.recovery_stop_control.event.is_set() is False
    assert record.towel_stop_event is not None
    assert record.towel_stop_event.is_set() is False
    assert record.task is not None and not record.task.done()
    assert record.lease is not None and record.lease.phase == "run_committed"

    lease_store.refuse = False
    accepted = await manager.start_towel(created.id)

    assert accepted.state == SessionState.TOWELLED
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert await lease_store.current() is None


# T4 · a run task that never observes the cooperative stop must not hold the ring.
async def test_towel_cancels_a_round_three_run_that_ignores_the_stop_control() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = DeafRecoveryEngine()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    manager._towel_stop_grace = 0.01
    created = await running_round_three(manager, engine)

    await manager.start_towel(created.id)
    settled = await wait_for_towel(manager, created.id, "ready")

    assert engine.cancelled.is_set()
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert engine.timeline == ["settle", "reset"]
    assert await lease_store.current() is None


# T3 · a cancelled cooldown task must not strand the towel at `cleaning`.
async def test_cancelled_towel_cleanup_lands_on_failed_and_stays_retryable() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine()
    engine.allow_reset.clear()
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await running_round_three(manager, engine)

    await manager.start_towel(created.id)
    record = manager._records[created.id]
    await asyncio.wait_for(engine.reset_started.wait(), timeout=1)
    cooldown = record.cooldown_task
    assert cooldown is not None
    cooldown.cancel()
    await asyncio.gather(cooldown, return_exceptions=True)

    stranded = await wait_for_towel(manager, created.id, "failed")

    assert stranded.towel is not None
    assert "cancelled" in stranded.towel.cleanup_failure
    # The lockout is deliberate -- nothing proved the artifacts gone -- but it is
    # now a state the retry button and the retry branch can both reach.
    assert await lease_store.current() is not None

    engine.allow_reset.set()
    await manager.start_towel(created.id)
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert await lease_store.current() is None


# T3 · the ring refusal must name what is holding it, not just that it is held.
async def test_a_wedged_towel_names_itself_in_the_ring_refusal() -> None:
    lease_store = InMemoryBoutLeaseStore()
    engine = TowelRecoveryEngine(reset_failures=1)
    manager = RunManager(
        recovery_factory=lambda: engine,
        lease_store=lease_store,
        clock_ns=lambda: 100_005_678_901,
    )
    created = await running_round_three(manager, engine)
    challenger = await manager.create(round_three_request())

    await manager.start_towel(created.id)
    failed = await wait_for_towel(manager, created.id, "failed")
    assert failed.towel is not None and failed.towel.cleanup_failure is not None

    with pytest.raises(InvalidStateError) as refusal:
        await manager.start_arm(challenger.id)

    message = str(refusal.value)
    assert "BOUT IN PROGRESS" in message
    assert "TOWEL CLEANUP FAILED" in message
    assert failed.towel.cleanup_failure in message

    await manager.start_towel(created.id)
    settled = await wait_for_towel(manager, created.id, "ready")
    assert settled.towel is not None and settled.towel.cleanup_failure is None
    assert await lease_store.current() is None


# T12 · the compensating action nothing called is gone rather than misleading.
def test_towel_has_no_unreachable_compensating_lease_action() -> None:
    assert not hasattr(RunManager, "_revert_towel_transition")


_COST_INSTALLATION_ID = "install-cost-window"


def cost_window_manager(store, **kwargs) -> RunManager:
    """A manager whose cost window really opens, for the two ledger defects.

    Both defects live on the real ``_open_cost_bout``/``_close_cost_bout`` path,
    so neither can be reached by a manager that has no cost manifest and skips
    the window entirely.
    """

    branch = f"projects/{_COST_INSTALLATION_ID}/branches/production"
    environment = SimpleNamespace(
        lakebase=SimpleNamespace(
            project_id=_COST_INSTALLATION_ID,
            project_uid="project-uid",
            branch_name=branch,
            branch_uid="branch-uid",
            endpoint_name=f"{branch}/endpoints/primary",
            endpoint_uid="endpoint-uid",
        ),
        aurora=SimpleNamespace(
            cluster_id="anti-demo-aurora",
            cluster_resource_id="cluster-resource",
            writer_instance_id="anti-demo-writer",
            secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:aurora",
        ),
        rds=SimpleNamespace(
            instance_id="anti-demo-rds",
            resource_id="db-resource",
            secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:rds",
        ),
    )
    manifest = DemoManifest.model_construct(
        manifest_version=7,
        installation_id=_COST_INSTALLATION_ID,
        aws=SimpleNamespace(account_id="123456789012", region="us-west-2"),
        round_environments={RoundId.WAKE_IDLE_APP: environment},
    )
    return RunManager(
        round_isolation=True,
        installation_id=_COST_INSTALLATION_ID,
        cost_ledger_store=store,
        cost_manifest=manifest,
        **kwargs,
    )


async def cost_window_session(manager: RunManager):
    return await manager.create(
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.PERFORMANCE],
            round_id=RoundId.WAKE_IDLE_APP,
        )
    )


# B1 · the bell's refusal must name the grant that caused it, not cost sealing.
async def test_a_refused_ledger_write_reaches_the_bell_as_a_grant_and_a_sqlstate() -> None:
    """The 409 an audience reads has to point at what actually refused.

    Every way of failing to open a cost window used to arrive as one sentence
    about sealing the v7 cost identity. The case reproduced here is the one that
    happened: the sealing succeeded and the *ledger write* was refused for a
    missing `GRANT`, so the sentence sent its reader at cost identity -- the one
    part that was fine -- and a connection fault would have read identically.

    The quoting boundary is asserted in both directions in the same test, for
    the reason `test_the_operator_diagnosis_keeps_databricks_words_and_still_
    drops_aws_ones` gives: pinning only the keeping half lets the dropping half
    widen. psycopg's own words stay out even though *this* message happens to
    name only a relation, because the boundary is who wrote the sentence and
    the next `OperationalError` on this path carries a DSN. What replaces them
    is the SQLSTATE, which is the actionable half and no one's identifier.
    """

    refusal = psycopg.errors.InsufficientPrivilege("permission denied for table cost_ledger")
    ledger = FailingCostLedgerStore(open_error=refusal)
    manager = cost_window_manager(ledger, resolver=FakeResolver(), verifier=make_verifier())
    created = await cost_window_session(manager)
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)

    with pytest.raises(InvalidStateError) as refused:
        await manager.start_run(created.id)

    # `str(exc)` is exactly what `server.api` puts in the 409 detail.
    detail = str(refused.value)
    assert ledger.open_calls == 1
    assert COST_LEDGER_GRANT_HEADLINE in detail
    assert "SQLSTATE 42501" in detail
    # The remedy is a GRANT, and the sentence says so rather than sending the
    # reader at IAM or at the manifest seal.
    assert "A LAKEBASE GRANT, NOT AWS IAM" in detail
    # It no longer blames the half that worked.
    assert "cost identity" not in detail
    # psycopg's words did not travel, and neither did a traceback.
    assert "permission denied for table" not in detail
    assert "\n" not in detail and "Traceback" not in detail

    # And the refusal really was a refusal: the ring went back, so the next
    # round is armable rather than wedged behind a bout that never started.
    assert await manager._lease_store.current() is None

    # The other direction of the same boundary: our own precondition failures
    # are ours to quote, and used to be replaced wholesale by the fixed string.
    ours = InvalidStateError("The original cost receipt is unavailable")
    mine = cost_window_refusal("The bout cost window could not be opened.", ours)
    assert "The original cost receipt is unavailable" in mine
    assert COST_LEDGER_GRANT_HEADLINE not in mine


# B2 · a bout that was won must not be undone by its own tidy-up.
async def test_a_refused_cost_settlement_keeps_the_outcome_and_frees_the_record() -> None:
    """Settlement runs in a `finally`, so anything it raises REPLACES the result.

    This repo already has a bout that reported "verified" while its settlement
    failed four times on a missing grant. The half that is provable from here is
    what that costs the process: the close is the last one on a non-towelled
    bout, so a refusal used to leave `cost_bout_id` set forever, and
    `_releasable` gates on exactly that -- an unreleasable record per bout, on an
    app whose whole point is running unattended for days.

    The last two assertions are the ones that would have caught tonight's other
    class of bug, where two correct fixes met at a shared structure: dropping
    the pointer and refusing to reopen a window that is already open are both
    right alone, and wrong together if the first does not actually happen. So
    the next window is opened for real rather than inspected.
    """

    ledger = FailingCostLedgerStore(failures=99)
    manager = cost_window_manager(ledger, resolver=FakeResolver(), verifier=make_verifier())
    created = await cost_window_session(manager)
    await manager.start_arm(created.id)
    await wait_for_state(manager, created.id, SessionState.ARMED)
    await manager.start_run(created.id)
    verified = await wait_for_state(manager, created.id, SessionState.VERIFIED)
    record = manager._records[created.id]
    assert record.task is not None
    await asyncio.gather(record.task, return_exceptions=True)

    # The bout was won and still says so.
    assert verified.state == SessionState.VERIFIED
    assert ledger.close_calls >= 1
    # The settlement really did refuse, and the run task still finished clean:
    # the ledger's RuntimeError used to come out of here in place of the result.
    assert record.task.done()
    assert record.task.exception() is None

    # No pinned record: the open row goes to reconciliation instead of holding
    # this session in memory for the life of the process.
    #
    # `record.task` is the run, and the run is not everything the record owns.
    # A Round 1 bout also carries the automatic cooldown watcher, scheduled by
    # the run and deliberately outliving it -- `_unfinished_operations` counts
    # it, and `_releasable` gates on that count. So asking the question the
    # instant the run task returns is really asking whether an unrelated
    # background task has happened to drain yet. In most item orders it has. In
    # two of forty randomised orders (seeds 406 and 434) it had not, and this
    # line failed with
    #   AssertionError: unfinished=['auto-idle-watch-90d3cc3e...'] leases=[None, ...]
    # which is the watcher, not the pinned cost row this test is about.
    #
    # Draining first is not a weakening of the claim. The claim is that a
    # refused settlement leaves the record releasable rather than pinned
    # forever, and "forever" is exactly what a bounded wait still catches: the
    # drain fails loudly if the record is still owned when the bound expires.
    await drain_record_operations(manager, record)
    assert record.cost_bout_id is None
    assert manager._releasable(record) is True

    # The whole path through the shared structure: the next window opens.
    reopened = await manager._open_cost_bout(
        record,
        kind="redo",
        started_at=datetime.now(UTC),
    )
    assert reopened is not None


# B2 · a failing tidy-up must not overwrite the reason a bout actually failed.
async def test_a_refused_cost_settlement_does_not_mask_the_operations_own_failure() -> None:
    ledger = FailingCostLedgerStore(failures=99)
    manager = cost_window_manager(ledger)
    created = await cost_window_session(manager)
    record = manager._records[created.id]
    bout_id = await manager._open_cost_bout(record, kind="run", started_at=datetime.now(UTC))

    async def operation() -> None:
        raise RuntimeError("the round itself failed")

    with pytest.raises(RuntimeError, match="the round itself failed"):
        await manager._run_cost_bout(record, bout_id, operation())

    assert ledger.close_calls >= 1
    assert record.cost_bout_id is None
