from __future__ import annotations

import hashlib
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID, uuid4

WARMUP_ATTEMPTS_PER_LANE = 4
SCORED_ATTEMPTS_PER_LANE = 128
MAX_CONCURRENT_ATTEMPTS_PER_LANE = 64
MAX_LAUNCH_SKEW_MS = 10.0
WITNESS_CLIENTS_PER_LANE = 64
SETUP_DEADLINE_SECONDS = 30 * 60
MAX_SETUP_WORKFLOW_LAUNCH_DELAY_MS = 10.0


class ConnectionSpikeError(RuntimeError):
    """Base error for malformed Round 5 proof inputs."""


class AttemptKind(StrEnum):
    WARMUP = "warmup"
    SCORED = "scored"


class AttemptStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"

    @property
    def terminal(self) -> bool:
        return self in (AttemptStatus.SUCCESS, AttemptStatus.ERROR)


@dataclass(frozen=True)
class ConnectionSpikeContract:
    warmup_attempts_per_lane: int = WARMUP_ATTEMPTS_PER_LANE
    scored_attempts_per_lane: int = SCORED_ATTEMPTS_PER_LANE
    max_concurrent_attempts_per_lane: int = MAX_CONCURRENT_ATTEMPTS_PER_LANE
    max_launch_skew_ms: float = MAX_LAUNCH_SKEW_MS
    witness_clients_per_lane: int = WITNESS_CLIENTS_PER_LANE

    def __post_init__(self) -> None:
        frozen = (
            self.warmup_attempts_per_lane == WARMUP_ATTEMPTS_PER_LANE
            and self.scored_attempts_per_lane == SCORED_ATTEMPTS_PER_LANE
            and self.max_concurrent_attempts_per_lane == MAX_CONCURRENT_ATTEMPTS_PER_LANE
            and self.max_launch_skew_ms == MAX_LAUNCH_SKEW_MS
            and self.witness_clients_per_lane == WITNESS_CLIENTS_PER_LANE
        )
        if not frozen:
            raise ValueError("The Round 5 connection-spike contract is frozen")

    @property
    def sha256(self) -> str:
        values = (
            str(self.warmup_attempts_per_lane),
            str(self.scored_attempts_per_lane),
            str(self.max_concurrent_attempts_per_lane),
            repr(self.max_launch_skew_ms),
            str(self.witness_clients_per_lane),
        )
        return hashlib.sha256("\0".join(values).encode()).hexdigest()


@dataclass(frozen=True)
class SetupPhaseContract:
    """Frozen timing contract for the independently scored setup race."""

    max_workflow_launch_delay_ms: float = MAX_SETUP_WORKFLOW_LAUNCH_DELAY_MS
    deadline_seconds: int = SETUP_DEADLINE_SECONDS

    def __post_init__(self) -> None:
        if (
            self.max_workflow_launch_delay_ms != MAX_SETUP_WORKFLOW_LAUNCH_DELAY_MS
            or self.deadline_seconds != SETUP_DEADLINE_SECONDS
        ):
            raise ValueError("The Round 5 setup-phase contract is frozen")

    @property
    def sha256(self) -> str:
        values = (repr(self.max_workflow_launch_delay_ms), str(self.deadline_seconds))
        return hashlib.sha256("\0".join(values).encode()).hexdigest()


class SetupLaneStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TOWELLED = "towelled"


PublicEvidenceValue = str | int | float | bool | None


@dataclass(frozen=True)
class PublicSetupEvidence:
    """One secret-free, JSON-scalar stop-gate fact."""

    key: str
    value: PublicEvidenceValue

    def __post_init__(self) -> None:
        if not self.key.strip():
            raise ValueError("public setup evidence key is required")
        if self.value is not None and not isinstance(self.value, (str, int, float, bool)):
            raise TypeError("public setup evidence values must be JSON scalars")
        if isinstance(self.value, float) and not math.isfinite(self.value):
            raise ValueError("public setup evidence values must be finite")


@dataclass(frozen=True)
class SetupStopGateEvidence:
    """Expected and observed public facts for one exact setup stop gate."""

    gate_id: str
    expected: tuple[PublicSetupEvidence, ...]
    observed: tuple[PublicSetupEvidence, ...]
    verified_at_ns: int

    def __post_init__(self) -> None:
        if not self.gate_id.strip():
            raise ValueError("setup stop gate ID is required")
        if not self.expected:
            raise ValueError("setup stop gate expected evidence is required")
        for name, evidence in (("expected", self.expected), ("observed", self.observed)):
            keys = [fact.key for fact in evidence]
            if len(keys) != len(set(keys)):
                raise ValueError(f"setup stop gate {name} evidence keys must be unique")
        if self.verified_at_ns < 0:
            raise ValueError("setup stop gate time cannot be negative")

    @property
    def exact(self) -> bool:
        expected = {fact.key: (type(fact.value), fact.value) for fact in self.expected}
        observed = {fact.key: (type(fact.value), fact.value) for fact in self.observed}
        return expected == observed

    def to_public_dict(self) -> dict[str, object]:
        return {
            "gate_id": self.gate_id,
            "expected": [{"key": fact.key, "value": fact.value} for fact in self.expected],
            "observed": [{"key": fact.key, "value": fact.value} for fact in self.observed],
            "exact": self.exact,
        }


@dataclass(frozen=True)
class SetupPhaseArm:
    """Two setup workflows sharing one monotonic T0 and absolute deadline."""

    lane_ids: tuple[str, str]
    t0_ns: int
    deadline_ns: int
    contract_sha256: str

    def __post_init__(self) -> None:
        if len(set(self.lane_ids)) != 2 or any(not lane.strip() for lane in self.lane_ids):
            raise ValueError("The setup phase requires two unique, non-empty lane IDs")
        if self.t0_ns < 0:
            raise ValueError("setup T0 cannot be negative")
        expected_deadline = self.t0_ns + SETUP_DEADLINE_SECONDS * 1_000_000_000
        if self.deadline_ns != expected_deadline:
            raise ValueError("setup deadline must be exactly 30 minutes after T0")
        if self.contract_sha256 != SetupPhaseContract().sha256:
            raise ValueError("setup arm contract digest does not match the frozen contract")


@dataclass(frozen=True)
class SetupLaneObservation:
    lane_id: str
    workflow_launched_ns: int
    status: SetupLaneStatus
    stop_gate_evidence: SetupStopGateEvidence | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if not self.lane_id.strip():
            raise ValueError("setup lane ID is required")
        if self.workflow_launched_ns < 0:
            raise ValueError("setup workflow launch time cannot be negative")


@dataclass(frozen=True)
class SetupLaneResult:
    lane_id: str
    status: SetupLaneStatus
    workflow_launched_ns: int
    workflow_launch_delay_ms: float
    setup_elapsed_ns: int | None
    setup_elapsed_ms: float | None
    stop_gate_evidence: SetupStopGateEvidence | None
    failures: tuple[str, ...] = ()

    @property
    def verified(self) -> bool:
        return (
            self.status == SetupLaneStatus.SUCCEEDED
            and self.setup_elapsed_ns is not None
            and self.setup_elapsed_ms is not None
            and self.stop_gate_evidence is not None
            and self.stop_gate_evidence.exact
            and not self.failures
        )

    def to_public_dict(self) -> dict[str, object]:
        return {
            "lane_id": self.lane_id,
            "status": self.status.value,
            "setup_elapsed_ms": self.setup_elapsed_ms,
            "stop_gate_evidence": (
                self.stop_gate_evidence.to_public_dict()
                if self.stop_gate_evidence is not None
                else None
            ),
            "verified": self.verified,
        }


@dataclass(frozen=True)
class AttemptProof:
    row_uuid: UUID
    value: str
    attempt_id: UUID

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("attempt proof value is required")


@dataclass(frozen=True)
class ScheduledAttempt:
    lane_id: str
    kind: AttemptKind
    ordinal: int
    worker_slot: int
    proof: AttemptProof
    scheduled_at_ns: int

    def __post_init__(self) -> None:
        if not self.lane_id.strip():
            raise ValueError("lane_id is required")
        if self.ordinal < 0:
            raise ValueError("ordinal cannot be negative")
        if not 0 <= self.worker_slot < MAX_CONCURRENT_ATTEMPTS_PER_LANE:
            raise ValueError("worker_slot is outside the frozen per-lane maximum")
        if self.scheduled_at_ns < 0:
            raise ValueError("scheduled_at_ns cannot be negative")

    @property
    def attempt_id(self) -> UUID:
        return self.proof.attempt_id


@dataclass(frozen=True)
class ConnectionSpikeSchedule:
    lane_ids: tuple[str, ...]
    attempts: tuple[ScheduledAttempt, ...]
    max_concurrent_attempts_per_lane: int = MAX_CONCURRENT_ATTEMPTS_PER_LANE

    def lane_attempts(
        self,
        lane_id: str,
        kind: AttemptKind | None = None,
    ) -> tuple[ScheduledAttempt, ...]:
        return tuple(
            attempt
            for attempt in self.attempts
            if attempt.lane_id == lane_id and (kind is None or attempt.kind == kind)
        )


@dataclass(frozen=True)
class SharedBarrier:
    release_ns: int
    first_launch_ns_by_lane: Mapping[str, int]

    @property
    def launch_skew_ms(self) -> float:
        launches = tuple(self.first_launch_ns_by_lane.values())
        if not launches:
            return math.inf
        return (max(launches) - min(launches)) / 1_000_000


@dataclass(frozen=True)
class AttemptObservation:
    attempt_id: UUID
    status: AttemptStatus
    started_ns: int | None = None
    completed_ns: int | None = None
    response: AttemptProof | None = None
    committed: AttemptProof | None = None
    error: str | None = None

    @property
    def latency_ms(self) -> float | None:
        if self.started_ns is None or self.completed_ns is None:
            return None
        return (self.completed_ns - self.started_ns) / 1_000_000


@dataclass(frozen=True)
class RetainedClientWitness:
    client_id: str
    retained: bool
    verified: bool
    backend_pid: int


@dataclass(frozen=True)
class ConnectionSpikeWitness:
    clients: tuple[RetainedClientWitness, ...]
    peak_backend_sessions: int

    @property
    def witness_verified_clients(self) -> int:
        return sum(client.retained and client.verified for client in self.clients)

    @property
    def unique_backend_pids(self) -> int:
        return len({client.backend_pid for client in self.clients})


@dataclass(frozen=True)
class ConnectionSpikeGates:
    cleanup: bool
    fairness: bool
    contracts: bool
    failures: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return self.cleanup and self.fairness and self.contracts and not self.failures


@dataclass(frozen=True)
class ConnectionSpikeLaneResult:
    lane_id: str
    scheduled_clients: int
    terminal_clients: int
    successful_clients: int
    error_clients: int
    successful_latency_ms: tuple[float, ...]
    application_p99_ms: float | None
    witness_verified_clients: int
    unique_backend_pids: int
    peak_backend_sessions: int
    launch_skew_ms: float
    gates: ConnectionSpikeGates

    @property
    def verified(self) -> bool:
        return self.gates.passed


class ComparisonOutcome(StrEnum):
    LEFT = "left"
    RIGHT = "right"
    TIE = "tie"


@dataclass(frozen=True)
class ConnectionSpikeComparison:
    left_lane_id: str
    right_lane_id: str
    outcome: ComparisonOutcome
    winner_lane_id: str | None


@dataclass(frozen=True)
class SetupPhaseComparison:
    left_lane_id: str
    right_lane_id: str
    outcome: ComparisonOutcome
    winner_lane_id: str | None
    margin_ns: int | None
    margin_ms: float | None

    def to_public_dict(self) -> dict[str, object]:
        return {
            "left_lane_id": self.left_lane_id,
            "right_lane_id": self.right_lane_id,
            "outcome": self.outcome.value,
            "winner_lane_id": self.winner_lane_id,
            "margin_ms": self.margin_ms,
        }


@dataclass(frozen=True)
class ConnectionSpikeArm:
    arm_id: str
    contract_sha256: str
    schedule: ConnectionSpikeSchedule


@dataclass(frozen=True)
class ConnectionSpikeRunResult:
    contract_sha256: str
    lanes: Mapping[str, ConnectionSpikeLaneResult]
    comparison: ConnectionSpikeComparison | None


@dataclass(frozen=True)
class SetupPhaseResult:
    contract_sha256: str
    t0_ns: int
    deadline_ns: int
    workflow_launch_skew_ms: float
    lanes: Mapping[str, SetupLaneResult]
    downstream_validated: bool
    comparison: SetupPhaseComparison | None

    @property
    def setup_validated(self) -> bool:
        return len(self.lanes) == 2 and all(lane.verified for lane in self.lanes.values())

    @property
    def winner_lane_id(self) -> str | None:
        return self.comparison.winner_lane_id if self.comparison is not None else None

    @property
    def margin_ns(self) -> int | None:
        return self.comparison.margin_ns if self.comparison is not None else None

    @property
    def margin_ms(self) -> float | None:
        return self.comparison.margin_ms if self.comparison is not None else None

    def to_public_dict(self) -> dict[str, object]:
        return {
            "workflow_launch_skew_ms": self.workflow_launch_skew_ms,
            "lanes": {lane_id: lane.to_public_dict() for lane_id, lane in self.lanes.items()},
            "downstream_validated": self.downstream_validated,
            "setup_validated": self.setup_validated,
            "comparison": (
                self.comparison.to_public_dict() if self.comparison is not None else None
            ),
        }


ConnectionSpikeProgressCallback = Callable[[object], Awaitable[None]]


class ConnectionSpikeEngineProtocol(Protocol):
    async def check(self) -> ConnectionSpikeArm: ...

    async def run(
        self,
        arm: ConnectionSpikeArm,
        on_progress: ConnectionSpikeProgressCallback | None = None,
    ) -> ConnectionSpikeRunResult: ...

    async def cancel_and_cleanup(self, arm: ConnectionSpikeArm) -> object: ...


class ConnectionSpikeLaneAdapter(Protocol):
    async def execute_warmup(self, attempt: ScheduledAttempt) -> AttemptObservation: ...

    async def execute_scored(
        self,
        attempt: ScheduledAttempt,
        barrier: SharedBarrier,
    ) -> AttemptObservation: ...

    async def collect_witness(self) -> ConnectionSpikeWitness: ...

    async def cleanup(self) -> bool: ...


def arm_setup_phase(
    lane_ids: Sequence[str],
    *,
    t0_ns: int,
    contract: SetupPhaseContract | None = None,
) -> SetupPhaseArm:
    """Freeze both setup lanes against one caller-supplied monotonic T0."""
    lanes = tuple(lane_ids)
    if len(lanes) != 2:
        raise ValueError("The setup phase requires exactly two lanes")
    frozen_contract = contract or SetupPhaseContract()
    return SetupPhaseArm(
        lane_ids=(lanes[0], lanes[1]),
        t0_ns=t0_ns,
        deadline_ns=t0_ns + frozen_contract.deadline_seconds * 1_000_000_000,
        contract_sha256=frozen_contract.sha256,
    )


def finalize_setup_lane(
    arm: SetupPhaseArm,
    observation: SetupLaneObservation,
) -> SetupLaneResult:
    """Validate one setup lane without rounding or accepting a caller's elapsed time."""
    if observation.lane_id not in arm.lane_ids:
        raise ValueError("setup observation belongs to an unarmed lane")

    failures: list[str] = []
    launch_delta_ns = observation.workflow_launched_ns - arm.t0_ns
    launch_delay_ms = launch_delta_ns / 1_000_000
    max_launch_delta_ns = int(MAX_SETUP_WORKFLOW_LAUNCH_DELAY_MS * 1_000_000)
    if not 0 <= launch_delta_ns <= max_launch_delta_ns:
        failures.append("workflow_launch_window")

    elapsed_ns: int | None = None
    elapsed_ms: float | None = None
    evidence = observation.stop_gate_evidence
    if observation.status == SetupLaneStatus.SUCCEEDED:
        if observation.error:
            failures.append("setup_error")
        if evidence is None or not evidence.exact:
            failures.append("stop_gate_evidence")
        else:
            elapsed_ns = evidence.verified_at_ns - arm.t0_ns
            elapsed_ms = elapsed_ns / 1_000_000
            if evidence.verified_at_ns < observation.workflow_launched_ns:
                failures.append("stop_gate_before_workflow_launch")
            if evidence.verified_at_ns > arm.deadline_ns:
                failures.append("setup_deadline")
    elif observation.status == SetupLaneStatus.FAILED:
        failures.append("setup_failed")
    else:
        failures.append("setup_towelled")

    return SetupLaneResult(
        lane_id=observation.lane_id,
        status=observation.status,
        workflow_launched_ns=observation.workflow_launched_ns,
        workflow_launch_delay_ms=launch_delay_ms,
        setup_elapsed_ns=elapsed_ns,
        setup_elapsed_ms=elapsed_ms,
        stop_gate_evidence=evidence,
        failures=tuple(dict.fromkeys(failures)),
    )


def compare_setup_lanes(
    left: SetupLaneResult,
    right: SetupLaneResult,
    *,
    downstream_validated: bool,
) -> SetupPhaseComparison | None:
    """Score only raw setup elapsed time after setup and downstream proof validate."""
    if not downstream_validated or not left.verified or not right.verified:
        return None
    if left.setup_elapsed_ns is None or right.setup_elapsed_ns is None:
        return None

    if left.setup_elapsed_ns == right.setup_elapsed_ns:
        return SetupPhaseComparison(
            left_lane_id=left.lane_id,
            right_lane_id=right.lane_id,
            outcome=ComparisonOutcome.TIE,
            winner_lane_id=None,
            margin_ns=None,
            margin_ms=None,
        )

    left_won = left.setup_elapsed_ns < right.setup_elapsed_ns
    margin_ns = abs(left.setup_elapsed_ns - right.setup_elapsed_ns)
    return SetupPhaseComparison(
        left_lane_id=left.lane_id,
        right_lane_id=right.lane_id,
        outcome=ComparisonOutcome.LEFT if left_won else ComparisonOutcome.RIGHT,
        winner_lane_id=left.lane_id if left_won else right.lane_id,
        margin_ns=margin_ns,
        margin_ms=margin_ns / 1_000_000,
    )


def finalize_setup_phase(
    arm: SetupPhaseArm,
    observations: Sequence[SetupLaneObservation],
    downstream_lanes: Mapping[str, ConnectionSpikeLaneResult],
) -> SetupPhaseResult:
    """Finalize the primary setup race and gate it on the later burst proof."""
    observations_by_lane: dict[str, SetupLaneObservation] = {}
    for observation in observations:
        if observation.lane_id in observations_by_lane:
            raise ConnectionSpikeError("setup observations must be unique by lane")
        observations_by_lane[observation.lane_id] = observation
    if set(observations_by_lane) != set(arm.lane_ids):
        raise ConnectionSpikeError("setup observations must exactly match the armed lanes")

    lanes = {
        lane_id: finalize_setup_lane(arm, observations_by_lane[lane_id]) for lane_id in arm.lane_ids
    }
    downstream_validated = set(downstream_lanes) == set(arm.lane_ids) and all(
        downstream_lanes[lane_id].verified for lane_id in arm.lane_ids
    )
    left, right = (lanes[lane_id] for lane_id in arm.lane_ids)
    comparison = compare_setup_lanes(
        left,
        right,
        downstream_validated=downstream_validated,
    )
    launches = [observation.workflow_launched_ns for observation in observations]
    return SetupPhaseResult(
        contract_sha256=arm.contract_sha256,
        t0_ns=arm.t0_ns,
        deadline_ns=arm.deadline_ns,
        workflow_launch_skew_ms=(max(launches) - min(launches)) / 1_000_000,
        lanes=lanes,
        downstream_validated=downstream_validated,
        comparison=comparison,
    )


def build_schedule(
    lane_ids: Sequence[str],
    *,
    scheduled_at_ns: int,
    uuid_factory: Callable[[], UUID] = uuid4,
) -> ConnectionSpikeSchedule:
    """Create all warmup and scored work before the caller releases the barrier."""
    lanes = tuple(lane_ids)
    if len(lanes) < 2 or len(set(lanes)) != len(lanes) or any(not lane.strip() for lane in lanes):
        raise ValueError("At least two unique, non-empty lane IDs are required")
    if scheduled_at_ns < 0:
        raise ValueError("scheduled_at_ns cannot be negative")

    attempts: list[ScheduledAttempt] = []
    for lane_id in lanes:
        for kind, count in (
            (AttemptKind.WARMUP, WARMUP_ATTEMPTS_PER_LANE),
            (AttemptKind.SCORED, SCORED_ATTEMPTS_PER_LANE),
        ):
            for ordinal in range(count):
                row_uuid = uuid_factory()
                attempt_id = uuid_factory()
                attempts.append(
                    ScheduledAttempt(
                        lane_id=lane_id,
                        kind=kind,
                        ordinal=ordinal,
                        worker_slot=ordinal % MAX_CONCURRENT_ATTEMPTS_PER_LANE,
                        proof=AttemptProof(
                            row_uuid=row_uuid,
                            value=f"round5-{row_uuid}",
                            attempt_id=attempt_id,
                        ),
                        scheduled_at_ns=scheduled_at_ns,
                    )
                )
    return ConnectionSpikeSchedule(lane_ids=lanes, attempts=tuple(attempts))


def nearest_rank_p99(latencies_ms: Sequence[float]) -> float | None:
    """Return the unrounded nearest-rank p99, or None when there were no successes."""
    if not latencies_ms:
        return None
    if any(not math.isfinite(value) or value < 0 for value in latencies_ms):
        raise ValueError("latencies must be finite and non-negative")
    ordered = sorted(latencies_ms)
    rank = math.ceil(0.99 * len(ordered))
    return ordered[rank - 1]


def finalize_lane(
    schedule: ConnectionSpikeSchedule,
    lane_id: str,
    observations: Sequence[AttemptObservation],
    witness: ConnectionSpikeWitness,
    barrier: SharedBarrier,
    *,
    cleanup_verified: bool,
    fairness_verified: bool,
    contracts_verified: bool,
) -> ConnectionSpikeLaneResult:
    expected = schedule.lane_attempts(lane_id)
    warmups = schedule.lane_attempts(lane_id, AttemptKind.WARMUP)
    scored = schedule.lane_attempts(lane_id, AttemptKind.SCORED)
    failures: list[str] = []

    schedule_ok = _schedule_is_frozen(schedule, lane_id, barrier)
    if not schedule_ok:
        failures.append("schedule_or_barrier")

    by_id: dict[UUID, AttemptObservation] = {}
    duplicate_ids: set[UUID] = set()
    for observation in observations:
        if observation.attempt_id in by_id:
            duplicate_ids.add(observation.attempt_id)
        else:
            by_id[observation.attempt_id] = observation
    expected_ids = {attempt.attempt_id for attempt in expected}
    settled = (
        not duplicate_ids
        and set(by_id) == expected_ids
        and all(observation.status.terminal for observation in by_id.values())
    )
    if not settled:
        failures.append("attempts_not_unique_terminal_and_settled")

    proof_by_id = {attempt.attempt_id: attempt.proof for attempt in expected}
    warmup_ids = {attempt.attempt_id for attempt in warmups}
    scored_ids = {attempt.attempt_id for attempt in scored}
    exact_contract = True
    for attempt_id, observation in by_id.items():
        expected_proof = proof_by_id.get(attempt_id)
        if observation.status == AttemptStatus.SUCCESS:
            timing_valid = (
                observation.completed_ns is not None
                and observation.started_ns is not None
                and observation.completed_ns >= observation.started_ns
            )
            if attempt_id in scored_ids:
                timing_valid = timing_valid and (
                    observation.started_ns >= barrier.release_ns
                    and observation.completed_ns >= barrier.release_ns
                )
            elif attempt_id in warmup_ids:
                timing_valid = timing_valid and observation.completed_ns <= barrier.release_ns
            if (
                expected_proof is None
                or observation.response != expected_proof
                or observation.committed != expected_proof
                or not timing_valid
                or observation.error is not None
            ):
                exact_contract = False
        elif observation.status == AttemptStatus.ERROR:
            if (
                not observation.error
                or observation.response is not None
                or observation.committed is not None
            ):
                exact_contract = False
        elif observation.status.terminal:
            exact_contract = False
    if not exact_contract:
        failures.append("response_or_commit_contract")

    warmups_ok = len(warmups) == WARMUP_ATTEMPTS_PER_LANE and all(
        (observation := by_id.get(attempt_id)) is not None
        and observation.status == AttemptStatus.SUCCESS
        for attempt_id in warmup_ids
    )
    if not warmups_ok:
        failures.append("warmups")

    witness_ok = _witness_is_valid(witness)
    if not witness_ok:
        failures.append("witness")

    scored_observations = [
        observation for attempt_id, observation in by_id.items() if attempt_id in scored_ids
    ]
    terminal_clients = sum(observation.status.terminal for observation in scored_observations)
    successful = [
        observation
        for observation in scored_observations
        if observation.status == AttemptStatus.SUCCESS
        and observation.response == proof_by_id[observation.attempt_id]
        and observation.committed == proof_by_id[observation.attempt_id]
        and observation.started_ns is not None
        and observation.started_ns >= barrier.release_ns
        and observation.completed_ns is not None
        and observation.completed_ns >= barrier.release_ns
    ]
    successful_latency_ms = tuple(
        (observation.completed_ns - observation.started_ns) / 1_000_000
        for observation in successful
        if observation.completed_ns is not None and observation.started_ns is not None
    )
    successful_clients = len(successful)
    error_clients = len(
        [
            observation
            for observation in scored_observations
            if observation.status == AttemptStatus.ERROR
        ]
    )

    fairness = fairness_verified and schedule_ok and settled
    contracts = contracts_verified and settled and exact_contract and warmups_ok and witness_ok
    if not cleanup_verified:
        failures.append("cleanup")
    if not fairness_verified:
        failures.append("fairness")
    if not contracts_verified:
        failures.append("contracts")

    return ConnectionSpikeLaneResult(
        lane_id=lane_id,
        scheduled_clients=len(scored),
        terminal_clients=terminal_clients,
        successful_clients=successful_clients,
        error_clients=error_clients,
        successful_latency_ms=successful_latency_ms,
        application_p99_ms=nearest_rank_p99(successful_latency_ms),
        witness_verified_clients=witness.witness_verified_clients,
        unique_backend_pids=witness.unique_backend_pids,
        peak_backend_sessions=witness.peak_backend_sessions,
        launch_skew_ms=barrier.launch_skew_ms,
        gates=ConnectionSpikeGates(
            cleanup=cleanup_verified,
            fairness=fairness,
            contracts=contracts,
            failures=tuple(dict.fromkeys(failures)),
        ),
    )


def compare_lanes(
    left: ConnectionSpikeLaneResult,
    right: ConnectionSpikeLaneResult,
) -> ConnectionSpikeComparison | None:
    """Compare only verified lanes: successes, errors, then the unrounded p99."""
    if not left.verified or not right.verified:
        return None

    outcome = ComparisonOutcome.TIE
    if left.successful_clients != right.successful_clients:
        outcome = (
            ComparisonOutcome.LEFT
            if left.successful_clients > right.successful_clients
            else ComparisonOutcome.RIGHT
        )
    elif left.error_clients != right.error_clients:
        outcome = (
            ComparisonOutcome.LEFT
            if left.error_clients < right.error_clients
            else ComparisonOutcome.RIGHT
        )
    elif left.application_p99_ms != right.application_p99_ms:
        if left.application_p99_ms is None:
            outcome = ComparisonOutcome.RIGHT
        elif right.application_p99_ms is None:
            outcome = ComparisonOutcome.LEFT
        else:
            outcome = (
                ComparisonOutcome.LEFT
                if left.application_p99_ms < right.application_p99_ms
                else ComparisonOutcome.RIGHT
            )

    winner = {
        ComparisonOutcome.LEFT: left.lane_id,
        ComparisonOutcome.RIGHT: right.lane_id,
        ComparisonOutcome.TIE: None,
    }[outcome]
    return ConnectionSpikeComparison(
        left_lane_id=left.lane_id,
        right_lane_id=right.lane_id,
        outcome=outcome,
        winner_lane_id=winner,
    )


def _schedule_is_frozen(
    schedule: ConnectionSpikeSchedule,
    lane_id: str,
    barrier: SharedBarrier,
) -> bool:
    warmups = schedule.lane_attempts(lane_id, AttemptKind.WARMUP)
    scored = schedule.lane_attempts(lane_id, AttemptKind.SCORED)
    launch_lanes = set(barrier.first_launch_ns_by_lane)
    launch_times = tuple(barrier.first_launch_ns_by_lane.values())
    all_scheduled_before_release = all(
        attempt.scheduled_at_ns < barrier.release_ns for attempt in schedule.attempts
    )
    valid_launch = (
        launch_lanes == set(schedule.lane_ids)
        and bool(launch_times)
        and all(
            barrier.release_ns <= launch <= barrier.release_ns + int(MAX_LAUNCH_SKEW_MS * 1_000_000)
            for launch in launch_times
        )
        and barrier.launch_skew_ms <= MAX_LAUNCH_SKEW_MS
    )
    slots = {attempt.worker_slot for attempt in scored}
    return (
        lane_id in schedule.lane_ids
        and schedule.max_concurrent_attempts_per_lane == MAX_CONCURRENT_ATTEMPTS_PER_LANE
        and len(warmups) == WARMUP_ATTEMPTS_PER_LANE
        and len(scored) == SCORED_ATTEMPTS_PER_LANE
        and slots == set(range(MAX_CONCURRENT_ATTEMPTS_PER_LANE))
        and len({attempt.attempt_id for attempt in schedule.attempts}) == len(schedule.attempts)
        and all_scheduled_before_release
        and valid_launch
    )


def _witness_is_valid(witness: ConnectionSpikeWitness) -> bool:
    client_ids = {client.client_id for client in witness.clients}
    return (
        len(witness.clients) == WITNESS_CLIENTS_PER_LANE
        and len(client_ids) == WITNESS_CLIENTS_PER_LANE
        and all(client.retained and client.verified for client in witness.clients)
        and all(client.backend_pid > 0 for client in witness.clients)
        and witness.unique_backend_pids < WITNESS_CLIENTS_PER_LANE
        and 1 <= witness.peak_backend_sessions < WITNESS_CLIENTS_PER_LANE
    )
