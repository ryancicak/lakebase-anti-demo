"""Pure Round 5 exact 10,000-client fan-in contract.

The live adapter and the sealed runner exchange aggregate evidence only.  This
module decides whether that evidence proves the v2 protocol; it deliberately
contains no socket, database, AWS, or wall-clock code.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

FANIN_PROTOCOL = "round5-fanin-v2"
FANIN_SCHEMA_VERSION = 2
TARGET_CLIENTS_PER_LANE = 10_000
# Mirrors runner.round5_fanin. Selected because m6i.xlarge has exactly WORKER_COUNT
# vCPUs, leaving nothing for the parent process, the SSM agent, or kernel packet
# processing across 20,000 sockets -- the starvation docs/ROUND5_10K_PROTOCOL.md
# records. c7i.2xlarge keeps four cores free for that housekeeping.
RUNNER_INSTANCE_TYPE = "c7i.2xlarge"
RUNNER_LANE_COUNT = 2
WORKER_COUNT = 4
MIN_RUNNER_CPU_COUNT = WORKER_COUNT
HOLD_SECONDS = 30
SAMPLED_QUERIES_PER_LANE = 64
SAMPLE_GROUPS = 8
CLIENTS_PER_SAMPLE_GROUP = 8
MAX_RETRIES = 0
INITIAL_WAVE_SIZE = 100
MIN_WAVE_SIZE = 20
MICRO_BATCH_SIZE = 2
# Mirrors runner.round5_fanin. The mirrored creation quantum and the launch
# pipeline depth are separate concerns: MICRO_BATCH_SIZE keeps both lanes
# interleaved identically, LANE_CONNECT_CONCURRENCY bounds how many of those
# clients may be in flight. Both batch limits are unbounded (<= 0) because
# capping either broke the asyncio invariant that a turn consumes the readiness
# it was handed, and under level-triggered epoll that turned O(N) readiness
# service into O(N**2 / K) while the un-run descriptors stayed readable.
LANE_CONNECT_CONCURRENCY = 32
READY_CALLBACK_BATCH_LIMIT = 0
SELECTOR_EVENT_BATCH_LIMIT = 0
PARTITION_CLIENTS_PER_LANE = TARGET_CLIENTS_PER_LANE // WORKER_COUNT
MAX_IN_FLIGHT_CONNECTS_PER_LANE = LANE_CONNECT_CONCURRENCY * WORKER_COUNT
# No-endpoint preflight readiness probe. Deliberately the whole-runner in-flight
# bound, four times what one worker loop can have handshaking at once.
SELECTOR_FANOUT_PROBE_SOCKETS = LANE_CONNECT_CONCURRENCY * WORKER_COUNT * RUNNER_LANE_COUNT
SELECTOR_FANOUT_PROBE_CALLBACK_CPU_NS = 150_000
# Wakeups per delivered readiness event; 1.0 is optimal. A capped selector
# re-reports what it dropped, which measured 32.5x at K=16 over 1,024
# descriptors while making a per-turn latency gate look 60x better.
MAX_SELECTOR_WAKEUP_AMPLIFICATION = 1.05
MAX_LAUNCH_SKEW_MS = 10.0
CONNECT_TIMEOUT_SECONDS = 20.0
RUN_TIMEOUT_SECONDS = 600.0
RUNTIME_MAX_EVENT_LOOP_P99_MS = 50.0
RAW_WALL_LAG_WARNING_MS = 50.0
RAW_WALL_LAG_CEILING_MS = 250.0
RAW_WALL_LAG_MAX_BREACHES = 3
OWNED_STALL_MIN_THREAD_CPU_MS = 5.0
OWNED_STALL_READY_BATCH = 16
RUNTIME_MAX_CPU_CAPACITY_FRACTION = 0.85
LOOP_MONITOR_INTERVAL_SECONDS = 0.01
RESOURCE_TELEMETRY_INTERVAL_SECONDS = 0.25
MEMORY_RESERVE_BYTES = 768 * 1024 * 1024
FD_CONTROL_RESERVE = 256
FD_USAGE_FRACTION = 0.80
EPHEMERAL_PORT_RESERVE_PER_LANE = 2_000
CALIBRATED_ONE_LANE_RSS_BYTES = int(3.11 * 1024**3)
RAMP_HEADROOM_FRACTION = 0.15
AUTH_CLEARTEXT = "tls-cleartext-password"
AUTH_SCRAM = "scram-sha-256"
SUPPORTED_AUTH_METHODS = frozenset({AUTH_CLEARTEXT, AUTH_SCRAM})
# Mirrors runner.round5_fanin's progress wire. A bout runs for minutes behind a
# single SSM command, so the only way the ring can show a ramp in flight is the
# runner printing bounded progress lines that the adapter reads back. The budget
# is the runner's: it stops printing rather than truncating, because a truncated
# JSON line is indistinguishable from a corrupted one on this side.
PROGRESS_PREFIX = "PROGRESS_JSON:"
PROGRESS_OUTPUT_BUDGET_BYTES = 12_000
PROGRESS_WIRE_FIELDS = (
    "protocol",
    "schema_version",
    "lane_id",
    "phase",
    "authenticated_clients",
    "held_clients",
    "terminal_failures",
    "elapsed_ms",
    "time_to_target_ms",
    "hold_remaining_ms",
    "sampled_queries_succeeded",
    "sampled_queries_failed",
    "event_loop_p99_ms",
)
# A fresh install cannot inspect /proc before EC2 exists.  Discount AWS's
# nominal memory by more than the observed m6i.large guest/kernel loss
# (7.598 GiB visible from an 8 GiB shape) so the same model can reject an
# undersized selection before Terraform creates anything.
PREPROVISION_GUEST_MEMORY_FRACTION = 0.94
PREPROVISION_FD_SOFT_LIMIT = 65_535


@dataclass(frozen=True, slots=True)
class RunnerInstanceCapacity:
    instance_type: str
    vcpu_count: int
    nominal_memory_bytes: int
    expected_fd_soft_limit: int = PREPROVISION_FD_SOFT_LIMIT


@dataclass(frozen=True, slots=True)
class RunnerProvisioningCapacity:
    selected: RunnerInstanceCapacity
    usable_memory_bytes: int
    required_memory_bytes: int
    projected_fds: int
    failures: tuple[str, ...]

    @property
    def sufficient(self) -> bool:
        return not self.failures


# Keys are literal, never RUNNER_INSTANCE_TYPE. Keying a table off a constant
# that moves silently drops the previous shape out of it the moment the constant
# changes, which is the same failure mode as deriving a superseded digest from
# the live contract.
RUNNER_INSTANCE_CAPACITIES = {
    "m6i.large": RunnerInstanceCapacity("m6i.large", 2, 8 * 1024**3),
    "m6i.xlarge": RunnerInstanceCapacity("m6i.xlarge", 4, 16 * 1024**3),
    # The selected shape. WORKER_COUNT stays 4 because each worker
    # is one asyncio event loop and a loop cannot span cores; the extra four
    # vCPUs exist so the parent, the off-loop telemetry threads, and kernel
    # packet processing for 20,000 sockets stop competing with the pinned
    # workers. Same 16 GiB as the xlarge, which the memory model already clears
    # with roughly 7 GiB to spare. See docs/ROUND5_10K_PROTOCOL.md.
    "c7i.2xlarge": RunnerInstanceCapacity("c7i.2xlarge", 8, 16 * 1024**3),
}


class FanInError(RuntimeError):
    """The exact fan-in evidence is malformed or incomplete."""


class FanInOutcome(StrEnum):
    LEFT = "left"
    RIGHT = "right"
    TIE = "tie"


@dataclass(frozen=True, slots=True)
class ConnectionSpikeContract:
    """Versioned immutable parameters shared by both fan-in lanes."""

    schema_version: int = FANIN_SCHEMA_VERSION
    protocol: str = FANIN_PROTOCOL
    target_clients_per_lane: int = TARGET_CLIENTS_PER_LANE
    hold_seconds: int = HOLD_SECONDS
    sampled_queries_per_lane: int = SAMPLED_QUERIES_PER_LANE
    sample_groups: int = SAMPLE_GROUPS
    clients_per_sample_group: int = CLIENTS_PER_SAMPLE_GROUP
    max_retries: int = MAX_RETRIES
    initial_wave_size: int = INITIAL_WAVE_SIZE
    min_wave_size: int = MIN_WAVE_SIZE
    micro_batch_size: int = MICRO_BATCH_SIZE
    lane_connect_concurrency: int = LANE_CONNECT_CONCURRENCY
    ready_callback_batch_limit: int = READY_CALLBACK_BATCH_LIMIT
    selector_event_batch_limit: int = SELECTOR_EVENT_BATCH_LIMIT
    worker_count: int = WORKER_COUNT
    partition_clients_per_lane: int = PARTITION_CLIENTS_PER_LANE
    max_in_flight_connects_per_lane: int = MAX_IN_FLIGHT_CONNECTS_PER_LANE
    max_launch_skew_ms: float = MAX_LAUNCH_SKEW_MS
    connect_timeout_seconds: float = CONNECT_TIMEOUT_SECONDS
    run_timeout_seconds: float = RUN_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        frozen = (
            self.schema_version == FANIN_SCHEMA_VERSION
            and self.protocol == FANIN_PROTOCOL
            and self.target_clients_per_lane == TARGET_CLIENTS_PER_LANE
            and self.hold_seconds == HOLD_SECONDS
            and self.sampled_queries_per_lane == SAMPLED_QUERIES_PER_LANE
            and self.sample_groups == SAMPLE_GROUPS
            and self.clients_per_sample_group == CLIENTS_PER_SAMPLE_GROUP
            and self.max_retries == MAX_RETRIES
            and self.initial_wave_size == INITIAL_WAVE_SIZE
            and self.min_wave_size == MIN_WAVE_SIZE
            and self.micro_batch_size == MICRO_BATCH_SIZE
            and self.lane_connect_concurrency == LANE_CONNECT_CONCURRENCY
            and self.ready_callback_batch_limit == READY_CALLBACK_BATCH_LIMIT
            and self.selector_event_batch_limit == SELECTOR_EVENT_BATCH_LIMIT
            and self.worker_count == WORKER_COUNT
            and self.partition_clients_per_lane == PARTITION_CLIENTS_PER_LANE
            and self.max_in_flight_connects_per_lane == MAX_IN_FLIGHT_CONNECTS_PER_LANE
            and self.max_launch_skew_ms == MAX_LAUNCH_SKEW_MS
            and self.connect_timeout_seconds == CONNECT_TIMEOUT_SECONDS
            and self.run_timeout_seconds == RUN_TIMEOUT_SECONDS
        )
        if not frozen:
            raise ValueError("The Round 5 exact fan-in contract is frozen")
        if self.sample_groups * self.clients_per_sample_group != self.sampled_queries_per_lane:
            raise ValueError("The Round 5 sample-group contract is incoherent")

    @property
    def public_dict(self) -> dict[str, int | float | str]:
        return {
            "schema_version": self.schema_version,
            "protocol": self.protocol,
            "target_clients_per_lane": self.target_clients_per_lane,
            "hold_seconds": self.hold_seconds,
            "sampled_queries_per_lane": self.sampled_queries_per_lane,
            "sample_groups": self.sample_groups,
            "clients_per_sample_group": self.clients_per_sample_group,
            "max_retries": self.max_retries,
            "initial_wave_size": self.initial_wave_size,
            "min_wave_size": self.min_wave_size,
            "micro_batch_size": self.micro_batch_size,
            "lane_connect_concurrency": self.lane_connect_concurrency,
            "ready_callback_batch_limit": self.ready_callback_batch_limit,
            "selector_event_batch_limit": self.selector_event_batch_limit,
            "worker_count": self.worker_count,
            "partition_clients_per_lane": self.partition_clients_per_lane,
            "max_in_flight_connects_per_lane": self.max_in_flight_connects_per_lane,
            "max_launch_skew_ms": self.max_launch_skew_ms,
            "connect_timeout_seconds": self.connect_timeout_seconds,
            "run_timeout_seconds": self.run_timeout_seconds,
            "supported_auth_methods": ",".join(sorted(SUPPORTED_AUTH_METHODS)),
        }

    @property
    def sha256(self) -> str:
        encoded = json.dumps(
            self.public_dict,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class CapacityPreflight:
    """Secret-free measured runner capacity and the conservative projection."""

    instance_type: str
    cpu_count: int
    physical_memory_bytes: int
    available_memory_bytes: int
    baseline_rss_bytes: int
    fd_soft_limit: int
    fd_hard_limit: int
    open_fds: int
    ephemeral_port_first: int
    ephemeral_port_last: int
    event_loop_p99_ms: float
    cpu_calibration_ms: float
    projected_held_rss_bytes: int
    projected_peak_rss_bytes: int
    projected_fds: int
    model_sha256: str
    event_loop_microbatch_p99_ms: float = 0.0
    event_loop_selector_fanout_peak_ms: float = 0.0
    event_loop_selector_fanout_baseline_peak_ms: float = 0.0
    event_loop_selector_fanout_peak_deferred: int = 0
    failures: tuple[str, ...] = ()

    @property
    def ephemeral_port_count(self) -> int:
        return self.ephemeral_port_last - self.ephemeral_port_first + 1

    @property
    def sufficient(self) -> bool:
        return not self.failures


def capacity_model_sha256() -> str:
    values = {
        "calibrated_one_lane_rss_bytes": CALIBRATED_ONE_LANE_RSS_BYTES,
        "ephemeral_port_reserve_per_lane": EPHEMERAL_PORT_RESERVE_PER_LANE,
        "fd_control_reserve": FD_CONTROL_RESERVE,
        "fd_usage_fraction": FD_USAGE_FRACTION,
        "memory_reserve_bytes": MEMORY_RESERVE_BYTES,
        "min_runner_cpu_count": MIN_RUNNER_CPU_COUNT,
        "runtime_max_cpu_capacity_fraction": RUNTIME_MAX_CPU_CAPACITY_FRACTION,
        "runtime_max_event_loop_p99_ms": RUNTIME_MAX_EVENT_LOOP_P99_MS,
        "event_loop_gate_semantics": "wall_plus_cpu_and_internal_work",
        "raw_wall_lag_warning_ms": RAW_WALL_LAG_WARNING_MS,
        "raw_wall_lag_ceiling_ms": RAW_WALL_LAG_CEILING_MS,
        "raw_wall_lag_max_breaches": RAW_WALL_LAG_MAX_BREACHES,
        "owned_stall_min_thread_cpu_ms": OWNED_STALL_MIN_THREAD_CPU_MS,
        "owned_stall_ready_batch": OWNED_STALL_READY_BATCH,
        "ready_callback_batch_limit": READY_CALLBACK_BATCH_LIMIT,
        "selector_event_batch_limit": SELECTOR_EVENT_BATCH_LIMIT,
        "lane_connect_concurrency": LANE_CONNECT_CONCURRENCY,
        "max_selector_wakeup_amplification": MAX_SELECTOR_WAKEUP_AMPLIFICATION,
        "selector_fanout_probe_sockets": SELECTOR_FANOUT_PROBE_SOCKETS,
        "selector_fanout_probe_callback_cpu_ns": SELECTOR_FANOUT_PROBE_CALLBACK_CPU_NS,
        "loop_monitor_interval_seconds": LOOP_MONITOR_INTERVAL_SECONDS,
        "resource_telemetry_interval_seconds": RESOURCE_TELEMETRY_INTERVAL_SECONDS,
        "gc_policy": "precollect-disabled-during-measurement-final-collect",
        "runner_instance_type": RUNNER_INSTANCE_TYPE,
        "runner_lane_count": RUNNER_LANE_COUNT,
        "ramp_headroom_fraction": RAMP_HEADROOM_FRACTION,
        "target_clients_per_lane": TARGET_CLIENTS_PER_LANE,
    }
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def evaluate_runner_provisioning_capacity(instance_type: str) -> RunnerProvisioningCapacity:
    """Evaluate a selected EC2 shape before Terraform can provision it."""

    selected = RUNNER_INSTANCE_CAPACITIES.get(instance_type)
    if selected is None:
        selected = RunnerInstanceCapacity(instance_type, 0, 0, 0)
    projected_held = RUNNER_LANE_COUNT * CALIBRATED_ONE_LANE_RSS_BYTES
    required_memory = (
        math.ceil(projected_held * (1 + RAMP_HEADROOM_FRACTION)) + MEMORY_RESERVE_BYTES
    )
    usable_memory = math.floor(selected.nominal_memory_bytes * PREPROVISION_GUEST_MEMORY_FRACTION)
    projected_fds = RUNNER_LANE_COUNT * TARGET_CLIENTS_PER_LANE + FD_CONTROL_RESERVE
    failures: list[str] = []
    if instance_type not in RUNNER_INSTANCE_CAPACITIES:
        failures.append("unsupported_instance_type")
    if selected.vcpu_count < MIN_RUNNER_CPU_COUNT:
        failures.append("runner_cpu_count")
    if usable_memory < required_memory:
        failures.append("physical_memory_projection")
    if projected_fds > math.floor(selected.expected_fd_soft_limit * FD_USAGE_FRACTION):
        failures.append("file_descriptor_projection")
    return RunnerProvisioningCapacity(
        selected=selected,
        usable_memory_bytes=usable_memory,
        required_memory_bytes=required_memory,
        projected_fds=projected_fds,
        failures=tuple(failures),
    )


def require_runner_provisioning_capacity(instance_type: str) -> RunnerProvisioningCapacity:
    """Refuse a shape that cannot carry the frozen dual-lane protocol."""

    capacity = evaluate_runner_provisioning_capacity(instance_type)
    if capacity.sufficient and instance_type == RUNNER_INSTANCE_TYPE:
        return capacity
    failures = ", ".join(capacity.failures or ("noncanonical_instance_type",))
    raise ValueError(
        "Round 5 runner pre-provision capacity refused "
        f"{instance_type!r} for {RUNNER_LANE_COUNT} exact "
        f"{TARGET_CLIENTS_PER_LANE:,}-client lanes ({failures}); "
        f"use {RUNNER_INSTANCE_TYPE}. No client-count, memory-reserve, "
        "FD-reserve, hold, or telemetry reduction is permitted."
    )


def fanin_config_sha256() -> str:
    values = {
        **ConnectionSpikeContract().public_dict,
        "lane_ids": ["competitor", "lakebase"],
        "scheduler": (
            "four-pinned-process-shared-t0-mirrored-two-pair-micro-batches-"
            "bounded-in-flight-unbounded-drain"
        ),
        "sample_schedule": "eight-groups-offset-250ms",
        "tls_mode": "verify-full",
        "client_role": "anti_demo_burst",
        "observer_role": "anti_demo_observer",
        "observer_ready_timeout_seconds": 120.0,
        "observer_quiesce_timeout_seconds": 120.0,
        "observer_retry_seconds": 0.5,
        "observer_sample_max_retries": 2,
        "observer_sample_retry_seconds": 0.25,
        "supported_auth_methods": sorted(SUPPORTED_AUTH_METHODS),
        "loop_monitor_interval_seconds": LOOP_MONITOR_INTERVAL_SECONDS,
        "resource_telemetry_interval_seconds": RESOURCE_TELEMETRY_INTERVAL_SECONDS,
    }
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def evaluate_capacity_preflight(
    *,
    instance_type: str,
    cpu_count: int,
    physical_memory_bytes: int,
    available_memory_bytes: int,
    baseline_rss_bytes: int,
    fd_soft_limit: int,
    fd_hard_limit: int,
    open_fds: int,
    ephemeral_port_first: int,
    ephemeral_port_last: int,
    event_loop_p99_ms: float,
    cpu_calibration_ms: float,
    event_loop_microbatch_p99_ms: float = 0.0,
    event_loop_selector_fanout_peak_ms: float = 0.0,
    event_loop_selector_fanout_baseline_peak_ms: float = 0.0,
    event_loop_selector_fanout_peak_deferred: int = 0,
) -> CapacityPreflight:
    """Evaluate measured runner facts without opening test sockets."""

    measured = (
        physical_memory_bytes,
        available_memory_bytes,
        baseline_rss_bytes,
        fd_soft_limit,
        fd_hard_limit,
        open_fds,
        ephemeral_port_first,
        ephemeral_port_last,
    )
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in measured
    ):
        raise ValueError("capacity measurements must be non-negative integers")
    if any(
        not math.isfinite(value) or value < 0
        for value in (
            event_loop_p99_ms,
            event_loop_microbatch_p99_ms,
            event_loop_selector_fanout_peak_ms,
            event_loop_selector_fanout_baseline_peak_ms,
            cpu_calibration_ms,
        )
    ):
        raise ValueError("capacity timing measurements must be finite and non-negative")

    projected_held = baseline_rss_bytes + RUNNER_LANE_COUNT * CALIBRATED_ONE_LANE_RSS_BYTES
    projected_peak = math.ceil(projected_held * (1 + RAMP_HEADROOM_FRACTION))
    projected_fds = open_fds + RUNNER_LANE_COUNT * TARGET_CLIENTS_PER_LANE + FD_CONTROL_RESERVE
    port_count = ephemeral_port_last - ephemeral_port_first + 1
    failures: list[str] = []
    if instance_type != RUNNER_INSTANCE_TYPE:
        failures.append("runner_instance_type")
    if cpu_count < MIN_RUNNER_CPU_COUNT:
        failures.append("runner_cpu_count")
    if physical_memory_bytes < projected_peak + MEMORY_RESERVE_BYTES:
        failures.append("physical_memory_projection")
    if available_memory_bytes < (projected_peak - baseline_rss_bytes + MEMORY_RESERVE_BYTES):
        failures.append("available_memory_projection")
    if projected_fds > math.floor(fd_soft_limit * FD_USAGE_FRACTION):
        failures.append("file_descriptor_projection")
    if port_count < TARGET_CLIENTS_PER_LANE + EPHEMERAL_PORT_RESERVE_PER_LANE:
        failures.append("ephemeral_port_projection")
    if event_loop_p99_ms > 20.0:
        failures.append("event_loop_calibration")
    if event_loop_microbatch_p99_ms > RUNTIME_MAX_EVENT_LOOP_P99_MS:
        failures.append("event_loop_microbatch_pressure")
    if event_loop_selector_fanout_peak_ms > RUNTIME_MAX_EVENT_LOOP_P99_MS:
        failures.append("event_loop_selector_fanout_pressure")
    if event_loop_selector_fanout_peak_deferred < 0:
        failures.append("event_loop_selector_fanout_invalid")
    if cpu_calibration_ms > 2_000.0:
        failures.append("cpu_calibration")
    return CapacityPreflight(
        instance_type=instance_type,
        cpu_count=cpu_count,
        physical_memory_bytes=physical_memory_bytes,
        available_memory_bytes=available_memory_bytes,
        baseline_rss_bytes=baseline_rss_bytes,
        fd_soft_limit=fd_soft_limit,
        fd_hard_limit=fd_hard_limit,
        open_fds=open_fds,
        ephemeral_port_first=ephemeral_port_first,
        ephemeral_port_last=ephemeral_port_last,
        event_loop_p99_ms=event_loop_p99_ms,
        cpu_calibration_ms=cpu_calibration_ms,
        projected_held_rss_bytes=projected_held,
        projected_peak_rss_bytes=projected_peak,
        projected_fds=projected_fds,
        model_sha256=capacity_model_sha256(),
        event_loop_microbatch_p99_ms=event_loop_microbatch_p99_ms,
        event_loop_selector_fanout_peak_ms=event_loop_selector_fanout_peak_ms,
        event_loop_selector_fanout_baseline_peak_ms=(event_loop_selector_fanout_baseline_peak_ms),
        event_loop_selector_fanout_peak_deferred=(event_loop_selector_fanout_peak_deferred),
        failures=tuple(failures),
    )


@dataclass(frozen=True, slots=True)
class ConnectionSpikeGates:
    exact_count: bool
    zero_failures: bool
    hold: bool
    sampled_queries: bool
    multiplexing: bool
    identity: bool
    observer_separation: bool
    fairness: bool
    telemetry: bool
    cleanup: bool
    failures: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return (
            self.exact_count
            and self.zero_failures
            and self.hold
            and self.sampled_queries
            and self.multiplexing
            and self.identity
            and self.observer_separation
            and self.fairness
            and self.telemetry
            and self.cleanup
            and not self.failures
        )


@dataclass(frozen=True, slots=True)
class FanInProgress:
    """One progress line from the runner, in flight.

    Every field after `sequence` and `lane_id` carries a default because the runner
    emits only the wire fields present in the value it is reporting: a ramp line has
    no `hold_remaining_ms`, and a line printed before the target is reached has no
    `time_to_target_ms`. Absent and zero are different facts here -- "the hold has
    not started" is not "nothing remains" -- so the optional numbers default to None
    rather than 0.0.

    Progress is evidence about a bout in flight, never evidence *of* one: nothing
    here is scored, and `finalize_lane` reads the sealed result instead.
    """

    sequence: int
    lane_id: str
    phase: str = ""
    protocol: str = ""
    schema_version: int = 0
    authenticated_clients: int = 0
    held_clients: int = 0
    terminal_failures: int = 0
    elapsed_ms: float = 0.0
    time_to_target_ms: float | None = None
    hold_remaining_ms: float | None = None
    sampled_queries_succeeded: int = 0
    sampled_queries_failed: int = 0
    event_loop_p99_ms: float | None = None


@dataclass(frozen=True, slots=True)
class ConnectionSpikeLaneResult:
    lane_id: str
    initiated_clients: int
    authenticated_clients: int
    held_clients_at_gate: int
    terminal_failures: int
    failure_codes: Mapping[str, int]
    retries: int
    disconnected_during_hold: int
    time_to_target_ns: int | None
    time_to_target_ms: float | None
    hold_elapsed_ms: float
    sampled_queries_attempted: int
    sampled_queries_succeeded: int
    sampled_queries_failed: int
    preexisting_client_role_sessions: int
    observer_role: str
    client_role: str
    observer_direct: bool
    current_backend_sessions: int
    peak_backend_sessions: int
    unique_backend_pids: int
    distinct_socket_fds: int
    distinct_local_endpoints: int
    socket_identity_sha256: str
    connect_latency_p50_ms: float | None
    connect_latency_p95_ms: float | None
    connect_latency_p99_ms: float | None
    endpoint_host_sha256: str
    credential_sha256: str
    observer_credential_sha256: str
    tls_mode: str
    auth_method: str
    config_sha256: str
    generator_sha256: str
    capacity_model_sha256: str
    telemetry_samples: int
    telemetry_physical_memory_bytes: int
    telemetry_min_available_memory_bytes: int
    telemetry_peak_rss_bytes: int
    telemetry_fd_soft_limit: int
    telemetry_peak_open_fds: int
    telemetry_ephemeral_port_count: int
    telemetry_min_ephemeral_port_reserve: int
    telemetry_peak_event_loop_p99_ms: float
    telemetry_peak_cpu_capacity_fraction: float
    telemetry_failures: tuple[str, ...]
    launch_skew_ms: float
    achieved_elapsed_ms: float
    gates: ConnectionSpikeGates
    telemetry_peak_raw_event_loop_p99_ms: float = 0.0
    telemetry_peak_external_event_loop_p99_ms: float = 0.0
    telemetry_raw_event_loop_warning_count: int = 0
    telemetry_raw_event_loop_ceiling_breaches: int = 0
    observer_sample_attempts: int = 0
    observer_sample_failures: int = 0
    observer_reconnect_attempts: int = 0
    observer_reconnect_elapsed_ms: float = 0.0
    observer_failure_codes: tuple[str, ...] = ()
    observer_sqlstates: tuple[str, ...] = ()
    observer_connection_states: tuple[str, ...] = ()

    @property
    def verified(self) -> bool:
        return self.gates.passed

    # Transitional aliases for code that reads old aggregate names. They carry
    # v2 meanings and are never sufficient to validate a v1 payload.
    @property
    def scheduled_clients(self) -> int:
        return self.initiated_clients

    @property
    def terminal_clients(self) -> int:
        return self.authenticated_clients + self.terminal_failures

    @property
    def successful_clients(self) -> int:
        return self.authenticated_clients

    @property
    def error_clients(self) -> int:
        return self.terminal_failures

    @property
    def application_p99_ms(self) -> float | None:
        return self.connect_latency_p99_ms


@dataclass(frozen=True, slots=True)
class ConnectionSpikeComparison:
    left_lane_id: str
    right_lane_id: str
    outcome: FanInOutcome
    winner_lane_id: str | None
    margin_ns: int | None
    margin_ms: float | None


@dataclass(frozen=True, slots=True)
class ConnectionSpikeArm:
    arm_id: str
    contract_sha256: str
    config_sha256: str
    generator_sha256: str
    capacity_model_sha256: str
    preflight: CapacityPreflight


@dataclass(frozen=True, slots=True)
class ConnectionSpikeRunResult:
    schema_version: int
    protocol: str
    contract_sha256: str
    config_sha256: str
    generator_sha256: str
    capacity_model_sha256: str
    lanes: Mapping[str, ConnectionSpikeLaneResult]
    comparison: ConnectionSpikeComparison | None
    runtime_diagnostics: Mapping[str, object] = field(default_factory=dict)


def compare_lanes(
    left: ConnectionSpikeLaneResult,
    right: ConnectionSpikeLaneResult,
) -> ConnectionSpikeComparison | None:
    """Compare exact shared-T0 time-to-10k only after both lanes verify."""

    if (
        not left.verified
        or not right.verified
        or left.time_to_target_ns is None
        or right.time_to_target_ns is None
        or left.config_sha256 != right.config_sha256
        or left.generator_sha256 != right.generator_sha256
        or left.capacity_model_sha256 != right.capacity_model_sha256
    ):
        return None
    if left.time_to_target_ns == right.time_to_target_ns:
        return ConnectionSpikeComparison(
            left_lane_id=left.lane_id,
            right_lane_id=right.lane_id,
            outcome=FanInOutcome.TIE,
            winner_lane_id=None,
            margin_ns=None,
            margin_ms=None,
        )
    left_won = left.time_to_target_ns < right.time_to_target_ns
    margin_ns = abs(left.time_to_target_ns - right.time_to_target_ns)
    return ConnectionSpikeComparison(
        left_lane_id=left.lane_id,
        right_lane_id=right.lane_id,
        outcome=FanInOutcome.LEFT if left_won else FanInOutcome.RIGHT,
        winner_lane_id=left.lane_id if left_won else right.lane_id,
        margin_ns=margin_ns,
        margin_ms=margin_ns / 1_000_000,
    )


def _count(raw: Mapping[str, object], key: str) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FanInError(f"{key}_invalid")
    return value


def _number(raw: Mapping[str, object], key: str, *, optional: bool = False) -> float | None:
    value = raw.get(key)
    if optional and value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FanInError(f"{key}_invalid")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise FanInError(f"{key}_invalid")
    return parsed


def finalize_lane(
    raw: Mapping[str, object],
    *,
    expected_lane_id: str,
    expected_config_sha256: str,
    expected_generator_sha256: str,
    expected_capacity_model_sha256: str,
    cleanup_verified: bool,
) -> ConnectionSpikeLaneResult:
    """Validate one aggregate runner lane; 9,999 and contamination fail closed."""

    lane_id = str(raw.get("lane_id") or "")
    if lane_id != expected_lane_id:
        raise FanInError("lane_id_invalid")
    initiated = _count(raw, "initiated_clients")
    authenticated = _count(raw, "authenticated_clients")
    held = _count(raw, "held_clients_at_gate")
    terminal_failures = _count(raw, "terminal_failures")
    raw_failure_codes = raw.get("failure_codes")
    if not isinstance(raw_failure_codes, Mapping) or any(
        not isinstance(key, str)
        or not key
        or len(key) > 64
        or not key.replace("_", "").isalnum()
        or isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        for key, value in raw_failure_codes.items()
    ):
        raise FanInError("failure_codes_invalid")
    failure_codes = {str(key): int(value) for key, value in raw_failure_codes.items()}
    retries = _count(raw, "retries")
    disconnected = _count(raw, "disconnected_during_hold")
    attempted = _count(raw, "sampled_queries_attempted")
    succeeded = _count(raw, "sampled_queries_succeeded")
    failed = _count(raw, "sampled_queries_failed")
    preexisting = _count(raw, "preexisting_client_role_sessions")
    current_backends = _count(raw, "current_backend_sessions")
    peak_backends = _count(raw, "peak_backend_sessions")
    unique_pids = _count(raw, "unique_backend_pids")
    distinct_fds = _count(raw, "distinct_socket_fds")
    distinct_endpoints = _count(raw, "distinct_local_endpoints")
    target_ms = _number(raw, "time_to_target_ms", optional=True)
    target_ns_value = raw.get("time_to_target_ns")
    target_ns = None if target_ns_value is None else _count(raw, "time_to_target_ns")
    hold_ms = _number(raw, "hold_elapsed_ms")
    p50 = _number(raw, "connect_latency_p50_ms", optional=True)
    p95 = _number(raw, "connect_latency_p95_ms", optional=True)
    p99 = _number(raw, "connect_latency_p99_ms", optional=True)
    launch_skew = _number(raw, "launch_skew_ms")
    achieved_elapsed = _number(raw, "achieved_elapsed_ms")
    telemetry_samples = _count(raw, "telemetry_samples")
    telemetry_physical_memory = _count(raw, "telemetry_physical_memory_bytes")
    telemetry_min_available_memory = _count(raw, "telemetry_min_available_memory_bytes")
    telemetry_peak_rss = _count(raw, "telemetry_peak_rss_bytes")
    telemetry_fd_soft_limit = _count(raw, "telemetry_fd_soft_limit")
    telemetry_peak_open_fds = _count(raw, "telemetry_peak_open_fds")
    telemetry_ephemeral_port_count = _count(raw, "telemetry_ephemeral_port_count")
    telemetry_min_ephemeral_reserve = _count(raw, "telemetry_min_ephemeral_port_reserve")
    telemetry_peak_loop = _number(raw, "telemetry_peak_event_loop_p99_ms")
    telemetry_peak_raw_loop = _number(raw, "telemetry_peak_raw_event_loop_p99_ms", optional=True)
    telemetry_peak_external_loop = _number(
        raw, "telemetry_peak_external_event_loop_p99_ms", optional=True
    )
    telemetry_raw_warning_count = (
        _count(raw, "telemetry_raw_event_loop_warning_count")
        if "telemetry_raw_event_loop_warning_count" in raw
        else 0
    )
    telemetry_raw_ceiling_breaches = (
        _count(raw, "telemetry_raw_event_loop_ceiling_breaches")
        if "telemetry_raw_event_loop_ceiling_breaches" in raw
        else 0
    )
    observer_sample_attempts = (
        _count(raw, "observer_sample_attempts") if "observer_sample_attempts" in raw else 0
    )
    observer_sample_failures = (
        _count(raw, "observer_sample_failures") if "observer_sample_failures" in raw else 0
    )
    observer_reconnect_attempts = (
        _count(raw, "observer_reconnect_attempts") if "observer_reconnect_attempts" in raw else 0
    )
    observer_reconnect_elapsed = (
        _number(raw, "observer_reconnect_elapsed_ms")
        if "observer_reconnect_elapsed_ms" in raw
        else 0.0
    )
    observer_lists: dict[str, tuple[str, ...]] = {}
    for field_name in (
        "observer_failure_codes",
        "observer_sqlstates",
        "observer_connection_states",
    ):
        values = raw.get(field_name, [])
        if not isinstance(values, list) or any(
            not isinstance(value, str)
            or not value
            or len(value) > 64
            or not value.replace("_", "").isalnum()
            for value in values
        ):
            raise FanInError(f"{field_name}_invalid")
        observer_lists[field_name] = tuple(values)
    telemetry_peak_cpu = _number(raw, "telemetry_peak_cpu_capacity_fraction")
    raw_telemetry_failures = raw.get("telemetry_failures")
    if not isinstance(raw_telemetry_failures, list) or any(
        not isinstance(value, str)
        or not value
        or len(value) > 64
        or not value.replace("_", "").isalnum()
        for value in raw_telemetry_failures
    ):
        raise FanInError("telemetry_failures_invalid")
    telemetry_failures = tuple(raw_telemetry_failures)
    assert (
        hold_ms is not None
        and launch_skew is not None
        and achieved_elapsed is not None
        and telemetry_peak_loop is not None
        and telemetry_peak_cpu is not None
    )

    config_sha256 = str(raw.get("config_sha256") or "")
    generator_sha256 = str(raw.get("generator_sha256") or "")
    model_sha256 = str(raw.get("capacity_model_sha256") or "")
    endpoint_digest = str(raw.get("endpoint_host_sha256") or "")
    credential_digest = str(raw.get("credential_sha256") or "")
    observer_credential_digest = str(raw.get("observer_credential_sha256") or "")
    auth_method = str(raw.get("auth_method") or "")
    socket_digest = str(raw.get("socket_identity_sha256") or "")
    digest_values = (
        config_sha256,
        generator_sha256,
        model_sha256,
        endpoint_digest,
        credential_digest,
        observer_credential_digest,
        socket_digest,
    )
    digests_valid = all(
        len(value) == 64 and all(character in "0123456789abcdef" for character in value)
        for value in digest_values
    )
    exact_count = (
        initiated == TARGET_CLIENTS_PER_LANE
        and authenticated == TARGET_CLIENTS_PER_LANE
        and held == TARGET_CLIENTS_PER_LANE
        and distinct_fds == TARGET_CLIENTS_PER_LANE
        and distinct_endpoints == TARGET_CLIENTS_PER_LANE
        and target_ns is not None
        and target_ms is not None
    )
    zero_failures = (
        terminal_failures == 0
        and sum(failure_codes.values()) == terminal_failures
        and retries == MAX_RETRIES
        and disconnected == 0
    )
    hold = hold_ms >= HOLD_SECONDS * 1_000
    samples = (
        attempted == SAMPLED_QUERIES_PER_LANE
        and succeeded == SAMPLED_QUERIES_PER_LANE
        and failed == 0
    )
    multiplexing = (
        1 <= current_backends < TARGET_CLIENTS_PER_LANE
        and 1 <= peak_backends < TARGET_CLIENTS_PER_LANE
        and 1 <= unique_pids < TARGET_CLIENTS_PER_LANE
    )
    client_role = str(raw.get("client_role") or "")
    observer_role = str(raw.get("observer_role") or "")
    observer_direct = raw.get("observer_direct") is True
    observer_separation = (
        observer_direct
        and bool(client_role)
        and bool(observer_role)
        and client_role != observer_role
        and preexisting == 0
    )
    identity = (
        digests_valid
        and str(raw.get("tls_mode") or "") == "verify-full"
        and auth_method in SUPPORTED_AUTH_METHODS
        and p50 is not None
        and p95 is not None
        and p99 is not None
        and p50 <= p95 <= p99
        and config_sha256 == expected_config_sha256
        and generator_sha256 == expected_generator_sha256
        and model_sha256 == expected_capacity_model_sha256
        and raw.get("identity_verified") is True
    )
    fairness = launch_skew <= MAX_LAUNCH_SKEW_MS and raw.get("fairness_verified") is True
    telemetry = (
        raw.get("telemetry_verified") is True
        and telemetry_samples >= 1
        and not telemetry_failures
        and telemetry_physical_memory >= telemetry_peak_rss + MEMORY_RESERVE_BYTES
        and telemetry_min_available_memory >= MEMORY_RESERVE_BYTES
        and telemetry_peak_open_fds <= math.floor(telemetry_fd_soft_limit * FD_USAGE_FRACTION)
        and telemetry_ephemeral_port_count
        >= TARGET_CLIENTS_PER_LANE + EPHEMERAL_PORT_RESERVE_PER_LANE
        and telemetry_min_ephemeral_reserve >= EPHEMERAL_PORT_RESERVE_PER_LANE
        and telemetry_peak_loop <= RUNTIME_MAX_EVENT_LOOP_P99_MS
        and telemetry_peak_cpu <= RUNTIME_MAX_CPU_CAPACITY_FRACTION
    )
    failures: list[str] = []
    for passed, label in (
        (exact_count, "exact_count"),
        (zero_failures, "zero_failures"),
        (hold, "hold"),
        (samples, "sampled_queries"),
        (multiplexing, "multiplexing"),
        (identity, "identity"),
        (observer_separation, "observer_separation"),
        (fairness, "fairness"),
        (telemetry, "telemetry"),
        (cleanup_verified, "cleanup"),
    ):
        if not passed:
            failures.append(label)

    return ConnectionSpikeLaneResult(
        lane_id=lane_id,
        initiated_clients=initiated,
        authenticated_clients=authenticated,
        held_clients_at_gate=held,
        terminal_failures=terminal_failures,
        failure_codes=failure_codes,
        retries=retries,
        disconnected_during_hold=disconnected,
        time_to_target_ns=target_ns,
        time_to_target_ms=target_ms,
        hold_elapsed_ms=hold_ms,
        sampled_queries_attempted=attempted,
        sampled_queries_succeeded=succeeded,
        sampled_queries_failed=failed,
        preexisting_client_role_sessions=preexisting,
        observer_role=observer_role,
        client_role=client_role,
        observer_direct=observer_direct,
        current_backend_sessions=current_backends,
        peak_backend_sessions=peak_backends,
        unique_backend_pids=unique_pids,
        distinct_socket_fds=distinct_fds,
        distinct_local_endpoints=distinct_endpoints,
        socket_identity_sha256=socket_digest,
        connect_latency_p50_ms=p50,
        connect_latency_p95_ms=p95,
        connect_latency_p99_ms=p99,
        endpoint_host_sha256=endpoint_digest,
        credential_sha256=credential_digest,
        observer_credential_sha256=observer_credential_digest,
        tls_mode=str(raw.get("tls_mode") or ""),
        auth_method=auth_method,
        config_sha256=config_sha256,
        generator_sha256=generator_sha256,
        capacity_model_sha256=model_sha256,
        telemetry_samples=telemetry_samples,
        telemetry_physical_memory_bytes=telemetry_physical_memory,
        telemetry_min_available_memory_bytes=telemetry_min_available_memory,
        telemetry_peak_rss_bytes=telemetry_peak_rss,
        telemetry_fd_soft_limit=telemetry_fd_soft_limit,
        telemetry_peak_open_fds=telemetry_peak_open_fds,
        telemetry_ephemeral_port_count=telemetry_ephemeral_port_count,
        telemetry_min_ephemeral_port_reserve=telemetry_min_ephemeral_reserve,
        telemetry_peak_event_loop_p99_ms=telemetry_peak_loop,
        telemetry_peak_raw_event_loop_p99_ms=(
            telemetry_peak_raw_loop if telemetry_peak_raw_loop is not None else telemetry_peak_loop
        ),
        telemetry_peak_external_event_loop_p99_ms=(
            telemetry_peak_external_loop if telemetry_peak_external_loop is not None else 0.0
        ),
        telemetry_raw_event_loop_warning_count=telemetry_raw_warning_count,
        telemetry_raw_event_loop_ceiling_breaches=telemetry_raw_ceiling_breaches,
        observer_sample_attempts=observer_sample_attempts,
        observer_sample_failures=observer_sample_failures,
        observer_reconnect_attempts=observer_reconnect_attempts,
        observer_reconnect_elapsed_ms=float(observer_reconnect_elapsed or 0.0),
        observer_failure_codes=observer_lists["observer_failure_codes"],
        observer_sqlstates=observer_lists["observer_sqlstates"],
        observer_connection_states=observer_lists["observer_connection_states"],
        telemetry_peak_cpu_capacity_fraction=telemetry_peak_cpu,
        telemetry_failures=telemetry_failures,
        launch_skew_ms=launch_skew,
        achieved_elapsed_ms=achieved_elapsed,
        gates=ConnectionSpikeGates(
            exact_count=exact_count,
            zero_failures=zero_failures,
            hold=hold,
            sampled_queries=samples,
            multiplexing=multiplexing,
            identity=identity,
            observer_separation=observer_separation,
            fairness=fairness,
            telemetry=telemetry,
            cleanup=cleanup_verified,
            failures=tuple(failures),
        ),
    )

#: Where the sealed CA bundle lives on the runner. Mirrors
#: `runner.connection_spike_runner.TRUST_BUNDLE_PATH`; the decoder compares the request
#: against its own constant, so a drift here is refused rather than silently trusted.
TRUST_BUNDLE_PATH = "/opt/lakebase-anti-demo/round5/round5-ca.pem"

#: The two lanes a fan-in request must name, no more and no fewer.
RUNTIME_LANE_IDS = frozenset({"lakebase", "competitor"})


def fanin_preflight_request(
    *,
    run_id: str,
    runner_instance_type: str,
    contract_sha256: str,
    config_sha256: str,
    generator_sha256: str,
    capacity_model_sha256: str,
) -> dict[str, object]:
    """The capacity-preflight request, which carries exactly nine keys.

    The runner compares the key set for equality, not containment, so an extra field is
    a refusal rather than something ignored. That is deliberate: a preflight is what
    decides whether 10,000 clients per lane can be held at all, and a request carrying
    fields the runner does not understand is a request built by a different version.
    """

    return {
        "protocol": FANIN_PROTOCOL,
        "schema_version": FANIN_SCHEMA_VERSION,
        "action": "preflight",
        "run_id": run_id,
        "runner_instance_type": runner_instance_type,
        "contract_sha256": contract_sha256,
        "config_sha256": config_sha256,
        "generator_sha256": generator_sha256,
        "capacity_model_sha256": capacity_model_sha256,
    }


def fanin_run_request(
    *,
    run_id: str,
    runner_instance_type: str = RUNNER_INSTANCE_TYPE,
    contract_sha256: str,
    config_sha256: str,
    generator_sha256: str,
    capacity_model_sha256: str,
    trust_bundle_sha256: str,
    lakebase_credential_sha256: str,
    lakebase_observer_credential_sha256: str,
    competitor_credential_sha256: str,
    competitor_observer_credential_sha256: str,
    competitor_credential_id: str,
    targets: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """The bout request: two lanes, one shared start, no retries.

    `baseline_auth` is asymmetric on purpose and the runner enforces it. The competitor
    lane carries `credential_id` because Aurora and RDS are two separately sealed
    credentials and the runner has to know which one it is holding; the Lakebase lane
    has exactly one, so passing an id there is refused as an over-specified request.

    Digests are passed in rather than recomputed here so that the caller proves it is
    sending the arm it armed. Recomputing them inside this function would make a stale
    arm indistinguishable from a current one, which is the whole thing the seal exists
    to catch.
    """

    if len(targets) != RUNNER_LANE_COUNT:
        raise FanInError("targets_invalid")
    lane_ids = {str(target.get("lane_id") or "") for target in targets}
    if lane_ids != set(RUNTIME_LANE_IDS):
        raise FanInError("targets_invalid")

    return {
        "protocol": FANIN_PROTOCOL,
        "schema_version": FANIN_SCHEMA_VERSION,
        "action": "run",
        "run_id": run_id,
        # The runner re-measures capacity before it opens a socket and compares the
        # instance type it was told against the shape the model was calibrated on. An
        # absent value there is not a missing field, it is a failed capacity gate
        # reported as `runner_capacity_insufficient_runner_instance_type`, which reads
        # like a small machine rather than like a request that forgot to say.
        "runner_instance_type": runner_instance_type,
        "contract_sha256": contract_sha256,
        "config_sha256": config_sha256,
        "generator_sha256": generator_sha256,
        "capacity_model_sha256": capacity_model_sha256,
        "trust_bundle_path": TRUST_BUNDLE_PATH,
        "trust_bundle_sha256": trust_bundle_sha256,
        "baseline_auth": {
            "lakebase": {
                "credential_sha256": lakebase_credential_sha256,
                "observer_credential_sha256": lakebase_observer_credential_sha256,
            },
            "competitor": {
                "credential_sha256": competitor_credential_sha256,
                "observer_credential_sha256": competitor_observer_credential_sha256,
                "credential_id": competitor_credential_id,
            },
        },
        "targets": [dict(target) for target in targets],
    }

def fanin_generator_sha256(generator: object | None = None) -> str:
    """The digest of the generator the runner will execute.

    Mirrors runner.round5_fanin.generator_sha256, which hashes that file's own bytes.
    The server needs it to arm a bout, because the runner compares the digest in the
    request against the file it is about to run and refuses a mismatch: a generator that
    still answers while differing from the armed contract produces numbers that look
    exactly like a measurement.

    The path is resolved rather than hardcoded so this stays correct if the runner
    directory moves, and passing an explicit path keeps it testable.
    """

    import hashlib as _hashlib
    from pathlib import Path as _Path

    path = _Path(generator) if generator is not None else (
        _Path(__file__).resolve().parents[1] / "runner" / "round5_fanin.py"
    )
    return _hashlib.sha256(path.read_bytes()).hexdigest()
