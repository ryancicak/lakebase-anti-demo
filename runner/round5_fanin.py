"""Lightweight PostgreSQL TLS/native-password fan-in generator for Round 5.

This module is imported by the sealed runner from one fixed installation path.
It uses one asyncio protocol object per client and no thread/process per
connection.  psycopg is used only for the two direct observer connections.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import gc
import hashlib
import hmac
import json
import math
import os
import re
import resource
import socket
import ssl
import struct
import threading
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import psycopg

# The generator is installed beside external_io.py on the instance, not inside a
# `runner` package, and run_connection_spike.sh execs the interpreter with -I so the
# script directory is not on sys.path either. Try the package, then the sibling.
try:
    from .external_io import connect_runner_database
except ImportError:
    import sys as _external_sys
    from pathlib import Path as _ExternalPath

    _external_directory = str(_ExternalPath(__file__).resolve().parent)
    if _external_directory not in _external_sys.path:
        _external_sys.path.insert(0, _external_directory)
    from external_io import connect_runner_database

PROTOCOL = "round5-fanin-v4"
SCHEMA_VERSION = 4
SAFETY_EVIDENCE_VERSION = 5
TARGET_CLIENTS_PER_LANE = 10_000
# c7i.2xlarge is load-compatible and evaluatable but not selected: the runner
# shape is also wired into the cost model (server/cost_model.py ec2_m6i_xlarge_hour
# and four other call sites), so changing it changes user-facing cost receipts.
# Revisit once a diagnostic's normalized CPU evidence justifies the housekeeping
# headroom.
RUNNER_INSTANCE_TYPE = "c7i.2xlarge"
#: The lanes this protocol knows. A v3 physical runner executes exactly one of
#: them, so its capacity model projects one retained 10,000-client fan-in.
LANE_IDS = frozenset({"lakebase", "competitor"})
RUNNER_LANE_COUNT = 1
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
MIN_ADMISSION_CONCURRENCY_PER_LANE = 2
ADMISSION_RECOVERY_STEP_PER_LANE = 1
ADMISSION_PRESSURE_INTERVALS = 2
ADMISSION_RECOVERY_CLEAN_INTERVALS = 3
# Mirrored creation quantum and pipeline depth are separate concerns.
# MICRO_BATCH_SIZE keeps both lanes interleaved identically; it must stay small.
# LANE_CONNECT_CONCURRENCY bounds how many of those mirrored clients may be
# in flight at once. Draining every quantum made the ramp latency-bound: a
# client costs five to six round trips, so the loop sat idle between batches
# and 2,500 clients per lane became 1,250 serial round-trip waits.
#: Measured cost of one TLS handshake on the event loop, on the sealed runner shape: about
#: 1.6 ms of CPU. Recorded as a constant because the concurrency below is derived from it, so a
#: future runner shape or cipher change has one number to revisit rather than a magic 32.
TLS_HANDSHAKE_CPU_MS = 1.6
#: How many connects may be in flight per lane per worker.
#:
#: This is the size of the handshake batch a single event-loop turn can be handed, so it is
#: what decides whether one turn can exceed `RUNTIME_MAX_EVENT_LOOP_P99_MS` (50 ms, defined
#: below) on handshake CPU alone. At 32 it could and did: 55 ms in one callback, and the ramp
#: broke out at a few hundred clients with no connection failures at all.
#:
#: Half the ceiling divided by the handshake cost, so a full batch spends about half the budget
#: and leaves the rest for the turn's other work. The result is also below
#: `OWNED_STALL_READY_BATCH`, so a batch this size is no longer big enough to be classified as
#: our own amplification.
#:
#: Capping the drain instead is not an option; see `READY_CALLBACK_BATCH_LIMIT` below.
LANE_CONNECT_CONCURRENCY = int((50.0 / 2) / TLS_HANDSHAKE_CPU_MS)
PARTITION_CLIENTS_PER_LANE = TARGET_CLIENTS_PER_LANE // WORKER_COUNT
MAX_IN_FLIGHT_CONNECTS_PER_LANE = LANE_CONNECT_CONCURRENCY * WORKER_COUNT
# Readiness burst used by the no-endpoint preflight probe. One worker loop can
# only have LANE_CONNECT_CONCURRENCY * RUNNER_LANE_COUNT (64) connects in flight,
# so probing the whole-runner bound is four times the concurrent readiness any
# single loop actually sees. Kept as deliberate headroom.
SELECTOR_FANOUT_PROBE_SOCKETS = LANE_CONNECT_CONCURRENCY * WORKER_COUNT * RUNNER_LANE_COUNT
SELECTOR_FANOUT_PROBE_CALLBACK_CPU_NS = 150_000
# Wakeups per delivered readiness event. Exactly 1.0 is optimal; a capped
# selector re-reports what it dropped, so K=16 over 1,024 descriptors measured
# 32.5x. Slack covers only genuine spurious wakeups.
MAX_SELECTOR_WAKEUP_AMPLIFICATION = 1.05
MAX_LAUNCH_SKEW_MS = 10.0
CONNECT_TIMEOUT_SECONDS = 20.0
OBSERVER_READY_TIMEOUT_SECONDS = 120.0
OBSERVER_RETRY_SECONDS = 0.5
OBSERVER_QUIESCE_TIMEOUT_SECONDS = 120.0
#: Consecutive identical readings that count as a settled baseline.
#:
#: Three, half a second apart, so a pool being actively borrowed from cannot look settled
#: between two samples while a genuinely idle warm pool clears the gate in about a second.
OBSERVER_QUIESCE_STABLE_READINGS = 3
#: The ceiling on that baseline, mirroring `connection_fanin.MAX_PREEXISTING_CLIENT_SESSIONS`
#: and derived the same way, from the most connections this protocol has in flight to one
#: lane at once.
MAX_PREEXISTING_CLIENT_SESSIONS = LANE_CONNECT_CONCURRENCY * WORKER_COUNT
OBSERVER_SAMPLE_MAX_RETRIES = 2
OBSERVER_SAMPLE_RETRY_SECONDS = 0.25
RUN_TIMEOUT_SECONDS = 600.0
RUNTIME_MAX_EVENT_LOOP_P99_MS = 50.0
RAW_WALL_LAG_WARNING_MS = 50.0
RAW_WALL_LAG_CEILING_MS = 250.0
RAW_WALL_LAG_MAX_BREACHES = 3
OWNED_STALL_MIN_THREAD_CPU_MS = 5.0
#: Ready-batch size retained as diagnostic context, never as independent proof
#: that a delay belongs to the generator.
#:
#: One physical worker deliberately keeps ``LANE_CONNECT_CONCURRENCY`` connects in flight.  The
#: ownership threshold must not move when the number of physical lanes changes: doing that in v3
#: lowered it from 30 to 15, exactly the ordinary one-lane batch, and a normal turn was enough to
#: stop a healthy ramp. Even a larger ready batch can be an ordinary coalesced kernel wakeup, so
#: only proportional CPU or GC evidence below is allowed to classify a stall as generator-owned.
OWNED_STALL_READY_BATCH = LANE_CONNECT_CONCURRENCY * 2
#: Nearest-rank p99 needs at least 100 observations before one outlier no
#: longer *is* the percentile. At the 10 ms monitor cadence this costs one
#: second before a sustained generator problem can stop a ramp.
EVENT_LOOP_P99_MIN_SAMPLES = 100
#: Ten seconds of monitor history bounds memory while retaining far more than
#: the minimum p99 population.
EVENT_LOOP_P99_WINDOW_SAMPLES = 1_000
RUNTIME_MAX_CPU_CAPACITY_FRACTION = 0.85
MEMORY_RESERVE_BYTES = 768 * 1024 * 1024
FD_CONTROL_RESERVE = 256
FD_USAGE_FRACTION = 0.80
EPHEMERAL_PORT_RESERVE_PER_LANE = 2_000
CALIBRATED_ONE_LANE_RSS_BYTES = int(3.11 * 1024**3)
RAMP_HEADROOM_FRACTION = 0.15
TLS_MODE = "verify-full"
AUTH_CLEARTEXT = "tls-cleartext-password"
AUTH_SCRAM = "scram-sha-256"
SUPPORTED_AUTH_METHODS = frozenset({AUTH_CLEARTEXT, AUTH_SCRAM})
CLIENT_ROLE = "anti_demo_burst"
OBSERVER_ROLE = "anti_demo_observer"
APP_PREFIX = "anti-demo-r5-fanin"
SSL_REQUEST = struct.pack("!II", 8, 80877103)
PROGRESS_PREFIX = "PROGRESS_JSON:"
PROGRESS_OUTPUT_BUDGET_BYTES = 12_000
CONNECTION_DIAGNOSTIC_LIMIT = 16
PROGRESS_WIRE_FIELDS = (
    "protocol",
    "schema_version",
    "lane_id",
    "phase",
    "initiated_clients",
    "authenticated_clients",
    "held_clients",
    "peak_held_clients",
    "terminal_failures",
    "elapsed_ms",
    "time_to_target_ms",
    "hold_remaining_ms",
    "sampled_queries_succeeded",
    "sampled_queries_failed",
    "event_loop_p99_ms",
    "milestone",
    "milestone_monotonic_ns",
    "first_socket_initiated_ms",
    "first_client_authenticated_ms",
)
_SHA256 = frozenset("0123456789abcdef")
_progress_sequence = 0
_progress_output_bytes = 0
_progress_callback: Callable[[Mapping[str, object]], None] | None = None
LOOP_MONITOR_INTERVAL_SECONDS = 0.01
RESOURCE_TELEMETRY_INTERVAL_SECONDS = 0.25
# Both limits are unbounded (<= 0). Capping either one breaks the asyncio
# invariant that a turn consumes the readiness it was handed, and Linux epoll
# registrations are level-triggered: an event dropped at the poll boundary is
# re-reported on the next poll, having already cost a full kernel copy-out and
# one selectors.py tuple/key lookup per ready descriptor. Servicing N
# simultaneously ready descriptors K at a time therefore costs O(N**2 / K), not
# O(N), and the un-run descriptors stay readable so the ready queue grows
# faster than it drains. Do not reintroduce a cap here to make a per-turn
# latency gate look better; it lengthens total ramp time, which is the quantity
# the sealed idle window actually constrains.
READY_CALLBACK_BATCH_LIMIT = 0
SELECTOR_EVENT_BATCH_LIMIT = 0
_runtime_diagnostics: RuntimeDiagnostics | None = None
_worker_execution_context: dict[str, object] | None = None
# Keyed by code object, so it is bounded by the number of distinct callbacks in
# the program rather than by callback volume. Without it every sampled callback
# paid an f-string plus a re.sub on the hottest path in the process.
_CALLBACK_TYPE_CACHE: dict[object, str] = {}
_CALLBACK_TYPE_CACHE_MAX = 512
_socket_states_cache: dict[str, int] = {}
_socket_states_cache_ns = 0


class FanInProtocolError(RuntimeError):
    pass


class HardSafetyCode(StrEnum):
    RSS_RESERVE_EXHAUSTED = "rss_reserve_exhausted"
    AVAILABLE_MEMORY_RESERVE_EXHAUSTED = "available_memory_reserve_exhausted"
    FILE_DESCRIPTOR_RESERVE_EXHAUSTED = "file_descriptor_reserve_exhausted"
    EPHEMERAL_PORT_RESERVE_EXHAUSTED = "ephemeral_port_reserve_exhausted"
    MANDATORY_EVIDENCE_MISSING = "mandatory_safety_evidence_missing"
    MANDATORY_EVIDENCE_MALFORMED = "mandatory_safety_evidence_malformed"


class AdvisoryTelemetryCode(StrEnum):
    EVENT_LOOP_PRESSURE = "event_loop_pressure"
    HOST_SCHEDULING_INSTABILITY = "host_scheduling_instability"
    CPU_PRESSURE = "cpu_pressure"
    EVENT_LOOP_CALIBRATION = "event_loop_calibration"
    EVENT_LOOP_MICROBATCH_PRESSURE = "event_loop_microbatch_pressure"
    EVENT_LOOP_SELECTOR_FANOUT_PRESSURE = "event_loop_selector_fanout_pressure"
    EVENT_LOOP_SELECTOR_FANOUT_AMPLIFIED = "event_loop_selector_fanout_amplified"
    EVENT_LOOP_SELECTOR_FANOUT_DEFERRED = "event_loop_selector_fanout_deferred"
    CPU_CALIBRATION = "cpu_calibration"


HARD_SAFETY_CODES = frozenset(code.value for code in HardSafetyCode)
ADVISORY_TELEMETRY_CODES = frozenset(code.value for code in AdvisoryTelemetryCode)


def classify_safety_code(code: str) -> str:
    """Classify only the protocol's closed typed vocabulary.

    Prefix matching is deliberately forbidden. A new or misspelled code is a
    protocol mismatch, not permission to silently weaken a hard failure.
    """

    if code in HARD_SAFETY_CODES:
        return "hard"
    if code in ADVISORY_TELEMETRY_CODES:
        return "advisory"
    raise FanInProtocolError("unknown_safety_code")


@dataclass(slots=True)
class AdmissionController:
    """Bound future admission concurrency without cancelling admitted clients."""

    lane_count: int
    current_concurrency: int = field(init=False)
    minimum_concurrency: int = field(init=False)
    maximum_concurrency: int = field(init=False)
    observed_minimum_concurrency: int = field(init=False)
    reductions: int = 0
    recoveries: int = 0
    throttled_ns: int = 0
    consecutive_clean_intervals: int = 0
    consecutive_pressure_intervals: int = 0
    consecutive_cpu_pressure_intervals: int = 0
    consecutive_loop_pressure_intervals: int = 0
    latest_loop_pressured: bool = False

    def __post_init__(self) -> None:
        if self.lane_count < 1:
            raise FanInProtocolError("admission_controller_lane_count_invalid")
        self.minimum_concurrency = self.lane_count * MIN_ADMISSION_CONCURRENCY_PER_LANE
        self.maximum_concurrency = self.lane_count * LANE_CONNECT_CONCURRENCY
        self.current_concurrency = self.maximum_concurrency
        self.observed_minimum_concurrency = self.current_concurrency

    def observe_interval(self, *, pressured: bool) -> None:
        self.observe_cpu_interval(pressured=pressured)

    def _observe_pressure(self, *, pressured: bool, signal: str) -> None:
        counter_name = (
            "consecutive_cpu_pressure_intervals"
            if signal == "cpu"
            else "consecutive_loop_pressure_intervals"
        )
        if pressured:
            self.consecutive_clean_intervals = 0
            next_count = min(
                ADMISSION_PRESSURE_INTERVALS,
                int(getattr(self, counter_name)) + 1,
            )
            setattr(self, counter_name, next_count)
            self.consecutive_pressure_intervals = max(
                self.consecutive_cpu_pressure_intervals,
                self.consecutive_loop_pressure_intervals,
            )
            if next_count < ADMISSION_PRESSURE_INTERVALS:
                return
            setattr(self, counter_name, 0)
            next_value = max(
                self.minimum_concurrency,
                self.current_concurrency // 2,
            )
            if next_value < self.current_concurrency:
                self.current_concurrency = next_value
                self.reductions += 1
                self.observed_minimum_concurrency = min(
                    self.observed_minimum_concurrency,
                    next_value,
                )
            return
        setattr(self, counter_name, 0)
        self.consecutive_pressure_intervals = max(
            self.consecutive_cpu_pressure_intervals,
            self.consecutive_loop_pressure_intervals,
        )
        if signal != "cpu":
            return
        if self.latest_loop_pressured:
            self.consecutive_clean_intervals = 0
            return
        self.consecutive_clean_intervals = min(
            ADMISSION_RECOVERY_CLEAN_INTERVALS,
            self.consecutive_clean_intervals + 1,
        )
        if self.consecutive_clean_intervals < ADMISSION_RECOVERY_CLEAN_INTERVALS:
            return
        self.consecutive_clean_intervals = 0
        next_value = min(
            self.maximum_concurrency,
            self.current_concurrency
            + self.lane_count * ADMISSION_RECOVERY_STEP_PER_LANE,
        )
        if next_value > self.current_concurrency:
            self.current_concurrency = next_value
            self.recoveries += 1

    def observe_loop_interval(self, *, pressured: bool) -> None:
        self.latest_loop_pressured = pressured
        self._observe_pressure(pressured=pressured, signal="loop")

    def observe_cpu_interval(self, *, pressured: bool) -> None:
        self._observe_pressure(pressured=pressured, signal="cpu")

    def record_throttle(self, started_ns: int) -> None:
        if self.current_concurrency < self.maximum_concurrency:
            self.throttled_ns += max(0, time.monotonic_ns() - started_ns)

    def public_dict(self) -> dict[str, int | float]:
        return {
            "admission_controller_min_concurrency": self.observed_minimum_concurrency,
            "admission_controller_reductions": self.reductions,
            "admission_controller_recoveries": self.recoveries,
            "admission_controller_throttled_ms": self.throttled_ns / 1_000_000,
            "admission_controller_recovery_hysteresis_intervals": (
                ADMISSION_RECOVERY_CLEAN_INTERVALS
            ),
            "admission_controller_pressure_hysteresis_intervals": (
                ADMISSION_PRESSURE_INTERVALS
            ),
        }


class BoundedReadyDeque(collections.deque[object]):
    """Observe CPython ready-queue depth and FIFO age without reordering it.

    `batch_limit` <= 0 reports the true length, which is the only correct
    behaviour: see READY_CALLBACK_BATCH_LIMIT. A positive limit is retained for
    regression coverage of the historical capped behaviour and must not be used
    for a scored run.

    Enqueue times live in a parallel deque kept in lockstep with the callbacks
    rather than in a dict keyed by `id()`. Handles are short-lived, so `id()`
    both collides after deallocation and collapses duplicate appends of the same
    persistent reader handle -- which is exactly what a level-triggered re-poll
    produces. That made `oldest_fifo_ready_age_ms` read 0.0 precisely when the
    backlog was deepest.
    """

    def __init__(self, values: object = (), *, batch_limit: int) -> None:
        super().__init__(values)  # type: ignore[arg-type]
        self.batch_limit = batch_limit
        now_ns = time.perf_counter_ns()
        self._enqueued_ns: collections.deque[int] = collections.deque(
            now_ns for _ in range(super().__len__())
        )
        self.last_oldest_age_ms = 0.0

    def __len__(self) -> int:
        actual = super().__len__()
        return actual if self.batch_limit <= 0 else min(actual, self.batch_limit)

    def append(self, value: object) -> None:
        self._enqueued_ns.append(time.perf_counter_ns())
        super().append(value)

    def appendleft(self, value: object) -> None:
        self._enqueued_ns.appendleft(time.perf_counter_ns())
        super().appendleft(value)

    def popleft(self) -> object:
        value = super().popleft()
        if self._enqueued_ns:
            enqueued_ns = self._enqueued_ns.popleft()
            self.last_oldest_age_ms = max(0.0, (time.perf_counter_ns() - enqueued_ns) / 1_000_000)
        else:
            self.last_oldest_age_ms = 0.0
        return value

    def clear(self) -> None:
        self._enqueued_ns.clear()
        super().clear()

    def actual_length(self) -> int:
        return super().__len__()


def _ready_backlog_size(loop: asyncio.AbstractEventLoop) -> int:
    ready = getattr(loop, "_ready", ())
    actual_length = getattr(ready, "actual_length", None)
    return int(actual_length()) if callable(actual_length) else len(ready)


@dataclass(frozen=True, slots=True)
class LoopDelaySample:
    wall_lag_ms: float
    process_cpu_ms: float
    thread_cpu_ms: float
    scheduler_wait_ms: float | None
    voluntary_context_switches: int | None
    involuntary_context_switches: int | None
    active_handshakes: int
    selector_batch_size: int
    ready_batch_size: int
    oldest_fifo_ready_age_ms: float
    callback_interval: dict[str, object]
    phase: str
    gc_pause_ms: float
    generator_owned: bool

    @property
    def generator_owned_lag_ms(self) -> float:
        return self.wall_lag_ms if self.generator_owned else 0.0


def classify_generator_owned_stall(
    *,
    wall_lag_ms: float,
    process_cpu_ms: float,
    thread_cpu_ms: float,
    ready_batch_size: int,
    selector_batch_size: int,
    phase: str,
    gc_pause_ms: float,
) -> bool:
    """Require proportional local work before blaming a wall-clock delay.

    Ready and selector batches are captured for diagnosis, but neither is
    causal evidence: the kernel may coalesce ordinary network readiness after
    descheduling the pinned process. CPU or GC time must explain at least half
    of the delayed interval.
    """

    if wall_lag_ms <= RUNTIME_MAX_EVENT_LOOP_P99_MS:
        return False
    # Proportional in both currencies. The CPU has to account for at least half the delay
    # before the delay is ours: 20 ms of work does not explain a 78 ms turn, and an absolute
    # 5 ms floor on the thread clause used to say it did, short-circuiting the process clause
    # that already required a share. The floor is kept as a lower bound so a tiny lag with a
    # tiny CPU reading cannot be attributed either way on noise.
    del ready_batch_size, selector_batch_size, phase
    corroborating_work_ms = max(OWNED_STALL_MIN_THREAD_CPU_MS, wall_lag_ms * 0.5)
    cpu_owned = (
        thread_cpu_ms >= corroborating_work_ms
        or process_cpu_ms >= corroborating_work_ms
    )
    gc_owned = gc_pause_ms >= corroborating_work_ms
    return cpu_owned or gc_owned


@dataclass(slots=True)
class RuntimeDiagnostics:
    phase_calls: dict[str, int] = field(default_factory=dict)
    phase_total_ms: dict[str, float] = field(default_factory=dict)
    phase_max_ms: dict[str, float] = field(default_factory=dict)
    gc_started_ns: dict[int, int] = field(default_factory=dict)
    gc_collections: dict[int, int] = field(default_factory=dict)
    gc_total_pause_ms: dict[int, float] = field(default_factory=dict)
    gc_max_pause_ms: dict[int, float] = field(default_factory=dict)
    loop_samples: int = 0
    peak_loop_lag_ms: float = 0.0
    peak_generator_owned_loop_lag_ms: float = 0.0
    peak_external_loop_lag_ms: float = 0.0
    peak_loop_process_cpu_ms: float = 0.0
    peak_loop_thread_cpu_ms: float = 0.0
    peak_scheduler_wait_ms: float = 0.0
    raw_lag_warnings: int = 0
    raw_lag_ceiling_breaches: int = 0
    peak_ready_batch: int = 0
    selector_calls: int = 0
    selector_wakeups: int = 0
    peak_selector_wakeups: int = 0
    selector_wait_total_ms: float = 0.0
    selector_wait_max_ms: float = 0.0
    deferred_selector_events: int = 0
    peak_deferred_selector_events: int = 0
    selector_event_batch_limit: int = SELECTOR_EVENT_BATCH_LIMIT
    last_selector_batch: int = 0
    peak_selector_batch: int = 0
    interval_selector_batch_max: int = 0
    callback_profiles: dict[str, dict[str, int | float]] = field(default_factory=dict)
    callback_cpu_total_ms: float = 0.0
    callback_cpu_max_ms: float = 0.0
    current_callback_type: str = "none"
    interval_callback_profiles: dict[str, dict[str, int | float]] = field(default_factory=dict)
    interval_callback_cpu_total_ms: float = 0.0
    interval_callback_cpu_max_ms: float = 0.0
    interval_callback_calls: int = 0
    latest_callback_interval: dict[str, object] = field(default_factory=dict)
    _callback_clock_ns: int = 0
    run_once_calls: int = 0
    run_once_cpu_total_ms: float = 0.0
    run_once_cpu_max_ms: float = 0.0
    run_once_ready_cpu_total_ms: float = 0.0
    telemetry_calls: int = 0
    telemetry_thread_cpu_total_ms: float = 0.0
    telemetry_thread_cpu_max_ms: float = 0.0
    telemetry_native_tids: set[int] = field(default_factory=set)
    native_ssl_calls: int = 0
    native_ssl_thread_cpu_total_ms: float = 0.0
    native_ssl_thread_cpu_max_ms: float = 0.0
    active_handshakes: int = 0
    peak_active_handshakes: int = 0
    latest_phase: str = "idle"
    latest_phase_elapsed_ms: float = 0.0
    latest_phase_finished_ns: int = 0
    latest_gc_pause_ms: float = 0.0
    latest_gc_finished_ns: int = 0
    significant_loop_delays: list[LoopDelaySample] = field(default_factory=list)
    significant_stall_envelopes: list[dict[str, object]] = field(default_factory=list)
    controlled_gc: bool = False
    gc_initial_collect_ms: float = 0.0
    gc_final_collect_ms: float = 0.0
    _selector: object | None = None
    _original_select: Callable[..., object] | None = None
    _loop: asyncio.AbstractEventLoop | None = None
    _original_ready: object | None = None
    _original_run_once: Callable[[], None] | None = None
    _original_handle_run: Callable[..., object] | None = None
    _original_ssl_do_handshake: Callable[..., object] | None = None

    def record(self, phase: str, started_ns: int) -> None:
        finished_ns = time.perf_counter_ns()
        elapsed_ms = (finished_ns - started_ns) / 1_000_000
        self.record_elapsed(phase, elapsed_ms, finished_ns=finished_ns)

    def record_elapsed(
        self,
        phase: str,
        elapsed_ms: float,
        *,
        finished_ns: int | None = None,
    ) -> None:
        self.phase_calls[phase] = self.phase_calls.get(phase, 0) + 1
        self.phase_total_ms[phase] = self.phase_total_ms.get(phase, 0.0) + elapsed_ms
        self.phase_max_ms[phase] = max(self.phase_max_ms.get(phase, 0.0), elapsed_ms)
        self.latest_phase = phase
        self.latest_phase_elapsed_ms = elapsed_ms
        self.latest_phase_finished_ns = finished_ns or time.perf_counter_ns()

    def gc_callback(self, phase: str, info: dict[str, int]) -> None:
        generation = int(info.get("generation", -1))
        if phase == "start":
            self.gc_started_ns[generation] = time.perf_counter_ns()
            return
        started_ns = self.gc_started_ns.pop(generation, None)
        if phase != "stop" or started_ns is None:
            return
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        self.gc_collections[generation] = self.gc_collections.get(generation, 0) + 1
        self.gc_total_pause_ms[generation] = (
            self.gc_total_pause_ms.get(generation, 0.0) + elapsed_ms
        )
        self.gc_max_pause_ms[generation] = max(
            self.gc_max_pause_ms.get(generation, 0.0), elapsed_ms
        )
        self.latest_gc_pause_ms = elapsed_ms
        self.latest_gc_finished_ns = time.perf_counter_ns()

    def install_selector_probe(self, loop: asyncio.AbstractEventLoop) -> None:
        selector = getattr(loop, "_selector", None)
        select = getattr(selector, "select", None)
        if selector is None or not callable(select):
            return
        self._selector = selector
        self._original_select = select

        def profiled_select(timeout: float | None = None) -> object:
            assert self._original_select is not None
            started_ns = time.perf_counter_ns()
            events = self._original_select(timeout)
            elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
            wakeups = len(events)  # type: ignore[arg-type]
            self.selector_calls += 1
            self.selector_wakeups += wakeups
            self.selector_wait_total_ms += elapsed_ms
            self.selector_wait_max_ms = max(self.selector_wait_max_ms, elapsed_ms)
            self.peak_selector_wakeups = max(self.peak_selector_wakeups, wakeups)
            # selectors.py has already paid this batch in full: one kernel
            # copy-out plus one key lookup and one tuple per ready descriptor.
            # Truncating here throws away completed work, and because epoll
            # registrations are level-triggered the dropped descriptors are
            # re-reported on the next poll and charged again. Deliver all of it.
            # Any positive limit is retained only so the historical capped
            # behaviour stays testable, and it now records what it discards.
            limit = self.selector_event_batch_limit
            if limit > 0 and wakeups > limit:
                delivered = events[:limit]  # type: ignore[index]
                deferred = wakeups - limit
                self.deferred_selector_events += deferred
                self.peak_deferred_selector_events = max(
                    self.peak_deferred_selector_events, deferred
                )
            else:
                delivered = events
            batch = len(delivered)  # type: ignore[arg-type]
            self.last_selector_batch = batch
            self.peak_selector_batch = max(self.peak_selector_batch, batch)
            self.interval_selector_batch_max = max(self.interval_selector_batch_max, batch)
            return delivered

        selector.select = profiled_select

    def restore_selector_probe(self) -> None:
        if self._selector is not None and self._original_select is not None:
            self._selector.select = self._original_select
        self._selector = None
        self._original_select = None

    def install_ready_batch_limit(self, loop: asyncio.AbstractEventLoop) -> None:
        ready = getattr(loop, "_ready", None)
        if ready is None or isinstance(ready, BoundedReadyDeque):
            return
        bounded = BoundedReadyDeque(ready, batch_limit=READY_CALLBACK_BATCH_LIMIT)
        ready.clear()
        self._loop = loop
        self._original_ready = ready
        loop._ready = bounded  # type: ignore[attr-defined]

    @staticmethod
    def _callback_type(handle: object) -> str:
        callback = getattr(handle, "_callback", None)
        owner = getattr(callback, "__self__", None)
        if isinstance(owner, asyncio.Task):
            coroutine = owner.get_coro()
            code = getattr(coroutine, "cr_code", None)
            key: object = code if code is not None else type(coroutine)
            cached = _CALLBACK_TYPE_CACHE.get(key)
            if cached is not None:
                return cached
            coroutine_name = str(
                getattr(
                    code,
                    "co_qualname",
                    getattr(coroutine, "__qualname__", "unknown"),
                )
            )
            value = f"asyncio.Task:{coroutine_name}"
            resolved = re.sub(r"[^A-Za-z0-9_.:<>=-]", "_", value)[:160]
            if len(_CALLBACK_TYPE_CACHE) < _CALLBACK_TYPE_CACHE_MAX:
                _CALLBACK_TYPE_CACHE[key] = resolved
            return resolved
        key = getattr(callback, "__code__", None) or callback.__class__
        cached = _CALLBACK_TYPE_CACHE.get(key)
        if cached is not None:
            return cached
        module = str(getattr(callback, "__module__", callback.__class__.__module__))
        qualname = str(getattr(callback, "__qualname__", callback.__class__.__qualname__))
        value = f"{module}.{qualname}"
        resolved = re.sub(r"[^A-Za-z0-9_.<>-]", "_", value)[:160]
        if len(_CALLBACK_TYPE_CACHE) < _CALLBACK_TYPE_CACHE_MAX:
            _CALLBACK_TYPE_CACHE[key] = resolved
        return resolved

    def install_execution_probes(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._original_run_once is None:
            self._original_run_once = loop._run_once  # type: ignore[attr-defined]

            def profiled_run_once() -> None:
                assert self._original_run_once is not None
                started_thread_ns = time.thread_time_ns()
                self._callback_clock_ns = started_thread_ns
                callback_cpu_before = self.callback_cpu_total_ms
                self._original_run_once()
                elapsed_ms = (time.thread_time_ns() - started_thread_ns) / 1_000_000
                self.run_once_calls += 1
                self.run_once_cpu_total_ms += elapsed_ms
                self.run_once_cpu_max_ms = max(self.run_once_cpu_max_ms, elapsed_ms)
                self.run_once_ready_cpu_total_ms += max(
                    0.0, self.callback_cpu_total_ms - callback_cpu_before
                )

            loop._run_once = profiled_run_once  # type: ignore[attr-defined,method-assign]

        if self._original_handle_run is None:
            self._original_handle_run = asyncio.Handle._run
            original_handle_run = self._original_handle_run

            def profiled_handle_run(handle: asyncio.Handle) -> None:
                try:
                    original_handle_run(handle)
                finally:
                    finished_thread_ns = time.thread_time_ns()
                    elapsed_ms = max(
                        0.0,
                        (finished_thread_ns - self._callback_clock_ns) / 1_000_000,
                    )
                    self._callback_clock_ns = finished_thread_ns
                    self.interval_callback_calls += 1
                    sampled = elapsed_ms >= 0.05 or self.interval_callback_calls % 16 == 0
                    callback_type = self._callback_type(handle) if sampled else "<unsampled-fast>"
                    self.callback_cpu_total_ms += elapsed_ms
                    self.callback_cpu_max_ms = max(self.callback_cpu_max_ms, elapsed_ms)
                    if sampled:
                        profile = self.callback_profiles.setdefault(
                            callback_type,
                            {"calls": 0, "cpu_total_ms": 0.0, "cpu_max_ms": 0.0},
                        )
                        profile["calls"] = int(profile["calls"]) + 1
                        profile["cpu_total_ms"] = float(profile["cpu_total_ms"]) + elapsed_ms
                        profile["cpu_max_ms"] = max(float(profile["cpu_max_ms"]), elapsed_ms)
                        interval_profile = self.interval_callback_profiles.setdefault(
                            callback_type,
                            {
                                "calls": 0,
                                "cpu_total_ms": 0.0,
                                "cpu_max_ms": 0.0,
                            },
                        )
                        interval_profile["calls"] = int(interval_profile["calls"]) + 1
                        interval_profile["cpu_total_ms"] = (
                            float(interval_profile["cpu_total_ms"]) + elapsed_ms
                        )
                        interval_profile["cpu_max_ms"] = max(
                            float(interval_profile["cpu_max_ms"]),
                            elapsed_ms,
                        )
                    self.interval_callback_cpu_total_ms += elapsed_ms
                    self.interval_callback_cpu_max_ms = max(
                        self.interval_callback_cpu_max_ms, elapsed_ms
                    )
                    self.current_callback_type = callback_type

            asyncio.Handle._run = profiled_handle_run

        if self._original_ssl_do_handshake is None:
            self._original_ssl_do_handshake = ssl.SSLObject.do_handshake
            original_ssl_do_handshake = self._original_ssl_do_handshake

            def profiled_ssl_do_handshake(value: ssl.SSLObject) -> object:
                started_thread_ns = time.thread_time_ns()
                try:
                    return original_ssl_do_handshake(value)
                finally:
                    elapsed_ms = (time.thread_time_ns() - started_thread_ns) / 1_000_000
                    self.native_ssl_calls += 1
                    self.native_ssl_thread_cpu_total_ms += elapsed_ms
                    self.native_ssl_thread_cpu_max_ms = max(
                        self.native_ssl_thread_cpu_max_ms, elapsed_ms
                    )

            ssl.SSLObject.do_handshake = profiled_ssl_do_handshake

    def consume_interval_selector_batch_max(self) -> int:
        """Largest selector batch since the previous heartbeat, then reset.

        Zero means no selector poll completed during the interval, which is
        itself evidence: the loop was inside callbacks rather than polling.
        """

        value = self.interval_selector_batch_max
        self.interval_selector_batch_max = 0
        return value

    def consume_callback_interval(self) -> dict[str, object]:
        value = {
            "calls": self.interval_callback_calls,
            "cpu_total_ms": self.interval_callback_cpu_total_ms,
            "cpu_max_ms": self.interval_callback_cpu_max_ms,
            "profiles": {
                key: {
                    "calls": int(profile["calls"]),
                    "cpu_total_ms": round(float(profile["cpu_total_ms"]), 6),
                    "cpu_max_ms": round(float(profile["cpu_max_ms"]), 6),
                }
                for key, profile in sorted(
                    self.interval_callback_profiles.items(),
                    key=lambda item: float(item[1]["cpu_total_ms"]),
                    reverse=True,
                )[:8]
            },
        }
        self.latest_callback_interval = value
        self.interval_callback_profiles.clear()
        self.interval_callback_cpu_total_ms = 0.0
        self.interval_callback_cpu_max_ms = 0.0
        self.interval_callback_calls = 0
        return value

    def restore_execution_probes(self) -> None:
        if self._loop is not None and self._original_run_once is not None:
            self._loop._run_once = self._original_run_once  # type: ignore[attr-defined,method-assign]
        self._original_run_once = None
        if self._original_handle_run is not None:
            asyncio.Handle._run = self._original_handle_run
        self._original_handle_run = None
        if self._original_ssl_do_handshake is not None:
            ssl.SSLObject.do_handshake = self._original_ssl_do_handshake
        self._original_ssl_do_handshake = None

    def restore_ready_batch_limit(self) -> None:
        if self._loop is None or self._original_ready is None:
            return
        bounded = getattr(self._loop, "_ready", ())
        self._original_ready.extend(bounded)  # type: ignore[attr-defined]
        self._loop._ready = self._original_ready  # type: ignore[attr-defined]
        self._loop = None
        self._original_ready = None

    def handshake_started(self) -> None:
        self.active_handshakes += 1
        self.peak_active_handshakes = max(self.peak_active_handshakes, self.active_handshakes)

    def handshake_finished(self) -> None:
        self.active_handshakes = max(0, self.active_handshakes - 1)

    def observe_loop(self, sample: LoopDelaySample) -> None:
        self.loop_samples += 1
        self.peak_loop_lag_ms = max(self.peak_loop_lag_ms, sample.wall_lag_ms)
        self.peak_generator_owned_loop_lag_ms = max(
            self.peak_generator_owned_loop_lag_ms,
            sample.generator_owned_lag_ms,
        )
        if not sample.generator_owned:
            self.peak_external_loop_lag_ms = max(self.peak_external_loop_lag_ms, sample.wall_lag_ms)
        self.peak_loop_process_cpu_ms = max(self.peak_loop_process_cpu_ms, sample.process_cpu_ms)
        self.peak_loop_thread_cpu_ms = max(self.peak_loop_thread_cpu_ms, sample.thread_cpu_ms)
        if sample.scheduler_wait_ms is not None:
            self.peak_scheduler_wait_ms = max(self.peak_scheduler_wait_ms, sample.scheduler_wait_ms)
        if sample.wall_lag_ms > RAW_WALL_LAG_WARNING_MS:
            self.raw_lag_warnings += 1
        if sample.wall_lag_ms > RAW_WALL_LAG_CEILING_MS:
            self.raw_lag_ceiling_breaches += 1
        self.peak_ready_batch = max(self.peak_ready_batch, sample.ready_batch_size)
        if sample.wall_lag_ms > 20.0:
            self.significant_loop_delays.append(sample)
            self.significant_loop_delays.sort(key=lambda value: value.wall_lag_ms, reverse=True)
            del self.significant_loop_delays[32:]
            self.significant_stall_envelopes.append(
                {
                    **worker_crash_context(),
                    "wall_lag_ms": sample.wall_lag_ms,
                    "process_cpu_ms": sample.process_cpu_ms,
                    "thread_cpu_ms": sample.thread_cpu_ms,
                    "scheduler_wait_ms": sample.scheduler_wait_ms,
                    "active_handshakes": sample.active_handshakes,
                    "selector_batch_size": sample.selector_batch_size,
                    "ready_batch_size": sample.ready_batch_size,
                    "oldest_fifo_ready_age_ms": (sample.oldest_fifo_ready_age_ms),
                    "callback_interval": sample.callback_interval,
                    "callback_type": self.current_callback_type,
                    "callback_cpu_max_ms": self.callback_cpu_max_ms,
                    "callback_cpu_total_ms": self.callback_cpu_total_ms,
                    "run_once_cpu_max_ms": self.run_once_cpu_max_ms,
                    "run_once_ready_cpu_total_ms": (self.run_once_ready_cpu_total_ms),
                    "native_ssl_calls": self.native_ssl_calls,
                    "native_ssl_thread_cpu_total_ms": (self.native_ssl_thread_cpu_total_ms),
                    "telemetry_thread_cpu_total_ms": (self.telemetry_thread_cpu_total_ms),
                    "phase": sample.phase,
                    "phase_elapsed_ms": self.latest_phase_elapsed_ms,
                    "gc_pause_ms": sample.gc_pause_ms,
                    "generator_owned": sample.generator_owned,
                }
            )
            self.significant_stall_envelopes.sort(
                key=lambda value: float(value["wall_lag_ms"]),
                reverse=True,
            )
            del self.significant_stall_envelopes[32:]

    def public_dict(self) -> dict[str, object]:
        return {
            "phase_calls": dict(sorted(self.phase_calls.items())),
            "phase_total_ms": {
                key: round(value, 6) for key, value in sorted(self.phase_total_ms.items())
            },
            "phase_max_ms": {
                key: round(value, 6) for key, value in sorted(self.phase_max_ms.items())
            },
            "gc_collections": {
                str(key): value for key, value in sorted(self.gc_collections.items())
            },
            "gc_total_pause_ms": {
                str(key): round(value, 6) for key, value in sorted(self.gc_total_pause_ms.items())
            },
            "gc_max_pause_ms": {
                str(key): round(value, 6) for key, value in sorted(self.gc_max_pause_ms.items())
            },
            "loop_samples": self.loop_samples,
            "peak_loop_lag_ms": self.peak_loop_lag_ms,
            "peak_raw_loop_lag_ms": self.peak_loop_lag_ms,
            "peak_generator_owned_loop_lag_ms": (self.peak_generator_owned_loop_lag_ms),
            "peak_external_loop_lag_ms": self.peak_external_loop_lag_ms,
            "peak_loop_process_cpu_ms": self.peak_loop_process_cpu_ms,
            "peak_loop_thread_cpu_ms": self.peak_loop_thread_cpu_ms,
            "peak_scheduler_wait_ms": self.peak_scheduler_wait_ms,
            "raw_lag_warnings": self.raw_lag_warnings,
            "raw_lag_ceiling_breaches": self.raw_lag_ceiling_breaches,
            "peak_ready_batch": self.peak_ready_batch,
            "selector_calls": self.selector_calls,
            "selector_wakeups": self.selector_wakeups,
            "peak_selector_wakeups": self.peak_selector_wakeups,
            "selector_wait_total_ms": round(self.selector_wait_total_ms, 6),
            "selector_wait_max_ms": round(self.selector_wait_max_ms, 6),
            "deferred_selector_events": self.deferred_selector_events,
            "peak_deferred_selector_events": self.peak_deferred_selector_events,
            "last_selector_batch": self.last_selector_batch,
            "peak_selector_batch": self.peak_selector_batch,
            "callback_profiles": {
                key: {
                    "calls": int(value["calls"]),
                    "cpu_total_ms": round(float(value["cpu_total_ms"]), 6),
                    "cpu_max_ms": round(float(value["cpu_max_ms"]), 6),
                }
                for key, value in sorted(
                    self.callback_profiles.items(),
                    key=lambda item: float(item[1]["cpu_total_ms"]),
                    reverse=True,
                )[:16]
            },
            "callback_cpu_total_ms": round(self.callback_cpu_total_ms, 6),
            "callback_cpu_max_ms": round(self.callback_cpu_max_ms, 6),
            "current_callback_type": self.current_callback_type,
            "run_once_calls": self.run_once_calls,
            "run_once_cpu_total_ms": round(self.run_once_cpu_total_ms, 6),
            "run_once_cpu_max_ms": round(self.run_once_cpu_max_ms, 6),
            "run_once_ready_cpu_total_ms": round(self.run_once_ready_cpu_total_ms, 6),
            "telemetry_calls": self.telemetry_calls,
            "telemetry_thread_cpu_total_ms": round(self.telemetry_thread_cpu_total_ms, 6),
            "telemetry_thread_cpu_max_ms": round(self.telemetry_thread_cpu_max_ms, 6),
            "telemetry_native_tids": sorted(self.telemetry_native_tids),
            "native_ssl_calls": self.native_ssl_calls,
            "native_ssl_thread_cpu_total_ms": round(self.native_ssl_thread_cpu_total_ms, 6),
            "native_ssl_thread_cpu_max_ms": round(self.native_ssl_thread_cpu_max_ms, 6),
            "peak_active_handshakes": self.peak_active_handshakes,
            "latest_phase": self.latest_phase,
            "latest_phase_elapsed_ms": self.latest_phase_elapsed_ms,
            "significant_loop_delays": [
                {
                    "wall_lag_ms": value.wall_lag_ms,
                    "process_cpu_ms": value.process_cpu_ms,
                    "thread_cpu_ms": value.thread_cpu_ms,
                    "scheduler_wait_ms": value.scheduler_wait_ms,
                    "voluntary_context_switches": value.voluntary_context_switches,
                    "involuntary_context_switches": (value.involuntary_context_switches),
                    "active_handshakes": value.active_handshakes,
                    "selector_batch_size": value.selector_batch_size,
                    "ready_batch_size": value.ready_batch_size,
                    "oldest_fifo_ready_age_ms": value.oldest_fifo_ready_age_ms,
                    "callback_interval": value.callback_interval,
                    "phase": value.phase,
                    "gc_pause_ms": value.gc_pause_ms,
                    "generator_owned": value.generator_owned,
                }
                for value in self.significant_loop_delays
            ],
            "significant_stall_envelopes": self.significant_stall_envelopes,
            "controlled_gc": self.controlled_gc,
            "gc_initial_collect_ms": self.gc_initial_collect_ms,
            "gc_final_collect_ms": self.gc_final_collect_ms,
        }


def _record_phase(phase: str, started_ns: int) -> None:
    if _runtime_diagnostics is not None:
        _runtime_diagnostics.record(phase, started_ns)


def _set_worker_operation(
    operation: str,
    *,
    phase: str | None = None,
    wave: int | None = None,
) -> None:
    if _worker_execution_context is None:
        return
    _worker_execution_context["operation"] = operation
    if phase is not None:
        _worker_execution_context["phase"] = phase
    if wave is not None:
        _worker_execution_context["wave"] = wave


def worker_crash_context() -> dict[str, object]:
    context = _worker_execution_context or {}
    lanes = context.get("lanes")
    observers = context.get("observers")
    frozen_lane_counts = context.get("lane_counts")
    lane_counts: dict[str, object] = (
        dict(frozen_lane_counts) if isinstance(frozen_lane_counts, Mapping) else {}
    )
    if not lane_counts and isinstance(lanes, Sequence):
        for lane in lanes:
            if not isinstance(lane, LaneRuntime):
                continue
            lane_counts[lane.lane_id] = {
                "initiated": lane.initiated,
                "authenticated": lane.authenticated,
                "held": sum(client.ready and not client.closed.done() for client in lane.clients),
            }
    observer_states: dict[str, object] = {}
    if isinstance(observers, Sequence):
        for observer in observers:
            if not isinstance(observer, DirectObserver):
                continue
            observer_states[observer.lane_id] = {
                "connection_state": observer.evidence.last_connection_state,
                "failure_code": observer.evidence.last_failure_code,
                "sqlstate": observer.evidence.last_sqlstate,
                "reconnect_attempts": observer.evidence.reconnect_attempts,
                "reconnect_elapsed_ms": observer.evidence.reconnect_elapsed_ms,
            }
    return {
        "worker_id": context.get("worker_id"),
        "worker_cpu": context.get("worker_cpu"),
        "phase": context.get("phase", "unknown"),
        "operation": context.get("operation", "unknown"),
        "wave": context.get("wave", 0),
        "partition_start": context.get("partition_start"),
        "partition_end_exclusive": context.get("partition_end_exclusive"),
        "lanes": lane_counts,
        "observers": observer_states,
    }


def _scheduler_context() -> tuple[int | None, int | None, int | None, int | None]:
    """Return loop-thread run-queue wait and context-switch counters on Linux."""

    running_ns: int | None = None
    waiting_ns: int | None = None
    voluntary: int | None = None
    involuntary: int | None = None
    try:
        fields = Path("/proc/thread-self/schedstat").read_text().split()
        running_ns, waiting_ns = int(fields[0]), int(fields[1])
    except (OSError, ValueError, IndexError):
        pass
    try:
        for line in Path("/proc/thread-self/status").read_text().splitlines():
            if line.startswith("voluntary_ctxt_switches:"):
                voluntary = int(line.split(":", 1)[1])
            elif line.startswith("nonvoluntary_ctxt_switches:"):
                involuntary = int(line.split(":", 1)[1])
    except (OSError, ValueError):
        pass
    return running_ns, waiting_ns, voluntary, involuntary


def _counter_delta(after: int | None, before: int | None) -> int | None:
    if after is None or before is None:
        return None
    return max(0, after - before)


def _loop_delay_sample(
    *,
    wall_lag_ms: float,
    process_cpu_started_ns: int,
    thread_cpu_started_ns: int,
    scheduler_started: tuple[int | None, int | None, int | None, int | None],
    sample_started_ns: int,
    active_handshakes: int,
    ready_batch_size: int,
    diagnostics: RuntimeDiagnostics,
    callback_interval: dict[str, object],
) -> LoopDelaySample:
    scheduler_finished = _scheduler_context()
    ready = getattr(asyncio.get_running_loop(), "_ready", None)
    oldest_fifo_ready_age_ms = float(getattr(ready, "last_oldest_age_ms", 0.0))
    process_cpu_ms = max(0.0, (time.process_time_ns() - process_cpu_started_ns) / 1_000_000)
    thread_cpu_ms = max(0.0, (time.thread_time_ns() - thread_cpu_started_ns) / 1_000_000)
    scheduler_wait_ns = _counter_delta(scheduler_finished[1], scheduler_started[1])
    phase = (
        diagnostics.latest_phase
        if diagnostics.latest_phase_finished_ns >= sample_started_ns
        else "event_loop_wait"
    )
    gc_pause_ms = (
        diagnostics.latest_gc_pause_ms
        if diagnostics.latest_gc_finished_ns >= sample_started_ns
        else 0.0
    )
    process_switches = _counter_delta(scheduler_finished[2], scheduler_started[2])
    involuntary_switches = _counter_delta(scheduler_finished[3], scheduler_started[3])
    # The interval's worst batch, not the last poll before the heartbeat ran.
    # A delayed heartbeat spans many polls, so reporting the final one made a
    # large fan-in burst read as a handful of events.
    selector_batch_size = diagnostics.consume_interval_selector_batch_max()
    owned = classify_generator_owned_stall(
        wall_lag_ms=wall_lag_ms,
        process_cpu_ms=process_cpu_ms,
        thread_cpu_ms=thread_cpu_ms,
        ready_batch_size=ready_batch_size,
        selector_batch_size=selector_batch_size,
        phase=phase,
        gc_pause_ms=gc_pause_ms,
    )
    return LoopDelaySample(
        wall_lag_ms=wall_lag_ms,
        process_cpu_ms=process_cpu_ms,
        thread_cpu_ms=thread_cpu_ms,
        scheduler_wait_ms=(
            scheduler_wait_ns / 1_000_000 if scheduler_wait_ns is not None else None
        ),
        voluntary_context_switches=process_switches,
        involuntary_context_switches=involuntary_switches,
        active_handshakes=max(active_handshakes, diagnostics.active_handshakes),
        selector_batch_size=selector_batch_size,
        ready_batch_size=ready_batch_size,
        oldest_fifo_ready_age_ms=oldest_fifo_ready_age_ms,
        callback_interval=callback_interval,
        phase=phase,
        gc_pause_ms=gc_pause_ms,
        generator_owned=owned,
    )


@dataclass(slots=True)
class ControlledGC:
    diagnostics: RuntimeDiagnostics
    was_enabled: bool = True
    active: bool = False

    def start(self) -> None:
        self.was_enabled = gc.isenabled()
        started_ns = time.perf_counter_ns()
        gc.collect()
        self.diagnostics.gc_initial_collect_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        gc.disable()
        self.active = True
        self.diagnostics.controlled_gc = True

    def stop(self) -> None:
        if not self.active:
            return
        if self.was_enabled:
            gc.enable()
        started_ns = time.perf_counter_ns()
        gc.collect()
        self.diagnostics.gc_final_collect_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        if not self.was_enabled:
            gc.disable()
        self.active = False


#: How many entries of an unbounded diagnostic map survive into the returned envelope.
#:
#: These maps are read to find what cost the most, never to enumerate everything, so the
#: expensive entries are the ones worth carrying. Four workers multiply whatever is kept, and
#: the whole envelope has to fit in the roughly 24,000 characters SSM will hand back, so this
#: is deliberately small. What is dropped is counted rather than silently omitted.
DIAGNOSTIC_MAP_LIMIT = 5


def bounded_diagnostic_map(value: object) -> object:
    """Keep the most expensive entries of a numeric-valued map, and count the rest.

    Returns anything that is not such a map unchanged, so this can be applied across a
    diagnostic dict without knowing which of its values are maps.
    """

    if not isinstance(value, Mapping) or not value:
        return value
    try:
        ordered = sorted(value.items(), key=lambda item: float(item[1]), reverse=True)
    except (TypeError, ValueError):
        # Not a numeric map, so there is no "most expensive" to keep. Bound it by key order
        # instead, which is at least stable, and still say how much was left out.
        ordered = sorted(value.items(), key=lambda item: str(item[0]))
    kept = dict(ordered[:DIAGNOSTIC_MAP_LIMIT])
    dropped = len(ordered) - len(kept)
    if dropped:
        kept["_entries_omitted"] = dropped
    return kept


def bounded_worker_diagnostics(value: Mapping[str, object]) -> dict[str, object]:
    """Bound every unbounded map in one worker's diagnostics."""

    return {key: bounded_diagnostic_map(item) for key, item in value.items()}


def canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def contract_values() -> dict[str, int | float | str]:
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol": PROTOCOL,
        "target_clients_per_lane": TARGET_CLIENTS_PER_LANE,
        "hold_seconds": HOLD_SECONDS,
        "sampled_queries_per_lane": SAMPLED_QUERIES_PER_LANE,
        "sample_groups": SAMPLE_GROUPS,
        "clients_per_sample_group": CLIENTS_PER_SAMPLE_GROUP,
        "max_retries": MAX_RETRIES,
        "initial_wave_size": INITIAL_WAVE_SIZE,
        "min_wave_size": MIN_WAVE_SIZE,
        "micro_batch_size": MICRO_BATCH_SIZE,
        "min_admission_concurrency_per_lane": MIN_ADMISSION_CONCURRENCY_PER_LANE,
        "admission_recovery_step_per_lane": ADMISSION_RECOVERY_STEP_PER_LANE,
        "admission_pressure_intervals": ADMISSION_PRESSURE_INTERVALS,
        "admission_recovery_clean_intervals": ADMISSION_RECOVERY_CLEAN_INTERVALS,
        "lane_connect_concurrency": LANE_CONNECT_CONCURRENCY,
        "ready_callback_batch_limit": READY_CALLBACK_BATCH_LIMIT,
        "selector_event_batch_limit": SELECTOR_EVENT_BATCH_LIMIT,
        "worker_count": WORKER_COUNT,
        "partition_clients_per_lane": PARTITION_CLIENTS_PER_LANE,
        "max_in_flight_connects_per_lane": MAX_IN_FLIGHT_CONNECTS_PER_LANE,
        "max_launch_skew_ms": MAX_LAUNCH_SKEW_MS,
        "connect_timeout_seconds": CONNECT_TIMEOUT_SECONDS,
        "run_timeout_seconds": RUN_TIMEOUT_SECONDS,
        "supported_auth_methods": ",".join(sorted(SUPPORTED_AUTH_METHODS)),
    }


def contract_sha256() -> str:
    return hashlib.sha256(canonical_json(contract_values())).hexdigest()


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
        "safety_evidence_version": SAFETY_EVIDENCE_VERSION,
        "event_loop_gate_semantics": "advisory_pacing_proportional_cpu_or_gc_only",
        "pressure_hysteresis_cadence": "cpu_and_loop_independent",
        "port_accounting_semantics": "every_sample_count_equals_used_plus_remaining",
        "raw_wall_lag_warning_ms": RAW_WALL_LAG_WARNING_MS,
        "raw_wall_lag_ceiling_ms": RAW_WALL_LAG_CEILING_MS,
        "raw_wall_lag_max_breaches": RAW_WALL_LAG_MAX_BREACHES,
        "owned_stall_min_thread_cpu_ms": OWNED_STALL_MIN_THREAD_CPU_MS,
        "owned_stall_ready_batch": OWNED_STALL_READY_BATCH,
        "event_loop_p99_min_samples": EVENT_LOOP_P99_MIN_SAMPLES,
        "event_loop_p99_window_samples": EVENT_LOOP_P99_WINDOW_SAMPLES,
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
    return hashlib.sha256(canonical_json(values)).hexdigest()


def generator_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def config_sha256() -> str:
    values = {
        **contract_values(),
        "lane_ids": ["competitor", "lakebase"],
        "scheduler": (
            "four-pinned-process-one-physical-lane-micro-batches-"
            "bounded-in-flight-unbounded-drain"
        ),
        "sample_schedule": "eight-groups-offset-250ms",
        "tls_mode": TLS_MODE,
        "client_role": CLIENT_ROLE,
        "observer_role": OBSERVER_ROLE,
        "observer_ready_timeout_seconds": OBSERVER_READY_TIMEOUT_SECONDS,
        "observer_quiesce_timeout_seconds": OBSERVER_QUIESCE_TIMEOUT_SECONDS,
        "observer_quiesce_stable_readings": OBSERVER_QUIESCE_STABLE_READINGS,
        "max_preexisting_client_sessions": MAX_PREEXISTING_CLIENT_SESSIONS,
        "observer_retry_seconds": OBSERVER_RETRY_SECONDS,
        "observer_sample_max_retries": OBSERVER_SAMPLE_MAX_RETRIES,
        "observer_sample_retry_seconds": OBSERVER_SAMPLE_RETRY_SECONDS,
        "supported_auth_methods": sorted(SUPPORTED_AUTH_METHODS),
        "loop_monitor_interval_seconds": LOOP_MONITOR_INTERVAL_SECONDS,
        "resource_telemetry_interval_seconds": RESOURCE_TELEMETRY_INTERVAL_SECONDS,
    }
    return hashlib.sha256(canonical_json(values)).hexdigest()


def _digest(value: object, error: str) -> str:
    text = str(value or "")
    if len(text) != 64 or any(character not in _SHA256 for character in text):
        raise FanInProtocolError(error)
    return text


def _memory() -> tuple[int, int]:
    values: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        key, _, raw = line.partition(":")
        if key in {"MemTotal", "MemAvailable"}:
            values[key] = int(raw.strip().split()[0]) * 1024
    if set(values) != {"MemTotal", "MemAvailable"}:
        raise FanInProtocolError("runner_memory_unavailable")
    return values["MemTotal"], values["MemAvailable"]


def _rss_bytes() -> int:
    resident_pages = int(Path("/proc/self/statm").read_text(encoding="ascii").split()[1])
    return resident_pages * os.sysconf("SC_PAGE_SIZE")


def _open_fds() -> int:
    return len(os.listdir("/proc/self/fd"))


def _process_rss_and_fds(process_ids: Sequence[int]) -> tuple[int, int]:
    rss = 0
    open_fds = 0
    for process_id in process_ids:
        if isinstance(process_id, bool) or not isinstance(process_id, int) or process_id <= 0:
            raise FanInProtocolError("mandatory_safety_evidence_malformed")
        process_root = Path(f"/proc/{process_id}")
        try:
            resident_pages = int(
                (process_root / "statm").read_text(encoding="ascii").split()[1]
            )
            rss += resident_pages * os.sysconf("SC_PAGE_SIZE")
            open_fds += sum(1 for _ in (process_root / "fd").iterdir())
        except (OSError, ValueError, IndexError) as exc:
            raise FanInProtocolError("mandatory_safety_evidence_missing") from exc
    return rss, open_fds


def host_safety_telemetry(process_ids: Sequence[int]) -> dict[str, object]:
    """One authoritative host observation for all four resident shards."""

    physical, available = _memory()
    rss, open_fds = _process_rss_and_fds(process_ids)
    fd_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    port_count, ports_in_use, ports_remaining = _ephemeral_port_usage()
    return {
        "physical_memory_bytes": physical,
        "available_memory_bytes": available,
        "rss_bytes": rss,
        "fd_soft_limit": fd_soft * len(process_ids),
        "open_fds": open_fds,
        "ephemeral_port_count": port_count,
        "ephemeral_ports_in_use": ports_in_use,
        "ephemeral_ports_remaining": ports_remaining,
        "event_loop_p99_ms": 0.0,
        "cpu_capacity_fraction": 0.0,
    }


def _ephemeral_ports() -> tuple[int, int]:
    first, last = Path("/proc/sys/net/ipv4/ip_local_port_range").read_text(encoding="ascii").split()
    return int(first), int(last)


def _ephemeral_port_usage(
    proc_root: Path | None = None,
) -> tuple[int, int, int]:
    """Return range size, host-network-namespace ports in use, and remaining ports."""

    root = proc_root or Path("/proc")
    try:
        if proc_root is None:
            first, last = _ephemeral_ports()
        else:
            first_raw, last_raw = (
                root / "sys/net/ipv4/ip_local_port_range"
            ).read_text(encoding="ascii").split()
            first, last = int(first_raw), int(last_raw)
    except (OSError, ValueError) as exc:
        raise FanInProtocolError("mandatory_safety_evidence_missing") from exc
    used_ports: set[int] = set()
    observed_table = False
    for table in (root / "net/tcp", root / "net/tcp6"):
        if not table.is_file():
            continue
        observed_table = True
        try:
            lines = table.read_text(encoding="ascii").splitlines()
            if not lines or "local_address" not in lines[0]:
                raise ValueError("missing TCP table header")
            for line in lines[1:]:
                fields = line.split()
                if len(fields) < 2:
                    raise ValueError("malformed TCP row")
                _, separator, encoded_port = fields[1].rpartition(":")
                if not separator:
                    raise ValueError("malformed local TCP address")
                port = int(encoded_port, 16)
                if first <= port <= last:
                    used_ports.add(port)
        except (OSError, ValueError) as exc:
            raise FanInProtocolError("mandatory_safety_evidence_malformed") from exc
    if not observed_table:
        raise FanInProtocolError("mandatory_safety_evidence_missing")
    count = last - first + 1
    used = len(used_ports)
    if count <= 0 or used > count:
        raise FanInProtocolError("mandatory_safety_evidence_malformed")
    return count, used, max(0, count - used)


def _network_bytes() -> tuple[int, int]:
    received = sent = 0
    for line in Path("/proc/net/dev").read_text(encoding="ascii").splitlines()[2:]:
        _, _, counters = line.partition(":")
        fields = counters.split()
        if len(fields) >= 16:
            received += int(fields[0])
            sent += int(fields[8])
    return received, sent


def _tcp_states() -> dict[str, int]:
    names = {
        "01": "established",
        "02": "syn_sent",
        "06": "time_wait",
        "08": "close_wait",
        "09": "last_ack",
    }
    counts = {name: 0 for name in names.values()}
    for path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        for line in path.read_bytes().splitlines()[1:]:
            fields = line.split(None, 4)
            if len(fields) > 3:
                state = fields[3].decode("ascii")
                if state in names:
                    counts[names[state]] += 1
    return counts


async def _event_loop_calibration() -> float:
    loop = asyncio.get_running_loop()
    samples: list[float] = []
    for _ in range(200):
        expected = loop.time()
        await asyncio.sleep(0)
        samples.append(max(0.0, (loop.time() - expected) * 1_000))
    return percentile(samples, 0.99) or 0.0


async def _event_loop_microbatch_benchmark() -> float:
    """Measure one mirrored scheduler quantum without opening test sockets."""

    loop = asyncio.get_running_loop()
    samples: list[float] = []
    for _ in range(100):
        completed = loop.create_future()
        remaining = RUNNER_LANE_COUNT * MICRO_BATCH_SIZE
        started = loop.time()

        def callback(future: asyncio.Future[None] = completed) -> None:
            nonlocal remaining
            remaining -= 1
            if remaining == 0 and not future.done():
                future.set_result(None)

        for _ in range(remaining):
            loop.call_soon(callback)
        await completed
        samples.append((loop.time() - started) * 1_000)
    return percentile(samples, 0.99) or 0.0


async def _event_loop_selector_fanout_benchmark(
    *,
    event_batch_limit: int = SELECTOR_EVENT_BATCH_LIMIT,
    sockets: int = SELECTOR_FANOUT_PROBE_SOCKETS,
) -> tuple[float, int, int, int]:
    """Drive a no-endpoint readiness burst and report what the selector cost.

    Returns the heartbeat delay, peak deferred events, total selector wakeups,
    and how many readiness events the burst actually contained.

    Wakeups per event is the load-bearing number. Under level-triggered epoll a
    descriptor dropped at the poll boundary stays ready and is re-reported next
    poll, and selectors.py has already paid a kernel copy-out plus one key
    lookup and tuple for every descriptor in the batch. Draining N ready
    descriptors K at a time therefore costs O(N**2 / K) wakeups instead of
    O(N) -- slower end to end, while making a per-turn latency gate look better.
    """

    loop = asyncio.get_running_loop()
    diagnostics = RuntimeDiagnostics()
    diagnostics.selector_event_batch_limit = event_batch_limit
    diagnostics.install_selector_probe(loop)
    diagnostics.install_ready_batch_limit(loop)
    readers: list[socket.socket] = []
    writers: list[socket.socket] = []
    completed = loop.create_future()
    heartbeat = loop.create_future()
    remaining = sockets

    def readable(reader: socket.socket) -> None:
        nonlocal remaining
        reader.recv(1)
        cpu_deadline_ns = time.thread_time_ns() + SELECTOR_FANOUT_PROBE_CALLBACK_CPU_NS
        while time.thread_time_ns() < cpu_deadline_ns:
            pass
        loop.remove_reader(reader.fileno())
        remaining -= 1
        if remaining == 0 and not completed.done():
            completed.set_result(None)

    try:
        for _ in range(sockets):
            reader, writer = socket.socketpair()
            reader.setblocking(False)
            writer.setblocking(False)
            readers.append(reader)
            writers.append(writer)
            loop.add_reader(reader.fileno(), readable, reader)
        expected = loop.time() + 0.001
        loop.call_at(
            expected,
            lambda: heartbeat.set_result(max(0.0, (loop.time() - expected) * 1_000)),
        )
        for writer in writers:
            writer.send(b"x")
        try:
            await asyncio.wait_for(
                asyncio.gather(completed, heartbeat),
                timeout=5.0,
            )
        except TimeoutError:
            # This benchmark runs only before the bell. A timeout is ugly
            # scheduler/fanout evidence, not proof that the host lacks memory,
            # descriptors, or ports. Encode it into the advisory measurements.
            return (
                max(
                    5_000.0,
                    float(heartbeat.result())
                    if heartbeat.done() and not heartbeat.cancelled()
                    else 5_000.0,
                ),
                max(diagnostics.peak_deferred_selector_events, remaining),
                diagnostics.selector_wakeups,
                sockets,
            )
        return (
            float(heartbeat.result()),
            diagnostics.peak_deferred_selector_events,
            diagnostics.selector_wakeups,
            sockets,
        )
    finally:
        for reader in readers:
            loop.remove_reader(reader.fileno())
        for value in (*readers, *writers):
            value.close()
        diagnostics.restore_selector_probe()
        diagnostics.restore_ready_batch_limit()


def _cpu_calibration() -> float:
    started = time.monotonic_ns()
    value = b"round5-capacity-calibration"
    for _ in range(100_000):
        value = hashlib.sha256(value).digest()
    if not value:
        raise FanInProtocolError("cpu_calibration_failed")
    return (time.monotonic_ns() - started) / 1_000_000


def _runner_boot_id() -> str:
    path = Path("/proc/sys/kernel/random/boot_id")
    if path.is_file():
        return path.read_text(encoding="utf-8").strip()
    # Deterministic local-test identity. Production Amazon Linux always takes
    # the procfs branch above.
    return "local-" + hashlib.sha256(socket.gethostname().encode()).hexdigest()[:32]


async def capacity_preflight(instance_type: str) -> dict[str, object]:
    physical, available = _memory()
    baseline_rss = _rss_bytes()
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    first, last = _ephemeral_ports()
    loop_p99, microbatch_p99, cpu_ms = await asyncio.gather(
        _event_loop_calibration(),
        _event_loop_microbatch_benchmark(),
        asyncio.to_thread(_cpu_calibration),
    )
    # Production configuration first; this is the measurement that is gated.
    (
        selector_fanout_peak_ms,
        selector_fanout_peak_deferred,
        selector_fanout_wakeups,
        selector_fanout_events,
    ) = await _event_loop_selector_fanout_benchmark()
    # The historical capped configuration, retained only as recorded evidence of
    # the re-delivery it caused. It is never gated and never selected.
    (
        selector_fanout_baseline_peak_ms,
        _baseline_deferred,
        selector_fanout_baseline_wakeups,
        _baseline_events,
    ) = await _event_loop_selector_fanout_benchmark(event_batch_limit=16)
    selector_fanout_amplification = (
        selector_fanout_wakeups / selector_fanout_events if selector_fanout_events else 0.0
    )
    selector_fanout_baseline_amplification = (
        selector_fanout_baseline_wakeups / _baseline_events if _baseline_events else 0.0
    )
    projected_held = baseline_rss + RUNNER_LANE_COUNT * CALIBRATED_ONE_LANE_RSS_BYTES
    projected_peak = math.ceil(projected_held * (1 + RAMP_HEADROOM_FRACTION))
    open_fds = _open_fds()
    projected_fds = open_fds + RUNNER_LANE_COUNT * TARGET_CLIENTS_PER_LANE + FD_CONTROL_RESERVE
    port_count = last - first + 1
    protocol_failures: list[str] = []
    hard_safety_failures: list[str] = []
    advisories: list[str] = []
    cpu_count = len(os.sched_getaffinity(0))
    if instance_type != RUNNER_INSTANCE_TYPE:
        protocol_failures.append("runner_instance_type")
    if cpu_count < MIN_RUNNER_CPU_COUNT:
        protocol_failures.append("runner_cpu_count")
    if physical < projected_peak + MEMORY_RESERVE_BYTES:
        hard_safety_failures.append("physical_memory_projection")
    if available < projected_peak - baseline_rss + MEMORY_RESERVE_BYTES:
        hard_safety_failures.append("available_memory_projection")
    if projected_fds > math.floor(soft * FD_USAGE_FRACTION):
        hard_safety_failures.append("file_descriptor_projection")
    if port_count < TARGET_CLIENTS_PER_LANE + EPHEMERAL_PORT_RESERVE_PER_LANE:
        hard_safety_failures.append("ephemeral_port_projection")
    if loop_p99 > 20.0:
        advisories.append(AdvisoryTelemetryCode.EVENT_LOOP_CALIBRATION.value)
    if microbatch_p99 > RUNTIME_MAX_EVENT_LOOP_P99_MS:
        advisories.append(
            AdvisoryTelemetryCode.EVENT_LOOP_MICROBATCH_PRESSURE.value
        )
    if selector_fanout_peak_ms > RUNTIME_MAX_EVENT_LOOP_P99_MS:
        advisories.append(
            AdvisoryTelemetryCode.EVENT_LOOP_SELECTOR_FANOUT_PRESSURE.value
        )
    if selector_fanout_amplification > MAX_SELECTOR_WAKEUP_AMPLIFICATION:
        advisories.append(
            AdvisoryTelemetryCode.EVENT_LOOP_SELECTOR_FANOUT_AMPLIFIED.value
        )
    if selector_fanout_peak_deferred:
        advisories.append(
            AdvisoryTelemetryCode.EVENT_LOOP_SELECTOR_FANOUT_DEFERRED.value
        )
    if cpu_ms > 2_000.0:
        advisories.append(AdvisoryTelemetryCode.CPU_CALIBRATION.value)
    failures = [*protocol_failures, *hard_safety_failures]
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol": PROTOCOL,
        "action": "preflight",
        "instance_type": instance_type,
        "boot_id": _runner_boot_id(),
        "cpu_count": cpu_count,
        "physical_memory_bytes": physical,
        "available_memory_bytes": available,
        "baseline_rss_bytes": baseline_rss,
        "fd_soft_limit": soft,
        "fd_hard_limit": hard,
        "open_fds": open_fds,
        "ephemeral_port_first": first,
        "ephemeral_port_last": last,
        "event_loop_p99_ms": loop_p99,
        "event_loop_microbatch_p99_ms": microbatch_p99,
        "event_loop_selector_fanout_peak_ms": selector_fanout_peak_ms,
        "event_loop_selector_fanout_baseline_peak_ms": (selector_fanout_baseline_peak_ms),
        "event_loop_selector_fanout_peak_deferred": selector_fanout_peak_deferred,
        "event_loop_selector_fanout_sockets": selector_fanout_events,
        "event_loop_selector_fanout_wakeups": selector_fanout_wakeups,
        "event_loop_selector_fanout_amplification": selector_fanout_amplification,
        "event_loop_selector_fanout_baseline_amplification": (
            selector_fanout_baseline_amplification
        ),
        "cpu_calibration_ms": cpu_ms,
        "projected_held_rss_bytes": projected_held,
        "projected_peak_rss_bytes": projected_peak,
        "projected_fds": projected_fds,
        "capacity_model_sha256": capacity_model_sha256(),
        "generator_sha256": generator_sha256(),
        "config_sha256": config_sha256(),
        "contract_sha256": contract_sha256(),
        "safety_evidence_version": SAFETY_EVIDENCE_VERSION,
        "protocol_failures": protocol_failures,
        "hard_safety_failures": hard_safety_failures,
        "telemetry_advisories": advisories,
        "failures": failures,
        "sufficient": not failures,
    }


def percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\0"


def _message(kind: bytes, body: bytes) -> bytes:
    return kind + struct.pack("!I", len(body) + 4) + body


def _password_message(password: str) -> bytes:
    try:
        return _message(b"p", password.encode("utf-8") + b"\0")
    except UnicodeError as exc:
        raise FanInProtocolError("password_encoding_invalid") from exc


def _scram_escape(value: str) -> str:
    return value.replace("=", "=3D").replace(",", "=2C")


def _scram_fields(value: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for item in value.split(","):
        key, separator, content = item.partition("=")
        if not separator or key in fields:
            raise FanInProtocolError("scram_message_invalid")
        fields[key] = content
    return fields


@dataclass(slots=True)
class ScramKeyCache:
    """Run-local cache for the password/salt work shared by every client."""

    values: dict[tuple[bytes, bytes, int], bytes] = field(default_factory=dict)

    def derive(self, password: str, salt: bytes, iterations: int) -> bytes:
        key = (hashlib.sha256(password.encode()).digest(), salt, iterations)
        salted = self.values.get(key)
        if salted is None:
            salted = hashlib.pbkdf2_hmac(
                "sha256",
                password.encode(),
                salt,
                iterations,
            )
            self.values[key] = salted
        return salted


@dataclass(slots=True)
class ScramState:
    user: str
    password: str
    nonce: str
    client_first_bare: str
    key_cache: ScramKeyCache
    server_signature: bytes | None = None

    @classmethod
    def new(
        cls,
        user: str,
        password: str,
        ordinal: int,
        key_cache: ScramKeyCache | None = None,
    ) -> ScramState:
        raw = hashlib.sha256(f"{time.monotonic_ns()}\0{ordinal}\0{os.getpid()}".encode()).digest()[
            :18
        ]
        nonce = base64.b64encode(raw).decode("ascii")
        first = f"n={_scram_escape(user)},r={nonce}"
        return cls(
            user=user,
            password=password,
            nonce=nonce,
            client_first_bare=first,
            key_cache=key_cache or ScramKeyCache(),
        )

    def initial(self) -> bytes:
        first = f"n,,{self.client_first_bare}".encode()
        return _cstring("SCRAM-SHA-256") + struct.pack("!I", len(first)) + first

    def continue_message(self, server_first: str) -> bytes:
        fields = _scram_fields(server_first)
        server_nonce = fields.get("r", "")
        if not server_nonce.startswith(self.nonce) or len(server_nonce) <= len(self.nonce):
            raise FanInProtocolError("scram_nonce_invalid")
        try:
            salt = base64.b64decode(fields["s"], validate=True)
            iterations = int(fields["i"])
        except (KeyError, ValueError) as exc:
            raise FanInProtocolError("scram_parameters_invalid") from exc
        if not 4_096 <= iterations <= 1_000_000:
            raise FanInProtocolError("scram_iterations_invalid")
        final_without_proof = f"c=biws,r={server_nonce}"
        auth_message = (f"{self.client_first_bare},{server_first},{final_without_proof}").encode()
        salted = self.key_cache.derive(self.password, salt, iterations)
        client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
        stored_key = hashlib.sha256(client_key).digest()
        signature = hmac.new(stored_key, auth_message, hashlib.sha256).digest()
        proof = bytes(left ^ right for left, right in zip(client_key, signature, strict=True))
        server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
        self.server_signature = hmac.new(server_key, auth_message, hashlib.sha256).digest()
        return f"{final_without_proof},p={base64.b64encode(proof).decode()}".encode()

    def verify_final(self, server_final: str) -> None:
        fields = _scram_fields(server_final)
        if "e" in fields:
            raise FanInProtocolError("scram_server_refused")
        try:
            signature = base64.b64decode(fields["v"], validate=True)
        except (KeyError, ValueError) as exc:
            raise FanInProtocolError("scram_final_invalid") from exc
        if self.server_signature is None or not hmac.compare_digest(
            signature, self.server_signature
        ):
            raise FanInProtocolError("scram_server_signature_invalid")


class PostgresClient(asyncio.Protocol):
    """One nonblocking pooled client from SSLRequest through ReadyForQuery."""

    def __init__(
        self,
        *,
        lane_id: str,
        ordinal: int,
        database: Mapping[str, object],
        application_name: str,
        ssl_context: ssl.SSLContext,
        on_unexpected_disconnect: Callable[[str], None],
        key_cache: ScramKeyCache | None = None,
    ) -> None:
        self.lane_id = lane_id
        self.ordinal = ordinal
        self.database = database
        self.application_name = application_name
        self.ssl_context = ssl_context
        self.on_unexpected_disconnect = on_unexpected_disconnect
        loop = asyncio.get_running_loop()
        self.authenticated: asyncio.Future[None] = loop.create_future()
        self.query_result: asyncio.Future[None] | None = None
        self.closed: asyncio.Future[None] = loop.create_future()
        self.transport: asyncio.Transport | None = None
        self.buffer = bytearray()
        self.awaiting_ssl = True
        self.upgrading_tls = False
        self.authentication_ok = False
        self.auth_method = ""
        self.scram_final_verified = False
        self.ready = False
        self.intentional_close = False
        self.scram = ScramState.new(
            str(database["user"]),
            str(database["password"]),
            ordinal,
            key_cache,
        )
        self.backend_pid = 0
        self.local_endpoint: tuple[str, int] | None = None
        self.fd = -1
        self.tls_version = ""
        self.cipher = ""
        self.tls_verified = False

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        if self.awaiting_ssl and not self.upgrading_tls:
            self.transport.write(SSL_REQUEST)

    def data_received(self, data: bytes) -> None:
        started_ns = time.perf_counter_ns()
        try:
            if self.awaiting_ssl:
                self.buffer.extend(data)
                if not self.buffer:
                    return
                response = bytes(self.buffer[:1])
                del self.buffer[:1]
                if response != b"S":
                    self._fail(FanInProtocolError("tls_refused"))
                    return
                self.upgrading_tls = True
                assert self.transport is not None
                self.transport.pause_reading()
                asyncio.create_task(self._start_tls())
                return
            self.buffer.extend(data)
            try:
                self._parse_messages()
            except BaseException as exc:
                self._fail(exc)
        finally:
            _record_phase("protocol_data_received", started_ns)

    async def _start_tls(self) -> None:
        assert self.transport is not None
        loop = asyncio.get_running_loop()
        handshake_started_ns = time.perf_counter_ns()
        handshake_thread_cpu_started_ns = time.thread_time_ns()
        handshake_process_cpu_started_ns = time.process_time_ns()
        diagnostics = _runtime_diagnostics
        if diagnostics is not None:
            diagnostics.handshake_started()
        try:
            upgraded = await loop.start_tls(
                self.transport,
                self,
                self.ssl_context,
                server_side=False,
                server_hostname=str(self.database["host"]),
                ssl_handshake_timeout=CONNECT_TIMEOUT_SECONDS,
            )
            if upgraded is None:
                raise FanInProtocolError("tls_upgrade_failed")
            _record_phase("ssl_handshake_wall", handshake_started_ns)
            if diagnostics is not None:
                diagnostics.record_elapsed(
                    "ssl_handshake_thread_cpu",
                    (time.thread_time_ns() - handshake_thread_cpu_started_ns) / 1_000_000,
                )
                diagnostics.record_elapsed(
                    "ssl_handshake_process_cpu",
                    (time.process_time_ns() - handshake_process_cpu_started_ns) / 1_000_000,
                )
            post_started_ns = time.perf_counter_ns()
            self.transport = upgraded
            self.awaiting_ssl = False
            self.upgrading_tls = False
            self.transport.resume_reading()
            ssl_object = self.transport.get_extra_info("ssl_object")
            if ssl_object is None:
                raise FanInProtocolError("tls_object_missing")
            self.tls_version = str(ssl_object.version() or "")
            cipher = ssl_object.cipher()
            self.cipher = str(cipher[0] if cipher else "")
            self.tls_verified = (
                self.ssl_context.check_hostname
                and self.ssl_context.verify_mode == ssl.CERT_REQUIRED
                and bool(self.tls_version)
                and bool(self.cipher)
            )
            if not self.tls_verified:
                raise FanInProtocolError("tls_verify_full_not_established")
            startup = b"".join(
                (
                    struct.pack("!I", 196608),
                    _cstring("user"),
                    _cstring(str(self.database["user"])),
                    _cstring("database"),
                    _cstring(str(self.database["dbname"])),
                    _cstring("application_name"),
                    _cstring(self.application_name),
                    _cstring("client_encoding"),
                    _cstring("UTF8"),
                    b"\0",
                )
            )
            self.transport.write(struct.pack("!I", len(startup) + 4) + startup)
            _record_phase("ssl_post_handshake_sync", post_started_ns)
            if self.buffer:
                pending = bytes(self.buffer)
                self.buffer.clear()
                self.data_received(pending)
        except BaseException as exc:
            self._fail(exc)
        finally:
            if diagnostics is not None:
                diagnostics.handshake_finished()

    def _parse_messages(self) -> None:
        while len(self.buffer) >= 5:
            size = struct.unpack("!I", self.buffer[1:5])[0]
            if size < 4 or size > 16 * 1024 * 1024:
                raise FanInProtocolError("message_length_invalid")
            total = size + 1
            if len(self.buffer) < total:
                return
            parse_started_ns = time.perf_counter_ns()
            kind = bytes(self.buffer[:1])
            body = bytes(self.buffer[5:total])
            del self.buffer[:total]
            self._handle_message(kind, body)
            _record_phase("protocol_message_parse_dispatch", parse_started_ns)

    def _handle_message(self, kind: bytes, body: bytes) -> None:
        if kind == b"R":
            self._authentication(body)
        elif kind == b"K" and len(body) == 8:
            self.backend_pid = struct.unpack("!I", body[:4])[0]
        elif kind == b"E":
            code = "unknown"
            for field in body.split(b"\0"):
                if field.startswith(b"C") and len(field) == 6:
                    code = field[1:].decode("ascii", errors="ignore").lower()
            raise FanInProtocolError(f"postgres_error_{code}")
        elif kind == b"D" and self.query_result is not None:
            if len(body) < 7 or struct.unpack("!H", body[:2])[0] != 1:
                raise FanInProtocolError("sample_row_invalid")
            length = struct.unpack("!I", body[2:6])[0]
            if length != 1 or body[6:7] != b"1":
                raise FanInProtocolError("sample_value_invalid")
        elif kind == b"Z":
            if body != b"I":
                raise FanInProtocolError("transaction_state_invalid")
            if not self.ready:
                if not self.authentication_ok:
                    raise FanInProtocolError("ready_before_authentication")
                self.ready = True
                identity_started_ns = time.perf_counter_ns()
                assert self.transport is not None
                socket_value = self.transport.get_extra_info("socket")
                local = self.transport.get_extra_info("sockname")
                if socket_value is None or not isinstance(local, tuple) or len(local) < 2:
                    raise FanInProtocolError("socket_identity_unavailable")
                self.fd = int(socket_value.fileno())
                self.local_endpoint = (str(local[0]), int(local[1]))
                if not self.authenticated.done():
                    self.authenticated.set_result(None)
                _record_phase("client_ready_socket_identity", identity_started_ns)
            elif self.query_result is not None and not self.query_result.done():
                self.query_result.set_result(None)

    def _authentication(self, body: bytes) -> None:
        started_ns = time.perf_counter_ns()
        try:
            self._authentication_inner(body)
        finally:
            _record_phase("authentication_processing", started_ns)

    def _authentication_inner(self, body: bytes) -> None:
        if len(body) < 4:
            raise FanInProtocolError("authentication_message_invalid")
        code = struct.unpack("!I", body[:4])[0]
        assert self.transport is not None
        if code == 3:
            cleartext_started_ns = time.perf_counter_ns()
            if len(body) != 4:
                raise FanInProtocolError("cleartext_challenge_malformed")
            if not self.tls_verified:
                raise FanInProtocolError("cleartext_requires_verified_tls")
            if self.auth_method:
                raise FanInProtocolError("authentication_method_changed")
            self.auth_method = AUTH_CLEARTEXT
            self.transport.write(_password_message(str(self.database["password"])))
            _record_phase("auth_cleartext_build_write", cleartext_started_ns)
        elif code == 10:
            scram_initial_started_ns = time.perf_counter_ns()
            if not self.tls_verified:
                raise FanInProtocolError("scram_requires_verified_tls")
            if self.auth_method:
                raise FanInProtocolError("authentication_method_changed")
            offered = body[4:]
            if not offered.endswith(b"\0\0"):
                raise FanInProtocolError("sasl_challenge_malformed")
            mechanisms = offered[:-2].split(b"\0")
            if not mechanisms or any(not mechanism for mechanism in mechanisms):
                raise FanInProtocolError("sasl_challenge_malformed")
            if b"SCRAM-SHA-256" not in mechanisms:
                raise FanInProtocolError("scram_unavailable")
            self.auth_method = AUTH_SCRAM
            self.transport.write(_message(b"p", self.scram.initial()))
            _record_phase("scram_initial_build_write", scram_initial_started_ns)
        elif code == 11:
            scram_proof_started_ns = time.perf_counter_ns()
            if self.auth_method != AUTH_SCRAM or self.scram.server_signature is not None:
                raise FanInProtocolError("scram_sequence_invalid")
            response = self.scram.continue_message(body[4:].decode("utf-8"))
            self.transport.write(_message(b"p", response))
            _record_phase("scram_proof_hmac_base64", scram_proof_started_ns)
        elif code == 12:
            scram_verify_started_ns = time.perf_counter_ns()
            if self.auth_method != AUTH_SCRAM or self.scram_final_verified:
                raise FanInProtocolError("scram_sequence_invalid")
            self.scram.verify_final(body[4:].decode("utf-8"))
            self.scram_final_verified = True
            _record_phase("scram_server_signature_verify", scram_verify_started_ns)
        elif code == 0:
            if len(body) != 4:
                raise FanInProtocolError("authentication_ok_malformed")
            if self.authentication_ok:
                raise FanInProtocolError("authentication_ok_repeated")
            if not self.auth_method:
                raise FanInProtocolError("authentication_without_challenge")
            if self.auth_method == AUTH_SCRAM and not self.scram_final_verified:
                raise FanInProtocolError("scram_server_signature_missing")
            self.authentication_ok = True
        else:
            raise FanInProtocolError(f"authentication_method_{code}_unsupported")

    async def sample_select_one(self) -> None:
        if not self.ready or self.transport is None:
            raise FanInProtocolError("sample_client_not_ready")
        if self.query_result is not None and not self.query_result.done():
            raise FanInProtocolError("sample_query_overlap")
        self.query_result = asyncio.get_running_loop().create_future()
        self.transport.write(_message(b"Q", b"SELECT 1\0"))
        try:
            async with asyncio.timeout(CONNECT_TIMEOUT_SECONDS):
                await self.query_result
        finally:
            self.query_result = None

    def _fail(self, exc: BaseException) -> None:
        if not self.authenticated.done():
            self.authenticated.set_exception(
                exc if isinstance(exc, Exception) else FanInProtocolError("client_failed")
            )
        if self.query_result is not None and not self.query_result.done():
            self.query_result.set_exception(
                exc if isinstance(exc, Exception) else FanInProtocolError("query_failed")
            )
        if self.transport is not None:
            self.transport.abort()

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.intentional_close and self.ready:
            self.on_unexpected_disconnect(self.lane_id)
        if not self.authenticated.done():
            self.authenticated.set_exception(
                FanInProtocolError("connection_lost_before_authentication")
            )
        if self.query_result is not None and not self.query_result.done():
            self.query_result.set_exception(FanInProtocolError("connection_lost_during_query"))
        if not self.closed.done():
            self.closed.set_result(None)

    def close(self) -> None:
        self.intentional_close = True
        if self.transport is not None:
            self.transport.abort()
        else:
            if not self.authenticated.done():
                self.authenticated.cancel()
            if not self.closed.done():
                self.closed.set_result(None)


@dataclass(slots=True)
class ObserverEvidence:
    lane_id: str
    client_role: str
    observer_role: str
    preexisting: int = 0
    current_backends: int = 0
    peak_backends: int = 0
    pids: set[int] = field(default_factory=set)
    role_verified: bool = False
    direct: bool = True
    sample_attempts: int = 0
    sample_failures: int = 0
    reconnect_attempts: int = 0
    reconnect_elapsed_ms: float = 0.0
    last_failure_code: str | None = None
    last_sqlstate: str | None = None
    last_connection_state: str = "not_open"


class DirectObserver:
    def __init__(
        self,
        lane_id: str,
        database: Mapping[str, object],
        application_name: str,
    ) -> None:
        self.lane_id = lane_id
        self.database = database
        self.application_name = application_name
        self.connection: Any | None = None
        self.evidence = ObserverEvidence(lane_id, CLIENT_ROLE, OBSERVER_ROLE)
        self._sample_lock = asyncio.Lock()

    #: The libpq connection fields `connect_runner_database` accepts. Selected rather
    #: than filtered, because the descriptor this observer is handed also carries the
    #: credential digest and the trust bundle path, and a removal list has to be updated
    #: every time one more of those is added. Omitting `sslrootcert` from a filter is
    #: what crashed the first fan-in bout ever dispatched.
    _CONNECT_FIELDS = ("host", "port", "dbname", "user", "username", "password")

    def _connect_database(self) -> dict[str, object]:
        return {
            key: self.database[key] for key in self._CONNECT_FIELDS if key in self.database
        }

    def _trust_bundle_path(self) -> Path | None:
        """The sealed bundle this observer verifies against, from its own descriptor.

        `sslmode` is verify-full, so without this the observer would verify against the
        system trust store: workable for a public CA and wrong for Amazon RDS, and wrong
        in principle for the one connection whose purpose is independent evidence.
        """

        value = self.database.get("sslrootcert")
        return Path(str(value)) if value else None

    def _connection_state(self) -> str:
        if self.connection is None:
            return "not_open"
        if bool(getattr(self.connection, "closed", False)):
            return "closed"
        if bool(getattr(self.connection, "broken", False)):
            return "broken"
        return "open"

    async def _connect(self) -> None:
        self.connection = await connect_runner_database(
            self._connect_database(),
            application_name=f"{self.application_name}-observer",
            trust_bundle_path=self._trust_bundle_path(),
            tls_mode=TLS_MODE,
            connect_timeout_seconds=CONNECT_TIMEOUT_SECONDS,
        )
        self.evidence.last_connection_state = self._connection_state()

    async def _verify_identity(self) -> None:
        if self.connection is None:
            raise FanInProtocolError("observer_not_open")
        async with self.connection.cursor() as cursor:
            await cursor.execute("SELECT current_user", prepare=False)
            row = await cursor.fetchone()
        await self.connection.commit()
        if row is None or str(row[0]) != OBSERVER_ROLE:
            raise FanInProtocolError("observer_role_invalid")
        self.evidence.role_verified = True

    async def open_and_preflight(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + OBSERVER_READY_TIMEOUT_SECONDS
        while True:
            try:
                await self._connect()
                break
            except psycopg.OperationalError as exc:
                if loop.time() >= deadline:
                    raise FanInProtocolError(f"{self.lane_id}_observer_connect_failed") from exc
                await asyncio.sleep(
                    min(
                        OBSERVER_RETRY_SECONDS,
                        max(0.0, deadline - loop.time()),
                    )
                )
        quiesce_deadline = loop.time() + OBSERVER_QUIESCE_TIMEOUT_SECONDS
        # Wait for the count to settle, not for it to vanish. A pooled lane starts with the
        # backend sessions that proving it ready created, and it is entitled to: what this
        # has to establish is that the number is stable and small enough to attribute the
        # bout's own sessions against, which is why the baseline is recorded on the lane
        # result rather than required to be zero.
        stable_readings = 0
        previous: int | None = None
        while True:
            async with self.connection.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT current_user,
                           count(*) FILTER (
                             WHERE usename = %s
                               AND pid <> pg_backend_pid()
                           )
                    FROM pg_stat_activity
                    """,
                    (CLIENT_ROLE,),
                    prepare=False,
                )
                row = await cursor.fetchone()
            await self.connection.commit()
            if row is None or str(row[0]) != OBSERVER_ROLE:
                raise FanInProtocolError("observer_role_invalid")
            self.evidence.role_verified = True
            self.evidence.preexisting = int(row[1])
            if self.evidence.preexisting > MAX_PREEXISTING_CLIENT_SESSIONS:
                # Above the ceiling nothing is attributable, so this refuses immediately
                # rather than waiting out a deadline it has no reason to think will help.
                raise FanInProtocolError(f"{self.lane_id}_preexisting_client_sessions")
            if self.evidence.preexisting == 0:
                break
            stable_readings = stable_readings + 1 if self.evidence.preexisting == previous else 0
            previous = self.evidence.preexisting
            if stable_readings >= OBSERVER_QUIESCE_STABLE_READINGS:
                break
            if loop.time() >= quiesce_deadline:
                # Still moving at the deadline. That is a lane in use, not a warm pool,
                # and its sessions cannot be told apart from the ones about to be opened.
                raise FanInProtocolError(f"{self.lane_id}_preexisting_client_sessions")
            await asyncio.sleep(
                min(
                    OBSERVER_RETRY_SECONDS,
                    max(0.0, quiesce_deadline - loop.time()),
                )
            )

    async def _sample_once(self) -> None:
        if self.connection is None:
            raise FanInProtocolError("observer_not_open")
        async with self.connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT count(*), coalesce(array_agg(pid), ARRAY[]::integer[])
                FROM pg_stat_activity
                WHERE usename = %s AND pid <> pg_backend_pid()
                """,
                (CLIENT_ROLE,),
                prepare=False,
            )
            row = await cursor.fetchone()
        await self.connection.commit()
        if row is None:
            raise FanInProtocolError("observer_sample_invalid")
        current = int(row[0])
        self.evidence.current_backends = current
        self.evidence.peak_backends = max(self.evidence.peak_backends, current)
        self.evidence.pids.update(int(value) for value in row[1])

    @staticmethod
    def _safe_sqlstate(exc: BaseException) -> str | None:
        value = getattr(exc, "sqlstate", None)
        return (
            value.upper()
            if isinstance(value, str) and len(value) == 5 and value.isalnum()
            else None
        )

    async def sample(self) -> bool:
        async with self._sample_lock:
            started_ns = time.perf_counter_ns()
            retry_started_ns: int | None = None
            try:
                for attempt in range(OBSERVER_SAMPLE_MAX_RETRIES + 1):
                    self.evidence.sample_attempts += 1
                    try:
                        if self._connection_state() != "open":
                            await self._connect()
                            await self._verify_identity()
                        await self._sample_once()
                        self.evidence.last_connection_state = self._connection_state()
                        if retry_started_ns is not None:
                            self.evidence.reconnect_elapsed_ms += (
                                time.monotonic_ns() - retry_started_ns
                            ) / 1_000_000
                        return True
                    except psycopg.OperationalError as exc:
                        sqlstate = self._safe_sqlstate(exc)
                        connection_state = self._connection_state()
                        self.evidence.sample_failures += 1
                        self.evidence.last_sqlstate = sqlstate
                        self.evidence.last_connection_state = connection_state
                        transient = (
                            (sqlstate is not None and sqlstate.startswith("08"))
                            or sqlstate in {"57P01", "57P02", "57P03"}
                            or (
                                sqlstate is None
                                and connection_state in {"not_open", "closed", "broken"}
                            )
                        )
                        if not transient:
                            self.evidence.last_failure_code = "observer_operational_permanent"
                            return False
                        if attempt >= OBSERVER_SAMPLE_MAX_RETRIES:
                            self.evidence.last_failure_code = "observer_transport_retry_exhausted"
                            if retry_started_ns is not None:
                                self.evidence.reconnect_elapsed_ms += (
                                    time.monotonic_ns() - retry_started_ns
                                ) / 1_000_000
                            return False
                        if retry_started_ns is None:
                            retry_started_ns = time.monotonic_ns()
                        self.evidence.reconnect_attempts += 1
                        await self.close()
                        await asyncio.sleep(OBSERVER_SAMPLE_RETRY_SECONDS)
                    except FanInProtocolError as exc:
                        self.evidence.sample_failures += 1
                        self.evidence.last_failure_code = str(exc)
                        self.evidence.last_connection_state = self._connection_state()
                        return False
                return False
            finally:
                _record_phase("observer_query_wall", started_ns)

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None


@dataclass(slots=True)
class LaneRuntime:
    lane_id: str
    database: dict[str, object]
    observer_database: dict[str, object]
    application_name: str
    ssl_context: ssl.SSLContext
    key_cache: ScramKeyCache = field(default_factory=ScramKeyCache)
    connect_host: str = ""
    target_clients: int = TARGET_CLIENTS_PER_LANE
    clients: list[PostgresClient] = field(default_factory=list)
    connect_latencies_ms: list[float] = field(default_factory=list)
    auth_methods: set[str] = field(default_factory=set)
    initiated: int = 0
    authenticated: int = 0
    cancelled: int = 0
    terminal_failures: int = 0
    retries: int = 0
    disconnected_during_hold: int = 0
    target_elapsed_ns: int | None = None
    sample_attempted: int = 0
    sample_succeeded: int = 0
    sample_failed: int = 0
    last_progress_milestone: int = 0
    failure_codes: dict[str, int] = field(default_factory=dict)
    connection_diagnostics: list[dict[str, object]] = field(default_factory=list)
    first_launch_ns: int | None = None

    def record_connection_diagnostic(self, *, ordinal: int, stage: str, code: str) -> None:
        if len(self.connection_diagnostics) >= CONNECTION_DIAGNOSTIC_LIMIT:
            return
        self.connection_diagnostics.append(
            {
                "ordinal": ordinal,
                "stage": stage,
                "code": code,
            }
        )

    def unexpected_disconnect(self, lane_id: str, ordinal: int) -> None:
        if lane_id == self.lane_id:
            self.disconnected_during_hold += 1
            self.record_connection_diagnostic(
                ordinal=ordinal,
                stage="hold",
                code="unexpected_disconnect",
            )


@dataclass(slots=True)
class TelemetrySummary:
    samples: int = 0
    physical_memory_bytes: int = 0
    min_available_memory_bytes: int = 2**63 - 1
    peak_rss_bytes: int = 0
    fd_soft_limit: int = 0
    peak_open_fds: int = 0
    ephemeral_port_count: int = 0
    peak_ephemeral_ports_in_use: int = 0
    min_ephemeral_port_reserve: int = 2**31 - 1
    peak_event_loop_p99_ms: float = 0.0
    peak_raw_event_loop_p99_ms: float = 0.0
    peak_external_event_loop_p99_ms: float = 0.0
    raw_event_loop_warning_count: int = 0
    raw_event_loop_ceiling_breaches: int = 0
    peak_cpu_capacity_fraction: float = 0.0
    # Only resource exhaustion is allowed to stop a proof.  Scheduler timing,
    # loop lag and CPU saturation are pacing inputs and diagnostic evidence;
    # treating them as proof failures is what stopped healthy shards at 7,134
    # authenticated clients with no connection failure.
    hard_failures: set[str] = field(default_factory=set)
    advisories: set[str] = field(default_factory=set)
    raw_loop_lag_samples_ms: collections.deque[float] = field(
        default_factory=lambda: collections.deque(maxlen=EVENT_LOOP_P99_WINDOW_SAMPLES),
        repr=False,
    )
    owned_loop_lag_samples_ms: collections.deque[float] = field(
        default_factory=lambda: collections.deque(maxlen=EVENT_LOOP_P99_WINDOW_SAMPLES),
        repr=False,
    )
    external_loop_lag_samples_ms: collections.deque[float] = field(
        default_factory=lambda: collections.deque(maxlen=EVENT_LOOP_P99_WINDOW_SAMPLES),
        repr=False,
    )

    def observe_event_loop(
        self,
        raw_lag_ms: float,
        *,
        generator_owned_lag_ms: float | None = None,
    ) -> None:
        owned_lag_ms = raw_lag_ms if generator_owned_lag_ms is None else generator_owned_lag_ms
        external_lag_ms = raw_lag_ms if owned_lag_ms <= 0 else 0.0
        self.raw_loop_lag_samples_ms.append(raw_lag_ms)
        self.owned_loop_lag_samples_ms.append(owned_lag_ms)
        self.external_loop_lag_samples_ms.append(external_lag_ms)
        sample_count = len(self.raw_loop_lag_samples_ms)
        raw_p99 = percentile(tuple(self.raw_loop_lag_samples_ms), 0.99) or 0.0
        external_p99 = percentile(tuple(self.external_loop_lag_samples_ms), 0.99) or 0.0
        self.peak_raw_event_loop_p99_ms = max(
            self.peak_raw_event_loop_p99_ms,
            raw_p99,
        )
        self.peak_external_event_loop_p99_ms = max(
            self.peak_external_event_loop_p99_ms,
            external_p99,
        )
        if sample_count >= EVENT_LOOP_P99_MIN_SAMPLES:
            owned_p99 = percentile(tuple(self.owned_loop_lag_samples_ms), 0.99) or 0.0
            self.peak_event_loop_p99_ms = max(
                self.peak_event_loop_p99_ms,
                owned_p99,
            )
            if owned_p99 > RUNTIME_MAX_EVENT_LOOP_P99_MS:
                self.advisories.add(AdvisoryTelemetryCode.EVENT_LOOP_PRESSURE.value)
        if raw_lag_ms > RAW_WALL_LAG_WARNING_MS:
            self.raw_event_loop_warning_count += 1
        if raw_lag_ms > RAW_WALL_LAG_CEILING_MS:
            self.raw_event_loop_ceiling_breaches += 1
            if self.raw_event_loop_ceiling_breaches >= RAW_WALL_LAG_MAX_BREACHES:
                self.advisories.add(
                    AdvisoryTelemetryCode.HOST_SCHEDULING_INSTABILITY.value
                )

    def observe(self, value: Mapping[str, object]) -> None:
        mandatory = {
            "physical_memory_bytes",
            "available_memory_bytes",
            "rss_bytes",
            "fd_soft_limit",
            "open_fds",
            "ephemeral_port_count",
            "ephemeral_ports_in_use",
            "ephemeral_ports_remaining",
            "event_loop_p99_ms",
            "cpu_capacity_fraction",
        }
        if not mandatory <= set(value):
            self.hard_failures.add(HardSafetyCode.MANDATORY_EVIDENCE_MISSING.value)
            return
        integer_fields = mandatory - {
            "event_loop_p99_ms",
            "cpu_capacity_fraction",
        }
        if any(
            isinstance(value[name], bool)
            or not isinstance(value[name], int)
            or int(value[name]) < 0
            for name in integer_fields
        ) or any(
            isinstance(value[name], bool)
            or not isinstance(value[name], (int, float))
            or not math.isfinite(float(value[name]))
            or float(value[name]) < 0
            for name in ("event_loop_p99_ms", "cpu_capacity_fraction")
        ):
            self.hard_failures.add(HardSafetyCode.MANDATORY_EVIDENCE_MALFORMED.value)
            return
        physical = int(value["physical_memory_bytes"])
        available = int(value["available_memory_bytes"])
        rss = int(value["rss_bytes"])
        fd_soft = int(value["fd_soft_limit"])
        open_fds = int(value["open_fds"])
        ports = int(value["ephemeral_port_count"])
        ports_in_use = int(value["ephemeral_ports_in_use"])
        port_reserve = int(value["ephemeral_ports_remaining"])
        loop_p99 = float(value["event_loop_p99_ms"])
        cpu_fraction = float(value["cpu_capacity_fraction"])
        if (
            physical <= 0
            or fd_soft <= 0
            or ports <= 0
            or available > physical
            or rss > physical
            or open_fds > fd_soft
            or ports != ports_in_use + port_reserve
        ):
            self.hard_failures.add(
                HardSafetyCode.MANDATORY_EVIDENCE_MALFORMED.value
            )
            return
        self.samples += 1
        self.physical_memory_bytes = physical
        self.min_available_memory_bytes = min(self.min_available_memory_bytes, available)
        self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
        self.fd_soft_limit = fd_soft
        self.peak_open_fds = max(self.peak_open_fds, open_fds)
        self.ephemeral_port_count = ports
        self.peak_ephemeral_ports_in_use = max(
            self.peak_ephemeral_ports_in_use,
            ports_in_use,
        )
        self.min_ephemeral_port_reserve = min(self.min_ephemeral_port_reserve, port_reserve)
        # This value is already a p99 from a 200-turn calibration, but it has no
        # interval CPU/GC attribution. Keep it as raw/external diagnostic
        # evidence; generator ownership comes only from the continuous monitor.
        self.peak_raw_event_loop_p99_ms = max(
            self.peak_raw_event_loop_p99_ms,
            loop_p99,
        )
        self.peak_external_event_loop_p99_ms = max(
            self.peak_external_event_loop_p99_ms,
            loop_p99,
        )
        if loop_p99 > RAW_WALL_LAG_WARNING_MS:
            self.raw_event_loop_warning_count += 1
        if loop_p99 > RAW_WALL_LAG_CEILING_MS:
            self.raw_event_loop_ceiling_breaches += 1
            if self.raw_event_loop_ceiling_breaches >= RAW_WALL_LAG_MAX_BREACHES:
                self.advisories.add(
                    AdvisoryTelemetryCode.HOST_SCHEDULING_INSTABILITY.value
                )
        self.peak_cpu_capacity_fraction = max(self.peak_cpu_capacity_fraction, cpu_fraction)
        if rss > physical - MEMORY_RESERVE_BYTES:
            self.hard_failures.add(HardSafetyCode.RSS_RESERVE_EXHAUSTED.value)
        if available < MEMORY_RESERVE_BYTES:
            self.hard_failures.add(
                HardSafetyCode.AVAILABLE_MEMORY_RESERVE_EXHAUSTED.value
            )
        if open_fds > math.floor(fd_soft * FD_USAGE_FRACTION):
            self.hard_failures.add(
                HardSafetyCode.FILE_DESCRIPTOR_RESERVE_EXHAUSTED.value
            )
        if port_reserve < EPHEMERAL_PORT_RESERVE_PER_LANE:
            self.hard_failures.add(
                HardSafetyCode.EPHEMERAL_PORT_RESERVE_EXHAUSTED.value
            )
        if cpu_fraction > RUNTIME_MAX_CPU_CAPACITY_FRACTION:
            self.advisories.add(AdvisoryTelemetryCode.CPU_PRESSURE.value)

    @property
    def verified(self) -> bool:
        return self.samples > 0 and not self.hard_failures

    @property
    def pacing_pressure(self) -> bool:
        return bool(
            self.advisories
            & {
                AdvisoryTelemetryCode.EVENT_LOOP_PRESSURE.value,
                AdvisoryTelemetryCode.HOST_SCHEDULING_INSTABILITY.value,
                AdvisoryTelemetryCode.CPU_PRESSURE.value,
            }
        )

    def public_dict(self) -> dict[str, object]:
        for code in (*self.hard_failures, *self.advisories):
            classify_safety_code(code)
        return {
            "safety_evidence_version": SAFETY_EVIDENCE_VERSION,
            "telemetry_samples": self.samples,
            "telemetry_physical_memory_bytes": self.physical_memory_bytes,
            "telemetry_min_available_memory_bytes": (
                self.min_available_memory_bytes if self.samples else 0
            ),
            "telemetry_peak_rss_bytes": self.peak_rss_bytes,
            "telemetry_fd_soft_limit": self.fd_soft_limit,
            "telemetry_peak_open_fds": self.peak_open_fds,
            "telemetry_ephemeral_port_count": self.ephemeral_port_count,
            "telemetry_peak_ephemeral_ports_in_use": (
                self.peak_ephemeral_ports_in_use
            ),
            "telemetry_min_ephemeral_port_reserve": (
                self.min_ephemeral_port_reserve if self.samples else 0
            ),
            "telemetry_peak_event_loop_p99_ms": self.peak_event_loop_p99_ms,
            "telemetry_peak_raw_event_loop_p99_ms": (self.peak_raw_event_loop_p99_ms),
            "telemetry_peak_external_event_loop_p99_ms": (self.peak_external_event_loop_p99_ms),
            "telemetry_raw_event_loop_warning_count": (self.raw_event_loop_warning_count),
            "telemetry_raw_event_loop_ceiling_breaches": (self.raw_event_loop_ceiling_breaches),
            "telemetry_peak_cpu_capacity_fraction": (self.peak_cpu_capacity_fraction),
            "hard_safety_verified": self.verified,
            "port_accounting_verified": self.samples > 0
            and not (
                {
                    HardSafetyCode.MANDATORY_EVIDENCE_MISSING.value,
                    HardSafetyCode.MANDATORY_EVIDENCE_MALFORMED.value,
                }
                & self.hard_failures
            ),
            "telemetry_failures": sorted(self.hard_failures),
            "telemetry_advisories": sorted(self.advisories),
        }


def _telemetry(
    start_cpu: float,
    started_ns: int,
    network_start: tuple[int, int],
    event_loop_p99_ms: float,
    include_socket_states: bool = True,
) -> dict[str, object]:
    global _socket_states_cache_ns
    received, sent = _network_bytes()
    if include_socket_states and (
        not _socket_states_cache or time.monotonic_ns() - _socket_states_cache_ns >= 1_000_000_000
    ):
        socket_scan_started_ns = time.perf_counter_ns()
        _socket_states_cache.clear()
        _socket_states_cache.update(_tcp_states())
        _socket_states_cache_ns = time.monotonic_ns()
        _record_phase("telemetry_tcp_state_scan", socket_scan_started_ns)
    states = dict(_socket_states_cache) if include_socket_states else {}
    physical, available = _memory()
    fd_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    port_count, ports_in_use, ports_remaining = _ephemeral_port_usage()
    elapsed_seconds = max(
        (time.monotonic_ns() - started_ns) / 1_000_000_000,
        0.001,
    )
    cpu_count = max(1, len(os.sched_getaffinity(0)))
    return {
        "cpu_seconds": max(0.0, time.process_time() - start_cpu),
        "cpu_capacity_fraction": min(
            1.0,
            max(0.0, time.process_time() - start_cpu) / elapsed_seconds / cpu_count,
        ),
        "physical_memory_bytes": physical,
        "available_memory_bytes": available,
        "rss_bytes": _rss_bytes(),
        "fd_soft_limit": fd_soft,
        "open_fds": _open_fds(),
        "ephemeral_port_count": port_count,
        "ephemeral_ports_in_use": ports_in_use,
        "ephemeral_ports_remaining": ports_remaining,
        "socket_states": states,
        "network_received_bytes": max(0, received - network_start[0]),
        "network_sent_bytes": max(0, sent - network_start[1]),
        "event_loop_p99_ms": event_loop_p99_ms,
    }


async def _telemetry_off_loop(
    start_cpu: float,
    started_ns: int,
    network_start: tuple[int, int],
    event_loop_p99_ms: float,
) -> dict[str, object]:
    def profiled_telemetry() -> dict[str, object]:
        thread_started_ns = time.thread_time_ns()
        native_tid = threading.get_native_id()
        try:
            return _telemetry(
                start_cpu,
                started_ns,
                network_start,
                event_loop_p99_ms,
                (int((_worker_execution_context or {}).get("worker_id", 0)) == 0),
            )
        finally:
            elapsed_ms = (time.thread_time_ns() - thread_started_ns) / 1_000_000
            diagnostics = _runtime_diagnostics
            if diagnostics is not None:
                diagnostics.telemetry_calls += 1
                diagnostics.telemetry_thread_cpu_total_ms += elapsed_ms
                diagnostics.telemetry_thread_cpu_max_ms = max(
                    diagnostics.telemetry_thread_cpu_max_ms,
                    elapsed_ms,
                )
                diagnostics.telemetry_native_tids.add(native_tid)

    started = time.perf_counter_ns()
    value = await asyncio.to_thread(profiled_telemetry)
    _record_phase("telemetry_sampling_wall", started)
    return value


def _progress(value: Mapping[str, object]) -> None:
    global _progress_output_bytes, _progress_sequence
    if _progress_callback is not None:
        _progress_callback(value)
        return
    started_ns = time.perf_counter_ns()
    payload = {name: value[name] for name in PROGRESS_WIRE_FIELDS if name in value}
    payload["sequence"] = _progress_sequence + 1
    line = PROGRESS_PREFIX + canonical_json(payload).decode("utf-8")
    encoded_bytes = len(line.encode("utf-8")) + 1
    if _progress_output_bytes + encoded_bytes > PROGRESS_OUTPUT_BUDGET_BYTES:
        return
    _progress_sequence += 1
    _progress_output_bytes += encoded_bytes
    print(line, flush=True)
    _record_phase("progress_serialization_write", started_ns)


async def _open_client(runtime: LaneRuntime, t0_ns: int) -> None:
    if runtime.first_launch_ns is None:
        runtime.first_launch_ns = time.monotonic_ns()
    ordinal = runtime.initiated
    runtime.initiated += 1
    if runtime.initiated == 1:
        _progress(
            {
                "schema_version": SCHEMA_VERSION,
                "protocol": PROTOCOL,
                "lane_id": runtime.lane_id,
                "phase": "ramp",
                "initiated_clients": runtime.initiated,
                "authenticated_clients": runtime.authenticated,
                "held_clients": 0,
                "terminal_failures": runtime.terminal_failures,
                "sampled_queries_succeeded": 0,
                "sampled_queries_failed": 0,
                "elapsed_ms": (time.monotonic_ns() - t0_ns) / 1_000_000,
                "time_to_target_ms": None,
                "milestone": "first_socket_initiated",
                "milestone_monotonic_ns": runtime.first_launch_ns,
            }
        )
    allocation_started_ns = time.perf_counter_ns()
    client = PostgresClient(
        lane_id=runtime.lane_id,
        ordinal=ordinal,
        database=runtime.database,
        application_name=runtime.application_name,
        ssl_context=runtime.ssl_context,
        on_unexpected_disconnect=lambda lane_id: runtime.unexpected_disconnect(
            lane_id,
            ordinal,
        ),
        key_cache=runtime.key_cache,
    )
    runtime.clients.append(client)
    _record_phase("client_allocate_and_append", allocation_started_ns)
    started_ns = time.monotonic_ns()
    try:
        loop = asyncio.get_running_loop()
        await loop.create_connection(
            lambda: client,
            host=runtime.connect_host or str(runtime.database["host"]),
            port=int(runtime.database["port"]),
        )
        async with asyncio.timeout(CONNECT_TIMEOUT_SECONDS):
            await client.authenticated
        completed_ns = time.monotonic_ns()
        runtime.authenticated += 1
        if runtime.authenticated == 1:
            _progress(
                {
                    "schema_version": SCHEMA_VERSION,
                    "protocol": PROTOCOL,
                    "lane_id": runtime.lane_id,
                    "phase": "ramp",
                    "initiated_clients": runtime.initiated,
                    "authenticated_clients": runtime.authenticated,
                    "held_clients": sum(
                        client.ready and not client.closed.done()
                        for client in runtime.clients
                    ),
                    "terminal_failures": runtime.terminal_failures,
                    "sampled_queries_succeeded": 0,
                    "sampled_queries_failed": 0,
                    "elapsed_ms": (time.monotonic_ns() - t0_ns) / 1_000_000,
                    "time_to_target_ms": None,
                    "milestone": "first_client_authenticated",
                    "milestone_monotonic_ns": completed_ns,
                }
            )
        runtime.auth_methods.add(client.auth_method)
        runtime.connect_latencies_ms.append((completed_ns - started_ns) / 1_000_000)
        if runtime.authenticated == runtime.target_clients:
            runtime.target_elapsed_ns = completed_ns - t0_ns
    except asyncio.CancelledError:
        # The launch already owns an ordinal and is included in ``initiated``.  Account for that
        # terminal disposition before propagating cancellation; otherwise a telemetry stop or a
        # towel leaves initiated > authenticated + failures with no explanation.
        runtime.cancelled += 1
        client.close()
        raise
    except Exception as exc:
        runtime.terminal_failures += 1
        if isinstance(exc, FanInProtocolError):
            raw_code = str(exc)
            code = (
                raw_code
                if raw_code and len(raw_code) <= 64 and raw_code.replace("_", "").isalnum()
                else "protocol_failed"
            )
        elif isinstance(exc, TimeoutError):
            code = "connect_timeout"
        elif isinstance(exc, ssl.SSLError):
            code = "tls_failed"
        else:
            code = "connect_failed"
        runtime.failure_codes[code] = runtime.failure_codes.get(code, 0) + 1
        runtime.record_connection_diagnostic(
            ordinal=ordinal,
            stage="connect",
            code=code,
        )
        client.close()


async def _open_equal_wave(
    lanes: Sequence[LaneRuntime],
    wave_size: int,
    t0_ns: int,
    admission_controller: AdmissionController,
) -> None:
    """Launch one mirrored wave, bounding in-flight connects instead of draining.

    Both lanes keep identical MICRO_BATCH_SIZE interleaving, equal counts, and
    one shared decision; only pipeline depth changed. Awaiting every micro-batch
    made the ramp latency-bound -- a client costs five to six round trips, so the
    loop idled between batches and each lane became 2,500 serial waits. That is
    what pushed the ramp toward the sealed idle window, and the idle closures it
    caused are what produced the mass-EOF selector bursts.

    Per-lane creation is counted from a snapshot rather than read back from
    `lane.initiated`, because `_open_client` increments that counter inside the
    coroutine body. With a serial gather it was always settled before the next
    guard ran; with a pipeline it is not.
    """

    baseline = {lane.lane_id: lane.initiated for lane in lanes}
    created = {lane.lane_id: 0 for lane in lanes}
    in_flight: set[asyncio.Task[None]] = set()
    launched = 0

    async def settle(target: int) -> None:
        while len(in_flight) > target:
            done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                in_flight.discard(task)
                task.result()

    try:
        while launched < wave_size:
            quantum = min(MICRO_BATCH_SIZE, wave_size - launched)
            quantum_tasks = quantum * len(lanes)
            headroom = max(
                0,
                admission_controller.current_concurrency - quantum_tasks,
            )
            throttle_started_ns = time.monotonic_ns()
            await settle(headroom)
            admission_controller.record_throttle(throttle_started_ns)
            creation_started_ns = time.perf_counter_ns()
            for _ in range(quantum):
                for lane in lanes:
                    if baseline[lane.lane_id] + created[lane.lane_id] < lane.target_clients:
                        created[lane.lane_id] += 1
                        in_flight.add(asyncio.create_task(_open_client(lane, t0_ns)))
            _record_phase("wave_task_creation", creation_started_ns)
            launched += quantum
            await asyncio.sleep(0)
        await settle(0)
    finally:
        # asyncio.wait does not cancel what it waits on, so cancellation of this
        # wave must not leave connect tasks running behind the towel path.
        pending = [task for task in in_flight if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def _open_equal_wave_guarded(
    lanes: Sequence[LaneRuntime],
    wave_size: int,
    t0_ns: int,
    *,
    start_cpu: float,
    network_start: tuple[int, int],
    telemetry_summary: TelemetrySummary,
    admission_controller: AdmissionController,
) -> float:
    """Open one symmetric wave while measuring pressure during the work."""

    loop = asyncio.get_running_loop()
    wave_peak_loop_lag_ms = 0.0
    monitor_expected = loop.time() + LOOP_MONITOR_INTERVAL_SECONDS
    last_process_cpu_ns = time.process_time_ns()
    last_thread_cpu_ns = time.thread_time_ns()
    last_scheduler = _scheduler_context()
    last_sample_started_ns = time.perf_counter_ns()
    last_active_handshakes = 0
    monitor_active = True
    monitor_handle: asyncio.TimerHandle | None = None

    def monitor_tick() -> None:
        nonlocal monitor_expected, wave_peak_loop_lag_ms
        nonlocal last_process_cpu_ns, last_thread_cpu_ns, last_scheduler
        nonlocal last_sample_started_ns, last_active_handshakes
        nonlocal monitor_handle
        if not monitor_active:
            return
        lag_ms = max(0.0, (loop.time() - monitor_expected) * 1_000)
        wave_peak_loop_lag_ms = max(wave_peak_loop_lag_ms, lag_ms)
        diagnostics = _runtime_diagnostics or RuntimeDiagnostics()
        callback_interval = diagnostics.consume_callback_interval()
        sample = _loop_delay_sample(
            wall_lag_ms=lag_ms,
            process_cpu_started_ns=last_process_cpu_ns,
            thread_cpu_started_ns=last_thread_cpu_ns,
            scheduler_started=last_scheduler,
            sample_started_ns=last_sample_started_ns,
            active_handshakes=last_active_handshakes,
            ready_batch_size=_ready_backlog_size(loop),
            diagnostics=diagnostics,
            callback_interval=callback_interval,
        )
        telemetry_summary.observe_event_loop(
            sample.wall_lag_ms,
            generator_owned_lag_ms=sample.generator_owned_lag_ms,
        )
        admission_controller.observe_loop_interval(
            pressured=(
                sample.generator_owned_lag_ms > 20.0
                or (
                    sample.scheduler_wait_ms is not None
                    and sample.scheduler_wait_ms > 20.0
                )
            )
        )
        if _runtime_diagnostics is not None:
            _runtime_diagnostics.observe_loop(sample)
        monitor_expected = loop.time() + LOOP_MONITOR_INTERVAL_SECONDS
        last_process_cpu_ns = time.process_time_ns()
        last_thread_cpu_ns = time.thread_time_ns()
        last_scheduler = _scheduler_context()
        last_sample_started_ns = time.perf_counter_ns()
        last_active_handshakes = diagnostics.active_handshakes
        monitor_handle = loop.call_at(monitor_expected, monitor_tick)

    monitor_handle = loop.call_at(monitor_expected, monitor_tick)
    wave = asyncio.create_task(
        _open_equal_wave(
            lanes,
            wave_size,
            t0_ns,
            admission_controller,
        )
    )
    try:
        while not wave.done():
            await asyncio.wait(
                (wave,),
                timeout=RESOURCE_TELEMETRY_INTERVAL_SECONDS,
            )
            telemetry = await _telemetry_off_loop(
                start_cpu,
                t0_ns,
                network_start,
                0.0,
            )
            telemetry_summary.observe(telemetry)
            admission_controller.observe_cpu_interval(
                pressured=float(telemetry["cpu_capacity_fraction"])
                > RUNTIME_MAX_CPU_CAPACITY_FRACTION
            )
            if telemetry_summary.hard_failures and not wave.done():
                wave.cancel()
                await asyncio.gather(wave, return_exceptions=True)
                break
        if not wave.cancelled():
            await wave
        return wave_peak_loop_lag_ms
    finally:
        monitor_active = False
        if monitor_handle is not None:
            monitor_handle.cancel()
        if not wave.done():
            wave.cancel()
            await asyncio.gather(wave, return_exceptions=True)


def _socket_evidence(runtime: LaneRuntime) -> tuple[int, int, str]:
    live = [client for client in runtime.clients if client.ready and not client.closed.done()]
    fds = {client.fd for client in live if client.fd >= 0}
    endpoints = {client.local_endpoint for client in live if client.local_endpoint is not None}
    identities = sorted(
        f"{client.ordinal}:{client.fd}:{client.local_endpoint[0]}:{client.local_endpoint[1]}"
        for client in live
        if client.local_endpoint is not None and client.fd >= 0
    )
    digest = hashlib.sha256("\n".join(identities).encode()).hexdigest()
    return len(fds), len(endpoints), digest


def _sample_group_indices(runtime: LaneRuntime, group_index: int) -> tuple[int, ...]:
    if not 0 <= group_index < SAMPLE_GROUPS:
        raise FanInProtocolError("sample_group_invalid")
    if len(runtime.clients) != runtime.target_clients:
        raise FanInProtocolError("sample_partition_incomplete")
    start = group_index * CLIENTS_PER_SAMPLE_GROUP
    return tuple(
        ((start + offset) * runtime.target_clients) // SAMPLED_QUERIES_PER_LANE
        for offset in range(CLIENTS_PER_SAMPLE_GROUP)
    )


async def _sample_group(runtime: LaneRuntime, group_index: int) -> None:
    _set_worker_operation(
        f"sample_group_{group_index}",
        phase="hold_sample",
    )
    clients = [runtime.clients[index] for index in _sample_group_indices(runtime, group_index)]
    runtime.sample_attempted += len(clients)
    results = await asyncio.gather(
        *(client.sample_select_one() for client in clients),
        return_exceptions=True,
    )
    successes = sum(not isinstance(result, BaseException) for result in results)
    runtime.sample_succeeded += successes
    runtime.sample_failed += len(results) - successes
    for client, result in zip(clients, results, strict=True):
        if isinstance(result, BaseException):
            runtime.record_connection_diagnostic(
                ordinal=client.ordinal,
                stage="sample",
                code="sample_query_failed",
            )


async def _hold_and_sample(
    lanes: Sequence[LaneRuntime],
    observers: Sequence[DirectObserver],
    *,
    t0_ns: int,
    start_cpu: float,
    network_start: tuple[int, int],
    telemetry_summary: TelemetrySummary,
    hold_started_ns: int | None = None,
    sample_groups: Sequence[int] = tuple(range(SAMPLE_GROUPS)),
) -> tuple[float, bool]:
    hold_started_ns = hold_started_ns or time.monotonic_ns()
    observer_stop = asyncio.Event()

    async def observe(observer: DirectObserver) -> bool:
        while not observer_stop.is_set():
            if not await observer.sample():
                return False
            try:
                await asyncio.wait_for(observer_stop.wait(), timeout=0.1)
            except TimeoutError:
                pass
        return True

    observer_tasks = [asyncio.create_task(observe(observer)) for observer in observers]
    final_observer_results: Sequence[object] = ()
    observer_results: Sequence[object] = ()
    try:
        for group in sample_groups:
            target_offset = 1.0 + group * ((HOLD_SECONDS - 2.0) / SAMPLE_GROUPS)
            remaining = hold_started_ns / 1_000_000_000 + target_offset - time.monotonic()
            if remaining > 0:
                await asyncio.sleep(remaining)
            # The 250ms offset is between lanes, not after each one: with two lanes this is
            # first lane, wait, second lane, exactly as before, and with one lane there is
            # nothing to stagger against.
            for lane_index, sampled_lane in enumerate(lanes):
                if lane_index:
                    await asyncio.sleep(0.25)
                await _sample_group(sampled_lane, group)
            hold_elapsed = (time.monotonic_ns() - hold_started_ns) / 1_000_000
            telemetry = await _telemetry_off_loop(
                start_cpu,
                t0_ns,
                network_start,
                telemetry_summary.peak_raw_event_loop_p99_ms,
            )
            telemetry_summary.observe(telemetry)
            for lane in lanes:
                _progress(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "protocol": PROTOCOL,
                        "lane_id": lane.lane_id,
                        "phase": "hold",
                        "initiated_clients": lane.initiated,
                        "authenticated_clients": lane.authenticated,
                        "held_clients": sum(
                            client.ready and not client.closed.done() for client in lane.clients
                        ),
                        "elapsed_ms": (time.monotonic_ns() - t0_ns) / 1_000_000,
                        "time_to_target_ms": (
                            lane.target_elapsed_ns / 1_000_000
                            if lane.target_elapsed_ns is not None
                            else None
                        ),
                        "hold_remaining_ms": max(0.0, HOLD_SECONDS * 1_000 - hold_elapsed),
                        "sampled_queries_succeeded": lane.sample_succeeded,
                        "sampled_queries_failed": lane.sample_failed,
                        **telemetry,
                    }
                )
            if telemetry_summary.hard_failures:
                break
        remaining = HOLD_SECONDS - (time.monotonic_ns() - hold_started_ns) / 1_000_000_000
        if remaining > 0 and not telemetry_summary.hard_failures:
            await asyncio.sleep(remaining)
        if not telemetry_summary.hard_failures:
            observer_stop.set()
            observer_results = await asyncio.gather(
                *observer_tasks,
                return_exceptions=True,
            )
            observer_tasks = []
            _set_worker_operation("final_observer_sample", phase="hold_verify")
            final_observer_results = await asyncio.gather(
                *(observer.sample() for observer in observers),
                return_exceptions=True,
            )
            telemetry_summary.observe(
                await _telemetry_off_loop(
                    start_cpu,
                    t0_ns,
                    network_start,
                    telemetry_summary.peak_raw_event_loop_p99_ms,
                )
            )
    finally:
        observer_stop.set()
        if observer_tasks:
            observer_results = await asyncio.gather(
                *observer_tasks,
                return_exceptions=True,
            )
    observer_ok = not any(
        isinstance(result, BaseException) or result is False
        for result in (*observer_results, *final_observer_results)
    )
    return (time.monotonic_ns() - hold_started_ns) / 1_000_000, observer_ok


async def _close_everything(
    lanes: Sequence[LaneRuntime],
    observers: Sequence[DirectObserver],
) -> bool:
    clients = [client for lane in lanes for client in lane.clients]
    for client in clients:
        client.close()
    if clients:
        try:
            async with asyncio.timeout(5.0):
                await asyncio.gather(*(client.closed for client in clients))
        except TimeoutError:
            pass
    await asyncio.gather(*(observer.close() for observer in observers))
    return all(client.closed.done() for client in clients)


def _lane_result(
    runtime: LaneRuntime,
    observer: DirectObserver,
    *,
    hold_elapsed_ms: float,
    launch_skew_ms: float,
    telemetry_verified: bool,
    telemetry_summary: TelemetrySummary,
    generator_digest: str,
    config_digest: str,
    model_digest: str,
    t0_ns: int,
) -> dict[str, object]:
    distinct_fds, distinct_endpoints, socket_digest = _socket_evidence(runtime)
    current_held = sum(client.ready and not client.closed.done() for client in runtime.clients)
    identity_verified = (
        all(client.tls_version and client.cipher for client in runtime.clients)
        and runtime.database["user"] == CLIENT_ROLE
        and runtime.observer_database["user"] == OBSERVER_ROLE
        and len(runtime.auth_methods) == 1
        and runtime.auth_methods <= SUPPORTED_AUTH_METHODS
    )
    auth_method = next(iter(runtime.auth_methods), "")
    evidence = observer.evidence
    result = {
        "lane_id": runtime.lane_id,
        "initiated_clients": runtime.initiated,
        "authenticated_clients": runtime.authenticated,
        "cancelled_clients": runtime.cancelled,
        "held_clients_at_gate": current_held,
        "terminal_failures": runtime.terminal_failures,
        "failure_codes": dict(sorted(runtime.failure_codes.items())),
        "connection_diagnostics": list(runtime.connection_diagnostics),
        "retries": runtime.retries,
        "disconnected_during_hold": runtime.disconnected_during_hold,
        "time_to_target_ns": runtime.target_elapsed_ns,
        "time_to_target_ms": (
            runtime.target_elapsed_ns / 1_000_000 if runtime.target_elapsed_ns is not None else None
        ),
        "hold_elapsed_ms": hold_elapsed_ms,
        "sampled_queries_attempted": runtime.sample_attempted,
        "sampled_queries_succeeded": runtime.sample_succeeded,
        "sampled_queries_failed": runtime.sample_failed,
        "preexisting_client_role_sessions": evidence.preexisting,
        "observer_role": evidence.observer_role,
        "client_role": evidence.client_role,
        "observer_direct": evidence.direct,
        "observer_sample_attempts": evidence.sample_attempts,
        "observer_sample_failures": evidence.sample_failures,
        "observer_reconnect_attempts": evidence.reconnect_attempts,
        "observer_reconnect_elapsed_ms": evidence.reconnect_elapsed_ms,
        "observer_last_failure_code": evidence.last_failure_code,
        "observer_last_sqlstate": evidence.last_sqlstate,
        "observer_last_connection_state": evidence.last_connection_state,
        "current_backend_sessions": evidence.current_backends,
        "peak_backend_sessions": evidence.peak_backends,
        "unique_backend_pids": len(evidence.pids),
        "distinct_socket_fds": distinct_fds,
        "distinct_local_endpoints": distinct_endpoints,
        "socket_identity_sha256": socket_digest,
        "connect_latency_p50_ms": percentile(runtime.connect_latencies_ms, 0.50),
        "connect_latency_p95_ms": percentile(runtime.connect_latencies_ms, 0.95),
        "connect_latency_p99_ms": percentile(runtime.connect_latencies_ms, 0.99),
        "endpoint_host_sha256": hashlib.sha256(str(runtime.database["host"]).encode()).hexdigest(),
        "credential_sha256": str(runtime.database["credential_sha256"]),
        "observer_credential_sha256": str(runtime.observer_database["credential_sha256"]),
        "tls_mode": TLS_MODE,
        "auth_method": auth_method,
        "config_sha256": config_digest,
        "generator_sha256": generator_digest,
        "capacity_model_sha256": model_digest,
        "launch_skew_ms": launch_skew_ms,
        "achieved_elapsed_ms": (time.monotonic_ns() - t0_ns) / 1_000_000,
        "identity_verified": identity_verified,
        "fairness_verified": (
            launch_skew_ms <= MAX_LAUNCH_SKEW_MS and auth_method in SUPPORTED_AUTH_METHODS
        ),
        "telemetry_verified": telemetry_verified,
        **telemetry_summary.public_dict(),
    }
    if runtime.target_clients != TARGET_CLIENTS_PER_LANE:
        result["connect_latency_samples_ms"] = list(runtime.connect_latencies_ms)
        result["observer_backend_pids"] = sorted(evidence.pids)
        result["first_launch_ns"] = runtime.first_launch_ns
    return result


def aggregate_worker_results(
    results: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    if len(results) != WORKER_COUNT:
        raise FanInProtocolError("fanin_worker_count_invalid")
    by_index = {int(result.get("worker_index", -1)): result for result in results}
    if set(by_index) != set(range(WORKER_COUNT)):
        raise FanInProtocolError("fanin_worker_index_invalid")
    ordered = [by_index[index] for index in range(WORKER_COUNT)]
    release_values = {int(result.get("release_ns", -1)) for result in ordered}
    hold_values = {int(result.get("hold_ns", -1)) for result in ordered}
    worker_cpus = {int(result.get("worker_cpu", -1)) for result in ordered}
    run_ids = {str(result.get("run_id") or "") for result in ordered}
    for name, expected in (
        ("schema_version", SCHEMA_VERSION),
        ("protocol", PROTOCOL),
        ("contract_sha256", contract_sha256()),
        ("config_sha256", config_sha256()),
        ("generator_sha256", generator_sha256()),
        ("capacity_model_sha256", capacity_model_sha256()),
        ("worker_count", WORKER_COUNT),
        ("partition_target_clients", PARTITION_CLIENTS_PER_LANE),
        ("worker_outcome", "completed"),
    ):
        if any(result.get(name) != expected for result in ordered):
            raise FanInProtocolError(f"fanin_worker_{name}_mismatch")
    if (
        len(release_values) != 1
        or next(iter(release_values)) < 1
        or len(hold_values) != 1
        or next(iter(hold_values)) <= next(iter(release_values))
        or len(run_ids) != 1
        or not next(iter(run_ids))
    ):
        raise FanInProtocolError("fanin_worker_release_mismatch")
    if len(worker_cpus) != WORKER_COUNT or min(worker_cpus) < 0:
        raise FanInProtocolError("fanin_worker_affinity_mismatch")

    # Collected from what the workers report rather than seeded with both names. Seeding left
    # an empty list for a lane that did not run, and the aggregation then compared four
    # workers' views of nothing.
    worker_lanes: dict[str, list[Mapping[str, object]]] = {}
    for result in ordered:
        lanes = result.get("lanes")
        if not isinstance(lanes, list):
            raise FanInProtocolError("fanin_worker_lanes_invalid")
        indexed = {str(lane.get("lane_id")): lane for lane in lanes if isinstance(lane, Mapping)}
        # The first worker establishes which lanes this bout ran; every other worker must
        # report exactly those. Comparing against a pre-seeded pair would have required both
        # lanes of every bout, and comparing against nothing would accept any worker
        # reporting any lane, so the set is taken once and then enforced.
        if not indexed or not set(indexed) <= LANE_IDS:
            raise FanInProtocolError("fanin_worker_lanes_invalid")
        if not worker_lanes:
            worker_lanes = {lane_id: [] for lane_id in indexed}
        elif set(indexed) != set(worker_lanes):
            raise FanInProtocolError("fanin_worker_lanes_invalid")
        for lane_id in worker_lanes:
            worker_lanes[lane_id].append(indexed[lane_id])

    telemetry_values = [
        result.get("telemetry")
        for result in ordered
        if isinstance(result.get("telemetry"), Mapping)
    ]
    if len(telemetry_values) != WORKER_COUNT:
        raise FanInProtocolError("fanin_worker_telemetry_invalid")
    mandatory_telemetry = {
        "safety_evidence_version",
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
        "telemetry_raw_event_loop_warning_count",
        "telemetry_raw_event_loop_ceiling_breaches",
        "telemetry_peak_cpu_capacity_fraction",
        "hard_safety_verified",
        "port_accounting_verified",
        "telemetry_failures",
        "telemetry_advisories",
        "admission_controller_min_concurrency",
        "admission_controller_reductions",
        "admission_controller_recoveries",
        "admission_controller_throttled_ms",
    }
    for telemetry in telemetry_values:
        if not mandatory_telemetry <= set(telemetry):
            raise FanInProtocolError("fanin_worker_telemetry_missing")
        numeric = mandatory_telemetry - {
            "hard_safety_verified",
            "port_accounting_verified",
            "telemetry_failures",
            "telemetry_advisories",
        }
        if any(
            isinstance(telemetry[name], bool)
            or not isinstance(telemetry[name], (int, float))
            or not math.isfinite(float(telemetry[name]))
            or float(telemetry[name]) < 0
            for name in numeric
        ):
            raise FanInProtocolError("fanin_worker_telemetry_malformed")
        if (
            telemetry["telemetry_samples"] <= 0
            or telemetry["telemetry_physical_memory_bytes"] <= 0
            or telemetry["telemetry_fd_soft_limit"] <= 0
            or telemetry["telemetry_ephemeral_port_count"] <= 0
            or telemetry["hard_safety_verified"] is not True
            or telemetry["port_accounting_verified"] is not True
            or not isinstance(telemetry["telemetry_failures"], list)
            or not isinstance(telemetry["telemetry_advisories"], list)
        ):
            raise FanInProtocolError("fanin_worker_telemetry_malformed")
    telemetry_failures = sorted(
        {
            str(failure)
            for telemetry in telemetry_values
            for failure in telemetry.get("telemetry_failures", [])
        }
    )
    telemetry_advisories = sorted(
        {
            str(advisory)
            for telemetry in telemetry_values
            for advisory in telemetry.get("telemetry_advisories", [])
        }
    )
    for code in (*telemetry_failures, *telemetry_advisories):
        classify_safety_code(code)
    if any(
        value.get("safety_evidence_version") != SAFETY_EVIDENCE_VERSION
        for value in telemetry_values
    ):
        raise FanInProtocolError("fanin_worker_safety_evidence_version_mismatch")
    aggregate_telemetry: dict[str, object] = {
        "safety_evidence_version": SAFETY_EVIDENCE_VERSION,
        "telemetry_samples": sum(int(value["telemetry_samples"]) for value in telemetry_values),
        "telemetry_physical_memory_bytes": int(
            telemetry_values[0]["telemetry_physical_memory_bytes"]
        ),
        "telemetry_min_available_memory_bytes": min(
            int(value["telemetry_min_available_memory_bytes"]) for value in telemetry_values
        ),
        "telemetry_peak_rss_bytes": sum(
            int(value["telemetry_peak_rss_bytes"]) for value in telemetry_values
        ),
        "telemetry_fd_soft_limit": int(telemetry_values[0]["telemetry_fd_soft_limit"]),
        "telemetry_peak_open_fds": sum(
            int(value["telemetry_peak_open_fds"]) for value in telemetry_values
        ),
        "telemetry_ephemeral_port_count": int(
            telemetry_values[0]["telemetry_ephemeral_port_count"]
        ),
        "telemetry_peak_ephemeral_ports_in_use": max(
            int(value["telemetry_peak_ephemeral_ports_in_use"])
            for value in telemetry_values
        ),
        "telemetry_min_ephemeral_port_reserve": min(
            int(value["telemetry_min_ephemeral_port_reserve"]) for value in telemetry_values
        ),
        "telemetry_peak_event_loop_p99_ms": max(
            float(value["telemetry_peak_event_loop_p99_ms"]) for value in telemetry_values
        ),
        "telemetry_peak_raw_event_loop_p99_ms": max(
            float(
                value.get(
                    "telemetry_peak_raw_event_loop_p99_ms",
                    value["telemetry_peak_event_loop_p99_ms"],
                )
            )
            for value in telemetry_values
        ),
        "telemetry_peak_external_event_loop_p99_ms": max(
            float(value.get("telemetry_peak_external_event_loop_p99_ms", 0.0))
            for value in telemetry_values
        ),
        "telemetry_raw_event_loop_warning_count": sum(
            int(value.get("telemetry_raw_event_loop_warning_count", 0))
            for value in telemetry_values
        ),
        "telemetry_raw_event_loop_ceiling_breaches": sum(
            int(value.get("telemetry_raw_event_loop_ceiling_breaches", 0))
            for value in telemetry_values
        ),
        # A peak is a maximum.  Averaging four worker peaks hid a saturated
        # shard behind three idle ones and made this diagnostic contradict its
        # own field name.
        "telemetry_peak_cpu_capacity_fraction": max(
            float(value["telemetry_peak_cpu_capacity_fraction"])
            for value in telemetry_values
        ),
        "hard_safety_verified": (
            not telemetry_failures
            and all(value.get("hard_safety_verified") is True for value in telemetry_values)
        ),
        "port_accounting_verified": all(
            value["port_accounting_verified"] is True
            for value in telemetry_values
        ),
        "telemetry_failures": telemetry_failures,
        "telemetry_advisories": telemetry_advisories,
        "admission_controller_min_concurrency": min(
            int(value["admission_controller_min_concurrency"])
            for value in telemetry_values
        ),
        "admission_controller_reductions": sum(
            int(value["admission_controller_reductions"])
            for value in telemetry_values
        ),
        "admission_controller_recoveries": sum(
            int(value["admission_controller_recoveries"])
            for value in telemetry_values
        ),
        "admission_controller_throttled_ms": sum(
            float(value["admission_controller_throttled_ms"])
            for value in telemetry_values
        ),
        "admission_controller_recovery_hysteresis_intervals": (
            ADMISSION_RECOVERY_CLEAN_INTERVALS
        ),
        "admission_controller_pressure_hysteresis_intervals": (
            ADMISSION_PRESSURE_INTERVALS
        ),
    }

    aggregated_lanes: list[dict[str, object]] = []
    all_first_launches: list[int] = []
    for values in worker_lanes.values():
        all_first_launches.extend(int(value["first_launch_ns"]) for value in values)
    launch_skew_ms = (max(all_first_launches) - min(all_first_launches)) / 1_000_000
    for lane_id, values in worker_lanes.items():
        worker_target_elapsed_ns = [
            int(value.get("time_to_target_ns") or 0) for value in values
        ]
        parent_hold_elapsed_ns = next(iter(hold_values)) - next(iter(release_values))
        if any(
            value <= 0 or value > parent_hold_elapsed_ns
            for value in worker_target_elapsed_ns
        ):
            raise FanInProtocolError("fanin_worker_target_timestamp_invalid")
        if any(
            float(value.get("hold_elapsed_ms") or 0.0) < HOLD_SECONDS * 1_000
            or float(value.get("achieved_elapsed_ms") or 0.0)
            < parent_hold_elapsed_ns / 1_000_000 + HOLD_SECONDS * 1_000
            for value in values
        ):
            raise FanInProtocolError("fanin_worker_completion_chronology_invalid")
        exact_gate_elapsed_ns = max(worker_target_elapsed_ns)
        samples = [
            float(sample)
            for value in values
            for sample in value.get("connect_latency_samples_ms", [])
        ]
        pids = {int(pid) for value in values for pid in value.get("observer_backend_pids", [])}
        connection_diagnostics = [
            {
                "worker_index": worker_index,
                "ordinal": int(item.get("ordinal", -1)),
                "stage": str(item.get("stage") or "unknown"),
                "code": str(item.get("code") or "unknown"),
            }
            for worker_index, value in enumerate(values)
            for item in value.get("connection_diagnostics", [])
            if isinstance(item, Mapping)
        ][:CONNECTION_DIAGNOSTIC_LIMIT]
        exact_fields = (
            "endpoint_host_sha256",
            "credential_sha256",
            "observer_credential_sha256",
            "tls_mode",
            "auth_method",
            "config_sha256",
            "generator_sha256",
            "capacity_model_sha256",
            "observer_role",
            "client_role",
        )
        # Before comparing, rule out the reading that produces a false disagreement. A worker
        # that authenticated nobody has an empty auth-method set, and an empty set reads as
        # the empty string, so it disagrees with every sibling that connected. That is a
        # connect failure wearing an identity failure's name.
        silent = sorted(
            str(value["lane_id"]) + ":" + str(index)
            for index, value in enumerate(values)
            if not int(value["authenticated_clients"])
        )
        if silent:
            print(
                "LANE_IDENTITY_DIAGNOSTIC_JSON:"
                + canonical_json(
                    {
                        "lane_id": lane_id,
                        "reason": "worker_authenticated_none",
                        "authenticated_by_worker": [
                            int(value["authenticated_clients"]) for value in values
                        ],
                        # Whether anything was attempted at all separates "the lane never
                        # started" from "every attempt failed", and the codes say which
                        # failure it was: a refusal, an expired connect timeout on an endpoint
                        # still waking, or a rejected credential.
                        "initiated_by_worker": [
                            int(value["initiated_clients"]) for value in values
                        ],
                        "terminal_failures_by_worker": [
                            int(value["terminal_failures"]) for value in values
                        ],
                        "failure_codes_by_worker": [
                            dict(value.get("failure_codes") or {}) for value in values
                        ],
                        "observer_connection_state_by_worker": [
                            str(value.get("observer_connection_state") or "") for value in values
                        ],
                    }
                ).decode("utf-8"),
                flush=True,
            )
            raise FanInProtocolError(f"fanin_worker_authenticated_none_{lane_id}"[:64])
        disagreed = sorted(
            field for field in exact_fields if len({str(value[field]) for value in values}) != 1
        )
        preexisting_values = {
            int(value["preexisting_client_role_sessions"])
            for value in values
        }
        if len(preexisting_values) != 1:
            raise FanInProtocolError(
                "fanin_worker_preexisting_session_observation_mismatch"
            )
        if disagreed:
            # The per-worker values, once, so a genuine disagreement does not cost another
            # twelve-minute reproduction to characterise. Auth methods are the fixed
            # vocabulary this file defines and the counts are integers, so nothing here names
            # a host, an ARN or a credential.
            print(
                "LANE_IDENTITY_DIAGNOSTIC_JSON:"
                + canonical_json(
                    {
                        "lane_id": lane_id,
                        "reason": "workers_disagreed",
                        "fields": disagreed,
                        "auth_method_by_worker": [str(value["auth_method"]) for value in values],
                        "authenticated_by_worker": [
                            int(value["authenticated_clients"]) for value in values
                        ],
                    }
                ).decode("utf-8"),
                flush=True,
            )
            # The field, not just the fact. Ten candidates share this gate and they send an
            # operator to ten different places; two live bouts died here with both setup
            # gates verified and no way to tell which one moved without another twelve-minute
            # Proxy build. Field names are the same fixed snake_case vocabulary the refusal
            # contract already allows, so they are safe to repeat, and the token stays inside
            # the encoder's 64-character bound.
            token = "fanin_worker_lane_identity_mismatch_" + "_".join(disagreed)
            raise FanInProtocolError(token[:64].rstrip("_"))
        initiated = sum(int(value["initiated_clients"]) for value in values)
        authenticated = sum(int(value["authenticated_clients"]) for value in values)
        cancelled = sum(int(value.get("cancelled_clients", 0)) for value in values)
        held = sum(int(value["held_clients_at_gate"]) for value in values)
        terminal_failures = sum(int(value["terminal_failures"]) for value in values)
        worker_counts_exact = all(
            int(value["initiated_clients"]) == PARTITION_CLIENTS_PER_LANE
            and int(value["authenticated_clients"]) == PARTITION_CLIENTS_PER_LANE
            and int(value.get("cancelled_clients", 0)) == 0
            and int(value["held_clients_at_gate"]) == PARTITION_CLIENTS_PER_LANE
            for value in values
        )
        auth_method = str(values[0]["auth_method"])
        aggregated_lanes.append(
            {
                "lane_id": lane_id,
                "initiated_clients": initiated,
                "authenticated_clients": authenticated,
                "cancelled_clients": cancelled,
                "held_clients_at_gate": held,
                "terminal_failures": terminal_failures,
                "failure_codes": {
                    code: sum(int(value.get("failure_codes", {}).get(code, 0)) for value in values)
                    for code in sorted(
                        {str(code) for value in values for code in value.get("failure_codes", {})}
                    )
                },
                "connection_diagnostics": connection_diagnostics,
                "retries": sum(int(value["retries"]) for value in values),
                "disconnected_during_hold": sum(
                    int(value["disconnected_during_hold"]) for value in values
                ),
                # The 10K clock stops only when the parent has validated all
                # four exact-held proofs. Individual authentication timestamps
                # can precede that synchronized gate and are diagnostic only.
                "time_to_target_ns": exact_gate_elapsed_ns,
                "time_to_target_ms": exact_gate_elapsed_ns / 1_000_000,
                "hold_elapsed_ms": min(float(value["hold_elapsed_ms"]) for value in values),
                "sampled_queries_attempted": sum(
                    int(value["sampled_queries_attempted"]) for value in values
                ),
                "sampled_queries_succeeded": sum(
                    int(value["sampled_queries_succeeded"]) for value in values
                ),
                "sampled_queries_failed": sum(
                    int(value["sampled_queries_failed"]) for value in values
                ),
                "preexisting_client_role_sessions": max(preexisting_values),
                "observer_role": values[0]["observer_role"],
                "client_role": values[0]["client_role"],
                "observer_direct": all(bool(value["observer_direct"]) for value in values),
                "observer_sample_attempts": sum(
                    int(value.get("observer_sample_attempts", 0)) for value in values
                ),
                "observer_sample_failures": sum(
                    int(value.get("observer_sample_failures", 0)) for value in values
                ),
                "observer_reconnect_attempts": sum(
                    int(value.get("observer_reconnect_attempts", 0)) for value in values
                ),
                "observer_reconnect_elapsed_ms": sum(
                    float(value.get("observer_reconnect_elapsed_ms", 0.0)) for value in values
                ),
                "observer_failure_codes": sorted(
                    {
                        str(value["observer_last_failure_code"])
                        for value in values
                        if value.get("observer_last_failure_code")
                    }
                ),
                "observer_sqlstates": sorted(
                    {
                        str(value["observer_last_sqlstate"])
                        for value in values
                        if value.get("observer_last_sqlstate")
                    }
                ),
                "observer_connection_states": sorted(
                    {
                        str(value["observer_last_connection_state"])
                        for value in values
                        if value.get("observer_last_connection_state")
                    }
                ),
                "current_backend_sessions": max(
                    int(value["current_backend_sessions"]) for value in values
                ),
                "peak_backend_sessions": max(
                    int(value["peak_backend_sessions"]) for value in values
                ),
                "unique_backend_pids": len(pids),
                "distinct_socket_fds": sum(int(value["distinct_socket_fds"]) for value in values),
                "distinct_local_endpoints": sum(
                    int(value["distinct_local_endpoints"]) for value in values
                ),
                "socket_identity_sha256": hashlib.sha256(
                    canonical_json(
                        {
                            "lane_id": lane_id,
                            "workers": [
                                {
                                    "worker_index": index,
                                    "digest": value["socket_identity_sha256"],
                                }
                                for index, value in enumerate(values)
                            ],
                        }
                    )
                ).hexdigest(),
                "connect_latency_p50_ms": percentile(samples, 0.50),
                "connect_latency_p95_ms": percentile(samples, 0.95),
                "connect_latency_p99_ms": percentile(samples, 0.99),
                "endpoint_host_sha256": values[0]["endpoint_host_sha256"],
                "credential_sha256": values[0]["credential_sha256"],
                "observer_credential_sha256": values[0]["observer_credential_sha256"],
                "tls_mode": values[0]["tls_mode"],
                "auth_method": auth_method,
                "config_sha256": values[0]["config_sha256"],
                "generator_sha256": values[0]["generator_sha256"],
                "capacity_model_sha256": values[0]["capacity_model_sha256"],
                "launch_skew_ms": launch_skew_ms,
                "achieved_elapsed_ms": max(float(value["achieved_elapsed_ms"]) for value in values),
                "identity_verified": (
                    initiated == TARGET_CLIENTS_PER_LANE
                    and authenticated == TARGET_CLIENTS_PER_LANE
                    and held == TARGET_CLIENTS_PER_LANE
                    and worker_counts_exact
                    and all(bool(value["identity_verified"]) for value in values)
                ),
                "fairness_verified": (
                    launch_skew_ms <= MAX_LAUNCH_SKEW_MS and auth_method in SUPPORTED_AUTH_METHODS
                ),
                "telemetry_verified": aggregate_telemetry["hard_safety_verified"],
                **aggregate_telemetry,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol": PROTOCOL,
        "run_id": ordered[0]["run_id"],
        "contract_sha256": ordered[0]["contract_sha256"],
        "config_sha256": ordered[0]["config_sha256"],
        "generator_sha256": ordered[0]["generator_sha256"],
        "capacity_model_sha256": ordered[0]["capacity_model_sha256"],
        "release_ns": next(iter(release_values)),
        "hold_ns": next(iter(hold_values)),
        "worker_count": WORKER_COUNT,
        "worker_cpus": sorted(worker_cpus),
        "lanes": aggregated_lanes,
        "telemetry": aggregate_telemetry,
        "runtime_diagnostics": {
            "worker_count": WORKER_COUNT,
            "workers": [
                {
                    "worker_index": index,
                    "worker_cpu": int(result["worker_cpu"]),
                    # Bounded here rather than at the source: a worker's own diagnostics are
                    # complete in its own process and in the queue, and it is only the
                    # four-way multiplication into one SSM response that has to fit.
                    **bounded_worker_diagnostics(result.get("runtime_diagnostics", {})),
                }
                for index, result in enumerate(ordered)
            ],
        },
    }


async def execute_fanin(
    request: Mapping[str, object],
    *,
    cancelled: asyncio.Event,
    trust_bundle_path: Path,
    worker_index: int = 0,
    worker_count: int = 1,
    partition_target_clients: int = TARGET_CLIENTS_PER_LANE,
    await_release: Callable[[], Awaitable[int]] | None = None,
    await_hold: Callable[
        [Callable[[], Mapping[str, Mapping[str, object]]]],
        Awaitable[int],
    ]
    | None = None,
    before_teardown: Callable[[], Awaitable[None]] | None = None,
    worker_cpu: int | None = None,
    capacity_preflight_verified: bool = False,
) -> dict[str, object]:
    global _runtime_diagnostics, _socket_states_cache_ns
    global _worker_execution_context
    _socket_states_cache.clear()
    _socket_states_cache_ns = 0
    if request.get("schema_version") != SCHEMA_VERSION or request.get("protocol") != PROTOCOL:
        raise FanInProtocolError("fanin_schema_invalid")
    if request.get("action") not in {"run", "run_lane_v3"}:
        raise FanInProtocolError("fanin_action_invalid")
    if (
        worker_count < 1
        or not 0 <= worker_index < worker_count
        or partition_target_clients < 1
        or partition_target_clients * worker_count != TARGET_CLIENTS_PER_LANE
    ):
        raise FanInProtocolError("fanin_worker_partition_invalid")
    expected_contract = _digest(request.get("contract_sha256"), "contract_digest_invalid")
    expected_config = _digest(request.get("config_sha256"), "config_digest_invalid")
    expected_generator = _digest(request.get("generator_sha256"), "generator_digest_invalid")
    expected_model = _digest(request.get("capacity_model_sha256"), "capacity_model_digest_invalid")
    if (
        expected_contract != contract_sha256()
        or expected_config != config_sha256()
        or expected_generator != generator_sha256()
        or expected_model != capacity_model_sha256()
    ):
        raise FanInProtocolError("fanin_digest_mismatch")
    raw_targets = request.get("targets")
    # One lane or two. A lane holds 10,000 clients on its own clock, and requiring two made
    # the fast lane wait through the slow lane's eleven-minute Proxy build, suspend at its
    # idle floor, and arrive cold. The count is the request's to decide.
    if (
        not isinstance(raw_targets, list)
        or not 1 <= len(raw_targets) <= RUNNER_LANE_COUNT
    ):
        raise FanInProtocolError("fanin_targets_invalid")
    targets = {
        str(value.get("lane_id") or ""): value
        for value in raw_targets
        if isinstance(value, Mapping)
    }
    # The lanes this request names, checked against the runtime set rather than against a
    # literal pair. A lane holds its 10,000 clients on its own clock, so one lane is a
    # complete bout for that lane; what must never happen is a lane this runner does not
    # know, or a duplicate hiding one behind the other.
    lane_order = sorted(targets)
    if not lane_order or len(lane_order) != len(raw_targets) or not set(lane_order) <= LANE_IDS:
        raise FanInProtocolError("fanin_targets_invalid")
    ssl_context = ssl.create_default_context(cafile=str(trust_bundle_path))
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    run_id = str(request.get("run_id") or "")
    lanes: list[LaneRuntime] = []
    observers: list[DirectObserver] = []
    key_cache = ScramKeyCache()
    for lane_id in lane_order:
        target = targets[lane_id]
        database = dict(target.get("database") or {})
        observer_database = dict(target.get("observer_database") or {})
        required = {"host", "port", "dbname", "user", "password", "credential_sha256"}
        if set(database) != required or set(observer_database) != required:
            raise FanInProtocolError("fanin_database_binding_invalid")
        if database["user"] != CLIENT_ROLE or observer_database["user"] != OBSERVER_ROLE:
            raise FanInProtocolError("fanin_role_binding_invalid")
        database["sslrootcert"] = str(trust_bundle_path)
        observer_database["sslrootcert"] = str(trust_bundle_path)
        application_name = f"{APP_PREFIX}-{run_id}-{lane_id}"
        runtime = LaneRuntime(
            lane_id=lane_id,
            database=database,
            observer_database=observer_database,
            application_name=application_name,
            ssl_context=ssl_context,
            key_cache=key_cache,
            target_clients=partition_target_clients,
        )
        lanes.append(runtime)
        observers.append(DirectObserver(lane_id, observer_database, application_name))
    _worker_execution_context = {
        "worker_id": worker_index,
        "worker_cpu": worker_cpu,
        "phase": "setup",
        "operation": "observer_preflight",
        "wave": 0,
        "partition_start": worker_index * partition_target_clients,
        "partition_end_exclusive": (worker_index + 1) * partition_target_clients,
        "lanes": lanes,
        "observers": observers,
    }
    await asyncio.gather(*(observer.open_and_preflight() for observer in observers))
    if not capacity_preflight_verified:
        preflight = await capacity_preflight(str(request.get("runner_instance_type") or ""))
        if not preflight["sufficient"]:
            failures = "_".join(str(value) for value in preflight.get("failures", ()))
            raise FanInProtocolError(
                f"runner_capacity_insufficient_{failures}"
                if failures
                else "runner_capacity_insufficient_without_evidence"
            )
    loop = asyncio.get_running_loop()
    for lane in lanes:
        addresses = await loop.getaddrinfo(
            str(lane.database["host"]),
            int(lane.database["port"]),
            type=socket.SOCK_STREAM,
        )
        connect_hosts = sorted(
            {str(address[4][0]) for address in addresses if len(address) >= 5 and address[4]}
        )
        if not connect_hosts:
            raise FanInProtocolError(f"{lane.lane_id}_host_resolution_failed")
        lane.connect_host = connect_hosts[0]
    diagnostics = RuntimeDiagnostics()
    _runtime_diagnostics = diagnostics
    gc.callbacks.append(diagnostics.gc_callback)
    diagnostics.install_selector_probe(loop)
    diagnostics.install_ready_batch_limit(loop)
    diagnostics.install_execution_probes(loop)
    controlled_gc = ControlledGC(diagnostics)
    network_start = _network_bytes()
    controlled_gc.start()
    ready_ns: dict[str, int] = {}
    for lane in lanes:
        ready_ns[lane.lane_id] = time.monotonic_ns()
    release_ns = await await_release() if await_release is not None else time.monotonic_ns()
    start_cpu = time.process_time()
    telemetry_summary = TelemetrySummary()
    diagnostic_evidence: dict[str, object] = {}
    hold_elapsed_ms = 0.0
    observer_ok = True
    ramp_ready = False
    shared_hold_started_ns: int | None = None
    admission_controller = AdmissionController(len(lanes))
    try:
        wave_number = 0
        while any(lane.initiated < lane.target_clients for lane in lanes):
            if cancelled.is_set():
                break
            remaining = min(lane.target_clients - lane.initiated for lane in lanes)
            wave = min(INITIAL_WAVE_SIZE, remaining)
            wave_number += 1
            _set_worker_operation(
                "open_equal_wave",
                phase="ramp",
                wave=wave_number,
            )
            await _open_equal_wave_guarded(
                lanes,
                wave,
                release_ns,
                start_cpu=start_cpu,
                network_start=network_start,
                telemetry_summary=telemetry_summary,
                admission_controller=admission_controller,
            )
            if cancelled.is_set():
                break
            if telemetry_summary.hard_failures:
                break
            telemetry = await _telemetry_off_loop(
                start_cpu,
                release_ns,
                network_start,
                telemetry_summary.peak_raw_event_loop_p99_ms,
            )
            telemetry_summary.observe(telemetry)
            if telemetry_summary.hard_failures:
                break
            if any(lane.terminal_failures for lane in lanes):
                break
            for lane in lanes:
                milestone = lane.authenticated // 1_000
                if (
                    milestone > lane.last_progress_milestone
                    or lane.authenticated == lane.target_clients
                ):
                    lane.last_progress_milestone = milestone
                    _progress(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "protocol": PROTOCOL,
                            "lane_id": lane.lane_id,
                            "phase": "ramp",
                            "initiated_clients": lane.initiated,
                            "authenticated_clients": lane.authenticated,
                            "held_clients": sum(
                                client.ready and not client.closed.done()
                                for client in lane.clients
                            ),
                            "terminal_failures": lane.terminal_failures,
                            "time_to_target_ms": (
                                lane.target_elapsed_ns / 1_000_000
                                if lane.target_elapsed_ns is not None
                                else None
                            ),
                            "elapsed_ms": (time.monotonic_ns() - release_ns) / 1_000_000,
                            **telemetry,
                        }
                    )
        def current_ramp_proof() -> dict[str, dict[str, object]]:
            # Deliberately recomputed.  The first proof says every shard reached
            # its partition; the parent then asks for a second, fresh socket
            # count before it mints the one shared hold epoch.
            return {
                lane.lane_id: {
                    "initiated": lane.initiated,
                    "authenticated": lane.authenticated,
                    "held": sum(
                        client.ready and not client.closed.done()
                        for client in lane.clients
                    ),
                    "terminal_failures": lane.terminal_failures,
                    "cancelled": lane.cancelled,
                    "target_elapsed_ns": lane.target_elapsed_ns or 0,
                    "run_id": run_id,
                    "lane_id": lane.lane_id,
                    "worker_index": worker_index,
                    "release_ns": release_ns,
                }
                for lane in lanes
            }

        ramp_proof = current_ramp_proof()
        if (
            telemetry_summary.verified
            and all(
                proof["initiated"] == lane.target_clients
                and proof["authenticated"] == lane.target_clients
                and proof["held"] == lane.target_clients
                and proof["terminal_failures"] == 0
                and proof["cancelled"] == 0
                for lane in lanes
                for proof in (ramp_proof[lane.lane_id],)
            )
        ):
            _set_worker_operation("await_shared_hold", phase="hold_barrier")
            shared_hold_started_ns = (
                await await_hold(current_ramp_proof)
                if await_hold is not None
                else time.monotonic_ns()
            )
            ramp_ready = True
            if not cancelled.is_set():
                _set_worker_operation("hold_and_sample", phase="hold")
                hold_elapsed_ms, observer_ok = await _hold_and_sample(
                    lanes,
                    observers,
                    t0_ns=release_ns,
                    start_cpu=start_cpu,
                    network_start=network_start,
                    telemetry_summary=telemetry_summary,
                    hold_started_ns=shared_hold_started_ns,
                    sample_groups=tuple(range(worker_index, SAMPLE_GROUPS, worker_count)),
                )
        launches = [lane.first_launch_ns for lane in lanes if lane.first_launch_ns is not None]
        launch_skew_ms = (
            (max(launches) - min(launches)) / 1_000_000 if len(launches) == 2 else math.inf
        )
        raw_lanes = [
            _lane_result(
                lane,
                observer,
                hold_elapsed_ms=hold_elapsed_ms,
                launch_skew_ms=launch_skew_ms,
                telemetry_verified=telemetry_summary.verified,
                telemetry_summary=telemetry_summary,
                generator_digest=expected_generator,
                config_digest=expected_config,
                model_digest=expected_model,
                t0_ns=release_ns,
            )
            for lane, observer in zip(lanes, observers, strict=True)
        ]
        for raw_lane in raw_lanes:
            raw_lane.update(admission_controller.public_dict())
        expected_worker_samples = (
            len(tuple(range(worker_index, SAMPLE_GROUPS, worker_count)))
            * CLIENTS_PER_SAMPLE_GROUP
        )
        local_gate_complete = (
            ramp_ready
            and observer_ok
            and hold_elapsed_ms >= HOLD_SECONDS * 1_000
            and all(
                int(lane["initiated_clients"]) == partition_target_clients
                and int(lane["authenticated_clients"]) == partition_target_clients
                and int(lane["cancelled_clients"]) == 0
                and int(lane["held_clients_at_gate"]) == partition_target_clients
                and int(lane["terminal_failures"]) == 0
                and int(lane["disconnected_during_hold"]) == 0
                and int(lane["sampled_queries_attempted"]) == expected_worker_samples
                and int(lane["sampled_queries_succeeded"]) == expected_worker_samples
                and int(lane["sampled_queries_failed"]) == 0
                for lane in raw_lanes
            )
        )
        worker_outcome = (
            "cancelled"
            if cancelled.is_set()
            else "hard_safety_failed"
            if telemetry_summary.hard_failures
            else "completed"
            if local_gate_complete
            else "partial_result"
        )
        diagnostic_evidence.update(diagnostics.public_dict())
        return {
            "schema_version": SCHEMA_VERSION,
            "protocol": PROTOCOL,
            "run_id": run_id,
            "contract_sha256": expected_contract,
            "config_sha256": expected_config,
            "generator_sha256": expected_generator,
            "capacity_model_sha256": expected_model,
            "release_ns": release_ns,
            "hold_ns": shared_hold_started_ns,
            "worker_index": worker_index,
            "worker_count": worker_count,
            "worker_cpu": worker_cpu,
            "partition_target_clients": partition_target_clients,
            "worker_outcome": worker_outcome,
            "launch_ready_ns_by_lane": ready_ns,
            "lanes": raw_lanes,
            "telemetry": {
                **telemetry_summary.public_dict(),
                **admission_controller.public_dict(),
            },
            "runtime_diagnostics": diagnostic_evidence,
        }
    except BaseException:
        if _worker_execution_context is not None:
            _worker_execution_context["lane_counts"] = {
                lane.lane_id: {
                    "initiated": lane.initiated,
                    "authenticated": lane.authenticated,
                    "cancelled": lane.cancelled,
                    "terminal_failures": lane.terminal_failures,
                    "held": sum(
                        client.ready and not client.closed.done() for client in lane.clients
                    ),
                }
                for lane in lanes
            }
        raise
    finally:
        try:
            if before_teardown is not None and lanes:
                await asyncio.shield(before_teardown())
            cleanup_verified = await asyncio.shield(_close_everything(lanes, observers))
            if not cleanup_verified:
                raise FanInProtocolError("fanin_socket_cleanup_incomplete")
        finally:
            controlled_gc.stop()
            diagnostic_evidence.clear()
            diagnostic_evidence.update(diagnostics.public_dict())
            if diagnostics.gc_callback in gc.callbacks:
                gc.callbacks.remove(diagnostics.gc_callback)
            diagnostics.restore_execution_probes()
            diagnostics.restore_selector_probe()
            diagnostics.restore_ready_batch_limit()
            _runtime_diagnostics = None
