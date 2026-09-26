from __future__ import annotations

import asyncio
import base64
import gc
import inspect
import ssl
import struct
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from runner import round5_fanin as runner
from server.connection_fanin import (
    ADVISORY_LAUNCH_SKEW_MS,
    FANIN_PROTOCOL,
    FANIN_SCHEMA_VERSION,
    LAUNCH_SKEW_SEMANTICS,
    MAX_PREEXISTING_CLIENT_SESSIONS,
    MAX_RETRIES,
    OWNED_STALL_READY_BATCH,
    CapacityPreflight,
    ConnectionSpikeArm,
    ConnectionSpikeContract,
    FanInError,
    capacity_model_sha256,
    compare_lanes,
    evaluate_capacity_preflight,
    evaluate_runner_provisioning_capacity,
    fanin_config_sha256,
    finalize_lane,
    require_runner_provisioning_capacity,
)
from server.connection_spike_live import (
    RUNNER_ASSETS,
    ConnectionSpikeLiveOperationError,
    LiveConnectionSpikeAdapter,
    _finalize_raw_result,
    _runner_result_has_forbidden_credential,
)

SHA = "a" * 64


def test_auth_method_label_is_not_mistaken_for_credential_material() -> None:
    assert not _runner_result_has_forbidden_credential(
        {
            "lanes": [
                {"auth_method": "tls-cleartext-password"},
                {"auth_method": "scram-sha-256"},
            ]
        }
    )
    assert _runner_result_has_forbidden_credential(
        {"lane": {"innocuous_name": "password=must-never-escape"}}
    )


def passing_preflight() -> CapacityPreflight:
    return evaluate_capacity_preflight(
        instance_type="c7i.2xlarge",
        cpu_count=8,
        physical_memory_bytes=15 * 1024**3,
        available_memory_bytes=14 * 1024**3,
        baseline_rss_bytes=100 * 1024**2,
        fd_soft_limit=65_535,
        fd_hard_limit=65_535,
        open_fds=7,
        ephemeral_port_first=20_000,
        ephemeral_port_last=49_999,
        event_loop_p99_ms=0.1,
        cpu_calibration_ms=50.0,
    )


def raw_lane(
    lane_id: str,
    *,
    held: int = 10_000,
    time_to_target_ms: float | None = 12_500.0,
    peak_backends: int = 37,
    preexisting: int = 0,
    observer_role: str = "anti_demo_observer",
    client_role: str = "anti_demo_burst",
    config_digest: str | None = None,
) -> dict[str, object]:
    return {
        "lane_id": lane_id,
        "initiated_clients": 10_000,
        "authenticated_clients": held,
        "held_clients_at_gate": held,
        "terminal_failures": 0,
        "failure_codes": {},
        "retries": 0,
        "disconnected_during_hold": 0,
        "time_to_target_ns": (
            int(time_to_target_ms * 1_000_000)
            if time_to_target_ms is not None
            else None
        ),
        "time_to_target_ms": time_to_target_ms,
        "hold_elapsed_ms": 30_000.001,
        "sampled_queries_attempted": 64,
        "sampled_queries_succeeded": 64,
        "sampled_queries_failed": 0,
        "preexisting_client_role_sessions": preexisting,
        "observer_role": observer_role,
        "client_role": client_role,
        "observer_direct": True,
        "current_backend_sessions": 5,
        "peak_backend_sessions": peak_backends,
        "unique_backend_pids": 19,
        "distinct_socket_fds": held,
        "distinct_local_endpoints": held,
        "socket_identity_sha256": SHA,
        "connect_latency_p50_ms": 80.0,
        "connect_latency_p95_ms": 150.0,
        "connect_latency_p99_ms": 201.455,
        "endpoint_host_sha256": SHA,
        "credential_sha256": SHA,
        "observer_credential_sha256": SHA,
        "tls_mode": "verify-full",
        "auth_method": (
            "tls-cleartext-password"
            if lane_id == "lakebase"
            else "scram-sha-256"
        ),
        "config_sha256": config_digest or fanin_config_sha256(),
        "generator_sha256": SHA,
        "capacity_model_sha256": capacity_model_sha256(),
        "telemetry_samples": 49,
        "telemetry_physical_memory_bytes": 15 * 1024**3,
        "telemetry_min_available_memory_bytes": 7 * 1024**3,
        "telemetry_peak_rss_bytes": 7 * 1024**3,
        "telemetry_fd_soft_limit": 65_535,
        "telemetry_peak_open_fds": 20_264,
        "telemetry_ephemeral_port_count": 28_232,
        "telemetry_peak_ephemeral_ports_in_use": 10_000,
        "telemetry_min_ephemeral_port_reserve": 18_232,
        "telemetry_peak_event_loop_p99_ms": 4.5,
        "telemetry_peak_raw_event_loop_p99_ms": 72.929,
        "telemetry_peak_external_event_loop_p99_ms": 72.929,
        "telemetry_raw_event_loop_warning_count": 1,
        "telemetry_raw_event_loop_ceiling_breaches": 0,
        "telemetry_peak_cpu_capacity_fraction": 0.31,
        "telemetry_failures": [],
        "telemetry_advisories": [],
        "safety_evidence_version": runner.SAFETY_EVIDENCE_VERSION,
        "hard_safety_verified": True,
        "port_accounting_verified": True,
        "admission_controller_min_concurrency": runner.LANE_CONNECT_CONCURRENCY,
        "admission_controller_reductions": 0,
        "admission_controller_recoveries": 0,
        "admission_controller_throttled_ms": 0.0,
        "admission_controller_recovery_hysteresis_intervals": (
            runner.ADMISSION_RECOVERY_CLEAN_INTERVALS
        ),
        "admission_controller_pressure_hysteresis_intervals": (
            runner.ADMISSION_PRESSURE_INTERVALS
        ),
        "launch_skew_ms": 0.25,
        "achieved_elapsed_ms": 42_500.0,
        "identity_verified": True,
        "fairness_verified": True,
        "telemetry_verified": True,
    }


def finalize(raw: dict[str, object]):
    return finalize_lane(
        raw,
        expected_lane_id=str(raw["lane_id"]),
        expected_config_sha256=fanin_config_sha256(),
        expected_generator_sha256=SHA,
        expected_capacity_model_sha256=capacity_model_sha256(),
        cleanup_verified=True,
    )


def test_exact_10k_launch_skew_over_advisory_still_verifies() -> None:
    # Over the 10 ms advisory target still verifies. Do not re-attach a
    # launch-skew comparison to fairness or any other fatal gate.
    raw = raw_lane("lakebase")
    raw["launch_skew_ms"] = 10.53

    lane = finalize(raw)

    assert lane.initiated_clients == 10_000
    assert lane.authenticated_clients == 10_000
    assert lane.held_clients_at_gate == 10_000
    assert lane.launch_skew_ms == pytest.approx(10.53)
    assert lane.gates.fairness is True
    assert lane.verified is True


@pytest.mark.parametrize("skew", [None, float("nan")])
def test_missing_or_nonfinite_launch_skew_evidence_fails_closed(
    skew: float | None,
) -> None:
    raw = raw_lane("lakebase")
    if skew is None:
        raw.pop("launch_skew_ms")
    else:
        raw["launch_skew_ms"] = skew

    with pytest.raises(FanInError, match="launch_skew_ms_invalid"):
        finalize(raw)


def worker_result(index: int, *, release_ns: int = 123) -> dict[str, object]:
    lanes = []
    for lane_id in ("lakebase", "competitor"):
        lane = raw_lane(
            lane_id,
            held=runner.PARTITION_CLIENTS_PER_LANE,
            time_to_target_ms=40_000.0 + index,
            config_digest=runner.config_sha256(),
        )
        lane.update(
            {
                "initiated_clients": runner.PARTITION_CLIENTS_PER_LANE,
                "cancelled_clients": 0,
                "sampled_queries_attempted": 16,
                "sampled_queries_succeeded": 16,
                "distinct_socket_fds": runner.PARTITION_CLIENTS_PER_LANE,
                "distinct_local_endpoints": runner.PARTITION_CLIENTS_PER_LANE,
                "generator_sha256": runner.generator_sha256(),
                "capacity_model_sha256": runner.capacity_model_sha256(),
                "connect_latency_samples_ms": [10.0 + index, 20.0 + index],
                "observer_backend_pids": [index + 1],
                "first_launch_ns": release_ns + index * 100,
                "telemetry_peak_rss_bytes": 512 * 1024**2,
                "telemetry_peak_open_fds": 5_064,
                "telemetry_peak_cpu_capacity_fraction": 0.1,
                "achieved_elapsed_ms": 85_000.0,
            }
        )
        lanes.append(lane)
    telemetry = {
        key: value
        for key, value in lanes[0].items()
        if key.startswith("telemetry_")
    }
    for key in (
        "safety_evidence_version",
        "hard_safety_verified",
        "port_accounting_verified",
        "admission_controller_min_concurrency",
        "admission_controller_reductions",
        "admission_controller_recoveries",
        "admission_controller_throttled_ms",
        "admission_controller_recovery_hysteresis_intervals",
        "admission_controller_pressure_hysteresis_intervals",
    ):
        telemetry[key] = lanes[0][key]
    return {
        "schema_version": runner.SCHEMA_VERSION,
        "protocol": runner.PROTOCOL,
        "run_id": "run",
        "contract_sha256": runner.contract_sha256(),
        "config_sha256": runner.config_sha256(),
        "generator_sha256": runner.generator_sha256(),
        "capacity_model_sha256": runner.capacity_model_sha256(),
        "release_ns": release_ns,
        "hold_ns": release_ns + 50_000_000_000,
        "worker_index": index,
        "worker_count": runner.WORKER_COUNT,
        "worker_cpu": index,
        "partition_target_clients": runner.PARTITION_CLIENTS_PER_LANE,
        "worker_outcome": "completed",
        "lanes": lanes,
        "telemetry": telemetry,
        "runtime_diagnostics": {"loop_samples": 10},
    }


def test_contract_and_runner_digests_are_exactly_the_same() -> None:
    contract = ConnectionSpikeContract()
    assert contract.sha256 == runner.contract_sha256()
    assert fanin_config_sha256() == runner.config_sha256()
    assert capacity_model_sha256() == runner.capacity_model_sha256()
    assert contract.protocol == FANIN_PROTOCOL
    assert contract.schema_version == FANIN_SCHEMA_VERSION
    with pytest.raises(ValueError, match="frozen"):
        replace(contract, target_clients_per_lane=9_999)
    with pytest.raises(ValueError, match="frozen"):
        replace(contract, initial_wave_size=251)


def test_public_contract_marks_launch_skew_advisory_not_fatal() -> None:
    """max_launch_skew_ms is a compatibility alias, not a validity max.

    Verification already records skew over 10 ms without failing an exact
    10k proof. Re-hardening that number, dropping launch_skew_semantics, or
    treating the old key as a fatal gate is a contract regression.
    """

    contract = ConnectionSpikeContract()
    public = contract.public_dict
    runner_public = runner.contract_values()
    assert public["advisory_launch_skew_ms"] == ADVISORY_LAUNCH_SKEW_MS
    assert public["max_launch_skew_ms"] == public["advisory_launch_skew_ms"]
    assert public["launch_skew_semantics"] == LAUNCH_SKEW_SEMANTICS
    assert "not_fatal" in str(public["launch_skew_semantics"])
    assert runner_public["advisory_launch_skew_ms"] == public["advisory_launch_skew_ms"]
    assert runner_public["max_launch_skew_ms"] == public["max_launch_skew_ms"]
    assert runner_public["launch_skew_semantics"] == public["launch_skew_semantics"]
    with pytest.raises(ValueError, match="frozen"):
        replace(contract, advisory_launch_skew_ms=0.0)
    with pytest.raises(ValueError, match="frozen"):
        replace(contract, launch_skew_semantics="fatal_validity_max")
    with pytest.raises(ValueError, match="frozen"):
        replace(contract, max_launch_skew_ms=0.0)


def test_capacity_model_uses_measured_limits_and_fails_the_known_tight_runner() -> None:
    sufficient = passing_preflight()
    assert sufficient.sufficient
    assert sufficient.projected_fds == 10_263
    assert sufficient.model_sha256 == capacity_model_sha256()

    tight = evaluate_capacity_preflight(
        instance_type="m6i.large",
        cpu_count=2,
        physical_memory_bytes=int(7.6 * 1024**3),
        available_memory_bytes=int(7.2 * 1024**3),
        baseline_rss_bytes=100 * 1024**2,
        fd_soft_limit=65_535,
        fd_hard_limit=65_535,
        open_fds=7,
        ephemeral_port_first=32_768,
        ephemeral_port_last=60_999,
        event_loop_p99_ms=0.1,
        cpu_calibration_ms=50.0,
    )
    assert not tight.sufficient
    assert "runner_instance_type" in tight.failures
    assert "runner_cpu_count" in tight.failures

    pressured = evaluate_capacity_preflight(
        instance_type="c7i.2xlarge",
        cpu_count=8,
        physical_memory_bytes=15 * 1024**3,
        available_memory_bytes=14 * 1024**3,
        baseline_rss_bytes=100 * 1024**2,
        fd_soft_limit=65_535,
        fd_hard_limit=65_535,
        open_fds=7,
        ephemeral_port_first=20_000,
        ephemeral_port_last=49_999,
        event_loop_p99_ms=0.1,
        event_loop_microbatch_p99_ms=50.001,
        cpu_calibration_ms=50.0,
    )
    assert pressured.sufficient
    assert pressured.failures == ()
    assert pressured.telemetry_advisories == ("event_loop_microbatch_pressure",)


def test_preprovision_capacity_refuses_large_and_accepts_the_selected_shape() -> None:
    large = evaluate_runner_provisioning_capacity("m6i.large")
    assert not large.sufficient
    assert large.selected.vcpu_count == 2
    assert "runner_cpu_count" in large.failures
    with pytest.raises(
        ValueError,
        match=r"use c7i\.2xlarge.*No client-count, memory-reserve",
    ):
        require_runner_provisioning_capacity("m6i.large")

    selected = require_runner_provisioning_capacity("c7i.2xlarge")
    assert selected.sufficient
    assert selected.selected.vcpu_count == 8
    # m6i.xlarge still clears the memory and FD model, so the table must keep
    # evaluating it -- but require_ refuses any shape that is not the selected one,
    # because provisioning a merely-adequate shape is how the sealed contract and
    # the built instance drift apart.
    assert evaluate_runner_provisioning_capacity("m6i.xlarge").sufficient
    assert selected.usable_memory_bytes > selected.required_memory_bytes
    assert selected.projected_fds == 10_256


@pytest.mark.parametrize(
    ("mutation", "failed_gate"),
    (
        ({"held_clients_at_gate": 9_999, "distinct_socket_fds": 9_999,
          "distinct_local_endpoints": 9_999}, "exact_count"),
        ({"terminal_failures": 1}, "zero_failures"),
        ({"cancelled_clients": 1}, "zero_failures"),
        ({"retries": MAX_RETRIES + 1}, "zero_failures"),
        ({"hold_elapsed_ms": 29_999.999}, "hold"),
        ({"sampled_queries_succeeded": 63, "sampled_queries_failed": 1},
         "sampled_queries"),
        ({"peak_backend_sessions": 10_000}, "multiplexing"),
        # Above the ceiling nothing is attributable, so the lane fails by its own name
        # rather than as an observer that was not separate.
        ({"preexisting_client_role_sessions": MAX_PREEXISTING_CLIENT_SESSIONS + 1},
         "clean_start"),
        ({"observer_role": "anti_demo_burst"}, "observer_separation"),
        ({"observer_direct": False}, "observer_separation"),
        ({"connect_latency_p95_ms": None}, "identity"),
        ({"connect_latency_p95_ms": 79.0}, "identity"),
        ({"auth_method": ""}, "identity"),
        ({"auth_method": "md5"}, "identity"),
        ({"telemetry_verified": False}, "telemetry"),
        ({"telemetry_min_available_memory_bytes": 767 * 1024**2}, "telemetry"),
        ({"telemetry_peak_open_fds": 52_429}, "telemetry"),
        ({"telemetry_min_ephemeral_port_reserve": 1_999}, "telemetry"),
        ({"telemetry_failures": ["available_memory_reserve_exhausted"]}, "telemetry"),
        ({"safety_evidence_version": runner.SAFETY_EVIDENCE_VERSION - 1}, "telemetry"),
        ({"hard_safety_verified": False}, "telemetry"),
        ({"admission_controller_min_concurrency": 1}, "telemetry"),
        ({"telemetry_peak_ephemeral_ports_in_use": 10_001}, "telemetry"),
    ),
)
def test_exact_stop_gate_rejects_every_mutation(
    mutation: dict[str, object],
    failed_gate: str,
) -> None:
    raw = {**raw_lane("lakebase"), **mutation}
    result = finalize(raw)
    assert not result.verified
    assert failed_gate in result.gates.failures


@pytest.mark.parametrize("retries", [1, MAX_RETRIES])
def test_retried_logins_inside_the_budget_still_verify_the_exact_lane(retries: int) -> None:
    """2026-09-26: one pooler refusal (08P01) at the 10,000-client ceiling failed a bout.

    A retried login is inside the timed window and reported on the lane; it is not a
    terminal failure, and it cannot stand in for a missing client or a hold disconnect.
    """

    result = finalize({**raw_lane("lakebase"), "retries": retries})

    assert result.verified
    assert "zero_failures" not in result.gates.failures


def test_advisory_loop_cpu_and_scheduling_pressure_do_not_fail_exact_proof() -> None:
    raw = raw_lane("lakebase")
    raw.update(
        {
            "telemetry_peak_event_loop_p99_ms": 60.499,
            "telemetry_peak_raw_event_loop_p99_ms": 71.721,
            "telemetry_peak_external_event_loop_p99_ms": 71.721,
            "telemetry_peak_cpu_capacity_fraction": 0.99,
            "telemetry_advisories": [
                "cpu_pressure",
                "event_loop_pressure",
                "host_scheduling_instability",
            ],
            "admission_controller_min_concurrency": (
                runner.MIN_ADMISSION_CONCURRENCY_PER_LANE
            ),
            "admission_controller_reductions": 3,
            "admission_controller_recoveries": 2,
            "admission_controller_throttled_ms": 123.0,
        }
    )

    result = finalize(raw)

    assert result.verified
    assert result.gates.telemetry
    assert result.telemetry_advisories == (
        "cpu_pressure",
        "event_loop_pressure",
        "host_scheduling_instability",
    )


def test_unknown_telemetry_code_is_a_protocol_error_not_a_soft_default() -> None:
    raw = raw_lane("lakebase")
    raw["telemetry_advisories"] = ["future_pressure"]

    with pytest.raises(FanInError, match="telemetry_code_unknown"):
        finalize(raw)

    with pytest.raises(runner.FanInProtocolError, match="unknown_safety_code"):
        runner.classify_safety_code("memoryish_pressure")


def test_missing_or_malformed_mandatory_safety_evidence_fails_hard() -> None:
    missing = runner.TelemetrySummary()
    missing.observe({})
    assert missing.hard_failures == {"mandatory_safety_evidence_missing"}
    assert not missing.verified

    malformed = runner.TelemetrySummary()
    malformed.observe(
        {
            "physical_memory_bytes": "lots",
            "available_memory_bytes": 7 * 1024**3,
            "rss_bytes": 100 * 1024**2,
            "fd_soft_limit": 65_535,
            "open_fds": 7,
            "ephemeral_port_count": 28_232,
            "ephemeral_ports_in_use": 100,
            "ephemeral_ports_remaining": 28_132,
            "event_loop_p99_ms": 1.0,
            "cpu_capacity_fraction": 0.1,
        }
    )
    assert malformed.hard_failures == {"mandatory_safety_evidence_malformed"}
    assert not malformed.verified


def test_admission_controller_reduces_then_recovers_without_cancelling() -> None:
    controller = runner.AdmissionController(lane_count=1)
    assert controller.current_concurrency == runner.LANE_CONNECT_CONCURRENCY

    controller.observe_interval(pressured=True)
    assert controller.current_concurrency == runner.LANE_CONNECT_CONCURRENCY
    controller.observe_interval(pressured=True)
    reduced = controller.current_concurrency
    assert runner.MIN_ADMISSION_CONCURRENCY_PER_LANE <= reduced
    assert reduced < runner.LANE_CONNECT_CONCURRENCY
    assert controller.reductions == 1

    for _ in range(runner.ADMISSION_RECOVERY_CLEAN_INTERVALS - 1):
        controller.observe_interval(pressured=False)
        assert controller.current_concurrency == reduced
        assert controller.recoveries == 0
    controller.observe_interval(pressured=False)
    assert controller.current_concurrency == (
        reduced + runner.ADMISSION_RECOVERY_STEP_PER_LANE
    )
    assert controller.recoveries == 1


def test_admission_hysteresis_requires_consecutive_mixed_cadence_intervals() -> None:
    controller = runner.AdmissionController(lane_count=1)
    initial = controller.current_concurrency
    for pressured in (True, False, True):
        controller.observe_interval(pressured=pressured)
    assert controller.current_concurrency == initial
    controller.observe_interval(pressured=True)
    reduced = controller.current_concurrency
    assert reduced < initial

    for pressured in (False, False, True, False, False):
        controller.observe_interval(pressured=pressured)
    assert controller.current_concurrency == reduced
    controller.observe_interval(pressured=False)
    assert controller.current_concurrency == (
        reduced + runner.ADMISSION_RECOVERY_STEP_PER_LANE
    )


def test_cpu_and_loop_pressure_cadences_do_not_combine() -> None:
    controller = runner.AdmissionController(lane_count=1)
    initial = controller.current_concurrency
    controller.observe_loop_interval(pressured=True)
    controller.observe_cpu_interval(pressured=True)
    assert controller.current_concurrency == initial
    controller.observe_cpu_interval(pressured=True)
    reduced = controller.current_concurrency
    assert reduced < initial

    for _ in range(runner.ADMISSION_RECOVERY_CLEAN_INTERVALS):
        controller.observe_cpu_interval(pressured=False)
    assert controller.current_concurrency == reduced
    controller.observe_loop_interval(pressured=False)
    for _ in range(runner.ADMISSION_RECOVERY_CLEAN_INTERVALS):
        controller.observe_cpu_interval(pressured=False)
    assert controller.current_concurrency > reduced


def test_a_warm_pool_inside_the_ceiling_still_verifies() -> None:
    """The other half of the bound, and the reason it exists.

    Setup proves the new RDS Proxy is ready by running an application transaction through
    it, which leaves a backend session open as the client role, and a proxy holds its pool.
    A lane is therefore entitled to start warm. What it may not do is start with enough
    sessions to account for the multiplexing being claimed, which is what the ceiling
    bounds: at most the connections this protocol has in flight to one lane at once, two
    orders of magnitude under the 10,000 clients the multiplexing gate compares against.
    """

    lane = finalize(raw_lane("lakebase", preexisting=1))
    assert lane.gates.clean_start
    assert lane.verified
    # Recorded, not merely tolerated, so the room can see what the lane started with.
    assert lane.preexisting_client_role_sessions == 1


def test_a_lane_at_the_ceiling_still_verifies_and_one_over_does_not() -> None:
    """The boundary itself, so the ceiling cannot drift by one in either direction."""

    assert finalize(raw_lane("lakebase", preexisting=MAX_PREEXISTING_CLIENT_SESSIONS)).verified
    over = finalize(raw_lane("lakebase", preexisting=MAX_PREEXISTING_CLIENT_SESSIONS + 1))
    assert not over.gates.clean_start
    assert not over.verified
    assert "clean_start" in over.gates.failures


def test_the_ceiling_is_not_zero() -> None:
    """Stated directly, because a ceiling of zero would satisfy every other assertion.

    Zero is what it used to be, and it is what made a pooled lane impossible to start.
    """

    assert MAX_PREEXISTING_CLIENT_SESSIONS >= 1


def test_backend_sessions_are_evidence_not_client_count() -> None:
    result = finalize(raw_lane("lakebase", peak_backends=37))
    assert result.verified
    assert result.held_clients_at_gate == 10_000
    assert result.peak_backend_sessions == 37
    assert result.peak_backend_sessions != result.held_clients_at_gate


def test_runtime_telemetry_summary_fails_on_each_pressure_dimension() -> None:
    summary = runner.TelemetrySummary()
    healthy = {
        "physical_memory_bytes": 15 * 1024**3,
        "available_memory_bytes": 7 * 1024**3,
        "rss_bytes": 7 * 1024**3,
        "fd_soft_limit": 65_535,
        "open_fds": 20_264,
        "ephemeral_port_count": 28_232,
        "ephemeral_ports_in_use": 10_000,
        "ephemeral_ports_remaining": 18_232,
        "event_loop_p99_ms": 4.5,
        "cpu_capacity_fraction": 0.31,
    }
    summary.observe(healthy)
    assert summary.verified
    assert summary.public_dict()["telemetry_failures"] == []

    pressure = runner.TelemetrySummary()
    pressure.observe(
        {
            **healthy,
            "available_memory_bytes": 767 * 1024**2,
            "open_fds": 60_000,
            "ephemeral_port_count": 11_999,
            "ephemeral_ports_in_use": 10_000,
            "ephemeral_ports_remaining": 1_999,
            "event_loop_p99_ms": 50.001,
            "cpu_capacity_fraction": 0.851,
        }
    )
    assert not pressure.verified
    assert set(pressure.public_dict()["telemetry_failures"]) == {
        "available_memory_reserve_exhausted",
        "ephemeral_port_reserve_exhausted",
        "file_descriptor_reserve_exhausted",
    }
    assert pressure.public_dict()["telemetry_advisories"] == ["cpu_pressure"]


def test_ephemeral_port_safety_measures_actual_host_namespace_ports(
    tmp_path,
) -> None:
    (tmp_path / "sys/net/ipv4").mkdir(parents=True)
    (tmp_path / "sys/net/ipv4/ip_local_port_range").write_text(
        "40000 40003\n",
        encoding="ascii",
    )
    (tmp_path / "self/fd").mkdir(parents=True)
    (tmp_path / "self/fd/3").symlink_to("socket:[123]")
    (tmp_path / "net").mkdir()
    header = "sl local_address rem_address st tx_queue tr tm->when retrnsmt uid timeout inode"
    rows = [
        "0: 0100007F:9C40 0100007F:1538 01 0 0 0 1000 0 123",
        "1: 0100007F:9C41 0100007F:1538 01 0 0 0 1000 0 999",
    ]
    (tmp_path / "net/tcp").write_text(
        "\n".join((header, *rows)),
        encoding="ascii",
    )
    (tmp_path / "net/tcp6").write_text(header + "\n", encoding="ascii")

    assert runner._ephemeral_port_usage(tmp_path) == (4, 2, 2)


def test_unavailable_or_malformed_proc_tcp_evidence_fails_closed(tmp_path) -> None:
    with pytest.raises(runner.FanInProtocolError, match="mandatory_safety_evidence_missing"):
        runner._ephemeral_port_usage(tmp_path)

    (tmp_path / "sys/net/ipv4").mkdir(parents=True)
    (tmp_path / "sys/net/ipv4/ip_local_port_range").write_text(
        "40000 40003\n",
        encoding="ascii",
    )
    (tmp_path / "net").mkdir()
    (tmp_path / "net/tcp").write_text("header\nmalformed\n", encoding="ascii")
    with pytest.raises(runner.FanInProtocolError, match="mandatory_safety_evidence_malformed"):
        runner._ephemeral_port_usage(tmp_path)

    (tmp_path / "net/tcp").write_text("", encoding="ascii")
    with pytest.raises(runner.FanInProtocolError, match="mandatory_safety_evidence_malformed"):
        runner._ephemeral_port_usage(tmp_path)


async def test_selector_fanout_timeout_is_advisory_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def force_timeout(awaitable, **kwargs):
        del kwargs
        awaitable.cancel()
        raise TimeoutError

    monkeypatch.setattr(runner.asyncio, "wait_for", force_timeout)
    peak_ms, deferred, wakeups, sockets = (
        await runner._event_loop_selector_fanout_benchmark(sockets=1)
    )

    assert peak_ms == 5_000.0
    assert deferred >= 1
    assert wakeups >= 0
    assert sockets == 1


def test_winner_exists_only_after_both_exact_gates_under_same_protocol() -> None:
    lakebase = finalize(raw_lane("lakebase", time_to_target_ms=12_500.0))
    competitor = finalize(raw_lane("competitor", time_to_target_ms=20_000.0))
    comparison = compare_lanes(lakebase, competitor)
    assert comparison is not None
    assert comparison.winner_lane_id == "lakebase"
    assert comparison.margin_ms == 7_500.0

    incomplete = finalize(raw_lane("competitor", held=9_999, time_to_target_ms=None))
    assert compare_lanes(lakebase, incomplete) is None
    mismatch = replace(competitor, config_sha256="b" * 64)
    assert compare_lanes(lakebase, mismatch) is None


def test_stale_schema_and_digest_cannot_be_finalized_as_v2() -> None:
    arm = ConnectionSpikeArm(
        arm_id="arm",
        contract_sha256=ConnectionSpikeContract().sha256,
        config_sha256=fanin_config_sha256(),
        generator_sha256=SHA,
        capacity_model_sha256=capacity_model_sha256(),
        preflight=passing_preflight(),
    )
    raw = {
        "schema_version": FANIN_SCHEMA_VERSION,
        "protocol": FANIN_PROTOCOL,
        "contract_sha256": arm.contract_sha256,
        "config_sha256": arm.config_sha256,
        "generator_sha256": arm.generator_sha256,
        "capacity_model_sha256": arm.capacity_model_sha256,
        "runner_harness_sha256": arm.preflight.runner_harness_sha256,
        "runner_boot_id": arm.preflight.boot_id,
        "runner_asset_sha256s": {
            name: arm.generator_sha256 for name in RUNNER_ASSETS
        },
        "lanes": [raw_lane("lakebase"), raw_lane("competitor")],
        "runtime_diagnostics": {"loop_samples": 100},
    }
    result = _finalize_raw_result(arm, raw)
    assert result.comparison is not None
    assert result.runtime_diagnostics == {"loop_samples": 100}

    with pytest.raises(ConnectionSpikeLiveOperationError, match="stale"):
        _finalize_raw_result(arm, {**raw, "schema_version": 1})
    with pytest.raises(ConnectionSpikeLiveOperationError, match="stale"):
        _finalize_raw_result(arm, {**raw, "config_sha256": "b" * 64})
    with pytest.raises(ConnectionSpikeLiveOperationError, match="diagnostics"):
        _finalize_raw_result(arm, {**raw, "runtime_diagnostics": None})


def test_scram_tls_state_machine_derives_and_verifies_server_signature() -> None:
    state = runner.ScramState.new("anti_demo_burst", "correct horse battery staple", 7)
    salt = b"round5-salt"
    server_first = (
        f"r={state.nonce}server,s={base64.b64encode(salt).decode()},i=4096"
    )
    final = state.continue_message(server_first)
    assert final.startswith(b"c=biws,r=")
    assert b",p=" in final
    assert state.server_signature is not None
    state.verify_final(
        f"v={base64.b64encode(state.server_signature).decode()}"
    )
    with pytest.raises(runner.FanInProtocolError, match="signature"):
        state.verify_final(f"v={base64.b64encode(b'wrong').decode()}")


def test_scram_reuses_the_run_local_salted_key_without_reusing_a_nonce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = runner.hashlib.pbkdf2_hmac
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(runner.hashlib, "pbkdf2_hmac", counted)
    cache = runner.ScramKeyCache()
    salt = base64.b64encode(b"one-postgres-role-salt").decode()
    states = [
        runner.ScramState.new(
            "anti_demo_burst",
            "correct horse battery staple",
            ordinal,
            cache,
        )
        for ordinal in range(500)
    ]
    for state in states:
        state.continue_message(
            f"r={state.nonce}server,s={salt},i=4096"
        )

    assert calls == 1
    assert len({state.nonce for state in states}) == len(states)
    other = runner.ScramState.new(
        "anti_demo_burst",
        "different password",
        501,
        cache,
    )
    other.continue_message(f"r={other.nonce}server,s={salt},i=4096")
    assert calls == 2


async def test_direct_observer_retries_transient_wake_before_shared_t0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, *unused, **kwargs):
            assert kwargs["prepare"] is False

        async def fetchone(self):
            return (runner.OBSERVER_ROLE, 0)

    class Connection:
        def cursor(self):
            return Cursor()

        async def commit(self):
            return None

        async def close(self):
            return None

    attempts = 0

    async def connect(**unused):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise runner.psycopg.OperationalError("endpoint is waking")
        return Connection()

    monkeypatch.setattr(runner.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(runner, "OBSERVER_RETRY_SECONDS", 0.0)
    observer = runner.DirectObserver(
        "lakebase",
        {
            "host": "direct.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": runner.OBSERVER_ROLE,
            "password": "test-only",
            "credential_sha256": SHA,
        },
        "anti-demo-r5-test",
    )

    await observer.open_and_preflight()

    assert attempts == 2
    assert observer.evidence.role_verified is True
    assert observer.evidence.preexisting == 0
    await observer.close()


async def test_direct_observer_waits_for_setup_sessions_to_quiesce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [(runner.OBSERVER_ROLE, 1), (runner.OBSERVER_ROLE, 0)]

    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, *unused, **kwargs):
            assert kwargs["prepare"] is False

        async def fetchone(self):
            return rows.pop(0)

    class Connection:
        def cursor(self):
            return Cursor()

        async def commit(self):
            return None

        async def close(self):
            return None

    async def connect(**unused):
        return Connection()

    monkeypatch.setattr(runner.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(runner, "OBSERVER_RETRY_SECONDS", 0.0)
    observer = runner.DirectObserver(
        "competitor",
        {
            "host": "direct.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": runner.OBSERVER_ROLE,
            "password": "test-only",
            "credential_sha256": SHA,
        },
        "anti-demo-r5-test",
    )

    await observer.open_and_preflight()

    assert rows == []
    assert observer.evidence.preexisting == 0
    await observer.close()


async def test_direct_observer_names_lane_when_sessions_do_not_quiesce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, *unused, **kwargs):
            assert kwargs["prepare"] is False

        async def fetchone(self):
            return (runner.OBSERVER_ROLE, 1)

    class Connection:
        def cursor(self):
            return Cursor()

        async def commit(self):
            return None

        async def close(self):
            return None

    async def connect(**unused):
        return Connection()

    monkeypatch.setattr(runner.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(runner, "OBSERVER_QUIESCE_TIMEOUT_SECONDS", 0.0)
    observer = runner.DirectObserver(
        "competitor",
        {
            "host": "direct.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": runner.OBSERVER_ROLE,
            "password": "test-only",
            "credential_sha256": SHA,
        },
        "anti-demo-r5-test",
    )

    with pytest.raises(
        runner.FanInProtocolError,
        match="^competitor_preexisting_client_sessions$",
    ):
        await observer.open_and_preflight()


async def test_direct_observer_exhaustion_returns_a_safe_lane_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def refused(**unused):
        raise runner.psycopg.OperationalError(
            "password=must-not-escape host=private.example.test"
        )

    monkeypatch.setattr(runner.psycopg.AsyncConnection, "connect", refused)
    monkeypatch.setattr(runner, "OBSERVER_READY_TIMEOUT_SECONDS", 0.0)
    observer = runner.DirectObserver(
        "competitor",
        {
            "host": "direct.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": runner.OBSERVER_ROLE,
            "password": "test-only",
            "credential_sha256": SHA,
        },
        "anti-demo-r5-test",
    )

    with pytest.raises(
        runner.FanInProtocolError,
        match="^competitor_observer_connect_failed$",
    ):
        await observer.open_and_preflight()


def _observer_for_sample_test() -> runner.DirectObserver:
    return runner.DirectObserver(
        "competitor",
        {
            "host": "direct.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": runner.OBSERVER_ROLE,
            "password": "test-only",
            "credential_sha256": SHA,
        },
        "anti-demo-r5-test",
    )


async def test_direct_observer_reconnects_after_transient_transport_drop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        def __init__(self, connection) -> None:
            self.connection = connection
            self.row = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, query, parameters=None, **unused):
            if "current_user" in query:
                self.row = (runner.OBSERVER_ROLE,)
            elif self.connection.fail_sample:
                self.connection.fail_sample = False
                self.connection.broken = True
                raise runner.psycopg.errors.ConnectionFailure("transport dropped")
            else:
                assert parameters == (runner.CLIENT_ROLE,)
                self.row = (7, [101, 102])

        async def fetchone(self):
            return self.row

    class Connection:
        def __init__(self, *, fail_sample: bool) -> None:
            self.fail_sample = fail_sample
            self.closed = False
            self.broken = False

        def cursor(self):
            return Cursor(self)

        async def commit(self):
            return None

        async def close(self):
            self.closed = True

    initial = Connection(fail_sample=True)
    reconnects: list[dict[str, object]] = []

    async def connect(**kwargs):
        reconnects.append(kwargs)
        return Connection(fail_sample=False)

    observer = _observer_for_sample_test()
    observer.connection = initial
    monkeypatch.setattr(runner.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(runner, "OBSERVER_SAMPLE_RETRY_SECONDS", 0.0)

    assert await observer.sample() is True
    assert initial.closed is True
    assert observer.evidence.reconnect_attempts == 1
    assert observer.evidence.sample_failures == 1
    assert observer.evidence.last_sqlstate == "08006"
    assert observer.evidence.last_connection_state == "open"
    assert observer.evidence.current_backends == 7
    assert len(reconnects) == 1
    assert reconnects[0]["user"] == runner.OBSERVER_ROLE
    assert reconnects[0]["application_name"].endswith("-observer")


async def test_direct_observer_transient_reconnect_exhaustion_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        def __init__(self, connection) -> None:
            self.connection = connection

        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, query, *unused, **unused_keywords):
            if "current_user" not in query:
                self.connection.broken = True
                raise runner.psycopg.errors.ConnectionFailure("transport dropped")

        async def fetchone(self):
            return (runner.OBSERVER_ROLE,)

    class Connection:
        closed = False
        broken = False

        def cursor(self):
            return Cursor(self)

        async def commit(self):
            return None

        async def close(self):
            self.closed = True

    async def connect(**unused):
        return Connection()

    observer = _observer_for_sample_test()
    observer.connection = Connection()
    monkeypatch.setattr(runner.psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr(runner, "OBSERVER_SAMPLE_RETRY_SECONDS", 0.0)

    assert await observer.sample() is False
    assert observer.evidence.reconnect_attempts == runner.OBSERVER_SAMPLE_MAX_RETRIES
    assert observer.evidence.sample_failures == 3
    assert (
        observer.evidence.last_failure_code
        == "observer_transport_retry_exhausted"
    )
    assert observer.evidence.last_sqlstate == "08006"


async def test_direct_observer_permanent_failure_never_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, *unused, **unused_keywords):
            raise runner.psycopg.errors.InvalidPassword("invalid identity")

    class Connection:
        closed = False
        broken = False

        def cursor(self):
            return Cursor()

    async def must_not_connect(**unused):
        raise AssertionError("permanent observer failure was retried")

    observer = _observer_for_sample_test()
    observer.connection = Connection()
    monkeypatch.setattr(runner.psycopg.AsyncConnection, "connect", must_not_connect)

    assert await observer.sample() is False
    assert observer.evidence.reconnect_attempts == 0
    assert observer.evidence.last_sqlstate == "28P01"
    assert observer.evidence.last_failure_code == "observer_operational_permanent"


async def test_direct_observer_serializes_sample_ownership() -> None:
    in_flight = 0
    peak = 0

    class Cursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return None

        async def execute(self, *unused, **unused_keywords):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0)
            in_flight -= 1

        async def fetchone(self):
            return (1, [101])

    class Connection:
        closed = False
        broken = False

        def cursor(self):
            return Cursor()

        async def commit(self):
            return None

    observer = _observer_for_sample_test()
    observer.connection = Connection()

    assert await asyncio.gather(observer.sample(), observer.sample()) == [True, True]
    assert peak == 1


async def test_equal_wave_scheduler_alternates_lanes_and_never_tunes_one_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def opened(runtime, t0_ns):
        assert t0_ns == 123
        calls.append(runtime.lane_id)
        runtime.initiated += 1

    monkeypatch.setattr(runner, "_open_client", opened)
    lanes = [
        SimpleNamespace(lane_id="lakebase", initiated=0, target_clients=4),
        SimpleNamespace(lane_id="competitor", initiated=0, target_clients=4),
    ]
    await runner._open_equal_wave(
        lanes,
        4,
        123,
        runner.AdmissionController(lane_count=len(lanes)),
    )
    assert calls == [
        "lakebase", "competitor",
        "lakebase", "competitor",
        "lakebase", "competitor",
        "lakebase", "competitor",
    ]
    assert [lane.initiated for lane in lanes] == [4, 4]


async def test_cancelled_connect_is_explicitly_accounted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = asyncio.Event()

    class Client:
        def __init__(self, **unused) -> None:
            self.authenticated = asyncio.get_running_loop().create_future()
            self.closed = asyncio.get_running_loop().create_future()

        def close(self) -> None:
            if not self.closed.done():
                self.closed.set_result(None)

    async def blocked_create_connection(*unused_args, **unused_kwargs):
        created.set()
        await asyncio.Event().wait()

    runtime = SimpleNamespace(
        first_launch_ns=None,
        initiated=0,
        authenticated=0,
        cancelled=0,
        terminal_failures=0,
        target_clients=2_500,
        lane_id="lakebase",
        database={"host": "example.test", "port": 5432},
        connect_host="127.0.0.1",
        application_name="test",
        ssl_context=object(),
        unexpected_disconnect=lambda unused: None,
        key_cache=object(),
        clients=[],
        auth_methods=set(),
        connect_latencies_ms=[],
        target_elapsed_ns=None,
        failure_codes={},
    )
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(runner, "PostgresClient", Client)
    monkeypatch.setattr(loop, "create_connection", blocked_create_connection)

    operation = asyncio.create_task(runner._open_client(runtime, time.monotonic_ns()))
    await created.wait()
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation

    assert runtime.initiated == 1
    assert runtime.authenticated == 0
    assert runtime.terminal_failures == 0
    assert runtime.cancelled == 1
    assert runtime.clients[0].closed.done()


def _retry_runtime(**overrides: object) -> SimpleNamespace:
    diagnostics: list[dict[str, object]] = []
    runtime = SimpleNamespace(
        first_launch_ns=None,
        initiated=0,
        authenticated=0,
        cancelled=0,
        terminal_failures=0,
        retries=0,
        target_clients=2_500,
        lane_id="lakebase",
        database={"host": "example.test", "port": 5432},
        connect_host="127.0.0.1",
        application_name="test",
        ssl_context=object(),
        unexpected_disconnect=lambda *unused: None,
        key_cache=object(),
        clients=[],
        auth_methods=set(),
        connect_latencies_ms=[],
        target_elapsed_ns=None,
        failure_codes={},
        diagnostics=diagnostics,
        record_connection_diagnostic=lambda **item: diagnostics.append(item),
    )
    for name, value in overrides.items():
        setattr(runtime, name, value)
    return runtime


def _scripted_connects(
    monkeypatch: pytest.MonkeyPatch,
    outcomes: list[BaseException | None],
) -> list[object]:
    """Each connect attempt resolves its login with the next outcome (None = success)."""

    made: list[object] = []

    class Client:
        def __init__(self, **unused) -> None:
            loop = asyncio.get_running_loop()
            self.authenticated = loop.create_future()
            self.closed = loop.create_future()
            self.auth_method = "cleartext"
            self.ready = False
            made.append(self)

        def close(self) -> None:
            if not self.closed.done():
                self.closed.set_result(None)

    async def create_connection(factory, *unused_args, **unused_kwargs):
        client = factory()
        outcome = outcomes.pop(0)
        if outcome is None:
            client.ready = True
            client.authenticated.set_result(None)
        else:
            client.authenticated.set_exception(outcome)
        return object(), client

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(runner, "PostgresClient", Client)
    monkeypatch.setattr(loop, "create_connection", create_connection)
    monkeypatch.setattr(runner, "CONNECT_RETRY_BACKOFF_SECONDS", (0, 0, 0))
    return made


async def test_a_pooler_refusal_is_retried_into_the_same_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """2026-09-26: one 08P01 from the Lakebase pooler at its 10,000-client ceiling failed a
    whole bout. The slot now retries; the lane still counts one client for it."""

    made = _scripted_connects(
        monkeypatch,
        [runner.FanInProtocolError("postgres_error_08p01"), None],
    )
    runtime = _retry_runtime()

    await runner._open_client(runtime, time.monotonic_ns())

    assert (runtime.initiated, runtime.authenticated, runtime.terminal_failures) == (1, 1, 0)
    assert runtime.retries == 1
    assert runtime.failure_codes == {}
    assert runtime.clients == [made[1]]
    assert made[0].closed.done() and not made[1].closed.done()
    assert runtime.diagnostics == [
        {"ordinal": 0, "stage": "connect_retry", "code": "postgres_error_08p01"}
    ]


@pytest.mark.parametrize(
    "failure",
    [
        runner.FanInProtocolError("scram_server_refused"),
        runner.FanInProtocolError("postgres_error_28p01"),
        runner.FanInProtocolError("tls_verify_full_not_established"),
        TimeoutError(),
    ],
)
async def test_credentials_tls_and_timeouts_stay_terminal_on_the_first_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    _scripted_connects(monkeypatch, [failure, None])
    runtime = _retry_runtime()

    await runner._open_client(runtime, time.monotonic_ns())

    assert (runtime.authenticated, runtime.terminal_failures, runtime.retries) == (0, 1, 0)
    assert sum(runtime.failure_codes.values()) == 1


async def test_a_slot_stops_retrying_after_its_own_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refusal = runner.FanInProtocolError("postgres_error_08p01")
    _scripted_connects(monkeypatch, [refusal] * (runner.MAX_RETRIES_PER_CLIENT + 1))
    runtime = _retry_runtime()

    await runner._open_client(runtime, time.monotonic_ns())

    assert runtime.retries == runner.MAX_RETRIES_PER_CLIENT
    assert runtime.terminal_failures == 1
    assert runtime.failure_codes == {"postgres_error_08p01": 1}


async def test_an_exhausted_shard_budget_makes_the_next_refusal_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _scripted_connects(monkeypatch, [runner.FanInProtocolError("postgres_error_08p01"), None])
    runtime = _retry_runtime(retries=runner.PARTITION_RETRY_BUDGET)

    await runner._open_client(runtime, time.monotonic_ns())

    assert runtime.retries == runner.PARTITION_RETRY_BUDGET
    assert (runtime.authenticated, runtime.terminal_failures) == (0, 1)


async def test_a_towel_during_the_retry_pause_is_counted_as_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _scripted_connects(monkeypatch, [runner.FanInProtocolError("postgres_error_08p01"), None])
    monkeypatch.setattr(runner, "CONNECT_RETRY_BACKOFF_SECONDS", (30, 30, 30))
    retrying = asyncio.Event()
    runtime = _retry_runtime(record_connection_diagnostic=lambda **unused: retrying.set())

    operation = asyncio.create_task(runner._open_client(runtime, time.monotonic_ns()))
    await asyncio.wait_for(retrying.wait(), timeout=1)
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation

    accounted = runtime.authenticated + runtime.terminal_failures + runtime.cancelled
    assert runtime.initiated == accounted
    assert runtime.cancelled == 1


async def test_equal_wave_scheduler_bounds_each_mirrored_microbatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    in_flight = {"lakebase": 0, "competitor": 0}
    peak = {"lakebase": 0, "competitor": 0}
    release = asyncio.Event()

    async def opened(runtime, _t0_ns):
        in_flight[runtime.lane_id] += 1
        peak[runtime.lane_id] = max(
            peak[runtime.lane_id],
            in_flight[runtime.lane_id],
        )
        runtime.initiated += 1
        if sum(in_flight.values()) == 2 * runner.MICRO_BATCH_SIZE:
            release.set()
        await release.wait()
        in_flight[runtime.lane_id] -= 1

    monkeypatch.setattr(runner, "_open_client", opened)
    lanes = [
        SimpleNamespace(lane_id="lakebase", initiated=0, target_clients=12),
        SimpleNamespace(lane_id="competitor", initiated=0, target_clients=12),
    ]
    await runner._open_equal_wave(
        lanes,
        12,
        123,
        runner.AdmissionController(lane_count=len(lanes)),
    )
    assert peak == {
        "lakebase": runner.MICRO_BATCH_SIZE,
        "competitor": runner.MICRO_BATCH_SIZE,
    }
    assert [lane.initiated for lane in lanes] == [12, 12]


def test_worker_aggregation_uses_one_shared_start_and_exact_partitions() -> None:
    workers = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    workers[-1]["telemetry"]["telemetry_peak_cpu_capacity_fraction"] = 0.99
    aggregated = runner.aggregate_worker_results(workers)

    assert aggregated["release_ns"] == 123
    assert aggregated["hold_ns"] == 50_000_000_123
    assert aggregated["worker_count"] == 4
    assert aggregated["telemetry"]["telemetry_peak_cpu_capacity_fraction"] == 0.99
    for lane in aggregated["lanes"]:
        assert lane["initiated_clients"] == 10_000
        assert lane["authenticated_clients"] == 10_000
        assert lane["held_clients_at_gate"] == 10_000
        assert lane["time_to_target_ns"] == 40_003_000_000
        assert lane["time_to_target_ms"] == 40_003.0
        assert lane["sampled_queries_attempted"] == 64
        assert lane["sampled_queries_succeeded"] == 64
        assert lane["distinct_socket_fds"] == 10_000
        assert lane["distinct_local_endpoints"] == 10_000
        assert lane["identity_verified"] is True
        assert lane["launch_skew_ms"] == pytest.approx(0.0003)
        assert lane["telemetry_peak_cpu_capacity_fraction"] == pytest.approx(
            0.99
        )


def test_worker_aggregation_keeps_launch_skew_advisory() -> None:
    workers = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    for lane in workers[-1]["lanes"]:
        lane["first_launch_ns"] = 10_530_123

    aggregated = runner.aggregate_worker_results(workers)

    for raw in aggregated["lanes"]:
        assert raw["launch_skew_ms"] == pytest.approx(10.53)
        assert raw["fairness_verified"] is True
        assert raw["identity_verified"] is True


def test_partition_sampling_indices_cover_all_groups_without_global_indexing() -> None:
    runtime = SimpleNamespace(
        target_clients=runner.PARTITION_CLIENTS_PER_LANE,
        clients=[object()] * runner.PARTITION_CLIENTS_PER_LANE,
    )
    sampled = [
        index
        for group in range(runner.SAMPLE_GROUPS)
        for index in runner._sample_group_indices(runtime, group)
    ]

    assert len(sampled) == runner.SAMPLED_QUERIES_PER_LANE
    assert len(set(sampled)) == runner.SAMPLED_QUERIES_PER_LANE
    assert min(sampled) == 0
    assert max(sampled) < runner.PARTITION_CLIENTS_PER_LANE
    assert "TARGET_CLIENTS_PER_LANE" not in inspect.getsource(
        runner._sample_group_indices
    )


@pytest.mark.parametrize("aggregate_count", [8_044, 8_045, 8_046, 10_000])
def test_aggregation_is_order_independent_at_boundary_counts(
    aggregate_count: int,
) -> None:
    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    quotient, remainder = divmod(aggregate_count, runner.WORKER_COUNT)
    for index, result in enumerate(results):
        count = quotient + (index < remainder)
        for lane in result["lanes"]:
            lane["initiated_clients"] = count
            lane["authenticated_clients"] = count
            lane["held_clients_at_gate"] = count
    forward = runner.aggregate_worker_results(results)
    reverse = runner.aggregate_worker_results(list(reversed(results)))

    for aggregate in (forward, reverse):
        assert {
            int(lane["authenticated_clients"]) for lane in aggregate["lanes"]
        } == {aggregate_count}
        assert {
            int(lane["held_clients_at_gate"]) for lane in aggregate["lanes"]
        } == {aggregate_count}
        assert all(
            bool(lane["identity_verified"]) is (aggregate_count == 10_000)
            for lane in aggregate["lanes"]
        )


@pytest.mark.parametrize("partition_count", [2_011, 2_012])
def test_sampling_rejects_incomplete_partition_before_indexing(
    partition_count: int,
) -> None:
    runtime = SimpleNamespace(
        target_clients=runner.PARTITION_CLIENTS_PER_LANE,
        clients=[object()] * partition_count,
    )
    with pytest.raises(runner.FanInProtocolError, match="sample_partition_incomplete"):
        runner._sample_group_indices(runtime, 2)


async def test_disconnect_during_sampling_cannot_invalidate_selected_indices() -> None:
    class Client:
        def __init__(self, ordinal: int) -> None:
            self.ordinal = ordinal
            self.ready = True
            self.closed = asyncio.get_running_loop().create_future()

        async def sample_select_one(self) -> None:
            await asyncio.sleep(0)
            if self.closed.done():
                raise ConnectionError("closed")

    runtime = SimpleNamespace(
        lane_id="lakebase",
        target_clients=runner.PARTITION_CLIENTS_PER_LANE,
        clients=[Client(ordinal) for ordinal in range(runner.PARTITION_CLIENTS_PER_LANE)],
        sample_attempted=0,
        sample_succeeded=0,
        sample_failed=0,
        record_connection_diagnostic=lambda **_kwargs: None,
    )
    indices = runner._sample_group_indices(runtime, 2)
    sample = asyncio.create_task(runner._sample_group(runtime, 2))
    await asyncio.sleep(0)
    for index in indices:
        runtime.clients[index].closed.set_result(None)
    await sample

    assert runtime.sample_attempted == runner.CLIENTS_PER_SAMPLE_GROUP
    assert runtime.sample_succeeded + runtime.sample_failed == runtime.sample_attempted


async def test_final_observer_disconnect_is_a_failed_check_not_worker_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DisconnectedObserver:
        async def sample(self) -> None:
            raise ConnectionError("observer disconnected")

    monkeypatch.setattr(
        runner,
        "_telemetry",
        lambda *unused: {
            "physical_memory_bytes": 16 * 1024**3,
            "available_memory_bytes": 12 * 1024**3,
            "rss_bytes": 1024**3,
            "fd_soft_limit": 65_535,
            "open_fds": 100,
            "ephemeral_port_count": 28_232,
            "ephemeral_ports_in_use": 10_000,
            "ephemeral_ports_remaining": 18_232,
            "event_loop_p99_ms": 0.01,
            "cpu_capacity_fraction": 0.1,
        },
    )
    summary = runner.TelemetrySummary()
    _, observer_ok = await runner._hold_and_sample(
        [],
        [DisconnectedObserver()],
        t0_ns=time.monotonic_ns(),
        start_cpu=time.process_time(),
        network_start=(0, 0),
        telemetry_summary=summary,
        hold_started_ns=time.monotonic_ns()
        - int((runner.HOLD_SECONDS + 1) * 1_000_000_000),
        sample_groups=(),
    )

    assert observer_ok is False


def test_worker_aggregation_rejects_double_count_and_shared_start_mutations() -> None:
    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    lane_zero = results[0]["lanes"][0]
    lane_one = results[1]["lanes"][0]
    lane_zero["authenticated_clients"] = 2_499
    lane_zero["held_clients_at_gate"] = 2_499
    lane_one["authenticated_clients"] = 2_501
    lane_one["held_clients_at_gate"] = 2_501
    aggregated = runner.aggregate_worker_results(results)
    lakebase = next(
        lane for lane in aggregated["lanes"] if lane["lane_id"] == "lakebase"
    )
    assert lakebase["authenticated_clients"] == 10_000
    assert lakebase["identity_verified"] is False

    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    results[-1]["release_ns"] = 124
    with pytest.raises(runner.FanInProtocolError, match="release"):
        runner.aggregate_worker_results(results)

    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    results[-1]["worker_cpu"] = results[0]["worker_cpu"]
    with pytest.raises(runner.FanInProtocolError, match="affinity"):
        runner.aggregate_worker_results(results)

    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    results[-1]["lanes"][0]["time_to_target_ns"] = 60_000_000_000
    with pytest.raises(runner.FanInProtocolError, match="target_timestamp"):
        runner.aggregate_worker_results(results)

    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    results[-1]["lanes"][0]["achieved_elapsed_ms"] = 79_999.0
    with pytest.raises(runner.FanInProtocolError, match="completion_chronology"):
        runner.aggregate_worker_results(results)


@pytest.mark.parametrize(
    "field,value,error",
    (
        ("telemetry_samples", True, "telemetry_malformed"),
        ("telemetry_peak_cpu_capacity_fraction", float("nan"), "telemetry_malformed"),
        ("telemetry_peak_open_fds", -1, "telemetry_malformed"),
        ("port_accounting_verified", False, "telemetry_malformed"),
    ),
)
def test_worker_aggregation_rejects_malformed_or_synthesized_telemetry(
    field: str,
    value: object,
    error: str,
) -> None:
    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    results[0]["telemetry"][field] = value
    with pytest.raises(runner.FanInProtocolError, match=error):
        runner.aggregate_worker_results(results)

    missing = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    missing[0]["telemetry"].pop(field)
    with pytest.raises(runner.FanInProtocolError, match="telemetry_missing"):
        runner.aggregate_worker_results(missing)


def test_global_preexisting_baseline_is_published_once_not_summed() -> None:
    results = [worker_result(index) for index in range(runner.WORKER_COUNT)]
    for result in results:
        result["lanes"][0]["preexisting_client_role_sessions"] = 20
    aggregated = runner.aggregate_worker_results(results)
    assert aggregated["lanes"][0]["preexisting_client_role_sessions"] == 20

    results[-1]["lanes"][0]["preexisting_client_role_sessions"] = 19
    with pytest.raises(
        runner.FanInProtocolError,
        match="preexisting_session_observation_mismatch",
    ):
        runner.aggregate_worker_results(results)


async def test_guarded_wave_cancels_both_lanes_on_runtime_pressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancelled = asyncio.Event()

    async def blocked(*unused):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(runner, "_open_equal_wave", blocked)
    monkeypatch.setattr(
        runner,
        "_telemetry",
        lambda *unused: {
            "physical_memory_bytes": 15 * 1024**3,
            "available_memory_bytes": 767 * 1024**2,
            "rss_bytes": 7 * 1024**3,
            "fd_soft_limit": 65_535,
            "open_fds": 20_264,
            "ephemeral_port_count": 28_232,
            "ephemeral_ports_in_use": 10_000,
            "ephemeral_ports_remaining": 18_232,
            "event_loop_p99_ms": 0.1,
            "cpu_capacity_fraction": 0.31,
        },
    )
    summary = runner.TelemetrySummary()
    await runner._open_equal_wave_guarded(
        [],
        250,
        123,
        start_cpu=0.0,
        network_start=(0, 0),
        telemetry_summary=summary,
        admission_controller=runner.AdmissionController(lane_count=1),
    )

    assert cancelled.is_set()
    assert summary.hard_failures == {"available_memory_reserve_exhausted"}


async def test_advisory_pressure_still_completes_full_hold_and_all_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lane = SimpleNamespace(
        lane_id="lakebase",
        initiated=runner.PARTITION_CLIENTS_PER_LANE,
        authenticated=runner.PARTITION_CLIENTS_PER_LANE,
        target_elapsed_ns=1,
        sample_attempted=0,
        sample_succeeded=0,
        sample_failed=0,
        clients=[],
    )

    async def sample_group(runtime, unused_group):
        runtime.sample_attempted += runner.CLIENTS_PER_SAMPLE_GROUP
        runtime.sample_succeeded += runner.CLIENTS_PER_SAMPLE_GROUP

    async def calibration():
        return 60.499

    async def advisory_telemetry(*unused_args, **unused_kwargs):
        return {
            "physical_memory_bytes": 15 * 1024**3,
            "available_memory_bytes": 7 * 1024**3,
            "rss_bytes": 100 * 1024**2,
            "fd_soft_limit": 65_535,
            "open_fds": 7,
            "ephemeral_port_count": 28_232,
            "ephemeral_ports_in_use": 10_000,
            "ephemeral_ports_remaining": 18_232,
            "event_loop_p99_ms": 60.499,
            "cpu_capacity_fraction": 0.99,
        }

    class Observer:
        async def sample(self):
            return True

    monkeypatch.setattr(runner, "_sample_group", sample_group)
    monkeypatch.setattr(runner, "_event_loop_calibration", calibration)
    monkeypatch.setattr(runner, "_telemetry_off_loop", advisory_telemetry)
    monkeypatch.setattr(runner, "_progress", lambda unused: None)
    summary = runner.TelemetrySummary()
    hold_started_ns = (
        time.monotonic_ns() - runner.HOLD_SECONDS * 1_000_000_000
    )

    hold_elapsed_ms, observer_ok = await runner._hold_and_sample(
        [lane],
        [Observer()],
        t0_ns=hold_started_ns,
        start_cpu=0.0,
        network_start=(0, 0),
        telemetry_summary=summary,
        hold_started_ns=hold_started_ns,
    )

    assert hold_elapsed_ms >= runner.HOLD_SECONDS * 1_000
    assert lane.sample_attempted == runner.SAMPLED_QUERIES_PER_LANE
    assert lane.sample_succeeded == runner.SAMPLED_QUERIES_PER_LANE
    assert observer_ok
    assert summary.hard_failures == set()
    assert "cpu_pressure" in summary.advisories


async def test_one_owned_loop_stall_is_diagnostic_not_a_fake_p99_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def stalled_wave(*unused):
        await asyncio.sleep(0)
        cpu_deadline = time.thread_time() + 0.06
        while time.thread_time() < cpu_deadline:
            pass
        await asyncio.sleep(0.02)

    monkeypatch.setattr(runner, "_open_equal_wave", stalled_wave)
    monkeypatch.setattr(runner, "LOOP_MONITOR_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(runner, "RESOURCE_TELEMETRY_INTERVAL_SECONDS", 1.0)
    monkeypatch.setattr(
        runner,
        "_telemetry",
        lambda *unused: {
            "physical_memory_bytes": 15 * 1024**3,
            "available_memory_bytes": 7 * 1024**3,
            "rss_bytes": 100 * 1024**2,
            "fd_soft_limit": 65_535,
            "open_fds": 7,
            "ephemeral_port_count": 28_232,
            "ephemeral_ports_in_use": 100,
            "ephemeral_ports_remaining": 28_132,
            "event_loop_p99_ms": 0.0,
            "cpu_capacity_fraction": 0.01,
        },
    )
    diagnostics = runner.RuntimeDiagnostics()
    monkeypatch.setattr(runner, "_runtime_diagnostics", diagnostics)
    summary = runner.TelemetrySummary()

    peak = await runner._open_equal_wave_guarded(
        [],
        10,
        time.monotonic_ns(),
        start_cpu=time.process_time(),
        network_start=(0, 0),
        telemetry_summary=summary,
        admission_controller=runner.AdmissionController(lane_count=1),
    )

    assert peak > 50.0
    assert summary.hard_failures == set()
    assert summary.peak_event_loop_p99_ms == 0.0
    assert diagnostics.loop_samples > 0
    assert diagnostics.peak_loop_lag_ms == peak


@pytest.mark.parametrize(
    ("process_cpu_ms", "thread_cpu_ms", "ready_batch", "phase", "gc_pause_ms"),
    (
        # CPU that accounts for the delay: ours.
        (60.0, 60.0, 1, "event_loop_wait", 0.0),
        # A garbage-collection pause past the ceiling: ours.
        (1.0, 1.0, 1, "gc_generation_2", 60.0),
    ),
)
def test_generator_owned_stalls_keep_the_50ms_gate(
    process_cpu_ms: float,
    thread_cpu_ms: float,
    ready_batch: int,
    phase: str,
    gc_pause_ms: float,
) -> None:
    assert runner.classify_generator_owned_stall(
        wall_lag_ms=60.0,
        process_cpu_ms=process_cpu_ms,
        thread_cpu_ms=thread_cpu_ms,
        ready_batch_size=ready_batch,
        selector_batch_size=1,
        phase=phase,
        gc_pause_ms=gc_pause_ms,
    )


def test_cpu_that_does_not_account_for_the_delay_is_not_ours() -> None:
    """The case a live ramp hit, and the reason it stopped short of 10,000.

    Sixty milliseconds of wall lag with twenty of CPU, an ordinary batch and no GC pause: forty
    of those milliseconds are the loop waiting on something else. An absolute five-millisecond
    CPU floor called all sixty ours, so the ramp gated itself on pressure it had not caused and
    stopped with zero connection failures. Corroboration has to be proportional to the delay it
    is corroborating, which is what this function's own docstring always claimed.
    """

    assert not runner.classify_generator_owned_stall(
        wall_lag_ms=60.0,
        process_cpu_ms=20.0,
        thread_cpu_ms=20.0,
        ready_batch_size=OWNED_STALL_READY_BATCH - 1,
        selector_batch_size=1,
        phase="ramp",
        gc_pause_ms=0.0,
    )


def test_normal_v3_connect_batch_cannot_stop_the_ramp_at_60ms() -> None:
    """The 7,134-client incident: one ordinary 15-connect batch is not amplification."""

    assert runner.RUNNER_LANE_COUNT == 1
    assert runner.OWNED_STALL_READY_BATCH > runner.LANE_CONNECT_CONCURRENCY
    assert not runner.classify_generator_owned_stall(
        wall_lag_ms=60.499,
        process_cpu_ms=20.0,
        thread_cpu_ms=20.0,
        ready_batch_size=runner.LANE_CONNECT_CONCURRENCY,
        selector_batch_size=runner.LANE_CONNECT_CONCURRENCY,
        phase="authentication_processing",
        gc_pause_ms=0.0,
    )


def test_even_a_large_ready_batch_needs_cpu_or_gc_corroboration() -> None:
    """A kernel wakeup is context, not proof that the generator caused the delay."""

    assert not runner.classify_generator_owned_stall(
        wall_lag_ms=60.0,
        process_cpu_ms=1.0,
        thread_cpu_ms=1.0,
        ready_batch_size=OWNED_STALL_READY_BATCH * 4,
        selector_batch_size=OWNED_STALL_READY_BATCH * 4,
        phase="authentication_processing",
        gc_pause_ms=0.0,
    )


def test_owned_event_loop_gate_uses_p99_not_one_peak_tick() -> None:
    summary = runner.TelemetrySummary()
    summary.observe_event_loop(60.0, generator_owned_lag_ms=60.0)
    for _ in range(runner.EVENT_LOOP_P99_MIN_SAMPLES - 1):
        summary.observe_event_loop(0.1, generator_owned_lag_ms=0.0)

    assert summary.peak_event_loop_p99_ms == 0.0
    assert "event_loop_pressure" not in summary.advisories

    sustained = runner.TelemetrySummary()
    for _ in range(runner.EVENT_LOOP_P99_MIN_SAMPLES):
        sustained.observe_event_loop(60.0, generator_owned_lag_ms=60.0)
    sustained.observe(
        {
            "physical_memory_bytes": 15 * 1024**3,
            "available_memory_bytes": 7 * 1024**3,
            "rss_bytes": 100 * 1024**2,
            "fd_soft_limit": 65_535,
            "open_fds": 7,
            "ephemeral_port_count": 28_232,
            "ephemeral_ports_in_use": 100,
            "ephemeral_ports_remaining": 28_132,
            "event_loop_p99_ms": 0.0,
            "cpu_capacity_fraction": 0.01,
        }
    )

    assert sustained.peak_event_loop_p99_ms == 60.0
    assert sustained.advisories == {"event_loop_pressure"}
    assert sustained.hard_failures == set()
    assert sustained.verified


def test_external_descheduling_is_raw_lag_not_generator_pressure() -> None:
    owned = runner.classify_generator_owned_stall(
        wall_lag_ms=72.929,
        process_cpu_ms=0.5,
        thread_cpu_ms=0.2,
        ready_batch_size=2,
        selector_batch_size=3,
        phase="ssl_handshake_wall",
        gc_pause_ms=0.0,
    )
    summary = runner.TelemetrySummary()
    summary.observe_event_loop(
        72.929,
        generator_owned_lag_ms=72.929 if owned else 0.0,
    )

    assert not owned
    assert summary.peak_raw_event_loop_p99_ms == 72.929
    assert summary.peak_event_loop_p99_ms == 0.0
    assert summary.raw_event_loop_warning_count == 1
    assert summary.hard_failures == set()


def test_repeated_extreme_external_lag_is_advisory_host_instability() -> None:
    summary = runner.TelemetrySummary()
    for _ in range(runner.RAW_WALL_LAG_MAX_BREACHES):
        summary.observe_event_loop(
            runner.RAW_WALL_LAG_CEILING_MS + 0.001,
            generator_owned_lag_ms=0.0,
        )

    assert summary.peak_event_loop_p99_ms == 0.0
    assert summary.raw_event_loop_ceiling_breaches == 3
    assert summary.hard_failures == set()
    assert summary.advisories == {"host_scheduling_instability"}


def test_runtime_diagnostics_records_gc_and_restores_selector_probe() -> None:
    diagnostics = runner.RuntimeDiagnostics()
    loop = asyncio.new_event_loop()
    selector = loop._selector
    original_select = selector.select
    diagnostics.install_selector_probe(loop)
    try:
        assert selector.select != original_select
        gc.callbacks.append(diagnostics.gc_callback)
        gc.collect(0)
        assert diagnostics.gc_collections[0] >= 1
        assert diagnostics.gc_max_pause_ms[0] >= 0.0
    finally:
        gc.callbacks.remove(diagnostics.gc_callback)
        diagnostics.restore_selector_probe()
        loop.close()
    assert selector.select == original_select


async def test_ready_queue_reports_true_length_and_drains_in_order() -> None:
    loop = asyncio.get_running_loop()
    diagnostics = runner.RuntimeDiagnostics()
    original_ready = loop._ready
    diagnostics.install_ready_batch_limit(loop)
    seen: list[int] = []
    drained = asyncio.Event()

    def record(value: int) -> None:
        seen.append(value)
        if value == 999:
            drained.set()

    try:
        for value in range(1_000):
            loop.call_soon(record, value)
        # The loop must see the real backlog. A capped __len__ leaves the un-run
        # descriptors readable and level-triggered epoll re-reports them, so the
        # cap buys re-delivery rather than saving work.
        assert runner.READY_CALLBACK_BATCH_LIMIT == 0
        assert runner._ready_backlog_size(loop) >= 1_000
        assert len(loop._ready) == runner._ready_backlog_size(loop)
        await drained.wait()
    finally:
        diagnostics.restore_ready_batch_limit()

    assert loop._ready is original_ready
    assert seen == list(range(1_000))


def test_ready_queue_still_caps_when_explicitly_asked() -> None:
    """Regression coverage for the historical capped behaviour only."""

    ready = runner.BoundedReadyDeque(batch_limit=64)
    for _ in range(1_000):
        ready.append(object())
    assert len(ready) == 64
    assert ready.actual_length() == 1_000


def test_ready_queue_age_survives_duplicate_handle_appends() -> None:
    """A level-triggered re-poll re-appends the same persistent reader handle.

    Keying enqueue times by id() collapsed those duplicates and removed the
    entry on the first dequeue, so oldest_fifo_ready_age_ms read 0.0 exactly
    when the backlog was deepest -- the one moment it had to be right.
    """

    handle = object()
    ready = runner.BoundedReadyDeque(batch_limit=0)
    ready.append(handle)
    time.sleep(0.005)
    ready.append(handle)

    assert ready.actual_length() == 2
    assert ready.popleft() is handle
    assert ready.last_oldest_age_ms >= 5.0
    assert ready.popleft() is handle
    assert ready.actual_length() == 0


async def test_mirrored_wave_pipelines_within_the_in_flight_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lanes stay equal and interleaved while connects overlap.

    Awaiting each micro-batch pinned in-flight at MICRO_BATCH_SIZE * lanes (4),
    which made the ramp latency-bound at five to six round trips per client.
    """

    class Lane:
        def __init__(self, lane_id: str) -> None:
            self.lane_id = lane_id
            self.initiated = 0
            self.target_clients = 200

    lanes = [Lane("lakebase"), Lane("competitor")]
    in_flight = 0
    peak_in_flight = 0
    order: list[str] = []

    async def fake_open_client(runtime: object, t0_ns: int) -> None:
        nonlocal in_flight, peak_in_flight
        del t0_ns
        runtime.initiated += 1  # type: ignore[attr-defined]
        order.append(runtime.lane_id)  # type: ignore[attr-defined]
        in_flight += 1
        peak_in_flight = max(peak_in_flight, in_flight)
        try:
            await asyncio.sleep(0.002)  # stands in for the connect round trips
        finally:
            in_flight -= 1

    monkeypatch.setattr(runner, "_open_client", fake_open_client)
    await runner._open_equal_wave(
        lanes,
        200,
        0,
        runner.AdmissionController(lane_count=len(lanes)),
    )

    bound = runner.LANE_CONNECT_CONCURRENCY * len(lanes)
    assert [lane.initiated for lane in lanes] == [200, 200]
    assert peak_in_flight <= bound
    assert peak_in_flight > runner.MICRO_BATCH_SIZE * len(lanes)
    assert in_flight == 0
    assert order[:4] == ["lakebase", "competitor", "lakebase", "competitor"]
    assert order.count("lakebase") == order.count("competitor") == 200


def test_ready_heartbeat_cannot_jump_fifo_backlog() -> None:
    class Handle:
        def __init__(self, name: str, *, priority: bool = False) -> None:
            self.name = name

            def callback() -> None:
                pass

            callback._round5_monitor_priority = priority  # type: ignore[attr-defined]
            self._callback = callback

    ready = runner.BoundedReadyDeque(batch_limit=64)
    ready.append(Handle("oldest"))
    ready.append(Handle("heartbeat", priority=True))
    time.sleep(0.001)

    assert ready.popleft().name == "oldest"
    assert ready.last_oldest_age_ms >= 1.0
    assert ready.popleft().name == "heartbeat"


async def test_selector_admission_cap_amplifies_wakeups_quadratically() -> None:
    """The capped selector reads better per turn and is worse end to end.

    Servicing N ready descriptors K at a time costs about N**2 / 2K wakeups
    because level-triggered readiness is re-reported, and selectors.py has
    already paid a kernel copy-out plus a key lookup and tuple for each one.
    """

    unbounded_ms, unbounded_deferred, unbounded_wakeups, events = (
        await runner._event_loop_selector_fanout_benchmark()
    )
    capped_ms, capped_deferred, capped_wakeups, capped_events = (
        await runner._event_loop_selector_fanout_benchmark(event_batch_limit=16)
    )

    assert events == capped_events == runner.SELECTOR_FANOUT_PROBE_SOCKETS

    # Production configuration delivers each readiness event about once.
    assert unbounded_deferred == 0
    assert unbounded_wakeups == events
    assert unbounded_wakeups / events <= runner.MAX_SELECTOR_WAKEUP_AMPLIFICATION

    # The historical cap re-reports what it dropped, and now says so. The floor
    # is a deliberately loose form of N**2 / 2K.
    assert capped_deferred > 0
    assert capped_wakeups > events * 2
    assert capped_wakeups / events > runner.MAX_SELECTOR_WAKEUP_AMPLIFICATION

    # The per-turn latency gate prefers the slower configuration, which is
    # exactly why it must never be the only signal.
    assert capped_ms < unbounded_ms


def test_owned_stall_envelope_captures_worker_wave_counts() -> None:
    diagnostics = runner.RuntimeDiagnostics()
    runner._worker_execution_context = {
        "worker_id": 1,
        "worker_cpu": 1,
        "phase": "ramp",
        "operation": "wave_27",
        "wave": 27,
        "partition_start": 2_500,
        "partition_end_exclusive": 5_000,
        "lane_counts": {
            "lakebase": {"initiated": 2_077, "authenticated": 2_076, "held": 2_076},
            "competitor": {
                "initiated": 2_077,
                "authenticated": 2_077,
                "held": 2_077,
            },
        },
    }
    try:
        diagnostics.observe_loop(
            runner.LoopDelaySample(
                wall_lag_ms=109.544,
                process_cpu_ms=119.231,
                thread_cpu_ms=78.310,
                scheduler_wait_ms=36.347,
                voluntary_context_switches=5,
                involuntary_context_switches=23,
                active_handshakes=1,
                selector_batch_size=0,
                ready_batch_size=618,
                oldest_fifo_ready_age_ms=109.544,
                callback_interval={
                    "calls": 618,
                    "cpu_total_ms": 78.31,
                    "profiles": {},
                },
                phase="telemetry_sampling_wall",
                gc_pause_ms=0.0,
                generator_owned=True,
            )
        )
    finally:
        runner._worker_execution_context = None

    envelope = diagnostics.public_dict()["significant_stall_envelopes"][0]
    assert envelope["worker_id"] == 1
    assert envelope["worker_cpu"] == 1
    assert envelope["wave"] == 27
    assert envelope["lanes"]["lakebase"]["authenticated"] == 2_076
    assert envelope["ready_batch_size"] == 618


def test_selector_probe_delivers_every_event_and_counts_a_cap_it_is_given() -> None:
    class Selector:
        def __init__(self, polls: list[list[tuple[str, int]]]) -> None:
            self.polls = polls

        def select(self, timeout=None):
            del timeout
            return self.polls.pop(0) if self.polls else []

    class Loop:
        def __init__(self, selector: Selector) -> None:
            self._selector = selector

    def fresh_polls() -> list[list[tuple[str, int]]]:
        return [
            [("old-registration", value) for value in range(610)],
            [("new-registration", 7)],
        ]

    # Production configuration drops nothing, so nothing is ever re-delivered
    # and no SelectorKey is retained across iterations to go stale.
    selector = Selector(fresh_polls())
    diagnostics = runner.RuntimeDiagnostics()
    original_select = selector.select
    diagnostics.install_selector_probe(Loop(selector))
    try:
        first = selector.select(None)
        second = selector.select(None)
    finally:
        diagnostics.restore_selector_probe()

    assert len(first) == 610
    assert second == [("new-registration", 7)]
    assert diagnostics.peak_deferred_selector_events == 0
    assert diagnostics.deferred_selector_events == 0
    assert diagnostics.peak_selector_wakeups == 610
    assert diagnostics.peak_selector_batch == 610
    assert selector.select == original_select

    # Given the historical cap, the discarded events are now counted instead of
    # silently reported as zero deferred.
    capped_selector = Selector(fresh_polls())
    capped = runner.RuntimeDiagnostics()
    capped.selector_event_batch_limit = 16
    capped.install_selector_probe(Loop(capped_selector))
    try:
        capped_first = capped_selector.select(None)
    finally:
        capped.restore_selector_probe()

    assert len(capped_first) == 16
    assert capped.deferred_selector_events == 610 - 16
    assert capped.peak_deferred_selector_events == 610 - 16


def test_socket_state_telemetry_is_single_worker_and_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def tcp_states() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"established": 16_345}

    monkeypatch.setattr(runner, "_tcp_states", tcp_states)
    monkeypatch.setattr(runner, "_network_bytes", lambda: (0, 0))
    monkeypatch.setattr(runner, "_memory", lambda: (16 * 1024**3, 10 * 1024**3))
    monkeypatch.setattr(runner, "_rss_bytes", lambda: 1024)
    monkeypatch.setattr(runner, "_open_fds", lambda: 10)
    monkeypatch.setattr(
        runner,
        "_ephemeral_port_usage",
        lambda: (28_232, 10_000, 18_232),
    )
    monkeypatch.setattr(runner.resource, "getrlimit", lambda unused: (65_535, 65_535))
    monkeypatch.setattr(
        runner.os,
        "sched_getaffinity",
        lambda unused: {0},
        raising=False,
    )
    runner._socket_states_cache.clear()
    runner._socket_states_cache_ns = 0

    non_owner = runner._telemetry(0.0, time.monotonic_ns(), (0, 0), 0.0, False)
    first = runner._telemetry(0.0, time.monotonic_ns(), (0, 0), 0.0, True)
    second = runner._telemetry(0.0, time.monotonic_ns(), (0, 0), 0.0, True)

    assert non_owner["socket_states"] == {}
    assert first["socket_states"] == {"established": 16_345}
    assert second["socket_states"] == first["socket_states"]
    assert calls == 1


def test_controlled_gc_always_restores_prior_state_and_collects() -> None:
    diagnostics = runner.RuntimeDiagnostics()
    control = runner.ControlledGC(diagnostics)
    gc.enable()
    control.start()
    assert not gc.isenabled()
    assert control.active
    control.stop()
    assert gc.isenabled()
    assert not control.active
    assert diagnostics.controlled_gc
    assert diagnostics.gc_initial_collect_ms >= 0.0
    assert diagnostics.gc_final_collect_ms >= 0.0

    gc.disable()
    try:
        control = runner.ControlledGC(runner.RuntimeDiagnostics())
        control.start()
        control.stop()
        assert not gc.isenabled()
    finally:
        gc.enable()


async def test_protocol_parser_completes_tls_scram_auth_and_sparse_select(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Socket:
        def fileno(self):
            return 42

    class SslObject:
        def version(self):
            return "TLSv1.3"

        def cipher(self):
            return ("TLS_AES_256_GCM_SHA384", "TLSv1.3", 256)

    class Transport:
        def __init__(self):
            self.writes: list[bytes] = []
            self.paused = False
            self.protocol = None

        def write(self, value):
            self.writes.append(bytes(value))

        def pause_reading(self):
            self.paused = True

        def resume_reading(self):
            self.paused = False

        def get_extra_info(self, name):
            return {
                "ssl_object": SslObject(),
                "socket": Socket(),
                "sockname": ("10.0.0.10", 40_042),
            }.get(name)

        def abort(self):
            if self.protocol is not None:
                protocol, self.protocol = self.protocol, None
                protocol.connection_lost(None)

    loop = asyncio.get_running_loop()
    transport = Transport()

    async def start_tls(original, protocol, context, **kwargs):
        assert original is transport
        assert isinstance(context, ssl.SSLContext)
        assert kwargs["server_hostname"] == "pool.example.test"
        transport.protocol = protocol
        return transport

    monkeypatch.setattr(loop, "start_tls", start_tls)
    disconnects: list[str] = []
    client = runner.PostgresClient(
        lane_id="lakebase",
        ordinal=7,
        database={
            "host": "pool.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": "anti_demo_burst",
            "password": "correct horse battery staple",
        },
        application_name="anti-demo-r5-fanin-test",
        ssl_context=ssl.create_default_context(),
        on_unexpected_disconnect=disconnects.append,
    )
    transport.protocol = client
    client.connection_made(transport)
    assert transport.writes == [runner.SSL_REQUEST]
    client.data_received(b"S")
    for _ in range(5):
        await asyncio.sleep(0)
        if len(transport.writes) > 1:
            break
    assert b"application_name\0anti-demo-r5-fanin-test\0" in transport.writes[-1]

    client.data_received(
        runner._message(
            b"R",
            struct.pack("!I", 10) + b"SCRAM-SHA-256\0\0",
        )
    )
    assert transport.writes[-1].startswith(b"p")
    salt = b"round5-parser-salt"
    server_first = (
        f"r={client.scram.nonce}server,"
        f"s={base64.b64encode(salt).decode()},i=4096"
    )
    client.data_received(
        runner._message(
            b"R",
            struct.pack("!I", 11) + server_first.encode(),
        )
    )
    assert client.scram.server_signature is not None
    client.data_received(
        b"".join(
            (
                runner._message(
                    b"R",
                    struct.pack("!I", 12)
                    + (
                        "v="
                        + base64.b64encode(
                            client.scram.server_signature
                        ).decode()
                    ).encode(),
                ),
                runner._message(b"R", struct.pack("!I", 0)),
                runner._message(b"K", struct.pack("!II", 321, 654)),
                runner._message(b"Z", b"I"),
            )
        )
    )
    await client.authenticated
    assert client.ready
    assert client.backend_pid == 321
    assert client.fd == 42
    assert client.local_endpoint == ("10.0.0.10", 40_042)
    assert client.tls_version == "TLSv1.3"
    assert client.auth_method == "scram-sha-256"

    sample = asyncio.create_task(client.sample_select_one())
    await asyncio.sleep(0)
    assert transport.writes[-1] == runner._message(b"Q", b"SELECT 1\0")
    data_row = struct.pack("!H", 1) + struct.pack("!I", 1) + b"1"
    client.data_received(
        runner._message(b"D", data_row)
        + runner._message(b"C", b"SELECT 1\0")
        + runner._message(b"Z", b"I")
    )
    await sample
    client.close()
    await client.closed
    assert disconnects == []


async def test_cleartext_password_requires_verified_tls_and_is_never_logged(
    capsys: pytest.CaptureFixture[str],
) -> None:
    password = "never-print-this-password"

    class Transport:
        def __init__(self) -> None:
            self.writes: list[bytes] = []

        def write(self, value: bytes) -> None:
            self.writes.append(bytes(value))

    client = runner.PostgresClient(
        lane_id="lakebase",
        ordinal=1,
        database={
            "host": "pool.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": "anti_demo_burst",
            "password": password,
        },
        application_name="anti-demo-r5-auth-test",
        ssl_context=ssl.create_default_context(),
        on_unexpected_disconnect=lambda _: None,
    )
    transport = Transport()
    client.transport = transport  # type: ignore[assignment]

    with pytest.raises(runner.FanInProtocolError, match="verified_tls") as before_tls:
        client._authentication(struct.pack("!I", 3))
    assert password not in str(before_tls.value)
    assert transport.writes == []

    client.tls_verified = True
    client._authentication(struct.pack("!I", 3))
    assert client.auth_method == "tls-cleartext-password"
    assert transport.writes == [runner._message(b"p", password.encode() + b"\0")]
    assert not hasattr(client, "password_message")
    client._authentication(struct.pack("!I", 0))
    assert client.authentication_ok
    captured = capsys.readouterr()
    assert password not in captured.out
    assert password not in captured.err


@pytest.mark.parametrize("code", [2, 5, 6, 7, 8, 9])
async def test_unsupported_authentication_methods_fail_closed(code: int) -> None:
    client = runner.PostgresClient(
        lane_id="competitor",
        ordinal=1,
        database={
            "host": "proxy.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": "anti_demo_burst",
            "password": "secret",
        },
        application_name="anti-demo-r5-auth-test",
        ssl_context=ssl.create_default_context(),
        on_unexpected_disconnect=lambda _: None,
    )
    client.transport = SimpleNamespace(write=lambda _: None)
    client.tls_verified = True
    with pytest.raises(
        runner.FanInProtocolError,
        match=rf"authentication_method_{code}_unsupported",
    ):
        client._authentication(struct.pack("!I", code))


async def test_authentication_downgrade_and_missing_scram_signature_fail() -> None:
    client = runner.PostgresClient(
        lane_id="competitor",
        ordinal=1,
        database={
            "host": "proxy.example.test",
            "port": 5432,
            "dbname": "anti_demo",
            "user": "anti_demo_burst",
            "password": "secret",
        },
        application_name="anti-demo-r5-auth-test",
        ssl_context=ssl.create_default_context(),
        on_unexpected_disconnect=lambda _: None,
    )
    client.transport = SimpleNamespace(write=lambda _: None)
    client.tls_verified = True
    client._authentication(struct.pack("!I", 10) + b"SCRAM-SHA-256\0\0")
    with pytest.raises(runner.FanInProtocolError, match="signature_missing"):
        client._authentication(struct.pack("!I", 0))
    with pytest.raises(runner.FanInProtocolError, match="method_changed"):
        client._authentication(struct.pack("!I", 3))


async def test_malformed_and_unsupported_sasl_challenges_fail_closed() -> None:
    def client() -> runner.PostgresClient:
        value = runner.PostgresClient(
            lane_id="competitor",
            ordinal=1,
            database={
                "host": "proxy.example.test",
                "port": 5432,
                "dbname": "anti_demo",
                "user": "anti_demo_burst",
                "password": "secret",
            },
            application_name="anti-demo-r5-auth-test",
            ssl_context=ssl.create_default_context(),
            on_unexpected_disconnect=lambda _: None,
        )
        value.transport = SimpleNamespace(write=lambda _: None)
        value.tls_verified = True
        return value

    with pytest.raises(runner.FanInProtocolError, match="sasl_challenge_malformed"):
        client()._authentication(struct.pack("!I", 10) + b"SCRAM-SHA-256")
    with pytest.raises(runner.FanInProtocolError, match="scram_unavailable"):
        client()._authentication(struct.pack("!I", 10) + b"SCRAM-SHA-256-PLUS\0\0")


def test_provider_selected_authentication_difference_preserves_fairness() -> None:
    lakebase = finalize(raw_lane("lakebase", time_to_target_ms=12_000))
    competitor = finalize(raw_lane("competitor", time_to_target_ms=13_000))
    comparison = compare_lanes(lakebase, competitor)
    assert lakebase.auth_method == "tls-cleartext-password"
    assert competitor.auth_method == "scram-sha-256"
    assert lakebase.gates.fairness and competitor.gates.fairness
    assert comparison is not None
    assert comparison.winner_lane_id == "lakebase"


def test_progress_sequence_is_monotonic_and_reconnect_deduplicates() -> None:
    values = [
        {
            "schema_version": FANIN_SCHEMA_VERSION,
            "protocol": FANIN_PROTOCOL,
            "sequence": sequence,
            "lane_id": lane,
            "phase": "ramping",
            "initiated_clients": sequence * 1_000,
            "authenticated_clients": sequence * 1_000,
            "held_clients": sequence * 1_000,
            "peak_held_clients": sequence * 1_000,
            "elapsed_ms": sequence * 10.0,
        }
        for sequence, lane in ((1, "lakebase"), (2, "competitor"))
    ]
    output = "\n".join(
        "PROGRESS_JSON:" + runner.canonical_json(value).decode()
        for value in values
    )
    progress, last = LiveConnectionSpikeAdapter._progress_from_output(
        output,
        after_sequence=0,
    )
    assert last == 2
    assert [item.authenticated_clients for item in progress] == [1_000, 2_000]
    replay, replay_last = LiveConnectionSpikeAdapter._progress_from_output(
        output,
        after_sequence=last,
    )
    assert replay == []
    assert replay_last == last
    broken = output.replace('"sequence":2', '"sequence":3')
    with pytest.raises(ConnectionSpikeLiveOperationError, match="sequence"):
        LiveConnectionSpikeAdapter._progress_from_output(
            broken,
            after_sequence=0,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        {"phase": "holding"},
        {"phase": "verified"},
        {"time_to_target_ms": 12_345.0},
    ],
)
def test_partial_progress_cannot_publish_hold_target_or_verified(
    mutation: dict[str, object],
) -> None:
    payload = {
        "schema_version": FANIN_SCHEMA_VERSION,
        "protocol": FANIN_PROTOCOL,
        "sequence": 1,
        "lane_id": "lakebase",
        "phase": "ramping",
        "initiated_clients": 7_162,
        "authenticated_clients": 7_134,
        "held_clients": 7_134,
        "peak_held_clients": 7_134,
        "terminal_failures": 0,
        "sampled_queries_succeeded": 32,
        "sampled_queries_failed": 0,
        "elapsed_ms": 42_000.0,
        "time_to_target_ms": None,
        **mutation,
    }
    output = "PROGRESS_JSON:" + runner.canonical_json(payload).decode()

    with pytest.raises(
        ConnectionSpikeLiveOperationError,
        match="semantic|aggregate barrier",
    ):
        LiveConnectionSpikeAdapter._progress_from_output(
            output,
            after_sequence=0,
        )


def test_progress_wire_output_is_compact_bounded_and_parseable(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "_progress_callback", None)
    monkeypatch.setattr(runner, "_progress_sequence", 0)
    monkeypatch.setattr(runner, "_progress_output_bytes", 0)
    value = {
        "schema_version": FANIN_SCHEMA_VERSION,
        "protocol": FANIN_PROTOCOL,
        "lane_id": "lakebase",
        "phase": "holding",
        "initiated_clients": 10_000,
        "authenticated_clients": 10_000,
        "held_clients": 10_000,
        "peak_held_clients": 10_000,
        "terminal_failures": 0,
        "elapsed_ms": 81_234.5,
        "time_to_target_ms": 48_123.4,
        "hold_remaining_ms": 12_345.6,
        "sampled_queries_succeeded": 48,
        "sampled_queries_failed": 0,
        "event_loop_p99_ms": 0.02,
        "available_memory_bytes": 9_000_000_000,
        "network_received_bytes": 123_456_789,
        "socket_states": {"established": 20_009},
    }

    for _ in range(100):
        runner._progress(value)

    output = capsys.readouterr().out
    assert len(output.encode()) <= runner.PROGRESS_OUTPUT_BUDGET_BYTES
    assert "available_memory_bytes" not in output
    assert "network_received_bytes" not in output
    assert "socket_states" not in output
    parsed, last = LiveConnectionSpikeAdapter._progress_from_output(
        output,
        after_sequence=0,
    )
    assert parsed
    assert last == len(parsed)
    assert [item.lane_id for item in parsed] == ["lakebase"] * len(parsed)


def test_execution_probes_capture_callback_cpu_and_fifo_age() -> None:
    async def scenario() -> tuple[
        runner.RuntimeDiagnostics,
        dict[str, object],
        dict[str, object],
    ]:
        loop = asyncio.get_running_loop()
        diagnostics = runner.RuntimeDiagnostics()
        diagnostics.install_ready_batch_limit(loop)
        diagnostics.install_execution_probes(loop)

        async def diagnostic_leaf_callback() -> None:
            await asyncio.sleep(0)

        try:
            await asyncio.gather(
                *(diagnostic_leaf_callback() for _ in range(64))
            )
            await asyncio.sleep(0)
            interval = diagnostics.consume_callback_interval()
            empty_interval = diagnostics.consume_callback_interval()
            return diagnostics, interval, empty_interval
        finally:
            diagnostics.restore_execution_probes()
            diagnostics.restore_ready_batch_limit()

    diagnostics, interval, empty_interval = asyncio.run(scenario())
    assert diagnostics.callback_cpu_total_ms > 0
    assert diagnostics.callback_profiles
    assert diagnostics.run_once_calls > 0
    assert diagnostics.run_once_ready_cpu_total_ms > 0
    assert interval["calls"] >= 64
    assert any(
        "diagnostic_leaf_callback" in callback_type
        for callback_type in interval["profiles"]
    )
    assert empty_interval == {
        "calls": 0,
        "cpu_total_ms": 0.0,
        "cpu_max_ms": 0.0,
        "profiles": {},
    }


def test_callback_profiling_cpu_overhead_is_bounded() -> None:
    def measure(*, probed: bool, callbacks: int = 10_000) -> float:
        loop = asyncio.new_event_loop()
        diagnostics = runner.RuntimeDiagnostics()
        diagnostics.install_ready_batch_limit(loop)
        if probed:
            diagnostics.install_execution_probes(loop)
        remaining = [callbacks]
        completed = loop.create_future()

        def callback() -> None:
            remaining[0] -= 1
            if remaining[0] == 0:
                completed.set_result(None)

        try:
            for _ in range(callbacks):
                loop.call_soon(callback)
            started_ns = time.thread_time_ns()
            loop.run_until_complete(completed)
            return (time.thread_time_ns() - started_ns) / 1_000_000
        finally:
            if probed:
                diagnostics.restore_execution_probes()
            diagnostics.restore_ready_batch_limit()
            loop.close()

    callbacks = 10_000
    baseline_ms = min(measure(probed=False, callbacks=callbacks) for _ in range(3))
    profiled_ms = min(measure(probed=True, callbacks=callbacks) for _ in range(3))

    # Budgeted per callback rather than as a ratio. The ratio was the wrong shape:
    # the baseline is a few milliseconds for 10,000 bare callbacks, so it measures
    # how fast this machine dispatches a no-op, and dividing by it turns a fixed
    # instrumentation cost into a number that swings with the hardware. It does
    # swing -- the probe reads a thread CPU clock per callback, which is close to
    # free on Apple silicon and materially slower on the x86 Linux runner CI uses,
    # so the same code measured 2.2x locally and 3.4x there.
    #
    # What the fan-in actually needs is that profiling not perturb the ramp it is
    # instrumenting. At one microsecond per callback, 10,000 clients pay ten
    # milliseconds against a ramp measured in seconds, which is inside the noise of
    # a single connect. Two microseconds is the ceiling; past that the
    # instrumentation is shaping the measurement rather than observing it.
    overhead_us_per_callback = (profiled_ms - baseline_ms) * 1_000 / callbacks
    assert overhead_us_per_callback < 2.0, (
        f"callback profiling costs {overhead_us_per_callback:.2f}us per callback "
        f"(baseline {baseline_ms:.2f}ms, profiled {profiled_ms:.2f}ms)"
    )


def test_malformed_counts_and_monotonic_times_fail_closed() -> None:
    with pytest.raises(FanInError, match="held_clients"):
        finalize({**raw_lane("lakebase"), "held_clients_at_gate": True})
    with pytest.raises(FanInError, match="time_to_target"):
        finalize({**raw_lane("lakebase"), "time_to_target_ms": -1.0})


def test_proxy_endpoint_resolution_retries_on_gaierror_within_a_bounded_window() -> None:
    """The per-bout Proxy endpoint's DNS can lag CreateDBProxy; prepare must not
    crash on the first gaierror. Resolution retries under a bounded deadline, and
    the RELEASE gate still keeps any client from connecting before the Proxy is
    available."""

    source = inspect.getsource(runner.execute_fanin)
    getaddr = source.index("loop.getaddrinfo(")
    # The resolve is wrapped in a retry that tolerates gaierror and polls until a
    # bounded deadline rather than raising on the first failure.
    assert "except socket.gaierror" in source
    deadline = source.index("PROXY_ENDPOINT_RESOLVE_TIMEOUT_SECONDS")
    poll = source.index("PROXY_ENDPOINT_RESOLVE_POLL_SECONDS")
    failure = source.index("_host_resolution_failed")
    # The bounded deadline and poll are set up before the resolve loop, and the
    # hard failure is only raised once the deadline is exceeded.
    assert deadline < getaddr < failure
    assert poll > getaddr
    assert runner.PROXY_ENDPOINT_RESOLVE_TIMEOUT_SECONDS > 0
    assert 0 < runner.PROXY_ENDPOINT_RESOLVE_POLL_SECONDS
    assert (
        runner.PROXY_ENDPOINT_RESOLVE_POLL_SECONDS
        < runner.PROXY_ENDPOINT_RESOLVE_TIMEOUT_SECONDS
    )
