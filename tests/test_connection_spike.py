from __future__ import annotations

import json
from dataclasses import replace

from server.connection_fanin import (
    ConnectionSpikeGates,
    ConnectionSpikeLaneResult,
)
from server.connection_spike import (
    PublicSetupEvidence,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupStopGateEvidence,
    arm_setup_phase,
    finalize_setup_phase,
)

SHA = "a" * 64


def setup_stop_gate(
    verified_at_ns: int,
    *,
    observed_state: str = "ready",
) -> SetupStopGateEvidence:
    return SetupStopGateEvidence(
        gate_id="pooled-endpoint-ready",
        expected=(
            PublicSetupEvidence("resource_name", "owned-resource"),
            PublicSetupEvidence("state", "ready"),
        ),
        observed=(
            PublicSetupEvidence("state", observed_state),
            PublicSetupEvidence("resource_name", "owned-resource"),
        ),
        verified_at_ns=verified_at_ns,
    )


def verified_fanin_lane(lane_id: str) -> ConnectionSpikeLaneResult:
    return ConnectionSpikeLaneResult(
        lane_id=lane_id,
        initiated_clients=10_000,
        authenticated_clients=10_000,
        held_clients_at_gate=10_000,
        terminal_failures=0,
        failure_codes={},
        retries=0,
        disconnected_during_hold=0,
        time_to_target_ns=1_000_000_000,
        time_to_target_ms=1_000.0,
        hold_elapsed_ms=30_000.0,
        sampled_queries_attempted=64,
        sampled_queries_succeeded=64,
        sampled_queries_failed=0,
        preexisting_client_role_sessions=0,
        observer_role="anti_demo_observer",
        client_role="anti_demo_burst",
        observer_direct=True,
        current_backend_sessions=4,
        peak_backend_sessions=8,
        unique_backend_pids=8,
        distinct_socket_fds=10_000,
        distinct_local_endpoints=10_000,
        socket_identity_sha256=SHA,
        connect_latency_p50_ms=50.0,
        connect_latency_p95_ms=150.0,
        auth_method=(
            "tls-cleartext-password"
            if lane_id == "lakebase"
            else "scram-sha-256"
        ),
        connect_latency_p99_ms=201.455,
        endpoint_host_sha256=SHA,
        credential_sha256=SHA,
        observer_credential_sha256=SHA,
        tls_mode="verify-full",
        config_sha256=SHA,
        generator_sha256=SHA,
        capacity_model_sha256=SHA,
        telemetry_samples=49,
        telemetry_physical_memory_bytes=15 * 1024**3,
        telemetry_min_available_memory_bytes=7 * 1024**3,
        telemetry_peak_rss_bytes=7 * 1024**3,
        telemetry_fd_soft_limit=65_535,
        telemetry_peak_open_fds=20_264,
        telemetry_ephemeral_port_count=28_232,
        telemetry_min_ephemeral_port_reserve=18_232,
        telemetry_peak_event_loop_p99_ms=4.5,
        telemetry_peak_cpu_capacity_fraction=0.31,
        telemetry_failures=(),
        launch_skew_ms=0.1,
        achieved_elapsed_ms=31_000.0,
        gates=ConnectionSpikeGates(
            exact_count=True,
            zero_failures=True,
            hold=True,
            sampled_queries=True,
            multiplexing=True,
            identity=True,
            observer_separation=True,
            fairness=True,
            telemetry=True,
            cleanup=True,
        ),
    )


def test_setup_phase_is_supporting_and_gated_by_downstream_fanin() -> None:
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=t0_ns,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 20_000_000_123),
        ),
        SetupLaneObservation(
            lane_id="competitor",
            workflow_launched_ns=t0_ns + 10_000_000,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 30_000_000_456),
        ),
    )
    fanin = {
        lane_id: verified_fanin_lane(lane_id)
        for lane_id in ("lakebase", "competitor")
    }
    result = finalize_setup_phase(arm, observations, fanin)

    assert result.setup_validated
    assert result.downstream_validated
    assert result.lanes["lakebase"].setup_elapsed_ms == 20_000.000123
    # This comparison remains supporting-only; the manager publishes the v2
    # fan-in comparison rather than this setup comparison.
    assert result.comparison is not None
    public = json.loads(json.dumps(result.to_public_dict()))
    assert "t0_ns" not in public
    assert "verified_at_ns" not in public["lanes"]["lakebase"]["stop_gate_evidence"]


def test_setup_failure_or_invalid_gate_never_validates_downstream() -> None:
    t0_ns = 5_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    fanin = {
        lane_id: verified_fanin_lane(lane_id)
        for lane_id in ("lakebase", "competitor")
    }
    result = finalize_setup_phase(
        arm,
        (
            SetupLaneObservation(
                lane_id="lakebase",
                workflow_launched_ns=t0_ns,
                status=SetupLaneStatus.SUCCEEDED,
                stop_gate_evidence=setup_stop_gate(
                    arm.deadline_ns + 1,
                    observed_state="starting",
                ),
            ),
            SetupLaneObservation(
                lane_id="competitor",
                workflow_launched_ns=t0_ns,
                status=SetupLaneStatus.SUCCEEDED,
                stop_gate_evidence=setup_stop_gate(t0_ns + 1_000_000_000),
            ),
        ),
        fanin,
    )
    assert not result.setup_validated
    assert result.comparison is None

    fanin["competitor"] = replace(
        fanin["competitor"],
        gates=replace(fanin["competitor"].gates, cleanup=False),
    )
    valid_setup = tuple(
        SetupLaneObservation(
            lane_id=lane_id,
            workflow_launched_ns=t0_ns,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 1_000_000_000),
        )
        for lane_id in ("lakebase", "competitor")
    )
    downstream_failed = finalize_setup_phase(arm, valid_setup, fanin)
    assert downstream_failed.setup_validated
    assert not downstream_failed.downstream_validated
    assert downstream_failed.comparison is None
