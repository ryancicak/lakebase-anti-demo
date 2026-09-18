from __future__ import annotations

import json
from dataclasses import replace

import pytest

from server.connection_fanin import (
    LANE_CONNECT_CONCURRENCY,
    SAFETY_EVIDENCE_VERSION,
    ConnectionSpikeGates,
    ConnectionSpikeLaneResult,
)
from server.connection_spike import (
    MAX_SETUP_REQUEST_LAUNCH_DELAY_MS,
    MAX_SETUP_WORKFLOW_LAUNCH_SKEW_MS,
    PublicSetupEvidence,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupStopGateEvidence,
    arm_setup_phase,
    finalize_setup_lane,
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
        telemetry_peak_ephemeral_ports_in_use=10_000,
        telemetry_min_ephemeral_port_reserve=18_232,
        telemetry_peak_event_loop_p99_ms=4.5,
        telemetry_peak_cpu_capacity_fraction=0.31,
        telemetry_failures=(),
        telemetry_advisories=(),
        safety_evidence_version=SAFETY_EVIDENCE_VERSION,
        hard_safety_verified=True,
        port_accounting_verified=True,
        admission_controller_min_concurrency=LANE_CONNECT_CONCURRENCY,
        admission_controller_reductions=0,
        admission_controller_recoveries=0,
        admission_controller_throttled_ms=0.0,
        admission_controller_recovery_hysteresis_intervals=3,
        admission_controller_pressure_hysteresis_intervals=2,
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
            # A pooled lane may start with the warm pool that proving it ready created;
            # these fixtures start clean, so the gate simply passes.
            clean_start=True,
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


# Exact live monotonic launch stamps from bout eb2b79b7173544fc92ff3a0da2ecec9f,
# relative to the shared setup T0. Pinned as literals (not constant+offset) so the
# regression is anchored to the field values the deployed HEAD actually recorded.
_LIVE_LAKEBASE_LAUNCH_DELTA_NS = 7_873_272
_LIVE_COMPETITOR_LAUNCH_DELTA_NS = 10_530_956
_LIVE_INTER_LANE_SKEW_NS = _LIVE_COMPETITOR_LAUNCH_DELTA_NS - _LIVE_LAKEBASE_LAUNCH_DELTA_NS


def test_live_inter_lane_launch_skew_passes() -> None:
    # The live pair's inter-lane workflow-start skew was 2.657684 ms -- well within
    # the 10 ms fairness bound -- even though the competitor's absolute delta
    # (10.530956 ms) exceeded the retired 10 ms absolute gate. workflow_launched_ns
    # is no longer scored absolutely; the exact live pair must verify.
    assert _LIVE_INTER_LANE_SKEW_NS == 2_657_684
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=t0_ns + _LIVE_LAKEBASE_LAUNCH_DELTA_NS,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 7_874_849),
        ),
        SetupLaneObservation(
            lane_id="competitor",
            workflow_launched_ns=t0_ns + _LIVE_COMPETITOR_LAUNCH_DELTA_NS,
            status=SetupLaneStatus.SUCCEEDED,
            # CreateDBProxy requested ~2 ms after the competitor workflow launched,
            # far inside the 100 ms absolute request budget.
            create_db_proxy_requested_ns=t0_ns + 12_500_000,
            stop_gate_evidence=setup_stop_gate(t0_ns + 607_808_521_774),
        ),
    )
    fanin = {
        lane_id: verified_fanin_lane(lane_id)
        for lane_id in ("lakebase", "competitor")
    }
    result = finalize_setup_phase(arm, observations, fanin)

    assert result.workflow_launch_skew_ms == pytest.approx(2.657684)
    assert result.workflow_launch_skew_ms <= MAX_SETUP_WORKFLOW_LAUNCH_SKEW_MS
    assert result.lanes["lakebase"].failures == ()
    assert result.lanes["competitor"].failures == ()
    assert result.setup_validated
    assert result.downstream_validated
    assert result.comparison is not None
    assert result.comparison.winner_lane_id == "lakebase"


def test_workflow_launched_ns_is_no_longer_scored_absolutely() -> None:
    # A lane whose workflow_launched stamp is far past the retired 10 ms ceiling
    # (here 90 ms) still verifies, as long as it issues no over-budget real
    # request. workflow_launched_ns is only a lower bound, never scored absolutely.
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observation = SetupLaneObservation(
        lane_id="competitor",
        workflow_launched_ns=t0_ns + 90_000_000,  # 90 ms after T0
        status=SetupLaneStatus.SUCCEEDED,
        create_db_proxy_requested_ns=t0_ns + 92_000_000,  # within 100 ms
        stop_gate_evidence=setup_stop_gate(t0_ns + 607_808_521_774),
    )
    lane = finalize_setup_lane(arm, observation)

    assert lane.failures == ()
    assert lane.verified


def test_create_db_proxy_request_beyond_100ms_is_a_nonfatal_advisory() -> None:
    # OVERRIDE: a present, non-negative but slow CreateDBProxy request (Aurora was
    # slow) is a SCHEDULING advisory, not a fatal gate. The lane still verifies;
    # the delay is already charged to Aurora's own bell->10k clock.
    t0_ns = 1_000_000_000
    beyond_ns = int(MAX_SETUP_REQUEST_LAUNCH_DELAY_MS * 1_000_000) + 1
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observation = SetupLaneObservation(
        lane_id="competitor",
        workflow_launched_ns=t0_ns + 5_000_000,
        status=SetupLaneStatus.SUCCEEDED,
        create_db_proxy_requested_ns=t0_ns + beyond_ns,
        requires_create_db_proxy_stamp=True,
        stop_gate_evidence=setup_stop_gate(t0_ns + 607_808_521_774),
    )
    lane = finalize_setup_lane(arm, observation)

    assert "create_db_proxy_window" in lane.scheduling_advisories
    assert "create_db_proxy_window" not in lane.failures
    assert lane.failures == ()
    assert lane.verified


def test_inter_lane_launch_skew_beyond_10ms_is_a_nonfatal_advisory() -> None:
    # OVERRIDE: an over-budget inter-lane skew is a SCHEDULING advisory, not a
    # fatal gate. Both lanes still verify and a comparison is still declared;
    # the skew is host-scheduling jitter charged to each lane's own clock.
    t0_ns = 1_000_000_000
    skew_ns = int(MAX_SETUP_WORKFLOW_LAUNCH_SKEW_MS * 1_000_000) + 1_000_000  # ~11 ms apart
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=t0_ns + 1_000_000,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 8_000_000),
        ),
        SetupLaneObservation(
            lane_id="competitor",
            workflow_launched_ns=t0_ns + 1_000_000 + skew_ns,
            status=SetupLaneStatus.SUCCEEDED,
            create_db_proxy_requested_ns=t0_ns + 1_000_000 + skew_ns + 500_000,
            requires_create_db_proxy_stamp=True,
            stop_gate_evidence=setup_stop_gate(t0_ns + 607_808_521_774),
        ),
    )
    fanin = {
        lane_id: verified_fanin_lane(lane_id)
        for lane_id in ("lakebase", "competitor")
    }
    result = finalize_setup_phase(arm, observations, fanin)

    assert result.workflow_launch_skew_ms > MAX_SETUP_WORKFLOW_LAUNCH_SKEW_MS
    assert "workflow_launch_skew" in result.lanes["lakebase"].scheduling_advisories
    assert "workflow_launch_skew" in result.lanes["competitor"].scheduling_advisories
    assert result.lanes["lakebase"].failures == ()
    assert result.lanes["competitor"].failures == ()
    assert result.setup_validated
    assert result.comparison is not None
    assert result.comparison.winner_lane_id == "lakebase"


def test_workflow_launch_before_t0_is_still_fatal() -> None:
    # A stamp before the shared T0 is a genuine ordering/clock fault -- it would
    # corrupt the shared-T0 elapsed and skew maths -- and remains fatal, unlike
    # positive scheduling jitter.
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observation = SetupLaneObservation(
        lane_id="lakebase",
        workflow_launched_ns=t0_ns - 1,
        status=SetupLaneStatus.SUCCEEDED,
        stop_gate_evidence=setup_stop_gate(t0_ns + 1_000_000_000),
    )
    lane = finalize_setup_lane(arm, observation)

    assert "workflow_launch_ordering" in lane.failures
    assert not lane.verified


# Exact live wake stamps (relative to the shared setup T0) from the 2026-09-17
# failed bout f7b20d9caa624dd88ef0f4542090512b, bell-a20671371fcc428c9dcce07d43ebba7c.
# Pinned as literals. The competitor's proxy CREATE_INTENT durable wall was
# ~163.510 ms after T0 on the OLD path (5 cold coordination connects before the
# boto3 call), which blew the 100 ms window; the exact monotonic proxy-request
# delta was never persisted. The corrected path pre-commits the intent before T0
# so the direct CreateDBProxy request lands a few ms after the gate.
_LIVE_F7B_LAKEBASE_WAKE_NS = 7_104_418
_LIVE_F7B_COMPETITOR_WAKE_NS = 8_827_005
_LIVE_F7B_INTER_LANE_SKEW_NS = _LIVE_F7B_COMPETITOR_WAKE_NS - _LIVE_F7B_LAKEBASE_WAKE_NS


def test_live_f7b_corrected_path_verifies_within_100ms() -> None:
    # With the pre-staged-intent fix, the competitor issues its direct
    # CreateDBProxy ~4 ms after its wake -- well inside the 100 ms bell-relative
    # window -- and the pinned live inter-lane skew (1.722587 ms) passes.
    assert _LIVE_F7B_INTER_LANE_SKEW_NS == 1_722_587
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=t0_ns + _LIVE_F7B_LAKEBASE_WAKE_NS,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 12_900_000_000),
        ),
        SetupLaneObservation(
            lane_id="competitor",
            workflow_launched_ns=t0_ns + _LIVE_F7B_COMPETITOR_WAKE_NS,
            status=SetupLaneStatus.SUCCEEDED,
            # Corrected path: intent pre-committed before T0, direct boto3 request
            # ~4 ms after the competitor wake (12.8 ms after T0).
            create_db_proxy_requested_ns=t0_ns + 12_800_000,
            requires_create_db_proxy_stamp=True,
            stop_gate_evidence=setup_stop_gate(t0_ns + 593_719_000_000),
        ),
    )
    fanin = {lane_id: verified_fanin_lane(lane_id) for lane_id in ("lakebase", "competitor")}
    result = finalize_setup_phase(arm, observations, fanin)

    assert result.workflow_launch_skew_ms == pytest.approx(1.722587)
    assert result.lanes["competitor"].failures == ()
    assert result.lanes["competitor"].create_db_proxy_request_delta_ms == pytest.approx(12.8)
    assert result.setup_validated
    assert result.comparison is not None
    assert result.comparison.winner_lane_id == "lakebase"


def test_live_f7b_old_path_163ms_declares_with_advisory() -> None:
    # OVERRIDE: the OLD path's ~163.510 ms CreateDBProxy request is now a
    # non-fatal scheduling advisory. The exact, honestly-obtained proof still
    # verifies and declares; the slow request is recorded, not fatal.
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observation = SetupLaneObservation(
        lane_id="competitor",
        workflow_launched_ns=t0_ns + _LIVE_F7B_COMPETITOR_WAKE_NS,
        status=SetupLaneStatus.SUCCEEDED,
        create_db_proxy_requested_ns=t0_ns + 163_510_000,
        requires_create_db_proxy_stamp=True,
        stop_gate_evidence=setup_stop_gate(t0_ns + 593_719_000_000),
    )
    lane = finalize_setup_lane(arm, observation)

    assert "create_db_proxy_window" in lane.scheduling_advisories
    assert lane.failures == ()
    assert lane.verified
    assert lane.create_db_proxy_request_delta_ms == pytest.approx(163.51)


def test_competitor_missing_create_db_proxy_stamp_is_fatal() -> None:
    # FATAL evidence fault: a SUCCEEDED competitor observation with no CreateDBProxy
    # stamp means the one timed AWS mutation was never observed at its request
    # boundary -- exactly how a skipped or pre-adopted Proxy would present.
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observation = SetupLaneObservation(
        lane_id="competitor",
        workflow_launched_ns=t0_ns + 5_000_000,
        status=SetupLaneStatus.SUCCEEDED,
        create_db_proxy_requested_ns=None,
        requires_create_db_proxy_stamp=True,
        stop_gate_evidence=setup_stop_gate(t0_ns + 593_719_000_000),
    )
    lane = finalize_setup_lane(arm, observation)

    assert "create_db_proxy_missing" in lane.failures
    assert not lane.verified
    assert lane.create_db_proxy_request_delta_ms is None


def test_competitor_pre_bell_create_db_proxy_is_fatal() -> None:
    # FATAL evidence fault: a CreateDBProxy stamp before the bell T0 is a
    # pre-bell request / wrong clock domain (a pre-created Proxy would look like
    # this). Distinct from a merely-slow request.
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observation = SetupLaneObservation(
        lane_id="competitor",
        workflow_launched_ns=t0_ns + 5_000_000,
        status=SetupLaneStatus.SUCCEEDED,
        create_db_proxy_requested_ns=t0_ns - 1,
        requires_create_db_proxy_stamp=True,
        stop_gate_evidence=setup_stop_gate(t0_ns + 593_719_000_000),
    )
    lane = finalize_setup_lane(arm, observation)

    assert "create_db_proxy_pre_bell" in lane.failures
    assert not lane.verified


@pytest.mark.parametrize(
    ("delta_ns", "fatal", "advisory"),
    [
        (-1, True, False),  # before T0: FATAL (pre-bell / wrong domain)
        (0, False, False),  # exactly at T0: clean
        (int(MAX_SETUP_REQUEST_LAUNCH_DELAY_MS * 1_000_000), False, False),  # exactly 100 ms
        (int(MAX_SETUP_REQUEST_LAUNCH_DELAY_MS * 1_000_000) + 1, False, True),  # +1ns: advisory
    ],
)
def test_create_db_proxy_window_boundaries(delta_ns: int, fatal: bool, advisory: bool) -> None:
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observation = SetupLaneObservation(
        lane_id="competitor",
        workflow_launched_ns=t0_ns + max(0, min(delta_ns, 1_000_000)),
        status=SetupLaneStatus.SUCCEEDED,
        create_db_proxy_requested_ns=t0_ns + delta_ns,
        requires_create_db_proxy_stamp=True,
        stop_gate_evidence=setup_stop_gate(t0_ns + 593_719_000_000),
    )
    lane = finalize_setup_lane(arm, observation)
    assert bool(lane.failures) is fatal
    assert lane.verified is (not fatal)
    assert ("create_db_proxy_window" in lane.scheduling_advisories) is advisory


@pytest.mark.parametrize(
    ("skew_ns", "advisory"),
    [
        (int(MAX_SETUP_WORKFLOW_LAUNCH_SKEW_MS * 1_000_000), False),  # exactly 10 ms
        (int(MAX_SETUP_WORKFLOW_LAUNCH_SKEW_MS * 1_000_000) + 1, True),  # 10 ms + 1 ns: advisory
    ],
)
def test_inter_lane_skew_boundaries(skew_ns: int, advisory: bool) -> None:
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=t0_ns)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=t0_ns + 1_000_000,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 8_000_000),
        ),
        SetupLaneObservation(
            lane_id="competitor",
            workflow_launched_ns=t0_ns + 1_000_000 + skew_ns,
            status=SetupLaneStatus.SUCCEEDED,
            create_db_proxy_requested_ns=t0_ns + 1_000_000 + skew_ns + 100_000,
            requires_create_db_proxy_stamp=True,
            stop_gate_evidence=setup_stop_gate(t0_ns + 593_719_000_000),
        ),
    )
    fanin = {lane_id: verified_fanin_lane(lane_id) for lane_id in ("lakebase", "competitor")}
    result = finalize_setup_phase(arm, observations, fanin)
    # Skew never voids: a comparison is always declared for an exact pair.
    assert result.comparison is not None
    assert result.setup_validated
    assert (
        "workflow_launch_skew" in result.lanes["competitor"].scheduling_advisories
    ) is advisory


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
