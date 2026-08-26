from __future__ import annotations

import json
from dataclasses import replace
from uuid import UUID

from server.connection_spike import (
    AttemptObservation,
    AttemptStatus,
    ComparisonOutcome,
    ConnectionSpikeWitness,
    PublicSetupEvidence,
    RetainedClientWitness,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupStopGateEvidence,
    SharedBarrier,
    arm_setup_phase,
    build_schedule,
    compare_lanes,
    finalize_lane,
    finalize_setup_phase,
    nearest_rank_p99,
)


def sequential_uuid_factory():
    value = 0

    def factory() -> UUID:
        nonlocal value
        value += 1
        return UUID(int=value)

    return factory


def valid_witness() -> ConnectionSpikeWitness:
    return ConnectionSpikeWitness(
        clients=tuple(
            RetainedClientWitness(
                client_id=f"client-{index}",
                retained=True,
                verified=True,
                backend_pid=10_000 + index % 16,
            )
            for index in range(64)
        ),
        peak_backend_sessions=16,
    )


def observations_for(
    schedule,
    lane_id: str,
    barrier: SharedBarrier,
):
    barrier_release_ns = barrier.release_ns
    return tuple(
        AttemptObservation(
            attempt_id=attempt.attempt_id,
            status=AttemptStatus.SUCCESS,
            started_ns=(
                barrier_release_ns - 5_000_000 if attempt.kind == "warmup" else barrier_release_ns
            ),
            completed_ns=(
                barrier_release_ns - 1_000_000
                if attempt.kind == "warmup"
                else barrier_release_ns + (attempt.ordinal + 1) * 1_000_000
            ),
            response=attempt.proof,
            committed=attempt.proof,
        )
        for attempt in schedule.lane_attempts(lane_id)
    )


def verified_burst_lanes():
    schedule = build_schedule(
        ("lakebase", "aurora"),
        scheduled_at_ns=1,
        uuid_factory=sequential_uuid_factory(),
    )
    barrier = SharedBarrier(
        release_ns=20_000_000,
        first_launch_ns_by_lane={"lakebase": 20_000_000, "aurora": 20_000_000},
    )
    return {
        lane_id: finalize_lane(
            schedule,
            lane_id,
            observations_for(schedule, lane_id, barrier),
            valid_witness(),
            barrier,
            cleanup_verified=True,
            fairness_verified=True,
            contracts_verified=True,
        )
        for lane_id in schedule.lane_ids
    }


def setup_stop_gate(
    verified_at_ns: int,
    *,
    observed_state: str = "ready",
) -> SetupStopGateEvidence:
    return SetupStopGateEvidence(
        gate_id="pooled-endpoint-ready",
        expected=(
            PublicSetupEvidence("resource_id", "owned-resource"),
            PublicSetupEvidence("state", "ready"),
        ),
        observed=(
            PublicSetupEvidence("state", observed_state),
            PublicSetupEvidence("resource_id", "owned-resource"),
        ),
        verified_at_ns=verified_at_ns,
    )


def test_schedule_requires_pre_barrier_unique_terminal_settlement() -> None:
    schedule = build_schedule(
        ("lakebase", "aurora"),
        scheduled_at_ns=1_000_000,
        uuid_factory=sequential_uuid_factory(),
    )
    barrier = SharedBarrier(
        release_ns=10_000_000,
        first_launch_ns_by_lane={"lakebase": 11_000_000, "aurora": 19_000_000},
    )
    observations = observations_for(schedule, "lakebase", barrier)

    result = finalize_lane(
        schedule,
        "lakebase",
        observations,
        valid_witness(),
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )

    assert len(schedule.lane_attempts("lakebase")) == 132
    assert {attempt.worker_slot for attempt in schedule.lane_attempts("lakebase")} == set(range(64))
    assert result.scheduled_clients == result.terminal_clients == 128
    assert result.successful_clients == 128
    assert result.error_clients == 0
    assert result.launch_skew_ms == 8.0
    assert result.verified

    duplicate = (*observations[:-1], observations[0])
    unsettled = finalize_lane(
        schedule,
        "lakebase",
        duplicate,
        valid_witness(),
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )
    assert not unsettled.verified
    assert "attempts_not_unique_terminal_and_settled" in unsettled.gates.failures


def test_exact_response_commit_p99_and_lexicographic_scoring() -> None:
    schedule = build_schedule(
        ("lakebase", "aurora"),
        scheduled_at_ns=1,
        uuid_factory=sequential_uuid_factory(),
    )
    barrier = SharedBarrier(
        release_ns=10,
        first_launch_ns_by_lane={"lakebase": 10, "aurora": 10},
    )
    lakebase_observations = list(observations_for(schedule, "lakebase", barrier))
    lakebase_observations = [
        replace(
            observation,
            started_ns=(
                observation.completed_ns - 500_000
                if observation.attempt_id
                in {item.attempt_id for item in schedule.lane_attempts("lakebase")[-128:]}
                else observation.started_ns
            ),
        )
        for observation in lakebase_observations
    ]
    lakebase = finalize_lane(
        schedule,
        "lakebase",
        lakebase_observations,
        valid_witness(),
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )
    aurora_observations = list(observations_for(schedule, "aurora", barrier))
    scored = schedule.lane_attempts("aurora")[-128:]
    error_id = scored[-1].attempt_id
    index = next(
        index
        for index, observation in enumerate(aurora_observations)
        if observation.attempt_id == error_id
    )
    aurora_observations[index] = AttemptObservation(
        attempt_id=error_id,
        status=AttemptStatus.ERROR,
        error="connection rejected",
    )
    aurora = finalize_lane(
        schedule,
        "aurora",
        aurora_observations,
        valid_witness(),
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )

    assert nearest_rank_p99([]) is None
    assert nearest_rank_p99([3.125, 1.0, 2.0]) == 3.125
    assert lakebase.application_p99_ms == 0.5
    comparison = compare_lanes(lakebase, aurora)
    assert comparison is not None
    assert comparison.outcome == ComparisonOutcome.LEFT
    assert comparison.winner_lane_id == "lakebase"

    bad = list(observations_for(schedule, "lakebase", barrier))
    bad[-1] = replace(bad[-1], response=bad[-2].response)
    invalid_contract = finalize_lane(
        schedule,
        "lakebase",
        bad,
        valid_witness(),
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )
    assert not invalid_contract.verified
    assert compare_lanes(invalid_contract, aurora) is None


def test_retained_witness_requires_all_clients_and_pid_session_reuse() -> None:
    schedule = build_schedule(
        ("lakebase", "aurora"),
        scheduled_at_ns=1,
        uuid_factory=sequential_uuid_factory(),
    )
    barrier = SharedBarrier(
        release_ns=2,
        first_launch_ns_by_lane={"lakebase": 2, "aurora": 2},
    )
    result = finalize_lane(
        schedule,
        "lakebase",
        observations_for(schedule, "lakebase", barrier),
        valid_witness(),
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )
    assert result.witness_verified_clients == 64
    assert result.unique_backend_pids == 16
    assert result.peak_backend_sessions == 16
    assert result.verified

    no_reuse = ConnectionSpikeWitness(
        clients=tuple(
            RetainedClientWitness(f"client-{index}", True, True, 20_000 + index)
            for index in range(64)
        ),
        peak_backend_sessions=64,
    )
    invalid = finalize_lane(
        schedule,
        "lakebase",
        observations_for(schedule, "lakebase", barrier),
        no_reuse,
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )
    assert not invalid.verified
    assert "witness" in invalid.gates.failures
    assert compare_lanes(result, invalid) is None

    zero_peak = finalize_lane(
        schedule,
        "lakebase",
        observations_for(schedule, "lakebase", barrier),
        replace(valid_witness(), peak_backend_sessions=0),
        barrier,
        cleanup_verified=True,
        fairness_verified=True,
        contracts_verified=True,
    )
    assert not zero_peak.verified
    assert "witness" in zero_peak.gates.failures
    assert compare_lanes(result, zero_peak) is None


def test_setup_phase_scores_raw_elapsed_only_after_downstream_validation() -> None:
    t0_ns = 1_000_000_000
    arm = arm_setup_phase(("lakebase", "aurora"), t0_ns=t0_ns)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=t0_ns,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 20_000_000_123),
        ),
        SetupLaneObservation(
            lane_id="aurora",
            workflow_launched_ns=t0_ns + 10_000_000,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=setup_stop_gate(t0_ns + 30_000_000_456),
        ),
    )
    bursts = verified_burst_lanes()
    bursts["aurora"] = replace(bursts["aurora"], application_p99_ms=1.0)

    result = finalize_setup_phase(arm, observations, bursts)

    assert arm.deadline_ns == t0_ns + 30 * 60 * 1_000_000_000
    assert result.workflow_launch_skew_ms == 10.0
    assert result.downstream_validated
    assert result.winner_lane_id == "lakebase"
    assert result.margin_ns == 10_000_000_333
    assert result.margin_ms == 10_000.000333
    assert result.lanes["lakebase"].setup_elapsed_ns == 20_000_000_123
    assert bursts["lakebase"].application_p99_ms == 127.0
    assert bursts["aurora"].application_p99_ms == 1.0
    public_result = json.loads(json.dumps(result.to_public_dict()))
    assert public_result["comparison"]["winner_lane_id"] == "lakebase"
    assert "t0_ns" not in public_result
    assert "deadline_ns" not in public_result
    assert "workflow_launched_ns" not in public_result["lanes"]["lakebase"]
    assert "verified_at_ns" not in public_result["lanes"]["lakebase"]["stop_gate_evidence"]
    assert "failures" not in public_result["lanes"]["lakebase"]


def test_setup_phase_failure_towel_and_invalid_evidence_have_no_score() -> None:
    t0_ns = 5_000_000_000
    arm = arm_setup_phase(("lakebase", "aurora"), t0_ns=t0_ns)
    bursts = verified_burst_lanes()
    invalid = finalize_setup_phase(
        arm,
        (
            SetupLaneObservation(
                lane_id="lakebase",
                workflow_launched_ns=t0_ns + 10_000_001,
                status=SetupLaneStatus.SUCCEEDED,
                stop_gate_evidence=setup_stop_gate(
                    arm.deadline_ns + 1,
                    observed_state="starting",
                ),
            ),
            SetupLaneObservation(
                lane_id="aurora",
                workflow_launched_ns=t0_ns,
                status=SetupLaneStatus.SUCCEEDED,
                stop_gate_evidence=setup_stop_gate(t0_ns + 1_000_000_000),
            ),
        ),
        bursts,
    )
    assert invalid.comparison is None
    assert invalid.winner_lane_id is invalid.margin_ns is invalid.margin_ms is None
    assert invalid.lanes["lakebase"].failures == (
        "workflow_launch_window",
        "stop_gate_evidence",
    )

    stopped = finalize_setup_phase(
        arm,
        (
            SetupLaneObservation(
                "lakebase", t0_ns, SetupLaneStatus.FAILED, error="provider failed"
            ),
            SetupLaneObservation("aurora", t0_ns, SetupLaneStatus.TOWELLED),
        ),
        bursts,
    )
    assert stopped.comparison is None
    assert stopped.winner_lane_id is stopped.margin_ns is stopped.margin_ms is None
    assert stopped.lanes["lakebase"].setup_elapsed_ns is None
    assert stopped.lanes["aurora"].setup_elapsed_ns is None

    good_observations = (
        SetupLaneObservation(
            "lakebase",
            t0_ns,
            SetupLaneStatus.SUCCEEDED,
            setup_stop_gate(t0_ns + 1_000_000_000),
        ),
        SetupLaneObservation(
            "aurora",
            t0_ns,
            SetupLaneStatus.SUCCEEDED,
            setup_stop_gate(t0_ns + 2_000_000_000),
        ),
    )
    bursts["aurora"] = replace(
        bursts["aurora"],
        gates=replace(bursts["aurora"].gates, cleanup=False),
    )
    downstream_failed = finalize_setup_phase(arm, good_observations, bursts)
    assert not downstream_failed.downstream_validated
    assert downstream_failed.comparison is None
    assert downstream_failed.margin_ms is None
