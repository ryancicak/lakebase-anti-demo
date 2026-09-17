"""Deterministic coverage for the Round 5 setup-stop public projection.

These tests pin the exact defect that made a genuinely verified Lakebase setup
lane finalize as ``verified=false`` / ``stop_gate_evidence=null``: the public
evidence fact key ``pooled_endpoint_binding_exact`` collided with the
sensitive-key denylist substring ``"endpoint"``, so ``_round_five_public_gate``
dropped the whole gate and downgraded the lane, blocking the bout-level
comparison. Aurora's fact set contains no such substring, which is why only the
Lakebase lane failed.

Every fact set here is derived from the production observation builder
(``LiveConnectionSpikeSetupOrchestrator._setup_observation``) rather than
hard-coded, so a regression in the shipped keys fails the suite. The sanitizer
is asserted to remain strict: the old ``endpoint`` key and any string carrying a
host/ARN/secret are still rejected.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from test_connection_spike import verified_fanin_lane

from server.connection_spike import (
    PublicSetupEvidence,
    SetupLaneObservation,
    SetupLaneStatus,
    SetupStopGateEvidence,
    arm_setup_phase,
    finalize_setup_phase,
)
from server.connection_spike_live import (
    ConnectionSpikeSetupLaneStop,
    LiveConnectionSpikeSetupOrchestrator,
)
from server.manager import RunManager

T0_NS = 1_000_000_000


def _production_observation(lane_id: str, *, launch_delay_ns: int, elapsed_ns: int):
    """Return the exact SetupLaneObservation the shipped orchestrator emits.

    ``_setup_observation`` reads only ``stop``; calling it unbound derives the
    real production fact keys so a rename regression fails this test.
    """

    stop = ConnectionSpikeSetupLaneStop(
        lane_id=lane_id,
        launched_ns=T0_NS + launch_delay_ns,
        stopped_ns=T0_NS + elapsed_ns,
        credential_sha256="a" * 64,
        endpoint_host="pooled.internal" if lane_id == "lakebase" else "proxy.internal",
        secret_arn="" if lane_id == "lakebase" else "arn:aws:secretsmanager:x:y:secret:z",
    )
    return LiveConnectionSpikeSetupOrchestrator._setup_observation(None, stop)


def _fake_snapshot():
    return SimpleNamespace(
        id="sess-projection",
        lanes={
            "lakebase": SimpleNamespace(name="Lakebase"),
            "competitor": SimpleNamespace(name="Aurora"),
        },
    )


def _production_setup_result():
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=T0_NS)
    observations = (
        _production_observation("lakebase", launch_delay_ns=2_000_000, elapsed_ns=3_600_000_000),
        _production_observation(
            "competitor", launch_delay_ns=6_000_000, elapsed_ns=650_000_000_000
        ),
    )
    fanin = {lane_id: verified_fanin_lane(lane_id) for lane_id in ("lakebase", "competitor")}
    return finalize_setup_phase(arm, observations, fanin)


def test_production_lakebase_and_aurora_fact_sets_survive_public_projection() -> None:
    for lane_id in ("lakebase", "competitor"):
        observation = _production_observation(lane_id, launch_delay_ns=2_000_000, elapsed_ns=5_000)
        # Sanity: the production key MUST NOT smuggle the denylisted substring
        # back in, or the gate would be nulled again.
        keys = {fact.key for fact in observation.stop_gate_evidence.expected}
        assert not any("endpoint" in key or "host" in key for key in keys), keys
        gate = RunManager._round_five_public_gate(observation.stop_gate_evidence)
        assert gate is not None, f"{lane_id} public gate was dropped by projection"
        assert gate.exact
        # Round trip through JSON leaves the gate intact (dict-fact aware).
        reloaded = RunManager._round_five_public_gate(
            json.loads(json.dumps(observation.stop_gate_evidence.to_public_dict()))
        )
        assert reloaded is not None and reloaded.exact


def test_full_finalize_to_snapshot_sets_setup_validated_true_and_declares_comparison() -> None:
    result = _production_setup_result()
    assert result.setup_validated
    assert result.downstream_validated

    snapshot = RunManager._round_five_setup_snapshot(_fake_snapshot(), result, terminal=True)
    assert snapshot.setup_validated
    assert snapshot.downstream_validated
    for lane_id in ("lakebase", "competitor"):
        lane = snapshot.lanes[lane_id]
        assert lane.verified, lane_id
        assert lane.stop_gate_evidence is not None
        assert lane.stop_gate_evidence.exact
        assert lane.setup_diagnostic is None
        # Absolute per-lane launch delay is persisted, not only inter-lane skew.
        assert lane.workflow_launch_delay_ms is not None
        assert 0.0 <= lane.workflow_launch_delay_ms <= 10.0

    comparison = RunManager._round_five_setup_comparison(result, "Aurora")
    assert comparison is not None
    assert comparison.winner_lane_id == "lakebase"


def test_sanitizer_still_rejects_endpoint_host_arn_and_secret_keys() -> None:
    # The old, colliding key must still be redacted: the fix is a rename, NOT a
    # broadened filter.
    assert (
        RunManager._round_five_public_evidence_fact(
            PublicSetupEvidence("pooled_endpoint_binding_exact", True)
        )
        is None
    )
    for key in ("proxy_host", "target_endpoint", "credential_sha256", "master_secret_arn"):
        assert (
            RunManager._round_five_public_evidence_fact(PublicSetupEvidence(key, "unused")) is None
        ), key
    # A safe key whose STRING value carries an ARN is still rejected by the
    # value sanitizer.
    assert (
        RunManager._round_five_public_evidence_fact(
            PublicSetupEvidence("binding", "arn:aws:rds:us-west-2:1:db:x")
        )
        is None
    )
    # A safe key whose value carries an ARN via a dict fact is also rejected.
    assert (
        RunManager._round_five_public_evidence_fact(
            {"key": "binding", "value": "arn:aws:iam::1:role/x"}
        )
        is None
    )


def test_public_fact_is_mapping_aware_for_persisted_dicts() -> None:
    fact = RunManager._round_five_public_evidence_fact(
        {"key": "pooled_path_binding_exact", "value": True}
    )
    assert fact is not None
    assert fact.key == "pooled_path_binding_exact"
    assert fact.value is True


def test_finalize_and_snapshot_are_idempotent_under_duplicate_calls() -> None:
    result_one = _production_setup_result()
    result_two = _production_setup_result()
    assert result_one.to_public_dict() == result_two.to_public_dict()

    snapshot_one = RunManager._round_five_setup_snapshot(
        _fake_snapshot(), result_one, terminal=True
    )
    snapshot_two = RunManager._round_five_setup_snapshot(
        _fake_snapshot(), result_two, terminal=True
    )
    assert snapshot_one.model_dump(mode="json") == snapshot_two.model_dump(mode="json")


def test_genuine_stop_gate_failure_stays_unverified_with_specific_subcode() -> None:
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=T0_NS)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=T0_NS + 2_000_000,
            status=SetupLaneStatus.SUCCEEDED,
            # A genuine miss: setup reported success but produced no stop gate.
            stop_gate_evidence=None,
        ),
        _production_observation(
            "competitor", launch_delay_ns=6_000_000, elapsed_ns=650_000_000_000
        ),
    )
    fanin = {lane_id: verified_fanin_lane(lane_id) for lane_id in ("lakebase", "competitor")}
    result = finalize_setup_phase(arm, observations, fanin)

    assert not result.setup_validated
    # A stopped-short setup must not yield a comparison margin merely because
    # both burst lanes verified downstream.
    assert result.comparison is None
    assert RunManager._round_five_setup_comparison(result, "Aurora") is None

    snapshot = RunManager._round_five_setup_snapshot(_fake_snapshot(), result, terminal=True)
    assert not snapshot.setup_validated
    lakebase = snapshot.lanes["lakebase"]
    assert not lakebase.verified
    assert lakebase.setup_diagnostic is not None
    assert "stop_gate_evidence" in lakebase.setup_diagnostic
    # A real miss is NOT reported as a projection rejection.
    assert "public_fact_key_rejected" not in lakebase.setup_diagnostic


def test_projection_rejection_surfaces_distinct_durable_subcode() -> None:
    # Simulate the pre-fix fact set reaching the snapshot: the core proof is
    # exact and verified, but a key collides with the denylist. The lane must be
    # downgraded AND carry the distinct, durable ``public_fact_key_rejected``
    # code rather than the generic failure text.
    colliding = SetupStopGateEvidence(
        gate_id="lakebase_dispatch_eligibility",
        expected=(
            PublicSetupEvidence("warm_launch_capsule_current", True),
            PublicSetupEvidence("pooled_endpoint_binding_exact", True),
        ),
        observed=(
            PublicSetupEvidence("warm_launch_capsule_current", True),
            PublicSetupEvidence("pooled_endpoint_binding_exact", True),
        ),
        verified_at_ns=T0_NS + 3_600_000_000,
    )
    arm = arm_setup_phase(("lakebase", "competitor"), t0_ns=T0_NS)
    observations = (
        SetupLaneObservation(
            lane_id="lakebase",
            workflow_launched_ns=T0_NS + 2_000_000,
            status=SetupLaneStatus.SUCCEEDED,
            stop_gate_evidence=colliding,
        ),
        _production_observation(
            "competitor", launch_delay_ns=6_000_000, elapsed_ns=650_000_000_000
        ),
    )
    fanin = {lane_id: verified_fanin_lane(lane_id) for lane_id in ("lakebase", "competitor")}
    result = finalize_setup_phase(arm, observations, fanin)
    # The CORE proof still verifies; only the public projection drops it.
    assert result.lanes["lakebase"].verified

    snapshot = RunManager._round_five_setup_snapshot(_fake_snapshot(), result, terminal=True)
    lakebase = snapshot.lanes["lakebase"]
    assert not lakebase.verified
    assert lakebase.stop_gate_evidence is None
    assert lakebase.setup_diagnostic == "public_fact_key_rejected"
    assert not snapshot.setup_validated
