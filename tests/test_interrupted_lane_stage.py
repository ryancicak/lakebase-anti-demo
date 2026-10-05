"""An install that stopped in an AWS lane stage can be finished, or removed, by what it says.

2026-10-04. rc16's fresh install lost the network in Round 6's lane plan, after the stage had
recorded the lane and before Terraform had built any of it. Its failure said "Stop the spend:
./antidemo cleanup --yes", and the cleanup refused: "Cleanup cannot safely inventory an
incomplete AWS apply; run ./antidemo resume first". The resume said "READY ... interrupted
provision recovered safely" without touching the lane, and the cleanup refused again. Only
`./bootstrap.sh --apply` could finish it, and nothing said so.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from test_lifecycle import make_manifest

from server import cli, lifecycle

INSTALLATION = "018f6f50-7d3a-7cc1-9d5d-4d9ac8d107a1"


def _recorded_lane(**updates):
    """A v7 installation whose Round 6 lane stage recorded the lane's external ID, then stopped."""

    return make_manifest().model_copy(
        update={
            "installation_id": INSTALLATION,
            "manifest_version": 7,
            "round6_aws_uc_external_id": "external-id",
            **updates,
        }
    )


def test_a_lane_stage_that_stopped_before_its_apply_can_be_taken_apart() -> None:
    manifest = _recorded_lane()
    expected = lifecycle._expected_aws_state_addresses(manifest)
    lane = lifecycle._ROUND6_AWS_STATE_ADDRESSES
    assert lane <= expected
    rc16 = expected - lane

    assert not lifecycle._aws_state_is_complete(manifest, rc16)
    assert lifecycle._aws_state_is_complete(manifest, rc16, allow_unfinished_lanes=True)


def test_so_can_one_that_stopped_partway_through_its_apply() -> None:
    manifest = _recorded_lane()
    expected = lifecycle._expected_aws_state_addresses(manifest)
    built = {"aws_s3_bucket.round6_aws[0]", "aws_iam_role.round6_uc[0]"}
    partway = (expected - lifecycle._ROUND6_AWS_STATE_ADDRESSES) | built

    assert lifecycle._aws_state_is_complete(manifest, partway, allow_unfinished_lanes=True)


def test_a_round_missing_outside_the_lane_is_still_an_incomplete_apply() -> None:
    manifest = _recorded_lane()
    expected = lifecycle._expected_aws_state_addresses(manifest)
    rest = sorted(expected - lifecycle._ROUND6_AWS_STATE_ADDRESSES)
    missing = expected - lifecycle._ROUND6_AWS_STATE_ADDRESSES - {rest[0]}

    assert not lifecycle._aws_state_is_complete(manifest, missing, allow_unfinished_lanes=True)


def test_an_address_nobody_expects_is_still_refused() -> None:
    manifest = _recorded_lane()
    expected = lifecycle._expected_aws_state_addresses(manifest)
    stray = (expected - lifecycle._ROUND6_AWS_STATE_ADDRESSES) | {"aws_instance.someone_else"}

    assert not lifecycle._aws_state_is_complete(manifest, stray, allow_unfinished_lanes=True)


def test_a_sealed_lane_missing_resources_is_drift_not_an_unfinished_stage() -> None:
    manifest = _recorded_lane(round6_aws=SimpleNamespace())
    expected = lifecycle._expected_aws_state_addresses(manifest)
    partway = expected - {"aws_dms_replication_instance.round6[0]"}

    assert not lifecycle._aws_state_is_complete(manifest, partway, allow_unfinished_lanes=True)


class _PastTheGate(Exception):
    """Cleanup's first step once its inventory has accepted the Terraform state."""


def _cleanup_inventory(monkeypatch, manifest, addresses: set[str]) -> None:
    monkeypatch.setattr(lifecycle, "load_manifest", lambda: manifest)
    monkeypatch.setattr(
        lifecycle, "_verify_databricks_identity", lambda _profile: manifest.databricks.user
    )
    monkeypatch.setattr(lifecycle, "_verify_aws_identity", lambda *_arguments: None)
    monkeypatch.setattr(lifecycle, "_terraform_init", lambda _manifest: None)
    monkeypatch.setattr(lifecycle, "_terraform_managed_addresses", lambda _manifest: addresses)
    monkeypatch.setattr(lifecycle, "reconcile_live", lambda _manifest, _factory: None)

    def past_the_gate(_manifest, *_arguments, **_options) -> None:
        raise _PastTheGate

    monkeypatch.setattr(lifecycle, "_hydrate_aws_resources", past_the_gate)


@pytest.mark.parametrize("dry_run", [True, False])
def test_cleanup_takes_rc16s_install_past_its_inventory(monkeypatch, dry_run: bool) -> None:
    manifest = _recorded_lane()
    expected = lifecycle._expected_aws_state_addresses(manifest)
    _cleanup_inventory(monkeypatch, manifest, expected - lifecycle._ROUND6_AWS_STATE_ADDRESSES)

    with pytest.raises(_PastTheGate):
        lifecycle.cleanup(dry_run=dry_run)


def test_cleanup_still_refuses_an_apply_that_stopped_outside_a_lane(monkeypatch) -> None:
    manifest = _recorded_lane()
    expected = lifecycle._expected_aws_state_addresses(manifest)
    rest = sorted(expected - lifecycle._ROUND6_AWS_STATE_ADDRESSES)
    _cleanup_inventory(
        monkeypatch, manifest, expected - lifecycle._ROUND6_AWS_STATE_ADDRESSES - {rest[0]}
    )

    with pytest.raises(RuntimeError, match="cannot safely inventory an incomplete AWS apply"):
        lifecycle.cleanup(dry_run=True)


def _resume(monkeypatch, manifest) -> list[str]:
    calls: list[str] = []

    def record(name: str):
        def step(candidate, *_arguments, **_options):
            calls.append(name)
            return candidate

        return step

    monkeypatch.setattr(lifecycle, "load_manifest", lambda: manifest)
    monkeypatch.setattr(lifecycle, "reconcile_infrastructure", record("reconcile"))
    monkeypatch.setattr(
        lifecycle, "resume_provision", lambda _timeout: calls.append("resume") or manifest
    )
    monkeypatch.setattr(lifecycle, "_prepare_and_reseal_round4_aws", record("round 4 lane"))
    monkeypatch.setattr(lifecycle, "_prepare_and_reseal_round6_aws", record("round 6 lane"))
    assert lifecycle.resume(321) is manifest
    return calls


def test_resume_finishes_a_stopped_lane_stage_as_setup_does(monkeypatch) -> None:
    stopped = SimpleNamespace(
        status="ready", round6_ready=True, round4_aws_pending=False, round6_aws_pending=True
    )

    # The reconcile first: each lane's own apply refuses a plan that finishes the other one.
    assert _resume(monkeypatch, stopped) == ["reconcile", "round 4 lane", "round 6 lane"]


def test_resume_of_an_earlier_stop_seals_both_lanes_after_it(monkeypatch) -> None:
    seeding = SimpleNamespace(
        status="seeding", round6_ready=False, round4_aws_pending=False, round6_aws_pending=False
    )

    assert _resume(monkeypatch, seeding) == ["resume", "round 4 lane", "round 6 lane"]


def test_setup_and_resume_read_a_stopped_lane_stage_the_same_way() -> None:
    stopped = SimpleNamespace(
        status="ready", round6_ready=True, round4_aws_pending=True, round6_aws_pending=False
    )
    finished = SimpleNamespace(
        status="ready", round6_ready=True, round4_aws_pending=False, round6_aws_pending=False
    )

    assert lifecycle._aws_lane_stage_stopped(stopped)
    assert not lifecycle._aws_lane_stage_stopped(finished)


def test_antidemo_resume_runs_the_whole_resume() -> None:
    assert cli.resume is lifecycle.resume
