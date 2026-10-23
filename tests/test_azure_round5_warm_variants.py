"""server/round5_warm.py: an installation warms AWS's two competitor variants or Azure's one.

The warm coordinator's receipts used to require Aurora and RDS exactly. The Azure edition has one
competitor, so they now take exactly one of two sets, AWS's or Azure's, and never a mix; AWS's two
are unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from server.round5_warm import (
    AWS_ROUND5_VARIANTS,
    AZURE_ROUND5_VARIANTS,
    Round5LaunchCapsule,
    Round5RunnerReceipt,
    Round5SharedReceipt,
    Round5Variant,
    Round5VariantReceipt,
    Round5WarmPreparation,
)

NOW = datetime(2026, 10, 20, 23, 30, tzinfo=UTC)
DIGEST = "a" * 64


def runner(lane: str, instance_id: str) -> Round5RunnerReceipt:
    return Round5RunnerReceipt(
        lane_id=lane, instance_id=instance_id, boot_id=f"boot-{lane}",
        process_boot_id=f"process-{lane}", process_pid=101, instance_type="c7i.2xlarge",
        image_sha256=DIGEST, loaded_harness_sha256=DIGEST, capacity_model_sha256=DIGEST,
        expires_at=NOW + timedelta(minutes=45))


def variant(value: Round5Variant) -> Round5VariantReceipt:
    return Round5VariantReceipt(
        variant=value, target_sha256=DIGEST, source_sha256=DIGEST, secret_ref_sha256=DIGEST,
        role_sha256=DIGEST, auth_sha256=DIGEST, tls_sha256=DIGEST,
        security_group_sha256=DIGEST, subnet_sha256=DIGEST, vpc_sha256=DIGEST,
        proxy_absent=True, proxy_absence_observed_at=NOW, request_template_sha256=DIGEST,
        expires_at=NOW + timedelta(minutes=45))


def capsule(control: set[Round5Variant], launch: set[Round5Variant]) -> Round5LaunchCapsule:
    return Round5LaunchCapsule(
        generation=1, coordinator_fence=1, credential_generation=1, broker_epoch="broker-1",
        runner_contexts={"lakebase": object(), "competitor": object()},
        aws_control_contexts={value: object() for value in control}, lakebase_context=object(),
        variant_contexts={value: object() for value in launch},
        control_expires_at=NOW + timedelta(hours=1),
        dispatch_expires_at={"lakebase": NOW + timedelta(hours=1),
                             "competitor": NOW + timedelta(hours=1)},
        expires_at=NOW + timedelta(minutes=50), renew_by=NOW + timedelta(minutes=30),
        warm_attempt_token="attempt-1")


def preparation(values: set[Round5Variant], runners: tuple[str, str]) -> Round5WarmPreparation:
    shared = Round5SharedReceipt(
        source_sha256=DIGEST, config_sha256=DIGEST, runner_image_sha256=DIGEST,
        fanin_contract_sha256=DIGEST, capacity_model_sha256=DIGEST,
        lakebase_binding_sha256=DIGEST, static_network_fixture_sha256=DIGEST,
        lakebase_runner=runner("lakebase", runners[0]),
        competitor_runner=runner("competitor", runners[1]))
    return Round5WarmPreparation(shared_receipt=shared,
                                 variants={value: variant(value) for value in values},
                                 capsule=capsule(set(values), set(values)))


AZURE_RUNNERS = ("vm-itest0000000000000000-r5-runner-0", "vm-itest0000000000000000-r5-runner-1")
AWS_RUNNERS = ("i-0123456789abcdef0", "i-0fedcba9876543210")


def test_azure_warms_its_one_variant_and_aws_its_two_unchanged() -> None:
    assert set(preparation(set(AZURE_ROUND5_VARIANTS), AZURE_RUNNERS).variants) == {
        Round5Variant.AZURE}
    assert set(preparation(set(AWS_ROUND5_VARIANTS), AWS_RUNNERS).variants) == {
        Round5Variant.AURORA, Round5Variant.RDS}


@pytest.mark.parametrize("values", [
    {Round5Variant.AURORA, Round5Variant.AZURE},
    {Round5Variant.AURORA, Round5Variant.RDS, Round5Variant.AZURE},
    {Round5Variant.RDS},
])
def test_a_mix_or_a_half_set_is_refused(values: set[Round5Variant]) -> None:
    with pytest.raises(ValueError, match="variant"):
        preparation(values, AWS_RUNNERS)


def test_a_capsule_launches_exactly_the_variants_it_controls() -> None:
    with pytest.raises(ValueError, match="variant launch contexts"):
        capsule(set(AZURE_ROUND5_VARIANTS), set(AWS_ROUND5_VARIANTS))


def test_runner_receipts_take_an_ec2_instance_or_an_azure_runner_vm_and_nothing_else() -> None:
    assert runner("lakebase", AZURE_RUNNERS[0]).instance_id == AZURE_RUNNERS[0]
    assert runner("lakebase", AWS_RUNNERS[0]).instance_id == AWS_RUNNERS[0]
    for bad in ("vm-r5-runner", "server-1", "i-xyz", "vm-test-r5-runner-10"):
        with pytest.raises(ValueError, match="instance_id"):
            runner("lakebase", bad)
