"""An AWS lane's /24 comes from what the VPC actually holds, not from a digest alone.

The v1.1 test installation of 2026-09-29 failed its Round 4 lane apply because the digest chose
172.31.122.0/24 in a shared default VPC that also holds a foreign 172.31.112.0/20. These pin the
replacement: the digest's /24 when it is free, the next free one when it is not, and the same
answer as Terraform's digest wherever nothing is in the way.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest

from server.lane_subnets import (
    NoFreeLaneSubnet,
    free_lane_cidr,
    round4_preferred_netnum,
    round6_preferred_netnum,
    vpc_subnet_cidrs,
)

DEFAULT_SUBNETS = ["172.31.0.0/20", "172.31.16.0/20", "172.31.32.0/20", "172.31.48.0/20"]
INSTALLATION = "11111111-1111-4111-8111-111111111111"
REPO = Path(__file__).resolve().parent.parent


def test_the_digests_24_is_chosen_when_it_is_free():
    assert free_lane_cidr("172.31.0.0/16", DEFAULT_SUBNETS, preferred_netnum=122) == (
        "172.31.122.0/24"
    )


def test_the_incident_a_foreign_20_over_the_digests_24_is_stepped_around():
    taken = [*DEFAULT_SUBNETS, "172.31.112.0/20", "172.31.128.0/24"]
    assert free_lane_cidr("172.31.0.0/16", taken, preferred_netnum=122) == "172.31.129.0/24"


def test_the_search_wraps_around_the_lane_band():
    taken = [*DEFAULT_SUBNETS, "172.31.192.0/18"]
    assert free_lane_cidr("172.31.0.0/16", taken, preferred_netnum=254) == "172.31.64.0/24"


def test_a_24_to_avoid_is_never_chosen():
    assert free_lane_cidr(
        "172.31.0.0/16", DEFAULT_SUBNETS, preferred_netnum=70, avoid=("172.31.70.0/24",)
    ) == ("172.31.71.0/24")


def test_a_full_band_names_the_failure():
    with pytest.raises(NoFreeLaneSubnet):
        free_lane_cidr("172.31.0.0/16", ["172.31.0.0/16"], preferred_netnum=100)


def test_a_small_explicit_vpc_uses_any_free_24_of_its_own():
    assert free_lane_cidr("10.0.0.0/22", ["10.0.0.0/24"], preferred_netnum=100) == "10.0.1.0/24"


def test_the_preferred_24s_are_terraforms_digests():
    round4 = (REPO / "infra" / "aws" / "round4_glue.tf").read_text(encoding="utf-8")
    round6 = (REPO / "infra" / "aws" / "round6_aws.tf").read_text(encoding="utf-8")
    assert (
        '64 + parseint(substr(sha256("${trimspace(var.installation_id)}:r4-glue"), 0, 2), 16) % 191'
        in (round4)
    )
    assert re.search(r"local\.round4_glue_subnet_netnum - 64 \+ 1", round6)
    assert 'sha256("${trimspace(var.installation_id)}:r6-dms"), 0, 2), 16) % 190' in round6
    digest = int(hashlib.sha256(f"{INSTALLATION}:r4-glue".encode()).hexdigest()[:2], 16)
    assert round4_preferred_netnum(INSTALLATION) == 64 + digest % 191
    assert round6_preferred_netnum(INSTALLATION) != round4_preferred_netnum(INSTALLATION)


def test_round_six_never_prefers_round_fours_24():
    for index in range(300):
        installation = f"{index:08d}-1111-4111-8111-111111111111"
        assert round6_preferred_netnum(installation) != round4_preferred_netnum(installation)


def test_every_subnet_in_the_vpc_counts_whoever_made_it():
    class Ec2:
        def get_paginator(self, operation):
            assert operation == "describe_subnets"

            class Pages:
                def paginate(self, Filters):
                    assert Filters == [{"Name": "vpc-id", "Values": ["vpc-1"]}]
                    return iter(
                        [
                            {"Subnets": [{"CidrBlock": "172.31.0.0/20"}]},
                            {"Subnets": [{"CidrBlock": "172.31.112.0/20"}]},
                        ]
                    )

            return Pages()

    assert vpc_subnet_cidrs(Ec2(), "vpc-1") == ["172.31.0.0/20", "172.31.112.0/20"]


def test_the_installer_passes_its_choice_to_terraform():
    source = (REPO / "server" / "lifecycle.py").read_text(encoding="utf-8")
    assert 'values["round4_glue_subnet_cidr"]' in source
    assert 'values["round6_dms_subnet_cidr"]' in source
    # Every plan that can build a lane chooses before it: both lane stages, and the reconcile,
    # which a resume runs first (the second failure of 2026-09-29).
    for stage in (
        "_prepare_and_reseal_round4_aws",
        "_prepare_and_reseal_round6_aws",
        "reconcile_infrastructure",
    ):
        body = source.split(f"def {stage}(", 1)[1].split("\ndef ", 1)[0]
        assert body.index("_choose_enabled_lane_subnets(manifest)") < body.index(
            "_plan_and_apply(manifest"
        )


def _chooser(monkeypatch, **recorded):
    from types import SimpleNamespace

    from server import lifecycle

    manifest = SimpleNamespace(
        installation_id=INSTALLATION,
        round4_aws_source_location=None,
        round6_aws_uc_external_id=None,
        round4_glue_subnet_cidr=None,
        round6_dms_subnet_cidr=None,
    )
    for name, value in recorded.items():
        setattr(manifest, name, value)
    chosen: list[tuple[str, tuple[str, ...]]] = []

    def choose(manifest, *, field, subnet_address, preferred_netnum, label, avoid=()):
        chosen.append((field, avoid))
        if getattr(manifest, field) is None:
            setattr(manifest, field, f"10.0.{len(chosen)}.0/24")

    monkeypatch.setattr(lifecycle, "_choose_lane_subnet", choose)
    lifecycle._choose_enabled_lane_subnets(manifest)
    return chosen


def test_a_resume_chooses_for_a_lane_whose_stage_stopped(monkeypatch):
    # The incident: Round 4's source was recorded, the lane's apply failed, and the resume's
    # reconcile planned the lane before its stage ran again.
    chosen = _chooser(monkeypatch, round4_aws_source_location="s3://bucket/round4")
    assert chosen == [("round4_glue_subnet_cidr", ())]


def test_round_six_is_chosen_after_round_four_and_avoids_it(monkeypatch):
    chosen = _chooser(
        monkeypatch,
        round4_aws_source_location="s3://bucket/round4",
        round6_aws_uc_external_id="external-id",
    )
    assert chosen == [
        ("round4_glue_subnet_cidr", ()),
        ("round6_dms_subnet_cidr", ("10.0.1.0/24",)),
    ]


def test_nothing_is_chosen_before_a_lane_is_enabled(monkeypatch):
    assert _chooser(monkeypatch) == []
