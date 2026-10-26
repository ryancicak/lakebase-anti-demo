"""`server.models.aws_lane`: the AWS lane key is explicit, and a third competitor is refused.

Round 4's and Round 6's lanes and Round 5's credential IDs used to read "aurora if Aurora,
else rds", which would send the Azure edition's competitor down RDS's path without a word.
"""

from __future__ import annotations

import pytest

from server.models import CompetitorId, aws_lane


def test_the_two_aws_competitors_map_as_before() -> None:
    assert aws_lane(CompetitorId.AURORA_SERVERLESS_V2) == "aurora"
    assert aws_lane(CompetitorId.RDS_POSTGRES) == "rds"
    assert aws_lane("aurora_serverless_v2") == "aurora"  # Round 5 passes the plain string


def test_any_other_competitor_is_a_named_refusal_not_rds() -> None:
    with pytest.raises(ValueError, match="has no AWS lane"):
        aws_lane("azure_postgres")


def test_round_fives_warm_variant_is_the_aws_lane_and_refuses_any_other() -> None:
    # manager.py picks the warm variant this way at its four Round 5 sites.
    from server.round5_warm import Round5Variant

    assert Round5Variant(aws_lane(CompetitorId.AURORA_SERVERLESS_V2)) is Round5Variant.AURORA
    assert Round5Variant(aws_lane(CompetitorId.RDS_POSTGRES)) is Round5Variant.RDS
    with pytest.raises(ValueError, match="has no AWS lane"):
        Round5Variant(aws_lane("azure_postgres"))


def test_aws_pricing_and_capacity_refuse_a_competitor_with_no_aws_lane() -> None:
    # These used to read "Aurora, else RDS", so a third competitor was priced as RDS in silence
    # (the Azure edition's Phase 1, "capabilities, not identities").
    from server.capacity import competitor_is_aurora
    from server.pricing import calculate_rds_proxy_cost

    for refused in (lambda: calculate_rds_proxy_cost("azure_postgres", 600.0),  # type: ignore[arg-type]
                    lambda: competitor_is_aurora("azure_postgres")):  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="has no AWS lane"):
            refused()
    assert calculate_rds_proxy_cost(CompetitorId.RDS_POSTGRES, 600.0)[0] == pytest.approx(
        2.0 * 600.0 / 3600.0)
    assert competitor_is_aurora(CompetitorId.AURORA_SERVERLESS_V2)
    assert not competitor_is_aurora(CompetitorId.RDS_POSTGRES)
