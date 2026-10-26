"""The Azure edition's catalog (design section 12): its competitor, its rounds and its words.

An installation offers its own cloud's competitors only, and the AWS edition's catalog doesn't
change: every AWS default below is compared with the catalog as it was.
"""

from __future__ import annotations

import re

import pytest

from server.catalog import (
    AZURE_ROUNDS,
    COMPETITORS,
    ROUNDS,
    build_presenter_pack,
    catalog,
    competitor_by_id,
    load_personas,
    persona_by_id,
    recommend_round,
    round_by_id,
)
from server.models import CompetitorId, Corner, RoundId, competitor_cloud

#: Words that name AWS's products. None may reach an Azure room.
AWS_WORDS = re.compile(r"\bAWS\b|Aurora|\bRDS\b|Glue|\bDMS\b|\bS3\b|\bIAM\b|c7i")


def _words(model) -> str:
    return model.model_dump_json()


def test_round_five_says_it_is_not_raced_on_azure_yet_rather_than_unavailable() -> None:
    # The card read "This round is unavailable right now", which reads as an outage (2026-10-19).
    planned = round_by_id(RoundId.SURVIVE_CONNECTION_SPIKE, cloud="azure")
    assert planned.availability_headline == "Not raced on the Azure edition yet"
    # Once it races, the planned round's words go; the AWS edition's never had any.
    raced = round_by_id(RoundId.SURVIVE_CONNECTION_SPIKE, connection_spike_available=True,
                        cloud="azure")
    assert raced.availability_headline is None
    assert round_by_id(RoundId.SURVIVE_CONNECTION_SPIKE).availability_headline is None


def test_an_azure_installation_offers_azure_postgres_alone() -> None:
    offered = catalog(cloud="azure")
    assert [item.id for item in offered.competitors] == [CompetitorId.AZURE_POSTGRES]
    assert competitor_by_id(CompetitorId.AZURE_POSTGRES).short_name == "Azure PostgreSQL"
    assert [item.id for item in offered.rounds] == [item.id for item in ROUNDS]
    for item in offered.rounds:
        assert item.competitors == [CompetitorId.AZURE_POSTGRES]


def test_an_aws_installation_offers_exactly_what_it_did() -> None:
    assert catalog() == catalog(cloud="aws")
    assert catalog().competitors == COMPETITORS
    for item in ROUNDS:
        assert CompetitorId.AZURE_POSTGRES not in item.competitors
    assert catalog().personas == list(load_personas())


@pytest.mark.parametrize("item", AZURE_ROUNDS, ids=lambda item: item.id.value)
def test_no_azure_round_names_an_aws_product(item) -> None:
    assert AWS_WORDS.search(_words(item)) is None, AWS_WORDS.findall(_words(item))


def test_the_azure_rounds_keep_the_aws_editions_clocks_and_verifiers() -> None:
    for azure, aws in zip(AZURE_ROUNDS, ROUNDS, strict=True):
        assert (azure.id, azure.title, azure.metric_specs, azure.redo, azure.comparison_kind) == (
            aws.id,
            aws.title,
            aws.metric_specs,
            aws.redo,
            aws.comparison_kind,
        )


def test_the_azure_personas_name_no_aws_product() -> None:
    for persona in catalog(cloud="azure").personas:
        assert AWS_WORDS.search(_words(persona)) is None, persona.id
    assert persona_by_id("software_engineer", "azure").presenter.objection.startswith(
        "Azure Database for PostgreSQL"
    )
    assert "RDS and Aurora" in persona_by_id("software_engineer").presenter.objection


def test_round_one_times_lakebase_alone_against_azure() -> None:
    primary = next(iter(load_personas()))
    selected, reason = recommend_round(CompetitorId.AZURE_POSTGRES, primary)
    assert selected.competitors == [CompetitorId.AZURE_POSTGRES]
    if selected.id == RoundId.WAKE_IDLE_APP:
        assert reason.startswith("Azure Database for PostgreSQL has no automatic scale-to-zero")
    pack = build_presenter_pack(
        primary,
        [],
        round_by_id(RoundId.WAKE_IDLE_APP, cloud="azure"),
        [Corner.PERFORMANCE],
        CompetitorId.AZURE_POSTGRES,
    )
    assert pack.stop_condition == (
        "Lakebase stops after commit + read-back; Azure PostgreSQL eligibility is checked "
        "before the bell and not timed."
    )


@pytest.mark.parametrize(
    "round_id", [RoundId.SURVIVE_CONNECTION_SPIKE, RoundId.ANALYZE_LIVE_ORDERS]
)
def test_the_azure_presenter_pack_names_azures_integrations(round_id: RoundId) -> None:
    primary = next(iter(load_personas()))
    available = {"connection_spike_available": True, "live_orders_available": True}
    azure = build_presenter_pack(
        primary,
        [],
        round_by_id(round_id, cloud="azure", **available),
        [Corner.PERFORMANCE, Corner.COST],
        CompetitorId.AZURE_POSTGRES,
    )
    aws = build_presenter_pack(
        primary,
        [],
        round_by_id(round_id, **available),
        [Corner.PERFORMANCE, Corner.COST],
        CompetitorId.RDS_POSTGRES,
    )
    assert AWS_WORDS.search(azure.stop_condition + azure.remembered_metric) is None
    assert AWS_WORDS.search(aws.stop_condition + aws.remembered_metric) is not None


def test_a_competitor_belongs_to_one_cloud() -> None:
    assert competitor_cloud(CompetitorId.AZURE_POSTGRES) == "azure"
    assert competitor_cloud(CompetitorId.RDS_POSTGRES) == "aws"
    assert competitor_cloud(CompetitorId.AURORA_SERVERLESS_V2) == "aws"
    with pytest.raises(ValueError, match="unknown competitor"):
        competitor_cloud("cockroach")
