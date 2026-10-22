"""The run manager on an Azure installation: it offers Azure's competitor alone, in Azure's words.

`RunManager(cloud="azure")` is what the App builds from an Azure manifest. A bout created for any
round carries Azure's capacity, cost receipt and presenter copy, and never an AWS product's name;
an AWS competitor is refused there, as Azure's is on an AWS installation.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from server import manager
from server.manager import RunManager
from server.models import CompetitorId, Corner, LaneState, RoundId, SessionCreate
from server.round4_race import COMPETITOR_LANE, LAKEBASE_LANE, Resolution, Verdict

AWS_WORDS = ("AWS", "Aurora", "RDS", "Glue", "DMS")
#: Rounds 4 and 6 need their live factories to be offered; these three need nothing installed.
OFFLINE_ROUNDS = (
    RoundId.WAKE_IDLE_APP,
    RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
    RoundId.RECOVER_DELETED_ORDER,
)


def create(manager: RunManager, competitor: CompetitorId, round_id: RoundId | None = None):
    return asyncio.run(manager.create(SessionCreate(
        competitor=competitor, primary_persona="software_engineer",
        corners=[Corner.PERFORMANCE, Corner.COST], round_id=round_id)))


@pytest.mark.parametrize("round_id", OFFLINE_ROUNDS, ids=lambda item: item.value)
def test_an_azure_bout_names_azures_products_alone(round_id: RoundId) -> None:
    snapshot = create(RunManager(cloud="azure"), CompetitorId.AZURE_POSTGRES, round_id)
    assert snapshot.competitor.id == CompetitorId.AZURE_POSTGRES
    assert snapshot.lanes["competitor"].name == "Azure PostgreSQL"
    assert snapshot.round.competitors == [CompetitorId.AZURE_POSTGRES]
    words = snapshot.model_dump_json()
    assert not [word for word in AWS_WORDS if word in words], [
        word for word in AWS_WORDS if word in words]
    assert snapshot.descent_cost is None and snapshot.bout_cost is None
    assert snapshot.standing_cost is None
    assert snapshot.cost_receipt is not None and snapshot.cost_receipt.region == "westus2"


def test_the_recommended_round_is_an_azure_one() -> None:
    snapshot = create(RunManager(cloud="azure"), CompetitorId.AZURE_POSTGRES)
    assert snapshot.round.competitors == [CompetitorId.AZURE_POSTGRES]


def test_each_installation_refuses_the_other_clouds_competitor() -> None:
    with pytest.raises(ValueError, match="does not offer this competitor"):
        create(RunManager(cloud="azure"), CompetitorId.RDS_POSTGRES,
               RoundId.MAKE_SCHEMA_CHANGE_SAFELY)
    with pytest.raises(ValueError, match="does not offer this competitor"):
        create(RunManager(), CompetitorId.AZURE_POSTGRES, RoundId.MAKE_SCHEMA_CHANGE_SAFELY)


def test_round_one_names_azure_as_the_challenger_that_cannot_enter() -> None:
    # The Azure edition's first browser bout read "LAKEBASE WINS — RDS CANNOT ENTER THE ROUND"
    # (2026-10-19). RDS's own words stay as they were (test_manager's Round 1 RDS bout).
    snapshot = create(RunManager(cloud="azure"), CompetitorId.AZURE_POSTGRES,
                      RoundId.WAKE_IDLE_APP)
    snapshot.lanes["lakebase"].state = LaneState.VERIFIED
    snapshot.lanes["competitor"].state = LaneState.NOT_SUPPORTED
    assert RunManager._remembered_result(snapshot) == (
        "LAKEBASE WINS — AZURE POSTGRESQL CANNOT ENTER THE ROUND")


def race(verdict: Verdict, *, competitor: bool = True) -> SimpleNamespace:
    """A Round 4 or 6 engine result, as `_round_four/six_comparison` read one."""

    lanes = {LAKEBASE_LANE: SimpleNamespace(lane_id=LAKEBASE_LANE, verified=True,
                                            elapsed_ms=19_000.0)}
    if competitor:
        stopped = verdict is Verdict.LOWER_BOUND
        lanes[COMPETITOR_LANE] = SimpleNamespace(
            lane_id=COMPETITOR_LANE, verified=not stopped,
            elapsed_ms=None if stopped else 277_000.0)
    winner = None if verdict is Verdict.WITHIN_RESOLUTION else LAKEBASE_LANE
    margin = 258_000.0 if verdict is Verdict.WIN else None
    return SimpleNamespace(resolution=Resolution(verdict, winner=winner, margin_ms=margin),
                           outcomes=lanes)


RESULTS = {
    "win": race(Verdict.WIN),
    "tie": race(Verdict.WITHIN_RESOLUTION),
    "stoppage": race(Verdict.LOWER_BOUND),
    "lane not installed": race(Verdict.WIN, competitor=False),
}


@pytest.mark.parametrize("compare", ["_round_four_comparison", "_round_six_comparison"])
@pytest.mark.parametrize("shape", list(RESULTS))
def test_rounds_four_and_six_verdicts_name_azures_products_alone(compare: str, shape: str) -> None:
    # Round 6's verdicts closed on "AWS DMS and Glue start cold at the bell", and a missing lane
    # was "Round N's AWS lane", on the Azure edition's first bouts (2026-10-19).
    result = RESULTS[shape]
    comparison, _ = getattr(manager, compare)(
        result, "Azure PostgreSQL", has_competitor=COMPETITOR_LANE in result.outcomes,
        competitor=CompetitorId.AZURE_POSTGRES)
    assert not [word for word in AWS_WORDS if word in comparison.detail], comparison.detail
    if compare == "_round_six_comparison" and shape != "lane not installed":
        assert "Azure Data Factory starts cold at the bell" in comparison.detail


@pytest.mark.parametrize("compare", ["_round_four_comparison", "_round_six_comparison"])
@pytest.mark.parametrize("shape", list(RESULTS))
def test_rounds_four_and_six_verdicts_on_aws_are_as_they_were(compare: str, shape: str) -> None:
    result = RESULTS[shape]
    function = getattr(manager, compare)
    has = COMPETITOR_LANE in result.outcomes
    before, _ = function(result, "RDS PostgreSQL", has_competitor=has)
    after, _ = function(result, "RDS PostgreSQL", has_competitor=has,
                        competitor=CompetitorId.RDS_POSTGRES)
    assert after == before


def test_an_aws_bout_is_exactly_as_it_was() -> None:
    snapshot = create(RunManager(), CompetitorId.RDS_POSTGRES, RoundId.WAKE_IDLE_APP)
    assert snapshot.lanes["competitor"].name == "RDS PostgreSQL"
    assert snapshot.descent_cost is not None and snapshot.bout_cost is not None
    assert snapshot.cost_receipt is not None and snapshot.cost_receipt.region == "us-west-2"
