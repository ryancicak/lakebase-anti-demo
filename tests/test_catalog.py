import pytest
from pydantic import ValidationError

from server.catalog import (
    build_presenter_pack,
    catalog,
    persona_by_id,
    recommend_round,
    round_by_id,
)
from server.models import (
    Availability,
    ComparisonKind,
    CompetitorId,
    Corner,
    RoundId,
    SessionCreate,
)


def test_catalog_keeps_scope_to_two_aws_competitors() -> None:
    result = catalog()

    assert [item.id for item in result.competitors] == [
        CompetitorId.AURORA_SERVERLESS_V2,
        CompetitorId.RDS_POSTGRES,
    ]
    assert len(result.personas) == 10
    assert set(result.corners) == set(Corner)


def test_round_four_catalog_availability_copy_and_metadata_are_exact() -> None:
    planned = round_by_id(RoundId.PUT_MODEL_SCORE_IN_APP)
    ready = round_by_id(RoundId.PUT_MODEL_SCORE_IN_APP, model_score_available=True)

    assert planned.availability == Availability.PLANNED
    assert ready.availability == Availability.READY
    assert planned.availability == Availability.PLANNED
    assert ready.title == "Move lakehouse data into live applications"
    assert ready.capability == (
        "Managed reverse ETL from Unity Catalog Delta to operational Lakebase Postgres"
    )
    assert ready.scorecard_by_corner[Corner.PERFORMANCE] == (
        "Reverse ETL sync and end-to-end proof time"
    )
    assert ready.comparison_kind == ComparisonKind.CAPABILITY_GAP
    assert [metric.id for metric in ready.metric_specs] == [
        "managed_availability_ms",
        "application_proof_elapsed_ms",
        "delta_commit_version",
        "exact_row_verified",
    ]
    assert ready.redo is not None
    assert ready.redo.policy == "show"
    assert ready.redo.badge == "★ SHOW"
    assert ready.redo.label == "CHANGE SCORE IN LAKEHOUSE → WATCH APP UPDATE"
    assert "from v1 to v2" in ready.redo.description
    assert "live app record update" in ready.redo.description
    assert ready.non_claims == [
        (
            "RDS/Aurora are destination databases only; the same outcome requires a "
            "separate reverse-ETL stack that must be selected or built, secured, "
            "networked, configured, monitored, and operated."
        ),
        "The AWS lane was not executed or timed.",
        "No cross-platform speed comparison or margin is claimed.",
        "No dollar savings are claimed.",
        "No eliminated system is claimed.",
        "No full model-serving capability is claimed.",
    ]
    assert next(
        item for item in catalog().rounds if item.id == RoundId.PUT_MODEL_SCORE_IN_APP
    ).availability == Availability.PLANNED
    assert next(
        item
        for item in catalog(model_score_available=True).rounds
        if item.id == RoundId.PUT_MODEL_SCORE_IN_APP
    ).availability == Availability.READY


def test_round_five_is_named_for_the_outcome_it_scores() -> None:
    """The scorecard renders entry.round_title straight from the server.

    frontend/src/round5.ts overrides the server title at every other render
    site, so a stale value here reaches the audience on exactly one screen.
    """
    assert round_by_id(RoundId.SURVIVE_CONNECTION_SPIKE).title == "Get spike-ready"
    assert (
        round_by_id(
            RoundId.SURVIVE_CONNECTION_SPIKE,
            connection_spike_available=True,
        ).title
        == "Get spike-ready"
    )


def test_round_five_catalog_supports_both_configured_rds_proxy_matchups() -> None:
    planned = round_by_id(RoundId.SURVIVE_CONNECTION_SPIKE)
    ready = round_by_id(
        RoundId.SURVIVE_CONNECTION_SPIKE,
        connection_spike_available=True,
    )

    assert planned.availability == Availability.PLANNED
    assert ready.availability == Availability.READY
    assert ready.competitors == [
        CompetitorId.RDS_POSTGRES,
        CompetitorId.AURORA_SERVERLESS_V2,
    ]
    assert ready.comparison_kind == ComparisonKind.MEASURED
    assert [metric.id for metric in ready.metric_specs] == [
        "setup_elapsed_ms",
        "successful_clients",
        "application_p99_ms",
        "error_clients",
    ]
    assert ready.metric_specs[0].role.value == "primary"
    assert ready.metric_specs[1].role.value == "secondary"
    assert "0 separate per-bout pooling components" in ready.non_claims[0]
    assert "0 per-bout pooling infrastructure mutations" in ready.non_claims[0]
    assert "native-login, ordinary-role, and runner-credential preparation" in ready.non_claims[0]
    assert "9 journaled competitor mutations" in ready.non_claims[1]
    assert "1 per-bout Proxy security group" in ready.non_claims[1]
    assert "1 default-egress change" in ready.non_claims[1]
    assert "4 exact security-group rules" in ready.non_claims[1]
    assert "1 RDS Proxy" in ready.non_claims[1]
    assert "1 target-group configuration" in ready.non_claims[1]
    assert "1 target registration" in ready.non_claims[1]
    assert "exact application transaction" in ready.non_claims[1]
    assert "sealed install-time prerequisites outside the setup clock" in ready.non_claims[2]
    assert "IAM service role, runner permission" in ready.non_claims[2]
    assert "added RDS Proxy, Secrets Manager, IAM, and network configuration" in ready.non_claims[2]
    assert "RDS Proxy and Secrets Manager remain billable" in ready.non_claims[2]
    assert ready.scorecard_by_corner[Corner.COST] == (
        "Published rates include the AWS opponent's added RDS Proxy minimum"
    )
    assert ready.scorecard_by_corner[Corner.SIMPLICITY] == (
        "Lakebase adds 0 per-bout pooling infrastructure mutations; the selected "
        "AWS opponent performs 9 journaled competitor mutations"
    )
    assert all("aurora" not in claim.lower() for claim in ready.non_claims)
    assert "never added" in ready.non_claims[4]
    assert ready.redo is None
    presenter = build_presenter_pack(
        persona_by_id("sre"),
        [],
        ready,
        [Corner.PERFORMANCE],
        CompetitorId.RDS_POSTGRES,
    )
    assert "Primary setup elapsed" in presenter.remembered_metric
    assert "setup winner or margin" in presenter.stop_condition


def test_redo_presentation_policy_is_locked_by_round() -> None:
    round_one = round_by_id(RoundId.WAKE_IDLE_APP)
    round_two = round_by_id(RoundId.MAKE_SCHEMA_CHANGE_SAFELY)
    round_three = round_by_id(RoundId.RECOVER_DELETED_ORDER)
    round_four = round_by_id(RoundId.PUT_MODEL_SCORE_IN_APP, model_score_available=True)
    round_five = round_by_id(RoundId.SURVIVE_CONNECTION_SPIKE)
    round_six = round_by_id(RoundId.ANALYZE_LIVE_ORDERS)

    assert round_one.redo is not None
    assert (round_one.redo.policy, round_one.redo.badge, round_one.redo.label) == (
        "show", "★ SHOW", "RE-DO ROUND"
    )
    assert round_two.redo is not None
    assert (round_two.redo.policy, round_two.redo.badge, round_two.redo.label) == (
        "optional", "OPTIONAL", "RE-DO ROUND"
    )
    assert round_three.redo is not None
    assert round_three.redo.policy == "skip"
    assert "retain" in round_three.redo.description
    assert round_four.redo is not None
    assert (round_four.redo.policy, round_four.redo.badge, round_four.redo.label) == (
        "show", "★ SHOW", "CHANGE SCORE IN LAKEHOUSE → WATCH APP UPDATE"
    )
    assert round_five.redo is None
    assert round_six.redo is None


def test_round_four_recommendation_requires_live_availability() -> None:
    persona = persona_by_id("data_engineer")

    planned, _ = recommend_round(CompetitorId.RDS_POSTGRES, persona)
    ready, _ = recommend_round(
        CompetitorId.RDS_POSTGRES,
        persona,
        model_score_available=True,
    )

    assert planned.id != RoundId.PUT_MODEL_SCORE_IN_APP
    assert ready.id == RoundId.PUT_MODEL_SCORE_IN_APP


def test_round_four_presenter_copy_has_one_clock_and_exact_stop_boundary() -> None:
    presenter = build_presenter_pack(
        persona_by_id("data_engineer"),
        [],
        round_by_id(RoundId.PUT_MODEL_SCORE_IN_APP, model_score_available=True),
        [Corner.COST, Corner.PERFORMANCE],
        CompetitorId.RDS_POSTGRES,
    )

    assert presenter.stop_condition == (
        "The clock ends only after the exact committed Delta version is observed synced and "
        "a fresh Postgres read returns the exact row."
    )
    assert "both" not in presenter.stop_condition.lower()
    assert "same outcome" not in presenter.remembered_metric.lower()


def test_aurora_recommendation_uses_ready_persona_round_when_possible() -> None:
    persona = persona_by_id("sre")

    selected, reason = recommend_round(CompetitorId.AURORA_SERVERLESS_V2, persona)

    assert selected.id == RoundId.WAKE_IDLE_APP
    assert "SRE" in reason


def test_rds_recommendation_uses_the_ready_safe_change_round_for_developers() -> None:
    persona = persona_by_id("software_engineer")

    selected, reason = recommend_round(CompetitorId.RDS_POSTGRES, persona)

    assert selected.id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY
    assert CompetitorId.RDS_POSTGRES in selected.competitors
    assert "Software Engineer" in reason


def test_one_primary_and_unique_secondaries_are_enforced() -> None:
    with pytest.raises(ValidationError):
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            secondary_personas=["sre"],
            corners=[Corner.PERFORMANCE],
        )

    with pytest.raises(ValidationError):
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            secondary_personas=["executive", "executive"],
            corners=[Corner.PERFORMANCE],
        )


def test_one_to_three_unique_customer_priorities_are_enforced() -> None:
    with pytest.raises(ValidationError):
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[],
        )

    with pytest.raises(ValidationError):
        SessionCreate(
            competitor=CompetitorId.AURORA_SERVERLESS_V2,
            primary_persona="sre",
            corners=[Corner.COST, Corner.COST],
        )


def test_presenter_pack_uses_truthful_multi_priority_and_round_stop_copy() -> None:
    presenter = build_presenter_pack(
        persona_by_id("software_engineer"),
        [],
        round_by_id(RoundId.MAKE_SCHEMA_CHANGE_SAFELY),
        [Corner.COST, Corner.SIMPLICITY, Corner.PERFORMANCE],
        CompetitorId.AURORA_SERVERLESS_V2,
    )

    assert presenter.remembered_metric == (
        "Cost inputs, workflow simplicity, and elapsed workflow time to the same verified outcome"
    )
    assert "spend" not in presenter.remembered_metric.lower()
    assert presenter.stop_condition == (
        "Each clock stops after the identical migration and transaction verify and the final "
        "source check passes."
    )
