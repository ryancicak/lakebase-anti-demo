from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from .models import (
    Availability,
    CatalogResponse,
    ComparisonKind,
    Competitor,
    CompetitorId,
    Corner,
    MetricDirection,
    MetricRole,
    MetricSpec,
    MetricUnit,
    Persona,
    PresenterLens,
    PresenterPack,
    RedoPresentation,
    RoundDefinition,
    RoundId,
)

ROOT = Path(__file__).resolve().parents[1]


COMPETITORS = [
    Competitor(
        id=CompetitorId.AURORA_SERVERLESS_V2,
        name="Amazon Aurora PostgreSQL Serverless v2",
        short_name="Aurora Serverless v2",
        edition="AURORA SERVERLESS v2 EDITION",
    ),
    Competitor(
        id=CompetitorId.RDS_POSTGRES,
        name="Amazon RDS for PostgreSQL",
        short_name="RDS PostgreSQL",
        edition="RDS FOR POSTGRESQL EDITION",
    ),
]


MODEL_SCORE_METRICS = [
    MetricSpec(
        id="managed_availability_ms",
        label="Reverse ETL sync",
        role=MetricRole.PRIMARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        id="application_proof_elapsed_ms",
        label="End-to-end proof",
        role=MetricRole.SECONDARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        id="delta_commit_version",
        label="Delta commit version",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.VERSION,
        direction=MetricDirection.EXACT,
    ),
    MetricSpec(
        id="exact_row_verified",
        label="Exact row verified",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.BOOLEAN,
        direction=MetricDirection.EXACT,
    ),
]


CONNECTION_SPIKE_METRICS = [
    MetricSpec(
        id="setup_elapsed_ms",
        label="Setup elapsed",
        role=MetricRole.PRIMARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        id="successful_clients",
        label="Successful clients",
        role=MetricRole.SECONDARY,
        unit=MetricUnit.COUNT,
        direction=MetricDirection.HIGHER_IS_BETTER,
    ),
    MetricSpec(
        id="application_p99_ms",
        label="Application p99",
        role=MetricRole.SECONDARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        id="error_clients",
        label="Client errors",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.COUNT,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
]

LIVE_ORDERS_METRICS = [
    MetricSpec(
        id="analytics_available_ms",
        label="Live answer available",
        role=MetricRole.PRIMARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        id="matching_live_orders",
        label="Exact live orders",
        role=MetricRole.SECONDARY,
        unit=MetricUnit.COUNT,
        direction=MetricDirection.EXACT,
    ),
    MetricSpec(
        id="checkout_verified",
        label="Checkout guardrail",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.BOOLEAN,
        direction=MetricDirection.EXACT,
    ),
]


ROUNDS = [
    RoundDefinition(
        id=RoundId.WAKE_IDLE_APP,
        title="Wake this idle app",
        capability="Autoscaling and scale-to-zero",
        scorecard_by_corner={
            Corner.COST: "Published compute and storage rates; billed usage reconciles later",
            Corner.SIMPLICITY: "Automatic wake path and time to a verified transaction",
            Corner.PERFORMANCE: "Eligibility to start at zero, then time to verification",
        },
        competitors=[CompetitorId.AURORA_SERVERLESS_V2, CompetitorId.RDS_POSTGRES],
        availability=Availability.READY,
        redo=RedoPresentation(
            policy="show",
            badge="★ SHOW",
            label="RE-DO ROUND",
            description="Repeat the wake proof to show the same automatic product behavior.",
        ),
    ),
    RoundDefinition(
        id=RoundId.MAKE_SCHEMA_CHANGE_SAFELY,
        title="Make this schema change safely",
        capability="Instant branching and isolated change testing",
        scorecard_by_corner={
            Corner.COST: "Published rates plus developer wait; billed usage reconciles later",
            Corner.SIMPLICITY: "Steps and time to an application-verified isolated change",
            Corner.PERFORMANCE: "Time to create, migrate, and verify an isolated environment",
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.READY,
        redo=RedoPresentation(
            policy="optional",
            badge="OPTIONAL",
            label="RE-DO ROUND",
            description="Repeat only when the room wants another isolated-change proof.",
        ),
    ),
    RoundDefinition(
        id=RoundId.RECOVER_DELETED_ORDER,
        title="Recover this deleted order",
        capability="Point-in-time branching and restore",
        scorecard_by_corner={
            Corner.COST: "Published rates plus recovery wait; billed usage reconciles later",
            Corner.SIMPLICITY: "Steps to the agreed recovery point and verified read",
            Corner.PERFORMANCE: "Verified application RTO at the agreed RPO",
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.READY,
        redo=RedoPresentation(
            policy="skip",
            badge="SKIP",
            label="RE-DO ROUND",
            description=(
                "Hide after success; retain owned recovery cleanup and retry controls "
                "after failure."
            ),
        ),
    ),
    RoundDefinition(
        id=RoundId.PUT_MODEL_SCORE_IN_APP,
        title="Move lakehouse data into live applications",
        capability=(
            "Managed reverse ETL from Unity Catalog Delta to operational Lakebase Postgres"
        ),
        scorecard_by_corner={
            Corner.COST: "Database list rates captured; required reverse ETL remains unpriced",
            Corner.SIMPLICITY: (
                "Analytics Delta to exact operational Postgres application row"
            ),
            Corner.PERFORMANCE: (
                "Reverse ETL sync and end-to-end proof time"
            ),
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.PLANNED,
        metric_specs=MODEL_SCORE_METRICS,
        comparison_kind=ComparisonKind.CAPABILITY_GAP,
        non_claims=[
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
        ],
        redo=RedoPresentation(
            policy="show",
            badge="★ SHOW",
            label="CHANGE SCORE IN LAKEHOUSE → WATCH APP UPDATE",
            description=(
                "Change this demo's customer risk score from v1 to v2 in the lakehouse, "
                "then watch the same live app record update."
            ),
        ),
    ),
    RoundDefinition(
        id=RoundId.SURVIVE_CONNECTION_SPIKE,
        title="Get spike-ready",
        capability="Built-in connection pooling",
        scorecard_by_corner={
            Corner.COST: (
                "Published rates include the AWS opponent's added RDS Proxy minimum"
            ),
            Corner.SIMPLICITY: (
                "Lakebase adds 0 per-bout pooling infrastructure mutations; the selected "
                "AWS opponent performs 9 journaled competitor mutations"
            ),
            Corner.PERFORMANCE: (
                "Primary setup elapsed; secondary successful clients, errors, and "
                "application p99 under burst"
            ),
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.PLANNED,
        metric_specs=CONNECTION_SPIKE_METRICS,
        comparison_kind=ComparisonKind.MEASURED,
        non_claims=[
            (
                "Lakebase native pooling adds 0 separate per-bout pooling components and "
                "0 per-bout pooling infrastructure mutations; baseline native-login, "
                "ordinary-role, and runner-credential preparation is disclosed outside "
                "the per-bout setup clock."
            ),
            (
                "The selected AWS opponent lane performs 9 journaled competitor mutations: "
                "1 per-bout Proxy security group, 1 default-egress change, 4 exact security-"
                "group rules, 1 RDS Proxy, 1 target-group configuration, and 1 target "
                "registration; its setup clock stops at the exact application transaction."
            ),
            (
                "The IAM service role, runner permission, and dedicated proxy credential "
                "secret or secrets are sealed install-time prerequisites outside the setup "
                "clock. The selected AWS design still requires added RDS Proxy, Secrets "
                "Manager, IAM, and network configuration; RDS Proxy and Secrets Manager "
                "remain billable AWS services; the receipt estimates them separately and "
                "makes no savings claim."
            ),
            (
                "Application p99 is nearest-rank p99 derived from raw, unrounded "
                "successful-client latencies."
            ),
            (
                "Setup elapsed is the primary result and is never added to the "
                "secondary burst p99."
            ),
            "This is one live proof session, not a benchmark.",
        ],
    ),
    RoundDefinition(
        id=RoundId.ANALYZE_LIVE_ORDERS,
        title="Move live application data into the lakehouse",
        capability="Built-in change feed (CDF) to separate Delta history",
        scorecard_by_corner={
            Corner.COST: "Database list rates captured; required AWS CDC stack remains unpriced",
            Corner.SIMPLICITY: "Built-in change feed: one checkout to one exact Delta answer",
            Corner.PERFORMANCE: (
                "Commit-to-answer freshness; a separate checkout is the correctness guardrail"
            ),
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.PREVIEW,
        metric_specs=LIVE_ORDERS_METRICS,
        comparison_kind=ComparisonKind.CAPABILITY_GAP,
    ),
]


@lru_cache(maxsize=1)
def load_personas() -> tuple[Persona, ...]:
    path = ROOT / "config" / "personas.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    return tuple(Persona.model_validate(item) for item in raw["personas"])


def catalog(
    model_score_available: bool = False,
    connection_spike_available: bool = False,
    live_orders_available: bool = False,
) -> CatalogResponse:
    return CatalogResponse(
        competitors=COMPETITORS,
        corners=list(Corner),
        personas=list(load_personas()),
        rounds=[
            round_by_id(
                item.id,
                model_score_available=model_score_available,
                connection_spike_available=connection_spike_available,
                live_orders_available=live_orders_available,
            )
            for item in ROUNDS
        ],
    )


def persona_by_id(persona_id: str) -> Persona:
    try:
        return next(persona for persona in load_personas() if persona.id == persona_id)
    except StopIteration as exc:
        raise ValueError(f"Unknown persona: {persona_id}") from exc


def competitor_by_id(competitor_id: CompetitorId) -> Competitor:
    return next(item for item in COMPETITORS if item.id == competitor_id)


def round_by_id(
    round_id: RoundId,
    model_score_available: bool = False,
    connection_spike_available: bool = False,
    live_orders_available: bool = False,
) -> RoundDefinition:
    item = next(item for item in ROUNDS if item.id == round_id)
    if item.id == RoundId.PUT_MODEL_SCORE_IN_APP:
        return item.model_copy(
            update={
                "availability": (
                    Availability.READY if model_score_available else Availability.PLANNED
                )
            },
            deep=True,
        )
    if item.id == RoundId.SURVIVE_CONNECTION_SPIKE:
        return item.model_copy(
            update={
                "availability": (
                    Availability.READY
                    if connection_spike_available
                    else Availability.PLANNED
                )
            },
            deep=True,
        )
    if item.id == RoundId.ANALYZE_LIVE_ORDERS:
        return item.model_copy(
            update={
                "availability": (
                    Availability.READY if live_orders_available else Availability.PREVIEW
                )
            },
            deep=True,
        )
    return item


def recommend_round(
    competitor: CompetitorId,
    primary: Persona,
    model_score_available: bool = False,
    connection_spike_available: bool = False,
    live_orders_available: bool = False,
) -> tuple[RoundDefinition, str]:
    for preferred in primary.recommended_rounds:
        if preferred == "inherit_primary_round":
            continue
        try:
            candidate = round_by_id(
                RoundId(preferred),
                model_score_available=model_score_available,
                connection_spike_available=connection_spike_available,
                live_orders_available=live_orders_available,
            )
        except ValueError:
            continue
        if competitor in candidate.competitors and candidate.availability == Availability.READY:
            if (
                competitor == CompetitorId.RDS_POSTGRES
                and candidate.id == RoundId.WAKE_IDLE_APP
            ):
                return candidate, (
                    "RDS PostgreSQL has no automatic scale-to-zero wake path; its capability "
                    "is checked before the bell and only Lakebase is timed."
                )
            return candidate, f"Recommended for {primary.role} and executable for this matchup."

    candidate = round_by_id(
        RoundId.WAKE_IDLE_APP,
        model_score_available=model_score_available,
        connection_spike_available=connection_spike_available,
        live_orders_available=live_orders_available,
    )
    if competitor == CompetitorId.RDS_POSTGRES:
        return candidate, (
            "RDS PostgreSQL has no automatic scale-to-zero wake path; its capability is "
            "checked before the bell and only Lakebase is timed."
        )
    return candidate, (
        f"Selected as the strongest honest {competitor_by_id(competitor).short_name} "
        f"round; the {primary.role} lens changes the explanation, not the evidence."
    )


def build_presenter_pack(
    primary: Persona,
    secondary: list[Persona],
    selected_round: RoundDefinition,
    corners: list[Corner],
    competitor: CompetitorId,
) -> PresenterPack:
    def lens(persona: Persona) -> PresenterLens:
        return PresenterLens(
            persona_id=persona.id,
            nickname=persona.nickname,
            role=persona.role,
            interpretation=persona.presenter.interpretation,
            objection=persona.presenter.objection,
            response=persona.presenter.response,
        )

    discovery_question = primary.questions.get("why") or next(iter(primary.questions.values()))
    if len(corners) == 1:
        remembered_metric = selected_round.scorecard_by_corner[corners[0]]
    else:
        measures = {
            Corner.COST: "cost inputs",
            Corner.SIMPLICITY: "workflow simplicity",
            Corner.PERFORMANCE: "elapsed workflow time",
        }
        selected = [measures[corner] for corner in corners]
        metric_list = (
            f"{selected[0]} and {selected[1]}"
            if len(selected) == 2
            else f"{', '.join(selected[:-1])}, and {selected[-1]}"
        )
        remembered_metric = f"{metric_list.capitalize()} to the same verified outcome"
    if selected_round.id == RoundId.PUT_MODEL_SCORE_IN_APP:
        remembered_metric = "Managed Sync exact-version proof and fresh Postgres exact-row read"
    elif selected_round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
        remembered_metric = (
            "Primary setup elapsed; secondary burst successes, errors, and nearest-rank p99"
        )
    elif selected_round.id == RoundId.ANALYZE_LIVE_ORDERS:
        remembered_metric = "One exact live order in Delta with checkout still verified"
    if (
        selected_round.id == RoundId.WAKE_IDLE_APP
        and competitor == CompetitorId.RDS_POSTGRES
    ):
        stop_condition = (
            "Lakebase stops after commit + read-back; RDS eligibility is checked before "
            "the bell and not timed."
        )
    elif selected_round.id == RoundId.WAKE_IDLE_APP:
        stop_condition = (
            "Each clock stops after commit and read-back of its run-unique value."
        )
    elif selected_round.id == RoundId.MAKE_SCHEMA_CHANGE_SAFELY:
        stop_condition = (
            "Each clock stops after the identical migration and transaction verify and "
            "the final source check passes."
        )
    elif selected_round.id == RoundId.RECOVER_DELETED_ORDER:
        stop_condition = (
            "Each clock stops after the exact order reads from recovery and remains absent "
            "at the final source check."
        )
    elif selected_round.id == RoundId.PUT_MODEL_SCORE_IN_APP:
        stop_condition = (
            "The clock ends only after the exact committed Delta version is observed synced "
            "and a fresh Postgres read returns the exact row."
        )
    elif selected_round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
        stop_condition = (
            "Lakebase stops after its built-in pooled transaction verifies. The AWS clock "
            "stops only after the new RDS Proxy is ready and its exact application "
            "transaction verifies; burst, witness, and cleanup gates must still pass "
            "before any setup winner or margin is declared."
        )
    elif selected_round.id == RoundId.ANALYZE_LIVE_ORDERS:
        stop_condition = (
            "The clock stops when the exact committed order appears once in Delta history. "
            "The result waits for a separate checkout to commit."
        )
    elif selected_round.availability == Availability.PREVIEW:
        stop_condition = (
            "This preview round is non-executable; it has no verifier or timing boundary."
        )
    else:
        stop_condition = (
            "This planned round is non-executable; it has no verifier or timing boundary."
        )

    return PresenterPack(
        opening=primary.presenter.opening,
        discovery_question=discovery_question,
        risk=primary.presenter.risk,
        stop_condition=stop_condition,
        remembered_metric=remembered_metric,
        primary=lens(primary),
        secondary=[lens(persona) for persona in secondary],
        closing=primary.presenter.closing,
    )
