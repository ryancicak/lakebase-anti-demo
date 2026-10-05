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

#: The protocol Round 5 runs. The runner evidence remains fan-in v2, but the
#: installation lifecycle, bell origin, clocks and physical isolation are v3.
#:
#: `ROUND5_BOUNDED_PROTOCOL` survives as a name only so that a stored scorecard written
#: under it can still be recognised and labelled as a legacy result rather than silently
#: relabelled with 10,000-client copy it never attempted. It is not selectable.
ROUND5_BOUNDED_PROTOCOL = "connection-spike-v1"
ROUND5_FANIN_PROTOCOL = "round5-fanin-v4"
ROUND5_BELL_PROTOCOL = "round5-bell-to-10k-v4"
ROUND5_PROTOCOLS = frozenset({ROUND5_BELL_PROTOCOL})


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
        # The scored clock, on both lanes: the bell, which starts both integrations from
        # parked and commits the change, to that lane's first read of the exact row.
        id="bell_to_exact_read_ms",
        label="Bell to the exact row in the app",
        role=MetricRole.PRIMARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        # Lakebase's own figure, from the synced table's timestamps. Shown, never compared:
        # the AWS lane has no service timestamp like it.
        id="managed_availability_ms",
        label="Reverse ETL sync (Lakebase's own timestamps)",
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
        # The scored result: one server bell to the observed exact held gate.
        id="bell_to_10000_observed_ms",
        label="Bell to 10,000 held clients",
        role=MetricRole.PRIMARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        # Still the finding, just not the same measurement. Lakebase verifies an
        # included pool; the AWS path has to provision a Proxy first, and that is most
        # of its clock.
        id="setup_elapsed_ms",
        label="Pooled-path setup time",
        role=MetricRole.SECONDARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        # The multiplexing evidence, read by a second role on its own direct connection
        # while the clients are held. 10,000 clients in front of a small number of
        # backend sessions is the whole claim.
        id="peak_backend_sessions",
        label="Peak backend sessions",
        role=MetricRole.SECONDARY,
        unit=MetricUnit.COUNT,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        # Exact, not "higher is better". 9,999 held clients is a failed bout, and a
        # direction that rewards more would let a lane pass by being close.
        id="held_clients_at_gate",
        label="Clients held at the gate",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.COUNT,
        direction=MetricDirection.EXACT,
    ),
    MetricSpec(
        id="terminal_failures",
        label="Client failures",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.COUNT,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
]

LIVE_ORDERS_METRICS = [
    MetricSpec(
        # The scored clock, on both lanes: the bell, which commits the same checkout on both
        # sources and starts AWS's DMS task and Glue job from parked, to that lane's first read
        # of the exact order in its own Delta history.
        id="bell_to_exact_history_ms",
        label="Bell to the exact order in the lakehouse",
        role=MetricRole.PRIMARY,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        # How far apart the two sources acknowledged the bell's checkout. Recorded, never
        # scored: each lane's clock already contains its own commit.
        id="commit_skew_ms",
        label="Checkout commit skew between the lanes",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.MILLISECONDS,
        direction=MetricDirection.LOWER_IS_BETTER,
    ),
    MetricSpec(
        id="exact_order_verified",
        label="Exact order verified",
        role=MetricRole.GUARDRAIL,
        unit=MetricUnit.BOOLEAN,
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
            "Managed reverse ETL from Unity Catalog Delta to operational Lakebase Postgres, "
            "raced against an AWS Glue job writing the same change into Aurora or RDS"
        ),
        scorecard_by_corner={
            Corner.COST: (
                "Published rates: the synced-table pipeline and the Glue job each bill only "
                "while a bout runs"
            ),
            Corner.SIMPLICITY: (
                "One synced table against a Glue job, its role, network, connection, ledger "
                "and checkpoint"
            ),
            Corner.PERFORMANCE: (
                "Bell to the exact row in the app, with both integrations cold starting at "
                "the bell"
            ),
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.PLANNED,
        metric_specs=MODEL_SCORE_METRICS,
        comparison_kind=ComparisonKind.MEASURED,
        non_claims=[
            (
                "Both integrations cold start at the bell, and each lane's clock "
                "contains its own start. Neither is warmed for the audience."
            ),
            (
                "AWS moves the AWS lane's data: an AWS Glue 5.0 job reads the Delta table's "
                "files straight from S3, around Unity Catalog's permissions, lineage and audit, "
                "and writes over JDBC. The supported routes for an outside engine (credential "
                "vending, Iceberg REST, Delta Sharing) would each put Databricks back in the lane."
            ),
            (
                "One change, one verifier: one Delta commit feeds both lanes, and each lane is "
                "read by the same query on the same client every 250 ms."
            ),
            "The Glue job, its role, network and connection are installed once and standing.",
            "This is one live proof session, not a benchmark.",
            "No dollar savings are claimed.",
            "No full model-serving capability is claimed.",
        ],
    ),
    RoundDefinition(
        id=RoundId.SURVIVE_CONNECTION_SPIKE,
        title="Ready a pooled application path",
        capability="Included pooling compared with a selected AWS managed pooling path",
        scorecard_by_corner={
            Corner.COST: (
                "Published rates include the new RDS Proxy selected for the AWS reference path"
            ),
            Corner.SIMPLICITY: (
                "Included Lakebase pooled endpoint versus 3 timed Proxy mutations on the "
                "selected AWS managed pooling path"
            ),
            Corner.PERFORMANCE: (
                "One server bell to observed exact 10,000 held client connections per lane; "
                "runner-local ramp time and peak backend sessions are supporting evidence"
            ),
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.PLANNED,
        metric_specs=CONNECTION_SPIKE_METRICS,
        comparison_kind=ComparisonKind.MEASURED,
        non_claims=[
            (
                "Lakebase dispatches its first retained pooled client immediately at the "
                "bell. Native login, ordinary roles, capacity, credentials, and endpoint "
                "bindings are prepared automatically backstage."
            ),
            (
                "The selected AWS path performs 3 journaled timed mutations: CreateDBProxy "
                "first, exact target-group configuration, and exact target registration. "
                "Least-privilege Proxy network fixtures stand warm but the Proxy does not."
            ),
            (
                "The IAM service role, runner permission, and dedicated proxy credential "
                "secret or secrets are sealed install-time prerequisites outside the setup "
                "clock. RDS Proxy is the AWS managed pooling option selected for this reference "
                "path, not a universal Aurora or RDS requirement. Direct connections, an "
                "existing RDS Proxy, PgBouncer, and application pooling were not compared."
            ),
            (
                "Each independent lane opens exactly 10,000 authenticated retained clients, "
                "holds them for 30 seconds, and answers 64 sparse queries with no retries. "
                "9,999 fails. Each lane owns a distinct physical c7i.2xlarge runner."
            ),
            (
                "Multiplexing is read during the hold by a second database role on its own "
                "direct connection, never inferred from the client count. Connect latency "
                "p50, p95 and p99 are nearest-rank over raw, unrounded per-client "
                "measurements. Runner-local ramp time is supporting evidence; the large "
                "comparison clock is always server-observed bell to exact 10,000 held."
            ),
            (
                "Lakebase's built-in PgBouncer product limit is up to 10,000 client "
                "connections, not PostgreSQL backend sessions or simultaneous transactions. "
                "The bout holds exactly that many per lane and proves multiplexing while "
                "they are held. Direct AWS connections, an existing Proxy, "
                "sustained throughput, and storm resilience remain outside this comparison."
            ),
            "This is one live proof session, not a benchmark.",
        ],
    ),
    RoundDefinition(
        id=RoundId.ANALYZE_LIVE_ORDERS,
        title="Move live application data into the lakehouse",
        capability=(
            "Lakebase's built-in change feed into Delta, raced against AWS DMS capturing the "
            "same checkout from Aurora or RDS and an AWS Glue job appending it to Delta"
        ),
        scorecard_by_corner={
            Corner.COST: (
                "Published rates: the DMS instance stands, and the Glue job bills only while a "
                "bout runs"
            ),
            Corner.SIMPLICITY: (
                "One built-in change feed against a DMS instance, task and endpoints, a Glue "
                "job, its role, network, bucket and checkpoint"
            ),
            Corner.PERFORMANCE: (
                "Bell to the exact order in the lakehouse, with AWS DMS and Glue cold starting "
                "at the bell"
            ),
        },
        competitors=[CompetitorId.RDS_POSTGRES, CompetitorId.AURORA_SERVERLESS_V2],
        availability=Availability.PREVIEW,
        metric_specs=LIVE_ORDERS_METRICS,
        comparison_kind=ComparisonKind.MEASURED,
        non_claims=[
            (
                "AWS DMS and Glue cold start at the bell, and the AWS lane's clock contains "
                "their start. Lakebase's change feed is built into the database and always on, "
                "so its side has nothing to start. What It Cost shows what keeping AWS's "
                "pipeline running all day would cost."
            ),
            (
                "AWS moves the AWS lane's data: DMS captures the checkout from the database's "
                "write-ahead log into S3, and an AWS Glue 5.0 job appends it to a Delta table "
                "that Unity Catalog reads as an external table."
            ),
            (
                "One checkout, one verifier: the bell commits the same order on both sources, "
                "and each lane's Delta history is read by the same query on the same SQL "
                "warehouse every second."
            ),
            (
                "Each history keeps its own shape: Lakebase's feed writes a change type and "
                "LSN, and DMS writes an operation and commit timestamp. The claim is the "
                "order's delivery, not identical tables."
            ),
            (
                "The DMS instance, task and endpoints, the Glue job, its role, network and "
                "bucket are installed once and standing."
            ),
            "This is one live proof session, not a benchmark.",
            "No dollar savings are claimed.",
        ],
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
    round5_protocol: str = ROUND5_BELL_PROTOCOL,
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
                round5_protocol=round5_protocol,
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
    round5_protocol: str = ROUND5_BELL_PROTOCOL,
) -> RoundDefinition:
    if round5_protocol not in ROUND5_PROTOCOLS:
        raise ValueError(f"Unknown Round 5 protocol: {round5_protocol}")
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
                ),
                "round5_protocol": ROUND5_BELL_PROTOCOL,
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
    round5_protocol: str = ROUND5_BELL_PROTOCOL,
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
                round5_protocol=round5_protocol,
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
        remembered_metric = (
            "Bell to the exact row in the app on each lane, both integrations cold starting "
            "at the bell"
        )
    elif selected_round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
        remembered_metric = (
            "Primary time to hold 10,000 authenticated clients per lane from one shared "
            "start; secondary pooled-path setup time, peak backend sessions, and "
            "nearest-rank connect p99"
        )
    elif selected_round.id == RoundId.ANALYZE_LIVE_ORDERS:
        remembered_metric = (
            "Bell to the exact order in the lakehouse on each lane, AWS DMS and Glue cold "
            "starting at the bell"
        )
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
            "The bell cold starts both integrations and commits one Delta change. Each "
            "lane's clock stops at its first application read of the exact row, polled every "
            "250 ms on both lanes."
        )
    elif selected_round.id == RoundId.SURVIVE_CONNECTION_SPIKE:
        stop_condition = (
            "Lakebase stops after its included pooled endpoint verifies an exact transaction. "
            "The selected AWS managed pooling path stops only after its new RDS Proxy and "
            "exact transaction verify. Each lane then holds exactly 10,000 authenticated "
            "clients for a 30-second hold and answers 64 sparse queries with no retries; "
            "9,999 fails. Multiplexing, fairness, and cleanup gates must all pass before any "
            "winner or margin is declared."
        )
    elif selected_round.id == RoundId.ANALYZE_LIVE_ORDERS:
        stop_condition = (
            "The bell commits the same checkout on both sources and cold starts AWS DMS and "
            "Glue. Each lane's clock stops at its first read of the exact order, once, in its "
            "own Delta history, polled every second on both lanes. A separate checkout must "
            "commit on each source."
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
