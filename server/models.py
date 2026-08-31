from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class CompetitorId(StrEnum):
    AURORA_SERVERLESS_V2 = "aurora_serverless_v2"
    RDS_POSTGRES = "rds_postgres"


class Corner(StrEnum):
    COST = "cost"
    SIMPLICITY = "simplicity"
    PERFORMANCE = "performance"


class RoundId(StrEnum):
    WAKE_IDLE_APP = "wake_idle_app"
    MAKE_SCHEMA_CHANGE_SAFELY = "make_schema_change_safely"
    RECOVER_DELETED_ORDER = "recover_deleted_order"
    PUT_MODEL_SCORE_IN_APP = "put_model_score_in_app"
    SURVIVE_CONNECTION_SPIKE = "survive_connection_spike"
    ANALYZE_LIVE_ORDERS = "analyze_live_orders_without_slowing_checkout"


class Availability(StrEnum):
    READY = "ready"
    PLANNED = "planned"
    PREVIEW = "preview"
    #: Built and executable, but something live says it cannot arm right now --
    #: an unready ring, a credential fault, a swept installation, or a context
    #: that physically cannot run it. Distinct from `PLANNED` and `PREVIEW`,
    #: which are facts about the build rather than about this minute, and always
    #: accompanied by both `RoundDefinition.availability_reason` and
    #: `RoundDefinition.availability_headline` -- one refusal, written for the
    #: operator and written for the room.
    UNAVAILABLE = "unavailable"


class SessionState(StrEnum):
    DRAFT = "draft"
    CHECKING = "checking"
    ARMED = "armed"
    RUNNING = "running"
    VERIFIED = "verified"
    TOWELLED = "towelled"
    FAILED = "failed"


class BoutOperator(BaseModel):
    display_name: str
    email: str | None = None
    # Stable Databricks SSO subject used to authorize mutations. It is deliberately
    # excluded from browser payloads; the friendly name and email are sufficient UI.
    subject: str | None = Field(default=None, exclude=True)


class BoutStatus(BaseModel):
    scope: Literal["global", "round"] = "global"
    round_id: RoundId | None = None
    ring_ready: bool = True
    can_start: bool = True
    maintenance_state: Literal["ready", "maintenance", "blocked"] = "ready"
    maintenance_detail: str | None = None
    active: bool
    operator: BoutOperator | None = None
    started_at: datetime | None = None
    updated_at: datetime | None = None
    expires_at: datetime | None = None
    phase: str | None = None
    state: SessionState | None = None
    round_title: str | None = None
    competitor: str | None = None


class FightCardState(StrEnum):
    READY = "ready"
    BOUT_IN_PROGRESS = "bout_in_progress"
    CLEANUP_IN_PROGRESS = "cleanup_in_progress"
    UNAVAILABLE = "unavailable"


class FightCardRoundStatus(BaseModel):
    """Sanitized shared state for one tile on the six-round fight card."""

    round_id: RoundId
    state: FightCardState
    can_start: bool
    active_phase: str | None = None
    detail: str | None = None
    updated_at: datetime | None = None
    expires_at: datetime | None = None


class AllBoutStatus(BaseModel):
    """One bounded observation of every round ring."""

    rounds: dict[RoundId, FightCardRoundStatus]
    updated_at: datetime


class LaneState(StrEnum):
    SEALED = "sealed"
    CONNECTING = "connecting"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    TOWELLED = "towelled"
    NOT_SUPPORTED = "not_supported"
    FAILED = "failed"


class TowelState(StrEnum):
    STOPPING = "stopping"
    CLEANING = "cleaning"
    FAILED = "failed"
    READY = "ready"


class CooldownState(StrEnum):
    WATCHING = "watching"
    READY = "ready"
    FAILED = "failed"


class ResetMode(StrEnum):
    RETURN_TO_IDLE = "return_to_idle"
    DELETE_ISOLATED_ENVIRONMENT = "delete_isolated_environment"
    DELETE_RECOVERY_ENVIRONMENT = "delete_recovery_environment"


class CooldownLaneState(StrEnum):
    WATCHING = "watching"
    CONFIRMED_ZERO = "confirmed_zero"
    CONFIRMED_DELETED = "confirmed_deleted"
    NOT_SUPPORTED = "not_supported"
    FAILED = "failed"


class MetricRole(StrEnum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    GUARDRAIL = "guardrail"


class MetricUnit(StrEnum):
    MILLISECONDS = "milliseconds"
    PERCENT = "percent"
    COUNT = "count"
    BOOLEAN = "boolean"
    VERSION = "version"


class MetricDirection(StrEnum):
    LOWER_IS_BETTER = "lower_is_better"
    HIGHER_IS_BETTER = "higher_is_better"
    EXACT = "exact"


class ComparisonKind(StrEnum):
    MEASURED = "measured"
    CAPABILITY_GAP = "capability_gap"
    ADJUDICATED_STOPPAGE = "adjudicated_stoppage"
    TIE = "tie"
    NOT_COMPARABLE = "not_comparable"


class RedoState(StrEnum):
    READY = "ready"
    RUNNING = "running"
    VERIFIED = "verified"
    FAILED = "failed"


class RoundFiveSetupState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    VERIFIED = "verified"
    TOWELLED = "towelled"
    FAILED = "failed"
    CLEANUP_FAILED = "cleanup_failed"


class MetricSpec(BaseModel):
    id: str
    label: str
    role: MetricRole
    unit: MetricUnit
    direction: MetricDirection


class MetricValue(BaseModel):
    spec_id: str
    lane_id: str | None = Field(default=None, exclude_if=lambda value: value is None)
    value: float | int | bool | str
    display_value: str | None = None


class ComparisonSnapshot(BaseModel):
    kind: ComparisonKind
    winner_lane_id: str | None = None
    margin: MetricValue | None = None
    detail: str | None = None

    @model_validator(mode="after")
    def validate_non_comparable_outcome(self) -> ComparisonSnapshot:
        if self.kind == ComparisonKind.CAPABILITY_GAP and (
            self.winner_lane_id is None or self.margin is not None
        ):
            raise ValueError("capability_gap requires a winner and forbids a margin")
        if self.kind == ComparisonKind.ADJUDICATED_STOPPAGE and (
            self.winner_lane_id is None or self.margin is not None
        ):
            raise ValueError("adjudicated_stoppage requires a winner and forbids a margin")
        if self.kind == ComparisonKind.NOT_COMPARABLE and (
            self.winner_lane_id is not None or self.margin is not None
        ):
            raise ValueError("not_comparable cannot declare a winner or margin")
        return self


class RedoPresentation(BaseModel):
    policy: str
    badge: str
    label: str
    description: str


class PresenterCopy(BaseModel):
    opening: str
    risk: str
    interpretation: str
    objection: str
    response: str
    closing: str


class Persona(BaseModel):
    id: str
    role: str
    nickname: str
    # Shown on the roster card, where nickname and role are all the audience gets.
    pain: str = ""
    portrait: str
    source_status: str
    source_slide: int | None = None
    discovery_order: str
    recommended_rounds: list[str]
    questions: dict[str, str]
    presenter: PresenterCopy


class Competitor(BaseModel):
    id: CompetitorId
    name: str
    short_name: str
    edition: str


class RoundDefinition(BaseModel):
    id: RoundId
    title: str
    capability: str
    scorecard_by_corner: dict[Corner, str]
    competitors: list[CompetitorId]
    availability: Availability
    #: Machine-readable context for a live refusal that is expected to clear
    #: without operator action. The refusal copy remains present for older
    #: clients and diagnostics; new clients branch on this field, never on the
    #: prose. Absent for every durable configuration, credential, permission,
    #: and health failure.
    availability_reason_code: Literal["cleanup_in_progress"] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    #: Why this round cannot be offered, when it cannot. Set for exactly the
    #: `UNAVAILABLE` rounds: "not ready" with no reason is barely better than a
    #: wrong "ready", because neither tells an operator what to do about it.
    availability_reason: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    #: The same refusal with none of the operator's vocabulary in it: no port, no
    #: security group, no CLI command, and no instruction only the person who
    #: provisioned the install could carry out. Set beside `availability_reason`
    #: and never instead of it, because the fight card is a screen an audience
    #: reads and `availability_reason` is written for the one person in the room
    #: who can act on it. `server.round_availability.RoundRefusal` decides both
    #: in one branch so they cannot come to disagree.
    availability_headline: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    metric_specs: list[MetricSpec] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
    )
    comparison_kind: ComparisonKind | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    non_claims: list[str] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
    )
    redo: RedoPresentation | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class CatalogResponse(BaseModel):
    competitors: list[Competitor]
    corners: list[Corner]
    personas: list[Persona]
    rounds: list[RoundDefinition]


class SessionCreate(BaseModel):
    competitor: CompetitorId
    primary_persona: str
    secondary_personas: list[str] = Field(default_factory=list, max_length=2)
    corners: list[Corner] = Field(min_length=1, max_length=3)
    round_id: RoundId | None = None

    @model_validator(mode="after")
    def validate_persona_selection(self) -> SessionCreate:
        if self.primary_persona in self.secondary_personas:
            raise ValueError("The primary persona cannot also be secondary")
        if len(set(self.secondary_personas)) != len(self.secondary_personas):
            raise ValueError("Secondary personas must be unique")
        if len(set(self.corners)) != len(self.corners):
            raise ValueError("Customer priorities must be unique")
        return self


class PresenterLens(BaseModel):
    persona_id: str
    nickname: str
    role: str
    interpretation: str
    objection: str
    response: str


class PresenterPack(BaseModel):
    opening: str
    discovery_question: str
    risk: str
    stop_condition: str
    remembered_metric: str
    primary: PresenterLens
    secondary: list[PresenterLens]
    closing: str


class LaneActivity(BaseModel):
    phase: str
    wire_call: str | None = None
    recovery_at: datetime | None = None


class LaneSnapshot(BaseModel):
    id: str
    name: str
    state: LaneState = LaneState.SEALED
    elapsed_ms: float | None = None
    # Public, server-derived lower bound at serialization time. Active timed
    # lanes use the bout's authoritative monotonic origin; terminal lanes keep
    # `elapsed_ms` as their exact stopped measurement.
    elapsed_at_snapshot_ms: float | None = None
    attempts: int = 0
    successes: int = 0
    errors: int = 0
    p99_ms: float | None = None
    status: str = "Sealed"
    error: str | None = None
    verified_at: datetime | None = None
    # Round 1's verifier returns only after the attempt's connection context has
    # exited. This is therefore the lane-owned origin for post-bout idle
    # observation, unlike `SessionSnapshot.updated_at`, which also includes
    # settlement and lease-transition work performed after every lane closed.
    connection_closed_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    activity: LaneActivity | None = None
    evidence: dict[str, Any] = Field(
        default_factory=dict,
        exclude_if=lambda value: not value,
    )


class RoundFiveSetupEvidenceSnapshot(BaseModel):
    key: str
    value: str | int | float | bool | None


class RoundFiveSetupGateSnapshot(BaseModel):
    gate_id: str
    expected: list[RoundFiveSetupEvidenceSnapshot]
    observed: list[RoundFiveSetupEvidenceSnapshot]
    exact: bool


class RoundFiveSetupLaneSnapshot(BaseModel):
    id: str
    name: str
    state: RoundFiveSetupState = RoundFiveSetupState.PENDING
    setup_elapsed_ms: float | None = None
    # Public, server-derived lower bound at the instant this snapshot was made.
    # Unlike the progress latch above, this advances across silent provider waits.
    # It carries no wall or monotonic timestamp, so clients cannot mix clock
    # domains; they only interpolate forward from this already-authoritative base.
    elapsed_at_snapshot_ms: float | None = None
    status: str = "Waiting for setup"
    stop_gate_evidence: RoundFiveSetupGateSnapshot | None = None
    verified: bool = False
    error: str | None = None


class RoundFiveSetupSnapshot(BaseModel):
    state: RoundFiveSetupState = RoundFiveSetupState.PENDING
    lanes: dict[str, RoundFiveSetupLaneSnapshot]
    workflow_launch_skew_ms: float | None = None
    setup_validated: bool = False
    downstream_validated: bool = False
    failure: str | None = None
    cleanup_retryable: bool = False
    # Why backstage cleanup was given up on, or None while it is still being
    # retried or once it has been confirmed. The towel's counterpart of this
    # field, and named to match, because they answer the same question for a
    # reader: did the tidy-up finish?
    #
    # `failure` cannot carry it. That is the *round's* failure, and a Round 5
    # bout can verify and still leave an RDS Proxy behind -- the measurement and
    # the tidy-up are separate facts. `cleanup_retryable` cannot carry it either:
    # it is true both while retries are still running and after they have been
    # abandoned, which is what left an abandoned cleanup indistinguishable from
    # an in-flight one.
    cleanup_failure: str | None = None


class RedoSnapshot(BaseModel):
    state: RedoState
    lanes: dict[str, LaneSnapshot] = Field(default_factory=dict)
    metric_specs: list[MetricSpec] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
    )
    metrics: list[MetricValue] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
    )
    comparison: ComparisonSnapshot | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    failure: str | None = Field(default=None, exclude_if=lambda value: value is None)
    started_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    completed_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class CooldownLaneSnapshot(BaseModel):
    id: str
    name: str
    state: CooldownLaneState = CooldownLaneState.WATCHING
    started_at: datetime
    confirmed_at: datetime | None = None
    elapsed_ms: float | None = None
    status: str = "Watching for confirmed zero"
    activity: LaneActivity | None = None
    observed_state: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    observation_count: int = 0
    confirmation_basis: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    provider_updated_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    checked_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class CooldownSnapshot(BaseModel):
    mode: ResetMode = ResetMode.RETURN_TO_IDLE
    state: CooldownState = CooldownState.WATCHING
    started_at: datetime
    lanes: dict[str, CooldownLaneSnapshot]
    failure: str | None = None


class TowelSnapshot(BaseModel):
    state: TowelState
    requested_at: datetime
    # Universal public cutoff evidence.  The legacy Round 3 fields below remain
    # readable so already-sealed manifests and older clients continue to load.
    cutoff_ms: float | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    censored_lower_bounds_ms: dict[str, float] = Field(default_factory=dict)
    public_result: str | None = None
    active_lane: str | None = None
    lower_bound_ms: float | None = None
    lakebase_verified_ms: float | None = None
    restore_started: bool = False
    cleanup_failure: str | None = None
    # The towel must never be blocked by the cost ledger, so a failure to close
    # the bout's cost window is recorded here and retried by the cleanup task
    # rather than raised. Excluded when unset so already-sealed manifests and
    # older clients keep the payload they were written with.
    cost_close_failure: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @model_validator(mode="before")
    @classmethod
    def populate_compatible_cutoff(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        compatible = dict(value)
        if "cutoff_ms" not in compatible and compatible.get("lower_bound_ms") is not None:
            compatible["cutoff_ms"] = compatible["lower_bound_ms"]
        if "lower_bound_ms" not in compatible and compatible.get("cutoff_ms") is not None:
            compatible["lower_bound_ms"] = compatible["cutoff_ms"]
        return compatible


class FairnessSnapshot(BaseModel):
    same_client: bool = True
    same_transaction: bool = True
    same_nonce: bool = True
    launch_skew_ms: float | None = None
    warmup_connections: int | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    concurrency: int | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    runner: str | None = Field(default=None, exclude_if=lambda value: value is None)
    tls: str | None = Field(default=None, exclude_if=lambda value: value is None)
    timeout: str | None = Field(default=None, exclude_if=lambda value: value is None)


class CapacityLaneDisclosure(BaseModel):
    """The compute one lane is configured with, for on-screen disclosure.

    `basis` distinguishes a figure read back from a live control plane during
    arming from one that is only the configuration the installer applies. A lane
    that reports nothing renders as unreported rather than as a constant.
    """

    lane_id: Literal["lakebase", "competitor"]
    product: str
    configured: str
    memory: str
    engine_version: str
    idle_policy: str
    basis: Literal["observed", "configured", "unreported"] = "configured"
    max_connections: int | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )


class CapacityDisclosure(BaseModel):
    """Configured compute on both sides, and whether the ceilings match."""

    lanes: list[CapacityLaneDisclosure]
    matched: bool
    summary: str
    note: str


class DescentCostLane(BaseModel):
    """What one lane is billed for a single return to idle.

    `descends` separates the two shapes of answer. An engine that descends pays a
    floor once per descent and then stops; provisioned RDS cannot descend, so its
    figure is a whole day rather than a floor, and the per-descent fields are None
    rather than zero.

    `band_reason` is carried alongside the band because a wide band is only honest
    if the reason it is wide is on screen next to it.
    """

    lane_id: Literal["lakebase", "competitor"]
    product: str
    descends: bool
    floor_label: str
    per_descent_low_usd: float | None = None
    per_descent_high_usd: float | None = None
    per_descent_display: str
    per_descent_headline: str
    per_day_display: str
    derivation: str
    rate_source: str
    band_reason: str


class DescentCostDisclosure(BaseModel):
    """The price of each engine's idle floor, for Round 1's return to idle."""

    lanes: list[DescentCostLane]
    floor_ratio_label: str
    illustrative_descents_per_day: int
    summary: str
    note: str


class BoutCostRound(BaseModel):
    """One round's measured Aurora-lane marginal cost.

    `provenance` is the whole point of the row. `measured` means a CloudWatch
    integral priced it; `structural_zero` means the Terraform stands no Aurora
    cluster up for the round at all, so its zero is exact rather than missing;
    `unavailable` means the quantity could not be established and the row must
    not be read as free.

    `band_kind` separates two bands that look identical and mean different
    things. `observed_spread` is the range two real bouts of the same round
    actually spanned. `unresolved_billing_question` is a question AWS has not
    answered -- whether capacity reported by an instance in `deleting` is
    billed -- with both ends observed and neither settled. Collapsing either
    into one number would be picking an answer nobody gave.
    """

    round_id: RoundId
    round_number: int
    label: str
    provenance: Literal["measured", "structural_zero", "unavailable"]
    band_kind: Literal[
        "single_bout",
        "observed_spread",
        "unresolved_billing_question",
        "exact_zero",
    ]
    usd_display: str
    usd_low: float | None = None
    usd_high: float | None = None
    derivation: str
    band_reason: str
    bouts: list[str] = Field(default_factory=list)


class BoutCostDisclosure(BaseModel):
    """The Aurora lane's marginal cost per round, and the total it sums to.

    This is the measured replacement for a published figure that was 2.6-2.8x
    too low, so it carries the superseded figure beside the new one rather than
    quietly replacing it.
    """

    rounds: list[BoutCostRound]
    total_display: str
    superseded_display: str
    dearest_claim: str
    lakebase_lane_claim: str
    summary: str
    scope_note: str
    note: str
    rate_source: str


class StandingCostLaneId(StrEnum):
    """The six things that bill while nothing is running.

    Six rather than four. RDS, Aurora, Lakebase and the RDS Proxy secrets are the
    four an operator names. The ``NEUTRAL_RUNNER`` belongs to neither corner -- it
    is the box that drives both lanes -- and ``DATABRICKS_PLATFORM`` is neither
    corner's engine. Dropping either would leave a total that does not reconcile.

    No share of the bill is written down for any of them here. The fraction this
    docstring used to quote for ``DATABRICKS_PLATFORM`` was computed before the
    lane stopped counting compute that predates the installation, and by then
    described neither total. A share moves with the rate card, with the posted
    read and with which components predate the run; the payload derives it at
    build time, and prose that restates one describes whichever reading it was
    typed against.

    ``DATABRICKS_PLATFORM`` is deliberately not ``LAKEBASE``. Lakebase's standing
    cost is small; the Round 4 synced-table pipeline and the app's own compute are
    not, and folding them in would overstate Lakebase by roughly ninefold. That
    error points against us, which does not make it safe.
    """

    RDS = "rds"
    AURORA = "aurora"
    LAKEBASE = "lakebase"
    RDS_PROXY = "rds_proxy"
    NEUTRAL_RUNNER = "neutral_runner"
    DATABRICKS_PLATFORM = "databricks_platform"


class StandingCostFigure(BaseModel):
    """One standing-cost figure, or an explicit absence -- never a bare zero.

    A bare ``$0.00`` on a lane is the defect this model exists to make
    unrepresentable. Three states, and the validator holds them apart:

    ``priced``
        A real amount. The display must contain a non-zero digit, so a figure
        small enough to round away cannot render as zero either -- Lakebase
        storage is $0.0034/day and has to survive being printed.
    ``structural_zero``
        A zero that means something, and the only way a zero is allowed. Aurora
        compute at ``min_capacity = 0`` is the case: the cluster genuinely parks
        free. ``zero_basis`` carries the derivation and must appear inside
        ``display``, so the reason travels on the same field as the number and a
        renderer cannot show one without the other.
    ``unavailable``
        Missing data. Follows ``Quantity.unavailable`` in ``server/cost_model.py``:
        a quantity that could not be established is unknown, not free.
    """

    state: Literal["priced", "structural_zero", "unavailable"]
    usd_per_hour: float | None = None
    usd_per_day: float | None = None
    display: str
    derivation: str
    zero_basis: str = ""
    rate_source: str = ""

    @model_validator(mode="after")
    def validate_no_bare_zero(self) -> StandingCostFigure:
        if not self.derivation.strip():
            raise ValueError("a standing-cost figure must carry the derivation beside it")
        if not self.display.strip():
            raise ValueError("a standing-cost figure must carry something to render")
        if self.state == "unavailable":
            if self.usd_per_hour is not None or self.usd_per_day is not None:
                raise ValueError("an unavailable figure cannot carry a dollar amount")
            if self.zero_basis:
                raise ValueError("an unavailable figure is not a zero and has no zero basis")
            return self
        if self.usd_per_hour is None or self.usd_per_day is None:
            raise ValueError("a known figure requires both the hourly and the daily amount")
        if self.usd_per_hour < 0 or self.usd_per_day < 0:
            raise ValueError("a standing-cost figure cannot be negative")
        if self.state == "structural_zero":
            if self.usd_per_hour != 0 or self.usd_per_day != 0:
                raise ValueError("a structural zero must actually be zero")
            if not self.zero_basis.strip():
                raise ValueError("a zero must carry the derivation that makes it structural")
            if self.zero_basis not in self.display:
                raise ValueError("a structural zero must render with its derivation beside it")
            return self
        if self.zero_basis:
            raise ValueError("only a structural zero carries a zero basis")
        if not any(digit in self.display for digit in "123456789"):
            raise ValueError(
                "a priced figure that renders as zero must be declared a structural "
                "zero and carry its basis"
            )
        return self


class StandingCostComponent(BaseModel):
    """One priced line inside a lane, keeping its own provenance.

    ``predates_installation`` is what makes the two totals separable. A component
    that would bill whether or not this ``run_id`` existed is not something this
    installation created, and the total that includes it may not be quoted without
    saying so.
    """

    component: str
    cloud: Literal["aws", "databricks"]
    kind: Literal["compute", "storage", "network", "other"]
    provenance: Literal["measured", "modeled", "assumed", "unavailable"]
    quantity_basis: str
    predates_installation: bool = False
    figure: StandingCostFigure


class StandingCostLane(BaseModel):
    """One of the six lanes, its components, and its own subtotal."""

    lane_id: StandingCostLaneId
    product: str
    side: Literal["lakebase", "competitor", "shared", "platform"]
    idle_label: str
    figure: StandingCostFigure
    components: list[StandingCostComponent] = Field(default_factory=list)
    evidence: Literal[
        "rate_card_derived",
        "posted_actual",
        "posted_projection",
        "sealed_shape_only",
        "unpriced",
    ]
    rate_source: str
    caveat: str
    counted_in_installation_total: bool
    counted_in_platform_total: bool

    @model_validator(mode="after")
    def validate_unpriced_lane_is_not_counted(self) -> StandingCostLane:
        if self.figure.state == "unavailable" and (
            self.counted_in_installation_total or self.counted_in_platform_total
        ):
            raise ValueError("an unpriced lane cannot be counted into a total")
        if not self.components:
            raise ValueError("a lane must show the components it is made of")
        return self


class StandingCostTotal(BaseModel):
    """One total, inseparable from the condition it holds under.

    ``condition`` is required and non-empty because neither of these two figures
    means anything on its own: one covers what this ``run_id`` created, the other
    adds compute that would bill anyway.
    """

    label: str
    usd_per_hour: float
    usd_per_day: float
    display: str
    condition: str
    lane_ids: list[StandingCostLaneId]
    partial: bool = False
    partial_reason: str = ""

    @model_validator(mode="after")
    def validate_condition_travels_with_the_total(self) -> StandingCostTotal:
        if not self.condition.strip():
            raise ValueError("a standing-cost total may not be quoted without its condition")
        if not self.lane_ids:
            raise ValueError("a total must name the lanes it was summed from")
        if self.partial:
            if not self.partial_reason.strip():
                raise ValueError("a partial total must say what it is missing")
            if "partial" not in self.label.lower():
                raise ValueError("a partial total must be labelled partial")
        elif self.partial_reason.strip():
            raise ValueError("a complete total cannot carry a reason for being partial")
        return self


class StandingCostTotals(BaseModel):
    """Both totals or neither.

    Required rather than optional on purpose. ``installation`` alone understates
    what the account is spending; ``with_platform`` alone attributes an app that
    predates this installation to it. Rendering one without the other is the
    failure this shape prevents.
    """

    installation: StandingCostTotal
    with_platform: StandingCostTotal


class StandingCostPosted(BaseModel):
    """Posted Databricks actuals, never blended into the projection.

    Three figures, not two. ``projection_usd`` covers the whole disclosure
    window; ``projection_in_posted_window_usd`` covers only the part the posted
    read covers; ``posted_usd`` is what was billed. Variance is computed against
    the window-restricted projection alone -- measuring a 24-hour projection
    against a partial posted day and calling the gap an error is arithmetic across
    two different windows.

    Nothing here is ever labelled reconciled, and neither figure is hidden when
    they disagree.
    """

    state: Literal["posted_through_window", "unavailable"]
    cloud: Literal["databricks"] = "databricks"
    source: str
    window_start: datetime | None = None
    window_end: datetime | None = None
    posted_usd: float | None = None
    projection_usd: float | None = None
    projection_in_posted_window_usd: float | None = None
    variance_usd: float | None = None
    variance_fraction: float | None = None
    posted_hours: float | None = None
    unposted_hours: float | None = None
    unposted_basis: str
    comparison_basis: str
    explanation: str
    aws_posted: Literal["no_posted_counterpart"] = "no_posted_counterpart"
    aws_posted_basis: str
    unavailable_reason: str = ""
    display: str

    @model_validator(mode="after")
    def validate_posted_never_blends(self) -> StandingCostPosted:
        if not self.aws_posted_basis.strip():
            raise ValueError("the absence of an AWS posted figure must be stated, not inferred")
        if not self.unposted_basis.strip():
            raise ValueError("the unposted remainder must be explicit")
        if self.state == "unavailable":
            if self.posted_usd is not None or self.variance_usd is not None:
                raise ValueError("an unavailable posted read carries neither figure")
            if not self.unavailable_reason.strip():
                raise ValueError("an unavailable posted read must say why")
            return self
        if self.posted_usd is None or self.projection_in_posted_window_usd is None:
            raise ValueError("a posted comparison needs both the posted figure and its window")
        if self.variance_usd is None:
            raise ValueError("a posted comparison must state its variance rather than hide it")
        if not self.comparison_basis.strip():
            raise ValueError("a variance must name the window it was computed over")
        return self


class StandingCostDriftFinding(BaseModel):
    """One unexpected accrual, aged from its own clock.

    ``accrued_usd`` is omitted rather than defaulted when the resource carries no
    readable creation time: a reaper that cannot age a resource must not assume it
    is new. ``charging_for_absent`` marks the findings whose sign runs the other
    way -- the sealed shape is being charged for something the account does not
    have.
    """

    code: str
    kind: str
    identifier: str
    detail: str
    usd_per_day: float | None = None
    accrued_usd: float | None = None
    accrual_basis: str
    rate_basis: str
    charging_for_absent: bool = False

    @model_validator(mode="after")
    def validate_accrual_is_explained(self) -> StandingCostDriftFinding:
        if not self.accrual_basis.strip():
            raise ValueError("an accrued figure, present or absent, must say what aged it")
        if not self.rate_basis.strip():
            raise ValueError("a drift rate, present or absent, must name its basis")
        return self


class StandingCostDrift(BaseModel):
    """Unexpected accrual, kept separate from the sealed kind.

    Sealed accrual and unexpected accrual are two figures. Adding them yields a
    bigger, less useful number and destroys the property that makes this a
    monitor, so nothing here is ever summed into the headline totals.
    """

    state: Literal["sealed_shape_holds", "unexpected_accrual", "unavailable"]
    badge: str
    summary: str
    unexpected_usd_per_day: float | None = None
    unexpected_accrued_usd: float | None = None
    findings: list[StandingCostDriftFinding] = Field(default_factory=list)
    separation_note: str
    unavailable_reason: str = ""

    @model_validator(mode="after")
    def validate_drift_stays_separate(self) -> StandingCostDrift:
        if not self.badge.strip() or not self.summary.strip():
            raise ValueError("a drift badge must render something an operator can read")
        if not self.separation_note.strip():
            raise ValueError("drift must state that it is never summed into the headline")
        if self.state == "unavailable":
            if not self.unavailable_reason.strip():
                raise ValueError("an unavailable reconciliation must say why")
            if self.unexpected_usd_per_day is not None:
                raise ValueError("an unavailable reconciliation cannot price drift")
        elif self.unavailable_reason.strip():
            raise ValueError("a completed reconciliation carries no unavailable reason")
        return self


class StandingCostFairness(BaseModel):
    """The existing fairness paragraph, with its figures derived rather than typed.

    The prose is the one already on the proof surface, reused rather than rewritten
    -- the house rule against a third voice applies to reasoning as much as to
    tone. Only the figures are filled in at build time, from the same derivation
    the lanes come from, so the paragraph cannot go stale behind a rate change.

    It is withheld rather than reworded when its own claim cannot be supported: the
    paragraph asserts that our half is the larger one, and stating that while the
    Databricks half is unpriced would be a claim with nothing behind it.
    """

    state: Literal["stated", "withheld"]
    paragraph: str = ""
    withheld_reason: str = ""

    @model_validator(mode="after")
    def validate_claim_is_supported(self) -> StandingCostFairness:
        if self.state == "stated":
            if not self.paragraph.strip():
                raise ValueError("a stated fairness paragraph must have prose in it")
            if self.withheld_reason.strip():
                raise ValueError("a stated paragraph is not withheld")
            return self
        if self.paragraph.strip():
            raise ValueError("a withheld paragraph must not be rendered anyway")
        if not self.withheld_reason.strip():
            raise ValueError("a withheld paragraph must say what it is missing")
        return self


class StandingCostContinuous(BaseModel):
    """The one standing line the panel says is deliberate, and what it costs.

    Round 4's synced-table pipeline carries ``continuous: true`` in its spec, and
    while it is running it bills a full day's rate for a round that lasts
    minutes. ``continuous`` describes how it syncs while it is up, not that it
    stays up: the pipeline is started when a round arms and stopped once that
    bout has settled, so the daily rate is what it costs *while running* rather
    than an unconditional standing charge. Continuous governed sync is the thing
    the round demonstrates, but a reader who works the arithmetic out unaided
    concludes the opposite of what this demo argues, and concludes it correctly
    from the figures on screen. So the panel states the rate, its condition, and
    the per-bout-hour figure the stop-between-bouts behaviour takes it down to.

    ``usd_per_day`` and ``share_of_databricks`` are carried as numbers as well as
    inside the prose so the identity can be checked without parsing a sentence:
    they are the pipeline component's own amount and its share of the Databricks
    side of ``totals.installation``, not a second derivation.

    Withheld on the same rule as :class:`StandingCostFairness`. The claim is a
    share of a half, so an unpriced half leaves it with no denominator, and a
    payload carrying no pipeline leaves nothing to disclose.
    """

    state: Literal["stated", "withheld"]
    component: str = ""
    paragraph: str = ""
    derivation: str = ""
    usd_per_hour: float | None = None
    usd_per_day: float | None = None
    share_of_databricks: float | None = None
    withheld_reason: str = ""

    @model_validator(mode="after")
    def validate_claim_is_supported(self) -> StandingCostContinuous:
        if self.state == "stated":
            if not self.paragraph.strip() or not self.component.strip():
                raise ValueError("a stated continuous disclosure must name and price its line")
            if not self.derivation.strip():
                raise ValueError("a stated continuous disclosure carries its derivation")
            if self.usd_per_day is None or self.share_of_databricks is None:
                raise ValueError("the prose and the figures behind it travel together")
            if self.withheld_reason.strip():
                raise ValueError("a stated disclosure is not withheld")
            return self
        if self.paragraph.strip() or self.usd_per_day is not None:
            raise ValueError("a withheld disclosure must not be rendered anyway")
        if not self.withheld_reason.strip():
            raise ValueError("a withheld disclosure must say what it is missing")
        return self


class StandingCostPredating(BaseModel):
    """Compute that would bill without this run, disclosed instead of folded in.

    A workspace app that was serving before this ``run_id`` existed belongs to
    ``totals.with_platform`` and to neither lane figure a reader is asked to add
    up. It used to be summed into the platform lane's subtotal, which made the six
    rendered lane figures add to ``with_platform`` while the panel led with
    ``installation`` -- so the Databricks side read high by an entire pre-existing
    app, and the lane's ``counted_in_installation_total`` said ``True`` beside it.

    The amount is carried here as its own figure so the arithmetic on screen
    works: the lane figures add to the headline, and this is the difference
    between that headline and the larger total beside it.

    Withheld on the same rule as :class:`StandingCostFairness` -- a payload in
    which nothing predates the installation has nothing to disclose, and a zero
    would be a lane figure's bare zero one field along.
    """

    state: Literal["stated", "withheld"]
    components: list[str] = Field(default_factory=list)
    paragraph: str = ""
    derivation: str = ""
    usd_per_hour: float | None = None
    usd_per_day: float | None = None
    withheld_reason: str = ""

    @model_validator(mode="after")
    def validate_claim_is_supported(self) -> StandingCostPredating:
        if self.state == "stated":
            if not self.paragraph.strip() or not self.components:
                raise ValueError("a stated predating disclosure names what it excludes")
            if not self.derivation.strip():
                raise ValueError("a stated predating disclosure carries its derivation")
            if self.usd_per_day is None or self.usd_per_hour is None:
                raise ValueError("the prose and the figures behind it travel together")
            if self.usd_per_day <= 0 or self.usd_per_hour <= 0:
                raise ValueError("an amount that renders as zero is withheld, not stated")
            if self.withheld_reason.strip():
                raise ValueError("a stated disclosure is not withheld")
            return self
        if self.paragraph.strip() or self.usd_per_day is not None:
            raise ValueError("a withheld disclosure must not be rendered anyway")
        if not self.withheld_reason.strip():
            raise ValueError("a withheld disclosure must say what it is missing")
        return self


class StandingCostCredits(BaseModel):
    """What has accrued since the origin, as one snapshot taken server-side.

    ``ticks`` is false and is a constant. This is a figure computed once for an
    injected ``as_of`` -- there is no counter, no clock arithmetic in the browser,
    and nothing here that a renderer is expected to advance.
    """

    as_of: datetime
    origin: datetime
    elapsed_hours: float
    elapsed_display: str
    installation_accrued_usd: float | None = None
    with_platform_accrued_usd: float | None = None
    display: str
    basis: str
    ticks: Literal[False] = False

    @model_validator(mode="after")
    def validate_snapshot_is_explained(self) -> StandingCostCredits:
        if not self.basis.strip():
            raise ValueError("an accrued figure must say what window it accrued over")
        if self.elapsed_hours <= 0:
            raise ValueError("an accrual needs a positive window to have accrued over")
        return self


class StandingCostDisclosure(BaseModel):
    """What this installation is billed for while no bout is running.

    ``origin_field`` is a constant for the same reason ``PricingBasis`` exists:
    the window is measured from ``created_at``, the moment the resources came into
    being. ``expires_at`` is a reaper deadline and ``last_reset_at`` re-seeds data
    without creating anything, so neither one bounds a meter.

    ``seal_state`` has no expired value on purpose. An expired seal does not stop
    billing, so this disclosure does not consult the manifest TTL at all and there
    is no code path for expiry to take.
    """

    run_id: str
    origin: datetime | None = None
    origin_field: Literal["created_at"] = "created_at"
    origin_basis: str
    as_of: datetime
    elapsed_hours: float | None = None
    seal_state: Literal["sealed", "unreadable"]
    seal_detail: str
    shape_basis: Literal["sealed_and_observed", "sealed_shape_only"]
    shape_detail: str
    lanes: list[StandingCostLane] = Field(default_factory=list)
    totals: StandingCostTotals | None = None
    credits: StandingCostCredits | None = None
    posted: StandingCostPosted
    drift: StandingCostDrift
    fairness: StandingCostFairness
    continuous: StandingCostContinuous | None = None
    predating: StandingCostPredating | None = None
    summary: str
    note: str

    @model_validator(mode="after")
    def validate_unreadable_seal_prices_nothing(self) -> StandingCostDisclosure:
        if self.seal_state == "unreadable":
            if self.lanes or self.totals is not None or self.credits is not None:
                raise ValueError("an unreadable seal cannot produce a dollar figure anywhere")
            if self.origin is not None or self.elapsed_hours is not None:
                raise ValueError("an unreadable seal has no origin to measure from")
            return self
        if not self.lanes:
            raise ValueError("a sealed installation bills something and must show its lanes")
        if self.totals is None or self.credits is None:
            raise ValueError("a sealed installation owes both totals and an accrued snapshot")
        return self


class CostLineItem(BaseModel):
    lane_id: Literal["lakebase", "competitor", "shared"]
    component: str
    quantity: float | None = None
    unit: str
    unit_rate_usd: float | None = None
    reference_list_unit_rate_usd: float | None = None
    subtotal_usd: float | None = None
    rate_basis: Literal["standard_list", "current_promotion"] = "standard_list"
    cadence: Literal["bout", "hour", "month", "usage"] = "usage"
    status: Literal["estimate", "usage_pending", "selection_required"]
    scope: Literal[
        "bout_estimate",
        "required_monthly_carrying_cost",
        "installation_overhead",
    ] = "bout_estimate"
    confidence: Literal["high", "medium", "low", "pending"] = "pending"
    quantity_method: Literal[
        "rate_card_pending",
        "exact_session_window",
        "result_evidence",
        "provider_reconciliation",
        "selected_configuration",
        "selection_required",
    ] = "rate_card_pending"
    reconciliation_status: Literal[
        "estimate",
        "pending",
        "reconciled",
        "estimate_only",
        "posted_partial",
        "posted_through_window",
        "corrected",
        "selection_required",
        "attribution_ambiguous",
        "unavailable",
    ] = "pending"
    original_estimate_usd: float | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    posted_cost_usd: float | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    variance_usd: float | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    revision: int | None = Field(default=None, exclude_if=lambda value: value is None)
    queried_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    posted_through: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    observed_from: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    observed_through: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    source: str
    source_as_of: datetime


class CostReceiptSnapshot(BaseModel):
    currency: Literal["USD"] = "USD"
    region: str
    price_basis: Literal["published_on_demand_rates"] = "published_on_demand_rates"
    status: Literal["estimate_usage_pending"] = "estimate_usage_pending"
    lines: list[CostLineItem]
    note: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    reconciled_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    reconciliation_status: Literal[
        "immediate_estimate",
        "partially_reconciled",
        "reconciled",
        "estimate_only",
        "posted_partial",
        "posted_through_window",
        "corrected",
        "selection_required",
        "attribution_ambiguous",
        "unavailable",
    ] = "immediate_estimate"
    original_estimate_usd: float | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    posted_cost_usd: float | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    variance_usd: float | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    revision: int | None = Field(default=None, exclude_if=lambda value: value is None)
    queried_at: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    posted_through: datetime | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    known_bout_estimate_usd: float | None = None
    known_monthly_carrying_cost_usd: float | None = None
    known_installation_overhead_usd: float | None = None


class SessionSnapshot(BaseModel):
    id: str
    state: SessionState
    created_at: datetime
    updated_at: datetime
    competitor: Competitor
    primary_persona: Persona
    secondary_personas: list[Persona]
    corners: list[Corner]
    round: RoundDefinition
    recommendation_reason: str
    presenter_pack: PresenterPack
    lanes: dict[str, LaneSnapshot]
    fairness: FairnessSnapshot = Field(default_factory=FairnessSnapshot)
    capacity: CapacityDisclosure | None = None
    descent_cost: DescentCostDisclosure | None = None
    bout_cost: BoutCostDisclosure | None = None
    standing_cost: StandingCostDisclosure | None = None
    cost_receipt: CostReceiptSnapshot | None = None
    cooldown: CooldownSnapshot | None = None
    towel: TowelSnapshot | None = None
    armed_at: datetime | None = None
    armed_expires_at: datetime | None = None
    run_started_at: datetime | None = None
    remembered_result: str | None = None
    failure: str | None = None
    metric_specs: list[MetricSpec] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
    )
    metrics: list[MetricValue] = Field(
        default_factory=list,
        exclude_if=lambda value: not value,
    )
    comparison: ComparisonSnapshot | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    round5_setup: RoundFiveSetupSnapshot | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    redo: RedoSnapshot | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @model_validator(mode="after")
    def validate_round_five_comparison(self) -> SessionSnapshot:
        if self.round.id != RoundId.SURVIVE_CONNECTION_SPIKE:
            return self
        setup = self.round5_setup
        expected_lanes = {"lakebase", "competitor"}
        if set(self.lanes) != expected_lanes or any(
            lane_id != lane.id for lane_id, lane in self.lanes.items()
        ):
            raise ValueError("Round 5 requires exactly the Lakebase and opponent lanes")
        if (
            setup is None
            or set(setup.lanes) != expected_lanes
            or any(lane_id != lane.id for lane_id, lane in setup.lanes.items())
        ):
            raise ValueError("Round 5 setup requires exactly the Lakebase and opponent lanes")
        if self.towel is not None:
            if self.state != SessionState.TOWELLED:
                raise ValueError("Round 5 towel evidence requires a toweled session")
            if setup.state != RoundFiveSetupState.TOWELLED:
                raise ValueError("Round 5 towel evidence requires towel cleanup state")
            return self
        setup_lanes_verified = all(lane.verified for lane in setup.lanes.values())
        if setup.setup_validated != setup_lanes_verified:
            raise ValueError("Round 5 setup lane verification is incoherent")
        if setup.state == RoundFiveSetupState.CLEANUP_FAILED and (
            not setup.cleanup_retryable or self.state != SessionState.FAILED
        ):
            raise ValueError("Round 5 cleanup retry state is incoherent")
        if setup.cleanup_retryable:
            if self.state == SessionState.FAILED:
                if (
                    setup.state != RoundFiveSetupState.CLEANUP_FAILED
                    or self.comparison is not None
                    or self.remembered_result is not None
                    or bool(self.metrics)
                ):
                    raise ValueError("A failed Round 5 cleanup cannot expose an outcome")
            elif self.state != SessionState.VERIFIED or setup.state != RoundFiveSetupState.VERIFIED:
                raise ValueError("Round 5 cleanup retry may accompany only a sealed outcome")
        if self.comparison is not None and self.comparison.kind not in {
            ComparisonKind.MEASURED,
            ComparisonKind.TIE,
        }:
            raise ValueError("Round 5 permits only measured or tie outcomes")
        main_lanes_verified = all(lane.state == LaneState.VERIFIED for lane in self.lanes.values())
        if main_lanes_verified != (self.state == SessionState.VERIFIED) or (
            self.state != SessionState.VERIFIED
            and any(lane.state == LaneState.VERIFIED for lane in self.lanes.values())
        ):
            raise ValueError("Round 5 lane and session verification states are incoherent")
        if setup.state == RoundFiveSetupState.VERIFIED and (
            self.state != SessionState.VERIFIED
            or not setup.setup_validated
            or not setup.downstream_validated
        ):
            raise ValueError("Round 5 setup verification state is incoherent")
        if self.state == SessionState.VERIFIED and (
            self.failure is not None
            or self.towel is not None
            or setup.state != RoundFiveSetupState.VERIFIED
            or not setup.setup_validated
            or not setup.downstream_validated
            or not all(lane.state == LaneState.VERIFIED for lane in self.lanes.values())
            or self.comparison is None
        ):
            raise ValueError("Round 5 verified state requires the complete two-phase proof")
        if self.comparison is None:
            return self
        if (
            self.state != SessionState.VERIFIED
            or self.failure is not None
            or self.towel is not None
            or not setup.setup_validated
            or not setup.downstream_validated
            or not all(lane.verified for lane in setup.lanes.values())
        ):
            raise ValueError("Round 5 comparison requires both setup lanes and downstream proof")
        if self.comparison.kind == ComparisonKind.MEASURED and (
            self.comparison.winner_lane_id not in setup.lanes
            or self.comparison.margin is None
            or self.comparison.margin.spec_id != "setup_elapsed_ms"
        ):
            raise ValueError("Round 5 measured comparison requires a setup margin")
        if self.comparison.kind == ComparisonKind.TIE and (
            self.comparison.winner_lane_id is not None or self.comparison.margin is not None
        ):
            raise ValueError("Round 5 tie cannot declare a winner or margin")
        return self


class RunEvent(BaseModel):
    sequence: int
    event: str
    occurred_at: datetime
    payload: dict[str, Any]
    # How many events immediately before this one the consumer will never receive,
    # because they were evicted from the in-memory log before it resumed. Absent on
    # every ordinary delivery -- an event that is part of an unbroken run carries no
    # gap -- so the wire shape of a healthy stream is unchanged. A resuming client
    # that ignores it sees the same events it always did; one that reads it can tell
    # a skipped beat from a beat that never happened.
    gap_before: int | None = Field(default=None, exclude_if=lambda value: value is None)

    @classmethod
    def now(cls, sequence: int, event: str, payload: dict[str, Any]) -> RunEvent:
        return cls(
            sequence=sequence,
            event=event,
            occurred_at=datetime.now(UTC),
            payload=payload,
        )
