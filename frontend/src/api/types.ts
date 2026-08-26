export type CompetitorId = 'aurora_serverless_v2' | 'rds_postgres'
export type PersonaId =
  | 'data_engineer'
  | 'software_engineer'
  | 'data_analyst'
  | 'architect_it'
  | 'data_scientist_ml'
  | 'dba'
  | 'sre'
  | 'executive'
  | 'infosec'
  | 'application_owner'
export type CustomerCorner = 'cost' | 'simplicity' | 'performance'
export type RoundId =
  | 'wake_idle_app'
  | 'make_schema_change_safely'
  | 'recover_deleted_order'
  | 'put_model_score_in_app'
  | 'survive_connection_spike'
  | 'analyze_live_orders_without_slowing_checkout'
// 'unavailable' is built-and-executable but refused right now by live state --
// an unready ring, a credential fault, a swept installation, or a context that
// physically cannot run it. Always accompanied by both `availability_reason`
// and `availability_headline`, and distinct from 'planned' and 'preview',
// which are facts about the build.
export type Availability = 'ready' | 'planned' | 'preview' | 'unavailable'
export type SessionState = 'draft' | 'checking' | 'armed' | 'running' | 'verified' | 'towelled' | 'failed'
export type LaneId = 'lakebase' | 'competitor'
export type LaneState = 'sealed' | 'connecting' | 'verifying' | 'verified' | 'towelled' | 'not_supported' | 'failed'
export type TowelState = 'stopping' | 'cleaning' | 'failed' | 'ready'
export type CooldownState = 'watching' | 'ready' | 'failed'
export type ResetMode = 'return_to_idle' | 'delete_isolated_environment' | 'delete_recovery_environment'
export type CooldownLaneState = 'watching' | 'confirmed_zero' | 'confirmed_deleted' | 'not_supported' | 'failed'
export type MetricRole = 'primary' | 'secondary' | 'guardrail'
export type MetricUnit = 'milliseconds' | 'percent' | 'count' | 'boolean' | 'version'
export type MetricDirection = 'lower_is_better' | 'higher_is_better' | 'exact'
export type ComparisonKind = 'measured' | 'capability_gap' | 'adjudicated_stoppage' | 'tie' | 'not_comparable'
export type RedoState = 'ready' | 'running' | 'verified' | 'failed'

/**
 * Four answers, never three, and never a boolean.
 *
 * `verified_missing` means the account was read and positively reported the
 * sealed resources absent. `unverified` means the account could not be read at
 * all, which is emphatically not the same thing -- and it is what a real
 * sandbox sweep produces, because the sweep deletes the IAM users along with
 * the databases. Collapsing the two is this project's most-repeated defect.
 */
export type InstallationState =
  | 'verified_present'
  | 'verified_missing'
  | 'unverified'
  | 'never_checked'

/** Why recovery is or is not on offer. Branch on this, never on the prose. */
export type RecoveryCode =
  | 'offered'
  | 'deployed'
  | 'unverified'
  | 'never_checked'
  | 'present'
  | 'cleanup_failed'
  | 'mutation_in_progress'
  | 'attempt_running'
  | 'rate_limited'

export type RecoveryPhase = 'spawned' | 'running' | 'succeeded' | 'failed' | 'lost'

export interface RecoveryAttempt {
  attempt_id: string
  phase: RecoveryPhase
  detail: string
  started_at: string
  finished_at: string
  exit_code: number | null
  pid: number | null
  log_tail: string[]
}

export interface RecoveryOffer {
  offered: boolean
  code: RecoveryCode
  refusal: string
  /** Issued by the server. Naming both the generation and the money is the point. */
  confirmation_phrase: string
  usd_per_day: string
  usd_per_day_basis: string
  /** Which of the three things `antidemo setup` would actually do here. */
  plan: string
  attempts_in_window: number
  attempts_allowed: number
}

/**
 * Who the server wrote this payload for.
 *
 * 'viewer' means the deployed app answered somebody who is not the sealed owner,
 * and every prose field below has already been emptied server-side. The banner
 * renders nothing for them: every remedy this surface names is a command on a
 * machine they are not sitting at, and the banner is fixed to the top of a
 * screen that gets projected. Which rounds can run tonight is `/api/catalog`'s
 * answer, on the round-select screen, and it is unaffected by any of this.
 */
export type InstallationAudience = 'operator' | 'viewer'

export interface InstallationStatus {
  /**
   * Optional so that a browser holding a bundle newer than the server falls
   * back to the operator view rather than to silence. That is the safe
   * direction: an absent field then behaves exactly as this screen behaved
   * before the field existed, and the local path -- where every bout has
   * actually been run -- cannot lose its diagnosis to a version skew.
   */
  audience?: InstallationAudience
  state: InstallationState
  detail: string
  sealed_resources: number
  absent_resources: number
  checked: boolean
  checked_seconds_ago: number
  reason: string
  deployed: boolean
  manifest_status: string
  manifest_run_id: string
  transitional_recovery: string
  mutation_in_progress: boolean
  mutation_holder: string
  recovery: RecoveryOffer
  attempt: RecoveryAttempt | null
}

export interface RecoverySpawned {
  attempt_id: string
  pid: number
  log_path: string
  plan: string
  usd_per_day: string
  poll: string
}

export interface MetricSpec {
  id: string
  label: string
  role: MetricRole
  unit: MetricUnit
  direction: MetricDirection
}

export interface MetricValue {
  spec_id: string
  lane_id?: string | null
  value: number | boolean | string
  display_value?: string | null
}

export interface ComparisonSnapshot {
  kind: ComparisonKind
  winner_lane_id?: string | null
  margin?: MetricValue | null
  detail?: string | null
}

export interface RedoPresentation {
  policy: string
  badge: string
  label: string
  description: string
}

export interface CompetitorDefinition {
  id: CompetitorId
  name: string
  short_name: string
  edition: string
}

export interface PresenterCopy {
  opening: string
  risk: string
  interpretation: string
  objection: string
  response: string
  closing: string
}

export interface PersonaDefinition {
  id: PersonaId
  nickname: string
  pain: string
  role: string
  portrait: string
  source_status: string
  source_slide: number | null
  discovery_order: string
  recommended_rounds: RoundId[] | string[]
  questions: Record<string, string>
  presenter: PresenterCopy
}

export interface RoundDefinition {
  id: RoundId
  title: string
  capability: string
  scorecard_by_corner: Record<CustomerCorner, string>
  competitors: CompetitorId[]
  availability: Availability
  // The operator's full account of the refusal: the port, the security group,
  // the exact permission Databricks named. Never the thing a fight card leads
  // with -- see `availability_headline`, which is what the room reads.
  availability_reason?: string | null
  // The same refusal in the demo's own voice. Optional because a browser can
  // outlive the server that answered it; when it is absent the card falls back
  // to the reason, which is what it has always shown.
  availability_headline?: string | null
  metric_specs?: MetricSpec[]
  comparison_kind?: ComparisonKind | null
  non_claims?: string[]
  redo?: RedoPresentation | null
}

export interface CatalogResponse {
  competitors: CompetitorDefinition[]
  personas: PersonaDefinition[]
  corners: CustomerCorner[]
  rounds: RoundDefinition[]
}

export interface BoutStatus {
  scope?: 'global' | 'round'
  round_id?: RoundId | null
  ring_ready: boolean
  can_start?: boolean
  maintenance_state: 'ready' | 'maintenance' | 'blocked'
  maintenance_detail: string | null
  active: boolean
  operator: { display_name: string; email: string | null } | null
  started_at: string | null
  updated_at: string | null
  expires_at: string | null
  phase: string | null
  state: SessionState | null
  round_title: string | null
  competitor: string | null
}

export interface PresenterLens {
  persona_id: PersonaId
  nickname: string
  role: string
  interpretation: string
  objection: string
  response: string
}

export interface PresenterPack {
  opening: string
  discovery_question: string
  risk: string
  stop_condition: string
  remembered_metric: string
  primary: PresenterLens
  secondary: PresenterLens[]
  closing: string
}

export interface LaneActivity {
  phase: string
  wire_call: string | null
  recovery_at?: string | null
}

export interface LaneSnapshot {
  id: LaneId
  name: string
  state: LaneState
  elapsed_ms: number | null
  /** Server-derived current floor for an active timed lane at serialization. */
  elapsed_at_snapshot_ms?: number | null
  attempts: number
  status: string
  error: string | null
  verified_at?: string | null
  activity?: LaneActivity | null
  evidence?: Record<string, unknown>
}

export interface RedoSnapshot {
  state: RedoState
  lanes: Record<LaneId, LaneSnapshot>
  metric_specs?: MetricSpec[]
  metrics?: MetricValue[]
  comparison?: ComparisonSnapshot | null
  failure?: string | null
  started_at?: string | null
  completed_at?: string | null
}

export interface CooldownLaneSnapshot {
  id: LaneId
  name: string
  state: CooldownLaneState
  started_at: string
  confirmed_at: string | null
  elapsed_ms: number | null
  status: string
  activity?: LaneActivity | null
}

export interface CooldownSnapshot {
  mode: ResetMode
  state: CooldownState
  started_at: string
  lanes: Record<LaneId, CooldownLaneSnapshot>
  failure: string | null
}

export interface TowelSnapshot {
  state: TowelState
  requested_at: string
  cutoff_ms?: number
  censored_lower_bounds_ms?: Partial<Record<LaneId, number>>
  /** Transitional Round 3 fields accepted until old persisted sessions age out. */
  active_lane?: LaneId
  lower_bound_ms?: number
  lakebase_verified_ms?: number
  restore_started: boolean
  cleanup_failure: string | null
  /** Absent unless the towel could not close the bout's cost window. */
  cost_close_failure?: string | null
}

export interface FairnessSnapshot {
  same_client: boolean
  same_transaction: boolean
  same_nonce: boolean
  launch_skew_ms: number | null
  warmup_connections?: number
  concurrency?: number
  runner?: string
  tls?: string
  timeout?: string
}

/** How a disclosed capacity figure was obtained. `observed` was read back from
 *  the live control plane during arming; `configured` is what the installer
 *  applies; `unreported` means the plane returned nothing for it. */
export type CapacityBasis = 'observed' | 'configured' | 'unreported'

export interface CapacityLaneDisclosure {
  lane_id: LaneId
  product: string
  configured: string
  memory: string
  engine_version: string
  idle_policy: string
  basis: CapacityBasis
  max_connections?: number | null
}

export interface CapacityDisclosureSnapshot {
  lanes: CapacityLaneDisclosure[]
  matched: boolean
  summary: string
  note: string
}

/**
 * What one return to idle is billed, per lane.
 *
 * `descends` picks the shape of the answer. An engine that descends pays a floor
 * once per descent, so the per-descent figures are populated. Provisioned RDS
 * cannot descend, so those figures are null -- never zero -- and `per_day_display`
 * carries a whole billed day instead.
 */
export interface DescentCostLane {
  lane_id: LaneId
  product: string
  descends: boolean
  floor_label: string
  per_descent_low_usd?: number | null
  per_descent_high_usd?: number | null
  per_descent_display: string
  per_descent_headline: string
  per_day_display: string
  derivation: string
  rate_source: string
  band_reason: string
}

export interface DescentCostSnapshot {
  lanes: DescentCostLane[]
  floor_ratio_label: string
  illustrative_descents_per_day: number
  summary: string
  note: string
}

/**
 * One round's measured Aurora-lane marginal cost.
 *
 * `provenance` decides how the row may be read, and the three values are not
 * interchangeable. `measured` is a CloudWatch integral. `structural_zero` is an
 * exact $0.00 -- Terraform stands no Aurora cluster up for the round, so there
 * is nothing to bill -- and it is the only case in which this panel prints a
 * zero. `unavailable` is a quantity that could not be established, and it must
 * never render as $0.00, because a reader cannot tell a real zero from a failed
 * lookup once both print the same glyph.
 *
 * `band_kind` separates two bands that look identical on screen and mean
 * different things. `observed_spread` is the range two real bouts of the same
 * round actually spanned. `unresolved_billing_question` is a question AWS has
 * not answered -- whether capacity reported by an instance in `deleting` is
 * billed -- with both ends observed and neither settled. Collapsing either to a
 * point would be picking an answer nobody gave, so `usd_display` keeps the
 * range for both.
 */
export interface BoutCostRound {
  round_id: RoundId
  round_number: number
  label: string
  provenance: 'measured' | 'structural_zero' | 'unavailable'
  band_kind: 'single_bout' | 'observed_spread' | 'unresolved_billing_question' | 'exact_zero'
  usd_display: string
  usd_low?: number | null
  usd_high?: number | null
  derivation: string
  band_reason: string
  bouts: string[]
}

/**
 * The Aurora lane's per-round marginal cost, measured rather than modelled.
 *
 * `superseded_display` carries the figure this replaces rather than dropping
 * it: the correction is 2.6-2.8x, so an audience may have written the old
 * number down and deserves to see it retired rather than silently swapped.
 *
 * The two superlative fields exist separately, and both name their lane on
 * purpose. Round 5 is the dearest round against Aurora and the cheapest on
 * Lakebase; both are measured and both are true, and a panel that said only one
 * of them would contradict the other panel on the same screen.
 */
export interface BoutCostSnapshot {
  rounds: BoutCostRound[]
  total_display: string
  superseded_display: string
  dearest_claim: string
  lakebase_lane_claim: string
  summary: string
  scope_note: string
  note: string
  rate_source: string
}

export type StandingCostLaneId =
  | 'rds'
  | 'aurora'
  | 'lakebase'
  | 'rds_proxy'
  | 'neutral_runner'
  | 'databricks_platform'

/**
 * One standing-cost figure, or an explicit absence -- never a bare zero.
 *
 * `state` decides how the figure may be rendered and the three values are not
 * interchangeable. `priced` carries a real amount whose `display` is guaranteed
 * to contain a non-zero digit, so a figure small enough to round away still
 * prints as something: Lakebase storage is a third of a cent a day.
 * `structural_zero` is the only permitted zero, and `zero_basis` is guaranteed
 * to appear inside `display` -- so a renderer that prints `display` cannot show
 * the zero without the reason for it, and stripping the basis in the view would
 * put the bare `$0.00` back. `unavailable` is missing data and renders as the
 * word, never as a figure.
 */
export interface StandingCostFigure {
  state: 'priced' | 'structural_zero' | 'unavailable'
  usd_per_hour?: number | null
  usd_per_day?: number | null
  display: string
  derivation: string
  zero_basis: string
  rate_source: string
}

/**
 * One priced line inside a lane.
 *
 * `predates_installation` is what makes the two totals separable: a component
 * that would bill whether or not this run existed is not something the
 * installation created. The app's own compute is the case that matters.
 */
export interface StandingCostComponent {
  component: string
  cloud: 'aws' | 'databricks'
  kind: 'compute' | 'storage' | 'network' | 'other'
  provenance: 'measured' | 'modeled' | 'assumed' | 'unavailable'
  quantity_basis: string
  predates_installation: boolean
  figure: StandingCostFigure
}

/**
 * One of the six lanes that bill with no bout running.
 *
 * Six rather than four. The neutral runner belongs to neither corner and the
 * platform lane is neither corner's engine, so dropping either leaves a total
 * that does not reconcile. No share of the bill is quoted for either here: the
 * fraction this comment used to give the platform lane was computed before that
 * lane stopped counting compute predating the installation, after which it
 * described neither total.
 *
 * `figure` is the lane's share of `totals.installation` — the total the panel
 * headlines — so the lane figures on screen add to it. Compute that predates the
 * installation is held out and disclosed by `StandingCostPredating` instead, and
 * `counted_in_installation_total` says whether this lane's figure is one of the
 * addends. `evidence` is what kind of number the lane is, which is not the same
 * as how big it is.
 */
export interface StandingCostLane {
  lane_id: StandingCostLaneId
  product: string
  side: 'lakebase' | 'competitor' | 'shared' | 'platform'
  idle_label: string
  figure: StandingCostFigure
  components: StandingCostComponent[]
  evidence:
    | 'rate_card_derived'
    | 'posted_actual'
    | 'posted_projection'
    | 'sealed_shape_only'
    | 'unpriced'
  rate_source: string
  caveat: string
  counted_in_installation_total: boolean
  counted_in_platform_total: boolean
}

/** One total, inseparable from the condition it holds under. */
export interface StandingCostTotal {
  label: string
  usd_per_hour: number
  usd_per_day: number
  display: string
  condition: string
  lane_ids: StandingCostLaneId[]
  partial: boolean
  partial_reason: string
}

/**
 * Both totals or neither. `installation` alone understates what the account is
 * spending; `with_platform` alone attributes an app that predates this
 * installation to it.
 */
export interface StandingCostTotals {
  installation: StandingCostTotal
  with_platform: StandingCostTotal
}

/**
 * Posted Databricks actuals, never blended into the projection.
 *
 * Three figures, not two. `projection_usd` covers the whole disclosure window,
 * `projection_in_posted_window_usd` only the part the posted read covers, and
 * the variance is computed against the second alone. AWS has no posted
 * counterpart at all, which is why `aws_posted` is a constant that says so.
 */
export interface StandingCostPosted {
  state: 'posted_through_window' | 'unavailable'
  cloud: 'databricks'
  source: string
  window_start?: string | null
  window_end?: string | null
  posted_usd?: number | null
  projection_usd?: number | null
  projection_in_posted_window_usd?: number | null
  variance_usd?: number | null
  variance_fraction?: number | null
  posted_hours?: number | null
  unposted_hours?: number | null
  unposted_basis: string
  comparison_basis: string
  explanation: string
  aws_posted: 'no_posted_counterpart'
  aws_posted_basis: string
  unavailable_reason: string
  display: string
}

export interface StandingCostDriftFinding {
  code: string
  kind: string
  identifier: string
  detail: string
  usd_per_day?: number | null
  accrued_usd?: number | null
  accrual_basis: string
  rate_basis: string
  charging_for_absent: boolean
}

/**
 * Unexpected accrual, kept separate from the sealed kind and never summed into
 * the headline: their sum answers neither question.
 */
export interface StandingCostDrift {
  state: 'sealed_shape_holds' | 'unexpected_accrual' | 'unavailable'
  badge: string
  summary: string
  unexpected_usd_per_day?: number | null
  unexpected_accrued_usd?: number | null
  findings: StandingCostDriftFinding[]
  separation_note: string
  unavailable_reason: string
}

/**
 * The fairness paragraph, with its figures derived rather than typed.
 *
 * `withheld` must render nothing at all. The paragraph asserts that our half is
 * the larger one, and the server withholds it rather than rewording it when
 * that claim stops holding -- so a view that substituted its own prose here
 * would reinstate exactly the claim the server declined to make.
 */
export interface StandingCostFairness {
  state: 'stated' | 'withheld'
  paragraph: string
  withheld_reason: string
}

/**
 * The one standing line the panel says is deliberate, and what it costs.
 *
 * A synced-table pipeline scheduled `continuous: true` bills a full day's rate
 * for a round that runs for minutes while it is up, and it is the largest single
 * line on the Databricks side. `continuous` governs how it syncs while running,
 * not whether it stays running: it is started when a round arms and stopped once
 * that bout has settled, so `usd_per_day` is a rate conditional on the pipeline
 * being up rather than an unconditional standing charge. Continuous governed
 * sync is what the round demonstrates -- but a reader who works the arithmetic
 * out unaided concludes the opposite of what this demo argues, and concludes it
 * correctly from the figures on screen. So the server states it, with its
 * condition attached.
 *
 * Optional because it postdates the recorded payload the frontend tests import.
 * `withheld` renders nothing, on the same rule as the fairness paragraph: the
 * claim is a share of a half, and a view that substituted prose for a missing
 * denominator would state exactly what the server declined to.
 */
export interface StandingCostContinuous {
  state: 'stated' | 'withheld'
  component: string
  paragraph: string
  derivation: string
  usd_per_hour?: number | null
  usd_per_day?: number | null
  share_of_databricks?: number | null
  withheld_reason: string
}

/**
 * Compute that would bill without this run, disclosed instead of folded in.
 *
 * A workspace app that was serving before this run existed belongs to
 * `totals.with_platform` and to no lane figure a reader is asked to add up. It
 * used to be summed into the platform lane's subtotal, which left the six
 * rendered lane figures adding to `with_platform` while the panel headlined
 * `installation` -- the Databricks side reading high by a whole pre-existing app,
 * next to a `counted_in_installation_total` that said otherwise. The amount is
 * carried on its own so the arithmetic on screen works, and it is the difference
 * between the two totals.
 *
 * Optional because it postdates the recorded payload the frontend tests import,
 * and `withheld` renders nothing -- a payload where nothing predates the
 * installation has nothing to disclose.
 */
export interface StandingCostPredating {
  state: 'stated' | 'withheld'
  components: string[]
  paragraph: string
  derivation: string
  usd_per_hour?: number | null
  usd_per_day?: number | null
  withheld_reason: string
}

/**
 * What has accrued since the origin, as one snapshot taken server-side.
 *
 * `ticks` is `false` and is a constant. There is no counter here: the figure is
 * computed once for an injected `as_of` and is only ever as recent as the
 * `as_of` beside it. Nothing in this payload is meant to advance, so nothing
 * rendering it should set an interval.
 */
export interface StandingCostCredits {
  as_of: string
  origin: string
  elapsed_hours: number
  elapsed_display: string
  installation_accrued_usd?: number | null
  with_platform_accrued_usd?: number | null
  display: string
  basis: string
  ticks: false
}

/**
 * What this installation is billed for while no bout is running.
 *
 * `seal_state` has no expired value on purpose: an expired seal does not stop
 * billing, so the disclosure never consults the manifest TTL. When the seal is
 * `unreadable` there are no lanes, no totals and no credits -- an unreadable
 * manifest is not a free installation, and the payload carries no dollar figure
 * anywhere rather than a zero.
 */
export interface StandingCostDisclosure {
  run_id: string
  origin?: string | null
  origin_field: 'created_at'
  origin_basis: string
  as_of: string
  elapsed_hours?: number | null
  seal_state: 'sealed' | 'unreadable'
  seal_detail: string
  shape_basis: 'sealed_and_observed' | 'sealed_shape_only'
  shape_detail: string
  lanes: StandingCostLane[]
  totals?: StandingCostTotals | null
  credits?: StandingCostCredits | null
  posted: StandingCostPosted
  drift: StandingCostDrift
  fairness: StandingCostFairness
  continuous?: StandingCostContinuous | null
  predating?: StandingCostPredating | null
  summary: string
  note: string
}

export interface CostLineItem {
  lane_id: LaneId | 'shared'
  component: string
  quantity: number | null
  unit: string
  unit_rate_usd: number | null
  reference_list_unit_rate_usd: number | null
  subtotal_usd: number | null
  rate_basis: 'standard_list' | 'current_promotion'
  cadence: 'bout' | 'hour' | 'month' | 'usage'
  status: 'estimate' | 'usage_pending' | CostReconciliationStatus
  scope?: CostScope
  confidence?: 'high' | 'medium' | 'low' | 'pending'
  quantity_method?: 'rate_card_pending' | 'exact_session_window' | 'result_evidence' | 'provider_reconciliation' | 'selected_configuration' | 'selection_required'
  reconciliation_status?: LegacyCostReconciliationStatus | CostReconciliationStatus
  observed_from?: string | null
  observed_through?: string | null
  original_estimate_usd?: CostValue | null
  posted_cost_usd?: CostValue | null
  variance_usd?: CostValue | null
  revision?: number | null
  queried_at?: string | null
  posted_through?: string | null
  source: string
  source_as_of: string
}

export type CostValue = number | string
export type CostScope = 'bout_estimate' | 'required_monthly_carrying_cost' | 'installation_overhead'
export type CostReconciliationStatus = 'estimate_only' | 'posted_partial' | 'posted_through_window' | 'corrected' | 'selection_required' | 'attribution_ambiguous' | 'unavailable'
export type LegacyCostReconciliationStatus = 'estimate' | 'pending' | 'reconciled' | 'immediate_estimate' | 'partially_reconciled'

export interface CostReceiptSnapshot {
  currency: 'USD'
  region: string
  price_basis: 'published_on_demand_rates'
  status: 'estimate_usage_pending' | CostReconciliationStatus
  lines: CostLineItem[]
  note: string
  generated_at?: string
  reconciled_at?: string | null
  reconciliation_status?: LegacyCostReconciliationStatus | CostReconciliationStatus
  known_bout_estimate_usd?: CostValue | null
  known_monthly_carrying_cost_usd?: CostValue | null
  known_installation_overhead_usd?: CostValue | null
  original_estimate_usd?: CostValue | null
  posted_cost_usd?: CostValue | null
  variance_usd?: CostValue | null
  revision?: number | null
  queried_at?: string | null
  posted_through?: string | null
}

export type RoundFiveSetupState = 'pending' | 'running' | 'verified' | 'failed' | 'towelled' | 'cleanup_failed'
export type PublicSetupEvidenceValue = string | number | boolean | null

export interface PublicSetupEvidence {
  key: string
  value: PublicSetupEvidenceValue
}

export interface SetupStopGateEvidence {
  gate_id: string
  expected: PublicSetupEvidence[]
  observed: PublicSetupEvidence[]
  exact: boolean
}

/** Sanitized browser projection. Absolute clocks, workflow IDs, and internal failures stay server-side. */
export interface SetupLaneResult {
  id: LaneId
  name: string
  state: RoundFiveSetupState
  /** Exact progress callback latch; terminal setup evidence is scored from this value. */
  setup_elapsed_ms: number | null
  /**
   * Server-derived floor when this snapshot was serialized. Active lanes may
   * advance beyond the callback latch during silent provider waits.
   */
  elapsed_at_snapshot_ms?: number | null
  status: string
  stop_gate_evidence: SetupStopGateEvidence | null
  verified: boolean
}

export interface SetupPhaseResult {
  state: RoundFiveSetupState
  workflow_launch_skew_ms: number | null
  lanes: Partial<Record<LaneId, SetupLaneResult>>
  setup_validated: boolean
  downstream_validated: boolean
  cleanup_retryable?: boolean
  /**
   * Why the backstage cleanup was given up on, in the server's own words.
   *
   * `cleanup_retryable` cannot carry this: it is true while retries are still
   * running as well as after they have been abandoned, so it leaves the two
   * indistinguishable. This is set only once the server stops retrying, and
   * cleared when cleanup genuinely completes -- which makes it the field that
   * says a run-owned resource was never proved gone.
   */
  cleanup_failure?: string | null
}

export interface DemoSession {
  id: string
  state: SessionState
  created_at: string
  updated_at: string
  competitor: CompetitorDefinition
  primary_persona: PersonaDefinition
  secondary_personas: PersonaDefinition[]
  corners: CustomerCorner[]
  round: RoundDefinition
  recommendation_reason: string
  presenter_pack: PresenterPack
  lanes: Record<LaneId, LaneSnapshot>
  fairness: FairnessSnapshot
  capacity?: CapacityDisclosureSnapshot | null
  descent_cost?: DescentCostSnapshot | null
  bout_cost?: BoutCostSnapshot | null
  standing_cost?: StandingCostDisclosure | null
  cost_receipt?: CostReceiptSnapshot | null
  cooldown?: CooldownSnapshot | null
  towel?: TowelSnapshot | null
  armed_at?: string | null
  armed_expires_at?: string | null
  run_started_at?: string | null
  remembered_result: string | null
  failure: string | null
  metric_specs?: MetricSpec[]
  metrics?: MetricValue[]
  comparison?: ComparisonSnapshot | null
  round5_setup?: SetupPhaseResult | null
  redo?: RedoSnapshot | null
}

export interface CreateSessionRequest {
  competitor: CompetitorId
  primary_persona: PersonaId
  secondary_personas: PersonaId[]
  corners: CustomerCorner[]
  round_id: RoundId | null
}

export interface ApiProblem {
  status: number
  title: string
  detail?: string
}

export type EventName =
  | 'session_created'
  | 'arm_started'
  | 'arm_waiting'
  | 'armed'
  | 'session_cancelled'
  | 'run_preparing'
  | 'run_started'
  | 'lane_update'
  | 'run_finished'
  | 'session_failed'
  | 'towel_started'
  | 'towel_update'
  | 'towel_finished'
  | 'cleanup_update'
  | 'cooldown_started'
  | 'cooldown_update'
  | 'cooldown_ready'
  | 'redo_started'
  | 'redo_lane_update'
  | 'redo_finished'
  | 'redo_failed'

type EventPayloads = {
  session_created: { session: DemoSession }
  arm_started: { state: 'checking' }
  arm_waiting: { state: 'checking'; status: string }
  armed: { state: 'armed'; evidence: Record<string, unknown>; session: DemoSession }
  session_cancelled: { state: 'failed'; message: string; session: DemoSession }
  run_preparing: { state: 'armed' }
  run_started: { state: 'running'; lanes: LaneId[]; session: DemoSession }
  lane_update:
    | {
        session: DemoSession
        lane_id?: LaneId
        state?: LaneState
        attempts?: number
        elapsed_ms?: number | null
        status?: string
        error?: string | null
        activity?: LaneActivity | null
      }
    | {
        session?: never
        lane_id: LaneId
        state: LaneState
        attempts?: number
        elapsed_ms?: number | null
        status?: string
        error?: string | null
        activity?: LaneActivity | null
      }
  run_finished: { state: 'verified' | 'failed'; session: DemoSession }
  session_failed: { state: 'failed'; message: string; session: DemoSession }
  towel_started: { session: DemoSession }
  towel_update: { session: DemoSession }
  towel_finished: { session: DemoSession }
  cleanup_update: { session: DemoSession }
  cooldown_started: { cooldown: CooldownSnapshot }
  cooldown_update: { cooldown: CooldownSnapshot }
  cooldown_ready: { cooldown: CooldownSnapshot }
  redo_started: { session: DemoSession }
  redo_lane_update: { lane_id: LaneId; state: LaneState; attempts?: number; elapsed_ms?: number | null; status?: string; error?: string | null; activity?: LaneActivity | null }
  redo_finished: { session: DemoSession }
  redo_failed: { message: string; session: DemoSession }
}

export type RunEvent = {
  [Name in EventName]: {
    sequence: number
    event: Name
    occurred_at: string
    payload: EventPayloads[Name]
    /**
     * How many events immediately before this one were evicted from the server's
     * in-memory log and will never arrive. Absent on every ordinary delivery. It
     * appears only on the first event of a resume that had to skip past the
     * server's retention floor, so a consumer can tell a missed beat from a beat
     * that never happened. Sequence numbers stay monotonic either way.
     */
    gap_before?: number | null
  }
}[EventName]
