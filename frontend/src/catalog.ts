import type {
  CatalogResponse,
  CompetitorId,
  CustomerCorner,
  PersonaDefinition,
  PersonaId,
  RoundDefinition,
  RoundId,
} from './api/types'
import { personaPortraits } from './assets'
import { ROUND_FIVE_DISPLAY_TITLE } from './round5'

export interface LocalRecommendation {
  round_id: RoundId
  reason: string
  metric: string
  presenter_opening: string
}

const personaCopy: Array<[PersonaId, string, string, string, string, RoundId | 'inherit_primary_round']> = [
  ['data_engineer', 'Backfill Bill', 'Data Engineer', 'Another pipeline, just to move one value.', 'How quickly can a new value travel from its source to a working application?', 'put_model_score_in_app'],
  ['software_engineer', 'Stacktrace Jack', 'Software Engineer', 'Needs a test database. Files a ticket. Waits.', 'Let us time the entire path to a safely tested database change.', 'make_schema_change_safely'],
  ['data_analyst', 'Count Query', 'Data Analyst', 'Two dashboards, two numbers, one meeting.', 'A number only matters if it is both correct and current.', 'analyze_live_orders_without_slowing_checkout'],
  ['architect_it', 'Major Pattern', 'Architect / IT', 'Wrote the standard. Watched six teams route around it.', 'The fastest path is useless if teams cannot repeat it safely.', 'make_schema_change_safely'],
  ['data_scientist_ml', 'Doctor Drift', 'Data Scientist / ML', 'The score is fine. Nothing is using it.', 'How quickly does a model output become usable by the application?', 'put_model_score_in_app'],
  ['dba', 'Lockjaw Lucy', 'DBA', 'Owns the restore nobody has rehearsed.', 'Available is not recovered; recovered is when the application reads the right data.', 'recover_deleted_order'],
  ['sre', '3 A.M. Sam', 'SRE', 'Paged for a database that says it is fine.', 'The only status that matters is a verified application transaction.', 'wake_idle_app'],
  ['executive', 'The Big Why', 'Executive', 'Has heard “faster” and wants “so what”.', 'Who benefits, why now, and what one number proves the outcome?', 'inherit_primary_round'],
  ['infosec', 'Cipher Viper', 'Infosec', 'Signs off on things built before anyone asked.', 'A workflow is safe only if its control and evidence survive the path.', 'make_schema_change_safely'],
  ['application_owner', 'Launch-Day Lola', 'Application Owner', 'Launch is Thursday. Checkout is the whole product.', 'Ready means the tested database action works—not that infrastructure says it should.', 'wake_idle_app'],
]

export const PERSONAS: PersonaDefinition[] = personaCopy.map(([id, nickname, role, pain, opening, recommended]) => ({
  id,
  nickname,
  pain,
  role,
  portrait: personaPortraits[id],
  source_status: 'bundled_fallback',
  source_slide: null,
  discovery_order: '',
  recommended_rounds: [recommended],
  questions: {},
  presenter: { opening, risk: '', interpretation: '', objection: '', response: '', closing: '' },
}))

const wakeRound: RoundDefinition = {
  id: 'wake_idle_app',
  title: 'Wake this idle app',
  capability: 'Autoscaling and scale-to-zero',
  scorecard_by_corner: {
    cost: 'Published compute and storage rates; billed usage reconciles later',
    simplicity: 'Automatic wake path and time to a verified transaction',
    performance: 'Eligibility to start at zero, then time to verification',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'ready',
  redo: {
    policy: 'show', badge: '★ SHOW', label: 'RE-DO ROUND',
    description: 'Repeat the wake proof to show the same automatic product behavior.',
  },
}

const safeChangeRound: RoundDefinition = {
  id: 'make_schema_change_safely',
  title: 'Make this schema change safely',
  capability: 'Instant branching and isolated change testing',
  scorecard_by_corner: {
    cost: 'Published rates plus developer wait; billed usage reconciles later',
    simplicity: 'Steps and time to an application-verified isolated change',
    performance: 'Time to create, migrate, and verify an isolated environment',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'ready',
  redo: {
    policy: 'optional', badge: 'OPTIONAL', label: 'RE-DO ROUND',
    description: 'Repeat only when the room wants another isolated-change proof.',
  },
}

const recoverRound: RoundDefinition = {
  id: 'recover_deleted_order',
  title: 'Recover this deleted order',
  capability: 'Point-in-time branching and restore',
  scorecard_by_corner: {
    cost: 'Published rates plus recovery wait; billed usage reconciles later',
    simplicity: 'Steps to the agreed recovery point and verified read',
    performance: 'Verified application RTO at the agreed RPO',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'ready',
  redo: {
    policy: 'skip', badge: 'SKIP', label: 'RE-DO ROUND',
    description: 'Hide after success; retain owned recovery cleanup and retry controls after failure.',
  },
}

const modelScoreRound: RoundDefinition = {
  id: 'put_model_score_in_app',
  title: 'Move lakehouse data into live applications',
  capability: 'Managed reverse ETL from Unity Catalog Delta to operational Lakebase Postgres',
  scorecard_by_corner: {
    cost: 'Database list rates captured; required reverse ETL remains unpriced',
    simplicity: 'Analytics Delta to exact operational Postgres application row',
    performance: 'Reverse ETL sync and end-to-end proof time',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'planned',
  metric_specs: [
    { id: 'managed_availability_ms', label: 'Reverse ETL sync', role: 'primary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'application_proof_elapsed_ms', label: 'End-to-end proof', role: 'secondary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'delta_commit_version', label: 'Delta commit version', role: 'guardrail', unit: 'version', direction: 'exact' },
    { id: 'exact_row_verified', label: 'Exact row verified', role: 'guardrail', unit: 'boolean', direction: 'exact' },
  ],
  comparison_kind: 'capability_gap',
  non_claims: [
    'RDS/Aurora are destination databases only; the same outcome requires a separate reverse-ETL stack that must be selected or built, secured, networked, configured, monitored, and operated.',
    'The AWS lane was not executed or timed.',
    'No cross-platform speed comparison or margin is claimed.',
    'No dollar savings are claimed.',
    'No eliminated system is claimed.',
    'No full model-serving capability is claimed.',
  ],
  redo: {
    policy: 'show',
    badge: '★ SHOW',
    label: 'CHANGE SCORE IN LAKEHOUSE → WATCH APP UPDATE',
    description: 'Change this demo’s customer risk score from v1 to v2 in the lakehouse, then watch the same live app record update.',
  },
}

const connectionSpikeRound: RoundDefinition = {
  id: 'survive_connection_spike',
  title: ROUND_FIVE_DISPLAY_TITLE,
  capability: 'Built-in connection pooling',
  scorecard_by_corner: {
    cost: 'Published rates include the AWS opponent’s added RDS Proxy minimum',
    simplicity: 'Built-in pooling vs an AWS best-practice RDS Proxy from the declared start',
    performance: 'Readiness setup is primary; the identical connection spike is pass/fail validation',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'ready',
  metric_specs: [
    { id: 'setup_elapsed_ms', label: 'Setup elapsed', role: 'primary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'successful_clients', label: 'Successful clients', role: 'secondary', unit: 'count', direction: 'higher_is_better' },
    { id: 'application_p99_ms', label: 'Warm-burst application p99', role: 'secondary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'error_clients', label: 'Client errors', role: 'guardrail', unit: 'count', direction: 'lower_is_better' },
  ],
  comparison_kind: 'measured',
  non_claims: [
    'RDS Proxy is AWS best practice for connection spikes. If it is already deployed, its setup delay does not apply. This round compares declared-start readiness, not burst performance.',
    'Lakebase uses its built-in pooled host: 0 separate per-bout pooling components and 0 per-bout pooling infrastructure mutations.',
    'The selected Aurora or RDS lane performs 9 journaled competitor mutations: 1 per-bout Proxy security group, 1 default-egress change, 4 exact security-group rules, 1 RDS Proxy, 1 target-group configuration, and 1 target registration.',
    'IAM service role, runner permission, and dedicated proxy credential secret or secrets are sealed install-time prerequisites outside the setup clock. The AWS design still requires added RDS Proxy, Secrets Manager, IAM, and network configuration.',
    'Warm-burst p99 is secondary validation and is never combined with primary setup elapsed time.',
    'Setup failure or a setup towel produces no winner and no margin.',
    'This is one live proof session, not a benchmark.',
  ],
}

const liveOrdersRound: RoundDefinition = {
  id: 'analyze_live_orders_without_slowing_checkout',
  title: 'Move live application data into the lakehouse',
  capability: 'Built-in change feed (CDF) to separate Delta history',
  scorecard_by_corner: {
    cost: 'Database list rates captured; required AWS CDC stack remains unpriced',
    simplicity: 'Built-in change feed: one checkout to one exact Delta answer',
    performance: 'Commit-to-answer freshness; a separate checkout is the correctness guardrail',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'preview',
}

export const FALLBACK_CATALOG: CatalogResponse = {
  competitors: [
    { id: 'aurora_serverless_v2', name: 'Amazon Aurora PostgreSQL Serverless v2', short_name: 'Aurora Serverless v2', edition: 'AURORA SERVERLESS v2 EDITION' },
    { id: 'rds_postgres', name: 'Amazon RDS for PostgreSQL', short_name: 'RDS PostgreSQL', edition: 'RDS FOR POSTGRESQL EDITION' },
  ],
  personas: PERSONAS,
  corners: ['cost', 'simplicity', 'performance'],
  rounds: [
    wakeRound,
    safeChangeRound,
    recoverRound,
    modelScoreRound,
    connectionSpikeRound,
    liveOrdersRound,
  ],
}

export function withBundledPortraits(catalog: CatalogResponse): CatalogResponse {
  return {
    ...catalog,
    rounds: catalog.rounds.map((round) => round.id === connectionSpikeRound.id
      ? {
          ...round,
          title: connectionSpikeRound.title,
          scorecard_by_corner: connectionSpikeRound.scorecard_by_corner,
          non_claims: connectionSpikeRound.non_claims,
        }
      : round),
    personas: catalog.personas.map((persona) => ({
      ...persona,
      portrait: personaPortraits[persona.id] ?? persona.portrait,
    })),
  }
}

export function recommend(
  catalog: CatalogResponse,
  competitor: CompetitorId,
  corners: CustomerCorner[],
  personaId: PersonaId,
): LocalRecommendation {
  const persona = catalog.personas.find((candidate) => candidate.id === personaId) ?? catalog.personas[0]
  const preferred = persona.recommended_rounds
    .map((id) => catalog.rounds.find((round) => round.id === id))
    .find((round) => round?.availability === 'ready' && round.competitors.includes(competitor))
  const baselineId: RoundId = 'wake_idle_app'
  const selected = preferred ?? catalog.rounds.find((round) => round.id === baselineId) ?? catalog.rounds[0]
  const reason = competitor === 'rds_postgres' && selected.id === 'wake_idle_app'
    ? 'RDS PostgreSQL has no automatic scale-to-zero wake path; its capability is checked before the bell and only Lakebase is timed.'
    : preferred
    ? `Recommended for ${persona.role} and executable for this matchup.`
    : `Selected as the strongest honest matchup; the ${persona.role} lens changes the explanation, not the evidence.`
  return {
    round_id: selected.id,
    reason,
    metric: metricForCorners(selected, corners),
    presenter_opening: persona.presenter.opening,
  }
}

export function metricForCorners(
  round: RoundDefinition,
  corners: CustomerCorner[],
): string {
  const active = corners.length ? corners : ['performance'] satisfies CustomerCorner[]
  if (active.length === 1) return round.scorecard_by_corner[active[0]]

  const measures: Record<CustomerCorner, string> = {
    cost: 'cost inputs',
    simplicity: 'workflow simplicity',
    performance: 'elapsed workflow time',
  }
  const selected = active.map((corner) => measures[corner])
  const list = selected.length === 2
    ? `${selected[0]} and ${selected[1]}`
    : `${selected.slice(0, -1).join(', ')}, and ${selected.at(-1)}`
  return `${list[0].toUpperCase()}${list.slice(1)} to the same verified outcome`
}

export function stopCondition(roundId: RoundId, competitor?: CompetitorId): string {
  if (roundId === 'wake_idle_app' && competitor === 'rds_postgres') {
    return 'Lakebase stops after commit + read-back; RDS eligibility is checked before the bell and not timed.'
  }
  if (roundId === 'wake_idle_app') {
    return 'Each clock stops after commit and read-back of its run-unique value.'
  }
  if (roundId === 'make_schema_change_safely') {
    return 'Each clock stops after the identical migration and transaction verify and the final source check passes.'
  }
  if (roundId === 'recover_deleted_order') {
    return 'Each clock stops after the exact order reads from recovery and remains absent at the final source check.'
  }
  if (roundId === 'put_model_score_in_app') {
    return 'The clock stops only after the committed Delta version is synchronized and a fresh application connection reads the exact operational Postgres row.'
  }
  if (roundId === 'survive_connection_spike') {
    return 'Each clock stops at a verified pooled application path from the declared start. RDS Proxy is AWS best practice; if it is already deployed, this setup delay does not apply. The identical spike must pass on both lanes.'
  }
  if (roundId === 'analyze_live_orders_without_slowing_checkout') {
    return 'The clock stops when the exact committed order appears once in Delta. The result waits for a separate checkout to commit.'
  }
  return 'This planned round is non-executable; it has no verifier or timing boundary.'
}
