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
import { ROUND_FIVE_BELL_PROTOCOL, ROUND_FIVE_DISPLAY_TITLE, ROUND_FIVE_MAX_RETRIES } from './round5'

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

// Mirrors server/catalog.py. Round 4 has no re-do: a second change could not start
// both integrations cold, so racing again is a new bout.
const modelScoreRound: RoundDefinition = {
  id: 'put_model_score_in_app',
  title: 'Move lakehouse data into live applications',
  capability: 'Managed reverse ETL from Unity Catalog Delta to operational Lakebase Postgres, raced against an AWS Glue job writing the same change into Aurora or RDS',
  scorecard_by_corner: {
    cost: 'Published rates: the synced-table pipeline and the Glue job each bill only while a bout runs',
    simplicity: 'One synced table against a Glue job, its role, network, connection, ledger and checkpoint',
    performance: 'Bell to the exact row in the app, with both integrations cold starting at the bell',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'planned',
  metric_specs: [
    { id: 'bell_to_exact_read_ms', label: 'Bell to the exact row in the app', role: 'primary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'managed_availability_ms', label: 'Reverse ETL sync (Lakebase’s own timestamps)', role: 'secondary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'delta_commit_version', label: 'Delta commit version', role: 'guardrail', unit: 'version', direction: 'exact' },
    { id: 'exact_row_verified', label: 'Exact row verified', role: 'guardrail', unit: 'boolean', direction: 'exact' },
  ],
  comparison_kind: 'measured',
  non_claims: [
    'Both integrations cold start at the bell, and each lane’s clock contains its own start. Neither is warmed for the audience.',
    'AWS moves the AWS lane’s data: an AWS Glue 5.0 job reads the Delta table’s files straight from S3, around Unity Catalog’s permissions, lineage and audit, and writes over JDBC. The supported routes for an outside engine (credential vending, Iceberg REST, Delta Sharing) would each put Databricks back in the lane.',
    'One change, one verifier: one Delta commit feeds both lanes, and each lane is read by the same query on the same client every 250 ms.',
    'The Glue job, its role, network and connection are installed once and standing.',
    'This is one live proof session, not a benchmark.',
    'No dollar savings are claimed.',
    'No full model-serving capability is claimed.',
  ],
}

const connectionSpikeRound: RoundDefinition = {
  id: 'survive_connection_spike',
  title: ROUND_FIVE_DISPLAY_TITLE,
  capability: 'Included pooling compared with a selected AWS managed pooling path',
  scorecard_by_corner: {
    cost: 'Published rates include the new RDS Proxy selected for the AWS reference path',
    simplicity: 'Included Lakebase pooled endpoint versus 3 timed Proxy mutations on the selected AWS managed pooling path',
    performance: 'One server bell to observed exact 10,000 held client connections per lane; runner-local ramp time and peak backend sessions are supporting evidence',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'ready',
  round5_protocol: ROUND_FIVE_BELL_PROTOCOL,
  metric_specs: [
    { id: 'bell_to_10000_observed_ms', label: 'Bell to 10,000 held clients', role: 'primary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'setup_elapsed_ms', label: 'Pooled-path setup time', role: 'secondary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'peak_backend_sessions', label: 'Peak backend sessions', role: 'secondary', unit: 'count', direction: 'lower_is_better' },
    { id: 'held_clients_at_gate', label: 'Clients held at the gate', role: 'guardrail', unit: 'count', direction: 'exact' },
    { id: 'terminal_failures', label: 'Client failures', role: 'guardrail', unit: 'count', direction: 'lower_is_better' },
  ],
  comparison_kind: 'measured',
  non_claims: [
    'Lakebase dispatches its first retained pooled client immediately at the bell. Native login, ordinary roles, capacity, credentials, and endpoint bindings are prepared automatically backstage.',
    'The selected AWS path performs 3 journaled timed mutations: CreateDBProxy first, exact target-group configuration, and exact target registration. Least-privilege Proxy network fixtures stand warm but the Proxy does not.',
    'The IAM service role, runner permission, and dedicated proxy credential secret or secrets are sealed install-time prerequisites outside the setup clock. RDS Proxy is the AWS managed pooling option selected for this reference path, not a universal Aurora or RDS requirement. Direct connections, an existing RDS Proxy, PgBouncer, and application pooling were not compared.',
    `Each independent lane opens exactly 10,000 authenticated retained clients, holds them for 30 seconds, and may spend up to ${ROUND_FIVE_MAX_RETRIES} retried logins during establishment. All 64 sampled queries must succeed with no sample retries. 9,999 fails. Each lane owns a distinct physical c7i.2xlarge runner.`,
    'Multiplexing is read during the hold by a second database role on its own direct connection, never inferred from the client count. Connect latency p50, p95 and p99 are nearest-rank over raw, unrounded per-client measurements. Runner-local ramp time is supporting evidence; the large comparison clock is always server-observed bell to exact 10,000 held.',
    'Lakebase’s built-in PgBouncer product limit is up to 10,000 client connections, not PostgreSQL backend sessions or simultaneous transactions. The bout holds exactly that many per lane and proves multiplexing while they are held. Direct AWS connections, an existing Proxy, sustained throughput, and storm resilience remain outside this comparison.',
    'This is one live proof session, not a benchmark.',
  ],
}

// Mirrors server/catalog.py. Round 6 races AWS DMS and Glue, cold at the bell, against
// Lakebase's built-in change feed, which is always on.
const liveOrdersRound: RoundDefinition = {
  id: 'analyze_live_orders_without_slowing_checkout',
  title: 'Move live application data into the lakehouse',
  capability: 'Lakebase’s built-in change feed into Delta, raced against AWS DMS capturing the same checkout from Aurora or RDS and an AWS Glue job appending it to Delta',
  scorecard_by_corner: {
    cost: 'Published rates: the DMS instance stands, and the Glue job bills only while a bout runs',
    simplicity: 'One built-in change feed against a DMS instance, task and endpoints, a Glue job, its role, network, bucket and checkpoint',
    performance: 'Bell to the exact order in the lakehouse, with AWS DMS and Glue cold starting at the bell',
  },
  competitors: ['aurora_serverless_v2', 'rds_postgres'],
  availability: 'preview',
  metric_specs: [
    { id: 'bell_to_exact_history_ms', label: 'Bell to the exact order in the lakehouse', role: 'primary', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'commit_skew_ms', label: 'Checkout commit skew between the lanes', role: 'guardrail', unit: 'milliseconds', direction: 'lower_is_better' },
    { id: 'exact_order_verified', label: 'Exact order verified', role: 'guardrail', unit: 'boolean', direction: 'exact' },
    { id: 'checkout_verified', label: 'Checkout guardrail', role: 'guardrail', unit: 'boolean', direction: 'exact' },
  ],
  comparison_kind: 'measured',
  non_claims: [
    'AWS DMS and Glue cold start at the bell, and the AWS lane’s clock contains their start. Lakebase’s change feed is built into the database and always on, so its side has nothing to start. What It Cost shows what keeping AWS’s pipeline running all day would cost.',
    'AWS moves the AWS lane’s data: DMS captures the checkout from the database’s write-ahead log into S3, and an AWS Glue 5.0 job appends it to a Delta table that Unity Catalog reads as an external table.',
    'One checkout, one verifier: the bell commits the same order on both sources, and each lane’s Delta history is read by the same query on the same SQL warehouse every second.',
    'Each history keeps its own shape: Lakebase’s feed writes a change type and LSN, and DMS writes an operation and commit timestamp. The claim is the order’s delivery, not identical tables.',
    'The DMS instance, task and endpoints, the Glue job, its role, network and bucket are installed once and standing.',
    'This is one live proof session, not a benchmark.',
    'No dollar savings are claimed.',
  ],
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

export function stopCondition(
  roundId: RoundId,
  competitor?: CompetitorId,
  fanIn = false,
): string {
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
    return 'The bell cold starts both integrations and commits one Delta change. Each lane’s clock stops at its first application read of the exact row, polled every 250 ms on both lanes.'
  }
  if (roundId === 'survive_connection_spike') {
    if (fanIn) {
      // Setup dominates the AWS lane and that is the finding, not a handicap:
      // an RDS Proxy is a decision you have to make and provision before you
      // need it, and Lakebase's pool is simply already there. The lanes are not
      // expected to reach 10,000 at the same moment.
      return `Lakebase verifies its included pool; the selected AWS path must first provision an RDS Proxy, which is most of its clock. Each lane then holds exactly 10,000 authenticated client connections from a shared start, keeps them for a 30-second hold, and may spend up to ${ROUND_FIVE_MAX_RETRIES} retried logins during establishment. All 64 verification samples must succeed with no sample retries. 9,999 fails.`
    }
    return 'Each pooled-path setup clock stops at an exact application transaction from the database-only declared start. Lakebase verifies its included pool; the selected AWS managed pooling path provisions a new RDS Proxy. The 128-attempt, maximum-64-concurrent check and separate 64-client witness must then pass.'
  }
  if (roundId === 'analyze_live_orders_without_slowing_checkout') {
    return 'The bell commits the same checkout on both sources and cold starts AWS DMS and Glue. Each lane’s clock stops at its first read of the exact order, once, in its own Delta history, polled every second on both lanes. A separate checkout must commit on each source.'
  }
  return 'This planned round is non-executable; it has no verifier or timing boundary.'
}
