import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import type {
  CompetitorId,
  CustomerCorner,
  DemoSession,
  PersonaId,
  RoundId,
} from './api/types'
import {
  linkedInReceipt,
  receiptPresentation,
  scorecardEntry,
} from './App'
import { FALLBACK_CATALOG } from './catalog'
import {
  LENS_LINE_RECORDS,
  LENS_LINES_SHA256,
  PERSONA_IDS,
  PRIORITIES,
  RESULT_LINE_RECORDS,
  RESULT_LINES_SHA256,
  ROLE_LINE_RECORDS,
  ROLE_LINES_SHA256,
  ROUND_IDS,
  buildRingsideBriefing,
  buildRingsideResult,
  classifyEvidence,
  classifyOutcome,
  classifyRingsideOutcome,
  getLensLine,
  getResultLine,
  getRoleLine,
  interpolateProof,
  orderedPriorities,
  type RingsideOutcomeId,
} from './ringside-cues'

/** The seven non-empty priority selections a room can make. */
const PRIORITY_SETS: readonly (readonly CustomerCorner[])[] = [
  ['cost'],
  ['simplicity'],
  ['performance'],
  ['cost', 'simplicity'],
  ['cost', 'performance'],
  ['simplicity', 'performance'],
  ['cost', 'simplicity', 'performance'],
]

const words = (text: string) => text.trim().split(/\s+/).length

const sourcePath = (name: string) => join(import.meta.dirname, 'ringside-cues', name)
const sourceText = (name: string) => readFileSync(sourcePath(name), 'utf8')
const parsedSource = <T,>(name: string): T[] => sourceText(name)
  .split(/\r?\n/)
  .filter(Boolean)
  .map((line) => JSON.parse(line) as T)
const sha256 = (value: string) => createHash('sha256').update(value).digest('hex')

function verifiedSession(
  roundId: RoundId,
  competitorId: CompetitorId = 'aurora_serverless_v2',
): DemoSession {
  const round = FALLBACK_CATALOG.rounds.find((candidate) => candidate.id === roundId)!
  // The fallback catalog is the AWS edition's; an Azure bout borrows Aurora's shape.
  const competitor = FALLBACK_CATALOG.competitors.find((candidate) => candidate.id === competitorId)
    ?? {
      ...FALLBACK_CATALOG.competitors.find((candidate) => candidate.id === 'aurora_serverless_v2')!,
      id: competitorId,
      name: 'Azure Database for PostgreSQL',
      short_name: 'Azure PostgreSQL',
    }
  const primary = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'software_engineer')!
  const session: DemoSession = {
    id: `fixture-${roundId}`,
    state: 'verified',
    created_at: '2026-08-26T12:00:00Z',
    updated_at: '2026-08-26T12:01:00Z',
    competitor,
    primary_persona: primary,
    secondary_personas: [],
    corners: ['performance'],
    round,
    recommendation_reason: 'Fixture',
    presenter_pack: {
      opening: '',
      discovery_question: '',
      risk: '',
      stop_condition: '',
      remembered_metric: '',
      primary: {
        persona_id: primary.id,
        nickname: primary.nickname,
        role: primary.role,
        interpretation: '',
        objection: '',
        response: '',
      },
      secondary: [],
      closing: '',
    },
    lanes: {
      lakebase: {
        id: 'lakebase',
        name: 'Lakebase',
        state: 'verified',
        elapsed_ms: 1_234,
        attempts: 1,
        status: 'Verified',
        error: null,
      },
      competitor: {
        id: 'competitor',
        name: competitor.short_name,
        state: 'verified',
        elapsed_ms: 5_678,
        attempts: 1,
        status: 'Verified',
        error: null,
      },
    },
    fairness: {
      same_client: true,
      same_transaction: true,
      same_nonce: true,
      launch_skew_ms: 2,
    },
    comparison: {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bout_elapsed_ms', value: 4_444, display_value: '4.44s' },
      detail: 'fixture',
    },
    remembered_result: 'RESULT DECLARED',
    failure: null,
  }

  if (roundId === 'put_model_score_in_app') {
    // A two-lane race like Rounds 1-3: both integrations start parked at the bell and
    // each lane's clock is its own bell-to-exact-read time.
    const row = {
      primary_key: 'customer-42',
      score: 0.81,
      model_version: 'risk-v1',
      proof_nonce: 'round4-proof',
      delta_version: 11,
    }
    session.lanes.lakebase.evidence = { ...row, managed_availability_ms: 980 }
    session.lanes.competitor.evidence = { ...row, glue_run: 'jr_fixture' }
    session.metrics = [
      { spec_id: 'bell_to_exact_read_ms', lane_id: 'lakebase', value: 1_234 },
      { spec_id: 'bell_to_exact_read_ms', lane_id: 'competitor', value: 5_678 },
      { spec_id: 'managed_availability_ms', lane_id: 'lakebase', value: 980 },
    ]
    session.comparison = {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bell_to_exact_read_ms', value: 4_444, display_value: '4.44 s' },
      detail: 'fixture',
    }
  }

  if (roundId === 'survive_connection_spike') {
    // The fan-in protocol. Round 5 has no bounded variant any more, so a fixture
    // shaped like the 128-attempt burst would be testing a protocol that no longer
    // exists.
    const fanInEvidence = (timeToTargetMs: number, authMethod: string) => ({
      protocol: 'round5-fanin-v2',
      schema_version: 2,
      initiated_clients: 10_000,
      authenticated_clients: 10_000,
      held_clients_at_gate: 10_000,
      terminal_failures: 0,
      retries: 0,
      disconnected_during_hold: 0,
      time_to_target_ms: timeToTargetMs,
      hold_elapsed_ms: 30_000.001,
      sampled_queries_attempted: 64,
      sampled_queries_succeeded: 64,
      sampled_queries_failed: 0,
      unique_backend_pids: 19,
      current_backend_sessions: 5,
      peak_backend_sessions: 37,
      preexisting_client_role_sessions: 0,
      observer_role: 'anti_demo_observer',
      client_role: 'anti_demo_burst',
      observer_direct: true,
      auth_method: authMethod,
      distinct_socket_fds: 10_000,
      distinct_local_endpoints: 10_000,
      telemetry_verified: true,
      telemetry_failures: [],
    })
    session.lanes.lakebase.elapsed_ms = 12_350
    session.lanes.competitor.elapsed_ms = 24_000
    session.lanes.lakebase.evidence = fanInEvidence(12_350, 'tls-cleartext-password')
    session.lanes.competitor.evidence = fanInEvidence(24_000, 'scram-sha-256')
    session.fairness = {
      same_client: true,
      same_transaction: true,
      same_nonce: true,
      launch_skew_ms: 2,
      protocol: 'round5-fanin-v2',
      warmup_connections: 0,
      concurrency: 10_000,
      target_clients_per_lane: 10_000,
      sampled_queries_per_lane: 64,
      hold_seconds: 30,
      max_retries: 0,
      runner: 'Python 3.12 event-driven TLS/native-password',
      tls: 'verify-full',
      timeout: '30m',
    }
    const gate = {
      gate_id: 'transaction',
      expected: [{ key: 'verified', value: true }],
      observed: [{ key: 'verified', value: true }],
      exact: true,
    }
    session.round5_setup = {
      state: 'verified',
      workflow_launch_skew_ms: 2,
      protocol: 'round5-fanin-v2',
      schema_version: 2,
      setup_validated: true,
      downstream_validated: true,
      cleanup_retryable: false,
      lanes: {
        lakebase: {
          id: 'lakebase',
          name: 'Lakebase',
          state: 'verified',
          setup_elapsed_ms: 12_350,
          status: 'Verified',
          stop_gate_evidence: gate,
          verified: true,
        },
        competitor: {
          id: 'competitor',
          name: competitor.short_name,
          state: 'verified',
          setup_elapsed_ms: 24_000,
          status: 'Verified',
          stop_gate_evidence: gate,
          verified: true,
        },
      },
    }
    session.comparison = {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      // Fan-in scores shared-T0 time to 10,000, not pooled-path setup.
      margin: { spec_id: 'time_to_10000_ms', value: 11_650, display_value: '11.65s' },
      detail: 'fixture',
    }
  }

  if (roundId === 'analyze_live_orders_without_slowing_checkout') {
    // A two-lane race like Round 4's: AWS DMS and Glue cold start at the bell, Lakebase's
    // change feed is built in, and each lane's clock is its own bell-to-exact-read time.
    const order = {
      order_id: 'order-42',
      total_cents: 8_450,
      total_display: '$84.50',
      proof_nonce: 'r6-bout-0123456789abcdef',
      checkout_guardrail_order_id: 'order-43',
    }
    session.lanes.lakebase.evidence = { ...order, history_lsn: 42 }
    session.lanes.competitor.evidence = { ...order, glue_run: 'jr_fixture' }
    session.metrics = [
      { spec_id: 'bell_to_exact_history_ms', lane_id: 'lakebase', value: 1_234 },
      { spec_id: 'bell_to_exact_history_ms', lane_id: 'competitor', value: 5_678 },
      { spec_id: 'checkout_verified', lane_id: 'lakebase', value: true },
      { spec_id: 'checkout_verified', lane_id: 'competitor', value: true },
      { spec_id: 'commit_skew_ms', value: 3 },
    ]
    session.comparison = {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bell_to_exact_history_ms', value: 4_444, display_value: '4.44 s' },
      detail: 'fixture',
    }
  }

  return session
}

function oneSided(roundId: RoundId): DemoSession {
  const session = verifiedSession(roundId)
  session.state = 'failed'
  session.lanes.competitor.state = 'failed'
  session.lanes.competitor.elapsed_ms = null
  session.remembered_result = null
  return session
}

function competitorOnly(roundId: RoundId): DemoSession {
  const session = verifiedSession(roundId)
  session.state = 'failed'
  session.lanes.lakebase.state = 'failed'
  session.lanes.lakebase.elapsed_ms = null
  session.remembered_result = null
  session.comparison = {
    kind: 'adjudicated_stoppage',
    winner_lane_id: 'competitor',
    margin: null,
    detail: 'The competitor exact proof completed first.',
  }
  return session
}

function noResult(roundId: RoundId): DemoSession {
  const session = verifiedSession(roundId)
  session.state = 'failed'
  session.lanes.lakebase.state = 'failed'
  session.lanes.lakebase.elapsed_ms = null
  session.lanes.competitor.state = 'failed'
  session.lanes.competitor.elapsed_ms = null
  session.metrics = []
  session.round5_setup = null
  return session
}

/** Round 4 on an installation without its AWS lane: Lakebase races alone. */
function roundFourWithoutAwsLane(): DemoSession {
  const session = verifiedSession('put_model_score_in_app')
  session.lanes.competitor.state = 'not_supported'
  session.lanes.competitor.elapsed_ms = null
  session.lanes.competitor.evidence = { unsupported_reason: 'The AWS lane is not installed.' }
  session.metrics = session.metrics!.filter((metric) => metric.lane_id !== 'competitor')
  session.comparison = {
    kind: 'capability_gap',
    winner_lane_id: 'lakebase',
    margin: null,
    detail: 'fixture',
  }
  session.remembered_result = 'LAKEBASE 1.2s · AWS LANE NOT INSTALLED'
  return session
}

/** Round 4 when the two arrivals overlap within the verifiers' 250 ms reads. */
function roundFourTie(): DemoSession {
  const session = verifiedSession('put_model_score_in_app')
  session.lanes.competitor.elapsed_ms = 1_300
  session.comparison = {
    kind: 'tie',
    winner_lane_id: null,
    margin: null,
    detail: 'Both rows arrived within the verifiers’ measurement resolution.',
  }
  session.remembered_result = 'TIE · WITHIN MEASUREMENT RESOLUTION'
  return session
}

/** Round 4 when the AWS lane ran out its whole bound: a declared lower bound, never a margin. */
function roundFourStoppage(): DemoSession {
  const session = verifiedSession('put_model_score_in_app')
  session.lanes.competitor.state = 'failed'
  session.lanes.competitor.elapsed_ms = null
  session.lanes.competitor.status = 'Did not deliver the row within its bound'
  session.comparison = {
    kind: 'adjudicated_stoppage',
    winner_lane_id: 'lakebase',
    margin: null,
    detail: 'fixture',
  }
  session.remembered_result = 'LAKEBASE WINS · MARGIN IS A LOWER BOUND'
  return session
}

/** Round 4 when the AWS lane errored: it measured nothing, so nobody wins. */
function roundFourErroredLane(): DemoSession {
  const session = verifiedSession('put_model_score_in_app')
  session.state = 'failed'
  session.lanes.competitor.state = 'failed'
  session.lanes.competitor.elapsed_ms = null
  session.lanes.competitor.status = 'Could not be measured'
  session.comparison = {
    kind: 'not_comparable',
    winner_lane_id: null,
    margin: null,
    detail: 'No verdict: the other lane failed rather than finished.',
  }
  session.remembered_result = null
  return session
}

/** Round 4 toweled before either lane read its row: both clocks are lower bounds. */
function roundFourNoResultTowel(): DemoSession {
  const session = noResult('put_model_score_in_app')
  session.state = 'towelled'
  session.lanes.lakebase.state = 'towelled'
  session.lanes.competitor.state = 'towelled'
  session.towel = {
    state: 'cleaning',
    requested_at: session.updated_at,
    cutoff_ms: 20_000,
    censored_lower_bounds_ms: { lakebase: 20_000, competitor: 20_000 },
    restore_started: true,
    cleanup_failure: null,
  }
  session.comparison = { kind: 'not_comparable', winner_lane_id: null, margin: null }
  return session
}

/** Round 6 on an installation without its AWS lane: Lakebase races alone. */
function roundSixWithoutAwsLane(): DemoSession {
  const session = verifiedSession('analyze_live_orders_without_slowing_checkout')
  session.lanes.competitor.state = 'not_supported'
  session.lanes.competitor.elapsed_ms = null
  session.lanes.competitor.evidence = { unsupported_reason: 'The AWS lane is not installed.' }
  session.metrics = session.metrics!.filter((metric) => metric.lane_id === 'lakebase')
  session.comparison = {
    kind: 'capability_gap',
    winner_lane_id: 'lakebase',
    margin: null,
    detail: 'fixture',
  }
  session.remembered_result = 'LAKEBASE 1.2s · AWS LANE NOT INSTALLED'
  return session
}

/** Round 6 when the two orders arrive within the verifiers' one-second reads. */
function roundSixTie(): DemoSession {
  const session = verifiedSession('analyze_live_orders_without_slowing_checkout')
  session.lanes.competitor.elapsed_ms = 1_300
  session.comparison = {
    kind: 'tie',
    winner_lane_id: null,
    margin: null,
    detail: 'Both orders arrived within the verifiers’ measurement resolution.',
  }
  session.remembered_result = 'TIE · WITHIN MEASUREMENT RESOLUTION'
  return session
}

/** Round 6 when the AWS lane ran out its whole bound: a declared lower bound, never a margin. */
function roundSixStoppage(): DemoSession {
  const session = verifiedSession('analyze_live_orders_without_slowing_checkout')
  session.lanes.competitor.state = 'failed'
  session.lanes.competitor.elapsed_ms = null
  session.lanes.competitor.status = 'Did not deliver the order within its bound'
  session.comparison = {
    kind: 'adjudicated_stoppage',
    winner_lane_id: 'lakebase',
    margin: null,
    detail: 'fixture',
  }
  session.remembered_result = 'LAKEBASE WINS · MARGIN IS A LOWER BOUND'
  return session
}

/** Round 6 when the AWS lane errored: it measured nothing, so nobody wins. */
function roundSixErroredLane(): DemoSession {
  const session = verifiedSession('analyze_live_orders_without_slowing_checkout')
  session.state = 'failed'
  session.lanes.competitor.state = 'failed'
  session.lanes.competitor.elapsed_ms = null
  session.lanes.competitor.status = 'Could not be measured'
  session.comparison = {
    kind: 'not_comparable',
    winner_lane_id: null,
    margin: null,
    detail: 'No verdict: the other lane failed rather than finished.',
  }
  session.remembered_result = null
  return session
}

/** Round 6 toweled before either lane read its order: both clocks are lower bounds. */
function roundSixNoResultTowel(): DemoSession {
  const session = noResult('analyze_live_orders_without_slowing_checkout')
  session.state = 'towelled'
  session.lanes.lakebase.state = 'towelled'
  session.lanes.competitor.state = 'towelled'
  session.towel = {
    state: 'cleaning',
    requested_at: session.updated_at,
    cutoff_ms: 20_000,
    censored_lower_bounds_ms: { lakebase: 20_000, competitor: 20_000 },
    restore_started: true,
    cleanup_failure: null,
  }
  session.comparison = { kind: 'not_comparable', winner_lane_id: null, margin: null }
  return session
}

function oneSidedTowel(): DemoSession {
  const session = oneSided('recover_deleted_order')
  session.state = 'towelled'
  session.towel = {
    state: 'cleaning',
    requested_at: session.updated_at,
    cutoff_ms: 90_000,
    censored_lower_bounds_ms: { competitor: 90_000 },
    restore_started: true,
    cleanup_failure: null,
  }
  return session
}

function noResultTowel(): DemoSession {
  const session = noResult('recover_deleted_order')
  session.state = 'towelled'
  session.lanes.lakebase.state = 'towelled'
  session.lanes.competitor.state = 'towelled'
  session.towel = {
    state: 'cleaning',
    requested_at: session.updated_at,
    cutoff_ms: 90_000,
    censored_lower_bounds_ms: { lakebase: 90_000, competitor: 90_000 },
    restore_started: true,
    cleanup_failure: null,
  }
  return session
}

function oneSidedRoundFiveSetupTowel(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'towelled'
  session.metrics = []
  session.lanes.lakebase = {
    ...session.lanes.lakebase,
    state: 'verified',
    elapsed_ms: 2_629.562715,
    status: 'Built-in Lakebase pool verified',
    evidence: undefined,
  }
  session.lanes.competitor = {
    ...session.lanes.competitor,
    name: 'Aurora Serverless v2 + RDS Proxy',
    state: 'towelled',
    elapsed_ms: null,
    status: 'Toweled · setup unfinished · >60.84s observed lower bound',
    evidence: {
      censored: true,
      lower_bound_ms: 60_840.221846,
      display_value: '>60.84s',
    },
  }
  session.round5_setup = {
    ...session.round5_setup!,
    state: 'towelled',
    workflow_launch_skew_ms: null,
    setup_validated: false,
    downstream_validated: false,
    cleanup_retryable: false,
    lanes: {
      lakebase: {
        id: 'lakebase',
        name: 'Lakebase',
        state: 'verified',
        setup_elapsed_ms: 2_629.562715,
        status: 'Built-in Lakebase pool verified',
        stop_gate_evidence: null,
        verified: false,
      },
      competitor: {
        id: 'competitor',
        name: 'Aurora Serverless v2 + RDS Proxy',
        state: 'towelled',
        setup_elapsed_ms: 60_840.221846,
        status: 'Toweled before the exact setup stop',
        stop_gate_evidence: null,
        verified: false,
      },
    },
  }
  session.towel = {
    state: 'ready',
    requested_at: session.updated_at,
    censored_lower_bounds_ms: { competitor: 60_840.221846 },
    active_lane: 'competitor',
    lakebase_verified_ms: 2_629.562715,
    restore_started: false,
    cleanup_failure: null,
  }
  session.comparison = {
    kind: 'not_comparable',
    winner_lane_id: null,
    margin: null,
    detail: 'The bounded check did not run, so no winner or margin was declared.',
  }
  session.remembered_result = 'Lakebase setup verified first; comparison incomplete.'
  return session
}

function noVerifiedRoundFiveSetupTowel(): DemoSession {
  const session = oneSidedRoundFiveSetupTowel()
  session.lanes.lakebase.state = 'towelled'
  session.lanes.lakebase.elapsed_ms = null
  session.round5_setup!.lanes.lakebase = {
    ...session.round5_setup!.lanes.lakebase!,
    state: 'towelled',
    setup_elapsed_ms: 60_840.221846,
    status: 'Toweled before the exact setup stop',
  }
  session.towel!.censored_lower_bounds_ms = {
    lakebase: 60_840.221846,
    competitor: 60_840.221846,
  }
  delete session.towel!.lakebase_verified_ms
  return session
}

function incompleteSetup(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'failed'
  for (const laneId of ['lakebase', 'competitor'] as const) {
    session.lanes[laneId].state = 'failed'
    session.lanes[laneId].elapsed_ms = null
    session.lanes[laneId].evidence = undefined
    const setupLane = session.round5_setup!.lanes[laneId]!
    setupLane.state = 'failed'
    setupLane.verified = false
    setupLane.setup_elapsed_ms = null
    setupLane.stop_gate_evidence = null
    setupLane.status = 'Transaction gate did not verify'
  }
  session.round5_setup!.state = 'failed'
  session.round5_setup!.setup_validated = false
  return session
}

function failedSpike(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'failed'
  // Both lanes reached 10,000 and therefore both have a fan-in time -- this is the
  // guardrail failure, not a lane that never arrived. Clearing the competitor's
  // evidence instead would leave it with no time to 10,000 at all, which is a
  // one-sided result: under this protocol a lane cannot have a fan-in time without
  // having reached the target.
  for (const laneId of ['lakebase', 'competitor'] as const) {
    session.lanes[laneId].evidence = {
      ...(session.lanes[laneId].evidence as Record<string, unknown>),
      // One sampled query failed, so the sparse-query check did not pass.
      sampled_queries_succeeded: 63,
      sampled_queries_failed: 1,
    }
  }
  session.round5_setup!.state = 'failed'
  session.round5_setup!.downstream_validated = false
  return session
}

function failedCleanup(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'failed'
  session.comparison = null
  session.round5_setup!.state = 'cleanup_failed'
  session.round5_setup!.setup_validated = false
  session.round5_setup!.downstream_validated = false
  session.round5_setup!.cleanup_failure = 'proxy deletion verification'
  session.round5_setup!.cleanup_retryable = true
  for (const laneId of ['lakebase', 'competitor'] as const) {
    session.lanes[laneId].state = 'failed'
    session.lanes[laneId].elapsed_ms = null
    session.lanes[laneId].evidence = undefined
    const setupLane = session.round5_setup!.lanes[laneId]!
    setupLane.state = 'failed'
    setupLane.setup_elapsed_ms = null
    setupLane.stop_gate_evidence = null
    setupLane.verified = false
  }
  return session
}

function verifiedCleanupFailure(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.round5_setup!.cleanup_failure = 'proxy deletion verification'
  session.round5_setup!.cleanup_retryable = true
  return session
}

function cooldownCleanupFailure(roundId: RoundId): DemoSession {
  const session = verifiedSession(roundId)
  const startedAt = session.updated_at
  const failedLane = (id: 'lakebase' | 'competitor', name: string) => ({
    id,
    name,
    state: 'failed' as const,
    started_at: startedAt,
    confirmed_at: null,
    elapsed_ms: null,
    status: 'Run-owned cleanup did not verify',
  })
  session.state = 'failed'
  session.cooldown = {
    mode: 'return_to_idle',
    state: 'failed',
    started_at: startedAt,
    failure: 'Run-owned cleanup did not verify',
    lanes: {
      lakebase: failedLane('lakebase', session.lanes.lakebase.name),
      competitor: failedLane('competitor', session.lanes.competitor.name),
    },
  }
  return session
}

describe('the three files behind Explain to the room', () => {
  it('are the approved files, pinned, loaded directly with no generated copy layer', () => {
    expect(sha256(sourceText('result-lines.jsonl'))).toBe(RESULT_LINES_SHA256)
    expect(sha256(sourceText('role-lines.jsonl'))).toBe(ROLE_LINES_SHA256)
    expect(sha256(sourceText('lens-lines.jsonl'))).toBe(LENS_LINES_SHA256)
    expect(RESULT_LINE_RECORDS).toEqual(parsedSource('result-lines.jsonl'))
    expect(ROLE_LINE_RECORDS).toEqual(parsedSource('role-lines.jsonl'))
    expect(LENS_LINE_RECORDS).toEqual(parsedSource('lens-lines.jsonl'))
  })

  it('has one shared role line for every round and role, and one lens line for every round and priority', () => {
    const roles = new Set(ROLE_LINE_RECORDS
      .filter((record) => record.competitor === '*')
      .map((record) => `${record.round_id}/${record.persona_id}`))
    expect(roles.size).toBe(ROUND_IDS.length * PERSONA_IDS.length)
    const lenses = new Set(LENS_LINE_RECORDS
      .filter((record) => record.competitor === '*')
      .map((record) => `${record.round_id}/${record.priority}`))
    expect(lenses.size).toBe(ROUND_IDS.length * PRIORITIES.length)
    for (const roundId of ROUND_IDS) {
      for (const personaId of PERSONA_IDS) {
        expect(roles.has(`${roundId}/${personaId}`), `${roundId}/${personaId}`).toBe(true)
        expect(getRoleLine(roundId, personaId).competitor).toBe('*')
      }
      for (const priority of PRIORITIES) {
        expect(lenses.has(`${roundId}/${priority}`), `${roundId}/${priority}`).toBe(true)
        expect(getLensLine(roundId, priority).competitor).toBe('*')
      }
    }
  })

  it('keeps every competitor variant attached to a shared line and scoped to a known kind', () => {
    const scopes = ['*', 'aurora', 'rds', 'azure']
    const keyed = [
      ...RESULT_LINE_RECORDS.map((record) => ({ scope: record.competitor, key: `result/${record.round_id}/${record.outcome_id}` })),
      ...ROLE_LINE_RECORDS.map((record) => ({ scope: record.competitor, key: `role/${record.round_id}/${record.persona_id}` })),
      ...LENS_LINE_RECORDS.map((record) => ({ scope: record.competitor, key: `lens/${record.round_id}/${record.priority}` })),
    ]
    const shared = new Set(keyed.filter((line) => line.scope === '*').map((line) => line.key))
    for (const line of keyed) {
      expect(scopes).toContain(line.scope)
      expect(shared.has(line.key), line.key).toBe(true)
    }
    // No two lines claim the same key and scope.
    expect(new Set(keyed.map((line) => `${line.key}@${line.scope}`)).size).toBe(keyed.length)
  })

  it('has one shared result line for every round and outcome the classifier can choose', () => {
    // Rounds 4 and 6 race two lanes: a comparison, a one-sided result and a towel with
    // no verified lane, plus the capability case for an installation without its AWS
    // lane. Round 6's separate checkout is the server's to enforce, so it has no row.
    const shared = RESULT_LINE_RECORDS.filter((record) => record.competitor === '*')
    expect(shared.map(({ round_id, outcome_id }) => [round_id, outcome_id])).toEqual([
      ['wake_idle_app', 'verified_comparison'],
      ['wake_idle_app', 'verified_rds_capability_gap'],
      ['wake_idle_app', 'one_sided_verified'],
      ['wake_idle_app', 'cleanup_failed'],
      ['wake_idle_app', 'no_result'],
      ['make_schema_change_safely', 'verified_comparison'],
      ['make_schema_change_safely', 'one_sided_verified'],
      ['make_schema_change_safely', 'cleanup_failed'],
      ['make_schema_change_safely', 'no_result'],
      ['recover_deleted_order', 'verified_comparison'],
      ['recover_deleted_order', 'one_sided_verified'],
      ['recover_deleted_order', 'one_sided_towel_lower_bound'],
      ['recover_deleted_order', 'towel_no_verified_lane'],
      ['recover_deleted_order', 'cleanup_failed'],
      ['recover_deleted_order', 'no_result'],
      ['put_model_score_in_app', 'verified_comparison'],
      ['put_model_score_in_app', 'verified_capability_gap'],
      ['put_model_score_in_app', 'one_sided_verified'],
      ['put_model_score_in_app', 'towel_no_verified_lane'],
      ['put_model_score_in_app', 'cleanup_failed'],
      ['put_model_score_in_app', 'no_result'],
      ['survive_connection_spike', 'verified_comparison'],
      ['survive_connection_spike', 'one_sided_setup_verified_towel'],
      ['survive_connection_spike', 'setup_incomplete'],
      ['survive_connection_spike', 'bounded_check_failed'],
      ['survive_connection_spike', 'cleanup_failed'],
      ['survive_connection_spike', 'no_result'],
      ['analyze_live_orders_without_slowing_checkout', 'verified_comparison'],
      ['analyze_live_orders_without_slowing_checkout', 'verified_capability_gap'],
      ['analyze_live_orders_without_slowing_checkout', 'one_sided_verified'],
      ['analyze_live_orders_without_slowing_checkout', 'towel_no_verified_lane'],
      ['analyze_live_orders_without_slowing_checkout', 'cleanup_failed'],
      ['analyze_live_orders_without_slowing_checkout', 'no_result'],
    ])
    for (const record of shared) {
      expect(getResultLine(record.round_id, record.outcome_id)).toBe(record)
    }
  })
})

describe('the briefing reads like a person talking', () => {
  const DASHES = /[–—]/
  const BRITISH = /\b(?:colour|behaviour|organis(?:e|ed|ation)|recognis(?:e|ed)|optimis(?:e|ed)|favour|centre|analyse|licence)\b/i
  const MACHINERY = /\b(?:contract test|lane shape|outcome id|copy mode|verified corpus|placeholder|template)\b/i
  const VERDICT = /\b(?:won|wins|winner|beat|faster|slower|sooner|margin)\b/i
  const PLACEHOLDER = /<[A-Z0-9_]+>/

  it('keeps every result line short, with its numbers early and its boundary last', () => {
    for (const record of RESULT_LINE_RECORDS) {
      // Round 5 has the most to say: two clocks, the setup inside one of them, what
      // the 10,000 are, and what was not measured.
      const limit = record.round_id === 'survive_connection_spike' ? 90 : 55
      expect(words(record.text), record.text).toBeLessThanOrEqual(limit)
      expect(record.text).not.toMatch(DASHES)
      expect(record.text).not.toMatch(BRITISH)
      expect(record.text).not.toMatch(MACHINERY)
      // A line that declares nothing says so in its first two words.
      if (['no_result', 'towel_no_verified_lane', 'setup_incomplete'].includes(record.outcome_id)) {
        expect(record.text).toMatch(/^No result\./)
      }
    }
  })

  it('keeps role lines about the round and the person, never this bout', () => {
    for (const record of ROLE_LINE_RECORDS) {
      const label = `${record.round_id}/${record.persona_id}/${record.competitor}`
      expect(words(record.why), label).toBeLessThanOrEqual(36)
      expect(words(record.ask), label).toBeLessThanOrEqual(18)
      expect(record.ask, label).toMatch(/\?$/)
      expect(record.ask.match(/\?/g), label).toHaveLength(1)
      const spoken = `${record.why} ${record.ask}`
      expect(spoken, label).not.toMatch(PLACEHOLDER)
      expect(spoken, label).not.toMatch(VERDICT)
      expect(spoken, label).not.toMatch(DASHES)
      expect(spoken, label).not.toMatch(BRITISH)
      expect(spoken, label).not.toMatch(MACHINERY)
      // Round 5's capability is "up to 10,000 client connections"; a role line never
      // borrows the bout's protocol vocabulary to say so.
      if (record.why.includes('10,000')) {
        expect(record.why, label).toMatch(/\bup to 10,000 client connections\b/)
        expect(record.why, label).not.toMatch(/\b(?:witness|scheduled clients?|terminal clients?|setup stop|128 attempts?|maximum 64 concurrent)\b/i)
      }
    }
    // Every role hears something different about the same round.
    for (const roundId of ROUND_IDS) {
      const shared = ROLE_LINE_RECORDS.filter((record) => record.round_id === roundId && record.competitor === '*')
      expect(new Set(shared.map((record) => record.why)).size).toBe(PERSONA_IDS.length)
      expect(new Set(shared.map((record) => record.ask)).size).toBe(PERSONA_IDS.length)
    }
  })

  it('keeps lens lines concrete to the round and silent on who won', () => {
    for (const record of LENS_LINE_RECORDS) {
      const label = `${record.round_id}/${record.priority}/${record.competitor}`
      expect(words(record.text), label).toBeLessThanOrEqual(40)
      expect(record.text, label).not.toMatch(PLACEHOLDER)
      expect(record.text, label).not.toMatch(VERDICT)
      expect(record.text, label).not.toMatch(DASHES)
      expect(record.text, label).not.toMatch(BRITISH)
      expect(record.text, label).not.toMatch(MACHINERY)
    }
    // Each priority says a different thing about the same round.
    for (const roundId of ROUND_IDS) {
      const shared = LENS_LINE_RECORDS.filter((record) => record.round_id === roundId && record.competitor === '*')
      expect(new Set(shared.map((record) => record.text)).size).toBe(PRIORITIES.length)
    }
    // Performance lines say what the clock covered; cost lines say what is billed.
    for (const roundId of ROUND_IDS) {
      expect(getLensLine(roundId, 'performance').text).toMatch(/\bclock\b/i)
      expect(getLensLine(roundId, 'cost').text).toMatch(/\b(?:bill|bills|billed|billable|paid|cost|expensive)\b/i)
    }
  })
})

describe('one generic evidence classifier with six round contracts', () => {
  const roundFourPartial = roundFourErroredLane()

  const roundSixPartial = roundSixErroredLane()

  const rdsGap = verifiedSession('wake_idle_app', 'rds_postgres')
  rdsGap.lanes.competitor.state = 'not_supported'
  rdsGap.lanes.competitor.elapsed_ms = null

  const cases: Array<[string, DemoSession, RingsideOutcomeId]> = [
    ['R1 verified comparison', verifiedSession('wake_idle_app'), 'verified_comparison'],
    ['R2 verified comparison', verifiedSession('make_schema_change_safely'), 'verified_comparison'],
    ['R3 verified comparison', verifiedSession('recover_deleted_order'), 'verified_comparison'],
    ['R4 verified comparison', verifiedSession('put_model_score_in_app'), 'verified_comparison'],
    ['R4 without its AWS lane', roundFourWithoutAwsLane(), 'verified_capability_gap'],
    ['R4 stoppage at the bound', roundFourStoppage(), 'one_sided_verified'],
    ['R5 verified comparison', verifiedSession('survive_connection_spike'), 'verified_comparison'],
    ['R6 verified comparison', verifiedSession('analyze_live_orders_without_slowing_checkout'), 'verified_comparison'],
    ['R6 without its AWS lane', roundSixWithoutAwsLane(), 'verified_capability_gap'],
    ['R6 stoppage at the bound', roundSixStoppage(), 'one_sided_verified'],
    ['R1 RDS gap', rdsGap, 'verified_rds_capability_gap'],
    ['R1 one-sided', oneSided('wake_idle_app'), 'one_sided_verified'],
    ['R2 one-sided', oneSided('make_schema_change_safely'), 'one_sided_verified'],
    ['R3 one-sided', oneSided('recover_deleted_order'), 'one_sided_verified'],
    ['R3 towel lower bound', oneSidedTowel(), 'one_sided_towel_lower_bound'],
    ['R5 one-sided setup towel', oneSidedRoundFiveSetupTowel(), 'one_sided_setup_verified_towel'],
    ['R5 both setups unverified at towel', noVerifiedRoundFiveSetupTowel(), 'setup_incomplete'],
    ['R4 errored lane', roundFourPartial, 'one_sided_verified'],
    ['R5 setup incomplete', incompleteSetup(), 'setup_incomplete'],
    ['R5 bounded check failed', failedSpike(), 'bounded_check_failed'],
    ['R5 cleanup failed', failedCleanup(), 'cleanup_failed'],
    ['R6 errored lane', roundSixPartial, 'one_sided_verified'],
    ...ROUND_IDS.map((roundId) => [`${roundId} no result`, noResult(roundId), 'no_result'] as [string, DemoSession, RingsideOutcomeId]),
    ['R3 towel without a verified lane', noResultTowel(), 'towel_no_verified_lane'],
    ['R4 towel without a verified lane', roundFourNoResultTowel(), 'towel_no_verified_lane'],
    ['R6 towel without a verified lane', roundSixNoResultTowel(), 'towel_no_verified_lane'],
  ]

  it.each(cases)('%s', (_label, session, expected) => {
    expect(classifyRingsideOutcome(session).outcome_id).toBe(expected)
  })

  it('cannot give incomplete R4-R6 gates a declared result', () => {
    for (const session of [
      roundFourPartial,
      oneSidedRoundFiveSetupTowel(),
      incompleteSetup(),
      failedSpike(),
      failedCleanup(),
      roundSixPartial,
    ]) {
      const briefing = buildRingsideBriefing(session, 'software_engineer', ['performance'])
      expect(briefing.declared).toBe(false)
      expect(briefing.result).toMatch(/no winner|no margin|no result/i)
      expect(briefing.result).not.toMatch(/<[A-Z0-9_]+>/)
      // The role and lens lines are the round's, so they still read true here.
      expect(briefing.why.length).toBeGreaterThan(0)
      expect(briefing.lenses).toHaveLength(1)
      expect(briefing.ask).toMatch(/\?$/)
    }
  })

  it('reads a Round 1 opponent with no automatic wake as checked before the bell, Azure included', () => {
    for (const competitorId of ['rds_postgres', 'azure_postgres'] as const) {
      const session = verifiedSession('wake_idle_app', competitorId)
      session.lanes.competitor.state = 'not_supported'
      session.lanes.competitor.elapsed_ms = null
      session.comparison = { kind: 'capability_gap', winner_lane_id: 'lakebase', margin: null, detail: 'fixture' }
      const briefing = buildRingsideBriefing(session, 'sre', ['cost', 'simplicity'])
      expect(briefing.outcomeId).toBe('verified_rds_capability_gap')
      expect(briefing.declared).toBe(true)
      expect(briefing.result).toContain(`${session.competitor.short_name} has no automatic wake.`)
      expect(briefing.result).toContain('Lakebase woke and committed in 1.23s.')
      expect(briefing.result).not.toMatch(/never finished|did not verify/i)
      // The lenses name that opponent's rule, not Aurora's floor.
      expect(briefing.lenses.map((lens) => lens.text).join(' ')).not.toMatch(/Aurora/)
    }
  })

  it.each([
    ['R1 comparison', verifiedSession('wake_idle_app'), 'both_exact_verified', 'declared_comparison', 'lakebase', 4_444],
    ['R2 comparison', verifiedSession('make_schema_change_safely'), 'both_exact_verified', 'declared_comparison', 'lakebase', 4_444],
    ['R3 comparison', verifiedSession('recover_deleted_order'), 'both_exact_verified', 'declared_comparison', 'lakebase', 4_444],
    ['R4 comparison', verifiedSession('put_model_score_in_app'), 'both_exact_verified', 'declared_comparison', 'lakebase', 4_444],
    ['R4 within resolution', roundFourTie(), 'both_exact_verified', 'declared_comparison', 'tie', null],
    ['R4 without its AWS lane', roundFourWithoutAwsLane(), 'capability_gap', 'declared_capability', 'lakebase', null],
    // A lane that ran out its bound leaves a stoppage; one that errored leaves nothing.
    ['R4 stoppage at the bound', roundFourStoppage(), 'lakebase_only_exact', 'adjudicated_stoppage', 'lakebase', null],
    ['R4 errored lane', roundFourPartial, 'lakebase_only_exact', 'comparison_incomplete', null, null],
    ['R4 both bounds', roundFourNoResultTowel(), 'both_lower_bounds', 'no_verified_evidence', null, null],
    ['R5 comparison', verifiedSession('survive_connection_spike'), 'both_exact_verified', 'declared_comparison', 'lakebase', 11_650],
    ['R6 comparison', verifiedSession('analyze_live_orders_without_slowing_checkout'), 'both_exact_verified', 'declared_comparison', 'lakebase', 4_444],
    ['R6 within resolution', roundSixTie(), 'both_exact_verified', 'declared_comparison', 'tie', null],
    ['R6 without its AWS lane', roundSixWithoutAwsLane(), 'capability_gap', 'declared_capability', 'lakebase', null],
    ['R6 stoppage at the bound', roundSixStoppage(), 'lakebase_only_exact', 'adjudicated_stoppage', 'lakebase', null],
    ['R6 errored lane', roundSixPartial, 'lakebase_only_exact', 'comparison_incomplete', null, null],
    ['R6 both bounds', roundSixNoResultTowel(), 'both_lower_bounds', 'no_verified_evidence', null, null],
    ['R2 Lakebase only', oneSided('make_schema_change_safely'), 'lakebase_only_exact', 'adjudicated_stoppage', 'lakebase', null],
    ['R2 competitor only', competitorOnly('make_schema_change_safely'), 'competitor_only_exact', 'adjudicated_stoppage', 'competitor', null],
    ['R3 exact plus bound', oneSidedTowel(), 'exact_and_censored_lower_bound', 'adjudicated_stoppage', 'lakebase', null],
    ['R3 both bounds', noResultTowel(), 'both_lower_bounds', 'no_verified_evidence', null, null],
    ['R5 verified-first', oneSidedRoundFiveSetupTowel(), 'exact_and_censored_lower_bound', 'comparison_incomplete', null, null],
    ['R5 both bounds', noVerifiedRoundFiveSetupTowel(), 'both_lower_bounds', 'no_verified_evidence', null, null],
    ['R5 bounded-check guardrail', failedSpike(), 'guardrail_failure', 'guardrail_failure', null, null],
    ['R5 cleanup before result', failedCleanup(), 'cleanup_failure', 'cleanup_failure', null, null],
    // Round-5-scoped decouple: the evidence still records the cleanup shape, but
    // the SEALED verified Round 5 result keeps its declared comparison and stays
    // shareable regardless of backstage ring cleanup.
    ['R5 cleanup after result', verifiedCleanupFailure(), 'cleanup_failure', 'declared_comparison', 'lakebase', 11_650],
  ] as const)(
    '%s has one evidence and contract decision',
    (_label, session, shape, status, winner, margin) => {
      const classified = classifyOutcome(session)
      expect(classified.evidence.shape).toBe(shape)
      expect(classified.status).toBe(status)
      expect(classified.formalWinner).toBe(winner)
      expect(classified.marginMs).toBe(margin)
      expect(classified.shareable).toBe(
        status === 'declared_comparison'
        || status === 'declared_capability'
        || status === 'adjudicated_stoppage',
      )
    },
  )

  it('words every Round 4 verdict the way the server declared it', () => {
    // The server's remembered line is the verdict for every declared bout; a bout the
    // server could not declare says so, and never borrows a winner from the lane clocks.
    const headline = (session: DemoSession) => classifyOutcome(session).headline
    expect(headline(verifiedSession('put_model_score_in_app'))).toBe('RESULT DECLARED')
    expect(headline(roundFourTie())).toMatch(/WITHIN MEASUREMENT RESOLUTION$/)
    expect(headline(roundFourStoppage())).toMatch(/MARGIN IS A LOWER BOUND$/)
    expect(headline(roundFourPartial)).toBe('NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A')
    expect(headline(roundFourWithoutAwsLane())).toBe('LAKEBASE 1.2s · AWS LANE NOT INSTALLED')
    const unremembered = roundFourWithoutAwsLane()
    unremembered.remembered_result = null
    expect(headline(unremembered)).toBe('LAKEBASE 1.23s · AWS LANE NOT INSTALLED')
    expect(buildRingsideResult(roundFourTie())).toBe(
      'One new score was written to the lakehouse at the bell, with both pipelines starting cold. The app on Lakebase could read it in 1.23s. Through AWS Glue → Aurora Serverless v2: 1.30s. The clock is the hop from lakehouse to app. The model itself wasn\'t run or timed.',
    )
    expect(buildRingsideResult(roundFourPartial)).toBe(
      'One new score was written to the lakehouse at the bell. Lakebase synced table delivered it to its app in 1.23s. AWS Glue → Aurora Serverless v2 never delivered it, so there is a result but no margin. The model itself wasn\'t run or timed.',
    )
  })

  it('words every Round 6 verdict the way the server declared it', () => {
    // As in Round 4: the server's remembered line is the verdict for every declared bout,
    // and a bout the server could not declare never borrows a winner from the clocks.
    const headline = (session: DemoSession) => classifyOutcome(session).headline
    expect(headline(verifiedSession('analyze_live_orders_without_slowing_checkout'))).toBe('RESULT DECLARED')
    expect(headline(roundSixTie())).toBe('TIE · WITHIN MEASUREMENT RESOLUTION')
    expect(headline(roundSixStoppage())).toBe('LAKEBASE WINS · MARGIN IS A LOWER BOUND')
    expect(headline(roundSixPartial)).toBe('NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A')
    expect(headline(roundSixWithoutAwsLane())).toBe('LAKEBASE 1.2s · AWS LANE NOT INSTALLED')
    const unremembered = roundSixWithoutAwsLane()
    unremembered.remembered_result = null
    expect(headline(unremembered)).toBe('LAKEBASE 1.23s · AWS LANE NOT INSTALLED')
    expect(buildRingsideResult(roundSixTie())).toBe(
      "The same checkout hit both databases at the bell. Lakebase's built-in change feed, which is always on, had it in the lakehouse in 1.23s. AWS DMS + Glue from Aurora Serverless v2, started cold at the bell, took 1.30s. A second checkout committed on each side meanwhile as a sanity check. Throughput and p99 weren't tested.",
    )
    expect(buildRingsideResult(roundSixPartial)).toBe(
      "The same checkout hit both databases at the bell. Lakebase built-in change feed had it in the lakehouse in 1.23s. AWS DMS + Glue from Aurora Serverless v2 never delivered it, so there is a result but no margin. Throughput and p99 weren't tested.",
    )
    expect(buildRingsideResult(roundSixWithoutAwsLane())).toBe(
      "The checkout hit the database at the bell. Lakebase's built-in change feed had it in the lakehouse in 1.23s. This install has no AWS lane, so there is nothing to compare it to. A second checkout committed meanwhile as a sanity check. Throughput and p99 weren't tested.",
    )
    // Lakebase's feed is never called cold, and nothing calls it warm.
    const copy = [roundSixTie(), roundSixStoppage(), roundSixWithoutAwsLane()]
      .map((session) => buildRingsideResult(session))
      .join(' ')
    expect(copy).not.toMatch(/Lakebase[^.]*cold|warm/i)
  })

  it('classifies the generic evidence shapes before applying a round contract', () => {
    const lane = (exactMs: number | null, lowerBoundMs: number | null, notSupported = false) => ({
      exactMs,
      lowerBoundMs,
      notSupported,
    })
    expect(classifyEvidence({
      lakebase: lane(1, null),
      competitor: lane(2, null),
    }).shape).toBe('both_exact_verified')
    expect(classifyEvidence({
      lakebase: lane(1, null),
      competitor: lane(null, 3),
    }).shape).toBe('exact_and_censored_lower_bound')
    expect(classifyEvidence({
      lakebase: lane(null, 3),
      competitor: lane(null, 3),
    }).shape).toBe('both_lower_bounds')
    expect(classifyEvidence({
      lakebase: lane(1, null),
      competitor: lane(null, null, true),
      capabilityGap: true,
    }).shape).toBe('capability_gap')
    expect(classifyEvidence({
      lakebase: lane(1, null),
      competitor: lane(null, null, true),
      guardrailFailure: true,
    }).shape).toBe('guardrail_failure')
    expect(classifyEvidence({
      lakebase: lane(1, null),
      competitor: lane(2, null),
      cleanupFailure: true,
    }).shape).toBe('cleanup_failure')
  })

  it.each([
    ['Round 4 without its AWS lane', roundFourWithoutAwsLane],
    ['Round 6 without its AWS lane', roundSixWithoutAwsLane],
  ] as const)(
    '%s complete capability gap unconditionally produces Lakebase health bars',
    (_label, fixture) => {
      const session = fixture()
      const classified = classifyOutcome(session)
      const receipt = receiptPresentation(session, 'round')

      expect(classified.status).toBe('declared_capability')
      expect(classified.contractComplete).toBe(true)
      expect(receipt.healthBars).toMatchObject({
        fill: { lakebase: 1, competitor: 0 },
        winner: 'lakebase',
        capabilityGap: true,
      })
    },
  )

  it.each(ROUND_IDS)(
    '%s treats failed cooldown cleanup as one non-shareable fenced outcome',
    (roundId) => {
      const session = cooldownCleanupFailure(roundId)
      const classified = classifyOutcome(session)
      const briefing = buildRingsideBriefing(session, 'software_engineer', ['performance'])
      const receipt = receiptPresentation(session, 'round')
      const scorecard = scorecardEntry(session)
      const share = linkedInReceipt(session, 5)

      expect(classified.evidence.shape).toBe('cleanup_failure')
      if (roundId === 'survive_connection_spike') {
        // Round-5-scoped decouple: a SEALED verified Round 5 bout keeps its
        // declared result and stays shareable regardless of backstage ring
        // cleanup (which converges automatically). Cleanup never fences the user
        // or produces a "CLEANUP FAILED · SHARING BLOCKED" headline here.
        expect(classified.status).not.toBe('cleanup_failure')
        expect(classified.outcome.outcome_id).not.toBe('cleanup_failed')
        expect(classified.shareable).toBe(true)
        expect(classified.headline).not.toMatch(/CLEANUP FAILED · SHARING BLOCKED/)
        expect(briefing.outcomeId).not.toBe('cleanup_failed')
        expect(share).not.toMatch(/sharing (?:is )?blocked/i)
        return
      }
      expect(classified.status).toBe('cleanup_failure')
      expect(classified.outcome.outcome_id).toBe('cleanup_failed')
      expect(classified.shareable).toBe(false)
      expect(classified.headline).toMatch(/RESULT RETAINED · CLEANUP FAILED · SHARING BLOCKED/)
      expect(briefing.outcomeId).toBe('cleanup_failed')
      expect(briefing.declared).toBe(false)
      // The result stands and the line says so first, then what the failure costs the room.
      expect(briefing.result).toMatch(/^Lakebase won the bout\. Then the cleanup after the bout failed at Run-owned cleanup did not verify\. The result stands, sharing is blocked, and this round stays closed until the cleanup succeeds\.$/)
      expect(receipt.verdict).toBe(classified.headline)
      expect(scorecard?.contract_status).toBe('cleanup_failure')
      expect(scorecard?.remembered_result).toBe(classified.headline)
      expect(share).toMatch(/sharing (?:is )?blocked/i)
    },
  )

  it('keeps main, receipt, share, Ringside, and scorecard semantics aligned', () => {
    const sessions = [
      ...ROUND_IDS.map((roundId) => verifiedSession(roundId)),
      oneSided('make_schema_change_safely'),
      oneSidedTowel(),
      oneSidedRoundFiveSetupTowel(),
      roundFourPartial,
      roundFourWithoutAwsLane(),
      roundFourStoppage(),
      roundFourNoResultTowel(),
      roundSixPartial,
      roundSixWithoutAwsLane(),
      roundSixTie(),
      roundSixStoppage(),
      roundSixNoResultTowel(),
    ]

    for (const session of sessions) {
      const classified = classifyOutcome(session)
      const briefing = buildRingsideBriefing(session, 'software_engineer', ['performance'])
      const receipt = receiptPresentation(session, 'round')
      const scorecard = scorecardEntry(session)
      const share = linkedInReceipt(session, 5)

      expect(briefing.outcomeId).toBe(classified.outcome.outcome_id)
      expect(briefing.status).toBe(classified.status)
      expect(receipt.winner).toBe(classified.formalWinner)
      expect(receipt.verdict).toBe(classified.headline)
      expect(scorecard?.formal_winner).toBe(classified.formalWinner)
      expect(scorecard?.margin_ms).toBe(classified.marginMs)
      expect(scorecard?.remembered_result).toBe(classified.headline)

      // The knockout hero never appears without a named winner on a complete,
      // untowelled contract, and it always leads with a figure the caption
      // can stand behind.
      if (receipt.knockout) {
        expect(receipt.winner === 'lakebase' || receipt.winner === 'competitor').toBe(true)
        expect(classified.contractComplete).toBe(true)
        expect(session.towel).toBeFalsy()
        expect(receipt.knockout.hero).toMatch(/^\d/)
        expect(receipt.knockout.qualifier.length).toBeLessThanOrEqual(80)
        expect(receipt.knockout.winner).toBe(receipt.winner)
        expect(receipt.knockout.capabilityGap).toBe(receipt.competitorCapabilityGap)
      }
      if (session.towel || !classified.contractComplete || (receipt.winner !== 'lakebase' && receipt.winner !== 'competitor')) {
        expect(receipt.knockout).toBeUndefined()
      }

      // The health bars (the default layout) obey the same gate as the knockout
      // card: never without a named winner on a complete, untowelled contract;
      // the slower lane always fills the track; the verdict is never empty; and
      // the capability flag matches the structured classification, not a string.
      const shouldHaveHealthBars = !session.towel
        && (classified.status === 'declared_comparison' || classified.status === 'declared_capability')
        && (receipt.winner === 'lakebase' || receipt.winner === 'competitor')
      if (shouldHaveHealthBars) {
        expect(receipt.healthBars).toBeDefined()
        const healthBars = receipt.healthBars!
        expect(receipt.winner === 'lakebase' || receipt.winner === 'competitor').toBe(true)
        expect(classified.contractComplete).toBe(true)
        expect(session.towel).toBeFalsy()
        expect(healthBars.winner).toBe(receipt.winner)
        expect(healthBars.capabilityGap).toBe(receipt.competitorCapabilityGap)
        expect(Math.max(healthBars.fill.lakebase, healthBars.fill.competitor)).toBe(1)
        expect(healthBars.fill[healthBars.winner]).toBeGreaterThanOrEqual(0)
        expect(healthBars.verdict.length).toBeGreaterThan(0)
      } else {
        expect(receipt.healthBars).toBeUndefined()
      }

      const allCopy = [
        classified.headline,
        briefing.result,
        briefing.why,
        receipt.verdict,
        scorecard?.remembered_result ?? '',
        share,
      ].join(' ')
      if (classified.evidence.exactLane !== null) {
        expect(allCopy).not.toMatch(/neither setup|neither readiness|no verified result/i)
      }
      if (classified.formalWinner === null) {
        expect(receipt.winner).toBeNull()
        expect(classified.marginMs).toBeNull()
      }
      if (
        session.round.id === 'put_model_score_in_app'
        || session.round.id === 'analyze_live_orders_without_slowing_checkout'
      ) {
        // Rounds 4 and 6 race, so a margin exists exactly when a winner was declared by
        // measurement; a tie, a lower bound, an errored lane or a missing AWS lane never
        // carries one.
        expect(classified.marginMs === null).toBe(
          classified.status !== 'declared_comparison' || classified.formalWinner === 'tie',
        )
        expect(allCopy).not.toMatch(/not built or timed|separate reverse-ETL stack|separate CDC stack/i)
      }
    }
  })

  it('is non-vacuous against the old Round 5 full-document mutant', () => {
    const observed = oneSidedRoundFiveSetupTowel()
    expect(observed.round5_setup!.lanes.lakebase!.stop_gate_evidence).toBeNull()
    expect(observed.round5_setup!.lanes.lakebase!.verified).toBe(false)
    expect(classifyOutcome(observed).outcome.outcome_id).toBe(
      'one_sided_setup_verified_towel',
    )

    const withoutPublishedExactStop = structuredClone(observed)
    withoutPublishedExactStop.round5_setup!.lanes.lakebase!.state = 'towelled'
    withoutPublishedExactStop.lanes.lakebase.state = 'towelled'
    withoutPublishedExactStop.lanes.lakebase.elapsed_ms = null
    withoutPublishedExactStop.towel!.censored_lower_bounds_ms!.lakebase = 60_840.221846
    expect(classifyOutcome(withoutPublishedExactStop).outcome.outcome_id).toBe(
      'setup_incomplete',
    )
  })
})

describe('Ringside output behavior', () => {
  it('reads the Software Engineer a Round 1 bout the way a person would say it', () => {
    const briefing = buildRingsideBriefing(
      verifiedSession('wake_idle_app'),
      'software_engineer',
      ['performance'],
    )
    expect(briefing.result).toBe(
      "Both databases were asleep at the bell. Lakebase was awake and committing in 1.23s. Aurora Serverless v2 took 5.68s. That's the database alone: first connection to a committed write, read back.",
    )
    expect(briefing.why).toBe(
      'The first request after idle is the one that times out, retries, and gets the bug filed. That wait is the number your retry logic has to cover.',
    )
    expect(briefing.lenses).toEqual([{
      priority: 'performance',
      text: 'The clock started at the first connection attempt and stopped at a committed write read back, not at available in a console. Everything after that, app startup included, is yours to add.',
    }])
    expect(briefing.ask).toBe("What's your app's timeout on the first request after idle?")
    expect(briefing.outcomeId).toBe('verified_comparison')
    expect(briefing.declared).toBe(true)
  })

  it('keeps the result invariant across people and priorities while the rest moves with them', () => {
    const session = verifiedSession('make_schema_change_safely')
    const results = new Set<string>()
    const whys = new Set<string>()
    const asks = new Set<string>()
    for (const personaId of PERSONA_IDS) {
      for (const corners of PRIORITY_SETS) {
        const briefing = buildRingsideBriefing(session, personaId, corners)
        results.add(briefing.result)
        whys.add(briefing.why)
        asks.add(briefing.ask)
        expect(briefing.lenses.map((lens) => lens.priority)).toEqual(orderedPriorities(corners))
      }
    }
    expect(results.size).toBe(1)
    expect(whys.size).toBe(PERSONA_IDS.length)
    expect(asks.size).toBe(PERSONA_IDS.length)
  })

  it('shows one labeled lens per priority the room picked, in reading order', () => {
    const session = verifiedSession('wake_idle_app')
    const briefing = buildRingsideBriefing(session, 'dba', ['performance', 'cost'])
    expect(briefing.lenses.map((lens) => lens.priority)).toEqual(['cost', 'performance'])
    expect(briefing.lenses[0].text).toMatch(/Aurora's floor is five times longer/)
    expect(briefing.lenses[1].text).toMatch(/^The clock started at the first connection attempt/)
    expect(buildRingsideBriefing(session, 'dba', ['simplicity']).lenses.map((lens) => lens.priority)).toEqual(['simplicity'])
  })

  it('tells a towel as a floor, never as an opponent time', () => {
    expect(buildRingsideResult(oneSidedTowel())).toBe(
      "The same order was deleted from both databases at the same instant. Lakebase had it back and readable in 1.23s. We stopped waiting at 90.00s. Aurora Serverless v2 was still recovering, so its time is more than 90.00s, not a measurement. Failover wasn't tested.",
    )
    expect(buildRingsideResult(noResultTowel())).toBe(
      "No result. We stopped at 90.00s with neither side finished: Lakebase was past 90.00s and Aurora Serverless v2 past 90.00s. Those are floors, not recovery times. Failover wasn't tested.",
    )
  })

  it('states a one-sided Round 5 towel consistently for every person in the room', () => {
    const session = oneSidedRoundFiveSetupTowel()
    const classified = classifyOutcome(session)
    const expectedResult = 'Lakebase had its pooled path up in 2.63s. Aurora Serverless v2 + RDS Proxy was still setting up at 60.84s, so its time is more than that. We stopped before the 10,000-client test ran, so there is no winner and no margin.'
    expect(classified.headline).toBe(
      'LAKEBASE SETUP VERIFIED 2.63s · AURORA SERVERLESS V2 + RDS PROXY UNVERIFIED BEYOND 60.84s · BOUNDED CHECK NOT RUN · NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A',
    )
    expect(classified.formalWinner).toBeNull()
    expect(classified.marginMs).toBeNull()
    expect(classified.shareable).toBe(false)
    expect(classified.scorecardEligible).toBe(true)
    for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
      for (const corners of PRIORITY_SETS) {
        const briefing = buildRingsideBriefing(session, personaId, corners)
        expect(briefing.outcomeId).toBe('one_sided_setup_verified_towel')
        expect(briefing.declared).toBe(false)
        expect(briefing.result).toBe(expectedResult)
        expect(briefing.why).toBe(getRoleLine('survive_connection_spike', personaId).why)
        expect(briefing.ask).toBe(getRoleLine('survive_connection_spike', personaId).ask)
        expect(`${briefing.result} ${briefing.why}`).not.toMatch(
          /neither setup|neither readiness|no verified result/i,
        )
      }
    }
  })

  it('keeps every Round 5 briefing truthful for every person, priority, competitor, and outcome', () => {
    const outcomeSessions = [
      oneSidedRoundFiveSetupTowel(),
      incompleteSetup(),
      failedSpike(),
      failedCleanup(),
      noResult('survive_connection_spike'),
    ]
    const forbidden = /\b(?:spike|survive|readiness|contract test)\b|[–—]|customer customer/i
    const bluntTenThousandDisclaimer = /(?:did|does) not (?:load-)?test.{0,32}10,000|10,000.{0,48}(?:not tested|not exercised|were not tested)|not (?:a )?10,000-client/i
    const protocolJargon = /\b(?:witness|scheduled clients?|terminal clients?|setup stop|128 attempts?|maximum 64 concurrent)\b/i

    for (const competitorId of ['aurora_serverless_v2', 'rds_postgres'] as const) {
      const session = verifiedSession('survive_connection_spike', competitorId)
      for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
        for (const corners of PRIORITY_SETS) {
          const briefing = buildRingsideBriefing(session, personaId, corners)
          const spoken = [briefing.result, briefing.why, ...briefing.lenses.map((lens) => lens.text), briefing.ask].join(' ')
          // The fan-in protocol measures exactly 10,000 held clients, so the result
          // names it; staying silent would be the inaccuracy.
          // This fixture is a legacy fan-in bout: a shared start after setup, so setup is
          // told as what came first rather than as part of a clock it was never in.
          expect(briefing.result).toContain(
            'Both paths got exactly 10,000 client connections open and held them for 30 seconds, timed from a shared start once both paths were ready.',
          )
          expect(briefing.result).toMatch(/Getting ready came first and is not in those times: Lakebase's pooled path was ready in 12\.35s; the AWS path needed 24\.00s to build an RDS Proxy before its first client could connect\./)
          expect(briefing.result).toMatch(/client connections, not database backends/)
          expect(briefing.result).toMatch(/64 test queries/)
          expect(briefing.result).toMatch(/Not measured: throughput, direct connections, or a Proxy built ahead of time\.$/)
          expect(briefing.why).not.toMatch(protocolJargon)
          expect(`${briefing.why} ${briefing.ask}`).not.toMatch(forbidden)
          expect(spoken).not.toMatch(bluntTenThousandDisclaimer)
          expect(spoken).not.toMatch(/<[A-Z0-9_]+>/)
        }
      }
    }

    for (const session of outcomeSessions) {
      for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
        for (const corners of PRIORITY_SETS) {
          const briefing = buildRingsideBriefing(session, personaId, corners)
          const spoken = [briefing.result, briefing.why, ...briefing.lenses.map((lens) => lens.text), briefing.ask].join(' ')
          expect(briefing.declared).toBe(false)
          expect(briefing.result).not.toBe('')
          // The bounded round measured 128 attempts and is retired; no outcome may
          // still describe the attempt count this protocol no longer runs.
          expect(briefing.result).not.toMatch(/128[- ]attempts?|maximum 64 concurrent/i)
          expect(briefing.why).not.toMatch(protocolJargon)
          expect(briefing.ask).toMatch(/\?$/)
          expect(`${briefing.why} ${briefing.ask}`).not.toMatch(forbidden)
          expect(spoken).not.toMatch(bluntTenThousandDisclaimer)
          expect(spoken).not.toMatch(/<[A-Z0-9_]+>/)
        }
      }
    }
  })

  it('tells a failed Round 5 check and an unready setup in plain words', () => {
    expect(buildRingsideResult(failedSpike())).toBe(
      'Both paths reached 10,000 clients from the same bell, but Lakebase and Aurora Serverless v2 both failed a required check, so there is no winner and no margin. The checks: every client held for 30 seconds, 64 of 64 test queries answered, fewer backends than clients, and clean identity, fairness, telemetry, and cleanup.',
    )
    expect(buildRingsideResult(incompleteSetup())).toBe(
      "No result. Neither pooled path was ready, so the 10,000-client test never ran. Lakebase's built-in pooler: Transaction gate did not verify. Aurora Serverless v2: Transaction gate did not verify.",
    )
    expect(buildRingsideResult(failedCleanup())).toBe(
      "The bout declared no winner. Then tearing down the bout's pooling setup failed at proxy deletion verification. The result stands, sharing is blocked, and Round 5 stays closed until the cleanup succeeds.",
    )
  })

  it('tells a bell-protocol Round 5 bout with setup inside the other side’s clock', () => {
    // Under the bell protocol each clock starts at the bell, so the AWS lane's time
    // to 10,000 contains building its Proxy, and the line attributes it.
    const session = verifiedSession('survive_connection_spike')
    const runtimeLane = (id: 'lakebase' | 'competitor', bellTo10kMs: number, readyMs: number) => ({
      id,
      phase: 'verified' as const,
      elapsed_at_snapshot_ms: bellTo10kMs + 30_000,
      bell_to_10000_observed_ms: bellTo10kMs,
      observation_uncertainty_ms: 5,
      pooled_path_ready_observed_ms: readyMs,
      ramp_started_observed_ms: readyMs,
      ramp_time_to_10000_ms: bellTo10kMs - readyMs,
      clients_initiated: 10_000,
      clients_authenticated: 10_000,
      held_clients: 10_000,
      sampled_queries_succeeded: 64,
      status: 'Verified',
    })
    session.round5_runtime = {
      protocol: 'round5-bell-to-10k-v4',
      warm_generation: 7,
      bell_id: 'bell-one',
      revision: 9,
      state: 'verified',
      bell_at_utc: session.created_at,
      lanes: {
        lakebase: runtimeLane('lakebase', 12_350, 400),
        competitor: runtimeLane('competitor', 36_000, 24_000),
      },
    }
    // A bell bout's margin is declared on the bell clock, not the fan-in clock.
    session.comparison = {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bell_to_10000_observed_ms', value: 23_650, display_value: '23.65s' },
      detail: 'fixture',
    }
    const result = buildRingsideResult(session)
    expect(result).toContain('held them for 30 seconds, timed from one bell.')
    expect(result).toContain(
      "Lakebase's built-in pooler: 12.35s. Aurora Serverless v2: 36.00s. Of the AWS path's time, 24.00s went to building an RDS Proxy before its first client could connect.",
    )
    expect(result).not.toMatch(/shared start/)
    expect(buildRingsideBriefing(session, 'executive', ['cost']).declared).toBe(true)
  })

  it('refuses missing placeholders instead of rendering raw tokens', () => {
    expect(() => interpolateProof('Observed <MISSING_VALUE>.', {})).toThrow(/MISSING_VALUE/)
    // A towel with no recorded floor for the opponent is a one-sided result, not a bound.
    const missingLowerBound = oneSidedTowel()
    missingLowerBound.towel!.censored_lower_bounds_ms = {}
    expect(buildRingsideResult(missingLowerBound)).toBe(
      "The same order was deleted from both databases at the same instant. Lakebase had it back and readable in 1.23s. Aurora Serverless v2 never finished, so there is a result but no margin. Failover wasn't tested.",
    )
  })

  it('reads priorities in one order and rejects an empty selection', () => {
    expect(orderedPriorities(['performance', 'cost'])).toEqual(['cost', 'performance'])
    expect(orderedPriorities(['simplicity', 'performance', 'cost'])).toEqual(['cost', 'simplicity', 'performance'])
    expect(() => orderedPriorities([])).toThrow(/one to three priorities/i)
    expect(() => buildRingsideBriefing(verifiedSession('wake_idle_app'), 'dba', [])).toThrow(/one to three priorities/i)
  })

  it('resolves every round, person, priority set, and outcome into complete copy', () => {
    const sessions: DemoSession[] = [
      ...ROUND_IDS.map((roundId) => verifiedSession(roundId)),
      ...ROUND_IDS.map((roundId) => verifiedSession(roundId, 'rds_postgres')),
      ...ROUND_IDS.map((roundId) => verifiedSession(roundId, 'azure_postgres')),
      ...ROUND_IDS.map((roundId) => noResult(roundId)),
      oneSided('wake_idle_app'),
      oneSided('make_schema_change_safely'),
      oneSided('recover_deleted_order'),
      competitorOnly('make_schema_change_safely'),
      oneSidedTowel(),
      noResultTowel(),
      roundFourWithoutAwsLane(),
      roundFourTie(),
      roundFourStoppage(),
      roundFourErroredLane(),
      roundFourNoResultTowel(),
      roundSixWithoutAwsLane(),
      roundSixTie(),
      roundSixStoppage(),
      roundSixErroredLane(),
      roundSixNoResultTowel(),
      oneSidedRoundFiveSetupTowel(),
      noVerifiedRoundFiveSetupTowel(),
      incompleteSetup(),
      failedSpike(),
      failedCleanup(),
      verifiedCleanupFailure(),
      ...ROUND_IDS.map((roundId) => cooldownCleanupFailure(roundId)),
    ]
    for (const session of sessions) {
      for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
        for (const corners of PRIORITY_SETS) {
          const briefing = buildRingsideBriefing(session, personaId, corners)
          const label = `${session.round.id}/${session.competitor.id}/${briefing.outcomeId}/${personaId}/${corners.join('+')}`
          expect(briefing.result, label).not.toBe('')
          expect(briefing.why, label).not.toBe('')
          expect(briefing.ask, label).toMatch(/\?$/)
          expect(briefing.lenses, label).toHaveLength(corners.length)
          const spoken = [briefing.result, briefing.why, ...briefing.lenses.map((lens) => lens.text), briefing.ask].join(' ')
          expect(spoken, label).not.toMatch(/<[A-Z0-9_]+>/)
          expect(spoken, label).not.toMatch(/undefined|null|NaN/)
        }
      }
    }
  })
})
