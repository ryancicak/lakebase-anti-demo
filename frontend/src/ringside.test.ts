import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import type {
  CompetitorId,
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
  OUTCOME_COPY_RECORDS,
  OUTCOME_COPY_SHA256,
  PERSONA_IDS,
  PRIORITY_KEYS,
  ROUND_FIVE_PERSONA_OUTCOME_RECORDS,
  ROUND_FIVE_PERSONA_OUTCOMES_SHA256,
  ROUND_IDS,
  VERIFIED_CORPUS_RECORDS,
  VERIFIED_CORPUS_SHA256,
  buildRingsideCue,
  buildRingsideShow,
  classifyEvidence,
  classifyOutcome,
  classifyRingsideOutcome,
  getOutcomeRecord,
  getRoundFivePersonaOutcomeRecord,
  getVerifiedRecord,
  interpolateProof,
  priorityKeyFor,
  type RingsideOutcomeId,
} from './ringside-cues'

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
  const competitor = FALLBACK_CATALOG.competitors.find((candidate) => candidate.id === competitorId)!
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

describe('canonical Ringside sources', () => {
  it('preserves the exact approved files and hashes', () => {
    const verified = sourceText('verified-corpus.jsonl')
    const outcomes = sourceText('outcome-copy.jsonl')
    const roundFivePersonaOutcomes = sourceText('round5-persona-outcomes.jsonl')
    expect(sha256(verified)).toBe(VERIFIED_CORPUS_SHA256)
    expect(sha256(outcomes)).toBe(OUTCOME_COPY_SHA256)
    expect(sha256(roundFivePersonaOutcomes)).toBe(ROUND_FIVE_PERSONA_OUTCOMES_SHA256)
    expect(parsedSource('verified-corpus.jsonl')).toHaveLength(420)
    expect(parsedSource('outcome-copy.jsonl')).toHaveLength(33)
    expect(parsedSource('round5-persona-outcomes.jsonl')).toHaveLength(50)
  })

  it('loads the canonical JSONL directly without a generated copy layer', () => {
    expect(VERIFIED_CORPUS_RECORDS).toEqual(parsedSource('verified-corpus.jsonl'))
    expect(OUTCOME_COPY_RECORDS).toEqual(parsedSource('outcome-copy.jsonl'))
    expect(ROUND_FIVE_PERSONA_OUTCOME_RECORDS).toEqual(
      parsedSource('round5-persona-outcomes.jsonl'),
    )
  })

  it('contains the complete 6 × 10 × 7 Cartesian product', () => {
    const expected = new Set<string>()
    for (const roundId of ROUND_IDS) {
      for (const personaId of PERSONA_IDS) {
        for (const priorityKey of PRIORITY_KEYS) {
          expected.add(`${roundId}/${personaId}/${priorityKey}`)
        }
      }
    }
    const actual = new Set(VERIFIED_CORPUS_RECORDS.map(
      (record) => `${record.round_id}/${record.persona_id}/${record.priority_key}`,
    ))
    expect(actual).toEqual(expected)
  })

  it('preserves every reviewed record ID, decision, and exact text', () => {
    for (const record of VERIFIED_CORPUS_RECORDS) {
      expect(getVerifiedRecord(record.round_id, record.persona_id, record.priority_key)).toBe(record)
      expect(record.meaning_record_id).toMatch(/^r[1-6]\./)
      expect(record.question_record_id).toMatch(/^r[1-6]\./)
      expect(record.meaning_decision).toMatch(/^(KEEP|REWRITE)$/)
      expect(record.question_decision).toMatch(/^(KEEP|REWRITE)$/)
    }
    expect(getVerifiedRecord('wake_idle_app', 'software_engineer', 'performance')).toMatchObject({
      meaning_record_id: 'r1.say.software-engineer.performance',
      question_record_id: 'r1.ask.software-engineer.performance',
      meaning: 'Your app finished a real transaction after the database woke. That times the database by itself, separate from the rest of startup.',
      question: 'What timeout does your app enforce while the database wakes up?',
    })
  })

  it('preserves the exact 33-record outcome matrix', () => {
    // Rounds 4 and 6 race two lanes: a comparison, a one-sided result and a towel with
    // no verified lane, plus the capability case for an installation without its AWS
    // lane. Round 6's separate checkout is the server's to enforce, so it has no row.
    expect(OUTCOME_COPY_RECORDS.map(({ round_id, outcome_id }) => [round_id, outcome_id])).toEqual([
      ['wake_idle_app', 'verified_comparison'],
      ['make_schema_change_safely', 'verified_comparison'],
      ['recover_deleted_order', 'verified_comparison'],
      ['put_model_score_in_app', 'verified_comparison'],
      ['put_model_score_in_app', 'verified_capability_gap'],
      ['survive_connection_spike', 'verified_comparison'],
      ['analyze_live_orders_without_slowing_checkout', 'verified_comparison'],
      ['analyze_live_orders_without_slowing_checkout', 'verified_capability_gap'],
      ['wake_idle_app', 'verified_rds_capability_gap'],
      ['wake_idle_app', 'one_sided_verified'],
      ['make_schema_change_safely', 'one_sided_verified'],
      ['recover_deleted_order', 'one_sided_verified'],
      ['put_model_score_in_app', 'one_sided_verified'],
      ['analyze_live_orders_without_slowing_checkout', 'one_sided_verified'],
      ['recover_deleted_order', 'one_sided_towel_lower_bound'],
      ['survive_connection_spike', 'one_sided_setup_verified_towel'],
      ['wake_idle_app', 'cleanup_failed'],
      ['make_schema_change_safely', 'cleanup_failed'],
      ['recover_deleted_order', 'cleanup_failed'],
      ['put_model_score_in_app', 'cleanup_failed'],
      ['survive_connection_spike', 'setup_incomplete'],
      ['survive_connection_spike', 'bounded_check_failed'],
      ['survive_connection_spike', 'cleanup_failed'],
      ['analyze_live_orders_without_slowing_checkout', 'cleanup_failed'],
      ['wake_idle_app', 'no_result'],
      ['make_schema_change_safely', 'no_result'],
      ['recover_deleted_order', 'no_result'],
      ['put_model_score_in_app', 'no_result'],
      ['survive_connection_spike', 'no_result'],
      ['analyze_live_orders_without_slowing_checkout', 'no_result'],
      ['recover_deleted_order', 'towel_no_verified_lane'],
      ['put_model_score_in_app', 'towel_no_verified_lane'],
      ['analyze_live_orders_without_slowing_checkout', 'towel_no_verified_lane'],
    ])
    for (const record of OUTCOME_COPY_RECORDS) {
      expect(getOutcomeRecord(record.round_id, record.outcome_id)).toBe(record)
    }
  })

  it('contains every Round 5 persona outcome override exactly once', () => {
    const outcomeIds = [
      'one_sided_setup_verified_towel',
      'setup_incomplete',
      'bounded_check_failed',
      'cleanup_failed',
      'no_result',
    ] as const
    const expected = new Set(
      outcomeIds.flatMap((outcomeId) => (
        PERSONA_IDS.map((personaId) => `${outcomeId}/${personaId}`)
      )),
    )
    const actual = new Set(ROUND_FIVE_PERSONA_OUTCOME_RECORDS.map(
      (record) => `${record.outcome_id}/${record.persona_id}`,
    ))
    expect(actual).toEqual(expected)
    for (const record of ROUND_FIVE_PERSONA_OUTCOME_RECORDS) {
      expect(getRoundFivePersonaOutcomeRecord(record.outcome_id, record.persona_id)).toBe(record)
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

  it('cannot give incomplete R4-R6 gates verified meaning or questions', () => {
    for (const session of [
      roundFourPartial,
      oneSidedRoundFiveSetupTowel(),
      incompleteSetup(),
      failedSpike(),
      failedCleanup(),
      roundSixPartial,
    ]) {
      const cue = buildRingsideCue(session, 'software_engineer', 'performance')
      expect(cue.outcome.copy_mode).toBe('OUTCOME_OVERRIDE')
      expect(cue.sayRecord.id).toMatch(/outcome|no-result/)
      if (session.round.id === 'survive_connection_spike') {
        expect(cue.askRecord.id).toMatch(/^r5\.ask\./)
      } else {
        expect(cue.askRecord.id).toMatch(/outcome|no-result/)
      }
      expect(cue.proofRecord.id).toBe(cue.outcome.proof_template_id)
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
    expect(buildRingsideShow(roundFourTie())).toBe(
      'Bell to exact app read, both integrations from a cold start: Lakebase 1.23s. AWS Glue → Aurora Serverless v2 1.30s. Model execution was not tested.',
    )
    expect(buildRingsideShow(roundFourPartial)).toBe(
      'Bell to exact app read: Lakebase 1.23s. Aurora Serverless v2 did not verify, so no margin was measured.',
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
    expect(buildRingsideShow(roundSixTie())).toBe(
      "Bell to exact Delta read, with AWS DMS and Glue cold starting at the bell and Lakebase's change feed built in: Lakebase 1.23s. AWS DMS + Glue from Aurora Serverless v2 1.30s. A separate checkout committed on each source. Throughput and p99 were not tested.",
    )
    expect(buildRingsideShow(roundSixPartial)).toBe(
      'Bell to exact Delta read: Lakebase 1.23s. Aurora Serverless v2 did not verify, so no margin was measured.',
    )
    expect(buildRingsideShow(roundSixWithoutAwsLane())).toBe(
      'Bell to exact Delta read: Lakebase 1.23s. A separate checkout committed. This installation has no AWS lane, so nothing was compared. Throughput and p99 were not tested.',
    )
    // Lakebase's feed is never called cold, and nothing calls it warm.
    const copy = [roundSixTie(), roundSixStoppage(), roundSixWithoutAwsLane()]
      .map((session) => buildRingsideShow(session))
      .join(' ')
    expect(copy).not.toMatch(/Lakebase[^.]*cold start|warm/i)
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
      const cue = buildRingsideCue(session, 'software_engineer', 'performance')
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
        expect(cue.outcome.outcome_id).not.toBe('cleanup_failed')
        expect(share).not.toMatch(/sharing (?:is )?blocked/i)
        return
      }
      expect(classified.status).toBe('cleanup_failure')
      expect(classified.outcome.outcome_id).toBe('cleanup_failed')
      expect(classified.shareable).toBe(false)
      expect(classified.headline).toMatch(/RESULT RETAINED · CLEANUP FAILED · SHARING BLOCKED/)
      expect(cue.outcome.outcome_id).toBe('cleanup_failed')
      expect(cue.say).toMatch(/cleanup did not verify.*fenced/i)
      expect(cue.show).toMatch(/sharing is blocked.*same round remains fenced/i)
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
      const cue = buildRingsideCue(session, 'software_engineer', 'performance')
      const receipt = receiptPresentation(session, 'round')
      const scorecard = scorecardEntry(session)
      const share = linkedInReceipt(session, 5)

      expect(cue.outcome).toBe(classified.outcome)
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
        cue.say,
        cue.show,
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
  it('renders the exact approved Software Engineer Round 1 performance copy', () => {
    const cue = buildRingsideCue(
      verifiedSession('wake_idle_app'),
      'software_engineer',
      'performance',
    )
    expect(cue.say).toBe(
      'Your app finished a real transaction after the database woke. That times the database by itself, separate from the rest of startup.',
    )
    expect(cue.ask).toBe('What timeout does your app enforce while the database wakes up?')
    expect(cue.show).toBe(
      'First committed transaction after idle: Lakebase 1.23s. Aurora Serverless v2 5.68s. Only the database transaction was tested.',
    )
  })

  it('keeps proof invariant across personas while persona copy changes', () => {
    const session = verifiedSession('make_schema_change_safely')
    const proof = PERSONA_IDS.map((personaId) => (
      buildRingsideCue(session, personaId, 'performance').show
    ))
    expect(new Set(proof)).toHaveLength(1)
    expect(
      buildRingsideCue(session, 'software_engineer', 'performance').ask,
    ).not.toBe(buildRingsideCue(session, 'executive', 'performance').ask)
  })

  it('changes meaning and question when priorities change', () => {
    const session = verifiedSession('wake_idle_app')
    const cost = buildRingsideCue(session, 'software_engineer', 'cost')
    const performance = buildRingsideCue(session, 'software_engineer', 'performance')
    expect(cost.say).not.toBe(performance.say)
    expect(cost.ask).not.toBe(performance.ask)
    expect(cost.show).toBe(performance.show)
  })

  it('interpolates lower bounds without inventing an exact opponent time', () => {
    expect(buildRingsideShow(oneSidedTowel())).toBe(
      'Deletion to exact recovered read: Lakebase 1.23s. Aurora Serverless v2 was still unverified at 90.00s, so its recovery time is greater than 90.00s. Failover was not tested.',
    )
    expect(buildRingsideShow(noResultTowel())).toBe(
      'No verified recovery result at 90.00s. Lakebase exceeded 90.00s. Aurora Serverless v2 exceeded 90.00s. Failover was not tested.',
    )
  })

  it('states a one-sided Round 5 towel consistently for every audience track', () => {
    const session = oneSidedRoundFiveSetupTowel()
    const classified = classifyOutcome(session)
    const expectedProof = 'Lakebase pooled-path setup verified at 2.63s. Aurora Serverless v2 + RDS Proxy exceeded 60.84s without verification. The 10,000-client fan-in never started.'
    expect(classified.headline).toBe(
      'LAKEBASE SETUP VERIFIED 2.63s · AURORA SERVERLESS V2 + RDS PROXY UNVERIFIED BEYOND 60.84s · BOUNDED CHECK NOT RUN · NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A',
    )
    expect(classified.formalWinner).toBeNull()
    expect(classified.marginMs).toBeNull()
    expect(classified.shareable).toBe(false)
    expect(classified.scorecardEligible).toBe(true)
    for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
      for (const priorityKey of PRIORITY_KEYS) {
        const cue = buildRingsideCue(session, personaId, priorityKey)
        expect(cue.outcome.outcome_id).toBe('one_sided_setup_verified_towel')
        expect(cue.outcome.copy_mode).toBe('OUTCOME_OVERRIDE')
        expect(cue.say).toBe(
          getRoundFivePersonaOutcomeRecord(
            'one_sided_setup_verified_towel',
            personaId,
          ).meaning,
        )
        expect(cue.ask).toBe(
          getVerifiedRecord('survive_connection_spike', personaId, priorityKey).question,
        )
        expect(cue.show).toBe(expectedProof)
        expect(`${cue.say} ${cue.show}`).not.toMatch(
          /neither setup|neither readiness|no verified result/i,
        )
      }
    }
  })

  it('keeps every Round 5 persona, priority, competitor, and outcome cue truthful', () => {
    const outcomeSessions = [
      oneSidedRoundFiveSetupTowel(),
      incompleteSetup(),
      failedSpike(),
      failedCleanup(),
      noResult('survive_connection_spike'),
    ]
    const forbidden = /\b(?:spike|survive|readiness|contract test)\b|[–—]|customer customer/i
    const bluntTenThousandDisclaimer = /(?:did|does) not (?:load-)?test.{0,32}10,000|10,000.{0,48}(?:not tested|not exercised|were not tested)|not (?:a )?10,000-client/i
    const sayProofJargon = /\b(?:witness|scheduled clients?|terminal clients?|setup stop|contract|128 attempts?|maximum 64 concurrent)\b/i

    for (const competitorId of ['aurora_serverless_v2', 'rds_postgres'] as const) {
      const session = verifiedSession('survive_connection_spike', competitorId)
      for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
        for (const priorityKey of PRIORITY_KEYS) {
          const cue = buildRingsideCue(session, personaId, priorityKey)
          expect(cue.say).toMatch(/\bup to 10,000 client connections\b/i)
          expect(cue.say).not.toMatch(sayProofJargon)
          expect(cue.show).toContain(
            'Both paths reached and held 10,000 clients, each timed from the same bell',
          )
          expect(cue.show).toMatch(/answered all 64 test queries sent over those held connections/i)
          expect(cue.show).toMatch(/Not measured:.*transaction throughput/i)
          // Under the bounded protocol the proof line was forbidden from naming
          // 10,000, because 10,000 was a product limit the bout never approached
          // and the round measured 128 attempts. The fan-in protocol measures
          // exactly 10,000 held clients, so naming it is now required and staying
          // silent would be the inaccuracy.
          expect(cue.show).toMatch(/\b10,000 clients\b/i)
          expect(`${cue.say} ${cue.ask}`).not.toMatch(forbidden)
          expect(`${cue.say} ${cue.ask} ${cue.show}`).not.toMatch(bluntTenThousandDisclaimer)
        }
      }
    }

    for (const session of outcomeSessions) {
      for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
        for (const priorityKey of PRIORITY_KEYS) {
          const cue = buildRingsideCue(session, personaId, priorityKey)
          expect(cue.say).toMatch(/\bup to 10,000 client connections\b/i)
          expect(cue.say).not.toMatch(sayProofJargon)
          expect(cue.ask).toMatch(/\?$/)
          expect(cue.show).not.toBe('')
          // Inverted with the protocol: the bounded round measured 128 attempts and
          // was forbidden from naming 10,000, which it never approached. The fan-in
          // round measures exactly 10,000 held clients, so the proof line must name
          // that and must not describe the attempt count it no longer runs.
          // Not every outcome names the target: a bout whose setup never verified
          // reports that, and inventing a client count for it would be worse than
          // silence. What holds across all of them is that none may still describe
          // the attempt count this protocol no longer runs. `cue.say` above already
          // requires the 10,000-client framing on every cue.
          expect(cue.show).not.toMatch(/128[- ]attempts?|maximum 64 concurrent/i)
          expect(`${cue.say} ${cue.ask}`).not.toMatch(forbidden)
          expect(`${cue.say} ${cue.ask} ${cue.show}`).not.toMatch(bluntTenThousandDisclaimer)
          expect(cue.say.trim().split(/\s+/).length).toBeLessThanOrEqual(34)
          expect(cue.ask.trim().split(/\s+/).length).toBeLessThanOrEqual(14)
        }
      }
    }
  })

  it('keeps Round 5 authored copy concise, natural, distinct, and non-vacuous', () => {
    const roundFiveVerified = VERIFIED_CORPUS_RECORDS.filter(
      (record) => record.round_id === 'survive_connection_spike',
    )
    expect(new Set(roundFiveVerified.map((record) => record.meaning))).toHaveLength(10)
    expect(new Set(roundFiveVerified.map((record) => record.question))).toHaveLength(70)
    expect(new Set(
      ROUND_FIVE_PERSONA_OUTCOME_RECORDS.map((record) => record.meaning),
    )).toHaveLength(10)

    for (const record of roundFiveVerified) {
      expect(record.meaning.trim().split(/\s+/).length).toBeLessThanOrEqual(34)
      expect(record.question.trim().split(/\s+/).length).toBeLessThanOrEqual(14)
      expect(record.meaning).toMatch(/\bup to 10,000 client connections\b/i)
      expect(record.meaning).not.toMatch(
        /\b(?:witness|scheduled clients?|terminal clients?|setup stop|contract|128 attempts?|maximum 64 concurrent)\b/i,
      )
      expect(record.question).toMatch(/\?$/)
      expect(record.question).not.toMatch(/^Which .+,.+,\s*(?:and\s+)?(?:which\s+)?owner/i)
      expect(record.question).not.toMatch(/What tradeoff matters most\?|separately value/i)
      expect(`${record.meaning} ${record.question}`).not.toMatch(
        /\b(?:spike|survive|readiness|contract test)\b|[–—]|customer customer/i,
      )
    }
    for (const record of ROUND_FIVE_PERSONA_OUTCOME_RECORDS) {
      expect(record.meaning.trim().split(/\s+/).length).toBeLessThanOrEqual(34)
      expect(record.meaning).toMatch(/\bup to 10,000 client connections\b/i)
      expect(record.meaning).not.toMatch(
        /\b(?:witness|scheduled clients?|terminal clients?|setup stop|contract|128 attempts?|maximum 64 concurrent)\b/i,
      )
      expect(record.meaning).not.toMatch(
        /\b(?:spike|survive|readiness|contract test)\b|[–—]|customer customer/i,
      )
    }

    const roleLeads = new Map(
      roundFiveVerified.map((record) => [record.persona_id, record.meaning]),
    )
    expect(roleLeads.size).toBe(10)
    const meaningfulWords = (text: string) => new Set(
      text.toLowerCase().match(/[a-z]+/g)?.filter(
        (word) => !['the', 'a', 'an', 'and', 'for', 'to', 'of', 'its', 'with', 'up'].includes(word),
      ) ?? [],
    )
    const entries = [...roleLeads.entries()]
    for (let left = 0; left < entries.length; left += 1) {
      for (let right = left + 1; right < entries.length; right += 1) {
        const leftWords = meaningfulWords(entries[left][1])
        const rightWords = meaningfulWords(entries[right][1])
        const overlap = [...leftWords].filter((word) => rightWords.has(word)).length
        const union = new Set([...leftWords, ...rightWords]).size
        expect(overlap / union).toBeLessThan(0.5)
      }
    }
  })

  it('refuses missing placeholders instead of rendering raw tokens', () => {
    expect(() => interpolateProof('Observed <MISSING_VALUE>.', {})).toThrow(/MISSING_VALUE/)
    const missingLowerBound = oneSidedTowel()
    missingLowerBound.towel!.censored_lower_bounds_ms = {}
    expect(buildRingsideShow(missingLowerBound)).toBe(
      'Deletion to exact recovered read: Lakebase 1.23s. Aurora Serverless v2 did not verify. Source deletion held. Failover was not tested.',
    )
  })

  it('canonicalizes priority order and rejects an empty selection', () => {
    expect(priorityKeyFor(['performance', 'cost'])).toBe('cost+performance')
    expect(priorityKeyFor(['simplicity', 'performance', 'cost'])).toBe('cost+simplicity+performance')
    expect(() => priorityKeyFor([])).toThrow(/one to three priorities/i)
  })

  it('resolves every approved audience track into non-empty copy', () => {
    for (const roundId of ROUND_IDS) {
      const session = verifiedSession(roundId)
      for (const personaId of PERSONA_IDS as readonly PersonaId[]) {
        for (const priorityKey of PRIORITY_KEYS) {
          const cue = buildRingsideCue(session, personaId, priorityKey)
          expect(cue.say).not.toBe('')
          expect(cue.ask).not.toBe('')
          expect(cue.show).not.toBe('')
        }
      }
    }
  })
})
