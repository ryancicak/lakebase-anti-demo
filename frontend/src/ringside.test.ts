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
  const successfulLatencyMs = Array.from({ length: 128 }, (_, index) => 10 + index / 100)
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
    session.lanes.lakebase.evidence = {
      primary_key: 'customer-42',
      score: 0.81,
      model_version: 'risk-v1',
      proof_nonce: 'round4-proof',
      delta_version: 11,
      verified_row: {
        primary_key: 'customer-42',
        score: 0.81,
        model_version: 'risk-v1',
        proof_nonce: 'round4-proof',
      },
    }
    session.metrics = [
      { spec_id: 'application_proof_elapsed_ms', lane_id: 'lakebase', value: 1_840 },
    ]
    session.lanes.competitor.state = 'not_supported'
    session.lanes.competitor.elapsed_ms = null
    session.comparison = {
      kind: 'capability_gap',
      winner_lane_id: 'lakebase',
      margin: null,
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
    session.metrics = [
      { spec_id: 'analytics_available_ms', lane_id: 'lakebase', value: 2_430 },
      { spec_id: 'checkout_verified', lane_id: 'lakebase', value: true },
    ]
    session.lanes.competitor.state = 'not_supported'
    session.lanes.competitor.elapsed_ms = null
    session.comparison = {
      kind: 'capability_gap',
      winner_lane_id: 'lakebase',
      margin: null,
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
    expect(parsedSource('outcome-copy.jsonl')).toHaveLength(29)
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
      meaning: 'The application completed a real database transaction after wake. That isolates database readiness from the rest of application startup.',
      question: 'What timeout does the application enforce while the database wakes?',
    })
  })

  it('preserves the exact 29-record outcome matrix', () => {
    expect(OUTCOME_COPY_RECORDS.map(({ round_id, outcome_id }) => [round_id, outcome_id])).toEqual([
      ['wake_idle_app', 'verified_comparison'],
      ['make_schema_change_safely', 'verified_comparison'],
      ['recover_deleted_order', 'verified_comparison'],
      ['put_model_score_in_app', 'verified_capability_gap'],
      ['survive_connection_spike', 'verified_comparison'],
      ['analyze_live_orders_without_slowing_checkout', 'verified_capability_gap'],
      ['wake_idle_app', 'verified_rds_capability_gap'],
      ['wake_idle_app', 'one_sided_verified'],
      ['make_schema_change_safely', 'one_sided_verified'],
      ['recover_deleted_order', 'one_sided_verified'],
      ['recover_deleted_order', 'one_sided_towel_lower_bound'],
      ['survive_connection_spike', 'one_sided_setup_verified_towel'],
      ['put_model_score_in_app', 'score_identity_unverified'],
      ['wake_idle_app', 'cleanup_failed'],
      ['make_schema_change_safely', 'cleanup_failed'],
      ['recover_deleted_order', 'cleanup_failed'],
      ['put_model_score_in_app', 'cleanup_failed'],
      ['survive_connection_spike', 'setup_incomplete'],
      ['survive_connection_spike', 'bounded_check_failed'],
      ['survive_connection_spike', 'cleanup_failed'],
      ['analyze_live_orders_without_slowing_checkout', 'cleanup_failed'],
      ['analyze_live_orders_without_slowing_checkout', 'checkout_guardrail_unverified'],
      ['wake_idle_app', 'no_result'],
      ['make_schema_change_safely', 'no_result'],
      ['recover_deleted_order', 'no_result'],
      ['put_model_score_in_app', 'no_result'],
      ['survive_connection_spike', 'no_result'],
      ['analyze_live_orders_without_slowing_checkout', 'no_result'],
      ['recover_deleted_order', 'towel_no_verified_lane'],
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
  const roundFourPartial = verifiedSession('put_model_score_in_app')
  roundFourPartial.lanes.lakebase.evidence = undefined

  const roundSixPartial = verifiedSession('analyze_live_orders_without_slowing_checkout')
  roundSixPartial.metrics = [
    { spec_id: 'analytics_available_ms', lane_id: 'lakebase', value: 2_430 },
  ]

  const rdsGap = verifiedSession('wake_idle_app', 'rds_postgres')
  rdsGap.lanes.competitor.state = 'not_supported'
  rdsGap.lanes.competitor.elapsed_ms = null

  const cases: Array<[string, DemoSession, RingsideOutcomeId]> = [
    ['R1 verified comparison', verifiedSession('wake_idle_app'), 'verified_comparison'],
    ['R2 verified comparison', verifiedSession('make_schema_change_safely'), 'verified_comparison'],
    ['R3 verified comparison', verifiedSession('recover_deleted_order'), 'verified_comparison'],
    ['R4 verified capability', verifiedSession('put_model_score_in_app'), 'verified_capability_gap'],
    ['R5 verified comparison', verifiedSession('survive_connection_spike'), 'verified_comparison'],
    ['R6 verified capability', verifiedSession('analyze_live_orders_without_slowing_checkout'), 'verified_capability_gap'],
    ['R1 RDS gap', rdsGap, 'verified_rds_capability_gap'],
    ['R1 one-sided', oneSided('wake_idle_app'), 'one_sided_verified'],
    ['R2 one-sided', oneSided('make_schema_change_safely'), 'one_sided_verified'],
    ['R3 one-sided', oneSided('recover_deleted_order'), 'one_sided_verified'],
    ['R3 towel lower bound', oneSidedTowel(), 'one_sided_towel_lower_bound'],
    ['R5 one-sided setup towel', oneSidedRoundFiveSetupTowel(), 'one_sided_setup_verified_towel'],
    ['R5 both setups unverified at towel', noVerifiedRoundFiveSetupTowel(), 'setup_incomplete'],
    ['R4 identity incomplete', roundFourPartial, 'score_identity_unverified'],
    ['R5 setup incomplete', incompleteSetup(), 'setup_incomplete'],
    ['R5 bounded check failed', failedSpike(), 'bounded_check_failed'],
    ['R5 cleanup failed', failedCleanup(), 'cleanup_failed'],
    ['R6 guardrail incomplete', roundSixPartial, 'checkout_guardrail_unverified'],
    ...ROUND_IDS.map((roundId) => [`${roundId} no result`, noResult(roundId), 'no_result'] as [string, DemoSession, RingsideOutcomeId]),
    ['R3 towel without a verified lane', noResultTowel(), 'towel_no_verified_lane'],
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
    ['R4 capability', verifiedSession('put_model_score_in_app'), 'capability_gap', 'declared_capability', 'lakebase', null],
    ['R5 comparison', verifiedSession('survive_connection_spike'), 'both_exact_verified', 'declared_comparison', 'lakebase', 11_650],
    ['R6 capability', verifiedSession('analyze_live_orders_without_slowing_checkout'), 'capability_gap', 'declared_capability', 'lakebase', null],
    ['R2 Lakebase only', oneSided('make_schema_change_safely'), 'lakebase_only_exact', 'adjudicated_stoppage', 'lakebase', null],
    ['R2 competitor only', competitorOnly('make_schema_change_safely'), 'competitor_only_exact', 'adjudicated_stoppage', 'competitor', null],
    ['R3 exact plus bound', oneSidedTowel(), 'exact_and_censored_lower_bound', 'adjudicated_stoppage', 'lakebase', null],
    ['R3 both bounds', noResultTowel(), 'both_lower_bounds', 'no_verified_evidence', null, null],
    ['R4 guardrail', roundFourPartial, 'guardrail_failure', 'guardrail_failure', null, null],
    ['R5 verified-first', oneSidedRoundFiveSetupTowel(), 'exact_and_censored_lower_bound', 'comparison_incomplete', null, null],
    ['R5 both bounds', noVerifiedRoundFiveSetupTowel(), 'both_lower_bounds', 'no_verified_evidence', null, null],
    ['R5 bounded-check guardrail', failedSpike(), 'guardrail_failure', 'guardrail_failure', null, null],
    ['R5 cleanup before result', failedCleanup(), 'cleanup_failure', 'cleanup_failure', null, null],
    ['R5 cleanup after result', verifiedCleanupFailure(), 'cleanup_failure', 'cleanup_failure', 'lakebase', 11_650],
    ['R6 guardrail', roundSixPartial, 'guardrail_failure', 'guardrail_failure', null, null],
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
      expect(classified.status).toBe('cleanup_failure')
      expect(classified.outcome.outcome_id).toBe('cleanup_failed')
      expect(classified.shareable).toBe(false)
      expect(classified.headline).toMatch(/RESULT RETAINED · CLEANUP FAILED · SHARING BLOCKED/)
      expect(cue.outcome.outcome_id).toBe('cleanup_failed')
      if (roundId === 'survive_connection_spike') {
        expect(cue.say).toMatch(/up to 10,000 client connections/i)
        expect(cue.say).not.toMatch(/cleanup|fenced|128 attempts|maximum 64 concurrent/i)
      } else {
        expect(cue.say).toMatch(/cleanup did not verify.*fenced/i)
      }
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
      roundSixPartial,
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
        expect(classified.marginMs).toBeNull()
        expect(allCopy).not.toMatch(/aws.*\d+\.\d+s sooner|speed margin \d/i)
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
      'The application completed a real database transaction after wake. That isolates database readiness from the rest of application startup.',
    )
    expect(cue.ask).toBe('What timeout does the application enforce while the database wakes?')
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
    const expectedProof = 'Lakebase pooled-path setup verified at 2.63s. Aurora Serverless v2 + RDS Proxy exceeded 60.84s without verification. The recurring 128-attempt check at maximum 64 concurrent did not run.'
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
            'This bout verified pooled-path setup and 128 attempts at maximum 64 concurrent.',
          )
          expect(cue.show).toMatch(/separate 64-client multiplexing phase passed/i)
          expect(cue.show).toMatch(/direct AWS connections and alternative pools were not compared/i)
          expect(cue.show).not.toMatch(/10,000|ten thousand/i)
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
          expect(cue.show).toMatch(/128[- ]attempt/i)
          expect(cue.show).toMatch(/maximum 64 concurrent/i)
          expect(cue.show).not.toMatch(/10,000|ten thousand/i)
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
