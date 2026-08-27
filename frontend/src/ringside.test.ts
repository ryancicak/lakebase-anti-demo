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
import { FALLBACK_CATALOG } from './catalog'
import {
  OUTCOME_COPY_RECORDS,
  OUTCOME_COPY_SHA256,
  PERSONA_IDS,
  PRIORITY_KEYS,
  ROUND_IDS,
  VERIFIED_CORPUS_RECORDS,
  VERIFIED_CORPUS_SHA256,
  buildRingsideCue,
  buildRingsideShow,
  classifyRingsideOutcome,
  getOutcomeRecord,
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
    const laneEvidence = {
      scheduled_clients: 128,
      terminal_clients: 128,
      successful_clients: 128,
      error_clients: 0,
      successful_latency_ms: successfulLatencyMs,
      witness_verified_clients: 64,
      unique_backend_pids: 8,
      peak_backend_sessions: 16,
    }
    session.lanes.lakebase.evidence = laneEvidence
    session.lanes.competitor.evidence = laneEvidence
    session.fairness = {
      same_client: true,
      same_transaction: true,
      same_nonce: true,
      launch_skew_ms: 2,
      warmup_connections: 4,
      concurrency: 64,
      runner: 'Python 3.12 + psycopg 3.3.4',
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
      margin: { spec_id: 'setup_elapsed_ms', value: 11_650, display_value: '11.65s' },
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

function incompleteSetup(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'failed'
  const competitor = session.round5_setup!.lanes.competitor!
  competitor.state = 'failed'
  competitor.verified = false
  competitor.setup_elapsed_ms = null
  competitor.status = 'Transaction gate did not verify'
  session.round5_setup!.state = 'failed'
  session.round5_setup!.setup_validated = false
  return session
}

function failedSpike(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'failed'
  session.lanes.competitor.state = 'failed'
  session.lanes.competitor.evidence = undefined
  session.round5_setup!.state = 'failed'
  session.round5_setup!.downstream_validated = false
  return session
}

function failedCleanup(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'failed'
  session.round5_setup!.state = 'cleanup_failed'
  session.round5_setup!.cleanup_failure = 'proxy deletion verification'
  session.round5_setup!.cleanup_retryable = true
  return session
}

describe('canonical Ringside sources', () => {
  it('preserves the exact approved files and hashes', () => {
    const verified = sourceText('verified-corpus.jsonl')
    const outcomes = sourceText('outcome-copy.jsonl')
    expect(sha256(verified)).toBe(VERIFIED_CORPUS_SHA256)
    expect(sha256(outcomes)).toBe(OUTCOME_COPY_SHA256)
    expect(parsedSource('verified-corpus.jsonl')).toHaveLength(420)
    expect(parsedSource('outcome-copy.jsonl')).toHaveLength(23)
  })

  it('loads the canonical JSONL directly without a generated copy layer', () => {
    expect(VERIFIED_CORPUS_RECORDS).toEqual(parsedSource('verified-corpus.jsonl'))
    expect(OUTCOME_COPY_RECORDS).toEqual(parsedSource('outcome-copy.jsonl'))
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

  it('preserves the exact 23-record outcome matrix', () => {
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
      ['put_model_score_in_app', 'score_identity_unverified'],
      ['survive_connection_spike', 'setup_incomplete'],
      ['survive_connection_spike', 'spike_contract_failed'],
      ['survive_connection_spike', 'cleanup_failed'],
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
})

describe('one round-specific outcome classifier', () => {
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
    ['R4 identity incomplete', roundFourPartial, 'score_identity_unverified'],
    ['R5 setup incomplete', incompleteSetup(), 'setup_incomplete'],
    ['R5 spike failed', failedSpike(), 'spike_contract_failed'],
    ['R5 cleanup failed', failedCleanup(), 'cleanup_failed'],
    ['R6 guardrail incomplete', roundSixPartial, 'checkout_guardrail_unverified'],
    ...ROUND_IDS.map((roundId) => [`${roundId} no result`, noResult(roundId), 'no_result'] as [string, DemoSession, RingsideOutcomeId]),
    ['R3 towel without a verified lane', noResultTowel(), 'towel_no_verified_lane'],
  ]

  it.each(cases)('%s', (_label, session, expected) => {
    expect(classifyRingsideOutcome(session).outcome_id).toBe(expected)
  })

  it('cannot give incomplete R4-R6 gates verified meaning or questions', () => {
    for (const session of [roundFourPartial, incompleteSetup(), failedSpike(), failedCleanup(), roundSixPartial]) {
      const cue = buildRingsideCue(session, 'software_engineer', 'performance')
      expect(cue.outcome.copy_mode).toBe('OUTCOME_OVERRIDE')
      expect(cue.sayRecord.id).toMatch(/outcome|no-result/)
      expect(cue.askRecord.id).toMatch(/outcome|no-result/)
      expect(cue.proofRecord.id).toBe(cue.outcome.proof_template_id)
    }
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

  it('refuses missing placeholders instead of rendering raw tokens', () => {
    expect(() => interpolateProof('Observed <MISSING_VALUE>.', {})).toThrow(/MISSING_VALUE/)
    const missingLowerBound = oneSidedTowel()
    missingLowerBound.towel!.censored_lower_bounds_ms = {}
    expect(() => buildRingsideShow(missingLowerBound)).toThrow(/LOWER_BOUND_SECONDS/)
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
