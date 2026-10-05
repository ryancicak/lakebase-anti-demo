import { describe, expect, it } from 'vitest'

import type { DemoSession, LaneSnapshot } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { reconcileRunEventSession, selectRound4Session } from './round4'
import {
  ROUND_SIX_CONDITION,
  isRoundSix,
  laneMetricValue,
  liveOrderEvidence,
  roundSixLaneLabel,
  roundSixStackLabel,
  roundSixUnsupportedReason,
} from './round6'

const aurora = { competitor: FALLBACK_CATALOG.competitors[0] }
const rds = { competitor: FALLBACK_CATALOG.competitors[1] }

function lane(evidence: Record<string, unknown> | undefined, status = 'Verified'): LaneSnapshot {
  return {
    id: 'lakebase',
    name: 'Lakebase',
    state: 'verified',
    elapsed_ms: 1_000,
    attempts: 1,
    status,
    error: null,
    evidence,
  }
}

describe('Round 6 lanes', () => {
  it('names each lane by what carries its checkout into Delta, in the server’s words', () => {
    expect(roundSixStackLabel(aurora, 'lakebase')).toBe('Lakebase built-in change feed')
    expect(roundSixStackLabel(aurora, 'competitor')).toBe('AWS DMS + Glue from Aurora Serverless v2')
    expect(roundSixStackLabel(rds, 'competitor')).toBe('AWS DMS + Glue from RDS PostgreSQL')
  })

  it('labels only the AWS lane a cold start: Lakebase’s feed is built in and always on', () => {
    expect(roundSixLaneLabel(aurora, 'lakebase')).toBe('Lakebase built-in change feed')
    expect(roundSixLaneLabel(aurora, 'competitor')).toBe('AWS DMS + Glue from Aurora Serverless v2 (cold start)')
    expect(ROUND_SIX_CONDITION).toBe('Lakebase: built-in change feed · AWS: DMS + Glue (cold start)')
    expect(`${roundSixLaneLabel(aurora, 'lakebase')} ${ROUND_SIX_CONDITION}`).not.toMatch(/warm/i)
  })

  it('reads the bout’s order from lane evidence and never invents a missing field', () => {
    expect(liveOrderEvidence(lane({
      order_id: 'order-42',
      total_display: '$84.50',
      proof_nonce: 'r6-bout-0123',
      checkout_guardrail_order_id: 'order-43',
    }))).toEqual({
      orderId: 'order-42',
      totalDisplay: '$84.50',
      proofNonce: 'r6-bout-0123',
      guardrailOrderId: 'order-43',
    })
    expect(liveOrderEvidence(lane(undefined))).toEqual({
      orderId: '—',
      totalDisplay: '—',
      proofNonce: '—',
      guardrailOrderId: '—',
    })
  })

  it('finds a lane’s own figure among metrics reported once per lane', () => {
    const session = {
      metrics: [
        { spec_id: 'bell_to_exact_history_ms', lane_id: 'lakebase', value: 11_520 },
        { spec_id: 'bell_to_exact_history_ms', lane_id: 'competitor', value: 74_100 },
        { spec_id: 'commit_skew_ms', value: 3 },
      ],
    }
    expect(laneMetricValue(session, 'bell_to_exact_history_ms', 'competitor')?.value).toBe(74_100)
    expect(laneMetricValue(session, 'bell_to_exact_history_ms', 'lakebase')?.value).toBe(11_520)
    expect(laneMetricValue(session, 'commit_skew_ms', 'lakebase')).toBeUndefined()
  })

  it('says why an AWS lane is not installed, from the server’s own reason', () => {
    expect(roundSixUnsupportedReason(lane({ unsupported_reason: 'Round 6’s AWS lane is not installed.' }, 'AWS lane not installed')))
      .toBe('Round 6’s AWS lane is not installed.')
    expect(roundSixUnsupportedReason(lane({}, 'AWS lane not installed on this installation')))
      .toBe('AWS lane not installed on this installation')
  })

  it('recognizes Round 6 by its round id alone', () => {
    expect(isRoundSix({ round: { id: 'analyze_live_orders_without_slowing_checkout' } })).toBe(true)
    expect(isRoundSix({ round: { id: 'put_model_score_in_app' } })).toBe(false)
    expect(isRoundSix(null)).toBe(false)
  })
})

function awsLane(state: LaneSnapshot['state'], status: string): LaneSnapshot {
  return { id: 'competitor', name: 'Aurora Serverless v2', state, elapsed_ms: null, attempts: 0, status, error: null }
}

function roundSixSession(state: DemoSession['state'], updatedAt: string, competitor: LaneSnapshot): DemoSession {
  const primary = FALLBACK_CATALOG.personas[0]
  return {
    id: 'round-6', state, created_at: '2026-09-30T02:27:47Z', updated_at: updatedAt,
    competitor: FALLBACK_CATALOG.competitors[0], primary_persona: primary,
    secondary_personas: [], corners: ['performance'],
    round: { ...FALLBACK_CATALOG.rounds[5], availability: 'ready' },
    recommendation_reason: '',
    presenter_pack: {
      opening: '', discovery_question: '', risk: '', stop_condition: '', remembered_metric: '',
      primary: { persona_id: primary.id, nickname: primary.nickname, role: primary.role, interpretation: '', objection: '', response: '' },
      secondary: [], closing: '',
    },
    lanes: {
      lakebase: { ...awsLane('sealed', 'Change feed streaming'), id: 'lakebase', name: 'Lakebase' },
      competitor,
    },
    fairness: { same_client: false, same_transaction: false, same_nonce: false, launch_skew_ms: null },
    remembered_result: null, failure: null,
  }
}

describe('Round 6 at the bell', () => {
  it('starts both clocks at the bell, even after a Prepare that began with the AWS lane not supported', () => {
    // Ryan's first try: the arm call returned the AWS lane not supported, and the
    // browser held that "NOT INSTALLED" through Prepare and past the bell.
    const created = roundSixSession('draft', '2026-09-30T02:27:47Z', awsLane('sealed', ''))
    expect(isRoundSix(created)).toBe(true)
    let current = selectRound4Session(created, roundSixSession(
      'checking', '2026-09-30T02:27:47.5Z', awsLane('not_supported', 'AWS lane not timed for this native CDF proof'),
    ))!
    const armed = roundSixSession('armed', '2026-09-30T02:27:52Z', awsLane('sealed', 'Parked, with the baseline in its source'))
    current = reconcileRunEventSession(current, {
      sequence: 5, event: 'armed', occurred_at: armed.updated_at,
      payload: { state: 'armed', evidence: {}, session: armed },
    }).session!
    expect(current.lanes.competitor.state).toBe('sealed')
    const running = roundSixSession('running', '2026-09-30T02:27:54Z', awsLane('sealed', 'Reading the source’s baseline before the bell'))
    current = reconcileRunEventSession(current, {
      sequence: 6, event: 'run_started', occurred_at: running.updated_at,
      payload: { state: 'running', lanes: ['lakebase', 'competitor'], session: running },
    }).session!
    for (const [sequence, laneId] of [[7, 'lakebase'], [8, 'competitor']] as const) {
      const bell = reconcileRunEventSession(current, {
        sequence, event: 'lane_update', occurred_at: '2026-09-30T02:27:54.5Z',
        payload: { lane_id: laneId, state: 'connecting', elapsed_ms: 0, status: 'Committing the checkout' },
      })
      expect(bell.accepted).toBe(true)
      current = bell.session!
    }
    expect(current.lanes.lakebase.state).toBe('connecting')
    expect(current.lanes.competitor.state).toBe('connecting')
  })

  it('still holds an AWS lane that Prepare found not installed against a stray update', () => {
    const decided = roundSixSession('armed', '2026-09-30T02:27:52Z', awsLane('not_supported', 'AWS lane not installed on this installation'))
    const stray = reconcileRunEventSession(decided, {
      sequence: 5, event: 'lane_update', occurred_at: decided.updated_at,
      payload: { lane_id: 'competitor', state: 'connecting', status: 'stale' },
    })
    expect(stray.accepted).toBe(false)
    expect(stray.session!.lanes.competitor.state).toBe('not_supported')
  })
})
