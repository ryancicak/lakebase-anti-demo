import { describe, expect, it } from 'vitest'
import { FALLBACK_CATALOG } from './catalog'
import type { DemoSession, RedoSnapshot, RunEvent } from './api/types'
import {
  ROUND_FOUR_FOOTER,
  ROUND_FOUR_LEGEND,
  ROUND_FOUR_SCOPE,
  acceptsReconciledSession,
  applyRunEventSnapshot,
  canStartRoundFourRedo,
  modelScoreEvidence,
  roundFourFooter,
  roundFourPresentation,
  roundFourUnsupportedReason,
} from './round4'

function proofLane(version: 'v1' | 'v2') {
  const v2 = version === 'v2'
  const nonce = `round4-${version}-nonce-full-value`
  return {
    id: 'lakebase' as const,
    name: 'Lakebase',
    state: 'verified' as const,
    elapsed_ms: v2 ? 720 : 840,
    attempts: 2,
    status: 'Exact committed version and fresh Postgres row verified',
    error: null,
    evidence: {
      primary_key: 'customer-42', score: v2 ? 0.33 : 0.81,
      model_version: v2 ? 'risk-v2' : 'risk-v1', proof_nonce: nonce,
      delta_version: v2 ? 12 : 11,
      verified_row: {
        primary_key: 'customer-42', score: v2 ? 0.33 : 0.81,
        model_version: v2 ? 'risk-v2' : 'risk-v1', proof_nonce: nonce,
      },
    },
  }
}

function redo(state: RedoSnapshot['state']): RedoSnapshot {
  return {
    state,
    lanes: {
      lakebase: { ...proofLane('v2'), state: state === 'running' ? 'verifying' : state === 'failed' ? 'failed' : 'verified' },
      competitor: { id: 'competitor', name: 'AWS', state: 'not_supported', elapsed_ms: null, attempts: 0, status: 'AWS lane not timed', error: null },
    },
  }
}

function session(updatedAt = '2026-08-18T20:00:00Z'): DemoSession {
  const primary = FALLBACK_CATALOG.personas[0]
  const round = { ...FALLBACK_CATALOG.rounds[3], availability: 'ready' as const }
  return {
    id: 'round-4', state: 'verified', created_at: updatedAt, updated_at: updatedAt,
    competitor: FALLBACK_CATALOG.competitors[0], primary_persona: primary,
    secondary_personas: [], corners: ['performance'], round,
    recommendation_reason: 'Managed Sync is configured.',
    presenter_pack: {
      opening: '', discovery_question: '', risk: '', stop_condition: '', remembered_metric: '',
      primary: { persona_id: primary.id, nickname: primary.nickname, role: primary.role, interpretation: '', objection: '', response: '' },
      secondary: [], closing: '',
    },
    lanes: {
      lakebase: proofLane('v1'),
      competitor: { id: 'competitor', name: 'AWS', state: 'not_supported', elapsed_ms: null, attempts: 0, status: 'AWS lane not timed', error: null },
    },
    fairness: { same_client: false, same_transaction: false, same_nonce: false, launch_skew_ms: null },
    remembered_result: 'LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A', failure: null,
    comparison: { kind: 'capability_gap', winner_lane_id: 'lakebase', margin: null },
    redo: { ...redo('ready'), lanes: { ...redo('ready').lanes, lakebase: { ...redo('ready').lanes.lakebase, state: 'sealed', evidence: {} } } },
  }
}

describe('Round 4 policy and state selection', () => {
  it('locks exact metadata and non-comparison copy', () => {
    expect(FALLBACK_CATALOG.rounds[0].redo).toMatchObject({ policy: 'show', badge: '★ SHOW', label: 'RE-DO ROUND' })
    expect(FALLBACK_CATALOG.rounds[1].redo).toMatchObject({ policy: 'optional', badge: 'OPTIONAL', label: 'RE-DO ROUND' })
    expect(FALLBACK_CATALOG.rounds[2].redo).toMatchObject({ policy: 'skip' })
    expect(FALLBACK_CATALOG.rounds[3].redo).toMatchObject({ policy: 'show', badge: '★ SHOW', label: 'CHANGE SCORE IN LAKEHOUSE → WATCH APP UPDATE' })
    expect(FALLBACK_CATALOG.rounds[4].redo).toBeUndefined()
    expect(FALLBACK_CATALOG.rounds[5].redo).toBeUndefined()
    expect(ROUND_FOUR_LEGEND).toBe('★ proves a different product behavior')
    expect(ROUND_FOUR_SCOPE).toBe('AWS NOT TIMED · MARGIN N/A')
    expect(ROUND_FOUR_FOOTER).toBe('LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A')
    expect(roundFourFooter({
      ...session(),
      state: 'running',
      comparison: null,
    })).toBe(ROUND_FOUR_SCOPE)
    expect(roundFourFooter(session())).toBe(ROUND_FOUR_FOOTER)
    expect(roundFourFooter({ ...session(), state: 'running', redo: redo('running') })).toBe(ROUND_FOUR_FOOTER)
    expect(roundFourFooter({ ...session(), redo: redo('verified') })).toBe(ROUND_FOUR_FOOTER)
    expect(roundFourFooter({ ...session(), redo: redo('failed') })).toBe(ROUND_FOUR_FOOTER)
    expect(roundFourFooter({
      ...session(),
      state: 'failed',
      lanes: {
        ...session().lanes,
        lakebase: { ...session().lanes.lakebase, state: 'failed' },
      },
      comparison: null,
      remembered_result: null,
    })).toBe(ROUND_FOUR_SCOPE)
  })

  it('gates redo to verified v1 with a ready show policy', () => {
    const ready = session()
    expect(canStartRoundFourRedo(ready)).toBe(true)
    expect(canStartRoundFourRedo({ ...ready, state: 'failed' })).toBe(false)
    expect(canStartRoundFourRedo({ ...ready, redo: redo('running') })).toBe(false)
    expect(roundFourPresentation({ ...ready, state: 'running', redo: null })).toBe('initial_running')
    expect(roundFourPresentation({ ...ready, state: 'failed', redo: null })).toBe('initial_failed')
    expect(roundFourPresentation({ ...ready, state: 'running', redo: redo('running') })).toBe('redo_running')
    expect(roundFourPresentation({ ...ready, redo: redo('verified') })).toBe('redo_verified')
    expect(roundFourPresentation({ ...ready, redo: { ...redo('failed'), failure: 'v2 failed' } })).toBe('redo_failed')
  })

  it('requires the exact row, including the full nonce', () => {
    expect(modelScoreEvidence(proofLane('v1')).exactRowVerified).toBe(true)
    const lane = proofLane('v1')
    lane.evidence.verified_row.proof_nonce = 'different'
    expect(modelScoreEvidence(lane).exactRowVerified).toBe(false)
  })
})

describe('Round 4 event and reconciliation policy', () => {
  it('routes initial lane events only to v1 and redo lane events only to v2', () => {
    const current = { ...session(), state: 'running' as const, redo: redo('running') }
    const initialEvent = {
      sequence: 1, event: 'lane_update', occurred_at: current.updated_at,
      payload: { lane_id: 'lakebase', state: 'failed', status: 'initial-only' },
    } satisfies RunEvent
    const initialUpdated = applyRunEventSnapshot(current, initialEvent)!
    expect(initialUpdated.lanes.lakebase.status).toBe('initial-only')
    expect(initialUpdated.redo!.lanes.lakebase.status).not.toBe('initial-only')

    const redoEvent = {
      sequence: 2, event: 'redo_lane_update', occurred_at: current.updated_at,
      payload: { lane_id: 'lakebase', state: 'verifying', status: 'redo-only' },
    } satisfies RunEvent
    const redoUpdated = applyRunEventSnapshot(current, redoEvent)!
    expect(redoUpdated.redo!.lanes.lakebase.status).toBe('redo-only')
    expect(redoUpdated.lanes.lakebase.status).not.toBe('redo-only')
  })

  it('replaces from full redo events and ignores unknown events', () => {
    const current = session()
    const replacement = { ...current, updated_at: '2026-08-18T20:00:02Z', redo: redo('verified') }
    const event = {
      sequence: 3, event: 'redo_finished', occurred_at: replacement.updated_at,
      payload: { session: replacement },
    } satisfies RunEvent
    expect(applyRunEventSnapshot(current, event)).toBe(replacement)
    expect(applyRunEventSnapshot(current, { sequence: 4, event: 'future_event', payload: {} } as unknown as RunEvent)).toBe(current)
  })

  it('rejects older GETs and terminal redo regression', () => {
    const running = { ...session('2026-08-18T20:00:02Z'), state: 'running' as const, redo: redo('running') }
    expect(acceptsReconciledSession(running, session('2026-08-18T20:00:01Z'))).toBe(false)
    const terminal = { ...session('2026-08-18T20:00:02Z'), redo: redo('verified') }
    expect(acceptsReconciledSession(terminal, { ...running, updated_at: '2026-08-18T20:00:03Z' })).toBe(false)
    expect(acceptsReconciledSession(running, { ...terminal, updated_at: '2026-08-18T20:00:03Z' })).toBe(true)

    const replayedInitial = applyRunEventSnapshot(terminal, {
      sequence: 5,
      event: 'lane_update',
      occurred_at: '2026-08-18T20:00:04Z',
      payload: { lane_id: 'lakebase', state: 'connecting', status: 'stale v1 replay' },
    })!
    const replayedRedo = applyRunEventSnapshot(terminal, {
      sequence: 6,
      event: 'redo_lane_update',
      occurred_at: '2026-08-18T20:00:05Z',
      payload: { lane_id: 'lakebase', state: 'verifying', status: 'stale v2 replay' },
    })!
    expect(replayedInitial.lanes.lakebase).toEqual(terminal.lanes.lakebase)
    expect(replayedRedo.redo!.lanes.lakebase).toEqual(terminal.redo!.lanes.lakebase)

    const terminalEvidenceUpdate = applyRunEventSnapshot(terminal, {
      sequence: 7,
      event: 'redo_lane_update',
      occurred_at: '2026-08-18T20:00:06Z',
      payload: { lane_id: 'lakebase', state: 'verified', status: 'authoritative terminal detail' },
    })!
    expect(terminalEvidenceUpdate.redo!.lanes.lakebase).toMatchObject({
      state: 'verified',
      status: 'authoritative terminal detail',
      evidence: terminal.redo!.lanes.lakebase.evidence,
    })

    const failedTerminal = { ...terminal, redo: redo('failed') }
    const replayedFailedAsVerified = applyRunEventSnapshot(failedTerminal, {
      sequence: 8,
      event: 'redo_lane_update',
      occurred_at: '2026-08-18T20:00:07Z',
      payload: { lane_id: 'lakebase', state: 'verified', status: 'stale terminal outcome' },
    })!
    expect(replayedFailedAsVerified.redo!.lanes.lakebase).toEqual(
      failedTerminal.redo!.lanes.lakebase,
    )
  })

  it('never switches between terminal redo outcomes', () => {
    const verified = { ...session('2026-08-18T20:00:02Z'), redo: redo('verified') }
    const failed = { ...session('2026-08-18T20:00:03Z'), redo: redo('failed') }
    expect(acceptsReconciledSession(verified, failed)).toBe(false)
    expect(acceptsReconciledSession(failed, {
      ...verified,
      updated_at: '2026-08-18T20:00:04Z',
    })).toBe(false)
  })

  it('uses the authoritative unsupported reason with a safe status fallback', () => {
    const lane = session().lanes.competitor
    expect(roundFourUnsupportedReason({
      ...lane,
      evidence: { unsupported_reason: 'No AWS-native equivalent lane was configured or timed in this scoped proof.' },
    })).toBe('No AWS-native equivalent lane was configured or timed in this scoped proof.')
    expect(roundFourUnsupportedReason({ ...lane, evidence: { unsupported_reason: 42 } })).toBe(lane.status)
  })
})
