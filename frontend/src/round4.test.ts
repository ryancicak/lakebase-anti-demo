import { describe, expect, it } from 'vitest'
import { FALLBACK_CATALOG } from './catalog'
import type { CooldownSnapshot, DemoSession, RedoSnapshot, RunEvent } from './api/types'
import {
  acceptsReconciledSession,
  applyRunEventSnapshot,
  isRoundFour,
  modelScoreEvidence,
  roundFourLaneLabel,
  roundFourStackLabel,
  roundFourUnsupportedReason,
  selectRound4Session,
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
  it('races both lanes as a measured round with no re-do', () => {
    expect(FALLBACK_CATALOG.rounds[0].redo).toMatchObject({ policy: 'show', badge: '★ SHOW', label: 'RE-DO ROUND' })
    expect(FALLBACK_CATALOG.rounds[1].redo).toMatchObject({ policy: 'optional', badge: 'OPTIONAL', label: 'RE-DO ROUND' })
    expect(FALLBACK_CATALOG.rounds[2].redo).toMatchObject({ policy: 'skip' })
    // A second change could not start both integrations cold, so racing again is a new bout.
    expect(FALLBACK_CATALOG.rounds[3].redo).toBeUndefined()
    expect(FALLBACK_CATALOG.rounds[4].redo).toBeUndefined()
    expect(FALLBACK_CATALOG.rounds[5].redo).toBeUndefined()
    const round = FALLBACK_CATALOG.rounds[3]
    expect(isRoundFour(session())).toBe(true)
    expect(round.comparison_kind).toBe('measured')
    expect(round.metric_specs?.find((spec) => spec.role === 'primary')?.id).toBe('bell_to_exact_read_ms')
    // Lakebase's own sync figure is shown and never compared.
    expect(round.metric_specs?.find((spec) => spec.id === 'managed_availability_ms')?.role).toBe('secondary')
    expect(round.non_claims?.[0]).toMatch(/Both integrations cold start at the bell/)
    expect(JSON.stringify(round)).not.toMatch(/application_proof_elapsed_ms|not executed or timed|separate reverse-ETL stack/i)
  })

  it('names each lane by the integration that carries the row', () => {
    const aurora = session()
    expect(roundFourStackLabel(aurora, 'lakebase')).toBe('Lakebase synced table')
    expect(roundFourStackLabel(aurora, 'competitor')).toBe('AWS Glue → Aurora Serverless v2')
    expect(roundFourStackLabel(
      { competitor: FALLBACK_CATALOG.competitors[1] },
      'competitor',
    )).toBe('AWS Glue → RDS PostgreSQL')
  })

  it('tags both lanes alike with their cold start, wherever a lane is labeled', () => {
    const aurora = session()
    expect(roundFourLaneLabel(aurora, 'lakebase')).toBe('Lakebase synced table (cold start)')
    expect(roundFourLaneLabel(aurora, 'competitor')).toBe(
      'AWS Glue → Aurora Serverless v2 (cold start)',
    )
  })

  it('reads the committed row from lane evidence and never invents a value', () => {
    expect(modelScoreEvidence(proofLane('v1'))).toEqual({
      primaryKey: 'customer-42',
      score: '0.81',
      modelVersion: 'risk-v1',
      proofNonce: 'round4-v1-nonce-full-value',
      deltaVersion: '11',
    })
    expect(modelScoreEvidence({ ...proofLane('v1'), evidence: {} })).toEqual({
      primaryKey: '—',
      score: '—',
      modelVersion: '—',
      proofNonce: '—',
      deltaVersion: '—',
    })
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

  it('rejects a same-timestamp GET that regresses an SSE-verified initial lane', () => {
    const verifiedLane = {
      ...session().lanes.lakebase,
      state: 'verified' as const,
      elapsed_ms: 812,
      status: 'Exact read-back verified',
    }
    const current = {
      ...session('2026-08-18T20:00:02Z'),
      state: 'running' as const,
      lanes: { ...session().lanes, lakebase: verifiedLane },
    }
    const stale = {
      ...current,
      lanes: {
        ...current.lanes,
        lakebase: {
          ...verifiedLane,
          state: 'verifying' as const,
          elapsed_ms: 500,
          status: 'Still checking',
        },
      },
    }

    expect(acceptsReconciledSession(current, stale)).toBe(false)
    expect(selectRound4Session(current, stale)).toBe(current)
  })

  it('merges initial and redo lane latches into a newer snapshot', () => {
    const current = {
      ...session('2026-08-18T20:00:02Z'),
      state: 'running' as const,
      lanes: {
        ...session().lanes,
        lakebase: {
          ...session().lanes.lakebase,
          state: 'verified' as const,
          elapsed_ms: 812,
          status: 'Initial verified',
        },
      },
      redo: redo('running'),
    }
    current.redo!.lanes.lakebase = {
      ...current.redo!.lanes.lakebase,
      state: 'verified',
      elapsed_ms: 440,
      status: 'Redo verified',
    }
    const newer = {
      ...current,
      updated_at: '2026-08-18T20:00:03Z',
      lanes: {
        ...current.lanes,
        lakebase: {
          ...current.lanes.lakebase,
          state: 'connecting' as const,
          elapsed_ms: null,
          status: 'Stale initial',
        },
      },
      redo: {
        ...current.redo!,
        lanes: {
          ...current.redo!.lanes,
          lakebase: {
            ...current.redo!.lanes.lakebase,
            state: 'verifying' as const,
            elapsed_ms: 100,
            status: 'Stale redo',
          },
        },
      },
    }

    const selected = selectRound4Session(current, newer)!
    expect(selected.updated_at).toBe(newer.updated_at)
    expect(selected.lanes.lakebase).toEqual(current.lanes.lakebase)
    expect(selected.redo!.lanes.lakebase).toEqual(current.redo!.lanes.lakebase)
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

function watchingCooldown(): CooldownSnapshot {
  return {
    mode: 'return_to_idle',
    state: 'watching',
    started_at: '2026-08-18T20:00:10Z',
    failure: null,
    lanes: {
      lakebase: {
        id: 'lakebase',
        name: 'Lakebase',
        state: 'watching',
        started_at: '2026-08-18T20:00:00Z',
        confirmed_at: null,
        elapsed_ms: null,
        status: 'Watching',
      },
      competitor: {
        id: 'competitor',
        name: 'Aurora Serverless v2',
        state: 'watching',
        started_at: '2026-08-18T20:00:10Z',
        confirmed_at: null,
        elapsed_ms: null,
        status: 'Watching',
      },
    },
  }
}

describe('Cooldown terminal lane merge policy', () => {
  it('latches the first confirmed elapsed value across later and reconciled snapshots', () => {
    const current = { ...session(), cooldown: watchingCooldown() }
    const confirmed = applyRunEventSnapshot(current, {
      sequence: 10,
      event: 'cooldown_update',
      occurred_at: '2026-08-18T20:01:17Z',
      payload: {
        cooldown: {
          ...watchingCooldown(),
          lanes: {
            ...watchingCooldown().lanes,
            lakebase: {
              ...watchingCooldown().lanes.lakebase,
              state: 'confirmed_zero',
              confirmed_at: '2026-08-18T20:01:17Z',
              elapsed_ms: 77_000,
              status: 'IDLE confirmed',
            },
          },
        },
      },
    } satisfies RunEvent)!

    const duplicate = applyRunEventSnapshot(confirmed, {
      sequence: 11,
      event: 'cooldown_update',
      occurred_at: '2026-08-18T20:02:39Z',
      payload: {
        cooldown: {
          ...watchingCooldown(),
          lanes: {
            ...watchingCooldown().lanes,
            lakebase: {
              ...watchingCooldown().lanes.lakebase,
              state: 'confirmed_zero',
              confirmed_at: '2026-08-18T20:02:39Z',
              elapsed_ms: 159_000,
              status: 'Repeated IDLE confirmation',
            },
          },
        },
      },
    } satisfies RunEvent)!
    expect(duplicate.cooldown!.lanes.lakebase).toEqual(
      confirmed.cooldown!.lanes.lakebase,
    )
    expect(duplicate.cooldown!.lanes.competitor.state).toBe('watching')

    const staleWaiting = applyRunEventSnapshot(duplicate, {
      sequence: 12,
      event: 'cooldown_update',
      occurred_at: '2026-08-18T20:02:40Z',
      payload: { cooldown: watchingCooldown() },
    } satisfies RunEvent)!
    expect(staleWaiting.cooldown!.lanes.lakebase.elapsed_ms).toBe(77_000)
    expect(staleWaiting.cooldown!.lanes.lakebase.state).toBe('confirmed_zero')

    const reconciled = selectRound4Session(staleWaiting, {
      ...staleWaiting,
      updated_at: '2026-08-18T20:03:00Z',
      cooldown: watchingCooldown(),
    })!
    expect(reconciled.cooldown!.lanes.lakebase.elapsed_ms).toBe(77_000)
    expect(reconciled.cooldown!.lanes.lakebase.state).toBe('confirmed_zero')
  })

  it('derives a frozen terminal elapsed value when an event omits it', () => {
    const current = { ...session(), cooldown: watchingCooldown() }
    const confirmed = applyRunEventSnapshot(current, {
      sequence: 10,
      event: 'cooldown_update',
      occurred_at: '2026-08-18T20:01:17Z',
      payload: {
        cooldown: {
          ...watchingCooldown(),
          lanes: {
            ...watchingCooldown().lanes,
            lakebase: {
              ...watchingCooldown().lanes.lakebase,
              state: 'confirmed_zero',
              confirmed_at: null,
              elapsed_ms: null,
              status: 'IDLE confirmed',
            },
          },
        },
      },
    } satisfies RunEvent)!
    expect(confirmed.cooldown!.lanes.lakebase).toMatchObject({
      state: 'confirmed_zero',
      confirmed_at: '2026-08-18T20:01:17Z',
      elapsed_ms: 77_000,
    })
  })
})
