import { beforeEach, describe, expect, it } from 'vitest'
import {
  LEGACY_SCORECARD_STORAGE_KEY,
  SCORECARD_STORAGE_KEY,
  loadScorecard,
  parseScorecardStorage,
  type ScorecardEntry,
} from './scorecard-storage'

const validEntry: ScorecardEntry = {
  session_id: 'session-1',
  round_id: 'wake_idle_app',
  round_title: 'Wake this idle app',
  competitor: 'Aurora Serverless v2',
  lakebase_ms: 812,
  competitor_ms: 1400,
  competitor_censored: false,
  competitor_capability_gap: false,
  evidence_shape: 'both_exact_verified',
  contract_status: 'declared_comparison',
  formal_winner: 'lakebase',
  margin_ms: 588,
  remembered_result: 'Lakebase wins by 0.59s',
  completed_at: '2026-08-26T00:00:30Z',
  cooldown: null,
}

describe('scorecard storage schema', () => {
  beforeEach(() => window.localStorage.clear())

  it('accepts only a complete versioned envelope', () => {
    expect(parseScorecardStorage({ version: 2, entries: [validEntry] })).toEqual([validEntry])
    expect(parseScorecardStorage({ version: 1, entries: [validEntry] })).toEqual([])
    expect(parseScorecardStorage({ version: 2, entries: [{ session_id: 'partial', lakebase_ms: 5 }] })).toEqual([])
    expect(parseScorecardStorage({
      version: 2,
      entries: [{ ...validEntry, competitor_ms: Number.NaN }],
    })).toEqual([])
  })

  it('strictly migrates a valid v1 array and rejects malformed legacy rows', () => {
    window.localStorage.setItem(LEGACY_SCORECARD_STORAGE_KEY, JSON.stringify([validEntry]))
    expect(loadScorecard()).toEqual([validEntry])
    expect(JSON.parse(window.localStorage.getItem(SCORECARD_STORAGE_KEY)!)).toEqual({
      version: 2,
      entries: [validEntry],
    })
    expect(window.localStorage.getItem(LEGACY_SCORECARD_STORAGE_KEY)).toBeNull()

    window.localStorage.clear()
    window.localStorage.setItem(LEGACY_SCORECARD_STORAGE_KEY, JSON.stringify([
      validEntry,
      { session_id: 'partial', lakebase_ms: 5 },
    ]))
    expect(loadScorecard()).toEqual([])
    expect(window.localStorage.getItem(SCORECARD_STORAGE_KEY)).toBeNull()
  })
})
