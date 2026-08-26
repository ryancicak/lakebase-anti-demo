import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'
import { ApiError, api } from './api/client'
import { FALLBACK_CATALOG, metricForCorners, stopCondition } from './catalog'

/**
 * Round selection goes through the six tiles on the fight card. The redundant
 * `SELECT · CHANGE ROUND` dropdown that used to sit under them was removed, so
 * these tests drive the control the room actually sees.
 */
async function pickRound(
  user: ReturnType<typeof userEvent.setup>,
  roundId: string,
): Promise<void> {
  const position = FALLBACK_CATALOG.rounds.findIndex((round) => round.id === roundId) + 1
  await user.click(await screen.findByRole('button', { name: new RegExp(`^Round ${position} · `, 'i') }))
}

/**
 * The fight card's one standing promise: a greyed-out PREPARE always carries a
 * reason, and a live one never carries a refusal.
 *
 * Read entirely off the rendered screen -- the button's own `disabled` and the
 * text that is actually painted -- so no flag a fake sets can satisfy it, and
 * deleting the strip fails the disabled branch rather than passing it.
 */
function refusalOnScreen(): string {
  const prepare = screen.getByRole('button', { name: /prepare fight card/i }) as HTMLButtonElement
  const words = Array.from(document.querySelectorAll('.game-lock-note'))
    .map((node) => node.textContent?.trim() ?? '')
    .filter((text) => text.length > 0)
    .join(' ')
  if (prepare.disabled) {
    expect(words, 'PREPARE is greyed out and the screen says nothing about why').not.toBe('')
  } else {
    expect(words, 'PREPARE is live but the screen is still refusing').not.toMatch(
      /bout in progress|checking the ring|unavailable tonight|offline|restoring/i,
    )
  }
  return words
}

/**
 * The six round tiles' lane lines, in card order, exactly as painted.
 *
 * Read out of the DOM rather than off a prop or a fake's own flag: the fault
 * this covers is a tile claiming a state nothing checked, and a guard that
 * trusted the value being passed in could not tell that apart from a tile that
 * renders it. One entry per tile whether or not it says anything, so a marking
 * on the WRONG round fails on position and not merely on presence.
 */
function laneLinesOnScreen(): string[] {
  return Array.from(document.querySelectorAll('.ring-key')).map((key) => (
    key.querySelector('.ring-key-l')?.textContent?.trim() ?? ''
  ))
}
import { compactDuration, preciseDuration } from './time'
import type { BoutReceipt } from './recap'
import type { DemoSession, RunEvent } from './api/types'

vi.mock('./hooks/useReducedMotion', () => ({ useReducedMotion: () => true }))
const audioMocks = vi.hoisted(() => ({
  playConfirm: vi.fn(),
  playCursor: vi.fn(),
  playOriginalBell: vi.fn(),
  playStart: vi.fn(),
  setOriginalCreditsThemeMuted: vi.fn(),
  setOriginalRoundThemeMuted: vi.fn(),
  setOriginalTitleThemeMuted: vi.fn(),
  startOriginalCreditsTheme: vi.fn(),
  startOriginalRoundTheme: vi.fn(),
  startOriginalTitleTheme: vi.fn(),
  stopOriginalCreditsTheme: vi.fn(),
  stopOriginalRoundTheme: vi.fn(),
  stopOriginalTitleTheme: vi.fn(),
}))
vi.mock('./audio', () => audioMocks)

class FakeEventSource {
  static instances: FakeEventSource[] = []

  onmessage: ((event: MessageEvent) => void) | null = null
  onopen: (() => void) | null = null
  onerror: (() => void) | null = null
  private listeners = new Map<string, Set<EventListener>>()

  constructor() {
    FakeEventSource.instances.push(this)
  }

  addEventListener = vi.fn((name: string, listener: EventListener) => {
    const listeners = this.listeners.get(name) ?? new Set<EventListener>()
    listeners.add(listener)
    this.listeners.set(name, listeners)
  })

  removeEventListener = vi.fn((name: string, listener: EventListener) => {
    this.listeners.get(name)?.delete(listener)
  })

  close = vi.fn()

  open() {
    this.onopen?.()
  }

  emit(event: RunEvent) {
    const message = new MessageEvent(event.event, { data: JSON.stringify(event) })
    this.listeners.get(event.event)?.forEach((listener) => listener(message))
  }
}

function jsonResponse(value: unknown) {
  return { ok: true, json: async () => value }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((onResolve, onReject) => {
    resolve = onResolve
    reject = onReject
  })
  return { promise, resolve, reject }
}

function stubReceiptCanvas() {
  const context = {
    fillRect: vi.fn(), strokeRect: vi.fn(), fillText: vi.fn(),
    save: vi.fn(), translate: vi.fn(), rotate: vi.fn(), restore: vi.fn(),
    measureText: vi.fn((value: string) => ({ width: value.length * 8 })),
  }
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(context as unknown as CanvasRenderingContext2D)
  vi.spyOn(HTMLCanvasElement.prototype, 'toBlob').mockImplementation((callback) => callback(new Blob(['pixel-card'], { type: 'image/png' })))
  return context
}

/**
 * A record on disk covering every outcome the finale ledger has to print.
 *
 * One receipt per shape rather than a realistic history, because the ledger's
 * job is to render each of these differently and the only way to catch a
 * regression is to have all of them on screen at once:
 *
 *   Round 1  declared, both lanes verified          a clean win with a margin
 *   Round 2  stopped short, their lane censored     a win, no margin, lower bound
 *   Round 3  attempted, nothing measured            abandoned, not "never run"
 *   Round 4  declared, their lane not supported     uncontested, no opponent
 *   Round 5  absent                                 never run
 *   Round 6  declared, their lane not supported     uncontested, the live round
 */
function ledgerReceipts(): BoutReceipt[] {
  const now = Date.now()
  const day = 86_400_000
  const lane = (
    ms: number | null,
    state: BoutReceipt['lakebase']['state'],
    lowerBound = false,
  ) => ({ ms, state, lower_bound: lowerBound, reason: null })
  const base = {
    session_id: 'ledger-session',
    opponent: 'Aurora Serverless v2',
    opponent_id: 'aurora_serverless_v2' as const,
    sealing_event: 'verified',
    metric: 'bout_elapsed_ms' as const,
    start_skew_ms: 40,
    remembered_result: null,
    failure: null,
  }
  return [
    {
      ...base,
      receipt: 'LEDGER-01',
      round_id: 'wake_idle_app',
      round_title: 'WAKE THIS IDLE APP',
      outcome: 'declared',
      has_measurements: true,
      lakebase: lane(2_324, 'verified'),
      opponent_lane: lane(13_417, 'verified'),
      margin_ms: 11_093,
      sealed_at: new Date(now).toISOString(),
    },
    {
      // Three days back, so the ledger has to date it rather than let it read
      // as part of today's sitting.
      ...base,
      receipt: 'LEDGER-02',
      round_id: 'make_schema_change_safely',
      round_title: 'MAKE A SCHEMA CHANGE SAFELY',
      outcome: 'stopped_short',
      has_measurements: true,
      lakebase: lane(14_240, 'verified'),
      opponent_lane: lane(93_997, 'incomplete', true),
      margin_ms: null,
      sealed_at: new Date(now - 3 * day).toISOString(),
    },
    {
      ...base,
      receipt: 'LEDGER-03',
      round_id: 'recover_deleted_order',
      round_title: 'RECOVER A DELETED ORDER',
      outcome: 'stopped_short',
      has_measurements: false,
      lakebase: lane(null, 'incomplete'),
      opponent_lane: lane(null, 'incomplete'),
      margin_ms: null,
      start_skew_ms: null,
      sealed_at: new Date(now).toISOString(),
    },
    {
      ...base,
      receipt: 'LEDGER-04',
      round_id: 'put_model_score_in_app',
      round_title: 'PUT A MODEL SCORE IN THE APP',
      outcome: 'declared',
      has_measurements: true,
      lakebase: lane(8_631, 'verified'),
      opponent_lane: lane(null, 'not_supported'),
      margin_ms: null,
      start_skew_ms: null,
      sealed_at: new Date(now).toISOString(),
    },
    {
      ...base,
      receipt: 'LEDGER-06',
      round_id: 'analyze_live_orders_without_slowing_checkout',
      round_title: 'ANALYZE LIVE ORDERS',
      outcome: 'declared',
      has_measurements: true,
      lakebase: lane(1_234, 'verified'),
      opponent_lane: lane(null, 'not_supported'),
      margin_ms: null,
      start_skew_ms: null,
      sealed_at: new Date(now).toISOString(),
    },
  ]
}

function session(state: DemoSession['state']): DemoSession {
  const primary = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'sre')!
  const competitor = FALLBACK_CATALOG.competitors[0]
  const round = FALLBACK_CATALOG.rounds[0]
  return {
    id: 'session-1', state, created_at: '2026-08-17T00:00:00Z', updated_at: '2026-08-17T00:00:00Z',
    competitor, primary_persona: primary, secondary_personas: [], corners: ['performance'], round,
    recommendation_reason: 'Recommended for SRE and executable for this matchup.',
    presenter_pack: {
      opening: primary.presenter.opening, discovery_question: 'What must recover?', risk: 'Available is not verified.',
      stop_condition: 'Both clocks stop only after verification.', remembered_metric: round.scorecard_by_corner.performance,
      primary: { persona_id: primary.id, nickname: primary.nickname, role: primary.role, interpretation: '', objection: '', response: '' },
      secondary: [], closing: 'Verified means the application works.',
    },
    lanes: {
      lakebase: { id: 'lakebase', name: 'Lakebase', state: state === 'running' ? 'connecting' : 'sealed', elapsed_ms: null, attempts: 0, status: state === 'running' ? 'Connecting' : 'Sealed', error: null },
      competitor: { id: 'competitor', name: competitor.short_name, state: state === 'running' ? 'connecting' : 'sealed', elapsed_ms: null, attempts: 0, status: state === 'running' ? 'Connecting' : 'Sealed', error: null },
    },
    fairness: { same_client: true, same_transaction: true, same_nonce: true, launch_skew_ms: null },
    remembered_result: null, failure: null,
  }
}

describe('receipt arithmetic', () => {
  it('keeps hundredths when an authoritative timer stops past ten seconds', () => {
    expect(preciseDuration(10_870)).toBe('10.87s')
  })

  it('never rounds a measured advantage up beyond the raw timestamps', () => {
    const proof: DemoSession = {
      ...session('verified'),
      cooldown: {
        mode: 'return_to_idle',
        state: 'ready',
        started_at: '2026-08-17T00:00:00Z',
        failure: null,
        lanes: {
          lakebase: {
            id: 'lakebase', name: 'Lakebase', state: 'confirmed_zero',
            started_at: '2026-08-17T00:00:00Z', confirmed_at: '2026-08-17T00:00:01.100Z',
            elapsed_ms: 1_100, status: 'Control plane confirmed zero',
          },
          competitor: {
            id: 'competitor', name: 'Aurora Serverless v2', state: 'confirmed_zero',
            started_at: '2026-08-17T00:00:00Z', confirmed_at: '2026-08-17T00:02:24.900Z',
            elapsed_ms: 144_900, status: 'Control plane confirmed zero',
          },
        },
      },
    }

    const difference = Math.abs(
      proof.cooldown!.lanes.lakebase.elapsed_ms! - proof.cooldown!.lanes.competitor.elapsed_ms!,
    )
    expect(compactDuration(difference)).toBe('2m 23s')
  })
})

function rdsSession(state: 'draft' | 'armed' | 'verified'): DemoSession {
  const base = session(state)
  const competitor = FALLBACK_CATALOG.competitors.find((item) => item.id === 'rds_postgres')!
  const verified = state === 'verified'
  return {
    ...base,
    state,
    competitor,
    recommendation_reason: 'RDS capability checked before the bell and only Lakebase is timed.',
    lanes: {
      lakebase: {
        ...base.lanes.lakebase,
        state: verified ? 'verified' : 'sealed',
        elapsed_ms: verified ? 842.6 : null,
        attempts: verified ? 1 : 0,
        status: verified ? 'Transaction verified' : 'Scale zero verified',
      },
      competitor: {
        id: 'competitor',
        name: competitor.short_name,
        state: state === 'draft' ? 'sealed' : 'not_supported',
        elapsed_ms: null,
        attempts: 0,
        status: state === 'draft' ? 'Sealed' : 'No automatic scale-to-zero or connection-triggered wake',
        error: null,
      },
    },
    remembered_result: verified ? 'LAKEBASE WINS — RDS CANNOT ENTER THE ROUND' : null,
  }
}

function safeChangeSession(state: DemoSession['state']): DemoSession {
  const base = session(state)
  const primary = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'software_engineer')!
  const round = FALLBACK_CATALOG.rounds.find((candidate) => candidate.id === 'make_schema_change_safely')!
  const verified = state === 'verified'
  const running = state === 'running'
  return {
    ...base,
    state,
    primary_persona: primary,
    round,
    presenter_pack: {
      ...base.presenter_pack,
      opening: primary.presenter.opening,
      remembered_metric: round.scorecard_by_corner.simplicity,
      primary: { persona_id: primary.id, nickname: primary.nickname, role: primary.role, interpretation: '', objection: '', response: '' },
    },
    lanes: {
      lakebase: { ...base.lanes.lakebase, state: verified ? 'verified' : running ? 'connecting' : 'sealed', elapsed_ms: verified ? 1800 : null, status: verified ? 'Isolated migration verified' : running ? 'Creating isolated environment' : 'Sealed' },
      competitor: { ...base.lanes.competitor, state: verified ? 'verified' : running ? 'connecting' : 'sealed', elapsed_ms: verified ? 9800 : null, status: verified ? 'Isolated migration verified' : running ? 'Creating isolated environment' : 'Sealed' },
    },
    remembered_result: verified ? 'LAKEBASE — 8.00 SECONDS SOONER' : null,
  }
}

function modelScoreSession(
  state: DemoSession['state'],
  redoState?: 'ready' | 'running' | 'verified' | 'failed',
): DemoSession {
  const base = session(state)
  const round = {
    ...FALLBACK_CATALOG.rounds.find((candidate) => candidate.id === 'put_model_score_in_app')!,
    availability: 'ready' as const,
  }
  const verified = state === 'verified' || Boolean(redoState)
  const v1Nonce = 'round4-v1-full-proof-nonce-aaaaaaaaaaaaaaaa'
  const v2Nonce = 'round4-v2-full-proof-nonce-bbbbbbbbbbbbbbbb'
  const evidence = (v2: boolean) => ({
    primary_key: 'customer-42', score: v2 ? 0.33 : 0.81,
    model_version: v2 ? 'risk-v2' : 'risk-v1', proof_nonce: v2 ? v2Nonce : v1Nonce,
    delta_version: v2 ? 12 : 11,
    verified_row: {
      primary_key: 'customer-42', score: v2 ? 0.33 : 0.81,
      model_version: v2 ? 'risk-v2' : 'risk-v1', proof_nonce: v2 ? v2Nonce : v1Nonce,
    },
  })
  const metrics = (v2: boolean) => [
    { spec_id: 'managed_availability_ms', lane_id: 'lakebase', value: v2 ? 510 : 640, display_value: v2 ? '510.00 ms' : '640.00 ms' },
    { spec_id: 'application_proof_elapsed_ms', lane_id: 'lakebase', value: v2 ? 720 : 840, display_value: v2 ? '720.00 ms' : '840.00 ms' },
    { spec_id: 'delta_commit_version', lane_id: 'lakebase', value: v2 ? 12 : 11, display_value: v2 ? '12' : '11' },
    { spec_id: 'exact_row_verified', lane_id: 'lakebase', value: true, display_value: 'Verified' },
  ]
  const initialLane = {
    ...base.lanes.lakebase,
    state: verified ? 'verified' as const : state === 'running' ? 'verifying' as const : 'sealed' as const,
    elapsed_ms: verified ? 840 : null,
    status: verified ? 'Exact committed version and fresh Postgres row verified' : state === 'running' ? 'Waiting for Managed Sync' : 'Sealed',
    activity: { phase: verified ? 'verified' : state === 'running' ? 'committing_source' : 'armed', wire_call: null },
    evidence: verified ? evidence(false) : {},
  }
  const redoLane = {
    ...base.lanes.lakebase,
    state: redoState === 'verified' ? 'verified' as const : redoState === 'failed' ? 'failed' as const : redoState === 'running' ? 'verifying' as const : 'sealed' as const,
    elapsed_ms: redoState === 'verified' ? 720 : null,
    status: redoState === 'verified' ? 'Exact committed version and fresh Postgres row verified' : redoState === 'failed' ? 'Managed Sync re-do could not be verified' : redoState === 'running' ? 'Reading the exact v2 row' : 'Ready',
    error: redoState === 'failed' ? 'The v2 exact row did not verify.' : null,
    activity: { phase: redoState === 'verified' ? 'verified' : redoState === 'failed' ? 'failed' : redoState === 'running' ? 'reading_application' : 'armed', wire_call: null },
    evidence: redoState === 'verified' ? evidence(true) : {},
  }
  return {
    ...base,
    state,
    updated_at: redoState ? '2026-08-18T00:00:02Z' : '2026-08-18T00:00:01Z',
    round,
    lanes: {
      lakebase: initialLane,
      competitor: {
        id: 'competitor', name: 'AWS', state: 'not_supported', elapsed_ms: null,
        attempts: 0, status: 'AWS lane not timed', error: null,
        evidence: { unsupported_reason: 'No AWS-native equivalent lane was configured or timed in this scoped proof.' },
      },
    },
    metrics: verified ? metrics(false) : [],
    comparison: verified ? {
      kind: 'capability_gap',
      winner_lane_id: 'lakebase',
      margin: null,
      detail: 'Lakebase verified the scoped native Synced Tables capability; no AWS-native equivalent lane was timed. This is not a speed comparison.',
    } : null,
    remembered_result: verified ? 'LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A' : null,
    failure: state === 'failed' ? 'The exact row proof did not verify.' : null,
    redo: redoState ? {
      state: redoState,
      lanes: {
        lakebase: redoLane,
        competitor: {
          id: 'competitor', name: 'AWS', state: 'not_supported', elapsed_ms: null,
          attempts: 0, status: 'AWS lane not timed', error: null,
          evidence: { unsupported_reason: 'No AWS-native equivalent lane was configured or timed in this scoped proof.' },
        },
      },
      metrics: redoState === 'verified' ? metrics(true) : [],
      comparison: redoState === 'verified' ? {
        kind: 'capability_gap',
        winner_lane_id: 'lakebase',
        margin: null,
        detail: 'Lakebase verified the scoped native Synced Tables capability; no AWS-native equivalent lane was timed. This is not a speed comparison.',
      } : null,
      failure: redoState === 'failed' ? 'The v2 exact row did not verify.' : null,
    } : verified ? {
      state: 'ready',
      lanes: {
        lakebase: redoLane,
        competitor: {
          id: 'competitor', name: 'AWS', state: 'not_supported', elapsed_ms: null,
          attempts: 0, status: 'AWS lane not timed', error: null,
          evidence: { unsupported_reason: 'No AWS-native equivalent lane was configured or timed in this scoped proof.' },
        },
      },
    } : null,
  }
}

type TowelFixtureState = 'eligible' | 'stopping' | 'cleaning' | 'failed' | 'ready'

function recoveryTowelSession(towelState: TowelFixtureState): DemoSession {
  const hasTowel = towelState !== 'eligible'
  const base = session(hasTowel ? 'towelled' : 'running')
  const round = FALLBACK_CATALOG.rounds.find((candidate) => candidate.id === 'recover_deleted_order')!
  const requestedAt = '2026-08-18T15:11:30Z'
  const snapshotTowelState = towelState === 'eligible' ? 'stopping' : towelState
  const cleanupState = towelState === 'failed' ? 'failed' : towelState === 'ready' ? 'ready' : 'watching'
  const cleanupLaneState = towelState === 'failed'
    ? 'failed'
    : towelState === 'ready'
      ? 'confirmed_deleted'
      : 'watching'
  return {
    ...base,
    state: hasTowel ? 'towelled' : 'running',
    updated_at: requestedAt,
    round,
    run_started_at: '2026-08-18T15:10:00Z',
    lanes: {
      lakebase: {
        ...base.lanes.lakebase,
        state: 'verified',
        elapsed_ms: 14_380,
        attempts: 1,
        status: 'Exact recovered order verified · Source deletion preserved',
        activity: { phase: 'verified', wire_call: null },
      },
      competitor: {
        ...base.lanes.competitor,
        state: hasTowel ? 'towelled' : 'connecting',
        elapsed_ms: null,
        attempts: 4,
        status: hasTowel
          ? 'Towel thrown · Opponent still recovering'
          : 'Aurora full-copy PITR recovery cluster is still restoring',
        activity: { phase: 'restoring', wire_call: 'RDS.RestoreDBClusterToPointInTime' },
      },
    },
    towel: hasTowel ? {
      state: snapshotTowelState,
      requested_at: requestedAt,
      active_lane: 'competitor',
      lower_bound_ms: 90_000,
      lakebase_verified_ms: 14_380,
      restore_started: true,
      cleanup_failure: towelState === 'failed' ? 'Recovery environments could not be safely removed.' : null,
    } : null,
    cooldown: towelState === 'eligible' || towelState === 'stopping' ? null : {
      mode: 'delete_recovery_environment',
      state: cleanupState,
      started_at: requestedAt,
      failure: towelState === 'failed' ? 'Recovery environments could not be safely removed.' : null,
      lanes: {
        lakebase: {
          id: 'lakebase', name: 'Lakebase', state: cleanupLaneState,
          started_at: requestedAt, confirmed_at: towelState === 'ready' ? requestedAt : null,
          elapsed_ms: towelState === 'ready' ? 2_000 : null,
          status: towelState === 'failed' ? 'Temporary cleanup failure' : 'Deleting owned recovery environment',
          activity: { phase: towelState === 'failed' ? 'failed' : 'resetting', wire_call: null },
        },
        competitor: {
          id: 'competitor', name: 'Aurora Serverless v2', state: cleanupLaneState,
          started_at: requestedAt, confirmed_at: towelState === 'ready' ? requestedAt : null,
          elapsed_ms: towelState === 'ready' ? 3_000 : null,
          status: towelState === 'failed'
            ? 'Temporary cleanup failure'
            : 'AWS RESTORE ALREADY IN MOTION · SAFE CLEANUP MAY TAKE MINUTES',
          activity: { phase: towelState === 'failed' ? 'failed' : 'resetting', wire_call: null },
        },
      },
    },
    remembered_result: hasTowel
      ? 'TOWEL THROWN AT 90.00s · LAKEBASE VERIFIED 14.38s · AURORA STILL RECOVERING'
      : null,
    failure: null,
  }
}

/**
 * The same Round 3 towel, thrown before Lakebase itself verified.
 *
 * Both lanes are censored to lower bounds and neither produced a figure, so the
 * bout proved nothing. `recap.ts` calls this state `abandoned`.
 */
function earlyTowelSession(): DemoSession {
  const base = recoveryTowelSession('ready')
  const towel = base.towel!
  return {
    ...base,
    lanes: {
      lakebase: {
        ...base.lanes.lakebase,
        state: 'towelled',
        elapsed_ms: null,
        status: 'Towel thrown · Lakebase still recovering',
        activity: { phase: 'restoring', wire_call: null },
      },
      competitor: base.lanes.competitor,
    },
    towel: {
      ...towel,
      cutoff_ms: 45_000,
      censored_lower_bounds_ms: { lakebase: 45_000, competitor: 45_000 },
      active_lane: undefined,
      lower_bound_ms: undefined,
      lakebase_verified_ms: undefined,
    },
    cooldown: null,
    remembered_result: 'TOWELED AT 45.00s · NO WINNER · MARGIN N/A',
  }
}

describe('backstage setup', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.localStorage.clear()
    window.sessionStorage.clear()
    FakeEventSource.instances = []
    window.history.replaceState({}, '', '/')
    vi.spyOn(api, 'boutStatus').mockResolvedValue({
      ring_ready: true,
      maintenance_state: 'ready',
      maintenance_detail: null,
      active: false,
      operator: null,
      started_at: null,
      updated_at: null,
      expires_at: null,
      phase: null,
      state: null,
      round_title: null,
      competitor: null,
    })
  })

  afterEach(() => {
    cleanup()
    vi.useRealTimers()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    window.localStorage.clear()
    window.sessionStorage.clear()
    FakeEventSource.instances = []
    window.history.replaceState({}, '', '/')
  })

  it('starts Final Bell from its explicit title control and clears it before the Start chime', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    render(<App />)

    expect(audioMocks.startOriginalTitleTheme).not.toHaveBeenCalled()
    audioMocks.startOriginalTitleTheme.mockReturnValueOnce(true).mockReturnValueOnce(true)
    const musicToggle = screen.getByRole('button', { name: /music off/i })
    expect(musicToggle).toBeEnabled()
    expect(musicToggle).toHaveAttribute('aria-pressed', 'false')
    await user.click(musicToggle)

    expect(audioMocks.startOriginalTitleTheme).toHaveBeenCalledTimes(1)
    expect(screen.getByRole('button', { name: /music on/i })).toHaveAttribute('aria-pressed', 'true')

    audioMocks.stopOriginalTitleTheme.mockReturnValueOnce(true)
    await user.click(screen.getByRole('button', { name: /press start/i }))

    expect(audioMocks.stopOriginalTitleTheme).toHaveBeenCalled()
    expect(audioMocks.playStart).toHaveBeenCalledWith(140)
    expect(screen.getByRole('heading', { name: /choose the opponent/i })).toBeInTheDocument()

    audioMocks.startOriginalTitleTheme.mockClear()
    await user.click(screen.getByRole('button', { name: /title screen/i }))
    await waitFor(() => expect(audioMocks.startOriginalTitleTheme).toHaveBeenCalledTimes(1))
    expect(screen.getByRole('button', { name: /music on/i })).toHaveAttribute('aria-pressed', 'true')
  })

  it('turns saved-off music on from the title screen click', async () => {
    window.localStorage.setItem('lakebase-anti-demo:setup:v1', JSON.stringify({ sound: false }))
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    audioMocks.startOriginalTitleTheme.mockReturnValueOnce(true)
    const user = userEvent.setup()
    render(<App />)

    const musicToggle = screen.getByRole('button', { name: /music off/i })
    expect(musicToggle).toBeEnabled()
    await user.click(musicToggle)

    expect(audioMocks.startOriginalTitleTheme).toHaveBeenCalledTimes(1)
    expect(screen.getByRole('button', { name: /music on/i })).toHaveAttribute('aria-pressed', 'true')

    audioMocks.stopOriginalTitleTheme.mockReturnValueOnce(true)
    await user.click(screen.getByRole('button', { name: /music on/i }))
    expect(audioMocks.stopOriginalTitleTheme).toHaveBeenCalled()
    expect(screen.getByRole('button', { name: /music off/i })).toHaveAttribute('aria-pressed', 'false')
  })

  it('uses browser Back and Forward to restore the previous setup screen', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    expect(screen.getByRole('heading', { name: /choose the lead voice/i })).toBeInTheDocument()

    window.history.back()
    await waitFor(() => expect(screen.getByRole('heading', { name: /choose the opponent/i })).toBeInTheDocument())

    window.history.forward()
    await waitFor(() => expect(screen.getByRole('heading', { name: /choose the lead voice/i })).toBeInTheDocument())
  })

  it('keeps fight-card preparation locked while backstage maintenance runs', async () => {
    vi.mocked(api.boutStatus).mockResolvedValue({
      ring_ready: false,
      maintenance_state: 'maintenance',
      maintenance_detail: 'BACKSTAGE CLEANUP IN PROGRESS · SHOWTIME WILL UNLOCK AUTOMATICALLY',
      active: false,
      operator: null,
      started_at: null,
      updated_at: null,
      expires_at: null,
      phase: null,
      state: null,
      round_title: null,
      competitor: null,
    })
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))

    expect(await screen.findByText(/backstage cleanup in progress.*unlock automatically/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /prepare fight card/i })).toBeDisabled()
  })

  it('keeps the title screen explicitly reachable without erasing saved selections', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    const first = render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('radio', { name: /rds postgresql/i }))
    await user.click(screen.getByRole('button', { name: /title screen/i }))
    expect(screen.getByRole('button', { name: /press start/i })).toBeInTheDocument()

    first.unmount()
    window.history.replaceState({}, '', '/#title')
    render(<App />)
    expect(screen.getByRole('button', { name: /press start/i })).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /press start/i }))
    expect(screen.getByRole('radio', { name: /rds postgresql/i })).toHaveAttribute('aria-checked', 'true')
  })

  it('restores saved opponent, priorities, personas, and setup location after remount', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    const first = render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('radio', { name: /rds postgresql/i }))
    await user.click(screen.getByRole('button', { name: /cost.*published rates now.*billed usage later/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /count query/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /backfill bill/i }))

    first.unmount()
    render(<App />)

    expect(screen.getByRole('heading', { name: /add up to two lenses/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /backfill bill/i })).toHaveAttribute('aria-pressed', 'true')
    await user.click(screen.getByRole('button', { name: /change lead/i }))
    expect(screen.getByRole('radio', { name: /count query/i })).toHaveAttribute('aria-checked', 'true')
    await user.click(screen.getByRole('button', { name: /change opponent/i }))
    expect(screen.getByRole('radio', { name: /rds postgresql/i })).toHaveAttribute('aria-checked', 'true')
    expect(screen.getByRole('button', { name: /cost.*published rates now.*billed usage later/i })).toHaveAttribute('aria-pressed', 'true')
  })

  it('returns a reloaded live-stage view to the saved fight card instead of reviving a stale session', async () => {
    window.history.replaceState({}, '', '/?review=1')
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    const user = userEvent.setup()
    const first = render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(screen.getByRole('button', { name: /preview fight card/i }))
    await user.click(await screen.findByRole('button', { name: /sound on/i }))
    expect(screen.getByRole('button', { name: /sound off/i })).toHaveAttribute('aria-pressed', 'false')
    audioMocks.playOriginalBell.mockClear()
    audioMocks.startOriginalRoundTheme.mockClear()
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    // Sound off is a mute, not a transport control: the one-shot bell is
    // skipped outright, but the round cue still starts -- silent, and already
    // at the right bar if the presenter switches sound back on mid-bout.
    expect(audioMocks.playOriginalBell).not.toHaveBeenCalled()
    expect(audioMocks.startOriginalRoundTheme).toHaveBeenCalledTimes(1)
    expect(audioMocks.setOriginalRoundThemeMuted).toHaveBeenLastCalledWith(true)
    expect(await screen.findByRole('heading', { name: /wake this idle app/i })).toBeInTheDocument()

    first.unmount()
    render(<App />)

    expect(screen.getByText(/· round \d+ of six/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /preview fight card/i })).toBeInTheDocument()
    expect(screen.queryByLabelText(/lakebase result/i)).not.toBeInTheDocument()
  })

  it('restores an authoritative live session after a browser refresh', async () => {
    const running = session('running')
    window.sessionStorage.setItem('lakebase-anti-demo:active-session:v1', JSON.stringify({
      id: running.id,
      stage: 'proof',
      resumeStage: 'proof',
    }))
    const fetchMock = vi.fn().mockImplementation((input: string) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === `/api/sessions/${running.id}`) return Promise.resolve(jsonResponse(running))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)

    render(<App />)

    expect(await screen.findByRole('heading', { name: /wake this idle app/i })).toBeInTheDocument()
    expect(screen.getByLabelText('Lakebase result')).toHaveAttribute('data-state', 'connecting')
    expect(fetchMock.mock.calls.some((call) => call[0] === '/api/sessions')).toBe(false)
  })

  it.each([
    ['generic proof', 'wake_idle_app'],
    ['Round 4', 'put_model_score_in_app'],
    ['Round 5', 'survive_connection_spike'],
    ['Round 6', 'analyze_live_orders_without_slowing_checkout'],
  ] as const)('shows one towel action only while the %s scene is running', async (_scene, roundId) => {
    const selectedRound = FALLBACK_CATALOG.rounds.find((round) => round.id === roundId)!
    const running = roundId === 'put_model_score_in_app'
      ? modelScoreSession('running')
      : { ...session('running'), round: selectedRound }
    window.sessionStorage.setItem('lakebase-anti-demo:active-session:v1', JSON.stringify({
      id: running.id,
      stage: 'proof',
      resumeStage: 'proof',
    }))
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input: string) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === `/api/sessions/${running.id}`) return Promise.resolve(jsonResponse(running))
      throw new Error(`Unexpected request: ${input}`)
    }))
    vi.stubGlobal('EventSource', FakeEventSource)

    const view = render(<App />)
    expect(await screen.findByRole('button', { name: /throw in the towel/i })).toBeEnabled()
    view.unmount()
    window.sessionStorage.clear()

    const terminal = roundId === 'put_model_score_in_app'
      ? modelScoreSession('verified')
      : { ...running, state: 'verified' as const }
    window.sessionStorage.setItem('lakebase-anti-demo:active-session:v1', JSON.stringify({
      id: terminal.id,
      stage: 'proof',
      resumeStage: 'proof',
    }))
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input: string) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === `/api/sessions/${terminal.id}`) return Promise.resolve(jsonResponse(terminal))
      throw new Error(`Unexpected request: ${input}`)
    }))
    render(<App />)
    await waitFor(() => expect(screen.queryByRole('button', { name: /throw in the towel/i })).not.toBeInTheDocument())
    if (roundId === 'survive_connection_spike') {
      expect(screen.queryByRole('button', { name: /ring again|re-do|replay/i })).not.toBeInTheDocument()
    }
  })

  it.each([
    ['generic proof', 'wake_idle_app'],
    ['Round 4', 'put_model_score_in_app'],
    ['Round 5 arena', 'survive_connection_spike'],
    ['Round 6', 'analyze_live_orders_without_slowing_checkout'],
  ] as const)('mutes a running bout from the %s screen without touching the transport', async (_scene, roundId) => {
    const selectedRound = FALLBACK_CATALOG.rounds.find((round) => round.id === roundId)!
    const running = roundId === 'put_model_score_in_app'
      ? modelScoreSession('running')
      : { ...session('running'), round: selectedRound }
    window.sessionStorage.setItem('lakebase-anti-demo:active-session:v1', JSON.stringify({
      id: running.id,
      stage: 'proof',
      resumeStage: 'proof',
    }))
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input: string) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === `/api/sessions/${running.id}`) return Promise.resolve(jsonResponse(running))
      throw new Error(`Unexpected request: ${input}`)
    }))
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    const toggle = await screen.findByRole('button', { name: /sound on/i })
    expect(toggle).toHaveAttribute('aria-pressed', 'true')
    // Tab-reachable: a real button, enabled, with nothing removing it from the
    // tab order. The focus ring is the global button:focus-visible rule.
    expect(toggle).toBeEnabled()
    expect(toggle).not.toHaveAttribute('tabindex')
    /* Clear of the bell in the strongest sense available: the bell is not on
       this screen at all, and the mute sits in the header while every bout
       control on these screens lives in the body or the footer. */
    expect(screen.queryByRole('button', { name: /ring the bell|ring again/i })).not.toBeInTheDocument()
    expect(toggle.closest('header')).not.toBeNull()

    /* Restoring into the arena legitimately stops cues on the way through, so
       the transport assertions below start from a clean slate.

       Wait for the last of those stops before clearing, rather than assuming
       it has already happened. Finding the button proves only that the arena
       committed: the cue teardown is a passive effect of the stage change, and
       React flushes those after the DOM the button lives in. On a loaded
       machine the flush lands after this point, and a clear taken too early
       books the restore's two stopOriginalTitleTheme calls against the click
       below. Leaving the title screen tears down the attract loop, so that
       stop is the specific thing being waited on. */
    await waitFor(() => expect(audioMocks.stopOriginalTitleTheme).toHaveBeenCalled())
    audioMocks.startOriginalRoundTheme.mockClear()
    audioMocks.stopOriginalRoundTheme.mockClear()
    audioMocks.startOriginalTitleTheme.mockClear()
    audioMocks.stopOriginalTitleTheme.mockClear()
    audioMocks.stopOriginalCreditsTheme.mockClear()
    audioMocks.startOriginalCreditsTheme.mockClear()

    await user.click(toggle)

    expect(screen.getByRole('button', { name: /sound off/i })).toHaveAttribute('aria-pressed', 'false')
    expect(audioMocks.setOriginalTitleThemeMuted).toHaveBeenLastCalledWith(true)
    expect(audioMocks.setOriginalRoundThemeMuted).toHaveBeenLastCalledWith(true)
    expect(audioMocks.setOriginalCreditsThemeMuted).toHaveBeenLastCalledWith(true)
    // A mute reaches the mute setters and no transport function whatsoever.
    expect(audioMocks.startOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.startOriginalTitleTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalTitleTheme).not.toHaveBeenCalled()
    expect(audioMocks.startOriginalCreditsTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalCreditsTheme).not.toHaveBeenCalled()
    // Persisted exactly the way the fight card's control persists it: one flag,
    // one saver, so a presenter who mutes in the arena stays muted next launch.
    expect(JSON.parse(window.localStorage.getItem('lakebase-anti-demo:setup:v1')!).sound).toBe(false)

    await user.click(screen.getByRole('button', { name: /sound off/i }))
    expect(screen.getByRole('button', { name: /sound on/i })).toHaveAttribute('aria-pressed', 'true')
    expect(audioMocks.setOriginalRoundThemeMuted).toHaveBeenLastCalledWith(false)
    expect(audioMocks.startOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalRoundTheme).not.toHaveBeenCalled()
  })

  it('labels browser connectivity as live updates rather than database verification', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    const user = userEvent.setup()
    render(<App />)
    await user.click(screen.getByRole('button', { name: /press start/i }))
    const indicator = await screen.findByRole('button', { name: /demo server offline/i })
    expect(indicator).toHaveAttribute('aria-expanded', 'false')
    expect(screen.queryByText(/verifier offline/i)).not.toBeInTheDocument()
    await user.click(indicator)
    const details = screen.getByRole('dialog', { name: /local api did not answer/i })
    expect(details).toHaveTextContent(/cannot currently reach the local anti-demo server\/API/i)
    expect(details).toHaveTextContent(/does not prove either database is awake, reachable, or verified/i)
    await user.keyboard('{Escape}')
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(indicator).toHaveFocus()
    expect(screen.getByRole('heading', { name: /choose the opponent/i })).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await waitFor(() => expect(screen.getByRole('button', { name: /prepare fight card/i })).toBeDisabled())
    expect(screen.getByText(/live proof locked/i)).toBeInTheDocument()
  })

  it('explains the exact boundary of a connected demo server', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    const indicator = await screen.findByRole('button', { name: /demo server connected/i })
    await user.click(indicator)

    const details = screen.getByRole('dialog', { name: /local api answered/i })
    expect(details).toHaveTextContent(/browser can reach the local anti-demo server\/API/i)
    expect(details).toHaveTextContent(/session event stream opens only during an active bout/i)
    expect(details).toHaveTextContent(/does not prove either database is awake, reachable, or verified/i)
    expect(details).toHaveTextContent(/database proof begins only after you choose Ring the Bell/i)
    await user.click(within(details).getByRole('button', { name: /close connection details/i }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(indicator).toHaveFocus()
  })

  it('enforces exactly one primary and no more than two secondary personas', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    expect(screen.getByText(/changes the question.*never the timers/i)).toBeInTheDocument()
    expect(screen.getAllByRole('radio').filter((radio) => radio.getAttribute('aria-checked') === 'true')).toHaveLength(1)

    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    const bill = screen.getByRole('button', { name: /backfill bill/i })
    const jack = screen.getByRole('button', { name: /stacktrace jack/i })
    await user.click(bill)
    await user.click(jack)
    expect(bill).toHaveAttribute('aria-pressed', 'true')
    expect(jack).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: /count query/i })).toBeDisabled()

    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await waitFor(() => expect(screen.getByRole('button', { name: /prepare fight card/i })).toBeEnabled())
    expect(screen.getByRole('button', { name: /^round 1 · wake this idle app/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^round 2 · make this schema change safely/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^round 3 · recover this deleted order/i })).toBeInTheDocument()
  })

  it('shows Round 5 for Aurora without changing the selected opponent', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    expect(screen.getByRole('radio', { name: /aurora serverless v2/i })).toHaveAttribute('aria-checked', 'true')
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))

    const roundFive = await screen.findByRole('button', {
      name: /round 5 · get spike-ready.*ready/i,
    })
    expect(roundFive).toBeEnabled()
    await user.click(roundFive)

    expect(screen.getByRole('heading', { name: /get spike-ready/i })).toBeInTheDocument()
    expect(screen.getByText('Aurora + RDS Proxy')).toBeInTheDocument()
    expect(screen.queryByText('RDS PostgreSQL')).not.toBeInTheDocument()
    // The fight card no longer prints the stop boundary in prose -- the owner
    // removed the explanatory panels -- but the boundary itself still governs
    // the round and rides onto the receipt, so it is pinned at its source.
    expect(stopCondition('survive_connection_spike', 'aurora_serverless_v2')).toMatch(
      /each clock stops at a verified pooled application path.*RDS Proxy is AWS best practice.*already deployed.*setup delay does not apply.*identical spike must pass on both lanes/i,
    )
    expect(screen.getByRole('button', { name: /prepare fight card/i })).toBeEnabled()

    // NO STAGING PARAGRAPH ON THE FIGHT CARD. The strip that used to explain
    // the ring's pacing in full was removed by the owner: it cost five lines of
    // the viewport and pushed PREPARE FIGHT CARD off the fold. Its absence is
    // pinned rather than merely allowed, because "the screen carries no prose
    // block under the tiles" is the layout property the buttons depend on.
    const card = document.querySelector('.card-scene')
    expect(card).not.toBeNull()
    expect(card!.querySelector('.ring-staging')).toBeNull()
    expect(screen.queryByText(/the ring stages the task, not the score/i)).not.toBeInTheDocument()

    // WHAT REMOVAL MAY NOT BRING BACK. Two sentences this screen once carried
    // are false now and must stay off it, whatever else changes here.
    //
    // The first denied that any corner was drawn quicker than another, which
    // THIS round's own artwork contradicts on screen: `.gate-near` is a
    // finished gate that passes five clients from the top of every 1.9s cycle,
    // while `.gate-far` sits at .18 opacity for the first 52% of a 7.6s cycle,
    // assembles from nine bolts on staggered delays, and starts passing three
    // clients 62% of the way in. That asymmetry is a true depiction -- Round
    // 5's own receipts reach the setup gate in ~3.6s against ~590s -- so the
    // denial was the defect and the animation was not.
    //
    // The second claimed no live run of a round was on record. Real bouts are
    // recorded now, so any surface still saying it is lying. Asserted across
    // the whole scene, not one element, because the element it used to sit in
    // no longer exists.
    expect(card).not.toHaveTextContent(/no corner here is drawn quicker/i)
    expect(card).not.toHaveTextContent(/no live run of this one is on record/i)
  })

  it('lets the operator select multiple customer priorities while keeping at least one', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    const cost = screen.getByRole('button', { name: /cost.*published rates now.*billed usage later/i })
    const simplicity = screen.getByRole('button', { name: /simplicity.*observed workflow and manual steps/i })
    const performance = screen.getByRole('button', { name: /performance.*elapsed workflow time to verified outcome/i })

    await user.click(cost)
    await user.click(simplicity)
    expect(cost).toHaveAttribute('aria-pressed', 'true')
    expect(simplicity).toHaveAttribute('aria-pressed', 'true')
    expect(performance).toHaveAttribute('aria-pressed', 'true')

    await user.click(performance)
    await user.click(simplicity)
    await user.click(cost)
    expect(cost).toHaveAttribute('aria-pressed', 'true')
  })

  it('derives override evidence, stop boundary, and reason from the selected round', async () => {
    window.history.replaceState({}, '', '/?review=1')
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /doctor drift/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    expect(screen.getByText('Doctor Drift says:').parentElement).toHaveTextContent(/model output become usable by the application/i)
    await pickRound(user, 'recover_deleted_order')

    // Evidence and stop boundary are no longer printed on the fight card, so
    // they are pinned where they are decided. Both still ride the selection
    // into the arena and onto the receipt.
    const recoverRound = FALLBACK_CATALOG.rounds.find((round) => round.id === 'recover_deleted_order')!
    expect(metricForCorners(recoverRound, ['performance'])).toBe('Verified application RTO at the agreed RPO')
    expect(stopCondition('recover_deleted_order', 'aurora_serverless_v2')).toBe(
      'Each clock stops after the exact order reads from recovery and remains absent at the final source check.',
    )
    expect(screen.getByText('Doctor Drift says:').parentElement).toHaveTextContent(/this round: Recover this deleted order.*Data Scientist \/ ML lens/i)
    expect(screen.getByText('Doctor Drift says:').parentElement).not.toHaveTextContent(/model output become usable by the application/i)
  })

  it('rings one bell when the start control is double-clicked', async () => {
    const run = deferred<ReturnType<typeof jsonResponse>>()
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return run.promise
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    audioMocks.playOriginalBell.mockClear()
    audioMocks.startOriginalRoundTheme.mockClear()

    const bell = await screen.findByRole('button', { name: /ring the bell/i })
    // Both clicks land before the first /run settles: the tight double-rung bell.
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
    fireEvent.click(bell)
    // The second press has nowhere to land -- the control disables itself inside
    // the first press's own event -- so what a double-tapper reads has to be on
    // screen already, and it is: a bell is in flight, and a second press cannot
    // start a second bout. Without this the only thing that moved under a
    // double-tap was the button's own label, on a control the presenter has just
    // committed to and stopped reading.
    const notice = screen.getByRole('status')
    expect(notice).toHaveTextContent(/bell rung/i)
    expect(notice).toHaveTextContent(/cannot start a second bout/i)
    // And nothing failed, so nothing may be announced as a failure.
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    fireEvent.click(bell)

    const runCalls = () => fetchMock.mock.calls.filter((call) => String(call[0]).endsWith('/run'))
    expect(runCalls()).toHaveLength(1)
    expect(audioMocks.playOriginalBell).toHaveBeenCalledTimes(1)
    expect(audioMocks.startOriginalRoundTheme).toHaveBeenCalledTimes(1)
    // The control locks itself while the first bell is unresolved, which is the
    // mechanism that makes the second press unreportable in the first place.
    await waitFor(() => expect(screen.getByRole('button', { name: /confirming the bell/i })).toBeDisabled())
    expect(screen.getByRole('status')).toBe(notice)

    run.resolve(jsonResponse(session('running')))
    await act(async () => { await Promise.resolve() })

    expect(await screen.findByRole('heading', { name: 'Wake this idle app' })).toBeInTheDocument()
    expect(runCalls()).toHaveLength(1)
    expect(audioMocks.playOriginalBell).toHaveBeenCalledTimes(1)
    // The notice described a request in flight, and that request has landed.
    expect(screen.queryByText(/cannot start a second bout/i)).not.toBeInTheDocument()
  })

  it('gives the in-flight notice a ground it can be read against', () => {
    // The slot this notice sits in lands on the ring band drawn by
    // .ready-screen::after, whose top edge is var(--red). Yellow on that red is
    // 2.69:1 and cream is 3.19:1 -- both under AA at this size -- and which of
    // the band's three colours the text overlaps moves with the viewport. So
    // the notice may not inherit its ground from whatever is behind it.
    const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
    const rule = css.slice(css.indexOf('.bell-notice {'))
    const body = rule.slice(0, rule.indexOf('}'))
    expect(body).toMatch(/background:\s*#050817/)
    expect(body).toMatch(/color:\s*var\(--yellow\)/)
    // And it may not be the width of the viewport: this column is laid out
    // inside the screen's own padding, so `vw` and the column disagree.
    expect(body).toMatch(/max-width:\s*min\([^)]*100%\)/)
    expect(body).not.toMatch(/vw/)
  })

  it('keeps the ringside commentator inside the screen at a phone width', () => {
    // WHY THIS GUARD EXISTS, and why it is NOT the minmax(0, 1fr) fix the roll
    // and .cooldown-match needed. That pin was already on this bar's middle
    // track and was working: measured in Chromium at 390x844 it had collapsed
    // the play-by-play to 0px. The overflow came from the two OUTER tracks,
    // which are `auto` carrying hard pixel floors -- 185px on the header and
    // 190px on the toggle -- so the row demanded 375px of the 338px
    // .between-screen has to give, and a flexible track cannot absorb a
    // sibling's pixel floor however it is written. .retro-screen is
    // overflow: hidden, so the excess was clipped rather than scrolled and the
    // toggle became unreachable.
    //
    // Asserted on the stylesheet rather than on a rendered width for the reason
    // credits-layout.test.tsx gives: jsdom lays nothing out, so scrollWidth is
    // always 0 here and a width assertion would pass against a broken sheet.
    const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
      .replace(/\/\*[\s\S]*?\*\//g, '')

    /**
     * Every `@media` block's body, brace-matched, alongside the sheet with all
     * of them cut out. Both halves are needed: a media block holds the narrow
     * fix, and only the remainder tells you what desktop still gets.
     */
    const narrowBlocks: string[] = []
    let unconditional = ''
    let cursor = 0
    for (const opener of css.matchAll(/@media\s*([^{]*)\{/g)) {
      unconditional += css.slice(cursor, opener.index!)
      let depth = 1
      let i = opener.index! + opener[0].length
      const from = i
      while (i < css.length && depth > 0) {
        if (css[i] === '{') depth += 1
        else if (css[i] === '}') depth -= 1
        i += 1
      }
      const width = /max-width:\s*(\d+)px/.exec(opener[1])
      if (width && Number(width[1]) <= 760) narrowBlocks.push(css.slice(from, i - 1))
      cursor = i
    }
    unconditional += css.slice(cursor)
    const narrow = narrowBlocks.filter((block) => block.includes('.ringside-commentator'))
    // Sanity: if the bar stops being mentioned at a narrow width at all, every
    // assertion below would pass vacuously against the pre-fix stylesheet.
    expect(narrow.length).toBeGreaterThan(0)
    const stacked = narrow.join('\n')

    // One column, so the three parts stack instead of competing for 338px.
    expect(stacked).toMatch(/\.ringside-commentator[^{]*\{[^}]*grid-template-columns:\s*minmax\(\s*0\s*,\s*1fr\s*\)/)
    // And both pixel floors released, including the Round 4 cut's 165px pair --
    // stacking alone would not help while a 185px floor still applied.
    const floors = [...stacked.matchAll(/min-width:\s*([^;}]+)/g)].map(([, value]) => value.trim())
    expect(floors.length).toBeGreaterThanOrEqual(2)
    expect(floors.every((value) => /^0(?:px)?$/.test(value))).toBe(true)
    // The collapsed pill is left as a row on purpose, and this pins that. It is
    // the same element on a different display type -- display: flex,
    // width: fit-content -- so it looks like it should have the same problem,
    // and it does not: measured in Chromium at 390x844 with the pixel font
    // loaded, "Play-by-play hidden" plus "Select · Show commentator" comes to
    // 202px of the same 338px, clipping nothing. Stacking it would turn a 42px
    // pill into a 71px one and buy nothing.
    expect(stacked).not.toMatch(/flex-direction:\s*column/)

    // Desktop is untouched: the three-track row is still what the
    // unconditional rule declares. Matched as a whole selector, because
    // `.round4-body > .ringside-commentator` contains this class name too.
    const base = [...unconditional.matchAll(/([^{}]+)\{([^{}]*)\}/g)]
      .filter(([, selector]) => selector.split(',').some((one) => one.trim() === '.ringside-commentator'))
      .map(([, , body]) => body)
    expect(base).toHaveLength(1)
    expect(base[0]).toMatch(/grid-template-columns:\s*auto\s+minmax\(\s*0\s*,\s*1fr\s*\)\s+auto/)
  })

  it('keeps the bell locked and does not claim a non-start when the run request aborts', async () => {
    const abort = deferred<ReturnType<typeof jsonResponse>>()
    let armedReads = 0
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return abort.promise
      if (input === '/api/sessions/session-1') {
        armedReads += 1
        // The ring still reports ARMED first, then shows the run that did land.
        return Promise.resolve(jsonResponse(session(armedReads === 1 ? 'armed' : 'running')))
      }
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))

    fireEvent.click(await screen.findByRole('button', { name: /ring the bell/i }))
    // A client-side abort surfaces as ApiError with status 0.
    abort.reject(new ApiError('The request timed out.', 0))

    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent(/may still be starting/i)
    expect(alert).not.toHaveTextContent(/did not start/i)
    expect(screen.getByRole('button', { name: /confirming the bell/i })).toBeDisabled()

    // Re-polling finds the run that did land and moves to the proof.
    expect(await screen.findByRole('heading', { name: 'Wake this idle app' }, { timeout: 5000 })).toBeInTheDocument()
    expect(fetchMock.mock.calls.filter((call) => String(call[0]).endsWith('/run'))).toHaveLength(1)

    /* That re-poll is the one loop in the app that runs on a bare timer instead
       of inside an effect, so nothing unwinds it when the app goes away: it can
       still be mid-wait here, with up to ten reads left to make. Unmounting has
       to end it. Left running it reads a session nobody is showing, and in a
       suite it reads it through whatever fetch the next test installed. */
    const readsAtUnmount = fetchMock.mock.calls.filter((call) => call[0] === '/api/sessions/session-1').length
    cleanup()
    await new Promise((resolve) => { setTimeout(resolve, 1_800) })
    expect(fetchMock.mock.calls.filter((call) => call[0] === '/api/sessions/session-1')).toHaveLength(readsAtUnmount)
  })

  it('cuts from the armed ritual to the minimal proof only after the run is accepted', async () => {
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(session('running')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    const view = render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /sound on/i }))
    await user.click(screen.getByRole('button', { name: /sound off/i }))
    audioMocks.playOriginalBell.mockClear()
    audioMocks.startOriginalRoundTheme.mockClear()
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    expect(await screen.findByRole('heading', { name: 'Wake this idle app' })).toBeInTheDocument()
    expect(audioMocks.playOriginalBell).toHaveBeenCalledTimes(1)
    expect(audioMocks.startOriginalRoundTheme).toHaveBeenCalledTimes(1)
    expect(audioMocks.startOriginalRoundTheme).toHaveBeenCalledWith('wake_idle_app')
    expect(screen.getByLabelText('Lakebase result')).toBeInTheDocument()
    expect(screen.getByLabelText('Aurora Serverless v2 result')).toBeInTheDocument()
    const createBody = JSON.parse(fetchMock.mock.calls.find((call) => call[0] === '/api/sessions')![1].body)
    expect(createBody).toMatchObject({ primary_persona: 'sre', secondary_personas: [], corners: ['performance'], round_id: null })
    audioMocks.stopOriginalRoundTheme.mockClear()
    view.unmount()
    expect(audioMocks.stopOriginalRoundTheme).toHaveBeenCalledTimes(1)
  })

  it('mutes the cues from the sound toggle instead of stopping them', async () => {
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(session('running')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    // Let arming settle first: the stage effects that legitimately call stop
    // are keyed on session state, and this test is about the toggle alone.
    await screen.findByRole('button', { name: /ring the bell/i })
    const stops = () => audioMocks.stopOriginalRoundTheme.mock.calls.length
      + audioMocks.stopOriginalTitleTheme.mock.calls.length
      + audioMocks.stopOriginalCreditsTheme.mock.calls.length
    const stopsBefore = stops()

    // The label reports the current state, so "Sound on" is the button that
    // turns it off. Off, then back on: this is the regression. It used to be a
    // transport control, so a presenter who silenced the room lost their place
    // in the cue and got bar one again on the way back.
    await user.click(await screen.findByRole('button', { name: /sound on/i }))
    expect(audioMocks.setOriginalTitleThemeMuted).toHaveBeenLastCalledWith(true)
    expect(audioMocks.setOriginalRoundThemeMuted).toHaveBeenLastCalledWith(true)
    expect(audioMocks.setOriginalCreditsThemeMuted).toHaveBeenLastCalledWith(true)

    await user.click(await screen.findByRole('button', { name: /sound off/i }))
    expect(audioMocks.setOriginalTitleThemeMuted).toHaveBeenLastCalledWith(false)
    expect(audioMocks.setOriginalRoundThemeMuted).toHaveBeenLastCalledWith(false)
    expect(audioMocks.setOriginalCreditsThemeMuted).toHaveBeenLastCalledWith(false)

    // Nothing was torn down on the way through, so there is nothing to restart.
    expect(stops()).toBe(stopsBefore)
    expect(audioMocks.startOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.startOriginalTitleTheme).not.toHaveBeenCalled()
  })

  it('leaves the round cue running for the length of a live bout', async () => {
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(session('running')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /sound on/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    expect(await screen.findByRole('heading', { name: 'Wake this idle app' })).toBeInTheDocument()
    expect(audioMocks.startOriginalRoundTheme).toHaveBeenCalledTimes(1)

    /* The arena carries the mute, because muting is the thing a presenter
     * actually needs mid-bout. It is the only control on this screen that
     * touches audio, so a live bout can lose its cue in exactly two ways: an
     * effect firing underneath it, or this button doing more than it says.
     * Both are checked here -- the toggle over a running bout, then the event
     * beats that arrive during one. */
    audioMocks.startOriginalRoundTheme.mockClear()
    audioMocks.stopOriginalRoundTheme.mockClear()
    audioMocks.stopOriginalTitleTheme.mockClear()
    audioMocks.stopOriginalCreditsTheme.mockClear()

    // Sound went off on the fight card, so the arena reports the same flag.
    const arenaToggle = screen.getByRole('button', { name: /sound off/i })
    expect(arenaToggle).toHaveAttribute('aria-pressed', 'false')
    const laneBefore = screen.getByLabelText('Lakebase result')

    await user.click(arenaToggle)

    expect(screen.getByRole('button', { name: /sound on/i })).toHaveAttribute('aria-pressed', 'true')
    expect(audioMocks.setOriginalTitleThemeMuted).toHaveBeenLastCalledWith(false)
    expect(audioMocks.setOriginalRoundThemeMuted).toHaveBeenLastCalledWith(false)
    expect(audioMocks.setOriginalCreditsThemeMuted).toHaveBeenLastCalledWith(false)
    // Unmuting reached the mute setters and nothing else: no cue was started,
    // stopped or repositioned, so the score is still wherever it got to.
    expect(audioMocks.startOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.startOriginalTitleTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalTitleTheme).not.toHaveBeenCalled()
    expect(audioMocks.stopOriginalCreditsTheme).not.toHaveBeenCalled()
    // The same DOM node, so the arena was re-rendered and not remounted --
    // which is what keeps the running timers and animations off the floor.
    expect(screen.getByLabelText('Lakebase result')).toBe(laneBefore)

    const stream = FakeEventSource.instances.at(-1)!
    const beats: RunEvent[] = [1, 2, 3].map((sequence) => ({
      sequence,
      event: 'lane_update',
      occurred_at: '2026-08-17T00:00:00Z',
      payload: { session: session('running'), lane_id: 'lakebase', attempts: sequence },
    }))
    for (const beat of beats) act(() => { stream.emit(beat) })
    expect(audioMocks.stopOriginalRoundTheme).not.toHaveBeenCalled()
    expect(audioMocks.startOriginalRoundTheme).not.toHaveBeenCalled()
    expect(screen.getByLabelText('Lakebase result')).toBe(laneBefore)
  })

  it('tells the room how many calls the play-by-play missed', async () => {
    /* The server reports `gap_before` on the first event of a resume it had to
     * serve past its retention floor. The play-by-play used to render that hole
     * with nothing to show it: the dedup tolerates the jump, so no line was
     * wrong, but lines were simply absent and the screen implied nothing had
     * happened. Every event the stream skipped is counted and said out loud. */
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(session('running')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    expect(await screen.findByRole('heading', { name: 'Wake this idle app' })).toBeInTheDocument()

    const stream = FakeEventSource.instances.at(-1)!
    const commentator = screen.getByLabelText('Ringside commentator')
    expect(commentator).not.toHaveTextContent(/MISSED CALLS/i)

    // A resume that had to skip sequences 1 through 4.
    act(() => {
      stream.emit({
        sequence: 5,
        gap_before: 4,
        event: 'lane_update',
        occurred_at: '2026-08-17T00:00:00Z',
        payload: { session: session('running'), lane_id: 'lakebase', attempts: 1 },
      })
    })

    expect(commentator).toHaveTextContent(
      /MISSED CALLS · 4 calls never reached this screen · The play-by-play picked up after them/i,
    )

    // A second hole adds to the first rather than replacing it: a long bout can
    // resume across the floor more than once, and the total is what was missed.
    act(() => {
      stream.emit({
        sequence: 9,
        gap_before: 3,
        event: 'lane_update',
        occurred_at: '2026-08-17T00:00:00Z',
        payload: { session: session('running'), lane_id: 'lakebase', attempts: 2 },
      })
    })
    expect(commentator).toHaveTextContent(/MISSED CALLS · 7 calls/i)

    // And an ordinary beat leaves the count alone.
    act(() => {
      stream.emit({
        sequence: 10,
        event: 'lane_update',
        occurred_at: '2026-08-17T00:00:00Z',
        payload: { session: session('running'), lane_id: 'lakebase', attempts: 3 },
      })
    })
    expect(commentator).toHaveTextContent(/MISSED CALLS · 7 calls/i)
  })

  it('explains when a previous bout is still returning to scale zero', async () => {
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('checking')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    expect(await screen.findByText(/checking both corners/i)).toBeInTheDocument()

    FakeEventSource.instances.at(-1)?.emit({
      sequence: 3,
      event: 'arm_waiting',
      occurred_at: '2026-08-17T00:00:00Z',
      payload: {
        state: 'checking',
        status: 'Lakebase endpoint is ACTIVE, not IDLE · Aurora writer has not produced two consecutive zero-capacity samples',
      },
    })

    expect(await screen.findByText(/lakebase is still cooling/i)).toBeInTheDocument()
    expect(screen.getByText(/aurora auto-pauses after 5 idle minutes.*aws confirmation may add ~1–2 minutes.*bell not started/i)).toBeInTheDocument()
  })

  it('lets the ring owner leave a long Round 1 start-state check', async () => {
    const cancelled = {
      ...session('failed'),
      failure: 'Fight-card check cancelled by the ring owner. No run started and no result was recorded.',
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('checking')))
      if (input.endsWith('/cancel-arm')) return Promise.resolve(jsonResponse(cancelled))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))

    const leave = await screen.findByRole('button', { name: /choose another round/i })
    await user.click(leave)

    expect(await screen.findByText(/· round \d+ of six/i)).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/sessions/session-1/cancel-arm',
      expect.objectContaining({ method: 'POST' }),
    )
    expect(screen.queryByText(/checking both corners/i)).not.toBeInTheDocument()
  })

  it('shows why a wedged towel is holding the ring instead of fixed copy', async () => {
    const refusal =
      'BOUT IN PROGRESS · RECOVER THIS DELETED ORDER · TOWEL CLEANUP · DEMO OPERATOR · FENCE 1 · '
      + 'HEARTBEAT 0S AGO · RING UNLOCKS IN 90S · TOWEL CLEANUP FAILED · '
      + 'Recovery environments could not be safely removed.'
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) {
        return Promise.resolve({
          ok: false,
          status: 409,
          statusText: 'Conflict',
          json: async () => ({ detail: refusal }),
        })
      }
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))

    expect(
      await screen.findByText(/towel cleanup failed.*recovery environments could not be safely removed/i),
    ).toBeInTheDocument()
    expect(screen.queryByText(/please wait · the current bout owns the ring/i)).not.toBeInTheDocument()
  })

  it('preserves global-scope locking and identifies the current owner before posting', async () => {
    const currentBout = {
      scope: 'global' as const,
      round_id: null,
      ring_ready: true,
      can_start: false,
      maintenance_state: 'ready' as const,
      maintenance_detail: null,
      active: true,
      operator: { display_name: 'Demo Operator', email: 'operator@example.com' },
      started_at: '2026-08-17T20:15:30Z',
      updated_at: '2026-08-17T20:15:45Z',
      expires_at: '2026-08-17T20:45:45Z',
      phase: 'run_committed',
      state: 'running' as const,
      round_title: 'Wake this idle app',
      competitor: 'Aurora Serverless v2',
    }
    vi.mocked(api.boutStatus).mockResolvedValue(currentBout)
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/bout') return Promise.resolve(jsonResponse(currentBout))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) {
        return Promise.resolve({
          ok: false,
          status: 409,
          statusText: 'Conflict',
          json: async () => ({ detail: 'BOUT IN PROGRESS · PLEASE WAIT FOR THE CURRENT VERDICT' }),
        })
      }
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    const prepare = await screen.findByRole('button', { name: /prepare fight card/i })
    expect(prepare).toBeDisabled()
    expect(await screen.findByText(/bout in progress.*wake this idle app.*demo operator.*unlocks automatically/i)).toBeInTheDocument()
    expect(fetchMock.mock.calls.some((call) => call[0] === '/api/sessions')).toBe(false)
  })

  it('polls the selected round, marks that round busy on the card, and never greys Prepare without saying why', async () => {
    const occupiedRound = {
      scope: 'round' as const,
      round_id: 'wake_idle_app' as const,
      ring_ready: true,
      can_start: false,
      maintenance_state: 'ready' as const,
      maintenance_detail: null,
      active: true,
      operator: { display_name: 'Round One Operator', email: 'round.one@databricks.com' },
      started_at: '2026-08-17T20:15:30Z',
      updated_at: '2026-08-17T20:15:45Z',
      expires_at: '2026-08-17T20:45:45Z',
      phase: 'run_committed',
      state: 'running' as const,
      round_title: 'Wake this idle app',
      competitor: 'Aurora Serverless v2',
    }
    // A FREE RING AS THE SERVER ACTUALLY REPORTS ONE. This used to spread
    // `occupiedRound` and flip `can_start`, which describes a status
    // `RunManager.bout_status` cannot return: it answers a ring with no lease
    // as `active=False` with every lease field absent, and an occupied one as
    // `active=True, can_start=False`. The two are exclusive there, so
    // `active=True` beside `can_start=True` was a shape nothing could produce.
    // It passed because nothing on screen read `active` for a round whose ring
    // was free -- and the round tiles now do.
    const selectedRoundAvailable = {
      scope: 'round' as const,
      round_id: 'make_schema_change_safely' as const,
      ring_ready: true,
      can_start: true,
      maintenance_state: 'ready' as const,
      maintenance_detail: null,
      active: false,
      operator: null,
      started_at: null,
      updated_at: null,
      expires_at: null,
      phase: null,
      state: null,
      round_title: null,
      competitor: null,
    }
    // Round 2's answer is held until the test releases it. On the deployed app
    // that window is a network round trip against the durable coordination row,
    // and it is the window the owner was looking at: the status is keyed by
    // round, so pressing a tile discards the one we have and there is nothing
    // to reason from until the new read lands. Deferring it makes that state
    // sit still to be inspected instead of being raced.
    let releaseRoundTwo = () => {}
    const roundTwoAnswered = new Promise<void>((resolve) => { releaseRoundTwo = resolve })
    vi.mocked(api.boutStatus).mockImplementation(async (roundId) => {
      if (roundId === 'wake_idle_app') return occupiedRound
      await roundTwoAnswered
      return selectedRoundAvailable
    })
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input: string) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      throw new Error(`Unexpected request: ${input}`)
    }))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))

    expect(await screen.findByRole('button', { name: /prepare fight card/i })).toBeDisabled()
    await waitFor(() => expect(refusalOnScreen()).toMatch(/wake this idle app.*round one operator/i))
    expect(api.boutStatus).toHaveBeenCalledWith('wake_idle_app')

    // THE ROUND ITSELF SAYS SO, not only the strip under the buttons. Round 1
    // is the round the ring answered about, so Round 1's tile is the one that
    // carries it -- and it carries it INSTEAD OF its lane line, not above it.
    // Both halves are load-bearing: dropping the position check would pass a
    // build that marked whichever tile it liked, and dropping the "gone" check
    // would pass one that added a row and pushed PREPARE toward the fold.
    await waitFor(() => expect(laneLinesOnScreen()[0]).toBe('BOUT IN PROGRESS'))
    expect(laneLinesOnScreen()[0]).not.toMatch(/both corners timed/i)
    expect(laneLinesOnScreen().slice(1)).not.toContain('BOUT IN PROGRESS')
    // The tile is a swap of copy and never a new element, so the lane line is
    // still one line's worth of words: the replacement may not outrun the
    // longest thing it displaces. jsdom lays nothing out, so this is a proxy for
    // the wrap and is deliberately measured against the real strings.
    expect('BOUT IN PROGRESS'.length).toBeLessThanOrEqual(
      Math.min(...laneLinesOnScreen().slice(1).map((line) => line.length)),
    )

    await pickRound(user, 'make_schema_change_safely')

    // THE DEFECT. Prepare greys out the instant the selection moves, because a
    // status it has not read cannot say the ring is free -- and every sentence
    // on this screen was written behind a status that exists, so the room got a
    // dead control and no words at all.
    expect(screen.getByRole('button', { name: /prepare fight card/i })).toBeDisabled()
    expect(refusalOnScreen()).toMatch(/checking the ring/i)
    expect(screen.queryByText(/wake this idle app.*round one operator/i)).not.toBeInTheDocument()
    // AND THE GRID STOPS CLAIMING IT. Round 1's status was read for Round 1 and
    // is discarded with the selection, so nothing is polling that ring any more.
    // Leaving the marking up would be the surface asserting a condition it is
    // no longer checking, which is the standing fault here, and it would go
    // stale silently rather than visibly.
    expect(laneLinesOnScreen()).not.toContain('BOUT IN PROGRESS')

    releaseRoundTwo()
    await waitFor(() => expect(screen.getByRole('button', { name: /prepare fight card/i })).toBeEnabled())
    expect(refusalOnScreen()).toBe('')
    expect(api.boutStatus).toHaveBeenCalledWith('make_schema_change_safely')
    expect(screen.queryByText(/wake this idle app.*round one operator/i)).not.toBeInTheDocument()
    // Round 2 answered `can_start`, so no tile is marked -- including Round 1,
    // whose ring this read says nothing at all about.
    expect(laneLinesOnScreen()).not.toContain('BOUT IN PROGRESS')
    expect(laneLinesOnScreen()[0]).toMatch(/both corners timed/i)
  })

  it('ticks in the ring for presentation, then replaces both clocks with server-verified times', async () => {
    const running = session('running')
    const executive = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'executive')!
    const verified: DemoSession = {
      ...session('verified'),
      secondary_personas: [executive],
      corners: ['cost', 'simplicity', 'performance'],
      lanes: {
        lakebase: {
          ...running.lanes.lakebase,
          state: 'verified',
          elapsed_ms: 842.6,
          attempts: 1,
          status: 'Transaction verified',
        },
        competitor: {
          ...running.lanes.competitor,
          state: 'verified',
          elapsed_ms: 1288.3,
          attempts: 1,
          status: 'Transaction verified',
        },
      },
      remembered_result: 'LAKEBASE WINS BY 0.45s',
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    const lakebaseLane = await screen.findByLabelText('Lakebase result')
    const liveClock = lakebaseLane.querySelector('.lane-time')
    expect(liveClock).toHaveTextContent('0.00s')
    await waitFor(() => expect(liveClock).not.toHaveTextContent('0.00s'))

    const source = FakeEventSource.instances.at(-1)!
    source.onerror?.()
    expect(await screen.findByRole('alert')).toHaveTextContent(/live evidence stream interrupted/i)
    source.emit({
      sequence: 8,
      event: 'lane_update',
      occurred_at: '2026-08-17T00:00:00.842Z',
      payload: {
        lane_id: 'lakebase',
        state: 'verified',
        attempts: 1,
        elapsed_ms: 842.6,
        status: 'Transaction verified',
      },
    })

    await waitFor(() => expect(lakebaseLane.querySelector('.lane-time')).toHaveTextContent('0.84s'))
    expect(screen.queryByText(/live evidence stream interrupted/i)).not.toBeInTheDocument()
    source.emit({
      sequence: 7,
      event: 'lane_update',
      occurred_at: '2026-08-17T00:00:00.700Z',
      payload: {
        lane_id: 'lakebase', state: 'connecting', attempts: 1,
        elapsed_ms: 700, status: 'Stale event must be ignored', error: 'stale error',
      },
    })
    expect(lakebaseLane).toHaveAttribute('data-state', 'verified')
    expect(lakebaseLane).not.toHaveTextContent(/stale event|stale error/i)
    await new Promise((resolve) => window.setTimeout(resolve, 100))
    expect(lakebaseLane.querySelector('.lane-time')).toHaveTextContent('0.84s')
    expect(screen.getByLabelText('Aurora Serverless v2 result').querySelector('.lane-time')).not.toHaveTextContent('0.00s')

    audioMocks.stopOriginalRoundTheme.mockClear()
    source.emit({
      sequence: 9,
      event: 'run_finished',
      occurred_at: '2026-08-17T00:00:01Z',
      payload: { state: 'verified', session: verified },
    })

    expect(await screen.findByText('LAKEBASE WINS BY 0.45s')).toBeInTheDocument()
    await waitFor(() => expect(audioMocks.stopOriginalRoundTheme).toHaveBeenCalledTimes(1))
    const starredRedo = screen.getByRole('button', { name: /re-do round/i })
    expect(starredRedo).toHaveTextContent('★ SHOW · RE-DO ROUND')
    expect(screen.getByText(/same db region · same data · same client · same transaction · same verification/i)).toBeInTheDocument()
    // The strip is a roster, not a briefing: header, portraits, name and role.
    // The sentences it used to print are asserted below, on the overlay that
    // still carries them.
    const ringsideMeanings = screen.getByLabelText(/what the result means at ringside/i)
    expect(ringsideMeanings).toHaveTextContent(/why this matters at ringside/i)
    expect(ringsideMeanings).toHaveTextContent(/cost \+ simplicity \+ performance/i)
    expect(ringsideMeanings).toHaveTextContent(/3 a\.m\. sam · sre/i)
    expect(ringsideMeanings).toHaveTextContent(/the big why · executive/i)
    expect(ringsideMeanings.querySelectorAll('img')).toHaveLength(2)
    expect(ringsideMeanings.querySelector('p')).toBeNull()
    expect(ringsideMeanings).not.toHaveTextContent(/on-call gets a tested database-action recovery signal from the transaction itself/i)
    expect(ringsideMeanings).not.toHaveTextContent(/idle infrastructure returned to useful work without an operator in the loop/i)
    // The per-corner evidence row is gone outright, along with the yellow label
    // block that used to render illegibly inside it.
    expect(ringsideMeanings.querySelector('.ringside-priority-evidence')).toBeNull()
    expect(screen.queryByLabelText(/selected priority evidence/i)).not.toBeInTheDocument()
    expect(ringsideMeanings).not.toHaveTextContent(/commit \+ read-back/i)
    // Cleanup is clear here, so the actions row owns the way into the overlay
    // and the strip does not duplicate it.
    expect(ringsideMeanings.querySelector('.ringside-meanings-explain')).toBeNull()
    expect(lakebaseLane.querySelector('.lane-time')).toHaveTextContent('0.84s')
    expect(screen.getByLabelText('Aurora Serverless v2 result').querySelector('.lane-time')).toHaveTextContent('1.29s')
    expect(screen.getByRole('button', { name: /re-do round/i })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /instant replay/i }))
    const replay = await screen.findByRole('dialog', { name: /how this round was proved/i })
    expect(replay).toHaveTextContent(/both control planes proved genuine scale zero/i)
    expect(replay).toHaveTextContent(/unique nonce/i)
    expect(replay).toHaveTextContent('0.84s')
    expect(replay).toHaveTextContent('1.29s')
    expect(replay).not.toHaveTextContent(/RDS\.DescribeDBClusters/i)
    await user.click(within(replay).getByRole('button', { name: /01.*control planes.*view calls/i }))
    const startCalls = within(replay).getByLabelText(/exact calls for step 01/i)
    expect(startCalls).toHaveTextContent(/databricks postgres get-endpoint <endpoint> -o json/i)
    expect(startCalls).toHaveTextContent(/RDS\.DescribeDBClusters/i)
    expect(startCalls).toHaveTextContent(/CloudWatch\.GetMetricStatistics/i)
    await user.click(within(replay).getByRole('button', { name: /03.*unique nonce.*view calls/i }))
    const transactionCalls = within(replay).getByLabelText(/exact calls for step 03/i)
    expect(transactionCalls).toHaveTextContent(/PostgreSQL wire—not a cloud API/i)
    expect(transactionCalls).toHaveTextContent(/INSERT INTO public\.anti_demo_probe/i)
    expect(replay).not.toHaveTextContent(/RDS\.DescribeDBClusters/i)
    await user.click(within(replay).getByRole('button', { name: /back to the ring/i }))

    await user.click(screen.getByRole('button', { name: /explain to the room/i }))
    const ringsideTake = await screen.findByRole('dialog', { name: /make the result matter/i })
    expect(ringsideTake).toHaveTextContent(/one-line takeaway for the sre/i)
    expect(ringsideTake).toHaveTextContent(/on-call gets a tested database-action recovery signal from the transaction itself/i)
    expect(ringsideTake).toHaveTextContent(/proof behind it.*0\.84s.*0\.45s before aurora serverless v2/i)
    expect(ringsideTake).toHaveTextContent(/shared exact proof.*lakebase 0\.84s.*aurora serverless v2 1\.29s.*commit \+ read-back/i)
    expect(ringsideTake).toHaveTextContent(/what this does not claim.*service recovery and customer experience were not tested/i)
    expect(ringsideTake).toHaveTextContent(/what must recover/i)
    await user.click(within(ringsideTake).getByRole('button', { name: /the big why/i }))
    expect(ringsideTake).toHaveTextContent(/who benefits.*why now/i)
    await user.click(within(ringsideTake).getByRole('button', { name: /back to the ring/i }))

    expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
    stubReceiptCanvas()
    await user.click(screen.getByRole('button', { name: /share the receipt/i }))
    const receipt = await screen.findByRole('dialog', { name: /share the proof/i })
    const poster = within(receipt).getByLabelText(/verified result poster preview/i)
    expect(within(poster).getByLabelText(/lakebase receipt result/i)).toHaveTextContent('0.84s')
    expect(within(poster).getByLabelText(/aurora serverless v2 receipt result/i)).toHaveTextContent('1.29s')
    expect(poster).toHaveTextContent(/one live run.*not a benchmark/i)
    expect(within(receipt).getByRole('button', { name: /copy caption/i })).toBeInTheDocument()
    expect(await within(receipt).findByRole('button', { name: /prepare linkedin post/i })).toBeInTheDocument()
    expect(receipt).not.toHaveTextContent(/caption copied with the post kit/i)
    expect(receipt).not.toHaveTextContent(/127\.0\.0\.1|localhost/i)
    expect(receipt).not.toHaveTextContent(/try the same round yourself/i)
    await user.click(within(receipt).getByRole('button', { name: /^b · back$/i }))

    await user.click(screen.getByRole('button', { name: /next round/i }))
    expect(screen.getByText(/· round \d+ of six/i)).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: /make this schema change safely/i })).toBeInTheDocument()
  })

  // The deployed app's own words for Round 1, copied from
  // `server/round_availability.py` so the test is exercising the real thing.
  // Every clause of it is true and every clause of it is written for the person
  // who provisioned the install.
  const OPERATOR_REASON = 'THIS ROUND CANNOT RUN IN THE DEPLOYED APP, AND THIS IS NOT A FAULT TO WAIT OUT. '
    + 'It races a live Aurora or RDS opponent over TCP 5432, and every one of those database security groups '
    + 'admits a single operator CIDR: the laptop that provisioned the install. There is no ingress rule to add.'
  const AUDIENCE_HEADLINE = "THIS ROUND IS NOT ON TONIGHT'S CARD. It races a live AWS database that only "
    + "the operator's own machine is allowed to reach, and this is the hosted app."

  function refusedCatalog(refused: string[], reason: string, headline: string | null) {
    return {
      ...FALLBACK_CATALOG,
      rounds: FALLBACK_CATALOG.rounds.map((round) => (refused.includes(round.id)
        ? {
          ...round,
          availability: 'unavailable' as const,
          availability_reason: reason,
          ...(headline === null ? {} : { availability_headline: headline }),
        }
        : round)),
    }
  }

  async function fightCardFor(catalog: ReturnType<typeof refusedCatalog>, roundId: string) {
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input: string) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      throw new Error(`Unexpected request: ${input}`)
    }))
    const user = userEvent.setup()
    render(<App />)
    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, roundId)
    return user
  }

  it('leads the fight card with the demo’s voice, not the operator’s paragraph', async () => {
    await fightCardFor(refusedCatalog(['wake_idle_app'], OPERATOR_REASON, AUDIENCE_HEADLINE), 'wake_idle_app')

    expect(screen.getByText(/not on tonight's card/i)).toBeVisible()
    // None of the operator's vocabulary is on the screen unasked. Every clause
    // named here was being shown to a room, above the fold, in the same type as
    // the rest of the card.
    expect(screen.getByText(/TCP 5432/)).not.toBeVisible()
    expect(screen.getByText(/security groups admits a single operator CIDR/i)).not.toBeVisible()
    expect(screen.getByText(/no ingress rule to add/i)).not.toBeVisible()
  })

  it('folds the operator’s account away rather than deleting it', async () => {
    const user = await fightCardFor(
      refusedCatalog(['wake_idle_app'], OPERATOR_REASON, AUDIENCE_HEADLINE),
      'wake_idle_app',
    )

    const disclosure = screen.getByText('Operator detail').closest('details')!
    expect(disclosure).not.toHaveAttribute('open')
    await user.click(screen.getByText('Operator detail'))
    expect(disclosure).toHaveAttribute('open')
    expect(screen.getByText(/TCP 5432/)).toBeVisible()
    expect(screen.getByText(/security groups admits a single operator CIDR/i)).toBeVisible()
  })

  it('never repeats the refusal in the strip under the buttons', async () => {
    // The regression: that strip is 5-8px type sized for a status token, and it
    // was being handed the whole paragraph. It rendered full-bleed with the
    // lines overstruck on each other, illegible, directly beneath a copy of the
    // same prose the WHY panel was already showing.
    await fightCardFor(refusedCatalog(['wake_idle_app'], OPERATOR_REASON, AUDIENCE_HEADLINE), 'wake_idle_app')

    const strip = document.querySelector('.game-lock-note')!
    expect(strip).toHaveTextContent(/unavailable tonight · change round to pick one that can run/i)
    expect(strip).not.toHaveTextContent(/TCP 5432/)
    expect(strip.textContent!.length).toBeLessThan(70)
  })

  it('does not send the room to the round list when no round can run', async () => {
    const everyRound = FALLBACK_CATALOG.rounds.map((round) => round.id)
    await fightCardFor(refusedCatalog(everyRound, OPERATOR_REASON, AUDIENCE_HEADLINE), 'wake_idle_app')

    expect(document.querySelector('.game-lock-note')).toHaveTextContent(
      /unavailable tonight · no round can run right now/i,
    )
  })

  it('still says something when the server is older than the browser', async () => {
    // A cached bundle can outlive the server that answers it. With no headline
    // the card falls back to the reason, which is what it has always shown --
    // worse than the new copy, and much better than an empty WHY panel.
    await fightCardFor(refusedCatalog(['wake_idle_app'], OPERATOR_REASON, null), 'wake_idle_app')

    expect(screen.getByText('Why').parentElement).toHaveTextContent(/CANNOT RUN IN THE DEPLOYED APP/)
    expect(screen.queryByText(/operator detail/i)).not.toBeInTheDocument()
  })

  it('renders the scoped Round 4 v1 and v2 proof without legacy or race claims', async () => {
    const catalog = {
      ...FALLBACK_CATALOG,
      rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === 'put_model_score_in_app'
        ? { ...round, availability: 'ready' as const }
        : round),
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(modelScoreSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(modelScoreSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(modelScoreSession('running')))
      if (input.endsWith('/redo')) return Promise.resolve(jsonResponse(modelScoreSession('running', 'running')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'put_model_score_in_app')
    // The Round 4 legend explained a star used only in the removed round
    // dropdown. It still rides the proof screen, which is asserted below.
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    expect(await screen.findByLabelText(/lakebase v1 result/i)).toHaveTextContent('0.00s')
    expect(screen.getByRole('heading', { name: 'Move lakehouse data into live applications' })).toBeInTheDocument()
    expect(screen.getByText(/Round 4 · Reverse ETL · OLAP → OLTP/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/lakehouse to live app data flow/i)).toHaveTextContent('Analytics Delta')
    expect(screen.getByLabelText(/lakehouse to live app data flow/i)).toHaveTextContent('Managed Reverse ETL')
    expect(screen.getByLabelText(/lakehouse to live app data flow/i)).toHaveTextContent('Operational Postgres / Live App')
    expect(screen.getByLabelText('AWS disclosure')).toHaveTextContent('Why Lakebase wins this round')
    expect(screen.getByLabelText('AWS disclosure')).toHaveTextContent('Built-in OLAP → OLTP')
    expect(screen.getByLabelText('AWS disclosure')).toHaveTextContent('Separate reverse-ETL stack required')
    expect(screen.getByLabelText('AWS disclosure')).toHaveTextContent('Add product + connectors + security + network + operations')
    expect(screen.getByLabelText('AWS disclosure')).toHaveTextContent('Not built or timed')
    expect(screen.queryByText('SCOPE')).not.toBeInTheDocument()
    expect(screen.getByText(/Syncing the lakehouse score and watching the live app/i, { selector: '.round4-footer > p' })).toBeInTheDocument()
    expect(screen.getByText(/Ringside commentator/i)).toBeInTheDocument()
    expect(screen.getByText('Ryan')).toBeInTheDocument()
    expect(screen.getByRole('img', { name: 'Ryan' })).toBeInTheDocument()
    expect(screen.getByLabelText(/ringside commentator/i).querySelector('.ringside-announcer-mic')).toBeNull()
    expect(screen.getByLabelText(/ringside commentator/i)).not.toHaveTextContent(/Server events only · No ETA/i)
    expect(screen.getByLabelText(/ringside commentator/i)).toHaveTextContent(/Lakehouse row committed · Bell starts now/i)
    const hidePlayByPlay = screen.getByRole('button', { name: /hide play-by-play/i })
    expect(hidePlayByPlay).toHaveAttribute('aria-controls', 'ringside-commentator-status')
    await user.click(hidePlayByPlay)
    const collapsedCommentator = screen.getByLabelText(/ringside commentator/i)
    expect(within(collapsedCommentator).queryByRole('img', { name: 'Ryan' })).not.toBeInTheDocument()
    expect(collapsedCommentator.querySelector('header')).toBeNull()
    const showPlayByPlay = within(collapsedCommentator).getByRole('button', { name: /show commentator/i })
    expect(showPlayByPlay).toHaveAttribute('aria-pressed', 'false')
    expect(showPlayByPlay).toHaveAttribute('aria-controls', 'ringside-commentator-status')
    await user.click(showPlayByPlay)
    expect(screen.getByRole('img', { name: 'Ryan' })).toBeInTheDocument()

    const source = FakeEventSource.instances.at(-1)!
    source.emit({
      sequence: 6, event: 'lane_update', occurred_at: '2026-08-18T00:00:00.400Z',
      payload: { lane_id: 'lakebase', state: 'verifying', elapsed_ms: 400, status: 'Waiting for Managed Sync', activity: { phase: 'waiting_sync', wire_call: null } },
    })
    await waitFor(() => expect(screen.getByLabelText(/lakehouse to live app data flow/i)).toHaveTextContent('0.40s'))
    expect(screen.getByLabelText(/ringside commentator/i)).toHaveTextContent(/Managed reverse ETL is moving the exact Delta row into Lakebase/i)
    source.emit({
      sequence: 7, event: 'lane_update', occurred_at: '2026-08-18T00:00:00.840Z',
      payload: { lane_id: 'lakebase', state: 'verifying', elapsed_ms: 840, status: 'Reading exact application row', activity: { phase: 'reading_application', wire_call: null } },
    })
    await waitFor(() => expect(screen.getByLabelText(/lakehouse to live app data flow/i)).toHaveTextContent('0.84s'))
    expect(screen.getByLabelText(/ringside commentator/i)).toHaveTextContent(/Reverse ETL is complete · A fresh app connection is reading that exact row/i)
    source.emit({
      sequence: 8, event: 'run_finished', occurred_at: '2026-08-18T00:00:01Z',
      payload: { state: 'verified', session: modelScoreSession('verified') },
    })

    await waitFor(() => expect(screen.getByLabelText(/lakehouse to live app data flow/i)).toHaveTextContent('0.84s'))
    const verifiedFlow = screen.getByLabelText(/lakehouse to live app data flow/i)
    const timingBreakdown = within(verifiedFlow).getByLabelText(/completed reverse etl timing breakdown/i)
    expect(timingBreakdown).toHaveTextContent('Reverse ETL sync0.64s')
    expect(timingBreakdown).toHaveTextContent('Full proof time0.84sSync check + fresh app connection + exact row read')
    expect(timingBreakdown).not.toHaveTextContent(/subsecond|row available.*app verified/i)
    expect(within(verifiedFlow).getByText('0.84s')).toBeInTheDocument()
    expect(screen.queryByLabelText(/integrity proof receipt/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/Reverse ETL sync 640\.00 ms/)).not.toBeInTheDocument()
    expect(screen.queryByText('round4-v1-full-proof-nonce-aaaaaaaaaaaaaaaa')).not.toBeInTheDocument()
    expect(screen.getByText(/Verified: customer customer-42 risk score 0\.81 reached the live app/i, { selector: '.round4-footer > p' })).toBeInTheDocument()
    expect(screen.getByLabelText('AWS disclosure')).toHaveTextContent(/Managed reverse ETL · live app verified in 0\.84s/i)
    const commentator = screen.getByLabelText(/ringside commentator/i)
    expect(commentator).toHaveTextContent(/The exact row reached Lakebase in 0\.64s.*0\.84s full proof adds a sync check, fresh app connection, and exact row read; it is not SQL query time/i)
    expect(commentator).toHaveTextContent(/Aurora \/ RDS alone do not move the row.*Add and operate a reverse-ETL stack/i)
    const redoButton = screen.getByRole('button', { name: 'B · RE-DO' })
    expect(redoButton).toBeInTheDocument()
    expect(redoButton.parentElement).not.toHaveTextContent('Change this demo’s customer risk score from v1 to v2 in the lakehouse')
    expect(screen.getByRole('button', { name: /explain to the room/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /instant replay/i })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /explain to the room/i }))
    const ringsideTake = screen.getByRole('dialog', { name: /make the result matter/i })
    expect(ringsideTake).toHaveTextContent(/one exact analytics row reached the live app in 0\.84s through managed reverse ETL/i)
    expect(ringsideTake).toHaveTextContent(/RDS\/Aurora alone are OLTP sinks/i)
    expect(ringsideTake).not.toHaveTextContent(/non-executable/i)
    await user.click(within(ringsideTake).getByRole('button', { name: /back to the ring/i }))

    stubReceiptCanvas()
    await user.click(screen.getByRole('button', { name: /share the receipt/i }))
    const shareReceipt = await screen.findByRole('dialog', { name: /share the proof/i })
    const poster = within(shareReceipt).getByLabelText(/verified result poster preview/i)
    expect(within(poster).getByLabelText(/lakebase receipt result/i)).toHaveTextContent(/0\.84s.*LIVE APP VERIFIED.*BUILT-IN MANAGED REVERSE ETL/i)
    expect(within(poster).getByLabelText(/lakebase receipt result/i)).not.toHaveTextContent(/SCORE 0\.81/i)
    expect(within(poster).getByLabelText(/aurora serverless v2 receipt result/i)).toHaveTextContent(/SEPARATE REVERSE-ETL STACK REQUIRED.*NOT BUILT OR TIMED/i)
    expect(poster).toHaveTextContent(/ANALYTICS CHANGE → LIVE APP · 0\.84s/i)
    expect(poster).toHaveTextContent(/INTEGRITY.*CUSTOMER customer-42.*RISK 0\.81.*MODEL risk-v1.*NONCE round4-v1-full-proof-nonce/i)
    expect(shareReceipt).not.toHaveTextContent(/no auto scale-to-zero|two live PostgreSQL databases/i)
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } })
    await user.click(within(shareReceipt).getByRole('button', { name: /copy caption/i }))
    expect(writeText).toHaveBeenCalledWith(expect.stringMatching(
      /^Lakebase moved an analytics change into the live app in 0\.84s[\s\S]*Lakebase · Live app verified in 0\.84s · built-in managed reverse ETL[\s\S]*separate reverse-ETL stack required · not built or timed[\s\S]*Integrity · customer customer-42[\s\S]*One live managed reverse-ETL proof, not a benchmark\./,
    ))
    expect(await within(shareReceipt).findByRole('button', { name: /prepare linkedin post/i })).toBeInTheDocument()
    await user.click(within(shareReceipt).getByRole('button', { name: /^b · back$/i }))

    await user.click(screen.getByRole('button', { name: /^B · RE-DO$/i }))
    const redoRequest = fetchMock.mock.calls.find((call) => String(call[0]).endsWith('/redo'))
    expect(redoRequest![1]).not.toHaveProperty('body')
    const v1Ribbon = await screen.findByLabelText(/immutable v1 verified proof/i)
    expect(v1Ribbon).toHaveTextContent(/Previous live app state · V1 verified.*Customer customer-42.*Risk score 0\.81.*Model risk-v1.*Exact row/i)
    expect(v1Ribbon).not.toHaveTextContent(/nonce/i)
    expect(screen.getByLabelText(/lakebase v2 result/i)).toHaveTextContent('Reading the exact v2 row')
    expect(screen.getByLabelText(/ringside commentator/i)).toHaveTextContent(/Reverse ETL is complete · A fresh app connection is reading that exact row/i)
    expect(screen.getByText(/Changing the score in the lakehouse and watching the live app/i, { selector: '.round4-footer > p' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /next round/i })).not.toBeInTheDocument()

    source.emit({
      sequence: 9, event: 'redo_lane_update', occurred_at: '2026-08-18T00:00:02Z',
      payload: { lane_id: 'lakebase', state: 'verifying', status: 'V2 ONLY STATUS' },
    })
    expect(await screen.findByText(/Clock stops after exact app row read-back · V2 ONLY STATUS/)).toBeInTheDocument()
    expect(screen.getByLabelText(/immutable v1 verified proof/i)).not.toHaveTextContent('V2 ONLY STATUS')
    expect(screen.getByText(/Changing the score in the lakehouse and watching the live app/i, { selector: '.round4-footer > p' })).toBeInTheDocument()
    source.emit({
      sequence: 10, event: 'redo_finished', occurred_at: '2026-08-18T00:00:03Z',
      payload: { session: modelScoreSession('verified', 'verified') },
    })
    expect(await screen.findByText('LIVE APP UPDATED AGAIN · CUSTOMER customer-42 · RISK SCORE 0.81 → 0.33 · MODEL risk-v1 → risk-v2')).toBeInTheDocument()
    expect(screen.getByLabelText(/ringside commentator/i)).toHaveTextContent(/The exact row reached Lakebase in 0\.51s.*0\.72s full proof adds a sync check, fresh app connection, and exact row read; it is not SQL query time/i)
    expect(screen.queryByText('round4-v2-full-proof-nonce-bbbbbbbbbbbbbbbb')).not.toBeInTheDocument()
    expect(screen.getByText(/Verified again: customer customer-42 risk score changed 0\.81 → 0\.33 in the lakehouse and reached the live app/i, { selector: '.round4-footer > p' })).toBeInTheDocument()
    expect(screen.queryByText(/faster|sooner|speedup/i)).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()
  })

  it('offers an instant replay on a verified Round 4 built from the managed sync evidence', async () => {
    const catalog = {
      ...FALLBACK_CATALOG,
      rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === 'put_model_score_in_app'
        ? { ...round, availability: 'ready' as const }
        : round),
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(modelScoreSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(modelScoreSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(modelScoreSession('verified')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'put_model_score_in_app')
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    await user.click(await screen.findByRole('button', { name: /instant replay/i }))
    const replay = screen.getByRole('dialog', { name: /how this round was proved/i })
    expect(replay).toHaveTextContent(/instant replay · round 4/i)

    // The scored lane clock is the end-to-end proof, and the untimed AWS lane
    // must never be dressed up as a competing number.
    expect(replay).toHaveTextContent(/Lakebase0\.84s/)
    expect(replay).toHaveTextContent(/NOT TIMED/)

    // A capability round released one lane, so there is no start gap to quote.
    expect(replay).toHaveTextContent(/Opponent lane/i)
    expect(replay).toHaveTextContent(/No AWS-native equivalent lane was configured or timed in this scoped proof/i)
    expect(replay).not.toHaveTextContent(/Start gap/i)
    expect(replay).not.toHaveTextContent(/Difference between lane start times/i)
    expect(replay).not.toHaveTextContent(/Provider-specific calls will appear/i)

    // The commit step carries the real Delta version the round advanced to.
    await user.click(within(replay).getByRole('button', { name: /One exact row was committed/i }))
    expect(within(replay).getByLabelText(/exact calls for step 02/i)).toHaveTextContent(/delta commit version = 11/)

    // Step 3 must carry the primary sync metric, not the end-to-end metric.
    await user.click(within(replay).getByRole('button', { name: /Managed Sync was then polled/i }))
    const syncStep = within(replay).getByLabelText(/exact calls for step 03/i)
    expect(syncStep).toHaveTextContent(/Reverse ETL sync \(primary\)640\.00 ms/)
    expect(syncStep).toHaveTextContent(/reports Delta version 11/)
    expect(syncStep).not.toHaveTextContent(/840\.00 ms/)

    // Step 4 must carry the secondary end-to-end metric and the exact row.
    await user.click(within(replay).getByRole('button', { name: /fresh application Postgres connection returning the exact row/i }))
    const readStep = within(replay).getByLabelText(/exact calls for step 04/i)
    expect(readStep).toHaveTextContent(/End-to-end proof \(secondary\)840\.00 ms/)
    expect(readStep).toHaveTextContent(/customer-42 · score 0\.81 · model risk-v1/)
    expect(readStep).toHaveTextContent(/Exact row verified/i)
  })

  it('offers an instant replay on a verified Round 6 built from the native CDF evidence', async () => {
    const selectedRound = {
      ...FALLBACK_CATALOG.rounds.find((round) => round.id === 'analyze_live_orders_without_slowing_checkout')!,
      availability: 'ready' as const,
    }
    const catalog = {
      ...FALLBACK_CATALOG,
      rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === selectedRound.id ? selectedRound : round),
    }
    const liveOrdersSession = (state: DemoSession['state']): DemoSession => {
      const base = session(state)
      const verified = state === 'verified'
      return {
        ...base,
        state,
        round: selectedRound,
        lanes: {
          lakebase: {
            ...base.lanes.lakebase,
            state: verified ? 'verified' : base.lanes.lakebase.state,
            elapsed_ms: verified ? 1_234 : null,
            status: verified ? 'Exact Delta answer · Separate checkout committed' : base.lanes.lakebase.status,
            activity: verified ? { phase: 'verified', wire_call: null } : null,
            evidence: verified ? {
              order_id: '00000000-0000-4000-8000-00000000009a',
              sku: 'RED-GLOVE',
              store: 'CHICAGO',
              quantity: 1,
              total_cents: 8450,
              total_display: '$84.50',
              status: 'committed',
              proof_nonce: 'round6-live-order-nonce',
              history_lsn: '0/1A2B3C4D',
              checkout_commit_ms: 41.5,
              checkout_guardrail_order_id: '00000000-0000-4000-8000-00000000009b',
              checkout_guardrail_proof_nonce: 'round6-guardrail-nonce',
              checkout_guardrail_commit_ms: 38.25,
              checkout_guardrail_read_ms: 12.75,
            } : {},
          },
          competitor: {
            ...base.lanes.competitor,
            state: verified ? 'not_supported' : base.lanes.competitor.state,
            status: verified ? 'AWS CDC pipeline not built or timed' : base.lanes.competitor.status,
            evidence: { unsupported_reason: 'Aurora/RDS require a separately configured CDC pipeline into Delta.' },
          },
        },
        metrics: verified ? [
          { spec_id: 'analytics_available_ms', lane_id: 'lakebase', value: 1_234, display_value: '1234.00 ms' },
          { spec_id: 'matching_live_orders', lane_id: 'lakebase', value: 1, display_value: '1 exact order' },
          { spec_id: 'checkout_verified', lane_id: 'lakebase', value: true, display_value: 'SEPARATE CHECKOUT COMMITTED ✓' },
        ] : [],
        comparison: verified ? {
          kind: 'capability_gap',
          winner_lane_id: 'lakebase',
          margin: null,
          detail: 'Lakebase native CDF produced the exact Delta answer; the selected AWS database requires a separately configured CDC pipeline and was not timed.',
        } : null,
      }
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(liveOrdersSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(liveOrdersSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(liveOrdersSession('verified')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'analyze_live_orders_without_slowing_checkout')
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    await user.click(await screen.findByRole('button', { name: /instant replay/i }))
    const replay = screen.getByRole('dialog', { name: /how this round was proved/i })
    expect(replay).toHaveTextContent(/instant replay · round 6/i)

    expect(replay).toHaveTextContent(/Lakebase1\.23s/)
    expect(replay).toHaveTextContent(/NOT TIMED/)
    expect(replay).toHaveTextContent(/Opponent lane/i)
    expect(replay).toHaveTextContent(/Aurora\/RDS require a separately configured CDC pipeline into Delta/i)
    expect(replay).not.toHaveTextContent(/Start gap/i)
    expect(replay).not.toHaveTextContent(/Provider-specific calls will appear/i)

    // The checkout commit is evidence, not the scored clock.
    await user.click(within(replay).getByRole('button', { name: /One checkout order was committed/i }))
    const checkoutStep = within(replay).getByLabelText(/exact calls for step 02/i)
    expect(checkoutStep).toHaveTextContent(/RED-GLOVE · CHICAGO · \$84\.50 · committed/)
    expect(checkoutStep).toHaveTextContent(/Checkout commit0\.04s/)
    expect(checkoutStep).toHaveTextContent(/round6-live-order-nonce/)

    // The scored primary metric and the exact single insert live together.
    await user.click(within(replay).getByRole('button', { name: /Delta history was then polled/i }))
    const cdfStep = within(replay).getByLabelText(/exact calls for step 03/i)
    expect(cdfStep).toHaveTextContent(/Analytics available \(primary\)1234\.00 ms/)
    expect(cdfStep).toHaveTextContent(/matching live orders = 1 exact order/)
    expect(cdfStep).toHaveTextContent(/0\/1A2B3C4D/)

    // The separate checkout is what backs the "without slowing checkout" claim.
    await user.click(within(replay).getByRole('button', { name: /separate checkout order then had to commit/i }))
    const guardrailStep = within(replay).getByLabelText(/exact calls for step 04/i)
    expect(guardrailStep).toHaveTextContent(/round6-guardrail-nonce/)
    expect(guardrailStep).toHaveTextContent(/SEPARATE CHECKOUT COMMITTED/)
    expect(guardrailStep).toHaveTextContent(/differ from the measured order in both order id and proof nonce/i)
  })

  it('shows Round 4 v2 failure separately while preserving the v1 proof', async () => {
    const catalog = { ...FALLBACK_CATALOG, rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === 'put_model_score_in_app' ? { ...round, availability: 'ready' as const } : round) }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(modelScoreSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(modelScoreSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(modelScoreSession('verified')))
      if (input.endsWith('/redo')) return Promise.resolve(jsonResponse(modelScoreSession('running', 'running')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'put_model_score_in_app')
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    await user.click(await screen.findByRole('button', { name: /^B · RE-DO$/i }))
    FakeEventSource.instances.at(-1)!.emit({
      sequence: 12, event: 'redo_failed', occurred_at: '2026-08-18T00:00:03Z',
      payload: { message: 'The v2 exact row did not verify.', session: modelScoreSession('verified', 'failed') },
    })

    expect(await screen.findByText('V2 RESULT NOT VERIFIED')).toBeInTheDocument()
    expect(screen.getByLabelText(/immutable v1 verified proof/i)).toHaveTextContent(/Previous live app state · V1 verified/i)
    expect(screen.getByText('V1 remains verified and unchanged.')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /change the score|retry/i })).not.toBeInTheDocument()
    expect(screen.getByText('No new live app update was verified.', { selector: '.round4-footer > p' })).toBeInTheDocument()
  })

  it('declares no verified result and offers no redo when Round 4 v1 fails', async () => {
    const catalog = { ...FALLBACK_CATALOG, rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === 'put_model_score_in_app' ? { ...round, availability: 'ready' as const } : round) }
    const failedSession = modelScoreSession('failed')
    expect(failedSession.comparison).toBeNull()
    expect(failedSession.remembered_result).toBeNull()
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(modelScoreSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(modelScoreSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(failedSession))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'put_model_score_in_app')
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    expect(await screen.findByText('NO RESULT VERIFIED')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /change the score|re-do/i })).not.toBeInTheDocument()
    expect(screen.queryByText(/verified outcome/i)).not.toBeInTheDocument()
    expect(screen.getByText('No new live app update was verified.', { selector: '.round4-footer > p' })).toBeInTheDocument()
  })

  it('reconciles an ambiguous redo POST with GET and enters v2 running', async () => {
    const catalog = { ...FALLBACK_CATALOG, rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === 'put_model_score_in_app' ? { ...round, availability: 'ready' as const } : round) }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(modelScoreSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(modelScoreSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(modelScoreSession('verified')))
      if (input.endsWith('/redo')) return Promise.reject(new TypeError('connection reset after send'))
      if (input === '/api/sessions/session-1') return Promise.resolve(jsonResponse(modelScoreSession('running', 'running')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'put_model_score_in_app')
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    await user.click(await screen.findByRole('button', { name: /^B · RE-DO$/i }))

    expect(await screen.findByLabelText(/lakebase v2 result/i)).toHaveTextContent('Reading the exact v2 row')
    expect(fetchMock).toHaveBeenCalledWith('/api/sessions/session-1', expect.any(Object))
  })

  it('keeps a newer terminal v2 proof when delayed GET and SSE candidates regress', async () => {
    const catalog = { ...FALLBACK_CATALOG, rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === 'put_model_score_in_app' ? { ...round, availability: 'ready' as const } : round) }
    const refresh = deferred<ReturnType<typeof jsonResponse>>()
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(modelScoreSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(modelScoreSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(modelScoreSession('verified')))
      if (input.endsWith('/redo')) return Promise.reject(new TypeError('connection reset after send'))
      if (input === '/api/sessions/session-1') return refresh.promise
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'put_model_score_in_app')
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    await user.click(await screen.findByRole('button', { name: /^B · RE-DO$/i }))
    await waitFor(() => expect(fetchMock.mock.calls.some((call) => call[0] === '/api/sessions/session-1')).toBe(true))

    const terminal = { ...modelScoreSession('verified', 'verified'), updated_at: '2026-08-18T00:00:03Z' }
    FakeEventSource.instances.at(-1)!.emit({
      sequence: 12, event: 'redo_finished', occurred_at: terminal.updated_at,
      payload: { session: terminal },
    })
    expect(await screen.findByText(/live app updated again.*risk-v1.*risk-v2/i)).toBeInTheDocument()

    refresh.resolve(jsonResponse(modelScoreSession('verified')))
    await waitFor(() => expect(screen.getByText('V2 VERIFIED', { selector: '.round4-live-region' })).toBeInTheDocument())
    expect(screen.getByLabelText(/lakebase v2 result/i)).toHaveAttribute('data-verified', 'true')
    expect(screen.getByText('V2 VERIFIED', { selector: '.round4-live-region' })).not.toHaveTextContent(/v1 proof is unchanged|stale refresh/i)

    const staleRedoStarted = {
      ...modelScoreSession('running', 'running'),
      updated_at: '2026-08-18T00:00:02Z',
    }
    FakeEventSource.instances.at(-1)!.emit({
      sequence: 13,
      event: 'redo_started',
      occurred_at: staleRedoStarted.updated_at,
      payload: { session: staleRedoStarted },
    })
    expect(screen.getByText('V2 VERIFIED', { selector: '.round4-live-region' })).toBeInTheDocument()
    expect(screen.getByLabelText(/lakebase v2 result/i)).toHaveAttribute('data-verified', 'true')
    expect(screen.queryByText('V2 SYNC RUNNING')).not.toBeInTheDocument()
  })

  it('coalesces SSE error refreshes, keeps EventSource open, and recovers a missed terminal event', async () => {
    const refresh = deferred<ReturnType<typeof jsonResponse>>()
    const running = session('running')
    const terminal: DemoSession = {
      ...session('verified'),
      updated_at: '2026-08-17T00:00:03Z',
      lanes: {
        lakebase: { ...running.lanes.lakebase, state: 'verified', elapsed_ms: 900, status: 'Transaction verified' },
        competitor: { ...running.lanes.competitor, state: 'verified', elapsed_ms: 1200, status: 'Transaction verified' },
      },
      remembered_result: 'LAKEBASE WINS BY 0.30s',
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      if (input === '/api/sessions/session-1') return refresh.promise
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    const source = FakeEventSource.instances.at(-1)!
    source.onerror?.()
    source.onerror?.()
    expect(await screen.findByRole('alert')).toHaveTextContent(/live evidence stream interrupted/i)
    await waitFor(() => expect(fetchMock.mock.calls.filter((call) => call[0] === '/api/sessions/session-1')).toHaveLength(1))
    expect(source.close).not.toHaveBeenCalled()

    refresh.resolve(jsonResponse(terminal))
    expect(await screen.findByText('LAKEBASE WINS BY 0.30s')).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByText(/live evidence stream interrupted/i)).not.toBeInTheDocument())
    expect(source.close).not.toHaveBeenCalled()
  })

  it('freezes Round 2 clocks while the live proof is offline, then resumes from the stream and accepts the terminal receipt', async () => {
    const running = safeChangeSession('running')
    const verified: DemoSession = {
      ...safeChangeSession('verified'),
      updated_at: '2026-08-17T00:00:10Z',
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(safeChangeSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(safeChangeSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      if (input === '/api/sessions/session-1') return Promise.reject(new TypeError('backend offline'))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /stacktrace jack/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    const source = FakeEventSource.instances.at(-1)!
    source.open()
    const lakebaseClock = (await screen.findByLabelText('Lakebase result')).querySelector('.lane-time')!
    const competitorClock = screen.getByLabelText('Aurora Serverless v2 result').querySelector('.lane-time')!
    await waitFor(() => expect(lakebaseClock).not.toHaveTextContent('0.00s'))

    source.onerror?.()

    expect(await screen.findByRole('alert')).toHaveTextContent(/reconnect/i)
    expect(screen.getByText(/proof paused.*reconnecting/i)).toBeInTheDocument()
    expect(lakebaseClock.closest('.proof-screen')).toHaveAttribute('data-session-state', 'offline')
    await waitFor(() => {
      expect(lakebaseClock).toHaveAttribute('data-live', 'false')
      expect(competitorClock).toHaveAttribute('data-live', 'false')
    })
    await waitFor(() => expect(fetchMock.mock.calls.some(
      (call) => call[0] === '/api/sessions/session-1',
    )).toBe(true))
    const frozenLakebase = lakebaseClock.textContent
    const frozenCompetitor = competitorClock.textContent
    await new Promise((resolve) => window.setTimeout(resolve, 120))
    expect(lakebaseClock).toHaveTextContent(frozenLakebase!)
    expect(competitorClock).toHaveTextContent(frozenCompetitor!)

    source.open()

    await waitFor(() => {
      expect(lakebaseClock).toHaveAttribute('data-live', 'true')
      expect(competitorClock).toHaveAttribute('data-live', 'true')
      expect(lakebaseClock.closest('.proof-screen')).toHaveAttribute('data-session-state', 'running')
      expect(screen.queryByText(/live evidence stream interrupted/i)).not.toBeInTheDocument()
    })
    await waitFor(() => expect(lakebaseClock.textContent).not.toBe(frozenLakebase))

    source.emit({
      sequence: 9,
      event: 'run_finished',
      occurred_at: verified.updated_at,
      payload: { state: 'verified', session: verified },
    })

    await waitFor(() => expect(lakebaseClock).toHaveTextContent('1.80s'))
    expect(competitorClock).toHaveTextContent('9.80s')
    expect(screen.getByText(/lakebase.*8\.00 seconds sooner/i)).toBeInTheDocument()
  })

  it('clears a stale live snapshot when authoritative session lookup returns 404', async () => {
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(session('running')))
      if (input === '/api/sessions/session-1') {
        return Promise.resolve({
          ok: false,
          status: 404,
          statusText: 'Not Found',
          json: async () => ({ detail: 'Session not found' }),
        })
      }
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    FakeEventSource.instances.at(-1)!.onerror?.()

    expect(await screen.findByRole('button', { name: /prepare fight card/i })).toBeInTheDocument()
    expect(screen.queryByText(/live evidence stream interrupted/i)).not.toBeInTheDocument()
    expect(audioMocks.stopOriginalRoundTheme).toHaveBeenCalled()
  })

  it('does not let an older successful redo POST reset SSE progress or overwrite its terminal result', async () => {
    const catalog = {
      ...FALLBACK_CATALOG,
      rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === 'put_model_score_in_app'
        ? { ...round, availability: 'ready' as const }
        : round),
    }
    const redoResponse = deferred<ReturnType<typeof jsonResponse>>()
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(modelScoreSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(modelScoreSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(modelScoreSession('verified')))
      if (input.endsWith('/redo')) return redoResponse.promise
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, 'put_model_score_in_app')
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    await user.click(await screen.findByRole('button', { name: /^B · RE-DO$/i }))
    await waitFor(() => expect(fetchMock.mock.calls.some((call) => String(call[0]).endsWith('/redo'))).toBe(true))

    const source = FakeEventSource.instances.at(-1)!
    const started = modelScoreSession('running', 'running')
    source.emit({
      sequence: 10,
      event: 'redo_started',
      occurred_at: started.updated_at,
      payload: { session: started },
    })
    const lakebaseV2Result = await screen.findByLabelText(/lakebase v2 result/i)
    const displayedSeconds = () => Number(
      lakebaseV2Result.querySelector('.timer-readout')?.textContent?.replace('s', ''),
    )
    await waitFor(() => expect(lakebaseV2Result).toHaveTextContent('0.00s'))
    await act(async () => {
      await new Promise((resolve) => window.setTimeout(resolve, 80))
    })
    expect(lakebaseV2Result).toHaveTextContent('0.00s')
    source.emit({
      sequence: 11,
      event: 'redo_lane_update',
      occurred_at: '2026-08-18T00:00:02Z',
      payload: { lane_id: 'lakebase', state: 'verifying', elapsed_ms: 400, status: 'Reading exact v2 row' },
    })
    await waitFor(() => expect(lakebaseV2Result).toHaveTextContent('0.40s'))
    await act(async () => {
      await new Promise((resolve) => window.setTimeout(resolve, 80))
    })
    expect(displayedSeconds()).toBeGreaterThan(0.40)
    expect(displayedSeconds()).toBeLessThan(0.72)
    source.emit({
      sequence: 12,
      event: 'redo_lane_update',
      occurred_at: '2026-08-18T00:00:02.720Z',
      payload: { lane_id: 'lakebase', state: 'verified', elapsed_ms: 720, status: 'Exact v2 application row verified' },
    })
    await waitFor(() => expect(lakebaseV2Result).toHaveTextContent('0.72s'))
    await act(async () => {
      await new Promise((resolve) => window.setTimeout(resolve, 80))
    })
    expect(lakebaseV2Result).toHaveTextContent('0.72s')
    await act(async () => {
      redoResponse.resolve(jsonResponse(started))
      await redoResponse.promise
    })
    expect(lakebaseV2Result).toHaveTextContent('0.72s')

    const terminal = {
      ...modelScoreSession('verified', 'verified'),
      updated_at: '2026-08-18T00:00:03Z',
    }
    source.emit({
      sequence: 13,
      event: 'redo_finished',
      occurred_at: terminal.updated_at,
      payload: { session: terminal },
    })
    expect(await screen.findByText(/live app updated again.*risk-v1.*risk-v2/i)).toBeInTheDocument()

    await waitFor(() => expect(screen.getByText(/live app updated again.*risk-v1.*risk-v2/i)).toBeInTheDocument())
    const terminalV2Result = screen.getByLabelText(/lakebase v2 result/i)
    expect(terminalV2Result).toHaveTextContent('0.72s')
    expect(terminalV2Result).toHaveAttribute('data-verified', 'true')
    expect(screen.queryByText('V2 SYNC RUNNING')).not.toBeInTheDocument()
  })

  it.each([
    'survive_connection_spike',
    'analyze_live_orders_without_slowing_checkout',
  ] as const)('never renders or executes generic redo for %s', async (roundId) => {
    const selectedRound = {
      ...FALLBACK_CATALOG.rounds.find((round) => round.id === roundId)!,
      availability: 'ready' as const,
      competitors: ['aurora_serverless_v2', 'rds_postgres'] as DemoSession['round']['competitors'],
    }
    const catalog = {
      ...FALLBACK_CATALOG,
      rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === roundId ? selectedRound : round),
    }
    const withRound = (state: DemoSession['state']): DemoSession => {
      const base = session(state)
      const roundSix = roundId === 'analyze_live_orders_without_slowing_checkout'
      const verified = state === 'verified'
      return {
        ...base,
        round: selectedRound,
        lanes: roundSix ? {
          lakebase: {
            ...base.lanes.lakebase,
            state: verified ? 'verified' : base.lanes.lakebase.state,
            elapsed_ms: verified ? 1_234 : null,
            status: verified ? 'Exact Delta answer · Separate checkout committed' : base.lanes.lakebase.status,
            activity: verified ? { phase: 'verified', wire_call: null } : null,
          },
          competitor: {
            ...base.lanes.competitor,
            state: verified ? 'not_supported' : base.lanes.competitor.state,
            status: verified ? 'Separate CDC stack required · not built or timed' : base.lanes.competitor.status,
          },
        } : base.lanes,
        metrics: roundSix && verified ? [
          { spec_id: 'analytics_available_ms', lane_id: 'lakebase', value: 1_234, display_value: '1234.00 ms' },
          { spec_id: 'matching_live_orders', lane_id: 'lakebase', value: 1, display_value: '1 exact order' },
          { spec_id: 'checkout_verified', lane_id: 'lakebase', value: true, display_value: 'SEPARATE CHECKOUT COMMITTED ✓' },
        ] : undefined,
        comparison: roundSix && verified ? {
          kind: 'capability_gap',
          winner_lane_id: 'lakebase',
          margin: null,
          detail: 'Lakebase native CDF produced the exact Delta answer; the AWS database requires a separate CDC stack.',
        } : null,
        remembered_result: verified ? 'ROUND VERIFIED' : null,
      }
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/receipts') return Promise.resolve(jsonResponse({ receipts: ledgerReceipts() }))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(withRound('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(withRound('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(withRound('verified')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, roundId)
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    const expectedHeading = roundId === 'analyze_live_orders_without_slowing_checkout'
      ? 'Move live application data into the lakehouse'
      : selectedRound.title
    expect(await screen.findByRole('heading', { name: expectedHeading })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /re-do round/i })).not.toBeInTheDocument()
    expect(fetchMock.mock.calls.some((call) => String(call[0]).endsWith('/cooldown'))).toBe(false)
    if (roundId === 'analyze_live_orders_without_slowing_checkout') {
      expect(screen.getByLabelText('Round 6 live analytical proof')).toHaveTextContent(
        /1 × RED-GLOVE.*1\.23s.*1 ORDER.*SEPARATE CHECKOUT COMMITTED.*ORDER INCLUDED/i,
      )
      const commentator = screen.getByLabelText(/ringside commentator/i)
      expect(within(commentator).getByRole('img', { name: 'Ryan' })).toBeInTheDocument()
      expect(commentator).toHaveTextContent(
        /Exact Delta answer verified in 1\.23s.*Separate checkout committed.*Public Preview freshness proof only/i,
      )
      await user.click(within(commentator).getByRole('button', { name: /hide play-by-play/i }))
      expect(within(commentator).getByText(/play-by-play hidden/i)).toBeInTheDocument()
      expect(within(commentator).queryByRole('img', { name: 'Ryan' })).not.toBeInTheDocument()
      await user.click(within(commentator).getByRole('button', { name: /show commentator/i }))
      expect(within(commentator).getByRole('img', { name: 'Ryan' })).toBeInTheDocument()
      expect(screen.queryByText(/Server events only · No ETA/i)).not.toBeInTheDocument()
      expect(screen.getByRole('button', { name: /explain to the room/i })).toBeInTheDocument()
      expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
      const finalRecap = screen.getByRole('button', { name: /next.*final recap/i })
      expect(finalRecap).toBeInTheDocument()

      await user.click(screen.getByRole('button', { name: /explain to the room/i }))
      expect(screen.getByRole('dialog', { name: /make the result matter/i })).toHaveTextContent(
        /exact Delta answer.*separate checkout.*no throughput or p99 impact claim/i,
      )
      await user.click(within(screen.getByRole('dialog', { name: /make the result matter/i })).getByRole('button', { name: /back to the ring/i }))
      stubReceiptCanvas()
      await user.click(finalRecap)
      const finale = screen.getByLabelText(/six-round final recap/i)
      expect(finale).toHaveTextContent(/six rounds/i)
      const rows = within(finale).getByLabelText(/all six round summaries/i)
      expect(rows.children).toHaveLength(6)
      expect(finale).toHaveTextContent(/get spike-ready.*existing proxy starts ready/i)

      // The fight card's own fighters, named once with their own chips.
      const corners = within(finale).getByLabelText('Corners')
      expect(corners).toHaveTextContent(/LB.*LAKEBASE/)
      expect(corners).toHaveTextContent(/AUR.*Aurora Serverless v2/)

      /* The headline requirement: every round says who took it, and each of the
         five outcomes is worded for what actually happened rather than being
         flattened into one. */
      await waitFor(() => expect(rows.children[0]).toHaveTextContent(/LB.*LAKEBASE.*2\.32s/))
      const [clean, stopped, abandoned, uncontested, unrun, live] = Array.from(rows.children)
      expect(clean).toHaveAttribute('data-status', 'lakebase_faster')

      // A stopped round keeps both figures, dates itself, and refuses a margin.
      expect(stopped).toHaveAttribute('data-status', 'lakebase_finished')
      expect(stopped).toHaveTextContent(/LAKEBASE.*14\.24s.*STOPPED SHORT/)
      // Their figure is a floor, printed m:ss because a minute-and-a-half lane
      // is unreadable as 93.99s.
      expect(stopped).toHaveTextContent(
        /AURORA SERVERLESS V2 · UNVERIFIED WHEN STOPPED · LOWER BOUND 1:33 · MARGIN N\/A/,
      )
      // A result sealed on an earlier day says so, or the ledger reads as one
      // sitting.
      expect(stopped).toHaveTextContent(/\d+ [A-Z]{3}$/)

      // Attempted and stopped before either lane measured anything. Not "never
      // run", which is what this used to render as.
      expect(abandoned).toHaveAttribute('data-status', 'abandoned')
      expect(abandoned).toHaveTextContent(/NO RESULT DECLARED.*ABANDONED/)

      /* No opponent lane at all: a real Lakebase figure, a winner, and the
         absence attributed to the blue corner in the fight card's own words.
         No margin and no opponent name, because neither exists. */
      expect(uncontested).toHaveAttribute('data-status', 'uncontested')
      expect(uncontested).toHaveTextContent(/LB.*LAKEBASE.*8\.63s.*UNCONTESTED/)
      expect(uncontested).toHaveTextContent(/BLUE CORNER · NO EQUIVALENT NATIVE PATH/)
      expect(uncontested).not.toHaveTextContent(/MARGIN/)

      expect(unrun).toHaveAttribute('data-status', 'unrun')
      expect(unrun).toHaveTextContent(/NOT RUN YET/)

      // The round just run, with the figure this room watched being produced.
      expect(live).toHaveAttribute('data-status', 'uncontested')
      expect(live).toHaveAttribute('data-latest', 'true')
      expect(live).toHaveTextContent(/LAKEBASE.*1\.23s.*UNCONTESTED/)

      // The claims strip is gone; the ledger shows results and nothing else.
      expect(finale).not.toHaveTextContent(/proof contracts name exact stop gates/i)
      expect(finale).not.toHaveTextContent(/not a benchmark/i)
      const shareFullCard = await within(finale).findByRole('button', { name: /share the full card/i })
      const writeText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } })
      Object.defineProperty(URL, 'createObjectURL', { configurable: true, value: vi.fn(() => 'blob:finale') })
      Object.defineProperty(URL, 'revokeObjectURL', { configurable: true, value: vi.fn() })
      const downloadClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined)
      const openLinkedIn = vi.spyOn(window, 'open').mockReturnValue(null)
      await user.click(shareFullCard)
      expect(openLinkedIn).toHaveBeenCalledWith(
        'https://www.linkedin.com/feed/?shareActive=true',
        '_blank',
        'noopener,noreferrer',
      )
      expect(downloadClick).toHaveBeenCalledTimes(1)
      expect(writeText).toHaveBeenCalledWith(expect.stringMatching(
        /Six proof contracts.*01 · Wake from zero.*05 · Built-in pooling.*06 · Live checkout.*1\.23s.*not a benchmark/is,
      ))
      expect(finale).toHaveTextContent(/ready.*downloaded png.*copied caption/i)
      expect(fetchMock.mock.calls.some((call) => String(call[0]).endsWith('/cooldown'))).toBe(false)
    }
  })

  it('reopens Ryan play-by-play when a running Round 6 reaches its terminal result', async () => {
    const selectedRound = {
      ...FALLBACK_CATALOG.rounds.find((round) => round.id === 'analyze_live_orders_without_slowing_checkout')!,
      availability: 'ready' as const,
    }
    const catalog = {
      ...FALLBACK_CATALOG,
      rounds: FALLBACK_CATALOG.rounds.map((round) => round.id === selectedRound.id ? selectedRound : round),
    }
    const withState = (state: DemoSession['state']): DemoSession => {
      const base = session(state)
      const verified = state === 'verified'
      return {
        ...base,
        state,
        round: selectedRound,
        lanes: {
          lakebase: {
            ...base.lanes.lakebase,
            state: verified ? 'verified' : state === 'running' ? 'verifying' : base.lanes.lakebase.state,
            elapsed_ms: verified ? 1_234 : state === 'running' ? 700 : null,
            status: verified ? 'Exact Delta answer · Separate checkout committed' : 'Waiting for exact Delta answer',
            activity: { phase: verified ? 'verified' : 'waiting_cdf', wire_call: null },
          },
          competitor: {
            ...base.lanes.competitor,
            state: verified ? 'not_supported' : base.lanes.competitor.state,
            status: verified ? 'Separate CDC stack required · not built or timed' : base.lanes.competitor.status,
          },
        },
        metrics: verified ? [
          { spec_id: 'analytics_available_ms', lane_id: 'lakebase', value: 1_234, display_value: '1234.00 ms' },
          { spec_id: 'matching_live_orders', lane_id: 'lakebase', value: 1, display_value: '1 exact order' },
          { spec_id: 'checkout_verified', lane_id: 'lakebase', value: true, display_value: 'SEPARATE CHECKOUT COMMITTED ✓' },
        ] : undefined,
        remembered_result: verified ? 'LAKEBASE NATIVE CDF WIN · AWS PIPELINE NOT BUILT · MARGIN N/A' : null,
      }
    }
    const running = withState('running')
    const verified = withState('verified')
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(catalog))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(withState('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(withState('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await pickRound(user, selectedRound.id)
    await user.click(screen.getByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    await user.click(await screen.findByRole('button', { name: /hide play-by-play/i }))
    expect(screen.getByText(/play-by-play hidden/i)).toBeInTheDocument()
    const source = FakeEventSource.instances.at(-1)!
    source.emit({
      sequence: 9,
      event: 'run_finished',
      occurred_at: verified.updated_at,
      payload: { state: 'verified', session: verified },
    })

    expect(await screen.findByRole('button', { name: /hide play-by-play/i })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('img', { name: 'Ryan' })).toBeInTheDocument()
    expect(screen.getByLabelText(/ringside commentator/i)).toHaveTextContent(/exact Delta answer verified in 1\.23s/i)
  })

  it('keeps shared evidence for the selected priorities in the overlay and out of the ringside strip', async () => {
    const executive = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'executive')!
    const verified: DemoSession = {
      ...safeChangeSession('verified'),
      corners: ['cost', 'simplicity', 'performance'],
      secondary_personas: [executive],
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(safeChangeSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(safeChangeSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(verified))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /stacktrace jack/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    const ringside = await screen.findByLabelText(/what the result means at ringside/i)
    const optionalRedo = screen.getByRole('button', { name: /re-do round/i })
    expect(optionalRedo.parentElement).not.toHaveTextContent('★ proves a different product behavior')
    // The strip is a roster now. It names who is at ringside and which
    // priorities are selected, and carries no evidence sentences at all.
    expect(within(ringside).queryByLabelText(/selected priority evidence/i)).not.toBeInTheDocument()
    expect(ringside).toHaveTextContent(/selected · cost \+ simplicity \+ performance/i)
    expect(ringside).not.toHaveTextContent(/developer wait measured/i)
    expect(ringside).not.toHaveTextContent(/orchestrator ran create → migrate/i)
    within(ringside).getAllByRole('article').forEach((card) => {
      expect(card.querySelector('img')).not.toBeNull()
      expect(card.querySelector('p')).toBeNull()
    })

    // Every one of those sentences is still one click away, still scoped to the
    // selected priorities and still unduplicated.
    await user.click(screen.getByRole('button', { name: /explain to the room/i }))
    const take = await screen.findByRole('dialog', { name: /make the result matter/i })
    const evidence = within(take).getByText(/shared exact proof/i).parentElement!
    expect(evidence).toHaveTextContent(/cost.*developer wait measured; storage and compute dollars not calculated/i)
    expect(evidence).toHaveTextContent(/simplicity.*orchestrator ran create → migrate → transaction → final source check; no manual timed step/i)
    expect(evidence).toHaveTextContent(/performance.*lakebase 1\.80s; aurora serverless v2 9\.80s to migration \+ transaction verify \+ final source check/i)
    expect(evidence).not.toHaveTextContent(/8\.00s sooner/i)
  })

  it('shows a failed lane error immediately while the other lane is still running', async () => {
    const running = safeChangeSession('running')
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(safeChangeSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(safeChangeSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /stacktrace jack/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    FakeEventSource.instances.at(-1)?.emit({
      sequence: 8,
      event: 'lane_update',
      occurred_at: '2026-08-17T00:00:06Z',
      payload: {
        lane_id: 'lakebase',
        state: 'failed',
        attempts: 1,
        elapsed_ms: 6651.7,
        status: 'The isolated schema change could not be verified',
        error: 'Lakebase isolated branch has no verified read-write endpoint',
      },
    })

    const lakebaseLane = await screen.findByLabelText('Lakebase result')
    expect(within(lakebaseLane).getByRole('alert')).toHaveTextContent(
      'Lakebase isolated branch has no verified read-write endpoint',
    )
    expect(screen.getByLabelText('Aurora Serverless v2 result')).toHaveAttribute('data-state', 'connecting')
    expect(screen.queryByText(/no winner declared/i)).not.toBeInTheDocument()
  })

  it('starts both re-do clocks at zero and freezes each at confirmed idle', async () => {
    const startedAt = new Date().toISOString()
    const transactionWireCall = 'PostgreSQL TLS connect → INSERT → COMMIT → SELECT'
    const running: DemoSession = {
      ...session('running'),
      lanes: {
        lakebase: { ...session('running').lanes.lakebase, activity: { phase: 'connecting', wire_call: transactionWireCall } },
        competitor: { ...session('running').lanes.competitor, activity: { phase: 'connecting', wire_call: transactionWireCall } },
      },
    }
    const verified: DemoSession = {
      ...session('verified'),
      remembered_result: 'LAKEBASE — 11.18 SECONDS SOONER',
    }
    const cooldownStarted: DemoSession = {
      ...verified,
      cooldown: {
        mode: 'return_to_idle',
        state: 'watching',
        started_at: startedAt,
        failure: null,
        lanes: {
          lakebase: {
            id: 'lakebase', name: 'Lakebase', state: 'watching',
            started_at: startedAt, confirmed_at: null,
            elapsed_ms: null, status: 'Lakebase endpoint is ACTIVE, not IDLE',
            activity: { phase: 'watching', wire_call: 'databricks postgres get-endpoint' },
          },
          competitor: {
            id: 'competitor', name: 'Aurora Serverless v2', state: 'watching',
            started_at: startedAt, confirmed_at: null,
            elapsed_ms: null, status: 'Aurora writer has not produced two consecutive zero-capacity samples',
            activity: { phase: 'watching', wire_call: 'RDS DescribeDBClusters + DescribeDBInstances + DescribeEvents → CloudWatch GetMetricStatistics fallback' },
          },
        },
      },
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      if (input.endsWith('/cooldown')) return Promise.resolve(jsonResponse(cooldownStarted))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    expect(await screen.findByText(/connection proof in progress · no verdict/i)).toBeInTheDocument()
    expect(screen.getByText(/ON THE WIRE/)).toHaveTextContent(transactionWireCall)
    const commentaryToggle = screen.getByRole('button', { name: /hide play-by-play/i })
    expect(commentaryToggle).toHaveAttribute('aria-pressed', 'true')
    await user.click(commentaryToggle)
    expect(screen.queryByText(/connection proof in progress · no verdict/i)).not.toBeInTheDocument()
    const hiddenCommentator = screen.getByLabelText(/ringside commentator/i)
    expect(within(hiddenCommentator).getByText(/play-by-play hidden/i)).toBeInTheDocument()
    expect(within(hiddenCommentator).queryByRole('img', { name: 'Ryan' })).not.toBeInTheDocument()
    expect(hiddenCommentator.querySelector('header')).toBeNull()
    const showCommentator = within(hiddenCommentator).getByRole('button', { name: /show commentator/i })
    expect(showCommentator).toHaveAttribute('aria-pressed', 'false')
    expect(showCommentator).toHaveAttribute('aria-controls', 'ringside-commentator-status')
    const source = FakeEventSource.instances.at(-1)!
    source.emit({
      sequence: 9,
      event: 'run_finished',
      occurred_at: '2026-08-17T00:00:13Z',
      payload: { state: 'verified', session: cooldownStarted },
    })

    const liveIdleClock = await screen.findByRole('button', { name: /re-do round/i })
    expect(liveIdleClock).toHaveTextContent(/back to idle.*live/i)
    await user.click(liveIdleClock)
    expect(screen.getByText(/play-by-play hidden/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /show commentator/i })).toHaveAttribute('aria-pressed', 'false')
    await user.click(screen.getByRole('button', { name: /show commentator/i }))
    expect(screen.getByText(/waiting for authoritative zero-state evidence/i)).toBeInTheDocument()
    expect(screen.getByText(/ON THE WIRE/)).toHaveTextContent('databricks postgres get-endpoint')
    expect(screen.getByText(/ON THE WIRE/)).toHaveTextContent('RDS DescribeDBClusters + DescribeDBInstances + DescribeEvents')
    expect(screen.getByRole('heading', { name: /back to idle/i })).toBeInTheDocument()
    expect(screen.getByText(/clocks auto-started at 0:00 when the round ended/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/lakebase return to idle/i)).toHaveTextContent('0:00')
    expect(screen.getByLabelText(/aurora serverless v2 return to idle/i)).toHaveTextContent('0:00')
    expect(screen.getByRole('button', { name: /waiting for idle/i })).toBeDisabled()
    expect(screen.queryByRole('button', { name: /share idle proof/i })).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /more details/i }))
    const details = await screen.findByRole('dialog', { name: /how the clocks stop/i })
    expect(details).toHaveTextContent(/databricks postgres get-endpoint/i)
    expect(details).toHaveTextContent(/status\.current_state/i)
    expect(details).toHaveTextContent(/rds describedbclusters \+ describeevents/i)
    expect(details).toHaveTextContent(/cloudwatch getmetricstatistics/i)
    expect(details).toHaveTextContent(/no wake effect.*watcher never opens sql/i)
    await user.click(within(details).getByRole('button', { name: /back to the clocks/i }))
    await user.click(screen.getByRole('button', { name: /hide play-by-play/i }))
    expect(screen.getByText(/play-by-play hidden/i)).toBeInTheDocument()

    source.emit({
      sequence: 10,
      event: 'cooldown_ready',
      occurred_at: '2026-08-17T00:06:53Z',
      payload: {
        cooldown: {
          ...cooldownStarted.cooldown!,
          state: 'ready',
          lanes: {
            lakebase: {
              ...cooldownStarted.cooldown!.lanes.lakebase,
              state: 'confirmed_zero', confirmed_at: '2026-08-17T00:05:02Z',
              elapsed_ms: 301000, status: 'Control plane confirmed zero',
            },
            competitor: {
              ...cooldownStarted.cooldown!.lanes.competitor,
              state: 'confirmed_zero', confirmed_at: '2026-08-17T00:06:53Z',
              elapsed_ms: 401000, status: 'Control plane confirmed zero',
            },
          },
        },
      },
    })

    expect(await screen.findByLabelText(/lakebase return to idle/i)).toHaveTextContent('5:01')
    expect(screen.getByLabelText(/aurora serverless v2 return to idle/i)).toHaveTextContent('6:41')
    expect(screen.getByRole('button', { name: /ring again/i })).toBeEnabled()

    stubReceiptCanvas()
    const shareIdle = screen.getByRole('button', { name: /share idle proof/i })
    await user.click(shareIdle)
    const receipt = await screen.findByRole('dialog', { name: /share idle proof/i })
    const poster = within(receipt).getByLabelText(/verified back to idle poster preview/i)
    expect(within(poster).getByLabelText(/lakebase receipt result/i)).toHaveTextContent('5:01')
    expect(within(poster).getByLabelText(/aurora serverless v2 receipt result/i)).toHaveTextContent('6:41')
    expect(poster).toHaveTextContent(/lakebase returned to zero 1m 40s sooner/i)
    expect(poster).toHaveTextContent(/one live run.*not a benchmark/i)

    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText },
    })
    Object.defineProperty(URL, 'createObjectURL', { configurable: true, value: vi.fn(() => 'blob:receipt') })
    Object.defineProperty(URL, 'revokeObjectURL', { configurable: true, value: vi.fn() })
    const downloadClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined)
    const openLinkedIn = vi.spyOn(window, 'open').mockReturnValue(null)
    await user.click(within(receipt).getByRole('button', { name: /copy caption/i }))
    expect(writeText).toHaveBeenCalledWith(expect.stringMatching(
      /Lakebase returned to zero 1m 40s before Aurora Serverless v2\. Same reset bell\. Two real control planes\.[\s\S]*Lakebase · 5m 01s[\s\S]*Aurora Serverless v2 · 6m 41s[\s\S]*Don't trust this post\. Ring the bell yourself\.[\s\S]*One live run, not a benchmark\./,
    ))
    const cardButton = await within(receipt).findByRole('button', { name: /prepare linkedin post/i })
    await waitFor(() => expect(cardButton).toBeEnabled())
    await user.click(cardButton)
    expect(openLinkedIn).toHaveBeenCalledWith(
      'https://www.linkedin.com/feed/?shareActive=true',
      '_blank',
      'noopener,noreferrer',
    )
    expect(downloadClick).toHaveBeenCalledTimes(1)
    expect(writeText).toHaveBeenCalledTimes(2)
    expect(receipt).toHaveTextContent(/ready.*add media.*downloaded png.*paste the caption/i)
    await user.click(within(receipt).getByRole('button', { name: /^b · back$/i }))

    await user.click(screen.getByRole('button', { name: /ring again/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    expect(await screen.findByRole('button', { name: /hide play-by-play/i })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('img', { name: 'Ryan' })).toBeInTheDocument()
  })

  it('hands a provisional re-do clock back to exact provider time without hiding excluded polling lag', async () => {
    vi.useFakeTimers()
    const startedAt = '2026-08-20T12:00:00.000Z'
    vi.setSystemTime(new Date(startedAt))
    const verified: DemoSession = {
      ...session('verified'),
      updated_at: '2026-08-20T11:59:59.000Z',
      lanes: {
        lakebase: {
          ...session('verified').lanes.lakebase,
          state: 'verified', elapsed_ms: 842.6, status: 'Transaction verified',
        },
        competitor: {
          ...session('verified').lanes.competitor,
          state: 'verified', elapsed_ms: 1288.3, status: 'Transaction verified',
        },
      },
      remembered_result: 'LAKEBASE WINS BY 0.45s',
    }
    const watching: DemoSession = {
      ...verified,
      cooldown: {
        mode: 'return_to_idle', state: 'watching', started_at: startedAt, failure: null,
        lanes: {
          lakebase: {
            id: 'lakebase', name: 'Lakebase', state: 'watching', started_at: startedAt,
            confirmed_at: null, elapsed_ms: null, status: 'Watching for confirmed zero',
          },
          competitor: {
            id: 'competitor', name: 'Aurora Serverless v2', state: 'watching', started_at: startedAt,
            confirmed_at: null, elapsed_ms: null, status: 'Watching for confirmed zero',
          },
        },
      },
    }
    window.sessionStorage.setItem('lakebase-anti-demo:active-session:v1', JSON.stringify({
      id: watching.id,
      stage: 'between',
      resumeStage: 'between',
    }))
    vi.stubGlobal('fetch', vi.fn().mockImplementation((input: string) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === `/api/sessions/${watching.id}`) return Promise.resolve(jsonResponse(watching))
      throw new Error(`Unexpected request: ${input}`)
    }))
    vi.stubGlobal('EventSource', FakeEventSource)
    render(<App />)

    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(screen.getByRole('heading', { name: /back to idle/i })).toBeInTheDocument()

    await act(async () => { await vi.advanceTimersByTimeAsync(67_000) })
    const provisionalLane = screen.getByLabelText(/lakebase return to idle/i)
    expect(within(provisionalLane).getByText('~1:07')).toBeInTheDocument()
    expect(provisionalLane).toHaveTextContent(/provisional wall time.*awaiting provider timestamp/i)

    const readyCooldown = {
      ...watching.cooldown!,
      state: 'ready' as const,
      lanes: {
        lakebase: {
          ...watching.cooldown!.lanes.lakebase,
          state: 'confirmed_zero' as const,
          confirmed_at: '2026-08-20T12:01:00.000Z',
          elapsed_ms: 60_000,
          status: 'Control plane confirmed zero',
        },
        competitor: {
          ...watching.cooldown!.lanes.competitor,
          state: 'confirmed_zero' as const,
          confirmed_at: '2026-08-20T12:01:05.000Z',
          elapsed_ms: 65_000,
          status: 'Control plane confirmed zero',
        },
      },
    }
    const source = FakeEventSource.instances.at(-1)!
    act(() => source.emit({
      sequence: 10,
      event: 'cooldown_ready',
      occurred_at: '2026-08-20T12:01:07.000Z',
      payload: { cooldown: readyCooldown },
    }))

    const authoritativeLane = screen.getByLabelText(/lakebase return to idle/i)
    expect(within(authoritativeLane).getByText('1:00')).toBeInTheDocument()
    expect(authoritativeLane).toHaveTextContent(/~0:07 polling\/delivery lag excluded/i)
    expect(authoritativeLane).toHaveTextContent(/idle confirmed.*provider transition time.*clock stopped/i)

    stubReceiptCanvas()
    fireEvent.click(screen.getByRole('button', { name: /share idle proof/i }))
    const receipt = screen.getByRole('dialog', { name: /share idle proof/i })
    const poster = within(receipt).getByLabelText(/verified back to idle poster preview/i)
    expect(within(poster).getByLabelText(/lakebase receipt result/i)).toHaveTextContent('1:00')
    expect(poster).not.toHaveTextContent(/polling\/delivery lag/i)
  })

  it('rejects a stale SSE refresh after cooldown completion and preserves frozen reset proof', async () => {
    const refresh = deferred<ReturnType<typeof jsonResponse>>()
    const running = session('running')
    const verified: DemoSession = {
      ...session('verified'),
      updated_at: '2026-08-17T00:00:01Z',
      lanes: {
        lakebase: { ...running.lanes.lakebase, state: 'verified', elapsed_ms: 900, status: 'Transaction verified' },
        competitor: { ...running.lanes.competitor, state: 'verified', elapsed_ms: 1200, status: 'Transaction verified' },
      },
      remembered_result: 'LAKEBASE WINS BY 0.30s',
    }
    const startedAt = '2026-08-17T00:00:10Z'
    const watching: DemoSession = {
      ...verified,
      cooldown: {
        mode: 'return_to_idle', state: 'watching', started_at: startedAt, failure: null,
        lanes: {
          lakebase: { id: 'lakebase', name: 'Lakebase', state: 'watching', started_at: startedAt, confirmed_at: null, elapsed_ms: null, status: 'Watching' },
          competitor: { id: 'competitor', name: 'Aurora Serverless v2', state: 'watching', started_at: startedAt, confirmed_at: null, elapsed_ms: null, status: 'Watching' },
        },
      },
    }
    const readyCooldown = {
      ...watching.cooldown!,
      state: 'ready' as const,
      lanes: {
        lakebase: { ...watching.cooldown!.lanes.lakebase, state: 'confirmed_zero' as const, confirmed_at: '2026-08-17T00:05:11Z', elapsed_ms: 301000, status: 'Control plane confirmed zero' },
        competitor: { ...watching.cooldown!.lanes.competitor, state: 'confirmed_zero' as const, confirmed_at: '2026-08-17T00:06:51Z', elapsed_ms: 401000, status: 'Control plane confirmed zero' },
      },
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(session('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(session('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      if (input.endsWith('/cooldown')) return Promise.resolve(jsonResponse(watching))
      if (input === '/api/sessions/session-1') return refresh.promise
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    const source = FakeEventSource.instances.at(-1)!
    source.emit({ sequence: 9, event: 'run_finished', occurred_at: verified.updated_at, payload: { state: 'verified', session: verified } })
    await user.click(await screen.findByRole('button', { name: /re-do round/i }))
    source.emit({ sequence: 10, event: 'cooldown_ready', occurred_at: '2026-08-17T00:06:51Z', payload: { cooldown: readyCooldown } })
    expect(await screen.findByLabelText(/lakebase return to idle/i)).toHaveTextContent('5:01')
    expect(screen.getByLabelText(/aurora serverless v2 return to idle/i)).toHaveTextContent('6:41')

    source.onerror?.()
    await waitFor(() => expect(fetchMock.mock.calls.some((call) => call[0] === '/api/sessions/session-1')).toBe(true))
    refresh.resolve(jsonResponse({ ...verified, updated_at: '2026-08-17T00:07:00Z', cooldown: null }))
    await waitFor(() => expect(screen.getByRole('button', { name: /ring again/i })).toBeEnabled())
    expect(screen.getByLabelText(/lakebase return to idle/i)).toHaveTextContent('5:01')
    expect(screen.getByLabelText(/aurora serverless v2 return to idle/i)).toHaveTextContent('6:41')

    await user.click(screen.getByRole('button', { name: /scorecard/i }))
    expect(screen.getByText(/return → confirmed zero/i).parentElement).toHaveTextContent('5:01 LB · 6:41 OPP')
  })

  it('uses deletion rather than idle as the round two re-do contract', async () => {
    const startedAt = new Date().toISOString()
    const verified = safeChangeSession('verified')
    const resetting: DemoSession = {
      ...verified,
      cooldown: {
        mode: 'delete_isolated_environment',
        state: 'watching',
        started_at: startedAt,
        failure: null,
        lanes: {
          lakebase: { id: 'lakebase', name: 'Lakebase', state: 'watching', started_at: startedAt, confirmed_at: null, elapsed_ms: null, status: 'Deleting branch' },
          competitor: { id: 'competitor', name: 'Aurora Serverless v2', state: 'watching', started_at: startedAt, confirmed_at: null, elapsed_ms: null, status: 'Deleting clone' },
        },
      },
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(safeChangeSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(safeChangeSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(safeChangeSession('running')))
      if (input.endsWith('/cooldown')) return Promise.resolve(jsonResponse(resetting))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /stacktrace jack/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    const source = FakeEventSource.instances.at(-1)!
    source.emit({
      sequence: 9,
      event: 'run_finished',
      occurred_at: '2026-08-17T00:00:10Z',
      payload: { state: 'verified', session: verified },
    })

    await user.click(await screen.findByRole('button', { name: /re-do round/i }))
    expect(screen.getByRole('heading', { name: /clear the test corner/i })).toBeInTheDocument()
    expect(screen.getByText(/isolated environment is confirmed deleted/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/lakebase isolated environment reset/i)).toHaveTextContent('RESETTING')
    expect(screen.getByRole('button', { name: /waiting for cleanup/i })).toBeDisabled()

    source.emit({
      sequence: 10,
      event: 'cooldown_ready',
      occurred_at: '2026-08-17T00:00:20Z',
      payload: {
        cooldown: {
          ...resetting.cooldown!,
          state: 'ready',
          lanes: {
            lakebase: { ...resetting.cooldown!.lanes.lakebase, state: 'confirmed_deleted', confirmed_at: '2026-08-17T00:00:12Z', elapsed_ms: 2000 },
            competitor: { ...resetting.cooldown!.lanes.competitor, state: 'confirmed_deleted', confirmed_at: '2026-08-17T00:00:20Z', elapsed_ms: 10000 },
          },
        },
      },
    })

    expect(await screen.findByText(/both isolated environments removed/i)).toBeInTheDocument()
    expect(screen.getAllByText(/deleted confirmed · clock stopped/i)).toHaveLength(2)
    expect(screen.getByRole('button', { name: /ring again/i })).toBeEnabled()
  })

  it('lets a failed round two clear its corner and retry failed cleanup', async () => {
    const failed: DemoSession = {
      ...safeChangeSession('failed'),
      failure: 'One or more isolated schema changes could not be verified.',
      lanes: {
        lakebase: {
          ...safeChangeSession('failed').lanes.lakebase,
          state: 'failed', elapsed_ms: 6651.7,
          status: 'The isolated schema change could not be verified',
          error: 'isolated endpoint contract rejected',
        },
        competitor: {
          ...safeChangeSession('failed').lanes.competitor,
          state: 'verified', elapsed_ms: 480000,
          status: 'Isolated migration verified', error: null,
        },
      },
    }
    const startedAt = new Date().toISOString()
    const failedCleanup: DemoSession = {
      ...failed,
      cooldown: {
        mode: 'delete_isolated_environment', state: 'failed', started_at: startedAt,
        failure: 'Isolated environments could not be safely removed.',
        lanes: {
          lakebase: { id: 'lakebase', name: 'Lakebase', state: 'failed', started_at: startedAt, confirmed_at: null, elapsed_ms: null, status: 'temporary cleanup failure' },
          competitor: { id: 'competitor', name: 'Aurora Serverless v2', state: 'confirmed_deleted', started_at: startedAt, confirmed_at: startedAt, elapsed_ms: 1000, status: 'Deleted' },
        },
      },
    }
    const retrying: DemoSession = {
      ...failed,
      cooldown: {
        ...failedCleanup.cooldown!, state: 'watching', failure: null,
        lanes: {
          lakebase: { ...failedCleanup.cooldown!.lanes.lakebase, state: 'watching', status: 'Deleting owned isolated environment' },
          competitor: { ...failedCleanup.cooldown!.lanes.competitor, state: 'watching', confirmed_at: null, elapsed_ms: null, status: 'Deleting owned isolated environment' },
        },
      },
    }
    let cleanupCalls = 0
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(safeChangeSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(safeChangeSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(failed))
      if (input.endsWith('/cooldown')) {
        cleanupCalls += 1
        return Promise.resolve(jsonResponse(cleanupCalls === 1 ? failedCleanup : retrying))
      }
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /stacktrace jack/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    await user.click(await screen.findByRole('button', { name: /clear test corner/i }))
    const retry = await screen.findByRole('button', { name: /retry cleanup/i })
    expect(screen.getByText(/cleanup stopped.*retry cleanup/i)).toBeInTheDocument()
    expect(screen.queryByText(/clocks are live/i)).not.toBeInTheDocument()
    expect(retry).toBeEnabled()
    await user.click(retry)
    expect(await screen.findByRole('button', { name: /waiting for cleanup/i })).toBeDisabled()
    expect(cleanupCalls).toBe(2)
  })

  it('keeps a round two re-do bound to the completed session after setup history is mutated', async () => {
    const startedAt = new Date().toISOString()
    const verified = safeChangeSession('verified')
    const readyToRedo: DemoSession = {
      ...verified,
      cooldown: {
        mode: 'delete_isolated_environment',
        state: 'ready',
        started_at: startedAt,
        failure: null,
        lanes: {
          lakebase: {
            id: 'lakebase', name: 'Lakebase', state: 'confirmed_deleted',
            started_at: startedAt, confirmed_at: '2026-08-17T00:00:12Z',
            elapsed_ms: 2000, status: 'Branch deleted',
          },
          competitor: {
            id: 'competitor', name: 'Aurora Serverless v2', state: 'confirmed_deleted',
            started_at: startedAt, confirmed_at: '2026-08-17T00:00:20Z',
            elapsed_ms: 10000, status: 'Clone deleted',
          },
        },
      },
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(safeChangeSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(safeChangeSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(safeChangeSession('running')))
      if (input.endsWith('/cooldown')) return Promise.resolve(jsonResponse(readyToRedo))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /stacktrace jack/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    FakeEventSource.instances.at(-1)?.emit({
      sequence: 9,
      event: 'run_finished',
      occurred_at: '2026-08-17T00:00:10Z',
      payload: { state: 'verified', session: verified },
    })
    await user.click(await screen.findByRole('button', { name: /re-do round/i }))
    expect(await screen.findByRole('button', { name: /ring again/i })).toBeEnabled()

    window.history.back()
    await screen.findByRole('button', { name: /re-do round/i })
    window.history.back()
    await screen.findByRole('button', { name: /round already run/i })
    window.history.back()
    await screen.findByText(/· round \d+ of six/i)
    window.history.back()
    await screen.findByRole('heading', { name: /add up to two lenses/i })
    window.history.back()
    await screen.findByRole('heading', { name: /choose the lead voice/i })
    window.history.back()
    await screen.findByRole('heading', { name: /choose the opponent/i })

    await user.click(screen.getByRole('radio', { name: /rds postgresql/i }))
    await user.click(screen.getByRole('button', { name: /cost.*published rates now.*billed usage later/i }))
    await user.click(screen.getByRole('button', { name: /performance.*elapsed workflow time to verified outcome/i }))

    window.history.forward()
    await screen.findByRole('heading', { name: /choose the lead voice/i })
    await user.click(screen.getByRole('radio', { name: /3 a\.m\. sam/i }))
    window.history.forward()
    await screen.findByRole('heading', { name: /add up to two lenses/i })
    await user.click(screen.getByRole('button', { name: /backfill bill/i }))
    window.history.forward()
    await screen.findByText(/· round \d+ of six/i)
    expect(screen.getByRole('heading', { name: /wake this idle app/i })).toBeInTheDocument()
    window.history.forward()
    await screen.findByRole('button', { name: /round already run/i })
    window.history.forward()
    await screen.findByRole('button', { name: /re-do round/i })
    window.history.forward()
    await screen.findByRole('button', { name: /ring again/i })

    await user.click(screen.getByRole('button', { name: /ring again/i }))
    await waitFor(() => {
      const creates = fetchMock.mock.calls.filter((call) => call[0] === '/api/sessions')
      expect(creates).toHaveLength(2)
    })
    const creates = fetchMock.mock.calls.filter((call) => call[0] === '/api/sessions')
    const secondCreate = JSON.parse(creates[1][1].body)
    expect(secondCreate).toEqual({
      competitor: 'aurora_serverless_v2',
      primary_persona: 'software_engineer',
      secondary_personas: [],
      corners: ['performance'],
      round_id: 'make_schema_change_safely',
    })
  })

  it('allows a local visual review without inventing a measurement', async () => {
    window.history.replaceState({}, '', '/?review=1')
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(screen.getByRole('button', { name: /preview fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    expect(screen.getByText(/no measurement recorded/i)).toBeInTheDocument()
    expect(screen.getByText(/no database connections · no result/i)).toBeInTheDocument()
    expect(screen.queryByText(/seconds sooner/i)).not.toBeInTheDocument()
  })

  it('previews the RDS capability gap without manufacturing a timer', async () => {
    window.history.replaceState({}, '', '/?review=1')
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('radio', { name: /rds postgresql/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    expect(screen.getByRole('heading', { name: /wake this idle app/i })).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /preview fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    const rdsLane = screen.getByLabelText('RDS PostgreSQL result')
    expect(within(rdsLane).getByText(/no scale-to-zero/i)).toBeInTheDocument()
    expect(within(rdsLane).getByText(/capability preview · no live check/i)).toBeInTheDocument()
    expect(within(rdsLane).queryByText(/0\.00/)).not.toBeInTheDocument()
    expect(screen.getByText(/rds capability not checked live · no result/i)).toBeInTheDocument()
    expect(screen.getByText(/no measurement recorded/i)).toBeInTheDocument()
  })

  it('shows the verified RDS capability win without a fake RDS time', async () => {
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(rdsSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(rdsSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(rdsSession('verified')))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('radio', { name: /rds postgresql/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    expect(await screen.findByText('LAKEBASE WINS — RDS CANNOT ENTER THE ROUND')).toBeInTheDocument()
    const rdsLane = screen.getByLabelText('RDS PostgreSQL result')
    expect(within(rdsLane).getByText(/no scale-to-zero/i)).toBeInTheDocument()
    expect(within(rdsLane).queryByText(/0\.00/)).not.toBeInTheDocument()
    expect(screen.getByText(/RDS has no automatic scale-to-zero wake, so there is no RDS timer/i)).toBeInTheDocument()
    expect(screen.queryByText(/seconds sooner/i)).not.toBeInTheDocument()

    // Only Lakebase was released, so a start gap computed from a single lane
    // would read as a perfect 0.000ms fairness number that nothing earned.
    await user.click(screen.getByRole('button', { name: /instant replay/i }))
    const replay = await screen.findByRole('dialog', { name: /how this round was proved/i })
    expect(replay).toHaveTextContent(/Opponent lane/i)
    expect(replay).toHaveTextContent(/no automatic scale-to-zero or connection-triggered wake/i)
    expect(replay).not.toHaveTextContent(/Start gap/i)
    expect(replay).not.toHaveTextContent(/0\.000ms/)
  })

  it('throws the Round 3 towel, censors the active opponent, and unlocks the receipt only after cleanup', async () => {
    const eligible = recoveryTowelSession('eligible')
    const stopping = recoveryTowelSession('stopping')
    const cleaning = recoveryTowelSession('cleaning')
    const executive = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'executive')!
    const towelled = { ...recoveryTowelSession('ready'), secondary_personas: [executive] }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') {
        return Promise.resolve(jsonResponse({ ...session('draft'), round: eligible.round }))
      }
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse({ ...session('armed'), round: eligible.round }))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(eligible))
      if (input.endsWith('/towel')) return Promise.resolve(jsonResponse(stopping))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /lockjaw lucy/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))

    const throwTowel = await screen.findByRole('button', { name: /throw in the towel/i })
    expect(screen.queryByRole('button', { name: /next round/i })).not.toBeInTheDocument()
    await user.click(throwTowel)

    const towelRequest = fetchMock.mock.calls.find((call) => call[0] === '/api/sessions/session-1/towel')
    expect(towelRequest?.[1]).toMatchObject({ method: 'POST' })
    const opponentLane = screen.getByLabelText('Aurora Serverless v2 result')
    await waitFor(() => expect(opponentLane.querySelector('.lane-time')).toHaveTextContent('>90.00s'))
    expect(screen.queryByRole('button', { name: /next round/i })).not.toBeInTheDocument()

    const source = FakeEventSource.instances.at(-1)!
    source.emit({
      sequence: 10,
      event: 'towel_update',
      occurred_at: '2026-08-18T15:11:31Z',
      payload: { session: cleaning },
    })
    expect(await screen.findByText(/cleaning owned recovery environments/i)).toBeInTheDocument()
    expect(await screen.findByText(/aws restore already in motion.*safe cleanup may take minutes/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /next round/i })).not.toBeInTheDocument()

    // Cleanup can sit here for minutes. The ringside strip is already up --
    // a towel lands in `towelled` with a remembered result -- but the actions
    // row is still withheld, so the strip has to carry its own way into the
    // overlay or the detail behind it is unreachable for the whole window.
    const cleaningStrip = screen.getByLabelText(/what the result means at ringside/i)
    expect(cleaningStrip).toBeInTheDocument()
    const strippedExplain = within(cleaningStrip).getByRole('button', { name: /explain to the room/i })
    await user.click(strippedExplain)
    const cleaningTake = await screen.findByRole('dialog', { name: /make the result matter/i })
    // The exact sentence the strip used to print inline, reachable from the
    // strip itself while the actions row is still withheld.
    expect(cleaningTake).toHaveTextContent(/one-line takeaway for the sre/i)
    expect(cleaningTake).toHaveTextContent(/the lease stayed with cleanup, preventing another bout from mutating the configured sources/i)
    await user.click(within(cleaningTake).getByRole('button', { name: /back to the ring/i }))

    source.emit({
      sequence: 11,
      event: 'towel_finished',
      occurred_at: '2026-08-18T15:11:34Z',
      payload: { session: towelled },
    })
    expect(await screen.findByText(
      'TOWEL THROWN AT 90.00s · LAKEBASE VERIFIED 14.38s · AURORA SERVERLESS V2 UNVERIFIED WHEN STOPPED · LOWER BOUND',
    )).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()
    expect(opponentLane.querySelector('.lane-time')).toHaveTextContent('>90.00s')
    expect(opponentLane).toHaveTextContent(/unverified when stopped.*lower bound/i)

    await user.click(screen.getByRole('button', { name: /explain to the room/i }))
    const towelTake = await screen.findByRole('dialog', { name: /make the result matter/i })
    expect(towelTake).toHaveTextContent(/proof behind it.*exact recovered read.*censored lower bound.*no service failover or customer slo was tested/i)
    expect(towelTake).toHaveTextContent(/shared exact proof.*aurora serverless v2 >90\.00s, unverified when stopped/i)
    await user.click(within(towelTake).getByRole('button', { name: /the big why/i }))
    expect(towelTake).toHaveTextContent(/proof behind it.*censored lower bound at the cutoff/i)
    await user.click(within(towelTake).getByRole('button', { name: /back to the ring/i }))

    stubReceiptCanvas()
    await user.click(screen.getByRole('button', { name: /share the receipt/i }))
    const receipt = await screen.findByRole('dialog', { name: /share the proof/i })
    expect(within(receipt).getByLabelText(/lakebase receipt result/i)).toHaveTextContent('14.38s')
    expect(within(receipt).getByLabelText(/aurora serverless v2 receipt result/i)).toHaveTextContent('>90.00s')
    expect(within(receipt).getByLabelText(/aurora serverless v2 receipt result/i)).toHaveTextContent(/unverified when stopped.*lower bound/i)

    await waitFor(() => {
      const entries = JSON.parse(window.localStorage.getItem('lakebase-anti-demo:scorecard:v1') ?? '[]')
      expect(entries).toEqual(expect.arrayContaining([
        expect.objectContaining({ session_id: 'session-1', competitor_ms: 90_000, competitor_censored: true }),
      ]))
    })

    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText },
    })
    await user.click(within(receipt).getByRole('button', { name: /copy caption/i }))
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(expect.stringMatching(
      /^I threw in the towel at 90\.00s[\s\S]*result is >90\.00s[\s\S]*Aurora Serverless v2 · >90\.00s/,
    )))
  })

  it('keeps a round toweled before Lakebase verified on the card, as abandoned', async () => {
    /* The vanishing round. `scorecardEntry` returned null whenever our own lane
       had no verified figure, so a towel thrown early left the scorecard with
       no row for that round at all -- indistinguishable from a round nobody
       selected, and on a card an absent row reads as data loss rather than as
       the decision the operator actually made.

       Nothing was proved here, so the row must carry no time and no win. It
       must simply be visible, in the words `recap.ts` already uses. */
    const eligible = recoveryTowelSession('eligible')
    const stopping = recoveryTowelSession('stopping')
    const abandoned = earlyTowelSession()
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') {
        return Promise.resolve(jsonResponse({ ...session('draft'), round: eligible.round }))
      }
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse({ ...session('armed'), round: eligible.round }))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(eligible))
      if (input.endsWith('/towel')) return Promise.resolve(jsonResponse(stopping))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    await user.click(await screen.findByRole('button', { name: /throw in the towel/i }))

    FakeEventSource.instances.at(-1)!.emit({
      sequence: 11,
      event: 'towel_finished',
      occurred_at: '2026-08-18T15:11:34Z',
      payload: { session: abandoned },
    })

    await waitFor(() => {
      const entries = JSON.parse(window.localStorage.getItem('lakebase-anti-demo:scorecard:v1') ?? '[]')
      expect(entries).toEqual([
        expect.objectContaining({
          session_id: 'session-1',
          round_id: 'recover_deleted_order',
          lakebase_ms: null,
          competitor_ms: null,
          competitor_censored: false,
        }),
      ])
    })
  })

  it('retries a failed towel cleanup without exposing Next', async () => {
    const eligible = recoveryTowelSession('eligible')
    const stopping = recoveryTowelSession('stopping')
    const failed = recoveryTowelSession('failed')
    const cleaning = recoveryTowelSession('cleaning')
    let towelCalls = 0
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') {
        return Promise.resolve(jsonResponse({ ...session('draft'), round: eligible.round }))
      }
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse({ ...session('armed'), round: eligible.round }))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(eligible))
      if (input.endsWith('/towel')) {
        towelCalls += 1
        return Promise.resolve(jsonResponse(towelCalls === 1 ? stopping : cleaning))
      }
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /lockjaw lucy/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    await user.click(await screen.findByRole('button', { name: /throw in the towel/i }))

    FakeEventSource.instances.at(-1)!.emit({
      sequence: 10,
      event: 'towel_update',
      occurred_at: '2026-08-18T15:11:35Z',
      payload: { session: failed },
    })
    const retry = await screen.findByRole('button', { name: /retry/i })
    expect(screen.getByText(/recovery environments could not be safely removed/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /next round/i })).not.toBeInTheDocument()

    await user.click(retry)
    await waitFor(() => expect(towelCalls).toBe(2))
    expect(screen.queryByRole('button', { name: /next round/i })).not.toBeInTheDocument()
    expect(await screen.findByText(/cleaning owned recovery environments/i)).toBeInTheDocument()
    expect(await screen.findByText(/aws restore already in motion.*safe cleanup may take minutes/i)).toBeInTheDocument()
  })

  it('runs Round 3 through verified recovery and hides a successful redo', async () => {
    const recoveryRound = FALLBACK_CATALOG.rounds.find((round) => round.id === 'recover_deleted_order')!
    const base = session('running')
    const running: DemoSession = {
      ...base,
      round: recoveryRound,
      lanes: {
        lakebase: { ...base.lanes.lakebase, status: 'Lakebase point-in-time recovery branch request accepted', activity: { phase: 'restoring', wire_call: 'databricks postgres create-branch' } },
        competitor: { ...base.lanes.competitor, status: 'Aurora full-copy PITR recovery cluster request accepted', activity: { phase: 'restoring', wire_call: 'RDS.RestoreDBClusterToPointInTime' } },
      },
    }
    const verified: DemoSession = {
      ...running,
      state: 'verified',
      remembered_result: 'LAKEBASE — 12.00 SECONDS SOONER',
      lanes: {
        lakebase: { ...running.lanes.lakebase, state: 'verified', elapsed_ms: 18_000, status: 'Exact recovered order verified · Source deletion preserved', activity: { phase: 'verified', wire_call: null } },
        competitor: { ...running.lanes.competitor, state: 'verified', elapsed_ms: 30_000, status: 'Exact recovered order verified · Source deletion preserved', activity: { phase: 'verified', wire_call: null } },
      },
    }
    const startedAt = '2026-08-18T15:10:00Z'
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse({ ...session('draft'), round: recoveryRound }))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse({ ...session('armed'), round: recoveryRound }))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(running))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('radio', { name: /lockjaw lucy/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    expect(await screen.findByText(/point-in-time recovery proof in progress/i)).toBeInTheDocument()
    expect(screen.getByText(/on the wire/i)).toHaveTextContent('Lakebase: databricks postgres create-branch')

    const source = FakeEventSource.instances.at(-1)!
    source.emit({ sequence: 9, event: 'run_finished', occurred_at: startedAt, payload: { state: 'verified', session: verified } })
    expect(await screen.findByText(/same exact row · one deletion barrier · eligibility \+ recovery \+ verified read timed/i)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /instant replay/i }))
    expect(await screen.findByText(/bell released one deletion barrier/i)).toBeInTheDocument()
    expect(screen.getByText(/timer includes exact deletion, recovery eligibility, restore readiness, tls, and verified reads/i)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /back to the ring/i }))
    expect(screen.queryByRole('button', { name: /re-do round/i })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()
  })

  it('keeps RDS as no scale-to-zero on the Round 1 reset scorecard', async () => {
    const verified = rdsSession('verified')
    const startedAt = '2026-08-17T00:00:10Z'
    const ready: DemoSession = {
      ...verified,
      cooldown: {
        mode: 'return_to_idle',
        state: 'ready',
        started_at: startedAt,
        failure: null,
        lanes: {
          lakebase: {
            id: 'lakebase', name: 'Lakebase', state: 'confirmed_zero',
            started_at: startedAt, confirmed_at: '2026-08-17T00:01:11Z',
            elapsed_ms: 61000, status: 'Control plane confirmed zero',
          },
          competitor: {
            id: 'competitor', name: 'RDS PostgreSQL', state: 'not_supported',
            started_at: startedAt, confirmed_at: startedAt,
            elapsed_ms: null, status: 'No automatic scale-to-zero',
          },
        },
      },
    }
    const fetchMock = vi.fn().mockImplementation((input: string, init?: RequestInit) => {
      if (input === '/api/catalog') return Promise.resolve(jsonResponse(FALLBACK_CATALOG))
      if (input === '/api/sessions' && init?.method === 'POST') return Promise.resolve(jsonResponse(rdsSession('draft')))
      if (input.endsWith('/arm')) return Promise.resolve(jsonResponse(rdsSession('armed')))
      if (input.endsWith('/run')) return Promise.resolve(jsonResponse(verified))
      if (input.endsWith('/cooldown')) return Promise.resolve(jsonResponse(ready))
      throw new Error(`Unexpected request: ${input}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    vi.stubGlobal('EventSource', FakeEventSource)
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: /press start/i }))
    await user.click(screen.getByRole('radio', { name: /rds postgresql/i }))
    await user.click(screen.getByRole('button', { name: /choose the lead voice/i }))
    await user.click(screen.getByRole('button', { name: /add supporting lenses/i }))
    await user.click(screen.getByRole('button', { name: /reveal the fight card/i }))
    await user.click(await screen.findByRole('button', { name: /prepare fight card/i }))
    await user.click(await screen.findByRole('button', { name: /ring the bell/i }))
    const source = FakeEventSource.instances.at(-1)!
    source.emit({
      sequence: 9,
      event: 'run_finished',
      occurred_at: '2026-08-17T00:00:09Z',
      payload: { state: 'verified', session: verified },
    })

    await user.click(await screen.findByRole('button', { name: /re-do round/i }))
    source.emit({
      sequence: 10,
      event: 'cooldown_ready',
      occurred_at: '2026-08-17T00:01:11Z',
      payload: { cooldown: ready.cooldown! },
    })
    await user.click(screen.getByRole('button', { name: /scorecard/i }))

    expect(screen.getByText(/return → confirmed zero/i).parentElement).toHaveTextContent(
      '1:01 LB · NO SCALE-TO-ZERO OPP',
    )
  })
})

describe('the staff roll entry points', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.localStorage.clear()
    window.sessionStorage.clear()
    FakeEventSource.instances = []
    window.history.replaceState({}, '', '/')
    vi.spyOn(api, 'boutStatus').mockResolvedValue({
      ring_ready: true, maintenance_state: 'ready', maintenance_detail: null, active: false,
      operator: null, started_at: null, updated_at: null, expires_at: null,
      phase: null, state: null, round_title: null, competitor: null,
    })
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(FALLBACK_CATALOG)))
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    window.localStorage.clear()
    window.history.replaceState({}, '', '/')
  })

  it('bills the title-screen entry as a staff roll, never as arcade credits', () => {
    render(<App />)

    const entry = screen.getByRole('button', { name: 'Staff roll' })
    expect(entry).toBeInTheDocument()
    // "CREDITS" on an arcade bottom line is the coin counter, so the old
    // wording must not survive anywhere on the attract screen.
    expect(screen.queryByRole('button', { name: /^▶? ?credits$/i })).not.toBeInTheDocument()
  })

  it('reads a stored abandoned round back off disk without billing it as a win', async () => {
    /* Two things at once, because they fail together. The stored row carries a
       null Lakebase time, so the load filter has to accept null as a value
       rather than treat it as a missing field -- otherwise the round vanishes
       on reload, which is the same disappearance by a slower route. And having
       survived the trip, it must not arrive in the roll as a verified win. */
    window.localStorage.setItem('lakebase-anti-demo:scorecard:v1', JSON.stringify([{
      session_id: 'session-1',
      round_id: 'recover_deleted_order',
      round_title: 'Recover this deleted order',
      competitor: 'Aurora Serverless v2',
      lakebase_ms: null,
      competitor_ms: null,
      competitor_censored: false,
      competitor_capability_gap: false,
      remembered_result: 'TOWELED AT 45.00s · NO WINNER · MARGIN N/A',
      completed_at: '2026-08-18T15:11:30Z',
      cooldown: null,
    }]))
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: 'Staff roll' }))
    const roll = await screen.findByRole('dialog', { name: 'Credits' })

    expect(roll).toHaveTextContent('1 bout · Lakebase 0 verified wins · 1 abandoned')
    expect(roll).not.toHaveTextContent('1 verified win')
    expect(roll).not.toHaveTextContent(/No bout completed on this card/)
  })

  it('keeps the entry keyboard reachable and opens the roll from the keyboard', async () => {
    const user = userEvent.setup()
    render(<App />)

    const entry = screen.getByRole('button', { name: 'Staff roll' })
    entry.focus()
    expect(entry).toHaveFocus()
    expect(entry).toHaveAttribute('aria-expanded', 'false')

    await user.keyboard('{Enter}')
    expect(await screen.findByRole('dialog', { name: 'Credits' })).toBeInTheDocument()
  })

  it('parks the menu cursor beside the entry without letting it move the label', () => {
    render(<App />)

    const entry = screen.getByRole('button', { name: 'Staff roll' })
    const cursor = entry.querySelector('.credits-entry-cursor')
    // Present in the box at all times so revealing it cannot reflow the label,
    // and silent to assistive tech, which already hears the label.
    expect(cursor).not.toBeNull()
    expect(cursor).toHaveTextContent('▶')
    expect(cursor).toHaveAttribute('aria-hidden', 'true')
    expect(entry).toHaveTextContent('Staff roll')
  })

  it('sits steady rather than blinking when reduced motion is asked for', () => {
    // useReducedMotion is mocked true for this suite, matching the roll's own
    // data-static convention.
    render(<App />)

    expect(screen.getByRole('button', { name: 'Staff roll' })).toHaveAttribute('data-static', 'true')
  })

  it('draws the commentator from the pixel sprite, not the old photograph', async () => {
    const user = userEvent.setup()
    render(<App />)

    await user.click(screen.getByRole('button', { name: 'Staff roll' }))
    const roll = await screen.findByRole('dialog', { name: 'Credits' })

    const portraits = [...roll.querySelectorAll('img')]
      .map((img) => img.getAttribute('src') ?? '')
      .filter((src) => src.includes('ryan'))
    expect(portraits.length).toBeGreaterThan(0)
    for (const src of portraits) {
      expect(src).toMatch(/ryan-pixel-portrait/)
      expect(src).not.toMatch(/ryan-ringside/)
    }
  })
})

describe('the ringside portrait asset', () => {
  it('exports both sprites and no longer exports the photograph', async () => {
    const { brandAssets } = await import('./assets')
    expect(brandAssets.ryanPixel).toMatch(/ryan-pixel-portrait/)
    expect(brandAssets.ryanPixelSmall).toMatch(/ryan-pixel-commentator/)
    expect(brandAssets).not.toHaveProperty('ryanRingside')
  })

  it('paints the commentator bar from the sprite cut to its own 48px box', async () => {
    // The bar's content box is fixed at 48px, so it must not be fed the 64px
    // sprite: at 0.75 scale the browser drops every fourth row and the cream
    // keyline breaks into dashes.
    const { brandAssets } = await import('./assets')
    expect(brandAssets.ryanPixelSmall).not.toEqual(brandAssets.ryanPixel)
  })

  it('ships both sprites with a real alpha channel', async () => {
    /* The frames were removed from both slots because the sprites are keyed
       transparent. If a future re-export flattens the alpha back onto navy,
       the CSS is no longer covering for it and the figure starts dragging a
       navy rectangle across the roll -- so the alpha channel is asserted from
       the real bytes rather than trusted. ?inline hands back the same file the
       build inlines, base64-encoded. */
    const sprites = await Promise.all([
      import('./ryan-pixel-commentator.png?inline').then((m) => [m.default, 48, 48] as const),
      import('./ryan-pixel-portrait.png?inline').then((m) => [m.default, 64, 64] as const),
    ])

    for (const [dataUri, width, height] of sprites) {
      expect(dataUri.startsWith('data:image/png;base64,')).toBe(true)
      const bytes = Uint8Array.from(
        atob(dataUri.slice('data:image/png;base64,'.length)),
        (char) => char.charCodeAt(0),
      )
      const be32 = (at: number) => new DataView(bytes.buffer).getUint32(at)

      expect([...bytes.subarray(0, 8)]).toEqual([137, 80, 78, 71, 13, 10, 26, 10])
      expect(String.fromCharCode(...bytes.subarray(12, 16))).toBe('IHDR')
      expect(be32(16)).toBe(width)
      expect(be32(20)).toBe(height)
      // PNG colour type 6 is truecolour WITH alpha. 2 would be a flattened
      // re-export, which is the regression this is here to catch.
      expect(bytes[25]).toBe(6)
      // Under Vite's 4096-byte assetsInlineLimit, so both stay data URIs and
      // the roll never waits on a request for a 2 kB portrait.
      expect(bytes.byteLength).toBeLessThan(4096)
    }
  })

  it('keeps no portrait in the folder but the two sprites', () => {
    // The photograph is gone from the tree, not merely unreferenced: a glob
    // over the folder is the only check that notices it coming back.
    const found = Object.keys(import.meta.glob('./ryan-*.png')).sort()
    expect(found).toEqual(['./ryan-pixel-commentator.png', './ryan-pixel-portrait.png'])
  })
})
