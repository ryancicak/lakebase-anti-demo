import { act, cleanup, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  InstantReplay,
  ShareReceipt,
  linkedInReceipt,
  receiptPresentation,
  renderReceiptCard,
} from './App'
import type { ReceiptCardStyle, ReceiptCardTheme } from './App'
import type { DemoSession, LaneSnapshot, RoundFiveRuntimeLane } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import {
  canvasFontSize,
  getCanvasRecordings,
  measuredTextWidth,
  resetCanvasRecordings,
} from './test/setup'
import type { CanvasOperation, CanvasRecording } from './test/setup'

/*
 * The share cards, drawn the way a browser draws them. The test canvas measures text
 * exactly as Press Start 2P does (one em a character), so a line that overflows its box
 * or lands on another here does the same on the PNG that posts.
 */

const CARD_WIDTH = 1200
const CARD_HEIGHT = 627
const STYLES: ReceiptCardStyle[] = ['health-bars', 'knockout']
const THEMES: ReceiptCardTheme[] = ['classic', 'halloween']
const ROUND_SIX_STATUS = 'The exact order is in the lakehouse · separate checkout committed'

type TextOperation = Extract<CanvasOperation, { kind: 'fillText' }>

interface TextBox {
  operation: TextOperation
  left: number
  right: number
  top: number
  bottom: number
}

beforeEach(() => resetCanvasRecordings())
afterEach(() => cleanup())

function texts(recording: CanvasRecording): TextOperation[] {
  return recording.operations.filter((operation): operation is TextOperation => operation.kind === 'fillText')
}

function textBox(operation: TextOperation): TextBox {
  const size = canvasFontSize(operation.font)
  const measured = measuredTextWidth(operation.text, operation.font)
  // A browser condenses a line wider than fillText's maxWidth into it.
  const width = operation.maxWidth === undefined ? measured : Math.min(measured, operation.maxWidth)
  const left = operation.textAlign === 'right' || operation.textAlign === 'end'
    ? operation.x - width
    : operation.textAlign === 'center'
      ? operation.x - width / 2
      : operation.x
  const top = operation.textBaseline === 'top' || operation.textBaseline === 'hanging'
    ? operation.y
    : operation.textBaseline === 'middle'
      ? operation.y - size / 2
      : operation.y - size
  return { operation, left, right: left + width, top, bottom: top + size }
}

/**
 * Every line on the card lies inside it, none is condensed by more than `squeeze`, and no
 * two different lines overlap. The same text drawn twice is a drop shadow, so it may.
 */
function expectNoTextCollisions(recording: CanvasRecording, squeeze = 1): void {
  const boxes = texts(recording).map(textBox)
  for (const box of boxes) {
    const where = `"${box.operation.text}" at ${box.operation.x},${box.operation.y} (${box.operation.font})`
    expect(box.left, `${where} starts left of the card`).toBeGreaterThanOrEqual(0)
    expect(box.right, `${where} runs past the card's right edge`).toBeLessThanOrEqual(CARD_WIDTH)
    expect(box.top, `${where} starts above the card`).toBeGreaterThanOrEqual(0)
    expect(box.bottom, `${where} runs past the card's bottom edge`).toBeLessThanOrEqual(CARD_HEIGHT)
    if (box.operation.maxWidth !== undefined) {
      const measured = measuredTextWidth(box.operation.text, box.operation.font)
      expect(measured / box.operation.maxWidth, `${where} is condensed into its box`).toBeLessThanOrEqual(squeeze)
    }
  }
  const tolerance = 0.5
  for (let i = 0; i < boxes.length; i += 1) {
    for (let j = i + 1; j < boxes.length; j += 1) {
      const a = boxes[i]
      const b = boxes[j]
      if (a.operation.text === b.operation.text) continue
      const overlaps = a.left < b.right - tolerance && b.left < a.right - tolerance
        && a.top < b.bottom - tolerance && b.top < a.bottom - tolerance
      expect(overlaps, `"${a.operation.text}" overlaps "${b.operation.text}"`).toBe(false)
    }
  }
}

async function drawCard(
  session: DemoSession,
  roundNumber: number,
  kind: 'round' | 'idle',
  style: ReceiptCardStyle,
  theme: ReceiptCardTheme,
): Promise<CanvasRecording> {
  const before = getCanvasRecordings().length
  await renderReceiptCard(session, roundNumber, kind, style, theme)
  const drawn = getCanvasRecordings()
  expect(drawn.length, 'the card was not drawn').toBe(before + 1)
  return drawn[drawn.length - 1]
}

function joined(recording: CanvasRecording): string {
  return texts(recording).map((operation) => operation.text).join(' ')
}

const aurora = FALLBACK_CATALOG.competitors.find((item) => item.id === 'aurora_serverless_v2')!
const sre = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'sre')!

function baseSession(roundId: DemoSession['round']['id']): Omit<DemoSession, 'lanes'> {
  return {
    id: 'a21b1384a4ac4154a78d72ae9c373f68',
    state: 'verified',
    created_at: '2026-10-14T17:40:00Z',
    updated_at: '2026-10-14T17:55:13.104Z',
    competitor: aurora,
    primary_persona: sre,
    secondary_personas: [],
    corners: ['performance'],
    round: FALLBACK_CATALOG.rounds.find((round) => round.id === roundId)!,
    recommendation_reason: '',
    presenter_pack: {
      opening: '', discovery_question: '', risk: '', stop_condition: '', remembered_metric: '',
      primary: { persona_id: sre.id, nickname: sre.nickname, role: sre.role, interpretation: '', objection: '', response: '' },
      secondary: [], closing: '',
    },
    fairness: { same_client: true, same_transaction: true, same_nonce: true, launch_skew_ms: 0.232079 },
    remembered_result: null,
    failure: null,
  }
}

/** A verified Round 1 bout: both lanes timed, with the status each lane printed. */
function roundOneSession(lakebaseMs: number, competitorMs: number, status = 'Transaction verified'): DemoSession {
  const lane = (id: 'lakebase' | 'competitor', name: string, elapsed: number): LaneSnapshot => ({
    id, name, state: 'verified', elapsed_ms: elapsed, attempts: 1, status, error: null,
  })
  return {
    ...baseSession('wake_idle_app'),
    lanes: {
      lakebase: lane('lakebase', 'Lakebase', lakebaseMs),
      competitor: lane('competitor', aurora.short_name, competitorMs),
    },
    remembered_result: lakebaseMs < competitorMs ? 'LAKEBASE WINS' : 'AURORA SERVERLESS V2 WINS',
  }
}

/** Round 1's back-to-idle proof: both control planes confirmed zero after the bout. */
function idleSession(competitorZeroMs = 144_900): DemoSession {
  return {
    ...roundOneSession(1_152.861831, 15_298.763181),
    cooldown: {
      mode: 'return_to_idle',
      state: 'ready',
      started_at: '2026-10-13T17:33:31.606288Z',
      failure: null,
      lanes: {
        lakebase: {
          id: 'lakebase', name: 'Lakebase', state: 'confirmed_zero',
          started_at: '2026-10-13T17:33:31.606288Z', confirmed_at: '2026-10-13T17:34:42.000Z',
          elapsed_ms: 70_400, status: 'Control plane confirmed zero',
        },
        competitor: {
          id: 'competitor', name: aurora.short_name, state: 'confirmed_zero',
          started_at: '2026-10-13T17:33:31.606288Z', confirmed_at: '2026-10-13T17:38:35.000Z',
          elapsed_ms: competitorZeroMs, status: 'Control plane confirmed zero',
        },
      },
    },
  }
}

function fanInEvidence(id: 'lakebase' | 'competitor', timeToTargetMs: number, launchSkewMs: number): Record<string, unknown> {
  return {
    schema_version: 4,
    protocol: 'round5-fanin-v4',
    safety_evidence_version: 5,
    hard_safety_verified: true,
    port_accounting_verified: true,
    telemetry_advisories: [],
    initiated_clients: 10_000,
    authenticated_clients: 10_000,
    held_clients_at_gate: 10_000,
    terminal_failures: 0,
    retries: 0,
    disconnected_during_hold: 0,
    time_to_target_ms: timeToTargetMs,
    launch_skew_ms: launchSkewMs,
    hold_elapsed_ms: 30_000.5,
    sampled_queries_attempted: 64,
    sampled_queries_succeeded: 64,
    sampled_queries_failed: 0,
    preexisting_client_role_sessions: 0,
    observer_role: 'anti_demo_observer',
    client_role: 'anti_demo_burst',
    observer_direct: true,
    current_backend_sessions: id === 'lakebase' ? 7 : 11,
    unique_backend_pids: id === 'lakebase' ? 7 : 11,
    peak_backend_sessions: id === 'lakebase' ? 9 : 14,
    distinct_socket_fds: 10_000,
    distinct_local_endpoints: 10_000,
    connect_latency_p50_ms: 80,
    connect_latency_p95_ms: 150,
    auth_method: id === 'lakebase' ? 'tls-cleartext-password' : 'scram-sha-256',
    connect_latency_p99_ms: 201.455,
    telemetry_samples: 49,
    telemetry_physical_memory_bytes: 15 * 1024 ** 3,
    telemetry_min_available_memory_bytes: 7 * 1024 ** 3,
    telemetry_peak_rss_bytes: 7 * 1024 ** 3,
    telemetry_fd_soft_limit: 65_535,
    telemetry_peak_open_fds: 20_264,
    telemetry_ephemeral_port_count: 28_232,
    telemetry_min_ephemeral_port_reserve: 18_232,
    telemetry_peak_event_loop_p99_ms: 4.5,
    telemetry_peak_raw_event_loop_p99_ms: 6.25,
    telemetry_peak_external_event_loop_p99_ms: 6.25,
    telemetry_peak_cpu_capacity_fraction: 0.31,
    telemetry_failures: [],
    telemetry_verified: true,
  }
}

function runtimeLane(id: 'lakebase' | 'competitor', bellToTenThousandMs: number, rampMs: number): RoundFiveRuntimeLane {
  return {
    id,
    phase: 'verified',
    elapsed_at_snapshot_ms: bellToTenThousandMs,
    bell_to_10000_observed_ms: bellToTenThousandMs,
    observation_uncertainty_ms: 90,
    pooled_path_ready_observed_ms: bellToTenThousandMs - rampMs,
    ramp_started_observed_ms: 5,
    ramp_time_to_10000_ms: rampMs,
    clients_initiated: 10_000,
    clients_authenticated: 10_000,
    held_clients: 10_000,
    peak_clients_authenticated: 10_000,
    peak_held_clients: 10_000,
    sampled_queries_succeeded: 64,
    status: 'Exact 10,000-client retained gate verified',
  }
}

/**
 * Receipt A21B1384 (2026-10-14), as the live server sent it: the lanes started 0.017 ms
 * apart after the bell, and Aurora's own four shards 5.270 ms apart.
 */
function roundFiveBellSession(): DemoSession {
  const lane = (id: 'lakebase' | 'competitor', name: string, ms: number, launchSkewMs: number): LaneSnapshot => ({
    id, name, state: 'verified', elapsed_ms: ms, attempts: 10_000,
    status: 'Exact 10,000-client fan-in verified', error: null,
    evidence: fanInEvidence(id, ms, launchSkewMs),
  })
  const setupLane = (id: 'lakebase' | 'competitor', name: string, setupMs: number) => ({
    id,
    name,
    state: 'verified' as const,
    setup_elapsed_ms: setupMs,
    status: 'Setup stop gate verified',
    stop_gate_evidence: {
      gate_id: id === 'lakebase' ? 'native_transaction' : 'proxy_transaction',
      expected: [{ key: 'transaction_verified', value: true }],
      observed: [{ key: 'transaction_verified', value: true }],
      exact: true,
    },
    verified: true,
    workflow_launch_delay_ms: 12.25,
  })
  return {
    ...baseSession('survive_connection_spike'),
    lanes: {
      lakebase: lane('lakebase', 'Lakebase', 13_334.124126, 1.926112),
      competitor: lane('competitor', 'Aurora Serverless v2 + RDS Proxy', 36_982.623966, 5.269954),
    },
    fairness: {
      same_client: true,
      same_transaction: true,
      same_nonce: true,
      launch_skew_ms: 5.269954,
      warmup_connections: 0,
      concurrency: 10_000,
      runner: 'Python 3.12 event-driven TLS/native-password',
      tls: 'verify-full',
      timeout: '20s connect · 600s run',
      protocol: 'round5-fanin-v4',
      target_clients_per_lane: 10_000,
      hold_seconds: 30,
      sampled_queries_per_lane: 64,
      max_retries: 100,
    },
    comparison: {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bell_to_10000_observed_ms', lane_id: 'lakebase', value: 691_901.937001, display_value: '691901.94 ms' },
      detail: 'Lakebase reached exactly 10,000 held clients sooner.',
    },
    round5_setup: {
      schema_version: 4,
      protocol: 'round5-fanin-v4',
      state: 'verified',
      workflow_launch_skew_ms: 0.01662,
      lanes: {
        lakebase: setupLane('lakebase', 'Lakebase', 15.04334),
        competitor: setupLane('competitor', 'Aurora Serverless v2 + RDS Proxy', 668_051.465558),
      },
      setup_validated: true,
      downstream_validated: true,
      cleanup_retryable: false,
    },
    round5_runtime: {
      protocol: 'round5-bell-to-10k-v4',
      warm_generation: 7,
      bell_id: 'bell-a21b1384',
      revision: 40,
      state: 'verified',
      bell_at_utc: '2026-10-14T17:42:55.578700Z',
      lanes: {
        lakebase: runtimeLane('lakebase', 14_079.423694, 13_334.124126),
        competitor: runtimeLane('competitor', 705_981.360695, 36_982.623966),
      },
    },
    remembered_result: 'LAKEBASE REACHED 10,000 · 691.90s SOONER',
  }
}

/** The same bout with the towel thrown during the hold: the receipt is the scorecard. */
function towelledRoundFiveSession(): DemoSession {
  const session = roundFiveBellSession()
  session.state = 'towelled'
  session.comparison = null
  session.remembered_result = null
  session.round5_runtime!.state = 'towelled'
  session.round5_runtime!.lanes.lakebase = {
    ...session.round5_runtime!.lanes.lakebase,
    phase: 'cancelled',
    sampled_queries_succeeded: 41,
    status: 'Towel thrown during the 30-second hold',
  }
  session.round5_runtime!.lanes.competitor = {
    ...session.round5_runtime!.lanes.competitor,
    phase: 'cancelled',
    bell_to_10000_observed_ms: null,
    elapsed_at_snapshot_ms: 23_890,
    clients_initiated: 8_400,
    clients_authenticated: 8_400,
    held_clients: 8_400,
    peak_clients_authenticated: 8_400,
    peak_held_clients: 8_400,
    sampled_queries_succeeded: 0,
    status: 'Towel thrown before reaching 10,000 clients',
  }
  session.lanes.lakebase = { ...session.lanes.lakebase, state: 'towelled' }
  session.lanes.competitor = { ...session.lanes.competitor, state: 'towelled' }
  session.towel = {
    state: 'cleaning',
    requested_at: '2026-10-14T17:43:40.000Z',
    censored_lower_bounds_ms: { competitor: 23_890 },
    restore_started: false,
    cleanup_failure: null,
  }
  return session
}

describe('Round 5: every surface prints the gap between the lanes as the start gap', () => {
  it('the fixture is the verified bout both card layouts draw', () => {
    const receipt = receiptPresentation(roundFiveBellSession(), 'round')
    expect(receipt.healthBars, 'health bars').toBeDefined()
    expect(receipt.knockout, 'knockout').toBeDefined()
    expect(receipt.knockout!.hero).toBe('50×')
  })

  for (const style of STYLES) {
    for (const theme of THEMES) {
      it(`${style} · ${theme}: START GAP 0.017ms, the AWS lane names its Proxy, nothing collides`, async () => {
        const card = await drawCard(roundFiveBellSession(), 5, 'round', style, theme)
        const words = joined(card)
        // Receipt A21B1384's health-bars card printed 5.270ms: Aurora's own shard spread.
        expect(words).toContain('START GAP 0.017ms')
        expect(words).not.toMatch(/START GAP 5\.270/)
        expect(words).toContain('AURORA SERVERLESS V2 + RDS PROXY')
        expectNoTextCollisions(card)
      })
    }
  }

  it('the towelled scorecard: the same start gap, a V4 fair-start line, nothing collides', async () => {
    const card = await drawCard(towelledRoundFiveSession(), 5, 'round', 'health-bars', 'classic')
    const words = joined(card)
    expect(words).toContain('FAIR-START CONTRACT')
    expect(words).toContain('START GAP 0.017ms')
    expect(words).not.toMatch(/START GAP 5\.270/)
    expect(words).toContain('LANES STARTED 0.017 MS APART')
    expect(words).not.toMatch(/POST-READINESS/)
    expect(words).toContain('AURORA SERVERLESS V2 + RDS PROXY')
    expectNoTextCollisions(card)
  })

  it('the caption says the lanes started 0.017 ms apart and nothing about a shared post-readiness T0', () => {
    const caption = linkedInReceipt(roundFiveBellSession(), 5)
    expect(caption).toContain('One server bell · Lanes started 0.017 ms apart · Each clock runs to exactly 10,000 authenticated held clients')
    expect(caption).toContain("Each lane's shards launched within 5.270 ms")
    expect(caption).not.toMatch(/post-readiness/i)
    expect(caption).not.toMatch(/fan-in launch skew/i)
  })

  it('the poster and the replay agree with the cards', async () => {
    render(<ShareReceipt session={roundFiveBellSession()} roundNumber={5} kind="round" onClose={() => undefined} />)
    const poster = await screen.findByLabelText('Verified result poster preview')
    expect(poster).toHaveTextContent(/Start gap 0\.017ms/)
    expect(poster).not.toHaveTextContent(/Start gap 5\.270/)
    expect(poster).toHaveTextContent(/Lanes started 0\.017 ms apart/)
    expect(poster).not.toHaveTextContent(/post-readiness/i)
    cleanup()

    render(<InstantReplay session={roundFiveBellSession()} roundNumber={5} onClose={() => undefined} />)
    const replay = screen.getByRole('dialog')
    const startGap = within(replay).getByText('Start gap').parentElement
    expect(startGap).toHaveTextContent(/0\.017ms/)
    expect(startGap).not.toHaveTextContent(/5\.270/)
  })
})

describe('the health bars keep a long status on one line', () => {
  // A 65-character status beside the winner's clock or inside its bar used to wrap into
  // two or three rows of 7 px type wherever the bar was of middling length.
  for (const theme of THEMES) {
    for (const share of [0.1, 0.2, 0.3, 0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.9, 0.95]) {
      it(`${theme} · winner at ${share} of the loser's clock`, async () => {
        const loserMs = 74_100
        const session = roundOneSession(Math.round(loserMs * share), loserMs, ROUND_SIX_STATUS)
        const card = await drawCard(session, 1, 'round', 'health-bars', theme)
        const status = ROUND_SIX_STATUS.toUpperCase()
        const statusLines = texts(card).filter((operation) => status.includes(operation.text) && operation.text.length > 3)
        expect(statusLines.map((operation) => operation.text)).toEqual([status, status])
        expectNoTextCollisions(card)
      })
    }
  }
})

describe('a long knockout hero stays in its column', () => {
  it('a 100.30s margin under 2× fits the Halloween 548px column and the classic 700px one', async () => {
    const session = roundOneSession(110_200, 210_500)
    expect(receiptPresentation(session, 'round').knockout!.hero).toBe('100.30s')

    const halloween = await drawCard(session, 1, 'round', 'knockout', 'halloween')
    const halloweenHero = texts(halloween).filter((operation) => operation.text === '100.30s' && canvasFontSize(operation.font) >= 70)
    expect(halloweenHero).toHaveLength(3)
    for (const layer of halloweenHero) {
      expect(measuredTextWidth(layer.text, layer.font)).toBeLessThanOrEqual(548)
    }
    expectNoTextCollisions(halloween)

    const classic = await drawCard(session, 1, 'round', 'knockout', 'classic')
    const classicHero = texts(classic).filter((operation) => operation.text === '100.30s' && canvasFontSize(operation.font) >= 100)
    expect(classicHero).toHaveLength(2)
    for (const layer of classicHero) {
      expect(measuredTextWidth(layer.text, layer.font)).toBeLessThanOrEqual(700)
    }
    expectNoTextCollisions(classic)
  })

  it('an existing hero keeps its size', async () => {
    // 3.2× (receipt 9625AA03) was drawn at 130px, and 50× (A21B1384) at 150px.
    const roundFour = await drawCard(roundOneSession(29_010, 95_151), 1, 'round', 'knockout', 'halloween')
    expect(texts(roundFour).filter((operation) => operation.text === '3.2×').map((operation) => canvasFontSize(operation.font)))
      .toEqual([130, 130, 130])
    const roundFive = await drawCard(roundFiveBellSession(), 5, 'round', 'knockout', 'halloween')
    expect(texts(roundFive).filter((operation) => operation.text === '50×').map((operation) => canvasFontSize(operation.font)))
      .toEqual([150, 150, 150])
    expectNoTextCollisions(roundFour)
    expectNoTextCollisions(roundFive)
  })
})

/** A declared Round 4 or Round 6 bout: the server's comparison, both lanes read exactly. */
function stackRoundSession(
  roundId: 'put_model_score_in_app' | 'analyze_live_orders_without_slowing_checkout',
  lakebaseMs: number,
  competitorMs: number,
  status: string,
  comparison: DemoSession['comparison'] = {
    kind: 'measured',
    winner_lane_id: lakebaseMs < competitorMs ? 'lakebase' : 'competitor',
    margin: { spec_id: 'bout_elapsed_ms', lane_id: 'lakebase', value: Math.abs(competitorMs - lakebaseMs), display_value: '' },
    detail: '',
  },
): DemoSession {
  return {
    ...roundOneSession(lakebaseMs, competitorMs, status),
    round: FALLBACK_CATALOG.rounds.find((round) => round.id === roundId)!,
    // One bell starts both integrations, so neither round records a gap between starts.
    fairness: { same_client: true, same_transaction: true, same_nonce: true, launch_skew_ms: null },
    comparison,
  }
}

describe('Rounds 4 and 6 name each lane by its stack on every card', () => {
  const rounds = [
    {
      name: 'Round 4',
      session: () => stackRoundSession('put_model_score_in_app', 29_010.580272, 95_151.084538, 'The exact row is in the application'),
      roundNumber: 4,
      lakebase: 'LAKEBASE SYNCED TABLE (COLD START)',
      competitor: 'AWS GLUE TO AURORA SERVERLESS V2 (COLD START)',
    },
    {
      name: 'Round 6',
      session: () => stackRoundSession('analyze_live_orders_without_slowing_checkout', 8_200, 48_000, ROUND_SIX_STATUS),
      roundNumber: 6,
      lakebase: 'LAKEBASE BUILT-IN CHANGE FEED',
      competitor: 'AWS DMS + GLUE FROM AURORA SERVERLESS V2 (COLD START)',
    },
  ]
  for (const round of rounds) {
    it(`${round.name}: the fixture is the declared bout both layouts draw`, () => {
      const receipt = receiptPresentation(round.session(), 'round')
      expect(receipt.healthBars).toBeDefined()
      expect(receipt.knockout).toBeDefined()
    })
    for (const style of STYLES) {
      for (const theme of THEMES) {
        it(`${round.name} · ${style} · ${theme}: stacks named, one bell, no arrow glyph, nothing collides`, async () => {
          const card = await drawCard(round.session(), round.roundNumber, 'round', style, theme)
          const words = joined(card)
          expect(words).toContain(round.lakebase)
          expect(words).toContain(round.competitor)
          // Press Start 2P has no "→": a canvas would draw it in another font mid-word.
          expect(words).not.toContain('→')
          expect(words).toContain('BOTH CLOCKS FROM ONE BELL')
          expect(words).not.toMatch(/START GAP N\/A/)
          expectNoTextCollisions(card)
        })
      }
    }
    it(`${round.name}: a tie's scorecard chips both lanes clear of their stack names`, async () => {
      const tie = round.session()
      tie.comparison = { kind: 'tie', winner_lane_id: null, margin: null, detail: '' }
      expect(receiptPresentation(tie, 'round').healthBars).toBeUndefined()
      const card = await drawCard(tie, round.roundNumber, 'round', 'health-bars', 'classic')
      const words = joined(card)
      expect(words).toContain(round.lakebase)
      expect(words).toContain(round.competitor)
      // Both lanes are chipped, and neither is the winner of a tie.
      expect(texts(card).filter((operation) => operation.text === 'TIED')).toHaveLength(2)
      expect(texts(card).some((operation) => operation.text === 'WINNER')).toBe(false)
      expectNoTextCollisions(card)
    })
  }
})

describe('the back-to-idle card', () => {
  for (const style of STYLES) {
    it(`${style} · halloween: its own name, and the audit line on one line`, async () => {
      const card = await drawCard(idleSession(), 1, 'idle', style, 'halloween')
      const words = joined(card)
      expect(words).toContain('BACK TO THE GRAVE')
      expect(words).not.toContain('WAKE THE DEAD')
      const audit = texts(card).filter((operation) => operation.text.includes('2026-10-13T17:38:35.000Z'))
      expect(audit.map((operation) => operation.text)).toEqual([
        'RESET 2026-10-13T17:33:31.606288Z · 2026-10-13T17:38:35.000Z',
      ])
      expectNoTextCollisions(card)
    })
  }

  it('classic cards draw it without a collision too', async () => {
    for (const style of STYLES) {
      expectNoTextCollisions(await drawCard(idleSession(), 1, 'idle', style, 'classic'))
    }
  })
})

describe('the share dialog redraws the card only when the receipt changes', () => {
  it('a new object with the same receipt is not a new card; a cooldown change is', async () => {
    const idle = idleSession()
    const { rerender } = render(<ShareReceipt session={idle} roundNumber={1} kind="idle" onClose={() => undefined} />)
    await waitFor(() => expect(getCanvasRecordings()).toHaveLength(1))

    // A reconcile's copy of the same snapshot: a new object, the same receipt.
    rerender(<ShareReceipt session={structuredClone(idle)} roundNumber={1} kind="idle" onClose={() => undefined} />)
    rerender(<ShareReceipt session={structuredClone(idle)} roundNumber={1} kind="idle" onClose={() => undefined} />)
    await act(async () => {
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(getCanvasRecordings()).toHaveLength(1)

    // A cooldown event changes what the idle card draws and keeps `updated_at`.
    const later = idleSession(150_100)
    expect(later.updated_at).toBe(idle.updated_at)
    rerender(<ShareReceipt session={later} roundNumber={1} kind="idle" onClose={() => undefined} />)
    await waitFor(() => expect(getCanvasRecordings()).toHaveLength(2))
    expect(joined(getCanvasRecordings()[1])).not.toBe(joined(getCanvasRecordings()[0]))
  })
})
