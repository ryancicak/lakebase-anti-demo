import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { useState } from 'react'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it } from 'vitest'

import { InstantReplay } from './App'
import type { DemoSession, RoundId } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { replayTape, replayTapeWordCount } from './instant-replay'
import { buildRingsideResult } from './ringside-cues'

afterEach(cleanup)

const ROUND_NUMBERS: Record<RoundId, number> = {
  wake_idle_app: 1,
  make_schema_change_safely: 2,
  recover_deleted_order: 3,
  put_model_score_in_app: 4,
  survive_connection_spike: 5,
  analyze_live_orders_without_slowing_checkout: 6,
}

function verifiedSession(roundId: RoundId): DemoSession {
  const round = FALLBACK_CATALOG.rounds.find((candidate) => candidate.id === roundId)!
  const competitor = FALLBACK_CATALOG.competitors[0]
  const primary = FALLBACK_CATALOG.personas[0]
  const session: DemoSession = {
    id: `replay-${roundId}`,
    state: 'verified',
    created_at: '2026-09-02T20:00:00Z',
    updated_at: '2026-09-02T20:01:00Z',
    competitor,
    primary_persona: primary,
    secondary_personas: [],
    corners: ['performance'],
    round,
    recommendation_reason: 'Replay fixture',
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
        elapsed_ms: 2_640,
        attempts: 1,
        status: 'Exact proof verified',
        error: null,
      },
      competitor: {
        id: 'competitor',
        name: competitor.short_name,
        state: 'verified',
        elapsed_ms: 9_800,
        attempts: 1,
        status: 'Exact proof verified',
        error: null,
      },
    },
    fairness: {
      same_client: true,
      same_transaction: true,
      same_nonce: true,
      launch_skew_ms: 1.25,
    },
    comparison: {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bout_elapsed_ms', value: 7_160 },
    },
    remembered_result: 'RESULT VERIFIED',
    failure: null,
  }

  if (roundId === 'put_model_score_in_app') {
    // Both lanes cold at the bell, each timed to its own first exact read.
    const row = {
      primary_key: 'customer-42',
      score: 0.81,
      model_version: 'risk-v1',
      proof_nonce: 'replay-round-four-nonce',
      delta_version: 11,
      reads: 126,
      max_read_gap_ms: 251,
    }
    session.lanes.lakebase.elapsed_ms = 31_500
    session.lanes.lakebase.status = 'The exact row is in the application'
    session.lanes.lakebase.evidence = {
      ...row,
      pipeline_update: 'update-1',
      managed_availability_ms: 27_550,
    }
    session.lanes.competitor.elapsed_ms = 77_800
    session.lanes.competitor.status = 'The exact row is in the application'
    session.lanes.competitor.evidence = {
      ...row,
      reads: 311,
      glue_run: 'jr_replay',
      glue_starting_version: '11',
    }
    session.metrics = [
      { spec_id: 'bell_to_exact_read_ms', lane_id: 'lakebase', value: 31_500 },
      { spec_id: 'bell_to_exact_read_ms', lane_id: 'competitor', value: 77_800 },
      { spec_id: 'managed_availability_ms', lane_id: 'lakebase', value: 27_550 },
      { spec_id: 'exact_row_verified', lane_id: 'lakebase', value: true },
      { spec_id: 'exact_row_verified', lane_id: 'competitor', value: true },
      { spec_id: 'delta_commit_version', value: 11 },
    ]
    session.comparison = {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bell_to_exact_read_ms', lane_id: 'lakebase', value: 46_300 },
    }
    session.remembered_result = 'LAKEBASE WINS · MARGIN 46.3s'
  }

  if (roundId === 'survive_connection_spike') {
    // The fan-in protocol: exactly 10,000 authenticated clients held per lane, a
    // 30-second hold, 64 sparse samples, and up to 100 retried logins. `elapsed_ms` carries the
    // scored quantity here -- shared-T0 time to 10,000 -- because that is the
    // primary result, with pooled-path setup as supporting evidence.
    const fanInEvidence = (elapsedMs: number, authMethod: string) => ({
      protocol: 'round5-fanin-v2',
      schema_version: 2,
      initiated_clients: 10_000,
      authenticated_clients: 10_000,
      held_clients_at_gate: 10_000,
      terminal_failures: 0,
      retries: 0,
      disconnected_during_hold: 0,
      time_to_target_ms: elapsedMs,
      hold_elapsed_ms: 30_000.001,
      sampled_queries_attempted: 64,
      sampled_queries_succeeded: 64,
      sampled_queries_failed: 0,
      // Backends must be far fewer than clients: that gap is the multiplexing
      // proof, and it is why 10,000 client connections is not 10,000 sessions.
      unique_backend_pids: 19,
      current_backend_sessions: 5,
      peak_backend_sessions: 37,
      // The observer watches from its own role over its own direct connection, so
      // what it counts cannot be an artefact of the pool it is measuring.
      preexisting_client_role_sessions: 0,
      observer_role: 'anti_demo_observer',
      client_role: 'anti_demo_burst',
      observer_direct: true,
      auth_method: authMethod,
      // One socket and one local endpoint per client: 9,999 distinct sockets under
      // 10,000 authenticated clients would mean a client was counted twice.
      distinct_socket_fds: 10_000,
      distinct_local_endpoints: 10_000,
      telemetry_verified: true,
      telemetry_failures: [],
    })
    session.lanes.lakebase = {
      ...session.lanes.lakebase,
      elapsed_ms: 12_350,
      status: '10,000 authenticated held clients',
      evidence: fanInEvidence(12_350, 'tls-cleartext-password'),
    }
    session.lanes.competitor = {
      ...session.lanes.competitor,
      name: 'Aurora Serverless v2 + RDS Proxy',
      elapsed_ms: 24_000,
      status: '10,000 authenticated held clients',
      evidence: fanInEvidence(24_000, 'scram-sha-256'),
    }
    session.fairness = {
      same_client: true,
      same_transaction: true,
      same_nonce: true,
      launch_skew_ms: 0.155,
      protocol: 'round5-fanin-v2',
      // Zero warmups under fan-in: every one of the 10,000 clients is scored, so a
      // warmed connection would be a client that did not have to race.
      warmup_connections: 0,
      concurrency: 10_000,
      target_clients_per_lane: 10_000,
      sampled_queries_per_lane: 64,
      hold_seconds: 30,
      max_retries: 0,
      runner: 'Python 3.12 event-driven TLS/native-password',
      tls: 'verify-full',
      timeout: '30 seconds',
    }
    const gate = {
      gate_id: 'transaction',
      expected: [{ key: 'verified', value: true }],
      observed: [{ key: 'verified', value: true }],
      exact: true,
    }
    session.round5_setup = {
      state: 'verified',
      workflow_launch_skew_ms: 1.761,
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
          setup_elapsed_ms: 2_640,
          status: 'Built-in pool ready',
          stop_gate_evidence: gate,
          verified: true,
        },
        competitor: {
          id: 'competitor',
          name: 'Aurora Serverless v2 + RDS Proxy',
          state: 'verified',
          setup_elapsed_ms: 693_050,
          status: 'New RDS Proxy ready',
          stop_gate_evidence: gate,
          verified: true,
        },
      },
    }
    // The scored quantity under fan-in is shared-T0 time to 10,000 held clients,
    // not pooled-path setup. 24.00s - 12.35s.
    session.comparison = {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'time_to_10000_ms', value: 11_650 },
    }
  }

  if (roundId === 'analyze_live_orders_without_slowing_checkout') {
    // AWS DMS and Glue cold at the bell, Lakebase's feed built in; each lane timed to
    // its own first exact read of the order in its own Delta history.
    const order = {
      order_id: 'order-42',
      total_cents: 8_450,
      total_display: '$84.50',
      proof_nonce: 'r6-bout-replay-nonce',
      checkout_guardrail_order_id: 'order-43',
      checkout_guardrail_commit_ms: 10,
      checkout_guardrail_read_ms: 4,
      max_read_gap_ms: 1_004,
    }
    session.lanes.lakebase.elapsed_ms = 1_230
    session.lanes.lakebase.status = 'The exact order is in the lakehouse · separate checkout committed'
    session.lanes.lakebase.evidence = { ...order, reads: 2, commit_ack_ms: 12, history_lsn: 42 }
    session.lanes.competitor.elapsed_ms = 74_100
    session.lanes.competitor.status = 'The exact order is in the lakehouse · separate checkout committed'
    session.lanes.competitor.evidence = {
      ...order,
      reads: 75,
      commit_ack_ms: 15,
      competitor: 'aurora',
      glue_run: 'jr_replay',
      dms_commit_ts: '2026-09-02 20:00:01',
      glue_applied_at: '2026-09-02 20:01:14',
      glue_run_started_on: '2026-09-02T20:00:02+00:00',
    }
    session.metrics = [
      { spec_id: 'bell_to_exact_history_ms', lane_id: 'lakebase', value: 1_230 },
      { spec_id: 'bell_to_exact_history_ms', lane_id: 'competitor', value: 74_100 },
      { spec_id: 'exact_order_verified', lane_id: 'lakebase', value: true },
      { spec_id: 'exact_order_verified', lane_id: 'competitor', value: true },
      { spec_id: 'checkout_verified', lane_id: 'lakebase', value: true },
      { spec_id: 'checkout_verified', lane_id: 'competitor', value: true },
      { spec_id: 'commit_skew_ms', value: 3, display_value: '3 ms' },
    ]
    session.comparison = {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bell_to_exact_history_ms', lane_id: 'lakebase', value: 72_870 },
    }
    session.remembered_result = 'LAKEBASE WINS · MARGIN 72.9s'
  }
  return session
}

function partialRecovery(): DemoSession {
  const session = verifiedSession('recover_deleted_order')
  session.state = 'towelled'
  session.lanes.competitor.state = 'towelled'
  session.lanes.competitor.elapsed_ms = null
  session.towel = {
    state: 'ready',
    requested_at: session.updated_at,
    cutoff_ms: 90_000,
    censored_lower_bounds_ms: { competitor: 90_000 },
    restore_started: true,
    cleanup_failure: null,
  }
  session.comparison = {
    kind: 'adjudicated_stoppage',
    winner_lane_id: 'lakebase',
    margin: null,
  }
  return session
}

function noResultRecovery(): DemoSession {
  const session = partialRecovery()
  session.lanes.lakebase.state = 'towelled'
  session.lanes.lakebase.elapsed_ms = null
  session.towel!.censored_lower_bounds_ms = { lakebase: 90_000, competitor: 90_000 }
  session.comparison = null
  return session
}

function partialRoundFive(): DemoSession {
  const session = verifiedSession('survive_connection_spike')
  session.state = 'towelled'
  session.lanes.competitor.state = 'towelled'
  session.lanes.competitor.evidence = undefined
  session.round5_setup = {
    ...session.round5_setup!,
    state: 'towelled',
    workflow_launch_skew_ms: null,
    setup_validated: false,
    downstream_validated: false,
    lanes: {
      ...session.round5_setup!.lanes,
      competitor: {
        ...session.round5_setup!.lanes.competitor!,
        state: 'towelled',
        setup_elapsed_ms: 60_840,
        status: 'Stopped before readiness verified',
        stop_gate_evidence: null,
        verified: false,
      },
    },
  }
  session.towel = {
    state: 'ready',
    requested_at: session.updated_at,
    censored_lower_bounds_ms: { competitor: 60_840 },
    restore_started: false,
    cleanup_failure: null,
  }
  session.comparison = null
  return session
}

/** Round 6 when the AWS lane errored: Lakebase read its order, and nothing was measured. */
function roundSixErroredLane(): DemoSession {
  const session = verifiedSession('analyze_live_orders_without_slowing_checkout')
  session.state = 'failed'
  session.lanes.competitor = {
    ...session.lanes.competitor,
    state: 'failed',
    elapsed_ms: null,
    status: 'Could not be measured',
    error: 'The Glue run ended FAILED: no error message',
  }
  session.comparison = { kind: 'not_comparable', winner_lane_id: null, margin: null }
  session.remembered_result = null
  return session
}

/** Round 6 on an installation without its AWS lane: Lakebase races alone. */
function roundSixWithoutAwsLane(): DemoSession {
  const session = verifiedSession('analyze_live_orders_without_slowing_checkout')
  session.lanes.competitor = {
    ...session.lanes.competitor,
    state: 'not_supported',
    elapsed_ms: null,
    status: 'AWS lane not installed on this installation',
    evidence: { unsupported_reason: "Round 6's AWS lane is not installed on this installation, so only Lakebase ran." },
  }
  session.metrics = session.metrics!.filter((metric) => metric.lane_id === 'lakebase')
  session.comparison = { kind: 'capability_gap', winner_lane_id: 'lakebase', margin: null }
  session.remembered_result = 'LAKEBASE 1.2s · AWS LANE NOT INSTALLED'
  return session
}

/** Round 4 when the AWS lane errored: Lakebase read its row, and nothing was measured. */
function roundFourErroredLane(): DemoSession {
  const session = verifiedSession('put_model_score_in_app')
  session.state = 'failed'
  session.lanes.competitor = {
    ...session.lanes.competitor,
    state: 'failed',
    elapsed_ms: null,
    status: 'Could not be measured',
    error: 'The Glue run ended FAILED: AccessDenied',
  }
  session.comparison = { kind: 'not_comparable', winner_lane_id: null, margin: null }
  session.remembered_result = null
  return session
}

/** Round 4 on an installation without its AWS lane: Lakebase races alone. */
function roundFourWithoutAwsLane(): DemoSession {
  const session = verifiedSession('put_model_score_in_app')
  session.lanes.competitor = {
    ...session.lanes.competitor,
    state: 'not_supported',
    elapsed_ms: null,
    status: 'AWS lane not installed on this installation',
    evidence: { unsupported_reason: "Round 4's AWS lane is not installed on this installation, so only Lakebase ran." },
  }
  session.comparison = { kind: 'capability_gap', winner_lane_id: 'lakebase', margin: null }
  session.remembered_result = 'LAKEBASE 31.5s · AWS LANE NOT INSTALLED'
  return session
}

describe('replayTape', () => {
  it.each([
    ['wake_idle_app', /^Bell\. Both databases are asleep, and the same connection goes in on both\.$/, /Awake\. The same transaction committed and read back\. Clock stops\./],
    ['make_schema_change_safely', /each makes its own copy to test the change on/, /Made a branch of the source, ran the schema change on it.*proved the source untouched\. Clock stops\./],
    ['recover_deleted_order', /same order is deleted on both sides at the same instant/, /Opened a branch at the recovery point.*source still showed it gone\. Clock stops\./],
    ['put_model_score_in_app', /^Bell\. One new score lands in the lakehouse: 0\.81 for customer-42 at Delta version 11\. Both pipelines start cold\.$/, /The app read the new score\. Clock stops\./],
    ['survive_connection_spike', /race to hold 10,000 client connections at once.*build an RDS Proxy first/, /10,000 clients held 12\.35s after the shared start/],
    ['analyze_live_orders_without_slowing_checkout', /^Bell\. The same \$84\.50 checkout commits on both databases\. DMS and Glue start cold; Lakebase's feed is already on\./, /The order shows up in the lakehouse\. Clock stops\./],
  ] as const)('opens %s on the bell and calls each lane in clock order', (roundId, opening, laneCall) => {
    const session = verifiedSession(roundId)
    const tape = replayTape(session)
    expect(tape.state).toBe('verified')
    expect(tape.calls[0]).toMatchObject({ who: 'Bell', atMs: 0, lane: 'bell' })
    expect(tape.calls[0].text).toMatch(opening)
    expect(tape.calls.map((call) => call.text).join(' ')).toMatch(laneCall)
    // Timed calls read in clock order; untimed ones follow in the order they were made.
    const timed = tape.calls.flatMap((call) => (call.atMs === null ? [] : [call.atMs]))
    expect([...timed].sort((left, right) => left - right)).toEqual(timed)
    // The final call is the one sentence Explain to the Room opens with.
    expect(tape.finalCall).toBe(buildRingsideResult(session))
  })

  it('uses the Round 5 receipt values once on the scoreboard and keeps a shared start off the bell clock', () => {
    const tape = replayTape(verifiedSession('survive_connection_spike'))
    expect(tape.metrics).toEqual([
      {
        laneId: 'lakebase',
        label: 'Lakebase time to 10,000',
        value: '12.35s',
        note: 'Exact authenticated held-client gate',
      },
      {
        laneId: 'competitor',
        label: 'Selected AWS path time to 10,000',
        value: '24.00s',
        note: 'Exact authenticated held-client gate',
      },
    ])
    // A legacy fan-in bout ran its 10,000 from a shared start after setup, so those
    // times are told after the shared start and never placed on the bell clock.
    const shared = tape.calls.find((call) => call.who === 'Shared start')
    expect(shared?.atMs).toBeNull()
    const fanIn = tape.calls.filter((call) => /after the shared start/.test(call.text))
    expect(fanIn).toHaveLength(2)
    expect(fanIn.every((call) => call.atMs === null)).toBe(true)
    expect(tape.calls.indexOf(shared!)).toBeLessThan(tape.calls.indexOf(fanIn[0]))
  })

  it('races Round 6 lane against lane, only the AWS clock labeled a cold start', () => {
    const tape = replayTape(verifiedSession('analyze_live_orders_without_slowing_checkout'))
    expect(tape.status).toBe('Result verified')
    expect(tape.metrics).toEqual([
      { laneId: 'lakebase', label: 'Lakebase built-in change feed', value: '1.23s', note: 'Bell to exact Delta read' },
      { laneId: 'competitor', label: 'AWS DMS + Glue from Aurora Serverless v2 (cold start)', value: '74.10s', note: 'Bell to exact Delta read' },
    ])
    const copy = [...tape.calls.map((call) => call.text), tape.finalCall].join(' ')
    expect(copy).not.toMatch(/not built or timed|capability|CDC stack|warm/i)
    expect(copy).not.toMatch(/Lakebase[^.]*cold/i)
  })

  it('tells Round 6 without its AWS lane as Lakebase alone, with no race or margin', () => {
    const tape = replayTape(roundSixWithoutAwsLane())
    expect(tape.status).toBe('Capability proved')
    expect(tape.metrics.map((metric) => metric.laneId)).toEqual(['lakebase'])
    expect(tape.calls[0].text).toMatch(/This install has no AWS lane/)
    expect(tape.calls.map((call) => call.text).join(' ')).toMatch(/no AWS clock and nothing to compare/)
    expect(tape.finalCall).toMatch(/nothing to compare it to/)
  })

  it('races Round 4 lane against lane, each clock named by its integration', () => {
    const tape = replayTape(verifiedSession('put_model_score_in_app'))
    expect(tape.status).toBe('Result verified')
    expect(tape.metrics).toEqual([
      { laneId: 'lakebase', label: 'Lakebase synced table (cold start)', value: '31.50s', note: 'Bell to exact app read' },
      { laneId: 'competitor', label: 'AWS Glue → Aurora Serverless v2 (cold start)', value: '77.80s', note: 'Bell to exact app read' },
    ])
    // Lakebase's own sync timestamp is a call of its own, before its app read.
    const calls = tape.calls.filter((call) => call.lane !== 'bell')
    expect(calls.map((call) => call.who)).toEqual(['Lakebase synced table', 'Lakebase synced table', 'AWS Glue → Aurora Serverless v2'])
    expect(calls[0].text).toMatch(/own sync timestamp, shown on its own and never compared/)
    expect(tape.calls.map((call) => call.text).join(' ')).not.toMatch(/not built or timed|capability/i)
  })

  it('tells Round 4 without its AWS lane as Lakebase alone, with no race or margin', () => {
    const tape = replayTape(roundFourWithoutAwsLane())
    expect(tape.status).toBe('Capability proved')
    expect(tape.metrics.map((metric) => metric.laneId)).toEqual(['lakebase'])
    expect(tape.calls[0].text).toMatch(/this install has no AWS lane/)
    expect(tape.finalCall).toMatch(/nothing to compare it to/)
  })

  it.each([
    ['one exact recovery', partialRecovery(), 'partial', /still recovering, so its time is more than .* not a measurement|never finished, so there is a result but no margin/],
    ['no-result recovery', noResultRecovery(), 'no-result', /^No result\./],
    ['one exact Round 5 setup', partialRoundFive(), 'partial', /no winner and no margin/],
    ['Round 4 errored lane', roundFourErroredLane(), 'partial', /never delivered it, so there is a result but no margin/],
    ['Round 6 errored lane', roundSixErroredLane(), 'partial', /never delivered it, so there is a result but no margin/],
  ] as const)('adapts %s without claiming completed proof', (_name, session, state, copy) => {
    const tape = replayTape(session)
    expect(tape.state).toBe(state)
    expect(tape.finalCall).toMatch(copy)
    expect(tape.status).not.toMatch(/^Result verified$|^Capability proved$/)
    expect([...tape.calls.map((call) => call.text), tape.finalCall].join(' ')).not.toMatch(/Clock stops\.[^]*Clock stops\.[^]*Clock stops\./)
  })

  it('keeps every tape inside a reading budget and out of the lab', () => {
    for (const roundId of Object.keys(ROUND_NUMBERS) as RoundId[]) {
      const tape = replayTape(verifiedSession(roundId))
      expect(replayTapeWordCount(tape)).toBeLessThanOrEqual(roundId === 'survive_connection_spike' ? 230 : 160)
      const copy = [...tape.calls.map((call) => call.text), tape.finalCall].join(' ')
      expect(copy).not.toMatch(/persona|test harness|contract test|leverage|delve|game-changer|run-owned|verifier|nonce|multiplexing/i)
      expect(copy).not.toMatch(/[–—]/)
    }
  })
})

describe('InstantReplay', () => {
  it.each(Object.entries(ROUND_NUMBERS) as Array<[RoundId, number]>)(
    'renders the clean %s replay as a scoreboard, a tape, and one evidence disclosure',
    (roundId, roundNumber) => {
      const { unmount } = render(
        <InstantReplay
          session={verifiedSession(roundId)}
          roundNumber={roundNumber}
          onClose={() => undefined}
        />,
      )
      const story = screen.getByLabelText('Play-by-play')
      expect(story.querySelectorAll('.replay-primary-metric')).toHaveLength(1)
      expect(story.querySelectorAll('.replay-tape > li').length).toBeGreaterThanOrEqual(3)
      expect(within(story).getByText('Bell')).toBeVisible()
      expect(within(story).getByText('Final call')).toBeVisible()
      expect(screen.getAllByText(/view full evidence/i)).toHaveLength(1)
      unmount()
    },
  )

  it('renders the scoreboard, the tape, and one evidence disclosure with no nested details', () => {
    render(
      <InstantReplay
        session={verifiedSession('survive_connection_spike')}
        roundNumber={5}
        onClose={() => undefined}
      />,
    )
    const story = screen.getByLabelText('Play-by-play')
    expect(story.querySelectorAll('.replay-primary-metric')).toHaveLength(1)
    const scoreboard = within(story).getByLabelText('Primary measured result')
    expect(within(scoreboard).getAllByText('12.35s')).toHaveLength(1)
    expect(within(scoreboard).getAllByText('24.00s')).toHaveLength(1)
    expect(screen.getAllByText(/view full evidence/i)).toHaveLength(1)
    expect(document.querySelectorAll('details')).toHaveLength(1)
    expect(document.querySelector('details details')).toBeNull()
  })

  it('fits a three-digit recovery time to its metric card instead of the viewport', () => {
    const session = verifiedSession('recover_deleted_order')
    session.lanes.lakebase.elapsed_ms = 7_540
    session.lanes.competitor.elapsed_ms = 700_090
    render(
      <InstantReplay
        session={session}
        roundNumber={3}
        onClose={() => undefined}
      />,
    )

    const scoreboard = screen.getByLabelText('Primary measured result')
    expect(within(scoreboard).getByText('7.54s')).toHaveAttribute('data-width', 'standard')
    expect(within(scoreboard).getByText('700.09s')).toHaveAttribute('data-width', 'long')

    const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
      .replace(/\/\*[\s\S]*?\*\//g, '')
    expect(css).toMatch(/\.replay-primary-metric > div\s*\{[^}]*container-type:\s*inline-size/)
    expect(css).toMatch(
      /\.replay-primary-metric strong\[data-width="long"\]\s*\{[^}]*font-size:\s*clamp\(\s*18px\s*,\s*20cqi\s*,\s*28px\s*\)/,
    )
    expect(css).toMatch(
      /\.replay-primary-metric strong\s*\{[^}]*overflow-wrap:\s*anywhere[^}]*white-space:\s*normal/,
    )
  })

  it('keeps calls hidden until the one evidence disclosure opens', () => {
    render(
      <InstantReplay
        session={verifiedSession('survive_connection_spike')}
        roundNumber={5}
        onClose={() => undefined}
      />,
    )
    const details = document.querySelector('details.replay-evidence') as HTMLDetailsElement
    expect(details.open).toBe(false)
    expect(screen.getByText(/journal: rds_proxy/i)).not.toBeVisible()

    fireEvent.click(within(details).getByText(/view full evidence/i))

    expect(details.open).toBe(true)
    expect(screen.getByText(/journal: rds_proxy/i)).toBeVisible()
    expect(screen.getByLabelText('Round 5 detailed proof')).toBeVisible()
  })

  it.each([
    ['partial', partialRecovery(), /stopped · partial proof/i],
    ['no result', noResultRecovery(), /stopped · no result/i],
    ['Round 6 errored lane', roundSixErroredLane(), /partial proof · no result/i],
  ] as const)('renders the %s state without a completed-proof badge', (_name, session, status) => {
    render(
      <InstantReplay
        session={session}
        roundNumber={ROUND_NUMBERS[session.round.id]}
        onClose={() => undefined}
      />,
    )
    expect(screen.getByText(status)).toBeVisible()
    expect(screen.queryByText(/^Result verified$|^Capability proved$/i)).not.toBeInTheDocument()
    expect(screen.getByLabelText('Play-by-play')).toHaveTextContent(/no margin|No result|a floor, not a result|not a measurement/i)
  })

  it.each([
    ['put_model_score_in_app', '31.50s'],
    ['survive_connection_spike', '24.00s'],
    ['analyze_live_orders_without_slowing_checkout', '1.23s'],
  ] as const)('renders %s proof values from its session', (roundId, expected) => {
    const session = verifiedSession(roundId)
    render(
      <InstantReplay
        session={session}
        roundNumber={ROUND_NUMBERS[roundId]}
        onClose={() => undefined}
      />,
    )
    expect(screen.getByLabelText('Primary measured result')).toHaveTextContent(expected)
  })

  it('traps focus, closes on Escape, and restores the replay trigger', async () => {
    function Harness() {
      const [open, setOpen] = useState(false)
      return (
        <>
          <button onClick={() => setOpen(true)}>Open replay</button>
          {open && (
            <InstantReplay
              session={verifiedSession('wake_idle_app')}
              roundNumber={1}
              onClose={() => setOpen(false)}
            />
          )}
        </>
      )
    }

    const user = userEvent.setup()
    render(<Harness />)
    const opener = screen.getByRole('button', { name: 'Open replay' })
    await user.click(opener)
    const summary = screen.getByText(/view full evidence/i).closest('summary')!
    await waitFor(() => expect(summary).toHaveFocus())
    await user.keyboard('{Shift>}{Tab}{/Shift}')
    expect(screen.getByRole('button', { name: /back to the ring/i })).toHaveFocus()
    await user.keyboard('{Tab}')
    expect(summary).toHaveFocus()
    await user.keyboard('{Escape}')
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    await waitFor(() => expect(opener).toHaveFocus())
  })

  it('prevents the dense duplicate-summary layout from returning', () => {
    const source = readFileSync(join(import.meta.dirname, 'App.tsx'), 'utf8')
    const start = source.indexOf('export function InstantReplay')
    const end = source.indexOf('/**\n * Who is at ringside', start)
    const component = source.slice(start, end)
    const visibleStory = component.slice(0, component.indexOf('<details className="replay-evidence">'))
    expect(visibleStory).toContain('className="replay-story"')
    expect(visibleStory).toContain('className="replay-tape"')
    expect(visibleStory).not.toContain('className="replay-lanes"')
    expect(visibleStory).not.toContain('steps.map')
    expect(component.match(/<details\b/g)).toHaveLength(1)
    expect(component).not.toContain('selectedStep')
    expect(component).not.toContain('replay-steps')
  })

  it('pins one scroll owner and narrow-screen stacking at every requested viewport', () => {
    const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
      .replace(/\/\*[\s\S]*?\*\//g, '')
    expect(css).toMatch(/\.replay-modal\s*\{[^}]*overflow-x:\s*hidden[^}]*overflow-y:\s*auto/)
    const narrow = css.slice(css.indexOf('@media (max-width: 759px)', css.indexOf('.replay-overlay')))
    expect(narrow).toMatch(/\.replay-modal\s*\{[^}]*width:\s*100vw[^}]*height:\s*100dvh[^}]*max-height:\s*100dvh/)
    expect(narrow).toMatch(/\.replay-story\s*\{[^}]*grid-template-columns:\s*minmax\(0,\s*1fr\)/)
    expect(narrow).toMatch(/\.replay-tape > li\s*\{[^}]*grid-template-columns:\s*auto minmax\(0,\s*1fr\)/)
    expect(narrow).toMatch(/\.replay-evidence-step > div\s*\{[^}]*grid-template-columns:\s*minmax\(0,\s*1fr\)/)
  })
})
