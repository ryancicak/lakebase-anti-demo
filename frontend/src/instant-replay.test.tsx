import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { useState } from 'react'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it } from 'vitest'

import { InstantReplay } from './App'
import type { DemoSession, RoundId } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { replayStory, replayStoryWordCount } from './instant-replay'

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
    session.lanes.lakebase.elapsed_ms = 840
    session.lanes.lakebase.evidence = {
      primary_key: 'customer-42',
      score: 0.81,
      model_version: 'risk-v1',
      proof_nonce: 'replay-round-four-nonce',
      delta_version: 11,
      verified_row: {
        primary_key: 'customer-42',
        score: 0.81,
        model_version: 'risk-v1',
        proof_nonce: 'replay-round-four-nonce',
      },
    }
    session.metrics = [
      { spec_id: 'managed_availability_ms', lane_id: 'lakebase', value: 640 },
      { spec_id: 'application_proof_elapsed_ms', lane_id: 'lakebase', value: 840 },
      { spec_id: 'exact_row_verified', lane_id: 'lakebase', value: true },
    ]
    session.lanes.competitor.state = 'not_supported'
    session.lanes.competitor.elapsed_ms = null
    session.lanes.competitor.status = 'No AWS reverse-ETL path was built or timed'
    session.comparison = {
      kind: 'capability_gap',
      winner_lane_id: 'lakebase',
      margin: null,
    }
  }

  if (roundId === 'survive_connection_spike') {
    // The fan-in protocol: exactly 10,000 authenticated clients held per lane, a
    // 30-second hold, 64 sparse samples and zero retries. `elapsed_ms` carries the
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
    session.lanes.lakebase.elapsed_ms = 1_230
    session.lanes.lakebase.evidence = {
      sku: 'RED-GLOVE',
      store: 'CHICAGO',
      total_display: '$84.50',
      status: 'COMMITTED',
      order_id: 'order-42',
      proof_nonce: 'replay-round-six-nonce',
      history_lsn: '0/42',
      checkout_commit_ms: 12,
      checkout_guardrail_commit_ms: 10,
      checkout_guardrail_read_ms: 4,
    }
    session.metrics = [
      { spec_id: 'analytics_available_ms', lane_id: 'lakebase', value: 1_230 },
      { spec_id: 'matching_live_orders', lane_id: 'lakebase', value: 1 },
      { spec_id: 'checkout_verified', lane_id: 'lakebase', value: true },
    ]
    session.lanes.competitor.state = 'not_supported'
    session.lanes.competitor.elapsed_ms = null
    session.lanes.competitor.status = 'No AWS CDC stack was built or timed'
    session.comparison = {
      kind: 'capability_gap',
      winner_lane_id: 'lakebase',
      margin: null,
    }
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

function guardrailFailure(roundId: 'put_model_score_in_app' | 'analyze_live_orders_without_slowing_checkout'): DemoSession {
  const session = verifiedSession(roundId)
  session.state = 'failed'
  session.comparison = null
  if (roundId === 'put_model_score_in_app') {
    session.lanes.lakebase.evidence = {
      ...session.lanes.lakebase.evidence,
      verified_row: {
        primary_key: 'customer-42',
        score: 0.33,
        model_version: 'wrong',
        proof_nonce: 'wrong',
      },
    }
  } else {
    session.metrics = session.metrics!.map((metric) => (
      metric.spec_id === 'checkout_verified' ? { ...metric, value: false } : metric
    ))
  }
  return session
}

describe('replayStory', () => {
  it.each([
    ['wake_idle_app', /genuine scale zero/i, /exact run-owned transaction/i, /does not measure the rest of the application/i],
    ['make_schema_change_safely', /isolated environment/i, /same migration.*source was unchanged/i, /production cleanup was not tested/i],
    ['recover_deleted_order', /aged to a recovery point.*deleted/i, /exact deleted order.*source read still proved it absent/i, /not a production failover/i],
    ['put_model_score_in_app', /score 0\.81.*Delta version 11/i, /Managed Reverse ETL.*fresh app connection/i, /no AWS race or margin/i],
    // Fan-in: setup is supporting evidence and the shared-T0 time to 10,000 held
    // clients is the primary result.
    ['survive_connection_spike', /included pool.*selected RDS Proxy separately/i, /raced both from one T0 to exactly 10,000 authenticated held clients.*held 30s.*multiplexing/i, /fan-in time is primary.*setup supports it.*client count is not backend count/i],
    ['analyze_live_orders_without_slowing_checkout', /checkout committed.*RED-GLOVE.*CHICAGO.*\$84\.50/i, /exact order once.*separate checkout/i, /no AWS race or margin/i],
  ] as const)(
    'maps %s to Setup, Same test, and Takeaway',
    (roundId, setup, sameTest, takeaway) => {
      const story = replayStory(verifiedSession(roundId))
      expect(story.state).toBe('verified')
      expect(story.beats.map((beat) => beat.title)).toEqual(['Setup', 'Same test', 'Takeaway'])
      expect(story.beats[0].body).toMatch(setup)
      expect(story.beats[1].body).toMatch(sameTest)
      expect(story.beats[2].body).toMatch(takeaway)
    },
  )

  it('uses the Round 5 receipt values once in one primary treatment', () => {
    const story = replayStory(verifiedSession('survive_connection_spike'))
    expect(story.metricBeat).toBe('setup')
    expect(story.metrics).toEqual([
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
    // The primary metric is the fan-in time, and the takeaway has to say that
    // setup supports it rather than scoring it.
    expect(story.beats[1].body).toMatch(/exactly 10,000 authenticated held clients/i)
    expect(story.beats[2].body).toMatch(/fan-in time is primary.*setup supports it/i)
  })

  it('keeps Rounds 4 and 6 as capability proofs without an AWS race', () => {
    for (const roundId of [
      'put_model_score_in_app',
      'analyze_live_orders_without_slowing_checkout',
    ] as const) {
      const story = replayStory(verifiedSession(roundId))
      expect(story.status).toBe('Capability proved')
      expect(story.beats[2].body).toMatch(/proves the Lakebase.*capability/i)
      expect(story.beats[2].body).toMatch(/no AWS race or margin/i)
    }
  })

  it.each([
    ['one exact recovery', partialRecovery(), 'partial', /did not.*no completed comparison or margin/i],
    ['no-result recovery', noResultRecovery(), 'no-result', /without an exact verified result/i],
    ['one exact Round 5 setup', partialRoundFive(), 'partial', /shared-T0 10,000-client fan-in did not run/i],
    ['Round 4 identity failure', guardrailFailure('put_model_score_in_app'), 'partial', /exact row identity did not verify/i],
    ['Round 6 checkout failure', guardrailFailure('analyze_live_orders_without_slowing_checkout'), 'partial', /checkout guardrail did not verify/i],
  ] as const)('adapts %s without claiming completed proof', (_name, session, state, copy) => {
    const story = replayStory(session)
    expect(story.state).toBe(state)
    expect(`${story.beats[1].body} ${story.beats[2].body}`).toMatch(copy)
    expect(story.status).not.toMatch(/^Result verified$|^Capability proved$/)
  })

  it('keeps every presenter story within the ten-second reading budget', () => {
    const stories = Object.keys(ROUND_NUMBERS).map((roundId) => (
      replayStory(verifiedSession(roundId as RoundId))
    ))
    for (const story of stories) {
      expect(replayStoryWordCount(story)).toBeLessThanOrEqual(82)
      const copy = story.beats.map((beat) => beat.body).join(' ')
      expect(copy).not.toMatch(/persona|test harness|contract test|leverage|delve|game-changer/i)
    }
  })
})

describe('InstantReplay', () => {
  it.each(Object.entries(ROUND_NUMBERS) as Array<[RoundId, number]>)(
    'renders the clean %s replay through the universal three-beat shell',
    (roundId, roundNumber) => {
      const { unmount } = render(
        <InstantReplay
          session={verifiedSession(roundId)}
          roundNumber={roundNumber}
          onClose={() => undefined}
        />,
      )
      const story = screen.getByLabelText('Three-beat replay story')
      expect(story.querySelectorAll('.replay-beat')).toHaveLength(3)
      expect(within(story).getByRole('heading', { name: 'Setup' })).toBeVisible()
      expect(within(story).getByRole('heading', { name: 'Same test' })).toBeVisible()
      expect(within(story).getByRole('heading', { name: 'Takeaway' })).toBeVisible()
      expect(screen.getAllByText(/view full evidence/i)).toHaveLength(1)
      unmount()
    },
  )

  it('renders three visible beats, one primary metric treatment, and one evidence disclosure', () => {
    render(
      <InstantReplay
        session={verifiedSession('survive_connection_spike')}
        roundNumber={5}
        onClose={() => undefined}
      />,
    )
    const story = screen.getByLabelText('Three-beat replay story')
    expect(story.querySelectorAll('.replay-beat')).toHaveLength(3)
    expect(story.querySelectorAll('.replay-primary-metric')).toHaveLength(1)
    expect(within(story).getAllByText('12.35s')).toHaveLength(1)
    expect(within(story).getAllByText('24.00s')).toHaveLength(1)
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

    expect(screen.getByText('7.54s')).toHaveAttribute('data-width', 'standard')
    expect(screen.getByText('700.09s')).toHaveAttribute('data-width', 'long')

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
    ['guardrail failure', guardrailFailure('analyze_live_orders_without_slowing_checkout'), /partial proof · no result/i],
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
    expect(screen.getByLabelText('Three-beat replay story')).toHaveTextContent(
      /did not|without an exact|guardrail did not verify/i,
    )
  })

  it.each([
    ['put_model_score_in_app', '0.84s'],
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
    expect(narrow).toMatch(/\.replay-evidence-step > div\s*\{[^}]*grid-template-columns:\s*minmax\(0,\s*1fr\)/)
  })
})
