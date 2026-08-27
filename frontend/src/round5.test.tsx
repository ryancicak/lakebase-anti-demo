import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { RoundFiveProof } from './App'
import type { DemoSession, LaneSnapshot } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { applyRunEventSnapshot, selectRound4Session } from './round4'

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

function burstLane(id: 'lakebase' | 'competitor', name: string, offset: number): LaneSnapshot {
  return {
    id,
    name,
    state: 'verified',
    elapsed_ms: null,
    attempts: 1,
    status: 'Burst count contract verified',
    error: null,
    evidence: {
      scheduled_clients: 128,
      terminal_clients: 128,
      successful_clients: 128,
      error_clients: 0,
      successful_latency_ms: Array.from({ length: 128 }, (_, index) => index + 0.1234 + offset),
      witness_verified_clients: 64,
      unique_backend_pids: id === 'lakebase' ? 7 : 11,
      peak_backend_sessions: id === 'lakebase' ? 9 : 14,
    },
  }
}

function roundFiveSession(): DemoSession {
  const primary = FALLBACK_CATALOG.personas[0]
  const competitor = FALLBACK_CATALOG.competitors.find((item) => item.id === 'rds_postgres')!
  const round = FALLBACK_CATALOG.rounds.find((item) => item.id === 'survive_connection_spike')!
  return {
    id: 'round-five-session',
    state: 'verified',
    created_at: '2026-08-18T20:00:00Z',
    updated_at: '2026-08-18T20:01:00Z',
    competitor,
    primary_persona: primary,
    secondary_personas: [],
    corners: ['cost', 'performance'],
    round,
    recommendation_reason: 'Focused connection burst proof.',
    presenter_pack: {
      opening: '', discovery_question: '', risk: '', stop_condition: '', remembered_metric: '',
      primary: { persona_id: primary.id, nickname: primary.nickname, role: primary.role, interpretation: '', objection: '', response: '' },
      secondary: [], closing: '',
    },
    lanes: {
      lakebase: burstLane('lakebase', 'Lakebase', 0),
      competitor: burstLane('competitor', 'RDS PostgreSQL + RDS Proxy', 10),
    },
    fairness: {
      same_client: true,
      same_transaction: true,
      same_nonce: true,
      launch_skew_ms: 1.23456,
      warmup_connections: 4,
      concurrency: 64,
      runner: 'Python 3.12 + psycopg 3.3.4',
      tls: 'required',
      timeout: '10 seconds',
    },
    cost_receipt: {
      currency: 'USD',
      region: 'us-west-2',
      price_basis: 'published_on_demand_rates',
      status: 'posted_partial',
      reconciliation_status: 'posted_partial',
      known_bout_estimate_usd: 0.005,
      known_monthly_carrying_cost_usd: 2.7,
      known_installation_overhead_usd: null,
      original_estimate_usd: 0.004,
      posted_cost_usd: 0.005,
      variance_usd: 0.001,
      revision: 2,
      queried_at: '2026-08-20T02:00:00Z',
      posted_through: '2026-08-20T01:45:00Z',
      lines: [
        {
          lane_id: 'lakebase',
          component: 'Lakebase compute',
          quantity: null,
          unit: 'DBU',
          unit_rate_usd: 0.26,
          reference_list_unit_rate_usd: 0.52,
          subtotal_usd: null,
          rate_basis: 'current_promotion',
          cadence: 'usage',
          status: 'usage_pending',
          scope: 'bout_estimate',
          source: 'system.billing.list_prices pricing.effective_list.default; normal pricing.default',
          source_as_of: '2026-08-20T01:35:04Z',
        },
        {
          lane_id: 'competitor',
          component: 'RDS Proxy · provisioned RDS · 2 vCPU · 10-minute minimum (final lifetime pending)',
          quantity: 2 / 6,
          unit: 'vCPU-hour',
          unit_rate_usd: 0.015,
          reference_list_unit_rate_usd: null,
          subtotal_usd: 0.005,
          rate_basis: 'standard_list',
          cadence: 'usage',
          status: 'estimate',
          scope: 'bout_estimate',
          source: 'Amazon RDS Proxy pricing · us-west-2 · per ACU/vCPU-hour · 10-minute minimum',
          source_as_of: '2026-08-18T00:11:58Z',
        },
        {
          lane_id: 'competitor',
          component: 'Secrets Manager API requests',
          quantity: null,
          unit: '10,000 requests',
          unit_rate_usd: 0.05,
          reference_list_unit_rate_usd: null,
          subtotal_usd: null,
          rate_basis: 'standard_list',
          cadence: 'usage',
          status: 'usage_pending',
          scope: 'installation_overhead',
          source: 'AWS Price List API · AWSSecretsManager · OnDemand · us-west-2',
          source_as_of: '2025-08-28T15:38:04Z',
        },
        {
          lane_id: 'shared',
          component: 'Provider adjustment',
          quantity: null,
          unit: 'usage',
          unit_rate_usd: null,
          reference_list_unit_rate_usd: null,
          subtotal_usd: null,
          rate_basis: 'standard_list',
          cadence: 'usage',
          status: 'usage_pending',
          scope: 'bout_estimate',
          source: 'Provider reconciliation pending',
          source_as_of: '2026-08-20T02:00:00Z',
        },
      ],
      note: 'Current Lakebase compute promotion has no published end date; revalidate before presenting.',
    },
    comparison: {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'setup_elapsed_ms', lane_id: 'lakebase', value: 11_654.322, display_value: '11654.32 ms' },
      detail: 'Lakebase completed the verified setup sooner.',
    },
    round5_setup: {
      state: 'verified',
      workflow_launch_skew_ms: 0.75,
      lanes: {
        lakebase: {
          id: 'lakebase',
          name: 'Lakebase',
          state: 'verified',
          setup_elapsed_ms: 12_345.678,
          status: 'Setup stop gate verified',
          stop_gate_evidence: {
            gate_id: 'native_transaction',
            expected: [{ key: 'transaction_verified', value: true }],
            observed: [{ key: 'transaction_verified', value: true }],
            exact: true,
          },
          verified: true,
        },
        competitor: {
          id: 'competitor',
          name: 'RDS PostgreSQL + RDS Proxy',
          state: 'verified',
          setup_elapsed_ms: 24_000,
          status: 'Setup stop gate verified',
          stop_gate_evidence: {
            gate_id: 'proxy_transaction',
            expected: [{ key: 'transaction_verified', value: true }],
            observed: [{ key: 'transaction_verified', value: true }],
            exact: true,
          },
          verified: true,
        },
      },
      setup_validated: true,
      downstream_validated: true,
      cleanup_retryable: false,
    },
    remembered_result: 'VERIFIED CONNECTION BURST',
    failure: null,
  }
}

function runningRoundFiveSession(): DemoSession {
  const proof = roundFiveSession()
  return {
    ...proof,
    state: 'running',
    updated_at: '2026-08-18T20:00:05Z',
    remembered_result: null,
    comparison: null,
    lanes: {
      lakebase: {
        ...proof.lanes.lakebase,
        state: 'connecting',
        elapsed_ms: null,
        status: 'Waiting for scored setup to finish',
      },
      competitor: {
        ...proof.lanes.competitor,
        state: 'connecting',
        elapsed_ms: null,
        status: 'Waiting for scored setup to finish',
      },
    },
    round5_setup: {
      state: 'running',
      workflow_launch_skew_ms: 0.75,
      lanes: {
        lakebase: {
          id: 'lakebase',
          name: 'Lakebase',
          state: 'running',
          setup_elapsed_ms: 500,
          status: 'Opening the built-in pooled connection',
          stop_gate_evidence: null,
          verified: false,
        },
        competitor: {
          id: 'competitor',
          name: 'RDS PostgreSQL + RDS Proxy',
          state: 'running',
          setup_elapsed_ms: 750,
          status: 'Creating the RDS Proxy endpoint',
          stop_gate_evidence: null,
          verified: false,
        },
      },
      setup_validated: false,
      downstream_validated: false,
      cleanup_retryable: false,
    },
  }
}

function stoppedLakebaseSetup(session: DemoSession): DemoSession {
  return {
    ...session,
    updated_at: '2026-08-18T20:00:06Z',
    round5_setup: {
      ...session.round5_setup!,
      lanes: {
        ...session.round5_setup!.lanes,
        lakebase: {
          ...session.round5_setup!.lanes.lakebase!,
          state: 'verified',
          setup_elapsed_ms: 1_234,
          status: 'Native transaction verified',
          stop_gate_evidence: {
            gate_id: 'native_transaction',
            expected: [{ key: 'transaction_verified', value: true }],
            observed: [{ key: 'transaction_verified', value: true }],
            exact: true,
          },
          verified: true,
        },
      },
    },
  }
}

function pendingRoundFiveSession(): DemoSession {
  const running = runningRoundFiveSession()
  return {
    ...running,
    round5_setup: {
      ...running.round5_setup!,
      state: 'pending',
      lanes: {
        lakebase: {
          ...running.round5_setup!.lanes.lakebase!,
          state: 'pending',
          setup_elapsed_ms: null,
          status: 'Preparing the shared setup barrier',
        },
        competitor: {
          ...running.round5_setup!.lanes.competitor!,
          state: 'pending',
          setup_elapsed_ms: null,
          status: 'Preparing the shared setup barrier',
        },
      },
    },
  }
}

function towelledRoundFiveSession(): DemoSession {
  const stopped = stoppedLakebaseSetup(runningRoundFiveSession())
  return {
    ...stopped,
    state: 'towelled',
    updated_at: '2026-08-18T20:00:10Z',
    comparison: null,
    remembered_result: null,
    towel: {
      state: 'ready',
      requested_at: '2026-08-18T20:00:10Z',
      censored_lower_bounds_ms: { competitor: 4_500 },
      restore_started: false,
      cleanup_failure: null,
    },
    lanes: {
      lakebase: {
        ...stopped.lanes.lakebase,
        state: 'verified',
        elapsed_ms: 91_000,
        status: 'Misleading downstream burst timing',
      },
      competitor: {
        ...stopped.lanes.competitor,
        state: 'towelled',
        elapsed_ms: 92_000,
        status: 'Misleading downstream burst timing',
      },
    },
    round5_setup: {
      ...stopped.round5_setup!,
      state: 'towelled',
      setup_validated: false,
      downstream_validated: false,
      lanes: {
        ...stopped.round5_setup!.lanes,
        competitor: {
          ...stopped.round5_setup!.lanes.competitor!,
          state: 'towelled',
          setup_elapsed_ms: 4_500,
          status: 'Stopped before the exact setup gate verified',
          stop_gate_evidence: null,
          verified: false,
        },
      },
    },
  }
}

function displayedSeconds(lane: HTMLElement): number {
  return Number(lane.querySelector('.timer-readout')?.textContent?.replace('s', ''))
}

function stubReceiptCanvas() {
  const context = {
    fillRect: vi.fn(), strokeRect: vi.fn(), fillText: vi.fn(),
    save: vi.fn(), translate: vi.fn(), rotate: vi.fn(), restore: vi.fn(),
    measureText: vi.fn((value: string) => ({ width: value.length * 8 })),
  }
  vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(
    context as unknown as CanvasRenderingContext2D,
  )
  vi.spyOn(HTMLCanvasElement.prototype, 'toBlob').mockImplementation(
    (callback) => callback(new Blob(['pixel-card'], { type: 'image/png' })),
  )
}

it('renders the running Round 5 race in the canonical two-clock arena', async () => {
  vi.useFakeTimers()
  const running = runningRoundFiveSession()
  const { container } = render(
    <RoundFiveProof
      session={running}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  expect(container.querySelector('.round5-result-card')).not.toBeInTheDocument()
  expect(container.querySelector('.proof-screen.round5-arena')).toBeInTheDocument()
  expect(screen.getByText(/pass\/fail spike: 128 fresh app connection attempts \/ lane · max 64 at once · after 4 untimed warmups/i)).toBeInTheDocument()
  expect(container.querySelector('.proof-lanes')).toBeInTheDocument()
  expect(container.querySelectorAll('.proof-lane')).toHaveLength(2)
  expect(container.querySelector('.lane-rule')).toHaveTextContent('VS')
  expect(container.querySelector('.proof-footer')).toBeInTheDocument()

  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  const initialLakebaseSeconds = displayedSeconds(lakebase)
  const initialCompetitorSeconds = displayedSeconds(competitor)
  const commentator = screen.getByLabelText('Ringside commentator')
  expect(commentator).toHaveTextContent(/Live setup call/i)
  expect(commentator).not.toHaveTextContent(/ON THE WIRE/i)

  await act(async () => { await vi.advanceTimersByTimeAsync(96) })

  expect(displayedSeconds(lakebase)).toBeGreaterThan(initialLakebaseSeconds)
  expect(displayedSeconds(competitor)).toBeGreaterThan(initialCompetitorSeconds)
})

it('restores a silent Round 5 lane from the server snapshot floor without rewinding', async () => {
  vi.useFakeTimers()
  const stale = stoppedLakebaseSetup(runningRoundFiveSession())
  stale.round5_setup!.lanes.competitor!.setup_elapsed_ms = 7_000
  const props = {
    roundNumber: 5,
    error: null,
    liveEvidenceConnected: true,
    uiReview: false,
    hasNextRound: true,
    commentaryOpen: true,
    onContinue: vi.fn(),
    onToggleCommentary: vi.fn(),
    onHome: vi.fn(),
  }
  const first = render(<RoundFiveProof session={stale} {...props} />)
  await act(async () => { await vi.advanceTimersByTimeAsync(50_100) })
  const beforeRefresh = displayedSeconds(
    screen.getByLabelText('RDS PostgreSQL + RDS Proxy result'),
  )
  expect(beforeRefresh).toBeGreaterThanOrEqual(57)
  first.unmount()

  const refreshed = {
    ...stale,
    round5_setup: {
      ...stale.round5_setup!,
      lanes: {
        ...stale.round5_setup!.lanes,
        competitor: {
          ...stale.round5_setup!.lanes.competitor!,
          // The callback latch is still 7s. This separate value is the floor
          // computed by the server at GET time from the same lane-owned clock
          // source used when a towel freezes the bout.
          elapsed_at_snapshot_ms: 57_100,
        },
      },
    },
  } as DemoSession
  render(<RoundFiveProof session={refreshed} {...props} />)

  const afterRefresh = displayedSeconds(
    screen.getByLabelText('RDS PostgreSQL + RDS Proxy result'),
  )
  expect(afterRefresh).toBeGreaterThanOrEqual(beforeRefresh)
  expect(afterRefresh).toBeGreaterThanOrEqual(57.1)
  await act(async () => { await vi.advanceTimersByTimeAsync(96) })
  expect(displayedSeconds(
    screen.getByLabelText('RDS PostgreSQL + RDS Proxy result'),
  )).toBeGreaterThan(afterRefresh)
})

it('keeps the shared preflight untimed until the setup clocks actually start', () => {
  render(
    <RoundFiveProof
      session={pendingRoundFiveSession()}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  const commentator = screen.getByLabelText('Ringside commentator')
  expect(commentator).toHaveTextContent(
    /Lakebase · Untimed shared preflight · Setup clock has not started/i,
  )
  expect(commentator).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy · Untimed shared preflight · Setup clock has not started/i,
  )
  expect(commentator).toHaveTextContent(
    /Untimed shared preflight in progress · Both setup clocks are sealed/i,
  )
  expect(commentator).not.toHaveTextContent(/clock live/i)
  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  expect(lakebase).toHaveAttribute('data-state', 'sealed')
  expect(competitor).toHaveAttribute('data-state', 'sealed')
  expect(lakebase).toHaveTextContent(/Untimed shared preflight · Setup clock has not started/i)
  expect(competitor).toHaveTextContent(/Untimed shared preflight · Setup clock has not started/i)
})

it('keeps the Round 5 towel action visible and forwards the stop request', async () => {
  const onTowel = vi.fn().mockResolvedValue(undefined)
  render(
    <RoundFiveProof
      session={runningRoundFiveSession()}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onTowel={onTowel}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  const towel = screen.getByRole('button', { name: /throw in the towel/i })
  expect(towel).toBeEnabled()
  await act(async () => { fireEvent.click(towel) })
  expect(onTowel).toHaveBeenCalledOnce()
})

it('shows only exact Round 5 setup evidence after a towel with no false comparison', () => {
  const { container } = render(
    <RoundFiveProof
      session={towelledRoundFiveSession()}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  const arena = container.querySelector('.proof-screen.round5-arena[data-session-state="towelled"]')
  expect(arena).toBeInTheDocument()
  expect(arena?.querySelector('.lane-rule')).toHaveTextContent('VS')
  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  expect(lakebase.querySelector('.lane-time')).toHaveTextContent('1.23s')
  expect(lakebase).toHaveTextContent(/Native transaction verified/i)
  expect(competitor.querySelector('.lane-time')).toHaveTextContent('>4.50s')
  expect(competitor).toHaveTextContent(/UNVERIFIED WHEN STOPPED · LOWER BOUND/i)
  expect(arena).not.toHaveTextContent(/4500\.00 ms|91\.00s|92\.00s/i)
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(
    /Bout stopped.*No winner · margin N\/A/i,
  )
  expect(screen.getByLabelText('Ringside commentator')).toHaveTextContent(/Live setup call/i)
})

it('offers Next Round while a towel cleanup continues backstage', () => {
  const onContinue = vi.fn()
  const towelled = towelledRoundFiveSession()
  towelled.towel = { ...towelled.towel!, state: 'cleaning' }
  towelled.round5_setup = {
    ...towelled.round5_setup!,
    cleanup_retryable: true,
  }
  render(
    <RoundFiveProof
      session={towelled}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={onContinue}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  expect(screen.getByText(/result posted · cleanup backstage/i)).toBeInTheDocument()
  const next = screen.getByRole('button', { name: /a · next round/i })
  fireEvent.click(next)
  expect(onContinue).toHaveBeenCalledOnce()
  expect(towelled.towel.state).toBe('cleaning')
  expect(towelled.round5_setup.cleanup_retryable).toBe(true)
})

it('stops Lakebase at its exact setup elapsed while AWS and the commentator continue', async () => {
  vi.useFakeTimers()
  const running = runningRoundFiveSession()
  const props = {
    roundNumber: 5,
    error: null,
    liveEvidenceConnected: true,
    uiReview: false,
    hasNextRound: true,
    commentaryOpen: true,
    onContinue: vi.fn(),
    onToggleCommentary: vi.fn(),
    onHome: vi.fn(),
  }
  const { rerender } = render(<RoundFiveProof session={running} {...props} />)

  const runningCommentary = screen.getByLabelText('Ringside commentator')
  expect(runningCommentary).toHaveTextContent(
    /Lakebase · Setup clock live · Opening the built-in pooled connection/i,
  )
  expect(runningCommentary).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy · Setup clock live · Creating the RDS Proxy endpoint/i,
  )

  await act(async () => { await vi.advanceTimersByTimeAsync(96) })
  rerender(<RoundFiveProof session={stoppedLakebaseSetup(running)} {...props} />)

  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  expect(lakebase).toHaveAttribute('data-state', 'verified')
  expect(lakebase.querySelector('.lane-time')).toHaveTextContent('1.23s')
  expect(runningCommentary).toHaveTextContent(
    /Lakebase · Exact setup gate verified · Clock stopped at 1\.23s · Native transaction verified/i,
  )
  expect(runningCommentary).toHaveTextContent(
    /Lakebase reached readiness at 1\.23s · RDS PostgreSQL \+ RDS Proxy readiness clock still running · No comparison yet/i,
  )

  const competitorBefore = displayedSeconds(competitor)
  await act(async () => { await vi.advanceTimersByTimeAsync(96) })

  expect(lakebase.querySelector('.lane-time')).toHaveTextContent('1.23s')
  expect(displayedSeconds(competitor)).toBeGreaterThan(competitorBefore)
})

it('freezes both Round 5 clocks and shows reconnecting when live evidence disconnects', async () => {
  vi.useFakeTimers()
  const running = runningRoundFiveSession()
  const baseProps = {
    session: running,
    roundNumber: 5,
    uiReview: false,
    hasNextRound: true,
    commentaryOpen: true,
    onContinue: vi.fn(),
    onToggleCommentary: vi.fn(),
    onHome: vi.fn(),
  }
  const { container, rerender } = render(
    <RoundFiveProof {...baseProps} error={null} liveEvidenceConnected />,
  )

  await act(async () => { await vi.advanceTimersByTimeAsync(96) })
  rerender(
    <RoundFiveProof
      {...baseProps}
      error="Live evidence stream interrupted. Reconnecting…"
      liveEvidenceConnected={false}
    />,
  )

  const arena = container.querySelector('.proof-screen.round5-arena')
  const lakebaseTime = screen.getByLabelText('Lakebase result').querySelector('.lane-time')!
  const competitorTime = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result').querySelector('.lane-time')!
  expect(arena).toHaveAttribute('data-session-state', 'offline')
  expect(screen.getByText(/proof paused · reconnecting/i)).toBeInTheDocument()
  expect(lakebaseTime).toHaveAttribute('data-live', 'false')
  expect(competitorTime).toHaveAttribute('data-live', 'false')
  const commentator = screen.getByLabelText('Ringside commentator')
  expect(commentator).toHaveTextContent(/Live setup call/i)
  expect(commentator).not.toHaveTextContent(/ON THE WIRE/i)
  expect(commentator).toHaveTextContent(
    /Lakebase · Live evidence interrupted · Display clock frozen while reconnecting/i,
  )
  expect(commentator).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy · Live evidence interrupted · Display clock frozen while reconnecting/i,
  )
  expect(commentator).toHaveTextContent(
    /Live evidence interrupted · No new result inferred · Reconnecting/i,
  )
  expect(commentator).not.toHaveTextContent(/setup clock live/i)

  const frozenLakebase = lakebaseTime.textContent
  const frozenCompetitor = competitorTime.textContent
  await act(async () => { await vi.advanceTimersByTimeAsync(128) })

  expect(lakebaseTime).toHaveTextContent(frozenLakebase!)
  expect(competitorTime).toHaveTextContent(frozenCompetitor!)
})

it('applies embedded Round 5 progress and cleanup snapshots without regressing the UI', () => {
  const current = {
    ...roundFiveSession(),
    state: 'running' as const,
    updated_at: '2026-08-18T20:00:02Z',
  }
  const newer = {
    ...current,
    updated_at: '2026-08-18T20:00:03Z',
    round5_setup: {
      ...current.round5_setup!,
      state: 'running' as const,
      lanes: {
        ...current.round5_setup!.lanes,
        competitor: {
          ...current.round5_setup!.lanes.competitor!,
          state: 'running' as const,
          status: 'Creating RDS Proxy',
          verified: false,
        },
      },
    },
  }
  const event = {
    sequence: 8,
    event: 'lane_update' as const,
    occurred_at: newer.updated_at,
    payload: { session: newer },
  }

  const applied = applyRunEventSnapshot(current, event)
  expect(selectRound4Session(current, applied)?.round5_setup?.lanes.competitor?.status).toBe('Creating RDS Proxy')
  expect(selectRound4Session(newer, current)).toBe(newer)

  const cleanupPending = {
    ...roundFiveSession(),
    updated_at: '2026-08-18T20:01:01Z',
    round5_setup: { ...roundFiveSession().round5_setup!, cleanup_retryable: true },
  }
  const cleanupReady = {
    ...cleanupPending,
    updated_at: '2026-08-18T20:01:02Z',
    round5_setup: { ...cleanupPending.round5_setup!, cleanup_retryable: false },
  }
  const cleanupApplied = applyRunEventSnapshot(cleanupPending, {
    sequence: 9,
    event: 'cleanup_update',
    occurred_at: cleanupReady.updated_at,
    payload: { session: cleanupReady },
  })
  expect(selectRound4Session(cleanupPending, cleanupApplied)?.round5_setup?.cleanup_retryable).toBe(false)
})

it('renders verified Round 5 as the canonical arena and keeps detailed evidence behind actions', () => {
  const proof = roundFiveSession()
  const { container, rerender } = render(
    <RoundFiveProof
      session={proof}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  const arena = container.querySelector('.proof-screen.round5-arena[data-session-state="verified"]')
  expect(arena).toBeInTheDocument()
  expect(arena?.querySelectorAll('.proof-lane')).toHaveLength(2)
  expect(arena?.querySelector('.lane-rule')).toHaveTextContent('VS')
  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  expect(lakebase).toHaveAttribute('data-corner', 'red')
  expect(competitor).toHaveAttribute('data-corner', 'blue')
  expect(lakebase.querySelector('.lane-time')).toHaveTextContent('12.35s')
  expect(competitor.querySelector('.lane-time')).toHaveTextContent('24.00s')
  expect(container.querySelector('.remembered')).toHaveTextContent(
    /Verified readiness comparison.*Lakebase verified a pooled path.*sooner/i,
  )

  const commentator = screen.getByLabelText('Ringside commentator')
  expect(commentator).toHaveTextContent(/Final verified call/i)
  expect(commentator).toHaveTextContent(
    /Lakebase · Exact setup gate verified · Clock stopped at 12\.35s/i,
  )
  expect(commentator).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy · Exact setup gate verified · Clock stopped at 24\.00s/i,
  )

  expect(container.querySelector('.round5-body')).not.toBeInTheDocument()
  expect(container.querySelector('.round5-phase')).not.toBeInTheDocument()
  expect(container.querySelector('.round5-components')).not.toBeInTheDocument()
  expect(screen.queryByLabelText(/warm burst evidence|fair proof contract|managed component disclosure/i)).not.toBeInTheDocument()
  expect(arena).not.toHaveTextContent(/witness clients|backend pids|peak sessions|component disclosure/i)
  expect(screen.queryByRole('button', { name: /redo|towel|cooldown/i })).not.toBeInTheDocument()

  expect(screen.queryByRole('button', { name: /ring again|re-do|redo/i })).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: /explain to the room/i }))
  const ringsideTake = screen.getByRole('dialog', { name: /for the data engineer/i })
  const selectedPriorities = within(ringsideTake).getByLabelText('Room priorities')
  expect(selectedPriorities.children).toHaveLength(2)
  expect(selectedPriorities).toHaveTextContent(/cost.*performance/i)
  expect(ringsideTake).toHaveTextContent(
    /what this means.*readiness difference needs launch frequency and pooling spend.*cost evidence/i,
  )
  expect(ringsideTake).toHaveTextContent(/question for the room.*what job deadline makes connection setup financially material/i)
  expect(ringsideTake).toHaveTextContent(
    /what we proved.*lakebase became ready in 12\.35s.*new RDS Proxy setup took 24\.00s.*both passed 128 attempts, 64 at a time.*existing contract-matching Proxies were not tested/i,
  )
  expect(ringsideTake.querySelector('details')).toBeNull()
  expect(ringsideTake).not.toHaveTextContent(
    /full verified proof|configured compute|pricing receipt|standing cost|component disclosure/i,
  )
  fireEvent.click(within(ringsideTake).getByRole('button', { name: /back to the ring/i }))
  expect(screen.queryByRole('dialog', { name: /for the data engineer/i })).not.toBeInTheDocument()

  fireEvent.click(screen.getByRole('button', { name: /what it cost/i }))
  const costRoom = screen.getByRole('dialog', { name: /the bill does not stop with the bell/i })
  fireEvent.click(within(costRoom).getByText(/pricing receipt.*usage later/i))
  expect(costRoom).toHaveTextContent(/Posted partial.*Posted through 2026-08-20 01:45:00 UTC.*Revision 2.*Queried 2026-08-20 02:00:00 UTC/i)
  expect(within(costRoom).getByLabelText('Cost scopes')).toHaveTextContent(/Bout estimate.*\$0\.005.*Monthly carrying.*\$2\.70.*month.*Installation overhead.*Pending/i)
  expect(within(costRoom).getByLabelText('Cost reconciliation')).toHaveTextContent(/Original.*\$0\.004.*Posted.*\$0\.005.*Variance.*\$0\.001/i)
  expect(costRoom).toHaveTextContent(/RDS Proxy.*10-minute minimum.*0\.333 vCPU-hour.*\$0\.015.*\$0\.005.*final Proxy lifetime pending/i)
  expect(costRoom).not.toHaveTextContent(/Provider adjustment.*\$0\.000/i)
  fireEvent.click(within(costRoom).getByRole('button', { name: /back to the ring/i }))

  stubReceiptCanvas()
  fireEvent.click(screen.getByRole('button', { name: /share the receipt/i }))
  const shareReceipt = screen.getByRole('dialog', { name: /share the proof/i })
  expect(within(shareReceipt).getByLabelText('Lakebase receipt result')).toHaveTextContent('12.35s')
  expect(within(shareReceipt).getByLabelText(
    /RDS PostgreSQL(?: \+ RDS Proxy)? receipt result/i,
  )).toHaveTextContent('24.00s')
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).not.toHaveTextContent(
    /not timed|non-executable/i,
  )
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).toHaveTextContent(
    /Lakebase verified a pooled path.*11s sooner.*EXACT SETUP MARGIN 11\.65s/i,
  )
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).toHaveTextContent(/Start gap 0\.750ms/i)
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).not.toHaveTextContent(/Start gap 1\.235ms/i)
  fireEvent.click(within(shareReceipt).getByRole('button', { name: /^b · back$/i }))
  expect(screen.queryByRole('dialog', { name: /share the proof/i })).not.toBeInTheDocument()

  const failed: DemoSession = {
    ...proof,
    state: 'failed',
    failure: 'internal fence-token=do-not-render journal=row-17',
    comparison: null,
    round5_setup: {
      ...proof.round5_setup!,
      state: 'failed',
      setup_validated: false,
      downstream_validated: false,
      lanes: {
        ...proof.round5_setup!.lanes,
        lakebase: {
          ...proof.round5_setup!.lanes.lakebase!,
          // Verbatim state shape from a terminal setup failure: progress had
          // reached the lane stop callback, but the final stop-gate evidence
          // was never attached because its peer failed.
          state: 'verified',
          setup_elapsed_ms: 3_112.673,
          stop_gate_evidence: null,
          verified: false,
        },
      },
    },
    lanes: {
      ...proof.lanes,
      lakebase: {
        ...proof.lanes.lakebase,
        state: 'failed',
        error: 'internal workflow-id=do-not-render',
        evidence: {
          ...proof.lanes.lakebase.evidence,
          terminal_clients: 127,
          successful_clients: 0,
          error_clients: 127,
          successful_latency_ms: [],
        },
      },
    },
  }
  rerender(
    <RoundFiveProof
      session={failed}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound={false}
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(/primary setup result.*setup not verified/i)
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(/both lanes did not reach the exact setup stop gate.*no timing comparison/i)
  expect(screen.queryByLabelText(/warm burst evidence|setup result|fair proof contract|managed component disclosure/i)).not.toBeInTheDocument()
  expect(screen.getByText('Technical details')).toBeInTheDocument()
  fireEvent.click(screen.getByText('Technical details'))
  expect(screen.getByText(/progress reached · not finalized · stop gate not verified/i)).toBeInTheDocument()
  expect(screen.queryByText(/^verified · stop gate not verified$/i)).not.toBeInTheDocument()
  expect(screen.queryByText(/fence-token|journal=row|workflow-id/i)).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /retry cleanup/i })).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /ring again/i })).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: /fight card/i })).toBeInTheDocument()

  const cleanupFailed: DemoSession = {
    ...failed,
    round5_setup: {
      ...failed.round5_setup!,
      state: 'cleanup_failed',
      cleanup_retryable: true,
    },
  }
  const retryCleanup = vi.fn()
  rerender(
    <RoundFiveProof
      session={cleanupFailed}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound={false}
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onRetryCleanup={retryCleanup}
      onHome={vi.fn()}
    />,
  )
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(/backstage recovery/i)
  expect(screen.queryByLabelText('Round 5 final receipt')).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /ring again|fight card|next round/i })).not.toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: /retry cleanup/i }))
  expect(retryCleanup).toHaveBeenCalledOnce()
  // No diagnostic yet, so the screen keeps its own sentence: `cleanup_failed`
  // is set while retries are still running too, and that is what this is.
  expect(screen.getByText(/automatic cleanup is retrying backstage.*ring stays protected.*clean baseline/i)).toBeInTheDocument()

  /* Verbatim from `_abandon_connection_spike_cleanup_retry`. Quoted rather
     than paraphrased: printing the server's own sentence instead of a house
     one is the behaviour being asserted, and a paraphrase here would pass
     while the screen said something the server never said. */
  const abandonedDiagnostic = 'Round 5 backstage cleanup did not converge after 6 automatic attempts. '
    + 'The ring stays held until cleanup is confirmed; retry cleanup.'
  const cleanupAbandoned: DemoSession = {
    ...cleanupFailed,
    round5_setup: { ...cleanupFailed.round5_setup!, cleanup_failure: abandonedDiagnostic },
  }
  rerender(
    <RoundFiveProof
      session={cleanupAbandoned}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound={false}
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onRetryCleanup={retryCleanup}
      onHome={vi.fn()}
    />,
  )
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(abandonedDiagnostic)
  expect(screen.queryByText(/automatic cleanup is retrying backstage/i)).not.toBeInTheDocument()

  rerender(
    <RoundFiveProof
      session={cleanupFailed}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound={false}
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      cleanupPending
      onRetryCleanup={retryCleanup}
      onHome={vi.fn()}
    />,
  )
  expect(screen.getByRole('button', { name: /retrying cleanup/i })).toBeDisabled()

  const verifiedCleanupPending: DemoSession = {
    ...proof,
    round5_setup: { ...proof.round5_setup!, cleanup_retryable: true },
  }
  rerender(
    <RoundFiveProof
      session={verifiedCleanupPending}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onRetryCleanup={retryCleanup}
      onHome={vi.fn()}
    />,
  )
  expect(screen.getByText(/automatic cleanup is settling backstage.*ring protected/i)).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /retry cleanup/i })).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /next round/i })).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /share the receipt/i })).not.toBeInTheDocument()

  /* The case that had nowhere to appear. This bout verified and keeps its win,
     and then the server gave up on tidying the proxy it built. `settling
     backstage` was all the arena could say about that, which reads as work in
     progress and is the opposite of what has happened. */
  const verifiedCleanupAbandoned: DemoSession = {
    ...verifiedCleanupPending,
    round5_setup: { ...verifiedCleanupPending.round5_setup!, cleanup_failure: abandonedDiagnostic },
  }
  rerender(
    <RoundFiveProof
      session={verifiedCleanupAbandoned}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onRetryCleanup={retryCleanup}
      onHome={vi.fn()}
    />,
  )
  const abandonedNotice = screen.getByRole('alert')
  expect(abandonedNotice).toHaveTextContent('Cleanup needs attention')
  expect(abandonedNotice).toHaveTextContent(abandonedDiagnostic)
  expect(screen.queryByText(/settling backstage/i)).not.toBeInTheDocument()
  /* Legibility only. The win still stands, the retry is still offered, and the
     exits are still shut -- this notice explains the lockout, it does not
     change who is allowed to walk away from a resource that may still exist. */
  expect(screen.getByText(/verified readiness comparison/i)).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /retry cleanup/i })).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /next round|fight card/i })).not.toBeInTheDocument()

  const towelled: DemoSession = {
    ...failed,
    state: 'towelled',
    failure: null,
    towel: {
      state: 'cleaning',
      requested_at: '2026-08-18T20:01:10Z',
      restore_started: false,
      cleanup_failure: null,
    },
    round5_setup: {
      ...failed.round5_setup!,
      state: 'towelled',
      cleanup_retryable: false,
      lanes: {
        ...failed.round5_setup!.lanes,
        lakebase: { ...failed.round5_setup!.lanes.lakebase!, state: 'towelled' },
      },
    },
  }
  rerender(
    <RoundFiveProof
      session={towelled}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onRetryCleanup={retryCleanup}
      onHome={vi.fn()}
    />,
  )
  expect(screen.queryByRole('button', { name: /retry cleanup/i })).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()

  rerender(
    <RoundFiveProof
      session={{ ...towelled, towel: { ...towelled.towel!, state: 'ready' } }}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onRetryCleanup={retryCleanup}
      onHome={vi.fn()}
    />,
  )
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(/bout stopped/i)
  expect(screen.queryByRole('button', { name: /ring again|re-do|redo/i })).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()
})

it('keeps Ryan and the play-by-play toggle available on terminal Round 5', () => {
  const proof = roundFiveSession()
  const onToggleCommentary = vi.fn()
  const props = {
    session: proof,
    roundNumber: 5,
    error: null,
    liveEvidenceConnected: true,
    uiReview: false,
    hasNextRound: true,
    onContinue: vi.fn(),
    onToggleCommentary,
    onHome: vi.fn(),
  }
  const { rerender } = render(<RoundFiveProof {...props} commentaryOpen />)

  let commentator = screen.getByLabelText('Ringside commentator')
  expect(within(commentator).getByRole('img', { name: 'Ryan' })).toBeInTheDocument()
  expect(commentator).toHaveTextContent(/Final verified call/i)
  fireEvent.click(within(commentator).getByRole('button', { name: /hide play-by-play/i }))
  expect(onToggleCommentary).toHaveBeenCalledOnce()

  rerender(<RoundFiveProof {...props} commentaryOpen={false} />)
  commentator = screen.getByLabelText('Ringside commentator')
  expect(within(commentator).getByText(/play-by-play hidden/i)).toBeInTheDocument()
  expect(within(commentator).queryByRole('img', { name: 'Ryan' })).not.toBeInTheDocument()
  fireEvent.click(within(commentator).getByRole('button', { name: /show commentator/i }))
  expect(onToggleCommentary).toHaveBeenCalledTimes(2)

  rerender(<RoundFiveProof {...props} commentaryOpen />)
  expect(within(screen.getByLabelText('Ringside commentator')).getByRole('img', { name: 'Ryan' })).toBeInTheDocument()
})

it('names Aurora in the canonical Round 5 arena and its on-demand explanation', () => {
  const proof = roundFiveSession()
  const competitor = FALLBACK_CATALOG.competitors.find((item) => item.id === 'aurora_serverless_v2')!
  const auroraProof: DemoSession = {
    ...proof,
    competitor,
    lanes: {
      ...proof.lanes,
      competitor: { ...proof.lanes.competitor, name: 'Aurora Serverless v2 + RDS Proxy' },
    },
    round5_setup: {
      ...proof.round5_setup!,
      lanes: {
        ...proof.round5_setup!.lanes,
        competitor: { ...proof.round5_setup!.lanes.competitor!, name: 'Aurora Serverless v2 + RDS Proxy' },
      },
    },
  }

  const { container } = render(
    <RoundFiveProof
      session={auroraProof}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound={false}
      commentaryOpen
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  expect(container.querySelector('.proof-screen.round5-arena[data-session-state="verified"]')).toBeInTheDocument()
  expect(screen.getByLabelText('Aurora Serverless v2 + RDS Proxy result')).toHaveTextContent('24.00s')
  expect(within(container).queryByLabelText('Managed component disclosure')).not.toBeInTheDocument()

  fireEvent.click(screen.getByRole('button', { name: /explain to the room/i }))
  const explanation = screen.getByRole('dialog', { name: /for the data engineer/i })
  expect(explanation).toHaveTextContent(
    /what we proved.*lakebase became ready in 12\.35s.*new RDS Proxy setup took 24\.00s.*both passed 128 attempts, 64 at a time.*existing contract-matching Proxies were not tested/i,
  )
  expect(explanation.querySelector('details')).toBeNull()
  expect(explanation).not.toHaveTextContent(/full verified proof|component disclosure|supporting changes/i)
  expect(explanation).not.toHaveTextContent(/Aurora unexecuted|not executed or scored/i)
})

it('offers an instant replay on a verified Round 5 with the scored setup evidence behind it', () => {
  const session = roundFiveSession()
  render(
    <RoundFiveProof
      session={session}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen={false}
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  // Round 5 must offer the same completion affordance as Rounds 1-3.
  const replayControl = screen.getByRole('button', { name: /instant replay/i })
  expect(replayControl).toBeInTheDocument()
  fireEvent.click(replayControl)

  const replay = screen.getByRole('dialog', { name: /how this round was proved/i })
  expect(replay).toHaveTextContent(/instant replay · round 5/i)

  // The scored clock is the setup elapsed, not the burst lane's empty elapsed_ms.
  const lanes = replay.querySelector('.replay-lanes')!
  expect(lanes).toHaveTextContent('12.35s')
  expect(lanes).toHaveTextContent('24.00s')
  expect(lanes).not.toHaveTextContent('N/A')

  // The start gap must be the setup barrier, never the downstream burst skew.
  expect(replay).toHaveTextContent('0.750ms')
  expect(replay).not.toHaveTextContent('1.235ms')
  expect(within(replay).getByLabelText('Round 5 detailed proof')).toHaveTextContent(
    /full verified proof.*readiness setup is scored.*identical spike is pass\/fail/i,
  )
  expect(within(replay).getByLabelText('Managed component disclosure')).toHaveTextContent(
    /new Proxy \+ 8 supporting changes.*already-deployed Proxy would not pay this setup delay/i,
  )

  // Every step is Round 5 specific, not the "adapter is not executable" placeholder.
  const steps = within(replay).getAllByRole('button', { expanded: false })
  expect(steps.length).toBeGreaterThanOrEqual(4)
  expect(replay).not.toHaveTextContent(/will appear when this round adapter is executable/i)

  // The nine journaled AWS mutations are the substance of the replay.
  fireEvent.click(within(replay).getByRole('button', { name: /nine journaled resources/i }))
  const calls = screen.getByLabelText('Exact calls for step 02')
  for (const resource of [
    'proxy_security_group', 'proxy_default_egress', 'proxy_ingress', 'proxy_egress',
    'runner_egress', 'rds_ingress', 'rds_proxy', 'proxy_target_group', 'proxy_target',
  ]) {
    expect(calls).toHaveTextContent(`journal: ${resource}`)
  }
  expect(calls).toHaveTextContent(/built-in Lakebase pooled endpoint/i)

  fireEvent.click(within(replay).getByRole('button', { name: /back to the ring/i }))
  expect(screen.queryByRole('dialog', { name: /how this round was proved/i })).not.toBeInTheDocument()
})

it('puts the sound toggle in the header of both Round 5 layouts', () => {
  const onToggleSound = vi.fn()
  const props = {
    roundNumber: 5,
    error: null,
    liveEvidenceConnected: true,
    uiReview: false,
    hasNextRound: true,
    commentaryOpen: true,
    onContinue: vi.fn(),
    onToggleCommentary: vi.fn(),
    onHome: vi.fn(),
    sound: true,
    onToggleSound,
  }

  // The arena, while the spike is running.
  const { container, rerender } = render(
    <RoundFiveProof {...props} session={runningRoundFiveSession()} />,
  )
  const arenaToggle = screen.getByRole('button', { name: 'Sound on' })
  expect(arenaToggle).toHaveAttribute('aria-pressed', 'true')
  expect(container.querySelector('.proof-header')).toContainElement(arenaToggle)
  fireEvent.click(arenaToggle)
  expect(onToggleSound).toHaveBeenCalledTimes(1)

  // The compact layout, which is the only thing a failed setup renders.
  const failed: DemoSession = { ...runningRoundFiveSession(), state: 'failed' }
  rerender(<RoundFiveProof {...props} sound={false} session={failed} />)
  expect(container.querySelector('.round5-screen')).toBeInTheDocument()
  const compactToggle = screen.getByRole('button', { name: 'Sound off' })
  expect(compactToggle).toHaveAttribute('aria-pressed', 'false')
  expect(container.querySelector('.round5-header')).toContainElement(compactToggle)

  // Neither layout puts a bell next to it, on either side of the tab order.
  expect(screen.queryByRole('button', { name: /ring the bell|ring again/i })).not.toBeInTheDocument()
})
