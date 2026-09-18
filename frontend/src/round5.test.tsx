import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { RoundFiveProof, humanDuration, knockoutRatioLabel, linkedInReceipt, receiptPresentation } from './App'
import type { DemoSession, LaneSnapshot } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { replayStory } from './instant-replay'
import { buildRingsideCue, classifyOutcome } from './ringside-cues'
import { applyRunEventSnapshot, reconcileRunEventSession, selectRound4Session } from './round4'
import {
  isRoundFiveSetupEvidence,
  roundFiveFightCardOpening,
  roundFiveHasComparison,
  roundFiveLanePresentation,
  roundFiveLaneResult,
  roundFiveSchedulingAdvisorySummary,
  roundFiveSetupDiagnosticSummary,
  roundFiveStoppedVerdict,
} from './round5'

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

it('consumes only exact backend V2/V3/V4 protocol-schema serialization pairs', () => {
  expect(isRoundFiveSetupEvidence({
    protocol: 'round5-fanin-v4',
    schema_version: 4,
  })).toBe(true)
  expect(isRoundFiveSetupEvidence({
    protocol: 'round5-fanin-v3',
    schema_version: 3,
  })).toBe(true)
  expect(isRoundFiveSetupEvidence({
    protocol: 'round5-fanin-v2',
    schema_version: 2,
  })).toBe(true)
  expect(isRoundFiveSetupEvidence({
    protocol: 'round5-fanin-v3',
    schema_version: 2,
  })).toBe(false)
  expect(isRoundFiveSetupEvidence({
    protocol: 'round5-fanin-v2',
    schema_version: 3,
  })).toBe(false)
  expect(isRoundFiveSetupEvidence({
    protocol: 'round5-fanin-v4',
    schema_version: 3,
  })).toBe(false)
})

it('gives every Round 5 lead voice a distinct human implication without proof mechanics', () => {
  const personaIds = FALLBACK_CATALOG.personas.map((persona) => persona.id)
  const openings = personaIds.map(roundFiveFightCardOpening)

  expect(personaIds).toHaveLength(10)
  expect(new Set(openings).size).toBe(personaIds.length)
  expect(roundFiveFightCardOpening('software_engineer')).toBe(
    'Lakebase includes pooling for up to 10,000 client connections. '
    + 'The selected AWS path adds a service the app team must secure and own.',
  )
  for (const opening of openings) {
    expect(opening).toMatch(/\bup to 10,000 client connections\b/i)
    expect(opening).not.toMatch(
      /[–—]|\b(?:spike|survive|witness|scheduled clients?|terminal clients?|setup stop|contract|128 attempts?|maximum 64 concurrent)\b/i,
    )
    expect(opening.split(/\s+/).length).toBeLessThanOrEqual(34)
  }
})

it('does not rewind live 10,000-client counters on refresh or SSE reconnect', () => {
  const current = roundFiveSession()
  current.state = 'running'
  current.updated_at = '2026-08-18T20:00:10Z'
  current.lanes.lakebase.state = 'verifying'
  current.lanes.lakebase.evidence = {
    ...current.lanes.lakebase.evidence,
    held_clients: 7_500,
    authenticated_clients: 7_500,
  }
  const stale = structuredClone(current)
  stale.lanes.lakebase.evidence = {
    ...stale.lanes.lakebase.evidence,
    held_clients: 5_000,
    authenticated_clients: 5_000,
  }

  const selected = selectRound4Session(current, stale)
  expect(selected?.lanes.lakebase.evidence?.held_clients).toBe(7_500)
})

function burstLane(id: 'lakebase' | 'competitor', name: string, offset: number): LaneSnapshot {
  return {
    id,
    name,
    state: 'verified',
    elapsed_ms: 3_112.673 + offset * 100,
    attempts: 10_000,
    status: 'Exact 10,000-client fan-in verified',
    error: null,
    evidence: {
      schema_version: 2,
      protocol: 'round5-fanin-v2',
      initiated_clients: 10_000,
      authenticated_clients: 10_000,
      held_clients_at_gate: 10_000,
      terminal_failures: 0,
      retries: 0,
      disconnected_during_hold: 0,
      time_to_target_ms: 3_112.673 + offset * 100,
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
      connect_latency_p50_ms: 80 + offset,
      connect_latency_p95_ms: 150 + offset,
      auth_method: id === 'lakebase' ? 'tls-cleartext-password' : 'scram-sha-256',
      connect_latency_p99_ms: 201.455 + offset,
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
      warmup_connections: 0,
      concurrency: 10_000,
      runner: 'Python 3.12 event-driven TLS/native-password',
      tls: 'verify-full',
      timeout: '20s connect · 600s run',
      protocol: 'round5-fanin-v2',
      target_clients_per_lane: 10_000,
      hold_seconds: 30,
      sampled_queries_per_lane: 64,
      max_retries: 0,
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
      margin: { spec_id: 'time_to_10000_ms', lane_id: 'lakebase', value: 1_000, display_value: '1000.00 ms' },
      detail: 'Lakebase completed the verified setup sooner.',
    },
    round5_setup: {
      schema_version: 2,
      protocol: 'round5-fanin-v2',
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

it('rejects stale or unversioned fan-in evidence before reading its counters', () => {
  for (const schemaVersion of [undefined, 1]) {
    const lane = burstLane('lakebase', 'Lakebase', 0)
    lane.evidence = {
      ...lane.evidence,
      schema_version: schemaVersion,
    }
    const result = roundFiveLaneResult(lane)
    expect(result.contractVerified).toBe(false)
    expect(result.held).toBeNull()
    expect(result.telemetryVerified).toBe(false)
  }
})

it('records each provider-selected authentication method without requiring equality', () => {
  const session = roundFiveSession()
  const lakebase = roundFiveLaneResult(session.lanes.lakebase)
  const competitor = roundFiveLaneResult(session.lanes.competitor)
  expect(lakebase.authMethod).toBe('tls-cleartext-password')
  expect(competitor.authMethod).toBe('scram-sha-256')
  expect(lakebase.contractVerified).toBe(true)
  expect(competitor.contractVerified).toBe(true)
  expect(roundFiveHasComparison(session)).toBe(true)
})

it('requires explicit v3 hard-safety semantics while retaining advisories', () => {
  const lane = burstLane('lakebase', 'Lakebase', 0)
  lane.evidence = {
    ...lane.evidence,
    schema_version: 4,
    protocol: 'round5-fanin-v4',
    safety_evidence_version: 5,
    hard_safety_verified: true,
    port_accounting_verified: true,
    telemetry_advisories: ['event_loop_pressure', 'cpu_pressure'],
  }

  const exact = roundFiveLaneResult(lane)
  expect(exact.contractVerified).toBe(true)
  expect(exact.telemetryAdvisories).toEqual(['event_loop_pressure', 'cpu_pressure'])

  lane.evidence = { ...lane.evidence, safety_evidence_version: 1 }
  expect(roundFiveLaneResult(lane).contractVerified).toBe(false)
})

it('uses serialized V3 model evidence for the formal comparison', () => {
  const session = roundFiveSession()
  session.round5_setup!.protocol = 'round5-fanin-v4'
  session.round5_setup!.schema_version = 4
  session.fairness.protocol = 'round5-fanin-v4'
  for (const laneId of ['lakebase', 'competitor'] as const) {
    session.lanes[laneId].evidence = {
      ...session.lanes[laneId].evidence,
      protocol: 'round5-fanin-v4',
      schema_version: 4,
      safety_evidence_version: 5,
      hard_safety_verified: true,
      port_accounting_verified: true,
      telemetry_advisories: ['event_loop_pressure'],
    }
  }
  session.round5_runtime = {
    ...withV3Runtime(runningRoundFiveSession()).round5_runtime!,
    state: 'verified',
    lanes: {
      lakebase: {
        ...withV3Runtime(runningRoundFiveSession()).round5_runtime!.lanes.lakebase,
        phase: 'verified',
        clients_initiated: 10_000,
        clients_authenticated: 10_000,
        held_clients: 10_000,
        bell_to_10000_observed_ms: 3_112.673,
        elapsed_at_snapshot_ms: 3_112.673,
      },
      competitor: {
        ...withV3Runtime(runningRoundFiveSession()).round5_runtime!.lanes.competitor,
        phase: 'verified',
        clients_initiated: 10_000,
        clients_authenticated: 10_000,
        held_clients: 10_000,
        bell_to_10000_observed_ms: 3_212.673,
        elapsed_at_snapshot_ms: 3_212.673,
      },
    },
  }
  session.comparison = {
    kind: 'measured',
    winner_lane_id: 'lakebase',
    margin: {
      spec_id: 'bell_to_10000_observed_ms',
      lane_id: 'lakebase',
      value: 100,
      display_value: '100.00 ms',
    },
    detail: 'V3 comparison',
  }
  session.lanes.lakebase.elapsed_ms = 1
  session.lanes.competitor.elapsed_ms = 2

  expect(roundFiveHasComparison(session)).toBe(true)
  const receipt = linkedInReceipt(session, 5)
  expect(receipt).toContain('3.11s')
  expect(receipt).toContain('3.21s')
  expect(receipt).not.toContain('0.00s')
})

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
        status: 'Waiting for supporting setup to finish',
        evidence: {},
      },
      competitor: {
        ...proof.lanes.competitor,
        state: 'connecting',
        elapsed_ms: null,
        status: 'Waiting for supporting setup to finish',
        evidence: {},
      },
    },
    round5_setup: {
      schema_version: 2,
      protocol: 'round5-fanin-v2',
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

function withV3Runtime(session: DemoSession): DemoSession {
  return {
    ...session,
    state: 'running',
    run_started_at: '2026-09-15T13:00:00Z',
    updated_at: '2026-09-15T13:00:01Z',
    round5_runtime: {
      protocol: 'round5-bell-to-10k-v4',
      warm_generation: 7,
      bell_id: 'bell-one',
      revision: 3,
      state: 'running',
      bell_at_utc: '2026-09-15T13:00:00Z',
      lanes: {
        lakebase: {
          id: 'lakebase',
          phase: 'dispatching',
          elapsed_at_snapshot_ms: 1_250,
          bell_to_10000_observed_ms: null,
          observation_uncertainty_ms: null,
          pooled_path_ready_observed_ms: null,
          ramp_started_observed_ms: null,
          ramp_time_to_10000_ms: null,
          clients_initiated: 0,
          clients_authenticated: 0,
          held_clients: 0,
          sampled_queries_succeeded: 0,
          status: 'Dispatching the retained first Lakebase client',
        },
        competitor: {
          id: 'competitor',
          phase: 'provisioning_proxy',
          elapsed_at_snapshot_ms: 1_250,
          bell_to_10000_observed_ms: null,
          observation_uncertainty_ms: null,
          pooled_path_ready_observed_ms: null,
          ramp_started_observed_ms: null,
          ramp_time_to_10000_ms: null,
          clients_initiated: 0,
          clients_authenticated: 0,
          held_clients: 0,
          sampled_queries_succeeded: 0,
          status: 'AWS is creating the per-bout RDS Proxy',
        },
      },
    },
  }
}

it('starts both v3 bell clocks on the first animation frame before provider progress', async () => {
  vi.useFakeTimers()
  const running = withV3Runtime(runningRoundFiveSession())
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
  const lanes = Array.from(container.querySelectorAll<HTMLElement>('.proof-lane'))
  expect(lanes).toHaveLength(2)
  expect(lanes.map(displayedSeconds)).toEqual([1.25, 1.25])

  await act(async () => {
    await vi.advanceTimersByTimeAsync(17)
  })

  expect(displayedSeconds(lanes[0])).toBeGreaterThan(1.25)
  expect(displayedSeconds(lanes[1])).toBeGreaterThan(1.25)
})

it('accepts only non-regressing V4 runtime revisions across reconnects', () => {
  const current = withV3Runtime(runningRoundFiveSession())
  current.round5_runtime!.revision = 8
  current.round5_runtime!.lanes.lakebase.elapsed_at_snapshot_ms = 8_000
  current.round5_runtime!.lanes.lakebase.held_clients = 7_500
  const stale = structuredClone(current)
  stale.round5_runtime!.revision = 7
  stale.round5_runtime!.lanes.lakebase.elapsed_at_snapshot_ms = 5_000
  stale.round5_runtime!.lanes.lakebase.held_clients = 5_000
  expect(selectRound4Session(current, stale)).toBe(current)

  const fresh = structuredClone(current)
  fresh.updated_at = '2026-09-15T13:00:00Z'
  fresh.round5_runtime!.revision = 9
  fresh.round5_runtime!.lanes.lakebase.elapsed_at_snapshot_ms = 9_000
  fresh.round5_runtime!.lanes.lakebase.held_clients = 8_000
  expect(selectRound4Session(current, fresh)?.round5_runtime?.revision).toBe(9)
})

it('treats an equal canonical v3 revision as immutable', () => {
  const current = withV3Runtime(runningRoundFiveSession())
  current.round5_runtime!.revision = 8
  const changed = structuredClone(current)
  changed.round5_runtime!.lanes.lakebase.clients_initiated = 1
  changed.round5_runtime!.lanes.lakebase.elapsed_at_snapshot_ms = 2_000

  expect(selectRound4Session(current, changed)).toBe(current)
})

it('rejects an equal-revision SSE payload without changing session state', () => {
  const current = withV3Runtime(runningRoundFiveSession())
  current.round5_runtime!.revision = 8
  const changed = structuredClone(current)
  changed.round5_runtime!.lanes.lakebase.clients_initiated = 1
  const result = reconcileRunEventSession(current, {
    sequence: 10,
    event: 'lane_update',
    occurred_at: changed.updated_at,
    payload: {
      session: changed,
      lane_id: 'lakebase',
      state: 'connecting',
    },
  })

  expect(result.accepted).toBe(false)
  expect(result.session).toBe(current)
})

it('accepts newer clock projection floors without changing canonical revision', () => {
  const current = withV3Runtime(runningRoundFiveSession())
  current.round5_runtime!.revision = 8
  current.round5_clock_projection = {
    protocol: 'round5-clock-projection-v1',
    bell_id: 'bell-one',
    projection_revision: 1,
    elapsed_ms: { lakebase: 2_000, competitor: 2_000 },
  }
  const projected = structuredClone(current)
  projected.round5_clock_projection = {
    protocol: 'round5-clock-projection-v1',
    bell_id: 'bell-one',
    projection_revision: 2,
    elapsed_ms: { lakebase: 3_000, competitor: 3_000 },
  }

  const selected = selectRound4Session(current, projected)
  expect(selected?.round5_runtime?.revision).toBe(8)
  expect(selected?.round5_clock_projection?.projection_revision).toBe(2)
  expect(selected?.round5_clock_projection?.elapsed_ms.lakebase).toBe(3_000)
})

it('keeps the first terminal v3 verdict absorbing', () => {
  const current = withV3Runtime(runningRoundFiveSession())
  current.state = 'verified'
  current.round5_runtime!.state = 'verified'
  current.round5_runtime!.revision = 8
  for (const laneId of ['lakebase', 'competitor'] as const) {
    current.lanes[laneId].state = 'verified'
    current.round5_runtime!.lanes[laneId] = {
      ...current.round5_runtime!.lanes[laneId],
      phase: 'verified',
      elapsed_at_snapshot_ms: 10_000,
      bell_to_10000_observed_ms: 10_000,
      clients_initiated: 10_000,
      clients_authenticated: 10_000,
      held_clients: 10_000,
    }
  }
  const failed = structuredClone(current)
  failed.state = 'failed'
  failed.failure = 'Exact lane result failed validation'
  failed.round5_runtime!.state = 'failed'
  failed.round5_runtime!.revision = 9
  failed.round5_runtime!.lanes.lakebase = {
    ...failed.round5_runtime!.lanes.lakebase,
    phase: 'failed',
    elapsed_at_snapshot_ms: 11_000,
    bell_to_10000_observed_ms: null,
    clients_initiated: 2_932,
    clients_authenticated: 2_895,
    held_clients: 2_895,
  }
  failed.lanes.lakebase.state = 'failed'

  const selected = selectRound4Session(current, failed)

  expect(selected).toBe(current)
  expect(selected?.state).toBe('verified')
  expect(selected?.round5_runtime?.revision).toBe(8)
})

it('accepts an authoritative same-bell terminal snapshot below stale RUNNING revision', () => {
  const staleRunning = withV3Runtime(runningRoundFiveSession())
  staleRunning.round5_runtime!.revision = 18
  staleRunning.round5_runtime!.lanes.lakebase.clients_initiated = 7_162
  staleRunning.round5_runtime!.lanes.lakebase.clients_authenticated = 7_134
  staleRunning.round5_runtime!.lanes.lakebase.held_clients = 7_134
  staleRunning.round5_runtime!.lanes.lakebase.sampled_queries_succeeded = 32

  const terminal = structuredClone(staleRunning)
  terminal.state = 'failed'
  terminal.failure = 'Exact lane result failed validation'
  terminal.updated_at = '2026-09-15T13:00:12Z'
  terminal.round5_runtime!.revision = 17
  terminal.round5_runtime!.state = 'failed'
  terminal.round5_runtime!.lanes.lakebase.phase = 'failed'
  terminal.lanes.lakebase.state = 'failed'

  const selected = selectRound4Session(staleRunning, terminal)

  expect(selected?.state).toBe('failed')
  expect(selected?.round5_runtime?.revision).toBe(17)
  expect(selected?.round5_runtime?.lanes.lakebase.phase).toBe('failed')
})

it('keeps a terminal same-bell v3 session absorbing against a later RUNNING revision', () => {
  const terminal = withV3Runtime(runningRoundFiveSession())
  terminal.state = 'failed'
  terminal.round5_runtime!.state = 'failed'
  terminal.round5_runtime!.revision = 20
  terminal.round5_runtime!.lanes.lakebase.phase = 'failed'

  const purportedLaterRunning = structuredClone(terminal)
  purportedLaterRunning.state = 'running'
  purportedLaterRunning.updated_at = '2026-09-15T13:00:30Z'
  purportedLaterRunning.round5_runtime!.state = 'running'
  purportedLaterRunning.round5_runtime!.revision = 21
  purportedLaterRunning.round5_runtime!.lanes.lakebase.phase = 'ramping'

  expect(selectRound4Session(terminal, purportedLaterRunning)).toBe(terminal)
})

it('renders 7,020 and 32 of 64 as frozen unscored evidence after failure', () => {
  const failed = withV3Runtime(runningRoundFiveSession())
  failed.state = 'failed'
  failed.failure = 'Fan-in stopped before the exact gate'
  failed.round5_runtime!.state = 'failed'
  failed.round5_runtime!.revision = 17
  failed.round5_runtime!.lanes.lakebase = {
    ...failed.round5_runtime!.lanes.lakebase,
    phase: 'failed',
    elapsed_at_snapshot_ms: 31_250,
    clients_initiated: 7_048,
    clients_authenticated: 7_020,
    held_clients: 7_020,
    sampled_queries_succeeded: 32,
    status: 'Fan-in stopped before the exact gate',
  }
  failed.round5_runtime!.lanes.competitor = {
    ...failed.round5_runtime!.lanes.competitor,
    phase: 'failed',
    elapsed_at_snapshot_ms: 31_250,
    clients_initiated: 7_616,
    clients_authenticated: 7_602,
    held_clients: 7_602,
    sampled_queries_succeeded: 48,
    status: 'Peer lane stopped before the exact gate',
  }
  failed.lanes.lakebase.state = 'failed'
  failed.lanes.competitor.state = 'failed'

  render(
    <RoundFiveProof
      session={failed}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={vi.fn()}
      onTowel={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  const evidence = screen.getByLabelText('Round 5 partial evidence')
  expect(evidence).toHaveTextContent(/Unscored partial evidence/i)
  expect(evidence).toHaveTextContent(
    /7,020 currently held.*7,020 peak held retained.*32 of 64 held-connection checks completed/i,
  )
  expect(evidence).toHaveTextContent(/Clocks frozen.*no winner or margin/i)
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(
    /Bout stopped.*cleanup underway.*unscored evidence.*no winner or margin/i,
  )
  expect(screen.queryByRole('button', { name: /throw in the towel/i })).not.toBeInTheDocument()
  expect(document.body).not.toHaveTextContent(/Verified exact 10,000-client fan-in comparison/i)
})

it('freezes both clocks and removes towel as soon as V4 runtime fails', () => {
  const failedRuntime = withV3Runtime(runningRoundFiveSession())
  failedRuntime.round5_runtime!.state = 'failed'
  failedRuntime.round5_runtime!.lanes.lakebase.phase = 'failed'
  failedRuntime.round5_runtime!.lanes.lakebase.elapsed_at_snapshot_ms = 12_000
  failedRuntime.round5_runtime!.lanes.competitor.elapsed_at_snapshot_ms = 13_000

  render(
    <RoundFiveProof
      session={failedRuntime}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen={false}
      onContinue={() => {}}
      onHome={() => {}}
      onToggleCommentary={() => {}}
    />,
  )

  expect(screen.queryByRole('button', { name: /throw in the towel/i })).not.toBeInTheDocument()
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(
    /Bout stopped · cleanup underway/i,
  )
  expect(document.querySelectorAll('[data-live="true"]')).toHaveLength(0)
})

it('keeps Throw in the Towel enabled while V3 SSE is disconnected', () => {
  const running = withV3Runtime(runningRoundFiveSession())
  const onTowel = vi.fn().mockResolvedValue(undefined)

  render(
    <RoundFiveProof
      session={running}
      roundNumber={5}
      error={null}
      liveEvidenceConnected={false}
      uiReview={false}
      hasNextRound
      commentaryOpen={false}
      onContinue={() => {}}
      onTowel={onTowel}
      onHome={() => {}}
      onToggleCommentary={() => {}}
    />,
  )

  const towel = screen.getByRole('button', { name: /throw in the towel/i })
  expect(towel).toBeEnabled()
  fireEvent.click(towel)
  expect(onTowel).toHaveBeenCalledTimes(1)
})

it('never renders a 2,895-client v3 result as verified', () => {
  const failed = withV3Runtime(runningRoundFiveSession())
  failed.state = 'failed'
  failed.failure = 'Exact lane result failed validation'
  failed.round5_runtime!.state = 'failed'
  failed.round5_runtime!.revision = 9
  failed.round5_runtime!.lanes.lakebase = {
    ...failed.round5_runtime!.lanes.lakebase,
    phase: 'failed',
    elapsed_at_snapshot_ms: 43_948,
    clients_initiated: 2_932,
    clients_authenticated: 2_895,
    held_clients: 2_895,
    status: 'Round 5 exact lane gate failed',
  }
  failed.lanes.lakebase.state = 'failed'
  failed.lanes.lakebase.status = 'Round 5 exact lane gate failed'

  const { container } = render(
    <RoundFiveProof
      session={failed}
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
  expect(container.querySelector('.proof-screen.round5-arena')).toBeInTheDocument()
  expect(
    container.querySelector(
      '.proof-screen.round5-arena[data-session-state="verified"]',
    ),
  ).not.toBeInTheDocument()
  expect(container).not.toHaveTextContent(
    /Exact 10,000-client retained gate verified/i,
  )
})

it('does not infer a legacy protocol when Round 5 evidence is absent', () => {
  const unknown = runningRoundFiveSession()
  unknown.round5_setup = null
  unknown.round5_runtime = null

  render(
    <RoundFiveProof
      session={unknown}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen={false}
      onContinue={() => {}}
      onHome={() => {}}
      onToggleCommentary={() => {}}
    />,
  )

  expect(screen.getAllByText(/Protocol evidence unavailable · no result inferred/i)).toHaveLength(2)
  expect(document.body).not.toHaveTextContent(/Legacy scorecard/i)
})

it('uses V4 runtime over contradictory setup and lane evidence everywhere', () => {
  const session = withV3Runtime(runningRoundFiveSession())
  session.round5_setup!.protocol = 'connection-spike-v1'
  session.round5_setup!.schema_version = 1
  session.round5_setup!.state = 'verified'
  session.lanes.lakebase.state = 'verified'
  session.lanes.lakebase.elapsed_ms = 1
  session.round5_runtime!.lanes.lakebase = {
    ...session.round5_runtime!.lanes.lakebase,
    phase: 'ramping',
    clients_initiated: 7_048,
    clients_authenticated: 7_020,
    held_clients: 7_020,
    sampled_queries_succeeded: 32,
    bell_to_10000_observed_ms: null,
    status: '7,020 / 10,000 clients held',
  }

  render(
    <RoundFiveProof
      session={session}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview={false}
      hasNextRound
      commentaryOpen
      onContinue={() => {}}
      onHome={() => {}}
      onToggleCommentary={() => {}}
    />,
  )

  expect(screen.getByLabelText('Lakebase result')).toHaveTextContent(
    /7,020 \/ 10,000 clients held/i,
  )
  expect(document.body).not.toHaveTextContent(/Legacy scorecard/i)
  expect(document.body).not.toHaveTextContent(/Verified exact 10,000-client fan-in comparison/i)
  expect(document.body).toHaveTextContent(/7,020 currently held/i)
  expect(document.body).not.toHaveTextContent(/10K GATE \+ HOLD ✓/i)
  const replay = JSON.stringify(replayStory(session))
  expect(replay).toContain('7,020')
  expect(replay).not.toContain('Legacy scorecard')
})

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
  expect(screen.getByText(/Phase 1 setup supports · Phase 2 scores exact 10,000-client fan-in · 30s hold/i)).toBeInTheDocument()
  expect(container.querySelector('.proof-lanes')).toBeInTheDocument()
  expect(container.querySelectorAll('.proof-lane')).toHaveLength(2)
  expect(container.querySelector('.lane-rule')).toHaveTextContent('VS')
  expect(container.querySelector('.proof-footer')).toBeInTheDocument()
  expect(container.querySelector('.round5-architecture')).not.toBeInTheDocument()
  expect(container.querySelectorAll('.database-fighter')).toHaveLength(2)

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

it('fails closed to explicit missing fan-in evidence when Round 5 setup is absent', () => {
  const withoutSetup = runningRoundFiveSession()
  withoutSetup.round5_setup = undefined

  const { container } = render(
    <RoundFiveProof
      session={withoutSetup}
      roundNumber={5}
      error={null}
      liveEvidenceConnected
      uiReview
      hasNextRound
      commentaryOpen={false}
      onContinue={vi.fn()}
      onToggleCommentary={vi.fn()}
      onHome={vi.fn()}
    />,
  )

  expect(container).toHaveTextContent(/Protocol evidence unavailable · no result inferred/i)
  expect(container).not.toHaveTextContent(/Legacy scorecard/i)
  expect(container).not.toHaveTextContent(/Phase 2 scores exact 10,000-client fan-in/i)
  expect(container).not.toHaveTextContent(/10,000 authenticated held clients per lane/i)
  expect(container).not.toHaveTextContent(
    /\b64\b|128 attempts|max(?:imum)? 64 concurrent|bounded (?:connection )?(?:check|proof|protocol)/i,
  )
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
    /Lakebase · Untimed preparation · Setup clock starts at the bell/i,
  )
  expect(commentator).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy · Untimed preparation · Setup clock starts at the bell/i,
  )
  expect(commentator).toHaveTextContent(
    /Untimed preparation in progress · Both setup clocks start at the bell/i,
  )
  expect(commentator).not.toHaveTextContent(/clock live/i)
  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  expect(lakebase).toHaveAttribute('data-state', 'sealed')
  expect(competitor).toHaveAttribute('data-state', 'sealed')
  expect(lakebase).toHaveTextContent(/Untimed preparation/i)
  expect(competitor).toHaveTextContent(/Untimed preparation/i)
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
  const towelled = towelledRoundFiveSession()
  // Live progress publishes the exact setup stop before the peer finishes, but
  // a towel can cancel the orchestrator before its final gate matrix is returned.
  towelled.round5_setup!.lanes.lakebase = {
    ...towelled.round5_setup!.lanes.lakebase!,
    stop_gate_evidence: null,
    verified: false,
  }
  const { container } = render(
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
  const verdict = screen.getByRole('status', { name: 'Round 5 setup status' })
  expect(verdict).toHaveTextContent(
    /Lakebase verified first.*RDS PostgreSQL \+ RDS Proxy unverified beyond 4\.50s/i,
  )
  expect(verdict).toHaveTextContent(
    /No declared winner · comparison incomplete · margin N\/A/i,
  )
  expect(screen.getByLabelText('Ringside commentator')).toHaveTextContent(/Live setup call/i)

  fireEvent.click(screen.getByRole('button', { name: /instant replay/i }))
  const replay = screen.getByRole('dialog', { name: /ready a pooled application path/i })
  const story = within(replay).getByLabelText('Three-beat replay story')
  expect(within(story).getAllByRole('heading', { level: 3 }).map((heading) => heading.textContent)).toEqual([
    'Setup',
    'Same test',
    'Takeaway',
  ])
  expect(within(story).getByLabelText('Primary measured result')).toHaveTextContent(
    /Lakebase time to 10,000.*1\.23s.*Selected AWS managed pool.*>4\.50s.*Unverified when stopped/i,
  )
  expect(story).toHaveTextContent(
    /Lakebase produced exact proof.*RDS PostgreSQL \+ RDS Proxy did not.*no completed comparison or margin/i,
  )
  expect(story).not.toHaveTextContent(/neither side verified|neither setup|no verified result/i)
  const evidence = within(replay).getByText(/view full evidence/i).closest('details')
  expect(evidence).not.toHaveAttribute('open')
  fireEvent.click(within(replay).getByText(/view full evidence/i))
  const setupEvidence = within(replay).getByLabelText('Pooled-path setup evidence')
  expect(within(setupEvidence).getByLabelText('Lakebase setup result')).toHaveTextContent(
    /SETUP STOP.*Setup state.*verified.*Stop gate.*EXACT STOP.*FINAL GATE MATRIX NOT RETURNED BEFORE TOWEL/i,
  )
  expect(within(setupEvidence).getByLabelText('Lakebase setup result')).not.toHaveTextContent(
    /stop gate.*not verified/i,
  )
  expect(replay).toHaveTextContent(/Exact setup stop published before the towel.*final expected\/observed gate matrix was not returned/i)
  expect(replay).toHaveTextContent(/Phase 2 held exactly 10,000 authenticated held clients per lane.*All 20,000 held 30s/i)
  expect(replay).toHaveTextContent(/One server bell starts both independent physical lanes.*30-second hold.*cleanup are exact gates/i)
  expect(replay).not.toHaveTextContent(/Non-executable round · No live fairness or timing contract/i)
  fireEvent.click(within(replay).getByRole('button', { name: /back to the ring/i }))
  expect(screen.queryByRole('dialog', { name: /ready a pooled application path/i })).not.toBeInTheDocument()

  fireEvent.click(screen.getByRole('button', { name: /explain to the room/i }))
  const explanation = screen.getByRole('dialog', { name: /for the data engineer/i })
  expect(explanation).toHaveTextContent(
    /what this means.*Lakebase includes pooling for up to 10,000 client connections.*application path without a separate pooling handoff/i,
  )
  expect(explanation).toHaveTextContent(
    /Lakebase pooled-path setup verified at 1\.23s.*RDS PostgreSQL \+ RDS Proxy exceeded 4\.50s without verification.*10,000-client fan-in never started/i,
  )
  expect(explanation).toHaveTextContent(/10,000-client fan-in never started/i)
  expect(explanation).not.toHaveTextContent(
    /neither setup|neither readiness|no verified result/i,
  )
})

it('posts the full towel action row immediately, before cleanup settles', () => {
  const onContinue = vi.fn()
  const towelled = towelledRoundFiveSession()
  // The screenshot state: result posted, cleanup still running backstage.
  towelled.towel = { ...towelled.towel!, state: 'cleaning' }
  towelled.round5_setup = {
    ...towelled.round5_setup!,
    cleanup_retryable: true,
  }
  const view = render(
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

  // Cleanup is still backstage, yet every post-bout action is already offered.
  expect(screen.getByText(/result posted · cleanup backstage/i)).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /instant replay/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /explain to the room/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
  const nextWhileCleaning = screen.getByRole('button', { name: /a · next round/i })
  fireEvent.click(nextWhileCleaning)
  expect(onContinue).toHaveBeenCalledTimes(1)

  // A failed cleanup keeps the retry affordance AND the action row: the towel
  // result is frozen, so the operator is never trapped on it.
  towelled.towel = { ...towelled.towel!, state: 'failed' }
  view.rerender(
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
  expect(screen.getByRole('button', { name: /retry cleanup/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /a · next round/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()

  // And once cleanup is ready the same row still leaves cleanly.
  towelled.towel = { ...towelled.towel!, state: 'ready' }
  towelled.round5_setup = {
    ...towelled.round5_setup!,
    cleanup_retryable: false,
  }
  view.rerender(
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
  const next = screen.getByRole('button', { name: /a · next round/i })
  fireEvent.click(next)
  expect(onContinue).toHaveBeenCalledTimes(2)
  expect(towelled.towel.state).toBe('ready')
  expect(towelled.round5_setup.cleanup_retryable).toBe(false)
})

it('opens the instant replay, explanation and share receipt from a still-cleaning towel', () => {
  const towelled = towelledRoundFiveSession()
  towelled.towel = { ...towelled.towel!, state: 'cleaning' }
  towelled.round5_setup = { ...towelled.round5_setup!, cleanup_retryable: true }
  stubReceiptCanvas()
  render(
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
      onHome={vi.fn()}
    />,
  )

  // Instant replay reads the towelled evidence and never declares a win.
  fireEvent.click(screen.getByRole('button', { name: /instant replay/i }))
  const replay = screen.getByRole('dialog', { name: /ready a pooled application path/i })
  expect(replay).toHaveTextContent(/no completed comparison or margin/i)
  fireEvent.click(within(replay).getByRole('button', { name: /back to the ring/i }))

  // Explain to the room works on the incomplete result.
  fireEvent.click(screen.getByRole('button', { name: /explain to the room/i }))
  const explanation = screen.getByRole('dialog', { name: /for the data engineer/i })
  expect(explanation).toHaveTextContent(/10,000-client fan-in never started/i)
  fireEvent.click(within(explanation).getByRole('button', { name: /back to the ring/i }))

  // Share reuses the receipt mechanism with the honest towel caption.
  fireEvent.click(screen.getByRole('button', { name: /share the receipt/i }))
  const share = screen.getByRole('dialog', { name: /share the proof/i })
  const poster = within(share).getByLabelText(/poster preview/i)
  expect(poster).toHaveTextContent(/TOWEL RESULT · THIS ROUND/i)
  expect(poster).toHaveTextContent(/NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N\/A/i)
  // The stopped-short result is never dressed up as a win.
  expect(poster).not.toHaveTextContent(/Earlier pooled path/i)
})

it('keeps the one-sided Round 5 share copy aligned with the stopped receipt', () => {
  const caption = linkedInReceipt(towelledRoundFiveSession(), 5)

  expect(caption).toMatch(
    /Lakebase reached verified pooled-path setup in 1\.23s.*RDS PostgreSQL \+ RDS Proxy was still unverified beyond 4\.50s/i,
  )
  expect(caption).toMatch(/10,000-client fan-in never started.*no winner or margin/i)
  expect(caption).toMatch(/NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N\/A/i)
  expect(caption).not.toMatch(
    /neither setup|no verified result|both passed 128|readiness result declared/i,
  )
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
    /Lakebase reached pooled-path setup at 1\.23s · RDS PostgreSQL \+ RDS Proxy setup clock still running · No comparison yet/i,
  )

  const competitorBefore = displayedSeconds(competitor)
  await act(async () => { await vi.advanceTimersByTimeAsync(96) })

  expect(lakebase.querySelector('.lane-time')).toHaveTextContent('1.23s')
  expect(displayedSeconds(competitor)).toBeGreaterThan(competitorBefore)
})

it('keeps both Round 5 clocks running and shows evidence reconnecting when live evidence disconnects', async () => {
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
  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  const lakebaseTime = lakebase.querySelector('.lane-time')!
  const competitorTime = competitor.querySelector('.lane-time')!
  expect(arena).toHaveAttribute('data-session-state', 'offline')
  // Sol Ultra contract: the shared server-T0 clock keeps running through an SSE
  // interruption; only the evidence goes stale. The clock is live, not paused.
  expect(screen.getByText(/live evidence reconnecting · clock live/i)).toBeInTheDocument()
  expect(lakebaseTime).toHaveAttribute('data-live', 'true')
  expect(competitorTime).toHaveAttribute('data-live', 'true')
  expect(lakebaseTime).toHaveAttribute('data-evidence-stale', 'true')
  expect(competitorTime).toHaveAttribute('data-evidence-stale', 'true')
  const commentator = screen.getByLabelText('Ringside commentator')
  expect(commentator).toHaveTextContent(/Live setup call/i)
  expect(commentator).not.toHaveTextContent(/ON THE WIRE/i)
  expect(commentator).toHaveTextContent(
    /Lakebase · Live evidence interrupted · Clock continues · evidence stale while reconnecting/i,
  )
  expect(commentator).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy · Live evidence interrupted · Clock continues · evidence stale while reconnecting/i,
  )
  expect(commentator).toHaveTextContent(
    /Live evidence interrupted · No new result inferred · Reconnecting/i,
  )
  expect(commentator).not.toHaveTextContent(/setup clock live/i)

  // The elapsed clocks advance locally rather than freezing while evidence is
  // interrupted; they stop only at the exact 10,000-client observed gate.
  const lakebaseBefore = displayedSeconds(lakebase)
  const competitorBefore = displayedSeconds(competitor)
  await act(async () => { await vi.advanceTimersByTimeAsync(128) })
  expect(displayedSeconds(lakebase)).toBeGreaterThan(lakebaseBefore)
  expect(displayedSeconds(competitor)).toBeGreaterThan(competitorBefore)
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
  expect(arena?.querySelector('.round5-architecture')).not.toBeInTheDocument()
  expect(arena?.querySelectorAll('.database-fighter')).toHaveLength(2)
  const lakebase = screen.getByLabelText('Lakebase result')
  const competitor = screen.getByLabelText('RDS PostgreSQL + RDS Proxy result')
  expect(lakebase).toHaveAttribute('data-corner', 'red')
  expect(competitor).toHaveAttribute('data-corner', 'blue')
  expect(lakebase.querySelector('.lane-time')).toHaveTextContent('3.11s')
  expect(competitor.querySelector('.lane-time')).toHaveTextContent('4.11s')
  expect(container.querySelector('.remembered')).toHaveTextContent(
    /Verified exact 10,000-client fan-in comparison.*Lakebase reached exactly 10,000 held clients.*sooner/i,
  )

  const commentator = screen.getByLabelText('Ringside commentator')
  expect(commentator).toHaveTextContent(/Final verified call/i)
  expect(commentator).toHaveTextContent(
    /Lakebase · 10,000 clients held at 3\.11s · Exact 10,000-client fan-in verified/i,
  )
  expect(commentator).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy · 10,000 clients held at 4\.11s · Exact 10,000-client fan-in verified/i,
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
    /what this means.*Lakebase includes pooling for up to 10,000 client connections.*application path without a separate pooling handoff/i,
  )
  expect(ringsideTake).toHaveTextContent(/question for the room.*when does a missed job window justify keeping a pool ready/i)
  expect(ringsideTake).toHaveTextContent(
    /what we proved.*Both paths connected and held 10,000 clients from the same start.*20,000 held for at least 30 seconds.*passed 64 held-connection checks.*provider-selected password exchange inside verify-full TLS.*TLS-protected password.*challenge-response password.*inside connection timing.*Setup.*Lakebase 12\.35s.*selected AWS path 24\.00s.*transaction throughput were not measured/i,
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
  expect(within(shareReceipt).getByLabelText('Lakebase receipt result')).toHaveTextContent('3.11s')
  expect(within(shareReceipt).getByLabelText(
    /RDS PostgreSQL(?: \+ RDS Proxy)? receipt result/i,
  )).toHaveTextContent('4.11s')
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).not.toHaveTextContent(
    /not timed|non-executable/i,
  )
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).toHaveTextContent(
    /BOTH PATHS CONNECTED AND HELD 10,000 CLIENTS FROM THE SAME START.*LAKEBASE REACHED 10,000.*1\.00s SOONER.*EXACT 10,000-CLIENT MARGIN 1\.00s/i,
  )
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).toHaveTextContent(/Start gap 0\.750ms/i)
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).not.toHaveTextContent(/Start gap 1\.235ms/i)
  // The modal shows ONE clean card -- the generated PNG (the full Fable layout).
  // The text poster stays mounted only as a visually-hidden mirror for this test
  // and for screen readers; it is never a second visible box beside the bitmap.
  expect(within(shareReceipt).getByLabelText('Verified result poster preview')).toHaveClass('receipt-poster--mirror')
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

  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(
    /primary setup result.*one setup verified/i,
  )
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy setup verified 24\.00s.*Lakebase not verified.*exact dual-10,000-client fan-in did not run.*no declared winner.*comparison incomplete/i,
  )
  expect(screen.queryByLabelText(/warm burst evidence|setup result|fair proof contract|managed component disclosure/i)).not.toBeInTheDocument()
  expect(screen.getByText('Technical details')).toBeInTheDocument()
  fireEvent.click(screen.getByText('Technical details'))
  expect(screen.getByText(/progress reached · not finalized · stop gate not verified/i)).toBeInTheDocument()
  expect(screen.queryByText(/^verified · stop gate not verified$/i)).not.toBeInTheDocument()
  expect(screen.queryByText(/fence-token|journal=row|workflow-id/i)).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /retry cleanup/i })).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /ring again/i })).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: /fight card/i })).toBeInTheDocument()

  // Backstage cleanup is now DECOUPLED from the end user (Round-5-scoped): a
  // failed bout with cleanup still pending shows NO "Retry cleanup" button, NO
  // "Cleanup needs attention" / "MAY STILL BE RUNNING AND BILLING" banner, and
  // does not fence terminal navigation. Cleanup converges automatically backstage.
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
  expect(screen.queryByRole('button', { name: /retry cleanup/i })).not.toBeInTheDocument()
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  expect(screen.queryByText(/MAY STILL BE RUNNING AND BILLING/i)).not.toBeInTheDocument()
  expect(screen.queryByText(/cleanup needs attention/i)).not.toBeInTheDocument()

  // A cleanup_failure diagnostic string on a failed bout is likewise never shown
  // to the end user as a billing/attention banner.
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
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  expect(screen.queryByText(abandonedDiagnostic)).not.toBeInTheDocument()
  expect(screen.queryByRole('button', { name: /retry cleanup/i })).not.toBeInTheDocument()

  // A VERIFIED Round 5 with cleanup still pending keeps its win AND every
  // end-user action: Instant replay, What it cost, Share, and Next round are
  // available, with no retry button and no cleanup banner.
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
  expect(screen.queryByRole('button', { name: /retry cleanup/i })).not.toBeInTheDocument()
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: /instant replay/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()

  // Even with a cleanup_failure diagnostic string set, a verified bout shows no
  // "Cleanup needs attention" alert and keeps its win and its actions.
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
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  expect(screen.queryByText(/cleanup needs attention/i)).not.toBeInTheDocument()
  expect(screen.getByText(/verified exact 10,000-client fan-in comparison/i)).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /next round/i })).toBeInTheDocument()

  const towelled: DemoSession = {
    ...failed,
    state: 'towelled',
    failure: null,
    lanes: {
      lakebase: {
        ...failed.lanes.lakebase,
        state: 'towelled',
        evidence: {},
      },
      competitor: {
        ...failed.lanes.competitor,
        state: 'verified',
        evidence: {},
      },
    },
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
  // The posted towel offers its full action row immediately, cleanup or not.
  expect(screen.getByRole('button', { name: /a · next round/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /instant replay/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()

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
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(
    /RDS PostgreSQL \+ RDS Proxy verified first.*Lakebase unverified.*no declared winner.*comparison incomplete/i,
  )
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
  expect(screen.getByLabelText('Aurora Serverless v2 + RDS Proxy result')).toHaveTextContent('4.11s')
  expect(within(container).queryByLabelText('Managed component disclosure')).not.toBeInTheDocument()

  fireEvent.click(screen.getByRole('button', { name: /explain to the room/i }))
  const explanation = screen.getByRole('dialog', { name: /for the data engineer/i })
  expect(explanation).toHaveTextContent(
    /what we proved.*Both paths connected and held 10,000 clients from the same start.*20,000 held for at least 30 seconds.*passed 64 held-connection checks.*provider-selected password exchange inside verify-full TLS.*TLS-protected password.*challenge-response password.*inside connection timing.*Setup.*Lakebase 12\.35s.*selected AWS path 24\.00s.*transaction throughput were not measured/i,
  )
  expect(explanation.querySelector('details')).toBeNull()
  expect(explanation).not.toHaveTextContent(/full verified proof|component disclosure|supporting changes/i)
  expect(explanation).not.toHaveTextContent(/Aurora unexecuted|not executed or scored/i)
})

it('offers an instant replay with primary fan-in and supporting setup evidence', () => {
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

  const replay = screen.getByRole('dialog', { name: /ready a pooled application path/i })
  expect(replay).toHaveTextContent(/instant replay · round 5/i)

  // The presenter surface has one three-beat story and one metric treatment.
  const story = within(replay).getByLabelText('Three-beat replay story')
  expect(story.querySelectorAll('.replay-beat')).toHaveLength(3)
  expect(story.querySelectorAll('.replay-primary-metric')).toHaveLength(1)
  expect(story).toHaveTextContent(/setup.*same test.*takeaway/i)
  expect(story).toHaveTextContent(/3\.11s/)
  expect(story).toHaveTextContent(/4\.11s/)
  expect(story).toHaveTextContent(/Phase 2 held exactly 10,000 authenticated held clients per lane.*all 20,000 held 30s.*64 held-connection checks passed per lane.*proved multiplexing/i)
  expect(story).toHaveTextContent(/Direct AWS connections and other pools were not tested/i)
  expect(story).not.toHaveTextContent('N/A')

  const evidence = within(replay).getByText(/view full evidence/i).closest('details')!
  expect(evidence).not.toHaveAttribute('open')
  fireEvent.click(within(evidence).getByText(/view full evidence/i))

  // The evidence keeps setup fairness, the full matrix, and component inventory.
  expect(replay).toHaveTextContent('0.750ms')
  expect(replay).not.toHaveTextContent('1.235ms')
  expect(within(replay).getByLabelText('Round 5 detailed proof')).toHaveTextContent(
    /full verified proof.*10,000-client fan-in is scored.*pooled-path setup is supporting.*exactly 10,000 authenticated held clients.*30s hold.*64 held-connection checks/i,
  )
  expect(within(replay).getByLabelText('Generator telemetry evidence')).toHaveTextContent(
    /generator telemetry.*peak cpu capacity.*31\.0%.*peak rss.*7\.00 GiB.*15\.00 GiB.*peak fds.*20264.*65535.*generator-owned event-loop peak.*4\.50 ms.*raw event-loop wall-lag peak.*6\.25 ms.*external scheduling-lag peak.*6\.25 ms.*ephemeral-port reserve.*18232/i,
  )
  expect(within(replay).getByLabelText('Managed component disclosure')).toHaveTextContent(
    /selected AWS managed pooling path.*new RDS Proxy \+ 8 supporting changes.*direct AWS connections.*existing Proxy.*PgBouncer.*application pooling.*were not tested/i,
  )

  // Every evidence step is Round 5 specific, with no nested accordion.
  expect(within(replay).getAllByText(/setup workflows|setup clock|exact client fan-in|common hold/i).length).toBeGreaterThanOrEqual(3)
  expect(replay.querySelectorAll('details')).toHaveLength(1)
  expect(replay).not.toHaveTextContent(/will appear when this round adapter is executable/i)

  // The nine journaled AWS mutations are the substance of the selected reference path.
  const calls = within(replay).getByText(
    /selected AWS reference path needed nine journaled resource mutations.*drained and rebound/i,
  ).closest('.replay-evidence-step')!
  for (const resource of [
    'proxy_security_group', 'proxy_default_egress', 'proxy_ingress', 'proxy_egress',
    'runner_egress', 'rds_ingress', 'rds_proxy', 'proxy_target_group', 'proxy_target',
  ]) {
    expect(calls).toHaveTextContent(`journal: ${resource}`)
  }
  expect(calls).toHaveTextContent(/built-in Lakebase pooled endpoint/i)
  expect(calls).toHaveTextContent(/deregister target.*zero targets.*re-register same target/i)
  expect(replay).toHaveTextContent(/connection churn p50 \/ p95 \/ p99.*80\.00 ms \/ 150\.00 ms \/ 201\.46 ms/i)

  fireEvent.click(within(replay).getByRole('button', { name: /back to the ring/i }))
  expect(screen.queryByRole('dialog', { name: /ready a pooled application path/i })).not.toBeInTheDocument()
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

  // The arena while exact fan-in is running.
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

// --- Two-phase live lane: "Time to 10,000" clock + "Hold checks" region ---

const twoPhaseArenaProps = {
  roundNumber: 5,
  error: null,
  liveEvidenceConnected: true,
  uiReview: false,
  hasNextRound: true,
  commentaryOpen: true,
  onContinue: () => {},
  onToggleCommentary: () => {},
  onHome: () => {},
}

function arenaLanes(container: HTMLElement): { lakebase: HTMLElement; competitor: HTMLElement } {
  const lanes = Array.from(container.querySelectorAll<HTMLElement>('.proof-lane'))
  return { lakebase: lanes[0], competitor: lanes[1] }
}

it('shows both lanes the same waiting scaffold while ramping (no missing region)', () => {
  const s = withV3Runtime(runningRoundFiveSession())
  s.round5_runtime!.lanes.lakebase.phase = 'ramping'
  s.round5_runtime!.lanes.lakebase.held_clients = 4_200
  s.round5_runtime!.lanes.competitor.phase = 'provisioning_proxy'
  const { container } = render(<RoundFiveProof session={s} {...twoPhaseArenaProps} />)
  const { lakebase, competitor } = arenaLanes(container)

  expect(lakebase).toHaveTextContent(/Time to 10,000 \(in progress\)/i)
  expect(competitor).toHaveTextContent(/Time to 10,000 \(in progress\)/i)
  expect(lakebase.querySelector('.lane-hold-checks')).toHaveAttribute('data-waiting', 'true')
  expect(competitor.querySelector('.lane-hold-checks')).toHaveAttribute('data-waiting', 'true')
  expect(lakebase).toHaveTextContent(/Waiting to reach 10,000/i)
  expect(competitor).toHaveTextContent(/Waiting to reach 10,000/i)
  expect(screen.getByLabelText('Round 5 live explainer')).toHaveTextContent(/No winner until both lanes verify/i)
})

it('labels the frozen clock final and explains hold checks when Lakebase hits 10,000 while Aurora still ramps', () => {
  const s = withV3Runtime(runningRoundFiveSession())
  s.round5_runtime!.lanes.lakebase = {
    ...s.round5_runtime!.lanes.lakebase,
    phase: 'holding',
    bell_to_10000_observed_ms: 3_112.673,
    elapsed_at_snapshot_ms: 3_112.673,
    clients_initiated: 10_000,
    clients_authenticated: 10_000,
    held_clients: 10_000,
    sampled_queries_succeeded: 32,
    status: '10,000 / 10,000 clients held',
  }
  s.round5_runtime!.lanes.competitor.phase = 'provisioning_proxy'
  const { container } = render(<RoundFiveProof session={s} {...twoPhaseArenaProps} />)
  const { lakebase, competitor } = arenaLanes(container)

  // Region A: frozen, explicitly labelled stopped.
  expect(lakebase).toHaveTextContent(/Time to 10,000 — stopped/i)
  expect(lakebase.querySelector('.lane-timer-caption')).toHaveAttribute('data-frozen', 'true')
  // Region B: hold and checks shown simultaneously, plain language.
  expect(lakebase).toHaveTextContent(/Now holding all 10,000 connections for 30 seconds/i)
  expect(lakebase).toHaveTextContent(/During hold · 32 \/ 64 checks on held connections/i)
  expect(lakebase).not.toHaveTextContent(/samples|snapshots|probe|score locked|proving it holds/i)

  // Aurora shares the scaffold, still racing to its own 10,000, plain reason shown.
  expect(competitor).toHaveTextContent(/Time to 10,000 \(in progress\)/i)
  expect(competitor).toHaveTextContent(/Waiting to reach 10,000/i)
  expect(competitor).toHaveTextContent(/RDS Proxy/i)
  expect(competitor).not.toHaveTextContent(/checks on held connections/i)

  expect(screen.getByLabelText('Round 5 live explainer')).toHaveTextContent(/No winner until both lanes verify/i)
})

it('keeps the time-to-10,000 clock frozen while hold checks run', async () => {
  vi.useFakeTimers()
  const s = withV3Runtime(runningRoundFiveSession())
  s.round5_runtime!.lanes.lakebase = {
    ...s.round5_runtime!.lanes.lakebase,
    phase: 'holding',
    bell_to_10000_observed_ms: 3_112.673,
    elapsed_at_snapshot_ms: 3_112.673,
    clients_initiated: 10_000,
    clients_authenticated: 10_000,
    held_clients: 10_000,
    sampled_queries_succeeded: 32,
    status: '10,000 / 10,000 clients held',
  }
  const { container } = render(<RoundFiveProof session={s} {...twoPhaseArenaProps} />)
  const { lakebase } = arenaLanes(container)
  const frozen = displayedSeconds(lakebase)
  expect(frozen).toBeCloseTo(3.11, 2)

  await act(async () => {
    await vi.advanceTimersByTimeAsync(2_000)
  })

  expect(displayedSeconds(lakebase)).toBe(frozen)
})

it('marks a locally verified lane as waiting on the other lane, never the bout winner', () => {
  const s = withV3Runtime(runningRoundFiveSession())
  s.round5_runtime!.lanes.lakebase = {
    ...s.round5_runtime!.lanes.lakebase,
    phase: 'verified',
    bell_to_10000_observed_ms: 3_112.673,
    elapsed_at_snapshot_ms: 3_112.673,
    clients_initiated: 10_000,
    clients_authenticated: 10_000,
    held_clients: 10_000,
    sampled_queries_succeeded: 64,
    status: 'Exact 10,000-client retained gate verified',
  }
  s.round5_runtime!.lanes.competitor.phase = 'ramping'
  const { container } = render(<RoundFiveProof session={s} {...twoPhaseArenaProps} />)
  const { lakebase } = arenaLanes(container)

  expect(lakebase.querySelector('.lane-hold-checks')).toHaveAttribute('data-phase', 'verified')
  expect(lakebase).toHaveTextContent(/This lane verified · waiting for the other/i)
  expect(lakebase).not.toHaveTextContent(/winner/i)
  expect(screen.getByLabelText('Round 5 live explainer')).toHaveTextContent(/No winner until both lanes verify/i)
})

it('shows both lanes hold-complete once verified, with no shared finish-line flourish', () => {
  const s = withV3Runtime(runningRoundFiveSession())
  s.state = 'verified'
  s.round5_runtime!.state = 'verified'
  for (const id of ['lakebase', 'competitor'] as const) {
    s.round5_runtime!.lanes[id] = {
      ...s.round5_runtime!.lanes[id],
      phase: 'verified',
      bell_to_10000_observed_ms: id === 'lakebase' ? 3_112.673 : 3_212.673,
      elapsed_at_snapshot_ms: id === 'lakebase' ? 3_112.673 : 3_212.673,
      clients_initiated: 10_000,
      clients_authenticated: 10_000,
      held_clients: 10_000,
      sampled_queries_succeeded: 64,
      status: 'Exact 10,000-client retained gate verified',
    }
    s.lanes[id].state = 'verified'
  }
  const { container } = render(<RoundFiveProof session={s} {...twoPhaseArenaProps} />)
  const { lakebase, competitor } = arenaLanes(container)

  expect(lakebase).toHaveTextContent(/This lane verified/i)
  expect(competitor).toHaveTextContent(/This lane verified/i)
  expect(lakebase).not.toHaveTextContent(/waiting for the other/i)
})

it('preserves the frozen time-to-10,000 and says verification failed after reaching 10k', () => {
  const s = withV3Runtime(runningRoundFiveSession())
  s.state = 'failed'
  s.failure = 'Telemetry gate failed after the hold'
  s.round5_runtime!.state = 'failed'
  s.round5_runtime!.lanes.lakebase = {
    ...s.round5_runtime!.lanes.lakebase,
    phase: 'failed',
    bell_to_10000_observed_ms: 3_112.673,
    elapsed_at_snapshot_ms: 3_112.673,
    clients_initiated: 10_000,
    clients_authenticated: 10_000,
    held_clients: 10_000,
    sampled_queries_succeeded: 41,
    status: 'Round 5 exact lane gate or barrier evidence failed',
  }
  s.lanes.lakebase.state = 'failed'
  const { container } = render(<RoundFiveProof session={s} {...twoPhaseArenaProps} />)
  const { lakebase } = arenaLanes(container)

  expect(lakebase).toHaveTextContent(/Time to 10,000 — stopped/i)
  expect(displayedSeconds(lakebase)).toBeCloseTo(3.11, 2)
  expect(lakebase).not.toHaveTextContent(/Could not verify/i)
  expect(lakebase).toHaveTextContent(/Failed during the 30-second hold/i)
})

it('keeps stale-evidence copy calm and sentence-case, not a shouty crash banner', () => {
  const s = withV3Runtime(runningRoundFiveSession())
  s.round5_runtime!.lanes.lakebase.phase = 'ramping'
  s.round5_runtime!.lanes.lakebase.status = '6,000 / 10,000 clients held'
  const { container } = render(
    <RoundFiveProof session={s} {...twoPhaseArenaProps} liveEvidenceConnected={false} />,
  )
  const { lakebase } = arenaLanes(container)

  expect(lakebase).toHaveTextContent(/Live evidence catching up · clock still running/i)
  expect(lakebase).not.toHaveTextContent(/LIVE EVIDENCE OFFLINE/)
  expect(lakebase.querySelector('.lane-status')).toHaveAttribute('data-subordinate', 'true')
})

// --- Towel thrown mid-hold, after a lane already locked time-to-10,000 ---

function towelDuringHoldRoundFiveSession(): DemoSession {
  // Lakebase locked its exact 10,000 at 14.62s and was 32/64 checks into the
  // 30-second hold when the operator towelled; Aurora never reached 10,000. The
  // server marks every non-verified runtime lane `cancelled` on a towel and
  // preserves each reached lane's `bell_to_10000_observed_ms`.
  const s = withV3Runtime(runningRoundFiveSession())
  s.state = 'towelled'
  s.updated_at = '2026-09-15T13:00:20Z'
  s.round5_runtime!.state = 'towelled'
  s.round5_runtime!.revision = 12
  s.round5_runtime!.lanes.lakebase = {
    ...s.round5_runtime!.lanes.lakebase,
    phase: 'cancelled',
    bell_to_10000_observed_ms: 14_620,
    elapsed_at_snapshot_ms: 14_620,
    clients_initiated: 10_000,
    clients_authenticated: 10_000,
    held_clients: 10_000,
    sampled_queries_succeeded: 32,
    status: 'Towel thrown during the 30-second hold',
  }
  s.round5_runtime!.lanes.competitor = {
    ...s.round5_runtime!.lanes.competitor,
    phase: 'cancelled',
    bell_to_10000_observed_ms: null,
    elapsed_at_snapshot_ms: 32_880,
    clients_initiated: 6_000,
    clients_authenticated: 6_000,
    held_clients: 6_000,
    sampled_queries_succeeded: 0,
    status: 'Towel thrown before reaching 10,000 clients',
  }
  s.lanes.lakebase.state = 'towelled'
  s.lanes.competitor.state = 'towelled'
  s.towel = {
    state: 'cleaning',
    requested_at: '2026-09-15T13:00:20Z',
    censored_lower_bounds_ms: { competitor: 32_880 },
    restore_started: false,
    cleanup_failure: null,
  }
  s.comparison = null
  s.remembered_result = null
  return s
}

it('preserves a lane\'s locked 10,000 time on a mid-hold towel and calls the hold interrupted, not failed', () => {
  const { container } = render(
    <RoundFiveProof session={towelDuringHoldRoundFiveSession()} {...twoPhaseArenaProps} />,
  )
  const { lakebase, competitor } = arenaLanes(container)

  // Lakebase reached 10,000: keep the frozen 14.62s, never "Not timed".
  expect(lakebase).toHaveTextContent(/Time to 10,000 — stopped/i)
  expect(lakebase.querySelector('.lane-timer-caption')).toHaveAttribute('data-frozen', 'true')
  expect(displayedSeconds(lakebase)).toBeCloseTo(14.62, 2)
  expect(lakebase).not.toHaveTextContent(/Not timed/i)
  expect(lakebase.querySelector('.lane-time')).not.toHaveAttribute('data-untimed', 'true')
  // The hold was cut short by the operator -- honest and neutral, not a failure.
  expect(lakebase).toHaveTextContent(/Hold not completed · towel thrown/i)
  expect(lakebase).not.toHaveTextContent(/Failed during the 30-second hold/i)
  expect(lakebase).not.toHaveTextContent(/Could not verify/i)
  expect(lakebase).not.toHaveTextContent(/This lane verified/i)

  // Aurora truly never reached 10,000: not-reached caption + lower bound, untouched.
  expect(competitor).toHaveTextContent(/Time to 10,000 \(not reached\)/i)
  expect(competitor).toHaveTextContent(/Stopped before reaching 10,000 clients/i)
  expect(competitor).not.toHaveTextContent(/Failed during the 30-second hold/i)
  expect(competitor.querySelector('.lane-time')).toHaveTextContent(/>32\.88s/)

  // Bout-level honesty is unchanged: no verified result, no winner, no margin.
  expect(screen.getByRole('status', { name: 'Round 5 setup status' })).toHaveTextContent(
    /no exact verified result · no declared winner · margin N\/A/i,
  )

  // The four immediate towel actions are still present.
  expect(screen.getByRole('button', { name: /instant replay/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /explain to the room/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /share the receipt/i })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: /a · (next round|fight card)/i })).toBeInTheDocument()
})

it('keeps a hold-completed lane verified through a towel of the other lane', () => {
  // Lakebase verified its full hold before the towel; the server leaves a
  // verified runtime lane verified and only cancels the unverified sibling.
  const s = towelDuringHoldRoundFiveSession()
  s.round5_runtime!.lanes.lakebase = {
    ...s.round5_runtime!.lanes.lakebase,
    phase: 'verified',
    sampled_queries_succeeded: 64,
    status: 'Exact 10,000-client retained gate verified',
  }
  s.lanes.lakebase.state = 'verified'
  const { container } = render(
    <RoundFiveProof session={s} {...twoPhaseArenaProps} />,
  )
  const { lakebase } = arenaLanes(container)

  expect(lakebase).toHaveTextContent(/Time to 10,000 — stopped/i)
  expect(displayedSeconds(lakebase)).toBeCloseTo(14.62, 2)
  expect(lakebase).toHaveTextContent(/This lane verified/i)
  expect(lakebase).not.toHaveTextContent(/Hold not completed/i)
  expect(lakebase).not.toHaveTextContent(/Failed during the 30-second hold/i)
  expect(lakebase).not.toHaveTextContent(/Not timed/i)
})

// --- Screenshot regression: V4 towel mid-hold, all surfaces must agree ---

/**
 * The exact live state from the reported defect, reproduced end-to-end:
 * Lakebase reached 10,000 at 14.15s and was mid-hold when towelled; Aurora never
 * reached 10,000 (lower bound 23.89s). The setup-stop poison the backend used to
 * write is recreated so the fix is proven to ignore it: `lanes.lakebase` is
 * VERIFIED at 0.01s and `towel.lakebase_verified_ms = 10`. No surface may show
 * 0.01s, "EXACT VERIFIED", or "fan-in never started" for Lakebase.
 */
function screenshotTowelRoundFiveSession(): DemoSession {
  const s = withV3Runtime(runningRoundFiveSession())
  const competitor = FALLBACK_CATALOG.competitors.find((item) => item.id === 'aurora_serverless_v2')!
  s.competitor = competitor
  s.lanes.competitor.name = 'Aurora Serverless v2 + RDS Proxy'
  s.round5_setup!.lanes.competitor!.name = 'Aurora Serverless v2 + RDS Proxy'
  s.state = 'towelled'
  s.updated_at = '2026-09-17T15:38:21Z'
  s.round5_runtime!.state = 'towelled'
  s.round5_runtime!.revision = 14
  s.round5_runtime!.lanes.lakebase = {
    ...s.round5_runtime!.lanes.lakebase,
    phase: 'cancelled',
    bell_to_10000_observed_ms: 14_150,
    elapsed_at_snapshot_ms: 14_150,
    clients_initiated: 10_000,
    clients_authenticated: 10_000,
    held_clients: 10_000,
    peak_clients_authenticated: 10_000,
    peak_held_clients: 10_000,
    sampled_queries_succeeded: 41,
    status: 'Towel thrown during the 30-second hold',
  }
  s.round5_runtime!.lanes.competitor = {
    ...s.round5_runtime!.lanes.competitor,
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
  // The backend poison this fix must ignore: setup stop promoted to a verified
  // lane at ~0.01s, and copied into the transitional Round 3 towel field.
  s.round5_setup!.lanes.lakebase!.state = 'verified'
  s.round5_setup!.lanes.lakebase!.setup_elapsed_ms = 10
  s.lanes.lakebase = {
    ...s.lanes.lakebase,
    state: 'verified',
    elapsed_ms: 10,
    status: 'Misleading pooled-path setup timing',
  }
  s.lanes.competitor = { ...s.lanes.competitor, state: 'towelled' }
  s.towel = {
    state: 'cleaning',
    requested_at: '2026-09-17T15:38:21Z',
    censored_lower_bounds_ms: { competitor: 23_890 },
    lakebase_verified_ms: 10,
    restore_started: false,
    cleanup_failure: null,
  }
  s.comparison = null
  s.remembered_result = null
  return s
}

it('screenshot fixture: arena shows Lakebase 14.15 reached + hold interrupted, Aurora not reached', () => {
  const { container } = render(
    <RoundFiveProof session={screenshotTowelRoundFiveSession()} {...twoPhaseArenaProps} />,
  )
  const { lakebase, competitor } = arenaLanes(container)
  expect(lakebase).toHaveTextContent(/Time to 10,000 — stopped/i)
  expect(displayedSeconds(lakebase)).toBeCloseTo(14.15, 2)
  expect(lakebase).toHaveTextContent(/Hold not completed · towel thrown/i)
  expect(lakebase).not.toHaveTextContent(/0\.01s/)
  expect(competitor).toHaveTextContent(/Time to 10,000 \(not reached\)/i)
  expect(competitor.querySelector('.lane-time')).toHaveTextContent(/>23\.89s/)
})

it('screenshot fixture: classifier keeps the bout honest (no exact verified, no winner)', () => {
  const session = screenshotTowelRoundFiveSession()
  const classified = classifyOutcome(session)
  expect(classified.evidence.lakebase.exactMs).toBeNull()
  expect(classified.evidence.competitor.exactMs).toBeNull()
  expect(classified.contractComplete).toBe(false)
  expect(classified.formalWinner).toBeNull()
  expect(classified.marginMs).toBeNull()
  expect(classified.headline).toMatch(
    /NO EXACT VERIFIED RESULT · NO DECLARED WINNER · MARGIN N\/A/i,
  )
})

it('screenshot fixture: canonical presentation never leaks the setup stop', () => {
  const session = screenshotTowelRoundFiveSession()
  const lakebase = roundFiveLanePresentation(session, 'lakebase')!
  const competitor = roundFiveLanePresentation(session, 'competitor')!
  expect(lakebase.semantic).toBe('reached_hold_interrupted')
  expect(lakebase.verified).toBe(false)
  expect(lakebase.value).toBe('14.15s')
  expect(lakebase.status).toBe('10,000 CLIENTS REACHED · HOLD INTERRUPTED')
  expect(lakebase.status).not.toMatch(/·\s*EXACT VERIFIED/)
  expect(competitor.semantic).toBe('not_reached_lower_bound')
  expect(competitor.value).toBe('>23.89s')
})

it('screenshot fixture: receiptPresentation uses 14.15 evidence, never 0.01/EXACT VERIFIED', () => {
  const receipt = receiptPresentation(screenshotTowelRoundFiveSession(), 'round')
  expect(receipt.lakebaseValue).toBe('14.15s')
  expect(receipt.lakebaseStatus).toBe('10,000 CLIENTS REACHED · HOLD INTERRUPTED')
  expect(receipt.lakebaseStatus).not.toMatch(/·\s*EXACT VERIFIED/)
  expect(receipt.lakebaseValue).not.toMatch(/0\.01/)
  expect(receipt.competitorValue).toBe('>23.89s')
  expect(receipt.competitorStatus).toMatch(/NOT REACHED · UNVERIFIED WHEN STOPPED · LOWER BOUND/i)
  expect(receipt.winner).toBeNull()
  expect(receipt.verdictLabel).toBe('TOWEL RESULT · THIS ROUND')
  expect(receipt.competitorLabel).toMatch(/RDS Proxy/i)
})

it('screenshot fixture: clicking Share renders the honest Lakebase lane node in the DOM', () => {
  stubReceiptCanvas()
  render(
    <RoundFiveProof session={screenshotTowelRoundFiveSession()} {...twoPhaseArenaProps} />,
  )
  fireEvent.click(screen.getByRole('button', { name: /share the receipt/i }))
  const share = screen.getByRole('dialog', { name: /share the proof/i })
  const poster = within(share).getByLabelText(/poster preview/i)
  expect(poster).not.toHaveTextContent(/Verified result poster/i)
  const lakebaseLane = within(share).getByLabelText('Lakebase receipt result')
  expect(lakebaseLane).toHaveTextContent('14.15s')
  expect(lakebaseLane).toHaveTextContent(/10,000 CLIENTS REACHED · HOLD INTERRUPTED/i)
  expect(lakebaseLane).not.toHaveTextContent(/·\s*EXACT VERIFIED/)
  expect(lakebaseLane).not.toHaveTextContent(/0\.01/)
  const auroraLane = within(share).getByLabelText(/Aurora Serverless v2(?: \+ RDS Proxy)? receipt result/i)
  expect(auroraLane).toHaveTextContent('>23.89s')
  expect(poster).not.toHaveTextContent(/Earlier pooled path|Same setup time/i)
})

it('screenshot fixture: caption, replay, and explain all agree with the poster', () => {
  const session = screenshotTowelRoundFiveSession()

  const caption = linkedInReceipt(session, 5)
  expect(caption).toContain('14.15s')
  expect(caption).toContain('10,000 CLIENTS REACHED · HOLD INTERRUPTED')
  expect(caption).toContain('>23.89s')
  expect(caption).not.toContain('0.01')
  // No positive "EXACT VERIFIED" status; the honest "NO EXACT VERIFIED" headline is fine.
  expect(caption).not.toMatch(/·\s*EXACT VERIFIED/)
  expect(caption).not.toMatch(/not shareable until the round contract completes/i)
  expect(caption).toMatch(/NO EXACT VERIFIED RESULT · NO DECLARED WINNER · MARGIN N\/A/i)

  const replay = JSON.stringify(replayStory(session))
  expect(replay).toContain('14.15s')
  expect(replay).toMatch(/10,000 clients reached · hold interrupted by towel/i)
  expect(replay).not.toMatch(/All 20,000 held 30s/i)
  expect(replay).not.toMatch(/0\.01s/)

  const cue = buildRingsideCue(session, 'data_engineer', 'performance')
  expect(cue.show).toMatch(/reached 10,000 clients at 14\.15s/i)
  expect(cue.show).toMatch(/hold was interrupted by the towel/i)
  expect(cue.show).not.toMatch(/fan-in (?:never started|did not run)/i)
  expect(cue.show).not.toContain('0.01')
  expect(cue.show).not.toMatch(/·\s*EXACT VERIFIED/)
})

// --- Companion: a genuinely verified V4 bout still reads EXACT VERIFIED ---

function verifiedBellRoundFiveSession(): DemoSession {
  // Mirrors the proven verified-V4 recipe (see "uses serialized V3 model
  // evidence for the formal comparison"): fully-verified fan-in setup + a
  // verified bell runtime, so roundFiveHasComparison holds. Competitor renamed
  // to Aurora to prove the RDS Proxy label survives.
  const session = roundFiveSession()
  const competitor = FALLBACK_CATALOG.competitors.find((item) => item.id === 'aurora_serverless_v2')!
  session.competitor = competitor
  session.lanes.competitor.name = 'Aurora Serverless v2 + RDS Proxy'
  session.round5_setup!.protocol = 'round5-fanin-v4'
  session.round5_setup!.schema_version = 4
  session.round5_setup!.lanes.competitor!.name = 'Aurora Serverless v2 + RDS Proxy'
  session.fairness.protocol = 'round5-fanin-v4'
  for (const laneId of ['lakebase', 'competitor'] as const) {
    session.lanes[laneId].evidence = {
      ...session.lanes[laneId].evidence,
      protocol: 'round5-fanin-v4',
      schema_version: 4,
      safety_evidence_version: 5,
      hard_safety_verified: true,
      port_accounting_verified: true,
      telemetry_advisories: ['event_loop_pressure'],
    }
  }
  session.round5_runtime = {
    ...withV3Runtime(runningRoundFiveSession()).round5_runtime!,
    state: 'verified',
    lanes: {
      lakebase: {
        ...withV3Runtime(runningRoundFiveSession()).round5_runtime!.lanes.lakebase,
        phase: 'verified',
        clients_initiated: 10_000,
        clients_authenticated: 10_000,
        held_clients: 10_000,
        peak_clients_authenticated: 10_000,
        peak_held_clients: 10_000,
        sampled_queries_succeeded: 64,
        bell_to_10000_observed_ms: 3_112.673,
        elapsed_at_snapshot_ms: 3_112.673,
        status: 'Exact 10,000-client retained gate verified',
      },
      competitor: {
        ...withV3Runtime(runningRoundFiveSession()).round5_runtime!.lanes.competitor,
        phase: 'verified',
        clients_initiated: 10_000,
        clients_authenticated: 10_000,
        held_clients: 10_000,
        peak_clients_authenticated: 10_000,
        peak_held_clients: 10_000,
        sampled_queries_succeeded: 64,
        bell_to_10000_observed_ms: 3_212.673,
        elapsed_at_snapshot_ms: 3_212.673,
        status: 'Exact 10,000-client retained gate verified',
      },
    },
  }
  session.comparison = {
    kind: 'measured',
    winner_lane_id: 'lakebase',
    margin: {
      spec_id: 'bell_to_10000_observed_ms',
      lane_id: 'lakebase',
      value: 100,
      display_value: '100.00 ms',
    },
    detail: 'Lakebase reached 10,000 first.',
  }
  session.lanes.lakebase.elapsed_ms = 3_112.673
  session.lanes.competitor.elapsed_ms = 3_212.673
  session.towel = undefined
  session.remembered_result = null
  return session
}

it('companion verified V4 fixture still reads EXACT VERIFIED for both lanes', () => {
  const session = verifiedBellRoundFiveSession()
  expect(roundFiveHasComparison(session)).toBe(true)
  const classified = classifyOutcome(session)
  expect(classified.contractComplete).toBe(true)
  const lakebase = roundFiveLanePresentation(session, 'lakebase')!
  const competitor = roundFiveLanePresentation(session, 'competitor')!
  expect(lakebase.semantic).toBe('verified')
  expect(lakebase.verified).toBe(true)
  expect(lakebase.status).toBe('EXACT VERIFIED')
  expect(lakebase.value).toBe('3.11s')
  expect(competitor.status).toBe('EXACT VERIFIED')

  const receipt = receiptPresentation(session, 'round')
  expect(receipt.lakebaseStatus).toBe('EXACT VERIFIED')
  expect(receipt.competitorStatus).toBe('EXACT VERIFIED')
  expect(receipt.lakebaseValue).toBe('3.11s')
  expect(receipt.winner).toBe('lakebase')
})

it('standing invariant: receipt status is EXACT VERIFIED iff the V4 lane is verified', () => {
  for (const session of [screenshotTowelRoundFiveSession(), verifiedBellRoundFiveSession()]) {
    const receipt = receiptPresentation(session, 'round')
    for (const laneId of ['lakebase', 'competitor'] as const) {
      const presentation = roundFiveLanePresentation(session, laneId)!
      const status = laneId === 'lakebase' ? receipt.lakebaseStatus : receipt.competitorStatus
      expect(status === 'EXACT VERIFIED').toBe(presentation.verified)
    }
  }
})

it('fail-after-10k keeps the frozen time and marks it not verified across surfaces', () => {
  const session = screenshotTowelRoundFiveSession()
  session.state = 'failed'
  session.failure = 'Telemetry gate failed after the hold'
  session.towel = undefined
  session.round5_runtime!.state = 'failed'
  session.round5_runtime!.lanes.lakebase.phase = 'failed'
  session.round5_runtime!.lanes.competitor.phase = 'failed'
  const lakebase = roundFiveLanePresentation(session, 'lakebase')!
  expect(lakebase.semantic).toBe('reached_hold_failed')
  expect(lakebase.value).toBe('14.15s')
  expect(lakebase.verified).toBe(false)
  expect(lakebase.status).not.toMatch(/EXACT VERIFIED/i)
})

it('knockout: a verified V4 win under 2× leads with the exact margin, not a multiplier', () => {
  const receipt = receiptPresentation(verifiedBellRoundFiveSession(), 'round')
  expect(receipt.knockout).toEqual({
    hero: '0.10s',
    qualifier: receipt.verdict,
    setup: '10,000 CLIENTS · ONE BELL · 30S HOLD',
    chip: 'EARLIER',
    winnerColor: '#e8482e',
    winner: 'lakebase',
    capabilityGap: false,
  })
  expect(receipt.verdict.length).toBeLessThanOrEqual(80)
})

it('knockout: a 45× gap prints the floored multiplier from the bell clocks', () => {
  const session = verifiedBellRoundFiveSession()
  session.round5_runtime!.lanes.competitor.bell_to_10000_observed_ms = 141_000
  session.round5_runtime!.lanes.competitor.elapsed_at_snapshot_ms = 141_000
  session.lanes.competitor.elapsed_ms = 141_000
  session.comparison = {
    kind: 'measured',
    winner_lane_id: 'lakebase',
    margin: { spec_id: 'bell_to_10000_observed_ms', lane_id: 'lakebase', value: 137_887.327, display_value: '137887.33 ms' },
    detail: 'Lakebase reached 10,000 first.',
  }
  const receipt = receiptPresentation(session, 'round')
  expect(receipt.winner).toBe('lakebase')
  expect(receipt.knockout?.hero).toBe('45×')
})

it('knockout: the ratio is floored and only printed from 2× up', () => {
  expect(knockoutRatioLabel(13_690, 621_260)).toBe('45×')
  expect(knockoutRatioLabel(3_080, 9_870)).toBe('3.2×')
  expect(knockoutRatioLabel(3_112.673, 3_212.673)).toBeNull()
  expect(knockoutRatioLabel(0, 5_000)).toBeNull()
  expect(knockoutRatioLabel(5_000, 4_000)).toBeNull()
})

it('knockout: a towel keeps the scorecard card', () => {
  expect(receiptPresentation(screenshotTowelRoundFiveSession(), 'round').knockout).toBeUndefined()
})

function stoppedContractGateSession(overrides: Partial<DemoSession> = {}): DemoSession {
  // A genuinely FATAL setup fault: the competitor never stamped its CreateDBProxy
  // request boundary (missing provenance). Cleanup completed. The verdict must
  // name the real gate, not blame cleanup.
  return {
    state: 'failed',
    failure: 'Round 5 contract gate failed; no comparison was declared.',
    round5_setup: {
      cleanup_failure: null,
      cleanup_retryable: false,
      lanes: {
        lakebase: { id: 'lakebase', name: 'Lakebase', verified: true, setup_diagnostic: null },
        competitor: {
          id: 'competitor',
          name: 'Aurora Serverless v2 + RDS Proxy',
          verified: false,
          setup_diagnostic: 'create_db_proxy_missing',
        },
      },
    },
    lanes: {
      lakebase: { name: 'Lakebase' },
      competitor: { name: 'Aurora Serverless v2 + RDS Proxy' },
    },
    ...overrides,
  } as unknown as DemoSession
}

it('a stopped contract-gate bout with settled cleanup reports the diagnostic, not cleanup', () => {
  const session = stoppedContractGateSession()

  const verdict = roundFiveStoppedVerdict(session)
  expect(verdict).not.toMatch(/cleanup must settle/i)
  expect(verdict).toContain('Aurora Serverless v2 + RDS Proxy')
  expect(verdict).toContain('CreateDBProxy request boundary')
  expect(roundFiveSetupDiagnosticSummary(session)).toContain('CreateDBProxy request boundary')
})

it('a stopped bout whose cleanup is genuinely unsettled still says cleanup must settle', () => {
  const session = stoppedContractGateSession({
    round5_setup: {
      cleanup_failure: 'RDS Proxy delete not yet confirmed',
      cleanup_retryable: true,
      lanes: {
        lakebase: { id: 'lakebase', name: 'Lakebase', verified: true, setup_diagnostic: null },
        competitor: {
          id: 'competitor',
          name: 'Aurora Serverless v2 + RDS Proxy',
          verified: false,
          setup_diagnostic: 'create_db_proxy_missing',
        },
      },
    },
  } as unknown as Partial<DemoSession>)

  expect(roundFiveStoppedVerdict(session)).toMatch(/cleanup must settle/i)
})

it('a slow-but-present CreateDBProxy is a non-fatal advisory, not a stopped verdict', () => {
  // OVERRIDE: exact bout that DECLARES a winner but the competitor's CreateDBProxy
  // was requested >100 ms after the bell. The advisory is surfaced for the
  // play-by-play; it never produces a FAILED verdict or diagnostic.
  const session = {
    state: 'verified',
    round5_setup: {
      cleanup_failure: null,
      cleanup_retryable: false,
      lanes: {
        lakebase: { id: 'lakebase', name: 'Lakebase', verified: true, setup_diagnostic: null },
        competitor: {
          id: 'competitor',
          name: 'Aurora Serverless v2 + RDS Proxy',
          verified: true,
          setup_diagnostic: null,
          scheduling_advisory: 'create_db_proxy_window',
          create_db_proxy_request_delta_ms: 163.51,
        },
      },
    },
    lanes: {
      lakebase: { name: 'Lakebase' },
      competitor: { name: 'Aurora Serverless v2 + RDS Proxy' },
    },
  } as unknown as DemoSession

  // No fatal diagnostic, so the stopped-verdict summary is empty.
  expect(roundFiveSetupDiagnosticSummary(session)).toBeNull()
  // The advisory IS available for the detailed play-by-play.
  const advisory = roundFiveSchedulingAdvisorySummary(session)
  expect(advisory).toContain('Aurora Serverless v2 + RDS Proxy')
  expect(advisory).toContain('100 ms after the bell')
})

// ---------------------------------------------------------------------------
// Health-bars share card (Fable 5.1 / "1b", the default layout). The knockout
// tests above are unchanged; these cover the parallel presentation and the
// math the audit required to be honest before it can be the default.
// ---------------------------------------------------------------------------

it('health bars: a verified V4 win under 2× keeps the ledger headline as the verdict', () => {
  const session = verifiedBellRoundFiveSession()
  const receipt = receiptPresentation(session, 'round')
  const clocks = session.round5_runtime!.lanes
  expect(receipt.healthBars).toMatchObject({
    title: 'BELL TO 10,000 HELD CLIENTS',
    verdict: receipt.verdict,
    aside: null,
    winner: 'lakebase',
    capabilityGap: false,
  })
  expect(receipt.healthBars?.fill.competitor).toBe(1)
  expect(receipt.healthBars?.fill.lakebase).toBeCloseTo(
    clocks.lakebase.bell_to_10000_observed_ms! / clocks.competitor.bell_to_10000_observed_ms!,
    6,
  )
})

it('health bars: a 45× gap reads in human units, winner first, with the exact margin beside it', () => {
  const session = verifiedBellRoundFiveSession()
  session.round5_runtime!.lanes.competitor.bell_to_10000_observed_ms = 141_000
  session.round5_runtime!.lanes.competitor.elapsed_at_snapshot_ms = 141_000
  session.lanes.competitor.elapsed_ms = 141_000
  session.comparison = {
    kind: 'measured',
    winner_lane_id: 'lakebase',
    margin: { spec_id: 'bell_to_10000_observed_ms', lane_id: 'lakebase', value: 137_887.327, display_value: '137887.33 ms' },
    detail: 'Lakebase reached 10,000 first.',
  }
  const receipt = receiptPresentation(session, 'round')
  const lakebaseMs = session.round5_runtime!.lanes.lakebase.bell_to_10000_observed_ms!
  expect(receipt.winner).toBe('lakebase')
  expect(receipt.healthBars?.verdict).toBe(`${humanDuration(lakebaseMs, 'up').label} VS 2 MINUTES`)
  expect(receipt.healthBars?.aside).toBe(`LAKEBASE · ${((141_000 - lakebaseMs) / 1000).toFixed(2)}s SOONER`)
  expect(receipt.healthBars?.fill).toEqual({ lakebase: lakebaseMs / 141_000, competitor: 1 })
})

it('health bars: human units round the winner up and the loser down, and hold at unit boundaries', () => {
  expect(humanDuration(13_690, 'up').label).toBe('14 SECONDS')
  expect(humanDuration(621_260, 'down').label).toBe('10 MINUTES')
  expect(humanDuration(2_310, 'up').label).toBe('2.4 SECONDS')
  expect(humanDuration(41_860, 'down').label).toBe('41 SECONDS')
  expect(humanDuration(165_000, 'down').label).toBe('2M 45S')
  expect(humanDuration(65_000, 'up').label).toBe('1M 05S')
  expect(humanDuration(60_000, 'down').label).toBe('1 MINUTE')
  expect(humanDuration(700, 'up').label).toBe('0.7 SECONDS')
  // The boundary the audit flagged: sub-millisecond noise must not tip the
  // decisecond in the wrong direction. 2300.001ms up stays 2.3s (not 2.4s);
  // 2399.999ms down stays 2.4s (not 2.3s). Winner still reads faster.
  expect(humanDuration(2_300.001, 'up').label).toBe('2.3 SECONDS')
  expect(humanDuration(2_399.999, 'down').label).toBe('2.4 SECONDS')
  expect(humanDuration(2_300.001, 'up').seconds).toBeLessThan(humanDuration(2_399.999, 'down').seconds)
})

it('health bars: a towel keeps the scorecard card (no bars)', () => {
  expect(receiptPresentation(screenshotTowelRoundFiveSession(), 'round').healthBars).toBeUndefined()
})

it('health bars: equal clocks fall back to the scorecard (no zero-margin bar)', () => {
  const session = verifiedBellRoundFiveSession()
  session.round5_runtime!.lanes.competitor.bell_to_10000_observed_ms = 3_112.673
  session.round5_runtime!.lanes.competitor.elapsed_at_snapshot_ms = 3_112.673
  session.lanes.competitor.elapsed_ms = 3_112.673
  session.comparison!.margin!.value = 0
  // Equal clocks cannot honestly credit a winner: the presentation must never
  // draw a zero-margin bar, whether the classifier reports a tie or the
  // health-bars strict guard rejects it.
  expect(receiptPresentation(session, 'round').healthBars).toBeUndefined()
})

it('health bars: a non-finite, negative, or zero challenger clock falls back to the scorecard', () => {
  for (const bad of [Number.NaN, Number.POSITIVE_INFINITY, -1, 0]) {
    const session = verifiedBellRoundFiveSession()
    session.round5_runtime!.lanes.competitor.bell_to_10000_observed_ms = bad
    session.round5_runtime!.lanes.competitor.elapsed_at_snapshot_ms = Number.isFinite(bad) ? bad : 0
    expect(receiptPresentation(session, 'round').healthBars).toBeUndefined()
  }
})

it('health bars: a huge but finite challenger clock renders safely with the true (unfloored) ratio', () => {
  const session = verifiedBellRoundFiveSession()
  const lakebaseMs = session.round5_runtime!.lanes.lakebase.bell_to_10000_observed_ms!
  session.round5_runtime!.lanes.competitor.bell_to_10000_observed_ms = 1_000_000_000
  session.round5_runtime!.lanes.competitor.elapsed_at_snapshot_ms = 1_000_000_000
  session.lanes.competitor.elapsed_ms = 1_000_000_000
  session.comparison!.margin!.value = 1_000_000_000 - lakebaseMs
  const receipt = receiptPresentation(session, 'round')
  expect(receipt.healthBars).toBeDefined()
  expect(receipt.healthBars!.fill.lakebase).toBeCloseTo(lakebaseMs / 1_000_000_000, 12)
  expect(receipt.healthBars!.fill.competitor).toBe(1)
  expect(receipt.healthBars!.verdict.length).toBeGreaterThan(0)
})

it('health bars: a human margin that disagrees with the ledger drops the aside and keeps the ledger verdict', () => {
  const session = verifiedBellRoundFiveSession()
  session.round5_runtime!.lanes.competitor.bell_to_10000_observed_ms = 141_000
  session.round5_runtime!.lanes.competitor.elapsed_at_snapshot_ms = 141_000
  session.lanes.competitor.elapsed_ms = 141_000
  // The ledger margin (100 ms) contradicts the drawn clocks (≈137.9 s). The
  // bars keep their true shape, but the misleading human aside is withheld and
  // the verdict falls back to the authoritative ledger headline.
  session.comparison!.margin!.value = 100
  const receipt = receiptPresentation(session, 'round')
  expect(classifyOutcome(session).marginMs).toBe(100)
  expect(receipt.healthBars).toBeDefined()
  expect(receipt.healthBars!.aside).toBeNull()
  expect(receipt.healthBars!.verdict).toBe(receipt.verdict)
  expect(receipt.healthBars!.fill.lakebase).toBeCloseTo(
    session.round5_runtime!.lanes.lakebase.bell_to_10000_observed_ms! / 141_000,
    6,
  )
})

it('health bars: Round 5 measures the bell runtime, never the setup clock (no V4 poison)', () => {
  const session = verifiedBellRoundFiveSession()
  // Corrupt the legacy setup clocks; the health bars must ignore them entirely.
  if (session.round5_setup?.lanes?.lakebase) session.round5_setup.lanes.lakebase.setup_elapsed_ms = 0.01
  if (session.round5_setup?.lanes?.competitor) session.round5_setup.lanes.competitor.setup_elapsed_ms = 999_999
  const receipt = receiptPresentation(session, 'round')
  expect(receipt.healthBars?.fill.lakebase).toBeCloseTo(
    session.round5_runtime!.lanes.lakebase.bell_to_10000_observed_ms!
      / session.round5_runtime!.lanes.competitor.bell_to_10000_observed_ms!,
    6,
  )
})
