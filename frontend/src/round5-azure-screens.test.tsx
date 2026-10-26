import { cleanup, render } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { finaleCaption, InstantReplay, linkedInReceipt, RoundFiveProof } from './App'
import type {
  CompetitorDefinition,
  DemoSession,
  LaneSnapshot,
  RoundFiveRuntimeLane,
} from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { buildRingsideCue } from './ringside-cues'
import { PERSONA_IDS } from './ringside-cues/corpus'
import { PRIORITY_KEYS } from './ringside-cues/types'
import {
  ROUND_FIVE_AZURE_CLEANUP_COPY,
  roundFiveSchedulingAdvisorySummary,
  roundFiveSetupDiagnosticSummary,
} from './round5'

afterEach(cleanup)

/**
 * The Azure edition's Round 5, screen by screen. Its first live bouts (2026-10-20) showed the
 * fight card and cues saying "the selected AWS path adds RDS Proxy", and the share text, the
 * evidence and the live verdict naming AWS's Proxy. In Round 5 "Proxy" is always RDS Proxy.
 */
const AWS_WORDS = /\b(AWS|Amazon|Aurora|RDS|Glue|DMS|CloudWatch|IAM|Proxy|CreateDBProxy)\b/

const azure: CompetitorDefinition = {
  id: 'azure_postgres',
  name: 'Azure Database for PostgreSQL',
  short_name: 'Azure PostgreSQL',
  edition: 'AZURE DATABASE FOR POSTGRESQL EDITION',
}
const sre = FALLBACK_CATALOG.personas.find((persona) => persona.id === 'sre')!

function runtimeLane(id: 'lakebase' | 'competitor', bellToTenThousandMs: number, rampMs: number): RoundFiveRuntimeLane {
  return {
    id,
    phase: 'verified',
    elapsed_at_snapshot_ms: bellToTenThousandMs,
    bell_to_10000_observed_ms: bellToTenThousandMs,
    observation_uncertainty_ms: 90,
    pooled_path_ready_observed_ms: bellToTenThousandMs - rampMs,
    ramp_started_observed_ms: 2,
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

function fanInEvidence(id: 'lakebase' | 'competitor', timeToTargetMs: number): Record<string, unknown> {
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
    launch_skew_ms: 2.1,
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
    connect_latency_p99_ms: 201,
    auth_method: id === 'lakebase' ? 'tls-cleartext-password' : 'scram-sha-256',
    telemetry_verified: true,
    telemetry_failures: [],
  }
}

/** Bout 2 on Azure (2026-10-20, 8:51 PM), as the server reported it. */
function azureRoundFiveSession(): DemoSession {
  const name = 'Azure PostgreSQL + PgBouncer'
  const lane = (id: 'lakebase' | 'competitor', laneName: string, ms: number): LaneSnapshot => ({
    id, name: laneName, state: 'verified', elapsed_ms: ms, attempts: 10_000,
    status: 'Exact 10,000-client fan-in verified', error: null, evidence: fanInEvidence(id, ms),
  })
  const facts = [
    { key: 'pgbouncer_enabled', value: true },
    { key: 'max_client_conn', value: 10_100 },
    { key: 'pooled_port', value: 6432 },
    { key: 'pooled_transaction_committed', value: true },
  ]
  return {
    id: 'b0b0a2c5d9e84c7fa3f1d2e3c4b5a697',
    state: 'verified',
    created_at: '2026-10-21T01:51:30Z',
    updated_at: '2026-10-21T01:52:50Z',
    competitor: azure,
    primary_persona: sre,
    secondary_personas: [],
    corners: ['performance'],
    round: FALLBACK_CATALOG.rounds.find((round) => round.id === 'survive_connection_spike')!,
    recommendation_reason: '',
    presenter_pack: {
      opening: '', discovery_question: '', risk: '', stop_condition: '', remembered_metric: '',
      primary: { persona_id: sre.id, nickname: sre.nickname, role: sre.role, interpretation: '', objection: '', response: '' },
      secondary: [], closing: '',
    },
    lanes: {
      lakebase: lane('lakebase', 'Lakebase', 19_829.4),
      competitor: lane('competitor', name, 23_415.4),
    },
    fairness: {
      same_client: true, same_transaction: true, same_nonce: true, launch_skew_ms: 2.1,
      warmup_connections: 0, concurrency: 10_000, runner: 'Python 3.12 event-driven TLS/native-password',
      tls: 'verify-full', timeout: '20s connect · 600s run', protocol: 'round5-fanin-v4',
      target_clients_per_lane: 10_000, hold_seconds: 30, sampled_queries_per_lane: 64, max_retries: 100,
    },
    comparison: {
      kind: 'measured',
      winner_lane_id: 'lakebase',
      margin: { spec_id: 'bell_to_10000_observed_ms', lane_id: 'lakebase', value: 19_843.5, display_value: '19843.50 ms' },
      detail: 'Lakebase reached exactly 10,000 held clients sooner.',
    },
    round5_setup: {
      schema_version: 4,
      protocol: 'round5-fanin-v4',
      state: 'verified',
      workflow_launch_skew_ms: 0.016,
      lanes: {
        lakebase: {
          id: 'lakebase', name: 'Lakebase', state: 'verified', setup_elapsed_ms: 4.4,
          status: 'Setup stop gate verified', verified: true,
          stop_gate_evidence: {
            gate_id: 'lakebase_dispatch_eligibility', exact: true,
            expected: [{ key: 'warm_launch_capsule_current', value: true }],
            observed: [{ key: 'warm_launch_capsule_current', value: true }],
          },
        },
        competitor: {
          id: 'competitor', name, state: 'verified', setup_elapsed_ms: 16_232.4,
          status: 'Setup stop gate verified', verified: true,
          stop_gate_evidence: {
            gate_id: 'azure_pgbouncer_exact_data_plane', exact: true, expected: facts, observed: facts,
          },
        },
      },
      setup_validated: true,
      downstream_validated: true,
      cleanup_retryable: false,
    },
    round5_runtime: {
      protocol: 'round5-bell-to-10k-v4',
      warm_generation: 2,
      bell_id: 'bell-b0b0a2c5',
      revision: 40,
      state: 'verified',
      bell_at_utc: '2026-10-21T01:51:37.622843Z',
      lanes: {
        lakebase: runtimeLane('lakebase', 21_081.8, 19_829.4),
        competitor: runtimeLane('competitor', 40_925.3, 23_415.4),
      },
    },
    remembered_result: 'LAKEBASE REACHED 10,000 · 19.84s SOONER',
    failure: null,
  }
}

describe('the Azure edition’s Round 5 screens', () => {
  it('share a receipt that names no AWS product', () => {
    const post = linkedInReceipt(azureRoundFiveSession(), 5)
    expect(post).not.toMatch(AWS_WORDS)
    expect(post).toContain('a self-managed PgBouncer')
  })

  it('show the result, its evidence and its live verdict without an AWS product', () => {
    const { container } = render(
      <RoundFiveProof
        session={azureRoundFiveSession()}
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
    expect(container.textContent ?? '').not.toMatch(AWS_WORDS)
    expect(container.textContent ?? '').toContain('PgBouncer')
  })

  it('replay the bout without an AWS product', () => {
    const { container } = render(
      <InstantReplay session={azureRoundFiveSession()} roundNumber={5} onClose={vi.fn()} />,
    )
    expect(document.body.textContent ?? '').not.toMatch(AWS_WORDS)
    expect(container).toBeTruthy()
  })

  it('cue every persona and priority without an AWS product', () => {
    const session = azureRoundFiveSession()
    for (const personaId of PERSONA_IDS) {
      for (const priorityKey of PRIORITY_KEYS) {
        const cue = buildRingsideCue(session, personaId, priorityKey)
        expect(`${cue.say} ${cue.ask} ${cue.show}`, `${personaId}/${priorityKey}`).not.toMatch(AWS_WORDS)
      }
    }
  })

  it('name the timed switch in Azure’s words when it is late or missing', () => {
    const session = azureRoundFiveSession()
    const competitorSetup = session.round5_setup!.lanes!.competitor!
    competitorSetup.scheduling_advisory = 'create_db_proxy_window'
    expect(roundFiveSchedulingAdvisorySummary(session)).toContain('PgBouncer’s switch was requested more than 100 ms after the bell')
    competitorSetup.verified = false
    competitorSetup.setup_diagnostic = 'create_db_proxy_missing'
    expect(roundFiveSetupDiagnosticSummary(session)).toContain('no timed Azure mutation')
    expect(`${roundFiveSchedulingAdvisorySummary(session)} ${roundFiveSetupDiagnosticSummary(session)}`)
      .not.toMatch(AWS_WORDS)
    // The AWS edition's own words are unchanged.
    session.competitor = FALLBACK_CATALOG.competitors.find((item) => item.id === 'rds_postgres')!
    expect(roundFiveSchedulingAdvisorySummary(session)).toContain('CreateDBProxy was requested more than 100 ms')
  })

  it('end the six-round share with Round 5 raced, in Azure’s words', () => {
    // Until 2026-10-21 the Azure finale's caption said "Round 5 is not raced on Azure yet".
    const caption = finaleCaption(azureRoundFiveSession())
    expect(caption).toContain('Round 5 scores one server bell to exactly 10,000 held clients per lane.')
    expect(caption).toContain('Direct Azure connections, existing pools')
    expect(caption).not.toMatch(/not raced/i)
    expect(caption).not.toMatch(AWS_WORDS)
  })

  it('say what the cleanup is doing in Azure’s words', () => {
    expect(ROUND_FIVE_AZURE_CLEANUP_COPY).not.toMatch(AWS_WORDS)
    expect(ROUND_FIVE_AZURE_CLEANUP_COPY).toContain('PgBouncer')
  })
})
