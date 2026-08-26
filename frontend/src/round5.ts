import type { DemoSession, LaneId, LaneSnapshot, RoundFiveSetupState } from './api/types'

export const ROUND_FIVE_ID = 'survive_connection_spike' as const
export const ROUND_FIVE_DISPLAY_TITLE = 'Get spike-ready'
export const ROUND_FIVE_SCHEDULED_CLIENTS = 128
export const ROUND_FIVE_WARMUPS = 4
export const ROUND_FIVE_CONCURRENCY = 64
export const ROUND_FIVE_WITNESS_CLIENTS = 64
export const ROUND_FIVE_RUNNER = 'Python 3.12 + psycopg 3.3.4'
export const ROUND_FIVE_SETUP_MAX_LAUNCH_SKEW_MS = 10

export interface RoundFiveLaneResult {
  scheduled: number | null
  terminal: number | null
  successes: number | null
  errors: number | null
  rawSuccessfulLatencyMs: number[]
  p99Ms: number | null
  witnessVerifiedClients: number | null
  uniqueBackendPids: number | null
  peakBackendSessions: number | null
  contractVerified: boolean
}

export interface RoundFiveSetupLaneResult {
  state: RoundFiveSetupState | 'unavailable'
  setupElapsedMs: number | null
  stopGateExact: boolean
  verified: boolean
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null ? value as Record<string, unknown> : {}
}

function count(value: unknown): number | null {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : null
}

function nonNegativeNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

function rawLatencies(value: unknown): number[] | null {
  if (!Array.isArray(value)) return null
  if (!value.every((item) => typeof item === 'number' && Number.isFinite(item) && item >= 0)) {
    return null
  }
  return value as number[]
}

/** Nearest-rank percentile. Inputs stay unrounded until the UI formats the result. */
export function nearestRankP99(values: number[]): number | null {
  if (values.length === 0) return null
  const sorted = [...values].sort((left, right) => left - right)
  return sorted[Math.ceil(0.99 * sorted.length) - 1] ?? null
}

export function roundFiveLaneResult(lane: LaneSnapshot): RoundFiveLaneResult {
  const evidence = record(lane.evidence)
  const scheduled = count(evidence.scheduled_clients)
  const terminal = count(evidence.terminal_clients)
  const successes = count(evidence.successful_clients)
  const errors = count(evidence.error_clients)
  const latencies = rawLatencies(evidence.successful_latency_ms)
  const witnessVerifiedClients = count(evidence.witness_verified_clients)
  const uniqueBackendPids = count(evidence.unique_backend_pids)
  const peakBackendSessions = count(evidence.peak_backend_sessions)
  const contractVerified = lane.state === 'verified'
    && scheduled === ROUND_FIVE_SCHEDULED_CLIENTS
    && terminal === ROUND_FIVE_SCHEDULED_CLIENTS
    && successes !== null
    && errors !== null
    && successes + errors === terminal
    && latencies !== null
    && latencies.length === successes
    && witnessVerifiedClients === ROUND_FIVE_WITNESS_CLIENTS
    && uniqueBackendPids !== null
    && uniqueBackendPids >= 1
    && uniqueBackendPids < ROUND_FIVE_WITNESS_CLIENTS
    && peakBackendSessions !== null
    && peakBackendSessions >= 1
    && peakBackendSessions < ROUND_FIVE_WITNESS_CLIENTS

  return {
    scheduled,
    terminal,
    successes,
    errors,
    rawSuccessfulLatencyMs: latencies ?? [],
    p99Ms: nearestRankP99(latencies ?? []),
    witnessVerifiedClients,
    uniqueBackendPids,
    peakBackendSessions,
    contractVerified,
  }
}

export function roundFiveSetupLaneResult(
  session: DemoSession,
  laneId: LaneId,
): RoundFiveSetupLaneResult {
  const lane = session.round5_setup?.lanes?.[laneId]
  if (!lane) {
    return {
      state: 'unavailable',
      setupElapsedMs: null,
      stopGateExact: false,
      verified: false,
    }
  }
  const setupElapsedMs = nonNegativeNumber(lane.setup_elapsed_ms)
  const gate = lane.stop_gate_evidence
  const stopGateExact = gate?.exact === true
    && Array.isArray(gate.expected)
    && gate.expected.length > 0
    && Array.isArray(gate.observed)
    && gate.expected.length === gate.observed.length
    && new Set(gate.expected.map((fact) => fact.key)).size === gate.expected.length
    && new Set(gate.observed.map((fact) => fact.key)).size === gate.observed.length
    && gate.expected.every((expected) => gate.observed.some(
      (observed) => observed.key === expected.key && Object.is(observed.value, expected.value),
    ))
  const verified = lane.id === laneId
    && lane.verified === true
    && lane.state === 'verified'
    && setupElapsedMs !== null
    && stopGateExact

  return {
    state: lane.state,
    setupElapsedMs,
    stopGateExact,
    verified,
  }
}

export function isRoundFive(session: DemoSession | null | undefined): boolean {
  return session?.round.id === ROUND_FIVE_ID
}

export function roundFiveHasComparison(session: DemoSession): boolean {
  if (!isRoundFive(session) || session.state !== 'verified') return false
  const setup = session.round5_setup
  const comparison = session.comparison
  const bothSetupLanesVerified = roundFiveSetupLaneResult(session, 'lakebase').verified
    && roundFiveSetupLaneResult(session, 'competitor').verified
  const comparisonValid = comparison?.kind === 'tie'
    ? !comparison.winner_lane_id && !comparison.margin
    : comparison?.kind === 'measured'
      && (comparison.winner_lane_id === 'lakebase' || comparison.winner_lane_id === 'competitor')
      && comparison.margin?.spec_id === 'setup_elapsed_ms'
      && nonNegativeNumber(comparison.margin.value) !== null
      && Number(comparison.margin.value) > 0
  const setupLaunchSkew = nonNegativeNumber(setup?.workflow_launch_skew_ms)

  return Boolean(setup)
    && bothSetupLanesVerified
    && setupLaunchSkew !== null
    && setupLaunchSkew <= ROUND_FIVE_SETUP_MAX_LAUNCH_SKEW_MS
    && setup?.state === 'verified'
    && setup?.setup_validated === true
    && setup?.downstream_validated === true
    && comparisonValid
    && session.lanes.lakebase.state === 'verified'
    && session.lanes.competitor.state === 'verified'
    && roundFiveLaneResult(session.lanes.lakebase).contractVerified
    && roundFiveLaneResult(session.lanes.competitor).contractVerified
    && session.fairness.warmup_connections === ROUND_FIVE_WARMUPS
    && session.fairness.concurrency === ROUND_FIVE_CONCURRENCY
    && session.fairness.same_client === true
    && session.fairness.same_transaction === true
    && session.fairness.same_nonce === true
    && session.fairness.runner === ROUND_FIVE_RUNNER
    && typeof session.fairness.tls === 'string'
    && session.fairness.tls.trim().length > 0
    && typeof session.fairness.timeout === 'string'
    && session.fairness.timeout.trim().length > 0
    && typeof session.fairness.launch_skew_ms === 'number'
    && Number.isFinite(session.fairness.launch_skew_ms)
    && session.fairness.launch_skew_ms >= 0
    && session.fairness.launch_skew_ms <= 10
    && Boolean(comparison)
}

export function roundFiveP99Display(result: RoundFiveLaneResult): string {
  return result.p99Ms === null ? 'N/A' : `${result.p99Ms.toFixed(2)} ms`
}

export function roundFiveSetupElapsedDisplay(result: RoundFiveSetupLaneResult): string {
  return result.setupElapsedMs === null ? 'N/A' : `${(result.setupElapsedMs / 1000).toFixed(2)}s`
}

export function roundFiveSetupMarginDisplay(session: DemoSession): string {
  const margin = session.comparison?.kind === 'tie' ? 0 : session.comparison?.margin?.value
  return nonNegativeNumber(margin) === null ? 'N/A' : `${(Number(margin) / 1000).toFixed(2)}s`
}

export function roundFiveCountDisplay(value: number | null): string {
  return value === null ? 'N/A' : String(value)
}
