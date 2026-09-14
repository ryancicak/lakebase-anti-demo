import type { DemoSession, LaneId, LaneSnapshot, RoundFiveSetupState } from './api/types'
import { ROUND_FIVE_PERSONA_MEANING } from './ringside-cues/corpus'

export const ROUND_FIVE_ID = 'survive_connection_spike' as const
export const ROUND_FIVE_DISPLAY_TITLE = 'Ready a pooled application path'
export const ROUND_FIVE_DISPLAY_TITLE_UPPER = 'READY A POOLED APPLICATION PATH'
export const ROUND_FIVE_TARGET_CLIENTS = 128
export const ROUND_FIVE_SCHEDULED_CLIENTS = ROUND_FIVE_TARGET_CLIENTS
export const ROUND_FIVE_WARMUPS = 4
export const ROUND_FIVE_CONCURRENCY = 64
export const ROUND_FIVE_SAMPLED_QUERIES = 64
export const ROUND_FIVE_WITNESS_CLIENTS = ROUND_FIVE_SAMPLED_QUERIES
export const ROUND_FIVE_HOLD_SECONDS = 0
export const ROUND_FIVE_RUNNER = 'Python 3.12 async psycopg'
export const ROUND_FIVE_SETUP_MAX_LAUNCH_SKEW_MS = 10
export const ROUND_FIVE_PROTOCOL = 'connection-spike-v1'
export const ROUND_FIVE_SCHEMA_VERSION = 1
const ROUND_FIVE_FANIN_PROTOCOL = 'round5-fanin-v2'
const ROUND_FIVE_FANIN_SCHEMA_VERSION = 2
const ROUND_FIVE_FANIN_TARGET_CLIENTS = 10_000
const ROUND_FIVE_FANIN_RUNNER = 'Python 3.12 event-driven TLS/native-password'
const ROUND_FIVE_AUTH_METHODS = new Set(['tls-cleartext-password', 'scram-sha-256'])

export function roundFiveFightCardOpening(personaId: string): string {
  const focus = ROUND_FIVE_PERSONA_MEANING[personaId as keyof typeof ROUND_FIVE_PERSONA_MEANING]
    ?? 'Lakebase includes a built-in pool for up to 10,000 client connections. Teams still decide who owns the application path around it.'
  return focus
}

export interface RoundFiveLaneResult {
  bounded: boolean
  targetClients: number
  initiated: number | null
  authenticated: number | null
  held: number | null
  terminalFailures: number | null
  retries: number | null
  disconnectedDuringHold: number | null
  timeToTargetMs: number | null
  holdElapsedMs: number | null
  sampledQueriesAttempted: number | null
  sampledQueriesSucceeded: number | null
  sampledQueriesFailed: number | null
  connectP50Ms: number | null
  connectP95Ms: number | null
  connectP99Ms: number | null
  uniqueBackendPids: number | null
  currentBackendSessions: number | null
  peakBackendSessions: number | null
  preexistingClientRoleSessions: number | null
  distinctSocketFds: number | null
  distinctLocalEndpoints: number | null
  observerDirect: boolean
  observerRole: string
  clientRole: string
  authMethod: string
  telemetrySamples: number | null
  telemetryPhysicalMemoryBytes: number | null
  telemetryMinAvailableMemoryBytes: number | null
  telemetryPeakRssBytes: number | null
  telemetryFdSoftLimit: number | null
  telemetryPeakOpenFds: number | null
  telemetryEphemeralPortCount: number | null
  telemetryMinEphemeralPortReserve: number | null
  telemetryPeakEventLoopP99Ms: number | null
  telemetryPeakRawEventLoopP99Ms: number | null
  telemetryPeakExternalEventLoopP99Ms: number | null
  telemetryPeakCpuCapacityFraction: number | null
  telemetryFailures: string[]
  telemetryVerified: boolean
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

export function isRoundFiveFanInEvidence(value: unknown): boolean {
  const evidence = record(value)
  return (
    evidence.schema_version === ROUND_FIVE_SCHEMA_VERSION
      && evidence.protocol === ROUND_FIVE_PROTOCOL
  ) || (
    evidence.schema_version === ROUND_FIVE_FANIN_SCHEMA_VERSION
      && evidence.protocol === ROUND_FIVE_FANIN_PROTOCOL
  )
}

export function isRoundFiveSetupEvidence(value: unknown): boolean {
  const setup = record(value)
  return (
    setup.schema_version === ROUND_FIVE_SCHEMA_VERSION
      && setup.protocol === ROUND_FIVE_PROTOCOL
  ) || (
    setup.schema_version === ROUND_FIVE_FANIN_SCHEMA_VERSION
      && setup.protocol === ROUND_FIVE_FANIN_PROTOCOL
  )
}

function count(value: unknown): number | null {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : null
}

function nonNegativeNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

export function roundFiveLaneResult(lane: LaneSnapshot): RoundFiveLaneResult {
  const candidate = record(lane.evidence)
  const evidence = isRoundFiveFanInEvidence(candidate) ? candidate : {}
  const fanIn = evidence.protocol === ROUND_FIVE_FANIN_PROTOCOL
  const initiated = count(fanIn ? evidence.initiated_clients : evidence.scheduled_clients)
  const authenticated = count(fanIn ? evidence.authenticated_clients : evidence.successful_clients)
  const held = count(fanIn ? evidence.held_clients_at_gate ?? evidence.held_clients : evidence.successful_clients)
  const terminalFailures = count(fanIn ? evidence.terminal_failures : evidence.error_clients)
  const retries = fanIn ? count(evidence.retries) : 0
  const disconnectedDuringHold = fanIn ? count(evidence.disconnected_during_hold) : 0
  const timeToTargetMs = nonNegativeNumber(evidence.time_to_target_ms)
  const holdElapsedMs = nonNegativeNumber(evidence.hold_elapsed_ms)
  const witnessVerifiedClients = count(evidence.witness_verified_clients)
  const sampledQueriesAttempted = fanIn ? count(evidence.sampled_queries_attempted) : witnessVerifiedClients
  const sampledQueriesSucceeded = fanIn ? count(evidence.sampled_queries_succeeded) : witnessVerifiedClients
  const sampledQueriesFailed = fanIn
    ? count(evidence.sampled_queries_failed)
    : witnessVerifiedClients === null ? null : 0
  const connectP50Ms = nonNegativeNumber(evidence.connect_latency_p50_ms)
  const connectP95Ms = nonNegativeNumber(evidence.connect_latency_p95_ms)
  const connectP99Ms = nonNegativeNumber(fanIn ? evidence.connect_latency_p99_ms : evidence.application_p99_ms)
  const uniqueBackendPids = count(evidence.unique_backend_pids)
  const currentBackendSessions = fanIn ? count(evidence.current_backend_sessions) : uniqueBackendPids
  const peakBackendSessions = count(evidence.peak_backend_sessions)
  const preexistingClientRoleSessions = fanIn ? count(evidence.preexisting_client_role_sessions) : 0
  const distinctSocketFds = count(evidence.distinct_socket_fds)
  const distinctLocalEndpoints = count(evidence.distinct_local_endpoints)
  const observerDirect = evidence.observer_direct === true
  const observerRole = typeof evidence.observer_role === 'string' ? evidence.observer_role : ''
  const clientRole = typeof evidence.client_role === 'string' ? evidence.client_role : ''
  const authMethod = typeof evidence.auth_method === 'string' ? evidence.auth_method : ''
  const telemetrySamples = count(evidence.telemetry_samples)
  const telemetryPhysicalMemoryBytes = count(evidence.telemetry_physical_memory_bytes)
  const telemetryMinAvailableMemoryBytes = count(evidence.telemetry_min_available_memory_bytes)
  const telemetryPeakRssBytes = count(evidence.telemetry_peak_rss_bytes)
  const telemetryFdSoftLimit = count(evidence.telemetry_fd_soft_limit)
  const telemetryPeakOpenFds = count(evidence.telemetry_peak_open_fds)
  const telemetryEphemeralPortCount = count(evidence.telemetry_ephemeral_port_count)
  const telemetryMinEphemeralPortReserve = count(evidence.telemetry_min_ephemeral_port_reserve)
  const telemetryPeakEventLoopP99Ms = nonNegativeNumber(evidence.telemetry_peak_event_loop_p99_ms)
  const telemetryPeakRawEventLoopP99Ms = nonNegativeNumber(
    evidence.telemetry_peak_raw_event_loop_p99_ms,
  )
  const telemetryPeakExternalEventLoopP99Ms = nonNegativeNumber(
    evidence.telemetry_peak_external_event_loop_p99_ms,
  )
  const telemetryPeakCpuCapacityFraction = nonNegativeNumber(evidence.telemetry_peak_cpu_capacity_fraction)
  const telemetryFailures = fanIn && Array.isArray(evidence.telemetry_failures)
    ? evidence.telemetry_failures.filter((value): value is string => typeof value === 'string')
    : []
  const telemetryVerified = fanIn && evidence.telemetry_verified === true
  const targetClients = fanIn ? ROUND_FIVE_FANIN_TARGET_CLIENTS : ROUND_FIVE_TARGET_CLIENTS
  const contractVerified = lane.state === 'verified'
    && isRoundFiveFanInEvidence(evidence)
    && initiated === targetClients
    && (fanIn || count(evidence.terminal_clients) === targetClients)
    && authenticated !== null
    && terminalFailures !== null
    && authenticated + terminalFailures === targetClients
    && (!fanIn || held === targetClients)
    && sampledQueriesAttempted === ROUND_FIVE_SAMPLED_QUERIES
    && sampledQueriesSucceeded === ROUND_FIVE_SAMPLED_QUERIES
    && sampledQueriesFailed === 0
    && uniqueBackendPids !== null
    && uniqueBackendPids >= 1
    && uniqueBackendPids < (fanIn ? targetClients : ROUND_FIVE_WITNESS_CLIENTS)
    && peakBackendSessions !== null
    && peakBackendSessions >= 1
    && peakBackendSessions < (fanIn ? targetClients : ROUND_FIVE_WITNESS_CLIENTS)
    && (!fanIn || telemetryVerified)
    && (!fanIn || telemetryFailures.length === 0)
    && (!fanIn || terminalFailures === 0)
    && (!fanIn || retries === 0)
    && (!fanIn || disconnectedDuringHold === 0)
    && (!fanIn || preexistingClientRoleSessions === 0)
    && (!fanIn || distinctSocketFds === targetClients)
    && (!fanIn || distinctLocalEndpoints === targetClients)
    && (!fanIn || observerDirect)
    && (!fanIn || (observerRole.length > 0 && observerRole !== clientRole))
    && (!fanIn || ROUND_FIVE_AUTH_METHODS.has(authMethod))
    && (!fanIn || timeToTargetMs !== null)
    && (!fanIn || (holdElapsedMs !== null && holdElapsedMs >= 30_000))

  return {
    bounded: !fanIn,
    targetClients,
    initiated,
    authenticated,
    held,
    terminalFailures,
    retries,
    disconnectedDuringHold,
    timeToTargetMs,
    holdElapsedMs,
    sampledQueriesAttempted,
    sampledQueriesSucceeded,
    sampledQueriesFailed,
    connectP50Ms,
    connectP95Ms,
    connectP99Ms,
    uniqueBackendPids,
    currentBackendSessions,
    peakBackendSessions,
    preexistingClientRoleSessions,
    distinctSocketFds,
    distinctLocalEndpoints,
    observerDirect,
    observerRole,
    clientRole,
    authMethod,
    telemetrySamples,
    telemetryPhysicalMemoryBytes,
    telemetryMinAvailableMemoryBytes,
    telemetryPeakRssBytes,
    telemetryFdSoftLimit,
    telemetryPeakOpenFds,
    telemetryEphemeralPortCount,
    telemetryMinEphemeralPortReserve,
    telemetryPeakEventLoopP99Ms,
    telemetryPeakRawEventLoopP99Ms,
    telemetryPeakExternalEventLoopP99Ms,
    telemetryPeakCpuCapacityFraction,
    telemetryFailures,
    telemetryVerified,
    contractVerified,
  }
}

export function roundFiveSetupLaneResult(
  session: DemoSession,
  laneId: LaneId,
): RoundFiveSetupLaneResult {
  const setup = session.round5_setup
  const lane = isRoundFiveSetupEvidence(setup) ? setup?.lanes?.[laneId] : undefined
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
  if (!isRoundFive(session)) return false
  const setup = session.round5_setup
  const comparison = session.comparison
  const cleanupFailed = Boolean(
    setup?.cleanup_failure
    || session.towel?.cleanup_failure
    || session.cooldown?.state === 'failed'
    || session.cooldown?.failure,
  )
  if (session.state !== 'verified' && !cleanupFailed) return false
  const bothSetupLanesVerified = roundFiveSetupLaneResult(session, 'lakebase').verified
    && roundFiveSetupLaneResult(session, 'competitor').verified
  const fanIn = setup?.protocol === ROUND_FIVE_FANIN_PROTOCOL
  const comparisonValid = comparison?.kind === 'tie'
    ? !comparison.winner_lane_id && !comparison.margin
    : comparison?.kind === 'measured'
      && (comparison.winner_lane_id === 'lakebase' || comparison.winner_lane_id === 'competitor')
      && comparison.margin?.spec_id === (fanIn ? 'time_to_10000_ms' : 'setup_elapsed_ms')
      && nonNegativeNumber(comparison.margin.value) !== null
      && Number(comparison.margin.value) > 0
  const setupLaunchSkew = nonNegativeNumber(setup?.workflow_launch_skew_ms)

  return Boolean(setup)
    && isRoundFiveSetupEvidence(setup)
    && bothSetupLanesVerified
    && setupLaunchSkew !== null
    && setupLaunchSkew <= ROUND_FIVE_SETUP_MAX_LAUNCH_SKEW_MS
    && (setup?.state === 'verified' || cleanupFailed)
    && setup?.setup_validated === true
    && setup?.downstream_validated === true
    && comparisonValid
    && session.lanes.lakebase.state === 'verified'
    && session.lanes.competitor.state === 'verified'
    && roundFiveLaneResult(session.lanes.lakebase).contractVerified
    && roundFiveLaneResult(session.lanes.competitor).contractVerified
    && session.fairness.warmup_connections === (fanIn ? 0 : ROUND_FIVE_WARMUPS)
    && session.fairness.concurrency === (fanIn ? ROUND_FIVE_FANIN_TARGET_CLIENTS : ROUND_FIVE_CONCURRENCY)
    && session.fairness.protocol === (fanIn ? ROUND_FIVE_FANIN_PROTOCOL : ROUND_FIVE_PROTOCOL)
    && session.fairness.target_clients_per_lane === (fanIn ? ROUND_FIVE_FANIN_TARGET_CLIENTS : ROUND_FIVE_TARGET_CLIENTS)
    && session.fairness.sampled_queries_per_lane === ROUND_FIVE_SAMPLED_QUERIES
    && session.fairness.same_client === true
    && session.fairness.same_transaction === true
    && session.fairness.same_nonce === true
    && session.fairness.runner === (fanIn ? ROUND_FIVE_FANIN_RUNNER : ROUND_FIVE_RUNNER)
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
  return result.connectP99Ms === null ? 'N/A' : `${result.connectP99Ms.toFixed(2)} ms`
}

export function roundFiveSetupElapsedDisplay(result: RoundFiveSetupLaneResult): string {
  return result.setupElapsedMs === null ? 'N/A' : `${(result.setupElapsedMs / 1000).toFixed(2)}s`
}

export function roundFiveFanInMarginDisplay(session: DemoSession): string {
  const margin = session.comparison?.kind === 'tie' ? 0 : session.comparison?.margin?.value
  return nonNegativeNumber(margin) === null ? 'N/A' : `${(Number(margin) / 1000).toFixed(2)}s`
}

export function roundFiveCountDisplay(value: number | null): string {
  return value === null ? 'N/A' : String(value)
}
