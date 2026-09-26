import type {
  DemoSession,
  LaneId,
  LaneSnapshot,
  RoundFiveRuntimeLane,
  RoundFiveRuntimeSnapshot,
  RoundFiveSetupState,
} from './api/types'
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
// Recognition only. Round 5 runs the fan-in protocol; this name survives so a
// scorecard stored under the retired bounded protocol is labelled as an earlier
// protocol rather than silently relabelled with 10,000-client copy it never attempted.
export const ROUND_FIVE_PROTOCOL = 'connection-spike-v1'
export const ROUND_FIVE_SCHEMA_VERSION = 1
export const ROUND_FIVE_FANIN_PROTOCOL = 'round5-fanin-v4'
const ROUND_FIVE_V3_FANIN_PROTOCOL = 'round5-fanin-v3'
const ROUND_FIVE_V2_FANIN_PROTOCOL = 'round5-fanin-v2'
export const ROUND_FIVE_BELL_PROTOCOL = 'round5-bell-to-10k-v4'
const ROUND_FIVE_FANIN_SCHEMA_VERSION = 4
const ROUND_FIVE_SAFETY_EVIDENCE_VERSION = 5
const ROUND_FIVE_FANIN_TARGET_CLIENTS = 10_000
/**
 * The settled warm-pool baseline a lane may start with, mirroring
 * `server/connection_fanin.MAX_PREEXISTING_CLIENT_SESSIONS` (and the runner's
 * copy), which is checked by `tests/test_round5_frontend_contract_mirror.py`.
 * Lakebase's pooler keeps its backends open between back-to-back bouts, so a
 * nonzero baseline is the normal warm case, not contamination. Requiring zero
 * here while the server accepts the baseline turned verified Lakebase wins into
 * "NO DECLARED WINNER" with sharing blocked whenever the pool was still warm.
 */
export const ROUND_FIVE_MAX_PREEXISTING_CLIENT_SESSIONS = 60
/**
 * Retried connects one lane may spend, mirroring `server/connection_fanin.MAX_RETRIES`
 * and the runner's copy (checked by `tests/test_round5_frontend_contract_mirror.py`).
 * Zero until 2026-09-26: the Lakebase pooler's documented ceiling is exactly the
 * 10,000 clients a lane opens, and one refused login failed a whole bout. A retry
 * is timed and shown on the lane ("Failures / retries"); it never excuses a
 * terminal failure, a missing client or a hold disconnect.
 */
export const ROUND_FIVE_MAX_RETRIES = 100
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
  telemetryAdvisories: string[]
  safetyEvidenceVersion: number | null
  hardSafetyVerified: boolean
  portAccountingVerified: boolean
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
    (evidence.schema_version === ROUND_FIVE_FANIN_SCHEMA_VERSION
      && evidence.protocol === ROUND_FIVE_FANIN_PROTOCOL)
      || (evidence.schema_version === 3
        && evidence.protocol === ROUND_FIVE_V3_FANIN_PROTOCOL)
      || (evidence.schema_version === 2
        && evidence.protocol === ROUND_FIVE_V2_FANIN_PROTOCOL)
  )
}

export function isRoundFiveSetupEvidence(value: unknown): boolean {
  const setup = record(value)
  return (
    setup.schema_version === ROUND_FIVE_SCHEMA_VERSION
      && setup.protocol === ROUND_FIVE_PROTOCOL
  ) || (
    (setup.schema_version === ROUND_FIVE_FANIN_SCHEMA_VERSION
      && setup.protocol === ROUND_FIVE_FANIN_PROTOCOL)
      || (setup.schema_version === 3
        && setup.protocol === ROUND_FIVE_V3_FANIN_PROTOCOL)
      || (setup.schema_version === 2
        && setup.protocol === ROUND_FIVE_V2_FANIN_PROTOCOL)
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
    || evidence.protocol === ROUND_FIVE_V3_FANIN_PROTOCOL
    || evidence.protocol === ROUND_FIVE_V2_FANIN_PROTOCOL
  const fanInCurrent = evidence.protocol === ROUND_FIVE_FANIN_PROTOCOL
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
  const telemetryAdvisories = fanIn && Array.isArray(evidence.telemetry_advisories)
    ? evidence.telemetry_advisories.filter((value): value is string => typeof value === 'string')
    : []
  const safetyEvidenceVersion = fanIn ? count(evidence.safety_evidence_version) : null
  const hardSafetyVerified = fanIn && evidence.hard_safety_verified === true
  const portAccountingVerified = fanIn && evidence.port_accounting_verified === true
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
    && (!fanInCurrent || safetyEvidenceVersion === ROUND_FIVE_SAFETY_EVIDENCE_VERSION)
    && (!fanInCurrent || hardSafetyVerified)
    && (!fanInCurrent || portAccountingVerified)
    && (!fanIn || terminalFailures === 0)
    && (!fanIn || (retries !== null && retries <= ROUND_FIVE_MAX_RETRIES))
    && (!fanIn || disconnectedDuringHold === 0)
    && (!fanIn || (
      preexistingClientRoleSessions !== null
      && preexistingClientRoleSessions <= ROUND_FIVE_MAX_PREEXISTING_CLIENT_SESSIONS
    ))
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
    telemetryAdvisories,
    safetyEvidenceVersion,
    hardSafetyVerified,
    portAccountingVerified,
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

export function roundFiveUsesFanIn(session: DemoSession): boolean {
  if (!isRoundFive(session)) return false
  return session.round5_runtime?.protocol === ROUND_FIVE_BELL_PROTOCOL
    || session.round5_setup?.protocol === ROUND_FIVE_FANIN_PROTOCOL
    || session.round5_setup?.protocol === ROUND_FIVE_V3_FANIN_PROTOCOL
    || session.round5_setup?.protocol === ROUND_FIVE_V2_FANIN_PROTOCOL
}

export function roundFiveIsExplicitLegacy(session: DemoSession): boolean {
  return isRoundFive(session)
    && session.round5_setup?.protocol === ROUND_FIVE_PROTOCOL
    && session.round5_setup?.schema_version === ROUND_FIVE_SCHEMA_VERSION
}

/**
 * Human-readable text for each fixed, secret-free setup finalizer subcode the
 * server may attach to a non-verified setup lane (`setup_diagnostic`). Lets a
 * stopped bout name its actual reason instead of always blaming cleanup.
 */
const ROUND_FIVE_SETUP_DIAGNOSTIC_TEXT: Record<string, string> = {
  // Retired label kept legible for a legacy sealed receipt.
  workflow_launch_window: 'setup dispatch window exceeded',
  // FATAL evidence/provenance/clock-domain faults (these void the bout).
  workflow_launch_ordering: 'setup workflow launched before the shared start',
  create_db_proxy_pre_bell: 'CreateDBProxy was requested before the bell (pre-bell or wrong clock domain)',
  create_db_proxy_missing: 'CreateDBProxy request boundary was never observed (no timed AWS mutation)',
  stop_gate_evidence: 'setup stop gate did not verify exactly',
  stop_gate_before_workflow_launch: 'setup stop gate preceded the workflow launch',
  setup_deadline: 'setup exceeded its 30-minute deadline',
  setup_error: 'setup reported an error',
  setup_failed: 'setup failed',
  setup_towelled: 'setup was toweled',
  setup_unverified: 'setup did not verify',
  public_fact_key_rejected: 'setup evidence was redacted before it could be scored',
}

/**
 * Human-readable text for each NON-FATAL scheduling-conformance advisory the
 * server may attach to a (possibly verified) setup lane (`scheduling_advisory`).
 * These describe operational scheduling quality and are surfaced in the detailed
 * play-by-play only; they never void an exact, honestly-obtained bout.
 */
const ROUND_FIVE_SETUP_ADVISORY_TEXT: Record<string, string> = {
  create_db_proxy_window: 'CreateDBProxy was requested more than 100 ms after the bell (slow, already charged to this lane\u2019s clock)',
  workflow_launch_skew: 'lane setup launches were more than 10 ms apart (host-scheduling jitter, charged to each lane\u2019s clock)',
}

/**
 * Reduce every lane's non-fatal scheduling advisories to a readable, advisory-only
 * summary for the detailed play-by-play. Returns null when no lane carries one.
 * A verified lane can still carry an advisory, so this does NOT skip verified lanes.
 */
export function roundFiveSchedulingAdvisorySummary(session: DemoSession): string | null {
  const setup = session.round5_setup
  if (!setup) return null
  const parts: string[] = []
  for (const laneId of ['lakebase', 'competitor'] as LaneId[]) {
    const lane = setup.lanes?.[laneId]
    const advisory = lane?.scheduling_advisory
    if (!lane || !advisory) continue
    const name = lane.name || session.lanes[laneId]?.name || laneId
    const readable = advisory
      .split(';')
      .map((code) => ROUND_FIVE_SETUP_ADVISORY_TEXT[code] ?? code)
      .join(' · ')
    parts.push(`${name}: ${readable}`)
  }
  return parts.length > 0 ? parts.join(' · ') : null
}

/**
 * Whether backstage cleanup for this Round 5 bout has NOT settled: it either
 * failed, is still being retried, or a cooldown watcher failed. Only in that
 * case is "cleanup must settle before the receipt is final" the honest verdict.
 */
export function roundFiveCleanupUnsettled(session: DemoSession): boolean {
  const setup = session.round5_setup
  return Boolean(
    setup?.cleanup_failure
    || setup?.cleanup_retryable === true
    || session.towel?.cleanup_failure
    || session.cooldown?.state === 'failed'
    || session.cooldown?.failure,
  )
}

/**
 * Reduce the non-verified setup lanes to a readable reason, e.g.
 * "Aurora Serverless v2 + RDS Proxy: lane setup launches exceeded the 10 ms
 * inter-lane skew budget". Returns null when no lane carries a diagnostic.
 */
export function roundFiveSetupDiagnosticSummary(session: DemoSession): string | null {
  const setup = session.round5_setup
  if (!setup) return null
  const parts: string[] = []
  for (const laneId of ['lakebase', 'competitor'] as LaneId[]) {
    const lane = setup.lanes?.[laneId]
    if (!lane || lane.verified) continue
    const diagnostic = lane.setup_diagnostic
    if (!diagnostic) continue
    const name = lane.name || session.lanes[laneId]?.name || laneId
    const readable = diagnostic
      .split(';')
      .map((code) => ROUND_FIVE_SETUP_DIAGNOSTIC_TEXT[code] ?? code)
      .join(' · ')
    parts.push(`${name}: ${readable}`)
  }
  return parts.length > 0 ? parts.join(' · ') : null
}

/**
 * The verdict line for a stopped (failed/towelled) V4 runtime. When cleanup is
 * genuinely still unsettled it says so; otherwise it names the actual setup
 * diagnostic (or the session failure) rather than falsely claiming cleanup is
 * still underway -- the live 2026-09-17 bug, where a contract-gate failure with
 * completed cleanup always read "cleanup must settle before the receipt is final".
 */
export function roundFiveStoppedVerdict(session: DemoSession): string {
  if (roundFiveCleanupUnsettled(session)) {
    return 'V4 runtime stopped · cleanup must settle before the receipt is final'
  }
  const diagnostic = roundFiveSetupDiagnosticSummary(session)
  if (diagnostic) {
    return `V4 runtime stopped · ${diagnostic} · no comparison declared`
  }
  return session.failure
    ? `V4 runtime stopped · ${session.failure}`
    : 'V4 runtime stopped · no comparison declared'
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
    || setup?.protocol === ROUND_FIVE_V3_FANIN_PROTOCOL
    || setup?.protocol === ROUND_FIVE_V2_FANIN_PROTOCOL
  const fanInProtocol = setup?.protocol
  const bellV3 = session.round5_runtime?.protocol === ROUND_FIVE_BELL_PROTOCOL
  const expectedMarginSpec = bellV3
    ? 'bell_to_10000_observed_ms'
    : fanIn
      ? 'time_to_10000_ms'
      : 'setup_elapsed_ms'
  const comparisonValid = comparison?.kind === 'tie'
    ? !comparison.winner_lane_id && !comparison.margin
    : comparison?.kind === 'measured'
      && (comparison.winner_lane_id === 'lakebase' || comparison.winner_lane_id === 'competitor')
      && comparison.margin?.spec_id === expectedMarginSpec
      && nonNegativeNumber(comparison.margin.value) !== null
      && Number(comparison.margin.value) > 0
  const setupLaunchSkew = nonNegativeNumber(setup?.workflow_launch_skew_ms)

  return Boolean(setup)
    && isRoundFiveSetupEvidence(setup)
    && bothSetupLanesVerified
    && setupLaunchSkew !== null
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
    && session.fairness.protocol === (fanIn ? fanInProtocol : ROUND_FIVE_PROTOCOL)
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

/**
 * How long every lane must keep all 10,000 authenticated clients held at once
 * before the lane is scored -- the same 30-second floor the contract gate
 * enforces (`holdElapsedMs >= 30_000`). Named here so the live UI can say
 * "30 seconds" without re-deriving it. Deliberately used only as a fixed word
 * in copy, never as the denominator of a progress bar: the bell runtime does
 * not expose live hold-elapsed, and it cannot be inferred from
 * `elapsed_at_snapshot_ms` because the server forces that value equal to
 * `bell_to_10000_observed_ms` once the gate is observed (so the difference is
 * always zero). A numeric "x / 30s" bar would therefore be invented, not
 * measured.
 */
export const ROUND_FIVE_HOLD_TARGET_MS = 30_000

/**
 * The live state of a single Round 5 lane, read off the bell runtime, for the
 * two on-screen regions a viewer must not confuse:
 *
 *   - "Time to 10,000": the timed race to the observed exact 10,000-client held
 *     gate. `timeToTenKMs` is that scored number; once `reachedTenK` is true it
 *     is frozen (the server stamps it at the exact-10k moment, not at verified)
 *     and never moves again for this bell.
 *   - "Hold checks": the unscored verification that follows -- all 10,000 client
 *     connections stay held for 30 seconds while 64 already-held connections are re-checked.
 *     `heldConnectionChecksDone` / `heldConnectionChecksTotal` is that progress.
 *
 * There is intentionally no live hold-elapsed field: see ROUND_FIVE_HOLD_TARGET_MS.
 */
/**
 * `towelled` is a fourth terminal distinct from `failed`: the operator stopped
 * the bout (server sets the runtime lane `phase = "cancelled"`), nothing in the
 * lane failed. A lane can be towelled after it already locked its exact
 * time-to-10,000 clients (hold interrupted mid-way) or before it ever reached
 * 10,000 clients; `reachedTenK` tells those two apart. Keeping it separate from `failed` is what
 * lets the UI show "Hold not completed · towel thrown" instead of the false
 * "Failed during the 30-second hold", and preserve the locked clock rather than
 * blanking it to "Not timed".
 */
export type RoundFiveVerificationPhase = 'ramping' | 'verifying' | 'verified' | 'failed' | 'towelled'

export interface RoundFiveLaneVerification {
  phase: RoundFiveVerificationPhase
  reachedTenK: boolean
  timeToTenKMs: number | null
  heldConnectionChecksDone: number
  heldConnectionChecksTotal: number
}

export function roundFiveLaneVerification(lane: RoundFiveRuntimeLane): RoundFiveLaneVerification {
  const timeToTenKMs = nonNegativeNumber(lane.bell_to_10000_observed_ms)
  const reachedTenK = timeToTenKMs !== null || lane.phase === 'verified'
  const checksRaw = count(lane.sampled_queries_succeeded) ?? 0
  const heldConnectionChecksDone = Math.max(0, Math.min(ROUND_FIVE_SAMPLED_QUERIES, checksRaw))
  const phase: RoundFiveVerificationPhase = lane.phase === 'verified'
    ? 'verified'
    // A towel stops the bout without failing the lane: the server marks the
    // runtime lane `cancelled`. Keep it distinct from a genuine `failed` so the
    // hold region can say the operator stopped it, not that it failed.
    : lane.phase === 'cancelled'
      ? 'towelled'
      : lane.phase === 'failed'
        ? 'failed'
        : reachedTenK
          ? 'verifying'
          : 'ramping'
  return {
    phase,
    reachedTenK,
    timeToTenKMs,
    heldConnectionChecksDone,
    heldConnectionChecksTotal: ROUND_FIVE_SAMPLED_QUERIES,
  }
}

/**
 * The single V4 bell runtime for a session, or null when this session is not
 * running the current `round5-bell-to-10k-v4` protocol. Every user-visible
 * Round 5 surface that shows a primary clock (arena, share receipt, PNG card,
 * caption, instant replay, explain-to-room) must read its lane truth from here
 * -- never from the legacy `session.lanes[*].elapsed_ms` / `state`, which a
 * towel promotes from the ~0.01s pooled-path *setup* stop, nor from the
 * transitional Round 3 `towel.lakebase_verified_ms` field.
 */
export function roundFiveBellRuntime(
  session: DemoSession,
): RoundFiveRuntimeSnapshot | null {
  return session.round5_runtime?.protocol === ROUND_FIVE_BELL_PROTOCOL
    ? session.round5_runtime
    : null
}

function roundFiveCensoredLowerBoundMs(
  session: DemoSession,
  laneId: LaneId,
): number | null {
  return nonNegativeNumber(session.towel?.censored_lower_bounds_ms?.[laneId])
}

function roundFiveSecondsLabel(milliseconds: number): string {
  return `${(Math.max(0, milliseconds) / 1000).toFixed(2)}s`
}

/**
 * How one lane's terminal (or in-flight) V4 result must read on every surface.
 *
 * `EXACT VERIFIED` is reserved for a lane whose runtime phase is `verified`
 * (10,000 authenticated held clients, the full 30-second hold, and all 64
 * held-connection checks). A lane that only *reached* 10,000 clients -- towelled
 * or failed mid-hold -- keeps its exact, sticky `bell_to_10000_observed_ms` as
 * timed evidence but is never labelled verified. A lane that never reached
 * 10,000 clients shows its censored lower bound (or "not timed"), not a setup clock.
 */
export type RoundFiveLaneSemantic =
  | 'verified'
  | 'reached_hold_in_progress'
  | 'reached_hold_interrupted'
  | 'reached_hold_failed'
  | 'not_reached_lower_bound'
  | 'not_reached'
  | 'ramping'
  | 'not_supported'

export interface RoundFiveLanePresentation {
  laneId: LaneId
  semantic: RoundFiveLaneSemantic
  reachedTenK: boolean
  verified: boolean
  /** Exact observed time to the 10,000-client held gate (present iff reached). */
  timeMs: number | null
  /** Censored lower bound when the lane never reached 10,000 clients, else null. */
  lowerBoundMs: number | null
  /** The primary clock string a surface prints: "14.15s", ">23.89s", "NOT TIMED", "N/A". */
  value: string
  /** The lane proof-state label, e.g. "EXACT VERIFIED" or "10,000 CLIENTS REACHED · HOLD INTERRUPTED". */
  status: string
}

/**
 * Canonical view-model for one V4 Round 5 lane, consumed by every surface so
 * none can independently infer state from partial fields. Returns null when the
 * session is not on the V4 bell runtime (legacy fan-in / setup paths keep their
 * own presentation).
 */
export function roundFiveLanePresentation(
  session: DemoSession,
  laneId: LaneId,
): RoundFiveLanePresentation | null {
  const runtime = roundFiveBellRuntime(session)
  if (!runtime) return null
  if (session.lanes[laneId]?.state === 'not_supported') {
    return {
      laneId,
      semantic: 'not_supported',
      reachedTenK: false,
      verified: false,
      timeMs: null,
      lowerBoundMs: null,
      value: 'N/A',
      status: 'NOT SUPPORTED · N/A',
    }
  }
  const verification = roundFiveLaneVerification(runtime.lanes[laneId])
  if (verification.reachedTenK) {
    const timeMs = verification.timeToTenKMs
    const value = timeMs === null ? 'N/A' : roundFiveSecondsLabel(timeMs)
    if (verification.phase === 'verified') {
      return {
        laneId,
        semantic: 'verified',
        reachedTenK: true,
        verified: true,
        timeMs,
        lowerBoundMs: null,
        value,
        status: 'EXACT VERIFIED',
      }
    }
    if (verification.phase === 'towelled') {
      return {
        laneId,
        semantic: 'reached_hold_interrupted',
        reachedTenK: true,
        verified: false,
        timeMs,
        lowerBoundMs: null,
        value,
        status: '10,000 CLIENTS REACHED · HOLD INTERRUPTED',
      }
    }
    if (verification.phase === 'failed') {
      return {
        laneId,
        semantic: 'reached_hold_failed',
        reachedTenK: true,
        verified: false,
        timeMs,
        lowerBoundMs: null,
        value,
        status: '10,000 CLIENTS REACHED · HOLD FAILED · NOT VERIFIED',
      }
    }
    return {
      laneId,
      semantic: 'reached_hold_in_progress',
      reachedTenK: true,
      verified: false,
      timeMs,
      lowerBoundMs: null,
      value,
      status: '10,000 CLIENTS REACHED · HOLD IN PROGRESS',
    }
  }
  const lowerBoundMs = roundFiveCensoredLowerBoundMs(session, laneId)
  if (lowerBoundMs !== null) {
    return {
      laneId,
      semantic: 'not_reached_lower_bound',
      reachedTenK: false,
      verified: false,
      timeMs: null,
      lowerBoundMs,
      value: `>${roundFiveSecondsLabel(lowerBoundMs)}`,
      status: 'NOT REACHED · UNVERIFIED WHEN STOPPED · LOWER BOUND',
    }
  }
  const stillRunning = verification.phase === 'ramping'
    || verification.phase === 'verifying'
  return {
    laneId,
    semantic: stillRunning ? 'ramping' : 'not_reached',
    reachedTenK: false,
    verified: false,
    timeMs: null,
    lowerBoundMs: null,
    value: 'NOT TIMED',
    status: stillRunning ? 'RACING TO 10,000 CLIENTS' : 'NOT REACHED · NO EXACT RESULT',
  }
}
