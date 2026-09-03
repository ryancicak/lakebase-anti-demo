import type { BoutReceipt } from '../recap'
import type { AllBoutStatus, BoutStatus, CatalogResponse, CreateSessionRequest, DemoSession, EventName, InstallationStatus, RecoveryAttempt, RecoverySpawned, RoundId, RunEvent } from './types'

export interface ReceiptsResponse {
  receipts: BoutReceipt[]
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

const REQUEST_TIMEOUT_MS = 15_000
/**
 * Ringing the bell seals the exact v7 cost identity before the server answers, so
 * /run legitimately takes longer than a read. Only this control gets the longer
 * budget; every other endpoint keeps the short timeout that surfaces a dead server.
 */
export const RUN_REQUEST_TIMEOUT_MS = 60_000

function validationIssue(value: unknown): string | null {
  if (typeof value === 'string') return value.trim() || null
  if (!value || typeof value !== 'object') return null
  const issue = value as { loc?: unknown; msg?: unknown }
  const location = Array.isArray(issue.loc)
    ? issue.loc
        .filter((part) => part !== 'body')
        .filter((part): part is string | number => typeof part === 'string' || typeof part === 'number')
        .join('.')
    : ''
  const message = typeof issue.msg === 'string' ? issue.msg.trim() : ''
  if (!message) return null
  return location ? `${location}: ${message}` : message
}

function apiErrorDetail(value: unknown): string | null {
  if (typeof value === 'string') return value.trim() || null
  if (Array.isArray(value)) {
    const issues = value.map(validationIssue).filter((item): item is string => Boolean(item))
    return issues.length ? issues.join('; ') : null
  }
  return validationIssue(value)
}

async function request<T>(
  path: string,
  init?: RequestInit,
  timeoutMs: number = REQUEST_TIMEOUT_MS,
): Promise<T> {
  let response: Response
  const controller = new AbortController()
  let timedOut = false
  const timeout = window.setTimeout(() => {
    timedOut = true
    controller.abort()
  }, timeoutMs)
  try {
    response = await fetch(path, {
      ...init,
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
        ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
        ...init?.headers,
      },
    })
  } catch {
    throw new ApiError(
      timedOut
        ? 'The API did not answer in time. The app will reconnect automatically.'
        : 'The API could not be reached. Check your connection, then try again.',
      0,
    )
  } finally {
    window.clearTimeout(timeout)
  }
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`
    try {
      const body = (await response.json()) as { detail?: unknown; title?: unknown }
      message = apiErrorDetail(body.detail) ?? apiErrorDetail(body.title) ?? message
    } catch {
      // A non-JSON proxy error still receives a useful HTTP fallback.
    }
    throw new ApiError(message, response.status)
  }
  return response.json() as Promise<T>
}

export const api = {
  catalog: () => request<CatalogResponse>('/api/catalog'),
  /**
   * The round is required, not optional. A v7 installation gives every round its
   * own ring and leaves the installation-wide one unused, so an unscoped ask has
   * no answer -- the server refuses it with a 400 rather than reporting idle.
   */
  boutStatus: (roundId: RoundId) => request<BoutStatus>(
    `/api/bout?round_id=${encodeURIComponent(roundId)}`,
  ),
  allBoutStatuses: () => request<AllBoutStatus>('/api/bout/all'),
  createSession: (body: CreateSessionRequest) =>
    request<DemoSession>('/api/sessions', { method: 'POST', body: JSON.stringify(body) }),
  getSession: (id: string) => request<DemoSession>(`/api/sessions/${encodeURIComponent(id)}`),
  /**
   * Every sealed bout this installation has on disk. The one read that does not go
   * through the in-memory session store, which is why it is what the summary is
   * built on: it is the only thing here that outlives the server process.
   */
  receipts: () => request<ReceiptsResponse>('/api/receipts'),
  armSession: (id: string) => request<DemoSession>(`/api/sessions/${encodeURIComponent(id)}/arm`, { method: 'POST' }),
  cancelArm: (id: string) => request<DemoSession>(`/api/sessions/${encodeURIComponent(id)}/cancel-arm`, { method: 'POST' }),
  runSession: (id: string) => request<DemoSession>(
    `/api/sessions/${encodeURIComponent(id)}/run`,
    { method: 'POST' },
    RUN_REQUEST_TIMEOUT_MS,
  ),
  redoSession: (id: string) => request<DemoSession>(`/api/sessions/${encodeURIComponent(id)}/redo`, { method: 'POST' }),
  retryCleanup: (id: string) => request<DemoSession>(`/api/sessions/${encodeURIComponent(id)}/retry-cleanup`, { method: 'POST' }),
  throwTowel: (id: string) => request<DemoSession>(`/api/sessions/${encodeURIComponent(id)}/towel`, { method: 'POST' }),
  startReset: (id: string) => request<DemoSession>(`/api/sessions/${encodeURIComponent(id)}/cooldown`, { method: 'POST' }),
  eventsUrl: (id: string) => `/api/sessions/${encodeURIComponent(id)}/events`,
  /**
   * Whether the sealed AWS infrastructure is still in the account.
   *
   * `recheck` drops the server's cached verdict and sweeps live. It is what the
   * "Check the account now" control calls, and it is the only way to turn
   * `never_checked` into an answer without waiting out a refresh interval. The
   * sweep is three paginated describes, so it gets the longer budget.
   */
  installation: (recheck = false) => request<InstallationStatus>(
    `/api/installation${recheck ? '?recheck=true' : ''}`,
    undefined,
    recheck ? RUN_REQUEST_TIMEOUT_MS : REQUEST_TIMEOUT_MS,
  ),
  /**
   * The one call in this client that spends money.
   *
   * `confirm` must be the phrase the server issued in `recovery.confirmation_phrase`.
   * It is not derivable here on purpose: a client that could compute it could
   * default it, and the confirmation would stop being a deliberate act.
   */
  recoverInstallation: (confirm: string) => request<RecoverySpawned>(
    '/api/installation/recover',
    { method: 'POST', body: JSON.stringify({ confirm }) },
    RUN_REQUEST_TIMEOUT_MS,
  ),
  recoveryAttempt: (attemptId: string) => request<RecoveryAttempt>(
    `/api/installation/recovery/${encodeURIComponent(attemptId)}`,
  ),
}

export function subscribeToSession(
  sessionId: string,
  onEvent: (event: RunEvent) => void,
  onError: (failure: SessionStreamFailure) => void,
  onOpen: () => void = () => {},
  options: SessionStreamOptions = {},
): () => void {
  const eventNames: EventName[] = [
    'session_created', 'arm_started', 'arm_waiting', 'armed', 'session_cancelled', 'run_preparing',
    'run_started', 'lane_update', 'run_finished', 'session_failed',
    'towel_started', 'towel_update', 'towel_finished', 'cleanup_update',
    'cooldown_started', 'cooldown_update', 'cooldown_ready',
    'redo_started', 'redo_lane_update', 'redo_finished', 'redo_failed',
  ]
  const baseDelayMs = options.baseDelayMs ?? 500
  const maxDelayMs = options.maxDelayMs ?? 10_000
  const permanentAfter = options.permanentAfter ?? 6
  const stableAfterMs = options.stableAfterMs ?? 10_000
  const random = options.random ?? Math.random
  const cursorKey = `lakebase-anti-demo:stream-cursor:${sessionId}`
  let lastSequence = Math.max(options.initialSequence ?? 0, readStreamCursor(cursorKey))
  let source: EventSource | null = null
  let reconnectTimer: number | undefined
  let stableTimer: number | undefined
  let stopped = false
  let failures = 0
  let expectedRotation = false

  const clearTimers = () => {
    if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer)
    if (stableTimer !== undefined) window.clearTimeout(stableTimer)
    reconnectTimer = undefined
    stableTimer = undefined
  }
  const detach = (current: EventSource) => {
    eventNames.forEach((name) => current.removeEventListener(name, handle))
    current.removeEventListener('stream_rotate', handleRotation)
    current.onopen = null
    current.onerror = null
  }
  const closeCurrent = () => {
    if (!source) return
    const current = source
    source = null
    detach(current)
    current.close()
  }
  const scheduleReconnect = (delay: number) => {
    if (stopped || reconnectTimer !== undefined) return
    reconnectTimer = window.setTimeout(() => {
      reconnectTimer = undefined
      connect()
    }, delay)
  }
  const handle = (message: Event) => {
    let event: RunEvent
    try {
      event = JSON.parse((message as MessageEvent<string>).data) as RunEvent
      if (!Number.isSafeInteger(event.sequence) || event.sequence < 1) throw new Error('Invalid sequence')
    } catch {
      closeCurrent()
      failures += 1
      onError({
        kind: 'protocol',
        attempts: failures,
        permanent: failures >= permanentAfter,
      })
      scheduleReconnect(reconnectDelay(failures, baseDelayMs, maxDelayMs, random))
      return
    }
    if (event.sequence <= lastSequence) return
    lastSequence = event.sequence
    writeStreamCursor(cursorKey, lastSequence)
    failures = 0
    onEvent(event)
    if (event.event === 'cooldown_ready' || event.event === 'session_cancelled') {
      stopped = true
      clearTimers()
      closeCurrent()
    }
  }
  const handleRotation = (message: Event) => {
    try {
      const rotation = JSON.parse((message as MessageEvent<string>).data) as { sequence?: unknown }
      if (typeof rotation.sequence === 'number' && Number.isSafeInteger(rotation.sequence)) {
        lastSequence = Math.max(lastSequence, rotation.sequence)
        writeStreamCursor(cursorKey, lastSequence)
      }
      expectedRotation = true
    } catch {
      // A malformed control event is a protocol failure, not a transport reset.
      closeCurrent()
      failures += 1
      onError({
        kind: 'protocol',
        attempts: failures,
        permanent: failures >= permanentAfter,
      })
      scheduleReconnect(reconnectDelay(failures, baseDelayMs, maxDelayMs, random))
    }
  }
  const connect = () => {
    if (stopped || source) return
    expectedRotation = false
    const separator = api.eventsUrl(sessionId).includes('?') ? '&' : '?'
    const current = new EventSource(
      `${api.eventsUrl(sessionId)}${separator}after=${encodeURIComponent(lastSequence)}`,
    )
    source = current
    eventNames.forEach((name) => current.addEventListener(name, handle))
    current.addEventListener('stream_rotate', handleRotation)
    current.onopen = () => {
      if (source !== current || stopped) return
      if (stableTimer !== undefined) window.clearTimeout(stableTimer)
      stableTimer = window.setTimeout(() => {
        stableTimer = undefined
        failures = 0
        onOpen()
      }, stableAfterMs)
    }
    current.onerror = () => {
      if (source !== current || stopped) return
      const rotated = expectedRotation
      if (stableTimer !== undefined) window.clearTimeout(stableTimer)
      stableTimer = undefined
      closeCurrent()
      if (rotated) {
        scheduleReconnect(0)
        return
      }
      failures += 1
      onError({
        kind: 'transport',
        attempts: failures,
        permanent: failures >= permanentAfter,
      })
      scheduleReconnect(reconnectDelay(failures, baseDelayMs, maxDelayMs, random))
    }
  }

  connect()
  return () => {
    stopped = true
    clearTimers()
    closeCurrent()
  }
}

export interface SessionStreamFailure {
  kind: 'transport' | 'protocol'
  attempts: number
  permanent: boolean
}

export interface SessionStreamOptions {
  initialSequence?: number
  baseDelayMs?: number
  maxDelayMs?: number
  permanentAfter?: number
  stableAfterMs?: number
  random?: () => number
}

function reconnectDelay(
  failures: number,
  baseDelayMs: number,
  maxDelayMs: number,
  random: () => number,
): number {
  const exponential = Math.min(maxDelayMs, baseDelayMs * (2 ** Math.max(0, failures - 1)))
  return Math.round(exponential + exponential * 0.25 * random())
}

function readStreamCursor(key: string): number {
  try {
    const value = Number(window.sessionStorage.getItem(key))
    return Number.isSafeInteger(value) && value >= 0 ? value : 0
  } catch {
    return 0
  }
}

function writeStreamCursor(key: string, sequence: number): void {
  try {
    window.sessionStorage.setItem(key, String(sequence))
  } catch {
    // Memory still owns this mount's cursor when storage is unavailable.
  }
}
