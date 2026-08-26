import type { BoutReceipt } from '../recap'
import type { BoutStatus, CatalogResponse, CreateSessionRequest, DemoSession, EventName, InstallationStatus, RecoveryAttempt, RecoverySpawned, RoundId, RunEvent } from './types'

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
        ? 'The demo server did not answer in time. The app will reconnect automatically.'
        : 'The demo server could not be reached. Make sure it is running, then try again.',
      0,
    )
  } finally {
    window.clearTimeout(timeout)
  }
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`
    try {
      const body = (await response.json()) as { detail?: string; title?: string }
      message = body.detail ?? body.title ?? message
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
  onError: () => void,
  onOpen: () => void = () => {},
): () => void {
  // No `after` cursor, deliberately.
  //
  // Reconnects are already exact without one: the browser resends
  // `Last-Event-ID` on its own and the server resumes from it, taking the
  // greater of that and any `after` in the URL. So a cursor here would buy
  // nothing on the path that matters and could only make it worse -- because the
  // server takes the *maximum*, a cursor the URL is wrong about cannot be
  // corrected downwards for the lifetime of this EventSource.
  //
  // What it would change is a remount, which starts from the server's retention
  // floor rather than from where the previous mount left off. To pass a cursor
  // there, the high-water sequence would have to outlive the component, and then
  // it would have to be scoped to the session or a fresh bout would resume from
  // the previous bout's number and silently skip its own opening -- with no
  // `gap_before`, because the server would be honouring the cursor it was given.
  // That failure is invisible; this one is reported. `RunEvent.gap_before` names
  // the hole on the first event of such a resume, the play-by-play now shows it,
  // and every snapshot-bearing event carries the whole session, so a mid-history
  // start reconstructs correctly.
  const source = new EventSource(api.eventsUrl(sessionId))
  const eventNames: EventName[] = [
    'session_created', 'arm_started', 'arm_waiting', 'armed', 'session_cancelled', 'run_preparing',
    'run_started', 'lane_update', 'run_finished', 'session_failed',
    'towel_started', 'towel_update', 'towel_finished', 'cleanup_update',
    'cooldown_started', 'cooldown_update', 'cooldown_ready',
    'redo_started', 'redo_lane_update', 'redo_finished', 'redo_failed',
  ]
  const handle = (message: Event) => {
    try {
      onEvent(JSON.parse((message as MessageEvent<string>).data) as RunEvent)
    } catch {
      onError()
    }
  }
  eventNames.forEach((name) => source.addEventListener(name, handle))
  source.onopen = onOpen
  source.onerror = onError
  return () => {
    eventNames.forEach((name) => source.removeEventListener(name, handle))
    source.onopen = null
    source.onerror = null
    source.close()
  }
}
