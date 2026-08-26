import type {
  DemoSession,
  LaneSnapshot,
  MetricValue,
  RedoSnapshot,
  RunEvent,
} from './api/types'

export const ROUND_FOUR_ID = 'put_model_score_in_app' as const
export const ROUND_FOUR_LEGEND = '★ proves a different product behavior'
export const ROUND_FOUR_SCOPE = 'AWS NOT TIMED · MARGIN N/A'
export const ROUND_FOUR_FOOTER = 'LAKEBASE CAPABILITY WIN · AWS NOT TIMED · MARGIN N/A'

export type RoundFourPresentation =
  | 'initial_running'
  | 'initial_verified'
  | 'initial_towelled'
  | 'initial_failed'
  | 'redo_running'
  | 'redo_verified'
  | 'redo_failed'

export function isRoundFour(
  session: DemoSession | null | undefined,
): session is DemoSession & { round: DemoSession['round'] & { id: typeof ROUND_FOUR_ID } } {
  return session?.round.id === ROUND_FOUR_ID
}

export function roundFourPresentation(session: DemoSession): RoundFourPresentation {
  if (session.redo?.state === 'running') return 'redo_running'
  if (session.redo?.state === 'verified') return 'redo_verified'
  if (session.redo?.state === 'failed') return 'redo_failed'
  if (session.state === 'verified') return 'initial_verified'
  if (session.state === 'towelled') return 'initial_towelled'
  if (session.state === 'failed') return 'initial_failed'
  return 'initial_running'
}

export function canStartRoundFourRedo(session: DemoSession): boolean {
  return isRoundFour(session)
    && session.state === 'verified'
    && session.redo?.state === 'ready'
    && session.round.redo?.policy === 'show'
}

export function roundFourFooter(session: DemoSession): string {
  const comparison = session.comparison
  return comparison?.kind === 'capability_gap'
    && comparison.winner_lane_id === 'lakebase'
    && comparison.margin == null
    ? ROUND_FOUR_FOOTER
    : ROUND_FOUR_SCOPE
}

export function metricValue(
  snapshot: Pick<DemoSession, 'metrics'> | Pick<RedoSnapshot, 'metrics'>,
  specId: string,
): MetricValue | undefined {
  return snapshot.metrics?.find((metric) => metric.spec_id === specId)
}

export function metricDisplay(
  snapshot: Pick<DemoSession, 'metrics'> | Pick<RedoSnapshot, 'metrics'>,
  specId: string,
): string {
  const metric = metricValue(snapshot, specId)
  if (!metric) return '—'
  if (metric.display_value) return metric.display_value
  return String(metric.value)
}

export interface ModelScoreEvidence {
  primaryKey: string
  score: string
  modelVersion: string
  proofNonce: string
  deltaVersion: string
  exactRowVerified: boolean
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null ? value as Record<string, unknown> : {}
}

function evidenceString(record: Record<string, unknown>, key: string): string {
  const value = record[key]
  return value === null || value === undefined ? '—' : String(value)
}

export function modelScoreEvidence(lane: LaneSnapshot): ModelScoreEvidence {
  const evidence = asRecord(lane.evidence)
  const verifiedRow = asRecord(evidence.verified_row)
  const primaryKey = evidenceString(evidence, 'primary_key')
  const score = evidenceString(evidence, 'score')
  const modelVersion = evidenceString(evidence, 'model_version')
  const proofNonce = evidenceString(evidence, 'proof_nonce')
  return {
    primaryKey,
    score,
    modelVersion,
    proofNonce,
    deltaVersion: evidenceString(evidence, 'delta_version'),
    exactRowVerified: primaryKey !== '—'
      && evidenceString(verifiedRow, 'primary_key') === primaryKey
      && evidenceString(verifiedRow, 'score') === score
      && evidenceString(verifiedRow, 'model_version') === modelVersion
      && evidenceString(verifiedRow, 'proof_nonce') === proofNonce,
  }
}

function timestamp(value: string): number {
  const parsed = new Date(value).getTime()
  return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed
}

function redoIsTerminal(session: DemoSession): boolean {
  return session.redo?.state === 'verified' || session.redo?.state === 'failed'
}

const activeSessionRank: Partial<Record<DemoSession['state'], number>> = {
  draft: 0,
  checking: 1,
  armed: 2,
  running: 3,
}

const runningLaneRank: Partial<Record<LaneSnapshot['state'], number>> = {
  sealed: 0,
  connecting: 1,
  verifying: 2,
}

function runningLaneRegresses(current: LaneSnapshot, incoming: LaneSnapshot): boolean {
  const currentRank = runningLaneRank[current.state]
  const incomingRank = runningLaneRank[incoming.state]
  if (laneIsTerminal(current) && incomingRank !== undefined) return true
  if (currentRank !== undefined && incomingRank !== undefined && incomingRank < currentRank) {
    return true
  }
  if (current.elapsed_ms === null) return false
  return incomingRank !== undefined
    && (incoming.elapsed_ms === null || incoming.elapsed_ms < current.elapsed_ms)
}

export function acceptsReconciledSession(current: DemoSession, incoming: DemoSession): boolean {
  if (current.id !== incoming.id) return false
  if (timestamp(incoming.updated_at) < timestamp(current.updated_at)) return false
  const currentActiveRank = activeSessionRank[current.state]
  const incomingActiveRank = activeSessionRank[incoming.state]
  if (
    currentActiveRank !== undefined
    && incomingActiveRank !== undefined
    && incomingActiveRank < currentActiveRank
  ) return false
  if (
    current.state === 'verified'
    && incoming.state !== 'verified'
    && !(
      incoming.state === 'running'
      && current.redo?.state === 'ready'
      && incoming.redo?.state === 'running'
    )
  ) return false
  if (redoIsTerminal(current) && !redoIsTerminal(incoming)) return false
  if (current.cooldown?.state === 'ready' && incoming.cooldown?.state !== 'ready') return false
  if (
    redoIsTerminal(current)
    && redoIsTerminal(incoming)
    && current.redo?.state !== incoming.redo?.state
  ) return false
  if ((current.state === 'failed' || current.state === 'towelled') && incoming.state !== current.state) return false
  if (
    current.state === 'running'
    && current.redo?.state === 'running'
    && incoming.redo?.state === 'ready'
  ) return false
  if (
    current.state === 'running'
    && incoming.state === 'running'
    && current.redo?.state === 'running'
    && incoming.redo?.state === 'running'
    && runningLaneRegresses(
      current.redo.lanes.lakebase,
      incoming.redo.lanes.lakebase,
    )
  ) return false
  return true
}

export function selectRound4Session(
  current: DemoSession | null,
  candidate: DemoSession | null,
): DemoSession | null {
  if (!current || !candidate) return candidate
  if (current.id !== candidate.id) return candidate
  return acceptsReconciledSession(current, candidate) ? candidate : current
}

export function roundFourUnsupportedReason(lane: LaneSnapshot): string {
  const reason = asRecord(lane.evidence).unsupported_reason
  return typeof reason === 'string' && reason.trim() ? reason : lane.status
}

function laneIsTerminal(lane: LaneSnapshot): boolean {
  return lane.state === 'verified'
    || lane.state === 'failed'
    || lane.state === 'towelled'
    || lane.state === 'not_supported'
}

function updateLane(
  lane: LaneSnapshot,
  payload: Extract<RunEvent, { event: 'lane_update' | 'redo_lane_update' }>['payload'] & {
    lane_id: NonNullable<Extract<RunEvent, { event: 'lane_update' | 'redo_lane_update' }>['payload']['lane_id']>
    state: NonNullable<Extract<RunEvent, { event: 'lane_update' | 'redo_lane_update' }>['payload']['state']>
  },
  enclosingStateIsTerminal: boolean,
): LaneSnapshot {
  if (enclosingStateIsTerminal && laneIsTerminal(lane) && payload.state !== lane.state) return lane
  return {
    ...lane,
    state: payload.state,
    attempts: payload.attempts ?? lane.attempts,
    elapsed_ms: payload.elapsed_ms ?? lane.elapsed_ms,
    status: payload.status ?? lane.status,
    error: payload.error === undefined ? lane.error : payload.error,
    activity: payload.activity === undefined ? lane.activity : payload.activity,
  }
}

export function applyRunEventSnapshot(current: DemoSession | null, event: RunEvent): DemoSession | null {
  if (
    event.event === 'session_created'
    || event.event === 'armed'
    || event.event === 'session_cancelled'
    || event.event === 'run_started'
    || event.event === 'run_finished'
    || event.event === 'session_failed'
    || event.event === 'towel_started'
    || event.event === 'towel_update'
    || event.event === 'towel_finished'
    || event.event === 'cleanup_update'
    || event.event === 'redo_started'
    || event.event === 'redo_finished'
    || event.event === 'redo_failed'
  ) return event.payload.session

  if (!current) return current
  if (event.event === 'arm_started' || event.event === 'arm_waiting') {
    return { ...current, state: 'checking' }
  }
  if (event.event === 'run_preparing') return current
  if (
    event.event === 'cooldown_started'
    || event.event === 'cooldown_update'
    || event.event === 'cooldown_ready'
  ) return { ...current, cooldown: event.payload.cooldown }
  if (event.event === 'lane_update') {
    if (event.payload.session) return event.payload.session
    const lane = current.lanes[event.payload.lane_id]
    if (!lane) return current
    return {
      ...current,
      lanes: {
        ...current.lanes,
        [event.payload.lane_id]: updateLane(
          lane,
          event.payload,
          current.state === 'verified' || current.state === 'failed' || current.state === 'towelled',
        ),
      },
    }
  }
  if (event.event === 'redo_lane_update') {
    const redo = current.redo
    const lane = redo?.lanes[event.payload.lane_id]
    if (!redo || !lane) return current
    return {
      ...current,
      redo: {
        ...redo,
        lanes: {
          ...redo.lanes,
          [event.payload.lane_id]: updateLane(
            lane,
            event.payload,
            redo.state === 'verified' || redo.state === 'failed',
          ),
        },
      },
    }
  }
  return current
}
