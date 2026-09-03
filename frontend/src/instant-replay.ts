import type { DemoSession, LaneId, RoundId } from './api/types'
import { classifyOutcome } from './ringside-cues'
import { metricValue, modelScoreEvidence } from './round4'
import { preciseDuration } from './time'

export type ReplayBeatId = 'setup' | 'same-test' | 'takeaway'
export type ReplayStoryState = 'verified' | 'partial' | 'no-result'

export interface ReplayMetric {
  laneId: LaneId
  label: string
  value: string
  note: string
}

export interface ReplayBeat {
  id: ReplayBeatId
  title: 'Setup' | 'Same test' | 'Takeaway'
  body: string
}

export interface ReplayStory {
  state: ReplayStoryState
  status: string
  beats: [ReplayBeat, ReplayBeat, ReplayBeat]
  metricBeat: 'setup' | 'takeaway'
  metrics: ReplayMetric[]
}

function number(value: unknown): number | null {
  const parsed = typeof value === 'string' ? Number(value) : value
  return typeof parsed === 'number' && Number.isFinite(parsed) && parsed >= 0
    ? parsed
    : null
}

function evidenceText(session: DemoSession, key: string): string | null {
  const value = session.lanes.lakebase.evidence?.[key]
  return value === null || value === undefined || value === '' ? null : String(value)
}

function laneName(session: DemoSession, laneId: LaneId): string {
  return session.round5_setup?.lanes?.[laneId]?.name || session.lanes[laneId].name
}

function exactMetric(
  session: DemoSession,
  laneId: LaneId,
  label: string,
  note: string,
): ReplayMetric | null {
  const classified = classifyOutcome(session)
  const milliseconds = classified.evidence[laneId].exactMs
  if (milliseconds === null) return null
  return {
    laneId,
    label,
    value: preciseDuration(milliseconds),
    note,
  }
}

function lowerBoundMetric(
  session: DemoSession,
  laneId: LaneId,
  label: string,
): ReplayMetric | null {
  const milliseconds = classifyOutcome(session).evidence[laneId].lowerBoundMs
  if (milliseconds === null) return null
  return {
    laneId,
    label,
    value: `>${preciseDuration(milliseconds)}`,
    note: 'Unverified when stopped',
  }
}

function observedMetrics(
  session: DemoSession,
  labels: Record<LaneId, string>,
  notes: Record<LaneId, string>,
): ReplayMetric[] {
  const metrics: ReplayMetric[] = []
  for (const laneId of ['lakebase', 'competitor'] as const) {
    const exact = exactMetric(session, laneId, labels[laneId], notes[laneId])
    if (exact) {
      metrics.push(exact)
      continue
    }
    const bound = lowerBoundMetric(session, laneId, labels[laneId])
    if (bound) metrics.push(bound)
  }
  return metrics
}

function storyState(session: DemoSession): Pick<ReplayStory, 'state' | 'status'> {
  const outcome = classifyOutcome(session)
  if (outcome.status === 'cleanup_failure') {
    return outcome.contractComplete
      ? { state: 'partial', status: 'Proof complete · Cleanup failed' }
      : { state: 'no-result', status: 'No result · Cleanup failed' }
  }
  if (session.towel) {
    return outcome.evidence.exactLane
      ? { state: 'partial', status: 'Stopped · Partial proof' }
      : { state: 'no-result', status: 'Stopped · No result' }
  }
  if (outcome.contractComplete) {
    return outcome.status === 'declared_capability'
      ? { state: 'verified', status: 'Capability proved' }
      : { state: 'verified', status: 'Result verified' }
  }
  if (outcome.evidence.exactLane || outcome.evidence.laneShape === 'both_exact_verified') {
    return { state: 'partial', status: 'Partial proof · No result' }
  }
  return { state: 'no-result', status: 'No result' }
}

function incompleteTakeaway(session: DemoSession): string | null {
  const outcome = classifyOutcome(session)
  if (outcome.contractComplete && outcome.status !== 'cleanup_failure' && !session.towel) return null
  if (outcome.status === 'cleanup_failure') {
    return outcome.contractComplete
      ? 'The measured proof still stands, but run-owned cleanup failed. Sharing stays blocked until cleanup is verified.'
      : 'The proof did not complete, and run-owned cleanup also failed. No winner or margin can be claimed.'
  }
  if (outcome.status === 'guardrail_failure') {
    return session.round.id === 'analyze_live_orders_without_slowing_checkout'
      ? 'The exact Delta answer was observed, but the separate checkout guardrail did not verify. The capability proof did not complete.'
      : 'A timing was observed, but the exact row identity did not verify. The capability proof did not complete.'
  }
  if (
    session.round.id === 'survive_connection_spike'
    && outcome.evidence.laneShape === 'both_exact_verified'
  ) {
    return 'Both readiness times were observed, but the common spike did not pass its full check. No result or margin was declared.'
  }
  if (outcome.evidence.exactLane) {
    const exact = laneName(session, outcome.evidence.exactLane)
    const otherLane: LaneId = outcome.evidence.exactLane === 'lakebase'
      ? 'competitor'
      : 'lakebase'
    return `${exact} produced exact proof. ${laneName(session, otherLane)} did not, so there is no completed comparison or margin.`
  }
  return 'The round ended without an exact verified result. No winner or margin can be claimed.'
}

function incompleteTestSuffix(session: DemoSession): string {
  const outcome = classifyOutcome(session)
  if (outcome.contractComplete && !session.towel) return ''
  if (
    session.round.id === 'survive_connection_spike'
    && outcome.evidence.exactLane
  ) {
    return ' The shared spike did not run, so the common pass/fail proof did not complete.'
  }
  if (outcome.status === 'guardrail_failure') {
    return ' A required guardrail did not verify.'
  }
  return ' That full proof did not complete.'
}

function roundOneStory(session: DemoSession): ReplayStory {
  const state = storyState(session)
  const unsupported = session.lanes.competitor.state === 'not_supported'
  const setup = unsupported
    ? 'Lakebase began at scale zero. Provisioned RDS cannot automatically scale to zero, so no RDS clock was started.'
    : 'Both databases were confirmed at genuine scale zero before the bell.'
  const metrics = observedMetrics(
    session,
    {
      lakebase: 'Lakebase wake + transaction',
      competitor: `${session.competitor.short_name} wake + transaction`,
    },
    { lakebase: 'Exact result', competitor: 'Exact result' },
  )
  return {
    ...state,
    metricBeat: 'takeaway',
    metrics,
    beats: [
      { id: 'setup', title: 'Setup', body: setup },
      {
        id: 'same-test',
        title: 'Same test',
        body: `A fresh connection had to commit and read back the exact run-owned transaction.${incompleteTestSuffix(session)}`,
      },
      {
        id: 'takeaway',
        title: 'Takeaway',
        body: incompleteTakeaway(session)
          ?? (unsupported
            ? 'Lakebase woke and completed the transaction. This proves the automatic wake path; RDS did not enter the race.'
            : 'The clock covers wake through the exact transaction. It does not measure the rest of the application.'),
      },
    ],
  }
}

function roundTwoStory(session: DemoSession): ReplayStory {
  const state = storyState(session)
  const copyKind = session.competitor.id === 'aurora_serverless_v2'
    ? 'point-in-time clone'
    : 'point-in-time restore'
  return {
    ...state,
    metricBeat: 'takeaway',
    metrics: observedMetrics(
      session,
      {
        lakebase: 'Lakebase isolated change',
        competitor: `${session.competitor.short_name} isolated change`,
      },
      { lakebase: 'Exact result', competitor: 'Exact result' },
    ),
    beats: [
      {
        id: 'setup',
        title: 'Setup',
        body: `Each side created an isolated environment from the same clean source: a Lakebase branch and an AWS ${copyKind}.`,
      },
      {
        id: 'same-test',
        title: 'Same test',
        body: `Both ran the same migration, committed and read back the same application row, then proved the source was unchanged.${incompleteTestSuffix(session)}`,
      },
      {
        id: 'takeaway',
        title: 'Takeaway',
        body: incompleteTakeaway(session)
          ?? 'This measures the safe-change path inside isolated copies. Only run-owned copies are removed afterward; production cleanup was not tested.',
      },
    ],
  }
}

function roundThreeStory(session: DemoSession): ReplayStory {
  const state = storyState(session)
  return {
    ...state,
    metricBeat: 'takeaway',
    metrics: observedMetrics(
      session,
      {
        lakebase: 'Lakebase recovery',
        competitor: `${session.competitor.short_name} recovery`,
      },
      { lakebase: 'Exact result', competitor: 'Exact result' },
    ),
    beats: [
      {
        id: 'setup',
        title: 'Setup',
        body: 'The same run-owned order was committed, aged to a recovery point, then deleted at one shared barrier.',
      },
      {
        id: 'same-test',
        title: 'Same test',
        body: `Each recovery had to return the exact deleted order while a fresh source read still proved it absent.${incompleteTestSuffix(session)}`,
      },
      {
        id: 'takeaway',
        title: 'Takeaway',
        body: incompleteTakeaway(session)
          ?? 'This is recovery timing at the agreed recovery point. It is not a production failover, availability, or high-availability test.',
      },
    ],
  }
}

function roundFourStory(session: DemoSession): ReplayStory {
  const state = storyState(session)
  const outcome = classifyOutcome(session)
  const evidence = modelScoreEvidence(session.lanes.lakebase)
  const score = evidence.score === '—' ? 'the model score' : `score ${evidence.score}`
  const delta = evidence.deltaVersion === '—' ? '' : ` at Delta version ${evidence.deltaVersion}`
  const metric = outcome.evidence.lakebase.exactMs === null
    ? null
    : number(metricValue(session, 'application_proof_elapsed_ms')?.value)
      ?? outcome.evidence.lakebase.exactMs
  return {
    ...state,
    metricBeat: 'takeaway',
    metrics: metric === null
      ? []
      : [{
          laneId: 'lakebase',
          label: 'Delta commit → exact app read',
          value: preciseDuration(metric),
          note: 'Exact Lakebase result',
        }],
    beats: [
      {
        id: 'setup',
        title: 'Setup',
        body: `One exact ${score} was committed to the source Delta table${delta}.`,
      },
      {
        id: 'same-test',
        title: 'Same test',
        body: `Managed Reverse ETL had to report that exact Delta commit, then a fresh app connection had to return the exact row.${incompleteTestSuffix(session)}`,
      },
      {
        id: 'takeaway',
        title: 'Takeaway',
        body: incompleteTakeaway(session)
          ?? 'This proves the Lakebase capability. No AWS reverse-ETL path was built or timed, so there is no AWS race or margin.',
      },
    ],
  }
}

function roundFiveStory(session: DemoSession): ReplayStory {
  const state = storyState(session)
  const setup = [
    exactMetric(
      session,
      'lakebase',
      'Lakebase built-in pool',
      'Ready',
    ) ?? lowerBoundMetric(session, 'lakebase', 'Lakebase built-in pool'),
    exactMetric(
      session,
      'competitor',
      'New RDS Proxy path',
      'Provisioned and ready',
    ) ?? lowerBoundMetric(session, 'competitor', 'New RDS Proxy path'),
  ].filter((metric): metric is ReplayMetric => metric !== null)
  return {
    ...state,
    metricBeat: 'setup',
    metrics: setup,
    beats: [
      {
        id: 'setup',
        title: 'Setup',
        body: 'Lakebase checked its built-in pool. The AWS path provisioned a new RDS Proxy and its supporting resources.',
      },
      {
        id: 'same-test',
        title: 'Same test',
        body: `Both paths had to pass 128 fresh connection attempts, with at most 64 running at once. The spike is pass/fail, not a second speed comparison.${incompleteTestSuffix(session)}`,
      },
      {
        id: 'takeaway',
        title: 'Takeaway',
        body: incompleteTakeaway(session)
          ?? 'Readiness setup for a newly provisioned Proxy is the scored difference. An already-ready, contract-matching Proxy would not pay this provisioning interval and was not tested.',
      },
    ],
  }
}

function roundSixStory(session: DemoSession): ReplayStory {
  const state = storyState(session)
  const outcome = classifyOutcome(session)
  const sku = evidenceText(session, 'sku')
  const store = evidenceText(session, 'store')
  const total = evidenceText(session, 'total_display')
  const order = [sku, store, total].filter(Boolean).join(' · ')
  const elapsed = outcome.evidence.lakebase.exactMs === null
    ? null
    : number(metricValue(session, 'analytics_available_ms')?.value)
      ?? outcome.evidence.lakebase.exactMs
  return {
    ...state,
    metricBeat: 'takeaway',
    metrics: elapsed === null
      ? []
      : [{
          laneId: 'lakebase',
          label: 'Checkout commit → exact Delta answer',
          value: preciseDuration(elapsed),
          note: 'Exact Lakebase result',
        }],
    beats: [
      {
        id: 'setup',
        title: 'Setup',
        body: order
          ? `One checkout committed to application Postgres: ${order}.`
          : 'One checkout committed to the live application Postgres table.',
      },
      {
        id: 'same-test',
        title: 'Same test',
        body: `Delta history had to return that exact order once, and a separate checkout had to commit and read back as the guardrail.${incompleteTestSuffix(session)}`,
      },
      {
        id: 'takeaway',
        title: 'Takeaway',
        body: incompleteTakeaway(session)
          ?? 'This proves the Lakebase change-data capability. No AWS CDC stack was built or timed, so there is no AWS race or margin.',
      },
    ],
  }
}

const STORY_BUILDERS: Record<RoundId, (session: DemoSession) => ReplayStory> = {
  wake_idle_app: roundOneStory,
  make_schema_change_safely: roundTwoStory,
  recover_deleted_order: roundThreeStory,
  put_model_score_in_app: roundFourStory,
  survive_connection_spike: roundFiveStory,
  analyze_live_orders_without_slowing_checkout: roundSixStory,
}

export function replayStory(session: DemoSession): ReplayStory {
  return STORY_BUILDERS[session.round.id](session)
}

export function replayStoryWordCount(story: ReplayStory): number {
  return story.beats
    .map((beat) => `${beat.title} ${beat.body}`)
    .join(' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .length
}
