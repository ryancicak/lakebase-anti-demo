import type { DemoSession, LaneId, RoundId } from './api/types'
import { classifyOutcome } from './ringside-cues'
import { metricValue, modelScoreEvidence } from './round4'
import { ROUND_FIVE_SAMPLED_QUERIES } from './round5'
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

function roundFiveUsesFanIn(session: DemoSession): boolean {
  return session.round5_setup?.protocol !== 'connection-spike-v1'
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
    return roundFiveUsesFanIn(session)
      ? 'Both lanes reached exactly 10,000 clients, but a required hold, sample, multiplexing, telemetry, identity, fairness, or cleanup check did not pass. No winner or margin was declared.'
      : 'Legacy Round 5 scorecard decoded, but the current 10,000-client fan-in result was not recorded. No fan-in result or margin was declared.'
  }
  if (
    session.round.id === 'survive_connection_spike'
    && roundFiveUsesFanIn(session)
    && Object.values(session.lanes).some(
      (lane) => lane.evidence?.protocol === 'round5-fanin-v2',
    )
  ) {
    const observed = (['lakebase', 'competitor'] as const).map((laneId) => {
      const lane = session.lanes[laneId]
      const raw = lane.evidence?.authenticated_clients ?? lane.evidence?.achieved_clients
      const count = typeof raw === 'number' && Number.isFinite(raw)
        ? `${Math.max(0, Math.floor(raw)).toLocaleString('en-US')}/10,000 clients`
        : 'no trustworthy count recorded'
      return `${lane.name}: ${count}`
    })
    return `The fan-in stopped before every required check completed. ${observed.join(' · ')}. No winner or margin was declared.`
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
    return roundFiveUsesFanIn(session)
      ? ' The per-lane 10,000-client fan-in did not run, so the primary proof did not complete.'
      : ' The legacy scorecard did not record the current 10,000-client fan-in contract, so no fan-in proof is shown.'
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
  const recurringFanIn = roundFiveUsesFanIn(session)
  const setup = [
    exactMetric(
      session,
      'lakebase',
      recurringFanIn ? 'Lakebase time to 10,000' : 'Lakebase built-in pool',
      recurringFanIn ? 'Exact authenticated held-client gate' : 'Included pool verified',
    ) ?? lowerBoundMetric(session, 'lakebase', 'Lakebase built-in pool'),
    exactMetric(
      session,
      'competitor',
      recurringFanIn ? 'Selected AWS path time to 10,000' : 'Selected AWS managed pool',
      recurringFanIn ? 'Exact authenticated held-client gate' : 'New RDS Proxy provisioned',
    ) ?? lowerBoundMetric(session, 'competitor', 'Selected AWS managed pool'),
  ].filter((metric): metric is ReplayMetric => metric !== null)
  return {
    ...state,
    metricBeat: 'setup',
    metrics: setup,
    beats: [
      {
        id: 'setup',
        title: 'Setup',
        body: "Phase 1 timed Lakebase's included pool and new selected RDS Proxy separately.",
      },
      {
        id: 'same-test',
        title: 'Same test',
        body: recurringFanIn
          ? `Phase 2 held exactly 10,000 authenticated held clients per lane, each lane on its own clock, under one verify-full TLS client. All 20,000 held 30s; ${ROUND_FIVE_SAMPLED_QUERIES} lane samples passed; observers proved multiplexing.${incompleteTestSuffix(session)}`
          : `Legacy Round 5 scorecard decoded. The current exact 10,000-client fan-in contract was not recorded, so the replay does not infer fan-in evidence.${incompleteTestSuffix(session)}`,
      },
      {
        id: 'takeaway',
        title: 'Takeaway',
        body: incompleteTakeaway(session)
          ?? (recurringFanIn
            ? 'Fan-in time is primary; setup supports it. Authentication is recorded, not excluded. Client count is not backend count or transaction throughput. Direct AWS connections and other pools were not tested.'
            : 'Legacy scorecard fields remain readable, but only the current 10,000-client fan-in contract is presented. No fan-in claim is inferred.'),
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
