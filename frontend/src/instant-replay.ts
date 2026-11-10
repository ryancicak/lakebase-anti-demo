import type { DemoSession, LaneId, RoundId } from './api/types'
import { byCompetitor, competitorCloud } from './competitor-kind'
import { buildRingsideResult, classifyOutcome } from './ringside-cues'
import { modelScoreEvidence, roundFourLaneLabel, roundFourStackLabel } from './round4'
import { liveOrderEvidence, roundSixLaneLabel, roundSixStackLabel } from './round6'
import {
  ROUND_FIVE_BELL_PROTOCOL,
  ROUND_FIVE_FANIN_PROTOCOL,
  ROUND_FIVE_SAMPLED_QUERIES,
  roundFiveBellRuntime,
  roundFiveLanePresentation,
  roundFiveLaneResult,
  roundFiveServerResetNote,
  roundFiveSetupLaneResult,
} from './round5'
import { preciseDuration } from './time'

export type ReplayStoryState = 'verified' | 'partial' | 'no-result'

export interface ReplayMetric {
  laneId: LaneId
  label: string
  value: string
  note: string
}

/**
 * One call on the tape: what happened, who did it, and when on the bout's clock.
 * `atMs` is milliseconds from the bell, or null for a call that belongs in the
 * sequence but was not stamped against that clock (a failure with no time, a
 * legacy Round 5 fan-in that ran from its own shared start). An untimed call keeps
 * its place in the order it was added; a timed call sorts by its time.
 */
export interface ReplayCall {
  atMs: number | null
  lane: LaneId | 'bell' | 'shared'
  who: string
  text: string
}

/**
 * The instant replay as a tape, not a report: the scoreboard figures, then the
 * calls in the order they happened, then the final call, which is the same
 * sentence Explain to the Room opens with, so the two doors tell one story.
 */
export interface ReplayTape {
  state: ReplayStoryState
  status: string
  metrics: ReplayMetric[]
  calls: ReplayCall[]
  finalCall: string
}

const LANE_IDS: readonly LaneId[] = ['lakebase', 'competitor']

function laneName(session: DemoSession, laneId: LaneId): string {
  if (session.round.id === 'put_model_score_in_app') return roundFourStackLabel(session, laneId)
  if (session.round.id === 'analyze_live_orders_without_slowing_checkout') return roundSixStackLabel(session, laneId)
  return session.round5_setup?.lanes?.[laneId]?.name || session.lanes[laneId].name
}

function roundFiveUsesFanIn(session: DemoSession): boolean {
  const protocol = session.round5_setup?.protocol
  return session.round5_runtime?.protocol === ROUND_FIVE_BELL_PROTOCOL
    || protocol === ROUND_FIVE_FANIN_PROTOCOL
    || protocol === 'round5-fanin-v3'
    || protocol === 'round5-fanin-v2'
}

function finite(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

function detail(status: string | null | undefined): string {
  const trimmed = status?.trim()
  return trimmed ? ` ${trimmed.replace(/\.?$/, '.')}` : ''
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
  for (const laneId of LANE_IDS) {
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

function storyState(session: DemoSession): Pick<ReplayTape, 'state' | 'status'> {
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

const bell = (text: string): ReplayCall => ({ atMs: 0, lane: 'bell', who: 'Bell', text })

function laneCall(session: DemoSession, laneId: LaneId, atMs: number | null, text: string): ReplayCall {
  return { atMs, lane: laneId, who: laneName(session, laneId), text }
}

/**
 * A lane's finishing call for the rounds whose clock is the lane's own elapsed
 * time: the verified sentence with the clock stop, a failure with the server's
 * reason, or a towel floor. Null for a lane that has nothing to say yet.
 */
function laneFinish(session: DemoSession, laneId: LaneId, verifiedText: string): ReplayCall | null {
  const lane = session.lanes[laneId]
  const evidence = classifyOutcome(session).evidence[laneId]
  if (lane.state === 'verified' && evidence.exactMs !== null) {
    return laneCall(session, laneId, evidence.exactMs, `${verifiedText} Clock stops.`)
  }
  if (evidence.lowerBoundMs !== null && lane.state !== 'verified') {
    return laneCall(
      session,
      laneId,
      evidence.lowerBoundMs,
      `Still going when the bout stopped. Its time is a floor, more than ${preciseDuration(evidence.lowerBoundMs)}.`,
    )
  }
  if (lane.state === 'failed') return laneCall(session, laneId, null, `Did not finish.${detail(lane.status)}`)
  if (lane.state === 'towelled') return laneCall(session, laneId, null, 'Stopped before it finished.')
  return null
}

function towelCall(session: DemoSession): ReplayCall | null {
  const cutoff = finite(session.towel?.cutoff_ms ?? session.towel?.lower_bound_ms)
  if (!session.towel || cutoff === null) return null
  const unfinished = LANE_IDS
    .filter((laneId) => session.lanes[laneId].state !== 'verified' && session.lanes[laneId].state !== 'not_supported')
    .map((laneId) => laneName(session, laneId))
  const who = unfinished.length === 0 ? 'Neither lane' : unfinished.join(' and ')
  return {
    atMs: cutoff,
    lane: 'shared',
    who: 'Towel',
    text: `Stopped waiting at ${preciseDuration(cutoff)}. ${who} had not finished, so what you see for ${unfinished.length === 1 ? 'it' : 'them'} is a floor, not a result.`,
  }
}

function cleanupCall(session: DemoSession): ReplayCall | null {
  const outcome = classifyOutcome(session)
  if (outcome.status !== 'cleanup_failure') return null
  const gate = session.round5_setup?.cleanup_failure?.trim()
    || session.towel?.cleanup_failure?.trim()
    || session.cooldown?.failure?.trim()
    || 'the clean-baseline check'
  return {
    atMs: null,
    lane: 'shared',
    who: 'After the bell',
    text: `Cleanup failed at ${gate}. The result above stands, and sharing stays blocked until the cleanup succeeds.`,
  }
}

/** Timed calls in clock order; untimed calls keep their place after the timed ones. */
function inOrder(calls: Array<ReplayCall | null>): ReplayCall[] {
  const present = calls.filter((call): call is ReplayCall => call !== null)
  return present
    .map((call, index) => ({ call, index }))
    .sort((left, right) => {
      const leftAt = left.call.atMs ?? Number.POSITIVE_INFINITY
      const rightAt = right.call.atMs ?? Number.POSITIVE_INFINITY
      if (leftAt !== rightAt) return leftAt - rightAt
      return left.index - right.index
    })
    .map(({ call }) => call)
}

function finish(session: DemoSession, metrics: ReplayMetric[], calls: Array<ReplayCall | null>): ReplayTape {
  return {
    ...storyState(session),
    metrics,
    calls: inOrder([...calls, towelCall(session), cleanupCall(session)]),
    finalCall: buildRingsideResult(session),
  }
}

function roundOneTape(session: DemoSession): ReplayTape {
  const unsupported = session.lanes.competitor.state === 'not_supported'
  const metrics = observedMetrics(
    session,
    {
      lakebase: 'Lakebase wake + transaction',
      competitor: `${session.competitor.short_name} wake + transaction`,
    },
    { lakebase: 'Exact result', competitor: 'Exact result' },
  )
  const wakeText = (laneId: LaneId) => {
    const attempts = session.lanes[laneId].attempts
    return attempts > 1
      ? `Awake after ${attempts} connection attempts. The same transaction committed and read back.`
      : 'Awake. The same transaction committed and read back.'
  }
  return finish(session, metrics, [
    bell(unsupported
      ? `Bell. Lakebase is asleep. ${session.competitor.short_name} can't wake on its own, so it sits this one out, untimed.`
      : 'Bell. Both databases are asleep, and the same connection goes in on both.'),
    laneFinish(session, 'lakebase', wakeText('lakebase')),
    unsupported ? null : laneFinish(session, 'competitor', wakeText('competitor')),
  ])
}

function roundTwoTape(session: DemoSession): ReplayTape {
  const copyKind = (laneId: LaneId) => (laneId === 'lakebase'
    ? 'a branch'
    : byCompetitor(session.competitor.id, {
      aurora: 'a point-in-time clone',
      rds: 'a point-in-time restore',
      azure: 'a point-in-time restore',
    }))
  const metrics = observedMetrics(
    session,
    {
      lakebase: 'Lakebase isolated change',
      competitor: `${session.competitor.short_name} isolated change`,
    },
    { lakebase: 'Exact result', competitor: 'Exact result' },
  )
  const done = (laneId: LaneId) =>
    `Made ${copyKind(laneId)} of the source, ran the schema change on it, committed a real app transaction and read it back, and proved the source untouched.`
  return finish(session, metrics, [
    bell('Bell. Both sides start from the same source, and each makes its own copy to test the change on.'),
    laneFinish(session, 'lakebase', done('lakebase')),
    laneFinish(session, 'competitor', done('competitor')),
  ])
}

function roundThreeTape(session: DemoSession): ReplayTape {
  const metrics = observedMetrics(
    session,
    {
      lakebase: 'Lakebase recovery',
      competitor: `${session.competitor.short_name} recovery`,
    },
    { lakebase: 'Exact result', competitor: 'Exact result' },
  )
  return finish(session, metrics, [
    bell('Bell. The same order is deleted on both sides at the same instant. The recovery point is one second before.'),
    laneFinish(
      session,
      'lakebase',
      'Opened a branch at the recovery point. A fresh connection read the order back while the source still showed it gone.',
    ),
    laneFinish(
      session,
      'competitor',
      'Restored a copy to the recovery point. A fresh connection read the order back while the source still showed it gone.',
    ),
  ])
}

function roundFourTape(session: DemoSession): ReplayTape {
  const evidence = modelScoreEvidence(session.lanes.lakebase)
  const score = evidence.score === '—' ? '' : `: ${evidence.score} for ${evidence.primaryKey}`
  const delta = evidence.deltaVersion === '—' ? '' : ` at Delta version ${evidence.deltaVersion}`
  const awsLane = session.lanes.competitor.state !== 'not_supported'
  const cloud = competitorCloud(session.competitor.id)
  const metrics = observedMetrics(
    session,
    {
      lakebase: roundFourLaneLabel(session, 'lakebase'),
      competitor: roundFourLaneLabel(session, 'competitor'),
    },
    { lakebase: 'Bell to exact app read', competitor: 'Bell to exact app read' },
  )
  const syncMs = finite(session.metrics?.find((metric) => metric.spec_id === 'managed_availability_ms' && metric.lane_id === 'lakebase')?.value)
  return finish(session, metrics, [
    bell(`Bell. One new score lands in the lakehouse${score}${delta}. ${awsLane
      ? 'Both pipelines start cold.'
      : `Only Lakebase's pipeline is here; this install has no ${cloud} lane.`}`),
    syncMs === null
      ? null
      : laneCall(session, 'lakebase', syncMs, "The synced table reports the row landed. That is Lakebase's own sync timestamp, shown on its own and never compared."),
    laneFinish(session, 'lakebase', 'The app read the new score.'),
    awsLane
      ? laneFinish(session, 'competitor', 'The app read the new score.')
      : laneCall(session, 'competitor', null, `Not installed here, so there is no ${cloud} clock and nothing to compare.`),
  ])
}

function roundFiveTape(session: DemoSession): ReplayTape {
  const recurringFanIn = roundFiveUsesFanIn(session)
  const bellRuntime = roundFiveBellRuntime(session)
  const cloud = competitorCloud(session.competitor.id)
  const build = byCompetitor(session.competitor.id, { aurora: 'build an RDS Proxy', rds: 'build an RDS Proxy', azure: 'switch on PgBouncer and raise its client limit' })
  const ready = byCompetitor(session.competitor.id, { aurora: 'The RDS Proxy is built and answering.', rds: 'The RDS Proxy is built and answering.', azure: 'PgBouncer is on and answering.' })

  // Under the V4 bell runtime the primary clock is the observed
  // bell_to_10000_observed_ms, which stays sticky through a hold, a towel, or a
  // failure after 10,000 clients. A lane that only reached 10,000 clients keeps
  // its exact time as timed evidence but is labelled reached/interrupted, never
  // verified; a lane that never reached 10,000 clients shows its lower bound.
  // This is the same canonical view-model the arena and the share receipt read.
  const bellFanInMetric = (laneId: LaneId, label: string): ReplayMetric | null => {
    const presentation = roundFiveLanePresentation(session, laneId)
    if (!presentation) return null
    if (presentation.timeMs !== null) {
      const note = presentation.semantic === 'verified'
        ? 'Exact authenticated held-client gate'
        : presentation.semantic === 'reached_hold_interrupted'
          ? '10,000 clients reached · hold interrupted by towel'
          : presentation.semantic === 'reached_hold_failed'
            ? '10,000 clients reached · hold did not verify'
            : '10,000 clients reached · hold still running'
      return { laneId, label, value: preciseDuration(presentation.timeMs), note }
    }
    if (presentation.lowerBoundMs !== null) {
      return {
        laneId,
        label,
        value: `>${preciseDuration(presentation.lowerBoundMs)}`,
        note: 'Never reached 10,000 clients · unverified when stopped',
      }
    }
    return null
  }
  const exactFanInMetric = (laneId: LaneId, label: string, note: string): ReplayMetric | null => {
    if (bellRuntime) return bellFanInMetric(laneId, label)
    const legacyLane = roundFiveLaneResult(session.lanes[laneId])
    const milliseconds = legacyLane.contractVerified ? legacyLane.timeToTargetMs : null
    return milliseconds === null
      ? null
      : { laneId, label, value: preciseDuration(milliseconds), note }
  }
  const pool = byCompetitor(session.competitor.id, {
    aurora: 'Selected AWS managed pool',
    rds: 'Selected AWS managed pool',
    azure: 'Azure built-in PgBouncer',
  })
  // Each lane's time to the exact 10,000 held-client gate, in the edition's words.
  const tenThousand = byCompetitor(session.competitor.id, {
    aurora: 'Selected AWS path time to 10,000', // the held-client gate
    rds: 'Selected AWS path time to 10,000', // the held-client gate
    azure: 'Azure PgBouncer time to 10,000', // the held-client gate
  })
  const provisioned = byCompetitor(session.competitor.id, { aurora: 'New RDS Proxy provisioned', rds: 'New RDS Proxy provisioned', azure: 'PgBouncer turned on' })
  const metrics = [
    exactFanInMetric(
      'lakebase',
      recurringFanIn ? 'Lakebase time to 10,000' : 'Lakebase built-in pool',
      recurringFanIn ? 'Exact authenticated held-client gate' : 'Included pool verified',
    ) ?? exactMetric(
      session,
      'lakebase',
      recurringFanIn ? 'Lakebase time to 10,000' : 'Lakebase built-in pool',
      recurringFanIn ? 'Exact authenticated held-client gate' : 'Included pool verified',
    ) ?? lowerBoundMetric(session, 'lakebase', 'Lakebase built-in pool'),
    exactFanInMetric(
      'competitor',
      recurringFanIn ? tenThousand : pool,
      recurringFanIn ? 'Exact authenticated held-client gate' : provisioned,
    ) ?? exactMetric(
      session,
      'competitor',
      recurringFanIn ? tenThousand : pool,
      recurringFanIn ? 'Exact authenticated held-client gate' : provisioned,
    ) ?? lowerBoundMetric(session, 'competitor', pool),
  ].filter((metric): metric is ReplayMetric => metric !== null)

  const opening = bell(
    `Bell. Both sides race to hold 10,000 client connections at once. Lakebase connects to its built-in pooler; the ${cloud} path has to ${build} first.`,
  )

  if (bellRuntime) {
    const calls: Array<ReplayCall | null> = [opening]
    for (const laneId of LANE_IDS) {
      const lane = bellRuntime.lanes[laneId]
      const readyMs = finite(lane.pooled_path_ready_observed_ms)
      const rampMs = finite(lane.ramp_started_observed_ms)
      const presentation = roundFiveLanePresentation(session, laneId)
      const reset = roundFiveServerResetNote(session.lanes[laneId])
      if (readyMs !== null) {
        calls.push(laneCall(session, laneId, readyMs, laneId === 'lakebase'
          ? 'Pooled path ready. The first client is through the built-in pooler.'
          : `Pooled path ready. ${ready}`))
      }
      if (rampMs !== null && (readyMs === null || rampMs - readyMs > 50)) {
        calls.push(laneCall(session, laneId, rampMs, 'Clients start piling in.'))
      }
      if (presentation?.timeMs !== null && presentation?.timeMs !== undefined) {
        calls.push(laneCall(session, laneId, presentation.timeMs, '10,000 clients connected and held at once. Clock stops.'))
        // A verified lane passed every check by definition; the live counter on the
        // runtime lane can lag the verdict, so the tape states the contract, not the counter.
        const held = presentation.semantic === 'verified'
          ? `Held all 10,000 clients for 30 seconds; all ${ROUND_FIVE_SAMPLED_QUERIES} checks passed.`
          : presentation.semantic === 'reached_hold_interrupted'
            ? 'The 30-second hold was interrupted by the towel before it could verify.'
            : presentation.semantic === 'reached_hold_failed'
              ? `The 30-second hold did not verify.${detail(lane.status)}`
              : 'The 30-second hold is still running.'
        calls.push(laneCall(session, laneId, null, reset ? `${held} ${reset[0].toUpperCase()}${reset.slice(1)}.` : held))
      } else if (presentation?.lowerBoundMs !== null && presentation?.lowerBoundMs !== undefined) {
        calls.push(laneCall(
          session,
          laneId,
          presentation.lowerBoundMs,
          `Never reached 10,000 clients. Still climbing when the bout stopped, so its time is a floor, more than ${preciseDuration(presentation.lowerBoundMs)}.`,
        ))
      } else if (lane.phase === 'failed') {
        calls.push(laneCall(session, laneId, null, `Did not verify.${detail(lane.status)}`))
      } else if (lane.held_clients > 0 || lane.clients_authenticated > 0) {
        calls.push(laneCall(
          session,
          laneId,
          null,
          `Reached ${Math.max(lane.held_clients, lane.clients_authenticated).toLocaleString('en-US')} of 10,000 clients, then the bout stopped.`,
        ))
      }
    }
    return finish(session, metrics, calls)
  }

  if (!recurringFanIn) {
    return finish(session, metrics, [
      opening,
      { atMs: null, lane: 'shared', who: 'Recording', text: "An older Round 5 recording. The current 10,000-client test isn't in it, so no fan-in call is made." },
    ])
  }

  // The retired fan-in protocols: setup on the bell clock, then a shared start.
  const calls: Array<ReplayCall | null> = [opening]
  for (const laneId of LANE_IDS) {
    const setup = roundFiveSetupLaneResult(session, laneId)
    if (setup.verified && setup.setupElapsedMs !== null) {
      calls.push(laneCall(session, laneId, setup.setupElapsedMs, laneId === 'lakebase'
        ? 'Pooled path ready. The first client is through the built-in pooler.'
        : `Pooled path ready. ${ready}`))
    } else {
      calls.push(laneFinish(session, laneId, 'Pooled path ready.'))
    }
  }
  const bothReady = LANE_IDS.every((laneId) => roundFiveSetupLaneResult(session, laneId).verified)
  if (bothReady) {
    calls.push({ atMs: null, lane: 'shared', who: 'Shared start', text: 'Both paths ready. The clocks restart together, and 10,000 clients pile in on each side.' })
    for (const laneId of LANE_IDS) {
      const result = roundFiveLaneResult(session.lanes[laneId])
      if (result.contractVerified && result.timeToTargetMs !== null) {
        calls.push(laneCall(
          session,
          laneId,
          null,
          `10,000 clients held ${preciseDuration(result.timeToTargetMs)} after the shared start, then held for 30 seconds with ${ROUND_FIVE_SAMPLED_QUERIES} checks passed.`,
        ))
      } else {
        calls.push(laneCall(session, laneId, null, `Did not get 10,000 clients held and checked.${detail(session.lanes[laneId].status)}`))
      }
    }
  }
  return finish(session, metrics, calls)
}

function roundSixTape(session: DemoSession): ReplayTape {
  const total = liveOrderEvidence(session.lanes.lakebase).totalDisplay
  const checkout = total === '—' ? 'The same checkout' : `The same ${total} checkout`
  const awsLane = session.lanes.competitor.state !== 'not_supported'
  const cloud = competitorCloud(session.competitor.id)
  const coldStack = byCompetitor(session.competitor.id, { aurora: 'DMS and Glue start cold', rds: 'DMS and Glue start cold', azure: 'The Azure data flow starts cold' })
  const metrics = observedMetrics(
    session,
    {
      lakebase: roundSixLaneLabel(session, 'lakebase'),
      competitor: roundSixLaneLabel(session, 'competitor'),
    },
    { lakebase: 'Bell to exact Delta read', competitor: 'Bell to exact Delta read' },
  )
  const skew = finite(session.metrics?.find((metric) => metric.spec_id === 'commit_skew_ms')?.value)
  const skewNote = skew === null || !awsLane ? '' : ` The two commits landed ${Math.round(skew)} ms apart.`
  return finish(session, metrics, [
    bell(awsLane
      ? `Bell. ${checkout} commits on both databases. ${coldStack}; Lakebase's feed is already on.${skewNote}`
      : `Bell. ${checkout} commits on Lakebase, whose feed is already on. This install has no ${cloud} lane.`),
    laneFinish(session, 'lakebase', 'The order shows up in the lakehouse.'),
    awsLane
      ? laneFinish(session, 'competitor', 'The order shows up in the lakehouse.')
      : laneCall(session, 'competitor', null, `Not installed here, so there is no ${cloud} clock and nothing to compare.`),
    { atMs: null, lane: 'shared', who: 'Meanwhile', text: 'A second checkout committed on each side as a sanity check, not a load test.' },
  ])
}

const TAPE_BUILDERS: Record<RoundId, (session: DemoSession) => ReplayTape> = {
  wake_idle_app: roundOneTape,
  make_schema_change_safely: roundTwoTape,
  recover_deleted_order: roundThreeTape,
  put_model_score_in_app: roundFourTape,
  survive_connection_spike: roundFiveTape,
  analyze_live_orders_without_slowing_checkout: roundSixTape,
}

export function replayTape(session: DemoSession): ReplayTape {
  return TAPE_BUILDERS[session.round.id](session)
}

/** Every word the tape puts on screen, for the reading-time budget. */
export function replayTapeWordCount(tape: ReplayTape): number {
  return [...tape.calls.map((call) => `${call.who} ${call.text}`), tape.finalCall]
    .join(' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .length
}
