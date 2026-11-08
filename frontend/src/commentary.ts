import type { CooldownLaneSnapshot, DemoSession, LaneId } from './api/types'
import { byCompetitor } from './competitor-kind'
import { classifyOutcome } from './ringside-cues'
import { isRoundFour, roundFourStackLabel } from './round4'
import {
  ROUND_FIVE_BELL_PROTOCOL,
  ROUND_FIVE_SAMPLED_QUERIES,
  isRoundFive,
  isRoundFiveFanInEvidence,
  isRoundFiveSetupEvidence,
  roundFiveLaneResult,
  roundFiveServerResetNote,
} from './round5'
import { isRoundSix, roundSixStackLabel } from './round6'

/**
 * What the ringside commentator says while a bout runs.
 *
 * `headline` is the one call for the room: where the bout stands right now, in a
 * sentence anyone can follow. `lanes` is one line per fighter, in plain words,
 * naming what that lane is doing at this moment. The server's own status string
 * is appended only where it carries a detail the words above do not (a failure
 * reason, a setup gate), never as the whole line. Nothing here decides a verdict:
 * every "done" below is the server's state, read back.
 */
export interface LiveCommentary {
  headline: string
  lanes: string[]
}

const LANE_IDS: readonly LaneId[] = ['lakebase', 'competitor']

function seconds(milliseconds: number | null | undefined): string {
  return typeof milliseconds === 'number' && Number.isFinite(milliseconds)
    ? `${(milliseconds / 1000).toFixed(2)}s`
    : ''
}

function at(milliseconds: number | null | undefined): string {
  const value = seconds(milliseconds)
  return value ? ` at ${value}` : ''
}

function cloudOf(session: DemoSession): string {
  return byCompetitor(session.competitor.id, { aurora: 'AWS', rds: 'AWS', azure: 'Azure' })
}

function detail(status: string | null | undefined): string {
  const trimmed = status?.trim()
  return trimmed ? ` ${trimmed.replace(/\.?$/, '.')}` : ''
}

function count(value: number): string {
  return value.toLocaleString('en-US')
}

/**
 * The headline every round shares once lanes start finishing: who is done, who is
 * still at it, and that the server, not this screen, calls the result.
 */
function raceHeadline(
  session: DemoSession,
  nameOf: (laneId: LaneId) => string,
  stillAt: string,
  bell: string,
): string {
  const inRace = LANE_IDS.filter((laneId) => session.lanes[laneId].state !== 'not_supported')
  const done = inRace.filter((laneId) => session.lanes[laneId].state === 'verified')
  if (inRace.length > 0 && done.length === inRace.length) {
    return 'Both are done. The server is checking the clocks.'
  }
  if (done.length === 1 && inRace.length === 2) {
    const other = inRace.find((laneId) => laneId !== done[0])!
    return `${nameOf(done[0])} is done${at(session.lanes[done[0]].elapsed_ms)}. ${nameOf(other)} is still ${stillAt}.`
  }
  return bell
}

function wakeCommentary(session: DemoSession): LiveCommentary {
  const lanes = LANE_IDS.map((laneId) => {
    const lane = session.lanes[laneId]
    if (lane.state === 'not_supported') {
      return `${lane.name}: can't wake on its own, so it isn't in this round.`
    }
    if (lane.state === 'verified') {
      return `${lane.name}: awake. The transaction committed and read back${at(lane.elapsed_ms)}.`
    }
    if (lane.state === 'failed') return `${lane.name}: never got a verified transaction through.${detail(lane.status)}`
    if (lane.state === 'towelled') return `${lane.name}: stopped before it finished.`
    if (lane.state === 'connecting') {
      return lane.attempts > 0
        ? `${lane.name}: still asleep. Connection attempt ${lane.attempts}.`
        : `${lane.name}: asleep. The first connection is going in.`
    }
    return `${lane.name}: waiting for its first connection.`
  })
  const unsupported = LANE_IDS.map((laneId) => session.lanes[laneId]).find((lane) => lane.state === 'not_supported')
  if (unsupported) {
    const lakebase = session.lanes.lakebase
    return {
      headline: lakebase.state === 'verified'
        ? `Lakebase is awake and done${at(lakebase.elapsed_ms)}. ${unsupported.name} can't wake on its own, so there was nothing to race.`
        : `Only Lakebase is in this one. ${unsupported.name} has no automatic wake, so it stays out and untimed.`,
      lanes,
    }
  }
  return {
    headline: raceHeadline(
      session,
      (laneId) => session.lanes[laneId].name,
      'waking up',
      'Bell. Both databases are asleep, and the first connection is on its way.',
    ),
    lanes,
  }
}

function copyKind(session: DemoSession, laneId: LaneId): string {
  if (laneId === 'lakebase') return 'a branch'
  return byCompetitor(session.competitor.id, {
    aurora: 'a point-in-time clone',
    rds: 'a point-in-time restore',
    azure: 'a point-in-time restore',
  })
}

function safeChangeCommentary(session: DemoSession): LiveCommentary {
  const lanes = LANE_IDS.map((laneId) => {
    const lane = session.lanes[laneId]
    const phase = lane.activity?.phase
    if (lane.state === 'verified' || phase === 'verified') {
      return `${lane.name}: done. Copy migrated, transaction verified, source untouched${at(lane.elapsed_ms)}.`
    }
    if (lane.state === 'failed' || phase === 'failed') return `${lane.name}: did not finish.${detail(lane.status)}`
    if (lane.state === 'towelled') return `${lane.name}: stopped before it finished.`
    switch (phase) {
      case 'creating': return `${lane.name}: making ${copyKind(session, laneId)} of the source.`
      case 'migrating': return `${lane.name}: running the schema change on its copy.`
      case 'verifying_application': return `${lane.name}: committing a real app transaction on the copy and reading it back.`
      case 'verifying_source': return `${lane.name}: checking the source is exactly as it was.`
      default: return `${lane.name}: getting started.`
    }
  })
  return {
    headline: raceHeadline(
      session,
      (laneId) => session.lanes[laneId].name,
      'working on its copy',
      'Bell. Each side is making its own copy of production to test the change on.',
    ),
    lanes,
  }
}

function recoveryCommentary(session: DemoSession): LiveCommentary {
  const lanes = LANE_IDS.map((laneId) => {
    const lane = session.lanes[laneId]
    const phase = lane.activity?.phase
    if (lane.state === 'verified' || phase === 'verified') {
      return `${lane.name}: the order is back and readable${at(lane.elapsed_ms)}.`
    }
    if (lane.state === 'failed' || phase === 'failed') return `${lane.name}: did not get the order back.${detail(lane.status)}`
    if (lane.state === 'towelled') return `${lane.name}: stopped while still recovering.`
    switch (phase) {
      case 'preparing_incident': return `${lane.name}: lining up the order to delete.`
      case 'deleting_incident': return `${lane.name}: deleting the order.`
      case 'waiting_recovery_point': return `${lane.name}: waiting until the recovery point is reachable.`
      case 'restoring':
        return laneId === 'lakebase'
          ? `${lane.name}: opening a branch at one second before the delete.`
          : `${lane.name}: restoring a copy to one second before the delete.`
      case 'connecting': return `${lane.name}: connecting to the recovered copy.`
      case 'verifying_recovered_order': return `${lane.name}: reading the recovered order back.`
      case 'verifying_source': return `${lane.name}: confirming the source still shows it deleted.`
      default: return `${lane.name}: getting started.`
    }
  })
  return {
    headline: raceHeadline(
      session,
      (laneId) => session.lanes[laneId].name,
      'recovering',
      'Bell. The same order was just deleted on both sides. Now they race to get it back.',
    ),
    lanes,
  }
}

function modelScoreCommentary(session: DemoSession): LiveCommentary {
  const nameOf = (laneId: LaneId) => roundFourStackLabel(session, laneId)
  const lanes = LANE_IDS.map((laneId) => {
    const lane = session.lanes[laneId]
    const name = nameOf(laneId)
    if (lane.state === 'not_supported') {
      return `${session.competitor.short_name}: the ${cloudOf(session)} lane isn't installed here, so there's no clock for it.`
    }
    if (lane.state === 'verified') return `${name}: the app read the new score${at(lane.elapsed_ms)}.`
    if (lane.state === 'failed') return `${name}: never delivered it.${detail(lane.status)}`
    if (lane.state === 'towelled') return `${name}: stopped before it delivered.`
    const phase = lane.activity?.phase
    if (phase === 'starting') return `${name}: starting cold.`
    if (phase === 'waiting') return `${name}: running. The app checks for the new score every quarter second.`
    return `${name}: getting started.${detail(lane.status)}`
  })
  if (session.lanes.competitor.state === 'not_supported') {
    return {
      headline: session.lanes.lakebase.state === 'verified'
        ? `Lakebase delivered the score${at(session.lanes.lakebase.elapsed_ms)}. There is no ${cloudOf(session)} lane here to race it.`
        : `Bell. One new score just landed in the lakehouse. Only Lakebase is carrying it here.`,
      lanes,
    }
  }
  return {
    headline: raceHeadline(
      session,
      nameOf,
      'carrying it',
      'Bell. One new score just landed in the lakehouse. Both pipelines start cold.',
    ),
    lanes,
  }
}

function liveOrdersCommentary(session: DemoSession): LiveCommentary {
  const nameOf = (laneId: LaneId) => roundSixStackLabel(session, laneId)
  const coldStack = byCompetitor(session.competitor.id, {
    aurora: 'DMS and Glue are starting cold',
    rds: 'DMS and Glue are starting cold',
    azure: 'the data flow is starting cold',
  })
  const lanes = LANE_IDS.map((laneId) => {
    const lane = session.lanes[laneId]
    const name = nameOf(laneId)
    if (lane.state === 'not_supported') {
      return `${session.competitor.short_name}: the ${cloudOf(session)} lane isn't installed here, so there's no clock for it.`
    }
    if (lane.state === 'verified') return `${name}: the order is in the lakehouse${at(lane.elapsed_ms)}.`
    if (lane.state === 'failed') return `${name}: never delivered it.${detail(lane.status)}`
    if (lane.state === 'towelled') return `${name}: stopped before it delivered.`
    const phase = lane.activity?.phase
    if (phase === 'starting') {
      return laneId === 'lakebase'
        ? `${name}: committing the checkout. The feed is already on.`
        : `${name}: committing the checkout. ${coldStack[0].toUpperCase()}${coldStack.slice(1)}.`
    }
    if (phase === 'waiting') return `${name}: carrying the order. The lakehouse is checked every second.`
    return `${name}: getting started.${detail(lane.status)}`
  })
  if (session.lanes.competitor.state === 'not_supported') {
    return {
      headline: session.lanes.lakebase.state === 'verified'
        ? `Lakebase has the order in the lakehouse${at(session.lanes.lakebase.elapsed_ms)}. There is no ${cloudOf(session)} lane here to race it.`
        : 'Bell. The checkout just hit the database. Only Lakebase is carrying it here.',
      lanes,
    }
  }
  return {
    headline: raceHeadline(
      session,
      nameOf,
      'carrying it',
      'Bell. The same checkout just hit both databases. Now they race to the lakehouse.',
    ),
    lanes,
  }
}

/** Round 5's result as a call, once the server has declared it. */
function pooledPathResult(session: DemoSession, nameOf: (laneId: LaneId) => string): string {
  const classified = classifyOutcome(session)
  if (classified.status === 'declared_comparison') {
    if (classified.formalWinner === 'tie') return 'Both reached 10,000 clients together. The server calls it a tie.'
    if (classified.formalWinner === 'lakebase' || classified.formalWinner === 'competitor') {
      const margin = classified.marginMs === null ? '' : `, by ${seconds(classified.marginMs)}`
      return `${nameOf(classified.formalWinner)} reached 10,000 clients first${margin}. Both held and passed every check.`
    }
  }
  return 'The bout stopped before both sides verified. No winner, no margin.'
}

function pooledPathCommentary(session: DemoSession, connected: boolean): LiveCommentary {
  const nameOf = (laneId: LaneId) => session.round5_setup?.lanes?.[laneId]?.name ?? session.lanes[laneId].name
  const pooler = byCompetitor(session.competitor.id, {
    aurora: ['building an RDS Proxy', 'the Proxy is up', 'still building its Proxy'],
    rds: ['building an RDS Proxy', 'the Proxy is up', 'still building its Proxy'],
    azure: ['switching on PgBouncer and raising its client limit', 'PgBouncer is on', 'still switching on PgBouncer'],
  })
  const runtime = session.round5_runtime?.protocol === ROUND_FIVE_BELL_PROTOCOL ? session.round5_runtime : null

  if (runtime) {
    const lanes = LANE_IDS.map((laneId) => {
      const lane = runtime.lanes[laneId]
      const name = nameOf(laneId)
      const reset = roundFiveServerResetNote(session.lanes[laneId])
      const resetNote = reset ? ` ${reset[0].toUpperCase()}${reset.slice(1)}.` : ''
      switch (lane.phase) {
        case 'dispatching': return `${name}: the first client is on its way.`
        case 'provisioning_proxy': return `${name}: ${pooler[0]} before its first client can connect. The clock is running.`
        case 'verifying_proxy': return `${name}: ${pooler[1]}. Checking it answers.`
        case 'verifying_path': return `${name}: first client through the pooled path.`
        case 'ramping': return `${name}: ${count(lane.held_clients)} of 10,000 clients connected and holding.${resetNote}`
        case 'holding': return `${name}: ${count(lane.held_clients)} connected and holding for 30 seconds. ${lane.sampled_queries_succeeded} of ${ROUND_FIVE_SAMPLED_QUERIES} checks passed.${resetNote}`
        case 'verified': return `${name}: 10,000 clients held and checked. Clock stopped${at(lane.bell_to_10000_observed_ms)}.${resetNote}`
        case 'failed': return `${name}: did not verify.${detail(lane.status)}`
        default: return `${name}: ${count(lane.held_clients)} of 10,000 clients held.${detail(lane.status)}`
      }
    })
    const stillAt = (laneId: LaneId): string => {
      const phase = runtime.lanes[laneId].phase
      if (phase === 'provisioning_proxy' || phase === 'verifying_proxy') return pooler[2]
      if (phase === 'ramping') return `at ${count(runtime.lanes[laneId].held_clients)} of 10,000 clients`
      if (phase === 'holding') return 'holding its 10,000 clients'
      return 'still getting there'
    }
    let headline: string
    if (runtime.state === 'verified') headline = pooledPathResult(session, nameOf)
    else if (runtime.state === 'running') {
      if (!connected) headline = 'The live feed dropped. Both clocks keep running on the server; reconnecting.'
      else {
        const done = LANE_IDS.filter((laneId) => runtime.lanes[laneId].phase === 'verified')
        headline = done.length === 1
          ? `${nameOf(done[0])} has 10,000 clients held and checked. ${nameOf(done[0] === 'lakebase' ? 'competitor' : 'lakebase')} is ${stillAt(done[0] === 'lakebase' ? 'competitor' : 'lakebase')}.`
          : 'Bell. Both sides race to hold 10,000 client connections at once. No winner until both verify.'
      }
    } else headline = 'The bout stopped before both sides verified. No winner, no margin.'
    return { headline, lanes }
  }

  // Older recordings: the fan-in protocols before the bell, then the setup-only phase.
  const fanInActive = LANE_IDS.some((laneId) => isRoundFiveFanInEvidence(session.lanes[laneId].evidence))
  if (fanInActive) {
    const lanes = LANE_IDS.map((laneId) => {
      const lane = session.lanes[laneId]
      const result = roundFiveLaneResult(lane)
      if (result.bounded) return `${lane.name}: an older recording. The current 10,000-client test isn't in it.`
      const held = result.held ?? result.authenticated ?? lane.attempts
      return result.timeToTargetMs === null
        ? `${lane.name}: ${count(held)} of ${count(result.targetClients)} clients held.${detail(lane.status)}`
        : `${lane.name}: ${count(result.targetClients)} clients held${at(result.timeToTargetMs)}.`
    })
    const headline = session.state === 'verified'
      ? pooledPathResult(session, nameOf)
      : !connected
        ? 'The live feed dropped. The clocks keep running on the server; reconnecting.'
        : 'Both sides are climbing to 10,000 client connections. No winner until both hold and pass every check.'
    return { headline, lanes }
  }

  const setup = isRoundFiveSetupEvidence(session.round5_setup) ? session.round5_setup : null
  const states = LANE_IDS.map((laneId) => setup?.lanes?.[laneId]?.state ?? 'pending')
  const lanes = LANE_IDS.map((laneId, index) => {
    const lane = setup?.lanes?.[laneId]
    const name = lane?.name || session.lanes[laneId].name
    switch (states[index]) {
      case 'running': return connected
        ? `${name}: getting its pooled path ready.${detail(lane?.status)}`
        : `${name}: the live feed dropped; its clock keeps running on the server.`
      case 'verified': return `${name}: pooled path ready${at(lane?.setup_elapsed_ms)}.`
      case 'failed': return `${name}: its pooled path never came up.${detail(lane?.status)}`
      case 'cleanup_failed': return `${name}: cleanup did not verify.${detail(lane?.status)}`
      case 'towelled': return `${name}: stopped before its pooled path was ready.`
      default: return `${name}: waiting for the bell.`
    }
  })
  const verifiedLanes = LANE_IDS.filter((_, index) => states[index] === 'verified')
  const failed = states.some((state) => state === 'failed' || state === 'cleanup_failed' || state === 'towelled')
  const headline = verifiedLanes.length === 2 && session.state === 'verified'
    ? pooledPathResult(session, nameOf)
    : verifiedLanes.length === 1
      ? `${nameOf(verifiedLanes[0])} is ready. ${nameOf(verifiedLanes[0] === 'lakebase' ? 'competitor' : 'lakebase')} is still setting up.`
      : failed
        ? 'The pooled-path setup did not complete. No comparison.'
        : !connected && states.includes('running')
          ? 'The live feed dropped. The clocks keep running on the server; reconnecting.'
          : states.includes('running')
            ? 'Bell. Each side is getting a pooled path ready.'
            : "Getting ready. Each side's clock starts at the bell."
  return { headline, lanes }
}

/** The commentary while a bout runs, in the commentator's voice. */
export function liveCommentary(session: DemoSession, connected = true): LiveCommentary {
  if (isRoundFive(session)) return pooledPathCommentary(session, connected)
  const commentary = isRoundFour(session)
    ? modelScoreCommentary(session)
    : isRoundSix(session)
      ? liveOrdersCommentary(session)
      : session.round.id === 'wake_idle_app'
        ? wakeCommentary(session)
        : session.round.id === 'make_schema_change_safely'
          ? safeChangeCommentary(session)
          : session.round.id === 'recover_deleted_order'
            ? recoveryCommentary(session)
            : {
                headline: 'Preview only. This round has no live adapter here, so nothing is timed.',
                lanes: LANE_IDS.map((laneId) => `${session.lanes[laneId].name}: not running.`),
              }
  if (!connected && !LANE_IDS.every((laneId) => session.lanes[laneId].state === 'verified')) {
    return {
      ...commentary,
      headline: 'The live feed dropped. The clocks keep running on the server; reconnecting.',
    }
  }
  return commentary
}

function idleLine(lane: CooldownLaneSnapshot): string {
  if (lane.state === 'confirmed_zero') return `${lane.name}: idle again.`
  if (lane.state === 'not_supported') return `${lane.name}: doesn't idle on its own, so there's nothing to wait for.`
  if (lane.state === 'failed') return `${lane.name}: idle was not confirmed.`
  if (lane.observed_state === 'IDLE') return `${lane.name}: idle. Confirming.`
  return `${lane.name}: winding down.`
}

/** The commentary while the ring is reset between bouts. */
export function cooldownCommentary(session: DemoSession): LiveCommentary {
  const cooldown = session.cooldown!
  const lanes = [cooldown.lanes.lakebase, cooldown.lanes.competitor]
  if (cooldown.mode === 'return_to_idle') {
    return {
      headline: cooldown.state === 'ready'
        ? 'Both databases are back to idle.'
        : 'Waiting for both databases to wind down to idle.',
      lanes: lanes.map(idleLine),
    }
  }
  const copy = cooldown.mode === 'delete_recovery_environment' ? 'recovery copy' : 'test copy'
  return {
    headline: `Cleaning up. Only the ${copy} each side made for this bout is removed.`,
    lanes: lanes.map((lane) => {
      if (lane.state === 'confirmed_deleted' || lane.state === 'confirmed_zero') return `${lane.name}: its ${copy} is gone.`
      if (lane.state === 'not_supported') return `${lane.name}: nothing to remove.`
      if (lane.state === 'failed') return `${lane.name}: removing its ${copy} failed.${detail(lane.status)}`
      return `${lane.name}: removing its ${copy}.${detail(lane.status)}`
    }),
  }
}
