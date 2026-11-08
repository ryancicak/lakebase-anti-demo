import type { CustomerCorner, DemoSession, LaneId, PersonaId } from '../api/types'
import { byCompetitor, scalesToZero } from '../competitor-kind'
import { modelScoreEvidence, roundFourStackLabel } from '../round4'
import { roundSixStackLabel } from '../round6'
import {
  roundFiveBellRuntime,
  roundFiveHasComparison,
  roundFiveLanePresentation,
  roundFiveLaneResult,
  roundFiveSetupLaneResult,
  roundFiveUsesFanIn,
} from '../round5'
import {
  classifyEvidence,
  resolveRoundContract,
  type ContractComparison,
  type FormalWinner,
  type RoundContractDecision,
} from '../outcome'
import {
  PRIORITIES,
  getLensLine,
  getResultLine,
  getRoleLine,
} from './corpus'
import type {
  ResultLineRecord,
  RingsideBriefing,
  RingsideLens,
  RingsideOutcomeId,
} from './types'

/**
 * The room's priorities in the order the briefing reads them, cost then simplicity
 * then performance, whatever order they were picked in. One to three; none is an
 * error, because a briefing with no lens has nothing to say about the room.
 */
export function orderedPriorities(corners: readonly CustomerCorner[]): CustomerCorner[] {
  const selected = new Set(corners)
  const ordered = PRIORITIES.filter((priority) => selected.has(priority))
  if (ordered.length === 0) {
    throw new Error('Ringside briefing requires one to three priorities; received none.')
  }
  return ordered
}

function nonNegativeNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

function verifiedElapsed(session: DemoSession, laneId: LaneId): number | null {
  const lane = session.lanes[laneId]
  return lane.state === 'verified' ? nonNegativeNumber(lane.elapsed_ms) : null
}

/**
 * Round 5's scored quantity, which is not the same measurement in both protocols.
 *
 * Under the bounded protocol the score *was* pooled-path setup, so the setup stop
 * is the exact time. Under the fan-in protocol the score is shared-T0 time to
 * exactly 10,000 authenticated held clients, and setup is supporting evidence --
 * the AWS lane spends most of its setup provisioning an RDS Proxy, which is the
 * finding rather than the result.
 *
 * Returning setup under fan-in was wrong twice over: the replay labelled the value
 * "time to 10,000" while showing a setup duration, and the declared margin comes
 * from `time_to_10000_ms`, so the lane times on screen did not add up to the margin
 * printed beside them.
 */
function roundFiveExactPrimaryMs(session: DemoSession, laneId: LaneId): number | null {
  const runtimeLane = session.round5_runtime?.lanes[laneId]
  if (session.round5_runtime?.protocol === 'round5-bell-to-10k-v4') {
    return (
      runtimeLane?.phase === 'verified'
      && session.lanes[laneId].state === 'verified'
    )
      ? nonNegativeNumber(runtimeLane.bell_to_10000_observed_ms)
      : null
  }
  if (roundFiveUsesFanIn(session)) {
    // Falls back to the setup stop rather than the lane's elapsed time. A bout
    // towelled during setup has a real exact setup measurement and no fan-in result
    // at all, while `elapsed_ms` on such a lane is the wall time the lane spent
    // before it was stopped -- a number that measures nothing this round claims.
    return roundFiveLaneResult(session.lanes[laneId]).timeToTargetMs
      ?? roundFiveExactSetupMs(session, laneId)
  }
  return roundFiveExactSetupMs(session, laneId)
}

function roundFiveExactSetupMs(session: DemoSession, laneId: LaneId): number | null {
  const strict = roundFiveSetupLaneResult(session, laneId)
  if (strict.verified) return strict.setupElapsedMs
  if (!session.towel) return null

  /*
   * A setup towel can freeze one exact stop before the two-lane gate document
   * is assembled. The orchestrator marks both the public setup lane and the
   * corresponding frozen lane verified; requiring the later gate document is
   * the old Round 5 "neither verified" bug.
   */
  const setupLane = session.round5_setup?.lanes?.[laneId]
  const elapsedMs = nonNegativeNumber(setupLane?.setup_elapsed_ms)
  return setupLane?.state === 'verified'
    && session.lanes[laneId].state === 'verified'
    && elapsedMs !== null
    ? elapsedMs
    : null
}

function comparisonFor(session: DemoSession): ContractComparison | null {
  const comparison = session.comparison
  if (comparison) {
    const winner = comparison.winner_lane_id
    const winnerLaneId = winner === 'lakebase' || winner === 'competitor' ? winner : null
    const marginValue = comparison.margin?.value
    const marginMs = nonNegativeNumber(marginValue)
    return {
      kind: comparison.kind,
      winnerLaneId,
      marginMs,
    }
  }

  if (
    session.round.id === 'wake_idle_app'
    || session.round.id === 'make_schema_change_safely'
    || session.round.id === 'recover_deleted_order'
  ) {
    const lakebaseMs = verifiedElapsed(session, 'lakebase')
    const competitorMs = verifiedElapsed(session, 'competitor')
    if (lakebaseMs !== null && competitorMs !== null) {
      if (lakebaseMs === competitorMs) {
        return { kind: 'tie', winnerLaneId: null, marginMs: null }
      }
      return {
        kind: 'measured',
        winnerLaneId: lakebaseMs < competitorMs ? 'lakebase' : 'competitor',
        marginMs: Math.abs(lakebaseMs - competitorMs),
      }
    }
  }
  return null
}

function sessionEvidence(session: DemoSession) {
  const roundFive = session.round.id === 'survive_connection_spike'
  // Rounds 4 and 6 score each lane's own clock, bell to its exact read: a lane is only
  // verified when its verifier read that exact row back, so it needs no guardrail
  // here. Round 6's separate checkout is the server's to enforce, and a bout whose
  // checkout did not read back fails there rather than reaching this screen verified.
  const lakebaseExactMs = roundFive
    ? roundFiveExactPrimaryMs(session, 'lakebase')
    : verifiedElapsed(session, 'lakebase')
  const competitorExactMs = roundFive
    ? roundFiveExactPrimaryMs(session, 'competitor')
    : verifiedElapsed(session, 'competitor')
  const roundFiveGuardrailFailed = roundFive
    && lakebaseExactMs !== null
    && competitorExactMs !== null
    && !roundFiveHasComparison(session)
  const capabilityGap = lakebaseExactMs !== null
    && competitorExactMs === null
    && session.lanes.competitor.state === 'not_supported'

  return classifyEvidence({
    lakebase: {
      exactMs: lakebaseExactMs,
      lowerBoundMs: towelLowerBoundMs(session, 'lakebase'),
      notSupported: session.lanes.lakebase.state === 'not_supported',
    },
    competitor: {
      exactMs: competitorExactMs,
      lowerBoundMs: towelLowerBoundMs(session, 'competitor'),
      notSupported: session.lanes.competitor.state === 'not_supported',
    },
    capabilityGap,
    guardrailFailure: roundFiveGuardrailFailed,
    cleanupFailure: Boolean(
      session.towel?.cleanup_failure
      || session.round5_setup?.cleanup_failure
      || session.cooldown?.state === 'failed'
      || session.cooldown?.failure,
    ),
  })
}

function sessionContractVerified(session: DemoSession): boolean {
  switch (session.round.id) {
    case 'wake_idle_app':
      return session.lanes.competitor.state === 'not_supported'
        ? !scalesToZero(session.competitor.id)
        : comparisonFor(session) !== null
    case 'make_schema_change_safely':
    case 'recover_deleted_order':
      return comparisonFor(session) !== null
    case 'put_model_score_in_app':
    case 'analyze_live_orders_without_slowing_checkout':
      // The verdict is the server's, from the engine's resolution; without the AWS
      // lane installed it can only be Lakebase's capability.
      return session.lanes.competitor.state === 'not_supported'
        ? session.comparison?.kind === 'capability_gap'
          && session.comparison.winner_lane_id === 'lakebase'
        : comparisonFor(session) !== null
    case 'survive_connection_spike':
      return roundFiveHasComparison(session)
  }
}

function outcomeIdFor(
  session: DemoSession,
  decision: RoundContractDecision,
): RingsideOutcomeId {
  const { evidence } = decision
  const oneExact = evidence.exactLane !== null

  if (decision.status === 'cleanup_failure') return 'cleanup_failed'

  switch (session.round.id) {
    case 'wake_idle_app':
      // Any opponent with no connection-triggered wake, not RDS by name: the Azure
      // server is checked before the bell the same way, and reading its bout as
      // "did not verify" would blame a lane that was never asked to run.
      return evidence.shape === 'capability_gap' && !scalesToZero(session.competitor.id)
        ? 'verified_rds_capability_gap'
        : evidence.laneShape === 'both_exact_verified'
          ? 'verified_comparison'
          : oneExact
            ? 'one_sided_verified'
            : 'no_result'
    case 'make_schema_change_safely':
      return evidence.laneShape === 'both_exact_verified'
        ? 'verified_comparison'
        : oneExact
          ? 'one_sided_verified'
          : 'no_result'
    case 'recover_deleted_order':
      return session.towel && evidence.laneShape === 'exact_and_censored_lower_bound'
        ? 'one_sided_towel_lower_bound'
        : session.towel && !oneExact
          ? 'towel_no_verified_lane'
          : evidence.laneShape === 'both_exact_verified'
            ? 'verified_comparison'
            : oneExact
              ? 'one_sided_verified'
              : 'no_result'
    case 'put_model_score_in_app':
    case 'analyze_live_orders_without_slowing_checkout':
      // Either lane can be the one that read its row, so the one-sided proof names
      // the lanes rather than assuming Lakebase finished.
      return decision.status === 'declared_capability'
        ? 'verified_capability_gap'
        : evidence.laneShape === 'both_exact_verified'
          ? 'verified_comparison'
          : oneExact
            ? 'one_sided_verified'
            : session.towel && evidence.laneShape === 'both_lower_bounds'
              ? 'towel_no_verified_lane'
              : 'no_result'
    case 'survive_connection_spike':
      return decision.status === 'declared_comparison'
          ? 'verified_comparison'
          : oneExact
            ? 'one_sided_setup_verified_towel'
            : evidence.laneShape === 'both_exact_verified'
              ? 'bounded_check_failed'
              : session.round5_setup
                ? 'setup_incomplete'
                : 'no_result'
  }
}

function winnerName(session: DemoSession, winner: FormalWinner): string | null {
  if (winner === 'tie') return 'TIE'
  if (winner === 'lakebase' || winner === 'competitor') {
    const setupName = session.round5_setup?.lanes?.[winner]?.name
    return (setupName || session.lanes[winner].name).toUpperCase()
  }
  return null
}

/** The bout's outcome as the opening sentence of a spoken line, not a verdict band. */
function resultSentence(session: DemoSession, winner: FormalWinner): string {
  if (winner === 'tie') return 'The bout ended in a tie.'
  if (winner === 'lakebase' || winner === 'competitor') {
    const name = session.round5_setup?.lanes?.[winner]?.name || session.lanes[winner].name
    return `${name} won the bout.`
  }
  return 'The bout declared no winner.'
}

/** The cloud the competitor lane belongs to, for a line about a lane that is not installed. */
function cloudName(session: DemoSession): string {
  return byCompetitor(session.competitor.id, { aurora: 'AWS', rds: 'AWS', azure: 'Azure' })
}

/**
 * A lane's name as the room should hear it. Rounds 4 and 6 differ by the integration
 * that moved the row, so their lanes are named for the stack, not the database.
 */
function laneDisplayName(session: DemoSession, laneId: LaneId): string {
  if (session.round.id === 'put_model_score_in_app') return roundFourStackLabel(session, laneId)
  if (session.round.id === 'analyze_live_orders_without_slowing_checkout') {
    return roundSixStackLabel(session, laneId)
  }
  return session.round5_setup?.lanes?.[laneId]?.name || session.lanes[laneId].name
}

function oneSidedRoundFiveHeadline(
  session: DemoSession,
  decision: RoundContractDecision,
): string {
  const exactLane = decision.evidence.exactLane
  if (!exactLane) {
    return 'NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A'
  }
  const unfinishedLane: LaneId = exactLane === 'lakebase' ? 'competitor' : 'lakebase'
  const exactName = winnerName(session, exactLane) ?? exactLane.toUpperCase()
  const unfinishedName = (
    session.round5_setup?.lanes?.[unfinishedLane]?.name
    || session.lanes[unfinishedLane].name
  ).toUpperCase()
  const exactMs = decision.evidence[exactLane].exactMs
  const lowerBoundMs = decision.evidence[unfinishedLane].lowerBoundMs
  const unfinished = lowerBoundMs === null
    ? `${unfinishedName} NOT VERIFIED · NO EXACT LOWER BOUND`
    : `${unfinishedName} UNVERIFIED BEYOND ${required(seconds(lowerBoundMs), 'lower bound')}`
  return (
    `${exactName} SETUP VERIFIED ${required(seconds(exactMs), 'exact setup time')} · ${unfinished} · `
    + 'BOUNDED CHECK NOT RUN · NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A'
  )
}

function outcomeHeadline(
  session: DemoSession,
  decision: RoundContractDecision,
): string {
  if (
    session.round.id === 'survive_connection_spike'
    && decision.evidence.exactLane
    && !decision.contractComplete
  ) {
    return oneSidedRoundFiveHeadline(session, decision)
  }
  if (decision.status === 'cleanup_failure') {
    const winner = winnerName(session, decision.formalWinner)
    return winner
      ? `${winner} RESULT RETAINED · CLEANUP FAILED · SHARING BLOCKED · SAME ROUND FENCED`
      : 'NO DECLARED WINNER · CLEANUP FAILED · SHARING BLOCKED · SAME ROUND FENCED'
  }
  if (decision.status === 'comparison_incomplete') {
    return decision.evidence.laneShape === 'both_exact_verified'
      ? 'EXACT PROOFS VERIFIED · FORMAL COMPARISON INCOMPLETE · NO DECLARED WINNER · MARGIN N/A'
      : 'NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A'
  }
  if (decision.status === 'guardrail_failure') {
    return 'PRIMARY PROOF OBSERVED · REQUIRED GUARDRAIL NOT VERIFIED · NO DECLARED WINNER · MARGIN N/A'
  }
  if (decision.status === 'no_verified_evidence') {
    return 'NO EXACT VERIFIED RESULT · NO DECLARED WINNER · MARGIN N/A'
  }
  if (
    session.round.id === 'survive_connection_spike'
    && decision.status === 'declared_comparison'
  ) {
    const winner = winnerName(session, decision.formalWinner)
    // Under fan-in the verdict names what was reached, not that a path verified:
    // both paths verified, and the result is which one got 10,000 clients there
    // first. "VERIFIED A POOLED PATH" was the bounded round's verdict, where
    // finishing setup was the whole achievement.
    const fanIn = roundFiveUsesFanIn(session)
    if (winner === 'TIE') {
      return fanIn
        ? 'BOTH PATHS REACHED 10,000 TOGETHER'
        : 'BOTH POOLED PATHS VERIFIED TOGETHER'
    }
    const verb = fanIn ? 'REACHED 10,000' : 'VERIFIED A POOLED PATH'
    return winner
      ? `${winner} ${verb} · ${decision.marginMs === null ? 'MARGIN N/A' : `${seconds(decision.marginMs)} SOONER`}`
      : 'NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A'
  }
  if (decision.status === 'adjudicated_stoppage') {
    const winner = winnerName(session, decision.formalWinner)
    if (session.towel && decision.evidence.exactLane) {
      // The lane that finished wins over one the towel stopped (Ryan, 2026-10-03: "Lakebase
      // should be deemed the winner!"). Each lane's clock is on screen above this line: the
      // finished lane's exact time and the other's floor.
      const exactLane = decision.evidence.exactLane
      const otherLane: LaneId = exactLane === 'lakebase' ? 'competitor' : 'lakebase'
      const cutoffMs = decision.evidence[otherLane].lowerBoundMs ?? towelCutoffMs(session)
      return (
        `${winner ?? exactLane.toUpperCase()} WINS · `
        + `TOWEL THROWN AT ${required(seconds(cutoffMs), 'towel cutoff')} · MARGIN IS A LOWER BOUND`
      )
    }
    if (session.remembered_result?.trim()) return session.remembered_result
    return winner
      ? `${winner} EXACT PROOF PRESERVED · ADJUDICATED STOPPAGE · MARGIN N/A`
      : 'NO DECLARED WINNER · MARGIN N/A'
  }
  if (decision.status === 'declared_capability') {
    const elapsed = required(
      seconds(decision.evidence.lakebase.exactMs),
      'capability elapsed time',
    )
    if (
      session.round.id === 'put_model_score_in_app'
      || session.round.id === 'analyze_live_orders_without_slowing_checkout'
    ) {
      return session.remembered_result?.trim()
        || `LAKEBASE ${elapsed} · AWS LANE NOT INSTALLED`
    }
  }
  if (session.remembered_result?.trim()) return session.remembered_result

  const winner = winnerName(session, decision.formalWinner)
  if (decision.status === 'declared_capability' && winner) {
    return `${winner} CAPABILITY WIN · OPPONENT NOT SUPPORTED · MARGIN N/A`
  }
  if (winner === 'TIE') return 'TIE · EXACT PROOFS VERIFIED'
  return winner
    ? `${winner} WINS · MARGIN ${decision.marginMs === null ? 'N/A' : seconds(decision.marginMs)}`
    : 'NO DECLARED WINNER · MARGIN N/A'
}

export interface ClassifiedSessionOutcome extends RoundContractDecision {
  outcome: ResultLineRecord
  headline: string
}

export function classifyOutcome(session: DemoSession): ClassifiedSessionOutcome {
  const evidence = sessionEvidence(session)
  const decision = resolveRoundContract({
    roundId: session.round.id,
    evidence,
    comparison: comparisonFor(session),
    roundContractVerified: sessionContractVerified(session),
    terminal:
      session.state === 'verified'
      || session.state === 'failed'
      || session.state === 'towelled',
    recordNoEvidence: Boolean(session.towel),
  })
  const outcome = getResultLine(
    session.round.id,
    outcomeIdFor(session, decision),
    session.competitor.id,
  )
  return {
    ...decision,
    outcome,
    headline: outcomeHeadline(session, decision),
  }
}

export function classifyRingsideOutcome(session: DemoSession): ResultLineRecord {
  return classifyOutcome(session).outcome
}

function seconds(value: number | null): string | null {
  return value === null ? null : `${(value / 1000).toFixed(2)}s`
}

function towelCutoffMs(session: DemoSession): number | null {
  return nonNegativeNumber(session.towel?.cutoff_ms ?? session.towel?.lower_bound_ms)
}

function towelLowerBoundMs(session: DemoSession, laneId: LaneId): number | null {
  const explicit = nonNegativeNumber(session.towel?.censored_lower_bounds_ms?.[laneId])
  if (explicit !== null) return explicit
  if (session.towel?.active_lane === laneId) return towelCutoffMs(session)
  return nonNegativeNumber(session.lanes[laneId].evidence?.lower_bound_ms)
}

function required(value: string | null, token: string): string {
  if (value === null || value.trim().length === 0) {
    throw new Error(`Ringside proof is missing ${token}.`)
  }
  return value
}

function oneSidedValues(session: DemoSession): Record<string, string> {
  const lakebaseMs = verifiedElapsed(session, 'lakebase')
  const verifiedLane: LaneId = lakebaseMs !== null ? 'lakebase' : 'competitor'
  const unverifiedLane: LaneId = verifiedLane === 'lakebase' ? 'competitor' : 'lakebase'
  return {
    VERIFIED_LANE: laneDisplayName(session, verifiedLane),
    VERIFIED_SECONDS: required(seconds(verifiedElapsed(session, verifiedLane)), 'VERIFIED_SECONDS'),
    UNVERIFIED_LANE: laneDisplayName(session, unverifiedLane),
  }
}

function setupStatus(session: DemoSession, laneId: LaneId): string {
  const result = roundFiveSetupLaneResult(session, laneId)
  if (result.verified) return `ready in ${required(seconds(result.setupElapsedMs), `${laneId} setup time`)}`
  const status = session.round5_setup?.lanes?.[laneId]?.status?.trim()
  return status || 'never ready'
}

function proofValues(
  session: DemoSession,
  classified: ClassifiedSessionOutcome,
): Record<string, string> {
  const { outcome } = classified
  const competitorName = laneDisplayName(session, 'competitor')
  switch (outcome.outcome_id) {
    case 'verified_comparison':
      if (session.round.id === 'survive_connection_spike') {
        // The scored quantity is each lane's time to 10,000 held clients. Under the
        // bell protocol that clock starts at the bell, so the other side's setup is
        // inside it and is told as the part that went to building its pooler. Under
        // the retired fan-in protocols the clock started at a shared T0 after both
        // setups, so setup is told as what came first and is not in those times.
        // Saying "of which" on a legacy bout would attribute 24 seconds of setup to
        // a 4-second clock.
        const bell = roundFiveBellRuntime(session) !== null
        const lakebaseSetup = required(
          seconds(roundFiveSetupLaneResult(session, 'lakebase').setupElapsedMs),
          'LAKEBASE_SETUP_SECONDS',
        )
        const competitorSetup = required(
          seconds(roundFiveSetupLaneResult(session, 'competitor').setupElapsedMs),
          'PROXY_SETUP_SECONDS',
        )
        const cloud = cloudName(session)
        const [building, build] = byCompetitor(session.competitor.id, {
          aurora: ['building an RDS Proxy', 'build an RDS Proxy'],
          rds: ['building an RDS Proxy', 'build an RDS Proxy'],
          azure: [
            'switching on its built-in PgBouncer and raising its client limit',
            'switch on its built-in PgBouncer and raise its client limit',
          ],
        })
        return {
          COMPETITOR_NAME: competitorName,
          LAKEBASE_TO_10K_SECONDS: required(
            seconds(roundFiveExactPrimaryMs(session, 'lakebase')),
            'LAKEBASE_TO_10K_SECONDS',
          ),
          COMPETITOR_TO_10K_SECONDS: required(
            seconds(roundFiveExactPrimaryMs(session, 'competitor')),
            'COMPETITOR_TO_10K_SECONDS',
          ),
          LAKEBASE_SETUP_SECONDS: lakebaseSetup,
          PROXY_SETUP_SECONDS: competitorSetup,
          TIMING_BASIS: bell
            ? 'timed from one bell'
            : 'timed from a shared start once both paths were ready',
          SETUP_SENTENCE: bell
            ? `Of the ${cloud} path's time, ${competitorSetup} went to ${building} before its first client could connect.`
            : `Getting ready came first and is not in those times: Lakebase's pooled path was ready in ${lakebaseSetup}; the ${cloud} path needed ${competitorSetup} to ${build} before its first client could connect.`,
        }
      }
      return {
        LAKEBASE_SECONDS: required(seconds(verifiedElapsed(session, 'lakebase')), 'LAKEBASE_SECONDS'),
        COMPETITOR_NAME: competitorName,
        COMPETITOR_SECONDS: required(seconds(verifiedElapsed(session, 'competitor')), 'COMPETITOR_SECONDS'),
      }
    case 'verified_capability_gap':
      if (session.round.id === 'put_model_score_in_app') {
        const evidence = modelScoreEvidence(session.lanes.lakebase)
        return {
          LAKEBASE_SECONDS: required(seconds(verifiedElapsed(session, 'lakebase')), 'LAKEBASE_SECONDS'),
          PRIMARY_KEY: required(evidence.primaryKey === '—' ? null : evidence.primaryKey, 'PRIMARY_KEY'),
          SCORE: required(evidence.score === '—' ? null : evidence.score, 'SCORE'),
          CLOUD: cloudName(session),
        }
      }
      return {
        LAKEBASE_SECONDS: required(seconds(verifiedElapsed(session, 'lakebase')), 'LAKEBASE_SECONDS'),
        CLOUD: cloudName(session),
      }
    case 'verified_rds_capability_gap':
      return {
        LAKEBASE_SECONDS: required(seconds(verifiedElapsed(session, 'lakebase')), 'LAKEBASE_SECONDS'),
        COMPETITOR_NAME: competitorName,
      }
    case 'one_sided_verified':
      return oneSidedValues(session)
    case 'one_sided_towel_lower_bound':
      return {
        LAKEBASE_SECONDS: required(seconds(verifiedElapsed(session, 'lakebase')), 'LAKEBASE_SECONDS'),
        COMPETITOR_NAME: competitorName,
        CUTOFF_SECONDS: required(seconds(towelCutoffMs(session)), 'CUTOFF_SECONDS'),
        LOWER_BOUND_SECONDS: required(seconds(towelLowerBoundMs(session, 'competitor')), 'LOWER_BOUND_SECONDS'),
      }
    case 'one_sided_setup_verified_towel':
      {
        const verifiedLane = classified.evidence.exactLane
        if (!verifiedLane) throw new Error('Round 5 one-sided proof has no exact setup lane.')
        const unverifiedLane: LaneId = verifiedLane === 'lakebase' ? 'competitor' : 'lakebase'
        const verifiedName = laneDisplayName(session, verifiedLane)
        const unverifiedName = laneDisplayName(session, unverifiedLane)
        const lowerBoundMs = classified.evidence[unverifiedLane].lowerBoundMs
        const unverifiedResult = lowerBoundMs === null
          ? `${unverifiedName} never finished setting up before the stop`
          : `${unverifiedName} was still setting up at ${required(seconds(lowerBoundMs), 'UNVERIFIED_SETUP_RESULT')}, so its time is more than that`
        return {
          VERIFIED_SETUP_LANE: verifiedName,
          VERIFIED_SETUP_SECONDS: required(
            seconds(classified.evidence[verifiedLane].exactMs),
            'VERIFIED_SETUP_SECONDS',
          ),
          UNVERIFIED_SETUP_LANE: unverifiedName,
          UNVERIFIED_SETUP_RESULT: unverifiedResult,
        }
      }
    case 'setup_incomplete':
      return {
        COMPETITOR_NAME: competitorName,
        LAKEBASE_SETUP_STATUS: setupStatus(session, 'lakebase'),
        PROXY_SETUP_STATUS: setupStatus(session, 'competitor'),
      }
    case 'bounded_check_failed': {
      const failed = (['lakebase', 'competitor'] as const)
        .filter((laneId) => !roundFiveLaneResult(session.lanes[laneId]).contractVerified)
        .map((laneId) => laneDisplayName(session, laneId))
      return {
        FAILED_GATE: failed.length === 0
          ? 'a shared fairness or telemetry check failed'
          : failed.length === 1
            ? `${failed[0]} failed a required check`
            : `${failed.join(' and ')} both failed a required check`,
      }
    }
    case 'cleanup_failed':
      return {
        ROUND_RESULT: resultSentence(session, classified.formalWinner),
        CLEANUP_GATE: required(
          session.round5_setup?.cleanup_failure?.trim()
            || session.towel?.cleanup_failure?.trim()
            || session.cooldown?.failure?.trim()
            || 'clean baseline verification',
          'CLEANUP_GATE',
        ),
      }
    case 'towel_no_verified_lane':
      return {
        CUTOFF_SECONDS: required(seconds(towelCutoffMs(session)), 'CUTOFF_SECONDS'),
        LAKEBASE_LOWER_BOUND_SECONDS: required(
          seconds(towelLowerBoundMs(session, 'lakebase')),
          'LAKEBASE_LOWER_BOUND_SECONDS',
        ),
        COMPETITOR_NAME: competitorName,
        COMPETITOR_LOWER_BOUND_SECONDS: required(
          seconds(towelLowerBoundMs(session, 'competitor')),
          'COMPETITOR_LOWER_BOUND_SECONDS',
        ),
      }
    case 'no_result':
      return {}
  }
}

export function interpolateProof(
  template: string,
  values: Readonly<Record<string, string>>,
): string {
  const rendered = template.replace(/<([A-Z0-9_]+)>/g, (_token, name: string) => (
    required(values[name] ?? null, name)
  ))
  if (/<[A-Z0-9_]+>/.test(rendered)) {
    throw new Error('Ringside proof contains an unresolved placeholder.')
  }
  return rendered
}

/**
 * The briefing behind "Explain to the room" for one person in it.
 *
 * The result line is this bout's: it comes from the outcome the classifier chose
 * and carries the bout's numbers and boundary. The role line and the lens lines
 * are the round's: they say why the round matters to this role and what it looks
 * like through each priority the room picked, and they claim nothing about who
 * won, so a stopped or failed bout reads them without inheriting a result.
 */
export function buildRingsideBriefing(
  session: DemoSession,
  personaId: PersonaId,
  corners: readonly CustomerCorner[],
): RingsideBriefing {
  const classified = classifyOutcome(session)
  const role = getRoleLine(session.round.id, personaId, session.competitor.id)
  const lenses: RingsideLens[] = orderedPriorities(corners).map((priority) => ({
    priority,
    text: getLensLine(session.round.id, priority, session.competitor.id).text,
  }))
  return {
    result: resultLine(session, classified),
    why: role.why,
    lenses,
    ask: role.ask,
    outcomeId: classified.outcome.outcome_id,
    status: classified.status,
    declared: classified.status === 'declared_comparison'
      || classified.status === 'declared_capability'
      || classified.status === 'adjudicated_stoppage',
  }
}

function resultLine(session: DemoSession, classified: ClassifiedSessionOutcome): string {
  return roundFiveBellIncompleteResult(session, classified)
    ?? interpolateProof(classified.outcome.text, proofValues(session, classified))
}

/**
 * What happened in a V4 bell Round 5 bout that did not complete its contract
 * (towelled or failed). It replaces the authored `setup_incomplete` / one-sided
 * lines, which say the 10,000-client test never ran -- false when the runtime
 * proves a lane reached 10,000. Reads the same canonical lane view-model as every
 * other surface. Returns null for verified bouts and every non-bell session so
 * their authored line is used.
 */
function roundFiveBellIncompleteResult(
  session: DemoSession,
  classified: RoundContractDecision,
): string | null {
  if (session.round.id !== 'survive_connection_spike') return null
  if (!roundFiveBellRuntime(session)) return null
  if (classified.contractComplete) return null

  const clause = (laneId: LaneId): string => {
    const name = laneDisplayName(session, laneId)
    const presentation = roundFiveLanePresentation(session, laneId)
    if (!presentation) return `${name} produced no evidence`
    switch (presentation.semantic) {
      case 'verified':
        return `${name} verified exactly 10,000 held clients at ${presentation.value}`
      case 'reached_hold_interrupted':
        return `${name} reached 10,000 clients at ${presentation.value}, but the 30-second hold was interrupted by the towel before it verified`
      case 'reached_hold_failed':
        return `${name} reached 10,000 clients at ${presentation.value}, but the hold did not verify`
      case 'reached_hold_in_progress':
        return `${name} reached 10,000 clients at ${presentation.value} with the hold still running`
      case 'not_reached_lower_bound':
        return `${name} never reached 10,000 clients (still climbing when stopped, ${presentation.value})`
      case 'not_supported':
        return `${name} was not supported`
      default:
        return `${name} never reached 10,000 clients`
    }
  }

  return (
    `${clause('lakebase')}. ${clause('competitor')}. `
    + 'The bout stopped before both lanes verified, so there is no winner and no margin.'
  )
}

/** The result line alone, for surfaces that show what happened without a role. */
export function buildRingsideResult(session: DemoSession): string {
  return resultLine(session, classifyOutcome(session))
}
