import type { CustomerCorner, DemoSession, LaneId, PersonaId } from '../api/types'
import { metricValue, modelScoreEvidence } from '../round4'
import {
  roundFiveHasComparison,
  roundFiveLaneResult,
  roundFiveSetupLaneResult,
} from '../round5'
import {
  classifyEvidence,
  resolveRoundContract,
  type ContractComparison,
  type FormalWinner,
  type RoundContractDecision,
} from '../outcome'
import {
  getOutcomeRecord,
  getVerifiedRecord,
} from './corpus'
import type {
  AuthoredText,
  OutcomeCopyRecord,
  PriorityKey,
  RingsideCue,
  RingsideOutcomeId,
} from './types'
import { PRIORITY_KEYS } from './types'

const PRIORITY_ORDER: CustomerCorner[] = ['cost', 'simplicity', 'performance']

export function priorityKeyFor(corners: readonly CustomerCorner[]): PriorityKey {
  const selected = new Set(corners)
  const key = PRIORITY_ORDER.filter((corner) => selected.has(corner)).join('+')
  if (!PRIORITY_KEYS.includes(key as PriorityKey)) {
    throw new Error(`Ringside cue requires one to three priorities; received "${key || 'none'}".`)
  }
  return key as PriorityKey
}

function nonNegativeNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

function verifiedElapsed(session: DemoSession, laneId: LaneId): number | null {
  const lane = session.lanes[laneId]
  return lane.state === 'verified' ? nonNegativeNumber(lane.elapsed_ms) : null
}

function roundMetricMs(session: DemoSession, specId: string): number | null {
  const value = metricValue(session, specId)?.value
  return nonNegativeNumber(typeof value === 'string' ? Number(value) : value)
}

function roundFourElapsed(session: DemoSession): number | null {
  return roundMetricMs(session, 'application_proof_elapsed_ms')
    ?? verifiedElapsed(session, 'lakebase')
}

function roundSixElapsed(session: DemoSession): number | null {
  return roundMetricMs(session, 'analytics_available_ms')
    ?? verifiedElapsed(session, 'lakebase')
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
  const lakebaseExactMs = roundFive
    ? roundFiveExactSetupMs(session, 'lakebase')
    : session.round.id === 'put_model_score_in_app'
      ? verifiedElapsed(session, 'lakebase') === null ? null : roundFourElapsed(session)
      : session.round.id === 'analyze_live_orders_without_slowing_checkout'
        ? verifiedElapsed(session, 'lakebase') === null ? null : roundSixElapsed(session)
        : verifiedElapsed(session, 'lakebase')
  const competitorExactMs = roundFive
    ? roundFiveExactSetupMs(session, 'competitor')
    : verifiedElapsed(session, 'competitor')
  const roundFourGuardrailFailed = session.round.id === 'put_model_score_in_app'
    && lakebaseExactMs !== null
    && !modelScoreEvidence(session.lanes.lakebase).exactRowVerified
  const roundSixGuardrailFailed = session.round.id === 'analyze_live_orders_without_slowing_checkout'
    && lakebaseExactMs !== null
    && metricValue(session, 'checkout_verified')?.value !== true
  const roundFiveGuardrailFailed = roundFive
    && lakebaseExactMs !== null
    && competitorExactMs !== null
    && !roundFiveHasComparison(session)
  const capabilityGap = lakebaseExactMs !== null
    && competitorExactMs === null
    && session.lanes.competitor.state === 'not_supported'
    && !roundFourGuardrailFailed
    && !roundSixGuardrailFailed

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
    guardrailFailure:
      roundFourGuardrailFailed || roundFiveGuardrailFailed || roundSixGuardrailFailed,
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
        ? session.competitor.id === 'rds_postgres'
        : comparisonFor(session) !== null
    case 'make_schema_change_safely':
    case 'recover_deleted_order':
      return comparisonFor(session) !== null
    case 'put_model_score_in_app':
      return modelScoreEvidence(session.lanes.lakebase).exactRowVerified
        && session.lanes.competitor.state === 'not_supported'
    case 'survive_connection_spike':
      return roundFiveHasComparison(session)
    case 'analyze_live_orders_without_slowing_checkout':
      return metricValue(session, 'checkout_verified')?.value === true
        && session.lanes.competitor.state === 'not_supported'
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
      return evidence.shape === 'capability_gap' && session.competitor.id === 'rds_postgres'
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
      return decision.status === 'declared_capability'
        ? 'verified_capability_gap'
        : evidence.exactLane === 'lakebase'
          ? 'score_identity_unverified'
          : 'no_result'
    case 'survive_connection_spike':
      return decision.status === 'declared_comparison'
          ? 'verified_comparison'
          : oneExact
            ? 'one_sided_setup_verified_towel'
            : evidence.laneShape === 'both_exact_verified'
              ? 'spike_contract_failed'
              : session.round5_setup
                ? 'setup_incomplete'
                : 'no_result'
    case 'analyze_live_orders_without_slowing_checkout':
      return decision.status === 'declared_capability'
        ? 'verified_capability_gap'
        : evidence.exactLane === 'lakebase'
          ? 'checkout_guardrail_unverified'
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
    + 'SHARED SPIKE NOT RUN · NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A'
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
    if (winner === 'TIE') return 'BOTH POOLED PATHS VERIFIED TOGETHER'
    return winner
      ? `${winner} VERIFIED A POOLED PATH · ${decision.marginMs === null ? 'MARGIN N/A' : `${seconds(decision.marginMs)} SOONER`}`
      : 'NO DECLARED WINNER · COMPARISON INCOMPLETE · MARGIN N/A'
  }
  if (decision.status === 'adjudicated_stoppage') {
    const winner = winnerName(session, decision.formalWinner)
    if (session.towel && decision.evidence.exactLane) {
      const exactLane = decision.evidence.exactLane
      const otherLane: LaneId = exactLane === 'lakebase' ? 'competitor' : 'lakebase'
      const exactMs = required(seconds(decision.evidence[exactLane].exactMs), 'exact time')
      const cutoffMs = decision.evidence[otherLane].lowerBoundMs ?? towelCutoffMs(session)
      const otherName = session.lanes[otherLane].name.toUpperCase()
      return (
        `TOWEL THROWN AT ${required(seconds(cutoffMs), 'towel cutoff')} · `
        + `${winner ?? exactLane.toUpperCase()} VERIFIED ${exactMs} · `
        + `${otherName} UNVERIFIED WHEN STOPPED · LOWER BOUND`
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
    if (session.round.id === 'put_model_score_in_app') {
      return `ANALYTICS CHANGE → LIVE APP · ${elapsed} · AWS NOT TIMED · MARGIN N/A`
    }
    if (session.round.id === 'analyze_live_orders_without_slowing_checkout') {
      return `EXACT DELTA ANSWER · ${elapsed} · AWS PIPELINE NOT BUILT · MARGIN N/A`
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
  outcome: OutcomeCopyRecord
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
  const outcome = getOutcomeRecord(session.round.id, outcomeIdFor(session, decision))
  return {
    ...decision,
    outcome,
    headline: outcomeHeadline(session, decision),
  }
}

export function classifyRingsideOutcome(session: DemoSession): OutcomeCopyRecord {
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
    VERIFIED_LANE: session.lanes[verifiedLane].name,
    VERIFIED_SECONDS: required(seconds(verifiedElapsed(session, verifiedLane)), 'VERIFIED_SECONDS'),
    UNVERIFIED_LANE: session.lanes[unverifiedLane].name,
  }
}

function setupStatus(session: DemoSession, laneId: LaneId): string {
  const result = roundFiveSetupLaneResult(session, laneId)
  if (result.verified) return `verified in ${required(seconds(result.setupElapsedMs), `${laneId} setup time`)}`
  const status = session.round5_setup?.lanes?.[laneId]?.status?.trim()
  return status || 'not verified'
}

function proofValues(
  session: DemoSession,
  classified: ClassifiedSessionOutcome,
): Record<string, string> {
  const { outcome } = classified
  const competitorName = session.competitor.short_name
  switch (outcome.outcome_id) {
    case 'verified_comparison':
      if (session.round.id === 'survive_connection_spike') {
        return {
          LAKEBASE_SETUP_SECONDS: required(
            seconds(roundFiveSetupLaneResult(session, 'lakebase').setupElapsedMs),
            'LAKEBASE_SETUP_SECONDS',
          ),
          PROXY_SETUP_SECONDS: required(
            seconds(roundFiveSetupLaneResult(session, 'competitor').setupElapsedMs),
            'PROXY_SETUP_SECONDS',
          ),
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
          LAKEBASE_SECONDS: required(seconds(roundFourElapsed(session)), 'LAKEBASE_SECONDS'),
          PRIMARY_KEY: required(evidence.primaryKey === '—' ? null : evidence.primaryKey, 'PRIMARY_KEY'),
          SCORE: required(evidence.score === '—' ? null : evidence.score, 'SCORE'),
        }
      }
      return {
        LAKEBASE_SECONDS: required(seconds(roundSixElapsed(session)), 'LAKEBASE_SECONDS'),
      }
    case 'verified_rds_capability_gap':
      return {
        LAKEBASE_SECONDS: required(seconds(verifiedElapsed(session, 'lakebase')), 'LAKEBASE_SECONDS'),
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
        const verifiedName = session.round5_setup?.lanes?.[verifiedLane]?.name
          || session.lanes[verifiedLane].name
        const unverifiedName = session.round5_setup?.lanes?.[unverifiedLane]?.name
          || session.lanes[unverifiedLane].name
        const lowerBoundMs = classified.evidence[unverifiedLane].lowerBoundMs
        const unverifiedResult = lowerBoundMs === null
          ? `${unverifiedName} did not verify before the stop; no exact lower bound was available`
          : `${unverifiedName} exceeded ${required(seconds(lowerBoundMs), 'UNVERIFIED_SETUP_RESULT')} without verification`
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
    case 'score_identity_unverified':
      return {
        LAKEBASE_SECONDS: required(seconds(roundFourElapsed(session)), 'LAKEBASE_SECONDS'),
      }
    case 'setup_incomplete':
      return {
        LAKEBASE_SETUP_STATUS: setupStatus(session, 'lakebase'),
        PROXY_SETUP_STATUS: setupStatus(session, 'competitor'),
      }
    case 'spike_contract_failed': {
      const failed = (['lakebase', 'competitor'] as const)
        .filter((laneId) => !roundFiveLaneResult(session.lanes[laneId]).contractVerified)
        .map((laneId) => session.lanes[laneId].name)
      return {
        FAILED_GATE: failed.length > 0
          ? `${failed.join(' and ')} spike proof`
          : 'the shared comparison or fairness gate',
      }
    }
    case 'cleanup_failed':
      return {
        ROUND_RESULT: classified.formalWinner === null
          ? 'No completed comparison was available.'
          : `${winnerName(session, classified.formalWinner)} remains the formal result.`,
        CLEANUP_GATE: required(
          session.round5_setup?.cleanup_failure?.trim()
            || session.towel?.cleanup_failure?.trim()
            || session.cooldown?.failure?.trim()
            || 'clean baseline verification',
          'CLEANUP_GATE',
        ),
      }
    case 'checkout_guardrail_unverified':
      return {
        LAKEBASE_SECONDS: required(seconds(roundSixElapsed(session)), 'LAKEBASE_SECONDS'),
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

function authoredText(id: string, text: string, decision: 'KEEP' | 'REWRITE'): AuthoredText {
  return { id, text, decision }
}

export function buildRingsideCue(
  session: DemoSession,
  personaId: PersonaId,
  priorityKey: PriorityKey,
): RingsideCue {
  const classified = classifyOutcome(session)
  const { outcome } = classified
  const values = proofValues(session, classified)
  const verified = outcome.copy_mode === 'INHERIT_VERIFIED_CORPUS'
    ? getVerifiedRecord(session.round.id, personaId, priorityKey)
    : null
  const sayRecord = verified
    ? authoredText(verified.meaning_record_id, verified.meaning, verified.meaning_decision)
    : authoredText(
        required(outcome.meaning_record_id, 'meaning_record_id'),
        interpolateProof(required(outcome.meaning, 'meaning'), values),
        required(outcome.meaning_decision, 'meaning_decision') as 'KEEP' | 'REWRITE',
      )
  const askRecord = verified
    ? authoredText(verified.question_record_id, verified.question, verified.question_decision)
    : authoredText(
        required(outcome.question_record_id, 'question_record_id'),
        required(outcome.question, 'question'),
        required(outcome.question_decision, 'question_decision') as 'KEEP' | 'REWRITE',
      )
  const proof = interpolateProof(outcome.proof_template, values)
  return {
    say: sayRecord.text,
    ask: askRecord.text,
    show: proof,
    sayRecord,
    askRecord,
    proofRecord: authoredText(outcome.proof_template_id, proof, outcome.proof_decision),
    outcome,
  }
}

export function buildRingsideShow(session: DemoSession): string {
  const classified = classifyOutcome(session)
  return interpolateProof(
    classified.outcome.proof_template,
    proofValues(session, classified),
  )
}
