import type { CustomerCorner, DemoSession, LaneId, PersonaId } from '../api/types'
import { metricValue, modelScoreEvidence } from '../round4'
import {
  roundFiveHasComparison,
  roundFiveLaneResult,
  roundFiveSetupLaneResult,
} from '../round5'
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

function roundFiveOutcome(session: DemoSession): RingsideOutcomeId {
  const setup = session.round5_setup
  if (!setup) return 'no_result'
  if (setup.state === 'cleanup_failed' || setup.cleanup_failure) return 'cleanup_failed'
  if (session.towel) return 'no_result'

  const bothSetupsVerified = roundFiveSetupLaneResult(session, 'lakebase').verified
    && roundFiveSetupLaneResult(session, 'competitor').verified
    && setup.setup_validated === true
  if (!bothSetupsVerified) return 'setup_incomplete'
  if (!roundFiveHasComparison(session)) return 'spike_contract_failed'
  return 'verified_comparison'
}

export function classifyRingsideOutcome(session: DemoSession): OutcomeCopyRecord {
  let outcomeId: RingsideOutcomeId
  const lakebaseMs = verifiedElapsed(session, 'lakebase')
  const competitorMs = verifiedElapsed(session, 'competitor')

  switch (session.round.id) {
    case 'wake_idle_app':
      outcomeId = lakebaseMs !== null
        && session.competitor.id === 'rds_postgres'
        && session.lanes.competitor.state === 'not_supported'
        ? 'verified_rds_capability_gap'
        : lakebaseMs !== null && competitorMs !== null
          ? 'verified_comparison'
          : lakebaseMs !== null || competitorMs !== null
            ? 'one_sided_verified'
            : 'no_result'
      break
    case 'make_schema_change_safely':
      outcomeId = lakebaseMs !== null && competitorMs !== null
        ? 'verified_comparison'
        : lakebaseMs !== null || competitorMs !== null
          ? 'one_sided_verified'
          : 'no_result'
      break
    case 'recover_deleted_order':
      outcomeId = session.towel && lakebaseMs !== null && competitorMs === null
        ? 'one_sided_towel_lower_bound'
        : session.towel && lakebaseMs === null && competitorMs === null
          ? 'towel_no_verified_lane'
          : lakebaseMs !== null && competitorMs !== null
            ? 'verified_comparison'
            : lakebaseMs !== null || competitorMs !== null
              ? 'one_sided_verified'
              : 'no_result'
      break
    case 'put_model_score_in_app': {
      const appReadCompleted = session.lanes.lakebase.state === 'verified'
        && roundFourElapsed(session) !== null
      outcomeId = !appReadCompleted
        ? 'no_result'
        : modelScoreEvidence(session.lanes.lakebase).exactRowVerified
          ? 'verified_capability_gap'
          : 'score_identity_unverified'
      break
    }
    case 'survive_connection_spike':
      outcomeId = roundFiveOutcome(session)
      break
    case 'analyze_live_orders_without_slowing_checkout': {
      const deltaAnswerArrived = session.lanes.lakebase.state === 'verified'
        && roundSixElapsed(session) !== null
      outcomeId = !deltaAnswerArrived
        ? 'no_result'
        : metricValue(session, 'checkout_verified')?.value === true
          ? 'verified_capability_gap'
          : 'checkout_guardrail_unverified'
      break
    }
  }
  return getOutcomeRecord(session.round.id, outcomeId)
}

export function isShareableRingsideOutcome(outcome: OutcomeCopyRecord): boolean {
  return outcome.copy_mode === 'INHERIT_VERIFIED_CORPUS'
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
  outcome: OutcomeCopyRecord,
): Record<string, string> {
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
      return { FAILED_LANES: required(failed.join(' and '), 'FAILED_LANES') }
    }
    case 'cleanup_failed':
      return {
        CLEANUP_GATE: required(
          session.round5_setup?.cleanup_failure?.trim() || 'clean baseline verification',
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
  const outcome = classifyRingsideOutcome(session)
  const verified = outcome.copy_mode === 'INHERIT_VERIFIED_CORPUS'
    ? getVerifiedRecord(session.round.id, personaId, priorityKey)
    : null
  const sayRecord = verified
    ? authoredText(verified.meaning_record_id, verified.meaning, verified.meaning_decision)
    : authoredText(
        required(outcome.meaning_record_id, 'meaning_record_id'),
        required(outcome.meaning, 'meaning'),
        required(outcome.meaning_decision, 'meaning_decision') as 'KEEP' | 'REWRITE',
      )
  const askRecord = verified
    ? authoredText(verified.question_record_id, verified.question, verified.question_decision)
    : authoredText(
        required(outcome.question_record_id, 'question_record_id'),
        required(outcome.question, 'question'),
        required(outcome.question_decision, 'question_decision') as 'KEEP' | 'REWRITE',
      )
  const proof = interpolateProof(outcome.proof_template, proofValues(session, outcome))
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
  const outcome = classifyRingsideOutcome(session)
  return interpolateProof(outcome.proof_template, proofValues(session, outcome))
}
