import type { ComparisonKind, LaneId, RoundId } from './api/types'

export type EvidenceShape =
  | 'both_exact_verified'
  | 'lakebase_only_exact'
  | 'competitor_only_exact'
  | 'exact_and_censored_lower_bound'
  | 'both_lower_bounds'
  | 'neither_verified'
  | 'capability_gap'
  | 'guardrail_failure'
  | 'cleanup_failure'

export type LaneEvidenceShape = Exclude<
  EvidenceShape,
  'capability_gap' | 'guardrail_failure' | 'cleanup_failure'
>

export type FormalWinner = LaneId | 'tie' | null

export type ContractStatus =
  | 'declared_comparison'
  | 'declared_capability'
  | 'adjudicated_stoppage'
  | 'comparison_incomplete'
  | 'guardrail_failure'
  | 'cleanup_failure'
  | 'no_verified_evidence'

export type ResultStatus = Exclude<ContractStatus, 'cleanup_failure'>

export interface LaneEvidence {
  exactMs: number | null
  lowerBoundMs: number | null
  notSupported: boolean
}

export interface EvidenceInput {
  lakebase: LaneEvidence
  competitor: LaneEvidence
  capabilityGap?: boolean
  guardrailFailure?: boolean
  cleanupFailure?: boolean
}

export interface ClassifiedEvidence {
  shape: EvidenceShape
  laneShape: LaneEvidenceShape
  lakebase: LaneEvidence
  competitor: LaneEvidence
  exactLane: LaneId | null
  lowerBoundLane: LaneId | null
}

export interface ContractComparison {
  kind: ComparisonKind
  winnerLaneId: LaneId | null
  marginMs: number | null
}

export interface RoundContractInput {
  roundId: RoundId
  evidence: ClassifiedEvidence
  comparison: ContractComparison | null
  roundContractVerified: boolean
  terminal: boolean
  recordNoEvidence?: boolean
}

export interface RoundContractDecision {
  evidence: ClassifiedEvidence
  status: ContractStatus
  /** Proof decision before the orthogonal cleanup overlay is applied. */
  resultStatus: ResultStatus
  formalWinner: FormalWinner
  marginMs: number | null
  contractComplete: boolean
  shareable: boolean
  scorecardEligible: boolean
}

function oneExactLane(lakebaseExact: boolean, competitorExact: boolean): LaneId | null {
  if (lakebaseExact === competitorExact) return null
  return lakebaseExact ? 'lakebase' : 'competitor'
}

function oneLowerBoundLane(
  lakebaseLowerBound: boolean,
  competitorLowerBound: boolean,
): LaneId | null {
  if (lakebaseLowerBound === competitorLowerBound) return null
  return lakebaseLowerBound ? 'lakebase' : 'competitor'
}

/**
 * Classify only the evidence shape. Round semantics deliberately do not live
 * here: an exact setup stop plus a lower bound decides R3, but cannot decide R5
 * until the bounded connection check also passes.
 */
export function classifyEvidence(input: EvidenceInput): ClassifiedEvidence {
  const lakebaseExact = input.lakebase.exactMs !== null
  const competitorExact = input.competitor.exactMs !== null
  const lakebaseLowerBound = !lakebaseExact && input.lakebase.lowerBoundMs !== null
  const competitorLowerBound = !competitorExact && input.competitor.lowerBoundMs !== null

  let laneShape: LaneEvidenceShape
  if (lakebaseExact && competitorExact) {
    laneShape = 'both_exact_verified'
  } else if (
    (lakebaseExact && competitorLowerBound)
    || (competitorExact && lakebaseLowerBound)
  ) {
    laneShape = 'exact_and_censored_lower_bound'
  } else if (lakebaseExact) {
    laneShape = 'lakebase_only_exact'
  } else if (competitorExact) {
    laneShape = 'competitor_only_exact'
  } else if (lakebaseLowerBound && competitorLowerBound) {
    laneShape = 'both_lower_bounds'
  } else {
    laneShape = 'neither_verified'
  }

  const shape: EvidenceShape = input.cleanupFailure
    ? 'cleanup_failure'
    : input.guardrailFailure
      ? 'guardrail_failure'
      : input.capabilityGap
        ? 'capability_gap'
        : laneShape

  return {
    shape,
    laneShape,
    lakebase: input.lakebase,
    competitor: input.competitor,
    exactLane: oneExactLane(lakebaseExact, competitorExact),
    lowerBoundLane: oneLowerBoundLane(lakebaseLowerBound, competitorLowerBound),
  }
}

function validComparison(
  comparison: ContractComparison | null,
): comparison is ContractComparison & { winnerLaneId: LaneId | null } {
  if (!comparison) return false
  if (comparison.kind === 'tie') {
    return comparison.winnerLaneId === null && comparison.marginMs === null
  }
  return comparison.kind === 'measured'
    && (comparison.winnerLaneId === 'lakebase' || comparison.winnerLaneId === 'competitor')
    && comparison.marginMs !== null
    && Number.isFinite(comparison.marginMs)
    && comparison.marginMs >= 0
}

function comparisonResult(
  comparison: ContractComparison | null,
): Pick<RoundContractDecision, 'formalWinner' | 'marginMs'> {
  if (!validComparison(comparison)) return { formalWinner: null, marginMs: null }
  if (comparison.kind === 'tie') return { formalWinner: 'tie', marginMs: null }
  return {
    formalWinner: comparison.winnerLaneId,
    marginMs: comparison.marginMs,
  }
}

function baseDecision(
  input: RoundContractInput,
): Omit<
  RoundContractDecision,
  'evidence' | 'status' | 'resultStatus' | 'scorecardEligible'
> & { status: ResultStatus } {
  const { evidence, roundId } = input
  const comparison = comparisonResult(input.comparison)
  const hasValidComparison = comparison.formalWinner !== null

  if (evidence.shape === 'guardrail_failure') {
    return {
      status: 'guardrail_failure',
      formalWinner: null,
      marginMs: null,
      contractComplete: false,
      shareable: false,
    }
  }

  if (roundId === 'put_model_score_in_app'
    || roundId === 'analyze_live_orders_without_slowing_checkout') {
    if (
      input.roundContractVerified
      && evidence.laneShape === 'lakebase_only_exact'
      && evidence.competitor.notSupported
    ) {
      return {
        status: 'declared_capability',
        formalWinner: 'lakebase',
        marginMs: null,
        contractComplete: true,
        shareable: true,
      }
    }
    return {
      status: evidence.exactLane ? 'comparison_incomplete' : 'no_verified_evidence',
      formalWinner: null,
      marginMs: null,
      contractComplete: false,
      shareable: false,
    }
  }

  if (roundId === 'survive_connection_spike') {
    if (
      input.roundContractVerified
      && evidence.laneShape === 'both_exact_verified'
      && hasValidComparison
    ) {
      return {
        status: 'declared_comparison',
        ...comparison,
        contractComplete: true,
        shareable: true,
      }
    }
    return {
      status: evidence.exactLane || evidence.laneShape === 'both_exact_verified'
        ? 'comparison_incomplete'
        : 'no_verified_evidence',
      formalWinner: null,
      marginMs: null,
      contractComplete: false,
      shareable: false,
    }
  }

  if (
    input.roundContractVerified
    && evidence.shape === 'capability_gap'
    && evidence.exactLane === 'lakebase'
  ) {
    return {
      status: 'declared_capability',
      formalWinner: 'lakebase',
      marginMs: null,
      contractComplete: true,
      shareable: true,
    }
  }

  if (evidence.laneShape === 'both_exact_verified') {
    if (input.roundContractVerified && hasValidComparison) {
      return {
        status: 'declared_comparison',
        ...comparison,
        contractComplete: true,
        shareable: true,
      }
    }
    return {
      status: 'comparison_incomplete',
      formalWinner: null,
      marginMs: null,
      contractComplete: false,
      shareable: false,
    }
  }

  if (evidence.exactLane) {
    return {
      status: 'adjudicated_stoppage',
      formalWinner: evidence.exactLane,
      marginMs: null,
      contractComplete: true,
      shareable: true,
    }
  }

  return {
    status: 'no_verified_evidence',
    formalWinner: null,
    marginMs: null,
    contractComplete: false,
    shareable: false,
  }
}

/**
 * Apply the six small round contracts to the generic evidence shape.
 *
 * Cleanup is an operational overlay that is now fully decoupled from a SEALED
 * result. Backstage ring cleanup (Proxy deletion / rewarm) is the operator's
 * concern and converges automatically; it must never gate the end user. So a
 * completed contract (a verified/declared comparison or capability) stays
 * shareable with its real status even while cleanup is still settling. The
 * ``cleanup_failure`` status is retained ONLY when there is no completed result
 * to show anyway (a genuine no-comparison bout), where nothing was shareable in
 * the first place -- so this never hides a real result behind a cleanup notice.
 */
export function resolveRoundContract(input: RoundContractInput): RoundContractDecision {
  const underlying = baseDecision(input)
  const cleanupFailed = input.evidence.shape === 'cleanup_failure'
  // Round-5-scoped decouple: Round 5 is the only round whose backstage ring
  // cleanup (per-bout Proxy teardown / rewarm) now converges automatically and
  // must never block a SEALED, completed result. Its completed contract stays
  // shareable with its real status. Every other round keeps the existing
  // cleanup overlay (a failed cooldown cleanup fences sharing), and any round
  // with no completed result still surfaces cleanup_failure (nothing to share).
  const decoupledFromCleanup = underlying.contractComplete
    && input.roundId === 'survive_connection_spike'
  const cleanupBlocksResult = cleanupFailed && !decoupledFromCleanup
  const scorecardEligible = input.terminal && (
    input.recordNoEvidence === true
    || input.evidence.laneShape !== 'neither_verified'
    || input.evidence.shape === 'guardrail_failure'
    || cleanupFailed
  )

  return {
    evidence: input.evidence,
    ...underlying,
    status: cleanupBlocksResult ? 'cleanup_failure' : underlying.status,
    resultStatus: underlying.status,
    shareable: cleanupBlocksResult ? false : underlying.shareable,
    scorecardEligible,
  }
}
