export {
  buildRingsideBriefing,
  buildRingsideResult,
  classifyOutcome,
  classifyRingsideOutcome,
  interpolateProof,
  orderedPriorities,
} from './classifier'
export type { ClassifiedSessionOutcome } from './classifier'
export {
  COMPETITOR_SCOPES,
  LENS_LINE_RECORDS,
  LENS_LINES_SHA256,
  PERSONA_IDS,
  PRIORITIES,
  RESULT_LINE_RECORDS,
  RESULT_LINES_SHA256,
  ROLE_LINE_RECORDS,
  ROLE_LINES_SHA256,
  ROUND_IDS,
  getLensLine,
  getResultLine,
  getRoleLine,
} from './corpus'
export { OUTCOME_IDS } from './types'
export type {
  CompetitorScope,
  LensLineRecord,
  ResultLineRecord,
  RingsideBriefing,
  RingsideLens,
  RingsideOutcomeId,
  RoleLineRecord,
} from './types'
export {
  classifyEvidence,
  resolveRoundContract,
} from '../outcome'
export type {
  ClassifiedEvidence,
  ContractComparison,
  ContractStatus,
  EvidenceInput,
  EvidenceShape,
  FormalWinner,
  LaneEvidence,
  LaneEvidenceShape,
  ResultStatus,
  RoundContractDecision,
  RoundContractInput,
} from '../outcome'
