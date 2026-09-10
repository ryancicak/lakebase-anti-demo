export {
  buildRingsideCue,
  buildRingsideShow,
  classifyOutcome,
  classifyRingsideOutcome,
  interpolateProof,
  priorityKeyFor,
} from './classifier'
export type { ClassifiedSessionOutcome } from './classifier'
export {
  OUTCOME_COPY_RECORDS,
  OUTCOME_COPY_SHA256,
  PERSONA_IDS,
  ROUND_FIVE_PERSONA_OUTCOME_RECORDS,
  ROUND_FIVE_PERSONA_OUTCOMES_SHA256,
  ROUND_IDS,
  VERIFIED_CORPUS_RECORDS,
  VERIFIED_CORPUS_SHA256,
  getOutcomeRecord,
  getRoundFivePersonaOutcomeRecord,
  getVerifiedRecord,
} from './corpus'
export {
  OUTCOME_IDS,
  PRIORITY_KEYS,
} from './types'
export type {
  AuthoredText,
  CopyDecision,
  CopyMode,
  OutcomeCopyRecord,
  PriorityKey,
  RoundFivePersonaOutcomeRecord,
  RingsideCue,
  RingsideOutcomeId,
  VerifiedCorpusRecord,
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
