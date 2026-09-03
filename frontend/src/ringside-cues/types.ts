import type { PersonaId, RoundId } from '../api/types'

export const PRIORITY_KEYS = [
  'cost',
  'simplicity',
  'performance',
  'cost+simplicity',
  'cost+performance',
  'simplicity+performance',
  'cost+simplicity+performance',
] as const

export type PriorityKey = typeof PRIORITY_KEYS[number]
export type CopyDecision = 'KEEP' | 'REWRITE'
export type CopyMode = 'INHERIT_VERIFIED_CORPUS' | 'OUTCOME_OVERRIDE'

export const OUTCOME_IDS = [
  'verified_comparison',
  'verified_capability_gap',
  'verified_rds_capability_gap',
  'one_sided_verified',
  'one_sided_towel_lower_bound',
  'one_sided_setup_verified_towel',
  'score_identity_unverified',
  'setup_incomplete',
  'spike_contract_failed',
  'cleanup_failed',
  'checkout_guardrail_unverified',
  'no_result',
  'towel_no_verified_lane',
] as const

export type RingsideOutcomeId = typeof OUTCOME_IDS[number]

export interface VerifiedCorpusRecord {
  readonly round_id: RoundId
  readonly persona_id: PersonaId
  readonly priority_key: PriorityKey
  readonly meaning_record_id: string
  readonly question_record_id: string
  readonly meaning: string
  readonly question: string
  readonly meaning_decision: CopyDecision
  readonly question_decision: CopyDecision
}

export interface OutcomeCopyRecord {
  readonly round_id: RoundId
  readonly outcome_id: RingsideOutcomeId
  readonly persona_id: '*'
  readonly priority_key: '*'
  readonly copy_mode: CopyMode
  readonly meaning_record_id: string | null
  readonly question_record_id: string | null
  readonly proof_template_id: string
  readonly meaning: string | null
  readonly question: string | null
  readonly proof_template: string
  readonly meaning_decision: CopyDecision | null
  readonly question_decision: CopyDecision | null
  readonly proof_decision: CopyDecision
}

export interface AuthoredText {
  readonly id: string
  readonly text: string
  readonly decision: CopyDecision
}

export interface RingsideCue {
  readonly say: string
  readonly ask: string
  readonly show: string
  readonly sayRecord: AuthoredText
  readonly askRecord: AuthoredText
  readonly proofRecord: AuthoredText
  readonly outcome: OutcomeCopyRecord
}
