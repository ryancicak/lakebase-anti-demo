import type { CustomerCorner, PersonaId, RoundId } from '../api/types'
import type { CompetitorKind } from '../competitor-kind'
import type { ContractStatus } from '../outcome'

export const OUTCOME_IDS = [
  'verified_comparison',
  'verified_capability_gap',
  'verified_rds_capability_gap',
  'one_sided_verified',
  'one_sided_towel_lower_bound',
  'one_sided_setup_verified_towel',
  'setup_incomplete',
  'bounded_check_failed',
  'cleanup_failed',
  'no_result',
  'towel_no_verified_lane',
] as const

export type RingsideOutcomeId = typeof OUTCOME_IDS[number]

/**
 * Which opponent a line is written for. `*` is the line every bout reads unless a
 * variant for the bout's competitor kind exists; a variant exists only where the
 * `*` line names the other side's stack (RDS Proxy, AWS Glue, a replication slot)
 * and the other edition's stack is a different thing.
 */
export type CompetitorScope = '*' | CompetitorKind

/**
 * What happened, for one round and one outcome: a sentence a person would say out
 * loud, with the bout's numbers in it and the measured boundary at the end. The
 * `<TOKENS>` are filled from the session; a missing one is an error, never a blank.
 */
export interface ResultLineRecord {
  readonly round_id: RoundId
  readonly outcome_id: RingsideOutcomeId
  readonly competitor: CompetitorScope
  readonly text: string
}

/**
 * Why one round matters to one role, and the one question to put to that role.
 * Written to the person ("your app", "your runbook"), about the stakes of the round
 * rather than this bout's result, so it reads true whatever the outcome was.
 */
export interface RoleLineRecord {
  readonly round_id: RoundId
  readonly persona_id: PersonaId
  readonly competitor: CompetitorScope
  readonly why: string
  readonly ask: string
}

/**
 * One round seen through one priority. Concrete to the round: what is billed and
 * what stands (cost), what has to be built and owned (simplicity), what the clock
 * covered and what it left out (performance). Never this bout's verdict.
 */
export interface LensLineRecord {
  readonly round_id: RoundId
  readonly priority: CustomerCorner
  readonly competitor: CompetitorScope
  readonly text: string
}

export interface RingsideLens {
  readonly priority: CustomerCorner
  readonly text: string
}

/**
 * The briefing behind "Explain to the room", in the order a presenter reads it:
 * what happened, why it matters to the person they are facing, the priorities the
 * room picked, and the question that turns it into a conversation.
 */
export interface RingsideBriefing {
  readonly result: string
  readonly why: string
  readonly lenses: readonly RingsideLens[]
  readonly ask: string
  readonly outcomeId: RingsideOutcomeId
  readonly status: ContractStatus
  /** A winner, a tie, or a capability was declared; false for stoppages without a result. */
  readonly declared: boolean
}
