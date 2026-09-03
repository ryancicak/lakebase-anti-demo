/**
 * Pure derivation for the recap scoreboard.
 *
 * The recap is reachable at any time, so zero receipts, one bout, and a partial
 * session are all first-class states rather than edge cases. Nothing in this file
 * touches the DOM, fetches, or reads localStorage; it turns `GET /api/receipts`
 * into rows a renderer can print without making any judgement calls of its own.
 *
 * Two invariants the whole file exists to protect:
 *
 * 1. Never manufacture a comparison the orchestrator declined to declare. When a
 *    lane did not verify, its elapsed value is a lower bound -- where the clock
 *    stood when the bout stopped -- and subtracting it would produce a margin that
 *    was never measured.
 * 2. Never let an absent value read as a measured one. A null start skew means
 *    there was no simultaneous start, which is a different statement from a skew
 *    of zero, and `Math.max()` over an empty list quietly returns -Infinity.
 */

import type { CompetitorId, LaneState, RoundId } from './api/types'
import {
  classifyEvidence,
  resolveRoundContract,
  type ContractComparison,
  type RoundContractDecision,
} from './outcome'

/** Server-derived record of one sealed bout. Mirrors server/receipts.py. */
export interface BoutReceipt {
  receipt: string
  session_id: string
  round_id: RoundId
  round_title: string
  opponent: string
  opponent_id: CompetitorId

  outcome: 'declared' | 'stopped_short' | 'pending'
  sealing_event: string
  has_measurements: boolean

  metric: 'bout_elapsed_ms' | 'setup_elapsed_ms'
  lakebase: LaneReceipt
  opponent_lane: LaneReceipt

  margin_ms: number | null
  start_skew_ms: number | null
  sealed_at: string
  remembered_result: string | null
  failure: string | null
  cleanup_failure?: string | null
}

export type LaneReceiptState =
  | Extract<LaneState, 'verified' | 'failed' | 'not_supported'>
  | 'incomplete'

export interface LaneReceipt {
  ms: number | null
  state: LaneReceiptState
  /** True when `ms` is where the clock stood when the bout stopped, not a time. */
  lower_bound: boolean
  reason: string | null
}

/** Canonical round order. The six-round arc is the product's spine, not a filter. */
export const ROUND_ORDER: readonly RoundId[] = [
  'wake_idle_app',
  'make_schema_change_safely',
  'recover_deleted_order',
  'put_model_score_in_app',
  'survive_connection_spike',
  'analyze_live_orders_without_slowing_checkout',
]

const ROUND_TITLES: Record<RoundId, string> = {
  wake_idle_app: 'WAKE THIS IDLE APP',
  make_schema_change_safely: 'MAKE A SCHEMA CHANGE SAFELY',
  recover_deleted_order: 'RECOVER A DELETED ORDER',
  put_model_score_in_app: 'PUT A MODEL SCORE IN THE APP',
  survive_connection_spike: 'SURVIVE A CONNECTION SPIKE',
  analyze_live_orders_without_slowing_checkout: 'ANALYZE LIVE ORDERS',
}

export function roundNumber(roundId: RoundId): string {
  const index = ROUND_ORDER.indexOf(roundId)
  return index < 0 ? '--' : String(index + 1).padStart(2, '0')
}

export function roundTitle(roundId: RoundId): string {
  return ROUND_TITLES[roundId] ?? roundId.replace(/_/g, ' ').toUpperCase()
}

/**
 * How a bout's result should be read.
 *
 * - `timed`      both lanes verified, so a margin is real
 * - `capability` our lane verified against a lane that was never launched or is
 *                not supported. A win, just not a stopwatch one.
 * - `bounded`    the opponent lane ran and did not verify within the bound. A fact
 *                about this run, not a verdict about the product.
 * - `unproven`   our own lane did not verify. The demo's credibility comes from
 *                keeping these visible.
 */
export type ResultKind = 'timed' | 'capability' | 'bounded' | 'unproven'

/**
 * The lane facts a `ResultKind` is derived from.
 *
 * Named separately from `LaneReceipt` because the receipt is not the only thing
 * that has to answer "was this a win". The final scorecard is built from
 * localStorage rows written during the bout, not from receipts, and it drifted
 * into a second answer: it read a censored opponent lane as a Lakebase victory
 * without ever asking whether our own lane finished. Both surfaces now go
 * through `resultKind` below, so there is one answer and it lives here.
 */
export interface LaneOutcome {
  /** Our lane produced a verified figure. */
  lakebaseVerified: boolean
  /** Their lane produced a verified figure. */
  opponentVerified: boolean
  /**
   * Their lane ran and did not verify, so whatever figure it carries is a floor.
   * A lane that was never built is not this: nothing ran to be bounded.
   */
  opponentBounded: boolean
}

/**
 * How a bout's result should be read, from the two lanes alone.
 *
 * The order matters and is the whole point. Our own lane is asked first,
 * because a censored opponent says only "they had not finished when we gave
 * up" -- which is a comparison only if we finished. If we did not, the bout is
 * `unproven` no matter what state the other lane is in.
 */
export function resultKind(lanes: LaneOutcome): ResultKind {
  const evidence = classifyEvidence({
    lakebase: {
      exactMs: lanes.lakebaseVerified ? 0 : null,
      lowerBoundMs: null,
      notSupported: false,
    },
    competitor: {
      exactMs: lanes.opponentVerified ? 0 : null,
      lowerBoundMs: lanes.opponentBounded ? 0 : null,
      notSupported: !lanes.opponentVerified && !lanes.opponentBounded,
    },
    capabilityGap:
      lanes.lakebaseVerified && !lanes.opponentVerified && !lanes.opponentBounded,
  })
  if (evidence.exactLane !== 'lakebase' && evidence.laneShape !== 'both_exact_verified') {
    return 'unproven'
  }
  if (evidence.laneShape === 'both_exact_verified') return 'timed'
  if (evidence.laneShape === 'exact_and_censored_lower_bound') return 'bounded'
  return 'capability'
}

export interface BoutView {
  receipt: BoutReceipt
  roundId: RoundId
  roundNumber: string
  roundTitle: string
  opponent: string
  kind: ResultKind
  contract: RoundContractDecision
  /** True when the number under that lane is a floor, not a time. */
  lakebaseIsLowerBound: boolean
  opponentIsLowerBound: boolean
  /** Present only when both lanes verified. */
  marginMs: number | null
  /** null means there was no simultaneous start to measure. */
  startSkewMs: number | null
  /**
   * Whether a start gap is a meaningful claim for this bout. A skew only means
   * something when both lanes actually started, and the orchestrator has been seen
   * to report 0.0 for a bout whose opponent lane was never launched -- which would
   * otherwise read as a perfect simultaneous start that never happened.
   */
  hadSharedStart: boolean
  /** Whether this bout is eligible to appear on a scoreboard at all. */
  scoreable: boolean
  sealedAt: number
}

/** Reduce one receipt to the facts a row needs. */
export function boutView(receipt: BoutReceipt): BoutView {
  const lakebaseExact = receipt.lakebase.state === 'verified' && !receipt.lakebase.lower_bound
    ? receipt.lakebase.ms
    : null
  const opponentExact = receipt.opponent_lane.state === 'verified'
    && !receipt.opponent_lane.lower_bound
    ? receipt.opponent_lane.ms
    : null
  const evidence = classifyEvidence({
    lakebase: {
      exactMs: lakebaseExact,
      lowerBoundMs: receipt.lakebase.lower_bound ? receipt.lakebase.ms : null,
      notSupported: receipt.lakebase.state === 'not_supported',
    },
    competitor: {
      exactMs: opponentExact,
      lowerBoundMs: receipt.opponent_lane.lower_bound ? receipt.opponent_lane.ms : null,
      notSupported: receipt.opponent_lane.state === 'not_supported',
    },
    capabilityGap:
      lakebaseExact !== null
      && receipt.opponent_lane.state === 'not_supported'
      && receipt.outcome === 'declared',
    guardrailFailure:
      (receipt.round_id === 'put_model_score_in_app'
        || receipt.round_id === 'analyze_live_orders_without_slowing_checkout')
      && lakebaseExact !== null
      && receipt.outcome !== 'declared',
    cleanupFailure: Boolean(receipt.cleanup_failure),
  })

  let comparison: ContractComparison | null = null
  if (lakebaseExact !== null && opponentExact !== null) {
    if (lakebaseExact === opponentExact) {
      comparison = { kind: 'tie', winnerLaneId: null, marginMs: null }
    } else {
      comparison = {
        kind: 'measured',
        winnerLaneId: lakebaseExact < opponentExact ? 'lakebase' : 'competitor',
        marginMs: Math.abs(receipt.margin_ms ?? opponentExact - lakebaseExact),
      }
    }
  }
  const contract = resolveRoundContract({
    roundId: receipt.round_id,
    evidence,
    comparison,
    roundContractVerified: receipt.round_id === 'survive_connection_spike'
      || receipt.round_id === 'put_model_score_in_app'
      || receipt.round_id === 'analyze_live_orders_without_slowing_checkout'
      ? receipt.outcome === 'declared'
      : comparison !== null || evidence.exactLane !== null,
    terminal: receipt.outcome !== 'pending',
    recordNoEvidence: receipt.outcome === 'stopped_short',
  })
  const kind: ResultKind = contract.resultStatus === 'declared_capability'
    ? 'capability'
    : contract.resultStatus === 'declared_comparison'
      ? 'timed'
      : contract.resultStatus === 'adjudicated_stoppage'
        ? contract.formalWinner !== 'lakebase'
          ? 'unproven'
          : evidence.laneShape === 'exact_and_censored_lower_bound'
            ? 'bounded'
            : 'capability'
        : 'unproven'

  const sealed = Date.parse(receipt.sealed_at)

  return {
    receipt,
    roundId: receipt.round_id,
    roundNumber: roundNumber(receipt.round_id),
    roundTitle: receipt.round_title || roundTitle(receipt.round_id),
    opponent: receipt.opponent,
    kind,
    contract,
    lakebaseIsLowerBound: receipt.lakebase.lower_bound,
    opponentIsLowerBound: receipt.opponent_lane.lower_bound,
    marginMs: contract.marginMs,
    startSkewMs: receipt.start_skew_ms,
    hadSharedStart: receipt.start_skew_ms !== null && receipt.opponent_lane.ms !== null,
    // An attempt that failed before measuring anything is kept on disk as evidence
    // but is not a result, so it must not sit on the board as though it were one.
    scoreable: contract.scorecardEligible
      && (receipt.has_measurements || receipt.outcome === 'declared'),
    sealedAt: Number.isNaN(sealed) ? 0 : sealed,
  }
}

export interface RoundGroup {
  roundId: RoundId
  roundNumber: string
  roundTitle: string
  /** Oldest first, so a re-run reads as the newest row of its group. */
  bouts: BoutView[]
  /** True when no bout was run for this round this session. */
  unrun: boolean
  /**
   * Bouts this round has on disk that are not results: an attempt that stopped
   * before either lane measured anything, which is what a towel thrown early
   * leaves behind.
   *
   * Counted rather than discarded because a round is not the same as a round
   * nobody tried. Dropping these made an abandoned round render identically to
   * one that was never selected, and on a scorecard an absent row reads as data
   * loss rather than as a decision somebody made.
   */
  abandonedOnRecord: number
}

/**
 * One row per bout, grouped under its round, in canonical round order.
 *
 * Nothing is collapsed and nothing is dropped: a round run against two opponents
 * shows two rows. Unrun rounds are kept as empty groups so the six-round arc still
 * reads top to bottom and the operator can see what they have not shown yet.
 */
export function groupByRound(receipts: readonly BoutReceipt[]): RoundGroup[] {
  const byRound = new Map<RoundId, BoutView[]>()
  const abandoned = new Map<RoundId, number>()
  for (const receipt of receipts) {
    const view = boutView(receipt)
    if (!view.scoreable) {
      abandoned.set(view.roundId, (abandoned.get(view.roundId) ?? 0) + 1)
      continue
    }
    const existing = byRound.get(view.roundId)
    if (existing) existing.push(view)
    else byRound.set(view.roundId, [view])
  }

  return ROUND_ORDER.map((roundId) => {
    const bouts = (byRound.get(roundId) ?? []).slice().sort((a, b) => a.sealedAt - b.sealedAt)
    return {
      roundId,
      roundNumber: roundNumber(roundId),
      roundTitle: bouts[0]?.roundTitle ?? roundTitle(roundId),
      bouts,
      unrun: bouts.length === 0,
      abandonedOnRecord: abandoned.get(roundId) ?? 0,
    }
  })
}

/**
 * The one bout to show for a round where only one cell is available.
 *
 * Prefers a bout carrying a real margin, because that is the only kind that can
 * state a timed result; otherwise the most recent. Callers that summarise this way
 * are expected to disclose it -- the share card says so on its face.
 */
export function headlineBout(bouts: readonly BoutView[]): BoutView | null {
  if (bouts.length === 0) return null
  const timed = bouts.filter((bout) => bout.kind === 'timed' && bout.marginMs !== null)
  const pool = timed.length > 0 ? timed : bouts
  return pool.reduce((best, bout) => (bout.sealedAt >= best.sealedAt ? bout : best))
}

/**
 * The one bout that describes a round: the most recently sealed one.
 *
 * Deliberately not `headlineBout`, which prefers a bout carrying a margin over a
 * more recent one. That preference is right for a share card, which is trying to
 * state the strongest thing it can honestly state. It is wrong here. A summary
 * whose job is "the rounds you ran" must not quietly promote an earlier, better
 * result over the one that actually happened last -- that is a thumb on the scale,
 * and the operator would have no way to see it.
 */
export function latestBout(bouts: readonly BoutView[]): BoutView | null {
  if (bouts.length === 0) return null
  return bouts.reduce((latest, bout) => (bout.sealedAt >= latest.sealedAt ? bout : latest))
}

/**
 * What a round is showing, in the summary's own vocabulary.
 *
 * `uncontested` is its own state rather than a win because a round with one lane
 * has no loser. Calling it a win would manufacture the comparison that
 * `ResultKind: 'capability'` exists to refuse.
 */
export type RoundStatus =
  | 'running'
  | 'lakebase_faster'
  | 'competitor_faster'
  | 'tie'
  | 'lakebase_finished'
  | 'uncontested'
  | 'no_result'
  /**
   * Attempted and stopped before anything was measured. Distinct from `unrun`,
   * which is a round nobody selected, and from `no_result`, which measured
   * something and still could not declare.
   */
  | 'abandoned'
  | 'unrun'

export interface RoundResult {
  roundId: RoundId
  roundNumber: string
  roundTitle: string
  status: RoundStatus
  /** Null for `unrun` and `running`, and for a round with no opponent lane. */
  opponent: string | null
  lakebaseMs: number | null
  opponentMs: number | null
  /** True when `opponentMs` is where their clock stood, not a finish time. */
  opponentIsLowerBound: boolean
  /** Present only when both lanes verified. */
  marginMs: number | null
  /** Which source described this round, so a caller can disclose it. */
  source: 'live' | 'receipt' | null
  /** How many scoreable bouts this round has on record, including superseded ones. */
  boutsOnRecord: number
  /**
   * True when the bout describing this round was stopped rather than declared.
   * The lane figures still stand; the comparison between them does not.
   */
  stoppedShort: boolean
  /** When the describing bout was sealed, so a caller can date an older result. */
  sealedAt: number | null
}

/**
 * A round being run right now by this process.
 *
 * Only the identity and the fact that it has not finished. A running bout has no
 * result to report, and a *finished* one is already described by the receipt the
 * server sealed from the very same snapshot -- so re-deriving one here could only
 * ever disagree with the server, never improve on it.
 */
export interface LiveRound {
  roundId: RoundId
  running: boolean
}

function statusOf(bout: BoutView): RoundStatus {
  if (bout.contract.resultStatus === 'declared_capability') return 'uncontested'
  if (bout.contract.resultStatus === 'adjudicated_stoppage') {
    return bout.contract.formalWinner === 'lakebase'
      ? 'lakebase_finished'
      : bout.contract.formalWinner === 'competitor'
        ? 'competitor_faster'
        : 'no_result'
  }
  if (bout.contract.resultStatus !== 'declared_comparison') return 'no_result'
  if (bout.contract.formalWinner === 'tie') return 'tie'
  return bout.contract.formalWinner === 'lakebase'
    ? 'lakebase_faster'
    : bout.contract.formalWinner === 'competitor'
      ? 'competitor_faster'
      : 'no_result'
}

/**
 * One line per round, in canonical order, merging durable receipts with the bout
 * this process is running.
 *
 * Precedence, in full:
 *
 * 1. A round being run *now* reads `running`, whatever it has on record. It is the
 *    freshest thing known about that round and its earlier result is about to be
 *    superseded; presenting the old number as current would be wrong within
 *    seconds. `boutsOnRecord` still counts what came before, so nothing is
 *    silently dropped.
 * 2. Otherwise the round is described by its latest sealed bout. A bout that
 *    finished in this process is not a separate source: the server sealed its
 *    receipt from the same snapshot, so "live wins" and "latest receipt wins"
 *    name the same bout.
 * 3. A round with nothing on record reads `unrun`, so the six-round arc still
 *    reads top to bottom.
 *
 * Re-runs resolve to the latest, never the best. See `latestBout`.
 */
export function summariseRounds(
  receipts: readonly BoutReceipt[],
  live: LiveRound | null = null,
): RoundResult[] {
  const groups = groupByRound(receipts)

  return groups.map((group) => {
    const base = {
      roundId: group.roundId,
      roundNumber: group.roundNumber,
      roundTitle: group.roundTitle,
      boutsOnRecord: group.bouts.length,
      stoppedShort: false,
      sealedAt: null,
    }

    if (live && live.running && live.roundId === group.roundId) {
      return {
        ...base,
        status: 'running' as const,
        opponent: null,
        lakebaseMs: null,
        opponentMs: null,
        opponentIsLowerBound: false,
        marginMs: null,
        source: 'live' as const,
      }
    }

    const bout = latestBout(group.bouts)
    if (!bout) {
      return {
        ...base,
        // An attempt that measured nothing is still an attempt. Saying "not run"
        // of a round somebody stopped would erase the decision to stop it.
        status: group.abandonedOnRecord > 0 ? 'abandoned' as const : 'unrun' as const,
        opponent: null,
        lakebaseMs: null,
        opponentMs: null,
        opponentIsLowerBound: false,
        marginMs: null,
        source: null,
      }
    }

    const status = statusOf(bout)
    return {
      ...base,
      roundTitle: bout.roundTitle,
      status,
      // A round with no opponent lane has no opponent to name. Printing one would
      // imply somebody lost.
      opponent: status === 'uncontested' ? null : bout.opponent,
      lakebaseMs: bout.receipt.lakebase.ms,
      opponentMs: status === 'uncontested' ? null : bout.receipt.opponent_lane.ms,
      opponentIsLowerBound: bout.opponentIsLowerBound,
      marginMs: bout.marginMs,
      source: 'receipt' as const,
      stoppedShort: bout.receipt.outcome === 'stopped_short',
      sealedAt: bout.sealedAt,
    }
  })
}

/**
 * Seconds under a minute, m:ss above it.
 *
 * An opponent lane that ran for eight minutes is unreadable as `480707.66ms` and
 * only slightly better as `480.71s`.
 */
export function summaryDuration(milliseconds: number): string {
  const safe = Math.max(0, milliseconds)
  if (safe < 60_000) return `${(safe / 1000).toFixed(2)}s`
  const totalSeconds = Math.floor(safe / 1000)
  return `${Math.floor(totalSeconds / 60)}:${String(totalSeconds % 60).padStart(2, '0')}`
}

/**
 * The winner line for one round.
 *
 * `uncontested` deliberately does not read as a win. The round finished and has a
 * time, but there was no second lane, so there is nobody it beat.
 */
export function winnerLabel(result: RoundResult): string {
  switch (result.status) {
    case 'running':
      return 'RUNNING NOW'
    case 'lakebase_faster':
    case 'lakebase_finished':
      return 'LAKEBASE'
    case 'competitor_faster':
      return result.opponent?.toUpperCase() ?? 'OPPONENT'
    case 'tie':
      return 'TIE'
    case 'uncontested':
      return 'LAKEBASE · UNCONTESTED'
    case 'no_result':
    case 'abandoned':
      return 'NO RESULT DECLARED'
    default:
      return 'NOT RUN YET'
  }
}

/**
 * The blue corner's refusal wording, in one place.
 *
 * Lives here rather than in `ring.tsx` because this is the module both the fight
 * card and the ledger can import without pulling React in, and the phrase has to
 * be identical on both: the ledger is the same production as the card, and a
 * scorecard that paraphrases the card's own vocabulary invites the reader to
 * wonder which of the two is the real claim.
 *
 * "Native" is the load-bearing word. Without it the sentence claims the outcome
 * is unreachable on AWS, which is false and correctable; with it the claim is
 * that no built-in path does it, which is the finding.
 */
export const NO_EQUIVALENT_NATIVE_PATH = 'NO EQUIVALENT NATIVE PATH'

/**
 * What a round somebody stopped says, on every surface that prints one.
 *
 * Here for the same reason as `NO_EQUIVALENT_NATIVE_PATH` above: the finale
 * ledger and the final scorecard describe the same stopped round, and a card
 * that paraphrases the ledger invites the reader to wonder which of the two is
 * the real claim. The scorecard is built from localStorage rather than from
 * receipts, so it cannot reach `ledgerVerdict` -- but it can and does reach
 * these three strings.
 *
 * `NO RESULT DECLARED` is deliberately the same phrase `no_result` uses. The
 * two states differ in what was measured, not in what can be claimed, and
 * nothing can be claimed for either.
 */
export const ABANDONED_VERDICT = {
  outcome: 'NO RESULT DECLARED',
  qualifier: 'ABANDONED',
  laneNote: 'STOPPED BEFORE EITHER LANE VERIFIED',
} as const

/**
 * One round as the ledger prints it: who took it, on what figure, and the single
 * token that qualifies the taking.
 *
 * Split into fields rather than returned as a sentence because the ledger sets
 * them in different type at different weights, and because each one has to be
 * omittable on its own -- a clean win carries no qualifier, an abandoned round
 * carries no figure, and a round with no opponent carries no margin to speak of.
 *
 * The honesty rules this encodes, all of which have been got wrong before:
 *
 * - A round with one lane has a winner and no loser. `uncontested` names
 *   Lakebase and its figure, and attributes the empty lane to the blue corner
 *   rather than reporting it as a gap in the round. Printing a margin, or an
 *   opponent's name, would manufacture a race that never started.
 * - A stopped round keeps both figures and loses the comparison. The opponent's
 *   number is where its clock stood, so it is stated as a lower bound and the
 *   margin is refused outright.
 * - Nothing here invents provenance. No field claims a figure is receipt-backed
 *   or log-derived, because this module cannot tell the difference and a badge
 *   saying either would be a guess printed in the same weight as a measurement.
 */
export interface LedgerVerdict {
  /** The corner that took the round. Null when nobody did. */
  winner: { badge: string; name: string } | null
  /** Stands in for a winner when there is none. Never both. */
  outcome: string | null
  /** Lakebase's own figure. Null when it never produced one. */
  figure: string | null
  /** One token qualifying the win. Never a clause, never a sentence. */
  qualifier: string | null
  /**
   * The lane fact behind the qualifier, for the round's evidence column.
   *
   * Deliberately not in the winner column: a qualifier sits beside a number in
   * large type, and the reason an opponent has no figure is a statement about
   * the opponent, which belongs next to the evidence rather than next to ours.
   */
  laneNote: string | null
}

export function ledgerVerdict(result: RoundResult): LedgerVerdict {
  const lakebase = { badge: 'LB', name: 'LAKEBASE' }
  const opponent = { badge: 'OPP', name: result.opponent?.toUpperCase() ?? 'OPPONENT' }
  const figure = result.lakebaseMs === null ? null : summaryDuration(result.lakebaseMs)

  switch (result.status) {
    case 'lakebase_faster':
      return { winner: lakebase, outcome: null, figure, qualifier: null, laneNote: null }

    case 'competitor_faster':
      return {
        winner: opponent,
        outcome: null,
        figure: result.opponentMs === null ? null : summaryDuration(result.opponentMs),
        qualifier: result.stoppedShort ? 'STOPPED SHORT' : null,
        laneNote: null,
      }

    case 'tie':
      return { winner: null, outcome: 'TIE', figure, qualifier: null, laneNote: null }

    case 'lakebase_finished': {
      // Ours finished, theirs did not. The figure they carry is a floor, so the
      // round has a winner and no measured distance between the two.
      const opponent = result.opponent ?? 'THE BLUE CORNER'
      const bound = result.opponentMs === null
        ? `${opponent.toUpperCase()} · UNVERIFIED WHEN STOPPED`
        : `${opponent.toUpperCase()} · UNVERIFIED WHEN STOPPED · LOWER BOUND ${summaryDuration(result.opponentMs)}`
      return {
        winner: lakebase,
        outcome: null,
        figure,
        qualifier: result.stoppedShort ? 'STOPPED SHORT' : 'MARGIN N/A',
        laneNote: `${bound} · MARGIN N/A`,
      }
    }

    case 'uncontested':
      return {
        winner: lakebase,
        outcome: null,
        figure,
        qualifier: 'UNCONTESTED',
        laneNote: `BLUE CORNER · ${NO_EQUIVALENT_NATIVE_PATH}`,
      }

    case 'running':
      return { winner: null, outcome: 'RUNNING NOW', figure: null, qualifier: null, laneNote: null }

    case 'no_result':
      // Something was measured and it still could not be declared. The figures
      // are on the receipt; the round has no winner and the ledger says so.
      return { winner: null, outcome: 'NO RESULT DECLARED', figure: null, qualifier: null, laneNote: null }

    case 'abandoned':
      return {
        winner: null,
        outcome: ABANDONED_VERDICT.outcome,
        figure: null,
        qualifier: ABANDONED_VERDICT.qualifier,
        laneNote: ABANDONED_VERDICT.laneNote,
      }

    default:
      return { winner: null, outcome: 'NOT RUN YET', figure: null, qualifier: null, laneNote: null }
  }
}

/**
 * How well the reader knows the record, which is not the same question as what
 * the record says.
 *
 * `unread` exists because a failed read and an empty record are different facts
 * and only one of them permits the words "not run yet". Conflating them puts a
 * false negative on a scorecard, and on the shared image there is nobody
 * standing beside it to correct the impression.
 */
export type RecordState = 'read' | 'reading' | 'unread'

/**
 * What a ledger row prints, given a round and the state of the record it came
 * from. Both surfaces that score rounds -- the finale screen and the shareable
 * card -- go through here, so neither can drift into wording the other does not
 * use, and neither can be given a verdict the record does not support.
 */
export function verdictFor(result: RoundResult | null, state: RecordState): LedgerVerdict {
  const bare = (outcome: string): LedgerVerdict => (
    { winner: null, outcome, figure: null, qualifier: null, laneNote: null }
  )
  if (state === 'reading') return bare('READING THE RECORD…')
  if (state === 'unread') return bare('RECORD UNREAD')
  return result ? ledgerVerdict(result) : bare('NOT RUN YET')
}

/**
 * The day a standing result was sealed, for a result that is not from today.
 *
 * A ledger spanning several days reads as one sitting unless the older rows say
 * otherwise, and "today" is the only reading an undated row invites.
 */
export function ledgerDay(result: RoundResult, now: number = Date.now()): string | null {
  if (result.sealedAt === null || result.sealedAt === 0) return null
  const sealed = new Date(result.sealedAt)
  const today = new Date(now)
  if (
    sealed.getFullYear() === today.getFullYear()
    && sealed.getMonth() === today.getMonth()
    && sealed.getDate() === today.getDate()
  ) return null
  return `${sealed.getDate()} ${sealed.toLocaleString('en-GB', { month: 'short' }).toUpperCase()}`
}

/** How many rounds the summary can state a result for. Never counts `unrun`. */
export function roundsWithResult(results: readonly RoundResult[]): number {
  return results.filter((result) => (
    result.status === 'lakebase_faster'
    || result.status === 'competitor_faster'
    || result.status === 'tie'
    || result.status === 'lakebase_finished'
    || result.status === 'uncontested'
  )).length
}

export interface Tally {
  boutsRun: number
  roundsRun: number
  /** Always 6. Kept explicit so a label can say "of the six" and mean it. */
  roundsTotal: number
  declared: number
  stoppedShort: number
  /** Rounds with at least one declared bout, the honest numerator for a scoreboard. */
  roundsDeclared: number
  /**
   * Worst observed start gap across bouts that had a simultaneous start, or null
   * when none did. Never -Infinity: `Math.max()` over an empty list poisons the
   * fairness line, which is the exact bug this field exists to prevent.
   */
  worstSkewMs: number | null
  /** Bouts with no shared start, because the opponent lane was never launched. */
  boutsWithoutSharedStart: number
  /** True when there is nothing to show yet, so a caller can pick an empty state. */
  empty: boolean
}

export function tally(receipts: readonly BoutReceipt[]): Tally {
  const views = receipts.map(boutView).filter((view) => view.scoreable)
  const rounds = new Set(views.map((view) => view.roundId))
  const declaredRounds = new Set(
    views.filter((view) => view.receipt.outcome === 'declared').map((view) => view.roundId),
  )
  const skews = views
    .filter((view) => view.hadSharedStart)
    .map((view) => view.startSkewMs)
    .filter((skew): skew is number => skew !== null)

  return {
    boutsRun: views.length,
    roundsRun: rounds.size,
    roundsTotal: ROUND_ORDER.length,
    declared: views.filter((view) => view.receipt.outcome === 'declared').length,
    stoppedShort: views.filter((view) => view.receipt.outcome === 'stopped_short').length,
    roundsDeclared: declaredRounds.size,
    worstSkewMs: skews.length > 0 ? Math.max(...skews) : null,
    boutsWithoutSharedStart: views.length - skews.length,
    empty: views.length === 0,
  }
}

/**
 * Whether a session is worth offering as a share card.
 *
 * A two-round session makes a legitimate recap page but a weak share image: four of
 * six cells would be empty. The page is always available; the share action is not.
 */
export function canShare(summary: Tally): boolean {
  return summary.roundsDeclared >= SHARE_THRESHOLD_ROUNDS
}

export const SHARE_THRESHOLD_ROUNDS = 3
