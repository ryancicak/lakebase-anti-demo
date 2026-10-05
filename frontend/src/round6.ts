import type { DemoSession, LaneId, LaneSnapshot, MetricValue } from './api/types'
export { roundFourUnsupportedReason as roundSixUnsupportedReason } from './round4'

export const ROUND_SIX_ID = 'analyze_live_orders_without_slowing_checkout' as const

export function isRoundSix(session: { round: { id: string } } | null | undefined): boolean {
  return session?.round.id === ROUND_SIX_ID
}

/**
 * The condition Round 6 runs under, as the v1.1 design names it (section 2).
 *
 * Only AWS is cold at the bell. Lakebase's change feed is part of the database and
 * never stops, so it has nothing to start; the card says so rather than letting a
 * shared start imply both sides began from rest.
 */
export const ROUND_SIX_CONDITION = 'Lakebase: built-in change feed · AWS: DMS + Glue (cold start)'

/**
 * What carries each Round 6 lane's checkout into the lakehouse, printed under that
 * lane's database.
 *
 * The lane's own name stays the database the checkout commits to (Lakebase, or the
 * matchup's Aurora or RDS). What moves the order into Delta is the stack, and it is
 * the only thing the two lanes do differently. The AWS wording is the server's own
 * lane label, so the ring and the engine's progress lines name it the same way.
 */
export function roundSixStackLabel(
  session: Pick<DemoSession, 'competitor'>,
  laneId: LaneId,
): string {
  return laneId === 'lakebase'
    ? 'Lakebase built-in change feed'
    : `AWS DMS + Glue from ${session.competitor.short_name}`
}

/**
 * A Round 6 lane's stack as the ring, the receipt and the replay label it.
 *
 * Only the AWS lane cold starts, and the result is only fair with that said, so it
 * carries "(cold start)" beside its own name. Lakebase's feed never does: it is
 * built in and always on.
 */
export function roundSixLaneLabel(
  session: Pick<DemoSession, 'competitor'>,
  laneId: LaneId,
): string {
  const stack = roundSixStackLabel(session, laneId)
  return laneId === 'lakebase' ? stack : `${stack} (cold start)`
}

/** One lane's own figure, where a round reports the same metric once per lane. */
export function laneMetricValue(
  session: Pick<DemoSession, 'metrics'>,
  specId: string,
  laneId: LaneId,
): MetricValue | undefined {
  return session.metrics?.find((metric) => metric.spec_id === specId && metric.lane_id === laneId)
}

/**
 * The checkout a Round 6 bout committed on every source, as a lane's evidence records it.
 *
 * Both lanes carry the same order, so either lane's evidence names it. A lane is only
 * `verified` when its verifier read exactly this order back from its own Delta history,
 * so the lane state is the proof; these are the facts it proved. Anything absent stays
 * an em dash instead of becoming an invented value.
 */
export interface LiveOrderEvidence {
  orderId: string
  totalDisplay: string
  proofNonce: string
  guardrailOrderId: string
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null ? value as Record<string, unknown> : {}
}

function evidenceString(record: Record<string, unknown>, key: string): string {
  const value = record[key]
  return value === null || value === undefined || value === '' ? '—' : String(value)
}

export function liveOrderEvidence(lane: LaneSnapshot): LiveOrderEvidence {
  const evidence = asRecord(lane.evidence)
  return {
    orderId: evidenceString(evidence, 'order_id'),
    totalDisplay: evidenceString(evidence, 'total_display'),
    proofNonce: evidenceString(evidence, 'proof_nonce'),
    guardrailOrderId: evidenceString(evidence, 'checkout_guardrail_order_id'),
  }
}
