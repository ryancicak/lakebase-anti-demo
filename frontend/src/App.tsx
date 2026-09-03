import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'
import { api, ApiError, subscribeToSession } from './api/client'
import type {
  AllBoutStatus,
  BoutStatus,
  CatalogResponse,
  CompetitorId,
  CooldownLaneState,
  CooldownSnapshot,
  CustomerCorner,
  DemoSession,
  DescentCostSnapshot,
  FightCardRoundStatus,
  LaneId,
  LaneSnapshot,
  PersonaId,
  RoundId,
  RunEvent,
} from './api/types'
import {
  playConfirm,
  playCursor,
  playOriginalBell,
  playStart,
  setOriginalCreditsThemeMuted,
  setOriginalRoundThemeMuted,
  setOriginalTitleThemeMuted,
  startOriginalRoundTheme,
  startOriginalTitleTheme,
  stopOriginalRoundTheme,
  stopOriginalTitleTheme,
} from './audio'
import { CreditsButton, type CreditsEntry } from './credits-entry'
import { creditsTally } from './credits-tally'
import { brandAssets, personaPortraits } from './assets'
import { useAccessibleDialog } from './hooks/useAccessibleDialog'
import { FALLBACK_CATALOG, metricForCorners, recommend, stopCondition, withBundledPortraits, type LocalRecommendation } from './catalog'
import { useReducedMotion } from './hooks/useReducedMotion'
import {
  FightRing,
  opponentBadge,
} from './ring'
import './ring.css'
import {
  type BoutReceipt,
  type LedgerVerdict,
  type RoundResult,
  ABANDONED_VERDICT,
  ledgerDay,
  summariseRounds,
  verdictFor,
} from './recap'
import { offerNativeShare, shareDismissalPrefix } from './share'
import {
  buildRingsideCue,
  classifyOutcome,
  priorityKeyFor,
  type FormalWinner,
} from './ringside-cues'
import { compactDuration, preciseDuration } from './time'
import { replayStory } from './instant-replay'
import { loadScorecard, saveScorecard, type ScorecardEntry } from './scorecard-storage'
export type { ScorecardEntry } from './scorecard-storage'
import {
  ROUND_FOUR_LEGEND,
  acceptsReconciledSession,
  applyRunEventSnapshot,
  canStartRoundFourRedo,
  isRoundFour,
  metricDisplay,
  metricValue,
  modelScoreEvidence,
  roundFourPresentation,
  roundFourUnsupportedReason,
  selectRound4Session,
} from './round4'
import {
  ROUND_FIVE_CONCURRENCY,
  ROUND_FIVE_DISPLAY_TITLE,
  ROUND_FIVE_RUNNER,
  ROUND_FIVE_SCHEDULED_CLIENTS,
  ROUND_FIVE_SETUP_MAX_LAUNCH_SKEW_MS,
  ROUND_FIVE_WARMUPS,
  ROUND_FIVE_WITNESS_CLIENTS,
  isRoundFive,
  roundFiveCountDisplay,
  roundFiveLaneResult,
  roundFiveP99Display,
  roundFiveSetupLaneResult,
  roundFiveSetupMarginDisplay,
} from './round5'
import { RoundSixProof } from './round6'
import {
  loadSetupProgress,
  readBrowserView,
  requiresSession,
  saveSetupProgress,
  writeBrowserView,
  type HistoryMode,
  type SetupScene,
  type Stage,
} from './progress'
import { Summary } from './summary'

type ApiStatus = 'checking' | 'online' | 'offline'

function roundCanStart(status: FightCardRoundStatus | null): boolean {
  return status?.can_start ?? false
}

/**
 * The words this installation uses for a ring that is holding a bout, named
 * once because two surfaces now say them: the sentence under the disabled
 * PREPARE, and the round tile the bout belongs to. The fight card already
 * carried a second, drifted copy of this fact once ("… · RING UNLOCKS
 * AUTOMATICALLY"); a third would be that defect again.
 */
const BOUT_IN_PROGRESS = 'BOUT IN PROGRESS'
/**
 * The broad status board is also the cross-viewer invalidation channel.
 *
 * An all-ready response cannot wait on an "idle" cadence longer than the
 * promised active-bout discovery bound: another browser can claim a round one
 * millisecond after this browser receives that response. Keep every successful
 * cadence below five seconds, with jitter so a room full of viewers does not
 * synchronize its seven bounded lease reads.
 */
const BOUT_BOARD_BLOCKED_POLL_MIN_MS = 2500
const BOUT_BOARD_BLOCKED_POLL_JITTER_MS = 1000
const BOUT_BOARD_READY_POLL_MIN_MS = 3500
const BOUT_BOARD_READY_POLL_JITTER_MS = 1000
const BOUT_BOARD_ERROR_RETRY_MIN_MS = 3000
const BOUT_BOARD_ERROR_RETRY_JITTER_MS = 1000
const ROUND_CLEANUP_COPY: Record<RoundId, string> = {
  wake_idle_app: 'This round will reopen when both corners return to the required idle state. Other rounds remain available.',
  make_schema_change_safely: 'This round will reopen when both isolated environments are confirmed deleted. Other rounds remain available.',
  recover_deleted_order: 'This round will reopen when both recovery environments are confirmed deleted. Other rounds remain available.',
  put_model_score_in_app: 'This round will reopen when its current cleanup finishes. Other rounds remain available.',
  survive_connection_spike: 'Round 5 will reopen automatically when its Proxy and security group are confirmed deleted. Other rounds remain available.',
  analyze_live_orders_without_slowing_checkout: 'This round will reopen when its current cleanup finishes. Other rounds remain available.',
}

type RoundCardState =
  | 'available'
  | 'bout_in_progress'
  | 'cleanup_in_progress'
  | 'unavailable'

/**
 * The fight card's semantic state for any round.
 *
 * A present all-round status is authoritative. The catalog fallback exists for
 * UI review and mixed-version tabs only; live selection stays locked until the
 * status board has answered.
 */
function roundCardState(
  round: CatalogResponse['rounds'][number],
  status: FightCardRoundStatus | null,
): RoundCardState {
  if (status) return status.state === 'ready' ? 'available' : status.state
  if (round.availability_reason_code === 'cleanup_in_progress') return 'cleanup_in_progress'
  return round.availability === 'ready' ? 'available' : 'unavailable'
}

function roundBoardMessage(
  roundId: RoundId,
  status: FightCardRoundStatus | null,
): string {
  if (status?.detail) return status.detail
  if (status?.state === 'bout_in_progress') return BOUT_IN_PROGRESS
  if (status?.state === 'cleanup_in_progress') return ROUND_CLEANUP_COPY[roundId]
  if (status?.state === 'unavailable') return 'This round is unavailable right now.'
  return 'CHECKING ALL SIX ROUNDS…'
}

interface FinaleBeat {
  number: string
  /**
   * Which round this beat describes. The beats are the product's story and the
   * results are this installation's record, and the two are joined on this: a
   * beat printed against the wrong round's result is the one fault on this
   * screen that would be both invisible and disqualifying.
   */
  roundId: RoundId
  title: string
  flow: string
  proof: string
  accent: 'red' | 'blue' | 'yellow'
}

const FINALE_BEATS: FinaleBeat[] = [
  { number: '01', roundId: 'wake_idle_app', title: 'Wake from zero', flow: 'Idle → exact transaction', proof: 'Application read-back stops the clock', accent: 'red' },
  { number: '02', roundId: 'make_schema_change_safely', title: 'Change safely', flow: 'Branch → isolated schema', proof: 'Changed copy verified · source untouched', accent: 'blue' },
  { number: '03', roundId: 'recover_deleted_order', title: 'Recover exactly', flow: 'Delete → exact row restored', proof: 'Recovered row verified · deletion preserved', accent: 'yellow' },
  { number: '04', roundId: 'put_model_score_in_app', title: 'Delta → live app', flow: 'Analytics → operational data', proof: 'Managed reverse ETL · app read verified', accent: 'red' },
  { number: '05', roundId: 'survive_connection_spike', title: 'Get spike-ready', flow: 'Pooling → connection readiness', proof: 'Declared-start setup · identical spike passed · existing Proxy starts ready', accent: 'blue' },
  { number: '06', roundId: 'analyze_live_orders_without_slowing_checkout', title: 'Live app → Delta', flow: 'Checkout → exact answer', proof: 'Native change feed · separate checkout verified', accent: 'yellow' },
]

/**
 * The six rounds in running order, and the only place their numbers are written.
 *
 * The scorecard used to carry a three-entry lookup and fall back to the row's
 * position in the list, so Round 6 -- the one round on the card that the lookup
 * did not know -- printed whatever index it happened to sit at. A number that
 * moves with the scroll order is worse than no number.
 */
const ROUND_NUMBERS: readonly RoundId[] = [
  'wake_idle_app',
  'make_schema_change_safely',
  'recover_deleted_order',
  'put_model_score_in_app',
  'survive_connection_spike',
  'analyze_live_orders_without_slowing_checkout',
]

const ACTIVE_SESSION_KEY = 'lakebase-anti-demo:active-session:v1'

interface ActiveSessionPointer {
  id: string
  stage: Stage
  resumeStage: Stage
}

function loadActiveSessionPointer(): ActiveSessionPointer | null {
  try {
    const value: unknown = JSON.parse(window.sessionStorage.getItem(ACTIVE_SESSION_KEY) ?? 'null')
    if (!value || typeof value !== 'object') return null
    const pointer = value as Partial<ActiveSessionPointer>
    if (typeof pointer.id !== 'string' || !pointer.id) return null
    const stage = pointer.stage
    const resumeStage = pointer.resumeStage
    if (
      stage !== 'title'
      && stage !== 'matchup'
      && stage !== 'ready'
      && stage !== 'proof'
      && stage !== 'between'
      && stage !== 'finale'
    ) return null
    return {
      id: pointer.id,
      stage,
      resumeStage: resumeStage === 'matchup'
        || resumeStage === 'ready'
        || resumeStage === 'proof'
        || resumeStage === 'between'
        || resumeStage === 'finale'
        ? resumeStage
        : stage,
    }
  } catch {
    return null
  }
}

function saveActiveSessionPointer(pointer: ActiveSessionPointer | null): void {
  try {
    if (pointer) window.sessionStorage.setItem(ACTIVE_SESSION_KEY, JSON.stringify(pointer))
    else window.sessionStorage.removeItem(ACTIVE_SESSION_KEY)
  } catch {
    // A locked-down browser may disable session storage; the live SSE path still works.
  }
}

function sessionStage(session: DemoSession, preferred: Stage = 'proof'): Stage {
  if (session.state === 'draft' || session.state === 'checking') return 'matchup'
  if (session.state === 'armed') return 'ready'
  if (preferred === 'between' && session.cooldown) return 'between'
  if (
    preferred === 'finale'
    && session.state === 'verified'
    && session.round.id === 'analyze_live_orders_without_slowing_checkout'
  ) return 'finale'
  return 'proof'
}

function isRoundSix(session: { round: { id: string } }): boolean {
  return session.round.id === 'analyze_live_orders_without_slowing_checkout'
}

function opponentLabel(roundId: RoundId, shortName: string): string {
  return roundId === 'survive_connection_spike' ? `${shortName} + RDS Proxy` : shortName
}

function fightCardOpponentLabel(roundId: RoundId, competitorId: CompetitorId, shortName: string): string {
  if (roundId !== 'survive_connection_spike') return shortName
  return competitorId === 'aurora_serverless_v2'
    ? 'Aurora + RDS Proxy'
    : 'RDS PostgreSQL + RDS Proxy'
}
const STREAM_INTERRUPTION_ERROR = 'Live evidence stream interrupted. Display timers are frozen at disconnect while the app reconnects; no result has been inferred.'
const STREAM_FAILURE_ERROR = 'Live evidence stream remains unavailable after repeated reconnects. Last-known proof is retained; check the server before continuing.'

function isStreamError(value: string | null): boolean {
  return value === STREAM_INTERRUPTION_ERROR || value === STREAM_FAILURE_ERROR
}

/**
 * A client-side abort tells us nothing about the server. The bell may already be
 * ringing, so never claim "no run started" until a read-back proves it.
 */
const RUN_START_UNCONFIRMED_ERROR = 'The bell response timed out. The round may still be starting — checking with the ring before you touch anything.'
const RUN_START_NOT_STARTED_ERROR = 'The bell response timed out and the ring still reports no run. Nothing was recorded. Ring again when you are ready.'
const RUN_START_RECONCILE_INTERVAL_MS = 1_200
const RUN_START_RECONCILE_ATTEMPTS = 10
const SESSION_RESTORE_ATTEMPTS = 4
const SESSION_RESTORE_INTERVAL_MS = 2_500

/**
 * An abandoned backstage cleanup, headed and worded the same way wherever it
 * surfaces.
 *
 * A towel and a bout that ended on its own reach this state by different
 * routes and carry the diagnostic on different fields, but they mean one
 * thing to the operator: run-owned resources were never proved gone. One
 * heading and one fallback, so the two cannot drift into two vocabularies.
 *
 * The fallback is only ever reached when the server sent no sentence of its
 * own. Its sentence is the one written to be read, and it says what was
 * actually attempted; ours only says that something was not finished.
 */
const CLEANUP_ABANDONED_TITLE = 'Cleanup needs attention'
const CLEANUP_ABANDONED_FALLBACK = 'Owned cleanup could not be safely completed.'

/**
 * What a double-tapped bell reads, from the moment the first tap lands.
 *
 * WHAT THE SECOND TAP ACTUALLY DOES. Nothing, and it cannot be made to do
 * anything: the first press sets `ringBlocked` inside its own discrete event, so
 * the control is already `disabled` before a second press can be dispatched, and
 * a press on a disabled button fires no event at all. There is no handler to
 * report from -- `ring()`'s own re-entry guard is unreachable from this screen
 * for the same reason. So a message that fires *on* the second tap is not
 * available, and one that claims to have received it would be a lie.
 *
 * WHAT WAS ACTUALLY WRONG. Before this, the only thing that changed under a
 * double-tap was the button's own label, from "Ring the bell" to "Confirming the
 * bell…" -- a control the presenter has just committed to and is no longer
 * reading, and which says nothing about the second press they are mid-way
 * through making. A presenter who taps twice and sees the round not start reads
 * a frozen app. So this states the two facts that answer them, beside the button
 * rather than on it: a bell is in flight, and a second one cannot start a second
 * bout. It is on screen before the second tap can land, which is the property
 * that matters, and it costs the bout nothing because it is true of the single
 * tap too.
 */
const BELL_IN_FLIGHT = 'Bell rung · Confirming the first · A second press cannot start a second bout'

// Server-side the window is ANTI_DEMO_ARM_TTL_SECONDS, which this deliberately
// does not try to know: the warning is a fraction of a window whose length is
// read off armed_expires_at, so it stays correct if the TTL is retuned.
const ARMED_WINDOW_WARNING_SECONDS = 30

const cornerCopy: Record<CustomerCorner, string> = {
  cost: 'Published rates now; billed usage later.',
  simplicity: 'Observed workflow and manual steps.',
  performance: 'Elapsed workflow time to verified outcome.',
}

// Fighter epithets for the opponent select. `tagline` carries the wake
// behaviour that decides the round, and stays optional so a future opponent
// without one worth stating can be added without a layout of empty lines.
const competitorEpithet: Record<CompetitorId, { epithet: string; tagline?: string }> = {
  aurora_serverless_v2: { epithet: 'NOT DEAD YET', tagline: 'Like the guy on Monty Python’s plague cart, it keeps yelling “I’m not dead yet.”' },
  rds_postgres: { epithet: 'NEVER SLEEPS', tagline: 'Like Samara from the Ring, it never sleeps.' },
}

interface FightSelection {
  competitor: CompetitorId
  corners: CustomerCorner[]
  primary: PersonaId
  secondary: PersonaId[]
  roundOverride: RoundId | null
}

const wait = (milliseconds: number) => new Promise<void>((resolve) => window.setTimeout(resolve, milliseconds))

// Exported for the all-surface contract matrix; it remains a pure reducer.
// eslint-disable-next-line react-refresh/only-export-components
export function scorecardEntry(session: DemoSession): ScorecardEntry | null {
  const classified = classifyOutcome(session)
  if (!classified.scorecardEligible) return null
  const lakebaseMs = classified.evidence.lakebase.exactMs
  const competitorExactMs = classified.evidence.competitor.exactMs
  const competitorLowerBoundMs = classified.evidence.competitor.lowerBoundMs
  const abandoned = classified.status === 'no_verified_evidence'

  return {
    session_id: session.id,
    round_id: session.round.id,
    round_title: session.round.title,
    competitor: session.competitor.short_name,
    lakebase_ms: lakebaseMs,
    competitor_ms: abandoned ? null : competitorExactMs ?? competitorLowerBoundMs,
    competitor_censored:
      !abandoned && competitorExactMs === null && competitorLowerBoundMs !== null,
    competitor_capability_gap: classified.evidence.shape === 'capability_gap',
    evidence_shape: classified.evidence.shape,
    contract_status: classified.status,
    formal_winner: classified.formalWinner,
    margin_ms: classified.marginMs,
    remembered_result: classified.headline,
    completed_at: session.towel?.requested_at ?? session.updated_at,
    cooldown: session.cooldown ? {
      mode: session.cooldown.mode,
      lakebase_ms: session.cooldown.lanes.lakebase.elapsed_ms,
      competitor_ms: session.cooldown.lanes.competitor.elapsed_ms,
      lakebase_state: session.cooldown.lanes.lakebase.state,
      competitor_state: session.cooldown.lanes.competitor.state,
    } : null,
  }
}

function costNumber(value: number | string): number | null {
  const parsed = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function formatUsd(value: number | string): string {
  const parsed = costNumber(value)
  if (parsed === null) return 'Pending'
  const sign = parsed < 0 ? '-' : ''
  const absolute = Math.abs(parsed)
  if (absolute >= 0.1) return `${sign}$${absolute.toFixed(2)}`
  return `${sign}$${absolute.toFixed(6).replace(/0+$/, '').replace(/\.$/, '')}`
}

function formatUsdRate(value: number | string): string {
  const parsed = costNumber(value)
  if (parsed === null) return 'Rate pending'
  return Math.abs(parsed) < 1 ? `$${parsed.toFixed(3)}` : `$${parsed.toFixed(2)}`
}

function formatCostTimestamp(value: string | null | undefined): string | null {
  if (!value) return null
  return value.replace('T', ' ').replace('Z', ' UTC')
}

const COST_STATUS_LABELS: Record<string, string> = {
  estimate_usage_pending: 'Estimate only',
  immediate_estimate: 'Estimate only',
  estimate: 'Estimate only',
  usage_pending: 'Estimate only',
  pending: 'Estimate only',
  partially_reconciled: 'Posted partial',
  reconciled: 'Posted through window',
  estimate_only: 'Estimate only',
  posted_partial: 'Posted partial',
  posted_through_window: 'Posted through window',
  corrected: 'Corrected',
  selection_required: 'Selection required',
  attribution_ambiguous: 'Attribution ambiguous',
  unavailable: 'Unavailable',
}

const COST_SCOPE_LABELS = {
  bout_estimate: 'Bout estimate',
  required_monthly_carrying_cost: 'Monthly carrying',
  installation_overhead: 'Installation overhead',
} as const

function CostScopeTotal({ label, value, suffix }: { label: string; value: number | string | null | undefined; suffix: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value == null ? 'Pending' : formatUsd(value)}{value == null ? '' : suffix}</strong>
    </div>
  )
}

function costStatusDetail(status: string, postedThrough: string | null): string {
  if (postedThrough) return `Posted through ${postedThrough}`
  if (status === 'posted_partial' || status === 'partially_reconciled') return 'Some provider usage posted'
  if (status === 'posted_through_window' || status === 'reconciled') return 'Full bout window posted'
  if (status === 'corrected') return 'Provider cost revised'
  if (status === 'selection_required') return 'Product and plan pending'
  if (status === 'attribution_ambiguous') return 'Exact attribution pending'
  if (status === 'unavailable') return 'Provider usage unavailable'
  return 'Posted usage pending'
}

/**
 * Configured compute for both lanes, behind a click.
 *
 * The project's own on-screen rule is that every untimed prerequisite is
 * disclosed, and compute sizing is exactly that: it is never measured, but it
 * bounds what the measurement can mean. It lives in Instant Replay because it
 * answers a technical challenge rather than carrying the presenter cue.
 *
 * `basis` is rendered rather than hidden in Instant Replay. A figure read back
 * from a live control plane during arming is a stronger claim than the value
 * the installer asked for, and the difference is worth showing there.
 *
 * WHY THE SUMMARY IS A FOUR-WAY VERDICT AND NOT `matched`. The payload's
 * `matched` answers "do the constants agree", which is not the question the
 * summary line was asking on behalf of the reader. Two claims came out false.
 *
 * At session-create there are no observed values, so every lane falls back to
 * its configured class and `matched` computes true off the constants alone --
 * the whole pre-arm window asserted a parity nobody had looked at. `matched`
 * only earns the word "matched" once every lane's `basis` is `observed`;
 * before that the honest line is that the two were configured to match.
 *
 * Rounds 4 and 6 have no AWS database at all, so their single Lakebase lane
 * returns `matched` true meaning "nothing to mismatch" -- and rendered as a
 * matched ceiling on a round with no opposing box. `hasOpposingLane` is
 * therefore checked before `matched`, because that true is not about parity.
 *
 * The branches are deliberately asymmetric. "Ceilings do not match" is not
 * gated on observation: a mismatch visible in the constants is a real defect in
 * the constants and should surface at session-create rather than waiting for an
 * arm that may never come. It is only the affirmative claim that has to be paid
 * for with a live reading.
 */
export function CapacityDisclosure({
  session,
  embedded = false,
}: {
  session: DemoSession
  embedded?: boolean
}) {
  const capacity = session.capacity
  if (!capacity) return null
  const allObserved = capacity.lanes.every((lane) => lane.basis === 'observed')
  const hasOpposingLane = capacity.lanes.length > 1
  const verdict = !hasOpposingLane
    ? 'No opposing box this round'
    : !capacity.matched
      ? 'Ceilings do not match'
      : allObserved
        ? 'Matched memory ceiling'
        : 'Configured to match · not read back this run'
  const contents = (
    <>
      <div className="capacity-lanes" aria-label="Configured compute per lane">
        {capacity.lanes.map((lane) => (
          <article key={`${lane.lane_id}:${lane.product}`} data-corner={lane.lane_id === 'lakebase' ? 'red' : 'blue'}>
            <strong>{lane.product}</strong>
            <b>{lane.configured}</b>
            <span>Memory · {lane.memory}</span>
            <span>Engine · {lane.engine_version}</span>
            <span>Idle · {lane.idle_policy}</span>
            <span>Max connections · {lane.max_connections ?? 'not published'}</span>
            <em data-basis={lane.basis}>
              {lane.basis === 'observed'
                ? 'Read from the live control plane'
                : lane.basis === 'configured'
                  ? 'Configured value · not read back this run'
                  : 'Not reported by the control plane'}
            </em>
          </article>
        ))}
      </div>
      <p>{capacity.note}</p>
    </>
  )
  if (embedded) {
    return (
      <section className="capacity-disclosure capacity-evidence" aria-label="Configured compute evidence">
        <strong className="capacity-evidence-title">Configured compute · {verdict}</strong>
        {contents}
      </section>
    )
  }
  return (
    <details className="capacity-disclosure">
      <summary>Configured compute · {verdict}</summary>
      {contents}
    </details>
  )
}

/**
 * What each engine's idle floor costs, behind a click.
 *
 * The arena screen gets one sentence and a ratio; the arithmetic lives here, for
 * the same reason capacity does -- it answers a challenge rather than carrying
 * the result. Every figure is rendered with the derivation that produced it and
 * the reason its band is as wide as it is, because a bound presented as an
 * estimate is the one way a small honest number becomes a dishonest one.
 *
 * The frequency projection is the point of the whole panel. A floor paid once a
 * day is noise at any ratio; the same floor paid on every descent is where a 5x
 * actually lands, which is the cost face of Round 5's spike story.
 *
 * `.descent-cost-measured` carries the one thing the server payload cannot: a
 * CloudWatch reading of what a real descent actually billed. It is written here
 * rather than derived because the app never samples that metric -- the arming
 * gate at `server/targets.py` reads a pre-bout window that is guaranteed to be
 * zero -- so the figures come from `.anti-demo-v7/aurora-acu-2026-08-21.md` and
 * are pinned by test instead. Four things it deliberately does not say: that
 * Lakebase idles free, that any of this applies to RDS, that the ratio is a
 * constant, or a per-round dollar total. The ratio and the two floors are what
 * was measured and what holds.
 */
export function DescentCostDisclosure({ session }: { session: DemoSession }) {
  const descent = session.descent_cost
  if (!descent) return null
  return (
    <details className="descent-cost-disclosure">
      <summary>Cost of returning to idle · {descent.floor_ratio_label}</summary>
      <p className="descent-cost-summary">{descent.summary}</p>
      <p className="descent-cost-measured">
        A wake is not a moment, it is a commitment. CloudWatch, out of band, one
        bout: a 15.31s Round 1 bout switched on 420s of billed Aurora capacity, and
        97.2% of that lane&rsquo;s cost landed after the bell. Both engines bill a
        floor — Aurora&rsquo;s is 300s against Lakebase&rsquo;s 60s, 5x longer. One
        lane, one reading: Rounds 2 and 3 measured 1.77x and 1.12x over their
        published figure, not 16x. The shorter the work, the more the floor dominates.
      </p>
      <div className="descent-cost-lanes" aria-label="Cost of one return to idle per lane">
        {descent.lanes.map((lane) => (
          <article key={`${lane.lane_id}:${lane.product}`} data-corner={lane.lane_id === 'lakebase' ? 'red' : 'blue'} data-descends={lane.descends}>
            <strong>{lane.product}</strong>
            <b>{lane.per_descent_display}</b>
            <span>Floor · {lane.floor_label}</span>
            <span>
              At {descent.illustrative_descents_per_day} descents/day · {lane.per_day_display}
            </span>
            <code>{lane.derivation}</code>
            <em>{lane.band_reason}</em>
            <small>{lane.rate_source}</small>
          </article>
        ))}
      </div>
      <p className="descent-cost-frequency">
        A floor is charged per descent, so cost follows how often a workload parks —
        not how long it sat idle. Paid once a day the difference is noise; the
        {' '}{descent.illustrative_descents_per_day}-descent column is an illustration, not a measurement.
      </p>
      <p className="descent-cost-note">{descent.note}</p>
    </details>
  )
}

/**
 * The three lanes an operator names, in the order the contrast reads best.
 *
 * One list, two readers: this panel shows these three first and puts the rest
 * behind a disclosure, and <CostRoom> renders the same three as its idle strip.
 * Two lists would let the two surfaces disagree about which lanes an audience
 * recognises, which is the only thing either of them uses this for.
 */
const NAMED_ENGINE_LANES: readonly StandingLane['lane_id'][] = ['rds', 'aurora', 'lakebase']

/**
 * One standing-cost lane. Identical on both sides of the disclosure below, on
 * purpose: a hidden lane that rendered a shorter row would look like a lane with
 * less evidence behind it, when the only difference is where it sits.
 */
function standingCostLane(lane: StandingLane) {
  return (
    <article key={lane.lane_id} data-side={lane.side} data-evidence={lane.evidence}>
      <strong>{lane.product}</strong>
      <b>{lane.figure.display}</b>
      <span>{lane.idle_label}</span>
      <code>{lane.figure.derivation}</code>
      <em>{lane.caveat}</em>
      <small>{lane.rate_source || lane.figure.rate_source}</small>
    </article>
  )
}

/**
 * What the installation is billed for while nobody is ringing the bell.
 *
 * WHY THERE IS NO FIGURE IN THIS FILE. Every number, every derivation and every
 * caveat below is read off `session.standing_cost`, built by
 * `server/standing_cost.py` from a sealed shape, a rate card and posted
 * Databricks usage. This panel used to carry its figures as literals, with a
 * comment explaining that there was no payload to read; there is one now, and a
 * guard in `standing-cost.test.tsx` asserts this component's own source contains
 * no dollar amount at all, so one cannot be typed back in. The literals that
 * were here had gone stale in five separate ways before they were removed --
 * which is the argument for the guard rather than for more careful editing.
 *
 * WHY THREE LANES ARE SHOWN AND SIX ARE RENDERED. RDS, Aurora and Lakebase are
 * the three an operator names, and together they are under a third of the bill.
 * The neutral runner belongs to neither corner, the RDS Proxy secrets stand
 * whether or not a proxy does, and the Databricks platform lane -- the
 * synced-table pipeline and the app's own compute -- is the largest of the six.
 * So the row an audience reads is the three, and the other three are one click
 * further in rather than deleted: a six-across row made every lane equally
 * unreadable, and dropping the three would leave totals that do not reconcile
 * against anything on screen.
 *
 * The disclosure is therefore not a footnote, it is the reconciliation, and its
 * summary says so -- "why the three above do not add up" -- because a reader who
 * cannot find the remainder cannot check the totals, and a total nobody can
 * check is the same as a total nobody should believe. What it may never become
 * is a fold of the platform lane into Lakebase: that would overstate Lakebase
 * roughly ninefold. The error points against us, which does not make it safe.
 *
 * WHY TWO TOTALS. One covers what this run created; the other adds compute that
 * predates the installation and would bill without it. Neither means anything
 * without the condition it holds under, so `condition` is rendered beside each
 * and the server refuses to build one without the other.
 *
 * WHY THE LANE FIGURES ADD TO THE FIRST OF THEM. `figure` on a lane is that
 * lane's share of the total this panel headlines, and the server holds compute
 * that predates the installation out of it. It did not, once: the platform lane
 * rendered a subtotal with a pre-existing workspace app inside it, so the lane
 * figures on screen added to the larger total while the panel led with the
 * smaller — the Databricks side reading high by an entire app the demo never
 * created, beside a `counted_in_installation_total` that said the lane belonged
 * to the smaller figure. That is the most audience-visible defect this panel can
 * have, because finding it takes arithmetic and no code. `predating` is what
 * carries the held-out amount, and the paragraph below the lanes is where it is
 * disclosed: visible, attributable, and not an addend.
 *
 * WHY NOTHING HERE TICKS. `credits.ticks` is a literal `false`. The accrued
 * figure is one snapshot computed server-side for an injected `as_of`, and it is
 * only ever as recent as the `as_of` beside it. The server rebuilds it on every
 * read, so the way to advance this panel is to poll the session -- not to set an
 * interval over a number the browser would then be inventing.
 *
 * WHY A ZERO IS PRINTED WHOLE. `figure.display` for a structural zero contains
 * its own basis, and the server validates that it does. Rendering anything
 * narrower than `display` would put a bare zero back on a cost screen, and a
 * bare zero is the one figure a reader cannot tell apart from a failed lookup.
 *
 * WHY THE FAIRNESS PARAGRAPH CAN VANISH. It concedes that our half is the larger
 * one. When either half is unpriced the server withholds it rather than
 * rewording it, and this component renders nothing in its place -- substituting
 * prose here would reinstate exactly the claim the server declined to make.
 *
 * WHY TWO PARAGRAPHS CARRY NO CLASS OF THEIR OWN. `continuous` discloses the
 * pipeline that is required to run between bouts as well as during them -- the
 * largest single line on the Databricks side, and a deliberate one -- and
 * `predating` discloses the compute the lane figures hold out. Both take a
 * paragraph rhythm from a direct-child rule -- `.standing-cost-disclosure > p`
 * for the one in the panel body, `.standing-cost-remainder > p` for the one
 * inside the door -- because they are step-back prose like the note and the
 * posted line, not claims the panel leads with. Both vanish on the same rule as
 * the fairness paragraph, and both are optional on the payload: the recorded
 * fixture these tests read predates them, which is why neither may be required
 * to render the panel and why no test here exercises either.
 *
 * WHY THE CONTINUOUS BREAKOUT SITS BEHIND THE SAME DOOR AS THE PLATFORM LANE.
 * It is that lane's largest component, so it is disclosed where that lane is
 * rather than a second time on first paint. What may not follow from moving it
 * is a reader concluding the cost is absent, and it does not: the headline total
 * is unchanged, the fairness paragraph on first paint still states the
 * Databricks half in dollars, and with every Lakebase endpoint scaling to zero
 * that half is very nearly this line on its own. So the money stays on first
 * paint and the attribution is what moved. The door says so on its own summary,
 * from `continuous.component` and never from prose typed here, and the lookup
 * behind that clause is the payload's own component list -- a summary that
 * named a line the disclosure did not hold would be the collapse turning into a
 * concealment. If there is no door -- no lane left over to open -- the paragraph
 * stays in the panel body instead. Collapsed, never deleted.
 */
export function StandingCostDisclosure({ session }: { session: DemoSession }) {
  const standing = session.standing_cost
  if (!standing) return null
  const { totals, credits, posted, drift, fairness } = standing
  // Partitioned out of the payload rather than mapped over the constant, so the
  // rows keep the order the server sent them in on both sides of the door.
  const engines = standing.lanes.filter((lane) => NAMED_ENGINE_LANES.includes(lane.lane_id))
  const remainder = standing.lanes.filter((lane) => !NAMED_ENGINE_LANES.includes(lane.lane_id))
  const continuous = standing.continuous?.state === 'stated' ? standing.continuous : null
  // The door may only advertise what is behind it, and the breakout may only
  // move behind a door it is actually inside. Both come off the same lookup, so
  // the summary cannot name a line the disclosure below it does not hold.
  const collapsed = continuous !== null && remainder.some(
    (lane) => lane.components.some((part) => part.component === continuous.component),
  ) ? continuous : null
  return (
    <details className="standing-cost-disclosure">
      <summary>Standing cost · What is billed with no bout running</summary>
      <p className="standing-cost-summary">{standing.summary}</p>
      {totals ? (
        <div className="standing-cost-totals" aria-label="Standing cost totals">
          {[totals.installation, totals.with_platform].map((total) => (
            <article key={total.label} data-partial={total.partial}>
              <strong>{total.label}</strong>
              <b>{total.display}</b>
              <em>{total.condition}</em>
              {total.partial ? <small>{total.partial_reason}</small> : null}
            </article>
          ))}
        </div>
      ) : (
        <p className="standing-cost-unreadable">{standing.seal_detail}</p>
      )}
      {credits ? (
        <p className="standing-cost-credits">
          {credits.display}. {credits.basis}
        </p>
      ) : null}
      {engines.length > 0 ? (
        <div className="standing-cost-lanes" aria-label="Standing cost per lane">
          {engines.map(standingCostLane)}
        </div>
      ) : null}
      {remainder.length > 0 ? (
        <details className="standing-cost-remainder">
          <summary>
            Why the three above do not add up · {remainder.map((lane) => lane.product).join(' · ')}
            {/* `continuous: true` governs how the pipeline SYNCS while it is up,
                not whether it stays up: the installation stops it between bouts
                and it bills nothing stopped. "Runs continuously" said the second
                thing. Same wording as `StandingCostContinuous` in `api/types.ts`
                and `server/models.py`, so the door and the panel behind it agree. */}
            {collapsed ? ` · includes ${collapsed.component}, which syncs continuously while it is up` : ''}
          </summary>
          <p>
            Every total above is computed over all {standing.lanes.length} lanes, these
            included, and names any lane it could not price. So the three shown first
            do not reconcile on their own: they are the three an operator names, and
            none of the lanes in here is a database engine at all.
          </p>
          <div className="standing-cost-lanes" aria-label="Standing cost per lane, the rest of the total">
            {remainder.map(standingCostLane)}
          </div>
          {collapsed ? <p>{collapsed.paragraph}</p> : null}
        </details>
      ) : null}
      {standing.predating?.state === 'stated' ? (
        <p>{standing.predating.paragraph}</p>
      ) : null}
      <p className="standing-cost-posted">
        {posted.display}. {posted.comparison_basis} {posted.explanation} {posted.aws_posted_basis}
      </p>
      <p className="standing-cost-drift" data-state={drift.state}>
        {drift.badge} · {drift.summary} {drift.separation_note}
      </p>
      {fairness.state === 'stated' ? (
        <p className="standing-cost-fairness">{fairness.paragraph}</p>
      ) : null}
      {continuous !== null && collapsed === null ? <p>{continuous.paragraph}</p> : null}
      <p className="standing-cost-note">{standing.note}</p>
      <p className="standing-cost-origin">
        {standing.origin_basis} {standing.seal_detail} {standing.shape_detail}
      </p>
    </details>
  )
}

/**
 * What each round's Aurora lane actually cost, per round, measured.
 *
 * WHY THIS PANEL EXISTS AT ALL. `_aurora_acu_quantity` used to price Aurora by
 * multiplying a lane clock by the 2 ACU ceiling, and CloudWatch showed that
 * convention understating every round it touched -- Round 1 by 16x, because
 * 97.2% of that bout's Aurora cost landed after the bell, inside the 300s
 * auto-pause descent no clock in this harness covers. The model was corrected
 * to stop inventing the quantity, which is right, and the consequence was that
 * every Aurora compute line went `unavailable`. This panel is the caller that
 * supplies the measurements explicitly, so the screen shows a measured figure
 * with its provenance rather than a gap. `server/bout_cost.py` is the only
 * caller that does it, and `estimate_bout_cost` still refuses to reach for the
 * measurement table itself.
 *
 * WHY PROVENANCE IS A PER-ROW BADGE AND NOT A FOOTNOTE. Three of these rows
 * mean genuinely different things and the difference is invisible if they are
 * styled alike. `measured` is an integral. `structural zero` is Rounds 4 and 6,
 * which provision no Aurora cluster, so their $0.00 is exact -- and it is the
 * only $0.00 this panel is allowed to print, because a zero that turns out to
 * be a failed lookup is the worst number on a cost screen. `unavailable` is
 * reachable only if a provisioned round loses its measurement, and it renders
 * as the word, never as a figure.
 *
 * WHY TWO KINDS OF BAND. Round 5's range is a spread between two real bouts,
 * 42% apart. Rounds 2 and 3's range is an unanswered question -- both reported a
 * flat 2.0 ACU for minutes after `DeleteDBInstance`, and whether AWS bills a
 * deleting instance is undocumented while `ce:GetCostAndUsage` is denied to this
 * principal. Both keep their range and each says which kind it is. Collapsing
 * either to one number would be answering a question nobody answered.
 *
 * WHY BOTH SUPERLATIVES ARE ON THE SAME PANEL. Round 5 is the dearest round
 * against Aurora and the cheapest round on Lakebase. Both are measured, both are
 * true, and either one alone reads as a contradiction of the other panel on the
 * same screen. So they are printed together with their lanes named, and the
 * claim the panel leads with is the stronger one anyway: the dearest round on
 * this lane is still cheaper than two ordinary rounds combined.
 */
export function BoutCostDisclosure({ session }: { session: DemoSession }) {
  const bout = session.bout_cost
  if (!bout) return null
  return (
    <details className="bout-cost-disclosure">
      <summary>What a bout costs on Aurora · {bout.total_display} for six rounds</summary>
      <p className="bout-cost-summary">{bout.summary}</p>
      <p className="bout-cost-superseded">
        Supersedes {bout.superseded_display}, which priced a lane clock at Aurora&rsquo;s
        ceiling. The clock stopped before Aurora did: on Round 1, 97.2% of the
        cost landed after the bell, inside the 300s auto-pause descent. The rate
        never moved — only the quantity, and only upward.
      </p>
      <div className="bout-cost-rounds" aria-label="Aurora lane marginal cost per round">
        {bout.rounds.map((round) => (
          <article
            key={round.round_id}
            data-provenance={round.provenance}
            data-band={round.band_kind}
          >
            <strong>
              R{round.round_number} · {round.label}
            </strong>
            <b>{round.usd_display}</b>
            <span>
              {round.provenance === 'measured'
                ? 'Measured · CloudWatch'
                : round.provenance === 'structural_zero'
                  ? 'Exact zero · no cluster'
                  : 'Unavailable · not zero'}
            </span>
            <code>{round.derivation}</code>
            <em>{round.band_reason}</em>
            {round.bouts.length > 0 && <small>Bouts · {round.bouts.join(' · ')}</small>}
          </article>
        ))}
      </div>
      <p className="bout-cost-dearest">{bout.dearest_claim}</p>
      <p className="bout-cost-lane">{bout.lakebase_lane_claim}</p>
      <p className="bout-cost-scope">{bout.scope_note}</p>
      <p className="bout-cost-note">{bout.note}</p>
      <p className="bout-cost-source">{bout.rate_source}</p>
    </details>
  )
}

export function CostReceiptDisclosure({ session }: { session: DemoSession }) {
  const receipt = session.cost_receipt
  if (!receipt || !session.corners.includes('cost')) return null
  const status = receipt.reconciliation_status ?? receipt.status
  const statusLabel = COST_STATUS_LABELS[status] ?? 'Estimate only'
  const postedThrough = formatCostTimestamp(receipt.posted_through)
  const queriedAt = formatCostTimestamp(receipt.queried_at ?? receipt.reconciled_at)
  const scopes = (['bout_estimate', 'required_monthly_carrying_cost', 'installation_overhead'] as const)
    .map((scope) => ({
      scope,
      lines: receipt.lines.filter((line) => (line.scope ?? 'bout_estimate') === scope),
    }))
    .filter(({ lines }) => lines.length > 0)
  const hasReconciliationMath = receipt.original_estimate_usd !== undefined
    || receipt.posted_cost_usd !== undefined
    || receipt.variance_usd !== undefined
  return (
    <details className="cost-receipt-disclosure">
      <summary>Pricing receipt · {statusLabel} · billed usage later</summary>
      <div className="cost-receipt-status" data-status={status}>
        <strong>{statusLabel}</strong>
        <span>{costStatusDetail(status, postedThrough)}</span>
        {receipt.revision != null && <span>Revision {receipt.revision}</span>}
        {queriedAt && <span>Queried {queriedAt}</span>}
      </div>
      <div className="cost-receipt-totals" aria-label="Cost scopes">
        <CostScopeTotal label="Bout estimate" value={receipt.known_bout_estimate_usd} suffix="" />
        <CostScopeTotal label="Monthly carrying" value={receipt.known_monthly_carrying_cost_usd} suffix=" / month" />
        <CostScopeTotal label="Installation overhead" value={receipt.known_installation_overhead_usd} suffix=" one-time" />
      </div>
      {hasReconciliationMath && (
        <div className="cost-receipt-math" aria-label="Cost reconciliation">
          <span>Original <strong>{receipt.original_estimate_usd == null ? 'Pending' : formatUsd(receipt.original_estimate_usd)}</strong></span>
          <span>Posted <strong>{receipt.posted_cost_usd == null ? 'Pending' : formatUsd(receipt.posted_cost_usd)}</strong></span>
          <span>Variance <strong>{receipt.variance_usd == null ? 'Pending' : formatUsd(receipt.variance_usd)}</strong></span>
        </div>
      )}
      <p>{receipt.region} · published on-demand rates · no contract discounts or free tier</p>
      {scopes.map(({ scope, lines }) => (
        <section className="cost-receipt-scope" key={scope}>
          <h4>{COST_SCOPE_LABELS[scope]}</h4>
          <div className="cost-receipt-lines">
            {lines.map((line) => {
              const isDatabricksSource = line.source.startsWith('system.billing')
              const isAwsSource = line.source.startsWith('AWS') || line.source.startsWith('Amazon')
              const sourceLabel = isDatabricksSource ? 'Databricks list prices' : isAwsSource ? 'AWS published pricing' : line.status === 'selection_required' ? 'Product selection required' : 'Published pricing'
              const timestampLabel = isAwsSource ? 'provider published' : 'checked'
              const timestamp = formatCostTimestamp(line.source_as_of)
              const rate = line.unit_rate_usd
              const rateDisplay = rate == null
                ? 'Rate pending'
                : line.rate_basis === 'current_promotion' && line.reference_list_unit_rate_usd != null
                  ? `current promo ${formatUsdRate(rate)} / ${line.unit} · normal list ${formatUsdRate(line.reference_list_unit_rate_usd)}`
                  : `${formatUsdRate(rate)} / ${line.unit}`
              const quantity = line.quantity == null ? null : `${line.quantity.toFixed(3)} ${line.unit}`
              const amount = line.status === 'selection_required'
                ? 'Selection required · price pending'
                : line.component.startsWith('RDS Proxy ·') && line.component.includes('final lifetime pending') && line.subtotal_usd != null
                  ? `10-minute minimum: ${quantity ?? 'quantity pending'} × ${rate == null ? 'rate pending' : formatUsdRate(rate)} = ${formatUsd(line.subtotal_usd)} · final Proxy lifetime pending`
                  : line.subtotal_usd != null
                    ? `${quantity ?? 'Quantity pending'}${rate == null ? ' · rate pending' : ` × ${formatUsdRate(rate)}`} = ${formatUsd(line.subtotal_usd)} / ${line.cadence}`
                    : quantity == null
                      ? `Quantity pending · ${rateDisplay}`
                      : `${quantity} · ${rateDisplay} · cost pending`
              const lineStatus = line.reconciliation_status ?? line.status
              const lineStatusLabel = COST_STATUS_LABELS[lineStatus] ?? 'Estimate only'
              const linePostedThrough = formatCostTimestamp(line.posted_through ?? line.observed_through)
              const lineMath = line.original_estimate_usd !== undefined || line.posted_cost_usd !== undefined || line.variance_usd !== undefined
              return (
                <div key={`${line.lane_id}-${line.component}`}>
                  <strong>{line.component}</strong>
                  <span>{amount}</span>
                  {lineMath && <small>Original {line.original_estimate_usd == null ? 'pending' : formatUsd(line.original_estimate_usd)} · Posted {line.posted_cost_usd == null ? 'pending' : formatUsd(line.posted_cost_usd)} · Variance {line.variance_usd == null ? 'pending' : formatUsd(line.variance_usd)}</small>}
                  <small>{lineStatusLabel}{linePostedThrough ? ` through ${linePostedThrough}` : ''} · {sourceLabel} · {timestampLabel} <time dateTime={line.source_as_of}>{timestamp}</time></small>
                </div>
              )
            })}
          </div>
        </section>
      ))}
      <p>{receipt.note}</p>
    </details>
  )
}

function priorityLabel(corners: CustomerCorner[]): string {
  return corners.map((corner) => corner.toUpperCase()).join(' + ')
}

interface RoundWhy {
  // What the room reads. One or two sentences, no port number, no security
  // group, no instruction only the operator could carry out.
  headline: string
  // The operator's full account, folded away rather than deleted. Null when the
  // headline is already the whole of it.
  detail: string | null
}

/**
 * The two registers of the WHY panel on the fight card.
 *
 * The refusal is checked before the recommender's rationale, and the order is
 * the point: a round that cannot run tonight has to say so in the slot marked
 * WHY, even when it is the round the recommender picked.
 */
function selectedRoundWhy(
  round: CatalogResponse['rounds'][number],
  recommendation: LocalRecommendation,
): RoundWhy {
  const headline = round.availability_headline ?? round.availability_reason
  if (headline) {
    const detail = round.availability_reason && round.availability_reason !== headline
      ? round.availability_reason
      : null
    return { headline, detail }
  }
  if (round.id === recommendation.round_id) return { headline: recommendation.reason, detail: null }
  if (round.availability !== 'ready') {
    return { headline: `This ${round.availability} round is non-executable; no adapter, verifier, or timer is available.`, detail: null }
  }
  return { headline: `Operator-selected ready round; the evidence and stop boundary follow “${round.title}.”`, detail: null }
}

/**
 * The one line under the fight card's buttons when the selected round is refused.
 *
 * This strip is a status token at 5-8px, and it used to be handed the entire
 * `availability_reason` paragraph: a full-width smear of overlapping lines
 * repeating, illegibly, prose the WHY panel was already showing in full. It now
 * says the one thing the panel cannot -- where to go next -- and only when
 * there is somewhere to go, because a ring that is not ready refuses every
 * round and pointing at the round list would be sending the room in a circle.
 */
function roundLockNote(
  round: CatalogResponse['rounds'][number],
  rounds: CatalogResponse['rounds'],
  state: RoundCardState,
  statuses: Record<RoundId, FightCardRoundStatus> | null,
): string {
  if (state === 'cleanup_in_progress') {
    return 'CLEANUP IN PROGRESS · OTHER ROUNDS REMAIN AVAILABLE'
  }
  const alternative = rounds.some((item) => (
    item.id !== round.id
    && (statuses?.[item.id]?.can_start ?? item.availability === 'ready')
  ))
  return alternative
    ? 'UNAVAILABLE TONIGHT · CHANGE ROUND TO PICK ONE THAT CAN RUN'
    : 'UNAVAILABLE TONIGHT · NO ROUND CAN RUN RIGHT NOW'
}

/**
 * The ring has not answered for the round on screen yet, so nothing is known
 * about it -- which is not the same fact as the ring being busy, and must not
 * borrow that sentence. It says what is happening and stops.
 */
const RING_STATUS_UNREAD = 'CHECKING ALL SIX ROUNDS · PREPARE UNLOCKS WHEN THE BOARD ANSWERS'

/**
 * Why PREPARE FIGHT CARD will not arm, or null when it will.
 *
 * THE DEFECT THIS REPLACES. Whether the button was live was decided in one
 * place and the reasons were written in another. The all-round snapshot is now
 * the one value both branches consume: missing knowledge locks the control,
 * and every non-ready machine state carries visible copy.
 *
 * That state is not a corner case. It is entered on every fresh tab, because
 * the catalog poll can turn the API indicator green before the status-board
 * poll lands. It no longer reappears on every tile press because one snapshot
 * already contains all six rounds.
 *
 * So the sentence and the disabled state are now ONE VALUE, and `canPrepare` is
 * this returning null and nothing else. A refusal with no wording stops being
 * a thing that can be written down.
 */
function prepareRefusal(params: {
  uiReview: boolean
  restoringSession: boolean
  apiStatus: ApiStatus
  boardFresh: boolean
  ringStatus: FightCardRoundStatus | null
  roundStatuses: Record<RoundId, FightCardRoundStatus> | null
  round: CatalogResponse['rounds'][number]
  rounds: CatalogResponse['rounds']
  roundState: RoundCardState
}): string | null {
  if (params.restoringSession) return 'RESTORING LIVE BOUT…'
  if (params.uiReview) return null
  if (params.apiStatus === 'offline') return 'LIVE UPDATES OFFLINE · LIVE PROOF LOCKED'
  // Ahead of the ring deliberately: the catalog already settled this without
  // asking, and "checking the ring" on a round that cannot run tonight is a
  // wait that never ends.
  if (params.roundState === 'cleanup_in_progress' || params.roundState === 'unavailable') {
    return roundLockNote(
      params.round,
      params.rounds,
      params.roundState,
      params.roundStatuses,
    )
  }
  if (params.apiStatus !== 'online' || !params.boardFresh || params.ringStatus === null) {
    return params.ringStatus && !params.boardFresh
      ? 'ROUND STATUS STALE · PREPARE LOCKED UNTIL THE BOARD ANSWERS'
      : RING_STATUS_UNREAD
  }
  if (!roundCanStart(params.ringStatus)) {
    return roundBoardMessage(params.round.id, params.ringStatus)
  }
  return null
}

function laneReceiptTime(milliseconds: number | null): string {
  return milliseconds === null ? 'NOT TIMED' : `${(milliseconds / 1000).toFixed(2)}s`
}

function towelCutoffMs(session: DemoSession): number | null {
  const cutoff = session.towel?.cutoff_ms ?? session.towel?.lower_bound_ms
  return typeof cutoff === 'number' && Number.isFinite(cutoff) && cutoff >= 0 ? cutoff : null
}

function towelLowerBoundMs(session: DemoSession, laneId: LaneId): number | null {
  const explicit = session.towel?.censored_lower_bounds_ms?.[laneId]
  if (typeof explicit === 'number' && Number.isFinite(explicit) && explicit >= 0) return explicit
  if (session.towel?.active_lane === laneId) return towelCutoffMs(session)
  const evidence = session.lanes[laneId].evidence
  const lowerBound = evidence && typeof evidence === 'object'
    ? (evidence as Record<string, unknown>).lower_bound_ms
    : null
  return typeof lowerBound === 'number' && Number.isFinite(lowerBound) && lowerBound >= 0
    ? lowerBound
    : null
}

function towelVerifiedMs(session: DemoSession, laneId: LaneId): number | null {
  if (session.lanes[laneId].state === 'verified') return session.lanes[laneId].elapsed_ms
  if (laneId === 'lakebase') return session.towel?.lakebase_verified_ms ?? null
  return null
}

function towelLaneValue(session: DemoSession, laneId: LaneId): string {
  const lane = session.lanes[laneId]
  if (lane.state === 'not_supported') return 'N/A'
  const verified = towelVerifiedMs(session, laneId)
  if (verified !== null) return laneReceiptTime(verified)
  const lowerBound = towelLowerBoundMs(session, laneId)
  return lowerBound === null ? 'NOT VERIFIED' : `>${laneReceiptTime(lowerBound)}`
}

function towelCleanupAllowsExit(session: DemoSession): boolean {
  return !session.towel || session.towel.state === 'ready'
}

function cleanupAllowsTerminalActions(session: DemoSession): boolean {
  return towelCleanupAllowsExit(session)
    && session.round5_setup?.cleanup_retryable !== true
}

function proofNavigationAllowsExit(session: DemoSession): boolean {
  // `towelled` is the terminal stopped-short result for every round. Leaving
  // that posted result only drops this browser's view; it neither cancels the
  // cleanup task, releases its per-round lease, nor marks cleanup complete.
  // Declared and failed results retain their existing cleanup requirements.
  return session.state === 'towelled' || cleanupAllowsTerminalActions(session)
}

function competitorReceiptValue(session: DemoSession): string {
  if (session.towel) return towelLaneValue(session, 'competitor')
  if (isRoundFour(session)) return 'NOT EXECUTED / TIMED'
  if (isRoundSix(session)) return 'SEPARATE CDC STACK REQUIRED'
  return session.lanes.competitor.state === 'not_supported'
    ? 'NO AUTO SCALE-TO-ZERO'
    : laneReceiptTime(session.lanes.competitor.elapsed_ms)
}

function measuredAt(session: DemoSession): string {
  const measured = new Date(session.updated_at)
  return Number.isNaN(measured.getTime()) ? session.updated_at : measured.toISOString()
}

/**
 * The same outcome, worded for the verdict band and nowhere else.
 *
 * The shared classification is the record. It travels to the scorecard and
 * onto the share receipt, and both of those leave this screen behind, so both
 * have to restate every caveat themselves. The band is the one surface that
 * does not have to. It
 * sits between two lane captions already carrying each lane's proof state and
 * a towel strip already saying the result is frozen, so spelling all of it out
 * again in 40px type ran the band to three lines and pushed the actions row
 * off the bottom of the screen.
 *
 * What is left is what nothing around the band says: the round outcome, who
 * the stoppage was adjudicated for, and that there is no margin. The copy
 * contract picks that vocabulary -- `STOPPED SHORT` and `NO WINNER DECLARED`
 * are its round-outcome phrases, and its lane-proof-state phrases are reserved
 * for lanes, which is where they already are.
 */
function verdictBandOutcome(session: DemoSession): string | null {
  const classified = classifyOutcome(session)
  const winner = classified.formalWinner === 'lakebase'
    || classified.formalWinner === 'competitor'
    ? session.lanes[classified.formalWinner].name.toUpperCase()
    : null
  if (!session.towel) return classified.headline
  if (classified.status === 'declared_capability' && winner) {
    return `STOPPED SHORT · ${winner} CAPABILITY WIN · MARGIN N/A`
  }
  if (classified.status === 'adjudicated_stoppage') return classified.headline
  if (
    classified.status === 'no_verified_evidence'
    && classified.evidence.exactLane === null
  ) {
    return 'STOPPED SHORT · NO WINNER DECLARED · MARGIN N/A'
  }
  return classified.headline
}

function receiptId(session: DemoSession): string {
  return session.id.replace(/[^a-z0-9]/gi, '').slice(0, 8).toUpperCase() || 'LIVE'
}

type ReceiptWinner = FormalWinner
type ReceiptKind = 'round' | 'idle'

export interface ReceiptPresentation {
  kind: ReceiptKind
  title: string
  focus: string
  winner: ReceiptWinner
  lakebaseLabel?: string
  competitorLabel?: string
  lakebaseValue: string
  competitorValue: string
  lakebaseStatus: string
  competitorStatus: string
  competitorCapabilityGap: boolean
  verdictLabel: string
  verdict: string
  fairness: string
  measuredAt: string
  verifiedStamp: string
  receiptLabel: string
  integrityDetail?: string
}

function receiptWinner(session: DemoSession): ReceiptWinner {
  return classifyOutcome(session).formalWinner
}

function roundFiveSetupMilliseconds(session: DemoSession, laneId: LaneId): number | null {
  const elapsedMs = session.round5_setup?.lanes?.[laneId]?.setup_elapsed_ms
  return typeof elapsedMs === 'number' && Number.isFinite(elapsedMs) && elapsedMs >= 0
    ? elapsedMs
    : null
}

function roundFiveCensoredLowerBoundMilliseconds(session: DemoSession, laneId: LaneId): number | null {
  const lowerBound = session.towel?.censored_lower_bounds_ms?.[laneId]
  return typeof lowerBound === 'number' && Number.isFinite(lowerBound) && lowerBound >= 0
    ? lowerBound
    : null
}

function receiptStartSkewDisplay(session: DemoSession): string {
  const skew = isRoundFive(session)
    ? session.round5_setup?.workflow_launch_skew_ms
    : session.fairness.launch_skew_ms
  return typeof skew === 'number' && Number.isFinite(skew) && skew >= 0
    ? `${skew.toFixed(3)}ms`
    : 'N/A'
}

function roundFiveSetupSeconds(session: DemoSession, laneId: LaneId): string {
  return laneReceiptTime(roundFiveSetupMilliseconds(session, laneId))
}

function roundFiveVerifiedVerdict(session: DemoSession): string {
  const classified = classifyOutcome(session)
  if (!classified.contractComplete) return classified.headline
  if (classified.formalWinner === 'tie') {
    return 'Both pooled paths verified together'
  }
  const winnerId = classified.formalWinner
  const winner = winnerId === 'lakebase' || winnerId === 'competitor'
    ? session.round5_setup?.lanes?.[winnerId]?.name ?? session.lanes[winnerId].name
    : 'Verified lane'
  const marginLabel = classified.marginMs !== null
    ? compactDuration(classified.marginMs)
    : 'an unreported margin'
  return `${winner} verified a pooled path · ${marginLabel} sooner`
}

function roundFiveSetupFairness(session: DemoSession): string {
  const skew = session.round5_setup?.workflow_launch_skew_ms
  const skewLabel = typeof skew === 'number' && Number.isFinite(skew) && skew >= 0
    ? `${skew.toFixed(3)} ms`
    : 'N/A'
  return `Shared post-preflight monotonic T0 · Workflow launch skew ${skewLabel} · Each clock stopped at its own exact application transaction`
}

function roundFiveIntegrityDetail(session: DemoSession): string {
  return `INTEGRITY · EXACT SETUP MARGIN ${roundFiveSetupMarginDisplay(session)} · BURST 128/128 BOTH LANES · WITNESS 64/64 BOTH LANES · CLEANUP VERIFIED ✓ · LAKEBASE BUILT-IN POOL / 0 PER-BOUT POOLING CHANGES · AWS NEW RDS PROXY + 8 SUPPORTING CHANGES / 9 TIMED TOTAL`
}

function publicDemoUrl(): string | null {
  const local = window.location.hostname === '127.0.0.1' || window.location.hostname === 'localhost'
  return local ? null : `${window.location.origin}${window.location.pathname}`
}

function linkedInHook(session: DemoSession): string {
  const classified = classifyOutcome(session)
  if (isRoundFour(session)) {
    const elapsed = roundFourAppElapsed(session)
    return `Lakebase moved an analytics change into the live app in ${elapsed} through built-in managed reverse ETL. Aurora/RDS requires a separate reverse-ETL stack; it was not built or timed. 🥊`
  }
  if (isRoundSix(session)) {
    const elapsed = metricValue(session, 'analytics_available_ms')?.value
    const elapsedLabel = typeof elapsed === 'number' ? laneReceiptTime(elapsed) : laneReceiptTime(session.lanes.lakebase.elapsed_ms)
    return `Lakebase turned one checkout into an exact Delta answer in ${elapsedLabel}. ${session.competitor.short_name} requires a separate CDC stack; it was not built or timed. 🥊`
  }
  if (session.towel) {
    if (isRoundFive(session) && classified.evidence.exactLane && !classified.contractComplete) {
      return `${classified.headline}. 🥊`
    }
    if (session.towel.cutoff_ms === undefined && session.towel.censored_lower_bounds_ms === undefined) {
      return `I threw in the towel at ${laneReceiptTime(towelCutoffMs(session))} — Lakebase verified in ${laneReceiptTime(towelVerifiedMs(session, 'lakebase'))}; ${session.competitor.short_name} was unverified when stopped, so its result is >${laneReceiptTime(towelLowerBoundMs(session, 'competitor'))}. 🥊`
    }
    const winner = classified.formalWinner
    const result = winner === 'lakebase' || winner === 'competitor'
      ? `${session.lanes[winner].name} kept its exact ${towelLaneValue(session, winner)} result`
      : 'no exact winner was declared'
    return `I threw in the towel at ${laneReceiptTime(towelCutoffMs(session))} — ${result}; ${session.lanes.lakebase.name} · ${towelLaneValue(session, 'lakebase')}; ${session.lanes.competitor.name} · ${towelLaneValue(session, 'competitor')}. 🥊`
  }
  if (isRoundFive(session)) {
    return `Declared-start readiness: Lakebase ${roundFiveSetupSeconds(session, 'lakebase')}; ${session.competitor.short_name} + RDS Proxy ${roundFiveSetupSeconds(session, 'competitor')}. ${roundFiveVerifiedVerdict(session)}. Both then passed the identical spike; an already-deployed RDS Proxy would not pay this setup delay. 🥊`
  }
  const winner = receiptWinner(session)
  const lakebaseMs = session.lanes.lakebase.elapsed_ms
  const competitorMs = session.lanes.competitor.elapsed_ms
  const difference = lakebaseMs === null || competitorMs === null
    ? null
    : `${(Math.abs(lakebaseMs - competitorMs) / 1000).toFixed(2)}s`

  if (session.round.id === 'wake_idle_app') {
    if (session.lanes.competitor.state === 'not_supported') {
      return `Lakebase woke from zero and completed a real PostgreSQL transaction. ${session.competitor.short_name} cannot enter this round. 🥊`
    }
    if (winner === 'lakebase' && difference) {
      return `Lakebase woke from zero and completed a real PostgreSQL transaction ${difference} before ${session.competitor.short_name}. 🥊`
    }
  }
  if (session.round.id === 'make_schema_change_safely' && winner === 'lakebase' && difference) {
    return `Lakebase branched, changed the schema, and verified the app transaction ${difference} before ${session.competitor.short_name}. 🥊`
  }
  if (session.round.id === 'recover_deleted_order' && winner === 'lakebase' && difference) {
    return `Lakebase recovered the exact deleted order ${difference} before ${session.competitor.short_name}, while the source stayed deleted. 🥊`
  }
  return `${session.remembered_result ?? 'Two live PostgreSQL databases completed the same proof.'} 🥊`
}

// Exported for the receipt-copy contract test; it remains a pure renderer.
// eslint-disable-next-line react-refresh/only-export-components
export function linkedInReceipt(session: DemoSession, roundNumber: number): string {
  const demoUrl = publicDemoUrl()
  const classified = classifyOutcome(session)
  if (
    isRoundFive(session)
    && session.towel
    && classified.outcome.outcome_id === 'one_sided_setup_verified_towel'
  ) {
    const exactLane = classified.evidence.exactLane
    if (!exactLane) throw new Error('Round 5 one-sided receipt has no exact setup lane.')
    const unfinishedLane: LaneId = exactLane === 'lakebase' ? 'competitor' : 'lakebase'
    const exact = classified.evidence[exactLane].exactMs
    const lowerBound = classified.evidence[unfinishedLane].lowerBoundMs
    const lowerBoundLabel = lowerBound === null ? 'NOT VERIFIED' : `>${laneReceiptTime(lowerBound)}`
    const exactName = session.round5_setup?.lanes?.[exactLane]?.name
      ?? session.lanes[exactLane].name
    const unfinishedName = session.round5_setup?.lanes?.[unfinishedLane]?.name
      ?? session.lanes[unfinishedLane].name
    const lines = [
      `${exactName} reached verified connection readiness in ${laneReceiptTime(exact)}. ${unfinishedName} was still unverified ${lowerBound === null ? 'without an exact lower bound' : `beyond ${laneReceiptTime(lowerBound)}`}. The shared spike did not run, so Round 5 declared no winner or margin. 🥊`,
      '',
      `${exactLane === 'lakebase' ? '🔴' : '🔵'} ${exactName} · ${laneReceiptTime(exact)} · exact setup verified`,
      `${unfinishedLane === 'lakebase' ? '🔴' : '🔵'} ${unfinishedName} · ${lowerBoundLabel} · unverified${lowerBound === null ? '' : ' lower bound'}`,
      '',
      `ROUND ${roundNumber} · ${ROUND_FIVE_DISPLAY_TITLE}`,
      classified.headline,
      `Receipt ${receiptId(session)} · Setup evidence only; the shared 128-attempt spike did not run · One live run, not a benchmark.`,
    ]
    if (demoUrl) lines.push(`Try the same round → ${demoUrl}`)
    lines.push('', '#Lakebase #PostgreSQL #AWS #Databricks')
    return lines.join('\n')
  }
  if (!classified.shareable) {
    return [
      `${classified.headline} 🥊`,
      '',
      `ROUND ${roundNumber} · ${session.round.title}`,
      'This outcome is not shareable until the round contract completes.',
      `Receipt ${receiptId(session)} · No unsupported winner or margin was inferred.`,
    ].join('\n')
  }
  if (isRoundFive(session)) {
    const lines = [
      linkedInHook(session),
      '',
      `🔴 Lakebase · ${roundFiveSetupSeconds(session, 'lakebase')} · built-in pooled host`,
      `🔵 ${session.competitor.short_name} + RDS Proxy · ${roundFiveSetupSeconds(session, 'competitor')} · AWS best-practice Proxy path`,
      '',
      `ROUND ${roundNumber} · ${ROUND_FIVE_DISPLAY_TITLE}`,
      roundFiveVerifiedVerdict(session),
      roundFiveSetupFairness(session),
      roundFiveIntegrityDetail(session),
      'Declared start · Lakebase built-in pooling · AWS best-practice RDS Proxy provisioned for this bout · An already-deployed Proxy would not pay this setup delay.',
      `Receipt ${receiptId(session)} · Readiness setup is scored; the identical 128-connection spike is pass/fail only · One live run, not a benchmark.`,
    ]
    if (demoUrl) lines.push(`Try the same round → ${demoUrl}`)
    lines.push('', '#Lakebase #PostgreSQL #AWS #Databricks')
    return lines.join('\n')
  }
  if (isRoundFour(session)) {
    const evidence = modelScoreEvidence(session.lanes.lakebase)
    const elapsed = roundFourAppElapsed(session)
    const lines = [
      linkedInHook(session),
      '',
      `🔴 Lakebase · Live app verified in ${elapsed} · built-in managed reverse ETL`,
      `🔵 ${session.competitor.short_name} · separate reverse-ETL stack required · not built or timed`,
      '',
      `ROUND ${roundNumber} · ${session.round.title}`,
      'One live OLAP → OLTP proof: Analytics Delta → managed reverse ETL → operational Lakebase Postgres → exact app read.',
      `Integrity · ${evidence.primaryKey} · risk score ${scoreText(evidence.score)} · model ${evidence.modelVersion} · Delta ${evidence.deltaVersion} · nonce ${evidence.proofNonce}`,
      '',
      'Aurora/RDS alone are OLTP sinks and do not move lakehouse data. The same outcome requires an added reverse-ETL stack with connectors, IAM/secrets, network access, mappings/upserts, checkpoints/retries, and monitoring.',
      'That added AWS stack was not built or timed; there is no honest AWS timer or speed margin.',
      `Receipt ${receiptId(session)} · One live managed reverse-ETL proof, not a benchmark.`,
    ]
    if (demoUrl) lines.push(`Try the same round → ${demoUrl}`)
    lines.push('', '#Lakebase #ReverseETL #PostgreSQL #Databricks')
    return lines.join('\n')
  }
  if (isRoundSix(session)) {
    const elapsed = metricValue(session, 'analytics_available_ms')?.value
    const elapsedLabel = typeof elapsed === 'number' ? laneReceiptTime(elapsed) : laneReceiptTime(session.lanes.lakebase.elapsed_ms)
    const lines = [
      linkedInHook(session),
      '',
      `🔴 Lakebase · Exact Delta answer in ${elapsedLabel} · native change feed`,
      `🔵 ${session.competitor.short_name} · separate CDC stack required · not built or timed`,
      '',
      `ROUND ${roundNumber} · ${session.round.title}`,
      'One exact live proof: checkout order committed → separate Delta history → 1 order / $84.50 answer.',
      'Guardrail · a separate checkout committed successfully while the analytical answer was verified.',
      '',
      'No throughput, p99 impact, AWS speed, or cost claim.',
      `Receipt ${receiptId(session)} · One live capability proof, not a benchmark.`,
    ]
    if (demoUrl) lines.push(`Try the same round → ${demoUrl}`)
    lines.push('', '#Lakebase #PostgreSQL #DeltaLake #Databricks')
    return lines.join('\n')
  }
  const lines = [
    linkedInHook(session),
    '',
    'No slides. No spin. Same task. Two live PostgreSQL databases.',
    '',
    `ROUND ${roundNumber} · ${session.round.title}`,
    `🔴 Lakebase · ${laneReceiptTime(session.lanes.lakebase.elapsed_ms)}`,
    `🔵 ${session.competitor.short_name} · ${competitorReceiptValue(session)}`,
    '',
    fairnessCopy(session.round.id),
    `Receipt ${receiptId(session)} · One live run, not a benchmark.`,
    '',
    "Don't trust this post. Ring the bell yourself.",
  ]
  if (demoUrl) lines.push(`Try the same round → ${demoUrl}`)
  lines.push('', '#Lakebase #PostgreSQL #AWS #Databricks')
  return lines.join('\n')
}

function isShareableIdleProof(session: DemoSession): boolean {
  const cooldown = session.cooldown
  if (!cooldown || cooldown.mode !== 'return_to_idle' || cooldown.state !== 'ready') return false
  const lakebase = cooldown.lanes.lakebase
  const competitor = cooldown.lanes.competitor
  return lakebase.state === 'confirmed_zero'
    && lakebase.elapsed_ms !== null
    && (
      (competitor.state === 'confirmed_zero' && competitor.elapsed_ms !== null)
      || competitor.state === 'not_supported'
    )
}

function idleReceiptWinner(session: DemoSession): ReceiptWinner {
  const cooldown = session.cooldown
  if (!cooldown || cooldown.lanes.competitor.state === 'not_supported') return 'lakebase'
  const lakebase = cooldown.lanes.lakebase.elapsed_ms
  const competitor = cooldown.lanes.competitor.elapsed_ms
  if (lakebase === null || competitor === null || Math.abs(lakebase - competitor) < 500) return 'tie'
  return lakebase < competitor ? 'lakebase' : 'competitor'
}

function idleReceiptVerdict(session: DemoSession): string {
  const cooldown = session.cooldown
  if (!cooldown) return 'NO IDLE RESULT DECLARED'
  const lakebase = cooldown.lanes.lakebase.elapsed_ms
  const competitor = cooldown.lanes.competitor.elapsed_ms
  if (cooldown.lanes.competitor.state === 'not_supported') {
    return 'LAKEBASE REACHED ZERO · RDS CANNOT'
  }
  if (lakebase === null || competitor === null) return 'NO IDLE RESULT DECLARED'
  const difference = compactDuration(Math.abs(lakebase - competitor))
  const winner = idleReceiptWinner(session)
  if (winner === 'tie') return 'BOTH RETURNED TO ZERO TOGETHER'
  return winner === 'lakebase'
    ? `LAKEBASE RETURNED TO ZERO ${difference} SOONER`
    : `${session.competitor.short_name.toUpperCase()} RETURNED TO ZERO ${difference} SOONER`
}

function idleMeasuredAt(session: DemoSession): string {
  const cooldown = session.cooldown
  if (!cooldown) return measuredAt(session)
  const candidates = Object.values(cooldown.lanes)
    .map((lane) => lane.confirmed_at)
    .filter((value): value is string => value !== null)
    .map((value) => new Date(value))
    .filter((value) => !Number.isNaN(value.getTime()))
  if (candidates.length === 0) return measuredAt(session)
  return new Date(Math.max(...candidates.map((value) => value.getTime()))).toISOString()
}

function idleFairnessCopy(session: DemoSession): string {
  return session.cooldown?.lanes.competitor.state === 'not_supported'
    ? 'Same reset bell · Lakebase clock stopped only at confirmed zero · RDS capability verified'
    : 'Same reset bell · Independent control planes · Each clock stopped only at confirmed zero'
}

function linkedInIdleReceipt(session: DemoSession): string {
  const cooldown = session.cooldown
  if (!cooldown || !isShareableIdleProof(session)) return ''
  const lakebaseMs = cooldown.lanes.lakebase.elapsed_ms!
  const competitorMs = cooldown.lanes.competitor.elapsed_ms
  const competitorUnsupported = cooldown.lanes.competitor.state === 'not_supported'
  const winner = idleReceiptWinner(session)
  let hook: string
  if (competitorUnsupported) {
    hook = `Lakebase returned to zero in ${compactDuration(lakebaseMs)}. ${session.competitor.short_name} has no automatic scale-to-zero. 🥊`
  } else if (competitorMs !== null && winner !== 'tie') {
    const leader = winner === 'lakebase' ? 'Lakebase' : session.competitor.short_name
    const trailer = winner === 'lakebase' ? session.competitor.short_name : 'Lakebase'
    hook = `${leader} returned to zero ${compactDuration(Math.abs(lakebaseMs - competitorMs))} before ${trailer}. Same reset bell. Two real control planes. 🥊`
  } else {
    hook = 'Lakebase and its opponent returned to zero together. Same reset bell. Two real control planes. 🥊'
  }
  const lines = [
    hook,
    '',
    `🔴 Lakebase · ${compactDuration(lakebaseMs)}`,
    `🔵 ${session.competitor.short_name} · ${competitorUnsupported ? 'NO AUTO SCALE-TO-ZERO' : compactDuration(competitorMs!)}`,
    '',
    "Don't trust this post. Ring the bell yourself.",
    `Receipt ${receiptId(session)} · One live run, not a benchmark.`,
  ]
  const demoUrl = publicDemoUrl()
  if (demoUrl) lines.push(`Try it → ${demoUrl}`)
  lines.push('', '#Lakebase #PostgreSQL #AWS #Databricks')
  return lines.join('\n')
}

// Exported for the all-surface contract matrix; it remains a pure renderer.
// eslint-disable-next-line react-refresh/only-export-components
export function receiptPresentation(
  session: DemoSession,
  kind: ReceiptKind,
): ReceiptPresentation {
  if (kind === 'idle') {
    const cooldown = session.cooldown
    if (!cooldown || !isShareableIdleProof(session)) {
      throw new Error('A verified back-to-idle result is not ready to share.')
    }
    const competitorUnsupported = cooldown.lanes.competitor.state === 'not_supported'
    return {
      kind,
      title: 'BACK TO IDLE',
      focus: 'SCALE TO ZERO',
      winner: idleReceiptWinner(session),
      lakebaseValue: cooldownTime(cooldown.lanes.lakebase.elapsed_ms!),
      competitorValue: competitorUnsupported
        ? 'NO AUTO SCALE-TO-ZERO'
        : cooldownTime(cooldown.lanes.competitor.elapsed_ms!),
      lakebaseStatus: 'IDLE CONFIRMED · CLOCK STOPPED',
      competitorStatus: competitorUnsupported
        ? 'NO AUTOMATIC SCALE-TO-ZERO'
        : 'IDLE CONFIRMED · CLOCK STOPPED',
      competitorCapabilityGap: competitorUnsupported,
      verdictLabel: 'SCALE-TO-ZERO RESULT DECLARED',
      verdict: idleReceiptVerdict(session),
      fairness: idleFairnessCopy(session),
      measuredAt: idleMeasuredAt(session),
      verifiedStamp: 'VERIFIED ZERO',
      receiptLabel: 'IDLE RECEIPT',
    }
  }
  const classified = classifyOutcome(session)
  if (session.towel) {
    return {
      kind,
      title: session.round.title,
      focus: priorityLabel(session.corners),
      winner: classified.formalWinner,
      lakebaseValue: towelLaneValue(session, 'lakebase'),
      competitorValue: towelLaneValue(session, 'competitor'),
      lakebaseStatus: towelVerifiedMs(session, 'lakebase') !== null ? 'EXACT VERIFIED' : towelLowerBoundMs(session, 'lakebase') !== null ? 'UNFINISHED · LOWER BOUND' : 'NO EXACT RESULT',
      competitorStatus: session.lanes.competitor.state === 'not_supported' ? 'NOT SUPPORTED · N/A' : towelVerifiedMs(session, 'competitor') !== null ? 'EXACT VERIFIED' : towelLowerBoundMs(session, 'competitor') !== null ? 'UNVERIFIED WHEN STOPPED · LOWER BOUND' : 'NO EXACT RESULT',
      competitorCapabilityGap: classified.evidence.shape === 'capability_gap',
      verdictLabel: 'TOWEL RESULT · THIS ROUND',
      verdict: classified.headline,
      fairness: fairnessCopy(session.round.id),
      measuredAt: session.towel.requested_at,
      verifiedStamp: 'TOWELED LIVE',
      receiptLabel: 'BOUT RECEIPT',
    }
  }
  if (isRoundFive(session)) {
    const lakebase = session.round5_setup?.lanes?.lakebase
    const competitor = session.round5_setup?.lanes?.competitor
    return {
      kind,
      title: ROUND_FIVE_DISPLAY_TITLE,
      focus: 'READINESS SETUP · SPIKE PASS/FAIL',
      winner: classified.formalWinner,
      lakebaseLabel: lakebase?.name ?? 'Lakebase',
      competitorLabel: competitor?.name ?? session.lanes.competitor.name,
      lakebaseValue: roundFiveSetupSeconds(session, 'lakebase'),
      competitorValue: roundFiveSetupSeconds(session, 'competitor'),
      lakebaseStatus: `EXACT SETUP STOP · ${lakebase?.status ?? 'VERIFIED'}`,
      competitorStatus: `EXACT SETUP STOP · ${competitor?.status ?? 'VERIFIED'}`,
      competitorCapabilityGap: false,
      verdictLabel: classified.contractComplete
        ? 'READINESS RESULT DECLARED · SPIKE PASSED'
        : 'READINESS COMPARISON INCOMPLETE',
      verdict: classified.headline,
      fairness: roundFiveSetupFairness(session),
      measuredAt: measuredAt(session),
      verifiedStamp: classified.contractComplete ? 'SETUP VERIFIED' : 'NOT DECLARED',
      receiptLabel: 'SPIKE-READINESS RECEIPT',
      integrityDetail: classified.contractComplete
        ? roundFiveIntegrityDetail(session)
        : undefined,
    }
  }
  if (isRoundFour(session)) {
    const evidence = modelScoreEvidence(session.lanes.lakebase)
    const elapsed = roundFourAppElapsed(session)
    return {
      kind,
      title: 'ANALYTICS CHANGE → LIVE APP',
      focus: 'REVERSE ETL · OLAP → OLTP',
      winner: classified.formalWinner,
      lakebaseValue: elapsed,
      competitorValue: 'SEPARATE REVERSE-ETL STACK REQUIRED',
      lakebaseStatus: classified.contractComplete
        ? 'LIVE APP VERIFIED · BUILT-IN MANAGED REVERSE ETL'
        : 'APP READ OBSERVED · SCORE IDENTITY NOT VERIFIED',
      competitorStatus: 'NOT BUILT OR TIMED · NO HONEST TIMER · NO SPEED MARGIN',
      competitorCapabilityGap: true,
      verdictLabel: classified.contractComplete
        ? 'OLAP → OLTP OUTCOME DECLARED'
        : 'OLAP → OLTP OUTCOME INCOMPLETE',
      verdict: classified.headline,
      fairness: fairnessCopy(session.round.id),
      measuredAt: measuredAt(session),
      verifiedStamp: classified.contractComplete ? 'LIVE APP VERIFIED' : 'NOT DECLARED',
      receiptLabel: 'REVERSE-ETL RECEIPT',
      integrityDetail: `INTEGRITY · CUSTOMER ${evidence.primaryKey} · RISK ${scoreText(evidence.score)} · MODEL ${evidence.modelVersion} · DELTA ${evidence.deltaVersion} · NONCE ${evidence.proofNonce}`,
    }
  }
  if (isRoundSix(session)) {
    const elapsed = metricValue(session, 'analytics_available_ms')?.value
    const elapsedLabel = typeof elapsed === 'number'
      ? laneReceiptTime(elapsed)
      : laneReceiptTime(session.lanes.lakebase.elapsed_ms)
    return {
      kind,
      title: 'CHECKOUT → EXACT DELTA ANSWER',
      focus: 'LIVE ORDERS → TRUSTED ANALYTICS',
      winner: classified.formalWinner,
      lakebaseValue: elapsedLabel,
      competitorValue: 'SEPARATE CDC STACK REQUIRED',
      lakebaseStatus: classified.contractComplete
        ? 'ORDER INCLUDED · COUNT VERIFIED · SEPARATE CHECKOUT COMMITTED'
        : 'DELTA ANSWER OBSERVED · SEPARATE CHECKOUT NOT VERIFIED',
      competitorStatus: 'NOT BUILT OR TIMED · NO HONEST TIMER · NO SPEED MARGIN',
      competitorCapabilityGap: true,
      verdictLabel: classified.contractComplete
        ? 'LIVE ANALYTICAL OUTCOME DECLARED'
        : 'LIVE ANALYTICAL OUTCOME INCOMPLETE',
      verdict: classified.headline,
      fairness: fairnessCopy(session.round.id),
      measuredAt: measuredAt(session),
      verifiedStamp: classified.contractComplete ? 'EXACT ANSWER VERIFIED' : 'NOT DECLARED',
      receiptLabel: 'LIVE-ORDERS RECEIPT',
      integrityDetail: classified.contractComplete
        ? 'ORDER INCLUDED ✓ · COUNT VERIFIED ✓ · SEPARATE CHECKOUT COMMITTED ✓'
        : 'SEPARATE CHECKOUT NOT VERIFIED · NO CAPABILITY RESULT DECLARED',
    }
  }
  return {
    kind,
    title: session.round.title,
    focus: priorityLabel(session.corners),
    winner: classified.formalWinner,
    lakebaseValue: laneReceiptTime(session.lanes.lakebase.elapsed_ms),
    competitorValue: competitorReceiptValue(session),
    lakebaseStatus: session.lanes.lakebase.status,
    competitorStatus: session.lanes.competitor.status,
    competitorCapabilityGap: session.lanes.competitor.state === 'not_supported',
    verdictLabel: classified.contractComplete
      ? 'RESULT DECLARED · THIS ROUND'
      : 'NO RESULT DECLARED · THIS ROUND',
    verdict: classified.headline,
    fairness: fairnessCopy(session.round.id),
    measuredAt: measuredAt(session),
    verifiedStamp: classified.contractComplete ? 'VERIFIED LIVE' : 'NOT DECLARED',
    receiptLabel: 'BOUT RECEIPT',
  }
}

function canvasLines(
  context: CanvasRenderingContext2D,
  text: string,
  maxWidth: number,
): string[] {
  const words = text.split(/\s+/)
  const lines: string[] = []
  let current = ''
  for (const word of words) {
    const candidate = current ? `${current} ${word}` : word
    if (current && context.measureText(candidate).width > maxWidth) {
      lines.push(current)
      current = word
    } else {
      current = candidate
    }
  }
  if (current) lines.push(current)
  return lines
}

function drawFittedCanvasText(
  context: CanvasRenderingContext2D,
  text: string,
  options: {
    x: number
    y: number
    maxWidth: number
    maxLines: number
    startSize: number
    minSize: number
    color: string
    align?: CanvasTextAlign
    lineHeight?: number
  },
): number {
  let size = options.startSize
  let lines: string[] = []
  while (size >= options.minSize) {
    context.font = `400 ${size}px "Press Start 2P", monospace`
    lines = canvasLines(context, text, options.maxWidth)
    if (lines.length <= options.maxLines) break
    size -= 1
  }
  const lineHeight = options.lineHeight ?? size * 1.28
  context.fillStyle = options.color
  context.textAlign = options.align ?? 'left'
  lines.forEach((line, index) => context.fillText(line, options.x, options.y + index * lineHeight))
  context.textAlign = 'left'
  return options.y + lines.length * lineHeight
}

/**
 * The ledger's winner field, painted into one cell of the share card.
 *
 * Same composition as the screen -- corner chip, name, figure, qualifier, and
 * the lane fact underneath -- because this is the same scorecard and the two
 * must not disagree. The split matters more here than on the screen: the image
 * travels without an operator, so the qualifier has to sit beside the figure
 * where nobody can read the number without it, and the lane fact has to be
 * present in full rather than paraphrased down to something that fits.
 *
 * Returns nothing: the caller owns the cell's geometry.
 */
function drawCardWinner(
  context: CanvasRenderingContext2D,
  verdict: LedgerVerdict,
  day: string | null,
  box: { x: number; y: number; width: number },
) {
  const right = box.x + box.width

  if (verdict.winner) {
    // The chip, then the name. Coloured by corner rather than hardcoded red, so
    // a blue-corner win would not be printed in the home corner's colour.
    const chipWidth = 30
    context.fillStyle = verdict.winner.badge === 'LB' ? '#e8482e' : '#4a83e8'
    context.fillRect(box.x, box.y, chipWidth, 15)
    context.fillStyle = '#fff4c2'
    context.font = '400 8px "Press Start 2P", monospace'
    context.textAlign = 'center'
    context.fillText(verdict.winner.badge, box.x + chipWidth / 2, box.y + 4)
    context.textAlign = 'left'
    context.font = '400 11px "Press Start 2P", monospace'
    context.fillStyle = '#fff4c2'
    context.fillText(verdict.winner.name, box.x + chipWidth + 9, box.y + 3)
  } else {
    // No winner: the outcome takes the whole field rather than leaving a blank
    // where a name would go, which would read as a result withheld.
    drawFittedCanvasText(context, verdict.outcome ?? 'NO RESULT DECLARED', {
      x: box.x, y: box.y + 3, maxWidth: box.width, maxLines: 1,
      startSize: 11, minSize: 8, color: '#8f9dcb',
    })
  }

  // The figure sits hard right, the one number on the row, so the eye finds it
  // without reading the name first.
  if (verdict.figure) {
    context.font = '400 13px "Press Start 2P", monospace'
    context.fillStyle = '#6bf39a'
    context.textAlign = 'right'
    context.fillText(verdict.figure, right, box.y + 1)
    context.textAlign = 'left'
  }

  /**
   * Qualifier and date on one line: both are conditions on the figure above.
   *
   * Set at the name's size, NOT at the lane note's. This image is read at about
   * 46% in a feed, where 8px stops being words -- and a figure that reads at
   * feed scale with a qualifier that does not is the same defect as printing the
   * figure bare. Tying the two sizes together means nobody can take the number
   * off this card without also taking the condition on it.
   */
  const tokens = [verdict.qualifier, day].filter((token): token is string => !!token)
  if (tokens.length > 0) {
    drawFittedCanvasText(context, tokens.join(' · '), {
      x: box.x, y: box.y + 19, maxWidth: box.width, maxLines: 1,
      startSize: 11, minSize: 9, color: '#f8d83b',
    })
  }

  // The lane fact, in full, at the same size as the round's own proof note. Two
  // lines is the budget and nothing is abbreviated to fit it: the wording IS
  // the disclosure. It reads at full size, not in a feed -- see the report.
  if (verdict.laneNote) {
    drawFittedCanvasText(context, verdict.laneNote, {
      x: box.x, y: box.y + 34, maxWidth: box.width, maxLines: 2,
      startSize: 8, minSize: 7, color: '#aeb9df', lineHeight: 9,
    })
  }
}

function drawPixelFighter(
  context: CanvasRenderingContext2D,
  x: number,
  y: number,
  color: string,
  label: string,
) {
  context.fillStyle = '#fff4c2'
  context.fillRect(x + 17, y, 46, 9)
  context.fillStyle = color
  context.fillRect(x + 10, y + 12, 60, 18)
  context.fillStyle = '#dca36f'
  context.fillRect(x + 18, y + 30, 44, 32)
  context.fillStyle = '#070b22'
  context.fillRect(x + 25, y + 40, 7, 7)
  context.fillRect(x + 48, y + 40, 7, 7)
  context.fillStyle = color
  context.fillRect(x + 12, y + 64, 56, 44)
  context.fillRect(x, y + 70, 17, 24)
  context.fillRect(x + 63, y + 70, 17, 24)
  context.fillStyle = '#fff4c2'
  context.font = '400 10px "Press Start 2P", monospace'
  context.textAlign = 'center'
  context.fillText(label, x + 40, y + 79)
  context.textAlign = 'left'
}

async function renderReceiptCard(
  session: DemoSession,
  roundNumber: number,
  kind: ReceiptKind,
): Promise<Blob> {
  const receipt = receiptPresentation(session, kind)
  if (document.fonts) await document.fonts.ready
  const canvas = document.createElement('canvas')
  canvas.width = 1200
  canvas.height = 627
  const context = canvas.getContext('2d')
  if (!context) throw new Error('This browser cannot create the result card.')
  context.imageSmoothingEnabled = false
  context.fillStyle = '#070b22'
  context.fillRect(0, 0, canvas.width, canvas.height)
  context.fillStyle = '#10183e'
  for (let x = 0; x < canvas.width; x += 32) {
    if ((x / 32) % 2 === 0) context.fillRect(x, 0, 16, canvas.height)
  }
  context.strokeStyle = '#f8d83b'
  context.lineWidth = 12
  context.strokeRect(12, 12, 1176, 603)
  context.strokeStyle = '#e8482e'
  context.lineWidth = 5
  context.strokeRect(29, 29, 1142, 569)

  context.textBaseline = 'top'
  context.fillStyle = '#e8482e'
  context.font = '400 30px "Press Start 2P", monospace'
  context.fillText('LAKEBASE', 58, 38)
  context.fillStyle = '#fff4c2'
  context.fillText('LAKEBASE', 54, 34)
  context.fillStyle = '#f8d83b'
  context.font = '400 16px "Press Start 2P", monospace'
  context.fillText('THE ANTI-DEMO', 55, 78)

  context.fillStyle = '#0a2c22'
  context.fillRect(862, 36, 274, 73)
  context.strokeStyle = '#6bf39a'
  context.lineWidth = 5
  context.strokeRect(862, 36, 274, 73)
  context.fillStyle = '#6bf39a'
  context.font = '400 15px "Press Start 2P", monospace'
  context.textAlign = 'center'
  context.fillText(receipt.verifiedStamp, 999, 51)
  context.fillStyle = '#fff4c2'
  context.font = '400 10px "Press Start 2P", monospace'
  context.fillText(`${receipt.receiptLabel} ${receiptId(session)}`, 999, 82)
  context.textAlign = 'left'

  context.fillStyle = '#e8482e'
  context.fillRect(38, 128, 146, 38)
  context.fillStyle = '#fff4c2'
  context.font = '400 13px "Press Start 2P", monospace'
  context.fillText(`ROUND ${String(roundNumber).padStart(2, '0')}`, 53, 140)
  drawFittedCanvasText(context, receipt.title.toUpperCase(), {
    x: 205, y: 137, maxWidth: 620, maxLines: 1, startSize: 21, minSize: 13, color: '#fff4c2',
  })
  context.fillStyle = '#f8d83b'
  context.font = '400 10px "Press Start 2P", monospace'
  context.textAlign = 'right'
  context.fillText(`FOCUS · ${receipt.focus}`, 1140, 142)
  context.textAlign = 'left'

  const winner = receipt.winner
  const laneY = 177
  const laneWidth = 548
  const laneHeight = 200
  const drawLane = (
    x: number,
    color: string,
    label: string,
    value: string,
    status: string,
    fighter: string,
    isWinner: boolean,
    capabilityGap = false,
  ) => {
    context.fillStyle = color === '#e8482e' ? '#27132c' : '#111e48'
    context.fillRect(x, laneY, laneWidth, laneHeight)
    context.fillStyle = color
    context.fillRect(x, laneY, laneWidth, 10)
    context.globalAlpha = 0.15
    context.fillStyle = '#fff4c2'
    context.fillRect(x, laneY + 61, laneWidth, 5)
    context.fillStyle = '#f8d83b'
    context.fillRect(x, laneY + 91, laneWidth, 5)
    context.fillStyle = '#4a83e8'
    context.fillRect(x, laneY + 121, laneWidth, 5)
    context.globalAlpha = 1
    drawPixelFighter(context, x + 25, laneY + 47, color, fighter)
    drawFittedCanvasText(context, label.toUpperCase(), {
      x: x + 125, y: laneY + 28, maxWidth: 380, maxLines: 2, startSize: 17, minSize: 11, color,
    })
    drawFittedCanvasText(context, value, {
      x: x + 125,
      y: laneY + 88,
      maxWidth: 382,
      maxLines: capabilityGap ? 2 : 1,
      startSize: capabilityGap ? 20 : 43,
      minSize: 14,
      color: '#fff4c2',
    })
    drawFittedCanvasText(context, status.toUpperCase(), {
      x: x + 125, y: laneY + 155, maxWidth: 390, maxLines: 2, startSize: 9, minSize: 7, color: '#aeb9df',
    })
    if (isWinner) {
      context.fillStyle = '#f8d83b'
      context.fillRect(x + laneWidth - 126, laneY + 17, 102, 26)
      context.fillStyle = '#070b22'
      context.font = '400 9px "Press Start 2P", monospace'
      context.textAlign = 'center'
      context.fillText(
        isRoundFive(session) ? winner === 'tie' ? 'TIED' : 'EARLIER' : 'WINNER',
        x + laneWidth - 75,
        laneY + 25,
      )
      context.textAlign = 'left'
    }
  }
  drawLane(
    38, '#e8482e', 'Lakebase', receipt.lakebaseValue,
    receipt.lakebaseStatus, 'LB', winner === 'lakebase' || winner === 'tie',
  )
  drawLane(
    614, '#4a83e8', session.competitor.short_name, receipt.competitorValue,
    receipt.competitorStatus,
    session.competitor.id === 'aurora_serverless_v2' ? 'AUR' : 'RDS',
    winner === 'competitor' || winner === 'tie',
    receipt.competitorCapabilityGap,
  )

  context.save()
  context.translate(600, 276)
  context.rotate(Math.PI / 4)
  context.fillStyle = '#e8482e'
  context.fillRect(-37, -37, 74, 74)
  context.strokeStyle = '#f8d83b'
  context.lineWidth = 7
  context.strokeRect(-37, -37, 74, 74)
  context.restore()
  context.fillStyle = '#fff4c2'
  context.font = '400 16px "Press Start 2P", monospace'
  context.textAlign = 'center'
  context.textBaseline = 'middle'
  context.fillText('VS', 600, 276)
  context.textBaseline = 'top'
  context.textAlign = 'left'

  context.fillStyle = '#e8482e'
  context.fillRect(47, 395, 1115, 86)
  context.fillStyle = '#f8d83b'
  context.fillRect(38, 386, 1115, 86)
  context.fillStyle = '#070b22'
  context.font = '400 9px "Press Start 2P", monospace'
  context.fillText(receipt.verdictLabel, 59, 400)
  drawFittedCanvasText(context, receipt.verdict, receipt.integrityDetail
    ? { x: 59, y: 420, maxWidth: 1060, maxLines: 1, startSize: 26, minSize: 17, color: '#070b22' }
    : { x: 59, y: 426, maxWidth: 1060, maxLines: 2, startSize: 24, minSize: 14, color: '#070b22' })
  if (receipt.integrityDetail) {
    drawFittedCanvasText(context, receipt.integrityDetail, {
      x: 59, y: 454, maxWidth: 1060, maxLines: 2, startSize: 7, minSize: 6, color: '#070b22', lineHeight: 9,
    })
  }

  context.fillStyle = '#f1ebd7'
  context.fillRect(38, 491, 1115, 106)
  context.fillStyle = '#070b22'
  for (let x = 52; x < 1136; x += 24) context.fillRect(x, 491, 12, 5)
  context.font = '400 8px "Press Start 2P", monospace'
  context.fillText('FAIR-START CONTRACT', 58, 509)
  drawFittedCanvasText(context, receipt.fairness.toUpperCase(), {
    x: 58, y: 528, maxWidth: 735, maxLines: 2, startSize: 10, minSize: 8, color: '#070b22',
  })
  const skew = receiptStartSkewDisplay(session)
  const auditStart = kind === 'idle'
    ? `RESET ${session.cooldown!.started_at}`
    : `START GAP ${skew}`
  context.font = '400 7px "Press Start 2P", monospace'
  context.fillText(`${auditStart} · ${receipt.measuredAt} · RECEIPT ${receiptId(session)}`, 58, 574)

  context.strokeStyle = '#e8482e'
  context.lineWidth = 5
  context.strokeRect(818, 509, 311, 70)
  context.fillStyle = '#e8482e'
  context.font = '400 12px "Press Start 2P", monospace'
  context.textAlign = 'center'
  context.fillText('ONE LIVE RUN', 973, 526)
  context.fillStyle = '#070b22'
  context.font = '400 9px "Press Start 2P", monospace'
  context.fillText('NOT A BENCHMARK', 973, 551)
  context.textAlign = 'left'

  return new Promise<Blob>((resolve, reject) => {
    canvas.toBlob((value) => value ? resolve(value) : reject(new Error('The result card could not be encoded.')), 'image/png')
  })
}

async function downloadReceiptCard(
  session: DemoSession,
  roundNumber: number,
  kind: ReceiptKind,
  rendered?: Blob,
): Promise<void> {
  const blob = rendered ?? await renderReceiptCard(session, roundNumber, kind)
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = receiptCardFilename(session, roundNumber, kind)
  link.click()
  window.setTimeout(() => URL.revokeObjectURL(url), 0)
}

function receiptCardFilename(session: DemoSession, roundNumber: number, kind: ReceiptKind): string {
  return kind === 'idle'
    ? `lakebase-anti-demo-idle-receipt-${receiptId(session)}.png`
    : `lakebase-anti-demo-round-${roundNumber}-receipt-${receiptId(session)}.png`
}

function finaleElapsed(session: DemoSession): string {
  const metric = metricValue(session, 'analytics_available_ms')?.value
  const milliseconds = typeof metric === 'number' ? metric : session.lanes.lakebase.elapsed_ms
  return milliseconds === null ? '—' : preciseDuration(milliseconds)
}

function finaleCaption(session: DemoSession): string {
  return [
    'Six proof contracts. One full data loop.',
    '',
    '01 · Wake from zero → exact application transaction',
    '02 · Branch safely → isolated schema change, source untouched',
    '03 · Recover exactly → deleted row restored, source deletion preserved',
    '04 · Analytics Delta → verified live application row',
    '05 · Built-in pooling → readiness verified, identical connection spike passed',
    `06 · Live checkout → exact Delta answer in ${finaleElapsed(session)}`,
    '',
    'Rounds 4 and 6 are capability proofs; the added AWS data-movement stacks were not built or timed. Round 5 scores declared-start readiness, not burst speed; an existing RDS Proxy would start ready.',
    '',
    `Latest live proof: Round 6 produced the exact Delta answer in ${finaleElapsed(session)} while a separate checkout committed.`,
    'Each result is one observed run, not a benchmark. Ring the bell yourself. 🥊',
    '',
    '#Lakebase #Databricks #PostgreSQL',
  ].join('\n')
}

function finaleCardFilename(session: DemoSession): string {
  return `lakebase-anti-demo-six-round-finale-${receiptId(session)}.png`
}

/**
 * The shareable card: the same six rounds the ledger scores, and now the same
 * winners.
 *
 * `results` is the record off disk, keyed by round. `recordRead` distinguishes
 * a record that says nothing from one that could not be read -- printing "not
 * run yet" against a round that did run because a fetch failed would be the
 * worst kind of wrong on an artefact that travels without a correction.
 */
async function renderFinaleCard(
  session: DemoSession,
  results: Map<RoundId, RoundResult>,
  recordRead: boolean,
): Promise<Blob> {
  if (document.fonts) await document.fonts.ready
  const canvas = document.createElement('canvas')
  canvas.width = 1200
  canvas.height = 627
  const context = canvas.getContext('2d')
  if (!context) throw new Error('This browser cannot create the final card.')
  context.imageSmoothingEnabled = false
  context.textBaseline = 'top'
  context.fillStyle = '#070b22'
  context.fillRect(0, 0, canvas.width, canvas.height)
  context.fillStyle = '#10183e'
  for (let x = 0; x < canvas.width; x += 32) {
    if ((x / 32) % 2 === 0) context.fillRect(x, 0, 16, canvas.height)
  }
  context.strokeStyle = '#f8d83b'
  context.lineWidth = 12
  context.strokeRect(12, 12, 1176, 603)
  context.strokeStyle = '#e8482e'
  context.lineWidth = 5
  context.strokeRect(29, 29, 1142, 569)

  context.fillStyle = '#f8d83b'
  context.font = '400 11px "Press Start 2P", monospace'
  context.fillText('FINAL BELL · THE SIX-ROUND STORY', 52, 47)
  // The same heading the screen carries. "ONE DATA LOOP." was cut from the
  // screen and this is the same artefact, so it is cut here too; the claims
  // strip lower down is NOT, because this image travels without the operator
  // and without the fight card beside it to supply the caveat.
  drawFittedCanvasText(context, 'SIX ROUNDS.', {
    x: 52, y: 78, maxWidth: 1090, maxLines: 1, startSize: 34, minSize: 25, color: '#fff4c2',
  })
  context.fillStyle = '#6bf39a'
  context.font = '400 12px "Press Start 2P", monospace'
  context.fillText(`LIVE APP → EXACT DELTA ANSWER · ${finaleElapsed(session)}`, 54, 122)

  const colors = { red: '#e8482e', blue: '#4a83e8', yellow: '#f8d83b' }
  /**
   * The grid keeps its shape -- three across, two down, same width, same gutter
   * -- and grows downwards to carry the winner field. The 36 units that costs
   * are taken from slack: the gap under the header, the gap between the rows,
   * and the band of empty frame under the claims strip. Nothing legible was
   * given up for it, and the heading kept its size because it is the one thing
   * on this card that still reads at feed scale.
   */
  const cardWidth = 350
  const cardHeight = 194
  const cardGap = 22
  const rowGap = 8
  const startX = 52
  const startY = 144
  FINALE_BEATS.forEach((beat, index) => {
    const column = index % 3
    const row = Math.floor(index / 3)
    const x = startX + column * (cardWidth + cardGap)
    const y = startY + row * (cardHeight + rowGap)
    const accent = colors[beat.accent]
    context.fillStyle = '#0b1230'
    context.fillRect(x, y, cardWidth, cardHeight)
    context.fillStyle = accent
    context.fillRect(x, y, cardWidth, 8)
    context.strokeStyle = '#46527c'
    context.lineWidth = 2
    context.strokeRect(x, y, cardWidth, cardHeight)
    context.fillStyle = '#070b22'
    context.fillRect(x + 16, y + 18, 43, 32)
    context.fillStyle = accent
    context.font = '400 12px "Press Start 2P", monospace'
    context.fillText(beat.number, x + 23, y + 28)
    drawFittedCanvasText(context, beat.title.toUpperCase(), {
      x: x + 73, y: y + 20, maxWidth: 255, maxLines: 2, startSize: 13, minSize: 9, color: '#fff4c2', lineHeight: 17,
    })
    drawFittedCanvasText(context, beat.flow.toUpperCase(), {
      x: x + 17, y: y + 62, maxWidth: 316, maxLines: 2, startSize: 12, minSize: 9, color: accent, lineHeight: 16,
    })
    drawFittedCanvasText(context, beat.proof.toUpperCase(), {
      x: x + 17, y: y + 100, maxWidth: 316, maxLines: 2, startSize: 8, minSize: 6, color: '#aeb9df', lineHeight: 11,
    })

    // The rule separates what the round was from who took it, the same job the
    // ledger's winner column does with a border.
    context.fillStyle = '#2b376c'
    context.fillRect(x + 17, y + 128, 316, 2)
    const result = results.get(beat.roundId) ?? null
    const verdict = verdictFor(result, recordRead ? 'read' : 'unread')
    drawCardWinner(context, verdict, recordRead && result ? ledgerDay(result) : null, {
      x: x + 17, y: y + 136, width: 316,
    })
  })

  // Tightened from 58 to 46 and moved down into the band of empty frame it used
  // to sit above. The wording is untouched: with winners now on the image the
  // case for this strip is stronger than it was, so it gave up padding rather
  // than words. Bottom edge stays inside the inner rule at 598.
  context.fillStyle = '#f1ebd7'
  context.fillRect(52, 546, 1094, 46)
  context.fillStyle = '#070b22'
  context.font = '400 9px "Press Start 2P", monospace'
  context.fillText('PROOF CONTRACTS NAME EXACT STOP GATES', 72, 556)
  context.fillStyle = '#e8482e'
  context.fillText('CAPABILITY GAPS SAY NOT TIMED · NOT A BENCHMARK', 72, 573)
  context.fillStyle = '#070b22'
  context.textAlign = 'right'
  context.fillText('LAKEBASE · THE ANTI-DEMO', 1126, 564)
  context.textAlign = 'left'

  return new Promise<Blob>((resolve, reject) => {
    canvas.toBlob((value) => value ? resolve(value) : reject(new Error('The final card could not be encoded.')), 'image/png')
  })
}

/**
 * `rendered` is required rather than optional: the card now needs the record to
 * name its winners, and a convenience path that re-rendered without it would
 * quietly produce a scorecard reading "not run yet" against six rounds that ran.
 */
async function downloadFinaleCard(session: DemoSession, rendered: Blob): Promise<void> {
  const url = URL.createObjectURL(rendered)
  const link = document.createElement('a')
  link.href = url
  link.download = finaleCardFilename(session)
  link.click()
  window.setTimeout(() => URL.revokeObjectURL(url), 0)
}

function withCooldown(entry: ScorecardEntry, cooldown: CooldownSnapshot): ScorecardEntry {
  return {
    ...entry,
    cooldown: {
      mode: cooldown.mode,
      lakebase_ms: cooldown.lanes.lakebase.elapsed_ms,
      competitor_ms: cooldown.lanes.competitor.elapsed_ms,
      lakebase_state: cooldown.lanes.lakebase.state,
      competitor_state: cooldown.lanes.competitor.state,
    },
  }
}

function App() {
  const reducedMotion = useReducedMotion()
  const [initialProgress] = useState(loadSetupProgress)
  const uiReview = useMemo(() => {
    const localHost = window.location.hostname === '127.0.0.1' || window.location.hostname === 'localhost'
    return localHost && new URLSearchParams(window.location.search).get('review') === '1'
  }, [])
  const [initialActiveSession] = useState(() => uiReview ? null : loadActiveSessionPointer())
  const [catalog, setCatalog] = useState<CatalogResponse>(FALLBACK_CATALOG)
  const [apiStatus, setApiStatus] = useState<ApiStatus>('checking')
  const [stage, setStage] = useState<Stage>(initialProgress.stage)
  const [setupScene, setSetupScene] = useState<SetupScene>(initialProgress.setupScene)
  const [competitor, setCompetitor] = useState<CompetitorId>(initialProgress.competitor)
  const [corners, setCorners] = useState<CustomerCorner[]>(initialProgress.corners)
  const [primary, setPrimary] = useState<PersonaId>(initialProgress.primary)
  const [secondary, setSecondary] = useState<PersonaId[]>(initialProgress.secondary)
  const [roundOverride, setRoundOverride] = useState<RoundId | null>(initialProgress.roundOverride)
  const [session, setSession] = useState<DemoSession | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [liveEvidenceConnected, setLiveEvidenceConnected] = useState(true)
  // How many events the play-by-play never got to show. The server reports this
  // on the first event of a resume it had to serve past its retention floor, and
  // the play-by-play would otherwise render the hole with no sign of it.
  const [missedCalls, setMissedCalls] = useState(0)
  const [armStatus, setArmStatus] = useState('Verifying the sealed start state…')
  const [activeBout, setActiveBout] = useState<BoutStatus | null>(null)
  const [boutBoard, setBoutBoard] = useState<AllBoutStatus | null>(null)
  const [boutBoardFresh, setBoutBoardFresh] = useState(false)
  const [sound, setSound] = useState(initialProgress.sound)
  const [titleMusicPlaying, setTitleMusicPlaying] = useState(false)
  const [scorecard, setScorecard] = useState<ScorecardEntry[]>(loadScorecard)
  const [commentaryOpen, setCommentaryOpen] = useState(true)
  const [redoPending, setRedoPending] = useState(false)
  const [roundFiveCleanupPending, setRoundFiveCleanupPending] = useState(false)
  const [armCancelPending, setArmCancelPending] = useState(false)
  const [sessionRestorePending, setSessionRestorePending] = useState(Boolean(initialActiveSession))
  const [sessionRestoreError, setSessionRestoreError] = useState<string | null>(null)
  const sessionRef = useRef<DemoSession | null>(null)
  const stageRef = useRef<Stage>(stage)
  const resumeStageRef = useRef<Stage>(initialActiveSession?.resumeStage ?? stage)
  const audioUnlockedRef = useRef(false)
  const titleMusicActiveRef = useRef(false)
  const transitionRef = useRef(0)
  const ringPendingRef = useRef(false)
  const preparePendingRef = useRef(false)
  /* reconcileRunStart polls on a bare timer rather than inside an effect, so it
     has no cleanup of its own and would otherwise keep reading the session for
     up to twelve seconds after the app that started it is gone. */
  const mountedRef = useRef(true)
  const [ringBlocked, setRingBlocked] = useState(false)

  const navigate = useCallback((
    nextStage: Stage,
    nextScene: SetupScene,
    mode: HistoryMode = 'push',
  ) => {
    writeBrowserView({ stage: nextStage, setupScene: nextScene }, mode)
    setStage(nextStage)
    setSetupScene(nextScene)
  }, [])

  /* The one mute setter, shared by the fight card and every arena screen, so
     "sound" means the same thing wherever it is pressed and persists the same
     way -- saveSetupProgress below is keyed on the flag, not on the control.
     Stable identity, so toggling re-renders the arena without ever giving
     React a reason to remount it. */
  const toggleSound = useCallback(() => setSound((current) => !current), [])

  const recommendation = useMemo(
    () => recommend(catalog, competitor, corners, primary),
    [catalog, competitor, corners, primary],
  )
  const selectedRoundId = roundOverride ?? recommendation.round_id
  const roundStatuses = boutBoard?.rounds ?? null
  const ringStatus = roundStatuses?.[selectedRoundId] ?? null
  const selectedCompetitor = catalog.competitors.find((item) => item.id === competitor) ?? FALLBACK_CATALOG.competitors[0]
  const currentRoundIndex = session
    ? catalog.rounds.findIndex((round) => round.id === session.round.id)
    : -1
  const nextRound = currentRoundIndex >= 0
    ? catalog.rounds.find((round, index) => (
        index > currentRoundIndex
        && round.competitors.includes(session?.competitor.id ?? competitor)
        && round.availability === 'ready'
        && (uiReview || roundStatuses?.[round.id]?.can_start === true)
      ))
    : undefined
  const selectedPersonas = [primary, ...secondary]
    .map((id) => catalog.personas.find((persona) => persona.id === id))
    .filter((persona): persona is NonNullable<typeof persona> => Boolean(persona))
  const sessionId = session?.id
  const terminalCommentary = session?.state === 'verified'
    && (
      session.round.id === 'survive_connection_spike'
      || session.round.id === 'analyze_live_orders_without_slowing_checkout'
    )

  useEffect(() => {
    sessionRef.current = session
  }, [session])

  useEffect(() => {
    if (!terminalCommentary) return
    let active = true
    queueMicrotask(() => {
      if (active) setCommentaryOpen(true)
    })
    return () => { active = false }
  }, [terminalCommentary, sessionId])

  useEffect(() => {
    stageRef.current = stage
    if (requiresSession(stage)) resumeStageRef.current = stage
  }, [stage])

  useEffect(() => {
    if (uiReview || sessionRestorePending) return
    if (session && (requiresSession(stage) || stage === 'title')) {
      saveActiveSessionPointer({
        id: session.id,
        stage,
        resumeStage: stage === 'title' ? resumeStageRef.current : stage,
      })
      return
    }
    saveActiveSessionPointer(null)
  }, [session, sessionRestorePending, stage, uiReview])

  /* `sound` is deliberately not a dependency here, and not part of the guard.
   * Leaving the title screen stops the attract loop; muting does not, because
   * a restart would drop it back to bar one. The title screen's own Music
   * button still stops it outright -- that button IS a transport control, and
   * it calls stopOriginalTitleTheme itself. */
  useEffect(() => {
    if (stage !== 'title') {
      stopOriginalTitleTheme()
      titleMusicActiveRef.current = false
      queueMicrotask(() => setTitleMusicPlaying(false))
      return
    }
    if (audioUnlockedRef.current && !titleMusicActiveRef.current) {
      let active = true
      const started = startOriginalTitleTheme()
      if (started) titleMusicActiveRef.current = true
      queueMicrotask(() => {
        if (active) setTitleMusicPlaying((current) => started || current)
      })
      return () => {
        active = false
        stopOriginalTitleTheme()
        titleMusicActiveRef.current = false
      }
    }
    return () => {
      stopOriginalTitleTheme()
      titleMusicActiveRef.current = false
    }
  }, [stage])

  /* Sound is a MUTE, not a transport control.
   *
   * It used to be both, and that is the bug this replaces: turning sound off
   * mid-bout stopped the round cue outright, and turning it back on could only
   * ever restart it from bar one, because the cues are start/stop with no
   * seek. A presenter muting to take a question lost their place in the score.
   *
   * Now the flag only moves a master gain, on a 40 ms ramp down and a 100 ms
   * ramp back. The cue underneath keeps running and keeps its position, so
   * unmuting resumes wherever the music actually got to. It cannot stop or
   * restart anything, which is precisely the property that matters over a live
   * bout. What sound still gates is STARTING audio -- the one-shot blips, and
   * whether ring() opens a cue at all -- so "sound off" remains a promise that
   * the app makes no noise. */
  useEffect(() => {
    setOriginalTitleThemeMuted(!sound)
    setOriginalRoundThemeMuted(!sound)
    setOriginalCreditsThemeMuted(!sound)
  }, [sound])

  useEffect(() => {
    const laneFailed = session?.lanes.lakebase.state === 'failed'
      || session?.lanes.competitor.state === 'failed'
    const roundFinished = session?.state === 'verified'
      || session?.state === 'towelled'
      || session?.state === 'failed'
    // `sound` is deliberately absent: a mute must not end the cue.
    if (stage !== 'proof' || !liveEvidenceConnected || laneFailed || roundFinished || session?.towel) {
      stopOriginalRoundTheme()
    }
  }, [stage, liveEvidenceConnected, session?.state, session?.towel, session?.lanes.lakebase.state, session?.lanes.competitor.state])

  useEffect(() => () => {
    mountedRef.current = false
    stopOriginalRoundTheme()
    stopOriginalTitleTheme()
  }, [])

  /* What the credits roll needs, assembled once for the two screens that can
     open it. See credits-entry.tsx; the roll reads all of this and writes none
     of it, and opening it changes no stage, session or audio state. */
  const credits = useMemo<CreditsEntry>(() => ({
    competitors: catalog.competitors,
    scorecard,
    sound,
    /* Only a running bout, not merely "a session exists". A verified, towelled
       or failed session is finished; the standing note and the suppressed bell
       are about an in-flight measurement. Not qualified by stage, because the
       title screen is one of the two ways in and a round can still be running
       underneath it -- the operator can leave the arena without cancelling. */
    boutInFlight: session?.state === 'running',
  }), [catalog.competitors, scorecard, session?.state, sound])

  useEffect(() => {
    writeBrowserView(
      { stage: initialProgress.stage, setupScene: initialProgress.setupScene },
      'replace',
    )
    const onPopState = (event: PopStateEvent) => {
      const requested = readBrowserView(event.state)
      if (!requested) return
      transitionRef.current += 1
      const view = requiresSession(requested.stage) && !sessionRef.current
        ? { stage: 'setup' as const, setupScene: 'card' as const }
        : requested
      if (view !== requested) writeBrowserView(view, 'replace')
      setStage(view.stage)
      setSetupScene(view.setupScene)
    }
    window.addEventListener('popstate', onPopState)
    return () => window.removeEventListener('popstate', onPopState)
  }, [initialProgress])

  useEffect(() => {
    if (!initialActiveSession || uiReview) return
    let active = true
    let retryTimer: number | undefined
    let attempts = 0
    const restore = () => {
      attempts += 1
      api.getSession(initialActiveSession.id)
        .then((incoming) => {
          if (!active) return
          if (incoming.state === 'draft') {
            saveActiveSessionPointer(null)
            setSessionRestorePending(false)
            setError(null)
            navigate('setup', 'card', 'replace')
            return
          }
          sessionRef.current = incoming
          resumeStageRef.current = initialActiveSession.resumeStage
          setCompetitor(incoming.competitor.id)
          setCorners([...incoming.corners])
          setPrimary(incoming.primary_persona.id)
          setSecondary(incoming.secondary_personas.map((persona) => persona.id))
          setRoundOverride(incoming.round.id)
          setCommentaryOpen(true)
          setSession(incoming)
          setSessionRestoreError(null)
          setError(null)
          const restoredStage = initialActiveSession.stage === 'title'
            ? 'title'
            : sessionStage(incoming, initialActiveSession.resumeStage)
          navigate(restoredStage, 'card', 'replace')
          const entry = scorecardEntry(incoming)
          if (entry) {
            setScorecard((current) => [
              ...current.filter((candidate) => candidate.session_id !== entry.session_id),
              entry,
            ])
          }
          setSessionRestorePending(false)
        })
        .catch((cause) => {
          if (!active) return
          if (cause instanceof ApiError && cause.status === 404) {
            saveActiveSessionPointer(null)
            setSessionRestorePending(false)
            setError(null)
            if (initialActiveSession.stage !== 'title') navigate('setup', 'card', 'replace')
            return
          }
          if (attempts >= SESSION_RESTORE_ATTEMPTS) {
            setSessionRestoreError(
              'The saved bout could not be restored after four attempts. No new bout was started.',
            )
            return
          }
          setError('Reconnecting to the saved bout…')
          retryTimer = window.setTimeout(restore, SESSION_RESTORE_INTERVAL_MS)
        })
    }
    restore()
    return () => {
      active = false
      if (retryTimer !== undefined) window.clearTimeout(retryTimer)
    }
  }, [initialActiveSession, navigate, uiReview])

  useEffect(() => {
    saveSetupProgress({
      stage,
      setupScene,
      competitor,
      corners,
      primary,
      secondary,
      roundOverride,
      sound,
    })
  }, [stage, setupScene, competitor, corners, primary, secondary, roundOverride, sound])

  useEffect(() => {
    saveScorecard(scorecard)
  }, [scorecard])

  useEffect(() => {
    let active = true
    let inFlight = false
    let timer: number | undefined
    const schedule = (delay: number) => {
      if (!active) return
      if (timer !== undefined) window.clearTimeout(timer)
      timer = window.setTimeout(inspect, delay)
    }
    const inspect = () => {
      if (!active || inFlight) return
      inFlight = true
      api.catalog()
        .then((response) => {
          if (!active) return
          setCatalog(withBundledPortraits(response))
          setApiStatus('online')
          schedule(30000)
        })
        .catch(() => {
          if (!active) return
          setApiStatus('offline')
          schedule(2500)
        })
        .finally(() => { inFlight = false })
    }
    const retryNow = () => {
      if (timer !== undefined) window.clearTimeout(timer)
      timer = undefined
      inspect()
    }
    inspect()
    window.addEventListener('online', retryNow)
    return () => {
      active = false
      window.removeEventListener('online', retryNow)
      if (timer !== undefined) window.clearTimeout(timer)
    }
  }, [])

  useEffect(() => {
    if (uiReview) return
    let active = true
    let inFlight = false
    let timer: number | undefined
    const inspect = () => {
      if (!active || inFlight) return
      inFlight = true
      api.allBoutStatuses()
        .then((status) => {
          if (!active) return
          setBoutBoardFresh(true)
          setBoutBoard(status)
          const blocked = Object.values(status.rounds).some((round) => !round.can_start)
          timer = window.setTimeout(
            inspect,
            blocked
              ? BOUT_BOARD_BLOCKED_POLL_MIN_MS
                + Math.random() * BOUT_BOARD_BLOCKED_POLL_JITTER_MS
              : BOUT_BOARD_READY_POLL_MIN_MS
                + Math.random() * BOUT_BOARD_READY_POLL_JITTER_MS,
          )
        })
        .catch(() => {
          if (!active) return
          setBoutBoardFresh(false)
          // A failed observation is not evidence that six known states became
          // unknown. Keep the last board painted while retrying; clearing it
          // hides another viewer's active bout and makes a later tile press look
          // like the action that discovered it.
          timer = window.setTimeout(
            inspect,
            BOUT_BOARD_ERROR_RETRY_MIN_MS
              + Math.random() * BOUT_BOARD_ERROR_RETRY_JITTER_MS,
          )
        })
        .finally(() => { inFlight = false })
    }
    inspect()
    return () => {
      active = false
      if (timer !== undefined) window.clearTimeout(timer)
    }
  }, [uiReview])

  useEffect(() => {
    if (!sessionId || uiReview) return
    let lastSequence = 0
    let active = true
    queueMicrotask(() => {
      if (!active) return
      setMissedCalls(0)
      setLiveEvidenceConnected(true)
    })
    let streamInterrupted = false
    let reconciliation: Promise<void> | null = null
    let reconciliationTimer: number | undefined
    const scheduleReconciliation = () => {
      if (!active || !streamInterrupted || reconciliationTimer !== undefined) return
      reconciliationTimer = window.setTimeout(() => {
        reconciliationTimer = undefined
        reconcile()
      }, 2500)
    }
    const reconcile = () => {
      if (!active || reconciliation) return
      reconciliation = api.getSession(sessionId)
        .then((incoming) => {
          if (!active) return
          let accepted = false
          let reconciled = incoming
          setSession((current) => {
            if (!current) return current
            const selected = selectRound4Session(current, incoming)
            if (!selected || selected === current) return current
            accepted = true
            reconciled = selected
            sessionRef.current = reconciled
            return reconciled
          })
          queueMicrotask(() => {
            if (!active || !accepted) return
            const terminal = reconciled.state === 'verified'
              || reconciled.state === 'failed'
              || reconciled.state === 'towelled'
            if (terminal) {
              streamInterrupted = false
              setLiveEvidenceConnected(true)
              setError((current) => isStreamError(current) ? null : current)
            }
            const entry = scorecardEntry(reconciled)
            if (entry) {
              setScorecard((current) => [
                ...current.filter((candidate) => candidate.session_id !== entry.session_id),
                entry,
              ])
            }
            if (
              (stageRef.current === 'matchup' || stageRef.current === 'ready')
              && reconciled.state === 'failed'
              && !reconciled.run_started_at
            ) {
              streamInterrupted = false
              sessionRef.current = null
              setSession(null)
              setError(reconciled.failure ?? 'The fight card could not be armed. No run started.')
              navigate('setup', 'card', 'replace')
            } else if (stageRef.current === 'matchup' && reconciled.state === 'armed') {
              navigate('ready', 'card', 'replace')
            } else if (
              (stageRef.current === 'matchup' || stageRef.current === 'ready')
              && (reconciled.state === 'running' || reconciled.run_started_at)
            ) {
              navigate('proof', 'card', 'replace')
            }
          })
        })
        .catch((cause) => {
          if (
            cause instanceof ApiError
            && cause.status === 404
            && active
            && sessionRef.current?.id === sessionId
          ) {
            streamInterrupted = false
            transitionRef.current += 1
            sessionRef.current = null
            setSession(null)
            setError(null)
            stopOriginalRoundTheme()
            navigate('setup', 'card', 'replace')
          }
        })
        .finally(() => {
          reconciliation = null
          scheduleReconciliation()
        })
    }
    const unsubscribe = subscribeToSession(
      sessionId,
      (event) => {
        if (event.sequence <= lastSequence) return
        // Counted before the dedup would hide it and before anything else runs:
        // this arrives on the first event of a resume the server had to serve
        // past its retention floor, and it is the only notice there will be.
        // Accumulated rather than replaced, because a long bout can resume
        // across a hole more than once.
        if (typeof event.gap_before === 'number' && event.gap_before > 0) {
          const missed = event.gap_before
          setMissedCalls((current) => current + missed)
        }
        lastSequence = event.sequence
        streamInterrupted = false
        setLiveEvidenceConnected(true)
        if (reconciliationTimer !== undefined) {
          window.clearTimeout(reconciliationTimer)
          reconciliationTimer = undefined
        }
        setError((current) => isStreamError(current) ? null : current)
        if (event.event === 'arm_started') {
          setArmStatus('Verifying the sealed start state…')
        }
        if (event.event === 'arm_waiting') {
          setArmStatus(armWaitingCopy(event.payload.status))
        }
        if (event.event === 'session_failed') {
          setError(event.payload.message)
          if (stageRef.current === 'matchup' || stageRef.current === 'ready') {
            navigate(event.payload.session.run_started_at ? 'proof' : 'setup', 'card', 'replace')
          }
        }
        if (event.event === 'session_cancelled') {
          transitionRef.current += 1
          setSession(null)
          setError(null)
          setArmCancelPending(false)
          navigate('setup', 'card', 'replace')
          return
        }
        if (event.event === 'redo_failed') setError(event.payload.message)
        if (event.event === 'run_finished') {
          const entry = scorecardEntry(event.payload.session)
          if (entry) {
            setScorecard((current) => [
              ...current.filter((candidate) => candidate.session_id !== entry.session_id),
              entry,
            ])
          }
        }
        if (event.event === 'towel_finished') {
          const entry = scorecardEntry(event.payload.session)
          if (entry) {
            setScorecard((current) => [
              ...current.filter((candidate) => candidate.session_id !== entry.session_id),
              entry,
            ])
          }
        }
        if (event.event === 'cooldown_ready') {
          setScorecard((current) => current.map((entry) => (
            entry.session_id === sessionId ? withCooldown(entry, event.payload.cooldown) : entry
          )))
        }
        applyServerEvent(event, setSession)
        if (event.event === 'armed' && stageRef.current === 'matchup') {
          navigate('ready', 'card', 'replace')
        }
        if (
          (event.event === 'run_started' || event.event === 'run_finished')
          && (stageRef.current === 'matchup' || stageRef.current === 'ready')
        ) {
          navigate('proof', 'card', 'replace')
        }
        if (
          event.event === 'lane_update'
          && event.payload.session?.state === 'running'
          && stageRef.current === 'ready'
        ) {
          navigate('proof', 'card', 'replace')
        }
      },
      (failure) => {
        streamInterrupted = true
        setLiveEvidenceConnected(false)
        const message = failure.permanent ? STREAM_FAILURE_ERROR : STREAM_INTERRUPTION_ERROR
        setError((current) => isStreamError(current) || current === null ? message : current)
        reconcile()
      },
      () => {
        streamInterrupted = false
        setLiveEvidenceConnected(true)
        if (reconciliationTimer !== undefined) {
          window.clearTimeout(reconciliationTimer)
          reconciliationTimer = undefined
        }
        setError((current) => isStreamError(current) ? null : current)
      },
    )
    return () => {
      active = false
      if (reconciliationTimer !== undefined) window.clearTimeout(reconciliationTimer)
      unsubscribe()
    }
  }, [sessionId, uiReview, navigate])

  function changePrimary(id: PersonaId) {
    if (sound) playCursor()
    setPrimary(id)
    setSecondary((current) => current.filter((candidate) => candidate !== id))
  }

  function toggleSecondary(id: PersonaId) {
    if (id === primary) return
    if (sound) playCursor()
    setSecondary((current) => {
      if (current.includes(id)) return current.filter((candidate) => candidate !== id)
      if (current.length >= 2) return current
      return [...current, id]
    })
  }

  function toggleCorner(corner: CustomerCorner) {
    if (sound) playCursor()
    setCorners((current) => {
      if (!current.includes(corner)) {
        return catalog.corners.filter((candidate) => current.includes(candidate) || candidate === corner)
      }
      return current.length === 1 ? current : current.filter((candidate) => candidate !== corner)
    })
  }

  function changeSetupScene(next: SetupScene) {
    if (sound) playConfirm()
    navigate('setup', next)
  }

  function startGame() {
    if (sound) audioUnlockedRef.current = true
    const titleWasPlaying = stopOriginalTitleTheme()
    titleMusicActiveRef.current = false
    setTitleMusicPlaying(false)
    if (sound) playStart(titleWasPlaying ? 140 : 0)
    if (session) {
      const resumedStage = sessionStage(session, resumeStageRef.current)
      if (resumedStage === 'proof' && session.state === 'running') {
        // Unconditional: the cue opens silent when sound is off, so switching
        // sound on mid-bout finds it already in place at the right bar.
        startOriginalRoundTheme(session.round.id)
      }
      navigate(resumedStage, 'card')
      return
    }
    navigate('setup', 'opponent')
  }

  function toggleTitleMusic() {
    if (titleMusicPlaying) {
      stopOriginalTitleTheme()
      titleMusicActiveRef.current = false
      setTitleMusicPlaying(false)
      setSound(false)
      return
    }
    audioUnlockedRef.current = true
    if (!sound) setSound(true)
    // Clear the mute before the start rather than one effect later, so the
    // attract loop opens at full level instead of ramping up out of silence.
    setOriginalTitleThemeMuted(false)
    if (startOriginalTitleTheme()) {
      titleMusicActiveRef.current = true
      setTitleMusicPlaying(true)
    }
  }

  async function prepareFight(selection: FightSelection) {
    // transitionRef only discards a stale continuation; it does not stop a second
    // click from creating a second session. This ref is the actual mutex.
    if (sessionRestorePending || preparePendingRef.current) return
    preparePendingRef.current = true
    try {
      await runPrepareFight(selection)
    } finally {
      preparePendingRef.current = false
    }
  }

  async function runPrepareFight(selection: FightSelection) {
    const selectionRecommendation = recommend(
      catalog,
      selection.competitor,
      selection.corners,
      selection.primary,
    )
    const selectionRoundId = selection.roundOverride ?? selectionRecommendation.round_id
    if (!uiReview) {
      let selectionRingStatus: FightCardRoundStatus | null
      try {
        const board = await api.allBoutStatuses()
        setBoutBoard(board)
        setBoutBoardFresh(true)
        selectionRingStatus = board.rounds[selectionRoundId]
      } catch {
        setBoutBoardFresh(false)
        setError('Round status could not be refreshed. Prepare stays locked until the board answers.')
        return
      }
      if (!roundCanStart(selectionRingStatus)) {
        setError(roundBoardMessage(selectionRoundId, selectionRingStatus))
        return
      }
    }
    const transition = ++transitionRef.current
    const selectionRound = catalog.rounds.find((round) => round.id === selectionRoundId)!
    const selectedRecommendation: LocalRecommendation = {
      ...selectionRecommendation,
      round_id: selectionRoundId,
      metric: metricForCorners(selectionRound, selection.corners),
      // The headline alone: this rides into the arena and onto the receipt, and
      // only ever describes a round that was allowed to start.
      reason: selectedRoundWhy(selectionRound, selectionRecommendation).headline,
    }
    setError(null)
    setActiveBout(null)
    setArmCancelPending(false)
    setArmStatus('Verifying the sealed start state…')
    if (uiReview) {
      setCommentaryOpen(true)
      setSession(buildReviewSession(
        catalog,
        selection.competitor,
        selection.corners,
        selection.primary,
        selection.secondary,
        selectionRoundId,
        selectedRecommendation,
      ))
      navigate('matchup', 'card')
      await wait(reducedMotion ? 0 : 1100)
      if (transition === transitionRef.current) navigate('ready', 'card', 'replace')
      return
    }
    let created: DemoSession | null = null
    try {
      created = await api.createSession({
        competitor: selection.competitor,
        primary_persona: selection.primary,
        secondary_personas: selection.secondary,
        corners: selection.corners,
        round_id: selection.roundOverride,
      })
      setCommentaryOpen(true)
      setSession(created)
      navigate('matchup', 'card')
      await wait(reducedMotion ? 0 : 1100)
      if (transition !== transitionRef.current) return
      const armed = await api.armSession(created.id)
      setSession((current) => selectRound4Session(current, armed))
      if (armed.state === 'armed' && transition === transitionRef.current) {
        navigate('ready', 'card', 'replace')
      }
    } catch (cause) {
      if (transition !== transitionRef.current) return
      if (cause instanceof ApiError && cause.status === 409 && cause.message.includes('BOUT IN PROGRESS')) {
        // Keep the server's refusal verbatim. It names the round, the phase and,
        // when a towel's cleanup is what is holding the ring, why. Substituting
        // fixed copy here is what left the server log as the only place an
        // operator could find out which round was wedged.
        setArmStatus(cause.message)
        try {
          setActiveBout(await api.boutStatus(selectionRoundId))
        } catch {
          setActiveBout(null)
        }
        return
      }
      if (created && cause instanceof ApiError && cause.status === 0) {
        try {
          const recovered = await api.getSession(created.id)
          if (transition !== transitionRef.current) return
          sessionRef.current = recovered
          setSession((current) => selectRound4Session(current, recovered))
          if (recovered.state !== 'draft') {
            if (recovered.state === 'failed' && !recovered.run_started_at) {
              setError(recovered.failure ?? cause.message)
              setSession(null)
              navigate('setup', 'card', 'replace')
            } else {
              navigate(sessionStage(recovered), 'card', 'replace')
            }
            return
          }
        } catch {
          // The normal setup error below remains actionable when reconciliation is unavailable.
        }
      }
      sessionRef.current = null
      setSession(null)
      navigate('setup', 'card', 'replace')
      setError(cause instanceof Error ? cause.message : 'The fight card could not be prepared.')
    }
  }

  function prepare() {
    return prepareFight({ competitor, corners, primary, secondary, roundOverride })
  }

  function changeRoundOverride(id: RoundId | null) {
    setRoundOverride(id)
  }

  function ringAgain() {
    if (!session || isRoundFive(session) || session.cooldown?.state !== 'ready') return
    const exactSelection: FightSelection = {
      competitor: session.competitor.id,
      corners: [...session.corners],
      primary: session.primary_persona.id,
      secondary: session.secondary_personas.map((persona) => persona.id),
      roundOverride: session.round.id,
    }
    setCompetitor(exactSelection.competitor)
    setCorners(exactSelection.corners)
    setPrimary(exactSelection.primary)
    setSecondary(exactSelection.secondary)
    setRoundOverride(exactSelection.roundOverride)
    return prepareFight(exactSelection)
  }

  async function inspectActiveBout(): Promise<BoutStatus | null> {
    try {
      const current = await api.boutStatus(session?.round.id ?? selectedRoundId)
      setActiveBout(current)
      return current
    } catch {
      return activeBout
    }
  }

  function leaveBlockedMatchup() {
    transitionRef.current += 1
    setSession(null)
    setError(null)
    navigate('setup', 'card', 'replace')
  }

  async function cancelCheckingFight() {
    if (
      !session
      || session.state !== 'checking'
      || session.round.id !== 'wake_idle_app'
      || armCancelPending
    ) return
    transitionRef.current += 1
    setArmCancelPending(true)
    setError(null)
    try {
      await api.cancelArm(session.id)
      setSession(null)
      setArmStatus('Verifying the sealed start state…')
      navigate('setup', 'card', 'replace')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The start-state check could not be cancelled.')
    } finally {
      setArmCancelPending(false)
    }
  }

  function releaseRing() {
    ringPendingRef.current = false
    setRingBlocked(false)
  }

  /**
   * Called only after a bell request aborted client-side while the ring still reads
   * ARMED. The run may be preparing, so the control stays locked until the server
   * shows a run or a terminal state.
   */
  async function reconcileRunStart(sessionId: string) {
    for (let attempt = 0; attempt < RUN_START_RECONCILE_ATTEMPTS; attempt += 1) {
      await wait(RUN_START_RECONCILE_INTERVAL_MS)
      // Nothing left to unlock or report once the app is gone.
      if (!mountedRef.current) return
      if (sessionRef.current && sessionRef.current.id !== sessionId) {
        releaseRing()
        return
      }
      let latest: DemoSession
      try {
        latest = await api.getSession(sessionId)
      } catch {
        continue
      }
      sessionRef.current = latest
      setSession((current) => selectRound4Session(current, latest))
      if (latest.state === 'running' || latest.run_started_at) {
        setError(null)
        navigate('proof', 'card', 'replace')
        releaseRing()
        return
      }
      if (latest.state !== 'armed') {
        stopOriginalRoundTheme()
        setError(latest.failure ?? RUN_START_NOT_STARTED_ERROR)
        releaseRing()
        return
      }
    }
    stopOriginalRoundTheme()
    setError(RUN_START_NOT_STARTED_ERROR)
    releaseRing()
  }

  async function ring() {
    if (!session || (!uiReview && session.state !== 'armed')) return
    // Absorbed, and unchanged by this fix. Nothing is reported from here because
    // nothing can be: `ringBlocked` is set below in the same discrete event as
    // the press that got here first, so by the time a second press could reach
    // this line the screen is already saying a bell is in flight. See the notice
    // in <Ready> for what a double-tapper actually reads.
    if (ringPendingRef.current) return
    ringPendingRef.current = true
    setRingBlocked(true)
    let reconciling = false
    try {
      setError(null)
      // The bell is a one-shot and stays gated; the cue is not, because a
      // presenter who unmutes mid-bout should hear the round they are in
      // rather than nothing until the next bell.
      if (sound) playOriginalBell()
      startOriginalRoundTheme(session.round.id)
      if (uiReview) {
        navigate('proof', 'card')
        return
      }
      try {
        const running = await api.runSession(session.id)
        setSession((current) => selectRound4Session(current, running))
        navigate('proof', 'card')
      } catch (cause) {
        // status 0 is an abort or a dead socket: the request may still have landed.
        const unresolvedByClient = cause instanceof ApiError && cause.status === 0
        try {
          const recovered = await api.getSession(session.id)
          sessionRef.current = recovered
          setSession((current) => selectRound4Session(current, recovered))
          if (recovered.state === 'running' || recovered.run_started_at) {
            navigate('proof', 'card', 'replace')
            return
          }
          if (unresolvedByClient && recovered.state === 'armed') {
            reconciling = true
            setError(RUN_START_UNCONFIRMED_ERROR)
            void reconcileRunStart(session.id)
            return
          }
        } catch {
          // Preserve the original mutation error if the read-back is also unavailable.
        }
        stopOriginalRoundTheme()
        setError(cause instanceof Error ? cause.message : 'The run did not start. No result was recorded.')
      }
    } finally {
      if (!reconciling) releaseRing()
    }
  }

  function continueAfterProof() {
    if (!session || (
      session.state !== 'verified'
      && session.state !== 'towelled'
      && session.state !== 'failed'
    ) || !proofNavigationAllowsExit(session)) return
    const completed = session
    setError(null)
    if (isRoundSix(completed) && completed.state === 'verified') {
      navigate('finale', 'card')
      return
    }
    sessionRef.current = null
    setSession(null)
    setCompetitor(completed.competitor.id)
    setCorners([...completed.corners])
    setPrimary(completed.primary_persona.id)
    setSecondary(completed.secondary_personas.map((persona) => persona.id))
    setRoundOverride(nextRound?.id ?? completed.round.id)
    navigate('setup', 'card')
  }

  function leaveFinale() {
    const completed = session
    sessionRef.current = null
    setSession(null)
    setError(null)
    if (completed) {
      setCompetitor(completed.competitor.id)
      setCorners([...completed.corners])
      setPrimary(completed.primary_persona.id)
      setSecondary(completed.secondary_personas.map((persona) => persona.id))
      setRoundOverride(completed.round.id)
    }
    navigate('setup', 'card')
  }

  async function redoAfterProof() {
    const failedOwnedArtifactRound = session?.state === 'failed'
      && (session.round.id === 'make_schema_change_safely' || session.round.id === 'recover_deleted_order')
    const genericRedoAllowed = session?.round.redo?.policy === 'show'
      || session?.round.redo?.policy === 'optional'
    if (!session || (!failedOwnedArtifactRound && (session.state !== 'verified' || !genericRedoAllowed))) return
    setError(null)
    if (isRoundFour(session)) {
      if (!canStartRoundFourRedo(session) || redoPending) return
      setRedoPending(true)
      try {
        const redone = await api.redoSession(session.id)
        setSession((current) => (
          current && acceptsReconciledSession(current, redone) ? redone : current
        ))
      } catch (cause) {
        if (cause instanceof ApiError && cause.status >= 400 && cause.status < 500) {
          setError(cause.message)
        } else {
          try {
            const reconciled = await api.getSession(session.id)
            let accepted = false
            setSession((current) => {
              if (!current || !acceptsReconciledSession(current, reconciled)) return current
              accepted = true
              return reconciled
            })
            queueMicrotask(() => {
              if (!accepted) return
              if (reconciled.redo?.state === 'ready') {
                setError('The score-change request was not confirmed. The verified v1 proof is unchanged.')
              }
            })
          } catch {
            setError(cause instanceof Error ? cause.message : 'The score-change request could not be confirmed.')
          }
        }
      } finally {
        setRedoPending(false)
      }
      return
    }
    if ((!session.cooldown || session.cooldown.state === 'failed') && !uiReview) {
      try {
        const latest = await api.startReset(session.id)
        setSession((current) => selectRound4Session(current, latest))
        if (!latest.cooldown) {
          setError('The re-do clocks could not be started. Try RE-DO ROUND again.')
          return
        }
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : 'The re-do clocks could not be started.')
        return
      }
    }
    navigate('between', 'card')
  }

  async function retryCleanup() {
    if (!session || session.cooldown?.state !== 'failed') return
    setError(null)
    try {
      const latest = await api.startReset(session.id)
      setSession((current) => selectRound4Session(current, latest))
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Cleanup could not be retried.')
    }
  }

  async function retryRoundFiveCleanup() {
    if (
      !session
      || !isRoundFive(session)
      || session.round5_setup?.cleanup_retryable !== true
      || roundFiveCleanupPending
    ) return
    const sessionId = session.id
    setRoundFiveCleanupPending(true)
    setError(null)
    try {
      const latest = await api.retryCleanup(sessionId)
      setSession((current) => selectRound4Session(current, latest))
    } catch {
      try {
        const latest = await api.getSession(sessionId)
        setSession((current) => selectRound4Session(current, latest))
        if (latest.round5_setup?.cleanup_retryable === true) {
          setError('The fallback retry was not confirmed. Automatic cleanup remains active.')
        }
      } catch {
        setError('Cleanup status could not be refreshed. Automatic cleanup remains active; live updates will reconnect.')
      }
    } finally {
      setRoundFiveCleanupPending(false)
    }
  }

  async function throwInTowel() {
    if (!session) return
    const retryingCleanup = session.towel?.state === 'failed'
    if (!retryingCleanup && (session.state !== 'running' || session.towel)) return
    setError(null)
    try {
      const latest = await api.throwTowel(session.id)
      setSession((current) => selectRound4Session(current, latest))
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The towel request could not be accepted.')
    }
  }

  function showScorecard() {
    navigate('scorecard', 'card')
  }

  function requestTitle() {
    if (session && !window.confirm(
      'Return to the title screen?\n\nThis changes screens only. It does not cancel the current bout or skip required cleanup.',
    )) return
    transitionRef.current += 1
    if (requiresSession(stageRef.current)) resumeStageRef.current = stageRef.current
    stopOriginalRoundTheme()
    setError(null)
    navigate('title', 'opponent')
  }

  function discardSavedSession() {
    saveActiveSessionPointer(null)
    sessionRef.current = null
    setSession(null)
    setSessionRestorePending(false)
    setSessionRestoreError(null)
    setError(null)
    navigate('setup', 'card', 'replace')
  }

  if (sessionRestoreError) {
    return (
      <main className="retro-screen restore-failure-screen" role="alert">
        <ApiIndicator status={apiStatus} />
        <p className="pixel-kicker">Saved session</p>
        <h1>Saved bout unavailable</h1>
        <p>{sessionRestoreError}</p>
        <button type="button" className="game-primary" onClick={discardSavedSession}>
          Discard saved bout
        </button>
      </main>
    )
  }

  if (stage === 'title') {
    return (
      <TitleScreen
        apiStatus={apiStatus}
        uiReview={uiReview}
        musicPlaying={titleMusicPlaying}
        hasSession={Boolean(session)}
        restoringSession={sessionRestorePending}
        credits={credits}
        onToggleMusic={toggleTitleMusic}
        onStart={startGame}
      />
    )
  }

  if (stage === 'matchup') {
    return (
      <Matchup
        competitor={opponentLabel(session?.round.id ?? selectedRoundId, selectedCompetitor.short_name)}
        personas={selectedPersonas}
        reducedMotion={reducedMotion}
        uiReview={uiReview}
        status={armStatus}
        activeBout={activeBout}
        onInspectBout={inspectActiveBout}
        onLeave={leaveBlockedMatchup}
        canCancel={session?.state === 'checking' && session.round.id === 'wake_idle_app'}
        cancelPending={armCancelPending}
        onCancel={cancelCheckingFight}
      />
    )
  }
  if (stage === 'ready' && session) {
    return (
      <Ready
        competitor={opponentLabel(session.round.id, selectedCompetitor.short_name)}
        roundTitle={isRoundFive(session) ? ROUND_FIVE_DISPLAY_TITLE : session.round.title}
        competitorLaneState={session.lanes.competitor.state}
        sessionState={session.state}
        armedExpiresAt={session.armed_expires_at}
        personas={selectedPersonas}
        sound={sound}
        onToggleSound={toggleSound}
        onRing={ring}
        ringBlocked={ringBlocked}
        onHome={requestTitle}
        error={error}
        uiReview={uiReview}
      />
    )
  }
  if (stage === 'proof' && session) {
    if (isRoundFour(session)) {
      return (
        <RoundFourProof
          session={session}
          missedCalls={missedCalls}
          roundNumber={currentRoundIndex + 1}
          error={error}
          uiReview={uiReview}
          sound={sound}
          onToggleSound={toggleSound}
          redoPending={redoPending}
          hasNextRound={Boolean(nextRound)}
          commentaryOpen={commentaryOpen}
          onContinue={continueAfterProof}
          onRedo={redoAfterProof}
          onTowel={throwInTowel}
          onHome={requestTitle}
          onToggleCommentary={() => setCommentaryOpen((current) => !current)}
        />
      )
    }
    if (isRoundFive(session)) {
      return (
        <RoundFiveProof
          session={session}
          missedCalls={missedCalls}
          roundNumber={currentRoundIndex + 1}
          error={error}
          liveEvidenceConnected={liveEvidenceConnected}
          uiReview={uiReview}
          sound={sound}
          onToggleSound={toggleSound}
          hasNextRound={Boolean(nextRound)}
          commentaryOpen={commentaryOpen}
          onContinue={continueAfterProof}
          onTowel={throwInTowel}
          cleanupPending={roundFiveCleanupPending}
          onRetryCleanup={retryRoundFiveCleanup}
          onHome={requestTitle}
          onToggleCommentary={() => setCommentaryOpen((current) => !current)}
        />
      )
    }
    if (isRoundSix(session)) {
      return (
        <RoundSixScene
          session={session}
          missedCalls={missedCalls}
          roundNumber={currentRoundIndex + 1}
          error={error}
          uiReview={uiReview}
          sound={sound}
          onToggleSound={toggleSound}
          hasNextRound={Boolean(nextRound)}
          commentaryOpen={commentaryOpen}
          onContinue={continueAfterProof}
          onTowel={throwInTowel}
          onHome={requestTitle}
          onToggleCommentary={() => setCommentaryOpen((current) => !current)}
        />
      )
    }
    return (
      <Proof
        session={session}
        missedCalls={missedCalls}
        roundNumber={currentRoundIndex + 1}
        task={session.round.title}
        competitor={selectedCompetitor.short_name}
        error={error}
        liveEvidenceConnected={liveEvidenceConnected}
        uiReview={uiReview}
        sound={sound}
        onToggleSound={toggleSound}
        hasNextRound={(session.state === 'verified' || session.state === 'towelled') && Boolean(nextRound)}
        onContinue={continueAfterProof}
        onRedo={redoAfterProof}
        onTowel={throwInTowel}
        onHome={requestTitle}
        commentaryOpen={commentaryOpen}
        onToggleCommentary={() => setCommentaryOpen((current) => !current)}
      />
    )
  }
  if (stage === 'between' && session) {
    return (
      <BetweenRounds
        session={session}
        missedCalls={missedCalls}
        competitor={session.competitor.short_name}
        error={error}
        onRingAgain={ringAgain}
        onRetryCleanup={retryCleanup}
        onNextRound={continueAfterProof}
        onScorecard={showScorecard}
        commentaryOpen={commentaryOpen}
        onToggleCommentary={() => setCommentaryOpen((current) => !current)}
      />
    )
  }
  if (stage === 'scorecard') {
    return <FinalScorecard entries={scorecard} credits={credits} onBack={() => navigate('setup', 'card')} />
  }
  if (stage === 'summary') {
    return (
      <Summary
        live={session && session.state === 'running'
          ? { roundId: session.round.id, running: true }
          : null}
        onBack={() => navigate('setup', 'card')}
      />
    )
  }
  if (stage === 'finale' && session && isRoundSix(session) && session.state === 'verified') {
    return <Finale session={session} onBack={leaveFinale} onSummary={() => navigate('summary', 'card')} />
  }
  return (
    <Setup
      scene={setupScene}
      apiStatus={apiStatus}
        boardFresh={boutBoardFresh}
      ringStatus={ringStatus}
      roundStatuses={roundStatuses}
      catalog={catalog}
      competitor={competitor}
      corners={corners}
      primary={primary}
      secondary={secondary}
      recommendation={recommendation}
      selectedRoundId={selectedRoundId}
      error={error}
      uiReview={uiReview}
      restoringSession={sessionRestorePending}
      onTitle={requestTitle}
      onScene={changeSetupScene}
      onCompetitor={(id) => { if (sound) playCursor(); setCompetitor(id); setRoundOverride(null) }}
      onCorner={toggleCorner}
      onPrimary={changePrimary}
      onSecondary={toggleSecondary}
      onRoundOverride={changeRoundOverride}
      onPrepare={prepare}
    />
  )
}

function TitleScreen({
  apiStatus,
  uiReview,
  musicPlaying,
  hasSession,
  restoringSession,
  credits,
  onToggleMusic,
  onStart,
}: {
  apiStatus: ApiStatus
  uiReview: boolean
  musicPlaying: boolean
  hasSession: boolean
  restoringSession: boolean
  credits: CreditsEntry
  onToggleMusic: () => void
  onStart: () => void
}) {
  return (
    <main className="game-viewport title-viewport">
      <section className="title-cartridge" aria-labelledby="title-heading">
        <h1 className="sr-only" id="title-heading">Lakebase: The Anti-Demo</h1>
        <img className="title-art" src={brandAssets.retroTitle} alt="" />
        {uiReview
          ? <p className="cartridge-status" data-status={apiStatus}>REVIEW MODE · NO DATABASES</p>
          : <ApiIndicator status={apiStatus} placement="title" />}
        <button
          className="title-music-toggle"
          type="button"
          aria-pressed={musicPlaying}
          onClick={onToggleMusic}
        >
          {musicPlaying ? '♪ Music on' : 'Music off'}
        </button>
        <button className="press-start" disabled={restoringSession} onClick={onStart}>
          ▶ {restoringSession ? 'Restoring bout…' : hasSession ? 'Resume bout' : 'Press start'}
        </button>
        {/* The attract screen is the conventional home for a credits entry, and
            it is the one screen every operator sees before anything else, so
            this is the discoverable way in. It sits in the grid slot the old
            production footer used to occupy, which is why the title screen's
            vertical rhythm is unchanged by that footer's removal. */}
        {/* "Staff roll", not "Credits": on an arcade bottom line CREDITS is the
            coin counter (CREDITS 00 / INSERT COIN), so the old wording read as a
            balance rather than a control. Staff roll is the era's own term for
            the sequence and cannot be misread here. */}
        <CreditsButton entry={credits} className="credits-entry title-credits">
          Staff roll
        </CreditsButton>
      </section>
    </main>
  )
}

function moveRovingRadio(
  event: KeyboardEvent<HTMLButtonElement>,
  index: number,
  count: number,
  select: (index: number) => void,
): void {
  let next: number | null = null
  if (event.key === 'ArrowRight' || event.key === 'ArrowDown') next = (index + 1) % count
  if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') next = (index - 1 + count) % count
  if (event.key === 'Home') next = 0
  if (event.key === 'End') next = count - 1
  if (next === null) return
  event.preventDefault()
  select(next)
  const radios = event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>('[role="radio"]')
  radios?.[next]?.focus()
}

function HomeLogo({ onHome, className }: { onHome: () => void; className?: string }) {
  return (
    <button className={`home-logo-lockup${className ? ` ${className}` : ''}`} type="button" onClick={onHome} aria-label="Home" title="Return to title screen">
      <img className="home-logo-ring" src={brandAssets.headerRing} alt="" />
      <span className="home-logo-copy"><strong>Lakebase</strong><small>The Anti-Demo</small></span>
    </button>
  )
}

/**
 * The sound control, in one place so the fight card and the arenas cannot
 * drift apart in wording, in aria-pressed or in what pressing it does. Only
 * the spacing differs: the fight card stacks it below the bell, an arena
 * header sits it beside the round status, and `sound-button-arena` is a
 * modifier on the same class rather than a second look.
 *
 * It is a MUTE and nothing else. It calls the flag setter, the effect in App
 * maps the flag onto the three mute setters in audio.ts, and no transport
 * function is reachable from here -- so pressing it over a live bout cannot
 * end, restart or reposition the round cue.
 */
function SoundToggle({ sound, onToggle, arena = false }: { sound: boolean; onToggle: () => void; arena?: boolean }) {
  return (
    <button
      type="button"
      className={arena ? 'sound-button sound-button-arena' : 'sound-button'}
      aria-pressed={sound}
      onClick={onToggle}
    >
      {sound ? 'Sound on' : 'Sound off'}
    </button>
  )
}

function Setup(props: {
  scene: SetupScene
  apiStatus: ApiStatus
  boardFresh: boolean
  ringStatus: FightCardRoundStatus | null
  roundStatuses: Record<RoundId, FightCardRoundStatus> | null
  catalog: CatalogResponse
  competitor: CompetitorId
  corners: CustomerCorner[]
  primary: PersonaId
  secondary: PersonaId[]
  recommendation: LocalRecommendation
  selectedRoundId: RoundId
  error: string | null
  uiReview: boolean
  restoringSession: boolean
  onTitle: () => void
  onScene: (scene: SetupScene) => void
  onCompetitor: (id: CompetitorId) => void
  onCorner: (corner: CustomerCorner) => void
  onPrimary: (id: PersonaId) => void
  onSecondary: (id: PersonaId) => void
  onRoundOverride: (id: RoundId | null) => void
  onPrepare: () => void
}) {
  const recommendedRound = props.catalog.rounds.find((round) => round.id === props.recommendation.round_id)!
  const selectedRound = props.catalog.rounds.find((round) => round.id === props.selectedRoundId)!
  const roundNumber = props.catalog.rounds.findIndex((round) => round.id === selectedRound.id) + 1
  const selectedRoundState = roundCardState(selectedRound, props.ringStatus)
  const roundRefused = selectedRoundState === 'cleanup_in_progress'
    || selectedRoundState === 'unavailable'
  const selectedWhy = selectedRoundState === 'cleanup_in_progress'
    ? {
        headline: props.ringStatus?.detail ?? ROUND_CLEANUP_COPY[selectedRound.id],
        detail: null,
      }
    : selectedRoundState === 'unavailable' && props.ringStatus?.detail
      ? { headline: props.ringStatus.detail, detail: null }
    : selectedRoundWhy(selectedRound, props.recommendation)
  const selectedCompetitor = props.catalog.competitors.find((item) => item.id === props.competitor)!
  const selectedOpponent = fightCardOpponentLabel(selectedRound.id, props.competitor, selectedCompetitor.short_name)
  const primaryPersona = props.catalog.personas.find((persona) => persona.id === props.primary)!
  const secondaryPersonas = props.secondary.map((id) => props.catalog.personas.find((persona) => persona.id === id)!).filter(Boolean)
  const selectedOpening = selectedRound.id === recommendedRound.id
    ? props.recommendation.presenter_opening
    : `This round: ${selectedRound.title}. Read it through the ${primaryPersona.role} lens; stop only when the application verifies the outcome.`
  const sceneNumber = { opponent: 1, lead: 2, lenses: 3, card: 4 }[props.scene]
  const sceneLabel = { opponent: 'Choose opponent', lead: 'Lead voice', lenses: 'Supporting lenses', card: 'Fight card' }[props.scene]
  const refusal = prepareRefusal({
    uiReview: props.uiReview,
    restoringSession: props.restoringSession,
    apiStatus: props.apiStatus,
    boardFresh: props.boardFresh,
    ringStatus: props.ringStatus,
    roundStatuses: props.roundStatuses,
    round: selectedRound,
    rounds: props.catalog.rounds,
    roundState: selectedRoundState,
  })
  const canPrepare = refusal === null

  return (
    <main className="game-viewport">
      <section className="game-screen" data-scene={props.scene}>
        <header className="game-topbar">
          <HomeLogo onHome={props.onTitle} />
          {/* The eyebrow names the flow and the strong names the step. On the
              last step both were "Fight card", which read as a stutter. */}
          <div>{sceneLabel !== 'Fight card' && <span>Fight card</span>}<strong>{sceneLabel}</strong></div>
          <p>{sceneNumber} / 4</p>
        </header>
        {props.uiReview
          ? <p className="game-mode">UI REVIEW · NO LIVE DATABASES · NO RESULT</p>
          : <ApiIndicator status={props.apiStatus} />}

        {props.scene === 'opponent' && (
          <div className="game-scene opponent-scene">
            <div className="scene-heading"><p>Blue corner</p><h1>Choose the opponent</h1></div>
            <div className="opponent-grid" role="radiogroup" aria-label="Competitor">
              {props.catalog.competitors.map((item, index) => (
                <button
                  className="opponent-card"
                  data-selected={props.competitor === item.id}
                  role="radio"
                  aria-checked={props.competitor === item.id}
                  tabIndex={props.competitor === item.id ? 0 : -1}
                  key={item.id}
                  onClick={() => props.onCompetitor(item.id)}
                  onKeyDown={(event) => moveRovingRadio(
                    event,
                    index,
                    props.catalog.competitors.length,
                    (next) => props.onCompetitor(props.catalog.competitors[next].id),
                  )}
                >
                  <span className="select-cursor" aria-hidden="true">▶</span>
                  <DatabaseFighter label={item.id === 'aurora_serverless_v2' ? 'AUR' : 'RDS'} corner="blue" />
                  <strong>{item.short_name}</strong>
                  <small>{competitorEpithet[item.id].epithet}</small>
                  {competitorEpithet[item.id].tagline && <em>{competitorEpithet[item.id].tagline}</em>}
                </button>
              ))}
            </div>
            <div className="corner-picker" role="group" aria-label="Customer priorities">
              <p>What matters in their corner?</p>
              <span>Choose one or more</span>
              <div>{props.catalog.corners.map((item) => (
                <button
                  aria-pressed={props.corners.includes(item)}
                  data-selected={props.corners.includes(item)}
                  key={item}
                  onClick={() => props.onCorner(item)}
                >
                  <b aria-hidden="true">{props.corners.includes(item) ? '✓' : '+'}</b>
                  <strong>{item}</strong>
                  <small>{cornerCopy[item]}</small>
                </button>
              ))}</div>
              <small>TAILORS THE TALK TRACK. NEVER THE EVIDENCE.</small>
            </div>
            <GameNav back="Title screen" next="Choose the lead voice" onBack={props.onTitle} onNext={() => props.onScene('lead')} />
          </div>
        )}

        {props.scene === 'lead' && (
          <div className="game-scene roster-scene">
            <div className="scene-heading"><p>Who must believe the result?</p><h1>Choose the lead voice</h1><small>CHANGES THE QUESTION, INTERPRETATION &amp; ANSWER · NEVER THE TIMERS</small></div>
            <PersonaRoster personas={props.catalog.personas} primary={props.primary} secondary={props.secondary} mode="lead" onPrimary={props.onPrimary} onSecondary={props.onSecondary} />
            <div className="roster-confirm"><span>LEAD VOICE</span><img src={primaryPersona.portrait} alt="" /><strong>{primaryPersona.nickname}</strong><small>{primaryPersona.role}</small></div>
            <GameNav back="Change opponent" next="Add supporting lenses" onBack={() => props.onScene('opponent')} onNext={() => props.onScene('lenses')} />
          </div>
        )}

        {props.scene === 'lenses' && (
          <div className="game-scene roster-scene">
            <div className="scene-heading"><p>Read the room</p><h1>Add up to two lenses</h1><small>OPTIONAL · ONE TAILORED EXPLANATION PER STAKEHOLDER · SAME EVIDENCE</small></div>
            <PersonaRoster personas={props.catalog.personas} primary={props.primary} secondary={props.secondary} mode="lenses" onPrimary={props.onPrimary} onSecondary={props.onSecondary} />
            <div className="lens-score">
              <span>RINGSIDE TEAM</span>
              <strong>{primaryPersona.nickname}</strong>
              <small>{secondaryPersonas.length ? `+ ${secondaryPersonas.map((persona) => persona.nickname).join(' + ')}` : '+ NO SUPPORTING LENSES'}</small>
              <b>{props.secondary.length} / 2</b>
            </div>
            <GameNav back="Change lead" next="Reveal the fight card" onBack={() => props.onScene('lead')} onNext={() => props.onScene('card')} />
          </div>
        )}

        {props.scene === 'card' && (
          <div className="game-scene card-scene">
            <div className="ring-card">
              <p className="ring-rnum">Fight card <u>· Round {String(roundNumber).padStart(2, '0')} of six</u></p>
              <h1 className="ring-rtitle" id="recommendation-heading">{selectedRound.title}</h1>
            </div>
            <FightRing
              rounds={props.catalog.rounds}
              competitors={props.catalog.competitors}
              competitor={props.competitor}
              selectedRoundId={props.selectedRoundId}
              opponentLabel={selectedOpponent}
              recommendedRoundId={recommendedRound.id}
              roundStatuses={props.roundStatuses}
              statusRequired={!props.uiReview}
              onRound={(id) => props.onRoundOverride(id === recommendedRound.id ? null : id)}
              onCompetitor={props.onCompetitor}
            />
            {/* THE ANIMATION DOES THE EXPLAINING.
                The three panels that stood here -- THIS ROUND, BLUE CORNER and
                BEFORE THE BELL -- described in prose what the ring is already
                showing, so they are gone and the word FIGHT stands in their
                place. What is NOT prose is a refused round: a disabled button
                with no stated reason is the one thing this screen may not do,
                so when the selected round cannot arm, the WHY panel takes the
                same space instead. The per-round capability labels on the six
                tiles, which carry the honest lane facts, are untouched. */}
            {/* THE APRON IS THE WHY PANEL AND NOTHING ELSE, so it comes and
                goes with the round. It used to render unconditionally to carry
                a permanent staging caveat under it; the owner removed that
                paragraph as over-engineering, and an apron with no occupant is
                a bare yellow rule under the tiles. A refused round is the one
                thing this screen may not leave unexplained, so that is what is
                left. */}
            {roundRefused && (
              <div className="ring-apron">
                <section className="ring-cell ring-refusal">
                  <p className="sub">
                    {selectedRoundState === 'cleanup_in_progress'
                      ? 'Cleanup in progress.'
                      : `This round is ${selectedRound.availability} and cannot arm tonight.`}
                  </p>
                  <dl className="scorecard-copy">
                    <div><dt>Why</dt><dd>
                      {selectedWhy.headline}
                      {selectedWhy.detail && (
                        <details className="round-why-detail">
                          <summary>Operator detail</summary>
                          <p>{selectedWhy.detail}</p>
                        </details>
                      )}
                    </dd></div>
                  </dl>
                </section>
              </div>
            )}
            <div className="persona-dialogue">
              <div className="dialogue-team" aria-label="People at ringside">{[primaryPersona, ...secondaryPersonas].map((persona) => <img src={persona.portrait} alt={persona.role} key={persona.id} />)}</div>
              <p><span>{primaryPersona.nickname} says:</span>“{selectedOpening}”</p>
            </div>
            {props.error && <p className="game-error" role="alert">{props.error}</p>}
            <div className="card-actions">
              <button className="game-back" onClick={() => props.onScene('lenses')}>B · Back</button>
              <button className="game-primary" disabled={!canPrepare} onClick={props.onPrepare}>{props.uiReview ? 'A · Preview fight card' : 'A · Prepare fight card'}</button>
            </div>
            {/* ONE STRIP, ONE SENTENCE, and it is the same value that greyed
                the button out. Five separate notes stood here, each with its
                own guard, and between them they left the ring's status-not-read
                state with no occupant at all -- a dead PREPARE and a silent
                screen. Rendering the refusal itself cannot reproduce that, and
                it costs less room than the old set did: two of those guards
                could be true at once, so the worst case here used to be two
                stacked rows of 5px type rather than one.

                Below `.card-actions` on purpose. This strip takes its space out
                of the ring's own flexible row, so nothing it ever says can push
                B · BACK or A · PREPARE FIGHT CARD toward the fold. */}
            {props.uiReview && <p className="game-lock-note">VISUAL REVIEW ONLY · NOTHING WILL CONNECT OR RUN</p>}
            {refusal && <p className="game-lock-note" role="status">{refusal}</p>}
          </div>
        )}
        <div className="game-scanlines" aria-hidden="true" />
      </section>
    </main>
  )
}

function PersonaRoster(props: {
  personas: CatalogResponse['personas']
  primary: PersonaId
  secondary: PersonaId[]
  mode: 'lead' | 'lenses'
  onPrimary: (id: PersonaId) => void
  onSecondary: (id: PersonaId) => void
}) {
  return (
    <div className="persona-roster" role={props.mode === 'lead' ? 'radiogroup' : 'group'} aria-label={props.mode === 'lead' ? 'Primary persona' : 'Secondary personas'}>
      {props.personas.map((persona, index) => {
        const lead = persona.id === props.primary
        const lensIndex = props.secondary.indexOf(persona.id)
        const selected = props.mode === 'lead' ? lead : lensIndex >= 0
        const disabled = props.mode === 'lenses' && (lead || (lensIndex < 0 && props.secondary.length >= 2))
        return (
          <button
            className="persona-card"
            data-selected={selected}
            data-lead={lead}
            disabled={disabled}
            role={props.mode === 'lead' ? 'radio' : undefined}
            aria-checked={props.mode === 'lead' ? lead : undefined}
            aria-pressed={props.mode === 'lenses' ? lensIndex >= 0 : undefined}
            tabIndex={props.mode === 'lead' ? (lead ? 0 : -1) : undefined}
            key={persona.id}
            onClick={() => props.mode === 'lead' ? props.onPrimary(persona.id) : props.onSecondary(persona.id)}
            onKeyDown={props.mode === 'lead'
              ? (event) => moveRovingRadio(
                  event,
                  index,
                  props.personas.length,
                  (next) => props.onPrimary(props.personas[next].id),
                )
              : undefined}
          >
            <span className="roster-cursor" aria-hidden="true">▶</span>
            <img src={persona.portrait} alt="" />
            <strong>{persona.nickname}</strong>
            <small>{persona.role}</small>
            {persona.pain && <span className="persona-pain">{persona.pain}</span>}
            {lead && <b>LEAD</b>}
            {lensIndex >= 0 && <b>LENS {lensIndex + 1}</b>}
            {persona.source_status.startsWith('draft_') && <em>DRAFT</em>}
          </button>
        )
      })}
    </div>
  )
}

function GameNav({ back, next, onBack, onNext }: { back?: string; next: string; onBack?: () => void; onNext: () => void }) {
  return (
    <nav className="game-nav" aria-label="Setup navigation">
      {back ? <button className="game-back" onClick={onBack}>B · {back}</button> : <span />}
      <button className="game-primary" onClick={onNext}>A · {next}</button>
    </nav>
  )
}

function DatabaseFighter({ label, corner }: { label: string; corner: 'red' | 'blue' }) {
  return (
    <div className="database-fighter" data-corner={corner} aria-hidden="true">
      <div className="fighter-crown"><i /><i /><i /></div>
      <div className="fighter-face"><i /><i /></div>
      <div className="fighter-body"><span>{label}</span></div>
      <div className="fighter-glove left" /><div className="fighter-glove right" />
    </div>
  )
}

function ApiIndicator({ status, placement = 'setup' }: { status: ApiStatus; placement?: 'setup' | 'title' }) {
  const [detailsOpen, setDetailsOpen] = useState(false)
  const closeDetails = useCallback(() => setDetailsOpen(false), [])
  const detailsRef = useAccessibleDialog<HTMLElement>(detailsOpen, closeDetails)
  const dialogId = 'api-connection-details'
  const copy = status === 'online'
    ? 'Demo server connected'
    : status === 'offline'
      ? 'Demo server offline'
      : 'Checking demo server'
  const headline = status === 'online'
    ? 'The local API answered'
    : status === 'offline'
      ? 'The local API did not answer'
      : 'The local API is being checked'
  const connectionCopy = status === 'online'
    ? 'Your browser can reach the local anti-demo server/API.'
    : status === 'offline'
      ? 'Your browser cannot currently reach the local anti-demo server/API, so no session event stream can open from this page.'
      : 'Your browser is checking whether it can reach the local anti-demo server/API. No connection result is available yet.'

  return (
    <div className="api-indicator-wrap" data-placement={placement}>
      <button
        className="api-indicator"
        data-status={status}
        type="button"
        aria-haspopup="dialog"
        aria-expanded={detailsOpen}
        aria-controls={dialogId}
        onClick={() => setDetailsOpen((open) => !open)}
      >
        <span className="api-status-dot" aria-hidden="true" />
        <span>{copy}</span>
        <span className="api-info-mark" aria-hidden="true">?</span>
      </button>
      {detailsOpen && (
        <section
          ref={detailsRef}
          className="api-context"
          id={dialogId}
          role="dialog"
          aria-modal="true"
          aria-labelledby={`${dialogId}-title`}
        >
          <header>
            <div>
              <p>Connection details</p>
              <h2 id={`${dialogId}-title`}>{headline}</h2>
            </div>
            <button type="button" onClick={closeDetails} aria-label="Close connection details">×</button>
          </header>
          <p>{connectionCopy}</p>
          <ul>
            <li>A session event stream opens only during an active bout.</li>
            <li>This badge does not prove either database is awake, reachable, or verified.</li>
            <li>Database proof begins only after you choose <strong>Ring the Bell</strong>.</li>
          </ul>
        </section>
      )}
    </div>
  )
}

function Matchup({
  competitor,
  personas,
  reducedMotion,
  uiReview,
  status,
  activeBout,
  onInspectBout,
  onLeave,
  canCancel,
  cancelPending,
  onCancel,
}: {
  competitor: string
  personas: CatalogResponse['personas']
  reducedMotion: boolean
  uiReview: boolean
  status: string
  activeBout: BoutStatus | null
  onInspectBout: () => Promise<BoutStatus | null>
  onLeave: () => void
  canCancel: boolean
  cancelPending: boolean
  onCancel: () => void
}) {
  const boutInProgress = status.includes('BOUT IN PROGRESS')
  const [showOwner, setShowOwner] = useState(false)
  const [inspectedBout, setInspectedBout] = useState<BoutStatus | null>(null)
  const displayedBout = inspectedBout ?? activeBout
  const closeOwner = useCallback(() => setShowOwner(false), [])
  const ownerRef = useAccessibleDialog<HTMLElement>(showOwner, closeOwner)

  async function inspectBout() {
    setInspectedBout(await onInspectBout())
    setShowOwner(true)
  }

  return (
    <main className="retro-screen matchup-screen" aria-live="polite">
      {uiReview && <p className="retro-review">UI review · No live databases · No result</p>}
      <p className="pixel-kicker">Lakebase: The Anti-Demo</p>
      <div className="matchup-lockup">
        <div className="matchup-corner"><DatabaseFighter label="LB" corner="red" /><strong>LAKEBASE</strong><span>Red corner</span></div>
        <b>VS</b>
        <div className="matchup-corner"><DatabaseFighter label={competitor.toLowerCase().includes('aurora') ? 'AUR' : 'RDS'} corner="blue" /><strong>{competitor.toUpperCase()}</strong><span>Blue corner</span></div>
      </div>
      <div className="ringside-strip">
        <p>At ringside</p>
        <div>{personas.map((persona, index) => <img key={persona.id} src={persona.portrait} alt={`${index === 0 ? 'Primary' : 'Secondary'}: ${persona.role}`} />)}</div>
      </div>
      <div className="matchup-wait" role="status">
        <strong className={reducedMotion || boutInProgress ? '' : 'blink'}>{boutInProgress ? 'Bout in progress' : 'Checking both corners…'}</strong>
        <span>{status}</span>
        {boutInProgress && (
          <div className="bout-wait-actions">
            <button onClick={onLeave}>B · Fight card</button>
            <button onClick={inspectBout}>Select · Who’s in the ring?</button>
          </div>
        )}
        {!boutInProgress && canCancel && (
          <div className="bout-wait-actions">
            <button disabled={cancelPending} onClick={onCancel}>
              {cancelPending ? 'Cancelling…' : 'B · Choose another round'}
            </button>
          </div>
        )}
      </div>
      {showOwner && (
        <div className="bout-owner-overlay" role="presentation" onClick={closeOwner}>
          <section ref={ownerRef} className="bout-owner-card" role="dialog" aria-modal="true" aria-labelledby="bout-owner-heading" onClick={(event) => event.stopPropagation()}>
            <p>Current ring lease</p>
            {displayedBout?.active ? (
              <>
                <h2 id="bout-owner-heading">{displayedBout.operator?.display_name ?? 'Authenticated operator'}</h2>
                {displayedBout.operator?.email && <strong>{displayedBout.operator.email}</strong>}
                <dl>
                  <div><dt>Round</dt><dd>{displayedBout.round_title}</dd></div>
                  <div><dt>Opponent</dt><dd>{displayedBout.competitor}</dd></div>
                  <div><dt>Status</dt><dd>{displayedBout.state}</dd></div>
                  <div><dt>Lease phase</dt><dd>{boutPhaseLabel(displayedBout.phase)}</dd></div>
                  <div><dt>Started</dt><dd>{boutStartedLabel(displayedBout.started_at)}</dd></div>
                  <div><dt>Last heartbeat</dt><dd>{boutStartedLabel(displayedBout.updated_at)}</dd></div>
                  <div><dt>Auto-release</dt><dd>{boutExpiryLabel(displayedBout.expires_at)}</dd></div>
                </dl>
              </>
            ) : (
              <h2 id="bout-owner-heading">The ring just cleared</h2>
            )}
            <button onClick={closeOwner}>B · Close</button>
          </section>
        </div>
      )}
    </main>
  )
}

function boutStartedLabel(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  const started = new Date(value)
  if (Number.isNaN(started.getTime())) return 'Unknown'
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric', minute: '2-digit', second: '2-digit',
  }).format(started)
}

function boutExpiryLabel(value: string | null | undefined): string {
  if (!value) return 'After active work completes'
  const expiry = new Date(value)
  if (Number.isNaN(expiry.getTime())) return 'Unknown'
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric', minute: '2-digit', second: '2-digit',
  }).format(expiry)
}

function boutPhaseLabel(value: string | null | undefined): string {
  return value ? value.replaceAll('_', ' ') : 'Unknown'
}

function Ready(props: {
  competitor: string
  roundTitle: string
  competitorLaneState: DemoSession['lanes']['competitor']['state']
  sessionState: DemoSession['state']
  armedExpiresAt: string | null | undefined
  personas: CatalogResponse['personas']
  sound: boolean
  onToggleSound: () => void
  onRing: () => void
  ringBlocked?: boolean
  onHome: () => void
  error: string | null
  uiReview: boolean
}) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!props.armedExpiresAt || props.sessionState !== 'armed') return
    const ticker = window.setInterval(() => setNow(Date.now()), 250)
    return () => window.clearInterval(ticker)
  }, [props.armedExpiresAt, props.sessionState])
  const expiresAt = props.armedExpiresAt ? new Date(props.armedExpiresAt).getTime() : null
  const secondsRemaining = expiresAt === null || Number.isNaN(expiresAt)
    ? null
    : Math.max(0, Math.ceil((expiresAt - now) / 1000))
  // The countdown alone reads as decoration until it is nearly gone, which is
  // exactly when the operator is talking to the room rather than watching it.
  // Below this it announces itself instead.
  const windowClosing = secondsRemaining !== null && secondsRemaining <= ARMED_WINDOW_WARNING_SECONDS
  const capabilityGap = props.competitorLaneState === 'not_supported'
  const canRing = !props.ringBlocked
    && (props.uiReview || (props.sessionState === 'armed' && secondsRemaining !== 0))
  const kicker = capabilityGap
    ? props.uiReview
      ? `Preview · ${props.competitor} capability gap · No live check`
      : `Armed · Lakebase sealed · ${props.competitor} capability gap confirmed`
    : 'Armed · Same task · Same verifier'
  return (
    <main className="retro-screen ready-screen">
      {props.uiReview && <p className="retro-review">UI review · No live databases · No result</p>}
      <HomeLogo className="ready-home-logo" onHome={props.onHome} />
      <p className="pixel-kicker">{kicker}</p>
      <h1>{props.roundTitle}</h1>
      <div className="ready-match"><span>Lakebase</span><b>VS</b><span>{props.competitor}</span></div>
      <div className="ready-ringside" aria-label="People at ringside">
        {props.personas.map((persona, index) => (
          <div key={persona.id}><img src={persona.portrait} alt="" /><span>{index === 0 ? 'Leads' : 'Lens'} · {persona.role}</span></div>
        ))}
      </div>
      {!props.uiReview && secondsRemaining !== null && <p className="armed-window" data-closing={windowClosing ? 'true' : undefined} role={windowClosing ? 'alert' : undefined}>Bell window · {String(Math.floor(secondsRemaining / 60)).padStart(2, '0')}:{String(secondsRemaining % 60).padStart(2, '0')} · {secondsRemaining === 0 ? 'Cleared · Prepare the fight card again' : windowClosing ? 'Closing · Ring now or it clears' : 'Auto-clears if abandoned'}</p>}
      {/* One slot, so the in-flight notice costs this fixed, overflow-hidden
          screen no height it did not already budget for an error. An error
          outranks it: the error is the newer fact, and the notice only ever
          describes a request that is still in flight. */}
      {props.error
        ? <p className="error retro-error" role="alert">{props.error}</p>
        : props.ringBlocked
          ? <p className="bell-notice" role="status">{BELL_IN_FLIGHT}</p>
          : null}
      <button className="bell-button" disabled={!canRing} onClick={props.onRing}>
        {canRing ? 'Ring the bell'
          : props.ringBlocked ? 'Confirming the bell…'
          : secondsRemaining === 0 ? 'Fight card expired'
          : props.sessionState === 'running' ? 'Round in progress'
          : 'Round already run'}
      </button>
      <SoundToggle sound={props.sound} onToggle={props.onToggleSound} />
    </main>
  )
}

function roundFiveExactSetupStopPublished(
  session: DemoSession,
  laneId: LaneId,
): boolean {
  const result = roundFiveSetupLaneResult(session, laneId)
  if (result.stopGateExact) return true
  return session.state === 'towelled'
    && session.round5_setup?.lanes?.[laneId]?.state === 'verified'
    && result.setupElapsedMs !== null
    && classifyOutcome(session).evidence.exactLane === laneId
}

function roundFiveSetupStopEvidenceNote(
  session: DemoSession,
  laneId: LaneId,
): string {
  const result = roundFiveSetupLaneResult(session, laneId)
  if (result.stopGateExact) return 'Stop gate exact: every expected fact matched.'
  if (roundFiveExactSetupStopPublished(session, laneId)) {
    return 'Exact setup stop published before the towel; the final expected/observed gate matrix was not returned.'
  }
  return 'Stop gate not verified for this lane.'
}

function RoundFiveSetupCard({
  session,
  laneId,
  corner,
}: {
  session: DemoSession
  laneId: LaneId
  corner: 'red' | 'blue'
}) {
  const lane = session.lanes[laneId]
  const result = roundFiveSetupLaneResult(session, laneId)
  const exactStopPublished = roundFiveExactSetupStopPublished(session, laneId)
  return (
    <section className="round5-setup-lane" data-corner={corner} aria-label={`${lane.name} setup result`}>
      <header><strong>{lane.name}</strong><span>{exactStopPublished ? 'SETUP STOP ✓' : result.state}</span></header>
      <dl>
        <div><dt>Setup state</dt><dd>{result.state}</dd></div>
        <div>
          <dt>Stop gate</dt>
          <dd>{result.stopGateExact
            ? 'EXACT ✓'
            : exactStopPublished
              ? 'EXACT STOP ✓ · FINAL GATE MATRIX NOT RETURNED BEFORE TOWEL'
              : 'NOT VERIFIED'}</dd>
        </div>
      </dl>
    </section>
  )
}

function RoundFiveBurstCard({
  lane,
  corner,
}: {
  lane: DemoSession['lanes'][LaneId]
  corner: 'red' | 'blue'
}) {
  const result = roundFiveLaneResult(lane)
  return (
    <section className="round5-lane" data-corner={corner} aria-label={`${lane.name} warm burst evidence`}>
      <header><strong>{lane.name}</strong><span>{result.contractVerified ? 'DOWNSTREAM GATES ✓' : 'NOT VERIFIED'}</span></header>
      <dl>
        <div><dt>Scheduled</dt><dd>{roundFiveCountDisplay(result.scheduled)} / {ROUND_FIVE_SCHEDULED_CLIENTS}</dd></div>
        <div><dt>Terminal</dt><dd>{roundFiveCountDisplay(result.terminal)} / {ROUND_FIVE_SCHEDULED_CLIENTS}</dd></div>
        <div><dt>Successes</dt><dd>{roundFiveCountDisplay(result.successes)}</dd></div>
        <div><dt>Errors</dt><dd>{roundFiveCountDisplay(result.errors)}</dd></div>
        <div className="round5-p99"><dt>Nearest-rank p99</dt><dd>{roundFiveP99Display(result)}</dd><small>Raw, unrounded successful-client latencies</small></div>
        <div><dt>Witness clients</dt><dd>{roundFiveCountDisplay(result.witnessVerifiedClients)} / {ROUND_FIVE_WITNESS_CLIENTS}</dd><small>Verified client responses</small></div>
        <div><dt>Backend PIDs</dt><dd>{roundFiveCountDisplay(result.uniqueBackendPids)}</dd><small>Unique returned process IDs</small></div>
        <div><dt>Peak sessions</dt><dd>{roundFiveCountDisplay(result.peakBackendSessions)}</dd><small>Backend sessions observed</small></div>
      </dl>
      {lane.error && <p role="alert">Burst evidence did not validate.</p>}
    </section>
  )
}

function RoundFiveEvidenceDetails({ session }: { session: DemoSession }) {
  const setupSkew = session.round5_setup?.workflow_launch_skew_ms
  const burstSkew = session.fairness.launch_skew_ms
  const awsEngineName = session.competitor.short_name
  const complete = classifyOutcome(session).contractComplete
  return (
    <section className="round5-explain-proof" aria-label="Round 5 detailed proof">
      <header>
        <span>{complete ? 'Full verified proof' : 'Recorded proof status'}</span>
        <strong>Readiness setup is scored · identical spike is pass/fail</strong>
      </header>
      <section className="round5-explain-phase" aria-label="Scored setup evidence">
        <h3>Phase 1 · Readiness setup (scored) · workflow launch skew {typeof setupSkew === 'number' ? `${setupSkew.toFixed(3)} ms` : 'N/A'}</h3>
        <div className="round5-explain-grid">
          <RoundFiveSetupCard session={session} laneId="lakebase" corner="red" />
          <RoundFiveSetupCard session={session} laneId="competitor" corner="blue" />
        </div>
      </section>
      <section className="round5-explain-phase" aria-label="Warm burst evidence">
        <h3>Phase 2 · Identical 128-connection spike (pass/fail) · launch skew {typeof burstSkew === 'number' ? `${burstSkew.toFixed(3)} ms` : 'N/A'}</h3>
        <div className="round5-explain-grid">
          <RoundFiveBurstCard lane={session.lanes.lakebase} corner="red" />
          <RoundFiveBurstCard lane={session.lanes.competitor} corner="blue" />
        </div>
      </section>
      <div className="round5-fairness" aria-label="Round 5 fair proof contract">
        <strong>PHASE 2 FAIRNESS</strong>
        <span>{session.fairness.warmup_connections ?? 'N/A'} warmups / lane</span>
        <span>{session.fairness.concurrency ?? 'N/A'} concurrent clients / lane</span>
        <span>Identical neutral runner · {session.fairness.runner ?? 'N/A'}</span>
        <span>TLS {session.fairness.tls ?? 'N/A'} · timeout {session.fairness.timeout ?? 'N/A'}</span>
      </div>
      <div className="round5-explain-components" aria-label="Managed component disclosure">
        <strong>COMPONENT DISCLOSURE</strong>
        <p>Lakebase · built-in pooled host · 0 extra per-bout pooling components · 0 per-bout pooling infrastructure changes.</p>
        <p>{awsEngineName} · RDS Proxy is the AWS best-practice pooling layer · this declared-start bout provisioned a new Proxy + 8 supporting changes · an already-deployed Proxy would not pay this setup delay.</p>
      </div>
    </section>
  )
}

function RoundSixScene({
  session,
  roundNumber,
  error,
  uiReview,
  sound,
  onToggleSound,
  hasNextRound,
  commentaryOpen,
  onContinue,
  onTowel,
  onHome,
  onToggleCommentary,
  missedCalls = 0,
}: {
  session: DemoSession
  roundNumber: number
  error: string | null
  uiReview: boolean
  sound: boolean
  onToggleSound: () => void
  hasNextRound: boolean
  commentaryOpen: boolean
  onContinue: () => void
  onTowel: () => Promise<void>
  onHome: () => void
  onToggleCommentary: () => void
  /** Calls the play-by-play never got to make; see `RingsideCommentator`. */
  missedCalls?: number
}) {
  const [showRingsideTake, setShowRingsideTake] = useState(false)
  const [showCostRoom, setShowCostRoom] = useState(false)
  const [showShareReceipt, setShowShareReceipt] = useState(false)
  const [showInstantReplay, setShowInstantReplay] = useState(false)
  const primaryMetric = metricValue(session, 'analytics_available_ms')?.value
  const elapsedMs = typeof primaryMetric === 'number'
    ? primaryMetric
    : session.lanes.lakebase.elapsed_ms
  const checkoutVerified = metricValue(session, 'checkout_verified')?.value === true
  const classified = classifyOutcome(session)
  const verified = classified.contractComplete
  const terminal = session.state === 'verified'
    || session.state === 'failed'
    || session.state === 'towelled'
  const failed = terminal && !verified && session.state !== 'towelled'
  const presentation = uiReview ? 'review' : session.state === 'towelled' ? 'towelled' : verified ? 'verified' : failed ? 'failed' : 'running'
  const cleanupAllowsActions = cleanupAllowsTerminalActions(session)
  const shareable = classified.shareable

  return (
    <>
      <RoundSixProof
        state={presentation}
        elapsedMs={elapsedMs}
        separateCheckoutVerified={checkoutVerified}
        competitorLabel={session.competitor.short_name}
        status={error ?? session.failure}
        censoredMs={towelLowerBoundMs(session, 'lakebase')}
        homeControl={<HomeLogo className="home-logo-compact" onHome={onHome} />}
        soundControl={<SoundToggle sound={sound} onToggle={onToggleSound} arena />}
        ringsideContent={!uiReview && session.state === 'towelled'
          ? <TowelLaneResults session={session} />
          : !uiReview && !failed
            ? <RingsideCommentator session={session} open={commentaryOpen} onToggle={onToggleCommentary} missedCalls={missedCalls} />
            : undefined}
        actions={!uiReview ? (
          <>
            <TowelControl session={session} uiReview={uiReview} onTowel={onTowel} />
            {terminal && proofNavigationAllowsExit(session) && <>
              {cleanupAllowsActions && <button type="button" className="proof-replay" onClick={() => setShowInstantReplay(true)}>Select · Instant replay</button>}
              <button type="button" className="round4-ringside" onClick={() => setShowRingsideTake(true)}>Select · Explain to the room</button>
              {cleanupAllowsActions && shareable && <button type="button" className="round4-cost" onClick={() => setShowCostRoom(true)}>Select · What it cost</button>}
              {cleanupAllowsActions && shareable && <button type="button" className="round4-share" onClick={() => setShowShareReceipt(true)}>Start · Share the receipt</button>}
              <button type="button" className="round4-next" onClick={onContinue}>A · {session.state === 'towelled' || hasNextRound ? 'Next round' : verified ? 'Next · Final recap' : 'Fight card'}</button>
            </>}
          </>
        ) : undefined}
      />
      {showRingsideTake && <RingsideTake session={session} onClose={() => setShowRingsideTake(false)} />}
      {showCostRoom && <CostRoom session={session} onClose={() => setShowCostRoom(false)} />}
      {showShareReceipt && <ShareReceipt session={session} roundNumber={roundNumber} kind="round" onClose={() => setShowShareReceipt(false)} />}
      {showInstantReplay && <InstantReplay session={session} roundNumber={roundNumber} onClose={() => setShowInstantReplay(false)} />}
    </>
  )
}

function roundFiveArenaLane(
  session: DemoSession,
  laneId: LaneId,
): DemoSession['lanes'][LaneId] {
  const setupLane = session.round5_setup?.lanes?.[laneId]
  const fallbackLane = session.lanes[laneId]
  const setupState = setupLane?.state ?? 'pending'
  const state: DemoSession['lanes'][LaneId]['state'] = setupState === 'running'
    ? 'connecting'
    : setupState === 'verified'
      ? 'verified'
      : setupState === 'failed' || setupState === 'cleanup_failed'
        ? 'failed'
        : setupState === 'towelled'
          ? 'towelled'
          : 'sealed'
  // Only a running lane consumes the projected floor. Once a lane reaches any
  // terminal state, the exact callback latch is the stopped measurement. This
  // keeps refresh recovery from rewinding a silent setup phase without letting
  // a stale projection leak past a stop gate, failure, or towel.
  const elapsedCandidate = setupState === 'running'
    ? setupLane?.elapsed_at_snapshot_ms ?? setupLane?.setup_elapsed_ms
    : setupLane?.setup_elapsed_ms
  const elapsedMs = typeof elapsedCandidate === 'number'
    && Number.isFinite(elapsedCandidate)
    && elapsedCandidate >= 0
    ? elapsedCandidate
    : null
  const status = state === 'sealed'
    ? 'Untimed shared preflight · Setup clock has not started'
    : setupLane?.status?.trim()
      || (state === 'verified'
        ? 'Exact setup stop gate verified'
        : state === 'failed'
          ? 'Setup stop gate did not verify'
          : state === 'towelled'
            ? 'Setup stopped before verification'
            : 'Setup clock live')

  return {
    id: laneId,
    name: setupLane?.name || fallbackLane.name,
    state,
    elapsed_ms: elapsedMs,
    attempts: 0,
    status,
    error: null,
    activity: {
      phase: `setup_${setupState}`,
      wire_call: null,
    },
  }
}

export function RoundFiveProof({
  session,
  roundNumber,
  error,
  liveEvidenceConnected,
  uiReview,
  sound = true,
  onToggleSound = () => {},
  hasNextRound,
  commentaryOpen,
  onContinue,
  onTowel = async () => {},
  cleanupPending = false,
  onRetryCleanup,
  onHome,
  onToggleCommentary,
  missedCalls = 0,
}: {
  session: DemoSession
  roundNumber: number
  error: string | null
  liveEvidenceConnected: boolean
  uiReview: boolean
  sound?: boolean
  onToggleSound?: () => void
  hasNextRound: boolean
  commentaryOpen: boolean
  onContinue: () => void
  onTowel?: () => Promise<void>
  cleanupPending?: boolean
  onRetryCleanup?: () => Promise<void> | void
  onHome: () => void
  onToggleCommentary: () => void
  /** Calls the play-by-play never got to make; see `RingsideCommentator`. */
  missedCalls?: number
}) {
  const [showRingsideTake, setShowRingsideTake] = useState(false)
  const [showShareReceipt, setShowShareReceipt] = useState(false)
  const [showInstantReplay, setShowInstantReplay] = useState(false)
  const [showCostRoom, setShowCostRoom] = useState(false)
  const cleanupRetryable = session.round5_setup?.cleanup_retryable === true
  /* The one fact `cleanup_retryable` cannot carry. It is true while cleanup is
     still retrying and true again once the server has given up, so a screen
     reading it alone cannot tell a tidy-up in flight from an abandoned one --
     and the second is the one where a run-owned proxy may still exist. Set
     only on abandonment, so its presence is the distinction. */
  const cleanupAbandoned = session.round5_setup?.cleanup_failure || null
  const cleanupAllowsActions = cleanupAllowsTerminalActions(session)
  const classified = classifyOutcome(session)
  const hasComparison = classified.contractComplete
  const shareable = classified.shareable
  const oneSidedSetupTowel = classified.outcome.outcome_id === 'one_sided_setup_verified_towel'
  const oneSidedExactLane = oneSidedSetupTowel ? classified.evidence.exactLane : null
  const oneSidedUnverifiedLane: LaneId | null = oneSidedExactLane === 'lakebase'
    ? 'competitor'
    : oneSidedExactLane === 'competitor' ? 'lakebase' : null
  const oneSidedLowerBound = oneSidedUnverifiedLane
    ? classified.evidence[oneSidedUnverifiedLane].lowerBoundMs
    : null
  const oneSidedSetupLead = oneSidedExactLane && oneSidedUnverifiedLane
    ? `${session.round5_setup?.lanes?.[oneSidedExactLane]?.name ?? session.lanes[oneSidedExactLane].name} verified first · ${session.round5_setup?.lanes?.[oneSidedUnverifiedLane]?.name ?? session.lanes[oneSidedUnverifiedLane].name} unverified${oneSidedLowerBound === null ? '' : ` beyond ${laneReceiptTime(oneSidedLowerBound)}`}`
    : 'Bout stopped · Setup clocks frozen'
  const showCleanupFallback = cleanupRetryable
    && !session.towel
    && !uiReview
    && Boolean(onRetryCleanup)
  if (uiReview || session.state !== 'failed') {
    const lakebaseSetupLane = roundFiveArenaLane(session, 'lakebase')
    const competitorSetupLane = roundFiveArenaLane(session, 'competitor')
    const verified = session.state === 'verified'
    const towelled = session.state === 'towelled'
    const liveEvidenceInterrupted = session.state === 'running' && !uiReview && !liveEvidenceConnected
    const verdict = roundFiveVerifiedVerdict(session)
    return (
      <>
        <main className="proof-screen round5-arena" data-session-state={liveEvidenceInterrupted ? 'offline' : session.state}>
          <header className="proof-header">
            <HomeLogo className="home-logo-compact" onHome={onHome} />
            <div className="proof-title">
              <p>
                Round {roundNumber} · Pass/fail spike: {ROUND_FIVE_SCHEDULED_CLIENTS} fresh app connection attempts / lane · max {ROUND_FIVE_CONCURRENCY} at once · after {ROUND_FIVE_WARMUPS} untimed warmups
              </p>
              <h1>{ROUND_FIVE_DISPLAY_TITLE}</h1>
            </div>
            <div className="proof-state" data-state={liveEvidenceInterrupted ? 'offline' : session.state}>
              {uiReview ? 'UI review' : liveEvidenceInterrupted ? 'Proof paused · reconnecting' : stateLabel(session.state)}
            </div>
            <SoundToggle sound={sound} onToggle={onToggleSound} arena />
          </header>
          <div className="proof-lanes">
            <Lane
              lane={lakebaseSetupLane}
              fallbackLabel="Lakebase"
              fighterLabel="LB"
              corner="red"
              sessionState={session.state}
              liveEvidenceConnected={liveEvidenceConnected}
              uiReview={uiReview}
              censoredMs={towelled ? roundFiveCensoredLowerBoundMilliseconds(session, 'lakebase') ?? undefined : undefined}
              notTimed={towelled
                && lakebaseSetupLane.state !== 'verified'
                && roundFiveCensoredLowerBoundMilliseconds(session, 'lakebase') === null}
            />
            <div className="lane-rule" aria-hidden="true"><span>VS</span></div>
            <Lane
              lane={competitorSetupLane}
              fallbackLabel={session.competitor.short_name}
              fighterLabel={session.competitor.short_name.toLowerCase().includes('aurora') ? 'AUR' : 'RDS'}
              corner="blue"
              sessionState={session.state}
              liveEvidenceConnected={liveEvidenceConnected}
              uiReview={uiReview}
              censoredMs={towelled ? roundFiveCensoredLowerBoundMilliseconds(session, 'competitor') ?? undefined : undefined}
              notTimed={towelled
                && competitorSetupLane.state !== 'verified'
                && roundFiveCensoredLowerBoundMilliseconds(session, 'competitor') === null}
            />
          </div>
          <footer className="proof-footer round5-arena-footer">
            {error && !verified && <p className="proof-error" role="alert">{error}</p>}
            {!uiReview && (
              <RingsideCommentator
                session={session}
                open={commentaryOpen}
                onToggle={onToggleCommentary}
                liveEvidenceConnected={verified || liveEvidenceConnected}
                missedCalls={missedCalls}
              />
            )}
            {verified ? (
              <>
                <div className="remembered" role="status" aria-atomic="true">
                  <span>{hasComparison ? 'Verified readiness comparison' : 'Contract gate · no comparison'}</span>
                  <strong>{verdict}</strong>
                </div>
                <p className="final-fairness">Readiness setup is scored · Identical 128-connection spike is pass/fail, not a speed comparison</p>
                {/* A verified Round 5 keeps its win and can still fail to tidy
                    up, which is the case this screen used to render as a bare
                    "settling backstage" line whether cleanup was still trying
                    or had been given up on hours ago. */}
                {cleanupAbandoned ? (
                  <div className="cleanup-abandoned" data-state="failed" role="alert" aria-live="polite">
                    <strong>{CLEANUP_ABANDONED_TITLE}</strong>
                    <span>{cleanupAbandoned}</span>
                  </div>
                ) : cleanupRetryable ? (
                  <p className="proof-error" role="status">Automatic cleanup is settling backstage · Ring protected</p>
                ) : null}
                {showCleanupFallback && (
                  <button
                    className="round5-retry-cleanup"
                    disabled={cleanupPending}
                    onClick={onRetryCleanup}
                  >
                    B · {cleanupPending ? 'Retrying cleanup…' : 'Retry cleanup'}
                  </button>
                )}
                {!uiReview && !cleanupAllowsActions && (
                  <button className="proof-ringside" onClick={() => setShowRingsideTake(true)}>Select · Explain to the room</button>
                )}
                {!uiReview && cleanupAllowsActions && (
                  <div className="proof-actions">
                    {shareable && <button className="proof-replay" onClick={() => setShowInstantReplay(true)}>Select · Instant replay</button>}
                    <button className="proof-ringside" onClick={() => setShowRingsideTake(true)}>Select · Explain to the room</button>
                    {shareable && <button className="proof-cost" onClick={() => setShowCostRoom(true)}>Select · What it cost</button>}
                    {shareable && <button className="proof-share" onClick={() => setShowShareReceipt(true)}>Start · Share the receipt</button>}
                    <button className="proof-next" onClick={onContinue}>A · {hasNextRound ? 'Next round' : 'Fight card'}</button>
                  </div>
                )}
              </>
            ) : towelled ? (
              <>
                <div className="remembered" role="status" aria-label="Round 5 setup status" aria-atomic="true">
                  <span>{oneSidedSetupLead}</span>
                  <strong>{oneSidedSetupTowel
                    ? 'No declared winner · comparison incomplete · margin N/A'
                    : classified.headline}</strong>
                </div>
                <TowelControl session={session} uiReview={uiReview} onTowel={onTowel} />
                {!uiReview && proofNavigationAllowsExit(session) && (
                  <div className="proof-actions">
                    {cleanupAllowsActions && <button className="proof-replay" onClick={() => setShowInstantReplay(true)}>Select · Instant replay</button>}
                    <button className="proof-ringside" onClick={() => setShowRingsideTake(true)}>Select · Explain to the room</button>
                    <button type="button" className="proof-next" onClick={onContinue}>A · {hasNextRound ? 'Next round' : 'Fight card'}</button>
                  </div>
                )}
              </>
            ) : (
              <>
                <div className="fairness">
                  <span aria-hidden="true">◆</span>
                  Readiness setup race · Each clock stops at its own exact application transaction
                  <span aria-hidden="true">◆</span>
                </div>
                <TowelControl
                  session={session}
                  uiReview={uiReview}
                  disabled={!liveEvidenceConnected}
                  onTowel={onTowel}
                />
              </>
            )}
          </footer>
          <div className="proof-scanlines" aria-hidden="true" />
        </main>
        {showInstantReplay && <InstantReplay session={session} roundNumber={roundNumber} onClose={() => setShowInstantReplay(false)} />}
        {showRingsideTake && <RingsideTake session={session} onClose={() => setShowRingsideTake(false)} />}
        {showCostRoom && <CostRoom session={session} onClose={() => setShowCostRoom(false)} />}
        {showShareReceipt && <ShareReceipt session={session} roundNumber={roundNumber} kind="round" onClose={() => setShowShareReceipt(false)} />}
      </>
    )
  }
  const terminal = session.state === 'failed'
  const cleanupFailed = session.state === 'failed'
    && session.round5_setup?.state === 'cleanup_failed'
  const setupSkew = typeof session.round5_setup?.workflow_launch_skew_ms === 'number'
    && Number.isFinite(session.round5_setup.workflow_launch_skew_ms)
    ? `${session.round5_setup.workflow_launch_skew_ms.toFixed(3)} ms`
    : 'N/A'
  const awsEngineName = session.competitor.short_name
  const setupResults = {
    lakebase: roundFiveSetupLaneResult(session, 'lakebase'),
    competitor: roundFiveSetupLaneResult(session, 'competitor'),
  }
  const setupStateLabel = (result: typeof setupResults.lakebase) => (
    terminal && result.state === 'verified' && !result.stopGateExact
      ? 'progress reached · not finalized'
      : result.state
  )
  const setupValidated = Boolean(
    session.round5_setup?.setup_validated
    && setupResults.lakebase.verified
    && setupResults.competitor.verified,
  )
  const compactResult = cleanupFailed
    ? 'BACKSTAGE RECOVERY'
    : session.state === 'failed'
      ? oneSidedSetupTowel
        ? 'ONE SETUP VERIFIED'
        : setupValidated ? 'BURST NOT VERIFIED' : 'SETUP NOT VERIFIED'
      : 'SETUP IN PROGRESS'
  /* Same order of preference the remembered result uses: the server's sentence
     if it sent one, ours only if it did not.

     The fallback stays as written, because it is still true of the case that
     reaches it. `cleanup_failed` is set both while retries are running and
     once they have been abandoned; only the abandoned one carries a
     diagnostic, so "retrying backstage" describes exactly the sessions that
     fall through to it. */
  const compactReason = cleanupFailed
    ? cleanupAbandoned ?? 'Automatic cleanup is retrying backstage. The ring stays protected until a clean baseline is verified.'
    : session.state === 'failed'
      ? oneSidedSetupTowel
        ? classified.headline
        : setupValidated
        ? 'Setup completed, but the warm connection burst did not pass every proof gate. No winner was declared.'
        : 'Both lanes did not reach the exact setup stop gate. No timing comparison was declared.'
      : setupValidated
        ? 'Both setup workflows verified. The warm connection burst is now being checked.'
        : `Lakebase and ${awsEngineName} are completing their setup workflows. Timing stops only after each exact transaction verifies.`

  return (
    <>
      <main className="round5-screen" data-session-state={session.state} data-layout="compact">
        <header className="round5-header">
          <HomeLogo className="home-logo-compact" onHome={onHome} />
          <div>
            <p>Round {roundNumber} · Built-in pool · Added RDS Proxy</p>
            <h1>{ROUND_FIVE_DISPLAY_TITLE}</h1>
            <p className="round5-matchup"><strong>Lakebase</strong><span>vs</span><strong>{session.lanes.competitor.name}</strong></p>
          </div>
          <strong>{uiReview ? 'UI REVIEW' : cleanupFailed ? 'Backstage recovery' : stateLabel(session.state)}</strong>
          <SoundToggle sound={sound} onToggle={onToggleSound} arena />
        </header>

        <div className="round5-compact-body">
            <section className="round5-result-card" aria-label="Round 5 setup status" role="status">
              <p>PRIMARY SETUP RESULT</p>
              <h2>{compactResult}</h2>
              <p className="round5-result-reason">{compactReason}</p>
              <details className="round5-technical-details">
                <summary>Technical details</summary>
                <dl>
                  <div><dt>Lakebase setup</dt><dd>{setupStateLabel(setupResults.lakebase)} · stop gate {setupResults.lakebase.stopGateExact ? 'exact' : 'not verified'}</dd></div>
                  <div><dt>{awsEngineName} setup</dt><dd>{setupStateLabel(setupResults.competitor)} · stop gate {setupResults.competitor.stopGateExact ? 'exact' : 'not verified'}</dd></div>
                  <div><dt>Workflow launch skew</dt><dd>{setupSkew}</dd></div>
                </dl>
                <p>Lakebase uses its built-in pooled host. The AWS lane adds a new RDS Proxy and 8 supporting changes; IAM and Proxy credentials are required before the clock.</p>
              </details>
              <TowelControl session={session} uiReview={uiReview} onTowel={onTowel} />
              {!uiReview && (terminal || showCleanupFallback) && (
                <div className="round5-compact-actions">
                  {showCleanupFallback && (
                    <button
                      className="round5-retry-cleanup"
                      disabled={cleanupPending}
                      onClick={onRetryCleanup}
                    >
                      B · {cleanupPending ? 'Retrying cleanup…' : 'Retry cleanup'}
                    </button>
                  )}
                  <button className="proof-ringside" onClick={() => setShowRingsideTake(true)}>Select · Explain to the room</button>
                  {!cleanupFailed && terminal && cleanupAllowsActions && (
                    <button className="round5-next" onClick={onContinue}>A · {hasNextRound ? 'Next round' : 'Fight card'}</button>
                  )}
                </div>
              )}
            </section>
          </div>
        <div className="proof-scanlines" aria-hidden="true" />
      </main>
      {showRingsideTake && <RingsideTake session={session} onClose={() => setShowRingsideTake(false)} />}
    </>
  )
}

type ModelScoreSnapshot = Pick<DemoSession, 'lanes' | 'metrics'>

function scoreText(value: string): string {
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric.toFixed(2) : value
}

function roundFourMetricMilliseconds(snapshot: ModelScoreSnapshot, specId: string): number | null {
  const metric = metricValue(snapshot, specId)
  const value = typeof metric?.value === 'number' || typeof metric?.value === 'string'
    ? Number(metric.value)
    : Number.NaN
  return Number.isFinite(value) && value >= 0 ? value : null
}

function roundFourDuration(snapshot: ModelScoreSnapshot, specId: string): string {
  const value = roundFourMetricMilliseconds(snapshot, specId)
  return value === null ? metricDisplay(snapshot, specId) : preciseDuration(value)
}

function roundFourAppElapsed(snapshot: ModelScoreSnapshot): string {
  const metricDuration = roundFourDuration(snapshot, 'application_proof_elapsed_ms')
  if (metricDuration !== '—') return metricDuration
  const elapsed = snapshot.lanes.lakebase.elapsed_ms
  return elapsed === null ? '—' : preciseDuration(elapsed)
}

function RoundFourCompletedTiming({ snapshot }: { snapshot: ModelScoreSnapshot }) {
  const syncMs = roundFourMetricMilliseconds(snapshot, 'managed_availability_ms')
  const totalMs = roundFourMetricMilliseconds(snapshot, 'application_proof_elapsed_ms')
  if (syncMs === null || totalMs === null || totalMs < syncMs) {
    return (
      <>
        <strong className="round4-elapsed">{roundFourAppElapsed(snapshot)}</strong>
        <p>Delta commit → exact app read</p>
      </>
    )
  }
  return (
    <div className="round4-timing-breakdown" aria-label="Completed reverse ETL timing breakdown">
      <div><span>Reverse ETL sync</span><strong>{preciseDuration(syncMs)}</strong></div>
      <div>
        <span>Full proof time</span>
        <strong>{preciseDuration(totalMs)}</strong>
        <small>Sync check + fresh app connection + exact row read</small>
      </div>
    </div>
  )
}

function ModelScoreStory({ snapshot, version, uiReview = false }: { snapshot: ModelScoreSnapshot; version: 'v1' | 'v2'; uiReview?: boolean }) {
  const lane = snapshot.lanes.lakebase
  const evidence = modelScoreEvidence(lane)
  const active = lane.state === 'connecting' || lane.state === 'verifying'
  const verified = evidence.exactRowVerified
  const sourceCustomer = evidence.primaryKey === '—' ? 'Incoming customer row' : evidence.primaryKey
  const sourceScore = evidence.score === '—' ? 'Pending' : scoreText(evidence.score)
  const sourceModel = evidence.modelVersion === '—' ? 'Pending' : evidence.modelVersion
  return (
    <section className="round4-story" data-verified={verified} aria-label={`Lakebase ${version} result`}>
      <header className="round4-story-header">
        <div>
          <span>Executed Lakebase path · {version}</span>
          <strong>{verified ? `Customer ${evidence.primaryKey} · live app row verified` : 'Lakehouse change → live app row'}</strong>
        </div>
        <b>{uiReview ? 'UI REVIEW' : verified ? 'LIVE APP VERIFIED ✓' : `${version} SYNC RUNNING`}</b>
      </header>

      <div className="round4-flow" aria-label="Lakehouse to live app data flow">
        <article className="round4-flow-node round4-source">
          <span>1 · Analytics Delta</span>
          <strong>Customer risk score</strong>
          <dl>
            <div><dt>Customer ID</dt><dd>{sourceCustomer}</dd></div>
            <div><dt>Risk score</dt><dd>{sourceScore}</dd></div>
            <div><dt>Model</dt><dd>{sourceModel}</dd></div>
          </dl>
        </article>

        <div className="round4-flow-arrow" aria-hidden="true"><span>Change written</span><b>→</b></div>

        <article className="round4-flow-node round4-sync">
          <span>2 · Managed Reverse ETL</span>
          {uiReview
            ? <strong className="round4-no-measurement">—</strong>
            : verified
              ? <RoundFourCompletedTiming snapshot={snapshot} />
              : <InterpolatedAuthoritativeTimerValue elapsedMs={lane.elapsed_ms} active={active} />}
          {!verified && <p>{uiReview
            ? 'No live transaction in UI review'
            : `Clock stops after exact app row read-back · ${lane.status}`}</p>}
        </article>

        <div className="round4-flow-arrow" aria-hidden="true"><span>Row applied</span><b>→</b></div>

        <article className="round4-flow-node round4-target">
          <span>3 · Operational Postgres / Live App</span>
          <small>Lakebase destination in this bout</small>
          <div className="round4-customer-card">
            <header><strong>Customer record</strong><b>{verified ? 'LIVE · UPDATED ✓' : 'WAITING FOR UPDATE'}</b></header>
            <dl>
              <div><dt>Customer ID</dt><dd>{verified ? evidence.primaryKey : '—'}</dd></div>
              <div><dt>Risk score</dt><dd>{verified ? scoreText(evidence.score) : '—'}</dd></div>
              <div><dt>Model</dt><dd>{verified ? evidence.modelVersion : '—'}</dd></div>
            </dl>
          </div>
        </article>
      </div>

    </section>
  )
}

function RoundFourV1Ribbon({ session }: { session: DemoSession }) {
  const evidence = modelScoreEvidence(session.lanes.lakebase)
  return (
    <aside className="round4-v1-ribbon" aria-label="Immutable v1 verified proof">
      <strong>Previous live app state · V1 verified</strong>
      <span>Customer {evidence.primaryKey}</span>
      <span>Risk score {scoreText(evidence.score)}</span>
      <span>Model {evidence.modelVersion}</span>
      <b>Exact row ✓</b>
    </aside>
  )
}

function RoundFourRunningProof({ session, redo = false, uiReview = false }: { session: DemoSession; redo?: boolean; uiReview?: boolean }) {
  const snapshot = redo ? session.redo! : session
  return <ModelScoreStory snapshot={snapshot} version={redo ? 'v2' : 'v1'} uiReview={uiReview} />
}

function RoundFourAwsDisclosure({ result, verified }: { result: string; verified: boolean }) {
  return (
    <aside className="round4-aws-disclosure" aria-label="AWS disclosure" data-verified={verified}>
      <header>
        <strong>Why Lakebase wins this round</strong>
      </header>
      <div className="round4-platform-path">
        <span>Lakebase</span>
        <strong>Built-in OLAP → OLTP</strong>
        <p>Managed reverse ETL · {result}</p>
      </div>
      <div className="round4-versus" aria-hidden="true"><span>VS</span></div>
      <div className="round4-added-stack">
        <span>Aurora / RDS</span>
        <strong>Separate reverse-ETL stack required</strong>
        <p>Add product + connectors + security + network + operations</p>
        <b>Not built or timed</b>
      </div>
    </aside>
  )
}

function RoundFourProof({
  session,
  roundNumber,
  error,
  uiReview,
  sound,
  onToggleSound,
  redoPending,
  hasNextRound,
  commentaryOpen,
  onContinue,
  onRedo,
  onTowel,
  onHome,
  onToggleCommentary,
  missedCalls = 0,
}: {
  session: DemoSession
  roundNumber: number
  error: string | null
  uiReview: boolean
  sound: boolean
  onToggleSound: () => void
  redoPending: boolean
  hasNextRound: boolean
  commentaryOpen: boolean
  onContinue: () => void
  onRedo: () => void
  onTowel: () => Promise<void>
  onHome: () => void
  onToggleCommentary: () => void
  /** Calls the play-by-play never got to make; see `RingsideCommentator`. */
  missedCalls?: number
}) {
  const serverPresentation = roundFourPresentation(session)
  const initialClassification = classifyOutcome(session)
  const presentation = serverPresentation === 'initial_verified'
    && !initialClassification.contractComplete
    ? 'initial_failed'
    : serverPresentation
  const redo = session.redo
  const initialEvidence = modelScoreEvidence(session.lanes.lakebase)
  const redoEvidence = redo ? modelScoreEvidence(redo.lanes.lakebase) : null
  const showV1Ribbon = presentation === 'redo_running' || presentation === 'redo_verified' || presentation === 'redo_failed'
  const terminal = presentation !== 'initial_running' && presentation !== 'redo_running'
  const cleanupAllowsActions = cleanupAllowsTerminalActions(session)
  const activeSession: DemoSession = presentation.startsWith('redo_') && redo
    ? { ...session, lanes: redo.lanes, metrics: redo.metrics, comparison: redo.comparison ?? null, failure: redo.failure ?? null }
    : session
  const resultText = presentation === 'initial_verified'
    ? `Verified: ${initialEvidence.primaryKey} risk score ${scoreText(initialEvidence.score)} reached the live app.`
    : presentation === 'redo_verified' && redoEvidence
      ? `Verified again: ${redoEvidence.primaryKey} risk score changed ${scoreText(initialEvidence.score)} → ${scoreText(redoEvidence.score)} in the lakehouse and reached the live app.`
      : presentation === 'redo_running'
        ? 'Changing the score in the lakehouse and watching the live app for the v2 update.'
      : presentation === 'initial_running'
          ? 'Syncing the lakehouse score and watching the live app for the exact customer update.'
          : presentation === 'initial_towelled'
            ? session.remembered_result ?? 'The bout was toweled at the server cutoff.'
          : 'No new live app update was verified.'
  const resultSession: DemoSession = presentation === 'redo_verified' && redo
    ? { ...activeSession, state: 'verified', remembered_result: resultText }
    : { ...session, remembered_result: resultText }
  const classified = classifyOutcome(resultSession)
  const shareable = classified.shareable
  const capabilityVerified = shareable
  const [showRingsideTake, setShowRingsideTake] = useState(false)
  const [showShareReceipt, setShowShareReceipt] = useState(false)
  const [showInstantReplay, setShowInstantReplay] = useState(false)
  const [showCostRoom, setShowCostRoom] = useState(false)
  const status = uiReview
    ? 'UI REVIEW · NO RESULT'
    : presentation === 'initial_failed'
    ? 'V1 FAILED'
    : presentation === 'initial_towelled'
      ? 'TOWELED'
    : presentation === 'redo_failed'
      ? 'V1 VERIFIED · V2 NOT VERIFIED'
      : presentation === 'redo_verified'
        ? 'V2 VERIFIED'
        : presentation === 'redo_running'
          ? 'V2 RUNNING'
          : presentation === 'initial_verified'
            ? 'V1 VERIFIED'
            : 'V1 RUNNING'
  const matchupResult = uiReview
    ? 'UI review · no result'
    : presentation === 'initial_verified'
      ? `live app verified in ${roundFourAppElapsed(session)}`
      : presentation === 'initial_towelled'
        ? session.lanes.lakebase.state === 'verified' ? `live app verified in ${roundFourAppElapsed(session)}` : 'stopped at server cutoff'
      : presentation === 'redo_verified' && redo
        ? `live app verified again in ${roundFourAppElapsed(redo)}`
        : presentation === 'redo_failed'
          ? 'v1 verified · v2 not verified'
          : presentation === 'initial_failed'
            ? 'not verified'
            : 'live proof running'
  return (
    <main className="round4-screen" data-presentation={presentation}>
      <header className="round4-header">
        <HomeLogo className="home-logo-compact" onHome={onHome} />
        <div><p>Round {roundNumber} · Reverse ETL · OLAP → OLTP</p><h1>{session.round.title}</h1></div>
        <strong>{status}</strong>
        <SoundToggle sound={sound} onToggle={onToggleSound} arena />
      </header>

      <div className="round4-live-region" role="status" aria-live="polite" aria-atomic="true">
        {error ?? (presentation === 'redo_failed' ? redo?.failure : presentation === 'initial_failed' ? session.failure : status)}
      </div>

      <div className="round4-body">
        <RoundFourAwsDisclosure result={matchupResult} verified={capabilityVerified} />
        {showV1Ribbon && <RoundFourV1Ribbon session={session} />}
        {presentation === 'initial_running' && <RoundFourRunningProof session={session} uiReview={uiReview} />}
        {presentation === 'initial_failed' && (
          <section className="round4-failure">
            <strong>{initialClassification.evidence.exactLane
              ? 'SCORE IDENTITY NOT VERIFIED'
              : 'NO RESULT VERIFIED'}</strong>
            <p>{initialClassification.headline}</p>
            {(session.failure || session.lanes.lakebase.error) && (
              <small>{session.failure ?? session.lanes.lakebase.error}</small>
            )}
          </section>
        )}
        {presentation === 'initial_verified' && (
          <ModelScoreStory snapshot={session} version="v1" />
        )}
        {presentation === 'initial_towelled' && <TowelLaneResults session={session} />}
        {presentation === 'redo_running' && <RoundFourRunningProof session={session} redo uiReview={uiReview} />}
        {presentation === 'redo_verified' && redo && (
          <>
            <ModelScoreStory snapshot={redo} version="v2" />
            <p className="round4-same-pk">
              LIVE APP UPDATED AGAIN · CUSTOMER {redoEvidence!.primaryKey} · RISK SCORE {scoreText(initialEvidence.score)} → {scoreText(redoEvidence!.score)} · MODEL {initialEvidence.modelVersion} → {redoEvidence!.modelVersion}
            </p>
          </>
        )}
        {presentation === 'redo_failed' && (
          <section className="round4-failure round4-redo-failure">
            <strong>V2 RESULT NOT VERIFIED</strong>
            <p>{redo?.failure ?? redo?.lanes.lakebase.error ?? 'The v2 exact row proof did not verify.'}</p>
            <small>V1 remains verified and unchanged.</small>
          </section>
        )}
        {!uiReview && (!terminal || shareable) && (
          <RingsideCommentator session={activeSession} open={commentaryOpen} onToggle={onToggleCommentary} missedCalls={missedCalls} />
        )}
        <TowelControl session={session} uiReview={uiReview} onTowel={onTowel} />
      </div>

      <footer className="round4-footer">
        <p className="sr-only">{resultText}</p>
        {terminal && proofNavigationAllowsExit(session) && (
          <div className="round4-actions">
            {cleanupAllowsActions && presentation === 'initial_verified' && canStartRoundFourRedo(session) && (
              <div className="starred-redo-action">
                <button className="round4-redo" disabled={redoPending} onClick={onRedo}>
                  B · RE-DO
                </button>
              </div>
            )}
            {cleanupAllowsActions && <button className="proof-replay" onClick={() => setShowInstantReplay(true)}>Select · Instant replay</button>}
            <button className="round4-ringside" onClick={() => setShowRingsideTake(true)}>Select · Explain to the room</button>
            {cleanupAllowsActions && shareable && <button className="round4-cost" onClick={() => setShowCostRoom(true)}>Select · What it cost</button>}
            {cleanupAllowsActions && shareable && <button className="round4-share" onClick={() => setShowShareReceipt(true)}>Start · Share the receipt</button>}
            <button type="button" className="round4-next" title={hasNextRound ? 'Continue to the next round' : 'Return to the fight card'} onClick={onContinue}>A · Next round</button>
          </div>
        )}
      </footer>
      {showRingsideTake && <RingsideTake session={resultSession} onClose={() => setShowRingsideTake(false)} />}
      {showCostRoom && <CostRoom session={resultSession} onClose={() => setShowCostRoom(false)} />}
      {showShareReceipt && <ShareReceipt session={resultSession} roundNumber={roundNumber} kind="round" onClose={() => setShowShareReceipt(false)} />}
      {showInstantReplay && <InstantReplay session={resultSession} roundNumber={roundNumber} onClose={() => setShowInstantReplay(false)} />}
      <div className="proof-scanlines" aria-hidden="true" />
    </main>
  )
}

function Proof({
  session,
  roundNumber,
  task,
  competitor,
  error,
  liveEvidenceConnected,
  uiReview,
  sound,
  onToggleSound,
  hasNextRound,
  onContinue,
  onRedo,
  onTowel,
  onHome,
  commentaryOpen,
  onToggleCommentary,
  missedCalls = 0,
}: {
  session: DemoSession
  roundNumber: number
  task: string
  competitor: string
  error: string | null
  liveEvidenceConnected: boolean
  uiReview: boolean
  sound: boolean
  onToggleSound: () => void
  hasNextRound: boolean
  onContinue: () => void
  onRedo: () => void
  onTowel: () => Promise<void>
  onHome: () => void
  commentaryOpen: boolean
  onToggleCommentary: () => void
  /** Calls the play-by-play never got to make; see `RingsideCommentator`. */
  missedCalls?: number
}) {
  const complete = session.state === 'verified' || session.state === 'towelled' || session.state === 'failed'
  const failedOwnedArtifactRound = session.state === 'failed'
    && (session.round.id === 'make_schema_change_safely' || session.round.id === 'recover_deleted_order')
  const genericRedoAllowed = session.round.redo?.policy === 'show'
    || session.round.redo?.policy === 'optional'
  const roundOneIdleClockLive = session.round.id === 'wake_idle_app'
    && session.cooldown?.state === 'watching'
  const capabilityGap = session.lanes.competitor.state === 'not_supported'
  const liveEvidenceInterrupted = !uiReview
    && session.state === 'running'
    && !liveEvidenceConnected
  const cleanupAllowsActions = cleanupAllowsTerminalActions(session)
  const classified = classifyOutcome(session)
  const shareable = classified.shareable
  const [showRingsideTake, setShowRingsideTake] = useState(false)
  const [showShareReceipt, setShowShareReceipt] = useState(false)
  const [showInstantReplay, setShowInstantReplay] = useState(false)
  const [showCostRoom, setShowCostRoom] = useState(false)
  return (
    <main className="proof-screen" data-session-state={liveEvidenceInterrupted ? 'offline' : session.state}>
      <header className="proof-header">
        <HomeLogo className="home-logo-compact" onHome={onHome} />
        <div className="proof-title"><p>Round {roundNumber} · Live competitive proof</p><h1>{task}</h1></div>
        <div className="proof-state" data-state={liveEvidenceInterrupted ? 'offline' : session.state}>
          {uiReview ? 'UI review' : liveEvidenceInterrupted ? 'Proof paused · reconnecting' : stateLabel(session.state)}
        </div>
        <SoundToggle sound={sound} onToggle={onToggleSound} arena />
      </header>
      <div className="proof-lanes">
        <Lane
          lane={session.lanes.lakebase}
          fallbackLabel="Lakebase"
          fighterLabel="LB"
          corner="red"
          sessionState={session.state}
          liveEvidenceConnected={liveEvidenceConnected}
          uiReview={uiReview}
          censoredMs={towelLowerBoundMs(session, 'lakebase') ?? undefined}
        />
        <div className="lane-rule" aria-hidden="true"><span>VS</span></div>
        <Lane
          lane={session.lanes.competitor}
          fallbackLabel={competitor}
          fighterLabel={competitor.toLowerCase().includes('aurora') ? 'AUR' : 'RDS'}
          corner="blue"
          sessionState={session.state}
          liveEvidenceConnected={liveEvidenceConnected}
          uiReview={uiReview}
          censoredMs={towelLowerBoundMs(session, 'competitor') ?? undefined}
        />
      </div>
      <footer className="proof-footer" data-capability={capabilityGap}>
        {error && !complete && <p className="proof-error" role="alert">{error}</p>}
        {uiReview ? (
          <div className="proof-review"><strong>No measurement recorded</strong><span>{capabilityGap ? 'UI review · RDS capability not checked live · No result' : 'UI review · No database connections · No result'}</span></div>
        ) : complete ? (
          <div className="remembered" role="status" aria-atomic="true">
            <span>{classified.formalWinner === null ? 'Outcome incomplete' : 'Verified outcome'}</span>
            <strong>{verdictBandOutcome(session)}</strong>
          </div>
        ) : capabilityGap ? (
          <CapabilityNote />
        ) : (
          <div className="fairness"><span aria-hidden="true">◆</span>{fairnessCopy(session.round.id)}<span aria-hidden="true">◆</span></div>
        )}
        {!uiReview && complete && capabilityGap && <CapabilityNote compact />}
        {/* A towel posts the terminal result immediately while auxiliary
            receipt actions remain cleanup-gated. The strip therefore keeps its
            own "Explain" route until cleanup settles; Next is independently
            available from the shared terminal-navigation rule below. */}
        {!uiReview && complete && (
          <RingsideMeanings
            session={session}
            onExplain={cleanupAllowsActions ? undefined : () => setShowRingsideTake(true)}
          />
        )}
        {!uiReview && complete && session.remembered_result && !capabilityGap && <p className="final-fairness">{fairnessCopy(session.round.id)}</p>}
        <TowelControl
          session={session}
          uiReview={uiReview}
          disabled={!liveEvidenceConnected}
          onTowel={onTowel}
        />
        {!uiReview && complete && proofNavigationAllowsExit(session) && (
          <div className="proof-actions">
            {cleanupAllowsActions && ((session.state === 'verified' && genericRedoAllowed) || failedOwnedArtifactRound) && (
              <div className="starred-redo-action">
                <button
                  className="proof-redo"
                  onClick={onRedo}
                  aria-label={roundOneIdleClockLive ? 'Re-do round — back to idle clock already live' : undefined}
                >
                  B · {failedOwnedArtifactRound
                    ? session.round.id === 'recover_deleted_order' ? 'Clear recovery corner' : 'Clear test corner'
                    : roundOneIdleClockLive
                      ? 'BACK TO IDLE · LIVE'
                    : `${session.round.redo?.badge ? `${session.round.redo.badge} · ` : ''}${session.round.redo?.label ?? 'RE-DO ROUND'}`}
                </button>
                {session.state === 'verified' && isRoundFour(session) && session.round.redo?.policy === 'show' && (
                  <small>{ROUND_FOUR_LEGEND}</small>
                )}
              </div>
            )}
            {cleanupAllowsActions && <button className="proof-replay" onClick={() => setShowInstantReplay(true)}>Select · Instant replay</button>}
            {cleanupAllowsActions && <button className="proof-ringside" onClick={() => setShowRingsideTake(true)}>Select · Explain to the room</button>}
            {cleanupAllowsActions && shareable && <button className="proof-cost" onClick={() => setShowCostRoom(true)}>Select · What it cost</button>}
            {cleanupAllowsActions && shareable && <button className="proof-share" onClick={() => setShowShareReceipt(true)}>Start · Share the receipt</button>}
            <button
              type="button"
              className="proof-next"
              title={hasNextRound ? 'Continue to the next round' : 'Return to the fight card'}
              onClick={onContinue}
            >
              A · Next round
            </button>
          </div>
        )}
        {!uiReview && !complete && !session.towel && (
          <RingsideCommentator
            session={session}
            open={commentaryOpen}
            onToggle={onToggleCommentary}
            missedCalls={missedCalls}
          />
        )}
      </footer>
      {showInstantReplay && <InstantReplay session={session} roundNumber={roundNumber} onClose={() => setShowInstantReplay(false)} />}
      {showRingsideTake && <RingsideTake session={session} onClose={() => setShowRingsideTake(false)} />}
      {showCostRoom && <CostRoom session={session} onClose={() => setShowCostRoom(false)} />}
      {showShareReceipt && <ShareReceipt session={session} roundNumber={roundNumber} kind="round" onClose={() => setShowShareReceipt(false)} />}
      <div className="proof-scanlines" aria-hidden="true" />
    </main>
  )
}

function TowelControl({
  session,
  uiReview,
  disabled = false,
  onTowel,
}: {
  session: DemoSession
  uiReview: boolean
  disabled?: boolean
  onTowel: () => Promise<void>
}) {
  const [submitting, setSubmitting] = useState(false)
  if (uiReview) return null
  if (session.towel) return <TowelProgress session={session} onRetry={onTowel} />
  if (session.state !== 'running') return null
  const submit = async () => {
    if (submitting) return
    setSubmitting(true)
    try {
      await onTowel()
    } finally {
      setSubmitting(false)
    }
  }
  return (
    <div className="towel-call">
      <button className="proof-towel" disabled={submitting || disabled} onClick={submit}>
        {disabled ? 'BACKEND OFFLINE · RECONNECTING' : submitting ? 'TOWEL IN...' : 'B · Throw in the Towel'}
      </button>
    </div>
  )
}

function TowelLaneResults({ session }: { session: DemoSession }) {
  if (!session.towel) return null
  return (
    <section className="towel-lane-results" aria-label="Toweled lane results">
      {(['lakebase', 'competitor'] as const).map((laneId) => {
        const lane = session.lanes[laneId]
        const verified = towelVerifiedMs(session, laneId) !== null
        const censored = towelLowerBoundMs(session, laneId) !== null
        return (
          <article key={laneId} data-corner={laneId === 'lakebase' ? 'red' : 'blue'}>
            <span>{lane.name}</span>
            <strong>{towelLaneValue(session, laneId)}</strong>
            <small>{lane.state === 'not_supported' ? 'NOT SUPPORTED · N/A' : verified ? 'EXACT VERIFIED' : censored ? 'UNFINISHED · LOWER BOUND' : 'NO EXACT RESULT'}</small>
          </article>
        )
      })}
    </section>
  )
}

function TowelProgress({ session, onRetry }: { session: DemoSession; onRetry: () => Promise<void> }) {
  const towel = session.towel
  if (!towel) return null
  const failed = towel.state === 'failed'
  const recoveryRound = session.round.id === 'recover_deleted_order'
  const roundFive = isRoundFive(session)
  const title = towel.state === 'stopping'
    ? 'TOWEL IN...'
    : towel.state === 'cleaning'
      ? recoveryRound ? 'Cleaning owned recovery environments' : 'Result posted · Cleanup backstage'
      : towel.state === 'ready'
        ? 'Toweled · Result frozen'
        : CLEANUP_ABANDONED_TITLE
  const detail = failed
    ? towel.cleanup_failure ?? CLEANUP_ABANDONED_FALLBACK
    : roundFive
      ? towel.state === 'stopping'
        ? 'Freezing exact setup stops and censored post-T0 lower bounds · No declared winner · comparison incomplete · margin N/A'
        : towel.state === 'cleaning'
          ? 'Removing only run-owned setup artifacts · Exact setup evidence stays frozen · No declared winner · comparison incomplete · margin N/A'
          : 'Exact setup evidence frozen · No declared winner · comparison incomplete · margin N/A'
    : towel.state === 'stopping'
      ? `Server cutoff ${laneReceiptTime(towelCutoffMs(session))} · Freezing exact evidence`
      : recoveryRound
        ? 'Only run-owned recovery environments are being removed · Cleanup continues backstage under this round lease'
        : 'Verified results stay exact · Unfinished lanes stay lower bounds'
  return (
    <div className="towel-progress" data-state={towel.state} role={failed ? 'alert' : 'status'} aria-live="polite">
      <strong>{title}</strong>
      <span>{detail}</span>
      {towel.restore_started && <em>AWS RESTORE ALREADY IN MOTION · SAFE CLEANUP MAY TAKE MINUTES</em>}
      {failed && <button className="proof-towel-retry" onClick={onRetry}>B · Retry cleanup</button>}
    </div>
  )
}

interface ReplayCall {
  label: string
  code: string
  note?: string
}

// Capability rounds publish their proof facts on the lane evidence map rather
// than as scored metrics. Anything absent stays an em dash instead of becoming
// an invented value.
function laneEvidenceText(lane: LaneSnapshot, key: string): string {
  const evidence = lane.evidence
  const value = evidence && typeof evidence === 'object'
    ? (evidence as Record<string, unknown>)[key]
    : undefined
  return value === null || value === undefined || value === '' ? '—' : String(value)
}

function replayMsText(lane: LaneSnapshot, key: string): string {
  const evidence = lane.evidence
  const raw = evidence && typeof evidence === 'object'
    ? (evidence as Record<string, unknown>)[key]
    : undefined
  const value = typeof raw === 'number' || typeof raw === 'string' ? Number(raw) : Number.NaN
  return Number.isFinite(value) && value >= 0 ? preciseDuration(value) : '—'
}

interface ReplayStep {
  summary: string
  lakebase: ReplayCall[]
  competitor: ReplayCall[]
  shared?: ReplayCall[]
}

function replaySteps(session: DemoSession): ReplayStep[] {
  if (session.round.id === 'wake_idle_app') {
    const competitorStart = session.lanes.competitor.state === 'not_supported'
      ? 'RDS capability checked: no automatic scale-to-zero or connection-triggered wake; no timer invented.'
      : 'Both control planes proved genuine scale zero before the bell.'
    const competitorStartCalls: ReplayCall[] = session.competitor.id === 'aurora_serverless_v2'
      ? [
          { label: 'AWS topology', code: 'RDS.DescribeDBClusters(DBClusterIdentifier=<cluster>)' },
          { label: 'Writer check', code: 'RDS.DescribeDBInstances(DBInstanceIdentifier=<writer>)' },
          { label: 'Primary pause proof', code: 'RDS.DescribeEvents(SourceIdentifier=<cluster>, EventCategories=["serverless"])', note: 'Requires the exact “Successfully paused” writer event.' },
          { label: 'Fallback only', code: 'CloudWatch.GetMetricStatistics(MetricName="ServerlessDatabaseCapacity")', note: 'Used only when no qualifying pause event is available.' },
        ]
      : [
          { label: 'AWS capability check', code: 'RDS.DescribeDBInstances(DBInstanceIdentifier=<instance>)' },
          { label: 'No call by design', code: 'No PostgreSQL connection; no RDS timer started.', note: 'RDS PostgreSQL has no automatic scale-to-zero plus connection-triggered wake path.' },
        ]
    const competitorConnectCalls: ReplayCall[] = session.competitor.id === 'aurora_serverless_v2'
      ? [
          { label: 'Credential lookup', code: 'SecretsManager.GetSecretValue(SecretId=<aurora-secret>)' },
          { label: 'PostgreSQL wire—not a cloud API', code: 'TLS connect → <aurora-writer>:5432' },
        ]
      : [{ label: 'No call by design', code: 'RDS lane is not eligible, so no connection or clock is started.' }]
    const probeTransaction: ReplayCall[] = [
      { label: 'PostgreSQL wire—not a cloud API', code: 'INSERT INTO public.anti_demo_probe (probe_id, expected_value, created_at) VALUES (<nonce>, <value>, clock_timestamp())' },
      { label: 'PostgreSQL wire—not a cloud API', code: 'COMMIT' },
      { label: 'PostgreSQL wire—not a cloud API', code: 'SELECT expected_value FROM public.anti_demo_probe WHERE probe_id = <nonce>', note: 'The returned value must match the unique value generated for this run.' },
    ]
    const competitorProbe = session.competitor.id === 'aurora_serverless_v2'
      ? probeTransaction
      : [{ label: 'No call by design', code: 'RDS lane remains NOT TIMED.' }]
    return [
      {
        summary: competitorStart,
        lakebase: [
          { label: 'Databricks control plane', code: 'databricks postgres get-endpoint <endpoint> -o json', note: 'Requires status.current_state = IDLE and disabled = false.' },
        ],
        competitor: competitorStartCalls,
      },
      {
        summary: 'One client released the same PostgreSQL transaction to both eligible lanes.',
        shared: [
          { label: 'Application API', code: 'POST /api/sessions/<session>/run' },
          { label: 'Live result stream', code: 'GET /api/sessions/<session>/events?after=<sequence>  (SSE)' },
          { label: 'Start barrier', code: 'time.monotonic_ns() → release both lane tasks', note: 'One in-process clock records the gap between lane start times.' },
        ],
        lakebase: [
          { label: 'Credential generation', code: 'databricks postgres generate-database-credential <endpoint> -o json' },
          { label: 'PostgreSQL wire—not a cloud API', code: 'TLS connect → <lakebase-host>:5432' },
        ],
        competitor: competitorConnectCalls,
      },
      {
        summary: 'Each lane committed a unique nonce, then read the same nonce back.',
        shared: [{ label: 'Same payload', code: 'probe_id=<same nonce> · expected_value=<same value>' }],
        lakebase: probeTransaction,
        competitor: competitorProbe,
      },
      {
        summary: 'Each clock stopped only when its own application transaction verified.',
        shared: [
          { label: 'Verified lane event', code: 'event: lane_update · state=verified · elapsed_ms=<monotonic elapsed>' },
          { label: 'Final receipt event', code: 'event: run_finished · state=verified' },
        ],
        lakebase: [{ label: 'Stop rule', code: 'Freeze Lakebase clock only after matching SELECT read-back.' }],
        competitor: session.competitor.id === 'aurora_serverless_v2'
          ? [{ label: 'Stop rule', code: 'Freeze Aurora clock only after matching SELECT read-back.' }]
          : [{ label: 'Stop rule', code: 'No RDS clock exists for this capability gap.' }],
      },
    ]
  }
  if (session.round.id === 'make_schema_change_safely') {
    const competitorCopy = session.competitor.id === 'aurora_serverless_v2'
      ? 'Aurora used a point-in-time copy-on-write clone and a new writer.'
      : 'RDS used a point-in-time restored instance.'
    const sourceChecks: ReplayCall[] = [
      { label: 'PostgreSQL wire—not a cloud API', code: 'SELECT customer_email, total_cents, status FROM public.orders WHERE order_id = <baseline-order>' },
      { label: 'PostgreSQL wire—not a cloud API', code: 'SELECT data_type, is_nullable FROM information_schema.columns WHERE table_name = \'orders\' AND column_name = \'delivery_instructions\'' },
    ]
    const competitorCreate: ReplayCall[] = session.competitor.id === 'aurora_serverless_v2'
      ? [
          { label: 'AWS control plane', code: 'RDS.RestoreDBClusterToPointInTime(RestoreType="copy-on-write", UseLatestRestorableTime=true)' },
          { label: 'AWS control plane', code: 'RDS.CreateDBInstance(DBInstanceClass="db.serverless", Engine="aurora-postgresql")' },
          { label: 'Readiness polling', code: 'RDS.DescribeDBClusters(<clone>) + RDS.DescribeDBInstances(<writer>)' },
        ]
      : [
          { label: 'AWS control plane', code: 'RDS.RestoreDBInstanceToPointInTime(UseLatestRestorableTime=true)' },
          { label: 'Readiness polling', code: 'RDS.DescribeDBInstances(DBInstanceIdentifier=<restored-instance>)' },
        ]
    const migration: ReplayCall[] = [
      { label: 'PostgreSQL wire—not a cloud API', code: 'ALTER TABLE public.orders ADD COLUMN delivery_instructions TEXT' },
      { label: 'PostgreSQL wire—not a cloud API', code: 'COMMIT' },
      { label: 'Schema verification', code: 'SELECT data_type, is_nullable FROM information_schema.columns WHERE column_name = \'delivery_instructions\'' },
    ]
    const appVerification: ReplayCall[] = [
      { label: 'PostgreSQL wire—not a cloud API', code: 'INSERT INTO public.orders (..., delivery_instructions) VALUES (<nonce>, ..., \'Leave at the front desk\')' },
      { label: 'PostgreSQL wire—not a cloud API', code: 'COMMIT' },
      { label: 'Application read-back', code: 'SELECT customer_email, total_cents, status, delivery_instructions FROM public.orders WHERE order_id = <nonce>' },
    ]
    const sourceIsolation: ReplayCall[] = [
      ...sourceChecks,
      { label: 'Leak check', code: 'SELECT count(*) FROM public.orders WHERE order_id = <nonce>', note: 'Must return 0 in the source.' },
    ]
    return [
      {
        summary: 'Both sources were checked against the same clean schema and baseline row.',
        shared: [{ label: 'Application API', code: 'POST /api/sessions/<session>/run' }],
        lakebase: sourceChecks,
        competitor: sourceChecks,
      },
      {
        summary: `Lakebase used a native branch. ${competitorCopy}`,
        lakebase: [
          { label: 'Databricks control plane', code: 'databricks postgres create-branch <project> <branch> --json <source-branch-spec>' },
          { label: 'Databricks control plane', code: 'databricks postgres create-endpoint <branch> <endpoint> --json <autoscaling-spec>' },
          { label: 'Readiness polling', code: 'databricks postgres get-endpoint <endpoint> -o json' },
        ],
        competitor: competitorCreate,
      },
      {
        summary: 'The same nullable ALTER TABLE ran only inside each isolated copy.',
        lakebase: migration,
        competitor: migration,
      },
      {
        summary: 'The same application write committed and read back in each copy.',
        shared: [{ label: 'Same payload', code: 'order_id=<same nonce> · delivery_instructions=<same value>' }],
        lakebase: appVerification,
        competitor: appVerification,
      },
      {
        summary: 'Each source was checked again to prove the schema and nonce never leaked.',
        shared: [{ label: 'Final result stream', code: 'event: lane_update → event: run_finished  (SSE)' }],
        lakebase: sourceIsolation,
        competitor: sourceIsolation,
      },
    ]
  }
  if (session.round.id === 'recover_deleted_order') {
    const competitorRestore: ReplayCall[] = session.competitor.id === 'aurora_serverless_v2'
      ? [
          { label: 'Recovery request', code: 'RDS.RestoreDBClusterToPointInTime(RestoreToTime=<recovery_at>)  // full-copy default' },
          { label: 'Writer request', code: 'RDS.CreateDBInstance(DBClusterIdentifier=<recovery-cluster>, DBInstanceClass="db.serverless")' },
          { label: 'Recovery readiness', code: 'RDS.DescribeDBClusters + RDS.DescribeDBInstances' },
        ]
      : [
          { label: 'Recovery request', code: 'RDS.RestoreDBInstanceToPointInTime(RestoreToTime=<recovery_at>)' },
          { label: 'Recovery readiness', code: 'RDS.DescribeDBInstances' },
        ]
    const exactRead: ReplayCall[] = [
      { label: 'Recovered TLS session', code: 'PostgreSQL TLS connect → <recovery-endpoint>:5432' },
      { label: 'Recovered order', code: 'SELECT customer_email, total_cents, status, created_at FROM public.orders WHERE order_id=<owned-order>' },
      { label: 'Fresh source session', code: 'PostgreSQL TLS reconnect → SELECT customer_email, total_cents, status, created_at FROM public.orders WHERE order_id=<owned-order>', note: 'Must return absent after the recovered row matches.' },
    ]
    return [
      {
        summary: 'Interactive arm committed and aged the same exact incident row, then confirmed that no recovery artifact existed.',
        shared: [
          { label: 'Exact incident', code: 'order_id=<run-owned UUID> · identical payload · identical created_at' },
          { label: 'Database clock', code: 'INSERT exact row → COMMIT → wait through floor(initial_clock)+2s' },
          { label: 'Honest precondition', code: 'Recovery artifact absent · eligibility not pre-waited' },
        ],
        lakebase: [],
        competitor: [],
      },
      {
        summary: 'The bell released one deletion barrier. Each timer includes exact deletion, recovery eligibility, restore readiness, TLS, and verified reads.',
        shared: [
          { label: 'Start barrier', code: 'time.monotonic_ns() → publish run_started → release both DELETE tasks' },
          { label: 'Timed boundary', code: 'DELETE exact owned row + clock_timestamp() → recovery_at=floor(observed_at)-1s → COMMIT' },
        ],
        lakebase: [
          { label: 'Recovery eligibility', code: 'Wait for the captured source recovery point inside the lane timer' },
          { label: 'Recovery branch', code: 'databricks postgres create-branch <project> <recovery-branch> --json {spec.source_branch_time:<recovery_at>}' },
          { label: 'Recovery endpoint', code: 'databricks postgres create-endpoint <recovery-branch> <endpoint>' },
        ],
        competitor: [
          { label: 'Recovery eligibility', code: 'RDS.DescribeDBClusters / RDS.DescribeDBInstances until recovery_at is restorable' },
          ...competitorRestore,
        ],
      },
      {
        summary: 'Each clock stopped only after the recovered exact row matched and a fresh source session still proved it absent.',
        shared: [{ label: 'Stop rule', code: 'Exact recovered SELECT matches + source SELECT returns absent' }],
        lakebase: exactRead,
        competitor: exactRead,
      },
    ]
  }
  if (session.round.id === 'survive_connection_spike') {
    const lakebaseBurst = roundFiveLaneResult(session.lanes.lakebase)
    const competitorBurst = roundFiveLaneResult(session.lanes.competitor)
    const burstContract: ReplayCall[] = [
      { label: 'Scheduled clients', code: `${ROUND_FIVE_SCHEDULED_CLIENTS} fresh application connections · max ${ROUND_FIVE_CONCURRENCY} concurrent`, note: `After ${ROUND_FIVE_WARMUPS} untimed warmups that are excluded from every number.` },
      { label: 'Per-client transaction', code: 'TLS connect → INSERT nonce → COMMIT → SELECT nonce read-back → close' },
    ]
    return [
      {
        summary: 'Both setup workflows were released from one shared monotonic T0 under a frozen contract, so the scored clock is the readiness work itself.',
        shared: [
          { label: 'Application API', code: 'POST /api/sessions/<session>/run' },
          { label: 'Setup start barrier', code: 'time.monotonic_ns() → release both setup workflows', note: `Both launches must occur within ${ROUND_FIVE_SETUP_MAX_LAUNCH_SKEW_MS} ms of the shared T0 or the setup race is void.` },
          { label: 'Live result stream', code: 'GET /api/sessions/<session>/events?after=<sequence>  (SSE)' },
          { label: 'Neutral runner', code: ROUND_FIVE_RUNNER, note: 'One client library and one TLS mode drive both lanes.' },
        ],
        lakebase: [{ label: 'Nothing to provision', code: 'Connection pooling is built into the Lakebase endpoint.', note: 'No per-bout pooling resource is created, so no creation is journaled.' }],
        competitor: [{ label: 'Journaled per-bout build', code: 'Every RDS Proxy resource is written to the creation journal before AWS is called.', note: 'The journal is what lets an interrupted bout be cleaned up instead of leaking.' }],
      },
      {
        summary: 'Lakebase validated the pool it already had. RDS PostgreSQL needed nine journaled resources built and frozen before one pooled connection could be attempted.',
        lakebase: [
          { label: 'validating_host', code: 'Check the built-in Lakebase pooled endpoint' },
          { label: 'verifying_transaction', code: 'Fresh pooled connection → full application transaction' },
        ],
        competitor: [
          { label: 'creating_proxy_network', code: 'journal: proxy_security_group' },
          { label: 'freezing_proxy_egress', code: 'journal: proxy_default_egress' },
          { label: 'authorizing_proxy_ingress', code: 'journal: proxy_ingress' },
          { label: 'authorizing_proxy_egress', code: 'journal: proxy_egress' },
          { label: 'authorizing_runner_egress', code: 'journal: runner_egress' },
          { label: 'authorizing_rds_ingress', code: 'journal: rds_ingress' },
          { label: 'creating_proxy', code: 'journal: rds_proxy', note: 'AWS creates a brand-new RDS Proxy for this bout only.' },
          { label: 'freezing_proxy_settings', code: 'journal: proxy_target_group' },
          { label: 'registering_proxy_target', code: 'journal: proxy_target' },
          { label: 'waiting_for_proxy_target', code: 'Poll until the Proxy and its database target both report AVAILABLE' },
          { label: 'verifying_topology', code: 'Confirm the exact Proxy and database bindings' },
          { label: 'verifying_transaction', code: 'Fresh pooled connection through the Proxy → full application transaction' },
        ],
      },
      {
        summary: 'Each setup clock stopped at its own exact stop gate. Neither lane could stop early, and every expected fact had to match what was observed.',
        shared: [{ label: 'Stop rule', code: 'event: lane_update · activity.phase=setup_stop · setup_elapsed_ms=<exact scored elapsed>' }],
        lakebase: [
          { label: 'Gate lakebase_fresh_pooled_transaction', code: 'fresh_pooled_path_verified=true · runner_verify_full_transaction=true' },
          { label: 'Scored clock boundary', code: 'Shared T0 → exact Lakebase setup stop gate', note: roundFiveSetupStopEvidenceNote(session, 'lakebase') },
        ],
        competitor: [
          { label: 'Gate rds_proxy_topology_transaction', code: 'sealed_proxy_auth_verified=true · proxy_target_state=AVAILABLE · max_connections_percent=90 · connection_borrow_timeout_seconds=120 · runner_verify_full_transaction=true' },
          { label: 'Scored clock boundary', code: 'Shared T0 → exact RDS Proxy setup stop gate', note: roundFiveSetupStopEvidenceNote(session, 'competitor') },
        ],
      },
      {
        summary: `The identical ${ROUND_FIVE_SCHEDULED_CLIENTS}-client spike then ran against both ready lanes as a pass/fail check. It validates the setup score; it is not a second speed comparison.`,
        shared: [
          ...burstContract,
          { label: 'Witness check', code: `${ROUND_FIVE_WITNESS_CLIENTS} witnessed clients must prove pooling reused backends`, note: 'Unique backend PIDs and peak backend sessions must both stay below the witnessed client count.' },
          { label: 'Cleanup gate', code: 'Delete every journaled per-bout resource, then confirm the journal has no unresolved entry' },
        ],
        lakebase: [
          { label: 'Clients', code: `${roundFiveCountDisplay(lakebaseBurst.successes)} succeeded · ${roundFiveCountDisplay(lakebaseBurst.errors)} errored of ${roundFiveCountDisplay(lakebaseBurst.scheduled)} scheduled` },
          { label: 'Application p99 (nearest rank)', code: roundFiveP99Display(lakebaseBurst), note: 'Secondary evidence only. The primary score is setup elapsed.' },
          { label: 'Pooling witness', code: `${roundFiveCountDisplay(lakebaseBurst.uniqueBackendPids)} unique backend PIDs · peak ${roundFiveCountDisplay(lakebaseBurst.peakBackendSessions)} backend sessions` },
        ],
        competitor: [
          { label: 'Clients', code: `${roundFiveCountDisplay(competitorBurst.successes)} succeeded · ${roundFiveCountDisplay(competitorBurst.errors)} errored of ${roundFiveCountDisplay(competitorBurst.scheduled)} scheduled` },
          { label: 'Application p99 (nearest rank)', code: roundFiveP99Display(competitorBurst), note: 'Secondary evidence only. The primary score is setup elapsed.' },
          { label: 'Pooling witness', code: `${roundFiveCountDisplay(competitorBurst.uniqueBackendPids)} unique backend PIDs · peak ${roundFiveCountDisplay(competitorBurst.peakBackendSessions)} backend sessions` },
        ],
      },
    ]
  }
  if (session.round.id === 'put_model_score_in_app') {
    const evidence = modelScoreEvidence(session.lanes.lakebase)
    const untimedOpponent: ReplayCall[] = [
      {
        label: 'No call by design',
        code: 'No AWS lane was built, connected, or timed.',
        note: roundFourUnsupportedReason(session.lanes.competitor),
      },
    ]
    return [
      {
        summary: 'Before the bell the Managed Sync pipeline was inspected and the baseline row was restored, so the clock could only measure this run’s change.',
        shared: [
          { label: 'Application API', code: 'POST /api/sessions/<session>/run' },
          { label: 'Live result stream', code: 'GET /api/sessions/<session>/events?after=<sequence>  (SSE)' },
        ],
        lakebase: [
          { label: 'preflight', code: 'Inspect the Managed Sync pipeline status', note: 'The pipeline must be RUNNING, its identity must match the sealed contract, and its status must be fresher than the staleness bound.' },
          { label: 'armed', code: 'Managed Sync baseline verified', note: 'The exact baseline row must be readable in the application before the round is armed.' },
        ],
        competitor: untimedOpponent,
      },
      {
        summary: 'One exact row was committed to the source Delta table. That commit is where the measured clock starts.',
        lakebase: [
          { label: 'committing_source', code: 'Commit one exact row to the source Delta table' },
          { label: 'Gate · version advanced', code: `delta commit version = ${evidence.deltaVersion}`, note: 'The commit must land on a strictly higher Delta version than the one observed before the bell.' },
          { label: 'Gate · committed source read-back', code: 'Read the source row and require an exact match with the committed update', note: 'Primary key, score, model version, and proof nonce must all match.' },
        ],
        competitor: untimedOpponent,
      },
      {
        summary: 'Managed Sync was then polled until it reported exactly that Delta version — not an earlier one and not a later one.',
        shared: [
          { label: 'Stop rule', code: 'last_sync_delta_version == committed version AND last_processed_version == committed version' },
        ],
        lakebase: [
          { label: 'waiting_sync', code: `Poll the Managed Sync status until it reports Delta version ${evidence.deltaVersion}`, note: 'Up to 20 polls at 0.25 s. Overshooting the requested version is rejected rather than accepted as success.' },
          { label: 'Gate · exact commit timestamp', code: 'status.last_sync_delta_commit_time == the exact Delta commit timestamp', note: 'A status describing any other commit is refused, so a stale or unrelated sync cannot be counted.' },
          { label: 'Reverse ETL sync (primary)', code: metricDisplay(session, 'managed_availability_ms'), note: 'Measured as sync end time minus the authoritative Delta commit time.' },
        ],
        competitor: untimedOpponent,
      },
      {
        summary: 'Only a fresh application Postgres connection returning the exact row stopped the clock.',
        shared: [
          { label: 'Stop rule', code: 'Fresh application read returns the exact primary key, score, model version, and proof nonce' },
        ],
        lakebase: [
          { label: 'reading_application', code: 'Fresh application Postgres connection → read the exact primary key' },
          { label: 'Verified row', code: `${evidence.primaryKey} · score ${evidence.score} · model ${evidence.modelVersion}`, note: `Proof nonce ${evidence.proofNonce} · ${evidence.exactRowVerified ? 'Exact row verified: every field matched the committed update.' : 'The exact row was not verified for this attempt.'}` },
          { label: 'End-to-end clock boundary', code: 'Delta commit → successful fresh application read', note: 'The measured value is shown once in the Takeaway story.' },
        ],
        competitor: untimedOpponent,
      },
    ]
  }
  if (session.round.id === 'analyze_live_orders_without_slowing_checkout') {
    const lane = session.lanes.lakebase
    const untimedOpponent: ReplayCall[] = [
      {
        label: 'No call by design',
        code: 'No AWS CDC pipeline was built, connected, or timed.',
        note: roundFourUnsupportedReason(session.lanes.competitor),
      },
    ]
    return [
      {
        summary: 'Before the bell native CDF had to be streaming and the exact baseline order had to already be visible in Delta history.',
        shared: [
          { label: 'Application API', code: 'POST /api/sessions/<session>/run' },
          { label: 'Live result stream', code: 'GET /api/sessions/<session>/events?after=<sequence>  (SSE)' },
        ],
        lakebase: [
          { label: 'preflight', code: 'Inspect the native CDF status for the checkout table' },
          { label: 'Gate · streaming feed', code: 'state == CDF_STATE_STREAMING with a non-empty committed LSN', note: 'The reported source table and destination Delta table must also match the sealed contract.' },
          { label: 'armed', code: 'The exact baseline order must already be present as one insert in Delta history', note: 'Arming is single-use, so one armed proof cannot be replayed into a second scored result.' },
        ],
        competitor: untimedOpponent,
      },
      {
        summary: 'One checkout order was committed to the live application Postgres table. The measured clock starts when that commit completes.',
        lakebase: [
          { label: 'checkout', code: 'Commit one checkout order to the application Postgres table' },
          { label: 'Committed order', code: `${laneEvidenceText(lane, 'sku')} · ${laneEvidenceText(lane, 'store')} · ${laneEvidenceText(lane, 'total_display')} · ${laneEvidenceText(lane, 'status')}` },
          { label: 'Checkout commit', code: replayMsText(lane, 'checkout_commit_ms'), note: `Order ${laneEvidenceText(lane, 'order_id')} · proof nonce ${laneEvidenceText(lane, 'proof_nonce')}. The order must differ from the baseline.` },
        ],
        competitor: untimedOpponent,
      },
      {
        summary: 'Delta history was then polled until that exact order appeared as one insert. Nothing weaker stopped the clock.',
        shared: [
          { label: 'Stop rule', code: 'Delta history returns exactly one insert row for the exact committed order' },
        ],
        lakebase: [
          { label: 'waiting_cdf', code: 'Poll Delta history for the exact order', note: 'Up to 60 polls at 1.0 s. Duplicate proof rows are rejected rather than counted as a match.' },
          { label: 'Gate · exact single insert', code: `matching live orders = ${metricDisplay(session, 'matching_live_orders')}`, note: `Delta history LSN ${laneEvidenceText(lane, 'history_lsn')}.` },
          { label: 'Analytics clock boundary', code: 'Completed checkout commit → exact Delta answer read', note: 'The measured value is shown once in the Takeaway story.' },
        ],
        competitor: untimedOpponent,
      },
      {
        summary: 'A separate checkout order then had to commit and read back exactly. That is the evidence behind the “without slowing checkout” claim.',
        shared: [
          { label: 'Guardrail rule', code: 'A second, distinct checkout order must commit and return its own exact row' },
        ],
        lakebase: [
          { label: 'checkout · guardrail', code: replayMsText(lane, 'checkout_guardrail_commit_ms'), note: `Order ${laneEvidenceText(lane, 'checkout_guardrail_order_id')} · proof nonce ${laneEvidenceText(lane, 'checkout_guardrail_proof_nonce')}. It must differ from the measured order in both order id and proof nonce.` },
          { label: 'reading_checkout · guardrail', code: replayMsText(lane, 'checkout_guardrail_read_ms'), note: 'The separate order must be returned exactly, so analytics reads cannot be shown to have blocked checkout.' },
          { label: 'Separate checkout committed', code: metricDisplay(session, 'checkout_verified') },
        ],
        competitor: untimedOpponent,
      },
    ]
  }
  return [
    {
      summary: `Both lanes followed the same ${session.round.capability.toLowerCase()} contract.`,
      lakebase: [{ label: 'Adapter status', code: 'Provider-specific calls will appear when this round adapter is executable.' }],
      competitor: [{ label: 'Adapter status', code: 'Provider-specific calls will appear when this round adapter is executable.' }],
    },
    {
      summary: 'One verifier recorded each lane independently.',
      shared: [{ label: 'Live result stream', code: 'GET /api/sessions/<session>/events?after=<sequence>  (SSE)' }],
      lakebase: [],
      competitor: [],
    },
    {
      summary: 'No result was inferred beyond the verified outcome shown here.',
      lakebase: [{ label: 'Receipt', code: 'Verified application outcome only.' }],
      competitor: [{ label: 'Receipt', code: 'Verified application outcome only.' }],
    },
  ]
}

function ReplayCallGroup({ title, corner, calls }: { title: string; corner: 'red' | 'blue' | 'shared'; calls: ReplayCall[] }) {
  if (calls.length === 0) return null
  return (
    <article className="replay-call-group" data-corner={corner}>
      <h3>{title}</h3>
      {calls.map((call, index) => (
        <div key={`${call.label}-${index}`}>
          <span>{call.label}</span>
          <code>{call.code}</code>
          {call.note && <p>{call.note}</p>}
        </div>
      ))}
    </article>
  )
}

export function InstantReplay({ session, roundNumber, onClose }: { session: DemoSession; roundNumber: number; onClose: () => void }) {
  const skew = receiptStartSkewDisplay(session)
  const steps = replaySteps(session)
  const story = replayStory(session)
  // A capability round never releases a second lane, so there is no gap between
  // lane start times to report. Naming the untimed opponent is honest; printing
  // a start gap for a race that did not happen is not.
  const untimedOpponent = session.lanes.competitor.state === 'not_supported'
  const dialogRef = useAccessibleDialog<HTMLElement>(true, onClose)
  return (
    <div className="replay-overlay" role="presentation" onClick={onClose}>
      <section ref={dialogRef} className="replay-modal" role="dialog" aria-modal="true" aria-labelledby="replay-heading" onClick={(event) => event.stopPropagation()}>
        <header>
          <div>
            <p>Instant replay · Round {roundNumber}</p>
            <h2 id="replay-heading">{session.round.title}</h2>
          </div>
          <span data-state={story.state}>{story.status}</span>
        </header>
        <section className="replay-story" aria-label="Three-beat replay story">
          {story.beats.map((beat, index) => (
            <article key={beat.id} className="replay-beat" data-beat={beat.id}>
              <header>
                <span>{String(index + 1).padStart(2, '0')}</span>
                <h3>{beat.title}</h3>
              </header>
              <p>{beat.body}</p>
              {story.metricBeat === beat.id && story.metrics.length > 0 && (
                <div className="replay-primary-metric" aria-label="Primary measured result">
                  {story.metrics.map((metric) => (
                    <div key={metric.laneId} data-corner={metric.laneId === 'lakebase' ? 'red' : 'blue'}>
                      <span>{metric.label}</span>
                      <strong data-width={metric.value.length >= 7 ? 'long' : 'standard'}>{metric.value}</strong>
                      <small>{metric.note}</small>
                    </div>
                  ))}
                </div>
              )}
            </article>
          ))}
        </section>
        <details className="replay-evidence">
          <summary data-dialog-initial-focus>
            <span>View full evidence</span>
            <small>Calls, gates, matrices, timestamps, and configuration</small>
          </summary>
          <div className="replay-evidence-body">
            <section className="replay-evidence-lanes" aria-label="Lane evidence status">
              {(['lakebase', 'competitor'] as LaneId[]).map((laneId) => {
                const lane = isRoundFive(session)
                  ? roundFiveArenaLane(session, laneId)
                  : session.lanes[laneId]
                return (
                  <article key={laneId} data-corner={laneId === 'lakebase' ? 'red' : 'blue'} data-state={lane.state}>
                    <strong>{lane.name}</strong>
                    <span>{lane.status}</span>
                    <small>{lane.state === 'not_supported' ? 'Not built or timed' : lane.state.replace(/_/g, ' ')}</small>
                    {lane.error && <p>{lane.error}</p>}
                  </article>
                )
              })}
            </section>
            <section className="replay-proof-sequence" aria-label="Exact proof sequence">
              <header>
                <strong>{story.state === 'verified' ? 'Exact proof sequence' : 'Required proof sequence · Incomplete'}</strong>
                <span>Placeholders shown · No secrets</span>
              </header>
              {steps.map((step, index) => {
                const stepNumber = String(index + 1).padStart(2, '0')
                return (
                  <article key={step.summary} className="replay-evidence-step">
                    <h3>
                      <span>{stepNumber}</span>
                      {story.state === 'verified'
                        ? step.summary
                        : `Required proof step ${stepNumber} · not all calls completed`}
                    </h3>
                    <div>
                      {step.shared && <ReplayCallGroup title="Shared verifier" corner="shared" calls={step.shared} />}
                      <ReplayCallGroup title="Lakebase" corner="red" calls={step.lakebase} />
                      <ReplayCallGroup title={session.competitor.short_name} corner="blue" calls={step.competitor} />
                    </div>
                  </article>
                )
              })}
            </section>
            {isRoundFive(session) && (
              <RoundFiveEvidenceDetails session={session} />
            )}
            <CapacityDisclosure session={session} embedded />
            <dl className="replay-facts">
              <div><dt>Fair start</dt><dd>{fairnessCopy(session.round.id)}</dd></div>
              {untimedOpponent
                ? (
                  <div><dt>Opponent lane</dt><dd>Not timed<small>{roundFourUnsupportedReason(session.lanes.competitor)}</small></dd></div>
                )
                : (
                  <div><dt>Start gap</dt><dd>{skew}<small>Difference between lane start times; lower is fairer.</small></dd></div>
                )}
              <div><dt>Receipt</dt><dd>{receiptId(session)} · {measuredAt(session)}</dd></div>
            </dl>
          </div>
        </details>
        <button className="replay-close" onClick={onClose}>B · Back to the ring</button>
      </section>
    </div>
  )
}

/**
 * Who is at ringside, as a scoreboard strip rather than a briefing note.
 *
 * The talk track belongs in <RingsideTake>; this strip only identifies who is
 * in the room and which priorities they selected. Keeping the receipt out of
 * the footer lets a presenter reveal it in the order they will actually speak:
 * role, takeaway, stakes, question, proof, then boundary.
 *
 * `onExplain` is the way into that overlay, and it is only passed when the
 * screen's own actions row is suppressed -- see the call site. Without it there
 * is a real window with no route to the detail.
 */
function RingsideMeanings({ session, onExplain }: { session: DemoSession; onExplain?: () => void }) {
  const personas = [session.primary_persona, ...session.secondary_personas]
  const priorities = priorityLabel(session.corners)
  return (
    <section className="ringside-meanings" data-count={personas.length} aria-label="What the result means at ringside">
      <header><span>Why this matters at ringside</span><small>Selected · {priorities}</small></header>
      {personas.map((persona) => (
        <article key={persona.id}>
          <img src={personaPortraits[persona.id] ?? persona.portrait} alt="" />
          <strong>{persona.nickname} · {persona.role}</strong>
        </article>
      ))}
      {onExplain && (
        <button type="button" className="ringside-meanings-explain" onClick={onExplain}>
          Select · Explain to the room
        </button>
      )}
    </section>
  )
}

function RingsideTake({ session, onClose }: { session: DemoSession; onClose: () => void }) {
  const personas = [session.primary_persona, ...session.secondary_personas]
  const [selectedId, setSelectedId] = useState<PersonaId>(session.primary_persona.id)
  const selected = personas.find((persona) => persona.id === selectedId) ?? personas[0]
  const tabRefs = useRef<Array<HTMLButtonElement | null>>([])
  const surfaceRef = useAccessibleDialog<HTMLElement>(true, onClose)
  const cue = buildRingsideCue(session, selected.id, priorityKeyFor(session.corners))
  const selectedTabId = `ringside-persona-${selected.id}-tab`

  const selectTabFromKeyboard = (event: React.KeyboardEvent<HTMLButtonElement>, index: number) => {
    let nextIndex: number | null = null
    if (event.key === 'ArrowRight' || event.key === 'ArrowDown') nextIndex = (index + 1) % personas.length
    if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') nextIndex = (index - 1 + personas.length) % personas.length
    if (event.key === 'Home') nextIndex = 0
    if (event.key === 'End') nextIndex = personas.length - 1
    if (nextIndex === null) return
    event.preventDefault()
    setSelectedId(personas[nextIndex].id)
    tabRefs.current[nextIndex]?.focus()
  }
  return (
    <div className="ringside-overlay" role="presentation" onClick={onClose}>
      <section
        ref={surfaceRef}
        className="ringside-take"
        role="dialog"
        aria-modal="true"
        aria-labelledby="ringside-take-heading"
        onClick={(event) => event.stopPropagation()}
      >
        <header>
          <div className="ringside-persona-identity">
            <img src={personaPortraits[selected.id] ?? selected.portrait} alt="" />
            <div>
              <h2 id="ringside-take-heading">For the {selected.role}</h2>
              <small>AKA {selected.nickname}</small>
            </div>
          </div>
          <div className="ringside-priority-chips" aria-label="Room priorities">
            {session.corners.map((corner) => <span key={corner}>{corner}</span>)}
          </div>
        </header>
        <nav role="tablist" aria-label="People in the room">
          {personas.map((persona, index) => (
            <button
              ref={(node) => { tabRefs.current[index] = node }}
              id={`ringside-persona-${persona.id}-tab`}
              role="tab"
              type="button"
              aria-selected={selected.id === persona.id}
              aria-controls="ringside-persona-panel"
              tabIndex={selected.id === persona.id ? 0 : -1}
              key={persona.id}
              onClick={() => setSelectedId(persona.id)}
              onKeyDown={(event) => selectTabFromKeyboard(event, index)}
            >
              {persona.role}
            </button>
          ))}
        </nav>
        <div
          id="ringside-persona-panel"
          className="ringside-persona-panel"
          role="tabpanel"
          aria-labelledby={selectedTabId}
        >
          <section className="ringside-cue ringside-say">
            <h3>What this means</h3>
            <p>{cue.say}</p>
          </section>
          <section className="ringside-cue ringside-ask">
            <h3>Question for the room</h3>
            <p>{cue.ask}</p>
          </section>
          <section className="ringside-cue ringside-show">
            <h3>What we proved</h3>
            <p>{cue.show}</p>
          </section>
        </div>
        <button type="button" className="ringside-close" onClick={onClose}>B · Back to the ring</button>
      </section>
    </div>
  )
}

/** One lane of the standing-cost payload, without importing its name. */
type StandingLane = NonNullable<DemoSession['standing_cost']>['lanes'][number]

/**
 * What kind of number a lane is, which is not the same as how big it is.
 *
 * `sealed_shape_only` is the one that matters most here: it is the RDS and
 * Aurora figures, and it is modelled from the sealed instance shape rather than
 * read off an invoice. It has to say so on the same element as the figure.
 */
const COST_ROOM_EVIDENCE: Record<StandingLane['evidence'], string> = {
  rate_card_derived: 'Modelled · rate-card derived',
  posted_actual: 'Measured · posted actual',
  posted_projection: 'Measured · posted projection',
  sealed_shape_only: 'Modelled · sealed shape only',
  unpriced: 'Unpriced',
}

/**
 * The cost view behind "What it cost", composed rather than authored.
 *
 * Every panel below the strip has one evidence destination here. Explain to the
 * Room deliberately renders none of them: a question about money has a door
 * with money written on it, while the presenter cue stays SAY / ASK / SHOW.
 * Nothing here re-derives a figure.
 *
 * The idle strip is the only new rendering and it invents nothing. Each lane's
 * product, `idle_label`, figure and `evidence` come straight off
 * `session.standing_cost`, which is also what keeps a dollar literal out of
 * this source. It exists because the contrast is the whole finding: RDS
 * PostgreSQL has no idle floor to descend into, so it bills through rounds it
 * is not competing in at all, while Lakebase and Aurora descend to their 60s
 * and 300s floors. That is a fact the payload already carries in `idle_label`,
 * not a claim this component makes on its own.
 *
 * A lane whose figure is `unavailable` prints the word, never a number, and
 * never a zero -- the same rule the panels below it follow.
 *
 * The standing-cost panel is pinned to exactly one render site here, so its
 * six-lane table cannot drift across rounds. The strip and disclosure read the
 * same payload and share `NAMED_ENGINE_LANES`.
 */
export function CostRoom({ session, onClose }: { session: DemoSession; onClose: () => void }) {
  const lanes = session.standing_cost?.lanes ?? []
  const engines = NAMED_ENGINE_LANES
    .map((id) => lanes.find((lane) => lane.lane_id === id))
    .filter((lane): lane is StandingLane => lane !== undefined)
  const dialogRef = useAccessibleDialog<HTMLElement>(true, onClose)
  return (
    <div className="cost-room-overlay" role="presentation" onClick={onClose}>
      <section
        ref={dialogRef}
        className="cost-room"
        role="dialog"
        aria-modal="true"
        aria-labelledby="cost-room-heading"
        onClick={(event) => event.stopPropagation()}
      >
        <header>
          <div>
            <p>What it cost</p>
            <h2 id="cost-room-heading">The bill does not stop with the bell.</h2>
          </div>
          <span>{session.competitor.short_name}</span>
        </header>
        {engines.length > 0 ? (
          <div className="cost-room-idle" aria-label="What each engine bills while no bout is running">
            {engines.map((lane) => (
              <article key={lane.lane_id} data-lane={lane.lane_id} data-evidence={lane.evidence}>
                <strong>{lane.product}</strong>
                <span>{lane.idle_label}</span>
                <b>{lane.figure.state === 'unavailable' ? 'Unavailable' : lane.figure.display}</b>
                <small>{COST_ROOM_EVIDENCE[lane.evidence]}</small>
              </article>
            ))}
          </div>
        ) : (
          <p className="cost-room-unavailable">
            Unavailable · this session carries no standing-cost payload
          </p>
        )}
        <DescentCostDisclosure session={session} />
        <BoutCostDisclosure session={session} />
        <StandingCostDisclosure session={session} />
        <CostReceiptDisclosure session={session} />
        <button className="cost-room-close" onClick={onClose}>B · Back to the ring</button>
      </section>
    </div>
  )
}

function ReceiptPoster({
  session,
  roundNumber,
  kind,
}: {
  session: DemoSession
  roundNumber: number
  kind: ReceiptKind
}) {
  const receipt = receiptPresentation(session, kind)
  const resultWidth = receipt.verdict.length > 52 ? 'long' : receipt.verdict.length > 36 ? 'medium' : 'short'
  const skew = receiptStartSkewDisplay(session)
  const competitorFighter = session.competitor.id === 'aurora_serverless_v2' ? 'AUR' : 'RDS'
  return (
    <article className="receipt-poster" aria-label={kind === 'idle' ? 'Verified back to idle poster preview' : 'Verified result poster preview'}>
      <header className="receipt-ticket-top">
        <div className="receipt-ticket-brand" aria-label="Lakebase: The Anti-Demo">
          <div><strong>Lakebase</strong><span>The Anti-Demo</span></div>
        </div>
        <div className="receipt-verified-stamp"><strong>{receipt.verifiedStamp}</strong><span>{receipt.receiptLabel} {receiptId(session)}</span></div>
      </header>
      <div className="receipt-round-strip">
        <strong>Round {String(roundNumber).padStart(2, '0')}</strong>
        <span>{receipt.title}</span>
        <small>Focus · {receipt.focus}</small>
      </div>
      <div className="receipt-preview-match">
        <section className="receipt-preview-lane" data-corner="red" data-winner={receipt.winner === 'lakebase' || receipt.winner === 'tie'} aria-label="Lakebase receipt result">
          <DatabaseFighter label="LB" corner="red" />
          <div><strong>{receipt.lakebaseLabel ?? 'Lakebase'}</strong><b>{receipt.lakebaseValue}</b><span>{receipt.lakebaseStatus}</span></div>
          {(receipt.winner === 'lakebase' || receipt.winner === 'tie') && <em>{isRoundFour(session) ? 'Verified path' : isRoundFive(session) ? receipt.winner === 'tie' ? 'Same readiness' : 'Earlier setup' : receipt.winner === 'tie' ? 'Verified' : 'Winner'}</em>}
        </section>
        <span className="receipt-preview-vs" aria-hidden="true">VS</span>
        <section className="receipt-preview-lane" data-corner="blue" data-winner={receipt.winner === 'competitor' || receipt.winner === 'tie'} data-capability={receipt.competitorCapabilityGap} aria-label={`${session.competitor.short_name} receipt result`}>
          <DatabaseFighter label={competitorFighter} corner="blue" />
          <div><strong>{receipt.competitorLabel ?? session.competitor.short_name}</strong><b>{receipt.competitorValue}</b><span>{receipt.competitorStatus}</span></div>
          {(receipt.winner === 'competitor' || receipt.winner === 'tie') && <em>{isRoundFive(session) ? receipt.winner === 'tie' ? 'Same readiness' : 'Earlier setup' : receipt.winner === 'tie' ? 'Verified' : 'Winner'}</em>}
        </section>
      </div>
      <div className="receipt-verdict" data-width={resultWidth}>
        <span>{receipt.verdictLabel}</span>
        <strong>{receipt.verdict}</strong>
        {receipt.integrityDetail && <small>{receipt.integrityDetail}</small>}
      </div>
      <footer className="receipt-stub">
        <div>
          <strong>Fair-start contract</strong>
          <p>{receipt.fairness}</p>
          <small>{kind === 'idle' ? `Reset ${session.cooldown!.started_at}` : `Start gap ${skew}`} · {receipt.measuredAt} · Receipt {receiptId(session)}</small>
        </div>
        <b>One live run<span>Not a benchmark</span></b>
      </footer>
    </article>
  )
}

function ShareReceipt({
  session,
  roundNumber,
  kind,
  onClose,
}: {
  session: DemoSession
  roundNumber: number
  kind: ReceiptKind
  onClose: () => void
}) {
  const post = kind === 'idle' ? linkedInIdleReceipt(session) : linkedInReceipt(session, roundNumber)
  const [status, setStatus] = useState<string | null>(null)
  const dialogRef = useAccessibleDialog<HTMLElement>(true, onClose)
  const cardKey = `${session.id}:${session.updated_at}:${roundNumber}:${kind}`
  const [renderedCard, setRenderedCard] = useState<{ key: string; blob: Blob } | null>(null)
  const [failedCardKey, setFailedCardKey] = useState<string | null>(null)
  const cardBlob = renderedCard?.key === cardKey ? renderedCard.blob : null
  const cardRenderFailed = failedCardKey === cardKey

  useEffect(() => {
    let current = true
    void renderReceiptCard(session, roundNumber, kind)
      .then((blob) => { if (current) setRenderedCard({ key: cardKey, blob }) })
      .catch(() => { if (current) setFailedCardKey(cardKey) })
    return () => { current = false }
  }, [cardKey, kind, roundNumber, session])

  async function copyPost(updateStatus = true): Promise<boolean> {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(post)
      } else {
        const textarea = document.createElement('textarea')
        textarea.value = post
        textarea.style.position = 'fixed'
        textarea.style.opacity = '0'
        document.body.appendChild(textarea)
        textarea.select()
        const copied = document.execCommand('copy')
        textarea.remove()
        if (!copied) throw new Error('Copy was blocked')
      }
      if (updateStatus) setStatus('Caption copied')
      return true
    } catch {
      if (updateStatus) setStatus('Caption copy was blocked')
      return false
    }
  }

  async function prepareLinkedInPost() {
    if (!cardBlob) return
    const linkedinUrl = 'https://www.linkedin.com/feed/?shareActive=true'
    const filename = receiptCardFilename(session, roundNumber, kind)
    const file = new File([cardBlob], filename, { type: 'image/png' })
    const shareData: ShareData = {
      files: [file],
      title: 'Lakebase: The Anti-Demo',
      text: post,
    }
    const outcome = await offerNativeShare(shareData)
    if (outcome === 'shared') {
      setStatus('8-bit card sent to your share target')
      return
    }

    // A dismissed sheet lands here rather than returning: the operator still
    // wants the post, so they still get the tab, the PNG and the caption.
    const dismissed = shareDismissalPrefix(outcome)
    window.open(linkedinUrl, '_blank', 'noopener,noreferrer')
    const download = downloadReceiptCard(session, roundNumber, kind, cardBlob)
    const caption = copyPost(false)
    const [downloadResult, captionCopied] = await Promise.all([download.then(() => true).catch(() => false), caption])
    if (downloadResult && captionCopied) {
      setStatus(`${dismissed}Ready · Add media → choose the downloaded PNG → paste the caption`)
    } else if (downloadResult) {
      setStatus(`${dismissed}PNG downloaded · Add it with LinkedIn’s media button`)
    } else {
      // Not "use Download card": there is no such control on this screen, and
      // this branch is the one where the automatic download already failed.
      setStatus(`${dismissed}LinkedIn opened · Screenshot this card, then Add media`)
    }
  }

  return (
    <div className="receipt-overlay" role="presentation" onClick={onClose}>
      <section ref={dialogRef} className="receipt-modal" role="dialog" aria-modal="true" aria-labelledby="receipt-heading" onClick={(event) => event.stopPropagation()}>
        <header>
          <h2 id="receipt-heading">{kind === 'idle' ? 'Share idle proof' : 'Share the proof'}</h2>
        </header>
        <ReceiptPoster session={session} roundNumber={roundNumber} kind={kind} />
        <p className="receipt-share-note">LinkedIn desktop needs the PNG added as media; browser image paste is not reliable.</p>
        {status && <p className="receipt-status" role="status">{status}</p>}
        <div className="receipt-actions">
          <button className="game-back" onClick={onClose}>B · Back</button>
          <button className="receipt-download" onClick={() => { void copyPost() }}>Select · Copy caption</button>
          <button
            className="receipt-linkedin"
            disabled={!cardBlob}
            onClick={prepareLinkedInPost}
          >
            A · {cardBlob ? 'Prepare LinkedIn post' : cardRenderFailed ? 'Card unavailable' : 'Preparing 8-bit card…'}{/* gitleaks:allow (16-char state identifier, not a credential) */}
          </button>
        </div>
      </section>
    </div>
  )
}

/**
 * Why the return-to-idle clocks differ before either product does anything.
 *
 * Lakebase's shortest supported suspend timeout is 60s; Aurora's shortest
 * supported auto-pause delay is 300s and AWS rejects anything lower, so at
 * least 240s of any margin on this screen is a configuration floor rather than
 * a difference in how fast either engine settles. Naming that is the difference
 * between a disclosed measurement and an implied speed claim.
 *
 * The RDS variant is not a smaller number, it is no number: provisioned RDS has
 * no automatic idle pause, so `server/targets.py` reports the lane ineligible
 * and it is never timed.
 *
 * The second line adds the half the floor framing leaves out: that floor is
 * *billed*. It deliberately does not say Lakebase idles free, because it does
 * not -- Lakebase bills its 60s exactly as Aurora bills its 300s, and a customer
 * who knows that would take apart any copy implying otherwise. What survives the
 * challenge is the ratio, and the fact that a floor is charged per descent rather
 * than per day. Figures come from the server payload rather than being written
 * here, so a resize or a price change cannot leave this sentence behind; with no
 * payload the line is omitted rather than guessed.
 *
 * The one hard-written clause is the measured tail: a 15.31s Round 1 bout against
 * 420s of billed capacity, 97.2% of it after the bell. That reading is CloudWatch
 * out of band, not something the app samples, so there is no payload to take it
 * from. It states a ratio and a duration and no dollar total, because the model's
 * Aurora method is being corrected and a total would move.
 */
export function IdlePolicyFloor({
  competitorCannotIdle,
  descentCost,
}: {
  competitorCannotIdle: boolean
  descentCost?: DescentCostSnapshot | null
}) {
  const lakebase = descentCost?.lanes.find((lane) => lane.lane_id === 'lakebase')
  const competitor = descentCost?.lanes.find((lane) => lane.lane_id === 'competitor')
  return (
    <>
      <p className="between-policy-floor">
        <span>Idle policy · not idle speed</span>
        {competitorCannotIdle
          ? 'Lakebase is set to its shortest supported timeout, 60s. Provisioned RDS has no automatic idle pause at all, so its lane is not timed here.'
          : 'Each side is set to its shortest supported timeout — Lakebase 60s, Aurora 300s. AWS will not accept below 300s, so 240s of any gap is a product floor, not a setting we picked.'}
      </p>
      {lakebase && competitor && (
        <p className="between-floor-cost">
          <span>Floor is billed · not free on either side</span>
          {competitor.descends
            ? `Both engines bill that floor — up to ${competitor.per_descent_headline} a descent on ${competitor.product.split(' ')[0]}, ${lakebase.per_descent_headline} on Lakebase. You pay it on every descent, not once a day. Measured once on Round 1: a 15.31s bout, 420s of billed capacity, 97.2% of it after the bell.`
            : `Lakebase's 60s is billed too, about ${lakebase.per_descent_headline} a descent. That lane never descends at all — it bills ${competitor.per_descent_headline} a day per instance, idle or not.`}
        </p>
      )}
    </>
  )
}

function CooldownDetails({
  session,
  onClose,
}: {
  session: DemoSession
  onClose: () => void
}) {
  const cooldown = session.cooldown
  const dialogRef = useAccessibleDialog<HTMLElement>(true, onClose)
  if (!cooldown) return null
  const lakebase = cooldown.lanes.lakebase
  const competitor = cooldown.lanes.competitor
  const aurora = session.competitor.id === 'aurora_serverless_v2'
  const deleting = cooldown.mode !== 'return_to_idle'
  const recovery = cooldown.mode === 'delete_recovery_environment'
  const originOffset = cooldownOriginOffsetCopy(cooldown, session.competitor.short_name)
  return (
    <div className="replay-overlay" role="presentation" onClick={onClose}>
      <section ref={dialogRef} className="replay-modal cooldown-details" role="dialog" aria-modal="true" aria-labelledby="cooldown-details-heading" onClick={(event) => event.stopPropagation()}>
        <header>
          <div><p>Control-plane replay</p><h2 id="cooldown-details-heading">How the clocks stop</h2></div>
          <span>{deleting ? 'Owned artifacts only' : 'No SQL polling'}</span>
        </header>
        <div className="replay-lanes cooldown-detail-lanes">
          <article data-corner="red">
            <strong>Lakebase control plane</strong>
            <b>{deleting ? lakebase.state === 'confirmed_deleted' ? 'DELETED confirmed' : 'Deleting' : lakebase.state === 'confirmed_zero' ? 'IDLE confirmed' : 'Watching'}</b>
            <span>Call · <code>{deleting ? 'databricks postgres get-branch' : 'databricks postgres get-endpoint'}</code></span>
            <span>Read · <code>{deleting ? 'resource absent' : 'status.current_state'}</code></span>
            <span>Stop · <code>{deleting ? `${recovery ? 'recovery branch' : 'branch'} absent` : 'verified transaction + final connection close + current IDLE'}</code></span>
            {!deleting && <span>Fallback · repeated independent <code>IDLE</code> polls with monotonic dwell</span>}
            <p>Now · {lakebase.status}</p>
          </article>
          <article>
            <strong>{session.competitor.short_name} control plane</strong>
            <b>{deleting ? competitor.state === 'confirmed_deleted' ? 'DELETED confirmed' : 'Deleting' : competitor.state === 'confirmed_zero' ? 'PAUSED confirmed' : competitor.state === 'not_supported' ? 'Not supported' : 'Watching'}</b>
            {deleting ? (
              <>
                <span>Calls · <code>RDS DescribeDBInstances{aurora ? ' + DescribeDBClusters' : ''}</code></span>
                <span>Read · owned {recovery ? 'recovery' : 'isolated'} resources absent</span>
                <span>Stop · deletion confirmed</span>
              </>
            ) : aurora ? (
              <>
                <span>Calls · <code>RDS DescribeDBClusters + DescribeEvents</code></span>
                <span>Fallback · <code>CloudWatch GetMetricStatistics</code></span>
                <span>Stop · <code>Successfully paused</code> event; fallback: two fresh 0 ACU samples</span>
              </>
            ) : (
              <>
                <span>Call · <code>RDS DescribeDBInstances</code></span>
                <span>Read · engine, version, class, status</span>
                <span>Stop · not timed; no automatic scale-to-zero</span>
              </>
            )}
            <p>Now · {competitor.status}</p>
          </article>
        </div>
        {!deleting && (
          <section className="cooldown-provenance" aria-label="Timer evidence and arithmetic">
            <h3>Timer evidence and arithmetic</h3>
            <p>{originOffset}</p>
            <div>
              {([lakebase, competitor]).map((lane) => (
                <article key={lane.id}>
                  <strong>{lane.name}</strong>
                  <span>Last connection closed · <code>{lane.started_at}</code></span>
                  <span>Last control-plane check · <code>{lane.checked_at ?? 'not recorded'}</code></span>
                  <span>Provider <code>update_time</code> · <code>{lane.provider_updated_at ?? 'not available'}</code></span>
                  <span>Evidence class · <code>{cooldownEvidenceClass(lane)}</code></span>
                  <span>
                    Arithmetic · <code>{lane.elapsed_ms === null
                      ? `live: shared display tick - ${lane.started_at}`
                      : `${lane.confirmed_at ?? lane.checked_at ?? 'stop observation'} - ${lane.started_at} = ${lane.elapsed_ms.toFixed(3)} ms`}</code>
                  </span>
                </article>
              ))}
            </div>
            <p>Polling and delivery lag never extends a frozen result. The server’s first terminal lane snapshot owns the displayed stop value.</p>
            <IdlePolicyFloor competitorCannotIdle={competitor.state === 'not_supported'} descentCost={session.descent_cost} />
          </section>
        )}
        <ol className="cooldown-proof-rules">
          <li><span>01</span><div><strong>Lane-owned starts</strong><small>Each final connection close</small></div></li>
          <li><span>02</span><div><strong>One display tick</strong><small>Both live clocks update together</small></div></li>
          <li><span>03</span><div><strong>Read-only watchers</strong><small>No SQL wake effect</small></div></li>
        </ol>
        <button className="replay-close" onClick={onClose}>B · Back to the clocks</button>
      </section>
    </div>
  )
}

function BetweenRounds({
  session,
  competitor,
  error,
  onRingAgain,
  onRetryCleanup,
  onNextRound,
  onScorecard,
  commentaryOpen,
  onToggleCommentary,
  missedCalls = 0,
}: {
  session: DemoSession
  competitor: string
  /** Why a press did nothing, when the ring refused it. */
  error?: string | null
  onRingAgain: () => void
  onRetryCleanup: () => void
  onNextRound: () => void
  onScorecard: () => void
  commentaryOpen: boolean
  onToggleCommentary: () => void
  /** Calls the play-by-play never got to make; see `RingsideCommentator`. */
  missedCalls?: number
}) {
  const [showIdleReceipt, setShowIdleReceipt] = useState(false)
  const [showDetails, setShowDetails] = useState(false)
  const cooldown = session.cooldown
  const sharedNow = useSharedCooldownTick(cooldown)
  if (!cooldown) return null
  const ready = cooldown.state === 'ready'
  const failed = cooldown.state === 'failed'
  const deleting = cooldown.mode !== 'return_to_idle'
  const recoveryCleanup = cooldown.mode === 'delete_recovery_environment'
  const shareableIdleProof = isShareableIdleProof(session)
  const competitorCannotIdle = !deleting && cooldown.lanes.competitor.state === 'not_supported'
  const resetStatus = deleting
    ? failed
      ? `Cleanup stopped · One or more ${recoveryCleanup ? 'recovery' : 'isolated'} environments could not be removed · Retry cleanup`
      : ready
        ? `Both ${recoveryCleanup ? 'recovery' : 'isolated'} environments removed · RING AGAIN is ready`
        : `Clocks are live · Removing only the owned ${recoveryCleanup ? 'recovery' : 'test'} environments`
    : competitorCannotIdle
      ? ready
        ? 'Lakebase IDLE · RDS has no automatic scale-to-zero · RING AGAIN is ready'
        : 'Lakebase clock is live · RDS has no automatic scale-to-zero'
      : ready
        ? 'Both databases IDLE · RING AGAIN is ready'
        : 'Waiting for each database to become idle'
  const liveReferenceStartedAt = latestCooldownLaneStart(cooldown)
  const waitingLabel = deleting ? 'Waiting for cleanup' : 'Waiting for idle'
  return (
    <main className="retro-screen between-screen">
      <p className="pixel-kicker">{recoveryCleanup ? 'Round 3' : deleting ? 'Round 2' : 'Round 1'} · Re-do reset</p>
      <h1>{recoveryCleanup ? 'Clear the recovery corner' : deleting ? 'Clear the test corner' : 'Back to idle'}</h1>
      <p className="between-rule">
        {recoveryCleanup
          ? 'Both clocks started together at 0:00. Each stops only when its owned recovery environment is confirmed deleted.'
          : deleting
          ? 'Both clocks started together at 0:00. Each stops only when its isolated environment is confirmed deleted.'
          : "Each timer starts after that database's last connection closes."}
      </p>
      <div className="cooldown-match">
        <CooldownLane key={`lakebase:${cooldown.lanes.lakebase.started_at}`} lane={cooldown.lanes.lakebase} label="Lakebase" corner="red" mode={cooldown.mode} sharedNow={sharedNow} liveReferenceStartedAt={liveReferenceStartedAt} />
        <b>VS</b>
        <CooldownLane key={`competitor:${cooldown.lanes.competitor.started_at}`} lane={cooldown.lanes.competitor} label={competitor} corner="blue" mode={cooldown.mode} sharedNow={sharedNow} liveReferenceStartedAt={liveReferenceStartedAt} />
      </div>
      <p className="between-status" data-ready={ready}>{resetStatus}</p>
      {error && <p className="between-refusal" role="alert">{error}</p>}
      {!ready && !failed && (
        <RingsideCommentator
          session={session}
          open={commentaryOpen}
          onToggle={onToggleCommentary}
          cooldown
          missedCalls={missedCalls}
        />
      )}
      <div className="between-actions">
        <div className="between-tools">
          <button className="between-details" onClick={() => setShowDetails(true)}>Select · More details</button>
          <button className="between-scorecard" onClick={onScorecard}>Select · Scorecard</button>
          {shareableIdleProof && <button className="between-share" onClick={() => setShowIdleReceipt(true)}>Start · Share idle proof</button>}
        </div>
        <div className="between-main-actions">
          <button className="game-back" onClick={onNextRound}>B · Next round</button>
          <button
            className="game-primary"
            disabled={!ready && !failed}
            onClick={failed ? onRetryCleanup : onRingAgain}
          >
            A · {failed ? 'Retry cleanup' : ready ? 'Ring again' : waitingLabel}
          </button>
        </div>
      </div>
      {showDetails && <CooldownDetails session={session} onClose={() => setShowDetails(false)} />}
      {showIdleReceipt && <ShareReceipt session={session} roundNumber={1} kind="idle" onClose={() => setShowIdleReceipt(false)} />}
    </main>
  )
}

/**
 * One round of the ledger: what the round is, what it proves, and who took it.
 *
 * Four aligned fields, left to right, in the order a reader needs them: which
 * round, what it did, what the proof was, and the verdict. The verdict sits last
 * and right-aligned because it is the column somebody scans down -- the whole
 * complaint that produced this screen was a scorecard you could read top to
 * bottom without ever learning who won.
 */
function FinaleRow({ beat, result, reading, latest }: {
  beat: FinaleBeat
  /** Null when the record has been read and holds nothing for this round. */
  result: RoundResult | null
  /** True while the record is still being read, so no absence is asserted yet. */
  reading: boolean
  /**
   * The round this operator just finished. Lit because it is the one row whose
   * figure was produced in front of the room, which is a fact about when it was
   * measured and not a claim that it is worth more than the other five.
   */
  latest: boolean
}) {
  const verdict: LedgerVerdict = verdictFor(result, reading ? 'reading' : 'read')
  // A result from an earlier day says so, or the ledger reads as one sitting.
  const day = reading || !result ? null : ledgerDay(result)

  return (
    <article
      className="finale-beat"
      data-accent={beat.accent}
      data-status={reading ? 'reading' : result?.status ?? 'unrun'}
      data-latest={latest ? 'true' : undefined}
    >
      <header><span>{beat.number}</span><strong>{beat.title}</strong></header>
      <h2>{beat.flow}</h2>
      <p>
        {beat.proof}
        {/* The lane fact that qualifies the verdict, next to the evidence rather
            than next to our own number: the reason a corner has no figure is a
            statement about that corner. */}
        {verdict.laneNote && <em>{verdict.laneNote}</em>}
      </p>
      <div className="finale-win">
        {verdict.winner
          ? <b><span aria-hidden="true">{verdict.winner.badge}</span>{verdict.winner.name}</b>
          : <b>{verdict.outcome}</b>}
        {verdict.figure && <i>{verdict.figure}</i>}
        {verdict.qualifier && <u>{verdict.qualifier}</u>}
        {day && <u>{day}</u>}
      </div>
    </article>
  )
}

function Finale({ session, onBack, onSummary }: {
  session: DemoSession
  onBack: () => void
  /**
   * Forward, to the record of what this operator actually ran. The beats above are
   * the same six every time -- they are the product's story, not this run's -- so
   * the run's own results live on their own screen rather than displacing them.
   */
  onSummary: () => void
}) {
  const [cardBlob, setCardBlob] = useState<Blob | null>(null)
  const [cardFailed, setCardFailed] = useState(false)
  const [shareStatus, setShareStatus] = useState<string | null>(null)
  /**
   * The record this installation has on disk, which is what the ledger scores.
   *
   * `null` while the read is outstanding and `[]` once it has failed, so the two
   * are never conflated: an empty record and an unread one produce different
   * rows, and printing "not run yet" against a round that did run because a
   * fetch failed would be the worst kind of wrong on a scorecard.
   */
  const [receipts, setReceipts] = useState<BoutReceipt[] | null>(null)
  const [recordFailed, setRecordFailed] = useState(false)

  useEffect(() => {
    let current = true
    api.receipts()
      .then((response) => {
        if (!current) return
        setReceipts(response.receipts)
        setRecordFailed(false)
      })
      .catch(() => {
        if (!current) return
        setReceipts([])
        setRecordFailed(true)
      })
    return () => { current = false }
  }, [session.id])

  /**
   * Memoised because the card effect depends on it: a fresh Map every render
   * would re-encode a 1200x627 PNG on every render.
   */
  const roundResults = useMemo(
    () => new Map(summariseRounds(receipts ?? []).map((result) => [result.roundId, result])),
    [receipts],
  )

  useEffect(() => {
    // The card names winners, so it waits for the record. Rendering before the
    // read lands would bake "NOT RUN YET" into the image for every round and
    // then quietly replace it, and whoever hit share first would have the wrong
    // one. `receipts` is null only while the read is outstanding.
    if (receipts === null) return
    let current = true
    void renderFinaleCard(session, roundResults, !recordFailed)
      .then((blob) => {
        if (!current) return
        setCardBlob(blob)
        setCardFailed(false)
      })
      .catch(() => {
        if (!current) return
        setCardFailed(true)
      })
    return () => { current = false }
  }, [session, receipts, roundResults, recordFailed])

  async function copyCaption(): Promise<boolean> {
    const caption = finaleCaption(session)
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(caption)
      } else {
        const textarea = document.createElement('textarea')
        textarea.value = caption
        textarea.style.position = 'fixed'
        textarea.style.opacity = '0'
        document.body.appendChild(textarea)
        textarea.select()
        const copied = document.execCommand('copy')
        textarea.remove()
        if (!copied) throw new Error('Copy was blocked')
      }
      return true
    } catch {
      return false
    }
  }

  async function shareFinale() {
    if (!cardBlob) return
    const filename = finaleCardFilename(session)
    const file = new File([cardBlob], filename, { type: 'image/png' })
    const shareData: ShareData = {
      files: [file],
      title: 'Lakebase: The Anti-Demo · Six-round finale',
      text: finaleCaption(session),
    }
    const outcome = await offerNativeShare(shareData)
    if (outcome === 'shared') {
      setShareStatus('Full fight card sent to your share target')
      return
    }

    // Same reason as the receipt card: dismissing the sheet is not abandoning
    // the post, so the download path runs either way.
    const dismissed = shareDismissalPrefix(outcome)
    window.open('https://www.linkedin.com/feed/?shareActive=true', '_blank', 'noopener,noreferrer')
    const [downloaded, copied] = await Promise.all([
      downloadFinaleCard(session, cardBlob).then(() => true).catch(() => false),
      copyCaption(),
    ])
    setShareStatus(dismissed + (downloaded && copied
      ? 'Ready · Add the downloaded PNG, then paste the copied caption'
      : downloaded
        ? 'PNG downloaded · Add it with LinkedIn’s media button'
        : copied
          ? 'Caption copied · Add this screen as media'
          : 'LinkedIn opened · Download or screenshot this final card'))
  }

  const results = roundResults

  return (
    <main className="retro-screen finale-screen" aria-label="Six-round final recap">
      <header className="finale-header">
        <div>
          <p>Final bell · The six-round story</p>
          <h1>Six rounds.</h1>
          <span>From live applications to the lakehouse—and back again.</span>
        </div>
        <div className="finale-bill">
          {/* The same two fighters as the fight card, named once with their own
              chips, so the ledger reads as the same production. Named here
              rather than on all six rows because each row's verdict belongs to
              the corner that actually took it -- printing both names on a row
              the blue corner was never in would imply it lost that round. */}
          <ul className="finale-corners" aria-label="Corners">
            <li data-corner="red"><b>LB</b><span>LAKEBASE</span></li>
            <li data-corner="blue">
              <b>{opponentBadge(session.competitor.id)}</b>
              <span>{session.competitor.short_name}</span>
            </li>
          </ul>
          <strong>SHARE READY</strong>
        </div>
      </header>

      <section className="finale-grid" aria-label="All six round summaries">
        {FINALE_BEATS.map((beat) => (
          <FinaleRow
            key={beat.number}
            beat={beat}
            result={results.get(beat.roundId) ?? null}
            reading={receipts === null}
            latest={beat.roundId === session.round.id}
          />
        ))}
      </section>

      <footer className="finale-footer">
        {recordFailed && (
          <p className="finale-share-status" role="status">
            The record on disk could not be read just now · every result above is unstated, not absent
          </p>
        )}
        {shareStatus && <p className="finale-share-status" role="status">{shareStatus}</p>}
        <div className="finale-actions">
          <button type="button" className="game-back" onClick={onBack}>B · Fight card</button>
          <button type="button" className="finale-summary" onClick={onSummary}>Next · The rounds you ran</button>
          <button
            type="button"
            className="finale-share"
            disabled={!cardBlob}
            onClick={() => { void shareFinale() }}
          >
            Start · {cardBlob ? 'Share the full card' : cardFailed ? 'Card unavailable' : 'Preparing full card…'}
          </button>
        </div>
      </footer>
    </main>
  )
}

export function FinalScorecard({ entries, credits, onBack }: { entries: ScorecardEntry[]; credits: CreditsEntry; onBack: () => void }) {
  const tally = creditsTally(entries)
  return (
    <main className="retro-screen scorecard-screen">
      <p className="pixel-kicker">Final bell</p>
      <h1>Scorecard</h1>
      {/* Raced and unraced rounds are counted apart. A round the opponent never
          entered is a capability gap, not a verified win, and folding it into the
          win count is the easiest number on this screen to challenge. The count
          comes from `creditsTally` so the staff roll cannot disagree with the
          card it says it is restating. */}
      <p className="scorecard-tally">
        Lakebase · {tally.lakebaseWins} verified win{tally.lakebaseWins === 1 ? '' : 's'}
        {tally.uncontested > 0 && ` · ${tally.uncontested} uncontested round${tally.uncontested === 1 ? '' : 's'} · no opponent time to compare`}
        {(tally.incomplete ?? 0) > 0 && ` · ${tally.incomplete} incomplete comparison${tally.incomplete === 1 ? '' : 's'} · no winner declared`}
        {tally.abandoned > 0 && ` · ${tally.abandoned} abandoned round${tally.abandoned === 1 ? '' : 's'} · no result declared`}
      </p>
      <div className="scorecard-list">
        {entries.length === 0 && <p className="scorecard-empty">No completed bouts yet.</p>}
        {entries.map((entry, index) => (
          <article
            className="scorecard-row"
            key={entry.session_id}
            data-comparison={scorecardComparison(entry)}
          >
            <div>
              <span>Round {scorecardRoundNumber(entry.round_id) ?? index + 1}</span>
              <strong>{entry.round_title}</strong>
              <small>{entry.competitor_capability_gap ? `${entry.competitor} · NO LANE BUILT` : `VS ${entry.competitor}`}</small>
            </div>
            <dl>
              <div><dt>{scorecardProofLabel(entry)}</dt><dd>{scorecardLakebaseValue(entry)} <span>LB</span> · {scorecardCompetitorValue(entry)} <span>{scorecardCompetitorLabel(entry)}</span></dd></div>
              {/* An abandoned round finished nothing, so it had nothing to reset.
                  Printing the reset contract's WATCHING placeholder beside it
                  would claim a cooldown that never started. */}
              {!scorecardIsAbandoned(entry) && (
                <div><dt>{scorecardResetLabel(entry)}</dt><dd>{scorecardResetValue(entry.cooldown?.lakebase_state, entry.cooldown?.lakebase_ms)} <span>LB</span> · {scorecardResetValue(entry.cooldown?.competitor_state, entry.cooldown?.competitor_ms)} <span>OPP</span></dd></div>
              )}
            </dl>
            {entry.competitor_capability_gap && (
              <p className="scorecard-gap">
                Capability gap, not a race · no AWS lane was built, so nothing was
                timed against it and there is no margin to report.
              </p>
            )}
            {(entry.contract_status === 'comparison_incomplete'
              || entry.contract_status === 'guardrail_failure'
              || (entry.contract_status === 'cleanup_failure' && entry.formal_winner === null)) && (
              <p className="scorecard-gap">
                Comparison incomplete · no formal winner or margin was declared.
              </p>
            )}
            {/* Verbatim from the ledger, not paraphrased: the finale screen
                describes the same stopped round, and a card that says it
                differently invites the reader to pick which one is the claim. */}
            {scorecardIsAbandoned(entry) && (
              <p className="scorecard-gap scorecard-abandoned">
                {ABANDONED_VERDICT.qualifier} · {ABANDONED_VERDICT.outcome}
              </p>
            )}
            <p>{entry.remembered_result}</p>
          </article>
        ))}
      </div>
      {/* The second way in, and the one somebody reaches for on purpose: the
          final bell is when a viewer asks who built this. It is also the only
          other screen whose own subject is the tally the roll restates. */}
      <div className="scorecard-actions">
        <button className="game-back scorecard-back" onClick={onBack}>B · Fight card</button>
        <CreditsButton entry={credits} className="credits-entry scorecard-credits">
          Select · Staff roll
        </CreditsButton>
      </div>
    </main>
  )
}

function scorecardRoundNumber(roundId: RoundId | undefined): number | null {
  if (roundId === undefined) return null
  const number = ROUND_NUMBERS.indexOf(roundId)
  return number === -1 ? null : number + 1
}

/**
 * Whether this row is a round somebody stopped before our own lane verified.
 *
 * The absent Lakebase figure IS the condition, not a proxy for it: the entry is
 * only written without one when nothing was proved. Kept as a named predicate so
 * every part of the row asks the same question.
 */
function scorecardIsAbandoned(entry: ScorecardEntry): boolean {
  return entry.contract_status === undefined
    ? entry.lakebase_ms === null
    : entry.contract_status === 'no_verified_evidence'
}

function scorecardComparison(entry: ScorecardEntry): string {
  if (scorecardIsAbandoned(entry)) return 'abandoned'
  if (entry.contract_status === 'comparison_incomplete') return 'incomplete'
  if (entry.contract_status === 'guardrail_failure') return 'guardrail_failure'
  if (entry.contract_status === 'cleanup_failure' && entry.formal_winner === null) {
    return 'cleanup_failure'
  }
  return entry.competitor_capability_gap ? 'capability_gap' : 'raced'
}

function scorecardProofLabel(entry: ScorecardEntry): string {
  // The round's proof contract is what it would have proved. It did not, so the
  // label states the stoppage instead of a verification that never happened.
  if (scorecardIsAbandoned(entry)) return ABANDONED_VERDICT.laneNote
  if (entry.round_id === 'recover_deleted_order' || entry.cooldown?.mode === 'delete_recovery_environment') return 'Deletion → verified recovery'
  if (entry.round_id === 'make_schema_change_safely' || entry.cooldown?.mode === 'delete_isolated_environment') return 'Copy + change → verified'
  if (entry.round_id === 'wake_idle_app') return 'Wake → verified'
  if (entry.round_id === 'put_model_score_in_app') return 'Delta score → exact app read'
  if (entry.round_id === 'survive_connection_spike') return 'Exact setup stops → shared spike'
  // Round 6 landed here and read "Non-executable round · no proof", which is
  // false twice over: it is the finale, it runs, and its proof is the exact
  // order arriving in Delta with the count verified. What it has no proof *of*
  // is a comparison, and that is what the capability-gap column says.
  if (entry.round_id === 'analyze_live_orders_without_slowing_checkout') return 'Live order → exact Delta answer'
  return 'Non-executable round · no proof'
}

function scorecardResetLabel(entry: ScorecardEntry): string {
  if (entry.round_id === 'recover_deleted_order' || entry.cooldown?.mode === 'delete_recovery_environment') return 'Delete recovery environment'
  if (entry.round_id === 'make_schema_change_safely' || entry.cooldown?.mode === 'delete_isolated_environment') return 'Delete isolated copy'
  if (entry.round_id === 'wake_idle_app' || entry.cooldown?.mode === 'return_to_idle') return 'Return → confirmed zero'
  return 'No executable reset contract'
}

function shortSeconds(milliseconds: number): string {
  return `${(milliseconds / 1000).toFixed(2)}s`
}

/** The same wording a lane carries mid-bout when it has produced no figure. */
const NOT_VERIFIED = 'NOT VERIFIED'

function scorecardLakebaseValue(entry: ScorecardEntry): string {
  return entry.lakebase_ms === null ? NOT_VERIFIED : shortSeconds(entry.lakebase_ms)
}

function scorecardCompetitorValue(entry: ScorecardEntry): string {
  if (scorecardIsAbandoned(entry)) return NOT_VERIFIED
  if (entry.competitor_capability_gap) return 'NOT TIMED'
  if (entry.competitor_ms === null) return NOT_VERIFIED
  return `${entry.competitor_censored ? '>' : ''}${shortSeconds(entry.competitor_ms)}`
}

/** What the figure beside the opponent's value actually is. */
function scorecardCompetitorLabel(entry: ScorecardEntry): string {
  if (scorecardIsAbandoned(entry)) return `OPP · ${NOT_VERIFIED}`
  if (entry.competitor_capability_gap) return 'OPP · NO LANE TO TIME'
  if (entry.competitor_censored) return 'OPP · LOWER BOUND'
  if (entry.competitor_ms === null) return `${entry.competitor} · NO EXACT RESULT`
  return 'OPP'
}

function useSharedCooldownTick(cooldown: CooldownSnapshot | null | undefined): number {
  const ticking = Boolean(
    cooldown
    && Object.values(cooldown.lanes).some((lane) => lane.state === 'watching'),
  )
  const originKey = cooldown
    ? Object.values(cooldown.lanes).map((lane) => lane.started_at).join('|')
    : ''
  const [now, setNow] = useState(Date.now)
  useEffect(() => {
    if (!ticking) return
    const interval = window.setInterval(() => setNow(Date.now()), 250)
    return () => window.clearInterval(interval)
  }, [originKey, ticking])
  return now
}

function latestCooldownLaneStart(cooldown: CooldownSnapshot): string {
  const starts = Object.values(cooldown.lanes).map((lane) => Date.parse(lane.started_at))
  return new Date(Math.max(...starts)).toISOString()
}

function displayedOriginOffsetMs(
  startedAt: string,
  liveReferenceStartedAt: string,
): number {
  const differenceMs = Math.max(
    0,
    Date.parse(liveReferenceStartedAt) - Date.parse(startedAt),
  )
  return Math.round(differenceMs / 1000) * 1000
}

function liveCooldownElapsedMs(
  startedAt: string,
  liveReferenceStartedAt: string,
  sharedNow: number,
): number {
  const sharedElapsedMs = cooldownDisplayMilliseconds(
    Math.max(0, sharedNow - Date.parse(liveReferenceStartedAt)),
  )
  return sharedElapsedMs + displayedOriginOffsetMs(startedAt, liveReferenceStartedAt)
}

function cooldownOriginOffsetCopy(
  cooldown: CooldownSnapshot,
  competitor: string,
): string {
  const lakebaseStartedAt = Date.parse(cooldown.lanes.lakebase.started_at)
  const competitorStartedAt = Date.parse(cooldown.lanes.competitor.started_at)
  const differenceMs = Math.abs(lakebaseStartedAt - competitorStartedAt)
  const displayedDifference = cooldownTime(Math.round(differenceMs / 1000) * 1000)
  if (displayedDifference === '0:00') {
    return 'One UI tick drives both live timers · Final connections closed within the same displayed second.'
  }
  const earlier = lakebaseStartedAt < competitorStartedAt ? 'Lakebase' : competitor
  const later = lakebaseStartedAt < competitorStartedAt ? competitor : 'Lakebase'
  return `One UI tick drives both live timers · ${earlier} closed its final connection ${displayedDifference} before ${later}, so its live clock starts ${displayedDifference} ahead.`
}


function CooldownLane({
  lane,
  label,
  corner,
  mode,
  sharedNow,
  liveReferenceStartedAt,
}: {
  lane: NonNullable<DemoSession['cooldown']>['lanes'][LaneId]
  label: string
  corner: 'red' | 'blue'
  mode: CooldownSnapshot['mode']
  sharedNow: number
  liveReferenceStartedAt: string
}) {
  const active = lane.state === 'watching'
  const provisional = liveCooldownElapsedMs(
    lane.started_at,
    liveReferenceStartedAt,
    sharedNow,
  )
  const elapsedMs = lane.elapsed_ms ?? provisional
  const display = cooldownTime(elapsedMs)
  const unsupported = lane.state === 'not_supported'
  const deleted = lane.state === 'confirmed_deleted'
  const idleObserved = lane.observed_state === 'IDLE'
  const providerTransition = lane.confirmation_basis === 'provider_transition'
  const observedUpperBound = lane.state === 'confirmed_zero' && !providerTransition
  const statusTitle = unsupported
    ? 'NO AUTOMATIC IDLE'
    : deleted
      ? 'DELETED'
      : lane.state === 'confirmed_zero'
      ? 'IDLE CONFIRMED'
      : lane.state === 'failed'
        ? mode === 'return_to_idle' ? 'IDLE NOT VERIFIED' : 'DELETE NOT VERIFIED'
        : mode === 'return_to_idle'
          ? idleObserved
            ? 'CONFIRMING IDLE'
            : 'WAITING FOR IDLE'
          : 'RESETTING'
  const statusDetail = unsupported
    ? 'This database cannot pause automatically'
    : deleted
      ? `Removed in ${display}`
      : lane.state === 'confirmed_zero'
        ? observedUpperBound
          ? `Within ${display} of its last connection`
          : `Idle ${display} after its last connection`
        : lane.state === 'failed'
          ? mode === 'return_to_idle' ? 'The idle state could not be confirmed' : 'Deletion could not be confirmed'
          : mode === 'return_to_idle'
            ? `${display} since its last connection`
            : `${display} since reset began`
  const visibleClock = unsupported
    ? 'NO SCALE-TO-ZERO'
    : `${observedUpperBound ? '≤' : active && mode !== 'return_to_idle' ? '~' : ''}${display}`
  const clockAriaLabel = unsupported
    ? 'No automatic scale to zero'
    : observedUpperBound
      ? `Observed upper bound: IDLE confirmed within ${display} of its last connection`
      : providerTransition
        ? `Exact provider transition: IDLE ${display} after its last connection`
        : active && mode === 'return_to_idle'
          ? `${display} since its last connection; waiting for IDLE`
          : visibleClock
  return (
    <section
      className="cooldown-lane"
      data-corner={corner}
      data-state={lane.state}
      aria-label={`${label} ${mode === 'return_to_idle' ? 'return to idle' : mode === 'delete_recovery_environment' ? 'recovery environment reset' : 'isolated environment reset'}`}
    >
      <div className="cooldown-name"><DatabaseFighter label={corner === 'red' ? 'LB' : label.toLowerCase().includes('aurora') ? 'AUR' : 'RDS'} corner={corner} /><strong>{label}</strong></div>
      <div className="cooldown-time">
        <span aria-label={clockAriaLabel}>{visibleClock}</span>
      </div>
      <p className="cooldown-summary"><strong>{statusTitle}</strong><span>{statusDetail}</span></p>
    </section>
  )
}

function cooldownEvidenceClass(
  lane: CooldownSnapshot['lanes'][LaneId],
): string {
  if (lane.confirmation_basis === 'provider_transition') return 'exact provider transition'
  if (lane.confirmation_basis === 'provider_update_corroboration') {
    return 'observed upper bound; provider update_time corroborates current state'
  }
  if (lane.confirmation_basis === 'observed_idle_dwell') {
    return 'observed upper bound; repeated IDLE checks'
  }
  if (lane.confirmation_basis === 'observed_samples') {
    return 'observed upper bound; repeated zero-capacity samples'
  }
  return lane.state === 'watching' ? 'live elapsed since lane close' : 'not recorded'
}

function cooldownDisplayMilliseconds(milliseconds: number): number {
  return Math.max(0, Math.floor(milliseconds / 1000) * 1000)
}

function cooldownTime(milliseconds: number): string {
  const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000))
  const minutes = Math.floor(totalSeconds / 60)
  const seconds = totalSeconds % 60
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

function RingsideCommentator({
  session,
  open,
  onToggle,
  cooldown = false,
  liveEvidenceConnected = true,
  missedCalls = 0,
}: {
  session: DemoSession
  open: boolean
  onToggle: () => void
  cooldown?: boolean
  liveEvidenceConnected?: boolean
  /**
   * Calls the play-by-play never got to make, because the stream resumed past
   * the server's retention floor. Shown rather than left as a silent hole: the
   * list below is missing lines, and an operator reading it deserves to know
   * that rather than assume nothing happened.
   */
  missedCalls?: number
}) {
  const contentId = 'ringside-commentator-status'
  const conciseFlowRound = isRoundFour(session) || isRoundFive(session) || isRoundSix(session)
  const subtitle = isRoundFive(session)
    ? session.state === 'verified' ? 'Final verified call' : 'Live setup call'
    : conciseFlowRound ? null : 'Live server events'
  const lines = cooldown
    ? cooldownCommentary(session)
    : proofCommentary(session, liveEvidenceConnected)
  const wireCalls = (cooldown
    ? Object.values(session.cooldown?.lanes ?? {})
    : Object.values(session.lanes))
    .flatMap((lane) => lane.activity?.wire_call
      ? [`${lane.name}: ${lane.activity.wire_call}`]
      : [])
  return (
    <aside className="ringside-commentator" data-open={open} data-round-four={isRoundFour(session)} data-concise-flow={conciseFlowRound} aria-label="Ringside commentator">
      {open && <header>
        <span className="ringside-announcer">
          {/* Alt stays the bare name: the <strong> beside it already says
              "Ringside commentator", and repeating that here would read it out
              twice. */}
          <img src={brandAssets.ryanPixelSmall} alt="Ryan" />
        </span>
        <strong>Ringside commentator <em className="ringside-announcer-tag">Ryan</em></strong>
        {subtitle && <small>{subtitle}</small>}
      </header>}
      {!open && <strong className="ringside-hidden-status">Play-by-play hidden</strong>}
      <div id={contentId} className="ringside-commentary" role="status" aria-live="polite" aria-atomic="true" hidden={!open}>
        {open && (
          <>
          {missedCalls > 0 && (
            <code className="ringside-missed-calls" data-missed-calls={missedCalls}>
              MISSED CALLS · {missedCalls} {missedCalls === 1 ? 'call' : 'calls'} never reached this screen · The play-by-play picked up after them
            </code>
          )}
          <ul>
            {lines.lanes.map((line, index) => <li key={index}>{line}</li>)}
          </ul>
          <p>{lines.verdict}</p>
          {wireCalls.length > 0 && !conciseFlowRound && !cooldown && <code>ON THE WIRE · {wireCalls.join(' · ')}</code>}
          </>
        )}
      </div>
      <button
        type="button"
        aria-pressed={open}
        aria-controls={contentId}
        onClick={onToggle}
      >
        {open ? 'Select · Hide play-by-play' : 'Select · Show commentator'}
      </button>
    </aside>
  )
}

function proofCommentary(
  session: DemoSession,
  liveEvidenceConnected = true,
): { lanes: string[]; verdict: string } {
  if (isRoundFour(session)) {
    const lane = session.lanes.lakebase
    const status = lane.status.toLowerCase()
    const phase = lane.activity?.phase
      ?? (lane.state === 'verified'
        ? 'verified'
        : lane.state === 'failed'
          ? 'failed'
          : status.includes('commit')
            ? 'committing_source'
            : status.includes('read') || status.includes('application')
              ? 'reading_application'
              : status.includes('sync')
                ? 'waiting_sync'
                : null)
    let lakebase: string
    if (phase === 'committing_source') {
      lakebase = 'Lakehouse row committed · Bell starts now'
    } else if (phase === 'waiting_sync') {
      lakebase = 'Managed reverse ETL is moving the exact Delta row into Lakebase'
    } else if (phase === 'reading_application') {
      lakebase = 'Reverse ETL is complete · A fresh app connection is reading that exact row'
    } else if (phase === 'verified') {
      const syncMs = roundFourMetricMilliseconds(session, 'managed_availability_ms')
      const totalMs = roundFourMetricMilliseconds(session, 'application_proof_elapsed_ms')
      if (syncMs !== null && totalMs !== null && totalMs >= syncMs) {
        lakebase = `The exact row reached Lakebase in ${preciseDuration(syncMs)}. The ${preciseDuration(totalMs)} full proof adds a sync check, fresh app connection, and exact row read; it is not SQL query time.`
      } else {
        lakebase = `Exact app read verified in ${roundFourAppElapsed(session)}`
      }
    } else if (phase === 'failed') {
      lakebase = 'Exact app read not verified · Proof stopped'
    } else {
      lakebase = `Waiting for the Round 4 proof · ${lane.status}`
    }
    return {
      lanes: [lakebase],
      verdict: 'Aurora / RDS alone do not move the row · Add and operate a reverse-ETL stack',
    }
  }
  if (isRoundFive(session)) {
    const setupLanes = (['lakebase', 'competitor'] as LaneId[]).map((laneId) => {
      const lane = session.round5_setup?.lanes?.[laneId]
      const name = lane?.name || session.lanes[laneId].name
      const state = lane?.state ?? 'pending'
      const elapsedMs = typeof lane?.setup_elapsed_ms === 'number'
        && Number.isFinite(lane.setup_elapsed_ms)
        && lane.setup_elapsed_ms >= 0
        ? lane.setup_elapsed_ms
        : null
      const status = lane?.status?.trim() || 'Waiting for first timed setup event'
      let line: string
      if (state === 'running') {
        line = liveEvidenceConnected
          ? `${name} · Setup clock live · ${status}`
          : `${name} · Live evidence interrupted · Display clock frozen while reconnecting`
      } else if (state === 'verified') {
        line = `${name} · Exact setup gate verified · Clock stopped${elapsedMs === null ? '' : ` at ${preciseDuration(elapsedMs)}`} · ${status}`
      } else if (state === 'failed') {
        line = `${name} · Setup stop gate did not verify · ${status}`
      } else if (state === 'cleanup_failed') {
        line = `${name} · Backstage cleanup did not verify · ${status}`
      } else if (state === 'towelled') {
        line = `${name} · Setup stopped before verification · ${status}`
      } else {
        line = `${name} · Untimed shared preflight · Setup clock has not started`
      }
      return { name, state, elapsedMs, line }
    })
    const verified = setupLanes.filter((lane) => lane.state === 'verified')
    const active = setupLanes.filter((lane) => lane.state === 'running')
    const stopped = verified[0]
    const other = stopped ? setupLanes.find((lane) => lane !== stopped) : undefined
    const hasFailure = setupLanes.some((lane) => lane.state === 'failed' || lane.state === 'cleanup_failed' || lane.state === 'towelled')
    const readinessUpdate = !liveEvidenceConnected && active.length > 0
      ? 'Live evidence interrupted · No new result inferred · Reconnecting'
      : verified.length === 2 && session.state === 'verified'
        ? `Both pooled paths and the identical spike verified · ${roundFiveVerifiedVerdict(session)}`
      : verified.length === 2
        ? 'Both readiness clocks stopped · Identical 128-connection spike validation in progress · No comparison until every gate verifies'
      : verified.length === 1 && other?.state === 'running'
        ? `${stopped.name} reached readiness${stopped.elapsedMs === null ? '' : ` at ${preciseDuration(stopped.elapsedMs)}`} · ${other.name} readiness clock still running · No comparison yet`
      : verified.length === 1
          ? `${stopped.name} readiness clock stopped · ${other?.name ?? 'Other lane'} has not verified · No comparison yet`
          : hasFailure
            ? 'Readiness proof did not complete · No timing comparison'
            : active.length > 0
              ? 'Scored readiness setup in progress · Each clock stops independently at its exact application transaction'
              : 'Untimed shared preflight in progress · Both setup clocks are sealed'
    const verdict = `${readinessUpdate} · RDS Proxy is AWS best practice; if already deployed, this setup delay does not apply`
    return {
      lanes: setupLanes.map((lane) => lane.line),
      verdict,
    }
  }
  if (isRoundSix(session)) {
    const lane = session.lanes.lakebase
    const phase = lane.activity?.phase
    let lakebase: string
    if (phase === 'checkout') {
      lakebase = lane.status.toLowerCase().includes('separate')
        ? 'A separate checkout is committing while the analytical answer catches up'
        : 'Checkout order committed · Freshness clock starts now'
    } else if (phase === 'waiting_cdf') {
      lakebase = 'The committed order is moving into separate Delta history'
    } else if (phase === 'reading_checkout') {
      lakebase = 'Exact Delta answer matched · Verifying the separate checkout'
    } else if (phase === 'verified' || lane.state === 'verified') {
      const elapsed = metricValue(session, 'analytics_available_ms')?.value
      const elapsedLabel = typeof elapsed === 'number'
        ? laneReceiptTime(elapsed)
        : laneReceiptTime(lane.elapsed_ms)
      lakebase = `Exact Delta answer verified in ${elapsedLabel} · Separate checkout committed`
    } else if (phase === 'failed' || lane.state === 'failed') {
      lakebase = 'Exact Delta answer not verified · Proof stopped'
    } else {
      lakebase = 'Native change feed checked · Waiting for the checkout commit'
    }
    return {
      lanes: [lakebase],
      verdict: `Public Preview freshness proof only · ${session.competitor.short_name} needs a separate CDC stack that was not built or timed`,
    }
  }
  if (session.round.id === 'wake_idle_app') {
    const lanes = ([session.lanes.lakebase, session.lanes.competitor]).map((lane) => {
      if (lane.state === 'not_supported') {
        return `${lane.name} · No automatic scale-to-zero wake path, so there is no timer`
      }
      if (lane.state === 'verified') {
        return `${lane.name} · Same transaction committed and read back on the existing endpoint${frozenLaneTime(lane.elapsed_ms)}`
      }
      if (lane.state === 'failed') return `${lane.name} · Existing endpoint transaction was not verified`
      if (lane.state === 'connecting') {
        const attempts = lane.attempts > 0 ? ` · Attempt ${lane.attempts}` : ''
        return `${lane.name} · Reconnecting the same transaction to the existing endpoint${attempts}`
      }
      return `${lane.name} · Waiting for an authoritative connection event`
    })
    const eligible = Object.values(session.lanes).filter((lane) => lane.state !== 'not_supported')
    const verified = eligible.filter((lane) => lane.state === 'verified')
    const unsupported = Object.values(session.lanes).find((lane) => lane.state === 'not_supported')
    const otherLane = verified.length === 1
      ? eligible.find((lane) => lane.id !== verified[0].id)
      : undefined
    const verdict = unsupported
      ? verified.length === 1
        ? `${verified[0].name} existing endpoint verified · ${unsupported.name} has no timer · Awaiting server verdict`
        : `${unsupported.name} has no timer · No verdict`
      : verified.length === eligible.length && eligible.length > 0
        ? 'Both existing endpoints verified · Awaiting server verdict'
        : verified.length === 1 && otherLane?.state === 'connecting'
          ? `${verified[0].name} verified · Other lane still connecting · No verdict`
          : verified.length === 1
            ? `${verified[0].name} verified · Other lane not verified · No verdict`
            : 'Connection proof in progress · No verdict'
    return { lanes, verdict }
  }

  if (session.round.id !== 'make_schema_change_safely' && session.round.id !== 'recover_deleted_order') {
    return {
      lanes: [
        `${session.lanes.lakebase.name} · Non-executable round; no live adapter or timer`,
        `${session.lanes.competitor.name} · Non-executable round; no live adapter or timer`,
      ],
      verdict: 'Preview only · No result can be inferred',
    }
  }

  const lanes = ([session.lanes.lakebase, session.lanes.competitor]).map((lane) => (
    `${lane.name} · ${session.round.id === 'recover_deleted_order' ? recoveryLaneCopy(lane.activity?.phase, lane.status) : safeChangeLaneCopy(lane.activity?.phase, lane.status)}${lane.state === 'verified' ? frozenLaneTime(lane.elapsed_ms) : ''}`
  ))
  const verified = Object.values(session.lanes).filter((lane) => lane.activity?.phase === 'verified')
  const recovery = session.round.id === 'recover_deleted_order'
  const verdict = verified.length === 2
    ? recovery
      ? 'Both recovered orders and source deletions verified · Awaiting server verdict'
      : 'Both isolated environments verified · Awaiting server verdict'
    : verified.length === 1
      ? `${verified[0].name} verified · Other lane remains in its reported phase · No verdict`
      : recovery
        ? 'Point-in-time recovery proof in progress · No result until the server verdict'
        : 'Identical proof contract in progress · No result until the server verdict'
  return { lanes, verdict }
}

function safeChangeLaneCopy(phase: string | undefined, status: string): string {
  const phaseLabel = phase === 'creating'
    ? 'Owned isolated environment'
    : phase === 'migrating'
      ? 'Identical migration'
      : phase === 'verifying_application'
        ? 'Commit + readback'
        : phase === 'verifying_source'
          ? 'Source unchanged proof'
          : phase === 'verified'
            ? 'Verified'
            : phase === 'failed'
              ? 'Failed'
              : 'Awaiting server phase'
  return status ? `${phaseLabel} · ${status}` : phaseLabel
}

function recoveryLaneCopy(phase: string | undefined, status: string): string {
  const phaseLabel = phase === 'preparing_incident'
    ? 'Exact incident precondition'
    : phase === 'deleting_incident'
      ? 'Deleting exact incident'
      : phase === 'waiting_recovery_point'
        ? 'Waiting for recovery eligibility'
        : phase === 'restoring'
          ? 'Point-in-time recovery'
          : phase === 'connecting'
            ? 'Connecting'
            : phase === 'verifying_recovered_order'
              ? 'Verifying recovered order'
              : phase === 'verifying_source'
                ? 'Verifying source deletion'
                : phase === 'verified'
                  ? 'Verified'
                  : phase === 'failed'
                    ? 'Failed'
                    : 'Awaiting server phase'
  return status ? `${phaseLabel} · ${status}` : phaseLabel
}

function frozenLaneTime(elapsedMs: number | null): string {
  return elapsedMs === null ? '' : ` · ${(elapsedMs / 1000).toFixed(2)}s`
}

function cooldownCommentary(session: DemoSession): { lanes: string[]; verdict: string } {
  const cooldown = session.cooldown!
  const recovery = cooldown.mode === 'delete_recovery_environment'
  if (cooldown.mode === 'return_to_idle') {
    const lanes = ([cooldown.lanes.lakebase, cooldown.lanes.competitor]).map((lane) => {
      if (lane.state === 'confirmed_zero') return `${lane.name} · IDLE confirmed`
      if (lane.state === 'not_supported') return `${lane.name} · No automatic idle`
      if (lane.state === 'failed') return `${lane.name} · IDLE not verified`
      if (lane.observed_state === 'IDLE') return `${lane.name} · Confirming IDLE`
      return `${lane.name} · Waiting for IDLE`
    })
    return {
      lanes,
      verdict: cooldown.state === 'ready'
        ? 'Both databases are idle'
        : 'Waiting for both databases to return to idle',
    }
  }
  const lanes = ([cooldown.lanes.lakebase, cooldown.lanes.competitor]).map((lane) => {
    const fallback = lane.state === 'confirmed_zero'
      ? 'Control plane confirmed zero'
      : lane.state === 'confirmed_deleted'
        ? `Owned ${recovery ? 'recovery' : 'isolated'} environment confirmed deleted`
        : lane.state === 'not_supported'
          ? 'Automatic scale-to-zero is not supported'
          : lane.state === 'failed'
            ? cooldown.mode === 'return_to_idle'
              ? 'Zero was not confirmed'
              : `Owned ${recovery ? 'recovery' : 'isolated'} environment deletion failed`
            : `Removing only the owned ${recovery ? 'recovery' : 'isolated'} environment`
    const current = lane.status || fallback
    return lane.state === 'watching' && current !== fallback
      ? `${lane.name} · ${fallback} · ${current}`
      : `${lane.name} · ${current}`
  })
  const verdict = `Cleanup in progress · Only owned ${recovery ? 'recovery' : 'isolated'} environments are targets`
  return { lanes, verdict }
}

function scorecardResetValue(
  state: CooldownLaneState | undefined,
  milliseconds: number | null | undefined,
): string {
  if (state === 'not_supported') return 'NO SCALE-TO-ZERO'
  if (state === 'failed') return 'FAILED'
  return milliseconds === null || milliseconds === undefined
    ? 'WATCHING'
    : cooldownTime(milliseconds)
}

function Lane({
  lane,
  fallbackLabel,
  fighterLabel,
  corner,
  sessionState,
  liveEvidenceConnected,
  uiReview,
  censoredMs,
  notTimed = false,
}: {
  lane: DemoSession['lanes'][LaneId]
  fallbackLabel: string
  fighterLabel: string
  corner: 'red' | 'blue'
  sessionState: DemoSession['state']
  liveEvidenceConnected: boolean
  uiReview: boolean
  censoredMs?: number
  notTimed?: boolean
}) {
  const failed = lane.state === 'failed'
  const unsupported = lane.state === 'not_supported'
  const censored = censoredMs !== undefined
  const couldBeActive = !uiReview && sessionState === 'running' && (
    lane.state === 'connecting' || lane.state === 'verifying'
  )
  const snapshotFloor = lane.elapsed_at_snapshot_ms
  const elapsedMs = couldBeActive
    && typeof snapshotFloor === 'number'
    && Number.isFinite(snapshotFloor)
    && snapshotFloor >= 0
    ? Math.max(lane.elapsed_ms ?? 0, snapshotFloor)
    : lane.elapsed_ms
  const active = couldBeActive && liveEvidenceConnected
  const disconnected = couldBeActive && !liveEvidenceConnected
  const status = censored
    ? 'UNVERIFIED WHEN STOPPED · LOWER BOUND'
    : notTimed
      ? 'UNFINISHED · NOT TIMED'
    : unsupported && uiReview
      ? 'Capability preview · no live check'
      : uiReview
        ? 'No live transaction'
        : disconnected
          ? `LIVE BACKEND OFFLINE · DISPLAY FROZEN · ${lane.status}`
          : lane.status
  return (
    <section className="proof-lane" data-state={lane.state} data-corner={corner} aria-label={`${lane.name || fallbackLabel} result`}>
      <div className="lane-intro">
        <div className="proof-fighter"><DatabaseFighter label={fighterLabel} corner={corner} /></div>
        <div><p className="lane-corner">{corner} corner</p><p className="lane-name">{lane.name || fallbackLabel}</p></div>
      </div>
      <div className="lane-time" data-failed={failed} data-unsupported={unsupported} data-censored={censored} data-untimed={notTimed} data-live={active}>
        {unsupported
          ? 'No scale-to-zero'
          : uiReview
            ? '—'
            : failed
              ? 'Could not verify'
              : notTimed
                ? 'Not timed'
              : censored
                ? <span className="timer-readout" data-width="long">&gt;{(censoredMs / 1000).toFixed(2)}<span className="timer-unit">s</span></span>
                : <TimerValue elapsedMs={elapsedMs} active={active} disconnected={disconnected} />}
      </div>
      <p className="lane-status"><span aria-hidden="true">{lane.state === 'verified' ? '✓' : lane.state === 'failed' ? '!' : unsupported ? '—' : '•'}</span>{status}</p>
      {failed && lane.error && (
        <p className="lane-error" role="alert" title={lane.error}>{lane.error}</p>
      )}
    </section>
  )
}

function TimerValue({
  elapsedMs,
  active,
  disconnected,
}: {
  elapsedMs: number | null
  active: boolean
  disconnected: boolean
}) {
  const [timer, setTimer] = useState<{
    observedElapsedMs: number | null
    observedActive: boolean
    authoritativeMs: number | null
    displayMs: number
    lastTickAt: number | null
  }>(() => ({
    observedElapsedMs: elapsedMs,
    observedActive: active,
    authoritativeMs: elapsedMs ?? (active ? 0 : null),
    displayMs: elapsedMs ?? 0,
    lastTickAt: null,
  }))

  if (elapsedMs !== timer.observedElapsedMs || active !== timer.observedActive) {
    const acceptsElapsed = elapsedMs !== null && (
      timer.authoritativeMs === null || elapsedMs >= timer.authoritativeMs
    )
    const authoritativeMs = acceptsElapsed
      ? elapsedMs
      : active && timer.authoritativeMs === null
        ? 0
        : timer.authoritativeMs
    setTimer({
      observedElapsedMs: elapsedMs,
      observedActive: active,
      authoritativeMs,
      displayMs: disconnected
        ? timer.displayMs
        : !active && elapsedMs !== null
        ? elapsedMs
        : acceptsElapsed
          ? Math.max(timer.displayMs, elapsedMs)
          : timer.displayMs,
      lastTickAt: null,
    })
  }

  useEffect(() => {
    if (!active) return
    const interval = window.setInterval(() => {
      setTimer((current) => {
        if (!current.observedActive || current.authoritativeMs === null) return current
        const now = window.performance.now()
        if (current.lastTickAt === null) return { ...current, lastTickAt: now }
        return {
          ...current,
          displayMs: current.displayMs + now - current.lastTickAt,
          lastTickAt: now,
        }
      })
    }, 32)
    return () => window.clearInterval(interval)
  }, [active])
  const display = (timer.displayMs / 1000).toFixed(2)
  const width = display.length >= 6 ? 'long' : display.length >= 5 ? 'medium' : 'short'
  return <span className="timer-readout" data-width={width}>{display}<span className="timer-unit">s</span></span>
}

function InterpolatedAuthoritativeTimerValue({ elapsedMs, active }: { elapsedMs: number | null; active: boolean }) {
  const [timer, setTimer] = useState<{
    observedElapsedMs: number | null
    observedActive: boolean
    authoritativeMs: number | null
    displayMs: number
    lastTickAt: number | null
  }>(() => ({
    observedElapsedMs: elapsedMs,
    observedActive: active,
    authoritativeMs: elapsedMs,
    displayMs: elapsedMs ?? 0,
    lastTickAt: null,
  }))

  if (elapsedMs !== timer.observedElapsedMs || active !== timer.observedActive) {
    const acceptsElapsed = elapsedMs !== null && (
      timer.authoritativeMs === null || elapsedMs >= timer.authoritativeMs
    )
    setTimer({
      observedElapsedMs: elapsedMs,
      observedActive: active,
      authoritativeMs: acceptsElapsed ? elapsedMs : timer.authoritativeMs,
      displayMs: acceptsElapsed
        ? active
          ? Math.max(timer.displayMs, elapsedMs)
          : elapsedMs
        : timer.displayMs,
      lastTickAt: null,
    })
  }

  useEffect(() => {
    if (!active) return
    const interval = window.setInterval(() => {
      setTimer((current) => {
        if (!current.observedActive || current.authoritativeMs === null) return current
        const now = window.performance.now()
        if (current.lastTickAt === null) return { ...current, lastTickAt: now }
        return {
          ...current,
          displayMs: current.displayMs + now - current.lastTickAt,
          lastTickAt: now,
        }
      })
    }, 32)
    return () => window.clearInterval(interval)
  }, [active])

  const display = (timer.displayMs / 1000).toFixed(2)
  const width = display.length >= 6 ? 'long' : display.length >= 5 ? 'medium' : 'short'
  return <span className="timer-readout" data-width={width}>{display}<span className="timer-unit">s</span></span>
}

function CapabilityNote({ compact = false }: { compact?: boolean }) {
  return (
    <div className="capability-note" data-compact={compact}>
      <strong>Lakebase measured live · RDS capability checked before the bell</strong>
      <span>RDS has no automatic scale-to-zero wake, so there is no RDS timer</span>
    </div>
  )
}

function buildReviewSession(
  catalog: CatalogResponse,
  competitorId: CompetitorId,
  corners: CustomerCorner[],
  primaryId: PersonaId,
  secondaryIds: PersonaId[],
  roundId: RoundId,
  recommendation: LocalRecommendation,
): DemoSession {
  const competitor = catalog.competitors.find((item) => item.id === competitorId)!
  const primary = catalog.personas.find((item) => item.id === primaryId)!
  const secondary = secondaryIds.map((id) => catalog.personas.find((item) => item.id === id)!).filter(Boolean)
  const round = catalog.rounds.find((item) => item.id === roundId)!
  const lens = (persona: typeof primary) => ({
    persona_id: persona.id,
    nickname: persona.nickname,
    role: persona.role,
    interpretation: persona.presenter.interpretation,
    objection: persona.presenter.objection,
    response: persona.presenter.response,
  })
  const now = new Date().toISOString()
  return {
    id: 'local-ui-review',
    state: 'armed',
    created_at: now,
    updated_at: now,
    competitor,
    primary_persona: primary,
    secondary_personas: secondary,
    corners,
    round,
    recommendation_reason: recommendation.reason,
    presenter_pack: {
      opening: primary.presenter.opening,
      discovery_question: primary.questions.why ?? Object.values(primary.questions)[0],
      risk: primary.presenter.risk,
      stop_condition: stopCondition(round.id, competitorId),
      remembered_metric: recommendation.metric,
      primary: lens(primary),
      secondary: secondary.map(lens),
      closing: primary.presenter.closing,
    },
    lanes: {
      lakebase: { id: 'lakebase', name: 'Lakebase', state: 'sealed', elapsed_ms: null, attempts: 0, status: 'No live transaction', error: null },
      competitor: competitorId === 'rds_postgres' && round.id === 'wake_idle_app'
        ? { id: 'competitor', name: competitor.short_name, state: 'not_supported', elapsed_ms: null, attempts: 0, status: 'No automatic scale-to-zero or connection-triggered wake', error: null }
        : { id: 'competitor', name: opponentLabel(round.id, competitor.short_name), state: 'sealed', elapsed_ms: null, attempts: 0, status: 'No live transaction', error: null },
    },
    fairness: { same_client: true, same_transaction: true, same_nonce: true, launch_skew_ms: null },
    remembered_result: null,
    failure: null,
  }
}

function stateLabel(state: DemoSession['state']): string {
  if (state === 'verified') return 'Verified live'
  if (state === 'towelled') return 'Toweled live'
  if (state === 'failed') return 'Verification failed'
  if (state === 'running') return 'Running'
  return state
}

function fairnessCopy(roundId: RoundId): string {
  if (roundId === 'put_model_score_in_app') {
    return 'One exact Delta row · Managed reverse ETL · Exact Lakebase Postgres app read · AWS lane not executed or timed'
  }
  if (roundId === 'analyze_live_orders_without_slowing_checkout') {
    return 'One checkout order · Exact Delta answer · Separate checkout committed · AWS lane not built or timed'
  }
  if (roundId === 'wake_idle_app') {
    return 'Same DB region · Same data · Same client · Same transaction · Same verification'
  }
  if (roundId === 'make_schema_change_safely') {
    return 'Same source · Same schema change · Same app transaction · Source unchanged'
  }
  if (roundId === 'recover_deleted_order') {
    return 'Same exact row · One deletion barrier · Eligibility + recovery + verified read timed · Source remains deleted'
  }
  if (roundId === 'survive_connection_spike') {
    return 'Shared post-preflight monotonic T0 · Each setup clock stops at its own exact application transaction · Identical 128-connection spike is pass/fail'
  }
  return 'Non-executable round · No live fairness or timing contract'
}

/**
 * Which lane is holding the arm up, read off the server's own refusal text.
 *
 * This is a string coupling to `server/targets.py` and it is load-bearing: the
 * `arm_waiting` payload is one joined line of whichever lanes refused, with no
 * lane id, so the phrases below are the only signal available. When Aurora's
 * wait stopped being narrated by the CloudWatch sampler -- the message it used
 * to send is a fallback that is not the gate -- matching only `zero-capacity`
 * silently stopped recognising the Aurora lane and printed raw server text.
 * Both replacement phrases are matched here, and pinned by App.test.tsx.
 */
function armWaitingCopy(status: string): string {
  const waitingForLakebase = status.includes('not IDLE')
  const waitingForAurora = status.includes('zero-capacity')
    || status.includes('cannot pause it before')
    || status.includes('successful-pause event')
  if (waitingForAurora) {
    const lakebase = waitingForLakebase ? 'Lakebase is still cooling · ' : ''
    return `${lakebase}Aurora auto-pauses after 5 idle minutes, AWS's documented minimum · AWS confirmation may add ~1–2 minutes · Bell not started`
  }
  if (waitingForLakebase) {
    return 'Lakebase is still returning to idle · Re-arming automatically when zero is verified'
  }
  return status
}

function applyServerEvent(
  event: RunEvent,
  setSession: React.Dispatch<React.SetStateAction<DemoSession | null>>,
) {
  setSession((current) => selectRound4Session(current, applyRunEventSnapshot(current, event)))
}

export default App
