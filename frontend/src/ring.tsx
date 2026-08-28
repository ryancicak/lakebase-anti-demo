import { useEffect, useMemo, useRef, useState } from 'react'
import type {
  CatalogResponse,
  CompetitorId,
  FightCardRoundStatus,
  RoundDefinition,
  RoundId,
} from './api/types'
import { NO_EQUIVALENT_NATIVE_PATH } from './recap'

/**
 * The fight card, drawn as a ring.
 *
 * Ported from the approved `A2-the-ring-final.html` mockup. The artwork, the
 * acts and the lane vocabulary are the mockup's. Everything the mockup had to
 * fake -- which rounds exist, which opponents exist, whether a round can arm --
 * is read off the real catalog instead, because the mockup's assumptions are not
 * evidence and `/api/catalog` has advertised rounds that could not arm.
 */

/** Which act the ring plays for a round. Keyed by the catalog's own round ids. */
export type RingAct = 'wake' | 'branch' | 'recover' | 'inbound' | 'spike' | 'outbound'

const ROUND_ACTS: Record<string, RingAct> = {
  wake_idle_app: 'wake',
  make_schema_change_safely: 'branch',
  recover_deleted_order: 'recover',
  put_model_score_in_app: 'inbound',
  survive_connection_spike: 'spike',
  analyze_live_orders_without_slowing_checkout: 'outbound',
}

export function ringAct(roundId: RoundId): RingAct {
  return ROUND_ACTS[roundId] ?? 'wake'
}

/**
 * What the blue corner is doing in this round, for this pairing.
 *
 *   race     both corners are timed
 *   untimed  that corner is in the ring and carries no clock
 *   none     that corner has no equivalent native path in this round
 *
 * A lane is a property of THE PAIRING, never of the round alone, and never of
 * the opponent's name. Receipt 434E2B99 carries `opponent.id` of
 * `aurora_serverless_v2` on a Round 4 lane whose state is `not_supported` and
 * which never ran, so naming an opponent proves nothing about whether it was
 * measured. This reads the catalog's own structural facts instead.
 */
export type BlueCornerLane = 'race' | 'untimed' | 'none'

/**
 * Rounds where the opponent is a destination, not a pipeline: the same outcome
 * needs a stack nobody built, so there is no native lane to time. Round 4 says
 * this in the catalog as `comparison_kind: 'capability_gap'`; Round 6 says it
 * in `scorecard_by_corner` ("required AWS CDC stack remains unpriced") and in
 * its non-claims, but carries no machine-readable marker, so it is named here.
 */
const CAPABILITY_GAP_ROUNDS = new Set<string>([
  'put_model_score_in_app',
  'analyze_live_orders_without_slowing_checkout',
])

export function blueCornerLane(round: RoundDefinition, competitorId: CompetitorId): BlueCornerLane {
  // The catalog decides who is even in this round. A pairing the round does not
  // list has an empty corner, whatever the opponent is called.
  if (!round.competitors.includes(competitorId)) return 'none'
  if (round.comparison_kind === 'capability_gap') return 'none'
  if (CAPABILITY_GAP_ROUNDS.has(round.id)) return 'none'
  // RDS cannot pause, so it is described through the control plane before the
  // bell and then left alone. It is present and it carries no clock; this is
  // the same fact `stopCondition()` states in prose for the same pairing.
  if (round.id === 'wake_idle_app' && competitorId === 'rds_postgres') return 'untimed'
  return 'race'
}

/**
 * The wording for a lane that is not being timed.
 *
 * Deliberately not the receipt's `NOT SUPPORTED / N/A`: on a selection screen a
 * spreadsheet blank reads as OUR limitation rather than as the blue corner
 * having no equivalent. "Native" is load-bearing -- without it the claim invites
 * a correct rebuttal, because the outcome is reachable, just not natively.
 *
 * The phrase itself lives in `recap.ts` so the finale ledger prints the same
 * words rather than a paraphrase of them.
 */
export function laneWording(round: RoundDefinition, competitorId: CompetitorId): string {
  if (round.id === 'wake_idle_app' && competitorId === 'rds_postgres') return 'NO SCALE-TO-ZERO'
  return NO_EQUIVALENT_NATIVE_PATH
}

/**
 * The lane wording on a key. There is no surrounding context naming whose lane
 * it is, so the non-racing states name the corner outright: a bare "no
 * equivalent path" on our own card would read as OUR limitation, which is the
 * exact inverse of the fact.
 */
export function laneKeyText(round: RoundDefinition, competitorId: CompetitorId): string {
  return blueCornerLane(round, competitorId) === 'race'
    ? 'Both corners timed'
    : `Blue corner · ${laneWording(round, competitorId)}`
}

/** The lane wording at the head of the blue corner's own zone. */
export function laneStateText(round: RoundDefinition, competitorId: CompetitorId): string {
  const lane = blueCornerLane(round, competitorId)
  if (lane === 'race') return 'Both corners timed'
  if (lane === 'untimed') return `In the ring, never timed · ${laneWording(round, competitorId)}`
  return 'No equivalent native path in the blue corner'
}

/**
 * What each round is, and what it refuses to prove.
 *
 * `brief` is true of every pairing -- anything true of only one corner lives on
 * that corner instead. `bell` is the last word before the button: what arming
 * spends and what the round will NOT prove. Every figure in both is quoted from
 * the tree. Nothing here is a result and no per-round bout duration is printed,
 * because none is published.
 */
interface RoundCopy {
  brief: string
  bell: string
}

const ROUND_COPY: Record<string, RoundCopy> = {
  // The bed is the reason this caveat exists: a fighter asleep reads as costing
  // nothing. True in every pairing, so it lives here.
  wake_idle_app: {
    brief: 'A clock stops only after its own transaction commits and reads back.',
    bell: 'A cold wake and nothing more — not a claim about either engine once it is up.',
  },
  make_schema_change_safely: {
    brief: 'We branch, they clone. Both take the same migration, and nobody verifies until the source proves it never moved.',
    bell: 'A real environment, built and deleted. Real minutes, real money. One run is existence, not a soak.',
  },
  recover_deleted_order: {
    brief: 'One row, one deletion barrier. The clock runs eligibility, restore, handshake and both reads. Ready stops nothing.',
    bell: 'A real point-in-time restore. Sit down and let the silence happen.',
  },
  // The empty trolley reads as zero setup unless somebody says otherwise.
  put_model_score_in_app: {
    brief: 'Governed lakehouse data, moved by the platform itself, read back out of the live app as one exact row.',
    bell: 'No opponent time and no margin — that is the finding, not a hole in it. An idle courier means no stack in the middle, not zero configuration and not zero security work.',
  },
  survive_connection_spike: {
    brief: 'Setup is the score. We bring nothing to pool with; the burst afterwards only checks it.',
    bell: 'Real changes to a real AWS account, made and removed, on one thirty-minute deadline. The burst is never added to the setup time.',
  },
  // An unbroken beat reads as measured zero impact. It is drawn, not measured.
  analyze_live_orders_without_slowing_checkout: {
    brief: 'One committed checkout, out through the built-in change feed, stopping when that exact row answers correctly.',
    bell: 'A second checkout rides along as guardrail. The steady queue is staged, not measured — no throughput, p99 or zero-impact claim. Needs a live-validated seal before it will arm.',
  },
}

export function roundBrief(roundId: RoundId): string {
  return ROUND_COPY[roundId]?.brief ?? ''
}

export function roundBell(roundId: RoundId): string {
  return ROUND_COPY[roundId]?.bell ?? ''
}

/**
 * `STAGING_DISCLOSURE` used to live here: a permanent paragraph on the fight
 * card explaining that the ring's pacing is drawn rather than recorded. The
 * owner removed it as over-engineering, and it is not a correctness hole --
 * nothing on that screen states a figure, so there is no measurement for a
 * caveat to walk back. Every number this app shows is produced after the bell,
 * by the verifier, on the receipt, and each of those surfaces carries its own
 * provenance line.
 *
 * WHAT MUST NOT COME BACK is the sentence it replaced, which denied that any
 * corner was drawn quicker than another. Round 5 contradicts that on screen --
 * the near gate passes clients from the top of its cycle while the far gate
 * assembles from nine staggered bolts first -- and that asymmetry is a true
 * depiction of a round whose receipts reach the setup gate in seconds against
 * minutes. Nor may anything here claim no live run of a round is on record;
 * real bouts are recorded now. `App.test.tsx` pins both absences.
 */

const OPPONENT_BADGES: Record<string, string> = {
  aurora_serverless_v2: 'AUR',
  rds_postgres: 'RDS',
}

export function opponentBadge(competitorId: CompetitorId): string {
  return OPPONENT_BADGES[competitorId] ?? '???'
}

/**
 * A sparse, static house. Depth, not a second thing competing for attention.
 *
 * Seeded off the drawn width so a longer floor gets a longer house rather than a
 * populated left half and an empty right one, and the seat count rises with it
 * to hold the density steady.
 */
function crowdSeats(width: number) {
  const seats: Array<{ x: number; y: number; s: number; o: string }> = []
  const count = Math.round(84 * (width / 1000))
  for (let i = 0; i < count; i += 1) {
    seats.push({
      x: (i * 137 + 41) % (width - 10),
      y: 24 + ((i * 61) % 104),
      s: 4 + ((i * 7) % 3),
      o: (0.2 + ((i * 13) % 5) / 11).toFixed(2),
    })
  }
  return seats
}

export function FightRing(props: {
  rounds: CatalogResponse['rounds']
  competitors: CatalogResponse['competitors']
  competitor: CompetitorId
  selectedRoundId: RoundId
  opponentLabel: string
  recommendedRoundId: RoundId
  /** One server snapshot for all six tiles; never six tile-owned requests. */
  roundStatuses: Record<RoundId, FightCardRoundStatus> | null
  /** Live cards fail closed until the first all-round snapshot arrives. */
  statusRequired: boolean
  onRound: (id: RoundId) => void
  onCompetitor: (id: CompetitorId) => void
}) {
  /**
   * The drawn width, in viewBox units, measured off the stage.
   *
   * The ring used to be drawn in a fixed 1000-unit box held at the artwork's own
   * ratio, which kept the figures the right size but left the stage narrower
   * than the accent rule above it -- the drawing stopped around 60% across and
   * the rest was dead floor.
   *
   * Widening it is deliberately NOT a scale-up: at a fixed ratio a wider stage
   * is a proportionally taller one, and there is no vertical slack on this
   * screen to pay for that. Instead the box gets longer while its height stays
   * at 460 units, so `meet` keeps resolving to the same height-driven scale and
   * every figure keeps the pixel size it has today. Only the floor, the ropes
   * and the distance between the corners grow.
   *
   * The width has to be measured rather than fixed: a hardcoded wide viewBox
   * flips `meet` from height-driven to width-driven at some viewports, which
   * silently shrinks the fighters -- the exact fault this is avoiding.
   */
  const stageRef = useRef<HTMLDivElement | null>(null)
  const [viewWidth, setViewWidth] = useState(1000)
  useEffect(() => {
    const node = stageRef.current
    if (!node || typeof ResizeObserver === 'undefined') return
    const measure = () => {
      const rect = node.getBoundingClientRect()
      // Unmeasured (jsdom, display:none) leaves the original 1000-unit drawing.
      if (rect.width <= 0 || rect.height <= 0) return
      const next = Math.max(1000, Math.round((rect.width * 460) / rect.height))
      setViewWidth((previous) => (Math.abs(previous - next) > 1 ? next : previous))
    }
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(node)
    return () => { observer.disconnect() }
  }, [])

  /**
   * How far the far corner has moved out. Every far-side group is translated by
   * this and nothing is rescaled, so the blue corner's furniture, fighter and
   * props stay in the same relationship to each other and to their own post.
   * Centre-of-ring props take half of it and stay centred.
   */
  const dx = viewWidth - 1000
  const mid = viewWidth / 2

  /**
   * How far each fighter steps back in off his own post.
   *
   * A longer floor moves the two corners apart by `dx`, which reads as the pair
   * being parked at opposite ends rather than working the same ring -- worst in
   * Round 2, where the exchange is the whole point. Both fighters come in by the
   * same amount so they stay symmetric about the centre line and stay in their
   * own halves.
   *
   * It is a fraction of `dx` rather than a constant because the drawn width
   * tracks the container: a fixed inset that reads well on a wide screen walks
   * the fighter into the centre prop on a narrow one. At `dx` of 0 -- the
   * original 1000-unit ring -- it is 0, so the untouched drawing is unchanged.
   *
   * The binding constraint is Round 6: its queue starts at 308 in centre-space,
   * which is the nearest any centre prop comes to the home corner. 0.4 clears it
   * at every width the stage takes.
   */
  const inset = Math.round(dx * 0.4)

  const crowd = useMemo(() => crowdSeats(viewWidth), [viewWidth])
  const roundIndex = props.rounds.findIndex((round) => round.id === props.selectedRoundId)
  const round = props.rounds[roundIndex] ?? props.rounds[0]
  const act = ringAct(round.id)
  const lane = blueCornerLane(round, props.competitor)
  const opponentIndex = props.competitors.findIndex((item) => item.id === props.competitor)

  // Who sleeps is a fact about the pairing. No corner is staged quicker than
  // another, because this screen holds no receipt that would license it.
  const wakeMode = act !== 'wake'
    ? ''
    : lane === 'none' ? ' wake-solo'
    : lane === 'untimed' ? ' wake-standing'
    : ' wake-even'

  /**
   * How far a figure's body travels along the floor when he lies down.
   *
   * Round 1 is the only act where the fighters are not on their feet, and
   * `ring-getup` pivots each one about his own feet rather than his middle, so
   * lying down swings the body sideways into the bed instead of turning it on
   * the spot. Measured on the rendered page: a standing centre of 512 becomes
   * 454, and the same 57 units on the far side.
   *
   * The pools are cast BY the figures, so they have to make that trip too. A
   * pool left at the standing spot lights an empty patch of floor beside the
   * bed, which is what it was doing -- the inset was never the fault there,
   * the pose was. Every other act leaves the figures upright, where the pools
   * already land within a unit of them.
   */
  const LIE_DOWN_SHIFT = 57

  /**
   * Which corners actually lie down. The engine that cannot pause has no bed
   * and stays on its feet for the whole round, so its light never moves.
   */
  const nearLiesDown = act === 'wake'
  const farLiesDown = act === 'wake' && lane === 'race'

  function rollOpponent(step: number) {
    if (props.competitors.length < 2) return
    const next = (opponentIndex + step + props.competitors.length) % props.competitors.length
    props.onCompetitor(props.competitors[next].id)
  }

  return (
    <div className="ring">
      {/* The ticked chase rail and the red/blue corner bar that used to sit here
          are gone. The only copy they carried was the benchmarking caveat, which
          has moved off this screen, and the corner split they drew is already
          said twice over -- by the two ring posts and by the nameplates. What is
          left is the solid red/blue rule along the top of those nameplates,
          which is load-bearing: it ties the floor edges to the plates and it is
          the ruler the ring's width is set against. */}

      {/* The stage and the two corner plates share one shrink-to-fit box, so the
          plates are exactly as wide as the drawn ring and each one sits under
          its own fighter. The tiles below stay full width. */}
      <div className="ring-arena">
      <div ref={stageRef} className={`ring-stage act-${act}${wakeMode}`} data-far={lane}>
        <svg viewBox={`0 0 ${viewWidth} 460`} preserveAspectRatio="xMidYMid meet" aria-hidden="true" focusable="false">
          <defs>
            <radialGradient id="ringPool">
              <stop offset="0" stopColor="#f1ebd7" stopOpacity=".24" />
              <stop offset="1" stopColor="#f1ebd7" stopOpacity="0" />
            </radialGradient>
            <linearGradient id="ringAir" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0" stopColor="#070b22" /><stop offset="1" stopColor="#0d1436" />
            </linearGradient>
          </defs>

          <rect x="0" y="0" width={viewWidth} height="460" fill="url(#ringAir)" />
          <g className="px" fill="#161f4a">
            {crowd.map((seat, i) => (
              <rect key={i} x={seat.x} y={seat.y} width={seat.s} height={seat.s} opacity={seat.o} />
            ))}
          </g>

          {/* The floor, the apron and the ropes are the only things that stretch:
              their right-hand ends are the drawn width less the same inset they
              always had, so the ring reaches the end of the accent rule instead
              of stopping short of it. */}
          <path className="px" d={`M70 300 L${viewWidth - 70} 300 L${viewWidth - 18} 420 L18 420 Z`} fill="#141c46" />
          <path className="px" d={`M70 300 L${viewWidth - 70} 300 L${viewWidth - 64} 308 L64 308 Z`} fill="#1d2861" />
          <path className="px" d={`M18 420 L${viewWidth - 18} 420 L${viewWidth - 18} 442 L18 442 Z`} fill="#0a1030" />
          <rect className="px" x="18" y="418" width={viewWidth - 36} height="4" fill="#2b376c" />

          {/* Corner light: the far pool dies when no clock is running in it.
              These read as scenery but they are cast BY the figures, so they
              carry the same inset -- a highlight the fighter has stepped out of
              is worse than no highlight -- and in Round 1 they follow the
              figures down into the beds as well. The resting position is the
              one the figure is in at rest; `ring-poolup` walks the light back
              out as he gets up, on the getup's own timing. */}
          <ellipse
            className="pool"
            cx={272 + inset - (nearLiesDown ? LIE_DOWN_SHIFT : 0)}
            cy="376" rx="168" ry="40" fill="url(#ringPool)"
          />
          <ellipse
            className="pool pool-far"
            cx={viewWidth - 268 - inset - (farLiesDown ? LIE_DOWN_SHIFT : 0)}
            cy="376" rx="168" ry="40" fill="url(#ringPool)"
          />

          <g className="px" opacity=".85">
            <path d={`M86 168 Q${mid} 178 ${viewWidth - 86} 168`} stroke="#8f9dcb" strokeWidth="4" fill="none" />
            <path d={`M86 212 Q${mid} 222 ${viewWidth - 86} 212`} stroke="#8f9dcb" strokeWidth="4" fill="none" />
            <path d={`M86 256 Q${mid} 266 ${viewWidth - 86} 256`} stroke="#8f9dcb" strokeWidth="4" fill="none" />
          </g>

          {/* The far post keeps its height, because the ropes are tied to it, and
              drains when no clock runs there: unclocked, not a broken drawing. */}
          <g className="px">
            <rect x="62" y="136" width="24" height="182" fill="#ee452d" />
            <rect x="56" y="124" width="36" height="16" fill="#ff735e" />
            <rect className="post-far" x={viewWidth - 86} y="136" width="24" height="182" />
            <rect className="cap-far" x={viewWidth - 92} y="124" width="36" height="16" />
          </g>

          {/* Props that must sit BEHIND a fighter: a bed only works if the frame
              is under the sleeper and the blanket is over him. */}
          {act === 'wake' && (
            <g>
              {/* The bed travels with the sleeper: the frame has to stay under
                  him and the blanket over him, so both take the same inset. */}
              <g className="px bed-near" transform={`translate(${inset},0)`}>
                <rect x="138" y="300" width="18" height="82" fill="#3b4680" />
                <rect x="138" y="348" width="172" height="16" fill="#5a6699" />
                <rect x="138" y="364" width="172" height="8" fill="#2b376c" />
                <rect x="142" y="372" width="12" height="14" fill="#2b376c" />
                <rect x="294" y="372" width="12" height="14" fill="#2b376c" />
                <rect x="158" y="332" width="42" height="16" fill="#e6dfc6" />
              </g>
              <g className="px bed-far" transform={`translate(${dx - inset},0)`}>
                <rect x="594" y="300" width="18" height="82" fill="#3b4680" />
                <rect x="594" y="348" width="172" height="16" fill="#5a6699" />
                <rect x="594" y="364" width="172" height="8" fill="#2b376c" />
                <rect x="598" y="372" width="12" height="14" fill="#2b376c" />
                <rect x="750" y="372" width="12" height="14" fill="#2b376c" />
                <rect x="614" y="332" width="42" height="16" fill="#e6dfc6" />
              </g>
            </g>
          )}

          <g transform={`translate(${241 + inset},258) scale(1.32)`}>
            <g className="g-bob px" id="ring-home">
              <rect x="12" y="0" width="24" height="12" fill="#e6dfc6" />
              <rect x="16" y="4" width="4" height="4" fill="#10152f" />
              <rect x="28" y="4" width="4" height="4" fill="#10152f" />
              <rect x="12" y="13" width="24" height="4" fill="#767b80" />
              <rect x="9" y="19" width="30" height="15" fill="#e6dfc6" />
              <rect x="9" y="36" width="30" height="15" fill="#cfc7ab" />
              <rect x="12" y="53" width="24" height="13" fill="#ee452d" />
              <text x="24" y="64" textAnchor="middle" fill="#f1ebd7" fontFamily="inherit" fontSize="9">LB</text>
              <rect x="14" y="67" width="7" height="19" fill="#cfc7ab" />
              <rect x="27" y="67" width="7" height="19" fill="#cfc7ab" />
              <rect x="11" y="82" width="12" height="6" fill="#10152f" />
              <rect x="25" y="82" width="12" height="6" fill="#10152f" />
              <rect x="-4" y="30" width="14" height="14" fill="#ee452d" />
              <rect className="jab-r" x="38" y="30" width="14" height="14" fill="#ee452d" />
            </g>
          </g>

          {lane !== 'none' && (
            <g transform={`translate(${701 + dx - inset},258) scale(1.32)`}>
              <g id="ring-away">
                <g className="g-bob px">
                  <rect x="10" y="0" width="28" height="6" fill="#74a5ff" />
                  <rect x="12" y="6" width="24" height="11" fill="#c8d2ee" />
                  <rect x="16" y="9" width="4" height="4" fill="#10152f" />
                  <rect x="28" y="9" width="4" height="4" fill="#10152f" />
                  <rect x="9" y="21" width="30" height="14" fill="#c8d2ee" />
                  <rect x="9" y="37" width="30" height="14" fill="#aab6dc" />
                  <rect x="12" y="53" width="24" height="13" fill="#4a83e8" />
                  <text x="24" y="64" textAnchor="middle" fill="#f1ebd7" fontFamily="inherit" fontSize="9">
                    {opponentBadge(props.competitor)}
                  </text>
                  <rect x="14" y="67" width="7" height="19" fill="#aab6dc" />
                  <rect x="27" y="67" width="7" height="19" fill="#aab6dc" />
                  <rect x="11" y="82" width="12" height="6" fill="#10152f" />
                  <rect x="25" y="82" width="12" height="6" fill="#10152f" />
                  <rect className="jab-l" x="-4" y="30" width="14" height="14" fill="#4a83e8" />
                  <rect x="38" y="30" width="14" height="14" fill="#4a83e8" />
                </g>
              </g>
            </g>
          )}

          {/* The empty corner keeps its furniture: a stool nobody is on, a towel
              still on the rope, and the round's own refusal wording stencilled
              low. The plate says whose corner it is, never that somebody was
              beaten in it. */}
          {lane === 'none' && (
            <g className="px vacant">
              {/* The empty corner's furniture travels with the corner it belongs
                  to, so the stool and towel stay under the blue post rather than
                  stranded mid-floor once the ring is longer. */}
              <g transform={`translate(${dx},0)`}>
                <rect x="838" y="352" width="42" height="6" fill="#2b376c" />
                <rect x="842" y="358" width="6" height="16" fill="#222d5c" />
                <rect x="870" y="358" width="6" height="16" fill="#222d5c" />
                <rect x="884" y="240" width="28" height="34" fill="#3b4680" />
              </g>
              {/* Centred in the empty upper third of the frame, not floated over
                  the blue corner's equipment. At x=796 and 15px this phrase is
                  ~420px wide, so it started mid-ring, crossed the fighters and
                  ran off the right edge of the 1000-unit viewBox, where the
                  stage clipped it. The wording is fixed and load-bearing, so the
                  caption moves and shrinks rather than being abbreviated. */}
              <text x={mid} y="150" textAnchor="middle" fill="#6b78a8" fontFamily="inherit" fontSize="13" letterSpacing="1">
                {laneWording(round, props.competitor)}
              </text>
            </g>
          )}

          {act === 'wake' && (
            <g>
              {/* Positioning lives on a PARENT of every animated prop, never on
                  the animated element itself: a CSS `transform` keyframe wins
                  outright over an SVG `transform` attribute, so an inset written
                  on the animated node is silently discarded the moment the
                  animation runs. A wrapper composes with it instead. */}
              <g transform={`translate(${inset},0)`}>
                <g className="bl-sleep bl-near px">
                  <rect x="194" y="326" width="96" height="30" fill="#6d7690" />
                  <rect x="194" y="326" width="96" height="7" fill="#a3accb" />
                </g>
              </g>
              <g transform={`translate(${dx - inset},0)`}>
                <g className="bl-sleep bl-far px">
                  <rect x="652" y="326" width="96" height="30" fill="#6d7690" />
                  <rect x="652" y="326" width="96" height="7" fill="#a3accb" />
                </g>
              </g>
              <g transform={`translate(${inset},0)`}>
                <g className="zzz-near px" fill="#c8d2ee">
                  <rect x="196" y="304" width="12" height="5" /><rect x="204" y="309" width="5" height="5" />
                  <rect x="196" y="314" width="12" height="5" />
                  <rect x="216" y="280" width="16" height="6" /><rect x="226" y="286" width="6" height="6" />
                  <rect x="216" y="292" width="16" height="6" />
                </g>
              </g>
              <g transform={`translate(${dx - inset},0)`}>
                <g className="zzz-far px" fill="#c8d2ee">
                  <rect x="654" y="304" width="12" height="5" /><rect x="662" y="309" width="5" height="5" />
                  <rect x="654" y="314" width="12" height="5" />
                  <rect x="674" y="280" width="16" height="6" /><rect x="684" y="286" width="6" height="6" />
                  <rect x="674" y="292" width="16" height="6" />
                </g>
              </g>
            </g>
          )}

          {act === 'recover' && (
            <g transform={`translate(${dx / 2},0)`}>
              <g className="ghost px">
                <rect x="462" y="256" width="76" height="92" fill="none" stroke="#4f5d90" strokeWidth="3" strokeDasharray="7 7" />
              </g>
              <g className="whole px">
                <rect x="462" y="256" width="76" height="92" fill="#f1ebd7" />
                <rect x="472" y="270" width="56" height="5" fill="#10152f" />
                <rect x="472" y="286" width="42" height="5" fill="#10152f" />
                <rect x="472" y="302" width="52" height="5" fill="#10152f" />
                <rect x="472" y="318" width="34" height="5" fill="#ee452d" />
                <rect x="458" y="252" width="84" height="100" fill="none" stroke="#f8d83b" strokeWidth="4" />
              </g>
              <g className="px">
                <g className="shard s1"><rect x="462" y="256" width="38" height="30" fill="#cfc7ab" /><rect x="472" y="270" width="24" height="5" fill="#10152f" /></g>
                <g className="shard s2"><rect x="500" y="256" width="38" height="30" fill="#e6dfc6" /><rect x="500" y="270" width="28" height="5" fill="#10152f" /></g>
                <g className="shard s3"><rect x="462" y="286" width="38" height="32" fill="#e6dfc6" /><rect x="472" y="302" width="26" height="5" fill="#10152f" /></g>
                <g className="shard s4"><rect x="500" y="286" width="38" height="32" fill="#cfc7ab" /><rect x="500" y="286" width="14" height="5" fill="#10152f" /></g>
                <g className="shard s5"><rect x="462" y="318" width="76" height="30" fill="#d8d0b6" /><rect x="472" y="330" width="34" height="5" fill="#ee452d" /></g>
              </g>
            </g>
          )}

          {act === 'spike' && (
            <g>
              <g className="px gate-near" transform={`translate(${inset},0)`}>
                <rect x="352" y="286" width="56" height="8" fill="#f8d83b" />
                <rect x="352" y="294" width="8" height="52" fill="#f8d83b" />
                <rect x="400" y="294" width="8" height="52" fill="#f8d83b" />
              </g>
              <g transform={`translate(${dx - inset},0)`}>
              <g className="gate-far px" fill="#4a83e8">
                {[
                  { x: 592, y: 286, w: 19, h: 8 }, { x: 611, y: 286, w: 19, h: 8 }, { x: 630, y: 286, w: 18, h: 8 },
                  { x: 592, y: 294, w: 8, h: 18 }, { x: 592, y: 312, w: 8, h: 17 }, { x: 592, y: 329, w: 8, h: 17 },
                  { x: 640, y: 294, w: 8, h: 18 }, { x: 640, y: 312, w: 8, h: 17 }, { x: 640, y: 329, w: 8, h: 17 },
                ].map((bolt, i) => (
                  <rect key={i} className="bolt" style={{ animationDelay: `${i * 0.42}s` }}
                    x={bolt.x} y={bolt.y} width={bolt.w} height={bolt.h} />
                ))}
              </g>
              </g>
              <g className="px" fill="#f1ebd7">
                {[0, 0.24, 0.48, 0.72, 0.96].map((delay, i) => (
                  <rect key={i} className="pip" style={{ animationDelay: `${delay}s` }}
                    x={[366, 380, 394, 373, 387][i] + inset} y="384" width="7" height="7" />
                ))}
                {[0, 0.32, 0.64].map((delay, i) => (
                  <rect key={`f${i}`} className="pip-far" style={{ animationDelay: `${delay}s` }}
                    x={[606, 620, 634][i] + dx - inset} y="384" width="7" height="7" />
                ))}
              </g>
            </g>
          )}

          {act === 'branch' && (
            <g>
              {/* The original, dead centre on a plinth. It has no animation:
                  not moving is the whole of its job. Centre of the drawn width,
                  so it stays equidistant from both copies as the ring lengthens. */}
              <g className="px" transform={`translate(${dx / 2},0)`}>
                <rect x="461" y="342" width="82" height="10" fill="#2b376c" />
                <rect x="471" y="254" width="62" height="88" fill="#1d2861" stroke="#f1ebd7" strokeWidth="3" />
                <rect x="483" y="272" width="38" height="5" fill="#c8d2ee" />
                <rect x="483" y="288" width="26" height="5" fill="#c8d2ee" />
                <rect x="483" y="304" width="32" height="5" fill="#c8d2ee" />
                <rect x="483" y="320" width="20" height="5" fill="#c8d2ee" />
              </g>
              <g transform={`translate(${inset},0)`}>
                <g className="copy copy-near px">
                  <rect x="334" y="266" width="62" height="80" fill="#1d2861" stroke="#8f9dcb" strokeWidth="3" />
                  <rect x="346" y="282" width="38" height="5" fill="#8f9dcb" />
                  <rect x="346" y="296" width="26" height="5" fill="#8f9dcb" />
                  <rect x="346" y="310" width="32" height="5" fill="#8f9dcb" />
                  <rect className="new-row" x="346" y="326" width="42" height="5" fill="#f8d83b" />
                </g>
              </g>
              {lane !== 'none' && (
                <g transform={`translate(${dx - inset},0)`}>
                  <g className="copy copy-far px">
                    <rect x="609" y="266" width="62" height="80" fill="#1d2861" stroke="#8f9dcb" strokeWidth="3" />
                    <rect x="621" y="282" width="38" height="5" fill="#8f9dcb" />
                    <rect x="621" y="296" width="26" height="5" fill="#8f9dcb" />
                    <rect x="621" y="310" width="32" height="5" fill="#8f9dcb" />
                    <rect className="new-row" x="621" y="326" width="42" height="5" fill="#f8d83b" />
                  </g>
                </g>
              )}
              <g transform={`translate(${inset},0)`}>
                <g className="whack whack-near px" fill="#f8d83b">
                  <rect x="322" y="290" width="9" height="7" /><rect x="328" y="274" width="7" height="12" />
                  <rect x="322" y="322" width="9" height="7" /><rect x="328" y="332" width="7" height="12" />
                </g>
              </g>
              {lane !== 'none' && (
                <g transform={`translate(${dx - inset},0)`}>
                  <g className="whack whack-far px" fill="#f8d83b">
                    <rect x="674" y="290" width="9" height="7" /><rect x="668" y="274" width="7" height="12" />
                    <rect x="674" y="322" width="9" height="7" /><rect x="668" y="332" width="7" height="12" />
                  </g>
                </g>
              )}
            </g>
          )}

          {act === 'inbound' && (
            <g transform={`translate(${dx / 2},0)`}>
              <g className="px">
                <rect x="466" y="192" width="148" height="62" fill="#101838" stroke="#8f9dcb" strokeWidth="4" />
                <rect x="466" y="192" width="148" height="9" fill="#8f9dcb" />
              </g>
              <g className="px">
                <rect x="524" y="292" width="10" height="84" fill="#8f9dcb" />
                <rect x="470" y="368" width="64" height="9" fill="#8f9dcb" />
                <rect x="474" y="377" width="16" height="15" fill="#5a6699" />
                <rect x="514" y="377" width="16" height="15" fill="#5a6699" />
                <rect x="472" y="354" width="60" height="14" fill="none" stroke="#3f4a80" strokeWidth="3" strokeDasharray="6 6" />
              </g>
              <g className="arm-l px"><rect x="382" y="292" width="15" height="38" fill="#e6dfc6" /></g>
              <g className="arm-r px"><rect x="446" y="292" width="15" height="38" fill="#e6dfc6" /></g>
              <g className="courier px">
                <rect x="402" y="254" width="38" height="10" fill="#f8d83b" />
                <rect x="406" y="264" width="30" height="24" fill="#e6dfc6" />
                <rect x="413" y="272" width="5" height="5" fill="#10152f" />
                <rect x="425" y="272" width="5" height="5" fill="#10152f" />
                <rect x="398" y="288" width="46" height="46" fill="#5a6699" />
                <rect x="398" y="288" width="46" height="7" fill="#a3accb" />
                <rect x="416" y="298" width="12" height="26" fill="#3b4680" />
                <rect x="404" y="334" width="14" height="32" fill="#3b4680" />
                <rect x="424" y="334" width="14" height="32" fill="#3b4680" />
                <rect x="400" y="364" width="20" height="9" fill="#10152f" />
                <rect x="422" y="364" width="20" height="9" fill="#10152f" />
              </g>
              <g className="huh px" fill="#f8d83b">
                <rect x="374" y="236" width="9" height="9" /><rect x="360" y="248" width="9" height="9" />
                <rect x="462" y="236" width="9" height="9" /><rect x="476" y="248" width="9" height="9" />
              </g>
              <g className="flyby px">
                <rect x="512" y="204" width="58" height="46" fill="#2b376c" stroke="#f8d83b" strokeWidth="4" />
                <rect x="524" y="218" width="34" height="7" fill="#f8d83b" />
                <rect x="524" y="231" width="22" height="7" fill="#c8d2ee" />
              </g>
            </g>
          )}

          {act === 'outbound' && (
            <g transform={`translate(${dx / 2},0)`}>
              <g className="px">
                {[
                  { c: 'q1', fill: '#e6dfc6', body: '#a8907c' },
                  { c: 'q2', fill: '#cfc7ab', body: '#7d6f8f' },
                  { c: 'q3', fill: '#e6dfc6', body: '#6f8f8a' },
                  { c: 'q4', fill: '#cfc7ab', body: '#8f7d6f' },
                ].map((person) => (
                  <g className={`q ${person.c}`} key={person.c}>
                    <rect x="313" y="298" width="15" height="14" fill={person.fill} />
                    <rect x="308" y="316" width="24" height="30" fill={person.body} />
                    <rect x="310" y="346" width="7" height="16" fill="#3b4680" />
                    <rect x="323" y="346" width="7" height="16" fill="#3b4680" />
                  </g>
                ))}
              </g>
              <g className="px">
                <rect x="446" y="292" width="118" height="12" fill="#6f7bb0" />
                <rect x="452" y="304" width="10" height="52" fill="#2b376c" />
                <rect x="548" y="304" width="10" height="52" fill="#2b376c" />
                <rect x="462" y="266" width="34" height="26" fill="#3b4680" />
                <rect x="468" y="273" width="22" height="10" fill="#74a5ff" />
              </g>
              <g className="px">
                <rect x="580" y="252" width="32" height="9" fill="#74a5ff" />
                <rect x="584" y="261" width="24" height="21" fill="#c8d2ee" />
                <rect x="589" y="269" width="5" height="5" fill="#10152f" />
                <rect x="599" y="269" width="5" height="5" fill="#10152f" />
                <rect x="576" y="282" width="40" height="44" fill="#1d2861" />
                <rect x="582" y="326" width="13" height="34" fill="#1d2861" />
                <rect x="598" y="326" width="13" height="34" fill="#1d2861" />
                <rect x="620" y="286" width="34" height="46" fill="#3b4680" stroke="#8f9dcb" strokeWidth="3" />
                <rect x="627" y="298" width="20" height="4" fill="#8f9dcb" />
                <rect x="627" y="308" width="20" height="4" fill="#8f9dcb" />
                <rect x="627" y="318" width="14" height="4" fill="#8f9dcb" />
              </g>
              <g className="sweep-arm px">
                <rect x="496" y="286" width="84" height="14" fill="#1d2861" />
                <rect x="496" y="286" width="84" height="4" fill="#4a83e8" />
              </g>
              <rect className="sweep-hand px" x="480" y="282" width="20" height="22" fill="#e6dfc6" />
              <g className="px">
                {['b1', 'b2', 'b3', 'b4'].map((beat, i) => (
                  <rect key={beat} className={`beat ${beat}`} x={446 + i * 30} y="362" width="24" height="8" fill="#4d5892" />
                ))}
              </g>
            </g>
          )}

          {/* The corners are named in DOM text on the identity rail below
              rather than stencilled into the artwork, so the names stay
              selectable, translatable and legible while the sprites move. */}
          <g transform="translate(6,300)">
            <g className="px">
              <rect x="30" y="6" width="46" height="12" fill="#f8d83b" />
              <rect x="66" y="10" width="22" height="8" fill="#f8d83b" />
              <rect x="34" y="18" width="38" height="30" fill="#d8b48a" />
              <rect x="42" y="28" width="5" height="5" fill="#10152f" />
              <rect x="58" y="28" width="5" height="5" fill="#10152f" />
              <rect x="44" y="40" width="18" height="4" fill="#8a6a4a" />
              <rect x="18" y="48" width="72" height="60" fill="#1d2861" />
              <rect x="18" y="48" width="72" height="7" fill="#f1ebd7" />
              <rect x="86" y="52" width="16" height="46" fill="#1d2861" />
              <rect x="94" y="44" width="16" height="14" fill="#d8b48a" />
              <rect x="0" y="86" width="22" height="22" fill="#2b376c" />
              <rect x="0" y="82" width="22" height="6" fill="#4f5d90" />
            </g>
          </g>
        </svg>

      </div>

      {/* Fighter identity in DOM text, legible during animation as well as at
          rest: the trunk badges ride the sprite and the full names sit still.
          A sibling of the stage rather than a child of it -- the stage clips its
          own overflow so the sprites cannot escape the ropes, and a name row
          inside that box is clipped along with them. */}
      <ul className="ring-identity" aria-label="Corners">
        <li data-corner="red"><b>LB</b><span>LAKEBASE</span></li>
        {/* The blue corner's plate IS the opponent picker. It used to be a
            static name sitting directly above a full-width row whose only job
            was to show the same name again. The label stays so it cannot be
            read as choosing the round. */}
        <li data-corner="blue" data-empty={lane === 'none' ? 'true' : undefined}>
          <b>{opponentBadge(props.competitor)}</b>
          <div className="ring-pick">
            <p id="ring-pick-label">Blue corner</p>
            <div className="ring-picker" role="group" aria-labelledby="ring-pick-label">
              <button className="ring-nudge" type="button" onClick={() => rollOpponent(-1)} aria-label="Previous opponent">◀</button>
              <span className="ring-who">
                <b>{props.opponentLabel}</b>
                <em>{opponentIndex + 1} of {props.competitors.length}</em>
              </span>
              <button className="ring-nudge" type="button" onClick={() => rollOpponent(1)} aria-label="Next opponent">▶</button>
            </div>
          </div>
        </li>
      </ul>
      </div>

      <div className="ring-keys" role="group" aria-label="The card · six rounds">
        {props.rounds.map((item, index) => {
          /* A SWAP, NOT A ROW. The lane line gives up its words for as long as
             the bout runs and takes them back after. A badge of its own would
             cost a line of tile height, and there is none to spend: at the
             1024x660 this is presented at, three tiles across leaves PREPARE
             FIGHT CARD 19px clear of the bottom edge. The lane fact is the
             right thing to give up -- static copy derived from the catalog,
             unchanged when it comes back, and not what anybody is reading the
             strip for while a bout is live. */
          const live = props.roundStatuses?.[item.id] ?? null
          const state = live?.state
            ?? (item.availability_reason_code === 'cleanup_in_progress'
              ? 'cleanup_in_progress'
              : item.availability === 'ready' ? 'ready' : 'unavailable')
          const busy = state === 'bout_in_progress'
          const cleaning = state === 'cleanup_in_progress'
          const stateNote = state.replaceAll('_', ' ').toUpperCase()
          const words = busy ? stateNote : laneKeyText(item, props.competitor)
          const badge = state === 'ready' || busy ? null : stateNote
          const selectable = live?.can_start
            ?? (!props.statusRequired && item.availability === 'ready')
          return (
            <button
              className="ring-key"
              type="button"
              key={item.id}
              data-lane={blueCornerLane(item, props.competitor)}
              data-busy={busy ? 'true' : undefined}
              data-cleanup={cleaning ? 'true' : undefined}
              data-availability={item.availability}
              aria-current={item.id === round.id ? 'true' : 'false'}
              aria-label={`Round ${index + 1} · ${item.title} · ${words} · ${stateNote}`}
              disabled={!selectable}
              onClick={() => props.onRound(item.id)}
            >
              <span className="ring-key-n">{index + 1}</span>
              <span className="ring-key-t">{item.title}</span>
              <span className="ring-key-l"><i aria-hidden="true" /><span>{words}</span></span>
              {/* Availability is the backend's answer and it outranks the artwork:
                  a round that cannot arm never renders as ready here. */}
              {badge && (
                <span className="ring-key-a" data-state={cleaning ? 'cleanup' : 'unavailable'}>
                  {badge}
                </span>
              )}
            </button>
          )
        })}
      </div>

    </div>
  )
}
