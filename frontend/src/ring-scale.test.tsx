import { cleanup, render } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { RoundId } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { FightRing, ringAct } from './ring'

/**
 * The figure size, per act.
 *
 * This exists because the size table silently reverted to the artwork's original
 * `1.32` for five of the six acts once during development, and nothing failed:
 * every act still rendered, still fitted, still passed. The only symptom was that
 * the rounds looked unfinished next to Round 5 again. A number that can regress
 * invisibly is a number worth pinning.
 *
 * The ceiling is the top rope at y=168. Feet rest on the floor line at 374, so a
 * sprite 88 units tall at scale `s` reaches 374 - 88s, and 2.3 is the largest
 * scale that keeps a head under the rope. Acts below 2.3 are held down by their
 * own furniture, not by preference.
 */
const EXPECTED_SCALE: Record<string, number> = {
  wake_idle_app: 2.3,
  make_schema_change_safely: 2.3,
  recover_deleted_order: 2.3,
  put_model_score_in_app: 2.1,
  survive_connection_spike: 2.3,
  analyze_live_orders_without_slowing_checkout: 2,
}

const FLOOR_LINE = 374
const SPRITE_FOOT = 88
const TOP_ROPE = 168
/** What every act was drawn at before the acts were sized individually. */
const OLD_UNIFORM_SCALE = 1.32

function props(selectedRoundId: RoundId) {
  return {
    rounds: FALLBACK_CATALOG.rounds,
    competitors: FALLBACK_CATALOG.competitors,
    competitor: 'aurora_serverless_v2' as const,
    selectedRoundId,
    opponentLabel: 'Aurora Serverless v2',
    recommendedRoundId: selectedRoundId,
    roundStatuses: null,
    statusRequired: false,
    onRound: vi.fn(),
    onCompetitor: vi.fn(),
  }
}

function placement(root: Element, selector: string) {
  const node = root.querySelector(selector)
  if (!node) return null
  const match = node.parentElement?.getAttribute('transform')
    ?.match(/^translate\((-?[\d.]+),(-?[\d.]+)\) scale\(([\d.]+)\)$/)
  expect(match, `unreadable transform on ${selector}`).not.toBeNull()
  return { x: Number(match![1]), y: Number(match![2]), scale: Number(match![3]) }
}

describe('figure size per act', () => {
  beforeEach(() => {
    vi.stubGlobal('ResizeObserver', class {
      observe = vi.fn()
      unobserve = vi.fn()
      disconnect = vi.fn()
    })
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it.each(FALLBACK_CATALOG.rounds.map((round) => [round.id, round.title] as const))(
    'draws %s larger than the original uniform size, with its feet on the floor line',
    (roundId) => {
      const { container } = render(<FightRing {...props(roundId)} />)
      const stage = container.querySelector(`.ring-stage.act-${ringAct(roundId)}`)!
      const near = placement(stage, '#ring-home')!

      expect(near.scale).toBe(EXPECTED_SCALE[roundId])
      // The regression that hides: every act still renders at the old size.
      expect(near.scale).toBeGreaterThan(OLD_UNIFORM_SCALE)
      // Feet on one floor line at every scale, so a taller sprite grows upward
      // off the boards rather than sinking through them.
      expect(near.y + SPRITE_FOOT * near.scale).toBeCloseTo(FLOOR_LINE, 0)
      // And a head that stays under the top rope, or it stops reading as a man
      // standing in a ring.
      expect(near.y).toBeGreaterThanOrEqual(TOP_ROPE)
    },
  )

  it('draws both corners at the same size wherever both are in the ring', () => {
    for (const round of FALLBACK_CATALOG.rounds) {
      const { container } = render(<FightRing {...props(round.id)} />)
      const stage = container.querySelector('.ring-stage')!
      const near = placement(stage, '#ring-home')!
      const far = placement(stage, '#ring-away')
      // A corner with no equivalent native path has no fighter at all; that is a
      // fact about the pairing, not a size difference.
      if (far) expect(far.scale).toBe(near.scale)
      cleanup()
    }
  })

  /**
   * Furniture grows WITH the figures. Each act's props were hand-placed against
   * the original figure, so they are carried up by a zoom about a fixed anchor
   * rather than re-derived; if that zoom is dropped, the act renders a full-size
   * boxer beside doll's-house furniture.
   */
  it('carries each act’s furniture up with its figures', () => {
    const zoomed = /translate\([-\d.]+,[-\d.]+\) scale\([\d.]+\) translate\([-\d.]+,[-\d.]+\)/
    const cases: Array<[RoundId, string]> = [
      ['wake_idle_app', '.bed-near'],
      ['make_schema_change_safely', '.copy-near'],
      ['recover_deleted_order', '.whole'],
    ]
    for (const [roundId, selector] of cases) {
      const { container } = render(<FightRing {...props(roundId)} />)
      const node = container.querySelector(selector)!
      // The zoom is on the prop or on an ancestor of it, never further out than
      // the stage: a prop whose whole chain carries no zoom stayed behind.
      let carrier: Element | null = node
      let found = false
      while (carrier && !carrier.classList.contains('ring-stage')) {
        if (zoomed.test(carrier.getAttribute('transform') ?? '')) { found = true; break }
        carrier = carrier.parentElement
      }
      expect(found, `${selector} in ${roundId} is not carried by a zoom`).toBe(true)
      cleanup()
    }
  })
})
