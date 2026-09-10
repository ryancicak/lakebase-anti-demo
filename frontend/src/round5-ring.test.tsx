import { cleanup, render, screen } from '@testing-library/react'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { FightCardRoundStatus, RoundId } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import { FightRing } from './ring'

const ROUND_FIVE = 'survive_connection_spike' as const
const ROUND_ONE = 'wake_idle_app' as const
const css = readFileSync(join(import.meta.dirname, 'ring.css'), 'utf8')
const aurora = FALLBACK_CATALOG.competitors.find(({ id }) => id === 'aurora_serverless_v2')!
const rds = FALLBACK_CATALOG.competitors.find(({ id }) => id === 'rds_postgres')!

function statuses(state: FightCardRoundStatus['state']): Record<RoundId, FightCardRoundStatus> {
  return Object.fromEntries(FALLBACK_CATALOG.rounds.map((round) => [
    round.id,
    {
      round_id: round.id,
      state: round.id === ROUND_FIVE ? state : 'ready',
      can_start: state === 'ready',
      active_phase: null,
      detail: null,
      updated_at: null,
      expires_at: null,
    },
  ])) as Record<RoundId, FightCardRoundStatus>
}

function props(overrides: Partial<Parameters<typeof FightRing>[0]> = {}) {
  return {
    rounds: FALLBACK_CATALOG.rounds,
    competitors: FALLBACK_CATALOG.competitors,
    competitor: aurora.id,
    selectedRoundId: ROUND_FIVE,
    opponentLabel: 'Aurora + RDS Proxy reference path',
    recommendedRoundId: ROUND_FIVE,
    roundStatuses: null,
    statusRequired: false,
    onRound: vi.fn(),
    onCompetitor: vi.fn(),
    ...overrides,
  }
}

/** The `translate(x,y) scale(s)` a fighter group is drawn with. */
function placement(node: Element | null) {
  const match = node?.getAttribute('transform')
    ?.match(/^translate\((-?[\d.]+),(-?[\d.]+)\) scale\(([\d.]+)\)$/)
  expect(match, `unreadable fighter transform: ${node?.getAttribute('transform')}`).not.toBeNull()
  return { x: Number(match![1]), y: Number(match![2]), scale: Number(match![3]) }
}

const nearFighter = (root: Element) => root.querySelector('#ring-home')!.parentElement
const farFighter = (root: Element) => root.querySelector('#ring-away')!.parentElement

/**
 * Where a bag is positioned. The translate sits on the bag's PARENT, because a
 * CSS `transform` keyframe replaces an SVG `transform` attribute rather than
 * composing with it -- a bag carrying both is drawn at the origin as soon as the
 * swing runs. Reading the parent is what pins that structure.
 */
function bagX(root: Element, side: 'near' | 'far') {
  const bag = root.querySelector(`.r5-bag-${side}`)!
  expect(bag.getAttribute('transform'), 'a swinging bag must carry no transform attribute').toBeNull()
  const wrapper = bag.parentElement!.getAttribute('transform')
  const match = wrapper?.match(/^translate\((\d+),0\)$/)
  expect(match, `unreadable bag wrapper transform: ${wrapper}`).not.toBeNull()
  return Number(match![1])
}

describe('Round 5 fight-card ring', () => {
  const disconnect = vi.fn()

  beforeEach(() => {
    disconnect.mockClear()
    vi.stubGlobal('ResizeObserver', class {
      observe = vi.fn()
      unobserve = vi.fn()
      disconnect = disconnect
    })
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('reuses the standard ring, ropes, floor, fighters, shadows, and identity rail', () => {
    const { container } = render(<FightRing {...props()} />)
    const stage = container.querySelector('.ring-stage.act-pool')

    expect(stage).toBeInTheDocument()
    expect(stage?.querySelectorAll('#ring-home, #ring-away')).toHaveLength(2)
    expect(stage?.querySelectorAll('.pool')).toHaveLength(2)
    expect(stage?.querySelectorAll('.post-far, .cap-far')).toHaveLength(2)
    expect(screen.getByRole('list', { name: 'Corners' })).toBeInTheDocument()
    expect(screen.getByText('LAKEBASE')).toBeInTheDocument()
    expect(screen.getAllByText('Aurora + RDS Proxy reference path')).toHaveLength(1)
  })

  /* The rejected passes, named so they cannot come back by accident: labelled
     client origins outside the ropes, converging rails into the gloves, floor
     plates naming each pooling product, and the architecture diorama that
     replaced the ring entirely. */
  it('draws no wiring, no origins, no product plates and no architecture surface', () => {
    const { container } = render(<FightRing {...props()} />)
    const stage = container.querySelector('.ring-stage.act-pool')!

    expect(stage.querySelectorAll('.r5-ring-rails, .r5-ring-packets, .r5-ring-client-origin')).toHaveLength(0)
    expect(stage.querySelectorAll('.r5-ring-gear')).toHaveLength(0)
    expect(stage.querySelector('.round5-architecture, .r5-target-board, .r5-backend-rails')).toBeNull()
    expect(stage.querySelectorAll('path')).toHaveLength(
      container.querySelectorAll('.ring-stage.act-pool > svg > path, .ring-stage.act-pool > svg > g > path').length,
    )
    expect(stage.textContent).not.toMatch(
      /IAM|SECRET|NETWORK|BACKEND SESSIONS|MEASURED|CLIENT FAN-IN|CLIENTS|BUILT-IN|PROXY|POOL/i,
    )
  })

  it('gives each fighter one heavy bag stencilled 10K, in his own half', () => {
    const { container } = render(<FightRing {...props()} />)
    const stage = container.querySelector('.ring-stage.act-pool')!
    const bags = [...stage.querySelectorAll('.r5-bag')]

    expect(bags).toHaveLength(2)
    expect(stage.querySelectorAll('.r5-bag text')).toHaveLength(2)
    for (const label of stage.querySelectorAll('.r5-bag text')) {
      expect(label.textContent).toBe('10K')
    }
    // 10K is the only text the stage carries, so nothing else needs reading.
    expect(stage.textContent?.replace(/10K/g, '').replace(/LB|AUR/g, '').trim()).toBe('')

    const near = placement(nearFighter(stage))
    const far = placement(farFighter(stage))
    const nearBag = bagX(stage, 'near')
    const farBag = bagX(stage, 'far')

    // Each bag sits inboard of its own fighter and outboard of nobody else's:
    // the near pair on the left of centre, the far pair on the right.
    expect(nearBag).toBeGreaterThan(near.x)
    expect(farBag).toBeLessThan(far.x)
    expect(nearBag).toBeLessThan(farBag)

    /* Neither prop crosses the centre line, at any ring length. The corners
       close on the centre as the stage narrows, and unclamped this overlapped
       the near bag with the far rig by 12 units at a 320px viewport. The widest
       edge of each prop is what has to clear: 28 for a bare bag, 68 for a rig. */
    const viewBox = Number(stage.querySelector('svg')!.getAttribute('viewBox')!.split(' ')[2])
    const centre = viewBox / 2
    expect(nearBag + 28).toBeLessThan(centre)
    expect(farBag - 68).toBeGreaterThan(centre)
    // The whole Round 5 addition stays sparse.
    expect(stage.querySelectorAll('.r5-ring-detail *').length).toBeLessThanOrEqual(24)
  })

  /* The claim, drawn: nothing was brought in for the near bag, and the far bag
     needs a frame stood up on the boards before it can hang. */
  it('hangs the near bag off the ring itself and gives only the far bag a rig', () => {
    const { container } = render(<FightRing {...props()} />)
    const stage = container.querySelector('.ring-stage.act-pool')!
    const rigs = [...stage.querySelectorAll('.r5-rig')]

    expect(rigs).toHaveLength(1)
    // The rig stands where the far bag hangs, and holds no bag of its own: the
    // bag swings inside equipment that stays still.
    expect(rigs[0].getAttribute('transform')).toBe(`translate(${bagX(stage, 'far')},0)`)
    expect(rigs[0].getAttribute('transform')).not.toBe(`translate(${bagX(stage, 'near')},0)`)
    expect(rigs[0].querySelector('.r5-bag')).toBeNull()
    expect(stage.querySelector('.r5-bag-near .r5-rig, .r5-bag-far .r5-rig')).toBeNull()
  })

  it('draws the fighters at the full boxer scale on the same floor line', () => {
    const { container } = render(<FightRing {...props()} />)
    const pool = container.querySelector('.ring-stage.act-pool')!
    const poolNear = placement(nearFighter(pool))
    const poolFar = placement(farFighter(pool))
    cleanup()

    const { container: one } = render(<FightRing {...props({ selectedRoundId: ROUND_ONE })} />)
    const wake = one.querySelector('.ring-stage.act-wake')!
    const wakeNear = placement(nearFighter(wake))

    expect(poolNear.scale).toBe(wakeNear.scale)
    expect(poolFar.scale).toBe(poolNear.scale)
    // Taller sprites grow up off the boards rather than through them: feet stay
    // on one floor line at every scale. Both y values are whole viewBox units --
    // the artwork's own convention, and Round 1's literal 258 is itself .16 off
    // the exact line -- so the two soles agree to within a unit, not exactly.
    const poolSole = poolNear.y + 88 * poolNear.scale
    const wakeSole = wakeNear.y + 88 * wakeNear.scale
    expect(Math.abs(poolSole - wakeSole)).toBeLessThan(1)
    expect(Number.isInteger(poolNear.y)).toBe(true)
    expect(wakeNear).toEqual({ x: 218, y: 172, scale: 2.3 })
  })

  it('states the drawing once for screen readers, without a live region', () => {
    const { container } = render(<FightRing {...props()} />)
    const summary = container.querySelector('.sr-only')!

    expect(container.querySelector('.ring-stage svg')).toHaveAttribute('aria-hidden', 'true')
    expect(summary).toBeInTheDocument()
    expect(container.querySelectorAll('.sr-only')).toHaveLength(1)
    expect(summary.textContent?.replace(/\s+/g, ' ')).toBe(
      'Both corners face an identical heavy bag marked 10K. The Lakebase bag hangs'
      + " from the ring's own rope; the Aurora + RDS Proxy reference path bag hangs from a rig"
      + ' stood up beside it.',
    )
    // Static text, read on arrival. A live region would announce the swing.
    expect(summary.getAttribute('aria-live')).toBeNull()
    expect(container.querySelector('[aria-live]')).toBeNull()
    expect(container.querySelector('[role="status"], [role="alert"]')).toBeNull()
    // No figure a reader could take for a measurement.
    expect(summary.textContent).not.toMatch(/ms|second|p99|margin|faster|verified/i)

    cleanup()
    const { container: one } = render(<FightRing {...props({ selectedRoundId: ROUND_ONE })} />)
    expect(one.querySelector('.sr-only')).toBeNull()
  })

  it('keeps both fighter nodes mounted when the competitor changes', () => {
    const { container, rerender } = render(<FightRing {...props()} />)
    const stage = container.querySelector('.ring-stage.act-pool')
    const red = container.querySelector('#ring-home')
    const blue = container.querySelector('#ring-away')

    rerender(
      <FightRing
        {...props({
          competitor: rds.id,
          opponentLabel: 'RDS PostgreSQL + RDS Proxy reference path',
        })}
      />,
    )

    expect(container.querySelector('.ring-stage.act-pool')).toBe(stage)
    expect(container.querySelector('#ring-home')).toBe(red)
    expect(container.querySelector('#ring-away')).toBe(blue)
    expect(screen.getByText('RDS', { selector: '#ring-away text' })).toBeInTheDocument()
    // Switching opponents changes the blue badge and nothing about the bags.
    expect(container.querySelectorAll('.r5-bag')).toHaveLength(2)
    expect(container.querySelectorAll('.r5-rig')).toHaveLength(1)
  })

  it('holds a static conceptual ring while status is still checking', () => {
    const { container } = render(
      <FightRing
        {...props({
          roundStatuses: null,
          statusRequired: true,
        })}
      />,
    )

    const stage = container.querySelector('.ring-stage.act-pool')
    expect(stage).toHaveAttribute('data-pool-motion', 'paused')
    expect(stage?.querySelectorAll('#ring-home, #ring-away')).toHaveLength(2)
    expect(stage?.querySelectorAll('.r5-bag')).toHaveLength(2)
  })

  it.each(['bout_in_progress', 'cleanup_in_progress', 'unavailable'] as const)(
    'holds a static conceptual ring while the round is %s',
    (state) => {
      const { container } = render(
        <FightRing
          {...props({
            roundStatuses: statuses(state),
            statusRequired: true,
          })}
        />,
      )

      const stage = container.querySelector('.ring-stage.act-pool')
      expect(stage).toHaveAttribute('data-pool-motion', 'paused')
      expect(stage?.querySelectorAll('#ring-home, #ring-away')).toHaveLength(2)
      expect(stage?.querySelectorAll('.r5-bag')).toHaveLength(2)
      expect(screen.getAllByText('10K')).toHaveLength(2)
    },
  )

  it('animates one punch-and-swing beat without timers and cleans up observation', () => {
    const setIntervalSpy = vi.spyOn(window, 'setInterval')
    const setTimeoutSpy = vi.spyOn(window, 'setTimeout')
    const { container, unmount } = render(<FightRing {...props()} />)

    expect(container.querySelector('.ring-stage.act-pool')).toHaveAttribute('data-pool-motion', 'active')
    expect(setIntervalSpy).not.toHaveBeenCalled()
    expect(setTimeoutSpy).not.toHaveBeenCalled()
    expect(css).toMatch(/r5-punch-near 6\.6s steps\(2, end\) infinite/)
    expect(css).toMatch(/r5-punch-far 6\.6s steps\(2, end\) infinite/)
    expect(css).toMatch(/r5-bag-swing-near 6\.6s steps\(3, end\) infinite/)
    expect(css).toMatch(/r5-bag-swing-far 6\.6s steps\(3, end\) infinite/)
    unmount()
    expect(disconnect).toHaveBeenCalledOnce()
  })

  it('uses transform only for its beat and preserves a complete reduced-motion still', () => {
    const punch = css.match(/@keyframes r5-punch-near\s*\{([\s\S]*?)\n\}/)?.[1] ?? ''
    const swingNear = css.match(/@keyframes r5-bag-swing-near\s*\{([\s\S]*?)\n\}/)?.[1] ?? ''
    const swingFar = css.match(/@keyframes r5-bag-swing-far\s*\{([\s\S]*?)\n\}/)?.[1] ?? ''

    expect(punch).toMatch(/34%, 40%[\s\S]*translateX\(12px\)/)
    // The bag is struck after the hand goes out, and mirrored across the ring.
    expect(swingNear).toMatch(/0%, 38% \{ transform: rotate\(0deg\); \}/)
    expect(swingNear).toMatch(/42%, 52% \{ transform: rotate\(6deg\); \}/)
    expect(swingFar).toMatch(/42%, 52% \{ transform: rotate\(-6deg\); \}/)
    // Nothing animates a property that reflows or repaints geometry.
    expect(`${punch}${swingNear}${swingFar}`).not.toMatch(/\b(?:left|top|width|height|opacity):/)
    expect(css).toMatch(/prefers-reduced-motion:\s*reduce[\s\S]*\.card-scene, \.card-scene \*[\s\S]*animation: none !important/)
    // With every loop dead the rest pose is the whole picture: an unswung bag
    // beside a fighter at guard needs no keyframe to be understandable.
    expect(css).toMatch(/\.ring-stage \.r5-bag \{[\s\S]*transform-origin: 50% 0;/)
    expect(css).not.toMatch(/prefers-reduced-motion[\s\S]*\.r5-bag[\s\S]*rotate/)
  })
})
