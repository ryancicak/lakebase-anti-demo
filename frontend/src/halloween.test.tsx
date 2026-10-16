import { cleanup, render } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { CompetitorId, RoundId } from './api/types'
import { FALLBACK_CATALOG } from './catalog'
import {
  HALLOWEEN_ROUND_NAMES,
  fighterCostume,
  fighterSprite,
  halloweenClock,
  isHalloweenSeason,
} from './halloween'
import { FightRing } from './ring'

/**
 * Halloween (v1.2.1). The season is October 1 through November 1 by the viewer's own date.
 * In it, Lakebase is the pumpkin and the opponent is Frankenstein in every round: the same
 * opponent all night wears the same costume all night. The test setup starts every test
 * outside the season; these move the clock in when they need it.
 */

const IN_SEASON = new Date(2026, 9, 13, 12, 0, 0)

function local(month: number, day: number, hours = 0, minutes = 0, seconds = 0, ms = 0): Date {
  return new Date(2026, month, day, hours, minutes, seconds, ms)
}

describe('the Halloween season', () => {
  it('runs from the first moment of October 1 to the last of November 1, local time', () => {
    expect(isHalloweenSeason(local(8, 30, 23, 59, 59, 999))).toBe(false)
    expect(isHalloweenSeason(local(9, 1))).toBe(true)
    expect(isHalloweenSeason(local(9, 31, 23, 59))).toBe(true)
    expect(isHalloweenSeason(local(10, 1, 23, 59, 59, 999))).toBe(true)
    expect(isHalloweenSeason(local(10, 2))).toBe(false)
    expect(isHalloweenSeason(local(5, 15))).toBe(false)
  })

  it('reads the clock it is given, which the test setup pins outside the season', () => {
    expect(isHalloweenSeason()).toBe(false)
    halloweenClock.now = () => IN_SEASON
    expect(isHalloweenSeason()).toBe(true)
  })

  it('names each of the six rounds', () => {
    expect(HALLOWEEN_ROUND_NAMES).toEqual({
      1: 'Wake the dead',
      2: 'Stitch the monster',
      3: 'Raise it from the grave',
      4: 'Summon the score',
      5: 'Ten thousand ghosts',
      6: 'Feed the lake',
    })
  })
})

describe('the cast', () => {
  it('wears nothing outside the season', () => {
    expect(fighterCostume('red', '')).toBeUndefined()
    expect(fighterCostume('blue', '')).toBeUndefined()
  })

  it('in season, dresses Lakebase as the pumpkin and the opponent as Frankenstein', () => {
    halloweenClock.now = () => IN_SEASON
    expect(fighterCostume('red', '')).toBe('pumpkin')
    expect(fighterCostume('blue', '')).toBe('frankenstein')
    expect(fighterCostume('blue', '?review=1&costumes=on')).toBe('frankenstein')
  })

  it('takes the costumes off with ?costumes=off, in season or not', () => {
    halloweenClock.now = () => IN_SEASON
    expect(fighterCostume('red', '?costumes=off')).toBeUndefined()
    expect(fighterCostume('blue', '?review=1&costumes=off')).toBeUndefined()
  })

  it("gives Aurora and RDS the same Frankenstein: only the body's colour differs", () => {
    const aurora = fighterSprite('frankenstein', '#3b7bea')
    const rds = fighterSprite('frankenstein', '#2aa3b8')
    const headCells = 16 * 12
    expect(aurora.columns).toBe(16)
    expect(aurora.cells).toHaveLength(16 * 20)
    expect(aurora.cells.slice(0, headCells)).toEqual(rds.cells.slice(0, headCells))
    expect(aurora.cells.slice(headCells)).toContain('#3b7bea')
    expect(rds.cells.slice(headCells)).toContain('#2aa3b8')
    expect(aurora.cells.slice(0, headCells)).toContain('#9ad46a')
  })
})

describe('the fight card ring in costume', () => {
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
    window.history.replaceState(null, '', '/')
  })

  function renderRing(selectedRoundId: RoundId, competitor: CompetitorId) {
    return render(
      <FightRing
        rounds={FALLBACK_CATALOG.rounds}
        competitors={FALLBACK_CATALOG.competitors}
        competitor={competitor}
        selectedRoundId={selectedRoundId}
        opponentLabel={competitor === 'aurora_serverless_v2' ? 'Aurora Serverless v2' : 'RDS PostgreSQL'}
        recommendedRoundId={selectedRoundId}
        roundStatuses={null}
        statusRequired={false}
        onRound={vi.fn()}
        onCompetitor={vi.fn()}
      />,
    )
  }

  /** The away head's first rect: the flat black top in costume, the blue hair without. */
  function awayHair(container: HTMLElement): string | null {
    return container.querySelector('#ring-away rect')?.getAttribute('fill') ?? null
  }

  it('puts the pumpkin and the same Frankenstein in the ring in every round, against either opponent', () => {
    halloweenClock.now = () => IN_SEASON
    let awayBoxers = 0
    for (const competitor of ['aurora_serverless_v2', 'rds_postgres'] as const) {
      for (const round of FALLBACK_CATALOG.rounds) {
        const { container, unmount } = renderRing(round.id, competitor)
        expect(container.querySelector('#ring-home')).toHaveAttribute('data-costume', 'pumpkin')
        const away = container.querySelector('#ring-away')
        // A corner with no equivalent native path has no boxer to dress.
        if (away) {
          awayBoxers += 1
          expect(away, `${competitor} in ${round.id}`).toHaveAttribute('data-costume', 'frankenstein')
          expect(awayHair(container)).toBe('#0d0818')
        }
        unmount()
      }
    }
    expect(awayBoxers).toBeGreaterThan(6)
  })

  it("keeps the pumpkin's stem inside the sprite, so its head stays under the top rope at the 2.3 ceiling", () => {
    // An independent review (2026-10-14): the stem was drawn at y=-4, above the 0-88 box the
    // 2.3 scale of Rounds 2, 3 and 5 is the ceiling for, so it crossed the rope at y=168.
    const rects = (container: HTMLElement) => [...container.querySelectorAll('#ring-home rect')]
    const ys = (container: HTMLElement) => rects(container).map((rect) => Number(rect.getAttribute('y')))
    const jaw = (container: HTMLElement) => rects(container).filter((rect) => rect.getAttribute('fill') === '#767b80')

    halloweenClock.now = () => IN_SEASON
    for (const round of FALLBACK_CATALOG.rounds) {
      const { container, unmount } = renderRing(round.id, 'aurora_serverless_v2')
      expect(Math.min(...ys(container)), round.id).toBeGreaterThanOrEqual(0)
      // The grey jaw belongs to the man; under a pumpkin it read as stubble.
      expect(jaw(container), round.id).toHaveLength(0)
      expect(rects(container).some((rect) => rect.getAttribute('fill') === '#4caf50'), round.id).toBe(true)
      unmount()
    }

    // The classic boxer is unchanged: head at the top of the box, jaw beneath it.
    halloweenClock.now = () => new Date(2026, 5, 15, 12, 0, 0)
    const { container } = renderRing('survive_connection_spike', 'aurora_serverless_v2')
    expect(Math.min(...ys(container))).toBe(0)
    expect(jaw(container)).toHaveLength(1)
  })

  it('draws the classic boxers outside the season, and with ?costumes=off', () => {
    const classic = () => {
      const { container, unmount } = renderRing('make_schema_change_safely', 'aurora_serverless_v2')
      expect(container.querySelector('#ring-home')).not.toHaveAttribute('data-costume')
      expect(container.querySelector('#ring-away')).not.toHaveAttribute('data-costume')
      expect(awayHair(container)).toBe('#74a5ff')
      unmount()
    }
    classic()
    halloweenClock.now = () => IN_SEASON
    window.history.replaceState(null, '', '/?costumes=off')
    classic()
  })
})
