// The cost view behind "What it cost", and the provenance rules its strip keeps.
//
// WHY THIS FILE EXISTS. The strip is the one piece of new rendering in the cost
// view -- everything below it is a disclosure panel that already had its own
// guard. The strip's whole job is a contrast between three engines, and the
// contrast is only worth showing if each figure still says what kind of number
// it is. RDS PostgreSQL and Aurora are modelled from a sealed instance shape;
// Lakebase is a posted projection. Rendering those four values without their
// evidence would turn a disclosure into a marketing claim, so the evidence
// label is asserted on the same element as the figure.
//
// The fixture is the real build_standing_cost_disclosure output, the same one
// standing-cost.test.tsx uses, so these assertions run against the payload the
// server actually sends rather than a hand-written echo of it.

import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { CostRoom } from './App'
import type { DemoSession } from './api/types'
import standingCost from './standing-cost.fixture.json'

afterEach(cleanup)

const app = readFileSync(join(import.meta.dirname, 'App.tsx'), 'utf8')

function sessionWith(standing: unknown): DemoSession {
  return {
    competitor: { id: 'aurora_serverless_v2', short_name: 'Aurora Serverless v2' },
    standing_cost: standing,
    corners: ['cost'],
  } as unknown as DemoSession
}

/** The strip's articles, in render order. */
function lanes() {
  return [...document.querySelectorAll('.cost-room-idle article')]
}

describe('the cost view idle strip', () => {
  it('leads with the engine that never descends, then the two that do', () => {
    render(<CostRoom session={sessionWith(standingCost.priced)} onClose={() => undefined} />)
    expect(lanes().map((lane) => lane.getAttribute('data-lane'))).toEqual(['rds', 'aurora', 'lakebase'])
  })

  it('puts every figure on the same element as the evidence it rests on', () => {
    render(<CostRoom session={sessionWith(standingCost.priced)} onClose={() => undefined} />)
    const payload = standingCost.priced.lanes
    for (const rendered of lanes()) {
      const id = rendered.getAttribute('data-lane')
      const lane = payload.find((one) => one.lane_id === id)!
      // The figure is the payload's own string. Nothing is recomputed here, so
      // there is no arithmetic in the view that could drift from the server's.
      expect(rendered.querySelector('b')?.textContent).toBe(lane.figure.display)
      expect(within(rendered as HTMLElement).getByText(lane.idle_label)).toBeTruthy()
      expect(rendered.getAttribute('data-evidence')).toBe(lane.evidence)
      // Modelled and measured are never rendered the same way.
      const label = rendered.querySelector('small')?.textContent ?? ''
      expect(label).toMatch(lane.evidence === 'posted_projection' || lane.evidence === 'posted_actual'
        ? /^Measured/
        : /^Modelled|^Unpriced/)
    }
    expect(lanes()).toHaveLength(3)
  })

  it('says RDS never sleeps, and marks the figure that says so as modelled', () => {
    render(<CostRoom session={sessionWith(standingCost.priced)} onClose={() => undefined} />)
    const rds = lanes().find((lane) => lane.getAttribute('data-lane') === 'rds')!
    expect(rds.textContent).toMatch(/never sleeps/i)
    expect(rds.querySelector('small')?.textContent).toBe('Modelled · sealed shape only')
  })

  it('prints the word rather than a number when a figure is unavailable', () => {
    const gutted = {
      ...standingCost.priced,
      lanes: standingCost.priced.lanes.map((lane) => lane.lane_id === 'rds'
        ? { ...lane, figure: { ...lane.figure, state: 'unavailable' } }
        : lane),
    }
    render(<CostRoom session={sessionWith(gutted)} onClose={() => undefined} />)
    const rds = lanes().find((lane) => lane.getAttribute('data-lane') === 'rds')!
    const figure = rds.querySelector('b')?.textContent ?? ''
    expect(figure).toBe('Unavailable')
    expect(figure).not.toMatch(/\d/)
  })

  it('renders an explicit absence rather than an empty strip with no payload', () => {
    render(<CostRoom session={sessionWith(undefined)} onClose={() => undefined} />)
    expect(lanes()).toHaveLength(0)
    expect(screen.getByText(/no standing-cost payload/i)).toBeTruthy()
  })
})

describe('house rules', () => {
  it('carries no dollar amount in its own source, so one cannot be typed back in', () => {
    // The same rule the standing-cost panel keeps, for the same reason: every
    // figure this view shows belongs to the payload, and a literal here would
    // be a number with no server behind it.
    const start = app.indexOf('export function CostRoom')
    const source = app.slice(start, app.indexOf('\nfunction ', start))
    expect(source.length).toBeGreaterThan(500)
    expect(source).toContain('session.standing_cost')
    expect(source.match(/\$\d/g) ?? []).toEqual([])
  })
})
