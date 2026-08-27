import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { DescentCostDisclosure, IdlePolicyFloor } from './App'
import type { DemoSession, DescentCostSnapshot } from './api/types'

afterEach(cleanup)

/** Mirrors what server/descent_cost.py builds for Round 1 against Aurora. */
const AURORA: DescentCostSnapshot = {
  floor_ratio_label: "Aurora's floor is 5x Lakebase's · 300s vs 60s",
  illustrative_descents_per_day: 20,
  summary:
    'Both engines bill their idle floor. Aurora\u2019s is 5x longer, and it is charged on every descent.',
  note:
    'Neither floor is free, and neither figure is an invoice. Storage bills on every engine regardless of idle state and is a separate line.',
  lanes: [
    {
      lane_id: 'lakebase',
      product: 'Lakebase',
      descends: true,
      floor_label: '60s suspend timeout (vendor minimum)',
      per_descent_low_usd: 0.0004615,
      per_descent_high_usd: 0.001846,
      per_descent_display: '$0.00046 – $0.00185',
      per_descent_headline: '$0.002',
      per_day_display: '$0.01 – $0.04',
      derivation: '0.5–2 CU × 0.213 DBU per CU-hour × 60s ÷ 3600 × $0.26/DBU',
      rate_source: 'Databricks posted list price (promotional)',
      band_reason: 'Billed at its 0.5 CU floor at least. Unlike Aurora this floor is not zero.',
    },
    {
      lane_id: 'competitor',
      product: 'Aurora Serverless v2',
      descends: true,
      floor_label: '300s auto-pause (AWS documented minimum)',
      per_descent_low_usd: 0,
      per_descent_high_usd: 0.02,
      per_descent_display: '$0.00000 – $0.02000',
      per_descent_headline: '$0.02',
      per_day_display: '$0.00 – $0.40',
      derivation: '0–2 ACU × $0.12/ACU-hour × 300s ÷ 3600',
      rate_source: 'AWS Price List API · us-west-2 · rate-card derived, not invoice-verified',
      // Mirrors `_aurora_lane`'s band_reason verbatim. It used to hedge that
      // the descent "decays ACU on the way down, and that decay was never
      // sampled here"; `.anti-demo-v7/aurora-acu-2026-08-21.md` section 3
      // sampled it twice and it does not decay -- it holds a flat 0.5 ACU, the
      // least a running Serverless v2 writer reports, all the way down. That is
      // the better fact: it is why the 300s wake commitment costs what it does.
      band_reason:
        'Upper bound, not an estimate, and now a sampled one. CloudWatch caught two real descents and both held a dead-flat 0.5 ACU \u2014 a quarter of the 2 ACU ceiling \u2014 for the whole way down, costing $0.005363 and $0.009508. The floor is $0 because Aurora\u2019s minimum capacity is 0 ACU, but that is the paused state: a descent cannot bill less than 300s at 0.5 ACU, which is $0.005.',
    },
  ],
}

const RDS: DescentCostSnapshot = {
  floor_ratio_label: 'Provisioned RDS never descends · billed 100% of the time',
  illustrative_descents_per_day: 20,
  summary:
    'Lakebase bills a 60s floor each time it descends. Provisioned RDS never descends, so it bills around the clock instead.',
  note:
    'Lakebase\u2019s floor is billed, not free \u2014 it is simply paid per descent and then stops. Storage is a separate line on both engines.',
  lanes: [
    AURORA.lanes[0],
    {
      lane_id: 'competitor',
      product: 'RDS PostgreSQL',
      descends: false,
      floor_label: 'No automatic idle pause exists for provisioned RDS',
      per_descent_low_usd: null,
      per_descent_high_usd: null,
      per_descent_display: 'Never descends · no floor to pay',
      per_descent_headline: '$1.56',
      per_day_display: '$1.56 every day, idle or not',
      derivation:
        'db.t4g.medium at $0.065/instance-hour × 24 h. No idle term: there is nothing to descend into.',
      rate_source: 'AWS Price List API · us-west-2 · rate-card derived, not invoice-verified',
      band_reason: 'No band. This is the whole day billed whether or not anything ever connects.',
    },
  ],
}

function sessionWith(descent: DescentCostSnapshot | null): DemoSession {
  return { descent_cost: descent } as unknown as DemoSession
}

/**
 * The on-screen line. The point of these tests is the *negative* space: a
 * knowledgeable customer will ask whether Lakebase's own 60s is billed, and any
 * copy implying it is not would collapse under that question.
 */
describe('IdlePolicyFloor · billed floor line', () => {
  it('says outright that both engines bill the floor', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} descentCost={AURORA} />)
    const copy = screen.getByText(/Both engines bill that floor/)
    expect(copy.textContent).toContain('up to $0.02 a descent on Aurora')
    expect(copy.textContent).toContain('$0.002 on Lakebase')
  })

  it('frames the floor as charged per descent rather than per day', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} descentCost={AURORA} />)
    expect(
      screen.getByText(/You pay it on every descent, not once a day/),
    ).toBeTruthy()
  })

  it('never implies Lakebase idles free', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} descentCost={AURORA} />)
    const copy = screen.getByText(/Both engines bill that floor/).textContent ?? ''
    // The label may say "not free on either side"; what must never appear is a
    // claim attaching freeness, or an absent charge, to Lakebase itself.
    expect(copy).not.toMatch(/lakebase[^.]*\b(free|unbilled|no charge|no cost)/i)
    expect(copy).not.toMatch(/\bfree\b(?!\s+on either side)/i)
    expect(copy).toContain('Both engines bill that floor')
  })

  it('does not claim Aurora burns compute indefinitely while idle', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} descentCost={AURORA} />)
    const copy = screen.getByText(/Both engines bill that floor/).textContent ?? ''
    expect(copy).not.toMatch(/forever|indefinitely|always|never stops|constantly/i)
  })

  it('keeps the ratio a ratio and never calls Lakebase faster', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} descentCost={AURORA} />)
    const copy = screen.getByText(/Both engines bill that floor/).textContent ?? ''
    expect(copy).not.toMatch(/faster|quicker|beats|wins/i)
  })

  it('states Lakebase pays its own floor even on the RDS variant', () => {
    render(<IdlePolicyFloor competitorCannotIdle descentCost={RDS} />)
    const copy = screen.getByText(/is billed too/)
    expect(copy.textContent).toContain("Lakebase's 60s is billed too")
    expect(copy.textContent).toContain('$0.002 a descent')
  })

  it('gives RDS the unconditional always-billed claim', () => {
    render(<IdlePolicyFloor competitorCannotIdle descentCost={RDS} />)
    const copy = screen.getByText(/is billed too/)
    expect(copy.textContent).toContain('never descends at all')
    expect(copy.textContent).toContain('$1.56 a day per instance, idle or not')
  })

  it('composes with the policy-floor line rather than replacing it', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} descentCost={AURORA} />)
    expect(screen.getByText('Idle policy · not idle speed')).toBeTruthy()
    expect(screen.getByText(/240s of any gap is a product floor/)).toBeTruthy()
    expect(screen.getByText('Floor is billed · not free on either side')).toBeTruthy()
  })

  it('omits the cost line rather than guessing when the server sent nothing', () => {
    render(<IdlePolicyFloor competitorCannotIdle={false} />)
    expect(screen.getByText('Idle policy · not idle speed')).toBeTruthy()
    expect(screen.queryByText(/Floor is billed/)).toBeNull()
  })
})

describe('DescentCostDisclosure', () => {
  it('puts the arithmetic behind a click, not on the arena screen', () => {
    const { container } = render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const details = container.querySelector('details.descent-cost-disclosure')
    expect(details).toBeTruthy()
    expect(details?.hasAttribute('open')).toBe(false)
    expect(screen.getByText(/Cost of returning to idle/)).toBeTruthy()
  })

  it('shows each derivation so the figure can be checked', () => {
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    expect(screen.getByText('0–2 ACU × $0.12/ACU-hour × 300s ÷ 3600')).toBeTruthy()
    expect(
      screen.getByText('0.5–2 CU × 0.213 DBU per CU-hour × 60s ÷ 3600 × $0.26/DBU'),
    ).toBeTruthy()
  })

  it('labels the Aurora band as a bound and says why it is wide', () => {
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const reason = screen.getByText(/Upper bound, not an estimate/)
    expect(reason.textContent).toContain('dead-flat 0.5 ACU')
    expect(reason.textContent).toContain('minimum capacity is 0 ACU')
  })

  it('no longer claims the descent went unsampled, because it was sampled', () => {
    // The old copy hedged twice over -- that Aurora "decays ACU on the way
    // down", and that "that decay was never sampled here". Two real descents
    // contradict both halves: the writer drops to 0.5 ACU and holds it flat.
    // A hedge that survives its own disproof is worse than no hedge, and this
    // one was on screen.
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const reason = screen.getByText(/Upper bound, not an estimate/).textContent ?? ''
    expect(reason).not.toMatch(/decays ACU/i)
    expect(reason).not.toMatch(/never sampled|not sampled|never observed/i)
    expect(reason).toMatch(/sampled/i)
  })

  it('quotes what the sampled descents cost, inside the band it prints', () => {
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const reason = screen.getByText(/Upper bound, not an estimate/).textContent ?? ''
    expect(reason).toContain('$0.005363')
    expect(reason).toContain('$0.009508')
    // Both sit inside the $0.00000 – $0.02000 band this lane renders, so the
    // measurement shows how far the ceiling overstates rather than replacing it.
    expect(0.005363).toBeGreaterThan(AURORA.lanes[1].per_descent_low_usd ?? 0)
    expect(0.009508).toBeLessThan(AURORA.lanes[1].per_descent_high_usd ?? 0)
  })

  it('qualifies the $0 floor as the paused state rather than a descent', () => {
    // $0 is honest about a parked cluster and misleading about a descent: a
    // descent that is happening is running, and a running writer reports no
    // less than 0.5 ACU. The band keeps its $0 floor; the copy may not leave
    // that floor looking reachable by a descent.
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const reason = screen.getByText(/Upper bound, not an estimate/).textContent ?? ''
    expect(reason).toContain('paused state')
    expect(reason).toContain('cannot bill less than 300s at 0.5 ACU')
    expect(reason).toContain('$0.005')
  })

  it('ties the per-descent floor to descent frequency', () => {
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const frequency = screen.getByText(/cost follows how often a workload parks/)
    expect(frequency.textContent).toContain('not how long it sat idle')
    expect(frequency.textContent).toContain('is an illustration, not a measurement')
    expect(screen.getByText(/At 20 descents\/day · \$0.00 – \$0.40/)).toBeTruthy()
  })

  it('never describes an AWS figure as invoice-verified', () => {
    const { container } = render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const text = container.textContent ?? ''
    expect(text).toContain('not invoice-verified')
    // Every mention must be the negated one: `ce:GetCostAndUsage` is denied, so
    // no AWS figure here has ever been checked against a bill.
    const mentions = text.match(/invoice[- ]verified/gi) ?? []
    const negated = text.match(/not invoice[- ]verified/gi) ?? []
    expect(mentions).toHaveLength(negated.length)
  })

  it('gives the RDS lane a whole billed day instead of a per-descent floor', () => {
    render(<DescentCostDisclosure session={sessionWith(RDS)} />)
    expect(screen.getByText('Never descends · no floor to pay')).toBeTruthy()
    expect(screen.getByText(/\$1.56 every day, idle or not/)).toBeTruthy()
  })

  it('renders nothing when the round has no descent cost', () => {
    const { container } = render(<DescentCostDisclosure session={sessionWith(null)} />)
    expect(container.innerHTML).toBe('')
  })
})

describe('house rules', () => {
  const app = readFileSync(resolve(process.cwd(), 'src/App.tsx'), 'utf8')
  const css = readFileSync(resolve(process.cwd(), 'src/styles.css'), 'utf8')

  it('keeps the cost disclosure in What It Cost and out of the presenter cue', () => {
    const room = app.slice(app.indexOf('export function CostRoom'))
    const body = room.slice(0, room.indexOf('\nfunction '))
    expect(body).toContain('<DescentCostDisclosure session={session} />')
    const cue = app.slice(app.indexOf('function RingsideTake'))
    expect(cue.slice(0, cue.indexOf('/** One lane'))).not.toContain('<DescentCostDisclosure')
  })

  it('passes the descent payload into the return-to-idle screen', () => {
    expect(app).toContain('descentCost={session.descent_cost}')
  })

  it('uses no purple in the new cost styles', () => {
    const block = css.slice(css.indexOf('.descent-cost-disclosure'))
    const scoped = block.slice(0, block.indexOf('.cost-receipt-disclosure'))
    expect(scoped).not.toMatch(/purple|violet|indigo|magenta|#[0-9a-f]*(8b|9[0-9a-f])[0-9a-f]*ff\b/i)
    const floorLine = css.slice(css.indexOf('.between-floor-cost'))
    expect(floorLine.slice(0, 400)).not.toMatch(/purple|violet|indigo|magenta/i)
  })
})
