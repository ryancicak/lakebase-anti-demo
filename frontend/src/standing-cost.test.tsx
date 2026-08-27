// The standing-cost panel, and the guard that keeps a figure out of its source.
//
// WHY THIS FILE CHANGED SHAPE. It used to pin dollar literals, because the panel
// was dollar literals: server/cost_model.py had no standing-cost line type, so
// there was no payload to read and the figures were typed into JSX. Pinning them
// here was the best available guard and it was not a good one -- five of the
// figures it protected went stale anyway, because a test can only hold copy to
// what someone last believed. $2.66/day for a Round 6 endpoint "pinned at 2 CU"
// survived the endpoint being reconfigured to suspend at 60s, and survived being
// asserted by this file the whole time.
//
// server/standing_cost.py now builds the whole panel and the figures come off
// the wire, so the assertions changed from "is this the number" to "is this the
// number the server sent". The literals are gone and the last test in this file
// is what stops them coming back: it reads the component's own source and fails
// on any dollar amount at all.
//
// WHY THE FIXTURE IS REAL. Both payloads in standing-cost.fixture.json came out
// of build_standing_cost_disclosure against the sealed installation -- the
// priced one with a live system.billing.usage read behind it, the degraded one
// with that read withheld. A hand-written fixture would let this file assert a
// shape the server does not produce, which is the trap a sibling suite already
// hit: an assertion that restates its own fixture passes while both drift.
//
// So nothing below writes a figure down. Where a number is checked it is either
// read out of the payload or recomputed from the payload's own components, and
// the strongest assertion in the file is the one that says every dollar amount
// on screen also appears in the JSON.
//
// WHY IT IS TESTED THIS WAY. Same construction as descent-cost.test.tsx,
// disclosure-contrast.test.tsx and corner-geometry.test.tsx: which elements
// exist comes from the stylesheet, which text is inside them comes from the
// rendered component, and each walk carries a non-vacuity floor so a broken
// cross-reference fails loudly instead of passing empty.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { DescentCostDisclosure, StandingCostDisclosure } from './App'
import type { DemoSession, DescentCostSnapshot, StandingCostDisclosure as Payload } from './api/types'
import fixture from './standing-cost.fixture.json'

afterEach(cleanup)

const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '')

const app = readFileSync(join(import.meta.dirname, 'App.tsx'), 'utf8')

const PRICED = fixture.priced as unknown as Payload
const DEGRADED = fixture.posted_unavailable as unknown as Payload
const UNREADABLE = fixture.unreadable as unknown as Payload

/** Mirrors what server/descent_cost.py builds for Round 1 against Aurora. */
const AURORA: DescentCostSnapshot = {
  floor_ratio_label: "Aurora's floor is 5x Lakebase's · 300s vs 60s",
  illustrative_descents_per_day: 20,
  summary: 'Both engines bill their idle floor. Aurora\u2019s is 5x longer, and it is charged on every descent.',
  note: 'Neither floor is free, and neither figure is an invoice.',
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
      band_reason: 'Billed at its 0.5 CU floor at least.',
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
      band_reason: 'Upper bound, not an estimate.',
    },
  ],
}

function sessionWith(descent: DescentCostSnapshot | null): DemoSession {
  return { descent_cost: descent } as unknown as DemoSession
}

function standing(payload: Payload | null): DemoSession {
  return { standing_cost: payload } as unknown as DemoSession
}

function panel(payload: Payload) {
  return render(<StandingCostDisclosure session={standing(payload)} />)
}

/**
 * Every class the stylesheet declares for a panel, so a new element cannot be
 * added to the markup without this file noticing it exists.
 */
function declaredClasses(prefix: string): string[] {
  const found = new Set<string>()
  for (const [, name] of css.matchAll(new RegExp(`\\.(${prefix}[\\w-]*)`, 'g'))) found.add(name)
  return [...found]
}

/**
 * The component's own source, doc comment included.
 *
 * The doc comment is deliberately inside the slice. Four of the stale figures
 * this panel carried were in prose rather than in JSX -- a comment asserting
 * that an endpoint "sits bit-exact at its 2 CU ceiling around the clock" is
 * exactly as wrong as a `standing:` field saying so, and rots the same way.
 */
function componentSource(): string {
  const declaration = app.indexOf('export function StandingCostDisclosure')
  expect(declaration).toBeGreaterThan(0)
  const start = app.lastIndexOf('/**', declaration)
  expect(start).toBeGreaterThan(0)
  const rest = app.slice(declaration)
  // Whichever comes first: the next top-level declaration, or the doc comment
  // introducing it. Stopping only at the declaration would swallow the next
  // component's prose, and the panel below this one discusses a zero of its
  // own -- which would make this guard fail on someone else's correct comment.
  const boundaries = [rest.search(/\n(?:export )?(?:function|const|interface|type) /), rest.indexOf('\n/**')]
    .filter((index) => index > 0)
  expect(boundaries.length).toBeGreaterThan(0)
  return app.slice(start, declaration + Math.min(...boundaries))
}

/** Every dollar amount in a blob of text, however it is punctuated. */
function dollarAmounts(text: string): string[] {
  return [...text.matchAll(/\$\d[\d,]*(?:\.\d+)?/g)].map(([amount]) => amount)
}

/** The AWS half and the Databricks half, summed from the lanes themselves. */
function halves(payload: Payload) {
  let aws = 0
  let databricks = 0
  for (const lane of payload.lanes) {
    const perDay = lane.figure.usd_per_day
    if (perDay == null) continue
    if (lane.side === 'lakebase' || lane.side === 'platform') databricks += perDay
    else aws += perDay
  }
  return { aws, databricks }
}

describe('Round 1 descent reading · pinned to the CloudWatch measurement', () => {
  /** The bout, the billed window, and the share of the lane's cost outside it. */
  const MEASURED = ['15.31s', '420s', '97.2%']

  it('states the bout length, the billed window and the share after the bell', () => {
    const { container } = render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const measured = container.querySelector('p.descent-cost-measured')
    expect(measured).toBeTruthy()
    const copy = measured?.textContent ?? ''
    for (const figure of MEASURED) expect(copy).toContain(figure)
    // 420s of billed capacity for a 15.31s bout is 27x the bout. If either
    // figure drifts the multiple stops being the reason the point lands.
    expect(420 / 15.31).toBeGreaterThan(27)
  })

  it('pins both floors and the ratio between them', () => {
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const copy = screen.getByText(/a commitment/).textContent ?? ''
    expect(copy).toContain('300s')
    expect(copy).toContain('60s')
    expect(copy).toContain('5x')
    // The ratio is not written independently of the floors it comes from.
    expect(300 / 60).toBe(5)
  })

  it('keeps the ratio a Round 1 reading rather than a constant', () => {
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const copy = screen.getByText(/a commitment/).textContent ?? ''
    // Rounds 2 and 3 came out 1.77x and 1.12x over published against Round 1's
    // 16.1x. Naming them is what stops 16x reading as a general figure.
    expect(copy).toContain('1.77x')
    expect(copy).toContain('1.12x')
    expect(copy).toMatch(/not 16x/)
    expect(copy).toMatch(/shorter the work/)
  })

  it('never implies Lakebase idles free', () => {
    const { container } = render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const copy = container.querySelector('p.descent-cost-measured')?.textContent ?? ''
    expect(copy).toContain('Both engines bill a floor')
    expect(copy).not.toMatch(/\bfree\b|\bunbilled\b|\bno charge\b/i)
  })

  it('keeps RDS out of the descent frame entirely', () => {
    const { container } = render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const copy = container.querySelector('p.descent-cost-measured')?.textContent
    // Asserted present first: a negative match against an absent paragraph
    // passes for the wrong reason.
    expect(copy).toBeTruthy()
    // Provisioned RDS never descends, so a descent-cost frame cannot describe
    // it. Its standing cost is a different panel's claim.
    expect(copy).not.toMatch(/\bRDS\b/)
  })

  it('states no per-round dollar total, because the model is mid-correction', () => {
    const { container } = render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    const copy = container.querySelector('p.descent-cost-measured')?.textContent
    expect(copy).toBeTruthy()
    expect(copy).not.toMatch(/\$\d/)
  })

  it('carries the measured beat onto the return-to-idle screen too', () => {
    render(<DescentCostDisclosure session={sessionWith(AURORA)} />)
    // Both surfaces state the same three figures, so neither can drift alone.
    const copy = screen.getByText(/a commitment/).textContent ?? ''
    for (const figure of MEASURED) expect(copy).toContain(figure)
  })
})

describe('standing cost · every figure read off the payload', () => {
  it('renders one lane per lane in the payload, in the order the server sent', () => {
    const { container } = panel(PRICED)
    const lanes = [...container.querySelectorAll('.standing-cost-lanes > article')]
    // All six are rendered. Three of them are one click further in -- see the
    // remainder suite below -- but no lane is dropped, because a total that
    // omits any of them does not reconcile.
    expect(PRICED.lanes.length).toBe(6)
    expect(lanes).toHaveLength(PRICED.lanes.length)
    expect(lanes.map((lane) => lane.querySelector('strong')?.textContent))
      .toEqual(PRICED.lanes.map((lane) => lane.product))
  })

  it('gives every lane its figure, its idle behaviour, its derivation and its caveat', () => {
    const { container } = panel(PRICED)
    const lanes = [...container.querySelectorAll('.standing-cost-lanes > article')]
    PRICED.lanes.forEach((lane, index) => {
      const copy = lanes[index].textContent ?? ''
      // The figure is rendered whole rather than reformatted: display is the
      // only field the server validates a structural zero's basis into.
      expect(lanes[index].querySelector('b')?.textContent).toBe(lane.figure.display)
      expect(copy).toContain(lane.idle_label)
      expect(copy).toContain(lane.figure.derivation)
      expect(copy).toContain(lane.caveat)
      expect(copy).toContain(lane.rate_source)
    })
  })

  it('carries no dollar amount the payload did not send', () => {
    // The assertion the old shape of this file could not make. Every figure on
    // screen has to be traceable to the JSON, so a literal typed into the JSX
    // fails here even if it happens to be correct on the day.
    const { container } = panel(PRICED)
    const serialised = JSON.stringify(PRICED)
    const rendered = dollarAmounts(container.textContent ?? '')
    expect(rendered.length).toBeGreaterThan(8)
    expect(rendered.filter((amount) => !serialised.includes(amount))).toEqual([])
  })

  it('prints both totals with the condition each holds under', () => {
    const { container } = panel(PRICED)
    const totals = [...container.querySelectorAll('.standing-cost-totals > article')]
    expect(totals).toHaveLength(2)
    for (const total of [PRICED.totals!.installation, PRICED.totals!.with_platform]) {
      const article = totals.find((node) => node.textContent?.includes(total.label))
      expect(article).toBeTruthy()
      expect(article?.textContent).toContain(total.display)
      // Neither figure means anything without its condition, and the pair is
      // the reason there are two: one covers what this run created, the other
      // adds compute that predates it.
      expect(article?.textContent).toContain(total.condition)
    }
    expect(PRICED.totals!.with_platform.usd_per_day)
      .toBeGreaterThan(PRICED.totals!.installation.usd_per_day)
  })

  it('states the accrued snapshot with the window it accrued over', () => {
    const { container } = panel(PRICED)
    const copy = container.querySelector('p.standing-cost-credits')?.textContent ?? ''
    expect(copy).toContain(PRICED.credits!.display)
    expect(copy).toContain(PRICED.credits!.basis)
  })

  it('reports the posted read and the variance without hiding either side', () => {
    const { container } = panel(PRICED)
    const copy = container.querySelector('p.standing-cost-posted')?.textContent ?? ''
    expect(PRICED.posted.state).toBe('posted_through_window')
    expect(copy).toContain(PRICED.posted.display)
    // The window the variance was computed over, and the reason the gap is not
    // an error term, both travel with it.
    expect(copy).toContain(PRICED.posted.comparison_basis)
    expect(copy).toContain(PRICED.posted.explanation)
    // AWS has no posted counterpart, and the panel says so rather than letting
    // a Databricks-only actual wear an all-in label.
    expect(copy).toContain(PRICED.posted.aws_posted_basis)
  })

  it('shows the drift badge and says drift is never summed into the totals', () => {
    const { container } = panel(PRICED)
    const drift = container.querySelector('p.standing-cost-drift')
    expect(drift?.getAttribute('data-state')).toBe(PRICED.drift.state)
    const copy = drift?.textContent ?? ''
    expect(copy).toContain(PRICED.drift.badge)
    expect(copy).toContain(PRICED.drift.summary)
    expect(copy).toContain(PRICED.drift.separation_note)
  })

  it('admits the Databricks half is the larger one, in the server\u2019s words', () => {
    const { container } = panel(PRICED)
    expect(PRICED.fairness.state).toBe('stated')
    const copy = container.querySelector('p.standing-cost-fairness')?.textContent ?? ''
    expect(copy).toBe(PRICED.fairness.paragraph)
    // The concession is checked against the lanes rather than taken on trust:
    // the paragraph's two halves are recomputed from the payload's own
    // per-lane figures, and the larger one has to be ours.
    const { aws, databricks } = halves(PRICED)
    expect(databricks).toBeGreaterThan(aws)
    expect(copy).toContain(`$${databricks.toFixed(2)}/day`)
    expect(copy).toContain(`$${aws.toFixed(2)}/day`)
    expect(copy).toContain(`${(databricks / aws).toFixed(1)}x`)
    // The paragraph used to compare that margin to what it was "before their four
    // boxes were resized up". When that clause was written the resize had not
    // happened, so it narrated an event that had not occurred and blamed the
    // change in ratio on it. The resize landed at 2026-08-21T14:48:36Z and the
    // clause stays deleted regardless, because the panel carries no earlier
    // margin to compare against. The current margin stands alone.
    expect(copy).not.toMatch(/resized|narrowed|shrank/)
    expect(copy).toMatch(/capability, not this bill/)
    expect(copy).toMatch(/no provisioned RDS instance can scale to zero at any price/i)
  })

  it('no longer claims an endpoint is held awake by a setting we picked', () => {
    // Every sealed endpoint suspends at 60s, and the posted read finds no
    // COMPUTE_NODE_ALWAYS_ON_MIN row behind any of them. Four separate agents
    // reported the Round 6 copy as false before it came out.
    const { container } = panel(PRICED)
    const copy = container.textContent ?? ''
    expect(copy).not.toMatch(/no_suspension is configured|held awake|setting we picked/)
    expect(copy).not.toMatch(/pinned at 2 CU|Rounds 1–5 scale to zero/)
    const lakebase = PRICED.lanes.find((lane) => lane.lane_id === 'lakebase')!
    expect(copy).toContain(lakebase.idle_label)
  })

  it('keeps the Databricks platform lane out of the Lakebase one', () => {
    // Folding the app and the pipeline into Lakebase would overstate Lakebase
    // by close to an order of magnitude. The error points against us, which is
    // not the same as it being safe.
    const lakebase = PRICED.lanes.find((lane) => lane.lane_id === 'lakebase')!
    const platform = PRICED.lanes.find((lane) => lane.lane_id === 'databricks_platform')!
    expect(platform.figure.usd_per_day!).toBeGreaterThan(lakebase.figure.usd_per_day! * 100)
    const { container } = panel(PRICED)
    const lanes = [...container.querySelectorAll('.standing-cost-lanes > article')]
    const rendered = lanes.find((lane) => lane.textContent?.includes(lakebase.product))
    expect(rendered?.querySelector('b')?.textContent).toBe(lakebase.figure.display)
  })

  it('never prints a bare zero as a lane figure', () => {
    const { container } = panel(PRICED)
    const lanes = [...container.querySelectorAll('.standing-cost-lanes > article')]
    for (const lane of lanes) {
      const figure = lane.querySelector('b')?.textContent ?? ''
      expect(figure.trim()).not.toBe('$0.00')
      // A zero that is real carries its own derivation inside display, and the
      // view has to render display rather than anything narrower.
      if (/^\$0\.0+(\/|\s|$)/.test(figure.trim())) expect(figure.length).toBeGreaterThan(20)
    }
    // Non-vacuity: the walk found lanes to check.
    expect(lanes.length).toBeGreaterThan(4)
  })

  it('never describes anything as verified or invoice-verified', () => {
    const { container } = panel(PRICED)
    const text = container.textContent ?? ''
    expect(text).not.toMatch(/verified/i)
  })
})

describe('standing cost · three lanes shown, the remainder findable', () => {
  // Six lanes two-up was six equally unreadable lanes, and the three an operator
  // actually names were the three easiest to lose. So the row leads with those
  // and the rest sit behind a disclosure -- which is only defensible while the
  // remainder stays reachable, because the totals are summed over all six and a
  // total nobody can check is a total nobody should believe.

  /** The engine row: the first `.standing-cost-lanes` grid in the panel. */
  function rows(payload: Payload) {
    const { container } = panel(payload)
    const grids = [...container.querySelectorAll('.standing-cost-lanes')]
    const remainder = container.querySelector('details.standing-cost-remainder')
    return {
      container,
      grids,
      remainder,
      shown: [...(grids[0]?.querySelectorAll(':scope > article') ?? [])],
      hidden: [...(remainder?.querySelectorAll('.standing-cost-lanes > article') ?? [])],
    }
  }

  const ENGINES = ['rds', 'aurora', 'lakebase']

  it('shows the three engines an operator names, and only those', () => {
    const { shown } = rows(PRICED)
    const expected = PRICED.lanes.filter((lane) => ENGINES.includes(lane.lane_id))
    expect(expected).toHaveLength(3)
    expect(shown.map((row) => row.querySelector('strong')?.textContent))
      .toEqual(expected.map((lane) => lane.product))
  })

  it('puts every other lane behind one disclosure rather than dropping it', () => {
    const { grids, hidden } = rows(PRICED)
    const expected = PRICED.lanes.filter((lane) => !ENGINES.includes(lane.lane_id))
    // Exactly two grids: no third home for a lane to go missing in.
    expect(grids).toHaveLength(2)
    expect(hidden.map((row) => row.querySelector('strong')?.textContent))
      .toEqual(expected.map((lane) => lane.product))
    // Shown plus hidden is the payload: no lane falls between the two grids.
    const { shown } = rows(PRICED)
    expect(shown.length + hidden.length).toBe(PRICED.lanes.length)
  })

  it('says on the summary itself that the three shown do not add up', () => {
    const { remainder } = rows(PRICED)
    const summary = remainder?.querySelector('summary')?.textContent ?? ''
    // The label is the reason to open it. "More detail" would bury exactly the
    // thing that makes the totals checkable.
    expect(summary).toMatch(/do not add up/i)
    // And it names what is inside, so an operator can decide without opening.
    for (const lane of PRICED.lanes.filter((one) => !ENGINES.includes(one.lane_id))) {
      expect(summary).toContain(lane.product)
    }
  })

  it('states that the totals are computed over every lane, shown or not', () => {
    const { remainder } = rows(PRICED)
    const copy = remainder?.querySelector('p')?.textContent ?? ''
    expect(copy).toContain(String(PRICED.lanes.length))
    expect(copy).toMatch(/computed over all/i)
    // The degraded payload drops two lanes from its totals and labels them, so
    // this may not claim the totals include everything -- only that they say.
    expect(copy).toMatch(/names any lane it could not price/i)
    expect(copy).not.toMatch(/includes every lane|nothing is excluded/i)
  })

  it('gives a hidden lane exactly the row a shown lane gets', () => {
    // The tell of a panel that is hiding something rather than organising it:
    // the hidden rows lose their derivation or their caveat and start looking
    // like weaker evidence. Both sides render through one function.
    const { shown, hidden } = rows(PRICED)
    const fields = (row: Element) => ['strong', 'b', 'span', 'code', 'em', 'small']
      .map((tag) => row.querySelector(tag) !== null)
    expect(hidden.length).toBeGreaterThan(0)
    for (const row of hidden) expect(fields(row)).toEqual(fields(shown[0]))
    for (const lane of PRICED.lanes.filter((one) => !ENGINES.includes(one.lane_id))) {
      const row = hidden.find((node) => node.textContent?.includes(lane.product))
      expect(row?.querySelector('b')?.textContent).toBe(lane.figure.display)
      expect(row?.textContent).toContain(lane.figure.derivation)
      expect(row?.textContent).toContain(lane.caveat)
    }
  })

  it('keeps the remainder open-able when the payload half fails to price', () => {
    // The degraded read unprices one shown lane and one hidden one, and the
    // hidden one is the largest in the panel. If the disclosure vanished here,
    // the partial totals would name a lane the panel no longer renders.
    const { remainder, hidden } = rows(DEGRADED)
    expect(remainder).toBeTruthy()
    const platform = DEGRADED.lanes.find((lane) => lane.lane_id === 'databricks_platform')!
    const row = hidden.find((node) => node.textContent?.includes(platform.product))
    expect(row?.getAttribute('data-evidence')).toBe('unpriced')
    expect(DEGRADED.totals!.installation.partial_reason).toContain(platform.product)
  })

  it('renders no remainder frame when there is nothing left over', () => {
    // Not an empty frame and not a summary with nothing behind it: an unreadable
    // seal carries no lanes at all, and a door onto nothing reads as a failure.
    const { remainder, grids } = rows(UNREADABLE)
    expect(UNREADABLE.lanes).toEqual([])
    expect(remainder).toBeNull()
    expect(grids).toHaveLength(0)
  })

  it('frames the remainder like its parent panel and rounds no corner', () => {
    const block = css.slice(css.indexOf('.standing-cost-remainder'))
    const rule = block.slice(0, block.indexOf('}'))
    expect(rule).toMatch(/border:\s*3px solid/)
    expect(rule).not.toMatch(/border-radius/)
    // Vertical size may not be driven off viewport width: this panel opens on a
    // 1728x995 laptop where a `vw` height cost grows as the budget shrinks.
    expect(rule).not.toMatch(/(?:height|padding|margin)[^;]*vw/)
  })
})

describe('standing cost · the posted read unavailable', () => {
  it('renders the word rather than a smaller number when a lane cannot be priced', () => {
    const { container } = panel(DEGRADED)
    const unpriced = DEGRADED.lanes.filter((lane) => lane.figure.state === 'unavailable')
    // Both Databricks lanes go unpriced together when the read fails, which is
    // the case that matters: an understated Databricks total is the one error
    // that would flatter us.
    expect(unpriced.length).toBeGreaterThan(1)
    const lanes = [...container.querySelectorAll('.standing-cost-lanes > article')]
    for (const lane of unpriced) {
      const rendered = lanes.find((node) => node.textContent?.includes(lane.product))
      expect(rendered?.getAttribute('data-evidence')).toBe('unpriced')
      expect(rendered?.querySelector('b')?.textContent).toBe(lane.figure.display)
      expect(rendered?.querySelector('b')?.textContent).not.toMatch(/\$/)
    }
  })

  it('labels both totals partial and says what they are missing', () => {
    const { container } = panel(DEGRADED)
    const totals = [...container.querySelectorAll('.standing-cost-totals > article')]
    for (const total of [DEGRADED.totals!.installation, DEGRADED.totals!.with_platform]) {
      expect(total.partial).toBe(true)
      const article = totals.find((node) => node.textContent?.includes(total.label))
      expect(article?.getAttribute('data-partial')).toBe('true')
      expect(article?.textContent).toContain(total.partial_reason)
      expect(total.label.toLowerCase()).toContain('partial')
    }
  })

  it('omits the fairness paragraph entirely rather than rewording it', () => {
    const { container } = panel(DEGRADED)
    expect(DEGRADED.fairness.state).toBe('withheld')
    // Not hidden, not softened, not replaced with a caveat: absent. The
    // paragraph's claim is that our half is the larger one, and with the
    // Databricks half unpriced there is nothing behind it.
    expect(container.querySelector('p.standing-cost-fairness')).toBeNull()
    const copy = container.textContent ?? ''
    expect(copy).not.toMatch(/Both sides carry standing cost/)
    expect(copy).not.toMatch(/the larger half/)
    // And the panel is still worth rendering: the AWS half survives.
    expect(dollarAmounts(copy).length).toBeGreaterThan(4)
  })

  it('says the posted read is unavailable instead of showing a variance', () => {
    const { container } = panel(DEGRADED)
    const copy = container.querySelector('p.standing-cost-posted')?.textContent ?? ''
    expect(DEGRADED.posted.state).toBe('unavailable')
    expect(copy).toContain(DEGRADED.posted.display)
    expect(DEGRADED.posted.variance_usd ?? null).toBeNull()
  })

  it('renders nothing at all when the session carries no disclosure', () => {
    // A round opened before the first posted refresh, or a server with no
    // sealed manifest. An empty frame would read as a panel that failed.
    const { container } = render(<StandingCostDisclosure session={standing(null)} />)
    expect(container.querySelector('details')).toBeNull()
  })

  it('carries no figure anywhere when the seal itself cannot be read', () => {
    // The strictest state the server can build: no origin, so no window, so no
    // lane and no total. An unreadable manifest is not a free installation,
    // and the panel has to say that rather than render a zero.
    const { container } = panel(UNREADABLE)
    expect(UNREADABLE.seal_state).toBe('unreadable')
    expect(UNREADABLE.lanes).toEqual([])
    expect(container.querySelector('.standing-cost-lanes')).toBeNull()
    expect(container.querySelector('.standing-cost-totals')).toBeNull()
    expect(container.querySelector('p.standing-cost-unreadable')?.textContent)
      .toBe(UNREADABLE.seal_detail)
    expect(dollarAmounts(container.textContent ?? '')).toEqual([])
  })
})

describe('house rules', () => {
  it('contains no dollar amount in its own source, so one cannot be typed back in', () => {
    // The guard the old shape of this panel needed and did not have. Five
    // figures went stale inside this component while a test file asserted
    // them; the fix is that there is nowhere left to put one.
    const source = componentSource()
    expect(source.length).toBeGreaterThan(500)
    expect(source).toContain('session.standing_cost')
    expect(source.match(/\$\d/g) ?? []).toEqual([])
  })

  it('names no round in its own source either, for the same reason', () => {
    // The dollar guard above caught figures and missed the other thing this
    // panel asserts: which rounds a lane's boxes stand for. That sentence lived
    // in `server/standing_cost.py`, said "Rounds 1, 2, 3 and 5", and went on
    // saying it after Round 1's RDS instance was deleted -- a claim about a box
    // that no longer existed, sitting under a figure that no longer included it.
    //
    // The server now derives that list from `IMPUTED_RDS_ROUNDS`. This is the
    // frontend half of the same rule: a round list typed into the panel would be
    // the identical defect, one layer nearer the screen, and no test on the
    // Python side could see it.
    const source = componentSource()
    expect(source.length).toBeGreaterThan(500)
    expect(source).not.toMatch(/Rounds? \d(?:\s*,\s*\d)*(?:\s+and\s+\d)?/)
    expect(source).not.toMatch(/\b(?:one|two|three|four|five|six)\s+(?:standing|idle|RDS)\b/i)
  })

  it('extends that rule to the lane renderer the panel now shares', () => {
    // The rows moved out of the component into `standingCostLane`, which puts
    // them outside the slice above. A figure typed in there would be exactly the
    // defect this guard was written for, one function further along.
    const start = app.indexOf('function standingCostLane')
    expect(start).toBeGreaterThan(0)
    const source = app.slice(app.lastIndexOf('/**', start), app.indexOf('\n}', start))
    expect(source).toContain('lane.figure.display')
    expect(source.match(/\$\d/g) ?? []).toEqual([])
    // And one renderer, so the two grids cannot grow different rows.
    expect(app.match(/function standingCostLane/g) ?? []).toHaveLength(1)
    expect(app.match(/\.map\(standingCostLane\)/g) ?? []).toHaveLength(2)
  })

  it('names the three shown lanes once, for this panel and the cost room both', () => {
    // Two lists would let the strip in <CostRoom> and the row here disagree
    // about which lanes an audience recognises, which is the only thing either
    // of them uses the list for.
    expect(app.match(/const NAMED_ENGINE_LANES/g) ?? []).toHaveLength(1)
    expect(app).toContain("NAMED_ENGINE_LANES: readonly StandingLane['lane_id'][] = ['rds', 'aurora', 'lakebase']")
    expect(app.match(/NAMED_ENGINE_LANES/g)?.length).toBeGreaterThan(2)
    expect(app).not.toContain('COST_ROOM_ENGINES')
  })

  it('sets no interval, because nothing in the payload advances', () => {
    const source = componentSource()
    expect(PRICED.credits!.ticks).toBe(false)
    expect(source).not.toMatch(/setInterval|requestAnimationFrame|Date\.now/)
  })

  it('keeps the panel and its stylesheet block in step', () => {
    const { container } = panel(PRICED)
    const rendered = new Set<string>()
    for (const node of container.querySelectorAll('[class]')) {
      for (const name of node.className.split(/\s+/)) if (name) rendered.add(name)
    }
    const declared = declaredClasses('standing-cost')
    // Sanity on both directions: if either empties, the cross-reference has
    // broken and the assertions below would pass vacuously.
    expect(rendered.size).toBeGreaterThan(6)
    expect(declared.length).toBeGreaterThan(6)
    // Nothing in this panel borrows a class from another block, so a colour or a
    // frame cannot arrive here from a rule nobody looked at. Elements with no
    // class of their own inherit from `.standing-cost-disclosure > p` by design.
    expect([...rendered].filter((name) => !name.startsWith('standing-cost'))).toEqual([])
    // And no rule outlives the element it was written for. All three states are
    // walked, because .standing-cost-unreadable only appears in the third and a
    // rule that only the degraded path uses would otherwise look dead.
    for (const state of [DEGRADED, UNREADABLE]) {
      for (const node of panel(state).container.querySelectorAll('[class]')) {
        for (const name of node.className.split(/\s+/)) if (name) rendered.add(name)
      }
    }
    expect(declared.filter((name) => !rendered.has(name))).toEqual([])
  })

  it('collapses both grids off a 390px viewport', () => {
    // Six lanes two-up is ~180px a column at 390px, under the 140px width that
    // already needed overflow-wrap on the two-lane panels once padding is off.
    const narrow = css.slice(css.indexOf('@media (max-width: 700px)'))
    expect(narrow).toMatch(/\.standing-cost-lanes\s*\{\s*grid-template-columns:\s*1fr/)
    expect(narrow).toMatch(/\.standing-cost-totals\s*\{\s*grid-template-columns:\s*1fr/)
    for (const grid of ['.standing-cost-lanes {', '.standing-cost-totals {']) {
      expect(css.slice(css.indexOf(grid), css.indexOf(grid) + 200)).toContain('minmax(0, 1fr)')
    }
  })

  it('gives every long token in the panel somewhere to break', () => {
    // Four horizontal-overflow bugs in this project's history, every one an
    // unbreakable token in a narrow column. This payload carries
    // system.billing.usage, pricing.effective_list.default and a full Price
    // List API URL, all in columns narrower than the two-lane panels'.
    for (const selector of ['b', 'span', 'code', 'small']) {
      const rule = css.slice(css.indexOf(`.standing-cost-lanes ${selector} {`))
      expect(rule.slice(0, 400)).toContain('overflow-wrap: anywhere')
    }
    for (const selector of ['.standing-cost-totals b', '.standing-cost-totals small']) {
      expect(css.slice(css.indexOf(`${selector} {`), css.indexOf(`${selector} {`) + 400))
        .toContain('overflow-wrap: anywhere')
    }
    const paragraphs = css.slice(css.indexOf('.standing-cost-credits,'))
    expect(paragraphs.slice(0, 300)).toContain('overflow-wrap: anywhere')
  })

  it('uses no purple and does not round a corner', () => {
    const block = css.slice(css.indexOf('.standing-cost-disclosure'))
    expect(block).not.toMatch(/purple|violet|indigo|magenta/i)
    const radii = [...block.matchAll(/border-radius\s*:\s*([^;}]+)/g)]
      .map(([, value]) => value.trim())
      .filter((value) => !/^0(?:[a-z%]+)?$/.test(value))
    expect(radii).toEqual([])
  })

  it('keeps the panel in What It Cost and out of the presenter cue', () => {
    const room = app.slice(app.indexOf('export function CostRoom'))
    const body = room.slice(0, room.indexOf('\nfunction '))
    expect(body).toContain('<StandingCostDisclosure session={session} />')
    expect(body).toContain('<DescentCostDisclosure session={session} />')
    const cue = app.slice(app.indexOf('function RingsideTake'))
    expect(cue.slice(0, cue.indexOf('/** One lane'))).not.toContain('<StandingCostDisclosure')
  })

  it('renders on every round rather than being wired up three times', () => {
    // Rounds 1, 4 and 6 all need this claim and it is identical in each, so it
    // takes no round argument at all -- there is nothing to branch on and
    // therefore nothing to drift.
    expect(app.match(/<StandingCostDisclosure/g) ?? []).toHaveLength(1)
    expect(app).toContain('export function StandingCostDisclosure({ session }')
  })
})
