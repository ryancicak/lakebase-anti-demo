// Prose inside the click-through disclosures must be readable on the navy.
//
// WHY THIS FILE EXISTS. .capacity-disclosure and .cost-receipt-disclosure were
// written against a light surface and then hung inside .ringside-take, which is
// navy (#10183e) with cream text. Seven declarations across the two panels came
// along for the ride: #49616b prose in both, #62747b labels and line metadata,
// #334d58 scope headings, and #0c232d on the reconciliation figures -- which
// measured 1.06:1, present in the DOM and invisible on screen. The user asked
// for capacities behind a click, so an unreadable disclosure is a broken
// feature, not an imperfect one.
//
// It had already propagated once by copy-paste before anyone noticed, and the
// third panel (.descent-cost-disclosure) only escaped because the agent
// building it caught the problem in its own screenshot. That is the regression
// this file exists to stop: the next panel gets pasted from one of these three
// and nobody screenshots it.
//
// The fourth panel (.standing-cost-disclosure) is the case this comment
// predicted, and it is in the walk below rather than beside it. It was pasted
// from the third, so it inherits every colour these assertions already cover --
// which is exactly why leaving it out would have been the whole bug again.
//
// The fifth panel (.bout-cost-disclosure) is in the walk for the same reason,
// and it did catch one: its source line sits on the navy rather than inside a
// row that paints its own darker fill, and --steel measures 4.02:1 there
// against the 4.67:1 it manages on the fill. The sibling panels' `small`
// elements are all inside a row, so copying their colour up one level would
// have shipped a failing ratio that looked like a precedent.
//
// WHY IT IS TESTED THIS WAY. jsdom does no layout and applies no stylesheet, so
// getComputedStyle here returns nothing useful and a rendered-colour assertion
// would pass against a broken stylesheet. What can be checked is the invariant:
// resolve the declared colours out of the stylesheet text and do the WCAG
// arithmetic directly.
//
// The two sources of truth are cross-referenced rather than restated. Which
// colours exist comes from styles.css; what counts as "inside a disclosure"
// comes from the rendered components. So neither a new rule in the stylesheet
// nor a new block in App.tsx can slip past by being absent from a list written
// here -- the same reason credits-layout.test.tsx is built this way.
//
// Comments are stripped before anything is matched. The stylesheet documents
// this very fix, so its prose contains #49616b and #0c232d verbatim; matching
// raw text would let the documentation fail the assertions it describes.
//
// Real pixel measurement is a browser's job and lives outside the unit suite.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { cleanup, render } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import {
  BoutCostDisclosure,
  CapacityDisclosure,
  CostReceiptDisclosure,
  DescentCostDisclosure,
  StandingCostDisclosure,
} from './App'
import type { DemoSession } from './api/types'
import standingCost from './standing-cost.fixture.json'

const css = readFileSync(join(import.meta.dirname, 'styles.css'), 'utf8')
  .replace(/\/\*[\s\S]*?\*\//g, '')

/** Flat `selector { body }` pairs. Rules nested in @media fall out on their own. */
const rules = [...css.matchAll(/([^{}]+)\{([^{}]*)\}/g)].map(([, selector, body]) => ({
  selectors: selector
    .split(',')
    .map((one: string) => one.trim())
    .filter((one: string) => one.length > 0 && !one.startsWith('@')),
  body,
}))

/** `--red: #ee452d` and friends, so `var(--cream)` can be resolved to a colour. */
const palette = new Map<string, string>(
  [...css.matchAll(/(--[\w-]+)\s*:\s*(#[0-9a-fA-F]{3,8})\s*[;}]/g)].map(([, name, value]) => [name, value]),
)

/**
 * .ringside-take's own background: the surface a disclosure sits on when
 * nothing between it and the overlay paints one of its own.
 *
 * Nested boxes do paint their own -- the capacity articles are --navy, the
 * descent articles #050817 -- and both are DARKER than this, which is why the
 * surface is resolved per element by walking up the tree rather than assumed.
 * Assuming this value flagged .capacity-lanes em and .descent-cost-lanes small,
 * both of which are comfortably legible on the article they actually sit in.
 */
const RINGSIDE_TAKE = '#10183e'

function channels(colour: string): [number, number, number] | null {
  let hex = colour.trim()
  const varied = /^var\(\s*(--[\w-]+)/.exec(hex)
  if (varied) hex = palette.get(varied[1]) ?? ''
  if (!/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/.test(hex)) return null
  const raw = hex.slice(1)
  const full = raw.length === 3 ? [...raw].map((c) => c + c).join('') : raw
  return [0, 2, 4].map((i) => parseInt(full.slice(i, i + 2), 16)) as [number, number, number]
}

function luminance(rgb: [number, number, number]) {
  const [r, g, b] = rgb.map((v) => {
    const c = v / 255
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
  })
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}

function contrast(foreground: string, background: string) {
  const fg = channels(foreground)
  const bg = channels(background)
  if (!fg || !bg) return null
  const [hi, lo] = [luminance(fg), luminance(bg)].sort((a, b) => b - a)
  return (hi + 0.05) / (lo + 0.05)
}

const CAPACITY = {
  matched: true,
  summary: 'Lakebase 0.5–2 CU (~1–4 GB) · Aurora Serverless v2 0–2 ACU (~0–4 GiB)',
  note: 'Both sides are hand-set to the same memory ceiling, single node, no HA on either side.',
  lanes: [
    {
      lane_id: 'lakebase',
      product: 'Lakebase',
      configured: '0.5–2 CU',
      memory: '~1–4 GB',
      engine_version: 'PostgreSQL 17 (major only)',
      idle_policy: 'Scale to zero after 60s (vendor minimum)',
      basis: 'configured',
      max_connections: 443,
    },
    {
      lane_id: 'competitor',
      product: 'Aurora Serverless v2',
      configured: '0–2 ACU',
      memory: '~0–4 GiB',
      engine_version: 'PostgreSQL 17.10',
      idle_policy: 'Auto-pause after 300s (AWS documented minimum)',
      basis: 'observed',
      max_connections: 450,
    },
  ],
}

const DESCENT = {
  floor_ratio_label: 'Aurora floor 5x Lakebase',
  summary: 'One return to idle costs more on the opponent than it does on Lakebase.',
  note: 'Both floors are published on-demand rates, not negotiated ones.',
  illustrative_descents_per_day: 12,
  lanes: [
    {
      lane_id: 'lakebase',
      product: 'Lakebase',
      per_descent_display: '$0.0002',
      per_day_display: '$0.0024',
      floor_label: '60s vendor minimum',
      derivation: '60s / 3600 x 0.5 CU x $0.026/CU-h',
      band_reason: 'CU floor is a range, so the figure is a bound.',
      rate_source: 'Databricks list prices',
      descends: true,
    },
    {
      lane_id: 'competitor',
      product: 'Aurora Serverless v2',
      per_descent_display: '$0.0010',
      per_day_display: '$0.0120',
      floor_label: '300s AWS documented minimum',
      derivation: '300s / 3600 x 0.5 ACU x $0.12/ACU-h',
      band_reason: 'AWS will not accept a timeout below 300s.',
      rate_source: 'AWS published pricing',
      descends: true,
    },
  ],
}

const RECEIPT = {
  currency: 'USD',
  region: 'us-west-2',
  price_basis: 'published_on_demand_rates',
  status: 'posted_partial',
  reconciliation_status: 'posted_partial',
  known_bout_estimate_usd: 0.005,
  known_monthly_carrying_cost_usd: 2.7,
  known_installation_overhead_usd: null,
  original_estimate_usd: 0.004,
  posted_cost_usd: 0.005,
  variance_usd: 0.001,
  revision: 2,
  queried_at: '2026-08-20T02:00:00Z',
  posted_through: '2026-08-20T01:45:00Z',
  lines: [
    {
      lane_id: 'lakebase',
      component: 'Lakebase compute',
      quantity: null,
      unit: 'DBU',
      unit_rate_usd: 0.26,
      reference_list_unit_rate_usd: 0.52,
      subtotal_usd: null,
      rate_basis: 'current_promotion',
      cadence: 'usage',
      status: 'usage_pending',
      scope: 'bout_estimate',
      source: 'system.billing.list_prices pricing.effective_list.default',
      source_as_of: '2026-08-20T01:35:04Z',
    },
    {
      lane_id: 'competitor',
      component: 'Secrets Manager API requests',
      quantity: null,
      unit: '10,000 requests',
      unit_rate_usd: 0.05,
      reference_list_unit_rate_usd: null,
      subtotal_usd: null,
      rate_basis: 'standard_list',
      cadence: 'usage',
      status: 'usage_pending',
      scope: 'installation_overhead',
      source: 'AWS Price List API · AWSSecretsManager · OnDemand · us-west-2',
      source_as_of: '2025-08-28T15:38:04Z',
    },
  ],
  note: 'Current Lakebase compute promotion has no published end date; revalidate before presenting.',
}

const BOUT = {
  total_display: '$0.130375 – $0.141727',
  superseded_display: '$0.049800 – $0.050340',
  dearest_claim: 'On the Aurora lane, Round 5 is the dearest single round at $0.055549.',
  lakebase_lane_claim: 'On the Lakebase lane, Round 5 is the cheapest round.',
  summary: 'Aurora\u2019s billed capacity, measured out of band rather than modelled.',
  scope_note: 'Aurora Serverless v2 compute, plus Round 5\u2019s RDS Proxy.',
  note: 'The quantity is measured; the rate is not invoice-verified.',
  rate_source: 'CloudWatch ServerlessDatabaseCapacity · rate-card derived, not invoice-verified',
  rounds: [
    {
      round_id: 'wake_idle_app',
      round_number: 1,
      label: 'Wake the idle app',
      provenance: 'measured',
      band_kind: 'single_bout',
      usd_display: '$0.015628',
      usd_low: 0.015628,
      usd_high: 0.015628,
      derivation: '468.85 ACU-s ÷ 3600 × $0.12/ACU-hour',
      band_reason: 'One bout. 97.2% of the cost landed after the bell.',
      bouts: ['7ECE1CB0'],
    },
    {
      round_id: 'put_model_score_in_app',
      round_number: 4,
      label: 'Lakehouse data into the app',
      provenance: 'structural_zero',
      band_kind: 'exact_zero',
      usd_display: '$0.00',
      usd_low: 0,
      usd_high: 0,
      derivation: 'infra/aws/locals.tf stands up no Aurora cluster for this round',
      band_reason: 'Exact, not unavailable. There is no cluster to wake.',
      bouts: [],
    },
  ],
}

const session = {
  capacity: CAPACITY,
  descent_cost: DESCENT,
  bout_cost: BOUT,
  cost_receipt: RECEIPT,
  // Real server output rather than a hand-written stand-in. The standing-cost
  // panel reads its whole body off this payload, so a fixture that omitted a
  // field would drop a text node this walk exists to measure -- the panel would
  // still render, and the colour on the missing line would go unchecked.
  standing_cost: standingCost.priced,
  corners: ['cost'],
} as unknown as DemoSession

/** All five disclosures, inside the navy surface RingsideTake puts them on. */
function overlay() {
  return render(
    <div className="ringside-take">
      <CapacityDisclosure session={session} />
      <DescentCostDisclosure session={session} />
      <BoutCostDisclosure session={session} />
        <StandingCostDisclosure session={session} />
        <CostReceiptDisclosure session={session} />
    </div>,
  )
}

afterEach(cleanup)

describe('disclosure prose on the navy surface', () => {
  it('reaches WCAG AA for every text node inside a disclosure', () => {
    const { container } = overlay()
    const panels = [...container.querySelectorAll('details')]
    // Six: the five panels this file renders, plus the remainder door nested in
    // the standing-cost one. Its prose sits on the same navy surface a click
    // further in, which changes nothing about what it has to be readable
    // against, so it is measured with the rest.
    expect(panels.length).toBe(6)

    /** Does `node` match `selector`? Unsupported selectors are simply skipped. */
    const matches = (node: Element, selector: string) => {
      try {
        return node.matches(selector)
      } catch {
        return false
      }
    }

    /**
     * Every rule body matching `node`, in source order, resolved once per node.
     *
     * The cache is not a micro-optimisation. `colourOf` and `surfaceOf` below
     * both walk from a node up to the overlay, so without it every ancestor is
     * re-matched against all ~900 selectors in styles.css once per descendant
     * per property -- upwards of a million `Element.matches` calls through
     * jsdom's selector engine for one assertion. That measured 1.2s on the
     * author's laptop and timed out at vitest's 5s default on a 4-vCPU CI
     * runner sharing itself with App.test.tsx, which is a guard that goes red
     * for a reason unrelated to contrast. Matching each node once is ~10x
     * cheaper and changes nothing about what is matched.
     */
    const matchedBodies = new Map<Element, string[]>()
    const bodiesFor = (node: Element) => {
      let bodies = matchedBodies.get(node)
      if (!bodies) {
        bodies = rules
          .filter((rule) => rule.selectors.some((selector) => matches(node, selector)))
          .map((rule) => rule.body)
        matchedBodies.set(node, bodies)
      }
      return bodies
    }

    /**
     * The last declaration of `property` that matches `node`.
     *
     * Source order stands in for the full cascade. These rules are single
     * classes and short descendant chains with no competing overrides, and the
     * resolved values were checked against a real browser's getComputedStyle
     * on the same markup before this was relied on.
     */
    const declaredOn = (node: Element, property: RegExp) => {
      let found: string | null = null
      for (const body of bodiesFor(node)) {
        const hit = property.exec(body)
        if (hit) found = hit[1].trim().split(/\s+/)[0]
      }
      return found
    }

    const COLOUR = /(?:^|[;{\s])color\s*:\s*([^;}]+)/
    const BACKGROUND = /(?:^|[;\s])background(?:-color)?\s*:\s*([^;}]+)/
    const FONT_SIZE = /(?:^|[;\s])font-size\s*:\s*(\d+(?:\.\d+)?)px/

    /** Nearest ancestor that paints an opaque colour, exactly as the eye finds it. */
    const surfaceOf = (node: Element) => {
      for (let el: Element | null = node; el; el = el.parentElement) {
        const painted = declaredOn(el, BACKGROUND)
        if (painted && channels(painted)) return painted
      }
      return RINGSIDE_TAKE
    }

    /** Inherited when the element declares nothing of its own. */
    const colourOf = (node: Element) => {
      for (let el: Element | null = node; el; el = el.parentElement) {
        const own = declaredOn(el, COLOUR)
        if (own && channels(own)) return own
      }
      // .ringside-take's own `color: var(--cream)` is the floor of the chain.
      return 'var(--cream)'
    }

    const failures: string[] = []
    let checked = 0

    for (const panel of panels) {
      for (const node of [panel, ...panel.querySelectorAll('*')]) {
        // Only elements that actually carry text of their own.
        const ownText = [...node.childNodes]
          .filter((child) => child.nodeType === 3)
          .map((child) => child.textContent ?? '')
          .join('')
          .trim()
        if (ownText.length === 0) continue

        const foreground = colourOf(node)
        const surface = surfaceOf(node)
        const ratio = contrast(foreground, surface)
        if (ratio === null) continue
        checked += 1

        const sized = declaredOn(node, FONT_SIZE)
        const threshold = sized && Number(sized) >= 24 ? 3 : 4.5
        if (ratio < threshold) {
          const where = node.className || node.tagName.toLowerCase()
          failures.push(
            `${where} "${ownText.slice(0, 40)}" ${foreground} on ${surface}`
            + ` = ${ratio.toFixed(2)}:1, needs ${threshold}:1`,
          )
        }
      }
    }

    // Sanity: if this ever empties, the cross-reference has broken and the
    // assertion below would pass vacuously. Five panels contribute over 100
    // text nodes between them, roughly evenly, so a floor of 80 also fails if
    // a whole panel stops being walked.
    expect(checked).toBeGreaterThan(80)
    expect(failures).toEqual([])
    // The timeout, not the assertions. Every element inside the six panels has
    // to be matched against all ~900 selectors in styles.css at least once --
    // 168 elements x 983 selectors through jsdom, ~340ms of the 450ms this
    // takes -- and there is no cheaper way to keep the stylesheet as the source
    // of truth short of reimplementing a selector engine here. That is a
    // synchronous CPU cost, so the wall clock it is measured against is the
    // runner's rather than this test's: 1.2s on the author's laptop before the
    // cache above, and a timeout at vitest's 5s default on a 4-vCPU
    // ubuntu-latest sharing itself with App.test.tsx's 83 tests. Raising it
    // weakens nothing -- every assertion above still runs and still fails on
    // any contrast regression -- and the alternative is a guard that goes red
    // for a reason that has nothing to do with contrast. If this ever runs
    // long enough to hit 30s, something is genuinely wrong and it should be
    // read as a real failure.
  }, 30_000)

  it('keeps the light-surface palette out of the stylesheet entirely', () => {
    // Belt and braces on the exact values the bug report named. The DOM
    // cross-reference above only reaches panels that are rendered; this reaches
    // a fourth panel pasted from one of these three before it is ever wired up.
    // None of these has a legitimate use here -- the dark text that genuinely
    // sits on the yellow and green fills uses #10152f, #081a10, #240905,
    // #241d03 and #080b1f, none of which appear below.
    for (const value of ['#49616b', '#62747b', '#334d58', '#0c232d', 'rgba(12, 35, 45']) {
      expect(css).not.toContain(value)
    }
  })

  it('still uses no purple anywhere in the disclosure styles', () => {
    const block = css.slice(css.indexOf('.capacity-disclosure'))
    expect(block).not.toMatch(/purple|violet|indigo|magenta/i)
  })
})

/**
 * The last declaration matching `selector` exactly, source order winning, in
 * the shape `declaredOn` above uses. Not `node.matches`, because these two
 * classes are asserted by name rather than by walking a rendered tree.
 */
const bodyOf = (selector: string) => {
  const hits = rules.filter((rule) => rule.selectors.includes(selector))
  return hits.length === 0 ? null : hits[hits.length - 1].body
}

// The fight card's status slot is a second contrast bug of the same shape, and
// it is guarded here because the arithmetic and the stylesheet reader already
// live in this file.
//
// WHY IT EXISTS. .ready-screen::after draws a ring band across the middle of the
// fight card -- a cream top border, then clamp(13px, 1.5vw, 29px) of var(--red)
// as an inset shadow, then #1c2b60, then a var(--blue) bottom border -- and the
// shared status slot lands inside it. Measured in Chromium on the real
// stylesheet, a two-line .retro-error sat on the #1c2b60 field at 6.46:1, and a
// three-line one -- the length a server message actually arrives in -- pushed
// its first line onto the red at 1.83:1 at 1024x634, 1440x900 and 1728x995
// alike. Which colour the text overlaps moves with BOTH the viewport and the
// length of the string, so no single foreground colour can be correct: the
// element has to bring its own ground. .bell-notice, the other occupant of the
// same slot, was built that way from the start; .retro-error was not, and it is
// the element that tells the presenter what went wrong.
//
// WHY IT IS TESTED THIS WAY, and why it is not a rendered-width assertion: as
// above, jsdom applies no stylesheet, so the invariant -- every occupant of this
// slot declares an opaque ground of its own and clears AA on it -- is resolved
// out of the stylesheet text and checked with the same WCAG arithmetic.
describe('the fight card status slot', () => {
  // Both occupants of the slot, which never render alongside each other. A
  // third one pasted from either is the regression this covers.
  const SLOT = ['.retro-error', '.bell-notice']

  it('gives every occupant an opaque ground of its own', () => {
    for (const selector of SLOT) {
      const body = bodyOf(selector)
      expect(body, `${selector} has no rule in styles.css`).not.toBeNull()
      const painted = /(?:^|[;{\s])background(?:-color)?\s*:\s*([^;}]+)/.exec(body!)
      expect(painted, `${selector} inherits its ground from the ring band`).not.toBeNull()
      expect(channels(painted![1].trim()), `${selector} ground is not an opaque colour`).not.toBeNull()
    }
  })

  it('reaches WCAG AA for every occupant on that ground', () => {
    for (const selector of SLOT) {
      const body = bodyOf(selector)!
      const colour = /(?:^|[;{\s])color\s*:\s*([^;}]+)/.exec(body)![1].trim()
      const ground = /(?:^|[;{\s])background(?:-color)?\s*:\s*([^;}]+)/.exec(body)![1].trim()
      const ratio = contrast(colour, ground)
      expect(ratio, `${selector} ${colour} on ${ground} is unresolvable`).not.toBeNull()
      expect(ratio!, `${selector} ${colour} on ${ground} = ${ratio?.toFixed(2)}:1`).toBeGreaterThanOrEqual(4.5)
    }
  })

  it('proves the ground is load-bearing and not decoration', () => {
    // If every colour in the band cleared AA, the declarations above would be
    // arbitrary and a future tidy-up would be right to drop them. At least one
    // band colour must actually fail each occupant, or this guard is guarding
    // nothing. var(--red) is the one the bug was reported on.
    const BAND = ['var(--red)', '#1c2b60', 'var(--cream)', 'var(--blue)']
    for (const selector of SLOT) {
      const colour = /(?:^|[;{\s])color\s*:\s*([^;}]+)/.exec(bodyOf(selector)!)![1].trim()
      const failing = BAND.filter((band) => (contrast(colour, band) ?? 21) < 4.5)
      expect(failing, `${selector} would be readable on the whole band unaided`).toContain('var(--red)')
    }
  })

  it('sizes the slot against its column and not against the viewport', () => {
    // Both occupants are laid out inside .retro-screen's own inset frame, so a
    // vw width and the column disagree -- the mistake .between-policy-floor and
    // the notice's own App.test.tsx guard both already record.
    for (const selector of SLOT) {
      const body = bodyOf(selector)!
      expect(body, selector).toMatch(/max-width:\s*min\([^)]*100%\)/)
      expect(body, selector).not.toMatch(/vw/)
    }
  })
})
