// The click-through disclosures must be square, and framed alike.
//
// WHY THIS FILE EXISTS. .capacity-disclosure and .cost-receipt-disclosure each
// carried `border-radius: 8px` with a 1px border while .descent-cost-disclosure
// was square with a 2px one, so the three panels behind the same click had three
// different frames. The radius was the real defect rather than the mismatch: the
// app is an 8-bit tribute -- "Press Start 2P" as the :root face, sprites drawn
// with image-rendering: pixelated, and `border-radius: 0` forced onto the
// control classes that would otherwise inherit rounding from the user agent --
// and an 8px radius is a modern-web idiom that reads as a mistake against it.
//
// This is the fourth time a wrong value has propagated between these panels by
// copy-paste: a dark prose colour reached seven declarations, a light-surface
// palette was pasted onto a navy background, typography was inherited by
// omission, and then this. The pattern is always the same -- a fourth panel gets
// pasted from one of these three and nobody looks at it -- so the guard is
// written the way disclosure-contrast.test.tsx and credits-layout.test.tsx are:
// cross-reference the stylesheet against the rendered components, so neither a
// new rule in styles.css nor a new block in App.tsx can slip past by being
// absent from a hand-written list here.
//
// WHY THE SCOPE IS THE DISCLOSURE OVERLAY AND NOT THE WHOLE STYLESHEET. A
// blanket "no border-radius anywhere" rule would need exactly one carve-out --
// .api-info-mark's `border-radius: 50%`, a 9px ring whose roundness is the only
// thing distinguishing it from the square .api-status-dot beside it in the same
// button -- and a rule whose allowlist is the interesting part is a rule nobody
// maintains. Inside the disclosures there are no legitimate exceptions at all,
// so this asserts an absolute within the region that actually had the bug, and
// stays silent about the rest of the app.
//
// WHY IT IS TESTED THIS WAY. jsdom does no layout and applies no stylesheet, so
// getComputedStyle returns nothing useful here and a rendered-geometry assertion
// would pass against a broken stylesheet. What can be checked is the invariant:
// resolve the declared geometry out of the stylesheet text.
//
// Comments are stripped before anything is matched. styles.css documents this
// very fix, so its prose contains `border-radius: 8px` and `border-radius: 0`
// verbatim; matching raw text would let the documentation fail -- or satisfy --
// the assertions that describe it.
//
// Real pixel measurement is a browser's job and lives outside the unit suite.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { cleanup, render } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import {
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

const CAPACITY = {
  matched: true,
  note: 'Both sides are hand-set to the same memory ceiling.',
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
  summary: 'One return to idle costs more on the opponent than on Lakebase.',
  note: 'Both floors are published on-demand rates.',
  illustrative_descents_per_day: 20,
  lanes: [
    {
      lane_id: 'lakebase',
      product: 'Lakebase',
      descends: true,
      floor_label: '60s vendor minimum',
      per_descent_display: '$0.0002',
      per_day_display: '$0.0024',
      derivation: '60s / 3600 x 0.5 CU x $0.026/CU-h',
      band_reason: 'CU floor is a range, so the figure is a bound.',
      rate_source: 'system.billing.list_prices pricing.effective_list.default',
    },
    {
      lane_id: 'competitor',
      product: 'Aurora Serverless v2',
      descends: false,
      floor_label: '300s AWS documented minimum',
      per_descent_display: '$0.0010',
      per_day_display: '$0.0120',
      derivation: '300s / 3600 x 0.5 ACU x $0.12/ACU-h',
      band_reason: 'AWS will not accept a timeout below 300s.',
      rate_source: 'AWS published pricing',
    },
  ],
}

const RECEIPT = {
  currency: 'USD',
  region: 'us-west-2',
  status: 'posted_partial',
  reconciliation_status: 'posted_partial',
  known_bout_estimate_usd: 0.005,
  known_monthly_carrying_cost_usd: 2.7,
  known_installation_overhead_usd: 0.41,
  original_estimate_usd: 0.004,
  posted_cost_usd: 0.005,
  variance_usd: 0.001,
  revision: 2,
  queried_at: '2026-08-20T02:00:00Z',
  posted_through: '2026-08-20T01:45:00Z',
  note: 'Revalidate the promotion before presenting.',
  lines: [
    {
      lane_id: 'lakebase',
      component: 'Lakebase compute',
      quantity: 0.019,
      unit: 'DBU',
      unit_rate_usd: 0.26,
      reference_list_unit_rate_usd: 0.52,
      subtotal_usd: 0.005,
      rate_basis: 'current_promotion',
      cadence: 'usage',
      status: 'usage_pending',
      scope: 'bout_estimate',
      source: 'system.billing.list_prices pricing.effective_list.default',
      source_as_of: '2026-08-20T01:35:04Z',
    },
    {
      lane_id: 'competitor',
      component: 'Aurora Serverless v2 ACU-hours',
      quantity: null,
      unit: 'ACU-hour',
      unit_rate_usd: 0.12,
      reference_list_unit_rate_usd: null,
      subtotal_usd: null,
      rate_basis: 'standard_list',
      cadence: 'month',
      status: 'usage_pending',
      scope: 'required_monthly_carrying_cost',
      source: 'AWS Price List API · AmazonRDS · OnDemand · us-west-2',
      source_as_of: '2025-08-28T15:38:04Z',
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
}

const session = {
  capacity: CAPACITY,
  descent_cost: DESCENT,
  cost_receipt: RECEIPT,
  // Real build_standing_cost_disclosure output. The panel renders nothing at
  // all without a payload, and a geometry walk over an absent panel would pass
  // by measuring three borders instead of four.
  standing_cost: standingCost.priced,
  corners: ['cost'],
} as unknown as DemoSession

/** All four disclosures, on the navy surface RingsideTake puts them on. */
function overlay() {
  return render(
    <div className="ringside-take">
      <CapacityDisclosure session={session} />
      <DescentCostDisclosure session={session} />
      <StandingCostDisclosure session={session} />
      <CostReceiptDisclosure session={session} />
    </div>,
  )
}

/** Does `node` match `selector`? Unsupported selectors are simply skipped. */
const matches = (node: Element, selector: string) => {
  try {
    return node.matches(selector)
  } catch {
    return false
  }
}

/**
 * The last declaration of `property` that matches `node`.
 *
 * Source order stands in for the full cascade, for the same reason
 * disclosure-contrast.test.tsx does it: these rules are single classes and
 * short descendant chains with no competing overrides.
 */
const declaredOn = (node: Element, property: RegExp) => {
  let found: string | null = null
  for (const rule of rules) {
    if (!rule.selectors.some((selector) => matches(node, selector))) continue
    const hit = property.exec(rule.body)
    if (hit) found = hit[1].trim()
  }
  return found
}

const RADIUS = /(?:^|[;{\s])border-radius\s*:\s*([^;}]+)/
/** `border: 3px solid #46527c` and `border-width: 3px` both land here. */
const BORDER_WIDTH = /(?:^|[;{\s])border(?:-width)?\s*:\s*(\d+)px/

/** Every value in the shorthand is zero, i.e. the corner is actually square. */
const isSquare = (value: string | null) =>
  value === null || value.split(/\s+/).every((part) => /^0(?:[a-z%]+)?$/.test(part))

afterEach(cleanup)

describe('disclosure corner geometry', () => {
  it('leaves every corner inside a disclosure square', () => {
    const { container } = overlay()
    const panels = [...container.querySelectorAll('details')]
    // Five: four panels, plus the remainder door nested inside the standing-cost
    // one that holds the three lanes an operator does not name. A nested
    // disclosure is still a disclosure, so it is held to the same geometry.
    expect(panels.length).toBe(5)

    const rounded: string[] = []
    let checked = 0

    for (const panel of panels) {
      for (const node of [panel, ...panel.querySelectorAll('*')]) {
        checked += 1
        const radius = declaredOn(node, RADIUS)
        if (!isSquare(radius)) {
          rounded.push(`${node.className || node.tagName.toLowerCase()} = ${radius}`)
        }
      }
    }

    // Sanity: if this ever empties, the cross-reference has broken and the
    // assertion below would pass vacuously. Both reported instances -- the two
    // panel roots -- are inside this walk, so it fails on the pre-fix file. The
    // panels contribute well over a hundred elements, so a floor of 60 also
    // fails if one of them stops being walked. (The nested door's children are
    // reached twice, by it and by its parent, which costs only repetition.)
    expect(checked).toBeGreaterThan(60)
    expect(rounded).toEqual([])
  })

  it('frames every panel behind a click identically', () => {
    const { container } = overlay()
    const panels = [...container.querySelectorAll('details')]

    const widths = panels.map((panel) => declaredOn(panel, BORDER_WIDTH))
    // One frame weight for every panel behind one click. 3px is the
    // panel-frame entry in this overlay's border vocabulary -- the weight
    // .ringside-script > div and .ringside-take nav button already use -- as
    // against 1px for a row rule and 2px for a divider inside a panel. The
    // fifth is the standing-cost remainder, framed rather than tucked in
    // because it is the reconciliation for the three lanes shown above it.
    expect(widths).toEqual(['3', '3', '3', '3', '3'])
  })

  it('keeps a rounded corner out of the disclosure styles entirely', () => {
    // Belt and braces, in the shape disclosure-contrast.test.tsx uses for the
    // light-surface palette: the DOM cross-reference above only reaches panels
    // that are rendered, and this reaches a fourth panel pasted from one of
    // these three before it is ever wired up.
    const region = css.slice(css.indexOf('.capacity-disclosure'))
    const declarations = [...region.matchAll(/border-radius\s*:\s*([^;}]+)/g)]
      .map(([, value]) => value.trim())
      .filter((value) => !isSquare(value))
    expect(declarations).toEqual([])
  })
})
